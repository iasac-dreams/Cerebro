"""Message sanitization, normalization, payload assembly and persistence."""
from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib.parse import urlparse

import config
from extensions import db, enqueue_task
from clients.sunshine import sunshine_wire_payload
from services.template_service import (
    approved_template,
    render_template_snapshot,
    template_body_parameter_count,
    template_name_from_payload,
    template_namespace,
)
from google.api_core.exceptions import AlreadyExists
from google.cloud import firestore

logger = logging.getLogger(__name__)


def ensure_message_dispatch(message: dict) -> None:
    """Repair an interrupted enqueue without changing the message's delivery state."""
    from tasks.event_tasks import emit_event

    message_id = message["message_id"]
    ref = db().collection(config.MESSAGES).document(message_id)
    if message.get("status") == "queued" and not message.get("dispatch_enqueued_at"):
        enqueue_task(
            config.SUNSHINE_QUEUE, "/internal/tasks/sunshine",
            {"kind": "send", "message_id": message_id}, f"send-{message_id}",
        )
        # AlreadyExists also confirms the deterministic task was created.
        ref.update({"dispatch_enqueued_at": firestore.SERVER_TIMESTAMP})
    if not message.get("queued_event_recorded_at"):
        emit_event(message, "queued", event_id=config.doc_id("queued", message_id))
        ref.update({"queued_event_recorded_at": firestore.SERVER_TIMESTAMP})


def mask_phone(phone: Any) -> str:
    return config.mask_phone(phone)


def normalize_ticket_id(value: Any) -> int | None:
    return config.normalize_ticket_id(value)


def normalize_https_url(value: Any) -> str:
    return config.normalize_https_url(value)


def sanitize_template_parameter(param: dict) -> dict:
    if not isinstance(param, dict):
        raise ValueError("invalid_template_parameter")
    ptype = str(param.get("type") or "").strip().lower()
    if ptype == "text":
        return {"type": "text", "text": config.safe_text(param.get("text"), 1000)}
    elif ptype == "image":
        img = param.get("image") if isinstance(param.get("image"), dict) else {}
        link = normalize_https_url(img.get("link") or param.get("link"))
        return {"type": "image", "image": {"link": link}}
    elif ptype == "document":
        doc = param.get("document") if isinstance(param.get("document"), dict) else {}
        link = normalize_https_url(doc.get("link") or param.get("link"))
        res = {"link": link}
        if doc.get("filename"):
            res["filename"] = config.safe_text(doc["filename"], 255)
        return {"type": "document", "document": res}
    elif ptype == "video":
        vid = param.get("video") if isinstance(param.get("video"), dict) else {}
        link = normalize_https_url(vid.get("link") or param.get("link"))
        return {"type": "video", "video": {"link": link}}
    elif ptype == "payload":
        return {"type": "payload", "payload": config.safe_text(param.get("payload"), 1000)}
    else:
        raise ValueError(f"invalid_template_parameter_type: {ptype}" if ptype else "invalid_template_parameter_type")


def sanitize_template_components(components: list) -> list:
    if not isinstance(components, list):
        return []
    sanitized = []
    for comp in components:
        if not isinstance(comp, dict):
            continue
        ctype = str(comp.get("type") or "").strip().lower()
        if ctype == "header":
            raw_params = comp.get("parameters") or []
            if not isinstance(raw_params, list):
                raise ValueError("invalid_template_component")
            params = [sanitize_template_parameter(p) for p in raw_params if isinstance(p, dict)]
            sanitized.append({"type": "header", "parameters": params})
        elif ctype == "body":
            raw_params = comp.get("parameters") or []
            if not isinstance(raw_params, list):
                raise ValueError("invalid_template_component")
            params = []
            for p in raw_params:
                if isinstance(p, dict):
                    sp = sanitize_template_parameter(p)
                    if sp.get("type") != "text":
                        raise ValueError("invalid_template_component")
                    params.append(sp)
            sanitized.append({"type": "body", "parameters": params})
        elif ctype == "button":
            sub_type = str(comp.get("sub_type") or "url").strip().lower()
            if sub_type not in ("url", "quick_reply"):
                sub_type = "url"
            index = str(comp.get("index") or "0").strip()
            if not index.isdigit() or not (0 <= int(index) <= 9):
                raise ValueError("invalid_template_component")
            raw_params = comp.get("parameters") or []
            params = [sanitize_template_parameter(p) for p in raw_params if isinstance(p, dict)]
            sanitized.append({
                "type": "button",
                "sub_type": sub_type,
                "index": index,
                "parameters": params,
            })
        else:
            raise ValueError("invalid_template_component")
    return sanitized


def sanitize_scalar_metadata(metadata: dict) -> dict:
    if not isinstance(metadata, dict):
        return {}
    clean = {}
    for k, v in metadata.items():
        key = config.safe_text(k, 100)
        if not key or key.lower() in ("ticket_id", "ticketid"):
            continue
        if isinstance(v, (str, int, float, bool)):
            clean[key] = config.safe_text(v, 1000) if isinstance(v, str) else v
    serialized = json.dumps(clean, ensure_ascii=False)
    if len(serialized.encode("utf-8")) > 4000:
        raise ValueError("metadata_too_large")
    return clean


def parse_incoming_notification(raw: dict, app_id: str | None = None) -> dict:
    if not isinstance(raw, dict):
        raise ValueError("json_object_required")
    effective_app = app_id or config.SUNSHINE_APP_ID
    if config.SUNSHINE_APP_ID and effective_app != config.SUNSHINE_APP_ID:
        raise ValueError("unknown_sunshine_app")
    return raw


def normalize_incoming_notification(raw: dict, app_id: str | None = None) -> dict:
    raw = parse_incoming_notification(raw, app_id)
    destination = raw.get("destination") if isinstance(raw.get("destination"), dict) else {}
    message = raw.get("message") if isinstance(raw.get("message"), dict) else {}
    if not message:
        raise ValueError("message_required")

    phone_raw = destination.get("destinationId") or raw.get("phone") or raw.get("destinationId")
    phone = config.normalize_phone(phone_raw)

    integration_id = config.safe_text(destination.get("integrationId") or config.SUNSHINE_INTEGRATION_ID, 200)

    raw_metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
    raw_tid = raw.get("ticket_id") or raw.get("ticket") or raw_metadata.get("ticket_id") or raw_metadata.get("ticketId")
    ticket_id = normalize_ticket_id(raw_tid)

    msg_type = str(message.get("type") or "template").strip().lower()
    if msg_type not in ("template", "text"):
        raise ValueError("invalid_message_type")

    template_name = ""
    template_lang = "es"
    template_namespace_val = ""
    template_components = []
    text_content = ""

    if msg_type == "template":
        tmpl = message.get("template") if isinstance(message.get("template"), dict) else {}
        template_name = config.safe_text(tmpl.get("name") or message.get("template_name"), 200)
        if not template_name:
            raise ValueError("template_name_required")
        lang_val = tmpl.get("language")
        if isinstance(lang_val, dict):
            template_lang = config.safe_text(lang_val.get("code") or "es", 20)
        elif isinstance(lang_val, str) and lang_val:
            template_lang = config.safe_text(lang_val, 20)
        else:
            template_lang = "es"

        template_namespace_val = config.SUNSHINE_NAMESPACE or config.safe_text(tmpl.get("namespace"), 200)
        template_components = sanitize_template_components(tmpl.get("components") or [])
    else:
        text_content = config.safe_text(message.get("text"), 4000)
        if not text_content:
            raise ValueError("text_required")

    clean_metadata = sanitize_scalar_metadata(raw_metadata)

    return {
        "ticket_id": ticket_id,
        "destination_phone": phone,
        "integration_id": integration_id,
        "message_type": msg_type,
        "template_name": template_name,
        "template_language": template_lang,
        "template_namespace": template_namespace_val,
        "template_components": template_components,
        "text_content": text_content,
        "metadata": clean_metadata,
        "external_id": config.safe_text(clean_metadata.get("external_id") or raw_metadata.get("external_id") or phone, 200),
        "name": config.safe_text(clean_metadata.get("name") or raw_metadata.get("name") or "Cliente", 200),
        "email": config.normalize_email(clean_metadata.get("email") or raw_metadata.get("email")),
    }


def validate_incoming_notification(normalized: dict) -> dict:
    if normalized["message_type"] == "template":
        tmpl_name = normalized["template_name"]
        tmpl_lang = normalized["template_language"]
        tmpl_record = approved_template(tmpl_name, tmpl_lang)

        body_values = []
        for comp in normalized["template_components"]:
            if comp.get("type") == "body":
                for p in comp.get("parameters") or []:
                    if p.get("type") == "text":
                        body_values.append(p.get("text", ""))

        expected_values = template_body_parameter_count(tmpl_record) if tmpl_record else 0
        if tmpl_record and len(body_values) != expected_values:
            raise ValueError("template_body_parameter_count_mismatch")

        snapshot = render_template_snapshot(tmpl_record, body_values) if tmpl_record else {}
        normalized["template_snapshot"] = snapshot
        normalized["body_values"] = body_values
    else:
        normalized["template_snapshot"] = {}
        normalized["body_values"] = []
    return normalized


def build_sunshine_notification(normalized: dict) -> dict:
    dest = {"destinationId": normalized["destination_phone"]}
    if normalized.get("integration_id"):
        dest["integrationId"] = normalized["integration_id"]
    payload = {
        "destination": dest,
        "author": {
            "role": "appMaker",
        },
    }
    if normalized["message_type"] == "template":
        payload["messageSchema"] = "whatsapp"
        tmpl_obj = {
            "name": normalized["template_name"],
            "language": {
                "policy": "deterministic",
                "code": normalized["template_language"],
            },
            "components": normalized["template_components"],
        }
        if normalized.get("template_namespace"):
            tmpl_obj["namespace"] = normalized["template_namespace"]
        payload["message"] = {
            "type": "template",
            "template": tmpl_obj,
        }
    else:
        payload["message"] = {
            "type": "text",
            "text": normalized["text_content"],
        }
    if normalized.get("metadata"):
        payload["metadata"] = normalized["metadata"]

    sunshine_wire_payload(payload)
    return payload


def sanitize_legacy_payload(raw: dict, app_id: str | None = None) -> tuple[dict, dict]:
    normalized = normalize_incoming_notification(raw, app_id)
    validated = validate_incoming_notification(normalized)
    payload = build_sunshine_notification(validated)

    recipient_info = {
        "phone": validated["destination_phone"],
        "name": validated["name"],
        "email": validated["email"],
        "external_id": validated["external_id"],
        "template_name": validated["template_name"],
        "template_snapshot": validated.get("template_snapshot") or {},
    }
    if validated.get("ticket_id"):
        recipient_info["ticket_id"] = validated["ticket_id"]
    return payload, recipient_info


def campaign_sunshine_payload(message: dict) -> tuple[dict, dict]:
    recipient = message.get("recipient") if isinstance(message.get("recipient"), dict) else {}
    template_config = message.get("template") if isinstance(message.get("template"), dict) else {}
    variables = message.get("variables") if isinstance(message.get("variables"), dict) else {}
    phone = config.normalize_phone(recipient.get("phone"))
    template_name = config.safe_text(template_config.get("name"), 200)
    if not template_name:
        raise ValueError("template_name_required")
    language = config.safe_text(template_config.get("language") or "es", 20)
    template_record = approved_template(template_name, language)
    native_components = []
    body_values = []
    for component in template_config.get("components") or []:
        if not isinstance(component, dict):
            continue
        component_type = config.safe_text(component.get("type"), 20).lower()
        parameter_names = component.get("parameters") or []
        if component_type == "header" and str(component.get("format", "")).lower() == "image":
            variable_name = config.safe_text(component.get("parameter") or "header_image", 100)
            image_url = config.safe_text(variables.get(variable_name), 2000)
            if not image_url.startswith("https://"):
                raise ValueError("invalid_header_image")
            native_components.append({"type": "header", "parameters": [{"type": "image", "image": {"link": image_url}}]})
        elif component_type == "body":
            values = [config.safe_text(variables.get(str(name)), 1000) for name in parameter_names]
            body_values.extend(values)
            native_components.append({
                "type": "body",
                "parameters": [{"type": "text", "text": value} for value in values],
            })
        elif component_type == "button":
            native_components.append({
                "type": "button",
                "sub_type": config.safe_text(component.get("sub_type") or "url", 20),
                "index": config.safe_text(component.get("index") or "0", 5),
                "parameters": [
                    {"type": "text", "text": config.safe_text(variables.get(str(name)), 1000)}
                    for name in parameter_names
                ],
            })
    template = {
        "name": template_name,
        "language": {"policy": "deterministic", "code": language},
        "components": native_components,
    }
    namespace = template_namespace()
    if namespace:
        template["namespace"] = namespace
    expected_values = template_body_parameter_count(template_record) if template_record else 0
    if template_record and len(body_values) != expected_values:
        raise ValueError("template_body_parameter_count_mismatch")
    payload = {
        "destination": {"integrationId": config.SUNSHINE_INTEGRATION_ID, "destinationId": phone},
        "author": {"role": "appMaker"},
        "messageSchema": "whatsapp",
        "message": {"type": "template", "template": template},
        "metadata": {
            "source_reference": config.safe_text(message.get("source_reference"), 200),
            "external_id": config.safe_text(recipient.get("external_id"), 200),
        },
    }
    sunshine_wire_payload(payload)
    return payload, {
        "phone": phone,
        "name": config.safe_text(recipient.get("name") or "Cliente", 200),
        "email": config.normalize_email(recipient.get("email")),
        "external_id": config.safe_text(recipient.get("external_id") or message.get("source_reference") or phone, 200),
        "template_name": template_name,
        "template_snapshot": render_template_snapshot(template_record, body_values) if template_record else {},
    }


def store_message(
    *,
    campaign_id: str,
    run_id: str,
    idempotency_key: str,
    source_reference: str,
    payload: dict,
    recipient: dict,
    campaign: dict,
    source_channel: str,
) -> tuple[dict, bool]:
    message_id = config.doc_id(campaign_id, run_id, idempotency_key)
    wire = sunshine_wire_payload(payload)
    payload_hash = config.doc_id(wire)
    origin_ticket_id = recipient.get("ticket_id") or (campaign.get("zendesk") or {}).get("ticket_id")
    message = {
        "message_id": message_id,
        "campaign_id": campaign_id,
        "run_id": run_id,
        "idempotency_key": idempotency_key,
        "source_reference": source_reference,
        "source_channel": source_channel,
        "recipient": recipient,
        "template_name": recipient.get("template_name"),
        "origin_ticket_id": origin_ticket_id,
        "sunshine_payload": payload,
        "payload_hash": payload_hash,
        "campaign": campaign,
        "status": "queued",
        "created_at": config.utcnow(),
        "updated_at": config.utcnow(),
        "expires_at": config.expires_at(),
    }
    try:
        db().collection(config.MESSAGES).document(message_id).create(message)
        duplicate = False
    except AlreadyExists:
        existing = db().collection(config.MESSAGES).document(message_id).get().to_dict()
        if not existing:
            raise RuntimeError("message_disappeared_during_ingestion")
        ensure_message_dispatch(existing)
        logger.info(
            "[SUNSHINE DUPLICATE] message_id=%s run_id=%s idempotency_key=%s",
            message_id,
            run_id,
            idempotency_key,
        )
        return existing, True

    ensure_message_dispatch(message)
    logger.info(
        "[SUNSHINE MESSAGE STORED] message_id=%s status=%s duplicate=%s ticket_id=%s phone=%s",
        message_id,
        message["status"],
        duplicate,
        origin_ticket_id or "-",
        config.mask_phone(recipient.get("phone")),
    )
    return message, False
