"""Message sanitization, normalization, payload assembly and persistence."""
from __future__ import annotations

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


def legacy_body_values(message: dict) -> list[str]:
    template = message.get("template") if isinstance(message.get("template"), dict) else {}
    for component in template.get("components") or []:
        if isinstance(component, dict) and str(component.get("type") or "").lower() == "body":
            return [
                config.safe_text(parameter.get("text"), 1000)
                for parameter in component.get("parameters") or []
                if isinstance(parameter, dict) and parameter.get("type") == "text"
            ]
    return []


def sanitize_legacy_payload(raw: dict, app_id: str | None = None) -> tuple[dict, dict]:
    app_id = app_id or config.SUNSHINE_APP_ID
    if config.SUNSHINE_APP_ID and app_id != config.SUNSHINE_APP_ID:
        raise ValueError("unknown_sunshine_app")
    destination = raw.get("destination") if isinstance(raw.get("destination"), dict) else {}
    message = raw.get("message") if isinstance(raw.get("message"), dict) else {}
    if not message:
        raise ValueError("message_required")
    integration_id = config.safe_text(destination.get("integrationId") or config.SUNSHINE_INTEGRATION_ID, 200)
    if config.SUNSHINE_INTEGRATION_ID and integration_id and integration_id != config.SUNSHINE_INTEGRATION_ID:
        raise ValueError("unknown_sunshine_integration")
    phone_raw = destination.get("destinationId") or raw.get("phone") or raw.get("destinationId")
    phone = config.normalize_phone(phone_raw)
    payload = {
        "destination": {"integrationId": integration_id, "destinationId": phone},
        "author": {"role": "appMaker"},
        "message": message,
    }
    if raw.get("messageSchema"):
        payload["messageSchema"] = config.safe_text(raw.get("messageSchema"), 30)
    else:
        payload["messageSchema"] = "whatsapp"
    if isinstance(raw.get("metadata"), dict):
        payload["metadata"] = raw["metadata"]
    sunshine_wire_payload(payload)
    metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
    detected_template = template_name_from_payload(payload)
    language_obj = ((message.get("template") or {}).get("language") or {}) if isinstance(message.get("template"), dict) else {}
    detected_language = config.safe_text(language_obj.get("code") if isinstance(language_obj, dict) else language_obj, 20) or "es"
    template_record = approved_template(detected_template, detected_language)
    body_values = legacy_body_values(message)
    expected_values = template_body_parameter_count(template_record) if template_record else 0
    if template_record and len(body_values) != expected_values:
        raise ValueError("template_body_parameter_count_mismatch")
    raw_tid = raw.get("ticket_id") or raw.get("ticket") or metadata.get("ticket_id") or metadata.get("ticketId")
    ticket_id = None
    if raw_tid is not None:
        try:
            ticket_id = int(str(raw_tid).strip())
        except (ValueError, TypeError):
            ticket_id = None

    recipient_info = {
        "phone": phone,
        "name": config.safe_text(metadata.get("name") or "Cliente", 200),
        "email": config.normalize_email(metadata.get("email")),
        "external_id": config.safe_text(metadata.get("external_id") or phone, 200),
        "template_name": detected_template,
        "template_snapshot": render_template_snapshot(template_record, body_values) if template_record else {},
    }
    if ticket_id:
        recipient_info["ticket_id"] = ticket_id
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
    message = {
        "message_id": message_id,
        "campaign_id": campaign_id,
        "run_id": run_id,
        "idempotency_key": idempotency_key,
        "source_reference": source_reference,
        "source_channel": source_channel,
        "recipient": recipient,
        "template_name": recipient.get("template_name"),
        "origin_ticket_id": recipient.get("ticket_id") or (campaign.get("zendesk") or {}).get("ticket_id"),
        "sunshine_payload": payload,
        "campaign": campaign,
        "status": "queued",
        "created_at": config.utcnow(),
        "updated_at": config.utcnow(),
        "expires_at": config.expires_at(),
    }
    try:
        db().collection(config.MESSAGES).document(message_id).create(message)
    except AlreadyExists:
        existing = db().collection(config.MESSAGES).document(message_id).get().to_dict()
        if not existing:
            raise RuntimeError("message_disappeared_during_ingestion")
        ensure_message_dispatch(existing)
        return existing, True

    ensure_message_dispatch(message)
    return message, False
