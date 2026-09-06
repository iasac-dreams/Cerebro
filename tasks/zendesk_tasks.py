"""Zendesk ticket creation background worker."""
from __future__ import annotations

import logging
import re
from flask import jsonify
from google.cloud import firestore
import requests
import config
from extensions import db
from clients.zendesk import find_or_create_user, search_ticket, create_solved_ticket, update_ticket_internal_note
from tasks.event_tasks import emit_event, enqueue_callback

logger = logging.getLogger(__name__)


def resolve_message_content(message: dict) -> str:
    recipient = message.get("recipient") or {}
    snapshot = recipient.get("template_snapshot") or message.get("template_snapshot") or {}
    if snapshot.get("body"):
        parts = []
        if snapshot.get("header"):
            parts.append(snapshot["header"])
        parts.append(snapshot["body"])
        if snapshot.get("footer"):
            parts.append(snapshot["footer"])
        return "\n\n".join(parts)

    template_name = message.get("template_name") or recipient.get("template_name")
    if template_name:
        try:
            for lang in ("es", "es_LA", "es_ES", "en"):
                key = config.doc_id(template_name, lang)
                snap = db().collection(config.TEMPLATES).document(key).get()
                if snap.exists:
                    td = snap.to_dict() or {}
                    body_text = td.get("body_text") or ""
                    sunshine = message.get("sunshine_payload") or {}
                    msg_obj = sunshine.get("message") or {}
                    tpl_obj = msg_obj.get("template") or {}
                    values = []
                    for comp in tpl_obj.get("components") or []:
                        if str(comp.get("type", "")).lower() == "body":
                            for param in comp.get("parameters") or []:
                                if isinstance(param, dict) and param.get("type") == "text":
                                    values.append(str(param.get("text") or ""))
                    if body_text:
                        rendered = body_text
                        for idx, val in enumerate(values, start=1):
                            rendered = re.sub(r"\{\{\s*" + str(idx) + r"\s*\}\}", val, rendered)
                        parts = []
                        if td.get("header_text"):
                            parts.append(td["header_text"])
                        parts.append(rendered)
                        if td.get("footer_text"):
                            parts.append(td["footer_text"])
                        return "\n\n".join(parts)
        except Exception as e:
            logger.warning("Error resolving template content: %s", e)

    sunshine = message.get("sunshine_payload") or {}
    msg_obj = sunshine.get("message") or {}
    if msg_obj.get("text"):
        return str(msg_obj["text"])

    return "WhatsApp entregado al destinatario."


def format_subject(template: str, message: dict) -> str:
    recipient = message.get("recipient") or {}
    values = {
        "campaign_name": (message.get("campaign") or {}).get("campaign_name") or message.get("campaign_id"),
        "name": recipient.get("name") or "Cliente",
        "template_name": message.get("template_name") or "",
    }
    result = str(template or "WhatsApp entregado - {campaign_name} - {name}")
    for key, value in values.items():
        result = result.replace("{" + key + "}", config.safe_text(value, 200))
    return result[:255]


def handle_zendesk_task(body: dict):
    message_id = str(body.get("message_id") or "")
    message_ref = db().collection(config.MESSAGES).document(message_id)
    snapshot = message_ref.get()
    if not snapshot.exists:
        return jsonify({"error": "message_not_found"}), 404
    message = snapshot.to_dict() or {}
    if message.get("ticket_id"):
        return jsonify({"ticket_id": message["ticket_id"], "duplicate_task": True})

    message_content = resolve_message_content(message)
    campaign_name = (message.get("campaign") or {}).get("campaign_name") or message.get("campaign_id") or "WhatsApp"
    comment_body = (
        f"WhatsApp entregado al usuario:\n\n"
        f"{message_content}\n\n"
        f"----------------------------------------\n"
        f"Campaña: {campaign_name}\n"
        f"Run: {message.get('run_id') or 'N/A'}\n"
        f"Notification ID: {message.get('notification_id') or 'N/A'}"
    )

    # 1. Verificar si el mensaje proviene de un disparador con ticket_id existente
    sunshine_payload = message.get("sunshine_payload") or {}
    metadata = sunshine_payload.get("metadata") if isinstance(sunshine_payload.get("metadata"), dict) else {}
    if not metadata and isinstance(message.get("metadata"), dict):
        metadata = message.get("metadata") or {}
    origin_ticket_id = metadata.get("ticket_id") or metadata.get("ticketId")
    if not origin_ticket_id and str(message.get("source_reference") or "").isdigit():
        origin_ticket_id = message.get("source_reference")

    if origin_ticket_id:
        try:
            if update_ticket_internal_note(origin_ticket_id, comment_body, ["cerebro_sunshine", "sin_disparo_whatsapp"]):
                tid = int(origin_ticket_id)
                message_ref.set({"ticket_id": tid, "ticket_updated_at": firestore.SERVER_TIMESTAMP, "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
                message["ticket_id"] = tid
                ev_id = emit_event(message, "zendesk_ticket_updated", {"ticket_id": tid})
                enqueue_callback(message, ev_id, "zendesk_ticket_updated", {"ticket_id": tid})
                return jsonify({"ticket_id": tid, "updated": True})
        except Exception as e:
            logger.warning("No se pudo actualizar ticket origen %s, se creará uno nuevo: %s", origin_ticket_id, e)

    # 2. Si no viene ticket_id previo, crear un nuevo ticket resuelto para trazabilidad
    recipient = message.get("recipient") or {}
    external_id = f"cerebro-{message_id}"
    try:
        existing_ticket_id = search_ticket(external_id)
        if existing_ticket_id:
            message_ref.set({"ticket_id": existing_ticket_id, "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
            message["ticket_id"] = existing_ticket_id
            recovered_event_id = emit_event(
                message, "zendesk_ticket_created", {"ticket_id": existing_ticket_id, "recovered": True}
            )
            enqueue_callback(message, recovered_event_id, "zendesk_ticket_created", {"ticket_id": existing_ticket_id})
            return jsonify({"ticket_id": existing_ticket_id, "recovered": True})

        requester_id = find_or_create_user(recipient)
        cfg = (message.get("campaign") or {}).get("zendesk") or {}
        tags = [config.safe_text(tag, 100) for tag in cfg.get("tags") or []]
        tags.extend(["cerebro_sunshine", "sin_disparo_whatsapp"])
        ticket_payload = {
            "external_id": external_id,
            "requester_id": requester_id,
            "subject": format_subject(cfg.get("subject"), message),
            "comment": {
                "body": comment_body,
                "public": False,
            },
            "tags": sorted(set(tags)),
            "status": "solved",
        }
        ticket_id = create_solved_ticket(ticket_payload)
        message_ref.set(
            {"ticket_id": ticket_id, "ticket_created_at": firestore.SERVER_TIMESTAMP, "updated_at": firestore.SERVER_TIMESTAMP},
            merge=True,
        )
        message["ticket_id"] = ticket_id
        ticket_event_id = emit_event(message, "zendesk_ticket_created", {"ticket_id": ticket_id})
        enqueue_callback(message, ticket_event_id, "zendesk_ticket_created", {"ticket_id": ticket_id})
        return jsonify({"ticket_id": ticket_id})
    except requests.RequestException:
        logger.exception("Zendesk OAuth request failed")
        return jsonify({"error": "zendesk_temporary_failure"}), 503
