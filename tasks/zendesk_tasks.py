"""Zendesk ticket creation background worker."""
from __future__ import annotations

import logging
from flask import jsonify
from google.cloud import firestore
import requests
import config
from extensions import db
from clients.zendesk import find_or_create_user, search_ticket, create_solved_ticket
from tasks.event_tasks import emit_event, enqueue_callback

logger = logging.getLogger(__name__)


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
                "body": (
                    f"WhatsApp entregado al usuario.\n"
                    f"Campaña: {message.get('campaign_id')}\n"
                    f"Run: {message.get('run_id')}\n"
                    f"Plantilla: {message.get('template_name')}\n"
                    f"Notification ID: {message.get('notification_id')}"
                ),
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
