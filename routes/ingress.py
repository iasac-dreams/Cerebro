"""Ingress routes protected by Gateway mutual authentication."""
from __future__ import annotations

from flask import Blueprint, jsonify, request
from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists
import config
from extensions import db, enqueue_task, canonical_json
from security.decorators import require_gateway
from services.message_service import sanitize_legacy_payload, store_message
from services.batch_service import save_incoming_batch
from services.reporting_service import run_report, schedule_report

ingress_bp = Blueprint("ingress", __name__)


@ingress_bp.get("/templates")
@require_gateway
def ingress_templates():
    items = []
    for snapshot in db().collection(config.TEMPLATES).where("status", "==", "APPROVED").stream():
        item = snapshot.to_dict() or {}
        item.pop("raw", None)
        items.append(item)
    return jsonify({"templates": items, "cached": True})


@ingress_bp.post("/legacy/apps/<app_id>/notifications")
@require_gateway
def ingress_legacy(app_id: str):
    raw = request.get_json(silent=True)
    if not isinstance(raw, dict):
        return jsonify({"error": "json_object_required"}), 400
    try:
        payload, recipient = sanitize_legacy_payload(raw, app_id)
        metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
        ticket_id = recipient.get("ticket_id")
        source_reference = config.safe_text(
            str(ticket_id) if ticket_id else (metadata.get("source_reference") or recipient["external_id"]),
            200,
        )
        idempotency_key = config.safe_text(metadata.get("idempotency_key") or config.doc_id(canonical_json(payload)), 200)
        now_key = config.utcnow().strftime("%Y%m%d")
        campaign = {
            "campaign_name": config.safe_text(metadata.get("campaign_name") or "Zendesk legacy", 200),
            "zendesk": {
                "create_ticket_on": "delivered",
                "subject": "WhatsApp entregado - {name}",
                "tags": ["cerebro_sunshine", "sin_disparo_whatsapp"],
            },
        }
        if ticket_id:
            campaign["zendesk"]["ticket_id"] = ticket_id
        message, duplicate = store_message(
            campaign_id="zendesk-legacy",
            run_id=f"zendesk-legacy-{now_key}",
            idempotency_key=idempotency_key,
            source_reference=source_reference,
            payload=payload,
            recipient=recipient,
            campaign=campaign,
            source_channel="zendesk_legacy",
        )
        return jsonify({
            "message_id": message["message_id"],
            "status": message.get("status"),
            "duplicate": duplicate,
        }), 200 if duplicate else 202
    except ValueError as error:
        return jsonify({"error": str(error)}), 413 if str(error) == "sunshine_payload_too_large" else 400


@ingress_bp.post("/campaigns/<campaign_id>/messages:batch")
@require_gateway
def ingress_campaign(campaign_id: str):
    """Asynchronous batch campaign ingestion with sub-150ms response."""
    try:
        campaign_id = config.assert_identifier(campaign_id, "campaign_id")
        body = request.get_json(silent=True)
        if not isinstance(body, dict):
            raise ValueError("json_object_required")
        run_id = config.assert_identifier(body.get("run_id"), "run_id")
        campaign = body.get("campaign") if isinstance(body.get("campaign"), dict) else {}
        raw_messages = body.get("messages")
        if not isinstance(raw_messages, list) or not raw_messages:
            raise ValueError("messages_required")
        if len(raw_messages) > config.MAX_BATCH_MESSAGES:
            raise ValueError("batch_too_large")

        # Fast async batching: saves chunks in cerebro_batches with 1-day TTL and returns 202
        response_data = save_incoming_batch(
            campaign_id=campaign_id,
            run_id=run_id,
            campaign=campaign,
            raw_messages=raw_messages,
            sealed=bool(body.get("sealed")),
        )
        return jsonify(response_data), 202
    except ValueError as error:
        conflicts = {"campaign_mismatch", "campaign_configuration_mismatch", "run_sealed", "batch_expired"}
        return jsonify({"error": str(error)}), 409 if str(error) in conflicts else 400


@ingress_bp.post("/campaigns/<campaign_id>/runs/<run_id>:seal")
@require_gateway
def ingress_seal(campaign_id: str, run_id: str):
    try:
        config.assert_identifier(campaign_id, "campaign_id")
        config.assert_identifier(run_id, "run_id")
    except ValueError as error:
        return jsonify({"error": str(error)}), 400
    ref = db().collection(config.RUNS).document(run_id)
    snapshot = ref.get()
    if not snapshot.exists:
        return jsonify({"error": "run_not_found"}), 404
    run = snapshot.to_dict() or {}
    if run.get("campaign_id") != campaign_id:
        return jsonify({"error": "campaign_mismatch"}), 409
    ref.set({"sealed": True, "sealed_at": firestore.SERVER_TIMESTAMP}, merge=True)
    schedule_report(run_id, run.get("campaign") or {})
    return jsonify({"run_id": run_id, "status": "report_scheduled"}), 202


@ingress_bp.get("/runs/<run_id>")
@require_gateway
def ingress_run(run_id: str):
    snapshot = db().collection(config.RUNS).document(run_id).get()
    if not snapshot.exists:
        return jsonify({"error": "run_not_found"}), 404
    return jsonify(config.serialize(snapshot.to_dict()))


@ingress_bp.get("/runs/<run_id>/report")
@require_gateway
def ingress_report(run_id: str):
    try:
        return jsonify(run_report(run_id))
    except KeyError:
        return jsonify({"error": "run_not_found"}), 404


@ingress_bp.post("/webhooks/sunshine")
@require_gateway
def ingress_sunshine_webhook():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        return jsonify({"error": "json_object_required"}), 400
    incoming = body.get("events") if isinstance(body.get("events"), list) else [body]
    accepted = 0
    for event in incoming:
        if not isinstance(event, dict):
            continue
        event_id = config.doc_id(canonical_json(event))
        ref = db().collection(config.EVENTS).document(event_id)
        try:
            ref.create({
                "event_id": event_id,
                "raw": event,
                "status": "webhook_received",
                "event_at": config.utcnow(),
                "expires_at": config.expires_at(),
            })
            enqueue_task(
                config.EVENT_QUEUE,
                "/internal/tasks/event",
                {"event_id": event_id},
                f"event-{event_id}",
            )
            accepted += 1
        except AlreadyExists:
            pass
    return jsonify({"accepted": accepted, "duplicates": len(incoming) - accepted}), 202
