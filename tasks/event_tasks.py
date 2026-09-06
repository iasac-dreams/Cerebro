"""Lifecycle event emission, webhook processing, callbacks and report delivery."""
from __future__ import annotations

import uuid
from datetime import timedelta
from flask import jsonify
from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists
import requests
import config
from extensions import db, enqueue_task, http_session
from services.reporting_service import run_report


def emit_event(message: dict, status: str, details: dict | None = None, *, event_id: str | None = None) -> str:
    event_id = event_id or str(uuid.uuid4())
    event = {
        "event_id": event_id,
        "message_id": message.get("message_id"),
        "run_id": message.get("run_id"),
        "campaign_id": message.get("campaign_id"),
        "source_reference": message.get("source_reference"),
        "template_name": message.get("template_name"),
        "status": status,
        "details": details or {},
        "event_at": config.utcnow(),
        "expires_at": config.expires_at(),
    }
    try:
        db().collection(config.EVENTS).document(event_id).create(event)
    except AlreadyExists:
        pass
    enqueue_task(
        config.ANALYTICS_QUEUE,
        "/internal/tasks/analytics",
        {"event_id": event_id},
        f"analytics-{event_id}",
    )
    return event_id


def enqueue_callback(message: dict, event_id: str, status: str, details: dict | None = None):
    campaign = message.get("campaign") or {}
    callback_url = config.allowed_callback_url(campaign.get("callback_url"))
    if not callback_url:
        return
    external_status = {
        "submitted": "provider_accepted",
        "user_delivered": "delivered",
        "delivery_unknown": "timed_out",
        "conversation_locked": "rejected",
        "zendesk_ticket_created": "ticket_created",
    }.get(status, status)
    detail = details or {}
    enqueue_task(
        config.EVENT_QUEUE,
        "/internal/tasks/callback",
        {
            "callback_url": callback_url,
            "event_id": event_id,
            "message_id": message.get("message_id"),
            "run_id": message.get("run_id"),
            "source_reference": message.get("source_reference"),
            "campaign_id": message.get("campaign_id"),
            "status": external_status,
            "notification_id": message.get("notification_id"),
            "ticket_id": detail.get("ticket_id"),
            "error": detail.get("error"),
        },
        f"callback-{event_id}",
    )


def enqueue_ticket(message: dict, event_id: str):
    zendesk = (message.get("campaign") or {}).get("zendesk") or {}
    if zendesk.get("create_ticket_on") not in {"delivered", "user_delivered"}:
        return
    enqueue_task(
        config.ZENDESK_QUEUE,
        "/internal/tasks/zendesk",
        {"message_id": message["message_id"], "event_id": event_id},
        f"ticket-{message['message_id']}",
    )


def handle_event_task(body: dict):
    event_id = str(body.get("event_id") or "")
    event_ref = db().collection(config.EVENTS).document(event_id)
    snapshot = event_ref.get()
    if not snapshot.exists:
        return jsonify({"error": "event_not_found"}), 404
    stored = snapshot.to_dict() or {}
    if stored.get("processed_at"):
        return jsonify({"status": "already_processed"})
    event = stored.get("raw") or {}
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else event
    notification = payload.get("notification") if isinstance(payload.get("notification"), dict) else event.get("notification") or {}
    notification_id = str(notification.get("id") or notification.get("_id") or "")
    if not notification_id:
        event_ref.set({"processed_at": firestore.SERVER_TIMESTAMP, "result": "missing_notification_id"}, merge=True)
        return jsonify({"status": "ignored"})
    index = db().collection(config.NOTIFICATION_INDEX).document(notification_id).get()
    if not index.exists:
        return jsonify({"error": "notification_index_pending"}), 503
    message_ref = db().collection(config.MESSAGES).document(index.to_dict()["message_id"])
    message_snapshot = message_ref.get()
    if not message_snapshot.exists:
        return jsonify({"error": "message_not_found"}), 404
    message = message_snapshot.to_dict() or {}
    trigger = str(event.get("trigger") or event.get("type") or "")
    status_map = {
        "notification:delivery:channel": "channel_delivered",
        "notification:delivery:user": "user_delivered",
        "notification:delivery:failure": "failed",
        "notification:match:failure": "failed",
    }
    status = status_map.get(trigger)
    if not status:
        event_ref.set({"processed_at": firestore.SERVER_TIMESTAMP, "result": "unsupported_trigger"}, merge=True)
        return jsonify({"status": "ignored"})
    update = {"status": status, "updated_at": firestore.SERVER_TIMESTAMP, "last_event_id": event_id}
    if status == "failed":
        update["error"] = payload.get("error") or event.get("error") or {"code": "delivery_failed"}

    @firestore.transactional
    def advance(transaction):
        current = message_ref.get(transaction=transaction).to_dict() or {}
        if current.get("last_event_id") == event_id:
            return True
        if current.get("delivery_final") or current.get("status") in {"user_delivered", "failed"}:
            return False
        update["delivery_final"] = bool(payload.get("isFinalEvent")) or status in {"user_delivered", "failed"}
        transaction.update(message_ref, update)
        return True

    if not advance(db().transaction()):
        event_ref.set({"processed_at": firestore.SERVER_TIMESTAMP, "result": "ignored_after_final"}, merge=True)
        return jsonify({"status": "ignored_after_final"})

    emitted_id = emit_event(message, status, {"sunshine_event_id": event_id})
    enqueue_callback(message, emitted_id, status, {"error": update.get("error")})
    if status == "user_delivered":
        enqueue_ticket(message, emitted_id)
    event_ref.set({"processed_at": firestore.SERVER_TIMESTAMP, "result": status, "message_id": message.get("message_id")}, merge=True)
    return jsonify({"status": status, "message_id": message.get("message_id")})


def handle_callback_task(body: dict):
    callback_url = config.allowed_callback_url(body.pop("callback_url", ""))
    if not callback_url:
        return jsonify({"error": "invalid_callback_url"}), 400
    session = http_session()
    response = session.post(
        f"{callback_url}/api/internal/messaging-events",
        headers={"X-Cerebro-Token": config.CALLBACK_TOKEN},
        json=body,
        timeout=20,
    )
    if response.status_code >= 500 or response.status_code == 429:
        return jsonify({"error": "callback_temporary_failure"}), 503
    return jsonify({"status": "callback_sent", "http_status": response.status_code})


def handle_report_task(body: dict):
    run_id = str(body.get("run_id") or "")
    try:
        report = run_report(run_id, include_rows=False)
    except KeyError:
        return jsonify({"error": "run_not_found"}), 404
    run = report.get("run") or {}
    if run.get("ingestion_error"):
        return jsonify({"error": run["ingestion_error"]}), 409
    if int(run.get("pending_chunks", 0)):
        return jsonify({"error": "batch_ingestion_pending"}), 503
    db().collection(config.RUNS).document(run_id).set({
        "status": "reported",
        "reported_at": firestore.SERVER_TIMESTAMP,
        "summary": {key: report[key] for key in ("total", "delivered", "failed", "contactability_rate", "counts")},
    }, merge=True)
    callback_url = config.allowed_callback_url((run.get("campaign") or {}).get("callback_url"))
    if callback_url:
        payload = {
            "event_id": str(uuid.uuid4()),
            "report_id": f"report-{run_id}",
            "run_id": run_id,
            "campaign_id": run.get("campaign_id"),
            "status": "sent",
            "total": report["total"],
            "delivered": report["delivered"],
            "failed": report["failed"],
            "contactability_rate": report["contactability_rate"],
            "counts": report["counts"],
        }
        session = http_session()
        response = session.post(
            f"{callback_url}/api/internal/messaging-reports",
            headers={"X-Cerebro-Token": config.CALLBACK_TOKEN},
            json=payload,
            timeout=20,
        )
        if response.status_code >= 500 or response.status_code == 429:
            return jsonify({"error": "report_callback_temporary_failure"}), 503
    return jsonify({"status": "reported", "run_id": run_id})
