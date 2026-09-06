import logging
import time
from datetime import timedelta
from flask import jsonify, request
from google.cloud import firestore
import requests
import config
from extensions import db, enqueue_task
from clients.sunshine import (
    send_notification,
    sunshine_notification_id,
    sunshine_wire_payload,
)
from services.template_service import sync_templates
from tasks.event_tasks import emit_event, enqueue_callback

logger = logging.getLogger(__name__)


def handle_sunshine_task(body: dict):
    if body.get("kind") == "sync_templates":
        try:
            return jsonify(sync_templates(config.safe_text(body.get("after"), 300)))
        except requests.RequestException as error:
            retry_after = error.response.headers.get("Retry-After", "") if error.response is not None else ""
            attempt = int(request.headers.get("X-CloudTasks-TaskRetryCount", "0")) + 1
            return (
                jsonify({"error": "sunshine_temporary_failure"}),
                503,
                {"Retry-After": str(int(config.retry_delay(attempt, retry_after)) + 1)},
            )

    message_id = str(body.get("message_id") or "")
    ref = db().collection(config.MESSAGES).document(message_id)
    snapshot = ref.get()
    if not snapshot.exists:
        return jsonify({"error": "message_not_found"}), 404
    message = snapshot.to_dict() or {}
    if message.get("status") in {
        "submitted",
        "channel_delivered",
        "user_delivered",
        "failed",
        "delivery_unknown",
        "conversation_locked",
        "sending",
    }:
        return jsonify({"status": message.get("status"), "duplicate_task": True})

    payload = message.get("sunshine_payload") or {}
    try:
        wire = sunshine_wire_payload(payload)
    except ValueError:
        ref.set(
            {"status": "failed", "error": "sunshine_payload_too_large", "updated_at": firestore.SERVER_TIMESTAMP},
            merge=True,
        )
        failure_id = emit_event(message, "failed", {"code": "payload_too_large"})
        enqueue_callback(message, failure_id, "failed", {"error": {"code": "payload_too_large"}})
        return jsonify({"error": "sunshine_payload_too_large"}), 400

    # Reserve before the external side effect. A crash must never blindly resend.
    @firestore.transactional
    def claim(transaction):
        current = ref.get(transaction=transaction).to_dict() or {}
        if current.get("status") != "queued":
            return False
        transaction.update(ref, {"status": "sending", "updated_at": firestore.SERVER_TIMESTAMP})
        return True

    if not claim(db().transaction()):
        return jsonify({"status": "already_claimed"})

    logger.info("[SUNSHINE TASK CLAIMED] message_id=%s", message_id)
    logger.info("[SUNSHINE REQUEST START] message_id=%s", message_id)
    start_time = time.monotonic()

    try:
        response = send_notification(wire)
        duration_ms = int((time.monotonic() - start_time) * 1000)
        if response.status_code == 429:
            attempt = int(message.get("retry_attempt", 0)) + 1
            delay = config.retry_delay(attempt, response.headers.get("Retry-After", ""))
            logger.warning("[SUNSHINE RESPONSE] message_id=%s status=429 duration_ms=%d attempt=%d", message_id, duration_ms, attempt)
            ref.set({"status": "queued" if attempt < 12 else "failed", "retry_attempt": attempt}, merge=True)
            if attempt < 12:
                enqueue_task(
                    config.SUNSHINE_QUEUE,
                    "/internal/tasks/sunshine",
                    {"kind": "send", "message_id": message_id},
                    f"rate-retry-{message_id}-{attempt}",
                    config.utcnow() + timedelta(seconds=delay),
                )
            return jsonify({"status": "retry_scheduled" if attempt < 12 else "failed"})
        if response.status_code >= 500:
            logger.warning("[SUNSHINE RESPONSE] message_id=%s status=%d duration_ms=%d ambiguous_provider_failure", message_id, response.status_code, duration_ms)
            raise requests.Timeout("ambiguous_provider_failure")
        if response.status_code == 423:
            logger.warning("[SUNSHINE RESPONSE] message_id=%s status=423 duration_ms=%d conversation_locked", message_id, duration_ms)
            ref.set(
                {"status": "conversation_locked", "error": "sunshine_423", "updated_at": firestore.SERVER_TIMESTAMP},
                merge=True,
            )
            locked_id = emit_event(message, "conversation_locked")
            enqueue_callback(
                message,
                locked_id,
                "conversation_locked",
                {"error": {"code": "sunshine_423", "message": "Conversation locked"}},
            )
            return jsonify({"error": "conversation_locked"}), 200
        if response.status_code not in {200, 201, 202}:
            try:
                provider_error = response.json().get("error") or {}
            except ValueError:
                provider_error = {}
            code = config.safe_text(provider_error.get("code"), 100)
            desc = config.safe_text(provider_error.get("description"), 500)
            logger.warning("[SUNSHINE RESPONSE] message_id=%s status=%d duration_ms=%d code=%s desc=%s", message_id, response.status_code, duration_ms, code or "-", desc or "-")
            logger.warning("[SUNSHINE FAILED] message_id=%s status=%d code=%s", message_id, response.status_code, code or "-")
            ref.set(
                {
                    "provider_error": {
                        "code": code,
                        "description": desc,
                    }
                },
                merge=True,
            )
            ref.set(
                {"status": "failed", "error": f"sunshine_{response.status_code}", "updated_at": firestore.SERVER_TIMESTAMP},
                merge=True,
            )
            failure_id = emit_event(message, "failed", {"http_status": response.status_code})
            enqueue_callback(
                message,
                failure_id,
                "failed",
                {"error": {"code": f"sunshine_{response.status_code}", "message": "Sunshine rejected the notification"}},
            )
            return jsonify({"error": "sunshine_rejected"}), 200

        try:
            result = response.json()
        except ValueError:
            logger.warning("[SUNSHINE RESPONSE] message_id=%s status=%d invalid_json", message_id, response.status_code)
            raise requests.Timeout("invalid_accepted_response")
        if not isinstance(result, dict):
            raise requests.Timeout("invalid_accepted_response")
        notification_id = sunshine_notification_id(result)
        if not notification_id:
            logger.warning("[SUNSHINE RESPONSE] message_id=%s missing notification_id", message_id)
            raise requests.Timeout("sunshine_missing_notification_id")

        logger.info("[SUNSHINE RESPONSE] message_id=%s status=%d duration_ms=%d notification_id=%s", message_id, response.status_code, duration_ms, notification_id)
        ref.set(
            {
                "status": "submitted",
                "notification_id": notification_id,
                "submitted_at": firestore.SERVER_TIMESTAMP,
                "updated_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        db().collection(config.NOTIFICATION_INDEX).document(notification_id).set(
            {"message_id": message_id, "expires_at": config.expires_at()}
        )
        message["notification_id"] = notification_id
        submitted_event_id = emit_event(message, "submitted")
        enqueue_callback(message, submitted_event_id, "submitted")
        logger.info("[SUNSHINE SUBMITTED] message_id=%s notification_id=%s", message_id, notification_id)
        return jsonify({"message_id": message_id, "notification_id": notification_id, "status": "submitted"})
    except requests.Timeout:
        logger.warning("[SUNSHINE DELIVERY UNKNOWN] message_id=%s error=sunshine_timeout", message_id)
        ref.set(
            {"status": "delivery_unknown", "error": "sunshine_timeout", "updated_at": firestore.SERVER_TIMESTAMP},
            merge=True,
        )
        unknown_id = emit_event(message, "delivery_unknown", {"code": "timeout_after_submit"})
        enqueue_callback(
            message,
            unknown_id,
            "delivery_unknown",
            {"error": {"code": "sunshine_timeout", "message": "Delivery result unknown after timeout"}},
        )
        return jsonify({"status": "delivery_unknown"}), 200
    except requests.RequestException as err:
        logger.warning("[SUNSHINE DELIVERY UNKNOWN] message_id=%s error=sunshine_network_failure detail=%s", message_id, type(err).__name__)
        ref.set({"status": "delivery_unknown", "error": "sunshine_network_failure"}, merge=True)
        return jsonify({"status": "delivery_unknown"})
