"""Private orchestration service for Cerebro Sunshine.

Cloud Run IAM is the primary network identity boundary. Application secrets,
signed actor claims and Cloud Tasks metadata provide additional layers.
Secrets are injected from Secret Manager and never persisted in Firestore or
BigQuery. Zendesk authentication uses OAuth client_credentials.

Meta templates synchronization queries graph.facebook.com asynchronously via
Cloud Tasks with the META_SYSTEM_TOKEN.
"""
from __future__ import annotations

import json
import os
import random
import time
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from flask import Flask, jsonify, request
from google.cloud import firestore
import requests

import config
from extensions import db, enqueue_task, bq_client
from security.decorators import require_gateway, require_task
from services.template_service import sync_templates, sync_meta_templates, sync_meta_namespace
from tasks.event_tasks import emit_event, enqueue_callback
from routes.ingress import ingress_bp
from routes.tasks import tasks_bp
from routes.admin import admin_bp

# Module-level variables for static inspection and test compatibility
MESSAGES = config.MESSAGES
RUNS = config.RUNS
EVENTS = config.EVENTS
NOTIFICATION_INDEX = config.NOTIFICATION_INDEX
TEMPLATES = config.TEMPLATES
CONFIG = config.CONFIG
BATCHES = config.BATCHES

SUNSHINE_JSON_LIMIT = config.SUNSHINE_JSON_LIMIT
SUNSHINE_API_ROOT = config.SUNSHINE_API_ROOT
SUNSHINE_APP_ID = config.SUNSHINE_APP_ID
SUNSHINE_KEY_ID = config.SUNSHINE_KEY_ID
SUNSHINE_SECRET_KEY = config.SUNSHINE_SECRET_KEY
SUNSHINE_QUEUE = config.SUNSHINE_QUEUE
META_QUEUE = os.getenv("META_QUEUE", "cerebro-meta")


def utcnow() -> datetime:
    return datetime.now(UTC)


def expires_at() -> datetime:
    return config.expires_at()


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def sunshine_wire_payload(payload: dict) -> bytes:
    def validate(value: Any):
        if isinstance(value, dict):
            if "metadata" in value:
                metadata = value["metadata"]
                if not isinstance(metadata, dict) or any(
                    not isinstance(v, (str, int, float, bool)) for v in metadata.values()
                ):
                    raise ValueError("invalid_metadata")
                if len(canonical_json(metadata)) > 4000:
                    raise ValueError("metadata_too_large")
            for child in value.values():
                validate(child)
        elif isinstance(value, list):
            for child in value:
                validate(child)

    validate(payload)
    wire = canonical_json(payload)
    if len(wire) > min(SUNSHINE_JSON_LIMIT, 100000):
        raise ValueError("sunshine_payload_too_large")
    return wire


def retry_delay(attempt: int, retry_after: str = "") -> float:
    delay = random.uniform(10, min(600, 10 * 2 ** min(attempt, 6)))
    try:
        minimum = float(retry_after)
    except (ValueError, TypeError):
        try:
            minimum = (parsedate_to_datetime(retry_after) - utcnow()).total_seconds()
        except (ValueError, TypeError, OverflowError):
            minimum = 0
    return max(delay, minimum)


def sunshine_notification_id(response_json: dict) -> str:
    notification = response_json.get("notification") if isinstance(response_json.get("notification"), dict) else {}
    return str(notification.get("_id") or notification.get("id") or response_json.get("notificationId") or "")


@require_task(SUNSHINE_QUEUE)
def task_sunshine():
    """Sunshine dispatch worker with rate limit backoff and anti-duplication guards."""
    body = request.get_json(silent=True) or {}
    if body.get("kind") == "sync_templates":
        try:
            return jsonify(sync_templates(config.safe_text(body.get("after"), 300)))
        except requests.RequestException as error:
            retry_after = error.response.headers.get("Retry-After", "") if error.response is not None else ""
            attempt = int(request.headers.get("X-CloudTasks-TaskRetryCount", "0")) + 1
            return (
                jsonify({"error": "sunshine_temporary_failure"}),
                503,
                {"Retry-After": str(int(retry_delay(attempt, retry_after)) + 1)},
            )
    message_id = str(body.get("message_id") or "")
    ref = db().collection(MESSAGES).document(message_id)
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
    try:
        response = requests.post(
            f"{SUNSHINE_API_ROOT}/v1.1/apps/{SUNSHINE_APP_ID}/notifications",
            auth=(SUNSHINE_KEY_ID, SUNSHINE_SECRET_KEY),
            data=wire,
            headers={"Content-Type": "application/json"},
            timeout=(5, 10),
            allow_redirects=False,
        )
        if response.status_code == 429:
            attempt = int(message.get("retry_attempt", 0)) + 1
            delay = retry_delay(attempt, response.headers.get("Retry-After", ""))
            ref.set({"status": "queued" if attempt < 12 else "failed", "retry_attempt": attempt}, merge=True)
            if attempt < 12:
                enqueue_task(
                    SUNSHINE_QUEUE,
                    "/internal/tasks/sunshine",
                    {"kind": "send", "message_id": message_id},
                    f"rate-retry-{message_id}-{attempt}",
                    utcnow() + timedelta(seconds=delay),
                )
            return jsonify({"status": "retry_scheduled" if attempt < 12 else "failed"})
        if response.status_code >= 500:
            raise requests.Timeout("ambiguous_provider_failure")
        if response.status_code == 423:
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
            ref.set(
                {
                    "provider_error": {
                        "code": config.safe_text(provider_error.get("code"), 100),
                        "description": config.safe_text(provider_error.get("description"), 1000),
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
            raise requests.Timeout("invalid_accepted_response")
        if not isinstance(result, dict):
            raise requests.Timeout("invalid_accepted_response")
        notification_id = sunshine_notification_id(result)
        if not notification_id:
            raise requests.Timeout("sunshine_missing_notification_id")
        ref.set(
            {
                "status": "submitted",
                "notification_id": notification_id,
                "submitted_at": firestore.SERVER_TIMESTAMP,
                "updated_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
        db().collection(NOTIFICATION_INDEX).document(notification_id).set(
            {"message_id": message_id, "expires_at": expires_at()}
        )
        message["notification_id"] = notification_id
        submitted_event_id = emit_event(message, "submitted")
        enqueue_callback(message, submitted_event_id, "submitted")
        return jsonify({"message_id": message_id, "notification_id": notification_id, "status": "submitted"})
    except requests.Timeout:
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
    except requests.RequestException:
        ref.set({"status": "delivery_unknown", "error": "sunshine_network_failure"}, merge=True)
        return jsonify({"status": "delivery_unknown"})


@require_task(META_QUEUE)
def task_meta():
    """Synchronizes message templates and namespace with Meta Graph API."""
    body = request.get_json(silent=True) or {}
    try:
        if body.get("kind") == "sync_templates":
            return jsonify(sync_meta_templates(config.safe_text(body.get("after"), 500)))
        if body.get("kind") == "sync_namespace":
            return jsonify(sync_meta_namespace())
        return jsonify({"error": "unknown_meta_task"}), 400
    except requests.RequestException:
        return jsonify({"error": "meta_temporary_failure"}), 503


def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = config.CORE_MAX_BODY_BYTES

    # Register modular Blueprints
    app.register_blueprint(ingress_bp, url_prefix="/internal/ingress")
    app.register_blueprint(tasks_bp, url_prefix="/internal/tasks")
    app.register_blueprint(admin_bp, url_prefix="/internal/admin")

    @app.after_request
    def security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/health")
    def health():
        missing = config.required_config()
        return jsonify({
            "service": "cerebro-sunshine",
            "status": "ok" if not missing else "misconfigured",
            "missing": missing,
        }), 200 if not missing else 503

    @app.errorhandler(413)
    def too_large(_error):
        return jsonify({"error": "payload_too_large"}), 413

    @app.errorhandler(Exception)
    def unhandled(_error):
        app.logger.exception("Unhandled Cerebro error")
        return jsonify({"error": "internal_error"}), 500

    return app


app = create_app()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=config.PORT, debug=config.LOCAL_DEV)
