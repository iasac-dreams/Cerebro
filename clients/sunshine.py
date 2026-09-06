"""Sunshine Conversations API client and wire payload validation."""
from __future__ import annotations

import hmac
import logging
import os
import threading
from typing import Any
import requests
from google.cloud import firestore
import config
from extensions import canonical_json, http_session, db

logger = logging.getLogger(__name__)

SUNSHINE_WEBHOOK_TRIGGERS = [
    "notification:delivery:channel",
    "notification:delivery:user",
    "notification:delivery:failure",
    "notification:match:failure",
    "message:delivery:channel",
    "message:delivery:user",
    "message:delivery:failure",
]
_sunshine_webhook_ready = False
_sunshine_webhook_secret = ""
_sunshine_webhook_setup_started = False
_sunshine_webhook_setup_lock = threading.Lock()


def _store_webhook_secret(secret_val: str):
    if not secret_val:
        return
    try:
        db().collection(config.CONFIG).document("sunshine_webhook").set({
            "secret": secret_val,
            "updated_at": firestore.SERVER_TIMESTAMP,
        }, merge=True)
    except Exception as err:
        logger.warning("Could not persist webhook secret to firestore: %s", err)


def _register_sunshine_webhook(app_id: str, key_id: str, secret_key: str):
    global _sunshine_webhook_ready, _sunshine_webhook_secret, _sunshine_webhook_setup_started
    if not (app_id and key_id and secret_key):
        return
    gateway_url = os.getenv("GATEWAY_PUBLIC_URL") or os.getenv("GATEWAY_SERVICE_URL") or "https://cerebro-gateway-462948619262.southamerica-west1.run.app"
    target = (os.getenv("SUNSHINE_WEBHOOK_TARGET") or f"{gateway_url.rstrip('/')}/webhooks/sunshine").rstrip("/")
    url = f"{config.SUNSHINE_API_ROOT}/v1.1/apps/{app_id}/webhooks"
    desired = set(SUNSHINE_WEBHOOK_TRIGGERS)
    try:
        session = http_session()
        response = session.get(url, auth=(key_id, secret_key), timeout=15)
        if response.status_code != 200:
            logger.warning("[SUNSHINE WEBHOOK] GET webhooks returned %s: %s", response.status_code, response.text[:500])
            return
        webhooks = response.json().get("webhooks", [])
        existing = next((item for item in webhooks if str(item.get("target", "")).rstrip("/") == target), None)
        body = {
            "target": target,
            "triggers": SUNSHINE_WEBHOOK_TRIGGERS,
            "includeFullAppUser": False,
        }
        if existing and desired.issubset(set(existing.get("triggers") or [])):
            _sunshine_webhook_secret = existing.get("secret") or _sunshine_webhook_secret
            _sunshine_webhook_ready = True
            logger.info("[SUNSHINE WEBHOOK ACTIVE] id=%s target=%s", existing.get("_id"), target)
            _store_webhook_secret(_sunshine_webhook_secret)
            return
        if existing:
            response = session.put(f"{url}/{existing['_id']}", auth=(key_id, secret_key), json=body, timeout=15)
        else:
            response = session.post(url, auth=(key_id, secret_key), json=body, timeout=15)
        if response.status_code not in (200, 201):
            logger.warning("[SUNSHINE WEBHOOK] Save webhook returned %s: %s", response.status_code, response.text[:500])
            return
        saved = response.json().get("webhook", {})
        _sunshine_webhook_secret = saved.get("secret") or _sunshine_webhook_secret
        _sunshine_webhook_ready = True
        logger.info("[SUNSHINE WEBHOOK REGISTERED] id=%s target=%s", saved.get("_id"), target)
        _store_webhook_secret(_sunshine_webhook_secret)
    except Exception as exc:
        logger.warning("[SUNSHINE WEBHOOK SETUP ERROR] %s", exc)
    finally:
        if not _sunshine_webhook_ready:
            with _sunshine_webhook_setup_lock:
                _sunshine_webhook_setup_started = False


def ensure_sunshine_webhook(app_id: str | None = None, key_id: str | None = None, secret_key: str | None = None):
    global _sunshine_webhook_setup_started
    app_id = app_id or config.SUNSHINE_APP_ID
    key_id = key_id or config.SUNSHINE_KEY_ID
    secret_key = secret_key or config.SUNSHINE_SECRET_KEY
    if not (app_id and key_id and secret_key):
        return
    if _sunshine_webhook_ready or _sunshine_webhook_setup_started:
        return
    with _sunshine_webhook_setup_lock:
        if _sunshine_webhook_ready or _sunshine_webhook_setup_started:
            return
        _sunshine_webhook_setup_started = True
        threading.Thread(
            target=_register_sunshine_webhook,
            args=(app_id, key_id, secret_key),
            daemon=True,
        ).start()


def sunshine_webhook_authorized(received: str) -> bool:
    global _sunshine_webhook_secret
    if not received:
        return False
    configured = config.SUNSHINE_WEBHOOK_SECRET or os.getenv("SUNSHINE_WEBHOOK_TOKEN")
    if configured and hmac.compare_digest(configured, received):
        return True
    if _sunshine_webhook_secret and hmac.compare_digest(_sunshine_webhook_secret, received):
        return True
    try:
        doc = db().collection(config.CONFIG).document("sunshine_webhook").get()
        if doc.exists:
            stored = doc.to_dict().get("secret")
            if stored:
                _sunshine_webhook_secret = stored
                if hmac.compare_digest(stored, received):
                    return True
    except Exception:
        pass
    app_id = config.SUNSHINE_APP_ID
    key_id = config.SUNSHINE_KEY_ID
    secret_key = config.SUNSHINE_SECRET_KEY
    if not (app_id and key_id and secret_key):
        return False
    try:
        gateway_url = os.getenv("GATEWAY_PUBLIC_URL") or os.getenv("GATEWAY_SERVICE_URL") or "https://cerebro-gateway-462948619262.southamerica-west1.run.app"
        target = (os.getenv("SUNSHINE_WEBHOOK_TARGET") or f"{gateway_url.rstrip('/')}/webhooks/sunshine").rstrip("/")
        session = http_session()
        response = session.get(f"{config.SUNSHINE_API_ROOT}/v1.1/apps/{app_id}/webhooks", auth=(key_id, secret_key), timeout=15)
        if response.status_code == 200:
            webhooks = response.json().get("webhooks", [])
            webhook = next((item for item in webhooks if str(item.get("target", "")).rstrip("/") == target), None)
            if webhook and webhook.get("secret"):
                _sunshine_webhook_secret = webhook["secret"]
                _store_webhook_secret(_sunshine_webhook_secret)
                return hmac.compare_digest(_sunshine_webhook_secret, received)
    except Exception as exc:
        logger.warning("[SUNSHINE WEBHOOK AUTH EXCEPTION] %s", exc)
    return False



def sunshine_wire_payload(payload: dict) -> bytes:
    """Validates metadata limits and serializes payload to under 95,000 bytes."""
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
    if len(wire) > min(config.SUNSHINE_JSON_LIMIT, 100000):
        raise ValueError("sunshine_payload_too_large")
    return wire


def sunshine_notification_id(response_json: dict) -> str:
    notification = response_json.get("notification") if isinstance(response_json.get("notification"), dict) else {}
    return str(notification.get("_id") or notification.get("id") or response_json.get("notificationId") or "")


def send_notification(wire: bytes) -> requests.Response:
    session = http_session()
    return session.post(
        f"{config.SUNSHINE_API_ROOT}/v1.1/apps/{config.SUNSHINE_APP_ID}/notifications",
        auth=(config.SUNSHINE_KEY_ID, config.SUNSHINE_SECRET_KEY),
        data=wire,
        headers={"Content-Type": "application/json"},
        timeout=(5, 10),
        allow_redirects=False,
    )


def fetch_message_templates(after: str = "") -> dict:
    url = f"{config.SUNSHINE_API_ROOT}/v1.1/apps/{config.SUNSHINE_APP_ID}/integrations/{config.SUNSHINE_INTEGRATION_ID}/messageTemplates"
    params = {"limit": 100, "status": "APPROVED"}
    if after:
        params["after"] = after
    session = http_session()
    response = session.get(url, auth=(config.SUNSHINE_KEY_ID, config.SUNSHINE_SECRET_KEY), params=params, timeout=30)
    response.raise_for_status()
    return response.json()
