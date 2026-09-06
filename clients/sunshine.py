"""Sunshine Conversations API client and wire payload validation."""
from __future__ import annotations

from typing import Any
import requests
import config
from extensions import canonical_json, http_session


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
