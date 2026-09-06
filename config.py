"""Configuration, environment parsing and foundational primitives for Cerebro Core."""
from __future__ import annotations

import hashlib
import hmac
import os
import random
import re
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from typing import Any
from urllib.parse import urlparse

LOCAL_DEV = os.getenv("LOCAL_DEV", "false").lower() == "true"
PORT = int(os.getenv("PORT", "8080"))
CORE_MAX_BODY_BYTES = int(os.getenv("CORE_MAX_BODY_BYTES", "5242880"))

def resolve_project_id() -> str:
    pid = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT_ID")
    if pid:
        return pid
    try:
        import google.auth
        _, default_pid = google.auth.default()
        if default_pid:
            return default_pid
    except Exception:
        pass
    return "dreams-reservas-nacional"


PROJECT_ID = resolve_project_id()
REGION = os.getenv("GCP_REGION", "southamerica-west1")
TASKS_LOCATION = os.getenv("CLOUD_TASKS_LOCATION") or os.getenv("TASKS_LOCATION") or os.getenv("GCP_REGION") or "southamerica-west1"
FIRESTORE_DATABASE = os.getenv("FIRESTORE_DATABASE", "cerebro-sunshine")
TTL_DAYS = int(os.getenv("FIRESTORE_TTL_DAYS", "3"))
BATCH_TTL_DAYS = int(os.getenv("BATCH_TTL_DAYS", "1"))  # 1 day TTL for incoming raw batches
BATCH_CHUNK_SIZE = int(os.getenv("BATCH_CHUNK_SIZE", "300"))
MAX_BATCH_MESSAGES = int(os.getenv("MAX_BATCH_MESSAGES", "2000"))

GATEWAY_SHARED_SECRET = os.getenv("GATEWAY_SHARED_SECRET", "")
ACTOR_SIGNING_SECRET = os.getenv("ACTOR_SIGNING_SECRET", "")
TASK_SHARED_SECRET = os.getenv("TASK_SHARED_SECRET", "")
CALLBACK_TOKEN = os.getenv("CEREBRO_CALLBACK_TOKEN", "")
CALLBACK_ALLOWED_HOSTS = {
    value.strip().lower()
    for value in os.getenv("CALLBACK_ALLOWED_HOSTS", "").split(",")
    if value.strip()
}
ANALYTICS_HASH_SECRET = os.getenv("ANALYTICS_HASH_SECRET", "")

SERVICE_URL = os.getenv("SERVICE_URL", "").rstrip("/")
TASK_INVOKER_SERVICE_ACCOUNT = os.getenv("TASK_INVOKER_SERVICE_ACCOUNT", "")
TASK_OIDC_AUDIENCE = os.getenv("TASK_OIDC_AUDIENCE", SERVICE_URL)
SUNSHINE_QUEUE = os.getenv("SUNSHINE_QUEUE", "cerebro-sunshine-dispatch")
EVENT_QUEUE = os.getenv("EVENT_QUEUE", "cerebro-events")
ZENDESK_QUEUE = os.getenv("ZENDESK_QUEUE", "cerebro-zendesk")
ANALYTICS_QUEUE = os.getenv("ANALYTICS_QUEUE", "cerebro-analytics")
META_QUEUE = os.getenv("META_QUEUE", "cerebro-meta")

SUNSHINE_API_ROOT = os.getenv("SUNSHINE_API_ROOT", "https://api.smooch.io").rstrip("/")
SUNSHINE_APP_ID = os.getenv("SUNSHINE_APP_ID", "")
SUNSHINE_KEY_ID = os.getenv("SUNSHINE_KEY_ID", "")
SUNSHINE_SECRET_KEY = os.getenv("SUNSHINE_SECRET_KEY", "")
SUNSHINE_INTEGRATION_ID = os.getenv("SUNSHINE_INTEGRATION_ID", "")
SUNSHINE_NAMESPACE = os.getenv("SUNSHINE_TEMPLATE_NAMESPACE", "")
SUNSHINE_JSON_LIMIT = int(os.getenv("SUNSHINE_JSON_LIMIT_BYTES", "95000"))
REQUIRE_APPROVED_TEMPLATE = os.getenv("REQUIRE_APPROVED_TEMPLATE", "true").lower() == "true"

META_GRAPH_VERSION = os.getenv("META_GRAPH_VERSION", "v26.0")
META_WABA_ID = os.getenv("META_WABA_ID", "")
META_SYSTEM_TOKEN = os.getenv("META_SYSTEM_TOKEN", "")

ZENDESK_SUBDOMAIN = os.getenv("ZENDESK_SUBDOMAIN", "")
ZENDESK_OAUTH_CLIENT_ID = os.getenv("ZENDESK_OAUTH_CLIENT_ID", "")
ZENDESK_OAUTH_CLIENT_SECRET = os.getenv("ZENDESK_OAUTH_CLIENT_SECRET", "")
ZENDESK_OAUTH_SCOPE = os.getenv("ZENDESK_OAUTH_SCOPE", "tickets:write users:write")

BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "cerebro_sunshine")
BIGQUERY_EVENTS_TABLE = os.getenv("BIGQUERY_EVENTS_TABLE", "message_events")

# Firestore collection names
MESSAGES = "cerebro_messages"
RUNS = "cerebro_runs"
EVENTS = "cerebro_events"
NOTIFICATION_INDEX = "cerebro_notification_index"
TEMPLATES = "cerebro_templates"
TICKET_JOBS = "cerebro_ticket_jobs"
CONFIG = "cerebro_config"
BATCHES = "cerebro_batches"
BATCH_RECEIPTS = "cerebro_batch_receipts"

ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,149}$")
EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")


def utcnow() -> datetime:
    return datetime.now(UTC)


def expires_at(days: int | None = None) -> datetime:
    d = days if days is not None else TTL_DAYS
    return utcnow() + timedelta(days=d)


def batch_expires_at() -> datetime:
    return expires_at(BATCH_TTL_DAYS)


def assert_identifier(value: str, label: str) -> str:
    value = str(value or "").strip()
    if not ID_RE.fullmatch(value):
        raise ValueError(f"invalid_{label}")
    return value


def normalize_phone(value: Any) -> str:
    digits = re.sub(r"\D", "", str(value or ""))
    if digits.startswith("00"):
        digits = digits[2:]
    if len(digits) == 9:
        digits = "56" + digits
    if not 10 <= len(digits) <= 15:
        raise ValueError("invalid_phone")
    return "+" + digits


def normalize_email(value: Any) -> str | None:
    email = str(value or "").strip().lower()
    if not email:
        return None
    if not EMAIL_RE.fullmatch(email):
        raise ValueError("invalid_email")
    return email


def safe_text(value: Any, maximum: int = 500) -> str:
    return re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f]", "", str(value or "")).strip()[:maximum]


def doc_id(*parts: Any) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode()).hexdigest()


def privacy_hash(value: Any) -> str:
    if not ANALYTICS_HASH_SECRET:
        raise RuntimeError("ANALYTICS_HASH_SECRET is required")
    return hmac.new(ANALYTICS_HASH_SECRET.encode(), str(value or "").encode(), hashlib.sha256).hexdigest()


def allowed_callback_url(value: Any) -> str:
    candidate = str(value or "").strip().rstrip("/")
    parsed = urlparse(candidate)
    if parsed.scheme != "https" or not parsed.hostname:
        return ""
    if not CALLBACK_ALLOWED_HOSTS or parsed.hostname.lower() not in CALLBACK_ALLOWED_HOSTS:
        return ""
    if parsed.username or parsed.password or parsed.port not in (None, 443):
        return ""
    return candidate


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


def task_url(path: str) -> str:
    base = SERVICE_URL or os.getenv("CORE_SERVICE_URL") or "https://cerebro-sunshine-462948619262.southamerica-west1.run.app"
    return f"{base.rstrip('/')}{path}"


def serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: serialize(item) for key, item in value.items()}
    if isinstance(value, list):
        return [serialize(item) for item in value]
    return value


def required_config() -> list[str]:
    values = {
        "PROJECT_ID": PROJECT_ID,
        "GATEWAY_SHARED_SECRET": GATEWAY_SHARED_SECRET,
        "ACTOR_SIGNING_SECRET": ACTOR_SIGNING_SECRET,
        "TASK_SHARED_SECRET": TASK_SHARED_SECRET,
        "ANALYTICS_HASH_SECRET": ANALYTICS_HASH_SECRET,
        "SERVICE_URL": SERVICE_URL,
        "TASK_INVOKER_SERVICE_ACCOUNT": TASK_INVOKER_SERVICE_ACCOUNT,
        "SUNSHINE_APP_ID": SUNSHINE_APP_ID,
        "SUNSHINE_KEY_ID": SUNSHINE_KEY_ID,
        "SUNSHINE_SECRET_KEY": SUNSHINE_SECRET_KEY,
        "SUNSHINE_INTEGRATION_ID": SUNSHINE_INTEGRATION_ID,
        "META_WABA_ID": META_WABA_ID,
        "META_SYSTEM_TOKEN": META_SYSTEM_TOKEN,
        "ZENDESK_SUBDOMAIN": ZENDESK_SUBDOMAIN,
        "ZENDESK_OAUTH_CLIENT_ID": ZENDESK_OAUTH_CLIENT_ID,
        "ZENDESK_OAUTH_CLIENT_SECRET": ZENDESK_OAUTH_CLIENT_SECRET,
    }
    return [key for key, value in values.items() if not value]
