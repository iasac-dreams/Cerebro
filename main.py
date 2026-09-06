"""Private orchestration service for Cerebro Sunshine.

Self-contained service for direct deployment in Google Cloud Console
(Cloud Run / Cloud Functions) without external repository or folder dependencies.
Zendesk authentication uses OAuth client_credentials.
Meta templates synchronization queries graph.facebook.com asynchronously via
Cloud Tasks with the META_SYSTEM_TOKEN.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import hmac
import json
import logging
import os
import random
import re
import time
import uuid
from datetime import UTC, datetime, timedelta
from email.utils import parsedate_to_datetime
from functools import wraps
from typing import Any
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, jsonify, request
from google.api_core.exceptions import AlreadyExists
from google.cloud import firestore, tasks_v2
try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None
from google.protobuf import timestamp_pb2

logger = logging.getLogger(__name__)

# =====================================================================
# 1. CONFIGURATION & CONSTANTS
# =====================================================================
LOCAL_DEV = os.getenv("LOCAL_DEV", "false").lower() == "true"
PORT = int(os.getenv("PORT", "8080"))
CORE_MAX_BODY_BYTES = int(os.getenv("CORE_MAX_BODY_BYTES", "5242880"))

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT_ID", "")
REGION = os.getenv("GCP_REGION", "southamerica-west1")
TASKS_LOCATION = os.getenv("CLOUD_TASKS_LOCATION", os.getenv("TASKS_LOCATION", "us-central1"))
FIRESTORE_DATABASE = os.getenv("FIRESTORE_DATABASE", "cerebro-sunshine")
TTL_DAYS = int(os.getenv("FIRESTORE_TTL_DAYS", "3"))
BATCH_TTL_DAYS = int(os.getenv("BATCH_TTL_DAYS", "1"))
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

# Firestore Collections
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

_db: firestore.Client | None = None
_tasks: tasks_v2.CloudTasksClient | None = None
_bq: Any = None
_session: requests.Session | None = None
_zendesk_access_token = ""
_zendesk_access_token_expires_at = 0
_template_status_cache: dict[str, tuple[float, dict | None]] = {}
_namespace_cache: tuple[float, str] = (0, "")


# =====================================================================
# 2. UTILITY & CLIENT EXTENSIONS
# =====================================================================
def utcnow() -> datetime:
    return datetime.now(UTC)


def expires_at(days: int | None = None) -> datetime:
    d = days if days is not None else TTL_DAYS
    return utcnow() + timedelta(days=d)


def batch_expires_at() -> datetime:
    return expires_at(BATCH_TTL_DAYS)


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def db() -> firestore.Client:
    global _db
    if _db is None:
        _db = firestore.Client(project=PROJECT_ID or None, database=FIRESTORE_DATABASE)
    return _db


def tasks_client() -> tasks_v2.CloudTasksClient:
    global _tasks
    if _tasks is None:
        _tasks = tasks_v2.CloudTasksClient()
    return _tasks


def bq_client() -> Any:
    global _bq
    if _bq is None and bigquery is not None:
        _bq = bigquery.Client(project=PROJECT_ID or None, location=REGION)
    return _bq


def http_session() -> requests.Session:
    global _session
    if _session is None:
        _session = requests.Session()
        adapter = HTTPAdapter(
            pool_connections=25,
            pool_maxsize=50,
            max_retries=Retry(total=2, backoff_factor=0.2, status_forcelist=[502, 503, 504]),
        )
        _session.mount("http://", adapter)
        _session.mount("https://", adapter)
    return _session


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


def enqueue_task(queue: str, path: str, payload: dict, task_key: str, schedule_at: datetime | None = None) -> bool:
    try:
        client = tasks_client()
        if client is None:
            return False
        parent = client.queue_path(PROJECT_ID, TASKS_LOCATION, queue)
        task_name = client.task_path(PROJECT_ID, TASKS_LOCATION, queue, doc_id(queue, task_key)[:40])
        sa_email = TASK_INVOKER_SERVICE_ACCOUNT or "462948619262-compute@developer.gserviceaccount.com"
        audience = TASK_OIDC_AUDIENCE or SERVICE_URL or "https://cerebro-sunshine-462948619262.southamerica-west1.run.app"
        task = {
            "name": task_name,
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": task_url(path),
                "headers": {
                    "Content-Type": "application/json",
                    "X-Cerebro-Task-Secret": TASK_SHARED_SECRET,
                },
                "body": canonical_json(payload),
                "oidc_token": {
                    "service_account_email": sa_email,
                    "audience": audience,
                },
            },
        }
        if schedule_at:
            stamp = timestamp_pb2.Timestamp()
            stamp.FromDatetime(schedule_at)
            task["schedule_time"] = stamp
        client.create_task(request={"parent": parent, "task": task})
        return True
    except AlreadyExists:
        return False
    except Exception as err:
        logger.error("Failed to enqueue task in queue '%s': %s", queue, err)
        return False


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


# =====================================================================
# 3. SECURITY & DECORATORS
# =====================================================================
def compare(left: str, right: str) -> bool:
    return bool(left and right and hmac.compare_digest(str(left), str(right)))


def require_gateway(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not LOCAL_DEV and not compare(request.headers.get("X-Cerebro-Gateway-Secret", ""), GATEWAY_SHARED_SECRET):
            return jsonify({"error": "gateway_unauthorized"}), 401
        return view(*args, **kwargs)

    return wrapped


def require_task(queue_name: str):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if LOCAL_DEV:
                return view(*args, **kwargs)
            if not compare(request.headers.get("X-Cerebro-Task-Secret", ""), TASK_SHARED_SECRET):
                return jsonify({"error": "task_secret_invalid"}), 401
            if request.headers.get("X-CloudTasks-QueueName", "") != queue_name:
                return jsonify({"error": "task_queue_invalid"}), 401
            if not request.headers.get("X-CloudTasks-TaskName", ""):
                return jsonify({"error": "task_name_missing"}), 401
            return view(*args, **kwargs)

        return wrapped

    return decorator


def decode_signed_actor() -> dict:
    token = request.headers.get("X-Cerebro-Actor", "")
    if not token or "." not in token or not ACTOR_SIGNING_SECRET:
        return {}
    try:
        raw_part, signature_part = token.split(".", 1)
        raw = base64.urlsafe_b64decode(raw_part + "=" * (-len(raw_part) % 4))
        supplied = base64.urlsafe_b64decode(signature_part + "=" * (-len(signature_part) % 4))
        expected = hmac.new(ACTOR_SIGNING_SECRET.encode(), raw, hashlib.sha256).digest()
        if not hmac.compare_digest(supplied, expected):
            return {}
        envelope = json.loads(raw)
        now = int(time.time())
        if int(envelope.get("iat", 0)) > now + 30 or int(envelope.get("exp", 0)) < now:
            return {}
        return envelope.get("actor") or {}
    except Exception:
        return {}


def require_actor(permission: str):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            actor = decode_signed_actor()
            if not actor:
                return jsonify({"error": "actor_unauthorized"}), 401
            if permission not in actor.get("permissions", []):
                return jsonify({"error": "forbidden", "permission": permission}), 403
            request.cerebro_actor = actor
            return view(*args, **kwargs)

        return wrapped

    return decorator


# =====================================================================
# 4. SUNSHINE, META & ZENDESK CLIENT SERVICES
# =====================================================================
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


def sunshine_notification_id(response_json: dict) -> str:
    notification = response_json.get("notification") if isinstance(response_json.get("notification"), dict) else {}
    return str(notification.get("_id") or notification.get("id") or response_json.get("notificationId") or "")


def template_name_from_payload(payload: dict) -> str:
    message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
    template = message.get("template") if isinstance(message.get("template"), dict) else {}
    hsm = message.get("hsm") if isinstance(message.get("hsm"), dict) else {}
    return safe_text(
        template.get("name") or hsm.get("templateName") or hsm.get("template_name") or message.get("template_name"),
        200,
    )


def approved_template(name: str, language: str) -> dict:
    if not name or not REQUIRE_APPROVED_TEMPLATE:
        return {}
    key = doc_id(name, language)
    cached = _template_status_cache.get(key)
    if cached and cached[0] > time.monotonic():
        if not cached[1] or cached[1].get("meta_status") != "APPROVED":
            raise ValueError("template_not_approved")
        return cached[1]
    snapshot = db().collection(TEMPLATES).document(key).get()
    template = snapshot.to_dict() if snapshot.exists else None
    _template_status_cache[key] = (time.monotonic() + 300, template)
    if not template or template.get("meta_status") != "APPROVED":
        raise ValueError("template_not_approved")
    return template


def template_body_parameter_count(template: dict) -> int:
    indexes = []
    for component in template.get("components") or []:
        if not isinstance(component, dict):
            continue
        if str(component.get("type", "")).upper() != "BODY":
            continue
        indexes.extend(int(value) for value in re.findall(r"\{\{\s*(\d+)\s*\}\}", str(component.get("text") or "")))
    return max(indexes, default=0)


def render_template_snapshot(template: dict, values: list[str]) -> dict:
    rendered = {
        "name": template.get("name"),
        "language": template.get("language"),
        "category": template.get("category"),
        "header": "",
        "body": "",
        "footer": "",
        "buttons": [],
        "parameters": values,
    }
    for component in template.get("components") or []:
        if not isinstance(component, dict):
            continue
        kind = str(component.get("type") or "").upper()
        if kind == "BODY":
            body = str(component.get("text") or "")
            for index, value in enumerate(values, start=1):
                body = re.sub(r"\{\{\s*" + str(index) + r"\s*\}\}", lambda _match, replacement=value: replacement, body)
            rendered["body"] = body
        elif kind == "HEADER":
            rendered["header"] = component.get("text") or ""
        elif kind == "FOOTER":
            rendered["footer"] = component.get("text") or ""
        elif kind == "BUTTONS":
            rendered["buttons"] = component.get("buttons") or []
    return rendered


def template_namespace() -> str:
    global _namespace_cache
    if SUNSHINE_NAMESPACE:
        return SUNSHINE_NAMESPACE
    if _namespace_cache[0] > time.monotonic():
        return _namespace_cache[1]
    snapshot = db().collection(CONFIG).document("meta").get()
    namespace = safe_text((snapshot.to_dict() or {}).get("message_template_namespace"), 300) if snapshot.exists else ""
    _namespace_cache = (time.monotonic() + 300, namespace)
    return namespace


def sync_templates(after: str = "") -> dict:
    url = f"{SUNSHINE_API_ROOT}/v1.1/apps/{SUNSHINE_APP_ID}/integrations/{SUNSHINE_INTEGRATION_ID}/messageTemplates"
    params = {"limit": 100, "status": "APPROVED"}
    if after:
        params["after"] = after
    session = http_session()
    response = session.get(url, auth=(SUNSHINE_KEY_ID, SUNSHINE_SECRET_KEY), params=params, timeout=30)
    response.raise_for_status()
    templates = response.json().get("messageTemplates", [])
    batch = db().batch()
    for item in templates:
        name = safe_text(item.get("name"), 200)
        language = safe_text(item.get("language"), 20)
        if not name:
            continue
        batch.set(
            db().collection(TEMPLATES).document(doc_id(name, language)),
            {
                "name": name,
                "language": language,
                "sunshine_status": item.get("status"),
                "sunshine_category": item.get("category"),
                "sunshine_components": item.get("components") or [],
                "source_sunshine": True,
                "synced_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
    batch.commit()
    _template_status_cache.clear()
    next_cursor = safe_text(response.json().get("after"), 300)
    if next_cursor:
        enqueue_task(
            SUNSHINE_QUEUE,
            "/internal/tasks/sunshine",
            {"kind": "sync_templates", "after": next_cursor},
            f"templates-page-{int(time.time()) // 7200}-{next_cursor}",
        )
    return {"templates": len(templates), "next_page_queued": bool(next_cursor)}


def sync_meta_templates(after: str = "") -> dict:
    url = f"https://graph.facebook.com/{META_GRAPH_VERSION}/{META_WABA_ID}/message_templates"
    params = {"limit": 100, "fields": "id,name,language,status,category,components"}
    if after:
        params["after"] = after
    session = http_session()
    response = session.get(
        url,
        headers={"Authorization": f"Bearer {META_SYSTEM_TOKEN}"},
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()
    templates = result.get("data") if isinstance(result.get("data"), list) else []
    batch = db().batch()
    stored = 0
    for item in templates:
        if not isinstance(item, dict):
            continue
        name = safe_text(item.get("name"), 200)
        language = safe_text(item.get("language"), 20)
        if not name or not language:
            continue
        components = item.get("components") if isinstance(item.get("components"), list) else []
        data = {
            "name": name,
            "language": language,
            "status": item.get("status"),
            "meta_status": item.get("status"),
            "category": item.get("category"),
            "meta_template_id": safe_text(item.get("id"), 200),
            "components": components,
            "body_parameter_count": template_body_parameter_count({"components": components}),
            "source_meta": True,
            "meta_synced_at": firestore.SERVER_TIMESTAMP,
            **{
                "header_text": next((c.get("text", "") for c in components if str(c.get("type", "")).upper() == "HEADER"), ""),
                "body_text": next((c.get("text", "") for c in components if str(c.get("type", "")).upper() == "BODY"), ""),
                "footer_text": next((c.get("text", "") for c in components if str(c.get("type", "")).upper() == "FOOTER"), ""),
                "buttons": next((c.get("buttons", []) for c in components if str(c.get("type", "")).upper() == "BUTTONS"), []),
            },
        }
        batch.set(db().collection(TEMPLATES).document(doc_id(name, language)), data, merge=True)
        stored += 1
    if stored:
        batch.commit()
    _template_status_cache.clear()
    paging = result.get("paging") if isinstance(result.get("paging"), dict) else {}
    cursors = paging.get("cursors") if isinstance(paging.get("cursors"), dict) else {}
    next_cursor = safe_text(cursors.get("after"), 500) if paging.get("next") else ""
    if next_cursor:
        enqueue_task(
            META_QUEUE,
            "/internal/tasks/meta",
            {"kind": "sync_templates", "after": next_cursor},
            f"meta-templates-page-{next_cursor}",
        )
    return {"templates": stored, "next_page_queued": bool(next_cursor)}


def sync_meta_namespace() -> dict:
    global _namespace_cache
    url = f"https://graph.facebook.com/{META_GRAPH_VERSION}/{META_WABA_ID}"
    session = http_session()
    response = session.get(
        url,
        headers={"Authorization": f"Bearer {META_SYSTEM_TOKEN}"},
        params={"fields": "message_template_namespace"},
        timeout=20,
    )
    response.raise_for_status()
    namespace = safe_text(response.json().get("message_template_namespace"), 300)
    if namespace:
        db().collection(CONFIG).document("meta").set({
            "message_template_namespace": namespace,
            "namespace_synced_at": firestore.SERVER_TIMESTAMP,
        }, merge=True)
        _namespace_cache = (time.monotonic() + 300, namespace)
    return {"namespace_available": bool(namespace)}


def zendesk_access_token() -> str:
    global _zendesk_access_token, _zendesk_access_token_expires_at
    now = int(time.time())
    if _zendesk_access_token and now < _zendesk_access_token_expires_at - 60:
        return _zendesk_access_token
    session = http_session()
    response = session.post(
        f"https://{ZENDESK_SUBDOMAIN}.zendesk.com/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": ZENDESK_OAUTH_CLIENT_ID,
            "client_secret": ZENDESK_OAUTH_CLIENT_SECRET,
            "scope": ZENDESK_OAUTH_SCOPE,
        },
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()
    _zendesk_access_token = data["access_token"]
    _zendesk_access_token_expires_at = now + int(data.get("expires_in") or 1800)
    return _zendesk_access_token


def zendesk_request(method: str, path: str, **kwargs) -> requests.Response:
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update({
        "Authorization": f"Bearer {zendesk_access_token()}",
        "Content-Type": "application/json",
    })
    session = http_session()
    return session.request(
        method,
        f"https://{ZENDESK_SUBDOMAIN}.zendesk.com{path}",
        headers=headers,
        timeout=30,
        **kwargs,
    )


# =====================================================================
# 5. MESSAGE & BATCH LOGIC
# =====================================================================
def storage_size(value: Any) -> int:
    if isinstance(value, dict):
        return 32 + sum(len(str(k).encode("utf-8")) + 1 + storage_size(v) for k, v in value.items())
    if isinstance(value, list):
        return sum(storage_size(v) for v in value)
    if isinstance(value, str):
        return len(value.encode("utf-8")) + 1
    if value is None or isinstance(value, bool):
        return 1
    if isinstance(value, (int, float, datetime)):
        return 8
    raise ValueError("unsupported_batch_value")


def partition_messages(messages: list, campaign: dict) -> list[list]:
    budget = 900_000 - storage_size(campaign) - 4096
    maximum = max(1, min(BATCH_CHUNK_SIZE, 300))
    chunks, current, size = [], [], 0
    for message in messages:
        item_size = storage_size(message)
        if item_size > budget:
            raise ValueError("batch_message_too_large")
        if current and (len(current) >= maximum or size + item_size > budget):
            chunks.append(current)
            current, size = [], 0
        current.append(message)
        size += item_size
    if current:
        chunks.append(current)
    if len(chunks) > 200:
        raise ValueError("too_many_batch_chunks")
    if sum(storage_size({"campaign": campaign, "messages": chunk}) + 4096 for chunk in chunks) > 8_000_000:
        raise ValueError("batch_transaction_too_large")
    return chunks


def ensure_message_dispatch(message: dict) -> None:
    message_id = message["message_id"]
    ref = db().collection(MESSAGES).document(message_id)
    if message.get("status") == "queued" and not message.get("dispatch_enqueued_at"):
        enqueue_task(
            SUNSHINE_QUEUE,
            "/internal/tasks/sunshine",
            {"kind": "send", "message_id": message_id},
            f"send-{message_id}",
        )
        ref.update({"dispatch_enqueued_at": firestore.SERVER_TIMESTAMP})
    if not message.get("queued_event_recorded_at"):
        emit_event(message, "queued", event_id=doc_id("queued", message_id))
        ref.update({"queued_event_recorded_at": firestore.SERVER_TIMESTAMP})


def legacy_body_values(message: dict) -> list[str]:
    template = message.get("template") if isinstance(message.get("template"), dict) else {}
    for component in template.get("components") or []:
        if isinstance(component, dict) and str(component.get("type") or "").lower() == "body":
            return [
                safe_text(parameter.get("text"), 1000)
                for parameter in component.get("parameters") or []
                if isinstance(parameter, dict) and parameter.get("type") == "text"
            ]
    return []


def sanitize_legacy_payload(raw: dict, app_id: str) -> tuple[dict, dict]:
    if app_id != SUNSHINE_APP_ID:
        raise ValueError("unknown_sunshine_app")
    destination = raw.get("destination") if isinstance(raw.get("destination"), dict) else {}
    message = raw.get("message") if isinstance(raw.get("message"), dict) else {}
    if not message:
        raise ValueError("message_required")
    integration_id = safe_text(destination.get("integrationId") or SUNSHINE_INTEGRATION_ID, 200)
    if integration_id != SUNSHINE_INTEGRATION_ID:
        raise ValueError("unknown_sunshine_integration")
    phone = normalize_phone(destination.get("destinationId"))
    payload = {
        "destination": {"integrationId": integration_id, "destinationId": phone},
        "author": {"role": "appMaker"},
        "message": message,
    }
    if raw.get("messageSchema"):
        payload["messageSchema"] = safe_text(raw.get("messageSchema"), 30)
    if isinstance(raw.get("metadata"), dict):
        payload["metadata"] = raw["metadata"]
    sunshine_wire_payload(payload)
    metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
    detected_template = template_name_from_payload(payload)
    language_obj = ((message.get("template") or {}).get("language") or {}) if isinstance(message.get("template"), dict) else {}
    detected_language = safe_text(language_obj.get("code") if isinstance(language_obj, dict) else language_obj, 20) or "es"
    template_record = approved_template(detected_template, detected_language)
    body_values = legacy_body_values(message)
    expected_values = template_body_parameter_count(template_record) if template_record else 0
    if template_record and len(body_values) != expected_values:
        raise ValueError("template_body_parameter_count_mismatch")
    return payload, {
        "phone": phone,
        "name": safe_text(metadata.get("name") or "Cliente", 200),
        "email": normalize_email(metadata.get("email")),
        "external_id": safe_text(metadata.get("external_id") or phone, 200),
        "template_name": detected_template,
        "template_snapshot": render_template_snapshot(template_record, body_values) if template_record else {},
    }


def campaign_sunshine_payload(message: dict) -> tuple[dict, dict]:
    recipient = message.get("recipient") if isinstance(message.get("recipient"), dict) else {}
    template_config = message.get("template") if isinstance(message.get("template"), dict) else {}
    variables = message.get("variables") if isinstance(message.get("variables"), dict) else {}
    phone = normalize_phone(recipient.get("phone"))
    template_name = safe_text(template_config.get("name"), 200)
    if not template_name:
        raise ValueError("template_name_required")
    language = safe_text(template_config.get("language") or "es", 20)
    template_record = approved_template(template_name, language)
    native_components = []
    body_values = []
    for component in template_config.get("components") or []:
        if not isinstance(component, dict):
            continue
        component_type = safe_text(component.get("type"), 20).lower()
        parameter_names = component.get("parameters") or []
        if component_type == "header" and str(component.get("format", "")).lower() == "image":
            variable_name = safe_text(component.get("parameter") or "header_image", 100)
            image_url = safe_text(variables.get(variable_name), 2000)
            if not image_url.startswith("https://"):
                raise ValueError("invalid_header_image")
            native_components.append({"type": "header", "parameters": [{"type": "image", "image": {"link": image_url}}]})
        elif component_type == "body":
            values = [safe_text(variables.get(str(name)), 1000) for name in parameter_names]
            body_values.extend(values)
            native_components.append({
                "type": "body",
                "parameters": [{"type": "text", "text": value} for value in values],
            })
        elif component_type == "button":
            native_components.append({
                "type": "button",
                "sub_type": safe_text(component.get("sub_type") or "url", 20),
                "index": safe_text(component.get("index") or "0", 5),
                "parameters": [
                    {"type": "text", "text": safe_text(variables.get(str(name)), 1000)}
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
        "destination": {"integrationId": SUNSHINE_INTEGRATION_ID, "destinationId": phone},
        "author": {"role": "appMaker"},
        "messageSchema": "whatsapp",
        "message": {"type": "template", "template": template},
        "metadata": {
            "source_reference": safe_text(message.get("source_reference"), 200),
            "external_id": safe_text(recipient.get("external_id"), 200),
        },
    }
    sunshine_wire_payload(payload)
    return payload, {
        "phone": phone,
        "name": safe_text(recipient.get("name") or "Cliente", 200),
        "email": normalize_email(recipient.get("email")),
        "external_id": safe_text(recipient.get("external_id") or message.get("source_reference") or phone, 200),
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
    message_id = doc_id(campaign_id, run_id, idempotency_key)
    message = {
        "message_id": message_id,
        "campaign_id": campaign_id,
        "run_id": run_id,
        "idempotency_key": idempotency_key,
        "source_reference": source_reference,
        "source_channel": source_channel,
        "recipient": recipient,
        "template_name": recipient.get("template_name"),
        "sunshine_payload": payload,
        "campaign": campaign,
        "status": "queued",
        "created_at": utcnow(),
        "updated_at": utcnow(),
        "expires_at": expires_at(),
    }
    try:
        db().collection(MESSAGES).document(message_id).create(message)
    except AlreadyExists:
        existing = db().collection(MESSAGES).document(message_id).get().to_dict()
        if not existing:
            raise RuntimeError("message_disappeared_during_ingestion")
        ensure_message_dispatch(existing)
        return existing, True

    ensure_message_dispatch(message)
    return message, False


def save_incoming_batch(
    campaign_id: str,
    run_id: str,
    campaign: dict,
    raw_messages: list[dict],
    sealed: bool = False,
) -> dict:
    if not raw_messages:
        raise ValueError("messages_required")
    reporting = campaign.get("reporting") or {}
    if not isinstance(reporting, dict):
        raise ValueError("invalid_reporting")
    try:
        int(reporting.get("delay_minutes") or 15)
    except (ValueError, TypeError):
        raise ValueError("invalid_report_delay") from None

    chunks = partition_messages(raw_messages, campaign)
    batch_id = doc_id(campaign_id, run_id, canonical_json({"campaign": campaign, "messages": raw_messages}))
    chunks = [(doc_id(batch_id, i, canonical_json(data)), data) for i, data in enumerate(chunks)]
    database = db()
    run_ref = database.collection(RUNS).document(run_id)
    now, ttl = utcnow(), batch_expires_at()

    @firestore.transactional
    def persist(transaction):
        run = run_ref.get(transaction=transaction).to_dict() or {}
        receipts = [database.collection(BATCH_RECEIPTS).document(key) for key, _ in chunks]
        previous = [ref.get(transaction=transaction).to_dict() or {} for ref in receipts]
        if any(old.get("status") == "expired" for old in previous):
            raise ValueError("batch_expired")
        if run and run.get("campaign_id") != campaign_id:
            raise ValueError("campaign_mismatch")
        if run and run.get("campaign", {}) != campaign:
            raise ValueError("campaign_configuration_mismatch")
        fresh = [(key, data, receipt) for (key, data), receipt, old in zip(chunks, receipts, previous) if not old]
        if run.get("sealed") and fresh:
            raise ValueError("run_sealed")
        for key, data, receipt in fresh:
            metadata = {
                "chunk_id": key, "batch_id": batch_id, "run_id": run_id,
                "campaign_id": campaign_id, "total_messages": len(data),
                "status": "pending", "created_at": now,
            }
            transaction.create(receipt, {**metadata, "expires_at": expires_at()})
            transaction.create(database.collection(BATCHES).document(key), {
                **metadata, "campaign": campaign, "messages": data, "expires_at": ttl,
            })
        update = {
            "run_id": run_id, "campaign_id": campaign_id, "campaign": campaign,
            "campaign_name": safe_text(campaign.get("campaign_name") or campaign_id, 200),
            "sealed": bool(run.get("sealed") or sealed),
            "received": int(run.get("received", 0)) + sum(len(data) for _, data, _ in fresh),
            "pending_chunks": int(run.get("pending_chunks", 0)) + len(fresh),
            "updated_at": firestore.SERVER_TIMESTAMP,
            "expires_at": expires_at(),
        }
        if not run:
            update.update(status="processing", created_at=firestore.SERVER_TIMESTAMP)
        transaction.set(run_ref, update, merge=True)
        return (
            [key for (key, _), old in zip(chunks, previous) if old.get("status") != "completed"],
            sum(len(data) for _, data, _ in fresh),
        )

    pending, received = persist(database.transaction())
    for key in pending:
        enqueue_task(EVENT_QUEUE, "/internal/tasks/unpack-batch", {"chunk_id": key}, f"unpack-{key}")
    if sealed:
        schedule_report(run_id, campaign)
    return {
        "run_id": run_id, "batch_id": batch_id, "accepted": received,
        "duplicates": len(raw_messages) - received, "rejected": 0,
        "chunks_queued": len(pending), "status": "queued" if pending else "already_processed",
        "status_url": f"/v1/runs/{run_id}", "report_url": f"/v1/runs/{run_id}/report",
    }


def unpack_batch_chunk(chunk_id: str) -> dict:
    database = db()
    chunk_ref = database.collection(BATCHES).document(chunk_id)
    receipt_ref = database.collection(BATCH_RECEIPTS).document(chunk_id)
    receipt = receipt_ref.get().to_dict() or {}
    if receipt.get("status") == "completed":
        return {"status": "already_unpacked", "chunk_id": chunk_id}
    if receipt.get("status") == "expired":
        return {"status": "batch_expired", "chunk_id": chunk_id}
    snapshot = chunk_ref.get()
    if not snapshot.exists:
        if receipt:
            @firestore.transactional
            def mark_missing(transaction):
                current = receipt_ref.get(transaction=transaction).to_dict() or {}
                raw = chunk_ref.get(transaction=transaction)
                run_ref = database.collection(RUNS).document(receipt["run_id"])
                run = run_ref.get(transaction=transaction).to_dict() or {}
                if current.get("status") in {"completed", "expired"} or raw.exists:
                    return
                transaction.set(receipt_ref, {"status": "expired"}, merge=True)
                if run:
                    transaction.update(run_ref, {
                        "ingestion_error": "batch_expired_before_dispatch",
                        "pending_chunks": max(0, int(run.get("pending_chunks", 0)) - 1),
                        "updated_at": firestore.SERVER_TIMESTAMP,
                    })
            mark_missing(database.transaction())
        return {"status": "chunk_gone", "chunk_id": chunk_id}

    chunk = snapshot.to_dict() or {}
    run_id, campaign_id = chunk["run_id"], chunk["campaign_id"]
    campaign = chunk.get("campaign") or {}
    if chunk.get("status") == "completed":
        if chunk.get("sealed"):
            schedule_report(run_id, campaign)

        @firestore.transactional
        def clean_legacy(transaction):
            current = receipt_ref.get(transaction=transaction).to_dict() or {}
            if current.get("status") == "completed":
                return
            transaction.set(receipt_ref, {
                "chunk_id": chunk_id, "run_id": run_id, "campaign_id": campaign_id,
                "status": "completed", "accepted": chunk.get("accepted", 0),
                "duplicates": chunk.get("duplicates", 0), "rejected": chunk.get("rejected", 0),
                "completed_at": firestore.SERVER_TIMESTAMP, "expires_at": expires_at(),
            })
            transaction.delete(chunk_ref)
        clean_legacy(database.transaction())
        return {"status": "already_unpacked", "chunk_id": chunk_id}

    accepted = duplicates = rejected = 0
    seen = set()
    for index, raw in enumerate(chunk.get("messages") or []):
        try:
            if not isinstance(raw, dict):
                raise ValueError("message_object_required")
            key = safe_text(raw.get("idempotency_key"), 200)
            if not key:
                raise ValueError("idempotency_key_required")
            message_id = doc_id(campaign_id, run_id, key)
            if message_id in seen:
                duplicates += 1
                continue
            seen.add(message_id)
            ref = database.collection(MESSAGES).document(message_id)
            message = ref.get().to_dict() or {}
            if not message:
                payload, recipient = campaign_sunshine_payload(raw)
                message = {
                    "message_id": message_id, "run_id": run_id, "campaign_id": campaign_id,
                    "idempotency_key": key, "source_reference": safe_text(raw.get("source_reference") or key, 200),
                    "source_channel": "campaign", "recipient": recipient,
                    "template_name": recipient.get("template_name"), "sunshine_payload": payload,
                    "campaign": campaign, "status": "queued", "ingestion_chunk_id": chunk_id,
                    "ingestion_index": index, "created_at": utcnow(),
                    "updated_at": utcnow(), "expires_at": expires_at(),
                }
                try:
                    ref.create(message)
                except AlreadyExists:
                    message = ref.get().to_dict() or {}
                    if not message:
                        raise RuntimeError("message_disappeared_during_ingestion")
        except ValueError:
            rejected += 1
            continue

        ensure_message_dispatch(message)
        if message.get("ingestion_chunk_id") == chunk_id and message.get("ingestion_index") == index:
            accepted += 1
        else:
            duplicates += 1

    run_ref = database.collection(RUNS).document(run_id)
    run = run_ref.get().to_dict() or {}
    if chunk.get("sealed") or run.get("sealed"):
        schedule_report(run_id, campaign)

    @firestore.transactional
    def finish(transaction):
        current = receipt_ref.get(transaction=transaction).to_dict() or {}
        current_run = run_ref.get(transaction=transaction).to_dict() or {}
        if current.get("status") in {"completed", "expired"}:
            return False
        transaction.set(run_ref, {
            "accepted": firestore.Increment(accepted), "duplicates": firestore.Increment(duplicates),
            "rejected": firestore.Increment(rejected),
            "pending_chunks": max(0, int(current_run.get("pending_chunks", 0)) - 1),
            "updated_at": firestore.SERVER_TIMESTAMP,
        }, merge=True)
        transaction.set(receipt_ref, {
            "chunk_id": chunk_id, "run_id": run_id, "campaign_id": campaign_id,
            "status": "completed", "accepted": accepted, "duplicates": duplicates,
            "rejected": rejected, "completed_at": firestore.SERVER_TIMESTAMP,
            "expires_at": expires_at(),
        }, merge=True)
        transaction.delete(chunk_ref)
        return True

    completed = finish(database.transaction())
    return {
        "status": "unpacked" if completed else "already_unpacked",
        "chunk_id": chunk_id, "accepted": accepted, "duplicates": duplicates, "rejected": rejected,
    }


# =====================================================================
# 6. EVENTS, CALLBACKS & REPORTING
# =====================================================================
def schedule_report(run_id: str, campaign: dict):
    reporting = campaign.get("reporting") if isinstance(campaign.get("reporting"), dict) else {}
    delay = max(1, min(int(reporting.get("delay_minutes") or 15), 1440))
    enqueue_task(
        EVENT_QUEUE,
        "/internal/tasks/report",
        {"run_id": run_id},
        f"report-{run_id}",
        utcnow() + timedelta(minutes=delay),
    )


def run_report(run_id: str, include_rows: bool = True) -> dict:
    run_snapshot = db().collection(RUNS).document(run_id).get()
    if not run_snapshot.exists:
        raise KeyError("run_not_found")
    counts: dict[str, int] = {}
    rows = []
    for snapshot in db().collection(MESSAGES).where("run_id", "==", run_id).stream():
        item = snapshot.to_dict() or {}
        status = item.get("status", "unknown")
        counts[status] = counts.get(status, 0) + 1
        if include_rows:
            recipient = item.get("recipient") or {}
            rows.append({
                "message_id": item.get("message_id"),
                "source_reference": item.get("source_reference"),
                "name": recipient.get("name"),
                "email": recipient.get("email"),
                "phone_masked": "***" + str(recipient.get("phone") or "")[-4:],
                "template_name": item.get("template_name"),
                "status": status,
                "notification_id": item.get("notification_id"),
                "ticket_id": item.get("ticket_id"),
                "error": item.get("error"),
            })
    delivered = counts.get("user_delivered", 0)
    denominator = sum(counts.values())
    return {
        "run": serialize(run_snapshot.to_dict() or {}),
        "total": denominator,
        "delivered": delivered,
        "failed": counts.get("failed", 0),
        "contactability_rate": round(delivered * 100 / denominator, 2) if denominator else 0,
        "counts": counts,
        "rows": rows,
    }


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
        "event_at": utcnow(),
        "expires_at": expires_at(),
    }
    try:
        db().collection(EVENTS).document(event_id).create(event)
    except AlreadyExists:
        pass
    enqueue_task(
        ANALYTICS_QUEUE,
        "/internal/tasks/analytics",
        {"event_id": event_id},
        f"analytics-{event_id}",
    )
    return event_id


def enqueue_callback(message: dict, event_id: str, status: str, details: dict | None = None):
    campaign = message.get("campaign") or {}
    callback_url = allowed_callback_url(campaign.get("callback_url"))
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
        EVENT_QUEUE,
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
        ZENDESK_QUEUE,
        "/internal/tasks/zendesk",
        {"message_id": message["message_id"], "event_id": event_id},
        f"ticket-{message['message_id']}",
    )


def format_subject(template: str, message: dict) -> str:
    recipient = message.get("recipient") or {}
    values = {
        "campaign_name": (message.get("campaign") or {}).get("campaign_name") or message.get("campaign_id"),
        "name": recipient.get("name") or "Cliente",
        "template_name": message.get("template_name") or "",
    }
    result = str(template or "WhatsApp entregado - {campaign_name} - {name}")
    for key, value in values.items():
        result = result.replace("{" + key + "}", safe_text(value, 200))
    return result[:255]


def get_whatsapp_analytics(days: int = 30) -> dict:
    """Compute WhatsApp analytics for the given period (days) comparing with previous period."""
    days = max(1, min(int(days or 30), 90))
    prev_days = days * 2
    client = bq_client()

    runs_list = []
    duplicates_prevented = 0
    try:
        run_snapshots = (
            db()
            .collection(RUNS)
            .order_by("updated_at", direction=firestore.Query.DESCENDING)
            .limit(100)
            .stream()
        )
        for s in run_snapshots:
            r = s.to_dict() or {}
            accepted = int(r.get("accepted", 0) or 0)
            duplicates = int(r.get("duplicates", 0) or 0)
            rejected = int(r.get("rejected", 0) or 0)
            total = accepted + duplicates + rejected
            duplicates_prevented += duplicates
            v_rate = round(accepted * 100.0 / (accepted + rejected), 1) if (accepted + rejected) > 0 else 0.0
            d_rate = round(duplicates * 100.0 / total, 1) if total > 0 else 0.0
            runs_list.append({
                "run_id": r.get("run_id", s.id),
                "campaign_id": r.get("campaign_id", ""),
                "campaign_name": (r.get("campaign") or {}).get("campaign_name") or r.get("campaign_name") or r.get("campaign_id", ""),
                "status": r.get("status", "completed"),
                "accepted": accepted,
                "duplicates": duplicates,
                "rejected": rejected,
                "total": total,
                "valid_rate": v_rate,
                "duplicate_rate": d_rate,
                "created_at": serialize(r.get("created_at") or r.get("updated_at") or utcnow()),
                "updated_at": serialize(r.get("updated_at") or utcnow()),
            })
    except Exception as e:
        logger.warning("Error fetching runs for analytics: %s", e)

    queued_count = 0
    try:
        queued_query = db().collection(MESSAGES).where("status", "==", "queued")
        try:
            count_res = queued_query.count().get()
            queued_count = int(count_res[0][0].value)
        except Exception:
            queued_count = len(list(queued_query.limit(500).stream()))
    except Exception as e:
        logger.warning("Error fetching queued count: %s", e)

    kpis = {
        "sent": 0,
        "delivered": 0,
        "failed": 0,
        "delivery_rate": 0.0,
        "queued": queued_count,
        "unique_clients": 0,
        "campaigns": len(runs_list),
        "duplicates_prevented": duplicates_prevented,
        "changes": {
            "sent": 0.0,
            "delivered": 0.0,
            "failed": 0.0,
            "delivery_rate": 0.0,
            "unique_clients": 0.0,
            "duplicates_prevented": 0.0,
        },
    }
    daily_series = []
    monthly_series = []
    hourly_distribution = []
    templates_ranking = []
    contact_pressure = {
        "7d": {"1": 0, "2": 0, "3": 0, "4+": 0},
        "30d": {"1": 0, "2": 0, "3": 0, "4+": 0},
    }
    errors_taxonomy = []

    if client is not None and PROJECT_ID and bigquery is not None:
        try:
            job_cfg = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("days", "INT64", days),
                    bigquery.ScalarQueryParameter("prev_days", "INT64", prev_days),
                ]
            )

            query_kpis = f"""
                WITH events_window AS (
                  SELECT
                    status,
                    source_reference_hash,
                    CASE
                      WHEN event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY) THEN 'current'
                      WHEN event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @prev_days DAY) THEN 'previous'
                      ELSE 'other'
                    END AS period
                  FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_EVENTS_TABLE}`
                  WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @prev_days DAY)
                )
                SELECT
                  period,
                  COUNTIF(status IN ('submitted', 'channel_delivered', 'user_delivered', 'failed')) AS sent,
                  COUNTIF(status = 'user_delivered') AS delivered,
                  COUNTIF(status = 'failed') AS failed,
                  COUNT(DISTINCT source_reference_hash) AS unique_clients
                FROM events_window
                WHERE period IN ('current', 'previous')
                GROUP BY period
            """
            curr_row = {}
            prev_row = {}
            for row in client.query(query_kpis, job_config=job_cfg, location=REGION).result():
                if row.period == "current":
                    curr_row = dict(row.items())
                elif row.period == "previous":
                    prev_row = dict(row.items())

            c_sent = int(curr_row.get("sent", 0) or 0)
            c_deliv = int(curr_row.get("delivered", 0) or 0)
            c_failed = int(curr_row.get("failed", 0) or 0)
            c_uniq = int(curr_row.get("unique_clients", 0) or 0)
            c_rate = round(c_deliv * 100.0 / (c_deliv + c_failed), 1) if (c_deliv + c_failed) > 0 else 0.0

            p_sent = int(prev_row.get("sent", 0) or 0)
            p_deliv = int(prev_row.get("delivered", 0) or 0)
            p_failed = int(prev_row.get("failed", 0) or 0)
            p_uniq = int(prev_row.get("unique_clients", 0) or 0)
            p_rate = round(p_deliv * 100.0 / (p_deliv + p_failed), 1) if (p_deliv + p_failed) > 0 else 0.0

            def calc_delta(curr: float, prev: float) -> float:
                if prev > 0:
                    return round(((curr - prev) / prev) * 100.0, 1)
                elif curr > 0:
                    return 100.0
                return 0.0

            kpis["sent"] = c_sent
            kpis["delivered"] = c_deliv
            kpis["failed"] = c_failed
            kpis["delivery_rate"] = c_rate
            kpis["unique_clients"] = c_uniq
            kpis["changes"] = {
                "sent": calc_delta(c_sent, p_sent),
                "delivered": calc_delta(c_deliv, p_deliv),
                "failed": calc_delta(c_failed, p_failed),
                "delivery_rate": round(c_rate - p_rate, 1),
                "unique_clients": calc_delta(c_uniq, p_uniq),
                "duplicates_prevented": 0.0,
            }

            query_daily = f"""
                SELECT
                  DATE(event_at) AS day,
                  COUNTIF(status IN ('submitted', 'channel_delivered', 'user_delivered', 'failed')) AS sent,
                  COUNTIF(status = 'user_delivered') AS delivered,
                  COUNTIF(status = 'failed') AS failed,
                  COUNT(DISTINCT source_reference_hash) AS unique_clients
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_EVENTS_TABLE}`
                WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
                GROUP BY day
                ORDER BY day ASC
            """
            for row in client.query(query_daily, job_config=job_cfg, location=REGION).result():
                s = int(row.sent or 0)
                d = int(row.delivered or 0)
                f = int(row.failed or 0)
                u = int(row.unique_clients or 0)
                rate = round(d * 100.0 / (d + f), 1) if (d + f) > 0 else 0.0
                daily_series.append({
                    "date": str(row.day),
                    "sent": s,
                    "delivered": d,
                    "failed": f,
                    "delivery_rate": rate,
                    "unique_clients": u,
                })

            query_monthly = f"""
                SELECT
                  FORMAT_DATE('%Y-%m', DATE(event_at)) AS month,
                  COUNTIF(status IN ('submitted', 'channel_delivered', 'user_delivered', 'failed')) AS sent,
                  COUNTIF(status = 'user_delivered') AS delivered,
                  COUNTIF(status = 'failed') AS failed,
                  COUNT(DISTINCT source_reference_hash) AS unique_clients
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_EVENTS_TABLE}`
                WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                GROUP BY month
                ORDER BY month ASC
            """
            for row in client.query(query_monthly, location=REGION).result():
                s = int(row.sent or 0)
                d = int(row.delivered or 0)
                f = int(row.failed or 0)
                u = int(row.unique_clients or 0)
                rate = round(d * 100.0 / (d + f), 1) if (d + f) > 0 else 0.0
                monthly_series.append({
                    "month": str(row.month),
                    "sent": s,
                    "delivered": d,
                    "failed": f,
                    "delivery_rate": rate,
                    "unique_clients": u,
                })

            query_hourly = f"""
                SELECT
                  EXTRACT(DAYOFWEEK FROM event_at) AS dow,
                  EXTRACT(HOUR FROM event_at) AS hour,
                  COUNT(*) AS total
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_EVENTS_TABLE}`
                WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
                  AND status IN ('submitted', 'channel_delivered', 'user_delivered', 'failed')
                GROUP BY dow, hour
                ORDER BY dow, hour
            """
            for row in client.query(query_hourly, job_config=job_cfg, location=REGION).result():
                hourly_distribution.append({
                    "day_of_week": int(row.dow),
                    "hour": int(row.hour),
                    "total": int(row.total or 0),
                })

            query_templates = f"""
                SELECT
                  template_name,
                  COUNTIF(status IN ('submitted', 'channel_delivered', 'user_delivered', 'failed')) AS total_sends,
                  COUNTIF(status = 'user_delivered') AS delivered,
                  COUNTIF(status = 'failed') AS failed,
                  MAX(event_at) AS last_used
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_EVENTS_TABLE}`
                WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
                  AND template_name IS NOT NULL AND template_name != ''
                GROUP BY template_name
                ORDER BY total_sends DESC
                LIMIT 50
            """
            for row in client.query(query_templates, job_config=job_cfg, location=REGION).result():
                s = int(row.total_sends or 0)
                d = int(row.delivered or 0)
                f = int(row.failed or 0)
                denom = d + f if (d + f) > 0 else s
                d_rate = round(d * 100.0 / denom, 1) if denom > 0 else 0.0
                f_rate = round(f * 100.0 / denom, 1) if denom > 0 else 0.0
                templates_ranking.append({
                    "name": str(row.template_name),
                    "sends": s,
                    "delivered": d,
                    "failed": f,
                    "delivery_rate": d_rate,
                    "failure_rate": f_rate,
                    "last_used": serialize(row.last_used),
                })

            query_pressure = f"""
                WITH p7 AS (
                  SELECT source_reference_hash, COUNT(*) as cnt
                  FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_EVENTS_TABLE}`
                  WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                    AND status = 'user_delivered'
                    AND source_reference_hash IS NOT NULL AND source_reference_hash != ''
                  GROUP BY source_reference_hash
                ),
                p30 AS (
                  SELECT source_reference_hash, COUNT(*) as cnt
                  FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_EVENTS_TABLE}`
                  WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
                    AND status = 'user_delivered'
                    AND source_reference_hash IS NOT NULL AND source_reference_hash != ''
                  GROUP BY source_reference_hash
                )
                SELECT
                  '7d' as window_label,
                  COUNTIF(cnt = 1) as c1,
                  COUNTIF(cnt = 2) as c2,
                  COUNTIF(cnt = 3) as c3,
                  COUNTIF(cnt >= 4) as c4_plus
                FROM p7
                UNION ALL
                SELECT
                  '30d' as window_label,
                  COUNTIF(cnt = 1) as c1,
                  COUNTIF(cnt = 2) as c2,
                  COUNTIF(cnt = 3) as c3,
                  COUNTIF(cnt >= 4) as c4_plus
                FROM p30
            """
            for row in client.query(query_pressure, location=REGION).result():
                w = str(row.window_label)
                if w in contact_pressure:
                    contact_pressure[w] = {
                        "1": int(row.c1 or 0),
                        "2": int(row.c2 or 0),
                        "3": int(row.c3 or 0),
                        "4+": int(row.c4_plus or 0),
                    }

            query_errors = f"""
                SELECT
                  COALESCE(
                    SAFE.JSON_VALUE(details_json, '$.error.message'),
                    SAFE.JSON_VALUE(details_json, '$.error'),
                    SAFE.JSON_VALUE(details_json, '$.reason'),
                    'Fallo de entrega'
                  ) AS reason,
                  COUNT(*) AS total
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_EVENTS_TABLE}`
                WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
                  AND status = 'failed'
                GROUP BY reason
                ORDER BY total DESC
                LIMIT 20
            """
            total_failed = kpis["failed"]
            for row in client.query(query_errors, job_config=job_cfg, location=REGION).result():
                tot = int(row.total or 0)
                pct = round(tot * 100.0 / total_failed, 1) if total_failed > 0 else 0.0
                errors_taxonomy.append({
                    "reason": str(row.reason or "Fallo desconocido"),
                    "total": tot,
                    "percentage": pct,
                })

        except Exception as bq_err:
            logger.warning("BigQuery analytics query failed: %s", bq_err)

    if not templates_ranking:
        try:
            for snap in db().collection(TEMPLATES).stream():
                td = snap.to_dict() or {}
                templates_ranking.append({
                    "name": td.get("name") or snap.id,
                    "sends": 0,
                    "delivered": 0,
                    "failed": 0,
                    "delivery_rate": 0.0,
                    "failure_rate": 0.0,
                    "status": td.get("status", "APPROVED"),
                    "last_used": serialize(td.get("updated_at") or td.get("created_at")),
                })
        except Exception as e:
            logger.warning("Error fetching templates fallback: %s", e)

    return {
        "days": days,
        "kpis": kpis,
        "daily": daily_series,
        "monthly": monthly_series,
        "hourly_distribution": hourly_distribution,
        "templates": templates_ranking,
        "contact_pressure": contact_pressure,
        "errors": errors_taxonomy,
        "runs": runs_list,
    }


# =====================================================================
# 7. FLASK APP & ROUTE HANDLERS
# =====================================================================
def create_app() -> Flask:
    app = Flask(__name__)
    app.config["MAX_CONTENT_LENGTH"] = CORE_MAX_BODY_BYTES

    @app.after_request
    def security_headers(response):
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Cache-Control"] = "no-store"
        return response

    @app.get("/health")
    def health():
        missing = required_config()
        return jsonify({
            "service": "cerebro-sunshine",
            "status": "ok" if not missing else "misconfigured",
            "missing": missing,
        }), 200 if not missing else 503

    # Ingress routes
    @app.get("/internal/ingress/templates")
    @require_gateway
    def ingress_templates():
        items = []
        for snapshot in db().collection(TEMPLATES).where("status", "==", "APPROVED").stream():
            item = snapshot.to_dict() or {}
            item.pop("raw", None)
            items.append(item)
        return jsonify({"templates": items, "cached": True})

    @app.post("/internal/ingress/legacy/apps/<app_id>/notifications")
    @require_gateway
    def ingress_legacy(app_id: str):
        raw = request.get_json(silent=True)
        if not isinstance(raw, dict):
            return jsonify({"error": "json_object_required"}), 400
        try:
            payload, recipient = sanitize_legacy_payload(raw, app_id)
            metadata = raw.get("metadata") if isinstance(raw.get("metadata"), dict) else {}
            source_reference = safe_text(metadata.get("source_reference") or recipient["external_id"], 200)
            idempotency_key = safe_text(metadata.get("idempotency_key") or doc_id(canonical_json(payload)), 200)
            now_key = utcnow().strftime("%Y%m%d")
            campaign = {
                "campaign_name": safe_text(metadata.get("campaign_name") or "Zendesk legacy", 200),
                "zendesk": {
                    "create_ticket_on": "delivered",
                    "subject": "WhatsApp entregado - {name}",
                    "tags": ["cerebro_sunshine", "sin_disparo_whatsapp"],
                },
            }
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

    @app.post("/internal/ingress/campaigns/<campaign_id>/messages:batch")
    @require_gateway
    def ingress_campaign(campaign_id: str):
        try:
            campaign_id = assert_identifier(campaign_id, "campaign_id")
            body = request.get_json(silent=True)
            if not isinstance(body, dict):
                raise ValueError("json_object_required")
            run_id = assert_identifier(body.get("run_id"), "run_id")
            campaign = body.get("campaign") if isinstance(body.get("campaign"), dict) else {}
            raw_messages = body.get("messages")
            if not isinstance(raw_messages, list) or not raw_messages:
                raise ValueError("messages_required")
            if len(raw_messages) > MAX_BATCH_MESSAGES:
                raise ValueError("batch_too_large")

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

    @app.post("/internal/ingress/campaigns/<campaign_id>/runs/<run_id>:seal")
    @require_gateway
    def ingress_seal(campaign_id: str, run_id: str):
        try:
            assert_identifier(campaign_id, "campaign_id")
            assert_identifier(run_id, "run_id")
        except ValueError as error:
            return jsonify({"error": str(error)}), 400
        ref = db().collection(RUNS).document(run_id)
        snapshot = ref.get()
        if not snapshot.exists:
            return jsonify({"error": "run_not_found"}), 404
        run = snapshot.to_dict() or {}
        if run.get("campaign_id") != campaign_id:
            return jsonify({"error": "campaign_mismatch"}), 409
        ref.set({"sealed": True, "sealed_at": firestore.SERVER_TIMESTAMP}, merge=True)
        schedule_report(run_id, run.get("campaign") or {})
        return jsonify({"run_id": run_id, "status": "report_scheduled"}), 202

    @app.get("/internal/ingress/runs/<run_id>")
    @require_gateway
    def ingress_run(run_id: str):
        snapshot = db().collection(RUNS).document(run_id).get()
        if not snapshot.exists:
            return jsonify({"error": "run_not_found"}), 404
        return jsonify(serialize(snapshot.to_dict()))

    @app.get("/internal/ingress/runs/<run_id>/report")
    @require_gateway
    def ingress_report(run_id: str):
        try:
            return jsonify(run_report(run_id))
        except KeyError:
            return jsonify({"error": "run_not_found"}), 404

    @app.post("/internal/ingress/webhooks/sunshine")
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
            event_id = doc_id(canonical_json(event))
            ref = db().collection(EVENTS).document(event_id)
            try:
                ref.create({
                    "event_id": event_id,
                    "raw": event,
                    "status": "webhook_received",
                    "event_at": utcnow(),
                    "expires_at": expires_at(),
                })
                enqueue_task(EVENT_QUEUE, "/internal/tasks/event", {"event_id": event_id}, f"event-{event_id}")
                accepted += 1
            except AlreadyExists:
                pass
        return jsonify({"accepted": accepted, "duplicates": len(incoming) - accepted}), 202

    # Task routes
    @app.post("/internal/tasks/sunshine")
    @require_task(SUNSHINE_QUEUE)
    def route_task_sunshine():
        return task_sunshine()

    @app.post("/internal/tasks/meta")
    @require_task(META_QUEUE)
    def route_task_meta():
        return task_meta()

    @app.post("/internal/tasks/event")
    @require_task(EVENT_QUEUE)
    def route_task_event():
        body = request.get_json(silent=True) or {}
        return task_event_handler(body)

    @app.post("/internal/tasks/zendesk")
    @require_task(ZENDESK_QUEUE)
    def route_task_zendesk():
        body = request.get_json(silent=True) or {}
        return task_zendesk_handler(body)

    @app.post("/internal/tasks/analytics")
    @require_task(ANALYTICS_QUEUE)
    def route_task_analytics():
        body = request.get_json(silent=True) or {}
        return task_analytics_handler(body)

    @app.post("/internal/tasks/callback")
    @require_task(EVENT_QUEUE)
    def route_task_callback():
        body = request.get_json(silent=True) or {}
        return task_callback_handler(body)

    @app.post("/internal/tasks/report")
    @require_task(EVENT_QUEUE)
    def route_task_report():
        body = request.get_json(silent=True) or {}
        return task_report_handler(body)

    @app.post("/internal/tasks/unpack-batch")
    @require_task(EVENT_QUEUE)
    def route_task_unpack_batch():
        body = request.get_json(silent=True) or {}
        chunk_id = str(body.get("chunk_id") or "")
        if not chunk_id:
            return jsonify({"error": "chunk_id_required"}), 400
        result = unpack_batch_chunk(chunk_id)
        return jsonify(result), 200

    @app.get("/internal/admin/analytics")
    @require_gateway
    @require_actor("dashboard:read")
    def admin_analytics():
        days = max(1, min(int(request.args.get("days", "30")), 90))
        return jsonify(get_whatsapp_analytics(days))

    @app.get("/internal/admin/metrics")
    @require_gateway
    @require_actor("dashboard:read")
    def admin_metrics():
        days = max(1, min(int(request.args.get("days", "7")), 90))
        client = bq_client()
        if client is None:
            return jsonify({"days": days, "series": []})
        try:
            query = f"""
                SELECT DATE(event_at) AS day, status, COUNT(*) AS total
                FROM `{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_EVENTS_TABLE}`
                WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
                GROUP BY day, status ORDER BY day DESC, status
            """
            job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("days", "INT64", days)])
            rows = [{"day": str(row.day), "status": row.status, "total": row.total} for row in client.query(query, job_config=job_config, location=REGION).result()]
            return jsonify({"days": days, "series": rows})
        except Exception as err:
            logger.warning("BigQuery metrics query error: %s", err)
            return jsonify({"days": days, "series": []})

    @app.get("/internal/admin/runs")
    @require_gateway
    @require_actor("runs:read")
    def admin_runs():
        limit = max(1, min(int(request.args.get("limit", "50")), 200))
        snapshots = db().collection(RUNS).order_by("updated_at", direction=firestore.Query.DESCENDING).limit(limit).stream()
        return jsonify({"items": [serialize(snapshot.to_dict()) for snapshot in snapshots]})

    @app.get("/internal/admin/runs/<run_id>")
    @require_gateway
    @require_actor("runs:read")
    def admin_run(run_id: str):
        snapshot = db().collection(RUNS).document(run_id).get()
        return (jsonify(serialize(snapshot.to_dict())), 200) if snapshot.exists else (jsonify({"error": "run_not_found"}), 404)

    @app.get("/internal/admin/runs/<run_id>/report")
    @require_gateway
    @require_actor("runs:read")
    def admin_run_report(run_id: str):
        try:
            return jsonify(run_report(run_id))
        except KeyError:
            return jsonify({"error": "run_not_found"}), 404

    @app.post("/internal/admin/runs/<run_id>/retry")
    @require_gateway
    @require_actor("runs:retry")
    def admin_retry(run_id: str):
        queued = 0
        retryable = {"failed", "delivery_unknown"}
        for snapshot in db().collection(MESSAGES).where("run_id", "==", run_id).stream():
            item = snapshot.to_dict() or {}
            if item.get("status") not in retryable:
                continue
            snapshot.reference.set({"status": "queued", "error": firestore.DELETE_FIELD, "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
            if enqueue_task(SUNSHINE_QUEUE, "/internal/tasks/sunshine", {"kind": "send", "message_id": item["message_id"]}, f"retry-{item['message_id']}-{int(time.time())}"):
                queued += 1
        return jsonify({"run_id": run_id, "queued": queued}), 202

    @app.post("/internal/admin/templates/sync")
    @require_gateway
    @require_actor("runs:retry")
    def admin_template_sync():
        meta_synced = False
        meta_count = 0
        meta_error = None
        if META_WABA_ID and META_SYSTEM_TOKEN:
            try:
                res = sync_meta_templates()
                sync_meta_namespace()
                meta_synced = True
                meta_count = res.get("templates", 0)
            except Exception as e:
                meta_error = str(e)
                logger.error("Direct Meta sync failed: %s", e)

        hour_key = str(int(time.time()) // 7200)
        tasks = {
            "sunshine": enqueue_task(SUNSHINE_QUEUE, "/internal/tasks/sunshine", {"kind": "sync_templates"}, f"templates-{hour_key}"),
            "meta_templates": enqueue_task(META_QUEUE, "/internal/tasks/meta", {"kind": "sync_templates"}, f"meta-templates-{hour_key}"),
            "meta_namespace": enqueue_task(META_QUEUE, "/internal/tasks/meta", {"kind": "sync_namespace"}, f"meta-namespace-{hour_key}"),
        }
        status = "synced" if meta_synced else ("queued" if any(tasks.values()) else ("failed" if meta_error else "already_queued"))
        return jsonify({
            "status": status,
            "meta_templates_synced": meta_count,
            "meta_error": meta_error,
            "tasks": tasks,
        }), 200

    @app.errorhandler(413)
    def too_large(_error):
        return jsonify({"error": "payload_too_large"}), 413

    @app.errorhandler(Exception)
    def unhandled(_error):
        logger.exception("Unhandled Cerebro error")
        return jsonify({"error": "internal_error"}), 500

    return app


# =====================================================================
# 8. TASK IMPLEMENTATIONS
# =====================================================================
@require_task(SUNSHINE_QUEUE)
def task_sunshine():
    body = request.get_json(silent=True) or {}
    if body.get("kind") == "sync_templates":
        try:
            return jsonify(sync_templates(safe_text(body.get("after"), 300)))
        except requests.RequestException as error:
            retry_after = error.response.headers.get("Retry-After", "") if error.response is not None else ""
            attempt = int(request.headers.get("X-CloudTasks-TaskRetryCount", "0")) + 1
            return jsonify({"error": "sunshine_temporary_failure"}), 503, {"Retry-After": str(int(retry_delay(attempt, retry_after)) + 1)}

    message_id = str(body.get("message_id") or "")
    ref = db().collection(MESSAGES).document(message_id)
    snapshot = ref.get()
    if not snapshot.exists:
        return jsonify({"error": "message_not_found"}), 404
    message = snapshot.to_dict() or {}
    if message.get("status") in {"submitted", "channel_delivered", "user_delivered", "failed", "delivery_unknown", "conversation_locked", "sending"}:
        return jsonify({"status": message.get("status"), "duplicate_task": True})

    payload = message.get("sunshine_payload") or {}
    try:
        wire = sunshine_wire_payload(payload)
    except ValueError:
        ref.set({"status": "failed", "error": "sunshine_payload_too_large", "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
        failure_id = emit_event(message, "failed", {"code": "payload_too_large"})
        enqueue_callback(message, failure_id, "failed", {"error": {"code": "payload_too_large"}})
        return jsonify({"error": "sunshine_payload_too_large"}), 400

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
                enqueue_task(SUNSHINE_QUEUE, "/internal/tasks/sunshine", {"kind": "send", "message_id": message_id}, f"rate-retry-{message_id}-{attempt}", utcnow() + timedelta(seconds=delay))
            return jsonify({"status": "retry_scheduled" if attempt < 12 else "failed"})
        if response.status_code >= 500:
            raise requests.Timeout("ambiguous_provider_failure")
        if response.status_code == 423:
            ref.set({"status": "conversation_locked", "error": "sunshine_423", "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
            locked_id = emit_event(message, "conversation_locked")
            enqueue_callback(message, locked_id, "conversation_locked", {"error": {"code": "sunshine_423", "message": "Conversation locked"}})
            return jsonify({"error": "conversation_locked"}), 200
        if response.status_code not in {200, 201, 202}:
            try:
                provider_error = response.json().get("error") or {}
            except ValueError:
                provider_error = {}
            ref.set({"provider_error": {"code": safe_text(provider_error.get("code"), 100), "description": safe_text(provider_error.get("description"), 1000)}}, merge=True)
            ref.set({"status": "failed", "error": f"sunshine_{response.status_code}", "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
            failure_id = emit_event(message, "failed", {"http_status": response.status_code})
            enqueue_callback(message, failure_id, "failed", {"error": {"code": f"sunshine_{response.status_code}", "message": "Sunshine rejected the notification"}})
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
        ref.set({"status": "submitted", "notification_id": notification_id, "submitted_at": firestore.SERVER_TIMESTAMP, "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
        db().collection(NOTIFICATION_INDEX).document(notification_id).set({"message_id": message_id, "expires_at": expires_at()})
        message["notification_id"] = notification_id
        submitted_event_id = emit_event(message, "submitted")
        enqueue_callback(message, submitted_event_id, "submitted")
        return jsonify({"message_id": message_id, "notification_id": notification_id, "status": "submitted"})
    except requests.Timeout:
        ref.set({"status": "delivery_unknown", "error": "sunshine_timeout", "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
        unknown_id = emit_event(message, "delivery_unknown", {"code": "timeout_after_submit"})
        enqueue_callback(message, unknown_id, "delivery_unknown", {"error": {"code": "sunshine_timeout", "message": "Delivery result unknown after timeout"}})
        return jsonify({"status": "delivery_unknown"}), 200
    except requests.RequestException:
        ref.set({"status": "delivery_unknown", "error": "sunshine_network_failure"}, merge=True)
        return jsonify({"status": "delivery_unknown"})


@require_task(META_QUEUE)
def task_meta():
    body = request.get_json(silent=True) or {}
    try:
        if body.get("kind") == "sync_templates":
            return jsonify(sync_meta_templates(safe_text(body.get("after"), 500)))
        if body.get("kind") == "sync_namespace":
            return jsonify(sync_meta_namespace())
        return jsonify({"error": "unknown_meta_task"}), 400
    except requests.RequestException:
        return jsonify({"error": "meta_temporary_failure"}), 503


def task_event_handler(body: dict):
    event_id = str(body.get("event_id") or "")
    event_ref = db().collection(EVENTS).document(event_id)
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
    index = db().collection(NOTIFICATION_INDEX).document(notification_id).get()
    if not index.exists:
        return jsonify({"error": "notification_index_pending"}), 503
    message_ref = db().collection(MESSAGES).document(index.to_dict()["message_id"])
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


def task_callback_handler(body: dict):
    callback_url = allowed_callback_url(body.pop("callback_url", ""))
    if not callback_url:
        return jsonify({"error": "invalid_callback_url"}), 400
    session = http_session()
    response = session.post(f"{callback_url}/api/internal/messaging-events", headers={"X-Cerebro-Token": CALLBACK_TOKEN}, json=body, timeout=20)
    if response.status_code >= 500 or response.status_code == 429:
        return jsonify({"error": "callback_temporary_failure"}), 503
    return jsonify({"status": "callback_sent", "http_status": response.status_code})


def task_report_handler(body: dict):
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
    db().collection(RUNS).document(run_id).set({
        "status": "reported",
        "reported_at": firestore.SERVER_TIMESTAMP,
        "summary": {key: report[key] for key in ("total", "delivered", "failed", "contactability_rate", "counts")},
    }, merge=True)
    callback_url = allowed_callback_url((run.get("campaign") or {}).get("callback_url"))
    if callback_url:
        payload = {
            "event_id": str(uuid.uuid4()), "report_id": f"report-{run_id}", "run_id": run_id,
            "campaign_id": run.get("campaign_id"), "status": "sent", "total": report["total"],
            "delivered": report["delivered"], "failed": report["failed"], "contactability_rate": report["contactability_rate"],
            "counts": report["counts"],
        }
        session = http_session()
        response = session.post(f"{callback_url}/api/internal/messaging-reports", headers={"X-Cerebro-Token": CALLBACK_TOKEN}, json=payload, timeout=20)
        if response.status_code >= 500 or response.status_code == 429:
            return jsonify({"error": "report_callback_temporary_failure"}), 503
    return jsonify({"status": "reported", "run_id": run_id})


def task_zendesk_handler(body: dict):
    message_id = str(body.get("message_id") or "")
    message_ref = db().collection(MESSAGES).document(message_id)
    snapshot = message_ref.get()
    if not snapshot.exists:
        return jsonify({"error": "message_not_found"}), 404
    message = snapshot.to_dict() or {}
    if message.get("ticket_id"):
        return jsonify({"ticket_id": message["ticket_id"], "duplicate_task": True})

    recipient = message.get("recipient") or {}
    external_id = f"cerebro-{message_id}"
    try:
        search = zendesk_request("GET", "/api/v2/search.json", params={"query": f"type:ticket external_id:{external_id}"})
        if search.status_code == 200 and search.json().get("results"):
            ticket_id = search.json()["results"][0]["id"]
            message_ref.set({"ticket_id": ticket_id, "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
            message["ticket_id"] = ticket_id
            recovered_event_id = emit_event(message, "zendesk_ticket_created", {"ticket_id": ticket_id, "recovered": True})
            enqueue_callback(message, recovered_event_id, "zendesk_ticket_created", {"ticket_id": ticket_id})
            return jsonify({"ticket_id": ticket_id, "recovered": True})

        user_payload = {"name": recipient.get("name") or "Cliente"}
        if recipient.get("email"):
            user_payload["email"] = recipient["email"]
        if recipient.get("phone"):
            user_payload["phone"] = recipient["phone"]
        user_response = zendesk_request("POST", "/api/v2/users/create_or_update.json", json={"user": user_payload})
        user_response.raise_for_status()
        requester_id = user_response.json()["user"]["id"]

        cfg = (message.get("campaign") or {}).get("zendesk") or {}
        tags = [safe_text(tag, 100) for tag in cfg.get("tags") or []]
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
        ticket_response = zendesk_request("POST", "/api/v2/tickets.json", json={"ticket": ticket_payload})
        ticket_response.raise_for_status()
        ticket_id = ticket_response.json()["ticket"]["id"]
        message_ref.set({"ticket_id": ticket_id, "ticket_created_at": firestore.SERVER_TIMESTAMP, "updated_at": firestore.SERVER_TIMESTAMP}, merge=True)
        message["ticket_id"] = ticket_id
        ticket_event_id = emit_event(message, "zendesk_ticket_created", {"ticket_id": ticket_id})
        enqueue_callback(message, ticket_event_id, "zendesk_ticket_created", {"ticket_id": ticket_id})
        return jsonify({"ticket_id": ticket_id})
    except requests.RequestException:
        logger.exception("Zendesk OAuth request failed")
        return jsonify({"error": "zendesk_temporary_failure"}), 503


def task_analytics_handler(body: dict):
    event_id = str(body.get("event_id") or "")
    snapshot = db().collection(EVENTS).document(event_id).get()
    if not snapshot.exists:
        return jsonify({"error": "event_not_found"}), 404
    event = snapshot.to_dict() or {}
    row = {
        "event_id": event_id,
        "message_id": event.get("message_id"),
        "run_id": event.get("run_id"),
        "campaign_id": event.get("campaign_id"),
        "source_reference_hash": privacy_hash(event.get("source_reference") or ""),
        "template_name": event.get("template_name"),
        "status": event.get("status"),
        "event_at": serialize(event.get("event_at") or utcnow()),
        "details_json": serialize(event.get("details") or {}),
    }
    if bq_client() is not None:
        errors = bq_client().insert_rows_json(f"{PROJECT_ID}.{BIGQUERY_DATASET}.{BIGQUERY_EVENTS_TABLE}", [row], row_ids=[event_id])
        if errors:
            logger.error("BigQuery insert failed: %s", errors)
            return jsonify({"error": "bigquery_insert_failed"}), 503
    snapshot.reference.set({"exported_at": firestore.SERVER_TIMESTAMP}, merge=True)
    return jsonify({"status": "exported", "event_id": event_id})


# =====================================================================
# 9. EXPORTS & FUNCTIONS FRAMEWORK / WSGI ENTRYPOINTS
# =====================================================================
app = create_app()

# Dual support: Functions Framework (for inline editor) and Gunicorn/WSGI
try:
    import functions_framework

    @functions_framework.http
    def hello_http(http_request):
        """Entry point compatible with the default GCP Cloud Functions template."""
        with app.request_context(http_request.environ):
            return app.full_dispatch_request()

except ImportError:
    pass

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=LOCAL_DEV)
