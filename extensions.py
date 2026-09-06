"""GCP client singletons, connection pooling and Cloud Tasks dispatcher."""
from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from google.api_core.exceptions import AlreadyExists
from google.cloud import firestore, tasks_v2
try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None
from google.protobuf import timestamp_pb2
import config

logger = logging.getLogger(__name__)

_db: firestore.Client | None = None
_tasks: tasks_v2.CloudTasksClient | None = None
_bq: bigquery.Client | None = None
_session: requests.Session | None = None


def db() -> firestore.Client:
    global _db
    if _db is None:
        _db = firestore.Client(project=config.PROJECT_ID or None, database=config.FIRESTORE_DATABASE)
    return _db


def tasks_client() -> tasks_v2.CloudTasksClient:
    global _tasks
    if _tasks is None:
        _tasks = tasks_v2.CloudTasksClient()
    return _tasks


def bq_client() -> bigquery.Client | None:
    global _bq
    if _bq is None and bigquery is not None:
        try:
            _bq = bigquery.Client(project=config.PROJECT_ID or None, location=config.REGION)
        except Exception as e:
            logger.warning("Could not initialize BigQuery client: %s", e)
            return None
    return _bq


def http_session() -> requests.Session:
    """Returns a shared HTTP session with connection pooling (Keep-Alive)."""
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


def canonical_json(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def enqueue_task(queue: str, path: str, payload: dict, task_key: str, schedule_at: datetime | None = None) -> bool:
    """Enqueues an HTTP task to Cloud Tasks with OIDC service account authentication."""
    try:
        client = tasks_client()
        if client is None:
            return False
        project_id = config.PROJECT_ID or config.resolve_project_id()
        tasks_location = config.TASKS_LOCATION or config.REGION or "southamerica-west1"
        parent = client.queue_path(project_id, tasks_location, queue)
        task_name = client.task_path(project_id, tasks_location, queue, config.doc_id(queue, task_key)[:40])
        sa_email = config.TASK_INVOKER_SERVICE_ACCOUNT or "462948619262-compute@developer.gserviceaccount.com"
        audience = config.TASK_OIDC_AUDIENCE or config.SERVICE_URL or "https://cerebro-sunshine-462948619262.southamerica-west1.run.app"
        task = {
            "name": task_name,
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": config.task_url(path),
                "headers": {
                    "Content-Type": "application/json",
                    "X-Cerebro-Task-Secret": config.TASK_SHARED_SECRET,
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
    except Exception:
        return False
