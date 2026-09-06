"""Admin management routes protected by Gateway and signed actor claims."""
from __future__ import annotations

import time
from flask import Blueprint, jsonify, request
from google.cloud import firestore
try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None
import config
from extensions import db, bq_client, enqueue_task
from security.decorators import require_gateway
from security.actors import require_actor
from services.reporting_service import run_report

admin_bp = Blueprint("admin", __name__)


@admin_bp.get("/metrics")
@require_gateway
@require_actor("dashboard:read")
def admin_metrics():
    days = max(1, min(int(request.args.get("days", "7")), 90))
    query = f"""
        SELECT DATE(event_at) AS day, status, COUNT(*) AS total
        FROM `{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_EVENTS_TABLE}`
        WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
        GROUP BY day, status ORDER BY day DESC, status
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("days", "INT64", days)]
    )
    rows = [
        {"day": str(row.day), "status": row.status, "total": row.total}
        for row in bq_client().query(query, job_config=job_config).result()
    ]
    return jsonify({"days": days, "series": rows})


@admin_bp.get("/runs")
@require_gateway
@require_actor("runs:read")
def admin_runs():
    limit = max(1, min(int(request.args.get("limit", "50")), 200))
    snapshots = (
        db()
        .collection(config.RUNS)
        .order_by("updated_at", direction=firestore.Query.DESCENDING)
        .limit(limit)
        .stream()
    )
    return jsonify({"items": [config.serialize(snapshot.to_dict()) for snapshot in snapshots]})


@admin_bp.get("/runs/<run_id>")
@require_gateway
@require_actor("runs:read")
def admin_run(run_id: str):
    snapshot = db().collection(config.RUNS).document(run_id).get()
    return (
        (jsonify(config.serialize(snapshot.to_dict())), 200)
        if snapshot.exists
        else (jsonify({"error": "run_not_found"}), 404)
    )


@admin_bp.get("/runs/<run_id>/report")
@require_gateway
@require_actor("runs:read")
def admin_run_report(run_id: str):
    try:
        return jsonify(run_report(run_id))
    except KeyError:
        return jsonify({"error": "run_not_found"}), 404


@admin_bp.post("/runs/<run_id>/retry")
@require_gateway
@require_actor("runs:retry")
def admin_retry(run_id: str):
    queued = 0
    retryable = {"failed", "delivery_unknown"}
    for snapshot in db().collection(config.MESSAGES).where("run_id", "==", run_id).stream():
        item = snapshot.to_dict() or {}
        if item.get("status") not in retryable:
            continue
        snapshot.reference.set(
            {"status": "queued", "error": firestore.DELETE_FIELD, "updated_at": firestore.SERVER_TIMESTAMP},
            merge=True,
        )
        if enqueue_task(
            config.SUNSHINE_QUEUE,
            "/internal/tasks/sunshine",
            {"kind": "send", "message_id": item["message_id"]},
            f"retry-{item['message_id']}-{int(time.time())}",
        ):
            queued += 1
    return jsonify({"run_id": run_id, "queued": queued}), 202


@admin_bp.post("/templates/sync")
@require_gateway
@require_actor("runs:retry")
def admin_template_sync():
    hour_key = str(int(time.time()) // 7200)
    created = {
        "sunshine": enqueue_task(
            config.SUNSHINE_QUEUE,
            "/internal/tasks/sunshine",
            {"kind": "sync_templates"},
            f"templates-{hour_key}",
        ),
        "meta_templates": enqueue_task(
            config.META_QUEUE,
            "/internal/tasks/meta",
            {"kind": "sync_templates"},
            f"meta-templates-{hour_key}",
        ),
        "meta_namespace": enqueue_task(
            config.META_QUEUE,
            "/internal/tasks/meta",
            {"kind": "sync_namespace"},
            f"meta-namespace-{hour_key}",
        ),
    }
    return jsonify({"status": "queued" if any(created.values()) else "already_queued", "tasks": created}), 202
