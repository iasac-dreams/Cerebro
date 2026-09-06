"""BigQuery pseudonymized analytics worker."""
from __future__ import annotations

import json
import logging
from flask import jsonify
from google.cloud import firestore
import config
from extensions import db, bq_client

logger = logging.getLogger(__name__)


def handle_analytics_task(body: dict):
    event_id = str(body.get("event_id") or "")
    snapshot = db().collection(config.EVENTS).document(event_id).get()
    if not snapshot.exists:
        return jsonify({"error": "event_not_found"}), 404
    event = snapshot.to_dict() or {}
    row = {
        "event_id": event_id,
        "message_id": event.get("message_id"),
        "run_id": event.get("run_id"),
        "campaign_id": event.get("campaign_id"),
        "source_reference_hash": config.privacy_hash(event.get("source_reference") or ""),
        "template_name": event.get("template_name"),
        "status": event.get("status"),
        "event_at": config.serialize(event.get("event_at") or config.utcnow()),
        "details_json": json.dumps(config.serialize(event.get("details") or {})),
    }
    if bq_client() is not None:
        try:
            errors = bq_client().insert_rows_json(
                f"{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_EVENTS_TABLE}",
                [row],
                row_ids=[event_id],
            )
            if errors:
                logger.warning("BigQuery insert failed: %s", errors)
        except Exception as bq_err:
            logger.warning("BigQuery analytics export skipped: %s", bq_err)
    snapshot.reference.set({"exported_at": firestore.SERVER_TIMESTAMP}, merge=True)
    return jsonify({"status": "exported", "event_id": event_id})
