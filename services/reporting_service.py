"""Reporting, contactability analytics and run status aggregation."""
from __future__ import annotations

from datetime import timedelta
import config
from extensions import db, enqueue_task


def schedule_report(run_id: str, campaign: dict):
    reporting = campaign.get("reporting") if isinstance(campaign.get("reporting"), dict) else {}
    delay = max(1, min(int(reporting.get("delay_minutes") or 15), 1440))
    enqueue_task(
        config.EVENT_QUEUE,
        "/internal/tasks/report",
        {"run_id": run_id},
        f"report-{run_id}",
        config.utcnow() + timedelta(minutes=delay),
    )


def run_report(run_id: str, include_rows: bool = True) -> dict:
    run_snapshot = db().collection(config.RUNS).document(run_id).get()
    if not run_snapshot.exists:
        raise KeyError("run_not_found")
    counts: dict[str, int] = {}
    rows = []
    for snapshot in db().collection(config.MESSAGES).where("run_id", "==", run_id).stream():
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
        "run": config.serialize(run_snapshot.to_dict() or {}),
        "total": denominator,
        "delivered": delivered,
        "failed": counts.get("failed", 0),
        "contactability_rate": round(delivered * 100 / denominator, 2) if denominator else 0,
        "counts": counts,
        "rows": rows,
    }
