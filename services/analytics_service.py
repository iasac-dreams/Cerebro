"""WhatsApp analytics computation from BigQuery and Firestore."""
from __future__ import annotations

import logging
from datetime import datetime, UTC
from google.cloud import firestore
try:
    from google.cloud import bigquery
except ImportError:
    bigquery = None

import config
from extensions import db, bq_client

logger = logging.getLogger(__name__)


def get_whatsapp_analytics(days: int = 30) -> dict:
    """Compute WhatsApp analytics for the given period (days) comparing with previous period."""
    days = max(1, min(int(days or 30), 90))
    prev_days = days * 2
    client = bq_client()

    # 1. Firestore Runs & Queued
    runs_list = []
    duplicates_prevented = 0
    try:
        run_snapshots = (
            db()
            .collection(config.RUNS)
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
                "created_at": config.serialize(r.get("created_at") or r.get("updated_at") or datetime.now(UTC)),
                "updated_at": config.serialize(r.get("updated_at") or datetime.now(UTC)),
            })
    except Exception as e:
        logger.warning("Error fetching runs for analytics: %s", e)

    queued_count = 0
    try:
        queued_query = db().collection(config.MESSAGES).where("status", "==", "queued")
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

    if client is not None and config.PROJECT_ID and bigquery is not None:
        try:
            job_cfg = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("days", "INT64", days),
                    bigquery.ScalarQueryParameter("prev_days", "INT64", prev_days),
                ]
            )

            # 2. Periods comparison
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
                  FROM `{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_EVENTS_TABLE}`
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
            for row in client.query(query_kpis, job_config=job_cfg, location=config.REGION).result():
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

            # 3. Daily Series
            query_daily = f"""
                SELECT
                  DATE(event_at) AS day,
                  COUNTIF(status IN ('submitted', 'channel_delivered', 'user_delivered', 'failed')) AS sent,
                  COUNTIF(status = 'user_delivered') AS delivered,
                  COUNTIF(status = 'failed') AS failed,
                  COUNT(DISTINCT source_reference_hash) AS unique_clients
                FROM `{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_EVENTS_TABLE}`
                WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
                GROUP BY day
                ORDER BY day ASC
            """
            for row in client.query(query_daily, job_config=job_cfg, location=config.REGION).result():
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

            # 4. Monthly Series
            query_monthly = f"""
                SELECT
                  FORMAT_DATE('%Y-%m', DATE(event_at)) AS month,
                  COUNTIF(status IN ('submitted', 'channel_delivered', 'user_delivered', 'failed')) AS sent,
                  COUNTIF(status = 'user_delivered') AS delivered,
                  COUNTIF(status = 'failed') AS failed,
                  COUNT(DISTINCT source_reference_hash) AS unique_clients
                FROM `{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_EVENTS_TABLE}`
                WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 365 DAY)
                GROUP BY month
                ORDER BY month ASC
            """
            for row in client.query(query_monthly, location=config.REGION).result():
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

            # 5. Hourly Distribution
            query_hourly = f"""
                SELECT
                  EXTRACT(DAYOFWEEK FROM event_at) AS dow,
                  EXTRACT(HOUR FROM event_at) AS hour,
                  COUNT(*) AS total
                FROM `{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_EVENTS_TABLE}`
                WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
                  AND status IN ('submitted', 'channel_delivered', 'user_delivered', 'failed')
                GROUP BY dow, hour
                ORDER BY dow, hour
            """
            for row in client.query(query_hourly, job_config=job_cfg, location=config.REGION).result():
                hourly_distribution.append({
                    "day_of_week": int(row.dow),
                    "hour": int(row.hour),
                    "total": int(row.total or 0),
                })

            # 6. Templates Ranking
            query_templates = f"""
                SELECT
                  template_name,
                  COUNTIF(status IN ('submitted', 'channel_delivered', 'user_delivered', 'failed')) AS total_sends,
                  COUNTIF(status = 'user_delivered') AS delivered,
                  COUNTIF(status = 'failed') AS failed,
                  MAX(event_at) AS last_used
                FROM `{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_EVENTS_TABLE}`
                WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
                  AND template_name IS NOT NULL AND template_name != ''
                GROUP BY template_name
                ORDER BY total_sends DESC
                LIMIT 50
            """
            for row in client.query(query_templates, job_config=job_cfg, location=config.REGION).result():
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
                    "last_used": config.serialize(row.last_used),
                })

            # 7. Contact Pressure
            query_pressure = f"""
                WITH p7 AS (
                  SELECT source_reference_hash, COUNT(*) as cnt
                  FROM `{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_EVENTS_TABLE}`
                  WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
                    AND status = 'user_delivered'
                    AND source_reference_hash IS NOT NULL AND source_reference_hash != ''
                  GROUP BY source_reference_hash
                ),
                p30 AS (
                  SELECT source_reference_hash, COUNT(*) as cnt
                  FROM `{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_EVENTS_TABLE}`
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
            for row in client.query(query_pressure, location=config.REGION).result():
                w = str(row.window_label)
                if w in contact_pressure:
                    contact_pressure[w] = {
                        "1": int(row.c1 or 0),
                        "2": int(row.c2 or 0),
                        "3": int(row.c3 or 0),
                        "4+": int(row.c4_plus or 0),
                    }

            # 8. Error Taxonomy
            query_errors = f"""
                SELECT
                  COALESCE(
                    SAFE.JSON_VALUE(details_json, '$.error.message'),
                    SAFE.JSON_VALUE(details_json, '$.error'),
                    SAFE.JSON_VALUE(details_json, '$.reason'),
                    'Fallo de entrega'
                  ) AS reason,
                  COUNT(*) AS total
                FROM `{config.PROJECT_ID}.{config.BIGQUERY_DATASET}.{config.BIGQUERY_EVENTS_TABLE}`
                WHERE event_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL @days DAY)
                  AND status = 'failed'
                GROUP BY reason
                ORDER BY total DESC
                LIMIT 20
            """
            total_failed = kpis["failed"]
            for row in client.query(query_errors, job_config=job_cfg, location=config.REGION).result():
                tot = int(row.total or 0)
                pct = round(tot * 100.0 / total_failed, 1) if total_failed > 0 else 0.0
                errors_taxonomy.append({
                    "reason": str(row.reason or "Fallo desconocido"),
                    "total": tot,
                    "percentage": pct,
                })

        except Exception as bq_err:
            logger.warning("BigQuery analytics query failed: %s", bq_err)

    # Fallback templates if none found in BigQuery
    if not templates_ranking:
        try:
            for snap in db().collection(config.TEMPLATES).stream():
                td = snap.to_dict() or {}
                templates_ranking.append({
                    "name": td.get("name") or snap.id,
                    "sends": 0,
                    "delivered": 0,
                    "failed": 0,
                    "delivery_rate": 0.0,
                    "failure_rate": 0.0,
                    "status": td.get("status", "APPROVED"),
                    "last_used": config.serialize(td.get("updated_at") or td.get("created_at")),
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
