"""Durable batch ingestion; delete raw chunks only after dispatch is confirmed."""
from __future__ import annotations

from datetime import datetime
from google.api_core.exceptions import AlreadyExists
from google.cloud import firestore
import config
from extensions import db, enqueue_task, canonical_json
from services.message_service import campaign_sunshine_payload, ensure_message_dispatch


def storage_size(value) -> int:
    """Conservative Firestore field-size estimate, including map overhead."""
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
    # Reserve space for document names and metadata below Firestore's 1 MiB limit.
    budget = 900_000 - storage_size(campaign) - 4096
    maximum = max(1, min(config.BATCH_CHUNK_SIZE, 300))
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
    # Two documents per chunk plus the run in the ingestion transaction.
    if len(chunks) > 200:
        raise ValueError("too_many_batch_chunks")
    if sum(storage_size({"campaign": campaign, "messages": chunk}) + 4096 for chunk in chunks) > 8_000_000:
        raise ValueError("batch_transaction_too_large")
    return chunks


def save_incoming_batch(campaign_id: str, run_id: str, campaign: dict,
                        raw_messages: list[dict], sealed: bool = False) -> dict:
    from services.reporting_service import schedule_report

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
    batch_id = config.doc_id(campaign_id, run_id, canonical_json({
        "campaign": campaign, "messages": raw_messages,
    }))
    # Include partition contents so changing chunk size cannot lose data.
    chunks = [(config.doc_id(batch_id, i, canonical_json(data)), data) for i, data in enumerate(chunks)]
    database = db()
    run_ref = database.collection(config.RUNS).document(run_id)
    now, ttl = config.utcnow(), config.batch_expires_at()

    @firestore.transactional
    def persist(transaction):
        run = run_ref.get(transaction=transaction).to_dict() or {}
        receipts = [database.collection(config.BATCH_RECEIPTS).document(key) for key, _ in chunks]
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
            transaction.create(receipt, {**metadata, "expires_at": config.expires_at()})
            transaction.create(database.collection(config.BATCHES).document(key), {
                **metadata, "campaign": campaign, "messages": data, "expires_at": ttl,
            })
        update = {
            "run_id": run_id, "campaign_id": campaign_id, "campaign": campaign,
            "campaign_name": config.safe_text(campaign.get("campaign_name") or campaign_id, 200),
            "sealed": bool(run.get("sealed") or sealed),
            "received": int(run.get("received", 0)) + sum(len(data) for _, data, _ in fresh),
            "pending_chunks": int(run.get("pending_chunks", 0)) + len(fresh),
            "updated_at": firestore.SERVER_TIMESTAMP,
            "expires_at": config.expires_at(),
        }
        if not run:
            update.update(status="processing", created_at=firestore.SERVER_TIMESTAMP)
        transaction.set(run_ref, update, merge=True)
        return [key for (key, _), old in zip(chunks, previous) if old.get("status") != "completed"], sum(len(data) for _, data, _ in fresh)

    pending, received = persist(database.transaction())
    for key in pending:
        enqueue_task(config.EVENT_QUEUE, "/internal/tasks/unpack-batch", {"chunk_id": key}, f"unpack-{key}")
    if sealed:
        schedule_report(run_id, campaign)
    return {
        "run_id": run_id, "batch_id": batch_id, "accepted": received,
        "duplicates": len(raw_messages) - received, "rejected": 0,
        "chunks_queued": len(pending), "status": "queued" if pending else "already_processed",
        "status_url": f"/v1/runs/{run_id}", "report_url": f"/v1/runs/{run_id}/report",
    }


def unpack_batch_chunk(chunk_id: str) -> dict:
    from services.reporting_service import schedule_report

    database = db()
    chunk_ref = database.collection(config.BATCHES).document(chunk_id)
    receipt_ref = database.collection(config.BATCH_RECEIPTS).document(chunk_id)
    receipt = receipt_ref.get().to_dict() or {}
    if receipt.get("status") == "completed":
        return {"status": "already_unpacked", "chunk_id": chunk_id}
    if receipt.get("status") == "expired":
        return {"status": "batch_expired", "chunk_id": chunk_id}
    snapshot = chunk_ref.get()
    if not snapshot.exists:
        # A repeated task after deletion is terminal. Missing pending input is an error.
        if receipt:
            @firestore.transactional
            def mark_missing(transaction):
                current = receipt_ref.get(transaction=transaction).to_dict() or {}
                raw = chunk_ref.get(transaction=transaction)
                run_ref = database.collection(config.RUNS).document(receipt["run_id"])
                run = run_ref.get(transaction=transaction).to_dict() or {}
                # Another worker may have completed and deleted the input since our read.
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
        # Migrate old completed chunks without replaying or recounting their messages.
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
                "completed_at": firestore.SERVER_TIMESTAMP, "expires_at": config.expires_at(),
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
            key = config.safe_text(raw.get("idempotency_key"), 200)
            if not key:
                raise ValueError("idempotency_key_required")
            message_id = config.doc_id(campaign_id, run_id, key)
            if message_id in seen:
                duplicates += 1
                continue
            seen.add(message_id)
            ref = database.collection(config.MESSAGES).document(message_id)
            message = ref.get().to_dict() or {}
            if not message:
                payload, recipient = campaign_sunshine_payload(raw)
                message = {
                    "message_id": message_id, "run_id": run_id, "campaign_id": campaign_id,
                    "idempotency_key": key, "source_reference": config.safe_text(raw.get("source_reference") or key, 200),
                    "source_channel": "campaign", "recipient": recipient,
                    "template_name": recipient.get("template_name"), "sunshine_payload": payload,
                    "campaign": campaign, "status": "queued", "ingestion_chunk_id": chunk_id,
                    "ingestion_index": index, "created_at": config.utcnow(),
                    "updated_at": config.utcnow(), "expires_at": config.expires_at(),
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
        # Repair enqueue failures, never reset provider state.
        ensure_message_dispatch(message)
        if message.get("ingestion_chunk_id") == chunk_id and message.get("ingestion_index") == index:
            accepted += 1
        else:
            duplicates += 1

    run_ref = database.collection(config.RUNS).document(run_id)
    run = run_ref.get().to_dict() or {}
    # Schedule before marking done: failures must remain retryable.
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
        # Compact receipt contains no recipient list or provider payload.
        transaction.set(receipt_ref, {
            "chunk_id": chunk_id, "run_id": run_id, "campaign_id": campaign_id,
            "status": "completed", "accepted": accepted, "duplicates": duplicates,
            "rejected": rejected, "completed_at": firestore.SERVER_TIMESTAMP,
            "expires_at": config.expires_at(),
        }, merge=True)
        transaction.delete(chunk_ref)
        return True

    completed = finish(database.transaction())
    return {"status": "unpacked" if completed else "already_unpacked", "chunk_id": chunk_id,
            "accepted": accepted, "duplicates": duplicates, "rejected": rejected}

