"""Batch unpacker worker for incoming asynchronous campaigns."""
from __future__ import annotations

from flask import jsonify
from services.batch_service import unpack_batch_chunk


def handle_unpack_batch_task(body: dict):
    chunk_id = str(body.get("chunk_id") or "")
    if not chunk_id:
        return jsonify({"error": "chunk_id_required"}), 400
    result = unpack_batch_chunk(chunk_id)
    return jsonify(result), 200
