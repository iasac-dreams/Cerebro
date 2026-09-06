"""Meta Graph API synchronization worker."""
from __future__ import annotations

import logging
from flask import jsonify
import requests
import config
from services.template_service import sync_meta_templates, sync_meta_namespace

logger = logging.getLogger(__name__)


def handle_meta_task(body: dict):
    try:
        if body.get("kind") == "sync_templates":
            return jsonify(sync_meta_templates(config.safe_text(body.get("after"), 500)))
        if body.get("kind") == "sync_namespace":
            return jsonify(sync_meta_namespace())
        return jsonify({"error": "unknown_meta_task"}), 400
    except requests.RequestException:
        logger.exception("Meta Graph synchronization failed")
        return jsonify({"error": "meta_temporary_failure"}), 503
