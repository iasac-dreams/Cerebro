"""Cloud Tasks worker routes protected by task secret and queue matching."""
from __future__ import annotations

from flask import Blueprint, request
import config
from security.decorators import require_task
from tasks.sunshine_tasks import handle_sunshine_task
from tasks.meta_tasks import handle_meta_task
from tasks.event_tasks import handle_event_task, handle_callback_task, handle_report_task
from tasks.zendesk_tasks import handle_zendesk_task
from tasks.analytics_tasks import handle_analytics_task
from tasks.batch_tasks import handle_unpack_batch_task

tasks_bp = Blueprint("tasks", __name__)


@tasks_bp.post("/sunshine")
@require_task(config.SUNSHINE_QUEUE)
def task_sunshine():
    body = request.get_json(silent=True) or {}
    return handle_sunshine_task(body)


@tasks_bp.post("/meta")
@require_task(config.META_QUEUE)
def task_meta():
    body = request.get_json(silent=True) or {}
    return handle_meta_task(body)


@tasks_bp.post("/event")
@require_task(config.EVENT_QUEUE)
def task_event():
    body = request.get_json(silent=True) or {}
    return handle_event_task(body)


@tasks_bp.post("/zendesk")
@require_task(config.ZENDESK_QUEUE)
def task_zendesk():
    body = request.get_json(silent=True) or {}
    return handle_zendesk_task(body)


@tasks_bp.post("/analytics")
@require_task(config.ANALYTICS_QUEUE)
def task_analytics():
    body = request.get_json(silent=True) or {}
    return handle_analytics_task(body)


@tasks_bp.post("/callback")
@require_task(config.EVENT_QUEUE)
def task_callback():
    body = request.get_json(silent=True) or {}
    return handle_callback_task(body)


@tasks_bp.post("/report")
@require_task(config.EVENT_QUEUE)
def task_report():
    body = request.get_json(silent=True) or {}
    return handle_report_task(body)


@tasks_bp.post("/unpack-batch")
@require_task(config.EVENT_QUEUE)
def task_unpack_batch():
    body = request.get_json(silent=True) or {}
    return handle_unpack_batch_task(body)
