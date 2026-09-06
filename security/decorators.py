"""Security decorators for Gateway calls and Cloud Tasks workers."""
from __future__ import annotations

import hmac
from functools import wraps
from flask import jsonify, request
import config


def compare(left: str, right: str) -> bool:
    return bool(left and right and hmac.compare_digest(str(left), str(right)))


def require_gateway(view):
    @wraps(view)
    def wrapped(*args, **kwargs):
        if not config.LOCAL_DEV and not compare(request.headers.get("X-Cerebro-Gateway-Secret", ""), config.GATEWAY_SHARED_SECRET):
            return jsonify({"error": "gateway_unauthorized"}), 401
        return view(*args, **kwargs)

    return wrapped


def require_task(queue_name: str):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            if config.LOCAL_DEV:
                return view(*args, **kwargs)
            if not compare(request.headers.get("X-Cerebro-Task-Secret", ""), config.TASK_SHARED_SECRET):
                return jsonify({"error": "task_secret_invalid"}), 401
            if request.headers.get("X-CloudTasks-QueueName", "") != queue_name:
                return jsonify({"error": "task_queue_invalid"}), 401
            if not request.headers.get("X-CloudTasks-TaskName", ""):
                return jsonify({"error": "task_name_missing"}), 401
            return view(*args, **kwargs)

        return wrapped

    return decorator
