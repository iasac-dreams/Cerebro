"""Validation of signed actor claims from Gateway."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from functools import wraps
from flask import jsonify, request
import config


def decode_signed_actor() -> dict:
    token = request.headers.get("X-Cerebro-Actor", "")
    if not token or "." not in token or not config.ACTOR_SIGNING_SECRET:
        return {}
    try:
        raw_part, signature_part = token.split(".", 1)
        raw = base64.urlsafe_b64decode(raw_part + "=" * (-len(raw_part) % 4))
        supplied = base64.urlsafe_b64decode(signature_part + "=" * (-len(signature_part) % 4))
        expected = hmac.new(config.ACTOR_SIGNING_SECRET.encode(), raw, hashlib.sha256).digest()
        if not hmac.compare_digest(supplied, expected):
            return {}
        envelope = json.loads(raw)
        now = int(time.time())
        if int(envelope.get("iat", 0)) > now + 30 or int(envelope.get("exp", 0)) < now:
            return {}
        return envelope.get("actor") or {}
    except Exception:
        return {}


def require_actor(permission: str):
    def decorator(view):
        @wraps(view)
        def wrapped(*args, **kwargs):
            actor = decode_signed_actor()
            if not actor:
                return jsonify({"error": "actor_unauthorized"}), 401
            if permission not in actor.get("permissions", []):
                return jsonify({"error": "forbidden", "permission": permission}), 403
            request.cerebro_actor = actor
            return view(*args, **kwargs)

        return wrapped

    return decorator
