"""Template validation, rendering and synchronization services."""
from __future__ import annotations

import re
import time
from google.cloud import firestore
import config
from extensions import db, enqueue_task
from clients.sunshine import fetch_message_templates
from clients.meta import fetch_meta_templates, fetch_meta_namespace, meta_component_summary

_template_status_cache: dict[str, tuple[float, dict | None]] = {}
_namespace_cache: tuple[float, str] = (0, "")


def template_name_from_payload(payload: dict) -> str:
    message = payload.get("message") if isinstance(payload.get("message"), dict) else {}
    template = message.get("template") if isinstance(message.get("template"), dict) else {}
    hsm = message.get("hsm") if isinstance(message.get("hsm"), dict) else {}
    return config.safe_text(
        template.get("name") or hsm.get("templateName") or hsm.get("template_name") or message.get("template_name"),
        200,
    )


def approved_template(name: str, language: str) -> dict:
    if not name or not config.REQUIRE_APPROVED_TEMPLATE:
        return {}
    key = config.doc_id(name, language)
    cached = _template_status_cache.get(key)
    if cached and cached[0] > time.monotonic():
        if not cached[1] or cached[1].get("meta_status") != "APPROVED":
            raise ValueError("template_not_approved")
        return cached[1]
    snapshot = db().collection(config.TEMPLATES).document(key).get()
    template = snapshot.to_dict() if snapshot.exists else None
    _template_status_cache[key] = (time.monotonic() + 300, template)
    if not template or template.get("meta_status") != "APPROVED":
        raise ValueError("template_not_approved")
    return template


def template_body_parameter_count(template: dict) -> int:
    indexes = []
    for component in template.get("components") or []:
        if not isinstance(component, dict):
            continue
        if str(component.get("type", "")).upper() != "BODY":
            continue
        indexes.extend(int(value) for value in re.findall(r"\{\{\s*(\d+)\s*\}\}", str(component.get("text") or "")))
    return max(indexes, default=0)


def render_template_snapshot(template: dict, values: list[str]) -> dict:
    rendered = {
        "name": template.get("name"),
        "language": template.get("language"),
        "category": template.get("category"),
        "header": "",
        "body": "",
        "footer": "",
        "buttons": [],
        "parameters": values,
    }
    for component in template.get("components") or []:
        if not isinstance(component, dict):
            continue
        kind = str(component.get("type") or "").upper()
        if kind == "BODY":
            body = str(component.get("text") or "")
            for index, value in enumerate(values, start=1):
                body = re.sub(r"\{\{\s*" + str(index) + r"\s*\}\}", lambda _match, replacement=value: replacement, body)
            rendered["body"] = body
        elif kind == "HEADER":
            rendered["header"] = component.get("text") or ""
        elif kind == "FOOTER":
            rendered["footer"] = component.get("text") or ""
        elif kind == "BUTTONS":
            rendered["buttons"] = component.get("buttons") or []
    return rendered


def template_namespace() -> str:
    global _namespace_cache
    if config.SUNSHINE_NAMESPACE:
        return config.SUNSHINE_NAMESPACE
    if _namespace_cache[0] > time.monotonic():
        return _namespace_cache[1]
    snapshot = db().collection(config.CONFIG).document("meta").get()
    namespace = config.safe_text((snapshot.to_dict() or {}).get("message_template_namespace"), 300) if snapshot.exists else ""
    _namespace_cache = (time.monotonic() + 300, namespace)
    return namespace


def sync_templates(after: str = "") -> dict:
    result = fetch_message_templates(after=after)
    templates = result.get("messageTemplates", [])
    batch = db().batch()
    for item in templates:
        name = config.safe_text(item.get("name"), 200)
        language = config.safe_text(item.get("language"), 20)
        if not name:
            continue
        batch.set(
            db().collection(config.TEMPLATES).document(config.doc_id(name, language)),
            {
                "name": name,
                "language": language,
                "sunshine_status": item.get("status"),
                "sunshine_category": item.get("category"),
                "sunshine_components": item.get("components") or [],
                "source_sunshine": True,
                "synced_at": firestore.SERVER_TIMESTAMP,
            },
            merge=True,
        )
    batch.commit()
    _template_status_cache.clear()
    next_cursor = config.safe_text(result.get("after"), 300)
    if next_cursor:
        enqueue_task(
            config.SUNSHINE_QUEUE,
            "/internal/tasks/sunshine",
            {"kind": "sync_templates", "after": next_cursor},
            f"templates-page-{int(time.time()) // 7200}-{next_cursor}",
        )
    return {"templates": len(templates), "next_page_queued": bool(next_cursor)}


def sync_meta_templates(after: str = "") -> dict:
    result = fetch_meta_templates(after=after)
    templates = result.get("data") if isinstance(result.get("data"), list) else []
    batch = db().batch()
    stored = 0
    for item in templates:
        if not isinstance(item, dict):
            continue
        name = config.safe_text(item.get("name"), 200)
        language = config.safe_text(item.get("language"), 20)
        if not name or not language:
            continue
        components = item.get("components") if isinstance(item.get("components"), list) else []
        data = {
            "name": name,
            "language": language,
            "status": item.get("status"),
            "meta_status": item.get("status"),
            "category": item.get("category"),
            "meta_template_id": config.safe_text(item.get("id"), 200),
            "components": components,
            "body_parameter_count": template_body_parameter_count({"components": components}),
            "source_meta": True,
            "meta_synced_at": firestore.SERVER_TIMESTAMP,
            **meta_component_summary(components),
        }
        batch.set(db().collection(config.TEMPLATES).document(config.doc_id(name, language)), data, merge=True)
        stored += 1
    if stored:
        batch.commit()
    _template_status_cache.clear()
    paging = result.get("paging") if isinstance(result.get("paging"), dict) else {}
    cursors = paging.get("cursors") if isinstance(paging.get("cursors"), dict) else {}
    next_cursor = config.safe_text(cursors.get("after"), 500) if paging.get("next") else ""
    if next_cursor:
        enqueue_task(
            config.META_QUEUE,
            "/internal/tasks/meta",
            {"kind": "sync_templates", "after": next_cursor},
            f"meta-templates-page-{next_cursor}",
        )
    return {"templates": stored, "next_page_queued": bool(next_cursor)}


def sync_meta_namespace() -> dict:
    global _namespace_cache
    namespace = fetch_meta_namespace()
    if namespace:
        db().collection(config.CONFIG).document("meta").set({
            "message_template_namespace": namespace,
            "namespace_synced_at": firestore.SERVER_TIMESTAMP,
        }, merge=True)
        _namespace_cache = (time.monotonic() + 300, namespace)
    return {"namespace_available": bool(namespace)}
