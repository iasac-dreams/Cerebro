"""Meta Graph API client for template and namespace synchronization."""
from __future__ import annotations

import config
from extensions import http_session


def meta_component_summary(components: list[dict]) -> dict:
    summary = {"header_text": "", "body_text": "", "footer_text": "", "buttons": []}
    for component in components:
        if not isinstance(component, dict):
            continue
        kind = str(component.get("type") or "").upper()
        if kind == "HEADER":
            summary["header_text"] = component.get("text") or ""
        elif kind == "BODY":
            summary["body_text"] = component.get("text") or ""
        elif kind == "FOOTER":
            summary["footer_text"] = component.get("text") or ""
        elif kind == "BUTTONS":
            summary["buttons"] = component.get("buttons") or []
    return summary


def fetch_meta_templates(after: str = "") -> dict:
    url = f"https://graph.facebook.com/{config.META_GRAPH_VERSION}/{config.META_WABA_ID}/message_templates"
    params = {"limit": 100, "fields": "id,name,language,status,category,components"}
    if after:
        params["after"] = after
    session = http_session()
    response = session.get(
        url,
        headers={"Authorization": f"Bearer {config.META_SYSTEM_TOKEN}"},
        params=params,
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def fetch_meta_namespace() -> str:
    url = f"https://graph.facebook.com/{config.META_GRAPH_VERSION}/{config.META_WABA_ID}"
    session = http_session()
    response = session.get(
        url,
        headers={"Authorization": f"Bearer {config.META_SYSTEM_TOKEN}"},
        params={"fields": "message_template_namespace"},
        timeout=20,
    )
    response.raise_for_status()
    return config.safe_text(response.json().get("message_template_namespace"), 300)
