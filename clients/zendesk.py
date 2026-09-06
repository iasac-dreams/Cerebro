"""Zendesk API client with OAuth Client Credentials."""
from __future__ import annotations

import time
import requests
import config
from extensions import http_session

_zendesk_access_token = ""
_zendesk_access_token_expires_at = 0


def zendesk_access_token() -> str:
    global _zendesk_access_token, _zendesk_access_token_expires_at
    now = int(time.time())
    if _zendesk_access_token and now < _zendesk_access_token_expires_at - 60:
        return _zendesk_access_token
    session = http_session()
    response = session.post(
        f"https://{config.ZENDESK_SUBDOMAIN}.zendesk.com/oauth/tokens",
        data={
            "grant_type": "client_credentials",
            "client_id": config.ZENDESK_OAUTH_CLIENT_ID,
            "client_secret": config.ZENDESK_OAUTH_CLIENT_SECRET,
            "scope": config.ZENDESK_OAUTH_SCOPE,
        },
        timeout=20,
    )
    response.raise_for_status()
    data = response.json()
    _zendesk_access_token = data["access_token"]
    _zendesk_access_token_expires_at = now + int(data.get("expires_in") or 1800)
    return _zendesk_access_token


def zendesk_request(method: str, path: str, **kwargs) -> requests.Response:
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update({
        "Authorization": f"Bearer {zendesk_access_token()}",
        "Content-Type": "application/json",
    })
    session = http_session()
    return session.request(
        method,
        f"https://{config.ZENDESK_SUBDOMAIN}.zendesk.com{path}",
        headers=headers,
        timeout=30,
        **kwargs,
    )


def search_ticket(external_id: str) -> int | None:
    search = zendesk_request(
        "GET",
        "/api/v2/search.json",
        params={"query": f"type:ticket external_id:{external_id}"},
    )
    if search.status_code == 200 and search.json().get("results"):
        return search.json()["results"][0]["id"]
    return None


def find_or_create_user(recipient: dict) -> int:
    user_payload = {"name": recipient.get("name") or "Cliente"}
    if recipient.get("email"):
        user_payload["email"] = recipient["email"]
    if recipient.get("phone"):
        user_payload["phone"] = recipient["phone"]
    response = zendesk_request(
        "POST",
        "/api/v2/users/create_or_update.json",
        json={"user": user_payload},
    )
    response.raise_for_status()
    return response.json()["user"]["id"]


def create_solved_ticket(ticket_payload: dict) -> int:
    response = zendesk_request(
        "POST",
        "/api/v2/tickets.json",
        json={"ticket": ticket_payload},
    )
    response.raise_for_status()
    return response.json()["ticket"]["id"]


def update_ticket_internal_note(ticket_id: int | str, body: str, tags: list[str] | None = None) -> bool:
    payload = {
        "ticket": {
            "comment": {"body": body, "public": False},
        }
    }
    if tags:
        payload["ticket"]["additional_tags"] = tags
    response = zendesk_request("PUT", f"/api/v2/tickets/{ticket_id}.json", json=payload)
    return response.status_code in (200, 201)

