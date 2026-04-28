"""Compatibility tests for the retired operator console route.

Covers:
  - /console redirects into the canonical app shell with chat docked
  - Legacy PWA assets stay gated while the root no longer serves a second UI
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from surfaces.api import rest  # noqa: E402


def test_operator_console_redirects_to_app_chat_when_gate_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PRAXIS_OPERATOR_DEV_MODE", raising=False)
    with TestClient(rest.app) as client:
        response = client.get("/console", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/app?chat=sidebar&source=console"
    assert response.headers["cache-control"].startswith("no-store")


@pytest.mark.parametrize("value", ["1", "true", "TRUE", "yes", "YES"])
def test_operator_console_redirects_to_app_chat_when_gate_on(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("PRAXIS_OPERATOR_DEV_MODE", value)
    with TestClient(rest.app) as client:
        response = client.get("/console", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/app?chat=sidebar&source=console"


def test_operator_console_root_no_longer_serves_agent_sessions_html(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PRAXIS_OPERATOR_DEV_MODE", "1")
    with TestClient(rest.app) as client:
        response = client.get("/console", follow_redirects=False)
    body = response.text
    assert "/api/agent-sessions/agents" not in body
    assert "Praxis Operator Console" not in body


def test_operator_console_trailing_slash_also_redirects(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PRAXIS_OPERATOR_DEV_MODE", raising=False)
    with TestClient(rest.app) as client:
        response = client.get("/console/", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/app?chat=sidebar&source=console"


def test_operator_console_zero_still_redirects_root(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRAXIS_OPERATOR_DEV_MODE", "0")
    with TestClient(rest.app) as client:
        response = client.get("/console", follow_redirects=False)
    assert response.status_code == 307
    assert response.headers["location"] == "/app?chat=sidebar&source=console"


def test_operator_console_serves_pwa_manifest_and_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PRAXIS_OPERATOR_DEV_MODE", "1")
    with TestClient(rest.app) as client:
        manifest = client.get("/console/manifest.webmanifest")
        worker = client.get("/console/sw.js")
        icon = client.get("/console/icon.svg")
        icon_192 = client.get("/console/icon-192.png")
        icon_512 = client.get("/console/icon-512.png")

    assert manifest.status_code == 200
    assert manifest.headers["content-type"].startswith("application/manifest+json")
    payload = manifest.json()
    assert payload["display"] == "standalone"
    assert payload["start_url"].startswith("/console")
    assert payload["icons"][0]["src"] == "/console/icon-192.png"
    assert payload["icons"][0]["sizes"] == "192x192"
    assert payload["icons"][1]["src"] == "/console/icon-512.png"
    assert payload["icons"][1]["sizes"] == "512x512"

    assert worker.status_code == 200
    assert "showNotification" in worker.text
    assert "notificationclick" in worker.text
    assert "/console/icon-192.png" in worker.text
    assert "/console/icon-512.png" in worker.text
    assert worker.headers["service-worker-allowed"] == "/console"

    assert icon.status_code == 200
    assert icon.headers["content-type"].startswith("image/svg+xml")
    assert "Praxis premium logo" in icon.text
    assert icon_192.status_code == 200
    assert icon_192.headers["content-type"].startswith("image/png")
    assert icon_192.content.startswith(b"\x89PNG")
    assert icon_512.status_code == 200
    assert icon_512.headers["content-type"].startswith("image/png")
    assert icon_512.content.startswith(b"\x89PNG")
