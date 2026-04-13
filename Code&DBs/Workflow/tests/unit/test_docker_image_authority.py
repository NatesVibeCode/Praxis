from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import runtime.docker_image_authority as authority


def test_resolve_docker_image_prefers_explicit_sources(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_DOCKER_IMAGE", "env-worker:test")

    image, meta = authority.resolve_docker_image(
        requested_image="arg-worker:test",
        image_exists=lambda image: image == "arg-worker:test",
    )

    assert image == "arg-worker:test"
    assert meta["source"] == "argument"
    assert meta["built_default"] is False


def test_resolve_docker_image_autobuilds_missing_default(monkeypatch) -> None:
    attempts = {"count": 0}

    monkeypatch.delenv("PRAXIS_DOCKER_IMAGE", raising=False)

    def _exists(image: str) -> bool:
        if image != authority.DEFAULT_DOCKER_IMAGE:
            return False
        return attempts["count"] > 0

    def _build_default(image_name: str = authority.DEFAULT_DOCKER_IMAGE, timeout_seconds: int = authority.DEFAULT_BUILD_TIMEOUT_SECONDS):
        del timeout_seconds
        assert image_name == authority.DEFAULT_DOCKER_IMAGE
        attempts["count"] += 1
        return True, None

    monkeypatch.setattr(authority, "build_default_docker_image", _build_default)

    image, meta = authority.resolve_docker_image(
        requested_image=None,
        image_exists=_exists,
    )

    assert image == authority.DEFAULT_DOCKER_IMAGE
    assert meta["source"] == "default"
    assert meta["built_default"] is True
    assert attempts["count"] == 1


def test_resolve_docker_image_surfaces_build_failure(monkeypatch) -> None:
    monkeypatch.delenv("PRAXIS_DOCKER_IMAGE", raising=False)
    monkeypatch.setattr(
        authority,
        "build_default_docker_image",
        lambda **kwargs: (False, "boom"),
    )

    image, meta = authority.resolve_docker_image(
        requested_image=None,
        image_exists=lambda image: False,
    )

    assert image == authority.DEFAULT_DOCKER_IMAGE
    assert meta["source"] == "default"
    assert meta["built_default"] is False
    assert meta["build_error"] == "boom"
