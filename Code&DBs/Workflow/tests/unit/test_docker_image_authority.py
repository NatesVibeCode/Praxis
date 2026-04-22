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


# -----------------------------------------------------------------------------
# Per-agent-family thin-image dispatch (M2).
# -----------------------------------------------------------------------------


def test_resolve_thin_image_for_agent_family_prefix() -> None:
    assert authority.resolve_thin_image_for_agent("openai/gpt-5.4") == "praxis-codex:latest"
    assert authority.resolve_thin_image_for_agent("anthropic/claude-opus-4-7") == "praxis-claude:latest"
    assert authority.resolve_thin_image_for_agent("google/gemini-3-pro") == "praxis-gemini:latest"
    # Bare family slugs (no model) also work.
    assert authority.resolve_thin_image_for_agent("codex") == "praxis-codex:latest"
    assert authority.resolve_thin_image_for_agent("claude") == "praxis-claude:latest"
    assert authority.resolve_thin_image_for_agent("gemini") == "praxis-gemini:latest"


def test_resolve_thin_image_for_agent_handles_empty_or_unknown() -> None:
    assert authority.resolve_thin_image_for_agent(None) is None
    assert authority.resolve_thin_image_for_agent("") is None
    assert authority.resolve_thin_image_for_agent("   ") is None
    assert authority.resolve_thin_image_for_agent("deepseek/chat") is None
    assert authority.resolve_thin_image_for_agent("cursor/local") is None


def test_resolve_docker_image_dispatches_to_thin_image(monkeypatch) -> None:
    """When agent_slug is anthropic/claude-* and the thin image is on the host,
    use it instead of the fat default."""
    monkeypatch.delenv("PRAXIS_DOCKER_IMAGE", raising=False)

    image, meta = authority.resolve_docker_image(
        requested_image=None,
        image_exists=lambda image: image == "praxis-claude:latest",
        agent_slug="anthropic/claude-opus-4-7",
    )

    assert image == "praxis-claude:latest"
    assert meta["source"] == "agent_family"
    assert meta["built_default"] is False


def test_resolve_docker_image_autobuilds_thin_when_missing(monkeypatch) -> None:
    """Missing thin image triggers auto-build of the thin Dockerfile."""
    monkeypatch.delenv("PRAXIS_DOCKER_IMAGE", raising=False)
    existed = {"praxis-codex:latest": False}

    def _exists(image: str) -> bool:
        return existed.get(image, False)

    def _build(image_name: str = authority.DEFAULT_DOCKER_IMAGE, timeout_seconds: int = authority.DEFAULT_BUILD_TIMEOUT_SECONDS):
        del timeout_seconds
        assert image_name == "praxis-codex:latest", (
            "auto-build must target the thin image, not the fat default"
        )
        existed["praxis-codex:latest"] = True
        return True, None

    monkeypatch.setattr(authority, "build_default_docker_image", _build)

    image, meta = authority.resolve_docker_image(
        requested_image=None,
        image_exists=_exists,
        agent_slug="openai/gpt-5.4",
    )

    assert image == "praxis-codex:latest"
    assert meta["source"] == "agent_family"
    assert meta["built_default"] is True


def test_resolve_docker_image_falls_back_to_fat_when_thin_build_fails(monkeypatch) -> None:
    """If the thin image can't be built, fall through to the fat default so
    jobs never hard-fail on a missing thin image."""
    monkeypatch.delenv("PRAXIS_DOCKER_IMAGE", raising=False)
    calls: list[str] = []

    def _exists(image: str) -> bool:
        return image == authority.DEFAULT_DOCKER_IMAGE  # only fat exists

    def _build(image_name: str = authority.DEFAULT_DOCKER_IMAGE, timeout_seconds: int = authority.DEFAULT_BUILD_TIMEOUT_SECONDS):
        del timeout_seconds
        calls.append(image_name)
        if image_name == "praxis-gemini:latest":
            return False, "no dockerfile"
        return True, None

    monkeypatch.setattr(authority, "build_default_docker_image", _build)

    image, meta = authority.resolve_docker_image(
        requested_image=None,
        image_exists=_exists,
        agent_slug="google/gemini-3-pro",
    )

    # Fell through to fat default after thin build failed.
    assert image == authority.DEFAULT_DOCKER_IMAGE
    assert meta["source"] == "default"
    assert "praxis-gemini:latest" in calls


def test_resolve_docker_image_explicit_override_beats_agent_family(monkeypatch) -> None:
    """An explicit requested_image wins over agent-family dispatch."""
    monkeypatch.delenv("PRAXIS_DOCKER_IMAGE", raising=False)

    image, meta = authority.resolve_docker_image(
        requested_image="custom-sandbox:v2",
        image_exists=lambda _image: True,
        agent_slug="anthropic/claude-opus-4-7",
    )

    assert image == "custom-sandbox:v2"
    assert meta["source"] == "argument"
