from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import runtime.docker_image_authority as authority


def test_build_default_docker_image_passes_layout_build_args(monkeypatch, tmp_path) -> None:
    dockerfile = tmp_path / "docker" / "praxis-worker.Dockerfile"
    dockerfile.parent.mkdir()
    dockerfile.write_text("FROM scratch\n", encoding="utf-8")
    captured: dict[str, object] = {}

    monkeypatch.setattr(authority, "workflow_root", lambda: tmp_path)
    monkeypatch.setattr(authority, "container_workspace_root", lambda: Path("/registry-workspace"))
    monkeypatch.setattr(authority, "container_home", lambda: Path("/registry-home"))

    def _run(args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return SimpleNamespace(returncode=0, stderr="", stdout="")

    monkeypatch.setattr(authority.subprocess, "run", _run)

    ok, error = authority.build_default_docker_image()

    assert ok is True
    assert error is None
    args = captured["args"]
    assert "--build-arg" in args
    assert "PRAXIS_CONTAINER_WORKSPACE_ROOT=/registry-workspace" in args
    assert "PRAXIS_CONTAINER_HOME=/registry-home" in args


def test_resolve_docker_image_prefers_explicit_sources(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_DOCKER_IMAGE", "env-worker:test")

    image, meta = authority.resolve_docker_image(
        requested_image="arg-worker:test",
        image_exists=lambda image: image == "arg-worker:test",
    )

    assert image == "arg-worker:test"
    assert meta["source"] == "argument"
    assert meta["built_default"] is False


def test_resolve_docker_image_without_agent_fails_closed(monkeypatch) -> None:
    monkeypatch.delenv("PRAXIS_DOCKER_IMAGE", raising=False)

    image, meta = authority.resolve_docker_image(
        requested_image=None,
        image_exists=lambda _image: False,
    )

    assert image == ""
    assert meta["source"] == "unresolved"
    assert meta["rejected"] is True
    assert meta["reason_code"] == "agent_family_image_unresolved"


def test_resolve_docker_image_rejects_control_worker_override(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_DOCKER_IMAGE", authority.CONTROL_WORKER_IMAGE)

    image, meta = authority.resolve_docker_image(
        requested_image=None,
        image_exists=lambda image: False,
        agent_slug="openai/gpt-5.4",
    )

    assert image == authority.CONTROL_WORKER_IMAGE
    assert meta["source"] == "env"
    assert meta["rejected"] is True
    assert meta["reason_code"] == "control_worker_image_not_model_sandbox"


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


def test_resolve_docker_image_fails_closed_when_thin_build_fails(monkeypatch) -> None:
    """If the thin image can't be built, do not fall back to the worker image."""
    monkeypatch.delenv("PRAXIS_DOCKER_IMAGE", raising=False)
    calls: list[str] = []

    def _exists(image: str) -> bool:
        return image == authority.DEFAULT_DOCKER_IMAGE  # only control worker exists

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

    assert image == "praxis-gemini:latest"
    assert meta["source"] == "agent_family"
    assert meta["reason_code"] == "thin_sandbox_image_unavailable"
    assert meta["build_error"] == "no dockerfile"
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
