"""Tests for adapters.docker_runner compatibility helpers."""

from __future__ import annotations

import threading

import pytest

import adapters.docker_runner as docker_runner
from adapters.deterministic import DeterministicExecutionControl
from adapters.docker_runner import (
    ExecutionResult,
    normalize_command_parts_for_docker,
    normalize_shell_command_for_docker,
    run_in_docker,
    run_model,
    run_on_host,
)


class TestRunOnHost:
    """Host-based execution (Docker fallback)."""

    def test_echo_via_stdin(self):
        result = run_on_host(
            command="cat",
            stdin_text="hello world",
            timeout=10,
        )
        assert result.exit_code == 0
        assert result.stdout.strip() == "hello world"
        assert result.execution_mode == "host"
        assert not result.timed_out

    def test_pipe_to_python(self):
        result = run_on_host(
            command="python3 -c 'import sys; data = sys.stdin.read(); print(f\"GOT: {data}\")'",
            stdin_text="test input",
            timeout=10,
        )
        assert result.exit_code == 0
        assert "GOT: test input" in result.stdout

    def test_timeout_kills_process(self):
        result = run_on_host(
            command="sleep 60",
            stdin_text="",
            timeout=1,
        )
        assert result.timed_out
        assert result.latency_ms < 5000  # Should be killed within ~1s + grace

    def test_cancel_signal_kills_process_group_before_timeout(self):
        control = DeterministicExecutionControl()
        timer = threading.Timer(0.2, control.request_cancel)
        timer.start()
        try:
            result = run_on_host(
                command="sleep 60",
                stdin_text="",
                timeout=10,
                execution_control=control,
            )
        finally:
            timer.cancel()
        assert result.cancelled
        assert not result.timed_out
        assert result.latency_ms < 5000

    def test_nonzero_exit_code(self):
        result = run_on_host(
            command="exit 42",
            stdin_text="",
            timeout=10,
        )
        assert result.exit_code == 42
        assert not result.timed_out

    def test_stderr_captured(self):
        result = run_on_host(
            command="echo error >&2",
            stdin_text="",
            timeout=10,
        )
        assert "error" in result.stderr

    def test_no_env_leakage(self):
        """Verify CLAUDECODE env vars are stripped."""
        result = run_on_host(
            command="env | grep '^CLAUDECODE=' | wc -l",
            stdin_text="",
            timeout=10,
        )
        assert result.stdout.strip().lstrip() == "0"

    def test_execution_result_immutable(self):
        result = ExecutionResult(
            stdout="test",
            stderr="",
            exit_code=0,
            timed_out=False,
            latency_ms=100,
            execution_mode="host",
        )
        with pytest.raises(AttributeError):
            result.stdout = "changed"


class TestProcessIsolation:
    """Verify process group isolation."""

    def test_child_processes_killed_on_timeout(self):
        """Spawn a child process and verify it's killed when parent times out."""
        result = run_on_host(
            command="bash -c 'sleep 60 & wait'",
            stdin_text="",
            timeout=1,
        )
        assert result.timed_out


def test_run_model_fails_closed_when_docker_is_unavailable(monkeypatch):
    monkeypatch.setattr("adapters.docker_runner._has_docker", lambda: False)

    with pytest.raises(RuntimeError, match="Docker is required"):
        run_model(
            command="echo hello",
            stdin_text="",
            timeout=1,
        )


def test_run_model_uses_host_when_docker_is_not_preferred(monkeypatch):
    monkeypatch.setattr(
        "adapters.docker_runner.run_on_host",
        lambda **kwargs: ExecutionResult(
            stdout="ok",
            stderr="",
            exit_code=0,
            timed_out=False,
            latency_ms=1,
            execution_mode="host",
        ),
    )
    monkeypatch.setattr(
        "adapters.docker_runner._has_docker",
        lambda: (_ for _ in ()).throw(AssertionError("docker availability should not be checked")),
    )

    result = run_model(
        command="echo hello",
        stdin_text="",
        timeout=1,
        prefer_docker=False,
    )

    assert result.execution_mode == "host"
    assert result.stdout == "ok"


def test_normalize_command_parts_for_docker_adds_codex_sandbox_flags():
    normalized = normalize_command_parts_for_docker(
        ["codex", "exec", "-", "--json", "--model", "gpt-5.4-mini"],
    )

    assert normalized == [
        "codex",
        "exec",
        "--dangerously-bypass-approvals-and-sandbox",
        "--skip-git-repo-check",
        "-",
        "--json",
        "--model",
        "gpt-5.4-mini",
    ]


def test_normalize_shell_command_for_docker_strips_legacy_full_auto():
    normalized = normalize_shell_command_for_docker(
        "codex exec --full-auto - --json --model gpt-5.4-mini",
    )

    assert normalized == (
        "codex exec --dangerously-bypass-approvals-and-sandbox "
        "--skip-git-repo-check - --json --model gpt-5.4-mini"
    )


def test_run_model_normalizes_codex_command_before_docker(monkeypatch):
    captured: dict[str, object] = {}

    monkeypatch.setattr("adapters.docker_runner._has_docker", lambda: True)
    monkeypatch.setattr(
        "adapters.docker_runner.run_in_docker",
        lambda **kwargs: captured.update(kwargs) or ExecutionResult(
            stdout="ok",
            stderr="",
            exit_code=0,
            timed_out=False,
            latency_ms=1,
            execution_mode="docker",
        ),
    )

    result = run_model(
        command="codex exec - --json --model gpt-5.4-mini",
        stdin_text="hello",
        timeout=1,
    )

    assert captured["command"] == (
        "codex exec --dangerously-bypass-approvals-and-sandbox "
        "--skip-git-repo-check - --json --model gpt-5.4-mini"
    )
    assert result.execution_mode == "docker"


def test_run_in_docker_mounts_provider_scoped_cli_auth(monkeypatch):
    captured: dict[str, object] = {}

    class _FakePopen:
        def __init__(self, cmd, **kwargs):
            captured["cmd"] = cmd
            captured["kwargs"] = kwargs
            self.returncode = 0

        def communicate(self, input=None, timeout=None):
            captured["stdin"] = input
            captured["timeout"] = timeout
            return ("ok", "")

    monkeypatch.setattr(
        "adapters.docker_runner.resolve_docker_image",
        lambda **kwargs: ("praxis-worker:latest", {"source": "default", "build_error": None}),
    )
    monkeypatch.setattr("adapters.docker_runner._has_docker_image", lambda image: True)
    monkeypatch.setattr(
        "adapters.docker_runner._cli_auth_volume_flags",
        lambda provider_slug=None: (
            ["-v", "/Users/praxis/.codex/auth.json:/root/.codex/auth.json:ro"]
            if provider_slug == "openai"
            else []
        ),
    )
    monkeypatch.setattr("adapters.docker_runner.subprocess.Popen", _FakePopen)

    result = run_in_docker(
        command="echo hello",
        stdin_text="",
        timeout=1,
        provider_slug="openai",
    )

    assert result.execution_mode == "docker"
    assert captured["cmd"][:8] == [
        "docker",
        "run",
        "--rm",
        "-i",
        "--memory",
        "4g",
        "--cpus",
        "2",
    ]
    assert "-v" in captured["cmd"]
    assert "/Users/praxis/.codex/auth.json:/root/.codex/auth.json:ro" in captured["cmd"]


def test_run_in_docker_rejects_unknown_auth_mount_policy(monkeypatch):
    monkeypatch.setattr(docker_runner, "resolve_docker_image", lambda **_kwargs: ("praxis-worker:latest", {}))
    monkeypatch.setattr(docker_runner, "_has_docker_image", lambda _image: True)

    with pytest.raises(ValueError, match="auth_mount_policy must be one of"):
        run_in_docker(
            command="echo hello",
            stdin_text="",
            auth_mount_policy="sideways",
        )


def test_run_in_docker_requires_local_image(monkeypatch):
    monkeypatch.setattr("adapters.docker_runner._has_docker", lambda: True)
    monkeypatch.setattr("adapters.docker_runner._has_docker_image", lambda image: False)
    monkeypatch.setattr(
        "adapters.docker_runner.resolve_docker_image",
        lambda **kwargs: ("praxis-worker:latest", {"source": "default", "build_error": None}),
    )

    with pytest.raises(RuntimeError, match="PRAXIS_DOCKER_IMAGE"):
        run_model(
            command="echo hello",
            stdin_text="",
            timeout=1,
        )


def test_run_in_docker_reads_image_from_env_per_call(monkeypatch):
    seen: dict[str, str] = {}

    monkeypatch.setattr("adapters.docker_runner._has_docker_image", lambda image: seen.setdefault("image", image) or True)
    monkeypatch.setattr("adapters.docker_runner.subprocess.Popen", lambda *args, **kwargs: type(
        "_Proc",
        (),
        {
            "returncode": 0,
            "communicate": staticmethod(lambda input=None, timeout=None: ("ok", "")),
        },
    )())
    monkeypatch.setenv("PRAXIS_DOCKER_IMAGE", "dag-worker:test")

    result = run_in_docker(
        command="echo hello",
        stdin_text="",
        timeout=1,
    )

    assert seen["image"] == "dag-worker:test"
    assert result.execution_mode == "docker"


def test_run_in_docker_accepts_autobuilt_default_image(monkeypatch):
    seen: dict[str, str] = {}

    monkeypatch.delenv("PRAXIS_DOCKER_IMAGE", raising=False)
    monkeypatch.setattr(
        "adapters.docker_runner.resolve_docker_image",
        lambda **kwargs: ("praxis-worker:latest", {"source": "default", "built_default": True, "build_error": None}),
    )
    monkeypatch.setattr(
        "adapters.docker_runner._has_docker_image",
        lambda image: seen.setdefault("image", image) or True,
    )
    monkeypatch.setattr("adapters.docker_runner.subprocess.Popen", lambda *args, **kwargs: type(
        "_Proc",
        (),
        {
            "returncode": 0,
            "communicate": staticmethod(lambda input=None, timeout=None: ("ok", "")),
        },
    )())

    result = run_in_docker(
        command="echo hello",
        stdin_text="",
        timeout=1,
    )

    assert seen["image"] == "praxis-worker:latest"
    assert result.execution_mode == "docker"
