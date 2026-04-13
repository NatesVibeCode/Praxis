"""Tests for adapters.docker_runner compatibility helpers."""

from __future__ import annotations

import threading

import pytest

from adapters.deterministic import DeterministicExecutionControl
from adapters.docker_runner import ExecutionResult, run_in_docker, run_model, run_on_host


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


def test_run_in_docker_requires_local_image(monkeypatch):
    monkeypatch.setattr("adapters.docker_runner._has_docker", lambda: True)
    monkeypatch.setattr("adapters.docker_runner._has_docker_image", lambda image: False)

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
