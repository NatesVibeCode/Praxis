from __future__ import annotations

from types import SimpleNamespace

from runtime.workflow import execution_backends


def _agent(**overrides):
    values = {
        "provider": "openai",
        "model": "gpt-5.4-mini",
        "wrapper_command": "wizard-cli --json",
        "timeout_seconds": 15,
        "max_output_tokens": 2048,
        "execution_transport": "cli",
        "sandbox_provider": "seatbelt_local",
        "sandbox_policy": SimpleNamespace(
            network_policy="provider_only",
            workspace_materialization="copy",
            secret_allowlist=(),
        ),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _sandbox_result(**overrides):
    values = {
        "exit_code": 0,
        "stdout": "plain output",
        "stderr": "",
        "timed_out": False,
        "execution_mode": "seatbelt_local",
        "sandbox_provider": "seatbelt_local",
        "execution_transport": "cli",
        "sandbox_session_id": "sandbox_session:run.alpha:job.alpha",
        "sandbox_group_id": "group:run.alpha",
        "artifact_refs": ("README.md",),
        "started_at": "2026-04-09T00:00:00+00:00",
        "finished_at": "2026-04-09T00:00:01+00:00",
        "network_policy": "provider_only",
        "provider_latency_ms": 12,
        "workspace_root": "/tmp/workspace",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_execute_cli_routes_through_sandbox_runtime(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured.update(kwargs)
            return _sandbox_result()

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        execution_backends,
        "augment_cli_command_for_workflow_mcp",
        lambda **kwargs: list(kwargs["command_parts"]),
    )

    result = execution_backends.execute_cli(
        _agent(),
        "hello from stdin",
        str(tmp_path),
        execution_bundle={
            "run_id": "run.alpha",
            "job_label": "job.alpha",
            "mcp_tool_names": ["praxis_query", "praxis_discover"],
            "skill_refs": ["workflow", "cli-summary"],
        },
    )

    assert captured["provider_name"] == "seatbelt_local"
    assert captured["command"] == "wizard-cli --json"
    assert captured["stdin_text"] == "hello from stdin"
    assert captured["execution_transport"] == "cli"
    assert captured["sandbox_session_id"] == "sandbox_session:run.alpha:job.alpha"
    assert captured["env"]["OPENAI_API_KEY"] == "test-key"
    assert captured["env"]["PRAXIS_ALLOWED_MCP_TOOLS"] == "praxis_query,praxis_discover"
    assert captured["env"]["PRAXIS_ALLOWED_MCP_TOOLS"] == "praxis_query,praxis_discover"
    assert result["status"] == "succeeded"
    assert result["sandbox_provider"] == "seatbelt_local"
    assert result["artifact_refs"] == ["README.md"]


def test_execute_cli_exports_ripgrep_config_path(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured.update(kwargs)
            return _sandbox_result()

    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    (repo_root / ".git").mkdir()
    (repo_root / ".ripgreprc").write_text(
        "--glob=!**/artifacts/workflow_outputs/**\n",
        encoding="utf-8",
    )
    workdir = repo_root / "nested" / "job"
    workdir.mkdir(parents=True)

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        execution_backends,
        "augment_cli_command_for_workflow_mcp",
        lambda **kwargs: list(kwargs["command_parts"]),
    )

    execution_backends.execute_cli(_agent(), "hello", str(workdir))

    assert captured["env"]["RIPGREP_CONFIG_PATH"] == "../../.ripgreprc"


def test_execute_cli_uses_argv_prompt_when_wrapper_declares_prompt_placeholder(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured.update(kwargs)
            return _sandbox_result()

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        execution_backends,
        "augment_cli_command_for_workflow_mcp",
        lambda **kwargs: list(kwargs["command_parts"]),
    )

    result = execution_backends.execute_cli(
        _agent(wrapper_command="wizard-cli --json '{prompt}'"),
        "hello from argv",
        str(tmp_path),
    )

    assert captured["command"] == "wizard-cli --json 'hello from argv'"
    assert captured["stdin_text"] == ""
    assert result["status"] == "succeeded"


def test_execute_cli_rejects_prompt_file_delivery(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")

    result = execution_backends.execute_cli(
        _agent(wrapper_command="wizard-cli --prompt-file {prompt_file}"),
        "hello",
        str(tmp_path),
    )

    assert result["status"] == "failed"
    assert result["error_code"] == "sandbox_error"


def test_execute_cli_returns_sandbox_error_on_runtime_failure(monkeypatch, tmp_path) -> None:
    class _FakeRuntime:
        def execute_command(self, **kwargs):
            del kwargs
            raise RuntimeError("seatbelt failed")

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        execution_backends,
        "augment_cli_command_for_workflow_mcp",
        lambda **kwargs: list(kwargs["command_parts"]),
    )

    result = execution_backends.execute_cli(_agent(), "hello", str(tmp_path))

    assert result["status"] == "failed"
    assert result["error_code"] == "sandbox_error"
    assert "seatbelt failed" in result["stderr"]


def test_execute_cli_builds_default_command_from_provider_registry(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured.update(kwargs)
            return _sandbox_result()

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        execution_backends,
        "augment_cli_command_for_workflow_mcp",
        lambda **kwargs: list(kwargs["command_parts"]),
    )
    monkeypatch.setattr(
        execution_backends,
        "build_command",
        lambda provider_slug, model=None: ["codex", "exec", "-", "--json", "--model", model or provider_slug],
    )

    result = execution_backends.execute_cli(
        _agent(wrapper_command=None, provider="openai", model="gpt-5.4-mini"),
        "hello from stdin",
        str(tmp_path),
    )

    assert captured["command"] == "codex exec --skip-git-repo-check - --json --model gpt-5.4-mini"
    assert captured["stdin_text"] == "hello from stdin"
    assert result["status"] == "succeeded"


def test_execute_cli_returns_sandbox_error_when_default_command_build_fails(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(
        execution_backends,
        "build_command",
        lambda provider_slug, model=None: (_ for _ in ()).throw(RuntimeError(f"unknown provider {provider_slug}")),
    )

    result = execution_backends.execute_cli(
        _agent(wrapper_command=None, provider="mystery"),
        "hello",
        str(tmp_path),
    )

    assert result["status"] == "failed"
    assert result["error_code"] == "sandbox_error"
    assert "unknown provider mystery" in result["stderr"]


def test_execute_api_routes_through_sandbox_runtime(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured.update(kwargs)
            return _sandbox_result(
                stdout="api output",
                execution_transport="api",
            )

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())

    result = execution_backends.execute_api(
        _agent(
            provider="anthropic",
            model="claude-haiku-4-5-20251001",
            wrapper_command=None,
            execution_transport="api",
        ),
        "hello from api",
        workdir=str(tmp_path),
        execution_bundle={"run_id": "run.alpha", "job_label": "job.api"},
    )

    command = str(captured["command"])
    assert "python3 -m runtime.api_transport_worker" in command
    assert "--provider anthropic" in command
    assert "--model claude-haiku-4-5-20251001" in command
    assert captured["stdin_text"] == "hello from api"
    assert captured["execution_transport"] == "api"
    assert captured["sandbox_session_id"] == "sandbox_session:run.alpha:job.api"
    assert result["status"] == "succeeded"
    assert result["stdout"] == "api output"
