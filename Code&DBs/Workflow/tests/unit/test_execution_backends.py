from __future__ import annotations

import sys
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
        "sandbox_provider": "docker_local",
        "sandbox_policy": SimpleNamespace(
            network_policy="provider_only",
            workspace_materialization="copy",
            secret_allowlist=(),
        ),
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _provider_profile(
    *,
    api_protocol_family: str | None = None,
    api_endpoint: str | None = None,
    api_key_env_vars: tuple[str, ...] = (),
    sandbox_env_overrides: dict[str, object] | None = None,
):
    return SimpleNamespace(
        api_protocol_family=api_protocol_family,
        api_endpoint=api_endpoint,
        api_key_env_vars=api_key_env_vars,
        sandbox_env_overrides=sandbox_env_overrides or {},
    )


def _sandbox_result(**overrides):
    values = {
        "exit_code": 0,
        "stdout": "plain output",
        "stderr": "",
        "timed_out": False,
        "execution_mode": "docker_local",
        "sandbox_provider": "docker_local",
        "execution_transport": "cli",
        "sandbox_session_id": "sandbox_session:run.alpha:job.alpha",
        "sandbox_group_id": "group:run.alpha",
        "artifact_refs": ("README.md",),
        "started_at": "2026-04-09T00:00:00+00:00",
        "finished_at": "2026-04-09T00:00:01+00:00",
        "workspace_snapshot_ref": "workspace_snapshot:test1234",
        "workspace_snapshot_cache_hit": True,
        "network_policy": "provider_only",
        "provider_latency_ms": 12,
        "workspace_root": "/tmp/workspace",
        "container_cpu_percent": None,
        "container_mem_bytes": None,
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
            "access_policy": {"write_scope": ["README.md"]},
        },
    )

    assert captured["provider_name"] == "docker_local"
    assert captured["command"] == "wizard-cli --json"
    assert captured["stdin_text"] == "hello from stdin"
    assert captured["execution_transport"] == "cli"
    assert captured["sandbox_session_id"] == "sandbox_session:run.alpha:job.alpha"
    assert captured["env"]["OPENAI_API_KEY"] == "test-key"
    assert captured["env"]["PRAXIS_ALLOWED_MCP_TOOLS"] == "praxis_query,praxis_discover"
    assert captured["env"]["PRAXIS_ALLOWED_MCP_TOOLS"] == "praxis_query,praxis_discover"
    assert captured["metadata"]["provider_slug"] == "openai"
    assert captured["metadata"]["execution_bundle"]["access_policy"]["write_scope"] == ["README.md"]
    assert result["status"] == "succeeded"
    assert result["sandbox_provider"] == "docker_local"
    assert result["artifact_refs"] == ["README.md"]
    assert result["workspace_snapshot_ref"] == "workspace_snapshot:test1234"
    assert result["workspace_snapshot_cache_hit"] is True


def test_execute_cli_prefers_bundle_sandbox_profile_contract(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured.update(kwargs)
            return _sandbox_result(
                sandbox_provider="cloudflare_remote",
                network_policy="disabled",
            )

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        execution_backends,
        "augment_cli_command_for_workflow_mcp",
        lambda **kwargs: list(kwargs["command_parts"]),
    )

    result = execution_backends.execute_cli(
        _agent(docker_image="agent-image:stale"),
        "hello from stdin",
        str(tmp_path),
        execution_bundle={
            "run_id": "run.alpha",
            "job_label": "job.alpha",
            "access_policy": {"write_scope": ["README.md"]},
            "sandbox_profile": {
                "sandbox_profile_ref": "sandbox_profile.praxis.default",
                "sandbox_provider": "cloudflare_remote",
                "docker_image": "registry/praxis@sha256:deadbeef",
                "network_policy": "disabled",
                "workspace_materialization": "copy",
                "secret_allowlist": ["OPENAI_API_KEY"],
                "auth_mount_policy": "none",
            },
        },
    )

    assert captured["provider_name"] == "cloudflare_remote"
    assert captured["image"] == "registry/praxis@sha256:deadbeef"
    assert captured["network_policy"] == "disabled"
    assert captured["metadata"]["sandbox_profile_ref"] == "sandbox_profile.praxis.default"
    assert result["sandbox_profile_ref"] == "sandbox_profile.praxis.default"
    assert result["docker_image"] == "registry/praxis@sha256:deadbeef"


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


def test_execute_cli_uses_stable_adhoc_sandbox_identity(monkeypatch, tmp_path) -> None:
    captured_ids: list[str] = []

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured_ids.append(str(kwargs["sandbox_session_id"]))
            return _sandbox_result(
                sandbox_session_id=str(kwargs["sandbox_session_id"]),
                sandbox_group_id=kwargs["sandbox_group_id"],
            )

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        execution_backends,
        "augment_cli_command_for_workflow_mcp",
        lambda **kwargs: list(kwargs["command_parts"]),
    )

    execution_backends.execute_cli(_agent(), "hello", str(tmp_path))
    execution_backends.execute_cli(_agent(), "hello", str(tmp_path))
    execution_backends.execute_cli(_agent(), "hello again", str(tmp_path))

    assert captured_ids[0] == captured_ids[1]
    assert captured_ids[0] != captured_ids[2]
    assert all(value.startswith("sandbox_session:adhoc:") for value in captured_ids)


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

    assert captured["command"] == "codex exec --dangerously-bypass-approvals-and-sandbox --skip-git-repo-check - --json --model gpt-5.4-mini"
    assert captured["stdin_text"] == "hello from stdin"
    assert result["status"] == "succeeded"


def test_execute_cli_parses_cursor_agent_usage_camel_case(monkeypatch, tmp_path) -> None:
    class _FakeRuntime:
        def execute_command(self, **kwargs):
            del kwargs
            return _sandbox_result(
                stdout='{"type":"result","subtype":"success","result":"CURSOR_CLI_OK","usage":{"inputTokens":123,"outputTokens":45,"cacheReadTokens":6,"cacheWriteTokens":7}}',
                artifact_refs=(),
            )

    monkeypatch.setenv("CURSOR_API_KEY", "cursor-test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        execution_backends,
        "augment_cli_command_for_workflow_mcp",
        lambda **kwargs: list(kwargs["command_parts"]),
    )
    monkeypatch.setattr(
        execution_backends,
        "build_command",
        lambda provider_slug, model=None: [
            "cursor-agent",
            "-p",
            "--output-format",
            "json",
            "--mode",
            "ask",
            "-f",
            "--model",
            model or provider_slug,
        ],
    )

    result = execution_backends.execute_cli(
        _agent(wrapper_command=None, provider="cursor_local", model="composer-2"),
        "hello from stdin",
        str(tmp_path),
    )

    assert result["status"] == "succeeded"
    assert result["stdout"] == "CURSOR_CLI_OK"
    assert result["token_input"] == 123
    assert result["token_output"] == 45
    assert result["cache_read_tokens"] == 6
    assert result["cache_creation_tokens"] == 7


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
    monkeypatch.setattr(
        "adapters.provider_registry.get_profile",
        lambda provider_slug: _provider_profile(
            api_protocol_family="anthropic_messages",
            api_endpoint="https://api.anthropic.test/v1/messages",
            api_key_env_vars=("ANTHROPIC_API_KEY",),
        )
        if provider_slug == "anthropic"
        else None,
    )

    result = execution_backends.execute_api(
        _agent(
            provider="anthropic",
            model="claude-haiku-4-5-20251001",
            wrapper_command=None,
            execution_transport="api",
        ),
        "hello from api",
        workdir=str(tmp_path),
        execution_bundle={
            "run_id": "run.alpha",
            "job_label": "job.api",
            "access_policy": {"write_scope": ["api_output.json"]},
        },
    )

    command = str(captured["command"])
    assert f"{sys.executable} -m runtime.api_transport_worker" in command
    assert "--api-protocol anthropic_messages" in command
    assert f"--workdir {tmp_path}" in command
    assert "--model claude-haiku-4-5-20251001" in command
    assert captured["stdin_text"] == "hello from api"
    assert captured["execution_transport"] == "api"
    assert captured["sandbox_session_id"] == "sandbox_session:run.alpha:job.api"
    assert captured["metadata"]["provider_slug"] == "anthropic"
    assert captured["metadata"]["execution_bundle"]["access_policy"]["write_scope"] == ["api_output.json"]
    assert result["status"] == "succeeded"
    assert result["stdout"] == "api output"
    assert result["workspace_snapshot_ref"] == "workspace_snapshot:test1234"
    assert result["workspace_snapshot_cache_hit"] is True


def test_execute_api_uses_stable_adhoc_sandbox_identity(monkeypatch, tmp_path) -> None:
    captured_ids: list[str] = []

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured_ids.append(str(kwargs["sandbox_session_id"]))
            return _sandbox_result(
                stdout="api output",
                execution_transport="api",
                sandbox_session_id=str(kwargs["sandbox_session_id"]),
                sandbox_group_id=kwargs["sandbox_group_id"],
            )

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        "adapters.provider_registry.get_profile",
        lambda provider_slug: _provider_profile(
            api_protocol_family="anthropic_messages",
            api_endpoint="https://api.anthropic.test/v1/messages",
            api_key_env_vars=("ANTHROPIC_API_KEY",),
        )
        if provider_slug == "anthropic"
        else None,
    )

    agent = _agent(
        provider="anthropic",
        model="claude-haiku-4-5-20251001",
        wrapper_command=None,
        execution_transport="api",
    )
    execution_backends.execute_api(agent, "hello from api", workdir=str(tmp_path))
    execution_backends.execute_api(agent, "hello from api", workdir=str(tmp_path))
    execution_backends.execute_api(agent, "hello from a different api call", workdir=str(tmp_path))

    assert captured_ids[0] == captured_ids[1]
    assert captured_ids[0] != captured_ids[2]
    assert all(value.startswith("sandbox_session:adhoc:") for value in captured_ids)


def test_execute_api_prefers_bundle_sandbox_profile_contract(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured.update(kwargs)
            return _sandbox_result(
                stdout="api output",
                execution_transport="api",
                sandbox_provider="cloudflare_remote",
                network_policy="disabled",
            )

    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        "adapters.provider_registry.get_profile",
        lambda provider_slug: _provider_profile(
            api_protocol_family="anthropic_messages",
            api_endpoint="https://api.anthropic.test/v1/messages",
            api_key_env_vars=("ANTHROPIC_API_KEY",),
        )
        if provider_slug == "anthropic"
        else None,
    )

    result = execution_backends.execute_api(
        _agent(
            provider="anthropic",
            model="claude-haiku-4-5-20251001",
            wrapper_command=None,
            execution_transport="api",
            docker_image="agent-image:stale",
        ),
        "hello from api",
        workdir=str(tmp_path),
        execution_bundle={
            "run_id": "run.alpha",
            "job_label": "job.api",
            "access_policy": {"write_scope": ["api_output.json"]},
            "sandbox_profile": {
                "sandbox_profile_ref": "sandbox_profile.praxis.default",
                "sandbox_provider": "cloudflare_remote",
                "docker_image": "registry/praxis@sha256:deadbeef",
                "network_policy": "disabled",
                "workspace_materialization": "copy",
                "secret_allowlist": ["ANTHROPIC_API_KEY"],
                "auth_mount_policy": "none",
            },
        },
    )

    assert captured["provider_name"] == "cloudflare_remote"
    assert captured["image"] == "registry/praxis@sha256:deadbeef"
    assert captured["network_policy"] == "disabled"
    assert captured["metadata"]["sandbox_profile_ref"] == "sandbox_profile.praxis.default"
    assert result["sandbox_profile_ref"] == "sandbox_profile.praxis.default"
    assert result["docker_image"] == "registry/praxis@sha256:deadbeef"
