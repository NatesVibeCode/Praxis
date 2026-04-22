from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest

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


@pytest.fixture(autouse=True)
def _stub_provider_api_key_names(monkeypatch) -> None:
    mapping = {
        "cursor": ("CURSOR_API_KEY",),
        "example": ("EXAMPLE_API_KEY",),
        "google": ("GEMINI_API_KEY", "GOOGLE_API_KEY"),
        "openai": ("OPENAI_API_KEY",),
    }

    def _env_vars(provider_slug: str) -> tuple[str, ...]:
        return mapping.get(provider_slug, ())

    monkeypatch.setattr(execution_backends, "_provider_api_key_names", _env_vars)
    monkeypatch.setattr(execution_backends, "resolve_api_key_env_vars", _env_vars)


def test_load_env_secret_from_keychain_uses_shared_secret_helper(monkeypatch) -> None:
    env: dict[str, str] = {}
    seen_envs: list[dict[str, str] | None] = []

    def _resolve_secret(name, env=None):
        seen_envs.append(env)
        return "keychain-secret" if name == "OPENAI_API_KEY" else None

    monkeypatch.setattr(
        "adapters.keychain.resolve_secret",
        _resolve_secret,
    )

    execution_backends._load_env_secret_from_keychain(env, "OPENAI_API_KEY")

    assert env == {"OPENAI_API_KEY": "keychain-secret"}
    assert seen_envs == [None]


class _FakeLoadBalancer:
    def __init__(self, *, acquired: bool) -> None:
        self.acquired = acquired
        self.providers: list[str] = []
        self.released: list[str] = []

    def slot(self, provider_slug: str, *, cost_weight: float = 1.0, timeout_s: float = 30.0):
        del cost_weight, timeout_s
        self.providers.append(provider_slug)

        class _Slot:
            def __init__(self, parent: "_FakeLoadBalancer", provider: str) -> None:
                self._parent = parent
                self._provider = provider

            def __enter__(self):
                return self._parent.acquired

            def __exit__(self, exc_type, exc, tb):
                if self._parent.acquired:
                    self._parent.released.append(self._provider)
                return False

        return _Slot(self, provider_slug)


def test_execute_cli_routes_through_sandbox_runtime(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured.update(kwargs)
            return _sandbox_result()

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())

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
    assert captured["metadata"]["agent_packet"]["metadata"]["job_label"] == "job.alpha"
    assert captured["metadata"]["provider_slug"] == "openai"
    assert captured["metadata"]["execution_bundle"]["access_policy"]["write_scope"] == ["README.md"]
    assert result["status"] == "succeeded"
    assert result["sandbox_provider"] == "docker_local"
    assert result["artifact_refs"] == ["README.md"]
    assert result["workspace_snapshot_ref"] == "workspace_snapshot:test1234"
    assert result["workspace_snapshot_cache_hit"] is True


def test_execute_cli_uses_provider_concurrency_slot(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}
    load_balancer = _FakeLoadBalancer(acquired=True)

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured.update(kwargs)
            return _sandbox_result()

    monkeypatch.setenv("OPENAI_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "get_load_balancer", lambda: load_balancer)
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())

    result = execution_backends.execute_cli(_agent(), "hello from stdin", str(tmp_path))

    assert load_balancer.providers == ["openai"]
    assert load_balancer.released == ["openai"]
    assert captured["provider_name"] == "docker_local"
    assert result["status"] == "succeeded"


def test_execute_cli_google_uses_packet_mcp_env_without_provider_settings_overlay(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured.update(kwargs)
            return _sandbox_result()

    monkeypatch.setenv("GEMINI_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        execution_backends,
        "build_command",
        lambda provider_slug, model=None: ["gemini", "-p", ".", "-o", "json", "--model", model or provider_slug],
    )
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_URL", "http://mcp.local/mcp")
    monkeypatch.setenv("PRAXIS_WORKFLOW_MCP_SIGNING_SECRET", "test-secret")

    result = execution_backends.execute_cli(
        _agent(wrapper_command=None, provider="google", model="gemini-2.5-flash"),
        "hello from stdin",
        str(tmp_path),
        execution_bundle={
            "run_id": "run.alpha",
            "workflow_id": "workflow.alpha",
            "job_label": "job.alpha",
            "mcp_tool_names": ["praxis_query", "praxis_discover"],
            "decision_pack": {
                "pack_version": 1,
                "authority_domains": ["sandbox_execution"],
                "decision_keys": ["architecture-policy::sandbox-execution::docker-only-authority"],
                "decisions": [
                    {
                        "decision_key": "architecture-policy::sandbox-execution::docker-only-authority",
                        "title": "Workflow sandbox execution is Docker-only",
                        "rationale": "Do not add host-local execution lanes.",
                        "decision_scope_ref": "sandbox_execution",
                    }
                ],
            },
        },
    )

    assert captured["command"] == "gemini -p . -o json --model gemini-2.5-flash"
    overlays = captured["metadata"]["workspace_overlays"]
    overlay_paths = {overlay["relative_path"] for overlay in overlays}
    assert ".gemini/settings.json" not in overlay_paths
    assert "_context/decision_pack.json" in overlay_paths
    assert "_context/decision_summary.md" in overlay_paths
    assert captured["env"]["GEMINI_API_KEY"] == "test-key"
    assert captured["env"]["PRAXIS_WORKFLOW_MCP_URL"] == "http://mcp.local/mcp"
    assert captured["env"]["PRAXIS_WORKFLOW_MCP_TOKEN"]
    assert captured["env"]["PRAXIS_ALLOWED_MCP_TOOLS"] == "praxis_query,praxis_discover"
    assert captured["metadata"]["agent_packet"]["mcp_tool_names"] == [
        "praxis_query",
        "praxis_discover",
    ]
    assert captured["metadata"]["provider_slug"] == "google"
    assert result["status"] == "succeeded"


def test_execute_cli_google_uses_google_api_key_without_aliasing(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured.update(kwargs)
            return _sandbox_result()

    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "google-test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        execution_backends,
        "build_command",
        lambda provider_slug, model=None: ["gemini", "-p", ".", "-o", "json", "--model", model or provider_slug],
    )

    result = execution_backends.execute_cli(
        _agent(wrapper_command=None, provider="google", model="gemini-2.5-flash"),
        "hello from stdin",
        str(tmp_path),
    )

    assert captured["env"]["GOOGLE_API_KEY"] == "google-test-key"
    assert "GEMINI_API_KEY" not in captured["env"]
    assert result["status"] == "succeeded"


def test_execute_integration_job_preserves_skipped_result_as_failed_job(monkeypatch) -> None:
    monkeypatch.setattr(
        "runtime.integrations.execute_integration",
        lambda *_args, **_kwargs: {
            "status": "skipped",
            "data": {"configured_channels": 0},
            "summary": "Notifications are not configured; nothing was sent.",
            "error": None,
        },
    )

    result = execution_backends.execute_integration(
        {
            "id": 123,
            "integration_id": "notifications",
            "integration_action": "send",
            "integration_args": {},
        },
        object(),
    )

    assert result["status"] == "failed"
    assert result["exit_code"] == 1
    assert result["integration_status"] == "skipped"
    assert result["error_code"] == "integration_skipped"
    assert "Integration status: skipped" in result["stdout"]


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

    monkeypatch.setenv("EXAMPLE_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        "registry.provider_execution_registry.get_profile",
        lambda provider_slug: _provider_profile(
            api_protocol_family="example_messages",
            api_endpoint="https://api.example.test/v1/messages",
            api_key_env_vars=("EXAMPLE_API_KEY",),
        )
        if provider_slug == "example"
        else None,
    )

    result = execution_backends.execute_api(
        _agent(
            provider="example",
            model="example-model",
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
    assert "--api-protocol example_messages" in command
    assert f"--workdir {tmp_path}" in command
    assert "--model example-model" in command
    assert captured["stdin_text"] == "hello from api"
    assert captured["execution_transport"] == "api"
    assert captured["sandbox_session_id"] == "sandbox_session:run.alpha:job.api"
    assert captured["metadata"]["provider_slug"] == "example"
    assert captured["metadata"]["execution_bundle"]["access_policy"]["write_scope"] == ["api_output.json"]
    assert result["status"] == "succeeded"
    assert result["stdout"] == "api output"
    assert result["workspace_snapshot_ref"] == "workspace_snapshot:test1234"
    assert result["workspace_snapshot_cache_hit"] is True


def test_execute_api_requires_registry_declared_auth_env(monkeypatch, tmp_path) -> None:
    class _FakeRuntime:
        def execute_command(self, **kwargs):
            raise AssertionError(f"API sandbox should not run without declared auth: {kwargs}")

    monkeypatch.setenv("BLANK_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        "registry.provider_execution_registry.get_profile",
        lambda provider_slug: _provider_profile(
            api_protocol_family="blank_messages",
            api_endpoint="https://api.blank.test/v1/messages",
            api_key_env_vars=(),
        )
        if provider_slug == "blank"
        else None,
    )

    result = execution_backends.execute_api(
        _agent(
            provider="blank",
            model="blank-model",
            wrapper_command=None,
            execution_transport="api",
        ),
        "hello from api",
        workdir=str(tmp_path),
    )

    assert result["status"] == "failed"
    assert result["error_code"] == "transport_auth_config_missing"
    assert "api_key_env_vars" in result["stderr"]


def test_build_execution_env_uses_keychain_before_process_env_or_dotenv(
    monkeypatch,
    tmp_path,
) -> None:
    (tmp_path / ".env").write_text(
        "EXAMPLE_API_KEY=dotenv-stale\nUNDECLARED_SECRET=should-not-export\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("EXAMPLE_API_KEY", "process-stale")
    monkeypatch.delenv("UNDECLARED_SECRET", raising=False)
    seen_envs: list[dict[str, str] | None] = []

    def _resolve_secret(name, env=None):
        seen_envs.append(env)
        return "keychain-key" if name == "EXAMPLE_API_KEY" else None

    monkeypatch.setattr("adapters.keychain.resolve_secret", _resolve_secret)
    monkeypatch.setattr(
        "registry.provider_execution_registry.get_profile",
        lambda provider_slug: _provider_profile()
        if provider_slug == "example"
        else None,
    )

    env = execution_backends._build_execution_env(
        _agent(provider="example"),
        workdir=str(tmp_path),
        execution_bundle=None,
    )

    assert env["EXAMPLE_API_KEY"] == "keychain-key"
    assert "UNDECLARED_SECRET" not in env
    assert seen_envs == [None]


def test_build_execution_env_does_not_read_workdir_dotenv_as_secret_authority(
    monkeypatch,
    tmp_path,
) -> None:
    (tmp_path / ".env").write_text("EXAMPLE_API_KEY=dotenv-hidden\n", encoding="utf-8")
    monkeypatch.delenv("EXAMPLE_API_KEY", raising=False)
    monkeypatch.setattr("adapters.keychain.resolve_secret", lambda name, env=None: None)
    monkeypatch.setattr(
        "registry.provider_execution_registry.get_profile",
        lambda provider_slug: _provider_profile()
        if provider_slug == "example"
        else None,
    )

    env = execution_backends._build_execution_env(
        _agent(provider="example"),
        workdir=str(tmp_path),
        execution_bundle=None,
    )

    assert "EXAMPLE_API_KEY" not in env


def test_build_execution_env_uses_configurable_sandbox_path_prefix(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("PRAXIS_SANDBOX_PATH_PREFIX", "/custom/bin:/custom/sbin")
    monkeypatch.setenv("PATH", "/usr/bin:/bin")
    monkeypatch.setattr("adapters.keychain.resolve_secret", lambda name, env=None: None)
    monkeypatch.setattr(
        "registry.provider_execution_registry.get_profile",
        lambda provider_slug: _provider_profile()
        if provider_slug == "example"
        else None,
    )

    env = execution_backends._build_execution_env(
        _agent(provider="example"),
        workdir=str(tmp_path),
        execution_bundle=None,
    )

    assert env["PATH"] == "/custom/bin:/custom/sbin:/usr/bin:/bin"


def test_execute_api_uses_provider_concurrency_slot(monkeypatch, tmp_path) -> None:
    captured: dict[str, object] = {}
    load_balancer = _FakeLoadBalancer(acquired=True)

    class _FakeRuntime:
        def execute_command(self, **kwargs):
            captured.update(kwargs)
            return _sandbox_result(
                stdout="api output",
                execution_transport="api",
            )

    monkeypatch.setenv("EXAMPLE_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "get_load_balancer", lambda: load_balancer)
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        "registry.provider_execution_registry.get_profile",
        lambda provider_slug: _provider_profile(
            api_protocol_family="example_messages",
            api_endpoint="https://api.example.test/v1/messages",
            api_key_env_vars=("EXAMPLE_API_KEY",),
        )
        if provider_slug == "example"
        else None,
    )

    result = execution_backends.execute_api(
        _agent(
            provider="example",
            model="example-model",
            wrapper_command=None,
            execution_transport="api",
        ),
        "hello from api",
        workdir=str(tmp_path),
    )

    assert load_balancer.providers == ["example"]
    assert load_balancer.released == ["example"]
    assert captured["execution_transport"] == "api"
    assert result["status"] == "succeeded"


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

    monkeypatch.setenv("EXAMPLE_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        "registry.provider_execution_registry.get_profile",
        lambda provider_slug: _provider_profile(
            api_protocol_family="example_messages",
            api_endpoint="https://api.example.test/v1/messages",
            api_key_env_vars=("EXAMPLE_API_KEY",),
        )
        if provider_slug == "example"
        else None,
    )

    agent = _agent(
        provider="example",
        model="example-model",
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

    monkeypatch.setenv("EXAMPLE_API_KEY", "test-key")
    monkeypatch.setattr(execution_backends, "SandboxRuntime", lambda: _FakeRuntime())
    monkeypatch.setattr(
        "registry.provider_execution_registry.get_profile",
        lambda provider_slug: _provider_profile(
            api_protocol_family="example_messages",
            api_endpoint="https://api.example.test/v1/messages",
            api_key_env_vars=("EXAMPLE_API_KEY",),
        )
        if provider_slug == "example"
        else None,
    )

    result = execution_backends.execute_api(
        _agent(
            provider="example",
            model="example-model",
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
                "secret_allowlist": ["EXAMPLE_API_KEY"],
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
