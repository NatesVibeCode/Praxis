from __future__ import annotations

from pathlib import Path

import runtime.sandbox_runtime as sandbox_runtime
from runtime.workflow.agent_packet import AgentPacket, build_sandbox_spec
from runtime.workspace_paths import container_auth_seed_dir, container_home

AUTH_HOME = Path("/tmp/praxis-auth-home")
OPENAI_AUTH_SEED_PATH = str(container_auth_seed_dir() / "openai-auth.json")
CONTAINER_HOME = str(container_home())


def test_openai_sandbox_spec_bootstraps_auth_as_root_then_drops_privileges(monkeypatch) -> None:
    monkeypatch.setattr(
        sandbox_runtime,
        "_load_cli_auth_catalog",
        lambda: sandbox_runtime._CliAuthCatalog(
            mount_specs=(
                sandbox_runtime._CliAuthMountSpec(
                    provider_slug="openai",
                    host_relative_path=".codex/auth.json",
                    container_path=f"{CONTAINER_HOME}/.codex/auth.json",
                    container_seed_path=OPENAI_AUTH_SEED_PATH,
                ),
            ),
            home_tmpfs_dirs=(".codex",),
        ),
    )
    monkeypatch.setattr(
        "runtime.sandbox_runtime.os.path.isfile",
        lambda path: path.endswith(".codex/auth.json"),
    )
    monkeypatch.setenv("PRAXIS_CLI_AUTH_HOME", str(AUTH_HOME))

    spec = build_sandbox_spec(
        agent_packet=AgentPacket(
            user_prompt="prompt",
            metadata={"provider_slug": "openai"},
        ),
        cli_command="codex exec - --json",
        container_name="praxis-test",
        sandbox_session_id="sandbox_session:run.alpha:job.alpha",
        workspace_source_root="/tmp/workspace",
        docker_image="praxis-codex:latest",
        docker_memory="500m",
        docker_cpus="2",
        timeout_seconds=10,
    )

    args = list(spec.docker_run_args)
    assert args[args.index("--user") + 1] == "0:0"
    assert f"{AUTH_HOME}/.codex/auth.json:{OPENAI_AUTH_SEED_PATH}:ro" in args
    assert args[-4:-1] == ["praxis-codex:latest", "bash", "-lc"]
    assert f"cp {OPENAI_AUTH_SEED_PATH} /home/praxis-agent/.codex/auth.json" in args[-1]
    assert "setpriv --reuid=1100 --regid=1100" in args[-1]


def test_anthropic_sandbox_spec_resolves_claude_oauth_token_from_authority(monkeypatch) -> None:
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "host-shell-leak-not-allowed")
    monkeypatch.setenv("ANTHROPIC_AUTH_TOKEN", "fallback-token")
    monkeypatch.setattr("runtime.sandbox_runtime.os.path.isfile", lambda path: False)
    monkeypatch.setattr(
        "adapters.docker_runner._cli_auth_env_forward",
        lambda provider_slug: (
            {"CLAUDE_CODE_OAUTH_TOKEN": "from-db-anthropic-token"}
            if provider_slug == "anthropic"
            else {}
        ),
    )

    spec = build_sandbox_spec(
        agent_packet=AgentPacket(
            user_prompt="prompt",
            env={"PATH": "/usr/bin:/bin"},
            metadata={"provider_slug": "anthropic"},
        ),
        cli_command="claude --print",
        container_name="praxis-test",
        sandbox_session_id="sandbox_session:run.alpha:job.claude",
        workspace_source_root="/tmp/workspace",
        docker_image="praxis-claude:latest",
        docker_memory="500m",
        docker_cpus="2",
        timeout_seconds=10,
    )

    args = list(spec.docker_run_args)
    env_values = [
        value
        for index, value in enumerate(args)
        if index > 0 and args[index - 1] == "-e"
    ]
    assert "CLAUDE_CODE_OAUTH_TOKEN=from-db-anthropic-token" in env_values
    assert "CLAUDE_CODE_OAUTH_TOKEN=host-shell-leak-not-allowed" not in env_values
    assert "ANTHROPIC_AUTH_TOKEN=fallback-token" not in env_values
