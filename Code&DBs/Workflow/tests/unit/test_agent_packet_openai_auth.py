from __future__ import annotations

from pathlib import Path

from runtime.sandbox_runtime import _OPENAI_AUTH_SEED_PATH
from runtime.workflow.agent_packet import AgentPacket, build_sandbox_spec

AUTH_HOME = Path("/tmp/praxis-auth-home")


def test_openai_sandbox_spec_bootstraps_auth_as_root_then_drops_privileges(monkeypatch) -> None:
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
        docker_image="praxis-worker:latest",
        docker_memory="4g",
        docker_cpus="2",
        timeout_seconds=10,
    )

    args = list(spec.docker_run_args)
    assert args[args.index("--user") + 1] == "0:0"
    assert f"{AUTH_HOME}/.codex/auth.json:{_OPENAI_AUTH_SEED_PATH}:ro" in args
    assert args[-4:-1] == ["praxis-worker:latest", "bash", "-lc"]
    assert f"cp {_OPENAI_AUTH_SEED_PATH} /home/praxis-agent/.codex/auth.json" in args[-1]
    assert "setpriv --reuid=1100 --regid=1100" in args[-1]
