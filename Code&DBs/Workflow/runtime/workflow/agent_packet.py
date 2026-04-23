"""Clean-seam packets between the three sandbox-execution concerns.

Historically the workflow runtime tangled three distinct jobs across
``execution_backends.py``, ``sandbox_runtime.py``, ``execution_bundle.py``,
``_context_building.py``, ``prompt_renderer.py``, ``docker_runner.py``, and
``mcp_bridge.py``:

1. **Agent interaction**  — what the agent sees (prompt + env + tool manifest).
2. **Sandbox lifecycle**  — how the CLI binary is spawned (docker args + stdin).
3. **Sandbox execution**  — the actual subprocess call (the only side-effect).

These three concerns now hand off through the three dataclasses in this
module. Each packet has a single purpose, a single owner, and is produced by
a pure function that can be previewed without Docker, without an LLM call,
and without touching the DB beyond a read-only shard fetch:

    spec + bundle + workdir
       │
       ▼
    build_agent_packet()   → AgentPacket   (preview-safe, no side effects)
       │
       ▼
    build_sandbox_spec()   → SandboxSpec   (preview-safe, no Docker)
       │
       ▼
    SandboxExecutor.run()  → ExecutionResult   (the only side-effect point)

The payoff: a change to "what the agent sees" touches only
``build_agent_packet``; a change to "how the CLI is spawned" touches only
``build_sandbox_spec``; a change to "how Docker is invoked" touches only
``SandboxExecutor.run``. Tests can target any layer in isolation, and a
``praxis workflow agent-packet <spec>`` CLI can render the agent-facing
packet without spending a token.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from runtime.workspace_paths import container_home

from runtime.workspace_paths import container_workspace_root


@dataclass(frozen=True, slots=True)
class AgentPacket:
    """Everything the agent sees when it wakes up inside a sandbox.

    Produced by ``build_agent_packet(spec, bundle, workdir)`` as a pure
    function — no Docker, no DB writes, no LLM call. The packet is
    self-describing and reproducible: two runs of the same inputs yield
    byte-identical packets.

    The packet owns only **agent-facing** state. Docker args, network
    policy, and workspace materialization live in ``SandboxSpec`` so that
    "what the agent reads" and "how the sandbox is shaped" can evolve
    independently.
    """

    # The text delivered to the CLI via stdin (or argv, depending on the
    # CLI's prompt mechanism). This is the authoritative rendered prompt —
    # the same string preview and execution both see, byte-identical
    # (BUG-D3CD86B8).
    user_prompt: str

    # The optional system message, rendered from spec.system_prompt plus
    # any authority-injected prelude. Empty string when the CLI does not
    # consume a system message distinct from the user prompt.
    system_prompt: str = ""

    # Environment variables injected into the sandbox container. Keys here
    # are authoritative — `build_sandbox_spec` only passes these through.
    # Includes PRAXIS_EXECUTION_BUNDLE, PRAXIS_WORKFLOW_MCP_URL,
    # PRAXIS_WORKFLOW_MCP_TOKEN, PRAXIS_ALLOWED_MCP_TOOLS, etc.
    env: Mapping[str, str] = field(default_factory=dict)

    # Admitted MCP tool names for this job. Injected into the bundle's
    # rendered prompt AND into the sandbox's Authorization scope — the
    # signed token carries this same set for server-side enforcement.
    mcp_tool_names: tuple[str, ...] = ()

    # Free-form metadata that downstream stages need but that aren't
    # agent-visible: agent_slug, provider_slug, job_label, run_id,
    # workflow_id. Kept here so that `build_sandbox_spec` can pick what
    # it needs without re-parsing the spec.
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SandboxSpec:
    """Everything the sandbox needs to run one CLI invocation.

    Produced by ``build_sandbox_spec(agent_packet, agent_slug, shard)`` as
    a pure function. No Docker call yet — the ``docker_run_args`` list is
    just the argv we'll hand to ``subprocess.run``.

    Keeping this as data (not behavior) means the full docker invocation
    is introspectable via ``praxis workflow sandbox-spec <spec>`` before
    we ever spawn a container. A diff between "expected args" and "actual
    args" is a straight list comparison instead of grepping Python.
    """

    # Full docker-run argv, ready for subprocess. First element is "docker".
    # Includes --rm, --name, --user, --memory, --cpus, volume mounts,
    # tmpfs, env flags, image name, and the final bash -lc "<cli_command>".
    docker_run_args: tuple[str, ...]

    # Text piped to the CLI over stdin. Matches the ``user_prompt`` from
    # the agent packet when the CLI takes stdin; empty when the CLI takes
    # the prompt via argv.
    stdin_text: str

    # Hard timeout in seconds. The executor enforces this with a SIGKILL
    # at the Docker-run level, not just at the CLI level — the whole
    # container dies on timeout.
    timeout_seconds: int

    # Sandbox session id — used for provenance, log correlation, and the
    # MCP bridge's bearer-token enforcement. Always of the shape
    # ``sandbox_session:{run_id}:{job_label}``.
    sandbox_session_id: str

    # Short-lived signed token for the MCP bridge. Present when the job
    # has admitted MCP tools; empty when no MCP surface is admitted.
    mcp_token: str = ""

    # Workspace source path on the host (before materialization into the
    # sandbox's /workspace mount). Used by the executor to copy files
    # into the ephemeral sandbox filesystem.
    workspace_source_root: str = ""


@dataclass(frozen=True, slots=True)
class ExecutionResult:
    """Normalized outcome of running one ``SandboxSpec``.

    Produced by ``SandboxExecutor.run(sandbox_spec)`` — the single place
    in the runtime where Docker subprocess calls actually happen.

    Everything downstream (receipt writing, submission reconciliation,
    verification gate evaluation) reads from this dataclass and never
    touches Docker.
    """

    sandbox_session_id: str
    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool
    started_at: str
    finished_at: str
    duration_ms: int
    artifact_refs: tuple[str, ...] = ()
    # Structured drift records from sandbox_runtime's submission-contract
    # capture (ephemeral scratch writes outside write_scope, etc.). Empty
    # when no drift observed or when the job is legacy-contract.
    artifact_scope_drift: tuple[Mapping[str, Any], ...] = ()
    # Peak container memory in bytes — used by the 500MB cap justification
    # and by future capacity tuning.
    container_mem_peak_bytes: int = 0
    # Peak container CPU percentage — same rationale.
    container_cpu_peak_percent: float = 0.0


def build_agent_packet(
    *,
    agent_config: Any,
    prompt: str,
    workdir: str,
    execution_bundle: Mapping[str, Any] | None,
    platform_context: str = "",
    execution_context_shard_text: str = "",
) -> AgentPacket:
    """Assemble the agent-facing packet from spec + bundle + workspace inputs.

    Pure function: no Docker, no LLM call, no DB writes. The same inputs
    always produce the same packet, so ``praxis workflow agent-packet`` can
    render it offline and a snapshot test can lock the shape.

    The packet re-uses the existing prompt-assembly and env-construction
    helpers — this function is a **seam**, not a rewrite. Behavior today
    is identical to what ``execute_cli`` does inline; the seam lets future
    changes to "what the agent sees" live in one place.
    """

    # Late imports avoid circular import with execution_backends which imports
    # from us once the runtime modules finish loading.
    from runtime.workflow._context_building import assemble_full_prompt
    from runtime.workflow.execution_backends import _build_execution_env
    from runtime.workflow.execution_bundle import render_execution_bundle

    bundle_dict = dict(execution_bundle) if isinstance(execution_bundle, Mapping) else None

    env = _build_execution_env(
        agent_config,
        workdir=workdir,
        execution_bundle=bundle_dict,
    )

    execution_bundle_text = render_execution_bundle(bundle_dict)

    user_prompt = assemble_full_prompt(
        prompt=prompt,
        platform_context=platform_context,
        execution_context_shard_text=execution_context_shard_text,
        execution_bundle_text=execution_bundle_text,
    )

    mcp_tool_names_raw = (bundle_dict or {}).get("mcp_tool_names") if bundle_dict else None
    if isinstance(mcp_tool_names_raw, Sequence):
        mcp_tool_names = tuple(str(name).strip() for name in mcp_tool_names_raw if str(name).strip())
    else:
        mcp_tool_names = ()

    metadata: dict[str, Any] = {
        "agent_slug": f"{getattr(agent_config, 'provider', '') or ''}/{getattr(agent_config, 'model', '') or ''}".strip("/"),
        "provider_slug": str(getattr(agent_config, "provider", "") or "").strip().lower() or None,
        "model_slug": getattr(agent_config, "model", None),
        "job_label": str((bundle_dict or {}).get("job_label") or "").strip() if bundle_dict else "",
        "run_id": str((bundle_dict or {}).get("run_id") or "").strip() if bundle_dict else "",
        "workflow_id": str((bundle_dict or {}).get("workflow_id") or "").strip() if bundle_dict else "",
    }

    return AgentPacket(
        user_prompt=user_prompt,
        system_prompt=str(getattr(agent_config, "system_prompt", "") or "") or "",
        env=dict(env),
        mcp_tool_names=mcp_tool_names,
        metadata=metadata,
    )


def build_sandbox_spec(
    *,
    agent_packet: AgentPacket,
    cli_command: str,
    container_name: str,
    sandbox_session_id: str,
    workspace_source_root: str,
    container_workspace: str | None = None,
    docker_image: str,
    docker_memory: str,
    docker_cpus: str,
    network_policy: str = "provider_only",
    auth_mount_policy: str = "provider_scoped",
    timeout_seconds: int,
    stdin_text: str = "",
) -> SandboxSpec:
    """Assemble the docker-run argv and ancillary state for one CLI invocation.

    Pure function: constructs the command but does not invoke it. The
    executor calls ``subprocess.run(spec.docker_run_args, ...)`` as the
    only side effect. Re-uses the in-place helpers from ``sandbox_runtime``
    so today's behavior is preserved byte-for-byte; this is a seam, not a
    behavior change.

    ``auth_mount_policy`` accepts the same tokens as the existing
    ``SandboxSession.metadata.auth_mount_policy``: ``"none"``,
    ``"provider_scoped"`` (mount only this agent's credential file), or
    ``"all"`` (mount every known CLI's credentials).
    """
    from adapters.docker_runner import _cli_auth_env_forward
    from runtime.sandbox_runtime import (
        _cli_auth_bootstrap_command,
        _cli_auth_volume_flags,
        _cli_home_tmpfs_flags,
        _cli_requires_root_auth_bootstrap,
    )

    provider_slug = agent_packet.metadata.get("provider_slug")
    container_workspace = container_workspace or str(container_workspace_root())
    normalized_provider = str(provider_slug or "").strip().lower() or None

    normalized_mount_policy = (auth_mount_policy or "provider_scoped").strip().lower()
    root_auth_bootstrap = _cli_requires_root_auth_bootstrap(
        provider_slug=normalized_provider,
        auth_mount_policy=normalized_mount_policy,
        requested_user="1100:1100",
    )
    args: list[str] = [
        "docker",
        "run",
        "--rm",
        "-i",
        "--name", container_name,
        "--user", "0:0" if root_auth_bootstrap else "1100:1100",
        "--memory", docker_memory,
        "--cpus", docker_cpus,
        "--workdir", container_workspace,
        "-v", f"{workspace_source_root}:{container_workspace}",
    ]

    if normalized_mount_policy != "none":
        effective_provider = (
            normalized_provider if normalized_mount_policy == "provider_scoped" else None
        )
        args.extend(_cli_home_tmpfs_flags())
        args.extend(_cli_auth_volume_flags(provider_slug=effective_provider))

    # HOME forced to the uid-1100 user home after the packet env so CLIs
    # resolve their config files from the mounted auth targets and a
    # parent-inherited HOME=/root does not win.
    env_items: dict[str, str] = {**dict(agent_packet.env), "HOME": str(container_home())}
    for key, value in sorted(env_items.items()):
        args.extend(["-e", f"{key}={value}"])

    for key, value in sorted(_cli_auth_env_forward(normalized_provider).items()):
        if key in env_items:
            continue
        args.extend(["-e", f"{key}={value}"])

    if (network_policy or "").strip().lower() == "disabled":
        args.append("--network=none")

    docker_command = (
        _cli_auth_bootstrap_command(cli_command, provider_slug=normalized_provider)
        if root_auth_bootstrap
        else cli_command
    )
    args.extend([docker_image, "bash", "-lc", docker_command])

    return SandboxSpec(
        docker_run_args=tuple(args),
        stdin_text=stdin_text,
        timeout_seconds=timeout_seconds,
        sandbox_session_id=sandbox_session_id,
        mcp_token=str(agent_packet.env.get("PRAXIS_WORKFLOW_MCP_TOKEN", "")),
        workspace_source_root=workspace_source_root,
    )


def execute_sandbox_spec(spec: SandboxSpec) -> ExecutionResult:
    """Run one ``SandboxSpec`` and normalize the outcome.

    This is the **only** place in the runtime where a Docker ``subprocess.run``
    happens for an agent invocation. Every feature that needs "run a
    sandbox" goes through this function. Workspace hydration, MCP session
    token minting, receipt writing, and scheduling all live outside this
    boundary — their job is to build a correct ``SandboxSpec``; this
    function's job is to execute it truthfully.

    Timeouts are enforced at the ``subprocess.run`` level. A timed-out
    container is terminated with ``docker kill`` before this function
    returns; the name is extracted from ``--name`` in the argv so no
    separate lifecycle tracking is needed.

    Intentionally minimal: no ``docker stats`` polling, no artifact
    collection, no dehydration. Those concerns belong to higher layers
    that compose this boundary (e.g. the full sandbox_runtime flow when
    a legacy-contract job needs host copy-back). Use this executor for
    plumbing-level tests and smoke checks where "did the container run
    and return" is the whole question.
    """
    import subprocess as _sp
    import time as _time
    from datetime import datetime, timezone

    started = datetime.now(timezone.utc)
    t_start = _time.monotonic()

    try:
        completed = _sp.run(
            list(spec.docker_run_args),
            input=spec.stdin_text,
            capture_output=True,
            text=True,
            timeout=spec.timeout_seconds,
        )
        timed_out = False
        stdout = completed.stdout or ""
        stderr = completed.stderr or ""
        exit_code = completed.returncode
    except _sp.TimeoutExpired as exc:
        timed_out = True
        stdout = (exc.stdout or b"").decode("utf-8", "replace") if isinstance(exc.stdout, bytes) else (exc.stdout or "")
        stderr = (exc.stderr or b"").decode("utf-8", "replace") if isinstance(exc.stderr, bytes) else (exc.stderr or "")
        exit_code = 124
        # Best-effort kill by container name. argv contains `--name <name>` so
        # we parse it out rather than tracking the Popen pid (which is the
        # docker-cli wrapper, not the container).
        try:
            idx = list(spec.docker_run_args).index("--name")
            container_name = spec.docker_run_args[idx + 1]
            _sp.run(["docker", "kill", container_name], capture_output=True, timeout=10)
        except Exception:
            pass

    finished = datetime.now(timezone.utc)
    duration_ms = int((_time.monotonic() - t_start) * 1000)

    return ExecutionResult(
        sandbox_session_id=spec.sandbox_session_id,
        exit_code=exit_code,
        stdout=stdout,
        stderr=stderr,
        timed_out=timed_out,
        started_at=started.isoformat(),
        finished_at=finished.isoformat(),
        duration_ms=duration_ms,
    )
