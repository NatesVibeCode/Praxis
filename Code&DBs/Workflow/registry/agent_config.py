"""Agent configuration registry.

Loads agent definitions from a JSON config file and provides immutable
lookup indexes by slug, provider, capability tier, and pipeline stage.
Failover chains are resolved eagerly at load time with cycle detection.
"""

from __future__ import annotations

import enum
import json
import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

_AUTO_ROUTE_TIERS = frozenset({"high", "medium", "low"})
_AUTO_LATENCY_CLASSES = frozenset({"reasoning", "instant"})
_SEMANTIC_AUTO_ROUTE_ALIASES = {
    "draft": "chat",
    "classify": "analysis",
}
class AgentConfigError(RuntimeError):
    """Raised when agent config loading or resolution fails."""

    def __init__(self, reason_code: str, details: str) -> None:
        super().__init__(details)
        self.reason_code = reason_code
        self.details = details


class ExecutionTransport(enum.Enum):
    """How the agent exchanges prompts and completions."""

    cli = "cli"
    api = "api"
    mcp = "mcp"


class ExecutionBackend(enum.Enum):
    """Compatibility alias for older callers that still speak in backends."""

    cli = ExecutionTransport.cli.value
    api = ExecutionTransport.api.value
    mcp = ExecutionTransport.mcp.value


class SandboxProvider(enum.Enum):
    """Which sandbox substrate isolates execution."""

    docker_local = "docker_local"
    cloudflare_remote = "cloudflare_remote"


_LEGACY_AGENT_SLUG_ALIASES = {
    "openai/codex-5.3-spark": "openai/gpt-5.3-codex-spark",
}


@dataclass(frozen=True)
class SandboxPolicy:
    """Explicit sandbox defaults for one agent definition."""

    share_mode: str = "exclusive"
    network_policy: str = "provider_only"
    workspace_materialization: str = "copy"
    secret_allowlist: tuple[str, ...] = ()
    timeout_profile: str = "default"


@dataclass(frozen=True)
class AgentConfig:
    """Immutable agent definition."""

    slug: str
    provider: str
    model: str
    wrapper_command: str | None
    docker_image: str | None
    context_window: int
    max_output_tokens: int
    cost_per_input_mtok: float
    cost_per_output_mtok: float
    timeout_seconds: int
    idle_timeout_seconds: int
    failover_targets: tuple[str, ...]
    allowed_stages: tuple[str, ...]
    capability_tier: str
    output_format: str
    execution_backend: ExecutionBackend | None = None
    execution_transport: ExecutionTransport = ExecutionTransport.cli
    sandbox_provider: SandboxProvider = SandboxProvider.docker_local
    sandbox_policy: SandboxPolicy = field(default_factory=SandboxPolicy)
    reasoning_control: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.execution_backend is not None:
            object.__setattr__(
                self,
                "execution_transport",
                ExecutionTransport(self.execution_backend.value),
            )
            return
        object.__setattr__(
            self,
            "execution_backend",
            ExecutionBackend(self.execution_transport.value),
        )


def _default_sandbox_provider(*, transport: ExecutionTransport) -> SandboxProvider:
    if transport is ExecutionTransport.mcp:
        return SandboxProvider.cloudflare_remote
    return SandboxProvider.docker_local


def _normalized_secret_allowlist(raw: Any) -> tuple[str, ...]:
    if isinstance(raw, str):
        values = [raw]
    elif isinstance(raw, Sequence):
        values = [str(value) for value in raw]
    else:
        values = []
    return tuple(value.strip() for value in values if value and str(value).strip())


def _parse_sandbox_policy(raw: Any) -> SandboxPolicy:
    policy = raw if isinstance(raw, Mapping) else {}
    return SandboxPolicy(
        share_mode=str(policy.get("share_mode") or "exclusive").strip() or "exclusive",
        network_policy=str(policy.get("network_policy") or "provider_only").strip() or "provider_only",
        workspace_materialization=(
            str(policy.get("workspace_materialization") or "copy").strip() or "copy"
        ),
        secret_allowlist=_normalized_secret_allowlist(policy.get("secret_allowlist")),
        timeout_profile=str(policy.get("timeout_profile") or "default").strip() or "default",
    )


def _normalize_execution_contract(
    raw: Mapping[str, Any],
    *,
    warn_on_legacy_backend: bool = False,
) -> tuple[ExecutionTransport, SandboxProvider]:
    raw_transport = raw.get("execution_transport")
    raw_backend = raw.get("execution_backend")
    raw_provider = raw.get("sandbox_provider")

    transport = (
        ExecutionTransport(str(raw_transport))
        if raw_transport is not None
        else (
            ExecutionTransport(str(raw_backend))
            if raw_backend is not None
            else ExecutionTransport.cli
        )
    )
    if raw_provider is not None and str(raw_provider).strip() == "host_local":
        raise AgentConfigError(
            "unsupported_sandbox_provider",
            "host_local sandbox execution is disabled; use docker_local or cloudflare_remote",
        )
    sandbox_provider = (
        SandboxProvider(str(raw_provider))
        if raw_provider is not None
        else _default_sandbox_provider(transport=transport)
    )
    return transport, sandbox_provider


def _parse_agent(raw: Mapping[str, Any]) -> AgentConfig:
    """Parse a single agent entry from JSON."""
    execution_transport, sandbox_provider = _normalize_execution_contract(
        raw,
    )
    return AgentConfig(
        slug=raw["slug"],
        provider=raw["provider"],
        model=raw["model"],
        wrapper_command=raw.get("wrapper_command"),
        docker_image=raw.get("docker_image"),
        context_window=raw["context_window"],
        max_output_tokens=raw["max_output_tokens"],
        cost_per_input_mtok=raw["cost_per_input_mtok"],
        cost_per_output_mtok=raw["cost_per_output_mtok"],
        timeout_seconds=raw["timeout_seconds"],
        idle_timeout_seconds=raw["idle_timeout_seconds"],
        failover_targets=tuple(raw.get("failover_targets", ())),
        allowed_stages=tuple(raw.get("allowed_stages", ())),
        capability_tier=raw["capability_tier"],
        output_format=raw.get("output_format", "text"),
        execution_transport=execution_transport,
        sandbox_provider=sandbox_provider,
        sandbox_policy=_parse_sandbox_policy(raw.get("sandbox_policy")),
    )


def _agent_with_reasoning_control(
    agent: AgentConfig,
    *,
    reasoning_control: Mapping[str, Any],
) -> AgentConfig:
    return replace(agent, reasoning_control=dict(reasoning_control))


def _normalize_auto_route_key(value: Any) -> str:
    key = str(value or "").strip().lower()
    if key.startswith("auto/"):
        key = key.split("/", 1)[1]
    return key


def _auto_route_slug(value: Any) -> str:
    key = _normalize_auto_route_key(value)
    return f"auto/{key}" if key else ""


def _transport_from_candidate_row(
    row: Mapping[str, Any],
    *,
    has_cli_template: bool,
) -> ExecutionTransport:
    raw_transport = str(row.get("transport_type") or "").strip().upper()
    if raw_transport == "CLI":
        return ExecutionTransport.cli
    if raw_transport == "API":
        return ExecutionTransport.api
    if raw_transport:
        raise AgentConfigError(
            "invalid_candidate_transport_type",
            f"provider_model_candidates.transport_type must be CLI or API, got {raw_transport!r}",
        )
    return ExecutionTransport.cli if has_cli_template else ExecutionTransport.api


def _transport_matches_route(
    route_row: Mapping[str, Any],
    concrete: AgentConfig,
) -> bool:
    raw_transport = str(route_row.get("transport_type") or "").strip().upper()
    if not raw_transport:
        return True
    if raw_transport == "CLI":
        return concrete.execution_transport is ExecutionTransport.cli
    if raw_transport == "API":
        return concrete.execution_transport is ExecutionTransport.api
    return False


def _agent_config_from_candidate_row(row: Mapping[str, Any]) -> AgentConfig:
    from registry.model_context_limits import context_window_for_model

    provider = str(row["provider_slug"])
    model = str(row["model_slug"])
    slug = f"{provider}/{model}"
    tags = row.get("capability_tags") or []
    if isinstance(tags, str):
        tags = json.loads(tags)

    tier = "mid"
    if isinstance(tags, list):
        if "frontier" in tags:
            tier = "frontier"
        elif "economy" in tags:
            tier = "economy"

    try:
        context_window = context_window_for_model(provider, model)
    except RuntimeError:
        defaults = row.get("default_parameters") or {}
        if isinstance(defaults, str):
            defaults = json.loads(defaults)
        if not isinstance(defaults, Mapping):
            defaults = {}
        context_window = int(defaults.get("context_window") or 128_000)

    cli_cfg = row.get("cli_config") or {}
    if isinstance(cli_cfg, str):
        cli_cfg = json.loads(cli_cfg)
    if not isinstance(cli_cfg, Mapping):
        cli_cfg = {}

    reasoning_control = row.get("reasoning_control") or {}
    if isinstance(reasoning_control, str):
        reasoning_control = json.loads(reasoning_control)
    if not isinstance(reasoning_control, Mapping):
        reasoning_control = {}

    cmd_template = cli_cfg.get("cmd_template")
    execution_transport = _transport_from_candidate_row(
        row,
        has_cli_template=bool(cmd_template),
    )

    if execution_transport is ExecutionTransport.cli and cmd_template:
        resolved = [str(part).replace("{model}", model) for part in cmd_template]
        import shlex
        wrapper = " ".join(shlex.quote(part) for part in resolved)
    else:
        wrapper = None
    sandbox_provider = _default_sandbox_provider(transport=execution_transport)

    return AgentConfig(
        slug=slug,
        provider=provider,
        model=model,
        wrapper_command=wrapper,
        docker_image=None,
        context_window=context_window,
        max_output_tokens=0,
        cost_per_input_mtok=5.0,
        cost_per_output_mtok=20.0,
        timeout_seconds=900,
        idle_timeout_seconds=180,
        failover_targets=(),
        allowed_stages=("plan", "build", "review", "debate", "test", "debug"),
        capability_tier=tier,
        output_format=str(cli_cfg.get("output_format") or "json"),
        execution_transport=execution_transport,
        sandbox_provider=sandbox_provider,
        sandbox_policy=SandboxPolicy(),
        reasoning_control=dict(reasoning_control),
    )


class AgentRegistry:
    """Immutable agent configuration registry.

    Built via the ``load`` classmethod.  After construction the registry
    is sealed -- mutation attempts raise ``AgentConfigError``.
    """

    def __init__(self, agents: Sequence[AgentConfig]) -> None:
        self._by_slug: dict[str, AgentConfig] = {}
        self._by_provider: dict[str, list[AgentConfig]] = {}
        self._by_tier: dict[str, list[AgentConfig]] = {}
        self._by_stage: dict[str, list[AgentConfig]] = {}
        self._auto_routes: dict[str, AgentConfig] = {}
        self._sealed = False

        for agent in agents:
            if agent.slug in self._by_slug:
                raise AgentConfigError(
                    "duplicate_slug",
                    f"Duplicate agent slug: {agent.slug}",
                )
            self._by_slug[agent.slug] = agent
            # BUG-C5342363: auto/* slugs are synthesized route selectors
            # (task-type / tier / latency aliases) that copy the primary
            # target's provider, capability_tier, and allowed_stages. If we
            # indexed them into _by_provider / _by_tier / _by_stage they'd
            # double-count the primary and pollute list_by_provider("openai"),
            # list_by_tier("high"), and list_by_stage("build") with alias
            # entries that are not independent agents. They stay in _by_slug
            # (so get("auto/high") still resolves) and are tracked in a
            # dedicated _auto_routes view for explicit alias enumeration.
            if agent.slug.startswith("auto/"):
                self._auto_routes[agent.slug] = agent
                continue
            self._by_provider.setdefault(agent.provider, []).append(agent)
            self._by_tier.setdefault(agent.capability_tier, []).append(agent)
            for stage in agent.allowed_stages:
                self._by_stage.setdefault(stage, []).append(agent)

        self._sealed = True

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    @classmethod
    def load(cls, config_path: str | Path) -> AgentRegistry:
        """Load agent definitions from a JSON file (legacy)."""
        path = Path(config_path)
        if not path.exists():
            raise AgentConfigError(
                "file_not_found",
                f"Agent config not found: {path}",
            )
        with path.open() as fh:
            data = json.load(fh)

        raw_agents: list[Mapping[str, Any]] = data if isinstance(data, list) else data.get("agents", [])
        agents = [_parse_agent(r) for r in raw_agents]
        return cls(agents)

    @classmethod
    def load_from_postgres(cls, conn) -> AgentRegistry:
        """Load agent definitions from Postgres provider_model_candidates table.

        Reads cli_config.cmd_template from the DB rather than hardcoding
        wrapper commands.  Template placeholders:
            {model} → model_slug
        Prompt defaults to stdin, but cmd_template may also carry a
        literal {prompt} placeholder for CLIs that require argv prompts.
        """
        rows = conn.execute(
            """SELECT DISTINCT ON (provider_slug, model_slug)
                provider_slug, model_slug, transport_type, status, priority, balance_weight,
                capability_tags, default_parameters, cli_config, reasoning_control
            FROM provider_model_candidates
            WHERE status = 'active'
            ORDER BY provider_slug, model_slug, priority ASC, created_at DESC"""
        )
        agents = []
        for r in rows:
            agents.append(_agent_config_from_candidate_row(r))

        agents_by_slug = {agent.slug: agent for agent in agents}
        try:
            route_rows = conn.execute(
                """SELECT task_type, provider_slug, model_slug, rank,
                          transport_type,
                          benchmark_score, cost_per_m_tokens,
                          route_tier, route_tier_rank,
                          latency_class, latency_rank,
                          reasoning_control, updated_at
                   FROM task_type_routing
                   WHERE permitted = true
                   ORDER BY task_type ASC, rank ASC, updated_at DESC"""
            )
        except Exception:
            route_rows = []
        try:
            effort_rows = conn.execute(
                """SELECT task_type, sub_task_type, provider_slug, model_slug,
                          transport_type, effort_slug, provider_payload,
                          cost_multiplier, latency_multiplier, quality_bias,
                          effort_policy_decision_ref, effort_matrix_decision_ref
                   FROM effective_task_type_effort_routes
                   WHERE permitted = true
                     AND effort_supported = true"""
            )
        except Exception:
            effort_rows = []
        effort_by_route: dict[tuple[str, str, str], dict[str, Any]] = {}
        for row in effort_rows:
            task_type = _normalize_auto_route_key(row.get("task_type"))
            concrete_slug = f"{row.get('provider_slug')}/{row.get('model_slug')}"
            effort_payload = row.get("provider_payload") or {}
            if isinstance(effort_payload, str):
                effort_payload = json.loads(effort_payload)
            if not isinstance(effort_payload, Mapping):
                effort_payload = {}
            effort_by_route[(task_type, concrete_slug, str(row.get("transport_type") or "").lower())] = {
                "effort_slug": str(row.get("effort_slug") or ""),
                "provider_payload": dict(effort_payload),
                "cost_multiplier": float(row.get("cost_multiplier") or 1.0),
                "latency_multiplier": float(row.get("latency_multiplier") or 1.0),
                "quality_bias": float(row.get("quality_bias") or 0.0),
                "effort_policy_decision_ref": str(row.get("effort_policy_decision_ref") or ""),
                "effort_matrix_decision_ref": str(row.get("effort_matrix_decision_ref") or ""),
            }
        task_type_targets: dict[str, dict[str, tuple[tuple[Any, ...], AgentConfig]]] = {}
        tier_targets: dict[str, dict[str, tuple[tuple[Any, ...], AgentConfig]]] = {
            tier: {} for tier in _AUTO_ROUTE_TIERS
        }
        latency_targets: dict[str, dict[str, tuple[tuple[Any, ...], AgentConfig]]] = {
            latency: {} for latency in _AUTO_LATENCY_CLASSES
        }

        def _store_best(
            bucket: dict[str, tuple[tuple[Any, ...], AgentConfig]],
            concrete_slug: str,
            sort_key: tuple[Any, ...],
            concrete: AgentConfig,
        ) -> None:
            existing = bucket.get(concrete_slug)
            if existing is None or sort_key < existing[0]:
                bucket[concrete_slug] = (sort_key, concrete)

        for row in route_rows:
            task_type = _normalize_auto_route_key(row["task_type"])
            concrete_slug = f"{row['provider_slug']}/{row['model_slug']}"
            concrete = agents_by_slug.get(concrete_slug)
            if concrete is None or not task_type:
                continue
            if not _transport_matches_route(row, concrete):
                continue
            row_reasoning_control = row.get("reasoning_control") or {}
            if isinstance(row_reasoning_control, str):
                row_reasoning_control = json.loads(row_reasoning_control)
            if not isinstance(row_reasoning_control, Mapping):
                row_reasoning_control = {}
            route_transport = "cli" if concrete.execution_transport is ExecutionTransport.cli else "api"
            effort_control = effort_by_route.get((task_type, concrete_slug, route_transport), {})
            if effort_control:
                merged_reasoning_control = {
                    **dict(concrete.reasoning_control),
                    **dict(row_reasoning_control),
                    "selected_effort": effort_control,
                }
                concrete = _agent_with_reasoning_control(
                    concrete,
                    reasoning_control=merged_reasoning_control,
                )
            benchmark = -float(row.get("benchmark_score") or 0.0)
            cost = float(row.get("cost_per_m_tokens") or 0.0)
            updated_at = str(row.get("updated_at") or "")
            task_bucket = task_type_targets.setdefault(task_type, {})
            _store_best(
                task_bucket,
                concrete_slug,
                (int(row.get("rank") or 99), benchmark, cost, updated_at, concrete_slug),
                concrete,
            )

            route_tier = _normalize_auto_route_key(row.get("route_tier"))
            if route_tier in tier_targets:
                _store_best(
                    tier_targets[route_tier],
                    concrete_slug,
                    (
                        int(row.get("route_tier_rank") or row.get("rank") or 99),
                        benchmark,
                        cost,
                        updated_at,
                        concrete_slug,
                    ),
                    concrete,
                )

            latency_class = _normalize_auto_route_key(row.get("latency_class"))
            if latency_class in latency_targets:
                _store_best(
                    latency_targets[latency_class],
                    concrete_slug,
                    (
                        int(row.get("latency_rank") or row.get("rank") or 99),
                        benchmark,
                        cost,
                        updated_at,
                        concrete_slug,
                    ),
                    concrete,
                )

        def _synthesize_alias(
            alias_slug: str,
            targets: list[AgentConfig],
            *,
            extend_stage: str | None = None,
        ) -> None:
            if alias_slug in agents_by_slug or not targets:
                return
            primary = targets[0]
            allowed_stages = primary.allowed_stages
            if extend_stage:
                allowed_stages = tuple(dict.fromkeys((*primary.allowed_stages, extend_stage)))
            alias = AgentConfig(
                slug=alias_slug,
                provider=primary.provider,
                model=primary.model,
                wrapper_command=primary.wrapper_command,
                docker_image=primary.docker_image,
                context_window=primary.context_window,
                max_output_tokens=primary.max_output_tokens,
                cost_per_input_mtok=primary.cost_per_input_mtok,
                cost_per_output_mtok=primary.cost_per_output_mtok,
                timeout_seconds=primary.timeout_seconds,
                idle_timeout_seconds=primary.idle_timeout_seconds,
                failover_targets=tuple(agent.slug for agent in targets[1:]),
                allowed_stages=allowed_stages,
                capability_tier=primary.capability_tier,
                output_format=primary.output_format,
                execution_transport=primary.execution_transport,
                sandbox_provider=primary.sandbox_provider,
                sandbox_policy=primary.sandbox_policy,
                reasoning_control=primary.reasoning_control,
            )
            agents.append(alias)
            agents_by_slug[alias_slug] = alias

        for task_type, target_map in task_type_targets.items():
            targets = [entry[1] for entry in sorted(target_map.values(), key=lambda item: item[0])]
            if not targets:
                continue
            _synthesize_alias(_auto_route_slug(task_type), targets, extend_stage=task_type)

        for alias_task_type, backing_task_type in _SEMANTIC_AUTO_ROUTE_ALIASES.items():
            target_map = task_type_targets.get(backing_task_type, {})
            targets = [entry[1] for entry in sorted(target_map.values(), key=lambda item: item[0])]
            if not targets:
                continue
            _synthesize_alias(_auto_route_slug(alias_task_type), targets, extend_stage=alias_task_type)

        for route_tier, target_map in tier_targets.items():
            targets = [entry[1] for entry in sorted(target_map.values(), key=lambda item: item[0])]
            _synthesize_alias(_auto_route_slug(route_tier), targets)

        for latency_class, target_map in latency_targets.items():
            targets = [entry[1] for entry in sorted(target_map.values(), key=lambda item: item[0])]
            _synthesize_alias(_auto_route_slug(latency_class), targets)

        return cls(agents)

    @classmethod
    def load_from_postgres_for_route(
        cls,
        conn,
        *,
        provider_slug: str,
        model_slug: str,
        transport_type: str,
        candidate_ref: str | None = None,
    ) -> AgentConfig | None:
        """Load the exact provider/model/transport candidate for execution.

        ``load_from_postgres`` keeps the legacy provider/model registry view.
        Execution routes that already carry candidate/transport authority use
        this exact lookup so a stale CLI template cannot hijack an API route
        for the same provider/model slug.
        """
        normalized_transport = str(transport_type or "").strip().upper()
        if normalized_transport not in {"CLI", "API"}:
            return None
        normalized_candidate_ref = str(candidate_ref or "").strip()
        if normalized_candidate_ref:
            rows = conn.execute(
                """SELECT provider_slug, model_slug, transport_type, status, priority, balance_weight,
                          capability_tags, default_parameters, cli_config, reasoning_control
                   FROM provider_model_candidates
                   WHERE status = 'active'
                     AND candidate_ref = $1
                     AND provider_slug = $2
                     AND model_slug = $3
                     AND transport_type = $4
                   ORDER BY priority ASC, created_at DESC
                   LIMIT 1""",
                normalized_candidate_ref,
                provider_slug,
                model_slug,
                normalized_transport,
            )
            if rows:
                return _agent_config_from_candidate_row(rows[0])
            return None
        rows = conn.execute(
            """SELECT provider_slug, model_slug, transport_type, status, priority, balance_weight,
                      capability_tags, default_parameters, cli_config, reasoning_control
               FROM provider_model_candidates
               WHERE status = 'active'
                 AND provider_slug = $1
                 AND model_slug = $2
                 AND transport_type = $3
               ORDER BY priority ASC, created_at DESC
               LIMIT 1""",
            provider_slug,
            model_slug,
            normalized_transport,
        )
        if not rows:
            return None
        return _agent_config_from_candidate_row(rows[0])

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    def get(self, slug: str) -> AgentConfig | None:
        """Return agent by slug, or ``None``."""
        direct = self._by_slug.get(slug)
        if direct is not None:
            return direct
        canonical_slug = _LEGACY_AGENT_SLUG_ALIASES.get(slug)
        if canonical_slug:
            return self._by_slug.get(canonical_slug)
        return None

    def list_by_provider(self, provider: str) -> list[AgentConfig]:
        """Return agents for a provider (defensive copy)."""
        return list(self._by_provider.get(provider, []))

    def list_by_tier(self, tier: str) -> list[AgentConfig]:
        """Return agents in a capability tier (defensive copy)."""
        return list(self._by_tier.get(tier, []))

    def list_by_stage(self, stage: str) -> list[AgentConfig]:
        """Return agents allowed in a pipeline stage (defensive copy)."""
        return list(self._by_stage.get(stage, []))

    def list_auto_routes(self) -> list[AgentConfig]:
        """Return synthesized ``auto/*`` route aliases (defensive copy).

        These are route selectors (task-type, tier, latency) that resolve
        to a primary concrete agent at dispatch time. They are deliberately
        excluded from :meth:`list_by_provider`, :meth:`list_by_tier`, and
        :meth:`list_by_stage` so those aggregates report canonical registry
        rows only. Callers that need to enumerate alias routes come here.
        """
        return list(self._auto_routes.values())

    def failover_chain(self, slug: str, *, strict: bool = False) -> list[AgentConfig]:
        """Resolve the ordered failover chain starting from *slug*.

        When *strict* is ``True``, raises ``AgentConfigError`` if a cycle
        is encountered.  Otherwise the walk silently stops at already-
        visited nodes (safe default for production use).

        The returned list does **not** include the starting agent.
        """
        chain: list[AgentConfig] = []
        visited: set[str] = {slug}
        current = self._by_slug.get(slug)
        if current is None:
            return chain

        for target_slug in current.failover_targets:
            self._walk_failover(target_slug, visited, chain, strict=strict)
        return chain

    def _walk_failover(
        self,
        slug: str,
        visited: set[str],
        chain: list[AgentConfig],
        *,
        strict: bool = False,
    ) -> None:
        if slug in visited:
            if strict:
                raise AgentConfigError(
                    "failover_cycle",
                    f"Cycle detected in failover chain at slug: {slug}",
                )
            return
        agent = self._by_slug.get(slug)
        if agent is None:
            return
        visited.add(slug)
        chain.append(agent)
        for target_slug in agent.failover_targets:
            self._walk_failover(target_slug, visited, chain, strict=strict)

    # ------------------------------------------------------------------
    # Immutability guard
    # ------------------------------------------------------------------

    def __setattr__(self, name: str, value: Any) -> None:
        if getattr(self, "_sealed", False) and name != "_sealed":
            raise AgentConfigError(
                "immutable",
                "AgentRegistry is immutable after load",
            )
        super().__setattr__(name, value)
