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
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

_AUTO_ROUTE_TIERS = frozenset({"high", "medium", "low"})
_AUTO_LATENCY_CLASSES = frozenset({"reasoning", "instant"})
_SEMANTIC_AUTO_ROUTE_ALIASES = {
    "draft": "chat",
    "classify": "support",
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
    host_local = "host_local"
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


def _normalize_auto_route_key(value: Any) -> str:
    key = str(value or "").strip().lower()
    if key.startswith("auto/"):
        key = key.split("/", 1)[1]
    return key


def _auto_route_slug(value: Any) -> str:
    key = _normalize_auto_route_key(value)
    return f"auto/{key}" if key else ""


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
        self._sealed = False

        for agent in agents:
            if agent.slug in self._by_slug:
                raise AgentConfigError(
                    "duplicate_slug",
                    f"Duplicate agent slug: {agent.slug}",
                )
            self._by_slug[agent.slug] = agent
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
                provider_slug, model_slug, status, priority, balance_weight,
                capability_tags, default_parameters, cli_config
            FROM provider_model_candidates
            WHERE status = 'active'
            ORDER BY provider_slug, model_slug, priority ASC, created_at DESC"""
        )
        from registry.model_context_limits import context_window_for_model
        agents = []
        for r in rows:
            provider = r["provider_slug"]
            model = r["model_slug"]
            slug = f"{provider}/{model}"
            tags = r["capability_tags"]
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
                # model_profiles missing context_window — fall back to
                # default_parameters from the candidate row, then 128k.
                _defaults = r.get("default_parameters") or {}
                if isinstance(_defaults, str):
                    _defaults = json.loads(_defaults)
                context_window = int(_defaults.get("context_window") or 128_000)

            # Read CLI config from DB
            cli_cfg = r.get("cli_config") or {}
            if isinstance(cli_cfg, str):
                cli_cfg = json.loads(cli_cfg)

            cmd_template = cli_cfg.get("cmd_template")
            envelope_key = cli_cfg.get("envelope_key")

            if cmd_template:
                # Build wrapper from DB template — replace {model}
                resolved = [part.replace("{model}", model) for part in cmd_template]
                import shlex
                wrapper = " ".join(shlex.quote(part) for part in resolved)
                execution_transport = ExecutionTransport.cli
            else:
                wrapper = None
                execution_transport = ExecutionTransport.api
            sandbox_provider = _default_sandbox_provider(transport=execution_transport)

            agents.append(AgentConfig(
                slug=slug,
                provider=provider,
                model=model,
                wrapper_command=wrapper,
                docker_image=None,
                context_window=context_window,
                max_output_tokens=0,  # CLIs handle their own limits
                cost_per_input_mtok=5.0,
                cost_per_output_mtok=20.0,
                timeout_seconds=900,
                idle_timeout_seconds=180,
                failover_targets=(),
                allowed_stages=("plan", "build", "review", "debate", "test", "debug"),
                capability_tier=tier,
                output_format=cli_cfg.get("output_format", "json"),
                execution_transport=execution_transport,
                sandbox_provider=sandbox_provider,
                sandbox_policy=SandboxPolicy(),
            ))

        agents_by_slug = {agent.slug: agent for agent in agents}
        try:
            route_rows = conn.execute(
                """SELECT task_type, provider_slug, model_slug, rank,
                          benchmark_score, cost_per_m_tokens,
                          route_tier, route_tier_rank,
                          latency_class, latency_rank,
                          updated_at
                   FROM task_type_routing
                   WHERE permitted = true
                   ORDER BY task_type ASC, rank ASC, updated_at DESC"""
            )
        except Exception:
            route_rows = []
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
