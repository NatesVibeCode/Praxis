"""Task-type routing for workflow specs.

Resolves ``auto/{task_type}`` and advanced auto buckets to concrete
provider/model slugs using Postgres authority.

Long-term authority split:
- ``provider_model_candidates`` = full executable model catalog
- ``task_type_route_profiles`` = task intent, affinity labels, metric weights
- ``market_benchmark_metric_registry`` = benchmark directions / metric meaning
- ``task_type_routing`` = explicit overrides plus durable route state/health
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable, Mapping, Optional

from asyncpg import PostgresError
from registry.provider_execution_registry import (
    resolve_default_adapter_type,
)
from registry.runtime_profile_admission import (
    RuntimeProfileAdmittedCandidate,
    load_admitted_runtime_profile_candidates,
)
from .provider_authority import ProviderAuthorityError, provider_authority_fail
from .route_authority_snapshot import (
    RouteAuthoritySnapshot,
    get_route_authority_snapshot,
    get_task_route_policy,
    invalidate_all_route_authority_snapshots,
    invalidate_route_authority_snapshot,
)
from .composite_scorer import CompositeScorer, ScaleFn
from .routing_economics import (
    BudgetAuthoritySnapshot,
    economic_rationale as _economic_rationale,
    load_budget_authority_snapshot as _load_budget_authority_snapshot,
    resolve_route_economics as _resolve_route_economics,
    row_effective_marginal_cost as _row_effective_marginal_cost,
)
from .routing_scorer import (
    apply_profile_benchmark_scores as _apply_profile_benchmark_scores,
    base_cost_per_m_tokens as _base_cost_per_m_tokens,
    candidate_affinity_labels as _candidate_affinity_labels,
    candidate_avoid_labels as _candidate_avoid_labels,
    candidate_common_metrics as _candidate_common_metrics,
    candidate_is_research_only as _candidate_is_research_only,
    derived_rank_from_score as _derived_rank_from_score,
    failure_penalty as _failure_penalty,
    match_affinity_bucket as _match_affinity_bucket,
    metric_value as _metric_value,
    positive_candidate_labels as _positive_candidate_labels,
    profile_task_rank_score as _profile_task_rank_score,
    rerank_rows as _rerank_rows,
)
from .reasoning_effort_routing import (
    ReasoningEffortRoutingError,
    resolve_reasoning_effort_route,
)
from storage.postgres.task_type_routing_repository import PostgresTaskTypeRoutingRepository

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)
_UNDEFINED_TABLE_SQLSTATE = "42P01"
_ROUTE_TIER_BUCKETS = frozenset({"high", "medium", "low"})
_LATENCY_CLASS_BUCKETS = frozenset({"reasoning", "instant"})
_SEMANTIC_AUTO_ROUTE_ALIASES = {
    "draft": "chat",
    "classify": "analysis",
}
_ROUTE_HEALTH_EXTERNAL_FAILURE_CATEGORIES = frozenset(
    {
        "timeout",
        "rate_limit",
        "provider_error",
        "network_error",
        "infrastructure",
    }
)
_ROUTE_HEALTH_CONFIG_FAILURE_CATEGORIES = frozenset(
    {
        "credential_error",
        "input_error",
        "model_error",
    }
)
_DEEPSEEK_SCOPE_TASK_TYPES = frozenset({"research"})
_DEEPSEEK_SCOPE_PROVIDER_SLUGS = frozenset({"openrouter", "together"})
_DEEPSEEK_MODEL_KEY = "deepseek"


def _is_deepseek_scoped_for_task_type(task_type: str) -> bool:
    normalized_task_type = task_type.strip().lower()
    return (
        normalized_task_type in _DEEPSEEK_SCOPE_TASK_TYPES
        or normalized_task_type == "materialize"
        or normalized_task_type.startswith("materialize_")
    )


def _is_scoped_deepseek_candidate(candidate: dict[str, Any]) -> bool:
    provider_slug = str(candidate.get("provider_slug") or "").strip().lower()
    model_slug = str(candidate.get("model_slug") or "").strip().lower()
    return (
        provider_slug in _DEEPSEEK_SCOPE_PROVIDER_SLUGS
        and _DEEPSEEK_MODEL_KEY in model_slug
    )


class TaskRouteAuthorityError(ProviderAuthorityError):
    """Raised when required task routing authority is missing from Postgres."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str = "provider_authority.task_route_unavailable",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(reason_code=reason_code, message=message, details=details)

@dataclass(frozen=True)
class TaskRoutePolicy:
    task_rank_weight: float
    route_health_weight: float
    cost_weight: float
    benchmark_weight: float
    prefer_cost_task_rank_weight: float
    prefer_cost_route_health_weight: float
    prefer_cost_cost_weight: float
    prefer_cost_benchmark_weight: float
    claim_route_health_weight: float
    claim_rank_weight: float
    claim_load_weight: float
    claim_internal_failure_penalty_step: float
    claim_priority_penalty_step: float
    neutral_benchmark_score: float
    mixed_benchmark_score: float
    neutral_route_health: float
    min_route_health: float
    max_route_health: float
    success_health_bump: float
    review_success_bump: float
    consecutive_failure_penalty_step: float
    consecutive_failure_penalty_cap: float
    internal_failure_penalties: dict[str, float]
    review_severity_penalties: dict[str, float]

    def scorer(self, *, prefer_cost: bool) -> CompositeScorer:
        if prefer_cost:
            return CompositeScorer([
                ("task_rank", self.prefer_cost_task_rank_weight, ScaleFn.LINEAR, True),
                ("route_health", self.prefer_cost_route_health_weight, ScaleFn.LINEAR, True),
                ("cost", self.prefer_cost_cost_weight, ScaleFn.LOGARITHMIC, True),
                ("benchmark_score", self.prefer_cost_benchmark_weight, ScaleFn.LINEAR, True),
            ])
        return CompositeScorer([
            ("task_rank", self.task_rank_weight, ScaleFn.LINEAR, True),
            ("route_health", self.route_health_weight, ScaleFn.LINEAR, True),
            ("cost", self.cost_weight, ScaleFn.LOGARITHMIC, True),
            ("benchmark_score", self.benchmark_weight, ScaleFn.LINEAR, True),
        ])



def _parse_json_field(value: object, default: object) -> object:
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return value if value is not None else default


_ELIGIBILITY_SQL = """
    SELECT task_route_eligibility_id, task_type, provider_slug, model_slug,
           eligibility_status, reason_code, rationale, effective_from, effective_to, decision_ref
    FROM task_type_route_eligibility
    WHERE provider_slug = ANY($1::text[])
      AND (task_type = $2 OR task_type IS NULL)
      AND (model_slug = ANY($3::text[]) OR model_slug IS NULL)
      AND effective_from <= $4
      AND (effective_to IS NULL OR effective_to > $4)
    ORDER BY effective_from DESC, task_route_eligibility_id DESC
"""

# BUG-DD1D48B1: route selection must honor provider_transport_admissions so
# providers whose default adapter transport is denied by policy (e.g.
# anthropic.llm_task per decision.2026-04-20.anthropic-cli-only-restored)
# never surface as a route candidate. Without this filter the router
# happily emits anthropic/claude-opus-4-7 for auto/review, the worker hits
# the Anthropic HTTP API with an unauthenticated key, and a whole batch
# collapses on 401 before any work starts.
_PROVIDER_TRANSPORT_ADMISSION_SQL = """
    SELECT provider_slug, adapter_type, admitted_by_policy, policy_reason, decision_ref
    FROM provider_transport_admissions
    WHERE provider_slug = ANY($1::text[])
      AND adapter_type = ANY($2::text[])
"""


def _state_int(state_row: dict[str, Any] | None, key: str) -> int:
    return int(state_row.get(key) or 0) if state_row is not None else 0


def _state_str(state_row: dict[str, Any] | None, key: str) -> str:
    return str(state_row.get(key) or "") if state_row is not None else ""


def _rotate_chain(chain: list[Any], task_type: str, rotation_counters: dict[str, int]) -> tuple[Any, tuple[str, ...]]:
    """Dedup by provider and rotate chain for diversity. Returns (primary, slugs)."""
    from registry.provider_execution_registry import get_profile as _rotation_profile
    seen_providers: set[str] = set()
    provider_best: list = []
    provider_rest: list = []
    for d in chain:
        rp = _rotation_profile(d.provider_slug)
        if rp and rp.exclude_from_rotation:
            provider_rest.append(d)
        elif d.provider_slug not in seen_providers:
            seen_providers.add(d.provider_slug)
            provider_best.append(d)
        else:
            provider_rest.append(d)
    deduped = provider_best + provider_rest
    idx = rotation_counters.get(task_type, 0)
    rotated = deduped[idx % len(deduped):] + deduped[:idx % len(deduped)]
    rotation_counters[task_type] = idx + 1
    return rotated[0], tuple(f"{d.provider_slug}/{d.model_slug}" for d in rotated)


def _normalize_auto_route_key(value: str) -> str:
    key = (value or "").strip().lower()
    if key.startswith("auto/"):
        key = key.split("/", 1)[1]
    # Strip tier suffix if present (e.g. "build/medium" → "build")
    if "/" in key and key.rsplit("/", 1)[-1] in _ROUTE_TIER_BUCKETS:
        key = key.rsplit("/", 1)[0]
    return key


def _parse_auto_tier_override(value: str) -> str | None:
    """Extract tier override from 'auto/build/medium' → 'medium', or None."""
    key = (value or "").strip().lower()
    if key.startswith("auto/"):
        key = key.split("/", 1)[1]
    if "/" in key:
        tier = key.rsplit("/", 1)[-1]
        if tier in _ROUTE_TIER_BUCKETS:
            return tier
    return None


def _resolve_semantic_auto_route(task_type: str) -> str:
    """Map product-facing semantic lanes onto backed route authority."""
    return _SEMANTIC_AUTO_ROUTE_ALIASES.get(task_type, task_type)


def _normalize_transport_type(value: object) -> str:
    normalized = str(value or "").strip().upper()
    return normalized if normalized in {"CLI", "API"} else ""


def _adapter_type_for_transport(transport_type: object) -> str | None:
    normalized = _normalize_transport_type(transport_type)
    if normalized == "CLI":
        return "cli_llm"
    if normalized == "API":
        return "llm_task"
    return None


def _transport_type_for_adapter(adapter_type: object) -> str:
    normalized = str(adapter_type or "").strip().lower()
    if normalized == "cli_llm":
        return "CLI"
    if normalized == "llm_task":
        return "API"
    return ""


def _candidate_adapter_type(
    candidate: dict[str, Any],
    *,
    default_adapter: str,
) -> str:
    explicit_adapter = str(candidate.get("adapter_type") or "").strip().lower()
    if explicit_adapter:
        return explicit_adapter
    transport_adapter = _adapter_type_for_transport(candidate.get("transport_type"))
    if transport_adapter:
        return transport_adapter
    return str(default_adapter or "").strip().lower()


def _route_candidate_payload(decision: "TaskRouteDecision") -> dict[str, Any]:
    provider_slug = str(getattr(decision, "provider_slug", "") or "").strip()
    model_slug = str(getattr(decision, "model_slug", "") or "").strip()
    return {
        "slug": f"{provider_slug}/{model_slug}",
        "candidate_ref": str(getattr(decision, "candidate_ref", "") or "").strip(),
        "provider_slug": provider_slug,
        "host_provider_slug": str(getattr(decision, "host_provider_slug", "") or "").strip(),
        "model_slug": model_slug,
        "transport_type": _normalize_transport_type(getattr(decision, "transport_type", "")),
        "adapter_type": str(getattr(decision, "adapter_type", "") or "").strip(),
        "variant": str(getattr(decision, "variant", "") or "").strip(),
        "effort_slug": str(getattr(decision, "effort_slug", "") or "").strip(),
        "task_type": str(getattr(decision, "task_type", "") or "").strip(),
        "rank": int(getattr(decision, "rank", 0) or 0),
    }


def _route_identity_tuple(row: Mapping[str, Any]) -> tuple[str, str, str, str, str, str]:
    return (
        str(row.get("provider_slug") or "").strip(),
        str(row.get("model_slug") or "").strip(),
        _normalize_transport_type(row.get("transport_type")),
        str(row.get("host_provider_slug") or "").strip(),
        str(row.get("variant") or "").strip(),
        str(row.get("effort_slug") or "").strip(),
    )


def _coerce_json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise TaskRouteAuthorityError(
                f"{field_name} must decode to a JSON object",
            ) from exc
    if not isinstance(value, dict):
        raise TaskRouteAuthorityError(f"{field_name} must be a JSON object")
    return dict(value)


def _coerce_json_array(value: object, *, field_name: str) -> list[Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError as exc:
            raise TaskRouteAuthorityError(
                f"{field_name} must decode to a JSON array",
            ) from exc
    if not isinstance(value, list):
        raise TaskRouteAuthorityError(f"{field_name} must be a JSON array")
    return list(value)


def _json_text_values(value: object, *, field_name: str) -> tuple[str, ...]:
    values = _coerce_json_array(value, field_name=field_name)
    normalized: list[str] = []
    for index, item in enumerate(values):
        if not isinstance(item, str) or not item.strip():
            raise TaskRouteAuthorityError(
                f"{field_name}[{index}] must be a non-empty string",
            )
        normalized.append(item.strip().lower())
    return tuple(dict.fromkeys(normalized))


def _json_float_map(value: object, *, field_name: str) -> dict[str, float]:
    obj = _coerce_json_object(value, field_name=field_name)
    normalized: dict[str, float] = {}
    for key, raw in obj.items():
        if not isinstance(key, str) or not key.strip():
            raise TaskRouteAuthorityError(f"{field_name} keys must be non-empty strings")
        try:
            normalized[key.strip()] = float(raw)
        except (TypeError, ValueError) as exc:
            raise TaskRouteAuthorityError(
                f"{field_name}.{key} must be numeric",
            ) from exc
    return normalized



@dataclass(frozen=True)
class BenchmarkMetricDefinition:
    metric_key: str
    higher_is_better: bool
    enabled: bool


@dataclass(frozen=True)
class TaskTypeRouteProfile:
    task_type: str
    affinity_labels: dict[str, tuple[str, ...]]
    affinity_weights: dict[str, float]
    task_rank_weights: dict[str, float]
    benchmark_metric_weights: dict[str, float]
    route_tier_preferences: tuple[str, ...]
    latency_class_preferences: tuple[str, ...]
    allow_unclassified_candidates: bool
    rationale: str


@dataclass(frozen=True)
class TaskRouteDecision:
    task_type: str
    model_slug: str
    provider_slug: str
    rank: int
    benchmark_score: float
    benchmark_name: str
    cost_per_m_tokens: float
    rationale: str
    was_auto: bool
    candidate_ref: str = ""
    adapter_type: str = "cli_llm"
    transport_type: str = "CLI"
    host_provider_slug: str = ""
    variant: str = ""
    effort_slug: str = ""
    billing_mode: str = "metered_api"
    budget_bucket: str = "unknown"
    effective_marginal_cost: float = 0.0
    spend_pressure: str = "unknown"
    budget_status: str = ""
    prefer_prepaid: bool = False
    allow_payg_fallback: bool = False
    # True iff the budget-window authority table was unreachable when this
    # decision was built. Downstream lane admission (chat_orchestrator +
    # _apply_lane_policy) must refuse paid lanes when this is set —
    # see runtime.lane_policy.admit_adapter_type.
    budget_authority_unreachable: bool = False
    # True iff an error-severity quality rule on provider_budget_windows
    # has a latest run in ('fail','error') — authority data is in a state
    # the operator has declared invalid. Same fail-closed treatment as
    # budget_authority_unreachable, reason code
    # lane.rejected.budget_window_data_quality_error.
    budget_window_data_quality_error: bool = False
    temperature: float | None = None
    max_tokens: int | None = None
    reasoning_effort_slug: str = ""
    reasoning_provider_payload: dict[str, Any] = field(default_factory=dict)


def _resolve_default_adapter_for_router(router: Any, provider_slug: str) -> str:
    resolver = getattr(router, "_default_adapter_for_provider", None)
    if callable(resolver):
        return str(resolver(provider_slug))
    injected_default = getattr(router, "_default_adapter_type", None)
    if injected_default:
        return str(injected_default)
    return resolve_default_adapter_type(provider_slug)


def _provider_registry_authority_available() -> bool:
    from registry.provider_execution_registry import registry_health

    try:
        return bool(registry_health().get("authority_available", False))
    except RuntimeError:
        return False


def _explicit_route_transport_unknown_economics(
    *,
    adapter_type: str,
    raw_cost_per_m_tokens: float,
    budget_authority: BudgetAuthoritySnapshot,
) -> dict[str, Any]:
    """Fail-closed economics when transport registry is unreachable.

    Explicit route resolution still needs to expose budget authority state.
    If the candidate row already names the adapter but the transport registry
    cannot prove economics, keep the decision inspectable while refusing any
    implicit PAYG fallback.
    """

    return {
        "adapter_type": adapter_type,
        "billing_mode": "metered_api",
        "budget_bucket": "unknown",
        "effective_marginal_cost": float(raw_cost_per_m_tokens or 0.0),
        "spend_pressure": "unknown",
        "budget_status": "",
        "prefer_prepaid": False,
        "allow_payg_fallback": False,
        "budget_authority_unreachable": not budget_authority.reachable,
        "budget_window_data_quality_error": not budget_authority.data_quality_ok,
    }


@dataclass(frozen=True)
class TaskRouteEligibilityDecision:
    task_route_eligibility_id: str
    task_type: str | None
    provider_slug: str
    model_slug: str | None
    eligibility_status: str
    reason_code: str
    rationale: str
    effective_from: datetime
    effective_to: datetime | None
    decision_ref: str


@dataclass(frozen=True)
class RoutePlan:
    """Complete routing plan: primary agent + failover chain + policy.

    The dispatcher just executes this — all routing intelligence lives here.
    """
    primary: str                        # "provider/model" for first attempt
    chain: tuple[str, ...]              # full ordered failover chain including primary
    failover_eligible_codes: frozenset[str]  # failure codes that trigger failover to next model
    transient_retry_codes: frozenset[str]    # failure codes that retry same model with backoff
    max_same_model_retries: int         # how many times to retry same model before failover
    backoff_seconds: tuple[int, ...]    # backoff schedule for same-model retries
    task_type: str
    original_slug: str
    route_candidates: tuple[dict[str, Any], ...] = ()

    @staticmethod
    def default_failover_codes() -> frozenset[str]:
        """Failure codes indicating a provider-level issue where the next model may help."""
        return frozenset({
            "model_not_found", "model_unavailable", "auth_failure",
            "rate_limited", "quota_exceeded", "provider_server_error",
            "connection_error", "network_error", "setup_failure",
        })

    @staticmethod
    def default_transient_codes() -> frozenset[str]:
        """Failure codes where same-model retry with backoff makes sense."""
        return frozenset({
            "rate_limited", "timeout", "provider_server_error",
            "connection_error", "network_error",
        })


class TaskTypeRouter:
    """Resolve auto routes from catalog authority plus task-specific state."""

    def __init__(
        self,
        conn: SyncPostgresConnection,
        *,
        now_factory: Callable[[], datetime] | None = None,
        use_default_runtime_profile: bool = False,
    ) -> None:
        self._conn = conn
        self._now_factory = now_factory or (lambda: datetime.now(timezone.utc))
        self._use_default_runtime_profile = use_default_runtime_profile
        self._routing_repository = PostgresTaskTypeRoutingRepository(self._conn)
        authority = get_route_authority_snapshot(
            self._conn,
            load_snapshot=self._load_static_authority_snapshot,
        )
        self._policy = authority.route_policy
        self._failure_zones = authority.failure_zones
        self._task_profiles = authority.task_profiles
        self._benchmark_metrics = authority.benchmark_metrics
        self._runtime_profile_candidate_cache: dict[str, tuple[dict[str, Any], ...]] = {}
        self._reasoning_effort_authority_available: bool | None = None

    def _default_adapter_for_provider(self, provider_slug: str) -> str:
        return resolve_default_adapter_type(provider_slug)

    def _has_reasoning_effort_authority(self) -> bool:
        if self._reasoning_effort_authority_available is not None:
            return self._reasoning_effort_authority_available
        try:
            rows = self._conn.execute(
                """
                SELECT to_regclass('public.provider_reasoning_effort_matrix') IS NOT NULL AS matrix_ready,
                       to_regclass('public.task_type_effort_policy') IS NOT NULL AS policy_ready
                """
            )
        except Exception:
            self._reasoning_effort_authority_available = False
            return False
        row = rows[0] if rows else {}
        self._reasoning_effort_authority_available = bool(
            row.get("matrix_ready") and row.get("policy_ready")
        )
        return self._reasoning_effort_authority_available

    def _reasoning_effort_fields(
        self,
        *,
        task_type: str,
        provider_slug: str,
        model_slug: str,
        adapter_type: str,
    ) -> dict[str, Any]:
        if not self._has_reasoning_effort_authority():
            return {}
        try:
            effort_route = resolve_reasoning_effort_route(
                self._conn,
                task_type=task_type,
                provider_slug=provider_slug,
                model_slug=model_slug,
                transport_type=adapter_type,
            )
        except ReasoningEffortRoutingError as exc:
            raise TaskRouteAuthorityError(
                f"reasoning effort authority rejected {provider_slug}/{model_slug}: {exc}",
                reason_code=f"provider_authority.{exc.reason_code}",
                details=exc.details,
            ) from exc
        return {
            "reasoning_effort_slug": effort_route.effort_slug,
            "reasoning_provider_payload": dict(effort_route.provider_payload),
        }

    @property
    def route_policy(self) -> TaskRoutePolicy:
        return self._policy

    def _policy_for(self, task_type: str) -> TaskRoutePolicy:
        """Get task-type-specific policy, falling back to default."""
        return get_task_route_policy(
            self._conn,
            task_type=task_type,
            load_policy=self._load_route_policy_for_task_type,
        )

    @classmethod
    def invalidate_authority_snapshot(cls, conn: SyncPostgresConnection) -> None:
        """Invalidate cached static authority for one authority source."""
        invalidate_route_authority_snapshot(conn)

    @classmethod
    def invalidate_all_authority_snapshots(cls) -> None:
        """Invalidate all cached static authority snapshots in this process."""
        invalidate_all_route_authority_snapshots()

    @classmethod
    def _load_static_authority_snapshot(cls, conn: SyncPostgresConnection) -> RouteAuthoritySnapshot:
        return RouteAuthoritySnapshot(
            route_policy=cls._load_route_policy_row(conn, task_type=None),
            failure_zones=cls._load_failure_zones(conn),
            task_profiles=cls._load_task_route_profiles(conn),
            benchmark_metrics=cls._load_benchmark_metric_registry(conn),
        )

    @staticmethod
    def _load_failure_zones(conn: SyncPostgresConnection) -> dict[str, str]:
        try:
            rows = conn.execute("SELECT category, zone FROM failure_category_zones")
        except Exception as exc:
            raise TaskRouteAuthorityError("failure_category_zones authority is required for task routing") from exc
        zone_map = {str(row["category"]): str(row["zone"]) for row in rows or [] if row.get("category")}
        if not zone_map:
            raise TaskRouteAuthorityError("failure_category_zones did not return any rows")
        return zone_map

    def _normalized_failure_details(
        self,
        *,
        failure_code: str | None,
        failure_category: str,
        failure_zone: str,
    ) -> tuple[str, str]:
        normalized_category = (failure_category or "").strip()
        normalized_zone = (failure_zone or "").strip()
        if failure_code:
            from runtime.failure_classifier import classify_failure

            classification = classify_failure(failure_code)
            normalized_category = classification.category.value
            normalized_zone = self._failure_zones.get(normalized_category, "")
            return normalized_category, normalized_zone
        if normalized_category and not normalized_zone:
            normalized_zone = self._failure_zones.get(normalized_category, "")
        return normalized_category, normalized_zone

    @staticmethod
    def _route_health_observation_column(failure_category: str) -> str:
        normalized_category = (failure_category or "").strip()
        if normalized_category in _ROUTE_HEALTH_CONFIG_FAILURE_CATEGORIES:
            return "observed_config_failure_count"
        if normalized_category in _ROUTE_HEALTH_EXTERNAL_FAILURE_CATEGORIES:
            return "observed_external_failure_count"
        return "observed_execution_failure_count"

    @staticmethod
    def _failure_counts_against_route_health(failure_category: str) -> bool:
        normalized_category = (failure_category or "").strip()
        if not normalized_category:
            return True
        return normalized_category not in (
            _ROUTE_HEALTH_EXTERNAL_FAILURE_CATEGORIES
            | _ROUTE_HEALTH_CONFIG_FAILURE_CATEGORIES
        )

    @classmethod
    def _load_route_policy_for_task_type(
        cls,
        conn: SyncPostgresConnection,
        task_type: str,
    ) -> TaskRoutePolicy:
        return cls._load_route_policy_row(conn, task_type=task_type)

    @staticmethod
    def _load_route_policy_row(
        conn: SyncPostgresConnection,
        task_type: str | None = None,
    ) -> TaskRoutePolicy:
        _sql = "SELECT * FROM route_policy_registry WHERE route_policy_key = $1 LIMIT 1"
        policy_key = f"task_type_router.{task_type}" if task_type else "task_type_router.default"
        try:
            rows = conn.execute(_sql, policy_key)
            if not rows and task_type:
                rows = conn.execute(_sql, "task_type_router.default")
        except Exception as exc:
            raise TaskRouteAuthorityError(
                "route_policy_registry authority is required for task routing",
            ) from exc
        if not rows:
            raise TaskRouteAuthorityError(
                "route_policy_registry did not return task_type_router.default",
            )
        row = dict(rows[0])
        internal_failure_penalties = _coerce_json_object(row.get("internal_failure_penalties"), field_name="route_policy_registry.internal_failure_penalties")
        review_severity_penalties = _coerce_json_object(row.get("review_severity_penalties"), field_name="route_policy_registry.review_severity_penalties")
        _float_fields = (
            "task_rank_weight", "route_health_weight", "cost_weight", "benchmark_weight",
            "prefer_cost_task_rank_weight", "prefer_cost_route_health_weight",
            "prefer_cost_cost_weight", "prefer_cost_benchmark_weight",
            "claim_route_health_weight", "claim_rank_weight", "claim_load_weight",
            "claim_internal_failure_penalty_step", "claim_priority_penalty_step",
            "neutral_benchmark_score", "mixed_benchmark_score", "neutral_route_health",
            "min_route_health", "max_route_health", "success_health_bump",
            "review_success_bump", "consecutive_failure_penalty_step", "consecutive_failure_penalty_cap",
        )
        return TaskRoutePolicy(
            **{f: float(row[f]) for f in _float_fields},
            internal_failure_penalties=dict(internal_failure_penalties),
            review_severity_penalties=dict(review_severity_penalties),
        )

    @staticmethod
    def _load_task_route_profiles(conn: SyncPostgresConnection) -> dict[str, TaskTypeRouteProfile]:
        try:
            rows = conn.execute(
                "SELECT * FROM task_type_route_profiles",
            )
        except Exception as exc:
            raise TaskRouteAuthorityError("task_type_route_profiles authority is required for broad task routing") from exc

        def _parse_profile(row: dict[str, Any]) -> TaskTypeRouteProfile:
            p = "task_type_route_profiles"
            affinity_labels_obj = _coerce_json_object(row.get("affinity_labels"), field_name=f"{p}.affinity_labels")
            task_type = _normalize_auto_route_key(str(row["task_type"]))
            return TaskTypeRouteProfile(
                task_type=task_type,
                affinity_labels={
                    b: _json_text_values(affinity_labels_obj.get(b, []), field_name=f"{p}.affinity_labels.{b}")
                    for b in ("primary", "secondary", "specialized", "fallback", "avoid")
                },
                affinity_weights=_json_float_map(row.get("affinity_weights"), field_name=f"{p}.affinity_weights"),
                task_rank_weights=_json_float_map(row.get("task_rank_weights"), field_name=f"{p}.task_rank_weights"),
                benchmark_metric_weights=_json_float_map(row.get("benchmark_metric_weights"), field_name=f"{p}.benchmark_metric_weights"),
                route_tier_preferences=_json_text_values(row.get("route_tier_preferences"), field_name=f"{p}.route_tier_preferences"),
                latency_class_preferences=_json_text_values(row.get("latency_class_preferences"), field_name=f"{p}.latency_class_preferences"),
                allow_unclassified_candidates=bool(row.get("allow_unclassified_candidates")),
                rationale=str(row.get("rationale") or ""),
            )

        return {_normalize_auto_route_key(str(row["task_type"])): _parse_profile(dict(row)) for row in rows or []}

    @staticmethod
    def _load_benchmark_metric_registry(conn: SyncPostgresConnection) -> dict[str, BenchmarkMetricDefinition]:
        try:
            rows = conn.execute("SELECT metric_key, higher_is_better, enabled FROM market_benchmark_metric_registry WHERE enabled = true")
        except Exception as exc:
            raise TaskRouteAuthorityError("market_benchmark_metric_registry authority is required for task routing") from exc
        return {
            str(row["metric_key"]): BenchmarkMetricDefinition(
                metric_key=str(row["metric_key"]),
                higher_is_better=bool(row["higher_is_better"]),
                enabled=bool(row.get("enabled", True)),
            )
            for row in rows or []
        }

    def resolve(
        self,
        agent_slug: str,
        task_type: Optional[str] = None,
        prefer_cost: bool = False,
        runtime_profile_ref: str | None = None,
    ) -> TaskRouteDecision:
        if agent_slug.startswith("auto/"):
            return self._resolve_auto(
                _resolve_semantic_auto_route(_normalize_auto_route_key(agent_slug)),
                prefer_cost,
                runtime_profile_ref=runtime_profile_ref,
            )

        parts = agent_slug.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid agent slug: '{agent_slug}'")
        provider, model = parts
        if task_type:
            self._check_permission(task_type, model, provider)
        provider_policy_id: str | None = None
        candidate_ref = ""
        candidate_adapter_type: str | None = None
        candidate_transport_type = ""
        candidate_host_provider_slug = ""
        candidate_variant = ""
        candidate_effort_slug = ""
        candidate_strict_adapter = False
        candidate_cost_per_m_tokens = 0.0
        for candidate in self._load_catalog_candidates(runtime_profile_ref=runtime_profile_ref):
            if (
                str(candidate.get("provider_slug") or "") == provider
                and str(candidate.get("model_slug") or "") == model
            ):
                candidate_ref = str(candidate.get("candidate_ref") or "").strip()
                provider_policy_id = str(candidate.get("provider_policy_id") or "").strip() or None
                candidate_transport_type = _normalize_transport_type(candidate.get("transport_type"))
                candidate_adapter_type = _candidate_adapter_type(
                    candidate,
                    default_adapter=self._default_adapter_for_provider(provider),
                )
                candidate_host_provider_slug = str(candidate.get("host_provider_slug") or "").strip()
                candidate_variant = str(candidate.get("variant") or "").strip()
                candidate_effort_slug = str(candidate.get("effort_slug") or "").strip()
                candidate_strict_adapter = bool(candidate_transport_type)
                candidate_cost_per_m_tokens = float(candidate.get("cost_per_m_tokens") or 0.0)
                break
        # BUG-6B34915A: explicit routes must consult the same budget-window
        # authority as auto routes. The snapshot carries by-provider-ref
        # lookups so we don't need provider_policy_id when it isn't known.
        budget_authority = self._load_budget_authority_snapshot()
        try:
            economics = _resolve_route_economics(
                provider_slug=provider,
                adapter_type=candidate_adapter_type,
                provider_policy_id=provider_policy_id,
                raw_cost_per_m_tokens=candidate_cost_per_m_tokens,
                budget_authority=budget_authority,
                default_adapter=candidate_adapter_type or self._default_adapter_for_provider(provider),
                strict_adapter=candidate_strict_adapter,
            )
        except RuntimeError as exc:
            message = str(exc)
            registry_blocked = (
                "provider execution registry has no authoritative provider profiles" in message
                or "has no supported adapter for routing economics" in message
            )
            if (
                candidate_adapter_type
                and not budget_authority.reachable
                and registry_blocked
                and not _provider_registry_authority_available()
            ):
                economics = _explicit_route_transport_unknown_economics(
                    adapter_type=candidate_adapter_type,
                    raw_cost_per_m_tokens=candidate_cost_per_m_tokens,
                    budget_authority=budget_authority,
                )
            else:
                raise
        resolved_adapter_type = str(
            economics.get("adapter_type") or self._default_adapter_for_provider(provider)
        )
        resolved_transport_type = (
            candidate_transport_type
            or _transport_type_for_adapter(resolved_adapter_type)
            or "API"
        )
        if (
            candidate_transport_type
            and _transport_type_for_adapter(resolved_adapter_type) != candidate_transport_type
        ):
            raise TaskRouteAuthorityError(
                (
                    f"candidate transport {candidate_transport_type} for {provider}/{model} "
                    f"does not match resolved adapter {resolved_adapter_type}"
                ),
                reason_code="provider_authority.transport_adapter_mismatch",
                details={
                    "provider_slug": provider,
                    "model_slug": model,
                    "transport_type": candidate_transport_type,
                    "adapter_type": resolved_adapter_type,
                },
            )
        reasoning_effort = self._reasoning_effort_fields(
            task_type=task_type or "build",
            provider_slug=provider,
            model_slug=model,
            adapter_type=resolved_adapter_type,
        )
        return TaskRouteDecision(
            task_type=task_type or "build",
            model_slug=model,
            provider_slug=provider,
            rank=0,
            benchmark_score=0,
            benchmark_name="",
            cost_per_m_tokens=0,
            rationale="explicit slug",
            was_auto=False,
            candidate_ref=candidate_ref,
            adapter_type=resolved_adapter_type,
            transport_type=resolved_transport_type,
            host_provider_slug=candidate_host_provider_slug,
            variant=candidate_variant,
            effort_slug=candidate_effort_slug,
            billing_mode=str(economics.get("billing_mode") or "metered_api"),
            budget_bucket=str(economics.get("budget_bucket") or "unknown"),
            effective_marginal_cost=_row_effective_marginal_cost(economics),
            spend_pressure=str(economics.get("spend_pressure") or "unknown"),
            budget_status=str(economics.get("budget_status") or ""),
            prefer_prepaid=bool(economics.get("prefer_prepaid", False)),
            allow_payg_fallback=bool(economics.get("allow_payg_fallback", False)),
            budget_authority_unreachable=bool(
                economics.get("budget_authority_unreachable", False)
            ),
            reasoning_effort_slug=str(reasoning_effort.get("reasoning_effort_slug") or ""),
            reasoning_provider_payload=dict(
                reasoning_effort.get("reasoning_provider_payload") or {}
            ),
        )

    def resolve_explicit_eligibility(
        self,
        agent_slug: str,
        *,
        task_type: str | None = None,
        as_of: datetime | None = None,
    ) -> TaskRouteEligibilityDecision | None:
        """Return the active route-eligibility decision for an explicit slug.

        This is the policy answer for direct ``provider/model`` jobs that bypass
        ``auto/`` routing. A rejected decision means execution should fail closed
        before any transport-specific readiness or subprocess work begins.
        """
        parts = agent_slug.split("/", 1)
        if len(parts) != 2:
            raise ValueError(f"Invalid agent slug: '{agent_slug}'")
        provider, model = parts
        normalized_task_type = (
            _resolve_semantic_auto_route(_normalize_auto_route_key(task_type))
            if task_type
            else ""
        )
        decisions = self._load_route_eligibility(
            normalized_task_type,
            provider_slugs=(provider,),
            model_slugs=(model,),
            as_of=as_of or self._now_factory(),
        )
        if not decisions:
            return None
        return self._matching_route_eligibility(
            normalized_task_type,
            provider_slug=provider,
            model_slug=model,
            decisions=decisions,
        )

    def _resolve_auto(
        self,
        task_type: str,
        prefer_cost: bool,
        *,
        runtime_profile_ref: str | None = None,
    ) -> TaskRouteDecision:
        chain = self._resolve_auto_chain(
            task_type,
            prefer_cost,
            runtime_profile_ref=runtime_profile_ref,
        )
        return chain[0]

    def _resolve_auto_chain(
        self,
        task_type: str,
        prefer_cost: bool,
        *,
        tier_override: str | None = None,
        runtime_profile_ref: str | None = None,
    ) -> list[TaskRouteDecision]:
        task_type = _resolve_semantic_auto_route(task_type)
        if task_type in _ROUTE_TIER_BUCKETS:
            return self._resolve_profile_chain(profile_column="route_tier", profile_rank_column="route_tier_rank", profile_value=task_type, prefer_cost=prefer_cost, runtime_profile_ref=runtime_profile_ref)
        if task_type in _LATENCY_CLASS_BUCKETS:
            return self._resolve_profile_chain(profile_column="latency_class", profile_rank_column="latency_rank", profile_value=task_type, prefer_cost=prefer_cost, runtime_profile_ref=runtime_profile_ref)
        return self._resolve_chain(task_type, prefer_cost, tier_override=tier_override, runtime_profile_ref=runtime_profile_ref)

    def _load_active_catalog_candidates(self) -> list[dict[str, Any]]:
        rows = self._conn.execute(
            """SELECT DISTINCT ON (provider_slug, model_slug, transport_type)
                      candidate_ref,
                      provider_slug,
                      model_slug,
                      transport_type,
                      host_provider_slug,
                      variant,
                      effort_slug,
                      priority,
                      route_tier,
                      route_tier_rank,
                      latency_class,
                      latency_rank,
                      capability_tags,
                      task_affinities,
                      benchmark_profile
               FROM provider_model_candidates
               WHERE status = 'active'
               ORDER BY provider_slug, model_slug, transport_type, priority ASC, created_at DESC""",
        )
        candidates: list[dict[str, Any]] = []
        for row in rows or []:
            capability_tags = _parse_json_field(row.get("capability_tags"), [])
            task_affinities = _parse_json_field(row.get("task_affinities"), {})
            benchmark_profile = _parse_json_field(row.get("benchmark_profile"), {})
            candidates.append({
                "candidate_ref": str(row.get("candidate_ref") or ""),
                "provider_slug": str(row["provider_slug"]),
                "model_slug": str(row["model_slug"]),
                "transport_type": _normalize_transport_type(row.get("transport_type")),
                "host_provider_slug": str(row.get("host_provider_slug") or ""),
                "variant": str(row.get("variant") or ""),
                "effort_slug": str(row.get("effort_slug") or ""),
                "priority": int(row.get("priority") or 999),
                "provider_policy_id": str(row.get("provider_policy_id") or "").strip() or None,
                "adapter_type": str(row.get("adapter_type") or "").strip() or None,
                "cost_per_m_tokens": float(row.get("cost_per_m_tokens") or 0.0),
                "route_tier": _normalize_auto_route_key(str(row.get("route_tier") or "")),
                "route_tier_rank": int(row.get("route_tier_rank") or 99),
                "latency_class": _normalize_auto_route_key(str(row.get("latency_class") or "")),
                "latency_rank": int(row.get("latency_rank") or 99),
                "capability_tags": tuple(str(t).strip().lower() for t in capability_tags if isinstance(t, str) and str(t).strip()),
                "task_affinities": task_affinities if isinstance(task_affinities, dict) else {},
                "benchmark_profile": benchmark_profile if isinstance(benchmark_profile, dict) else {},
            })
        return candidates

    @staticmethod
    def _admitted_candidate_to_catalog_row(candidate: RuntimeProfileAdmittedCandidate) -> dict[str, Any]:
        return {
            "candidate_ref": candidate.candidate_ref, "provider_ref": candidate.provider_ref,
            "provider_name": candidate.provider_name, "provider_slug": candidate.provider_slug,
            "provider_policy_id": candidate.provider_policy_id,
            "model_slug": candidate.model_slug,
            "transport_type": _normalize_transport_type(candidate.transport_type),
            "host_provider_slug": candidate.host_provider_slug,
            "variant": candidate.variant,
            "effort_slug": candidate.effort_slug,
            "priority": candidate.priority,
            "balance_weight": candidate.balance_weight,
            "route_tier": _normalize_auto_route_key(candidate.route_tier or ""),
            "route_tier_rank": int(candidate.route_tier_rank or 99),
            "latency_class": _normalize_auto_route_key(candidate.latency_class or ""),
            "latency_rank": int(candidate.latency_rank or 99),
            "capability_tags": tuple(candidate.capability_tags),
            "task_affinities": dict(candidate.task_affinities) if isinstance(candidate.task_affinities, dict) else {},
            "benchmark_profile": dict(candidate.benchmark_profile) if isinstance(candidate.benchmark_profile, dict) else {},
        }

    def _load_catalog_candidates(
        self,
        *,
        runtime_profile_ref: str | None = None,
    ) -> list[dict[str, Any]]:
        if not runtime_profile_ref:
            return self._load_active_catalog_candidates()

        cached = self._runtime_profile_candidate_cache.get(runtime_profile_ref)
        if cached is None:
            admitted_candidates = load_admitted_runtime_profile_candidates(
                self._conn,
                runtime_profile_ref=runtime_profile_ref,
                as_of=self._now_factory(),
            )
            cached = tuple(
                self._admitted_candidate_to_catalog_row(candidate)
                for candidate in admitted_candidates
            )
            self._runtime_profile_candidate_cache[runtime_profile_ref] = cached
        return [dict(candidate) for candidate in cached]

    def _effective_runtime_profile_ref(
        self,
        runtime_profile_ref: str | None,
    ) -> str | None:
        normalized = str(runtime_profile_ref or "").strip()
        if normalized:
            return normalized
        if not self._use_default_runtime_profile:
            return None
        try:
            from registry.native_runtime_profile_sync import (
                default_native_runtime_profile_ref,
            )

            return str(default_native_runtime_profile_ref(self._conn) or "").strip() or None
        except Exception:
            return None

    def _apply_effective_provider_job_catalog_filter(
        self,
        task_type: str,
        rows: list[dict[str, Any]],
        *,
        runtime_profile_ref: str | None,
    ) -> list[dict[str, Any]]:
        if not rows:
            return rows
        effective_runtime_profile_ref = self._effective_runtime_profile_ref(runtime_profile_ref)
        if not effective_runtime_profile_ref:
            return rows

        from storage.postgres import PostgresProviderControlPlaneRepository

        catalog_rows = PostgresProviderControlPlaneRepository(
            self._conn
        ).list_provider_control_plane_rows(
            runtime_profile_ref=effective_runtime_profile_ref,
            job_type=task_type,
        )
        allowed_slugs = {
            f"{row.provider_slug}/{row.model_slug}"
            for row in catalog_rows
            if row.is_runnable
        }
        allowed_candidate_refs = {
            str(getattr(row, "candidate_ref", "") or "").strip()
            for row in catalog_rows
            if row.is_runnable and str(getattr(row, "candidate_ref", "") or "").strip()
        }
        allowed_exact = {
            (
                f"{row.provider_slug}/{row.model_slug}",
                _normalize_transport_type(getattr(row, "transport_type", "")),
            )
            for row in catalog_rows
            if row.is_runnable and _normalize_transport_type(getattr(row, "transport_type", ""))
        }
        if not allowed_slugs:
            raise provider_authority_fail(
                "provider_authority.effective_catalog_empty",
                "effective provider job catalog returned no runnable candidates",
                runtime_profile_ref=effective_runtime_profile_ref,
                task_type=task_type,
            )
        def _row_allowed(row: dict[str, Any]) -> bool:
            slug = f"{row.get('provider_slug')}/{row.get('model_slug')}"
            row_transport = _normalize_transport_type(row.get("transport_type"))
            row_candidate_ref = str(row.get("candidate_ref") or "").strip()
            if row_candidate_ref and allowed_candidate_refs:
                if row_candidate_ref not in allowed_candidate_refs:
                    return False
                if row_transport and allowed_exact:
                    return (slug, row_transport) in allowed_exact
                return True
            if row_transport and allowed_exact:
                return (slug, row_transport) in allowed_exact
            return slug in allowed_slugs

        filtered = [row for row in rows if _row_allowed(row)]
        dropped = len(rows) - len(filtered)
        if dropped:
            logger.info(
                "effective provider job catalog narrowed auto/%s chain: "
                "dropped %d disabled/unadmitted candidate(s) for runtime_profile_ref=%s",
                task_type,
                dropped,
                effective_runtime_profile_ref,
            )
        return filtered

    def _load_task_route_rows(self, task_type: str) -> list[dict[str, Any]]:
        rows = self._routing_repository.load_routes_for_task(task_type=task_type)
        if not rows:
            rows = self._routing_repository.load_routes_for_task(task_type=f"auto/{task_type}")
        return [dict(row) for row in rows]

    def _provider_usage_unavailable_slugs(self, provider_slugs: set[str]) -> set[str]:
        if not provider_slugs:
            return set()
        try:
            rows = self._conn.execute(
                """
                SELECT DISTINCT ON (subject_id)
                       subject_id,
                       status
                  FROM heartbeat_probe_snapshots
                 WHERE probe_kind = 'provider_usage'
                   AND subject_id = ANY($1::text[])
                   AND captured_at >= now() - interval '24 hours'
                 ORDER BY subject_id, captured_at DESC
                """,
                sorted(provider_slugs),
            )
        except Exception as exc:
            raise provider_authority_fail(
                "provider_authority.provider_usage_unavailable",
                f"provider usage availability unavailable during routing: {exc}",
                provider_slugs=sorted(provider_slugs),
            ) from exc
        unavailable: set[str] = set()
        for row in rows or []:
            provider_slug = str(row.get("subject_id") or "").strip().lower()
            status = str(row.get("status") or "").strip().lower()
            if provider_slug and status in {"degraded", "failed"}:
                unavailable.add(provider_slug)
        return unavailable

    def _apply_provider_usage_availability_filter(
        self,
        task_type: str,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        unavailable = self._provider_usage_unavailable_slugs(
            {str(row.get("provider_slug") or "").strip().lower() for row in rows}
        )
        if not unavailable:
            return rows
        filtered = [
            row
            for row in rows
            if str(row.get("provider_slug") or "").strip().lower() not in unavailable
        ]
        dropped = len(rows) - len(filtered)
        if dropped:
            logger.warning(
                "auto/%s routing dropped %d candidate(s) due to provider_usage availability: %s",
                task_type,
                dropped,
                ", ".join(sorted(unavailable)),
            )
        return filtered


    def _load_budget_authority_snapshot(self) -> BudgetAuthoritySnapshot:
        return _load_budget_authority_snapshot(self._conn)

    def _build_profile_task_rows(
        self,
        task_type: str,
        profile: TaskTypeRouteProfile,
        *,
        runtime_profile_ref: str | None = None,
    ) -> list[dict[str, Any]]:
        route_rows = self._load_task_route_rows(task_type)
        state_by_candidate_ref = {
            str(row.get("candidate_ref") or "").strip(): row
            for row in route_rows
            if str(row.get("candidate_ref") or "").strip()
        }
        state_by_identity = {_route_identity_tuple(row): row for row in route_rows}
        state_by_slug = {
            (str(row["provider_slug"]), str(row["model_slug"])): row
            for row in route_rows
        }
        catalog_candidates = list(self._load_catalog_candidates(runtime_profile_ref=runtime_profile_ref))
        budget_authority = self._load_budget_authority_snapshot()
        built_rows: list[dict[str, Any]] = []
        for candidate in catalog_candidates:
            if (
                not _is_deepseek_scoped_for_task_type(task_type)
                and _is_scoped_deepseek_candidate(candidate)
            ):
                logger.debug(
                    "skipping deepseek candidate %s/%s for task_type=%s due scope memo",
                    candidate.get("provider_slug"),
                    candidate.get("model_slug"),
                    task_type,
                )
                continue

            key = (candidate["provider_slug"], candidate["model_slug"])
            candidate_ref = str(candidate.get("candidate_ref") or "").strip()
            state_row = (
                state_by_candidate_ref.get(candidate_ref)
                or state_by_identity.get(_route_identity_tuple(candidate))
                or state_by_slug.get(key)
            )
            explicit_override = state_row if (
                state_row is not None and str(state_row.get("route_source") or "explicit") == "explicit"
            ) else None
            if task_type != "research" and _candidate_is_research_only(
                candidate,
                profile_task_primary=profile.affinity_labels.get("primary", ()),
                profile_task_secondary=profile.affinity_labels.get("secondary", ()),
                profile_task_specialized=profile.affinity_labels.get("specialized", ()),
            ):
                continue
            affinity_bucket = _match_affinity_bucket(candidate, profile)
            avoid_labels = _candidate_avoid_labels(candidate)
            matched_general_label = affinity_bucket in {"primary", "secondary", "specialized"}
            if explicit_override is not None and not bool(explicit_override.get("permitted", True)):
                continue
            if affinity_bucket == "avoid":
                continue
            if task_type in avoid_labels:
                continue
            if "general-routing" in avoid_labels and not matched_general_label:
                continue
            if affinity_bucket == "unclassified" and not profile.allow_unclassified_candidates:
                continue

            task_rank_score = _profile_task_rank_score(candidate, profile, affinity_bucket=affinity_bucket)
            benchmark_score = float(explicit_override.get("benchmark_score") or 0.0) if explicit_override else 0.0
            benchmark_name = str(explicit_override.get("benchmark_name") or "") if explicit_override else ""
            row_temperature = state_row.get("temperature") if state_row is not None else None
            row_max_tokens = state_row.get("max_tokens") if state_row is not None else None
            raw_cost_per_m_tokens = _base_cost_per_m_tokens(candidate, state_row=state_row)
            provider_slug = str(candidate["provider_slug"])
            default_adapter = self._default_adapter_for_provider(provider_slug)
            candidate_adapter = _candidate_adapter_type(
                candidate,
                default_adapter=default_adapter,
            )
            candidate_transport = _normalize_transport_type(candidate.get("transport_type"))
            try:
                economics = _resolve_route_economics(
                    provider_slug=provider_slug,
                    adapter_type=candidate_adapter,
                    provider_policy_id=str(candidate["provider_policy_id"]) if candidate.get("provider_policy_id") else None,
                    raw_cost_per_m_tokens=raw_cost_per_m_tokens,
                    budget_authority=budget_authority,
                    default_adapter=candidate_adapter or default_adapter,
                    strict_adapter=bool(candidate_transport),
                )
            except RuntimeError as exc:
                logger.debug("skipping candidate %s/%s: %s", provider_slug, candidate.get("model_slug"), exc)
                continue
            resolved_adapter = str(economics.get("adapter_type") or candidate_adapter or default_adapter).strip().lower()
            row_transport_type = candidate_transport or _transport_type_for_adapter(resolved_adapter) or "API"
            if (
                candidate_transport
                and _transport_type_for_adapter(resolved_adapter) != candidate_transport
            ):
                logger.debug(
                    "skipping candidate %s/%s: transport_type=%s mismatches adapter_type=%s",
                    provider_slug,
                    candidate.get("model_slug"),
                    candidate_transport,
                    resolved_adapter,
                )
                continue
            rationale = str(explicit_override.get("rationale") or "") if explicit_override is not None else (
                f"auto-derived: {affinity_bucket} affinity for {task_type}; "
                f"route_tier={candidate.get('route_tier') or 'unknown'}; "
                f"latency={candidate.get('latency_class') or 'unknown'}"
            )
            built_rows.append({
                "task_type": task_type,
                "candidate_ref": str(candidate.get("candidate_ref") or ""),
                "provider_slug": candidate["provider_slug"],
                "host_provider_slug": str(candidate.get("host_provider_slug") or ""),
                "model_slug": candidate["model_slug"],
                "transport_type": row_transport_type,
                "variant": str(candidate.get("variant") or ""),
                "effort_slug": str(candidate.get("effort_slug") or ""),
                "permitted": True,
                "rank": int(explicit_override.get("rank") or 99) if explicit_override is not None else _derived_rank_from_score(task_rank_score),
                "benchmark_score": benchmark_score,
                "benchmark_name": benchmark_name,
                "cost_per_m_tokens": raw_cost_per_m_tokens,
                "temperature": float(row_temperature) if row_temperature is not None else None,
                "max_tokens": int(row_max_tokens) if row_max_tokens is not None else None,
                "rationale": rationale,
                "route_tier": candidate.get("route_tier"),
                "route_tier_rank": candidate.get("route_tier_rank"),
                "latency_class": candidate.get("latency_class"),
                "latency_rank": candidate.get("latency_rank"),
                "route_source": "explicit" if explicit_override is not None else "derived",
                "route_health_score": state_row.get("route_health_score") if state_row is not None else None,
                "observed_completed_count": _state_int(state_row, "observed_completed_count"),
                "observed_execution_failure_count": _state_int(state_row, "observed_execution_failure_count"),
                "observed_external_failure_count": _state_int(state_row, "observed_external_failure_count"),
                "observed_config_failure_count": _state_int(state_row, "observed_config_failure_count"),
                "observed_downstream_failure_count": _state_int(state_row, "observed_downstream_failure_count"),
                "observed_downstream_bug_count": _state_int(state_row, "observed_downstream_bug_count"),
                "consecutive_internal_failures": _state_int(state_row, "consecutive_internal_failures"),
                "last_failure_category": _state_str(state_row, "last_failure_category"),
                "last_failure_zone": _state_str(state_row, "last_failure_zone"),
                "_task_rank_score": task_rank_score,
                "_affinity_bucket": affinity_bucket,
                "_common_metrics": _candidate_common_metrics(candidate),
                **economics,
            })
        _apply_profile_benchmark_scores(task_type, profile, built_rows, self._benchmark_metrics)
        return built_rows

    def _materialize_derived_rows(self, task_type: str, rows: list[dict[str, Any]]) -> None:
        for row in rows:
            if str(row.get("route_source") or "derived") != "derived":
                continue
            self._routing_repository.upsert_derived_route(
                task_type=task_type,
                model_slug=str(row["model_slug"]),
                provider_slug=str(row["provider_slug"]),
                transport_type=str(row.get("transport_type") or "API"),
                candidate_ref=str(row.get("candidate_ref") or "") or None,
                host_provider_slug=str(row.get("host_provider_slug") or ""),
                variant=str(row.get("variant") or ""),
                effort_slug=str(row.get("effort_slug") or ""),
                permitted=bool(row.get("permitted", True)),
                rank=int(row.get("effective_rank") or row.get("rank") or 99),
                benchmark_score=float(row.get("benchmark_score") or 0.0),
                benchmark_name=str(row.get("benchmark_name") or ""),
                cost_per_m_tokens=float(row.get("cost_per_m_tokens") or 0.0),
                rationale=str(row.get("rationale") or ""),
                route_tier=str(row.get("route_tier") or "") or None,
                route_tier_rank=int(row.get("route_tier_rank") or 99),
                latency_class=str(row.get("latency_class") or "") or None,
                latency_rank=int(row.get("latency_rank") or 99),
                route_health_score=float(row["route_health_score"]) if row.get("route_health_score") is not None else self._policy.neutral_route_health,
                observed_completed_count=int(row.get("observed_completed_count") or 0),
                observed_execution_failure_count=int(row.get("observed_execution_failure_count") or 0),
                observed_external_failure_count=int(row.get("observed_external_failure_count") or 0),
                observed_config_failure_count=int(row.get("observed_config_failure_count") or 0),
                observed_downstream_failure_count=int(row.get("observed_downstream_failure_count") or 0),
                observed_downstream_bug_count=int(row.get("observed_downstream_bug_count") or 0),
                consecutive_internal_failures=int(row.get("consecutive_internal_failures") or 0),
                last_failure_category=str(row.get("last_failure_category") or ""),
                last_failure_zone=str(row.get("last_failure_zone") or ""),
            )

    def _persist_explicit_benchmark_scores(
        self, task_type: str, rows: list[dict[str, Any]]
    ) -> None:
        """Write back in-memory benchmark scores to explicit task_type_routing rows.

        _apply_profile_benchmark_scores computes scores from market data in-memory
        but only _materialize_derived_rows persists them. Explicit routes (those
        already in task_type_routing) would keep stale 0.0 scores in the DB without
        this write-back — confusing for observability and operator tooling.
        """
        for row in rows:
            if str(row.get("route_source") or "derived") == "derived":
                continue
            score = float(row.get("benchmark_score") or 0.0)
            name = str(row.get("benchmark_name") or "")
            if not name:
                continue
            self._routing_repository.update_explicit_benchmark_score(
                task_type=task_type,
                provider_slug=str(row["provider_slug"]),
                model_slug=str(row["model_slug"]),
                candidate_ref=str(row.get("candidate_ref") or "") or None,
                benchmark_score=score,
                benchmark_name=name,
            )

    def _resolve_chain(
        self,
        task_type: str,
        prefer_cost: bool = False,
        *,
        tier_override: str | None = None,
        runtime_profile_ref: str | None = None,
    ) -> list[TaskRouteDecision]:
        profile = self._task_profiles.get(task_type)
        if profile is None:
            raise TaskRouteAuthorityError(
                f"task_type_route_profiles authority is missing task_type '{task_type}'",
            )
        effective_runtime_profile_ref = self._effective_runtime_profile_ref(runtime_profile_ref)

        rows = self._build_profile_task_rows(
            task_type,
            profile,
            runtime_profile_ref=effective_runtime_profile_ref,
        )
        rows = self._apply_route_eligibility(task_type, rows, as_of=self._now_factory())
        rows = self._apply_effective_provider_job_catalog_filter(
            task_type,
            rows,
            runtime_profile_ref=effective_runtime_profile_ref,
        )
        # BUG-DD1D48B1: drop candidates whose default adapter transport is
        # denied by provider_transport_admissions (e.g. anthropic.llm_task
        # under the CLI-only standing order). Keeps auto/review from being
        # routed to Anthropic HTTP and collapsing on 401.
        rows = self._apply_provider_transport_admission_filter(task_type, rows)
        rows = self._apply_provider_usage_availability_filter(task_type, rows)
        if not rows:
            raise ValueError(f"No permitted models for task type '{task_type}'")
        policy = self._policy_for(task_type)
        rows = _rerank_rows(rows, prefer_cost, policy)

        # Tier override: filter to requested tier (e.g. auto/build/medium)
        if tier_override:
            tier_rows = [r for r in rows if str(r.get("route_tier") or "").lower() == tier_override]
            if tier_rows:
                rows = tier_rows

        for index, row in enumerate(rows, start=1):
            row["effective_rank"] = index
        self._materialize_derived_rows(task_type, rows)
        self._persist_explicit_benchmark_scores(task_type, rows)
        return self._decision_chain(task_type, rows)

    def _resolve_profile_chain(
        self,
        *,
        profile_column: str,
        profile_rank_column: str,
        profile_value: str,
        prefer_cost: bool,
        runtime_profile_ref: str | None = None,
    ) -> list[TaskRouteDecision]:
        profile = self._task_profiles.get(profile_value)
        rows: list[dict[str, Any]] = []
        effective_runtime_profile_ref = self._effective_runtime_profile_ref(runtime_profile_ref)
        catalog_candidates = list(
            self._load_catalog_candidates(runtime_profile_ref=effective_runtime_profile_ref)
        )
        budget_authority = self._load_budget_authority_snapshot()
        for candidate in catalog_candidates:
            if _normalize_auto_route_key(str(candidate.get(profile_column) or "")) != profile_value:
                continue
            if profile_value != "research" and _candidate_is_research_only(
                candidate,
                profile_task_primary=profile.affinity_labels.get("primary", ()) if profile else None,
                profile_task_secondary=profile.affinity_labels.get("secondary", ()) if profile else None,
                profile_task_specialized=profile.affinity_labels.get("specialized", ())
                if profile
                else None,
            ):
                continue
            if (
                not _is_deepseek_scoped_for_task_type(profile_value)
                and _is_scoped_deepseek_candidate(candidate)
            ):
                logger.debug(
                    "skipping deepseek candidate %s/%s for auto profile value=%s due scope memo",
                    candidate.get("provider_slug"),
                    candidate.get("model_slug"),
                    profile_value,
                )
                continue
            if "general-routing" in _candidate_avoid_labels(candidate):
                continue
            profile_rank = int(candidate.get(profile_rank_column) or 99)
            raw_cost_per_m_tokens = _base_cost_per_m_tokens(candidate, state_row=None)
            provider_slug = str(candidate["provider_slug"])
            default_adapter = self._default_adapter_for_provider(provider_slug)
            candidate_adapter = _candidate_adapter_type(
                candidate,
                default_adapter=default_adapter,
            )
            candidate_transport = _normalize_transport_type(candidate.get("transport_type"))
            try:
                economics = _resolve_route_economics(
                    provider_slug=provider_slug,
                    adapter_type=candidate_adapter,
                    provider_policy_id=str(candidate["provider_policy_id"]) if candidate.get("provider_policy_id") else None,
                    raw_cost_per_m_tokens=raw_cost_per_m_tokens,
                    budget_authority=budget_authority,
                    default_adapter=candidate_adapter or default_adapter,
                    strict_adapter=bool(candidate_transport),
                )
            except RuntimeError as exc:
                logger.debug("skipping candidate %s/%s: %s", provider_slug, candidate.get("model_slug"), exc)
                continue
            resolved_adapter = str(economics.get("adapter_type") or candidate_adapter or default_adapter).strip().lower()
            row_transport_type = candidate_transport or _transport_type_for_adapter(resolved_adapter) or "API"
            if (
                candidate_transport
                and _transport_type_for_adapter(resolved_adapter) != candidate_transport
            ):
                logger.debug(
                    "skipping candidate %s/%s: transport_type=%s mismatches adapter_type=%s",
                    provider_slug,
                    candidate.get("model_slug"),
                    candidate_transport,
                    resolved_adapter,
                )
                continue
            rows.append({
                "candidate_ref": str(candidate.get("candidate_ref") or ""),
                "provider_slug": candidate["provider_slug"],
                "host_provider_slug": str(candidate.get("host_provider_slug") or ""),
                "model_slug": candidate["model_slug"],
                "transport_type": row_transport_type,
                "variant": str(candidate.get("variant") or ""),
                "effort_slug": str(candidate.get("effort_slug") or ""),
                "rank": profile_rank,
                "benchmark_score": 0.0,
                "benchmark_name": "",
                "cost_per_m_tokens": raw_cost_per_m_tokens,
                "rationale": (
                    f"catalog {profile_column}={profile_value}; "
                    f"{profile_rank_column}={profile_rank}"
                ),
                "route_source": "derived",
                "consecutive_internal_failures": 0,
                **economics,
            })
        rows = self._apply_route_eligibility(profile_value, rows, as_of=self._now_factory())
        rows = self._apply_effective_provider_job_catalog_filter(
            profile_value,
            rows,
            runtime_profile_ref=effective_runtime_profile_ref,
        )
        # BUG-DD1D48B1: same admission filter as _resolve_chain — auto/*
        # profile resolution is the hot path for auto/review routes, so the
        # filter must live here too, not only on explicit task-type routes.
        rows = self._apply_provider_transport_admission_filter(profile_value, rows)
        rows = self._apply_provider_usage_availability_filter(profile_value, rows)
        if not rows:
            raise ValueError(f"No permitted models for auto/{profile_value}")
        rows = _rerank_rows(rows, prefer_cost, self._policy)
        for index, row in enumerate(rows, start=1):
            row["effective_rank"] = index
        return self._decision_chain(profile_value, rows)

    def _decision_chain(self, task_type: str, rows: list[dict[str, Any]]) -> list[TaskRouteDecision]:
        rows = self._apply_lane_policy(task_type, rows)
        for row in rows:
            adapter_type = str(
                row.get("adapter_type")
                or self._default_adapter_for_provider(str(row.get("provider_slug") or ""))
            )
            row.update(
                self._reasoning_effort_fields(
                    task_type=task_type,
                    provider_slug=str(row.get("provider_slug") or ""),
                    model_slug=str(row.get("model_slug") or ""),
                    adapter_type=adapter_type,
                )
            )
        chain = [
            TaskRouteDecision(
                task_type=task_type,
                model_slug=str(row["model_slug"]),
                provider_slug=str(row["provider_slug"]),
                rank=int(row.get("effective_rank") or row.get("rank") or 99),
                benchmark_score=float(row.get("benchmark_score") or 0.0),
                benchmark_name=str(row.get("benchmark_name") or ""),
                cost_per_m_tokens=float(row.get("cost_per_m_tokens") or 0.0),
                rationale=(
                    f"{str(row.get('rationale') or '')}; {_economic_rationale(row)}".strip("; ")
                    if _economic_rationale(row)
                    else str(row.get("rationale") or "")
                ),
                was_auto=True,
                candidate_ref=str(row.get("candidate_ref") or ""),
                adapter_type=str(
                    row.get("adapter_type")
                    or self._default_adapter_for_provider(str(row.get("provider_slug") or ""))
                ),
                transport_type=str(
                    row.get("transport_type")
                    or _transport_type_for_adapter(row.get("adapter_type"))
                    or "API"
                ),
                host_provider_slug=str(row.get("host_provider_slug") or ""),
                variant=str(row.get("variant") or ""),
                effort_slug=str(row.get("effort_slug") or ""),
                billing_mode=str(row.get("billing_mode") or "metered_api"),
                budget_bucket=str(row.get("budget_bucket") or "unknown"),
                effective_marginal_cost=_row_effective_marginal_cost(row),
                spend_pressure=str(row.get("spend_pressure") or "unknown"),
                budget_status=str(row.get("budget_status") or ""),
                prefer_prepaid=bool(row.get("prefer_prepaid", False)),
                allow_payg_fallback=bool(row.get("allow_payg_fallback", False)),
                budget_authority_unreachable=bool(
                    row.get("budget_authority_unreachable", False)
                ),
                budget_window_data_quality_error=bool(
                    row.get("budget_window_data_quality_error", False)
                ),
                temperature=(
                    float(row["temperature"])
                    if row.get("temperature") is not None
                    else None
                ),
                max_tokens=(
                    int(row["max_tokens"])
                    if row.get("max_tokens") is not None
                    else None
                ),
                reasoning_effort_slug=str(row.get("reasoning_effort_slug") or ""),
                reasoning_provider_payload=dict(
                    row.get("reasoning_provider_payload") or {}
                ),
            )
            for row in rows
        ]
        logger.info(
            "auto/%s → %s/%s (rank=%d, %s) [+%d failover candidates]",
            task_type,
            chain[0].provider_slug,
            chain[0].model_slug,
            chain[0].rank,
            chain[0].rationale,
            len(chain) - 1,
        )
        return chain

    def _apply_lane_policy(
        self,
        task_type: str,
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Narrow rows to adapter types admitted by provider lane policy.

        Gate 1 of the lane-control hierarchy: a provider's policy row
        declares which adapter types may be admitted. Rows whose resolved
        adapter_type is not in the policy's allowed set are dropped here.
        """
        if not rows:
            return rows
        from runtime.lane_policy import admit_adapter_type, load_provider_lane_policies
        policies = load_provider_lane_policies(self._conn)
        if not policies:
            raise TaskRouteAuthorityError(
                f"auto/{task_type}: provider lane policy authority returned no rows"
            )
        kept: list[dict[str, Any]] = []
        rejected: list[str] = []
        for row in rows:
            provider = str(row.get("provider_slug") or "")
            adapter_type = str(row.get("adapter_type") or _resolve_default_adapter_for_router(self, provider))
            spend_pressure = str(row.get("spend_pressure") or "") or None
            budget_authority_unreachable = bool(
                row.get("budget_authority_unreachable", False)
            )
            budget_window_data_quality_error = bool(
                row.get("budget_window_data_quality_error", False)
            )
            # BUG-2A950857: thread the economics-authority PAYG fallback flag
            # into admission. ``None`` when the row is missing the key (legacy
            # rows, tests with synthetic rows) → admission stays permissive.
            raw_allow_payg_fallback = row.get("allow_payg_fallback")
            allow_payg_fallback = (
                bool(raw_allow_payg_fallback)
                if raw_allow_payg_fallback is not None
                else None
            )
            admitted, reason = admit_adapter_type(
                policies,
                provider,
                adapter_type,
                spend_pressure=spend_pressure,
                budget_authority_unreachable=budget_authority_unreachable,
                budget_window_data_quality_error=budget_window_data_quality_error,
                allow_payg_fallback=allow_payg_fallback,
            )
            if admitted:
                kept.append(row)
            else:
                rejected.append(f"{provider}/{row.get('model_slug')}:{adapter_type}:{reason}")
        if rejected:
            logger.info(
                "lane_policy narrowed auto/%s chain: dropped %d candidate(s) [%s]",
                task_type, len(rejected), "; ".join(rejected),
            )
        if not kept:
            raise TaskRouteAuthorityError(
                f"auto/{task_type}: all candidates rejected by provider lane policy "
                f"({'; '.join(rejected)})"
            )
        # Explicit operator rows (route_source='explicit') lead the chain —
        # an operator writing a task_type_routing row is pinning intent that
        # overrides cost-first preferences. Within explicit and within
        # derived, zero-marginal-cost routes (subscription_included,
        # prepaid_credit, owned_compute) rank before metered rows. Stable-sort
        # preserves the rerank ordering within each bucket.
        from runtime.routing_economics import _PREPAID_BILLING_MODES
        kept.sort(
            key=lambda r: (
                0 if str(r.get("route_source") or "derived") == "explicit" else 1,
                0 if str(r.get("billing_mode") or "").strip() in _PREPAID_BILLING_MODES else 1,
            )
        )
        return kept

    def _apply_provider_transport_admission_filter(
        self,
        task_type: str,
        rows: list[Any],
    ) -> list[Any]:
        """BUG-DD1D48B1: drop candidates whose resolved default adapter_type
        has admitted_by_policy=false in provider_transport_admissions.

        The classic failure the filter prevents: auto/review resolves to
        anthropic/claude-opus-4-7; the worker's default adapter for
        anthropic is llm_task (HTTP); provider_transport_admissions has
        that row marked admitted_by_policy=false per the CLI-only policy;
        but without this filter the route is emitted anyway and the worker
        hits Anthropic with an invalid HTTP key, collapsing on 401.

        If provider_transport_admissions is unavailable, the filter fails
        closed. Missing policy authority must not emit runnable provider
        routes.
        """
        if not rows:
            return rows
        provider_slugs = sorted({str(row["provider_slug"]) for row in rows})
        adapter_by_row: dict[int, str] = {}
        for index, row in enumerate(rows):
            provider_slug = str(row["provider_slug"])
            explicit_adapter = str(row.get("adapter_type") or "").strip()
            if explicit_adapter:
                adapter_by_row[index] = explicit_adapter
                continue
            try:
                adapter_by_row[index] = _resolve_default_adapter_for_router(
                    self, provider_slug
                )
            except Exception:
                logger.warning(
                    "AUTO ROUTE BLOCKED: auto/%s provider %s has no resolvable default adapter",
                    task_type,
                    provider_slug,
                )
                adapter_by_row[index] = ""
        adapter_types = sorted({a for a in adapter_by_row.values() if a})
        if not adapter_types:
            return []
        try:
            admission_rows = self._conn.execute(
                _PROVIDER_TRANSPORT_ADMISSION_SQL,
                list(provider_slugs),
                list(adapter_types),
            )
        except PostgresError as exc:
            if getattr(exc, "sqlstate", None) == _UNDEFINED_TABLE_SQLSTATE:
                raise provider_authority_fail(
                    "provider_authority.transport_admissions_missing",
                    "provider_transport_admissions table missing; blocking provider routes",
                    table_name="provider_transport_admissions",
                    task_type=task_type,
                ) from exc
            raise
        admission_index: dict[tuple[str, str], dict[str, Any]] = {
            (str(r["provider_slug"]), str(r["adapter_type"])): dict(r)
            for r in admission_rows
        }
        kept: list[Any] = []
        for index, row in enumerate(rows):
            provider_slug = str(row["provider_slug"])
            adapter_type = adapter_by_row.get(index, "")
            if not adapter_type:
                kept.append(row)
                continue
            admission = admission_index.get((provider_slug, adapter_type))
            if admission is None:
                raise provider_authority_fail(
                    "provider_authority.transport_admission_missing",
                    "provider_transport_admissions has no row for provider/adapter",
                    task_type=task_type,
                    provider_slug=provider_slug,
                    model_slug=str(row["model_slug"]),
                    adapter_type=adapter_type,
                )
            if bool(admission.get("admitted_by_policy")):
                kept.append(row)
                continue
            reason = str(admission.get("policy_reason") or "").strip()
            decision_ref = str(admission.get("decision_ref") or "").strip()
            logger.warning(
                "AUTO ROUTE BLOCKED: auto/%s -> %s/%s because "
                "provider_transport_admissions.admitted_by_policy=false for "
                "(%s, %s); reason=%r decision_ref=%r",
                task_type,
                provider_slug,
                row["model_slug"],
                provider_slug,
                adapter_type,
                reason,
                decision_ref,
            )
        return kept

    def _apply_route_eligibility(
        self,
        task_type: str,
        rows: list[Any],
        *,
        as_of: datetime,
    ) -> list[Any]:
        if not rows:
            return rows

        provider_slugs = tuple(sorted({str(row["provider_slug"]) for row in rows}))
        model_slugs = tuple(sorted({str(row["model_slug"]) for row in rows}))
        decisions = self._load_route_eligibility(task_type, provider_slugs=provider_slugs, model_slugs=model_slugs, as_of=as_of)
        if not decisions:
            return rows

        filtered: list[Any] = []
        for row in rows:
            decision = self._matching_route_eligibility(
                task_type,
                provider_slug=str(row["provider_slug"]),
                model_slug=str(row["model_slug"]),
                decisions=decisions,
            )
            if decision is None or decision.eligibility_status == "eligible":
                filtered.append(row)
                continue
            logger.warning(
                "AUTO ROUTE BLOCKED: auto/%s → %s/%s by %s (%s, decision_ref=%s)",
                task_type,
                row["provider_slug"],
                row["model_slug"],
                decision.task_route_eligibility_id,
                decision.reason_code,
                decision.decision_ref,
            )
        return filtered

    def _load_route_eligibility(
        self,
        task_type: str,
        *,
        provider_slugs: tuple[str, ...],
        model_slugs: tuple[str, ...],
        as_of: datetime,
    ) -> tuple[TaskRouteEligibilityDecision, ...]:
        if not provider_slugs:
            return ()
        try:
            rows = self._conn.execute(_ELIGIBILITY_SQL, list(provider_slugs), task_type, list(model_slugs), as_of)
        except PostgresError as exc:
            if getattr(exc, "sqlstate", None) == _UNDEFINED_TABLE_SQLSTATE:
                logger.debug("task_type_route_eligibility table missing; skipping route eligibility checks")
                return ()
            raise

        return tuple(
            TaskRouteEligibilityDecision(
                task_route_eligibility_id=str(row["task_route_eligibility_id"]),
                task_type=str(row["task_type"]) if row["task_type"] is not None else None,
                provider_slug=str(row["provider_slug"]),
                model_slug=str(row["model_slug"]) if row["model_slug"] is not None else None,
                eligibility_status=str(row["eligibility_status"]),
                reason_code=str(row["reason_code"]),
                rationale=str(row["rationale"] or ""),
                effective_from=row["effective_from"],
                effective_to=row["effective_to"],
                decision_ref=str(row["decision_ref"]),
            )
            for row in rows
        )

    @staticmethod
    def _matching_route_eligibility(
        task_type: str,
        *,
        provider_slug: str,
        model_slug: str,
        decisions: tuple[TaskRouteEligibilityDecision, ...],
    ) -> TaskRouteEligibilityDecision | None:
        matches = [
            decision
            for decision in decisions
            if decision.provider_slug == provider_slug
            and (decision.task_type is None or decision.task_type == task_type)
            and (decision.model_slug is None or decision.model_slug == model_slug)
        ]
        if not matches:
            return None
        return max(
            matches,
            key=lambda decision: (
                1 if decision.task_type == task_type else 0,
                1 if decision.model_slug == model_slug else 0,
                decision.effective_from,
                decision.decision_ref,
                decision.task_route_eligibility_id,
            ),
        )

    def resolve_failover_chain(
        self,
        agent_slug: str,
        prefer_cost: bool = False,
        tier_override: str | None = None,
        runtime_profile_ref: str | None = None,
    ) -> list[TaskRouteDecision]:
        """Return the full ranked failover chain for an auto/ slug.

        For explicit slugs, builds a failover chain from the router for
        task types the model is permitted for, preferring candidates from
        different providers.
        """
        if agent_slug.startswith("auto/"):
            slug_tier = _parse_auto_tier_override(agent_slug)
            effective_tier = tier_override or slug_tier
            return self._resolve_auto_chain(
                _normalize_auto_route_key(agent_slug),
                prefer_cost=prefer_cost,
                tier_override=effective_tier,
                runtime_profile_ref=runtime_profile_ref,
            )
        # Explicit slug — resolve primary, then build cross-provider failover
        primary = self.resolve(agent_slug, runtime_profile_ref=runtime_profile_ref)
        chain = [primary]

        # Find a task type this model is permitted for
        parts = agent_slug.split("/", 1)
        if len(parts) == 2:
            provider, model = parts
            task_type = self._routing_repository.load_permitted_task_type_for_model(
                provider_slug=provider,
                model_slug=model,
            )
            if task_type is not None:
                try:
                    auto_chain = self._resolve_auto_chain(
                        task_type,
                        prefer_cost=prefer_cost,
                        runtime_profile_ref=runtime_profile_ref,
                    )
                    # Add candidates from different providers as failover
                    for candidate in auto_chain:
                        if candidate.provider_slug != provider:
                            chain.append(candidate)
                except (ValueError, TaskRouteAuthorityError):
                    pass  # No auto chain available — primary-only is fine

        return chain

    def _ensure_route_state_row(self, task_type: str, provider_slug: str, model_slug: str) -> None:
        if not self._routing_repository.route_exists(
            task_type=task_type,
            provider_slug=provider_slug,
            model_slug=model_slug,
        ):
            if task_type in self._task_profiles:
                self._resolve_chain(task_type)

    def _check_permission(self, task_type: str, model: str, provider: str) -> None:
        row = self._routing_repository.load_route_permission(
            task_type=task_type,
            provider_slug=provider,
            model_slug=model,
        )
        if row and not row.get("permitted"):
            rationale = row.get("rationale") or "not permitted"
            raise PermissionError(
                f"{provider}/{model} is not permitted for task type '{task_type}': {rationale}"
            )

    def resolve_spec_jobs(
        self,
        jobs: list[dict],
        *,
        runtime_profile_ref: str | None = None,
    ) -> list[dict]:
        """Resolve all auto/ agents in a job list. Mutates in place.

        Attaches a ``_route_plan`` (:class:`RoutePlan`) to each auto/ job.
        The plan contains the full failover chain, retry policy, and trigger
        codes.  The dispatcher reads the plan and executes it — no routing
        logic lives in the dispatcher.

        For task types that benefit from model diversity (debate, review),
        jobs rotate through the permitted models round-robin so each job
        gets a different provider's perspective.
        """
        # Track per-task-type rotation counters for diversity.
        # All auto-routed task types rotate so specs with multiple jobs
        # of the same type spread across providers naturally.
        _ROTATION_TASK_TYPES = {
            "debate", "review", "build", "refactor", "test",
            "research", "architecture", "wiring",
        }
        rotation_counters: dict[str, int] = {}

        for job in jobs:
            slug = job.get("agent", "")
            job_runtime_profile_ref = (
                str(job.get("runtime_profile_ref")).strip()
                if isinstance(job.get("runtime_profile_ref"), str) and str(job.get("runtime_profile_ref")).strip()
                else runtime_profile_ref
            )
            if slug.startswith("auto/"):
                job_complexity = str(job.get("complexity", "moderate")).strip().lower()
                explicit_prefer_cost = job.get("prefer_cost")
                prefer_cost = explicit_prefer_cost if isinstance(explicit_prefer_cost, bool) else (job_complexity == "low")
                chain = self.resolve_failover_chain(
                    slug,
                    prefer_cost=prefer_cost,
                    runtime_profile_ref=job_runtime_profile_ref,
                )
                task_type = chain[0].task_type

                # For rotation-eligible types, dedup by provider then cycle
                # so different jobs get different providers' best models.
                # If the chain has only 1 entry, try to enrich it with
                # cross-provider candidates so quota exhaustion on the
                # primary doesn't kill the entire wave (BUG-45B10C25).
                slug_tier = _parse_auto_tier_override(slug)
                if task_type in _ROTATION_TASK_TYPES and len(chain) <= 1:
                    try:
                        broader = self._resolve_auto_chain(
                            task_type, prefer_cost=prefer_cost,
                            tier_override=slug_tier,
                            runtime_profile_ref=job_runtime_profile_ref,
                        )
                        primary_provider = chain[0].provider_slug if chain else None
                        for candidate in broader:
                            if candidate.provider_slug != primary_provider:
                                chain.append(candidate)
                    except (ValueError, TaskRouteAuthorityError):
                        pass
                if task_type in _ROTATION_TASK_TYPES and len(chain) > 1:
                    primary, slugs = _rotate_chain(chain, task_type, rotation_counters)
                else:
                    primary = chain[0]
                    slugs = tuple(f"{d.provider_slug}/{d.model_slug}" for d in chain)
                decision_by_slug = {
                    f"{decision.provider_slug}/{decision.model_slug}": decision
                    for decision in chain
                }
                route_candidates = tuple(
                    _route_candidate_payload(decision_by_slug[slug])
                    for slug in slugs
                    if slug in decision_by_slug
                )

                plan = RoutePlan(
                    primary=slugs[0],
                    chain=slugs,
                    failover_eligible_codes=RoutePlan.default_failover_codes(),
                    transient_retry_codes=RoutePlan.default_transient_codes(),
                    max_same_model_retries=2,
                    backoff_seconds=(5, 15),
                    task_type=primary.task_type,
                    original_slug=slug,
                    route_candidates=route_candidates,
                )

                job["agent"] = plan.primary
                job["_route_plan"] = plan
                job["_route_decision"] = {
                    "original": slug, "resolved": job["agent"],
                    "rank": primary.rank,
                    "rationale": primary.rationale,
                    "transport_type": getattr(primary, "transport_type", ""),
                    "adapter_type": getattr(primary, "adapter_type", ""),
                }
        return jobs

    def list_routes(self, task_type: Optional[str] = None) -> list[dict]:
        if task_type:
            rows = self._routing_repository.load_routes(task_type=task_type)
        else:
            rows = self._routing_repository.load_routes()
        return [dict(r) for r in rows]

    def validate_routes(self) -> list[str]:
        """Check that every model in task_type_routing exists in provider_model_candidates.

        Returns a list of problems found (empty = healthy).  Automatically
        disables routes that reference missing models so stale rows can't
        cause runtime failures.
        """
        problems: list[str] = []
        active = self._conn.execute(
            "SELECT DISTINCT provider_slug, model_slug FROM provider_model_candidates WHERE status = 'active'"
        )
        active_set = {(r["provider_slug"], r["model_slug"]) for r in active}

        routes = self._routing_repository.load_routes()
        for r in routes:
            key = (r["provider_slug"], r["model_slug"])
            if key not in active_set and r["permitted"]:
                task_type = str(r["task_type"])
                provider_slug = str(r["provider_slug"])
                model_slug = str(r["model_slug"])
                problems.append(
                    f"{task_type}: {provider_slug}/{model_slug} not in active registry — auto-disabling"
                )
                self._routing_repository.disable_route(
                    task_type=task_type,
                    provider_slug=provider_slug,
                    model_slug=model_slug,
                    rationale="auto-disabled: model not in active registry",
                )
                logger.warning(
                    "Auto-disabled route %s/%s for task_type=%s: not in active registry",
                    provider_slug, model_slug, task_type,
                )

        if not problems:
            logger.info("Route validation passed: all routing table models exist in registry")
        return problems

    # ------------------------------------------------------------------
    # Failure-based rank demotion
    # ------------------------------------------------------------------

    # If a route accumulates this many consecutive failures without a
    # success, it gets demoted (rank pushed behind the next candidate).
    FAILURE_THRESHOLD = 3

    def record_outcome(
        self,
        task_type: str,
        provider_slug: str,
        model_slug: str,
        succeeded: bool,
        failure_code: str | None = None,
        failure_category: str = "",
        failure_zone: str = "",
    ) -> None:
        """Record a success or failure and auto-demote if threshold is hit.

        Only route-relevant failures count against a model's routing rank.
        Provider/network retries and config noise are observability only;
        route health should move only on canonical failures that actually
        reflect route quality.
        """
        self._ensure_route_state_row(task_type, provider_slug, model_slug)
        normalized_category, normalized_zone = self._normalized_failure_details(
            failure_code=failure_code,
            failure_category=failure_category,
            failure_zone=failure_zone,
        )
        if succeeded:
            self._routing_repository.record_success(
                task_type=task_type,
                provider_slug=provider_slug,
                model_slug=model_slug,
                max_route_health=self._policy.max_route_health,
                success_health_bump=self._policy.success_health_bump,
            )
            return

        counter_column = self._route_health_observation_column(normalized_category)
        if not self._failure_counts_against_route_health(normalized_category):
            self._routing_repository.record_failure_count_only(
                task_type=task_type,
                provider_slug=provider_slug,
                model_slug=model_slug,
                counter_column=counter_column,
                failure_category=normalized_category,
                failure_zone=normalized_zone,
            )
            logger.debug(
                "Skipping routing penalty for %s/%s: failure_category=%s (not route-relevant)",
                provider_slug, model_slug, normalized_category,
            )
            return

        # Internal failure path
        penalty = _failure_penalty(normalized_category, policy=self._policy)
        self._routing_repository.record_internal_failure(
            task_type=task_type,
            provider_slug=provider_slug,
            model_slug=model_slug,
            penalty=penalty,
            failure_category=normalized_category,
            failure_zone=normalized_zone or "internal",
            min_route_health=self._policy.min_route_health,
        )

        # Check if we need to demote
        row = self._routing_repository.load_outcome_state(
            task_type=task_type,
            provider_slug=provider_slug,
            model_slug=model_slug,
        )
        if row is None:
            return

        current_rank = int(row["rank"])
        failures = int(row["recent_failures"])

        if failures < self.FAILURE_THRESHOLD:
            return

        # Find the next-ranked permitted route to swap with
        next_row = self._routing_repository.load_next_permitted_route(
            task_type=task_type,
            current_rank=current_rank,
        )
        if next_row is None:
            logger.warning(
                "Route %s/%s has %d consecutive failures but no lower-ranked "
                "alternative to swap with for task_type=%s",
                provider_slug, model_slug, failures, task_type,
            )
            return

        next_rank = int(next_row["rank"])
        next_provider = str(next_row["provider_slug"])
        next_model = str(next_row["model_slug"])

        # Swap ranks: demote the failing route, promote the next one
        self._routing_repository.set_route_rank(
            task_type=task_type,
            provider_slug=provider_slug,
            model_slug=model_slug,
            rank=next_rank,
        )
        self._routing_repository.set_route_rank(
            task_type=task_type,
            provider_slug=next_provider,
            model_slug=next_model,
            rank=current_rank,
        )

        logger.warning(
            "AUTO-DEMOTED %s/%s (rank %d → %d) after %d consecutive failures "
            "for task_type=%s. Promoted %s/%s (rank %d → %d).",
            provider_slug, model_slug, current_rank, next_rank, failures, task_type,
            next_provider, next_model, next_rank, current_rank,
        )

    def record_review_feedback(
        self,
        task_type: str,
        provider_slug: str,
        model_slug: str,
        *,
        bug_count: int,
        severity_counts: dict[str, int] | None = None,
    ) -> None:
        """Apply downstream review feedback to the route's durable health state."""
        try:
            from .feedback_authority import (
                RecordAuthorityFeedbackCommand,
                record_feedback_event_via_gateway,
            )

            record_feedback_event_via_gateway(
                self._conn,
                RecordAuthorityFeedbackCommand(
                    feedback_stream_ref="feedback.route_review",
                    target_ref=f"{task_type}:{provider_slug}/{model_slug}",
                    source_ref="runtime.review_tracker",
                    signal_kind="route_review_feedback",
                    signal_payload={
                        "task_type": task_type,
                        "provider_slug": provider_slug,
                        "model_slug": model_slug,
                        "bug_count": int(bug_count),
                        "severity_counts": dict(severity_counts or {}),
                    },
                    proposed_action={
                        "kind": "route_health_adjustment_candidate",
                        "target_authority_domain_ref": "authority.task_route_eligibility",
                    },
                    recorded_by="runtime.task_type_router",
                    idempotency_key=None,
                ),
            )
        except Exception:
            logger.warning("route review feedback authority intake failed", exc_info=True)

        self._ensure_route_state_row(task_type, provider_slug, model_slug)
        severity_counts = severity_counts or {}
        severity_penalties = self._policy.review_severity_penalties or {}
        penalty = min(
            0.40,
            (max(0, int(severity_counts.get("high", 0))) * float(severity_penalties.get("high", 0.15)))
            + (max(0, int(severity_counts.get("medium", 0))) * float(severity_penalties.get("medium", 0.08)))
            + (max(0, int(severity_counts.get("low", 0))) * float(severity_penalties.get("low", 0.03)))
            + (max(0, int(bug_count)) * 0.01),
        )
        if bug_count <= 0:
            self._routing_repository.record_review_success(
                task_type=task_type,
                provider_slug=provider_slug,
                model_slug=model_slug,
                max_route_health=self._policy.max_route_health,
                review_success_bump=self._policy.review_success_bump,
            )
            return

        self._routing_repository.record_review_failure(
            task_type=task_type,
            provider_slug=provider_slug,
            model_slug=model_slug,
            bug_count=int(bug_count),
            review_penalty=penalty,
            min_route_health=self._policy.min_route_health,
        )
