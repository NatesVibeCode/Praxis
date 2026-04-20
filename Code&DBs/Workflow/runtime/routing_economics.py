"""Cost/value computation for task-type routing.

Covers budget spend pressure, marginal cost computation, economic rationale,
and billing mode analysis.  DB I/O is limited to loading budget windows.
"""
from __future__ import annotations

from typing import TYPE_CHECKING, Any

from asyncpg import PostgresError
from registry.provider_execution_registry import (
    resolve_default_adapter_type,
    resolve_adapter_economics,
    supports_adapter,
)

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

_PREPAID_BILLING_MODES = frozenset({"subscription_included", "prepaid_credit", "owned_compute"})
_ADAPTER_COST_ORDER = ("cli_llm", "llm_task")
_UNDEFINED_TABLE_SQLSTATE = "42P01"


# ---------------------------------------------------------------------------
# Cost helpers
# ---------------------------------------------------------------------------

def float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def row_effective_marginal_cost(row: Any) -> float:
    configured = row.get("effective_marginal_cost")
    if configured is not None:
        try:
            return float(configured)
        except (TypeError, ValueError):
            pass
    try:
        return float(row.get("cost_per_m_tokens") or 0.0)
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# Budget spend pressure
# ---------------------------------------------------------------------------

def budget_spend_pressure(window: dict[str, Any] | None) -> str:
    if not window:
        return "unknown"
    status = str(window.get("budget_status") or "").strip().lower()
    limit_values = (
        (float_or_none(window.get("request_limit")), float_or_none(window.get("requests_used"))),
        (float_or_none(window.get("token_limit")), float_or_none(window.get("tokens_used"))),
        (float_or_none(window.get("spend_limit_usd")), float_or_none(window.get("spend_used_usd"))),
    )
    ratios = [
        used / limit
        for limit, used in limit_values
        if limit is not None and used is not None and limit > 0
    ]
    max_ratio = max(ratios) if ratios else None
    if any(token in status for token in ("blocked", "exhaust", "denied", "hard")):
        return "high"
    if any(token in status for token in ("warning", "warn", "soft", "approach", "constrain")):
        return "medium"
    if max_ratio is None:
        return "low" if status else "unknown"
    if max_ratio >= 0.95:
        return "high"
    if max_ratio >= 0.75:
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# Economic rationale
# ---------------------------------------------------------------------------

def economic_rationale(row: dict[str, Any]) -> str:
    adapter_type = str(row.get("adapter_type") or "").strip()
    billing_mode = str(row.get("billing_mode") or "").strip()
    budget_bucket = str(row.get("budget_bucket") or "").strip()
    spend_pressure = str(row.get("spend_pressure") or "").strip()
    if not any((adapter_type, billing_mode, budget_bucket, spend_pressure)):
        return ""
    return (
        f"economics={adapter_type or 'unknown'}"
        f"/{billing_mode or 'unknown'}"
        f"/{budget_bucket or 'unknown'}"
        f"/spend:{spend_pressure or 'unknown'}"
    )


# ---------------------------------------------------------------------------
# Budget window loading
# ---------------------------------------------------------------------------

def load_provider_budget_windows(
    conn: "SyncPostgresConnection",
    provider_policy_ids: set[str],
) -> dict[str, dict[str, Any]]:
    if not provider_policy_ids:
        return {}
    try:
        rows = conn.execute(
            """SELECT DISTINCT ON (provider_policy_id)
                      provider_policy_id,
                      budget_status,
                      request_limit,
                      requests_used,
                      token_limit,
                      tokens_used,
                      spend_limit_usd,
                      spend_used_usd,
                      window_started_at,
                      window_ended_at,
                      created_at
               FROM public.provider_budget_windows
               WHERE provider_policy_id = ANY($1::text[])
               ORDER BY provider_policy_id,
                        window_ended_at DESC NULLS LAST,
                        created_at DESC NULLS LAST""",
            list(sorted(provider_policy_ids)),
        )
    except PostgresError as exc:
        if getattr(exc, "sqlstate", None) == _UNDEFINED_TABLE_SQLSTATE:
            return {}
        raise
    return {
        str(row["provider_policy_id"]): dict(row)
        for row in rows or []
        if row.get("provider_policy_id")
    }


# ---------------------------------------------------------------------------
# Marginal cost computation / adapter selection
# ---------------------------------------------------------------------------

def resolve_route_economics(
    *,
    provider_slug: str,
    adapter_type: str | None,
    provider_policy_id: str | None,
    raw_cost_per_m_tokens: float,
    budget_windows: dict[str, dict[str, Any]],
    default_adapter: str | None = None,
) -> dict[str, Any]:
    """Pick the cheapest viable adapter and compute effective marginal cost.

    Returns a dict with keys: adapter_type, billing_mode, budget_bucket,
    effective_marginal_cost, spend_pressure, budget_status, prefer_prepaid,
    allow_payg_fallback.
    """
    if default_adapter is None:
        default_adapter = resolve_default_adapter_type(provider_slug)

    budget_window = budget_windows.get(provider_policy_id or "")
    spend_pressure = budget_spend_pressure(budget_window)

    candidate_adapters: list[str] = []
    normalized_adapter_type = (adapter_type or "").strip().lower()
    if normalized_adapter_type and supports_adapter(provider_slug, normalized_adapter_type):
        candidate_adapters.append(normalized_adapter_type)
    for candidate_adapter_type in (default_adapter, *_ADAPTER_COST_ORDER):
        normalized_candidate = (candidate_adapter_type or "").strip().lower()
        if (
            normalized_candidate
            and normalized_candidate not in candidate_adapters
            and supports_adapter(provider_slug, normalized_candidate)
        ):
            candidate_adapters.append(normalized_candidate)
    if not candidate_adapters:
        raise RuntimeError(
            f"provider {provider_slug!r} has no supported adapter for routing economics"
        )

    options: list[dict[str, Any]] = []
    for candidate_adapter_type in candidate_adapters:
        economics = resolve_adapter_economics(provider_slug, candidate_adapter_type)
        billing_mode = str(economics.get("billing_mode") or "metered_api")
        configured_marginal_cost = economics.get("effective_marginal_cost")
        effective_marginal_cost = float(
            configured_marginal_cost
            if configured_marginal_cost is not None
            else (raw_cost_per_m_tokens or 0.0)
        )
        if billing_mode not in _PREPAID_BILLING_MODES:
            effective_marginal_cost = max(effective_marginal_cost, raw_cost_per_m_tokens)
            if spend_pressure == "medium":
                effective_marginal_cost *= 3.0
            elif spend_pressure == "high":
                effective_marginal_cost *= 10.0
        options.append({
            "adapter_type": candidate_adapter_type,
            "billing_mode": billing_mode,
            "budget_bucket": str(economics.get("budget_bucket") or "unknown"),
            "effective_marginal_cost": effective_marginal_cost,
            "spend_pressure": spend_pressure,
            "budget_status": str((budget_window or {}).get("budget_status") or ""),
            "prefer_prepaid": bool(economics.get("prefer_prepaid", False)),
            "allow_payg_fallback": bool(economics.get("allow_payg_fallback", False)),
        })

    options.sort(
        key=lambda option: (
            row_effective_marginal_cost(option),
            0 if option.get("billing_mode") in _PREPAID_BILLING_MODES else 1,
            0 if bool(option.get("prefer_prepaid")) else 1,
            {"low": 0, "unknown": 1, "medium": 2, "high": 3}.get(
                str(option.get("spend_pressure") or "unknown"),
                1,
            ),
            0 if str(option.get("adapter_type") or "") == default_adapter else 1,
        )
    )
    return options[0]
