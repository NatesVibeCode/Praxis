"""DB-backed reasoning-effort routing helpers.

The router owns provider/model choice. This module owns the adjacent effort
choice so provider-specific knobs stay visible, queryable, and receiptable.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

EFFORT_ORDER: tuple[str, ...] = ("instant", "low", "medium", "high", "max")
EFFORT_RANK = {effort: index for index, effort in enumerate(EFFORT_ORDER, start=1)}


class ReasoningEffortRoutingError(RuntimeError):
    """Raised when effort authority cannot produce an allowed route."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


@dataclass(frozen=True, slots=True)
class TaskEffortPolicy:
    task_type: str
    sub_task_type: str
    default_effort_slug: str
    min_effort_slug: str
    max_effort_slug: str
    escalation_rules: Mapping[str, Any]
    decision_ref: str | None


@dataclass(frozen=True, slots=True)
class ReasoningEffortRoute:
    task_type: str
    provider_slug: str
    model_slug: str
    transport_type: str
    effort_slug: str
    provider_payload: Mapping[str, Any]
    cost_multiplier: float
    latency_multiplier: float
    quality_bias: float
    failure_risk: float
    effort_matrix_ref: str
    policy_decision_ref: str | None
    matrix_decision_ref: str | None

    def as_reasoning_control(self) -> dict[str, Any]:
        return {
            "effort_slug": self.effort_slug,
            "transport_type": self.transport_type,
            "provider_payload": dict(self.provider_payload),
            "cost_multiplier": self.cost_multiplier,
            "latency_multiplier": self.latency_multiplier,
            "quality_bias": self.quality_bias,
            "failure_risk": self.failure_risk,
            "effort_matrix_ref": self.effort_matrix_ref,
            "policy_decision_ref": self.policy_decision_ref,
            "matrix_decision_ref": self.matrix_decision_ref,
        }


def normalize_effort_slug(value: object) -> str:
    effort = str(value or "").strip().lower()
    if effort not in EFFORT_RANK:
        raise ReasoningEffortRoutingError(
            "reasoning_effort.invalid_slug",
            f"unsupported reasoning effort slug: {value!r}",
            details={"effort_slug": value},
        )
    return effort


def normalize_transport_type(value: object) -> str:
    transport = str(value or "").strip().lower()
    if transport in {"", "cli_llm", "cli"}:
        return "cli"
    if transport in {"llm_task", "api"}:
        return "api"
    return transport


def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, str):
        decoded = json.loads(value)
        if not isinstance(decoded, dict):
            raise ReasoningEffortRoutingError(
                "reasoning_effort.invalid_authority",
                f"{field_name} must decode to a JSON object",
                details={"field": field_name},
            )
        return decoded
    if isinstance(value, Mapping):
        return dict(value)
    raise ReasoningEffortRoutingError(
        "reasoning_effort.invalid_authority",
        f"{field_name} must be a JSON object",
        details={"field": field_name, "value_type": type(value).__name__},
    )


def _policy_from_row(row: Mapping[str, Any]) -> TaskEffortPolicy:
    return TaskEffortPolicy(
        task_type=str(row["task_type"]).strip(),
        sub_task_type=str(row.get("sub_task_type") or "*").strip() or "*",
        default_effort_slug=normalize_effort_slug(row["default_effort_slug"]),
        min_effort_slug=normalize_effort_slug(row["min_effort_slug"]),
        max_effort_slug=normalize_effort_slug(row["max_effort_slug"]),
        escalation_rules=_json_object(row.get("escalation_rules"), field_name="escalation_rules"),
        decision_ref=str(row.get("decision_ref") or "").strip() or None,
    )


def _require_effort_within_policy(
    requested_effort: str,
    *,
    policy: TaskEffortPolicy,
) -> None:
    requested_rank = EFFORT_RANK[requested_effort]
    if requested_rank < EFFORT_RANK[policy.min_effort_slug]:
        raise ReasoningEffortRoutingError(
            "reasoning_effort.below_policy_minimum",
            (
                f"requested effort {requested_effort!r} is below the minimum "
                f"{policy.min_effort_slug!r} for task_type={policy.task_type!r}"
            ),
            details={
                "task_type": policy.task_type,
                "requested_effort_slug": requested_effort,
                "min_effort_slug": policy.min_effort_slug,
            },
        )
    if requested_rank > EFFORT_RANK[policy.max_effort_slug]:
        raise ReasoningEffortRoutingError(
            "reasoning_effort.above_policy_maximum",
            (
                f"requested effort {requested_effort!r} is above the maximum "
                f"{policy.max_effort_slug!r} for task_type={policy.task_type!r}"
            ),
            details={
                "task_type": policy.task_type,
                "requested_effort_slug": requested_effort,
                "max_effort_slug": policy.max_effort_slug,
            },
        )


def load_task_effort_policy(
    conn: "SyncPostgresConnection",
    *,
    task_type: str,
    sub_task_type: str = "*",
) -> TaskEffortPolicy:
    rows = conn.execute(
        """
        SELECT task_type, sub_task_type, default_effort_slug, min_effort_slug,
               max_effort_slug, escalation_rules, decision_ref
        FROM task_type_effort_policy
        WHERE task_type IN ($1, '*')
          AND sub_task_type IN ($2, '*')
        ORDER BY CASE WHEN task_type = $1 THEN 0 ELSE 1 END,
                 CASE WHEN sub_task_type = $2 THEN 0 ELSE 1 END
        LIMIT 1
        """,
        str(task_type).strip(),
        str(sub_task_type or "*").strip() or "*",
    )
    if not rows:
        raise ReasoningEffortRoutingError(
            "reasoning_effort.policy_missing",
            f"task_type_effort_policy has no policy for task_type={task_type!r}",
            details={"task_type": task_type, "sub_task_type": sub_task_type},
        )
    return _policy_from_row(rows[0])


def resolve_reasoning_effort_route(
    conn: "SyncPostgresConnection",
    *,
    task_type: str,
    provider_slug: str,
    model_slug: str,
    transport_type: str = "cli",
    sub_task_type: str = "*",
    requested_effort: str | None = None,
) -> ReasoningEffortRoute:
    policy = load_task_effort_policy(
        conn,
        task_type=task_type,
        sub_task_type=sub_task_type,
    )
    effort_slug = (
        normalize_effort_slug(requested_effort)
        if requested_effort is not None
        else policy.default_effort_slug
    )
    _require_effort_within_policy(effort_slug, policy=policy)
    normalized_transport = normalize_transport_type(transport_type)
    rows = conn.execute(
        """
        SELECT effort_matrix_ref, provider_payload, cost_multiplier,
               latency_multiplier, quality_bias, failure_risk, decision_ref
        FROM provider_reasoning_effort_matrix
        WHERE provider_slug = $1
          AND model_slug = $2
          AND transport_type = $3
          AND effort_slug = $4
          AND supported = true
        LIMIT 1
        """,
        str(provider_slug).strip(),
        str(model_slug).strip(),
        normalized_transport,
        effort_slug,
    )
    if not rows:
        raise ReasoningEffortRoutingError(
            "reasoning_effort.matrix_missing",
            "provider reasoning-effort matrix has no supported row for the selected route",
            details={
                "task_type": task_type,
                "provider_slug": provider_slug,
                "model_slug": model_slug,
                "transport_type": normalized_transport,
                "effort_slug": effort_slug,
            },
        )
    row = rows[0]
    return ReasoningEffortRoute(
        task_type=str(task_type).strip(),
        provider_slug=str(provider_slug).strip(),
        model_slug=str(model_slug).strip(),
        transport_type=normalized_transport,
        effort_slug=effort_slug,
        provider_payload=_json_object(row.get("provider_payload"), field_name="provider_payload"),
        cost_multiplier=float(row.get("cost_multiplier") or 1.0),
        latency_multiplier=float(row.get("latency_multiplier") or 1.0),
        quality_bias=float(row.get("quality_bias") or 0.0),
        failure_risk=float(row.get("failure_risk") or 0.0),
        effort_matrix_ref=str(row["effort_matrix_ref"]),
        policy_decision_ref=policy.decision_ref,
        matrix_decision_ref=str(row.get("decision_ref") or "").strip() or None,
    )


__all__ = [
    "EFFORT_ORDER",
    "ReasoningEffortRoute",
    "ReasoningEffortRoutingError",
    "TaskEffortPolicy",
    "load_task_effort_policy",
    "normalize_effort_slug",
    "normalize_transport_type",
    "resolve_reasoning_effort_route",
]
