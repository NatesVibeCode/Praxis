"""Paid-model access control helpers.

This module owns the runtime-facing contract for paid model use:

* durable hard-off still lives in ``private_provider_model_access_denials``;
* one-run paid access lives in ``private_paid_model_access_leases``;
* presentation soft-off lives in ``private_provider_model_access_soft_offs``.

The helpers here are intentionally small and SQL-backed so launch, route
selection, and execution all ask the same authority questions.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Mapping


PAID_MODEL_POLICY_DECISION_REF = (
    "architecture-policy::model-access-control::"
    "paid-model-use-requires-explicit-scoped-approval-and-hard-off"
)

_FREE_COST_STRUCTURES = {
    "free",
    "local",
    "included",
    "subscription",
    "subscription_included",
    "subscription-included",
    "prepaid_subscription",
}

_PAID_COST_STRUCTURES = {
    "paid",
    "metered",
    "metered_api",
    "metered-api",
    "payg",
    "pay_as_you_go",
    "usage",
    "usage_based",
    "token_metered",
    "per_token",
}


class PaidModelAccessError(RuntimeError):
    """Raised before dispatch when paid-model authority is missing."""

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


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _text(value: object) -> str:
    return str(value or "").strip()


def _json_dumps(value: Mapping[str, Any]) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _stable_lease_id(payload: Mapping[str, Any]) -> str:
    digest = hashlib.sha256(_json_dumps(payload).encode("utf-8")).hexdigest()[:24]
    return f"paid-model-lease.{digest}"


def _datetime_param(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(_text(value))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone offset")
    return parsed


def adapter_type_for_transport(transport_type: str) -> str:
    return "cli_llm" if _text(transport_type).upper() == "CLI" else "llm_task"


def transport_type_for_adapter(adapter_type: str) -> str:
    return "CLI" if _text(adapter_type).lower() == "cli_llm" else "API"


def is_paid_model_route(
    *,
    cost_structure: object = None,
    cost_metadata: Mapping[str, Any] | None = None,
    reason_code: object = None,
    operator_message: object = None,
) -> bool:
    """Return whether a route must use explicit paid-model approval."""

    reason = _text(reason_code).lower()
    message = _text(operator_message).lower()
    if "paid_model" in reason or "paid model" in message or "paid-model" in message:
        return True

    metadata = dict(cost_metadata or {})
    candidates = {
        _text(cost_structure).lower(),
        _text(metadata.get("billing_mode")).lower(),
        _text(metadata.get("billing_model")).lower(),
        _text(metadata.get("cost_structure")).lower(),
    }
    candidates.discard("")
    if candidates & _PAID_COST_STRUCTURES:
        return True
    if candidates and candidates <= _FREE_COST_STRUCTURES:
        return False
    return False


def normalize_paid_selector(selector: Mapping[str, Any]) -> dict[str, str]:
    transport_type = _text(selector.get("transport_type")).upper() or "API"
    adapter_type = _text(selector.get("adapter_type")) or adapter_type_for_transport(transport_type)
    return {
        "runtime_profile_ref": _text(selector.get("runtime_profile_ref")) or "praxis",
        "job_type": _text(selector.get("job_type")),
        "transport_type": transport_type,
        "adapter_type": adapter_type,
        "provider_slug": _text(selector.get("provider_slug")).lower(),
        "model_slug": _text(selector.get("model_slug")),
    }


def require_exact_paid_selector(selector: Mapping[str, Any]) -> dict[str, str]:
    normalized = normalize_paid_selector(selector)
    missing = [key for key, value in normalized.items() if not value or value == "*"]
    if missing:
        raise ValueError(
            "paid model one-run leases require exact selector fields; "
            f"missing or wildcard: {', '.join(missing)}"
        )
    if normalized["transport_type"] not in {"CLI", "API"}:
        raise ValueError("transport_type must be CLI or API")
    return normalized


def fetch_paid_route_control(
    conn: Any,
    *,
    runtime_profile_ref: str,
    job_type: str,
    provider_slug: str,
    model_slug: str,
    transport_type: str | None = None,
    adapter_type: str | None = None,
) -> dict[str, Any] | None:
    """Read the matrix row that explains whether a route is paid-gated."""

    rows = conn.execute(
        """
        SELECT
            runtime_profile_ref,
            job_type,
            transport_type,
            adapter_type,
            provider_slug,
            model_slug,
            cost_structure,
            cost_metadata,
            control_enabled,
            control_state,
            control_scope,
            control_reason_code,
            control_operator_message,
            control_decision_ref
        FROM private_model_access_control_matrix
        WHERE runtime_profile_ref = $1
          AND job_type = $2
          AND provider_slug = $3
          AND model_slug = $4
          AND ($5::text IS NULL OR transport_type = $5)
          AND ($6::text IS NULL OR adapter_type = $6)
        ORDER BY
          CASE WHEN transport_type = $5 THEN 0 ELSE 1 END,
          CASE WHEN adapter_type = $6 THEN 0 ELSE 1 END,
          transport_type,
          adapter_type
        LIMIT 1
        """,
        runtime_profile_ref,
        job_type,
        provider_slug,
        model_slug,
        transport_type.upper() if transport_type else None,
        adapter_type or None,
    )
    if not rows:
        return None
    return dict(rows[0])


def paid_route_requirement_from_control(
    row: Mapping[str, Any],
    *,
    label: str | None = None,
    resolved_agent: str | None = None,
) -> dict[str, Any] | None:
    if not is_paid_model_route(
        cost_structure=row.get("cost_structure"),
        cost_metadata=dict(row.get("cost_metadata") or {}),
        reason_code=row.get("control_reason_code"),
        operator_message=row.get("control_operator_message"),
    ):
        return None

    selector = normalize_paid_selector(row)
    return {
        "label": label,
        "resolved_agent": resolved_agent
        or f"{selector['provider_slug']}/{selector['model_slug']}",
        "runtime_profile_ref": selector["runtime_profile_ref"],
        "job_type": selector["job_type"],
        "transport_type": selector["transport_type"],
        "adapter_type": selector["adapter_type"],
        "provider_slug": selector["provider_slug"],
        "model_slug": selector["model_slug"],
        "cost_posture": {
            "cost_structure": row.get("cost_structure"),
            "cost_metadata": dict(row.get("cost_metadata") or {}),
        },
        "hard_state": row.get("control_state"),
        "hard_reason_code": row.get("control_reason_code"),
        "hard_decision_ref": row.get("control_decision_ref"),
        "operator_message": row.get("control_operator_message"),
        "lease_scope": "one_workflow_run",
        "decision_ref": PAID_MODEL_POLICY_DECISION_REF,
    }


def find_paid_requirement_for_route(
    conn: Any,
    *,
    runtime_profile_ref: str,
    job_type: str,
    provider_slug: str,
    model_slug: str,
    transport_type: str | None = None,
    adapter_type: str | None = None,
    label: str | None = None,
) -> dict[str, Any] | None:
    row = fetch_paid_route_control(
        conn,
        runtime_profile_ref=runtime_profile_ref,
        job_type=job_type,
        provider_slug=provider_slug,
        model_slug=model_slug,
        transport_type=transport_type,
        adapter_type=adapter_type,
    )
    if row is None:
        return None
    return paid_route_requirement_from_control(
        row,
        label=label,
        resolved_agent=f"{provider_slug}/{model_slug}",
    )


def grant_paid_model_lease(
    conn: Any,
    *,
    selector: Mapping[str, Any],
    approval_ref: str,
    approved_by: str,
    proposal_hash: str,
    expires_at: str,
    approval_note: str | None = None,
    cost_posture: Mapping[str, Any] | None = None,
    route_truth_ref: str | None = None,
    decision_ref: str | None = None,
) -> dict[str, Any]:
    exact = require_exact_paid_selector(selector)
    if not _text(approval_ref):
        raise ValueError("approval_ref is required")
    if not _text(approved_by):
        raise ValueError("approved_by is required")
    if not _text(proposal_hash):
        raise ValueError("proposal_hash is required")
    lease_payload = {
        **exact,
        "approval_ref": _text(approval_ref),
        "proposal_hash": _text(proposal_hash),
        "lease_scope": "one_workflow_run",
    }
    lease_id = _stable_lease_id(lease_payload)
    expires_at_param = _datetime_param(expires_at, field_name="expires_at")
    row = conn.fetchrow(
        """
        INSERT INTO private_paid_model_access_leases (
            lease_id,
            runtime_profile_ref,
            job_type,
            transport_type,
            adapter_type,
            provider_slug,
            model_slug,
            approval_ref,
            approved_by,
            approval_note,
            proposal_hash,
            status,
            max_runs,
            consumed_runs,
            expires_at,
            cost_posture,
            route_truth_ref,
            decision_ref,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7,
            $8, $9, $10, $11,
            'active', 1, 0,
            $12::timestamptz,
            $13::jsonb,
            $14, $15, now()
        )
        ON CONFLICT (lease_id) DO UPDATE SET
            approved_by = EXCLUDED.approved_by,
            approval_note = EXCLUDED.approval_note,
            status = CASE
                WHEN private_paid_model_access_leases.status IN ('consumed', 'revoked')
                THEN private_paid_model_access_leases.status
                ELSE 'active'
            END,
            expires_at = EXCLUDED.expires_at,
            cost_posture = EXCLUDED.cost_posture,
            route_truth_ref = EXCLUDED.route_truth_ref,
            decision_ref = EXCLUDED.decision_ref,
            updated_at = now()
        RETURNING *
        """,
        lease_id,
        exact["runtime_profile_ref"],
        exact["job_type"],
        exact["transport_type"],
        exact["adapter_type"],
        exact["provider_slug"],
        exact["model_slug"],
        _text(approval_ref),
        _text(approved_by),
        _text(approval_note) or None,
        _text(proposal_hash),
        expires_at_param,
        json.dumps(dict(cost_posture or {}), sort_keys=True, default=str),
        _text(route_truth_ref) or None,
        _text(decision_ref) or PAID_MODEL_POLICY_DECISION_REF,
    )
    return dict(row) if row is not None else {"lease_id": lease_id}


def bind_paid_model_leases_to_run(
    conn: Any,
    *,
    lease_ids: list[str],
    run_id: str,
) -> list[dict[str, Any]]:
    if not lease_ids:
        return []
    normalized_run_id = _text(run_id)
    if not normalized_run_id:
        raise ValueError("run_id is required to bind paid model leases")
    rows = conn.execute(
        """
        UPDATE private_paid_model_access_leases
           SET status = 'bound',
               bound_run_id = $2,
               bound_at = COALESCE(bound_at, now()),
               updated_at = now()
         WHERE lease_id = ANY($1::text[])
           AND status = 'active'
           AND consumed_at IS NULL
           AND consumed_runs = 0
           AND expires_at > now()
         RETURNING *
        """,
        lease_ids,
        normalized_run_id,
    )
    bound = [dict(row) for row in rows or ()]
    if len(bound) != len(set(lease_ids)):
        raise PaidModelAccessError(
            "paid_model_access.lease_bind_failed",
            "one or more paid model leases could not be bound to the workflow run",
            details={"lease_ids": lease_ids, "run_id": normalized_run_id, "bound": bound},
        )
    return bound


def close_paid_model_leases_for_run(conn: Any, *, run_id: str) -> list[dict[str, Any]]:
    normalized_run_id = _text(run_id)
    if not normalized_run_id:
        return []
    rows = conn.execute(
        """
        UPDATE private_paid_model_access_leases
           SET status = 'consumed',
               consumed_runs = max_runs,
               consumed_at = COALESCE(consumed_at, now()),
               updated_at = now()
         WHERE bound_run_id = $1
           AND status IN ('active', 'bound')
           AND consumed_at IS NULL
         RETURNING *
        """,
        normalized_run_id,
    )
    return [dict(row) for row in rows or ()]


def active_paid_lease_for_run(
    conn: Any,
    *,
    run_id: str,
    runtime_profile_ref: str,
    job_type: str,
    transport_type: str,
    adapter_type: str,
    provider_slug: str,
    model_slug: str,
) -> dict[str, Any] | None:
    rows = conn.execute(
        """
        SELECT *
        FROM private_paid_model_access_leases
        WHERE bound_run_id = $1
          AND runtime_profile_ref = $2
          AND job_type = $3
          AND transport_type = $4
          AND adapter_type = $5
          AND provider_slug = $6
          AND model_slug = $7
          AND status = 'bound'
          AND consumed_at IS NULL
          AND consumed_runs < max_runs
          AND expires_at > now()
        ORDER BY bound_at DESC, created_at DESC
        LIMIT 1
        """,
        run_id,
        runtime_profile_ref,
        job_type,
        transport_type,
        adapter_type,
        provider_slug,
        model_slug,
    )
    if not rows:
        return None
    return dict(rows[0])


def paid_lease_route_candidates_for_run(
    conn: Any,
    *,
    run_id: str,
    runtime_profile_ref: str,
    job_type: str,
    candidates: list[str],
) -> list[str]:
    if not run_id or not runtime_profile_ref or not job_type or not candidates:
        return []
    rows = conn.execute(
        """
        SELECT DISTINCT lease.provider_slug || '/' || lease.model_slug AS slug
        FROM private_paid_model_access_leases AS lease
        JOIN task_type_routing AS route
          ON route.task_type = lease.job_type
         AND route.transport_type = lease.transport_type
         AND route.provider_slug = lease.provider_slug
         AND route.model_slug = lease.model_slug
         AND route.permitted IS TRUE
        WHERE lease.bound_run_id = $1
          AND lease.runtime_profile_ref = $2
          AND lease.job_type = $3
          AND lease.status = 'bound'
          AND lease.consumed_at IS NULL
          AND lease.consumed_runs < lease.max_runs
          AND lease.expires_at > now()
          AND (lease.provider_slug || '/' || lease.model_slug) = ANY($4::text[])
        ORDER BY slug
        """,
        run_id,
        runtime_profile_ref,
        job_type,
        candidates,
    )
    return [str(row["slug"]) for row in rows or () if row.get("slug")]


def ensure_paid_model_access_for_job(
    conn: Any,
    *,
    run_id: str,
    job_label: str,
    runtime_profile_ref: str,
    job_type: str,
    agent_slug: str,
    transport_type: str,
    adapter_type: str,
) -> dict[str, Any] | None:
    if "/" not in agent_slug:
        return None
    provider_slug, model_slug = agent_slug.split("/", 1)
    requirement = find_paid_requirement_for_route(
        conn,
        runtime_profile_ref=runtime_profile_ref,
        job_type=job_type,
        provider_slug=provider_slug,
        model_slug=model_slug,
        transport_type=transport_type,
        adapter_type=adapter_type,
        label=job_label,
    )
    if requirement is None:
        return None

    lease = active_paid_lease_for_run(
        conn,
        run_id=run_id,
        runtime_profile_ref=runtime_profile_ref,
        job_type=job_type,
        transport_type=transport_type,
        adapter_type=adapter_type,
        provider_slug=provider_slug,
        model_slug=model_slug,
    )
    if lease is None:
        raise PaidModelAccessError(
            "paid_model_access.lease_required",
            (
                "paid model dispatch blocked before provider call: "
                f"{provider_slug}/{model_slug} requires a bound one-run lease "
                f"for task {job_type!r}"
            ),
            details={
                "run_id": run_id,
                "job_label": job_label,
                "requirement": requirement,
            },
        )
    return lease


__all__ = [
    "PAID_MODEL_POLICY_DECISION_REF",
    "PaidModelAccessError",
    "active_paid_lease_for_run",
    "adapter_type_for_transport",
    "bind_paid_model_leases_to_run",
    "close_paid_model_leases_for_run",
    "ensure_paid_model_access_for_job",
    "find_paid_requirement_for_route",
    "grant_paid_model_lease",
    "is_paid_model_route",
    "normalize_paid_selector",
    "paid_lease_route_candidates_for_run",
    "paid_route_requirement_from_control",
    "require_exact_paid_selector",
    "transport_type_for_adapter",
]
