"""Sync runtime-profile admission authority for provider/model routing.

This module resolves a runtime profile to the exact provider/model candidates
admitted for that profile using canonical Postgres authority:

- ``registry_runtime_profile_authority``
- ``provider_policies``
- ``model_profile_candidate_bindings``
- ``provider_model_candidates``
- ``route_eligibility_states``

It intentionally fails closed when a runtime profile is present but no eligible
candidates are admitted.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from .native_runtime_profile_sync import (
    is_native_runtime_profile_ref,
)

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection


class RuntimeProfileAdmissionError(RuntimeError):
    """Raised when runtime-profile candidate admission cannot be resolved."""

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
class RuntimeProfileAdmittedCandidate:
    """A concrete provider/model candidate admitted for one runtime profile."""

    runtime_profile_ref: str
    model_profile_id: str
    provider_policy_id: str
    candidate_ref: str
    provider_ref: str
    provider_name: str
    provider_slug: str
    model_slug: str
    transport_type: str | None
    host_provider_slug: str
    variant: str
    effort_slug: str
    priority: int
    balance_weight: int
    position_index: int
    route_tier: str | None
    route_tier_rank: int | None
    latency_class: str | None
    latency_rank: int | None
    reasoning_control: Mapping[str, Any] | None
    capability_tags: tuple[str, ...]
    task_affinities: Mapping[str, Any] | None
    benchmark_profile: Mapping[str, Any] | None


def _normalize_as_of(as_of: datetime | None) -> datetime:
    if as_of is None:
        return datetime.now(timezone.utc)
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise RuntimeProfileAdmissionError(
            "routing.invalid_as_of",
            "as_of must be timezone-aware",
        )
    return as_of.astimezone(timezone.utc)


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeProfileAdmissionError(
            "routing.invalid_authority",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _optional_text(value: object) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise RuntimeProfileAdmissionError(
            "routing.invalid_authority",
            "optional text authority field must be a string or null",
            details={"value_type": type(value).__name__},
        )
    normalized = value.strip()
    return normalized or None


def _effective_provider_policy_name(
    *,
    runtime_profile_ref: str,
    provider_name: str | None,
    allowed_provider_refs: tuple[str, ...] = (),
) -> str | None:
    """Return the legacy scalar provider_name filter when no provider_ref allowlist exists."""

    del runtime_profile_ref
    if allowed_provider_refs:
        return None
    return provider_name


def _native_transport_ready_source(
    source_window_refs: object,
) -> bool:
    refs = _json_text_array(
        source_window_refs,
        field_name="source_window_refs",
    )
    return any(ref.startswith("transport:") for ref in refs)


def _candidate_is_admitted_for_runtime_profile(
    *,
    runtime_profile_ref: str,
    eligibility_status: str,
    reason_code: str,
    source_window_refs: object,
    conn: "SyncPostgresConnection | None" = None,
) -> bool:
    if eligibility_status == "eligible":
        return True
    if not is_native_runtime_profile_ref(runtime_profile_ref, conn=conn):
        return False
    if reason_code not in {
        "provider_route_authority.no_live_probe_state",
        "provider_route_authority.health_degraded",
        "provider_route_control_tower.health_degraded",
    }:
        return False
    return _native_transport_ready_source(source_window_refs)


def _require_int(value: object, *, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise RuntimeProfileAdmissionError(
            "routing.invalid_authority",
            f"{field_name} must be an integer",
            details={"field": field_name},
        )
    return value


def _json_object(value: object, *, field_name: str) -> Mapping[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, str):
        decoded = json.loads(value)
        if not isinstance(decoded, dict):
            raise RuntimeProfileAdmissionError(
                "routing.invalid_authority",
                f"{field_name} must decode to a JSON object",
                details={"field": field_name},
            )
        return decoded
    if isinstance(value, Mapping):
        return dict(value)
    raise RuntimeProfileAdmissionError(
        "routing.invalid_authority",
        f"{field_name} must be a JSON object or JSON text",
        details={"field": field_name, "value_type": type(value).__name__},
    )


def _json_text_array(value: object, *, field_name: str) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        decoded = json.loads(value)
        if not isinstance(decoded, list):
            raise RuntimeProfileAdmissionError(
                "routing.invalid_authority",
                f"{field_name} must decode to a JSON array",
                details={"field": field_name},
            )
        value = decoded
    if not isinstance(value, list):
        raise RuntimeProfileAdmissionError(
            "routing.invalid_authority",
            f"{field_name} must be a JSON array or JSON text",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    normalized: list[str] = []
    for index, item in enumerate(value):
        normalized.append(_require_text(item, field_name=f"{field_name}[{index}]").lower())
    return tuple(dict.fromkeys(normalized))


def load_admitted_runtime_profile_candidates(
    conn: "SyncPostgresConnection",
    *,
    runtime_profile_ref: str,
    as_of: datetime | None = None,
) -> tuple[RuntimeProfileAdmittedCandidate, ...]:
    """Resolve the exact admitted provider/model candidates for a runtime profile."""

    normalized_runtime_profile_ref = _require_text(
        runtime_profile_ref,
        field_name="runtime_profile_ref",
    )
    normalized_as_of = _normalize_as_of(as_of)

    profile_rows = conn.execute(
        """
        SELECT model_profile_id, provider_policy_id
        FROM registry_runtime_profile_authority
        WHERE runtime_profile_ref = $1
        LIMIT 1
        """,
        normalized_runtime_profile_ref,
    )
    if not profile_rows:
        raise RuntimeProfileAdmissionError(
            "routing.profile_unknown",
            f"runtime profile {normalized_runtime_profile_ref!r} is missing authority",
            details={"runtime_profile_ref": normalized_runtime_profile_ref},
        )

    model_profile_id = _require_text(profile_rows[0]["model_profile_id"], field_name="model_profile_id")
    provider_policy_id = _require_text(
        profile_rows[0]["provider_policy_id"],
        field_name="provider_policy_id",
    )

    policy_rows = conn.execute(
        """
        SELECT provider_name,
               allowed_provider_refs,
               preferred_provider_ref
        FROM provider_policies
        WHERE provider_policy_id = $1
          AND status = 'active'
          AND effective_from <= $2
          AND (effective_to IS NULL OR effective_to > $2)
        ORDER BY effective_from DESC, provider_policy_id DESC
        LIMIT 1
        """,
        provider_policy_id,
        normalized_as_of,
    )
    if not policy_rows:
        raise RuntimeProfileAdmissionError(
            "routing.provider_policy_unknown",
            (
                f"runtime profile {normalized_runtime_profile_ref!r} references inactive "
                f"or missing provider policy {provider_policy_id!r}"
            ),
            details={
                "runtime_profile_ref": normalized_runtime_profile_ref,
                "provider_policy_id": provider_policy_id,
            },
        )

    allowed_provider_refs = _json_text_array(
        policy_rows[0].get("allowed_provider_refs"),
        field_name="allowed_provider_refs",
    )
    provider_name = _effective_provider_policy_name(
        runtime_profile_ref=normalized_runtime_profile_ref,
        provider_name=_optional_text(policy_rows[0].get("provider_name")),
        allowed_provider_refs=allowed_provider_refs,
    )
    candidate_rows = conn.execute(
        """
        SELECT binding.position_index,
               candidate.candidate_ref,
               candidate.provider_ref,
               candidate.provider_name,
               candidate.provider_slug,
               candidate.model_slug,
               candidate.transport_type,
               candidate.host_provider_slug,
               candidate.variant,
               candidate.effort_slug,
               candidate.priority,
               candidate.balance_weight,
               candidate.route_tier,
               candidate.route_tier_rank,
               candidate.latency_class,
               candidate.latency_rank,
               candidate.reasoning_control,
               candidate.capability_tags,
               candidate.task_affinities,
               candidate.benchmark_profile
        FROM model_profile_candidate_bindings binding
        JOIN provider_model_candidates candidate
          ON candidate.candidate_ref = binding.candidate_ref
        WHERE binding.model_profile_id = $1
          AND binding.effective_from <= $2
          AND (binding.effective_to IS NULL OR binding.effective_to > $2)
          AND candidate.status = 'active'
          AND candidate.effective_from <= $2
          AND (candidate.effective_to IS NULL OR candidate.effective_to > $2)
          AND ($3::text IS NULL OR candidate.provider_name = $3)
          AND ($4::text[] IS NULL OR cardinality($4::text[]) = 0 OR candidate.provider_ref = ANY($4::text[]))
        ORDER BY binding.position_index ASC,
                 candidate.priority ASC,
                 candidate.candidate_ref ASC
        """,
        model_profile_id,
        normalized_as_of,
        provider_name,
        list(allowed_provider_refs) if allowed_provider_refs else None,
    )
    if not candidate_rows:
        raise RuntimeProfileAdmissionError(
            "routing.no_profile_candidates",
            (
                f"runtime profile {normalized_runtime_profile_ref!r} resolved to no active "
                "candidate bindings"
            ),
            details={
                "runtime_profile_ref": normalized_runtime_profile_ref,
                "model_profile_id": model_profile_id,
                "provider_policy_id": provider_policy_id,
                "provider_name": provider_name,
                "allowed_provider_refs": list(allowed_provider_refs),
            },
        )

    candidate_refs = [
        _require_text(row["candidate_ref"], field_name="candidate_ref")
        for row in candidate_rows
    ]
    eligibility_rows = conn.execute(
        """
        SELECT DISTINCT ON (candidate_ref)
               candidate_ref,
               eligibility_status,
               reason_code,
               source_window_refs
        FROM route_eligibility_states
        WHERE model_profile_id = $1
          AND provider_policy_id = $2
          AND candidate_ref = ANY($3::text[])
          AND evaluated_at <= $4
        ORDER BY candidate_ref, evaluated_at DESC, route_eligibility_state_id DESC
        """,
        model_profile_id,
        provider_policy_id,
        candidate_refs,
        normalized_as_of,
    )
    eligibility_by_ref = {
        _require_text(row["candidate_ref"], field_name="candidate_ref"): {
            "eligibility_status": _require_text(
                row["eligibility_status"],
                field_name="eligibility_status",
            ),
            "reason_code": _require_text(
                row["reason_code"],
                field_name="reason_code",
            ),
            "source_window_refs": row.get("source_window_refs"),
        }
        for row in eligibility_rows or []
    }

    admitted: list[RuntimeProfileAdmittedCandidate] = []
    for row in candidate_rows:
        candidate_ref = _require_text(row["candidate_ref"], field_name="candidate_ref")
        eligibility = eligibility_by_ref.get(candidate_ref)
        if eligibility is None:
            continue
        if not _candidate_is_admitted_for_runtime_profile(
            runtime_profile_ref=normalized_runtime_profile_ref,
            eligibility_status=str(eligibility["eligibility_status"]),
            reason_code=str(eligibility["reason_code"]),
            source_window_refs=eligibility.get("source_window_refs"),
            conn=conn,
        ):
            continue
        admitted.append(
            RuntimeProfileAdmittedCandidate(
                runtime_profile_ref=normalized_runtime_profile_ref,
                model_profile_id=model_profile_id,
                provider_policy_id=provider_policy_id,
                candidate_ref=candidate_ref,
                provider_ref=_require_text(row["provider_ref"], field_name="provider_ref"),
                provider_name=_require_text(row["provider_name"], field_name="provider_name"),
                provider_slug=_require_text(row["provider_slug"], field_name="provider_slug"),
                model_slug=_require_text(row["model_slug"], field_name="model_slug"),
                transport_type=_optional_text(row.get("transport_type")),
                host_provider_slug=str(row.get("host_provider_slug") or ""),
                variant=str(row.get("variant") or ""),
                effort_slug=str(row.get("effort_slug") or ""),
                priority=_require_int(row["priority"], field_name="priority"),
                balance_weight=_require_int(row["balance_weight"], field_name="balance_weight"),
                position_index=_require_int(row["position_index"], field_name="position_index"),
                route_tier=_optional_text(row.get("route_tier")),
                route_tier_rank=(
                    _require_int(row["route_tier_rank"], field_name="route_tier_rank")
                    if row.get("route_tier_rank") is not None
                    else None
                ),
                latency_class=_optional_text(row.get("latency_class")),
                latency_rank=(
                    _require_int(row["latency_rank"], field_name="latency_rank")
                    if row.get("latency_rank") is not None
                    else None
                ),
                reasoning_control=_json_object(
                    row.get("reasoning_control"),
                    field_name="reasoning_control",
                ),
                capability_tags=_json_text_array(
                    row.get("capability_tags"),
                    field_name="capability_tags",
                ),
                task_affinities=_json_object(
                    row.get("task_affinities"),
                    field_name="task_affinities",
                ),
                benchmark_profile=_json_object(
                    row.get("benchmark_profile"),
                    field_name="benchmark_profile",
                ),
            )
        )

    if not admitted:
        raise RuntimeProfileAdmissionError(
            "routing.no_allowed_candidates",
            (
                f"runtime profile {normalized_runtime_profile_ref!r} resolved to no eligible "
                "provider/model candidates"
            ),
            details={
                "runtime_profile_ref": normalized_runtime_profile_ref,
                "model_profile_id": model_profile_id,
                "provider_policy_id": provider_policy_id,
                "candidate_refs": candidate_refs,
                "eligible_candidate_refs": tuple(
                    ref
                    for ref, details in eligibility_by_ref.items()
                    if _candidate_is_admitted_for_runtime_profile(
                        runtime_profile_ref=normalized_runtime_profile_ref,
                        eligibility_status=str(details["eligibility_status"]),
                        reason_code=str(details["reason_code"]),
                        source_window_refs=details.get("source_window_refs"),
                        conn=conn,
                    )
                ),
            },
        )

    return tuple(admitted)


__all__ = [
    "RuntimeProfileAdmissionError",
    "RuntimeProfileAdmittedCandidate",
    "load_admitted_runtime_profile_candidates",
]
