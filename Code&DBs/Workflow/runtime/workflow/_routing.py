"""Route selection, touch-key conflict detection, and request envelope building."""
from __future__ import annotations

import logging
import random
from typing import TYPE_CHECKING

from ._shared import (
    _DEFAULT_NATIVE_RUNTIME_PROFILE_REF,
    _DEFAULT_NATIVE_WORKSPACE_REF,
    _READ_ONLY_MODE,
    _WRITE_MODE,
    _json_loads_maybe,
    _normalize_paths,
    _workflow_id_for_spec,
)

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)

__all__ = [
    "_build_request_envelope",
    "_derive_touch_keys",
    "_job_has_touch_conflict",
    "_job_touch_entries",
    "_record_task_route_outcome",
    "_route_candidates",
    "_runtime_profile_admitted_route_candidates",
    "_runtime_profile_ref_for_run",
    "_runtime_profile_ref_from_spec",
    "_select_claim_route",
    "_touch_entry",
    "_touches_conflict",
    "_workspace_ref_from_spec",
    "_db_admitted_route_candidates",
    "_active_provider_load",
    "_failure_zone_lookup",
    "_task_route_candidate_meta",
    "_blocked_candidates_for_task",
]


def _record_task_route_outcome(
    conn: "SyncPostgresConnection",
    *,
    task_type: str,
    effective_agent: str,
    succeeded: bool,
    failure_code: str | None = None,
    failure_category: str = "",
    failure_zone: str = "",
) -> None:
    """Record task-type routing feedback when the router supports it."""
    normalized_task_type = str(task_type or "").strip()
    normalized_agent = str(effective_agent or "").strip()
    if not normalized_task_type or "/" not in normalized_agent:
        return
    provider_slug, model_slug = normalized_agent.split("/", 1)
    if not provider_slug or not model_slug:
        return
    from ._shared import ROUTING_METRICS_FROZEN

    if ROUTING_METRICS_FROZEN:
        logger.info(
            "Route outcome captured (frozen): %s via %s/%s succeeded=%s code=%s",
            normalized_task_type, provider_slug, model_slug, succeeded, failure_code,
        )
        return

    from runtime.task_type_router import TaskTypeRouter

    router = TaskTypeRouter(conn)
    record_outcome = getattr(router, "record_outcome", None)
    if not callable(record_outcome):
        logger.debug(
            "Skipping task-type route outcome recording for %s via %s: router has no record_outcome",
            normalized_task_type,
            normalized_agent,
        )
        return
    record_outcome(
        normalized_task_type,
        provider_slug,
        model_slug,
        succeeded=succeeded,
        failure_code=failure_code,
        failure_category=failure_category,
        failure_zone=failure_zone,
    )


def _touch_entry(key: str, mode: str) -> dict[str, str]:
    return {"key": key, "mode": mode if mode == _READ_ONLY_MODE else _WRITE_MODE}


def _derive_touch_keys(job: dict) -> list[dict[str, str]]:
    entries: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()

    def _append(key: str, mode: str) -> None:
        normalized = (key.strip(), mode if mode == _READ_ONLY_MODE else _WRITE_MODE)
        if not normalized[0] or normalized in seen:
            return
        seen.add(normalized)
        entries.append(_touch_entry(*normalized))

    for raw_touch in job.get("touch_keys", []) or []:
        if isinstance(raw_touch, str):
            _append(raw_touch, _WRITE_MODE)
        elif isinstance(raw_touch, dict):
            _append(str(raw_touch.get("key", "")), str(raw_touch.get("mode", _WRITE_MODE)))

    for path in _normalize_paths(job.get("write_scope")):
        _append(f"file:{path}", _WRITE_MODE)
    for path in _normalize_paths(job.get("read_scope")):
        _append(f"file:{path}", _READ_ONLY_MODE)

    scope = job.get("scope") or {}
    for path in _normalize_paths(scope.get("write")):
        _append(f"file:{path}", _WRITE_MODE)
    for path in _normalize_paths(scope.get("read")):
        _append(f"file:{path}", _READ_ONLY_MODE)

    return entries


def _build_request_envelope(
    spec,
    *,
    raw_snapshot: dict,
    workflow_id: str,
    total_jobs: int,
    parent_run_id: str | None,
    trigger_depth: int,
) -> dict:
    workspace_ref = _workspace_ref_from_spec(spec)
    runtime_profile_ref = _runtime_profile_ref_from_spec(spec)
    envelope = {
        "name": getattr(spec, "name", ""),
        "workflow_id": workflow_id,
        "phase": getattr(spec, "phase", ""),
        "total_jobs": total_jobs,
        "outcome_goal": getattr(spec, "outcome_goal", ""),
        "output_dir": getattr(spec, "output_dir", "") or "",
        "parent_run_id": parent_run_id,
        "trigger_depth": trigger_depth,
        "verify_refs": getattr(spec, "verify_refs", []),
        "spec_snapshot": raw_snapshot,
        "workspace_ref": workspace_ref,
        "runtime_profile_ref": runtime_profile_ref,
    }
    if isinstance(raw_snapshot, dict):
        adoption_key = str(raw_snapshot.get("queue_id") or "").strip()
        if adoption_key:
            envelope["adoption_key"] = adoption_key
    return envelope


def _job_touch_entries(job: dict) -> list[dict[str, str]]:
    return _json_loads_maybe(job.get("touch_keys"), []) or []


def _touches_conflict(candidate_entries: list[dict[str, str]], active_entries: list[dict[str, str]]) -> bool:
    for candidate in candidate_entries:
        candidate_key = str(candidate.get("key", ""))
        candidate_mode = str(candidate.get("mode", _WRITE_MODE))
        if not candidate_key:
            continue
        for active in active_entries:
            if candidate_key != str(active.get("key", "")):
                continue
            active_mode = str(active.get("mode", _WRITE_MODE))
            if candidate_mode == _READ_ONLY_MODE and active_mode == _READ_ONLY_MODE:
                continue
            return True
    return False


def _job_has_touch_conflict(conn: SyncPostgresConnection, candidate_job: dict) -> bool:
    candidate_entries = _job_touch_entries(candidate_job)
    if not candidate_entries:
        return False
    active_rows = conn.execute(
        """SELECT touch_keys
           FROM workflow_jobs
           WHERE status IN ('claimed', 'running')
             AND id != $1""",
        candidate_job["id"],
    )
    for row in active_rows or []:
        if _touches_conflict(candidate_entries, _json_loads_maybe(row.get("touch_keys"), []) or []):
            return True
    return False


def _route_candidates(job: dict) -> list[str]:
    chain = job.get("failover_chain") or []
    if isinstance(chain, str):
        chain = [chain]
    candidates = [str(candidate) for candidate in chain if candidate]
    if not candidates:
        primary = job.get("agent_slug")
        if primary:
            candidates = [str(primary)]
    return candidates


def _runtime_profile_ref_for_run(conn: SyncPostgresConnection, run_id: str) -> str | None:
    rows = conn.execute(
        """SELECT request_envelope->>'runtime_profile_ref' AS runtime_profile_ref
           FROM workflow_runs
           WHERE run_id = $1""",
        run_id,
    )
    if not rows:
        return None
    runtime_profile_ref = rows[0].get("runtime_profile_ref")
    if isinstance(runtime_profile_ref, str) and runtime_profile_ref.strip():
        return runtime_profile_ref.strip()
    return None


def _runtime_profile_ref_from_spec(spec) -> str | None:
    raw_snapshot = getattr(spec, "_raw", {})
    if isinstance(raw_snapshot, dict):
        runtime_profile_ref = raw_snapshot.get("runtime_profile_ref")
        if isinstance(runtime_profile_ref, str) and runtime_profile_ref.strip():
            return runtime_profile_ref.strip()
    runtime_profile_ref = getattr(spec, "runtime_profile_ref", None)
    if isinstance(runtime_profile_ref, str) and runtime_profile_ref.strip():
        return runtime_profile_ref.strip()
    return _DEFAULT_NATIVE_RUNTIME_PROFILE_REF


def _workspace_ref_from_spec(spec) -> str | None:
    raw_snapshot = getattr(spec, "_raw", {})
    if isinstance(raw_snapshot, dict):
        workspace_ref = raw_snapshot.get("workspace_ref")
        if isinstance(workspace_ref, str) and workspace_ref.strip():
            return workspace_ref.strip()
    workspace_ref = getattr(spec, "workspace_ref", None)
    if isinstance(workspace_ref, str) and workspace_ref.strip():
        return workspace_ref.strip()
    return _DEFAULT_NATIVE_WORKSPACE_REF


def _runtime_profile_admitted_route_candidates(
    conn: SyncPostgresConnection,
    *,
    runtime_profile_ref: str,
    candidates: list[str],
) -> list[str]:
    from registry.runtime_profile_admission import load_admitted_runtime_profile_candidates

    admitted_candidates = load_admitted_runtime_profile_candidates(
        conn,
        runtime_profile_ref=runtime_profile_ref,
    )
    admitted_slugs = {
        f"{candidate.provider_slug}/{candidate.model_slug}"
        for candidate in admitted_candidates
    }
    eligible_slugs = [candidate for candidate in candidates if candidate in admitted_slugs]
    if not eligible_slugs:
        raise RuntimeError(
            f"runtime profile {runtime_profile_ref!r} resolved to no admitted route candidates",
        )
    return eligible_slugs


def _db_admitted_route_candidates(
    conn: SyncPostgresConnection,
    *,
    run_id: str,
    candidates: list[str],
    enforce_runtime_profile: bool,
) -> tuple[list[str], dict[str, dict[str, object]]]:
    if not candidates:
        return [], {}

    candidate_rows = conn.execute(
        """SELECT candidate_ref,
                  provider_slug,
                  model_slug,
                  COALESCE(priority, 0) AS priority
           FROM provider_model_candidates
           WHERE status = 'active'
             AND (provider_slug || '/' || model_slug) = ANY($1::text[])""",
        candidates,
    )
    by_slug: dict[str, dict[str, object]] = {}
    for row in candidate_rows or []:
        slug = f"{row['provider_slug']}/{row['model_slug']}"
        by_slug[slug] = dict(row)

    active_slugs = [candidate for candidate in candidates if candidate in by_slug]
    if not active_slugs:
        return list(candidates), {}

    if not enforce_runtime_profile:
        return active_slugs, by_slug

    runtime_profile_ref = _runtime_profile_ref_for_run(conn, run_id)
    if not runtime_profile_ref:
        return active_slugs, by_slug
    eligible_slugs = _runtime_profile_admitted_route_candidates(
        conn,
        runtime_profile_ref=runtime_profile_ref,
        candidates=active_slugs,
    )
    return eligible_slugs, by_slug


def _active_provider_load(conn: SyncPostgresConnection) -> dict[str, int]:
    rows = conn.execute(
        """SELECT split_part(COALESCE(resolved_agent, agent_slug), '/', 1) AS provider_slug,
                  COUNT(*) AS active_count
           FROM workflow_jobs
           WHERE status IN ('claimed', 'running')
           GROUP BY 1""",
    )
    return {str(row["provider_slug"]): int(row["active_count"]) for row in rows or []}


def _failure_zone_lookup(conn: SyncPostgresConnection) -> dict[str, str]:
    rows = conn.execute(
        """SELECT category, zone
           FROM failure_category_zones""",
    )
    return {
        str(row["category"]): str(row["zone"])
        for row in rows or []
        if row.get("category")
    }


def _blocked_candidates_for_task(
    conn: SyncPostgresConnection,
    task_type: str,
    candidates: list[str],
) -> set[str]:
    """Return the subset of *candidates* explicitly blocked (permitted=false)."""
    if not task_type or not candidates:
        return set()
    rows = conn.execute(
        """SELECT provider_slug || '/' || model_slug AS slug
           FROM task_type_routing
           WHERE task_type = $1
             AND permitted = false
             AND (provider_slug || '/' || model_slug) = ANY($2::text[])""",
        task_type,
        candidates,
    )
    return {row["slug"] for row in rows or []}


def _task_route_candidate_meta(
    conn: SyncPostgresConnection,
    *,
    task_type: str,
    candidates: list[str],
) -> dict[str, dict[str, float | int]]:
    rows = conn.execute(
        """SELECT provider_slug,
                  model_slug,
                  rank,
                  route_health_score,
                  consecutive_internal_failures
           FROM task_type_routing
           WHERE task_type = $1
             AND permitted = true
             AND (provider_slug || '/' || model_slug) = ANY($2::text[])""",
        task_type,
        candidates,
    )
    return {
        f"{row['provider_slug']}/{row['model_slug']}": {
            "rank": int(row.get("rank") or 99),
            "route_health_score": float(row.get("route_health_score") or 0.65),
            "consecutive_internal_failures": int(row.get("consecutive_internal_failures") or 0),
        }
        for row in rows or []
    }


def _select_claim_route(conn: SyncPostgresConnection, job: dict) -> str:
    # If a retry/failover decision already wrote the next agent into resolved_agent,
    # honor it directly rather than re-scoring. This prevents the infinite loop where
    # health-score re-ranking always picks the same rate-limited model.
    pre_resolved = str(job.get("resolved_agent") or "").strip()
    if pre_resolved and job.get("last_error_code"):
        chain = _route_candidates(job)
        if pre_resolved in chain:
            return pre_resolved

    candidates = _route_candidates(job)
    if not candidates:
        return str(job.get("agent_slug", ""))

    route_task_type = str(job.get("route_task_type") or "").strip()
    admitted_candidates, candidate_meta = _db_admitted_route_candidates(
        conn,
        run_id=str(job.get("run_id", "")),
        candidates=candidates,
        enforce_runtime_profile=bool(route_task_type),
    )
    provider_load = _active_provider_load(conn)
    available = admitted_candidates or candidates
    if not route_task_type:
        return min(
            available,
            key=lambda candidate: (
                provider_load.get(candidate.split("/", 1)[0], 0),
                int(candidate_meta.get(candidate, {}).get("priority", 0)),
                candidates.index(candidate),
            ),
        )

    route_meta = _task_route_candidate_meta(
        conn,
        task_type=route_task_type,
        candidates=available,
    )
    if not route_meta:
        # No permitted models found for this task type among candidates.
        # Filter available down to models that are explicitly permitted
        # (or have no routing row at all — legacy models without explicit
        # task_type_routing entries are allowed through).
        blocked = _blocked_candidates_for_task(conn, route_task_type, available)
        fallback = [c for c in available if c not in blocked]
        if not fallback:
            fallback = available  # all blocked — degrade to original chain
        return min(
            fallback,
            key=lambda candidate: (
                provider_load.get(candidate.split("/", 1)[0], 0),
                int(candidate_meta.get(candidate, {}).get("priority", 0)),
                candidates.index(candidate),
            ),
        )

    from runtime.task_type_router import TaskTypeRouter

    route_policy = getattr(TaskTypeRouter(conn), "route_policy", None)

    # Small jitter so similarly-ranked models rotate across claims
    _EXPLORATION_JITTER = 0.04

    def _route_score(candidate: str) -> tuple[float, int]:
        meta = route_meta.get(candidate, {})
        health = float(meta.get("route_health_score") or 0.65)
        internal_failures = int(meta.get("consecutive_internal_failures") or 0)
        # Use the DB rank (so tied-rank models score equally) with
        # chain position as tiebreaker
        db_rank = int(meta.get("rank") or candidates.index(candidate) + 1)
        provider = candidate.split("/", 1)[0]
        load_penalty = min(1.0, provider_load.get(provider, 0) / max(len(available), 1))
        priority_penalty_step = (
            route_policy.claim_priority_penalty_step
            if route_policy is not None
            else 0.01
        )
        internal_failure_penalty_step = (
            route_policy.claim_internal_failure_penalty_step
            if route_policy is not None
            else 0.08
        )
        priority_penalty = min(0.2, int(candidate_meta.get(candidate, {}).get("priority", 0)) * priority_penalty_step)
        route_health_weight = (
            route_policy.claim_route_health_weight
            if route_policy is not None
            else 0.55
        )
        route_rank_weight = (
            route_policy.claim_rank_weight
            if route_policy is not None
            else 0.30
        )
        route_load_weight = (
            route_policy.claim_load_weight
            if route_policy is not None
            else 0.15
        )
        jitter = random.uniform(0, _EXPLORATION_JITTER)
        score = (
            (health * route_health_weight)
            + ((1.0 / max(db_rank, 1)) * route_rank_weight)
            + ((1.0 - load_penalty) * route_load_weight)
            + jitter
            - min(0.25, internal_failures * internal_failure_penalty_step)
            - priority_penalty
        )
        return (score, -candidates.index(candidate))

    return max(available, key=_route_score)
