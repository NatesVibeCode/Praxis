"""Runtime truth, firecheck, and remediation planning.

This module owns the read-side substrate for "is work actually able to fire?"
It deliberately reports observed authority and typed blockers instead of
asking an LLM to infer platform state from scattered logs.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from functools import lru_cache
import json
import os
from pathlib import Path, PurePosixPath
import subprocess
from typing import Any

from runtime.failure_classifier import classify_failure
from runtime.queue_admission import (
    DEFAULT_QUEUE_CRITICAL_THRESHOLD,
    DEFAULT_QUEUE_WARNING_THRESHOLD,
    query_queue_depth_snapshot,
)
from storage.postgres.connection import (
    WORKFLOW_POOL_MAX_SIZE_ENV,
    WORKFLOW_POOL_MIN_SIZE_ENV,
)
from runtime.workspace_paths import repo_root


_PROVIDER_SLOT_STALE_SECONDS = 600
_DB_CONNECTION_WARNING_UTILIZATION_PCT = 80.0
_DB_CONNECTION_CRITICAL_UTILIZATION_PCT = 90.0


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _row_value(row: Any, key: str, default: Any = None) -> Any:
    if isinstance(row, Mapping):
        return row.get(key, default)
    try:
        return row[key]
    except Exception:
        return default


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return default


def _as_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


@lru_cache(maxsize=4)
def _workspace_authority_admitted_paths(root: str) -> frozenset[str] | None:
    try:
        result = subprocess.run(
            ["git", "-C", root, "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            capture_output=True,
            timeout=30,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    paths: set[str] = set()
    for raw in (result.stdout or b"").split(b"\0"):
        if not raw:
            continue
        paths.add(PurePosixPath(raw.decode("utf-8", errors="surrogateescape")).as_posix())
    return frozenset(paths)


def _workspace_manifest_filter_active(audit: Mapping[str, Any]) -> bool:
    return any(
        audit.get(key) not in (None, "", [], {})
        for key in (
            "workspace_snapshot_ref",
            "workspace_materialization",
            "hydrated_file_count",
        )
    )


def _manifest_path_admitted_by_workspace_authority(
    path: object,
    *,
    admitted_paths: frozenset[str] | None,
) -> bool:
    if admitted_paths is None:
        return True
    normalized = PurePosixPath(str(path or "").strip().replace("\\", "/").lstrip("./")).as_posix()
    if not normalized or normalized == ".":
        return False
    if Path(normalized).is_absolute():
        return True
    if normalized in admitted_paths:
        return True
    prefix = f"{normalized.rstrip('/')}/"
    return any(candidate.startswith(prefix) for candidate in admitted_paths)


def _actionable_missing_manifest_paths(
    audit: Mapping[str, Any],
    missing: list[Any],
    *,
    admitted_paths: frozenset[str] | None,
) -> list[Any]:
    if not _workspace_manifest_filter_active(audit):
        return list(missing)
    return [
        path
        for path in missing
        if _manifest_path_admitted_by_workspace_authority(
            path,
            admitted_paths=admitted_paths,
        )
    ]


def _iso(value: Any) -> str | None:
    formatter = getattr(value, "isoformat", None)
    if callable(formatter):
        return str(formatter())
    return str(value) if value else None


def _execute_rows(conn: Any, query: str, *args: Any) -> tuple[list[Any], str | None]:
    if conn is None or not hasattr(conn, "execute"):
        return [], "pg connection unavailable"
    try:
        rows = conn.execute(query, *args)
    except Exception as exc:
        return [], str(exc)
    return list(rows or []), None


def _provider_control_plane_by_provider(conn: Any) -> dict[str, dict[str, Any]]:
    from registry.native_runtime_profile_sync import default_native_runtime_profile_ref

    try:
        runtime_profile_ref = default_native_runtime_profile_ref(conn)
    except Exception:
        return {}
    rows, _error = _execute_rows(
        conn,
        """
        SELECT
            provider_slug,
            BOOL_OR(COALESCE(control_state = 'on', FALSE)) AS any_control_on,
            BOOL_OR(COALESCE(control_state = 'off', FALSE)) AS any_control_off
        FROM private_model_access_control_matrix
        WHERE runtime_profile_ref = $1
        GROUP BY provider_slug
        """,
        runtime_profile_ref,
    )
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        provider_slug = str(_row_value(row, "provider_slug") or "").strip()
        if not provider_slug:
            continue
        result[provider_slug] = {
            "any_control_on": bool(_row_value(row, "any_control_on")),
            "any_control_off": bool(_row_value(row, "any_control_off")),
        }
    return result


def _blocker(
    code: str,
    severity: str,
    message: str,
    *,
    evidence: Mapping[str, Any] | None = None,
    remediation_type: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "code": code,
        "severity": severity,
        "message": message,
    }
    if evidence:
        payload["evidence"] = dict(evidence)
    if remediation_type:
        payload["remediation_type"] = remediation_type
    return payload


def _pool_config_snapshot() -> dict[str, Any]:
    return {
        "min_size": _as_int(os.environ.get(WORKFLOW_POOL_MIN_SIZE_ENV), 1),
        "max_size": _as_int(os.environ.get(WORKFLOW_POOL_MAX_SIZE_ENV), 8),
        "min_size_env": WORKFLOW_POOL_MIN_SIZE_ENV,
        "max_size_env": WORKFLOW_POOL_MAX_SIZE_ENV,
    }


def _db_authority_snapshot(conn: Any) -> dict[str, Any]:
    rows, error = _execute_rows(conn, "SELECT 1 AS ok")
    pool_config = _pool_config_snapshot()
    return {
        "status": "ok" if not error and bool(rows) else "unavailable",
        "error": error,
        "pool_config": pool_config,
        "connection_pressure": _db_connection_pressure_snapshot(conn, pool_config=pool_config),
    }


def _db_connection_pressure_snapshot(
    conn: Any,
    *,
    pool_config: Mapping[str, Any],
) -> dict[str, Any]:
    rows, error = _execute_rows(
        conn,
        """
        SELECT
            current_setting('max_connections')::int AS max_connections,
            COUNT(*) AS cluster_connections,
            COUNT(*) FILTER (WHERE state = 'active') AS cluster_active_connections,
            COUNT(*) FILTER (WHERE state = 'idle') AS cluster_idle_connections,
            COUNT(*) FILTER (WHERE datname = current_database()) AS database_connections,
            COUNT(*) FILTER (
                WHERE datname = current_database() AND state = 'active'
            ) AS database_active_connections,
            COUNT(*) FILTER (
                WHERE datname = current_database() AND state = 'idle'
            ) AS database_idle_connections
        FROM pg_stat_activity
        """,
    )
    if error or not rows:
        return {
            "status": "unknown",
            "error": error,
        }
    row = rows[0]
    max_connections = _as_int(_row_value(row, "max_connections"))
    cluster_connections = _as_int(_row_value(row, "cluster_connections"))
    cluster_active_connections = _as_int(_row_value(row, "cluster_active_connections"))
    cluster_idle_connections = _as_int(_row_value(row, "cluster_idle_connections"))
    database_connections = _as_int(_row_value(row, "database_connections"))
    database_active_connections = _as_int(_row_value(row, "database_active_connections"))
    database_idle_connections = _as_int(_row_value(row, "database_idle_connections"))
    free_connection_slots = (
        max(0, max_connections - cluster_connections) if max_connections > 0 else 0
    )
    utilization_pct = (
        round((cluster_connections / max_connections) * 100.0, 1)
        if max_connections > 0
        else 0.0
    )
    configured_pool_headroom_slots = max(1, _as_int(pool_config.get("max_size"), 1))
    status = "ok"
    if (
        max_connections <= 0
        or free_connection_slots <= 0
        or utilization_pct >= _DB_CONNECTION_CRITICAL_UTILIZATION_PCT
    ):
        status = "critical"
    elif (
        free_connection_slots <= configured_pool_headroom_slots
        or utilization_pct >= _DB_CONNECTION_WARNING_UTILIZATION_PCT
    ):
        status = "warning"
    return {
        "status": status,
        "error": None,
        "max_connections": max_connections,
        "cluster_connections": cluster_connections,
        "cluster_active_connections": cluster_active_connections,
        "cluster_idle_connections": cluster_idle_connections,
        "database_connections": database_connections,
        "database_active_connections": database_active_connections,
        "database_idle_connections": database_idle_connections,
        "free_connection_slots": free_connection_slots,
        "utilization_pct": utilization_pct,
        "warning_utilization_pct": _DB_CONNECTION_WARNING_UTILIZATION_PCT,
        "critical_utilization_pct": _DB_CONNECTION_CRITICAL_UTILIZATION_PCT,
        "configured_pool_headroom_slots": configured_pool_headroom_slots,
    }


def _queue_snapshot(conn: Any) -> dict[str, Any]:
    try:
        snapshot = query_queue_depth_snapshot(
            conn,
            warning_threshold=DEFAULT_QUEUE_WARNING_THRESHOLD,
            critical_threshold=DEFAULT_QUEUE_CRITICAL_THRESHOLD,
        )
        return {
            "status": snapshot.status,
            "pending": snapshot.pending,
            "ready": snapshot.ready,
            "claimed": snapshot.claimed,
            "running": snapshot.running,
            "total_queued": snapshot.total_queued,
            "warning_threshold": snapshot.warning_threshold,
            "critical_threshold": snapshot.critical_threshold,
            "utilization_pct": snapshot.utilization_pct,
            "error": None,
        }
    except Exception as exc:
        return {
            "status": "unknown",
            "pending": 0,
            "ready": 0,
            "claimed": 0,
            "running": 0,
            "total_queued": 0,
            "warning_threshold": DEFAULT_QUEUE_WARNING_THRESHOLD,
            "critical_threshold": DEFAULT_QUEUE_CRITICAL_THRESHOLD,
            "utilization_pct": 0.0,
            "error": str(exc),
        }


def _worker_snapshot(conn: Any, *, heartbeat_fresh_seconds: int) -> dict[str, Any]:
    rows, error = _execute_rows(
        conn,
        f"""
        SELECT
            status,
            COUNT(*) AS count,
            COUNT(*) FILTER (
                WHERE heartbeat_at IS NOT NULL
                  AND heartbeat_at >= NOW() - INTERVAL '{int(heartbeat_fresh_seconds)} seconds'
            ) AS fresh_heartbeats,
            COUNT(*) FILTER (
                WHERE heartbeat_at IS NULL
                  AND status IN ('claimed', 'running')
            ) AS missing_heartbeats,
            MIN(heartbeat_at) AS oldest_heartbeat_at,
            MAX(heartbeat_at) AS newest_heartbeat_at
        FROM workflow_jobs
        WHERE status IN ('pending', 'ready', 'claimed', 'running')
        GROUP BY status
        ORDER BY status
        """,
    )
    by_status: dict[str, dict[str, Any]] = {}
    fresh_total = 0
    missing_total = 0
    active_total = 0
    for row in rows:
        status = str(_row_value(row, "status") or "unknown")
        count = _as_int(_row_value(row, "count"))
        fresh = _as_int(_row_value(row, "fresh_heartbeats"))
        missing = _as_int(_row_value(row, "missing_heartbeats"))
        by_status[status] = {
            "count": count,
            "fresh_heartbeats": fresh,
            "missing_heartbeats": missing,
            "oldest_heartbeat_at": _iso(_row_value(row, "oldest_heartbeat_at")),
            "newest_heartbeat_at": _iso(_row_value(row, "newest_heartbeat_at")),
        }
        fresh_total += fresh
        missing_total += missing
        if status in {"claimed", "running"}:
            active_total += count
    return {
        "status": "unknown" if error else "ok",
        "heartbeat_fresh_seconds": heartbeat_fresh_seconds,
        "fresh_heartbeats": fresh_total,
        "missing_heartbeats": missing_total,
        "active_jobs": active_total,
        "by_status": by_status,
        "error": error,
    }


def _provider_slot_snapshot(conn: Any) -> dict[str, Any]:
    control_plane = _provider_control_plane_by_provider(conn)
    rows, error = _execute_rows(
        conn,
        """
        SELECT
            provider_slug,
            max_concurrent,
            active_slots,
            cost_weight_default,
            updated_at,
            EXTRACT(EPOCH FROM (NOW() - updated_at)) AS age_seconds
        FROM provider_concurrency
        ORDER BY provider_slug
        """,
    )
    providers: list[dict[str, Any]] = []
    stale: list[dict[str, Any]] = []
    saturated: list[dict[str, Any]] = []
    for row in rows:
        provider_slug = str(_row_value(row, "provider_slug") or "")
        max_concurrent = _as_int(_row_value(row, "max_concurrent"))
        active_slots = _as_float(_row_value(row, "active_slots"))
        available = max(0.0, float(max_concurrent) - active_slots)
        age_seconds = _as_float(_row_value(row, "age_seconds"))
        provider_control = control_plane.get(provider_slug, {})
        provider_runnable = provider_control.get("any_control_on")
        provider_disabled = bool(provider_control.get("any_control_off")) and not bool(provider_runnable)
        provider = {
            "provider_slug": provider_slug,
            "max_concurrent": max_concurrent,
            "active_slots": active_slots,
            "available": available,
            "cost_weight_default": _as_float(_row_value(row, "cost_weight_default")),
            "updated_at": _iso(_row_value(row, "updated_at")),
            "age_seconds": round(age_seconds, 1),
            "stale": active_slots > 0 and age_seconds > _PROVIDER_SLOT_STALE_SECONDS,
            "at_capacity": max_concurrent > 0 and available <= 0,
            "provider_runnable": provider_runnable,
            "provider_disabled": provider_disabled,
        }
        providers.append(provider)
        if provider["stale"]:
            stale.append(provider)
        if provider["at_capacity"] and not provider_disabled:
            saturated.append(provider)
    return {
        "status": "unknown" if error else "ok",
        "providers": providers,
        "stale_slots": stale,
        "saturated_providers": saturated,
        "stale_after_seconds": _PROVIDER_SLOT_STALE_SECONDS,
        "error": error,
    }


def _host_resource_snapshot(conn: Any) -> dict[str, Any]:
    rows, error = _execute_rows(
        conn,
        """
        SELECT
            lease_id,
            holder_id,
            resource_key,
            acquired_at,
            expires_at,
            EXTRACT(EPOCH FROM (expires_at - NOW())) AS ttl_seconds
        FROM execution_leases
        WHERE resource_key LIKE 'host_resource:%'
        ORDER BY expires_at ASC
        LIMIT 50
        """,
    )
    leases: list[dict[str, Any]] = []
    expired = 0
    for row in rows:
        ttl_seconds = _as_float(_row_value(row, "ttl_seconds"))
        lease = {
            "lease_id": str(_row_value(row, "lease_id") or ""),
            "holder_id": str(_row_value(row, "holder_id") or ""),
            "resource_key": str(_row_value(row, "resource_key") or ""),
            "acquired_at": _iso(_row_value(row, "acquired_at")),
            "expires_at": _iso(_row_value(row, "expires_at")),
            "ttl_seconds": round(ttl_seconds, 1),
            "expired": ttl_seconds <= 0,
        }
        leases.append(lease)
        if lease["expired"]:
            expired += 1
    return {
        "status": "unknown" if error else "ok",
        "active_host_resource_leases": len(leases),
        "expired_host_resource_leases": expired,
        "leases": leases,
        "error": error,
    }


def _docker_snapshot() -> dict[str, Any]:
    try:
        from runtime.sandbox_runtime import _docker_available

        available = bool(_docker_available())
        error = None
    except Exception as exc:
        available = False
        error = str(exc)
    return {
        "status": "ok" if available else "unavailable",
        "available": available,
        "error": error,
    }


def _manifest_audit_snapshot(
    conn: Any,
    *,
    run_id: str | None,
    since_minutes: int,
    limit: int,
) -> dict[str, Any]:
    params: tuple[Any, ...]
    where = "outputs ? 'workspace_manifest_audit'"
    if run_id:
        where += " AND run_id = $1 AND finished_at >= NOW() - ($2 || ' minutes')::INTERVAL"
        params = (run_id, str(int(since_minutes)), limit)
        limit_placeholder = "$3"
    else:
        where += " AND finished_at >= NOW() - ($1 || ' minutes')::INTERVAL"
        params = (str(int(since_minutes)), limit)
        limit_placeholder = "$2"
    rows, error = _execute_rows(
        conn,
        f"""
        SELECT
            receipt_id,
            run_id,
            node_id,
            status,
            failure_code,
            finished_at,
            outputs -> 'workspace_manifest_audit' AS workspace_manifest_audit
        FROM receipts
        WHERE {where}
        ORDER BY finished_at DESC
        LIMIT {limit_placeholder}
        """,
        *params,
    )
    latest_success_index_by_job: dict[tuple[str, str], tuple[int, str]] = {}
    for index, row in enumerate(rows):
        row_run_id = str(_row_value(row, "run_id") or "")
        node_id = str(_row_value(row, "node_id") or "")
        if not row_run_id or not node_id:
            continue
        if str(_row_value(row, "status") or "") != "succeeded":
            continue
        latest_success_index_by_job.setdefault(
            (row_run_id, node_id),
            (index, str(_row_value(row, "receipt_id") or "")),
        )

    records: list[dict[str, Any]] = []
    total_missing = 0
    outside_observed = 0
    admitted_paths = _workspace_authority_admitted_paths(str(repo_root()))
    for index, row in enumerate(rows):
        audit = _as_mapping(_row_value(row, "workspace_manifest_audit"))
        missing = list(audit.get("missing_intended_paths") or [])
        workspace_actionable_missing = _actionable_missing_manifest_paths(
            audit,
            missing,
            admitted_paths=admitted_paths,
        )
        workspace_actionable_missing_set = set(workspace_actionable_missing)
        non_actionable_missing = [
            path for path in missing if path not in workspace_actionable_missing_set
        ]
        observed = list(audit.get("observed_file_read_refs") or [])
        observed_mode = str(audit.get("observed_file_read_mode") or "")
        status = str(_row_value(row, "status") or "")
        row_run_id = str(_row_value(row, "run_id") or "")
        node_id = str(_row_value(row, "node_id") or "")
        success_index, success_receipt_id = latest_success_index_by_job.get(
            (row_run_id, node_id),
            (-1, ""),
        )
        superseded_by_success = success_index >= 0 and success_index < index
        actionable_missing = [
            path
            for path in workspace_actionable_missing
            if not superseded_by_success
            and (
                status != "succeeded"
                or (
                    observed_mode != "provider_output_path_mentions"
                    and observed_mode
                    and path in observed
                )
            )
        ]
        total_missing += len(actionable_missing)
        outside_observed += len(list(audit.get("observed_outside_manifest") or []))
        records.append(
            {
                "receipt_id": str(_row_value(row, "receipt_id") or ""),
                "run_id": row_run_id,
                "job_label": node_id,
                "status": status,
                "failure_code": str(_row_value(row, "failure_code") or ""),
                "finished_at": _iso(_row_value(row, "finished_at")),
                "missing_intended_paths": missing,
                "actionable_missing_intended_paths": actionable_missing,
                "non_actionable_missing_intended_paths": non_actionable_missing,
                "superseded_by_success": superseded_by_success,
                "superseded_by_receipt_id": success_receipt_id if superseded_by_success else None,
                "hydrated_manifest_paths": list(audit.get("hydrated_manifest_paths") or []),
                "observed_file_read_refs": observed,
                "observed_file_read_mode": observed_mode,
            }
        )
    return {
        "status": "unknown" if error else "ok",
        "records": records,
        "records_with_audit": len(records),
        "missing_intended_path_count": total_missing,
        "observed_outside_manifest_count": outside_observed,
        "error": error,
    }


def _recent_failure_snapshot(
    conn: Any,
    *,
    run_id: str | None,
    since_minutes: int,
    limit: int,
) -> dict[str, Any]:
    params: tuple[Any, ...]
    where = "status = 'failed'"
    if run_id:
        where += " AND run_id = $1 AND finished_at >= NOW() - ($2 || ' minutes')::INTERVAL"
        params = (run_id, str(int(since_minutes)), limit)
        limit_placeholder = "$3"
    else:
        where += " AND finished_at >= NOW() - ($1 || ' minutes')::INTERVAL"
        params = (str(int(since_minutes)), limit)
        limit_placeholder = "$2"
    rows, error = _execute_rows(
        conn,
        f"""
        SELECT receipt_id, run_id, node_id, failure_code, outputs, finished_at
        FROM receipts
        WHERE {where}
        ORDER BY finished_at DESC
        LIMIT {limit_placeholder}
        """,
        *params,
    )
    failures: list[dict[str, Any]] = []
    for row in rows:
        outputs = _as_mapping(_row_value(row, "outputs"))
        failure_code = str(_row_value(row, "failure_code") or "")
        failure_type = classify_runtime_failure(
            failure_code=failure_code,
            stderr=str(outputs.get("stderr") or outputs.get("error") or ""),
            outputs=outputs,
        )
        failures.append(
            {
                "receipt_id": str(_row_value(row, "receipt_id") or ""),
                "run_id": str(_row_value(row, "run_id") or ""),
                "job_label": str(_row_value(row, "node_id") or ""),
                "failure_code": failure_code,
                "failure_type": failure_type,
                "finished_at": _iso(_row_value(row, "finished_at")),
                "remediation": remediation_plan_for_failure(failure_type),
            }
        )
    return {
        "status": "unknown" if error else "ok",
        "failures": failures,
        "error": error,
    }


def classify_runtime_failure(
    *,
    failure_code: str | None = None,
    stderr: str | None = None,
    outputs: Mapping[str, Any] | None = None,
) -> str:
    outputs_dict = dict(outputs or {})
    manifest_audit = _as_mapping(outputs_dict.get("workspace_manifest_audit"))
    missing_intended_paths = list(manifest_audit.get("missing_intended_paths") or [])
    actionable_missing = _actionable_missing_manifest_paths(
        manifest_audit,
        missing_intended_paths,
        admitted_paths=_workspace_authority_admitted_paths(str(repo_root())),
    )
    if actionable_missing:
        return "context_not_hydrated"
    code = str(failure_code or "").strip()
    lowered = f"{code}\n{stderr or ''}".lower()
    if "workspace" in lowered and "container" in lowered and "workdir" in lowered:
        return "verifier_workspace_mismatch"
    if code in {"host_resource_capacity", "host_resource_admission_unavailable", "provider.capacity"}:
        return code
    if "too many connections" in lowered or "remaining connection slots" in lowered:
        return "db_pool_pressure"
    if "not logged in" in lowered or "unauthorized" in lowered or "401" in lowered:
        return "credential_error"
    classification = classify_failure(code or None, outputs=outputs_dict).to_dict()
    return str(classification.get("category") or "unknown")


_REMEDIATION_REGISTRY: dict[str, dict[str, Any]] = {
    "context_not_hydrated": {
        "tier": "safe_auto",
        "action": "block_retry_until_manifest_hydration_passes",
        "reason": "Declared read context did not hydrate into the sandbox.",
        "evidence_required": [
            "workspace_manifest_audit.intended_manifest_paths",
            "workspace_manifest_audit.hydrated_manifest_paths",
            "workspace_manifest_audit.missing_intended_paths",
        ],
        "retry_delta_required": "repair read-scope hydration or regenerate the execution manifest",
        "approval_required": False,
    },
    "verifier_workspace_mismatch": {
        "tier": "safe_auto",
        "action": "translate_verifier_workspace_to_container_path",
        "reason": "Verifier is checking the wrong workspace coordinate system.",
        "evidence_required": ["verifier inputs", "container workspace path"],
        "retry_delta_required": "verifier workdir translation changed",
        "approval_required": False,
    },
    "host_resource_capacity": {
        "tier": "controlled_auto",
        "action": "retry_same_job_after_host_capacity_frees",
        "reason": "Local sandbox slots are saturated.",
        "evidence_required": ["host_resource_admission claim snapshot"],
        "retry_delta_required": "capacity window changed; no spec mutation",
        "approval_required": False,
    },
    "host_resource_admission_unavailable": {
        "tier": "safe_auto",
        "action": "restore_db_backed_admission_then_retry_one_proof",
        "reason": "Host resource admission authority is unavailable.",
        "evidence_required": ["db_authority", "execution_leases readback"],
        "retry_delta_required": "admission authority reachable",
        "approval_required": False,
    },
    "db_pool_pressure": {
        "tier": "safe_auto",
        "action": "reduce_pool_minimum_and_recheck_connection_pressure",
        "reason": "Postgres has insufficient free connection slots.",
        "evidence_required": ["db_authority error", "pool_config"],
        "retry_delta_required": "pool pressure changed",
        "approval_required": False,
    },
    "provider.capacity": {
        "tier": "controlled_auto",
        "action": "retry_or_failover_after_provider_slot_changes",
        "reason": "Provider route is at concurrency capacity.",
        "evidence_required": ["provider slot snapshot"],
        "retry_delta_required": "provider slot or route changed",
        "approval_required": False,
    },
    "credential_error": {
        "tier": "human_gated",
        "action": "repair_provider_credentials",
        "reason": "Credential failures require operator-owned secrets.",
        "evidence_required": ["cli auth doctor", "provider error"],
        "retry_delta_required": "credential authority changed",
        "approval_required": True,
    },
    "sandbox_error": {
        "tier": "controlled_auto",
        "action": "run_sandbox_doctor_before_retry",
        "reason": "Sandbox or CLI execution failed.",
        "evidence_required": ["docker availability", "sandbox stderr", "image authority"],
        "retry_delta_required": "sandbox/image/tooling diagnosis changed",
        "approval_required": False,
    },
    "unknown": {
        "tier": "human_gated",
        "action": "inspect_receipt_before_retry",
        "reason": "Failure is not typed enough for automatic remediation.",
        "evidence_required": ["receipt outputs", "stderr", "failure_code"],
        "retry_delta_required": "typed root cause identified",
        "approval_required": True,
    },
}


def remediation_plan_for_failure(failure_type: str | None) -> dict[str, Any]:
    key = str(failure_type or "unknown").strip() or "unknown"
    plan = dict(_REMEDIATION_REGISTRY.get(key) or _REMEDIATION_REGISTRY["unknown"])
    plan["failure_type"] = key
    return plan


def build_remediation_plan(
    conn: Any,
    *,
    failure_type: str | None = None,
    failure_code: str | None = None,
    stderr: str | None = None,
    run_id: str | None = None,
) -> dict[str, Any]:
    latest_failure: dict[str, Any] | None = None
    if run_id and not failure_type and not failure_code:
        failures = _recent_failure_snapshot(conn, run_id=run_id, since_minutes=24 * 60, limit=1)
        failure_rows = failures.get("failures") if isinstance(failures, Mapping) else None
        if isinstance(failure_rows, list) and failure_rows:
            latest_failure = dict(failure_rows[0])
            failure_type = str(latest_failure.get("failure_type") or "")
            failure_code = str(latest_failure.get("failure_code") or "")
    resolved_type = failure_type or classify_runtime_failure(
        failure_code=failure_code,
        stderr=stderr,
        outputs={},
    )
    return {
        "view": "remediation_plan",
        "run_id": run_id,
        "failure_code": failure_code,
        "failure_type": resolved_type,
        "latest_failure": latest_failure,
        "plan": remediation_plan_for_failure(resolved_type),
    }


def build_runtime_truth_snapshot(
    conn: Any,
    *,
    run_id: str | None = None,
    since_minutes: int = 60,
    heartbeat_fresh_seconds: int = 60,
    manifest_audit_limit: int = 10,
) -> dict[str, Any]:
    db = _db_authority_snapshot(conn)
    queue = _queue_snapshot(conn)
    workers = _worker_snapshot(conn, heartbeat_fresh_seconds=heartbeat_fresh_seconds)
    providers = _provider_slot_snapshot(conn)
    host_resources = _host_resource_snapshot(conn)
    docker = _docker_snapshot()
    manifest_audit = _manifest_audit_snapshot(
        conn,
        run_id=run_id,
        since_minutes=since_minutes,
        limit=manifest_audit_limit,
    )
    recent_failures = _recent_failure_snapshot(
        conn,
        run_id=run_id,
        since_minutes=since_minutes,
        limit=5,
    )

    blockers = truth_blockers(
        db=db,
        queue=queue,
        workers=workers,
        providers=providers,
        host_resources=host_resources,
        docker=docker,
        manifest_audit=manifest_audit,
    )
    return {
        "view": "runtime_truth_snapshot",
        "authority": "workflow_runtime_truth",
        "as_of": _utc_now_iso(),
        "run_id": run_id,
        "since_minutes": since_minutes,
        "truth_state": "blocked"
        if any(item["severity"] == "critical" for item in blockers)
        else ("degraded" if blockers else "ready"),
        "blockers": blockers,
        "db_authority": db,
        "queue": queue,
        "workers": workers,
        "provider_slots": providers,
        "host_resources": host_resources,
        "docker": docker,
        "manifest_audit": manifest_audit,
        "recent_failures": recent_failures,
    }


def truth_blockers(
    *,
    db: Mapping[str, Any],
    queue: Mapping[str, Any],
    workers: Mapping[str, Any],
    providers: Mapping[str, Any],
    host_resources: Mapping[str, Any],
    docker: Mapping[str, Any],
    manifest_audit: Mapping[str, Any],
) -> list[dict[str, Any]]:
    blockers: list[dict[str, Any]] = []
    db_pressure = _as_mapping(db.get("connection_pressure"))
    if db.get("status") != "ok":
        blockers.append(
            _blocker(
                "db_authority_unavailable",
                "critical",
                "Workflow DB authority is unavailable.",
                evidence={"error": db.get("error"), "pool_config": db.get("pool_config")},
                remediation_type="db_pool_pressure",
            )
        )
    elif db_pressure.get("status") in {"warning", "critical"}:
        free_slots = _as_int(db_pressure.get("free_connection_slots"))
        blockers.append(
            _blocker(
                "db_pool_pressure",
                "critical" if db_pressure.get("status") == "critical" else "warning",
                "Postgres connection headroom is below the workflow pool safety margin.",
                evidence={
                    "free_connection_slots": free_slots,
                    "max_connections": db_pressure.get("max_connections"),
                    "cluster_connections": db_pressure.get("cluster_connections"),
                    "utilization_pct": db_pressure.get("utilization_pct"),
                    "configured_pool_headroom_slots": db_pressure.get(
                        "configured_pool_headroom_slots"
                    ),
                    "pool_config": db.get("pool_config"),
                },
                remediation_type="db_pool_pressure",
            )
        )
    if queue.get("status") == "critical":
        blockers.append(
            _blocker(
                "queue_depth_critical",
                "critical",
                "Workflow queue depth is at or above the critical threshold.",
                evidence={"total_queued": queue.get("total_queued")},
            )
        )
    queued = _as_int(queue.get("pending")) + _as_int(queue.get("ready"))
    if queued > 0 and _as_int(workers.get("fresh_heartbeats")) <= 0:
        blockers.append(
            _blocker(
                "queued_without_fresh_worker_heartbeat",
                "critical",
                "Queued work exists, but no fresh worker heartbeat proves it can be claimed.",
                evidence={"queued": queued, "workers": workers.get("by_status")},
            )
        )
    if providers.get("stale_slots"):
        blockers.append(
            _blocker(
                "provider_slots_stale",
                "warning",
                "Provider slots appear stale.",
                evidence={"stale_slots": providers.get("stale_slots")},
                remediation_type="provider.capacity",
            )
        )
    if providers.get("saturated_providers"):
        blockers.append(
            _blocker(
                "provider_capacity",
                "warning",
                "At least one provider is at capacity.",
                evidence={"saturated_providers": providers.get("saturated_providers")},
                remediation_type="provider.capacity",
            )
        )
    if _as_int(host_resources.get("expired_host_resource_leases")) > 0:
        blockers.append(
            _blocker(
                "expired_host_resource_leases",
                "warning",
                "Expired host-resource leases are still visible.",
                evidence={"leases": host_resources.get("leases")},
                remediation_type="host_resource_admission_unavailable",
            )
        )
    if docker.get("status") != "ok":
        blockers.append(
            _blocker(
                "docker_unavailable",
                "critical",
                "Docker is unavailable for local sandbox execution.",
                evidence={"error": docker.get("error")},
                remediation_type="sandbox_error",
            )
        )
    if _as_int(manifest_audit.get("missing_intended_path_count")) > 0:
        blockers.append(
            _blocker(
                "context_not_hydrated",
                "critical",
                "Recent execution receipts show intended manifest paths missing from hydrated sandbox context.",
                evidence={"records": manifest_audit.get("records")},
                remediation_type="context_not_hydrated",
            )
        )
    return blockers


def build_firecheck(
    conn: Any,
    *,
    run_id: str | None = None,
    since_minutes: int = 60,
    heartbeat_fresh_seconds: int = 60,
) -> dict[str, Any]:
    snapshot = build_runtime_truth_snapshot(
        conn,
        run_id=run_id,
        since_minutes=since_minutes,
        heartbeat_fresh_seconds=heartbeat_fresh_seconds,
    )
    blockers = list(snapshot.get("blockers") or [])
    critical = [item for item in blockers if item.get("severity") == "critical"]
    next_actions: list[dict[str, Any]] = []
    for blocker in blockers:
        remediation_type = blocker.get("remediation_type") or blocker.get("code")
        next_actions.append(
            {
                "blocker_code": blocker.get("code"),
                "severity": blocker.get("severity"),
                "plan": remediation_plan_for_failure(str(remediation_type)),
            }
        )
    if not blockers:
        next_actions.append(
            {
                "blocker_code": None,
                "severity": "info",
                "plan": {
                    "failure_type": "none",
                    "tier": "safe_auto",
                    "action": "launch_one_proof_then_scale",
                    "reason": "No launch blockers observed.",
                    "evidence_required": ["fresh firecheck snapshot"],
                    "retry_delta_required": "not applicable",
                    "approval_required": False,
                },
            }
        )
    return {
        "view": "firecheck",
        "can_fire": not critical,
        "fire_state": "blocked" if critical else ("degraded" if blockers else "ready"),
        "run_id": run_id,
        "blockers": blockers,
        "next_actions": next_actions,
        "summary": {
            "db": snapshot["db_authority"]["status"],
            "db_connection_pressure": snapshot["db_authority"]["connection_pressure"]["status"],
            "db_free_connection_slots": snapshot["db_authority"]["connection_pressure"].get(
                "free_connection_slots"
            ),
            "queue": snapshot["queue"]["status"],
            "fresh_worker_heartbeats": snapshot["workers"]["fresh_heartbeats"],
            "docker": snapshot["docker"]["status"],
            "manifest_missing_paths": snapshot["manifest_audit"]["missing_intended_path_count"],
        },
        "snapshot": snapshot,
    }


__all__ = [
    "build_firecheck",
    "build_remediation_plan",
    "build_runtime_truth_snapshot",
    "classify_runtime_failure",
    "remediation_plan_for_failure",
    "truth_blockers",
]
