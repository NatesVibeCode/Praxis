from __future__ import annotations

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import json
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator

from runtime.primitive_contracts import bug_query_default_open_only_backlog
from runtime.quality_views import load_failure_category_zones
from runtime.queue_admission import (
    DEFAULT_QUEUE_CRITICAL_THRESHOLD,
    DEFAULT_QUEUE_WARNING_THRESHOLD,
    query_queue_depth_snapshot,
)
from runtime.receipt_store import list_receipts, receipt_stats
from surfaces.api.handlers import _bug_surface_contract as _bug_contract
from surfaces.api.handlers._shared import _bug_to_dict, _serialize
from surfaces.api.operator_read import NativeOperatorQueryFrontdoor


def _resolved_env(subsystems: Any) -> dict[str, str] | None:
    env = getattr(subsystems, "_postgres_env", None)
    return env() if callable(env) else None


def _operator_view_payload(view: object) -> dict[str, Any]:
    if is_dataclass(view):
        payload = asdict(view)
    elif isinstance(view, dict):
        payload = dict(view)
    else:
        payload = {"value": view}
    return _serialize(payload)


def _queue_depth_snapshot(pg: Any) -> dict[str, Any]:
    warning_threshold = DEFAULT_QUEUE_WARNING_THRESHOLD
    critical_threshold = DEFAULT_QUEUE_CRITICAL_THRESHOLD
    if pg is None or not hasattr(pg, "execute"):
        return {
            "queue_depth": 0,
            "queue_depth_status": "unknown",
            "queue_depth_pending": 0,
            "queue_depth_ready": 0,
            "queue_depth_claimed": 0,
            "queue_depth_running": 0,
            "queue_depth_total": 0,
            "queue_depth_warning_threshold": warning_threshold,
            "queue_depth_critical_threshold": critical_threshold,
            "queue_depth_utilization_pct": 0.0,
            "queue_depth_error": "pg connection unavailable",
        }
    try:
        snapshot = query_queue_depth_snapshot(
            pg,
            warning_threshold=warning_threshold,
            critical_threshold=critical_threshold,
        )
        return {
            "queue_depth": snapshot.total_queued,
            "queue_depth_status": snapshot.status,
            "queue_depth_pending": snapshot.pending,
            "queue_depth_ready": snapshot.ready,
            "queue_depth_claimed": snapshot.claimed,
            "queue_depth_running": snapshot.running,
            "queue_depth_total": snapshot.total_queued,
            "queue_depth_warning_threshold": warning_threshold,
            "queue_depth_critical_threshold": critical_threshold,
            "queue_depth_utilization_pct": snapshot.utilization_pct,
            "queue_depth_error": None,
        }
    except Exception as exc:
        return {
            "queue_depth": 0,
            "queue_depth_status": "unknown",
            "queue_depth_pending": 0,
            "queue_depth_ready": 0,
            "queue_depth_claimed": 0,
            "queue_depth_running": 0,
            "queue_depth_total": 0,
            "queue_depth_warning_threshold": warning_threshold,
            "queue_depth_critical_threshold": critical_threshold,
            "queue_depth_utilization_pct": 0.0,
            "queue_depth_error": str(exc),
        }


def _parse_bug_status(bt_mod: Any, raw_status: object):
    return _bug_contract.parse_bug_status(bt_mod, raw_status)


def _parse_bug_severity(bt_mod: Any, raw_severity: object):
    return _bug_contract.parse_bug_severity(bt_mod, raw_severity)


def _parse_bug_category(bt_mod: Any, raw_category: object):
    return _bug_contract.parse_bug_category(bt_mod, raw_category)


class QueryOperatorStatusSnapshot(BaseModel):
    since_hours: int = 24

    @field_validator("since_hours", mode="before")
    @classmethod
    def _normalize_since_hours(cls, value: object) -> int:
        if value in (None, ""):
            return 24
        try:
            return max(1, int(value))
        except (TypeError, ValueError) as exc:
            raise ValueError("since_hours must be an integer") from exc


class QueryOperatorIssueBacklog(BaseModel):
    limit: int = 50
    open_only: bool = Field(default_factory=bug_query_default_open_only_backlog)
    status: str | None = None

    @field_validator("limit", mode="before")
    @classmethod
    def _normalize_limit(cls, value: object) -> int:
        if value in (None, ""):
            return 50
        try:
            return max(1, int(value))
        except (TypeError, ValueError) as exc:
            raise ValueError("limit must be an integer") from exc

    @field_validator("status", mode="before")
    @classmethod
    def _normalize_optional_status(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("status must be a non-empty string when provided")
        return value.strip()


class QueryReplayReadyBugs(BaseModel):
    limit: int = 50
    refresh_backfill: bool = False

    @field_validator("limit", mode="before")
    @classmethod
    def _normalize_limit(cls, value: object) -> int:
        if value in (None, ""):
            return 50
        try:
            return max(1, int(value))
        except (TypeError, ValueError) as exc:
            raise ValueError("limit must be an integer") from exc

    @model_validator(mode="after")
    def _reject_refresh_backfill(self) -> "QueryReplayReadyBugs":
        if self.refresh_backfill:
            raise ValueError(
                "replay_ready_bugs is read-only; use maintenance backfill instead"
            )
        return self


class QueryOperatorGraphProjection(BaseModel):
    as_of: datetime | None = None


class QueryRunScopedOperatorView(BaseModel):
    run_id: str

    @field_validator("run_id", mode="before")
    @classmethod
    def _normalize_run_id(cls, value: object) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("run_id is required")
        return value.strip()


def handle_query_operator_status_snapshot(
    query: QueryOperatorStatusSnapshot,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    totals = receipt_stats(since_hours=query.since_hours, conn=conn).get("totals", {})
    receipt_count = int(totals.get("receipts") or 0)
    records = (
        list_receipts(limit=receipt_count, since_hours=query.since_hours)
        if receipt_count > 0
        else []
    )
    total = len(records)
    succeeded = sum(1 for record in records if record.status == "succeeded")
    failure_counts: dict[str, int] = {}
    for record in records:
        if record.failure_code:
            failure_counts[record.failure_code] = (
                failure_counts.get(record.failure_code, 0) + 1
            )
    top_failures = dict(
        sorted(failure_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    )
    pass_rate = (succeeded / total) if total > 0 else 0.0

    zone_counts: dict[str, int] = {}
    category_counts: dict[str, int] = {}
    zone_authority_ready = True
    zone_authority_error: str | None = None
    try:
        zone_lookup = load_failure_category_zones(conn, consumer="operator.status_snapshot")
        for record in records:
            payload = record.to_dict()
            failure_classification = payload.get("failure_classification")
            if isinstance(failure_classification, dict):
                category = str(failure_classification.get("category") or "").strip()
            else:
                category = str(payload.get("failure_category") or "").strip()
            if not category:
                continue
            category_counts[category] = category_counts.get(category, 0) + 1
            zone = zone_lookup.get(category, "internal")
            zone_counts[zone] = zone_counts.get(zone, 0) + 1
    except Exception as exc:
        zone_authority_ready = False
        zone_authority_error = str(exc)

    external_failures = zone_counts.get("external", 0)
    adjusted_denominator = total - external_failures
    adjusted_pass_rate = None
    if zone_authority_ready:
        adjusted_pass_rate = (
            (succeeded / adjusted_denominator) if adjusted_denominator > 0 else 0.0
        )

    in_flight = []
    in_flight_authority_ready = True
    in_flight_error: str | None = None
    try:
        running_rows = conn.execute(
            """SELECT run_id, current_state, requested_at, request_envelope
            FROM workflow_runs
            WHERE current_state = 'running'
            ORDER BY requested_at DESC LIMIT 10""",
        )
        now = datetime.now(timezone.utc)
        for row in running_rows:
            envelope = row["request_envelope"]
            if isinstance(envelope, str):
                try:
                    envelope = json.loads(envelope)
                except json.JSONDecodeError:
                    envelope = {}
            if not isinstance(envelope, dict):
                envelope = {}
            outbox_count = conn.execute(
                "SELECT COUNT(*) as cnt FROM workflow_outbox WHERE run_id = $1 AND authority_table = 'receipts'",
                row["run_id"],
            )
            completed = int(outbox_count[0]["cnt"]) if outbox_count else 0
            elapsed = None
            if row["requested_at"]:
                elapsed = round((now - row["requested_at"]).total_seconds(), 1)
            total_jobs = int(envelope.get("total_jobs") or 0)
            if total_jobs > 0 and completed >= total_jobs:
                continue
            in_flight.append(
                {
                    "run_id": row["run_id"],
                    "workflow_name": envelope.get("name") or envelope.get("spec_name", ""),
                    "total_jobs": total_jobs,
                    "completed_jobs": completed,
                    "elapsed_seconds": elapsed,
                }
            )
    except Exception as exc:
        in_flight_authority_ready = False
        in_flight_error = str(exc)

    result: dict[str, Any] = {
        "total_workflows": total,
        "pass_rate": round(pass_rate, 4),
        "adjusted_pass_rate": (
            round(adjusted_pass_rate, 4)
            if adjusted_pass_rate is not None
            else None
        ),
        "failure_breakdown": {
            "by_zone": zone_counts,
            "by_category": category_counts,
        },
        "top_failure_codes": top_failures,
        "since_hours": query.since_hours,
        "zone_authority_ready": zone_authority_ready,
        "in_flight_authority_ready": in_flight_authority_ready,
        "in_flight_error": in_flight_error,
        "observability_state": (
            "ready"
            if zone_authority_ready and in_flight_authority_ready
            else "degraded"
        ),
        **_queue_depth_snapshot(conn),
    }
    errors: list[dict[str, str]] = []
    if zone_authority_error:
        errors.append({
            "code": "failure_category_zones_lookup_failed",
            "message": zone_authority_error,
        })
    if in_flight_error:
        errors.append({
            "code": "in_flight_workflows_lookup_failed",
            "message": in_flight_error,
        })
    if errors:
        result["errors"] = errors
    if in_flight:
        result["in_flight_workflows"] = in_flight
    return result


def handle_query_operator_issue_backlog(
    query: QueryOperatorIssueBacklog,
    _subsystems: Any,
) -> dict[str, Any]:
    backlog = NativeOperatorQueryFrontdoor().query_issue_backlog(
        limit=query.limit,
        open_only=query.open_only,
        status=query.status,
    )
    return {
        "view": "issue_backlog",
        "requires": {
            "runtime": "sync_postgres",
            "driver": "postgres",
        },
        **backlog,
    }


def handle_query_replay_ready_bugs(
    query: QueryReplayReadyBugs,
    subsystems: Any,
) -> dict[str, Any]:
    bt = subsystems.get_bug_tracker()
    bt_mod = subsystems.get_bug_tracker_mod()
    payload = _bug_contract.list_bugs_payload(
        bt=bt,
        bt_mod=bt_mod,
        body={
            "limit": query.limit,
            "open_only": bug_query_default_open_only_backlog(),
            "replay_ready_only": True,
        },
        serialize_bug=_bug_to_dict,
        default_limit=query.limit,
        include_replay_details=True,
        parse_status=_parse_bug_status,
        parse_severity=_parse_bug_severity,
        parse_category=_parse_bug_category,
    )
    return {
        "view": "replay_ready_bugs",
        "requires": {
            "runtime": "sync_postgres",
            "driver": "postgres",
        },
        "bugs": payload.get("bugs", []),
        "count": payload.get("count", 0),
        "returned_count": payload.get("returned_count", 0),
        "limit": query.limit,
    }


async def handle_query_operator_graph_projection(
    query: QueryOperatorGraphProjection,
    subsystems: Any,
) -> dict[str, Any]:
    from observability.operator_topology import load_operator_graph_projection
    from storage.postgres import connect_workflow_database

    as_of = query.as_of or datetime.now(timezone.utc)
    conn = await connect_workflow_database(env=_resolved_env(subsystems))
    try:
        read_model = await load_operator_graph_projection(conn, as_of=as_of)
    finally:
        await conn.close()
    return {
        "view": "operator_graph",
        "requires": {
            "runtime": "sync_postgres",
            "driver": "postgres",
        },
        "payload": _operator_view_payload(read_model),
    }


async def _load_run_scoped_view(
    *,
    query: QueryRunScopedOperatorView,
    subsystems: Any,
    view_kind: str,
) -> dict[str, Any]:
    from observability import (
        cutover_scoreboard_run,
        graph_lineage_run,
        graph_topology_run,
        inspect_run,
        load_native_operator_support,
        operator_status_run,
        render_cutover_scoreboard,
        render_operator_status,
    )
    from storage.postgres import PostgresEvidenceReader
    from surfaces.cli.render import render_graph_lineage, render_graph_topology

    env = _resolved_env(subsystems)
    evidence_reader = PostgresEvidenceReader(env=env)
    canonical_evidence = await evidence_reader.load_evidence_timeline(run_id=query.run_id)
    inspection = inspect_run(
        run_id=query.run_id,
        canonical_evidence=canonical_evidence,
    )
    support = await load_native_operator_support(run_id=query.run_id, env=env)

    if view_kind == "status":
        read_model = operator_status_run(
            run_id=query.run_id,
            canonical_evidence=canonical_evidence,
            support=support,
        )
        return {
            "view": view_kind,
            "run_id": query.run_id,
            "requires": {
                "runtime": "sync_postgres",
                "driver": "postgres",
            },
            "payload": _operator_view_payload(read_model),
            "rendered": render_operator_status(read_model),
        }

    if view_kind == "graph":
        read_model = graph_topology_run(
            run_id=query.run_id,
            canonical_evidence=canonical_evidence,
        )
        return {
            "view": view_kind,
            "run_id": query.run_id,
            "requires": {
                "runtime": "sync_postgres",
                "driver": "postgres",
            },
            "payload": _operator_view_payload(read_model),
            "rendered": render_graph_topology(read_model),
        }

    if view_kind == "lineage":
        read_model = graph_lineage_run(
            run_id=query.run_id,
            canonical_evidence=canonical_evidence,
            operator_frame_source=inspection.operator_frame_source,
            operator_frames=inspection.operator_frames,
        )
        return {
            "view": view_kind,
            "run_id": query.run_id,
            "requires": {
                "runtime": "sync_postgres",
                "driver": "postgres",
            },
            "payload": _operator_view_payload(read_model),
            "rendered": render_graph_lineage(read_model),
        }

    from surfaces.api import frontdoor

    status_payload = frontdoor.status(run_id=query.run_id)
    read_model = cutover_scoreboard_run(
        run_id=query.run_id,
        canonical_evidence=canonical_evidence,
        status_snapshot=status_payload.get("run"),
        support=support,
    )
    return {
        "view": "scoreboard",
        "run_id": query.run_id,
        "requires": {
            "runtime": "sync_postgres",
            "driver": "postgres",
        },
        "payload": _operator_view_payload(read_model),
        "rendered": render_cutover_scoreboard(read_model),
    }


async def handle_query_run_status_view(
    query: QueryRunScopedOperatorView,
    subsystems: Any,
) -> dict[str, Any]:
    return await _load_run_scoped_view(
        query=query,
        subsystems=subsystems,
        view_kind="status",
    )


async def handle_query_run_scoreboard_view(
    query: QueryRunScopedOperatorView,
    subsystems: Any,
) -> dict[str, Any]:
    return await _load_run_scoped_view(
        query=query,
        subsystems=subsystems,
        view_kind="scoreboard",
    )


async def handle_query_run_graph_view(
    query: QueryRunScopedOperatorView,
    subsystems: Any,
) -> dict[str, Any]:
    return await _load_run_scoped_view(
        query=query,
        subsystems=subsystems,
        view_kind="graph",
    )


async def handle_query_run_lineage_view(
    query: QueryRunScopedOperatorView,
    subsystems: Any,
) -> dict[str, Any]:
    return await _load_run_scoped_view(
        query=query,
        subsystems=subsystems,
        view_kind="lineage",
    )


__all__ = [
    "QueryOperatorGraphProjection",
    "QueryOperatorIssueBacklog",
    "QueryOperatorStatusSnapshot",
    "QueryReplayReadyBugs",
    "QueryRunScopedOperatorView",
    "handle_query_operator_graph_projection",
    "handle_query_operator_issue_backlog",
    "handle_query_operator_status_snapshot",
    "handle_query_replay_ready_bugs",
    "handle_query_run_graph_view",
    "handle_query_run_lineage_view",
    "handle_query_run_scoreboard_view",
    "handle_query_run_status_view",
]
