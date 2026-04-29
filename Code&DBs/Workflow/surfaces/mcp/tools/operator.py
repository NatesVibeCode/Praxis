"""Catalog-backed operator MCP tools."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, Callable

from runtime.primitive_contracts import bug_query_default_open_only_list

from ..subsystems import _subs

_OPERATOR_READ_LIMIT_MAX = 500
_OPERATOR_BOOLEAN_VALUES = frozenset(
    {"true", "false", "1", "0", "yes", "no", "on", "off", "y", "n"}
)
_OPERATOR_BOOLEAN_TRUE = frozenset({"1", "true", "yes", "on", "y"})


def _parse_iso_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty ISO-8601 datetime string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO-8601 datetime string") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include a timezone offset")
    return parsed


def _structured_runtime_error(exc: Exception, *, operation_name: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "ok": False,
        "error": str(exc),
        "error_code": getattr(exc, "reason_code", f"{operation_name}.failed"),
        "operation_name": operation_name,
    }
    details = getattr(exc, "details", None)
    if isinstance(details, dict) and details:
        payload["details"] = details
    return payload


def _structured_input_error(exc: Exception, *, operation_name: str) -> dict[str, Any]:
    return {
        "ok": False,
        "error": str(exc),
        "error_code": f"{operation_name}.invalid_input",
        "operation_name": operation_name,
    }


def _parse_positive_int(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a positive integer")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text.isdigit():
            raise ValueError(f"{field_name} must be a positive integer")
        parsed = int(text)
    else:
        raise ValueError(f"{field_name} must be a positive integer")
    if parsed <= 0:
        raise ValueError(f"{field_name} must be greater than zero")
    return parsed


def _parse_bool(value: Any, *, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in _OPERATOR_BOOLEAN_VALUES:
            return normalized in _OPERATOR_BOOLEAN_TRUE
    raise ValueError(f"{field_name} must be a boolean")


def _bounded_limit(
    params: dict[str, Any],
    *,
    default: int,
    maximum: int = _OPERATOR_READ_LIMIT_MAX,
) -> int:
    raw = params.get("limit", default)
    if raw is None or raw == "":
        raw = default
    limit = _parse_positive_int(raw, field_name="limit")
    return min(limit, maximum)


def _execute_catalog_tool(
    *,
    operation_name: str,
    payload: dict[str, Any],
    fallback: Callable[[], dict[str, Any]] | None = None,
) -> dict:
    try:
        result = execute_operation_from_subsystems(
            _subs,
            operation_name=operation_name,
            payload=payload,
        )
        if fallback is not None and isinstance(result, dict) and result.get("ok") is False:
            return fallback()
        if isinstance(result, dict) and "ok" not in result:
            result["ok"] = True
        return result
    except Exception as exc:
        if fallback is not None:
            return fallback()
        return _structured_runtime_error(exc, operation_name=operation_name)


def _optional_sequence_payload(params: dict[str, Any], field_name: str) -> Any:
    """Avoid leaking ``None`` into tuple-backed operator command contracts."""

    value = params.get(field_name)
    return [] if value is None else value


def execute_operation_from_subsystems(*args: Any, **kwargs: Any) -> Any:
    from runtime.operation_catalog_gateway import (
        execute_operation_from_subsystems as _execute_operation_from_subsystems,
    )

    return _execute_operation_from_subsystems(*args, **kwargs)


def _bug_query_default_open_only_backlog() -> bool:
    from runtime.primitive_contracts import bug_query_default_open_only_backlog

    return bug_query_default_open_only_backlog()


def tool_praxis_status_snapshot(params: dict) -> dict:
    """Read the canonical workflow status snapshot."""

    def _fallback() -> dict[str, Any]:
        from runtime.operations.queries.operator_observability import (
            QueryOperatorStatusSnapshot,
            handle_query_operator_status_snapshot,
        )

        since_hours = _parse_positive_int(
            params.get("since_hours", 24),
            field_name="since_hours",
        )
        return handle_query_operator_status_snapshot(
            QueryOperatorStatusSnapshot(since_hours=since_hours),
            _subs,
        )

    return _execute_catalog_tool(
        operation_name="operator.status_snapshot",
        payload={"since_hours": params.get("since_hours", 24)},
        fallback=_fallback,
    )


def tool_praxis_runtime_truth_snapshot(params: dict) -> dict:
    """Read the runtime truth snapshot."""

    operation_name = "operator.runtime_truth_snapshot"
    try:
        since_minutes = min(
            _parse_positive_int(params.get("since_minutes", 60), field_name="since_minutes"),
            24 * 60,
        )
        heartbeat_fresh_seconds = min(
            _parse_positive_int(
                params.get("heartbeat_fresh_seconds", 60),
                field_name="heartbeat_fresh_seconds",
            ),
            24 * 60,
        )
        manifest_audit_limit = min(
            _parse_positive_int(
                params.get("manifest_audit_limit", 10),
                field_name="manifest_audit_limit",
            ),
            100,
        )
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "run_id": str(params.get("run_id") or "").strip() or None,
            "since_minutes": since_minutes,
            "heartbeat_fresh_seconds": heartbeat_fresh_seconds,
            "manifest_audit_limit": manifest_audit_limit,
        },
    )


def tool_praxis_firecheck(params: dict) -> dict:
    """Preflight whether workflow work can actually fire now."""

    operation_name = "operator.firecheck"
    try:
        since_minutes = min(
            _parse_positive_int(params.get("since_minutes", 60), field_name="since_minutes"),
            24 * 60,
        )
        heartbeat_fresh_seconds = min(
            _parse_positive_int(
                params.get("heartbeat_fresh_seconds", 60),
                field_name="heartbeat_fresh_seconds",
            ),
            24 * 60,
        )
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "run_id": str(params.get("run_id") or "").strip() or None,
            "since_minutes": since_minutes,
            "heartbeat_fresh_seconds": heartbeat_fresh_seconds,
        },
    )


def tool_praxis_remediation_plan(params: dict) -> dict:
    """Read the remediation plan for a typed workflow failure."""

    return _execute_catalog_tool(
        operation_name="operator.remediation_plan",
        payload={
            "failure_type": str(params.get("failure_type") or "").strip() or None,
            "failure_code": str(params.get("failure_code") or "").strip() or None,
            "stderr": str(params.get("stderr") or "").strip() or None,
            "run_id": str(params.get("run_id") or "").strip() or None,
        },
    )


def tool_praxis_remediation_apply(params: dict) -> dict:
    """Apply guarded runtime remediation for a typed workflow failure."""

    operation_name = "operator.remediation_apply"
    try:
        stale_after_seconds = min(
            _parse_positive_int(
                params.get("stale_after_seconds", 600),
                field_name="stale_after_seconds",
            ),
            24 * 60 * 60,
        )
        dry_run = _parse_bool(params.get("dry_run", True), field_name="dry_run")
        confirm = _parse_bool(params.get("confirm", False), field_name="confirm")
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "failure_type": str(params.get("failure_type") or "").strip() or None,
            "failure_code": str(params.get("failure_code") or "").strip() or None,
            "blocker_code": str(params.get("blocker_code") or "").strip() or None,
            "stderr": str(params.get("stderr") or "").strip() or None,
            "run_id": str(params.get("run_id") or "").strip() or None,
            "provider_slug": str(params.get("provider_slug") or "").strip() or None,
            "stale_after_seconds": stale_after_seconds,
            "dry_run": dry_run,
            "confirm": confirm,
        },
    )


def tool_praxis_legal_tools(params: dict) -> dict:
    """Deprecated alias for praxis_next(action='unlock_frontier')."""

    operation_name = "operator.next"
    try:
        limit = _bounded_limit(params, default=20, maximum=100)
        include_blocked = _parse_bool(
            params.get("include_blocked", True),
            field_name="include_blocked",
        )
        include_mutating = _parse_bool(
            params.get("include_mutating", False),
            field_name="include_mutating",
        )
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)

    result = _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "action": "unlock_frontier",
            "detail": params.get("detail") or "standard",
            "intent": params.get("intent"),
            "run_id": params.get("run_id"),
            "state": params.get("state") or {},
            "allowed_tools": params.get("allowed_tools"),
            "include_blocked": include_blocked,
            "include_mutating": include_mutating,
            "limit": limit,
        },
    )
    if isinstance(result, dict):
        result["deprecated_alias"] = {
            "tool": "praxis_legal_tools",
            "replacement": "praxis_next",
            "replacement_input": {"action": "unlock_frontier"},
        }
        legality = result.get("tool_legality")
        if isinstance(legality, dict):
            for key in (
                "legal_action_count",
                "blocked_action_count",
                "legal_actions",
                "blocked_actions",
                "typed_gaps",
                "repair_actions",
                "state",
                "authority_sources",
            ):
                if key in legality:
                    result.setdefault(key, legality[key])
    return result


def tool_praxis_execution_proof(params: dict) -> dict:
    """Prove whether a run or trace anchor produced runtime execution evidence."""

    operation_name = "operator.execution_proof"
    try:
        stale_after_seconds = _parse_positive_int(
            params.get("stale_after_seconds", 180),
            field_name="stale_after_seconds",
        )
        include_trace = _parse_bool(
            params.get("include_trace", True),
            field_name="include_trace",
        )
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)

    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "run_id": params.get("run_id"),
            "receipt_id": params.get("receipt_id"),
            "event_id": params.get("event_id"),
            "correlation_id": params.get("correlation_id"),
            "bug_id": params.get("bug_id"),
            "stale_after_seconds": stale_after_seconds,
            "include_trace": include_trace,
        },
    )


def tool_praxis_orient(params: dict) -> dict:
    """Return the canonical orientation payload for a fresh agent or operator.

    Delegates to ``surfaces.api.handlers.workflow_admin._handle_orient``, the
    single HTTP authority behind POST /orient. One implementation, one shape,
    no drift between MCP and HTTP consumers.
    """

    from surfaces.api.handlers.workflow_admin import _handle_orient

    body = dict(params) if isinstance(params, dict) else {}
    return _handle_orient(_subs, body)


def tool_praxis_metrics_reset(params: dict) -> dict:
    """Reset observability metrics through explicit operator maintenance authority."""

    operation_name = "operator.metrics_reset"
    try:
        confirm = _parse_bool(params.get("confirm", False), field_name="confirm")
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)

    return _execute_catalog_tool(
        operation_name="operator.metrics_reset",
        payload={"confirm": confirm, "before_date": params.get("before_date")},
    )


def tool_praxis_bug_replay_provenance_backfill(params: dict) -> dict:
    """Backfill replay provenance from authoritative bug and receipt state."""

    operation_name = "operator.bug_replay_provenance_backfill"
    try:
        limit = _bounded_limit(params, default=50)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)

    try:
        open_only = _parse_bool(
            params.get(
                "open_only",
                _bug_query_default_open_only_backlog(),
            ),
            field_name="open_only",
        )
        receipt_limit = _parse_positive_int(
            params.get("receipt_limit", 1),
            field_name="receipt_limit",
        )
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    receipt_limit = min(receipt_limit, 500)

    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "limit": limit,
            "open_only": open_only,
            "receipt_limit": receipt_limit,
        },
    )


def tool_praxis_semantic_bridges_backfill(params: dict) -> dict:
    """Replay semantic bridges from canonical operator authority."""

    as_of = params.get("as_of")
    operation_name = "operator.semantic_bridges_backfill"
    try:
        include_object_relations = _parse_bool(
            params.get("include_object_relations", True),
            field_name="include_object_relations",
        )
        include_operator_decisions = _parse_bool(
            params.get("include_operator_decisions", True),
            field_name="include_operator_decisions",
        )
        include_roadmap_items = _parse_bool(
            params.get("include_roadmap_items", True),
            field_name="include_roadmap_items",
        )
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)

    return _execute_catalog_tool(
        operation_name="operator.semantic_bridges_backfill",
        payload={
            "include_object_relations": include_object_relations,
            "include_operator_decisions": include_operator_decisions,
            "include_roadmap_items": include_roadmap_items,
            "as_of": (
                _parse_iso_datetime(as_of, field_name="as_of")
                if as_of is not None
                else None
            ),
        },
    )


def tool_praxis_semantic_projection_refresh(params: dict) -> dict:
    """Refresh the semantic projection through explicit operator maintenance authority."""

    operation_name = "operator.semantic_projection_refresh"
    as_of = params.get("as_of")
    try:
        limit = _bounded_limit(params, default=100)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "limit": limit,
            "as_of": (
                _parse_iso_datetime(as_of, field_name="as_of")
                if as_of is not None
                else None
            ),
        },
    )


def tool_praxis_run_status(params: dict) -> dict:
    """Read one run-scoped operator status view."""

    return _execute_catalog_tool(
        operation_name="operator.run_status",
        payload={"run_id": params.get("run_id")},
    )


def tool_praxis_run_scoreboard(params: dict) -> dict:
    """Read one run-scoped cutover scoreboard."""

    return _execute_catalog_tool(
        operation_name="operator.run_scoreboard",
        payload={"run_id": params.get("run_id")},
    )


def tool_praxis_run_graph(params: dict) -> dict:
    """Read one run-scoped workflow graph."""

    return _execute_catalog_tool(
        operation_name="operator.run_graph",
        payload={"run_id": params.get("run_id")},
    )


def tool_praxis_graph_projection(params: dict) -> dict:
    """Read the cross-domain operator graph projection."""

    def _fallback() -> dict[str, Any]:
        from runtime.operations.queries.operator_observability import (
            QueryOperatorGraphProjection,
            handle_query_operator_graph_projection,
        )

        as_of_raw = params.get("as_of")
        as_of = (
            _parse_iso_datetime(as_of_raw, field_name="as_of")
            if as_of_raw is not None
            else None
        )
        return asyncio.run(
            handle_query_operator_graph_projection(
                QueryOperatorGraphProjection(as_of=as_of),
                _subs,
            )
        )

    as_of = params.get("as_of")
    return _execute_catalog_tool(
        operation_name="operator.graph_projection",
        payload={
            "as_of": (
                _parse_iso_datetime(as_of, field_name="as_of")
                if as_of is not None
                else None
            ),
        },
        fallback=_fallback,
    )


def tool_praxis_ui_experience_graph(params: dict) -> dict:
    """Read the LLM-facing app experience graph."""

    operation_name = "operator.ui_experience_graph"
    try:
        limit = _bounded_limit(params, default=80)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)

    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "focus": params.get("focus"),
            "surface_name": params.get("surface_name"),
            "limit": limit,
        },
    )


def tool_praxis_run_lineage(params: dict) -> dict:
    """Read one run-scoped lineage view."""

    return _execute_catalog_tool(
        operation_name="operator.run_lineage",
        payload={"run_id": params.get("run_id")},
    )


_RUN_ACTION_OPERATIONS: dict[str, str] = {
    "status": "operator.run_status",
    "scoreboard": "operator.run_scoreboard",
    "graph": "operator.run_graph",
    "lineage": "operator.run_lineage",
}


def tool_praxis_run(params: dict) -> dict:
    """Consolidated run-scoped operator view.

    Replaces ``praxis_run_status`` / ``_scoreboard`` / ``_graph`` /
    ``_lineage`` — those four tools were each a 4-line wrapper around
    the same gateway dispatch with a different operation_name. The old
    names continue to work as aliases for one window per the no-shims
    standing order. ``action`` is canonical; ``view`` is accepted as a
    selector alias so HTTP and MCP callers can point at the same CQRS
    authority without translating vocabulary by hand.
    """

    raw_action = str(params.get("action") or "").strip().lower()
    raw_view = str(params.get("view") or "").strip().lower()
    if raw_action and raw_view and raw_action != raw_view:
        return _structured_input_error(
            ValueError("action and view must match when both are provided"),
            operation_name="operator.run",
        )

    action = raw_action or raw_view or "status"
    operation = _RUN_ACTION_OPERATIONS.get(action)
    if operation is None:
        return {
            "ok": False,
            "error": (
                f"unknown action '{action}' — supported: "
                f"{sorted(_RUN_ACTION_OPERATIONS)}"
            ),
        }
    return _execute_catalog_tool(
        operation_name=operation,
        payload={"run_id": params.get("run_id")},
    )


def tool_praxis_trace(params: dict) -> dict:
    """Walk the cause tree for any anchor (receipt_id, event_id, correlation_id, run_id, bug_id)."""

    operation_name = "trace.walk"
    anchors = {
        "receipt_id": str(params.get("receipt_id") or "").strip() or None,
        "event_id": str(params.get("event_id") or "").strip() or None,
        "correlation_id": str(params.get("correlation_id") or "").strip() or None,
        "run_id": str(params.get("run_id") or "").strip() or None,
        "bug_id": str(params.get("bug_id") or "").strip() or None,
    }
    provided = [v for v in anchors.values() if v]
    if len(provided) != 1:
        return _structured_input_error(
            ValueError(
                "trace.walk requires exactly one of receipt_id, event_id, "
                "correlation_id, run_id, or bug_id"
            ),
            operation_name=operation_name,
        )
    payload: dict[str, Any] = {key: value for key, value in anchors.items() if value}
    return _execute_catalog_tool(operation_name=operation_name, payload=payload)


def tool_praxis_issue_backlog(params: dict) -> dict:
    """Read the canonical operator issue backlog."""

    operation_name = "operator.issue_backlog"
    try:
        limit = _bounded_limit(params, default=50)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    try:
        open_only = _parse_bool(
            params.get("open_only", _bug_query_default_open_only_backlog()),
            field_name="open_only",
        )
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "limit": limit,
            "open_only": open_only,
            "status": params.get("status"),
        },
    )


def tool_praxis_operator_ideas(params: dict) -> dict:
    """Record, resolve, promote, or list pre-commitment operator ideas."""

    operation_name = "operator.ideas"
    try:
        limit = _bounded_limit(params, default=50)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    try:
        open_only = _parse_bool(
            params.get("open_only", _bug_query_default_open_only_backlog()),
            field_name="open_only",
        )
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return execute_operation_from_subsystems(
        _subs,
        operation_name=operation_name,
        payload={
            "action": params.get("action", "list"),
            "idea_id": params.get("idea_id"),
            "idea_key": params.get("idea_key"),
            "title": params.get("title"),
            "summary": params.get("summary"),
            "source_kind": params.get("source_kind", "operator"),
            "source_ref": params.get("source_ref"),
            "owner_ref": params.get("owner_ref"),
            "decision_ref": params.get("decision_ref"),
            "status": params.get("status"),
            "resolution_summary": params.get("resolution_summary"),
            "roadmap_item_id": params.get("roadmap_item_id"),
            "promoted_by": params.get("promoted_by"),
            "opened_at": params.get("opened_at"),
            "resolved_at": params.get("resolved_at"),
            "promoted_at": params.get("promoted_at"),
            "created_at": params.get("created_at"),
            "updated_at": params.get("updated_at"),
            "idea_ids": _optional_sequence_payload(params, "idea_ids"),
            "open_only": open_only,
            "limit": limit,
        },
    )


def tool_praxis_replay_ready_bugs(params: dict) -> dict:
    """Read the replay-ready bug backlog."""

    operation_name = "operator.replay_ready_bugs"
    try:
        limit = _bounded_limit(params, default=50)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "limit": limit,
        },
    )


def tool_praxis_bug_triage_packet(params: dict) -> dict:
    """Read the LLM-oriented bug triage packet."""

    operation_name = "operator.bug_triage_packet"
    try:
        limit = _bounded_limit(params, default=50)
        open_only = _parse_bool(
            params.get("open_only", _bug_query_default_open_only_backlog()),
            field_name="open_only",
        )
        include_inactive = _parse_bool(
            params.get("include_inactive", False),
            field_name="include_inactive",
        )
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "limit": limit,
            "open_only": open_only,
            "classification": params.get("classification"),
            "include_inactive": include_inactive,
        },
    )


def tool_praxis_refactor_heatmap(params: dict) -> dict:
    """Read the ranked refactor heatmap."""

    operation_name = "operator.refactor_heatmap"
    try:
        limit = _bounded_limit(params, default=15, maximum=50)
        bug_limit = _bounded_limit(
            {"limit": params.get("bug_limit", 250)},
            default=250,
            maximum=1000,
        )
        include_tests = _parse_bool(
            params.get("include_tests", False),
            field_name="include_tests",
        )
        open_only = _parse_bool(
            params.get("open_only", True),
            field_name="open_only",
        )
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "limit": limit,
            "include_tests": include_tests,
            "include_domains": params.get("include_domains"),
            "bug_limit": bug_limit,
            "long_symbol_threshold": params.get("long_symbol_threshold", 120),
            "open_only": open_only,
        },
    )


def tool_praxis_operator_write(params: dict) -> dict:
    """Preview, validate, commit, update, retire, or re-parent roadmap rows."""

    operation_name = "operator.roadmap_write"
    action = str(params.get("action") or "").strip() or "preview"
    title = str(params.get("title") or "").strip()
    intent_brief = str(params.get("intent_brief") or "").strip()
    roadmap_item_id = str(params.get("roadmap_item_id") or "").strip() or None
    phase_order = str(params.get("phase_order") or "").strip() or None
    dry_run = params.get("dry_run")
    if not action:
        return _structured_input_error(
            ValueError("action is required and cannot be empty"), operation_name=operation_name
        )
    normalized_action = action.lower()
    if normalized_action in {"update", "retire", "re-parent", "reparent"}:
        if roadmap_item_id is None:
            return _structured_input_error(
                ValueError(f"roadmap_item_id is required for action='{action}'"),
                operation_name=operation_name,
            )
        if normalized_action in {"re-parent", "reparent"} and not str(
            params.get("parent_roadmap_item_id") or ""
        ).strip():
            return _structured_input_error(
                ValueError("parent_roadmap_item_id is required for action='re-parent'"),
                operation_name=operation_name,
            )
        if dry_run is None:
            dry_run = True
        else:
            try:
                dry_run = _parse_bool(dry_run, field_name="dry_run")
            except ValueError as exc:
                return _structured_input_error(exc, operation_name=operation_name)
    elif dry_run is not None:
        try:
            dry_run = _parse_bool(dry_run, field_name="dry_run")
        except ValueError as exc:
            return _structured_input_error(exc, operation_name=operation_name)
    if not title and roadmap_item_id is None:
        return _structured_input_error(
            ValueError(
                "title is required unless roadmap_item_id is provided for update"
            ),
            operation_name=operation_name,
        )
    if not intent_brief and roadmap_item_id is None:
        return _structured_input_error(
            ValueError(
                "intent_brief is required unless roadmap_item_id is provided for update"
            ),
            operation_name=operation_name,
        )

    return execute_operation_from_subsystems(
        _subs,
        operation_name=operation_name,
        payload={
            "action": action,
            "title": title or None,
            "intent_brief": intent_brief or None,
            "template": params.get("template", "single_capability"),
            "priority": params.get("priority", "p2"),
            "parent_roadmap_item_id": params.get("parent_roadmap_item_id"),
            "slug": params.get("slug"),
            "depends_on": _optional_sequence_payload(params, "depends_on"),
            "source_bug_id": params.get("source_bug_id"),
            "source_idea_id": params.get("source_idea_id"),
            "registry_paths": _optional_sequence_payload(params, "registry_paths"),
            "decision_ref": params.get("decision_ref"),
            "item_kind": params.get("item_kind"),
            "status": params.get("status"),
            "lifecycle": params.get("lifecycle"),
            "tier": params.get("tier"),
            "phase_ready": params.get("phase_ready"),
            "approval_tag": params.get("approval_tag"),
            "reference_doc": params.get("reference_doc"),
            "outcome_gate": params.get("outcome_gate"),
            "proof_kind": params.get("proof_kind"),
            "roadmap_item_id": roadmap_item_id,
            "phase_order": phase_order,
            "dry_run": dry_run,
        },
    )


def tool_praxis_operator_decisions(params: dict) -> dict:
    """Record or list canonical operator decisions through operator_decisions."""

    action = str(params.get("action") or "list").strip().lower()
    if action == "list":
        operation_name = "operator.decision_list"
        as_of = params.get("as_of")
        try:
            limit = _bounded_limit(params, default=100)
        except ValueError as exc:
            return _structured_input_error(exc, operation_name=operation_name)
        return execute_operation_from_subsystems(
            _subs,
            operation_name=operation_name,
            payload={
                "decision_kind": params.get("decision_kind"),
                "decision_scope_kind": params.get("decision_scope_kind"),
                "decision_scope_ref": params.get("decision_scope_ref"),
                "as_of": (
                    _parse_iso_datetime(as_of, field_name="as_of")
                    if as_of is not None
                    else None
                ),
                "limit": limit,
            },
        )
    if action != "record":
        return {"error": "Unknown action. Supported actions: list, record"}
    effective_from = params.get("effective_from")
    effective_to = params.get("effective_to")
    return execute_operation_from_subsystems(
        _subs,
        operation_name="operator.decision_record",
        payload={
            "decision_key": str(params.get("decision_key") or ""),
            "decision_kind": str(params.get("decision_kind") or ""),
            "decision_status": str(params.get("decision_status") or "decided"),
            "title": str(params.get("title") or ""),
            "rationale": str(params.get("rationale") or ""),
            "decided_by": str(params.get("decided_by") or ""),
            "decision_source": str(params.get("decision_source") or ""),
            "decision_scope_kind": params.get("decision_scope_kind"),
            "decision_scope_ref": params.get("decision_scope_ref"),
            "scope_clamp": params.get("scope_clamp"),
            "effective_from": (
                _parse_iso_datetime(effective_from, field_name="effective_from")
                if effective_from is not None
                else None
            ),
            "effective_to": (
                _parse_iso_datetime(effective_to, field_name="effective_to")
                if effective_to is not None
                else None
            ),
        },
    )


def tool_praxis_operator_relations(params: dict) -> dict:
    """Record canonical functional areas and cross-object semantic relations."""

    action = str(params.get("action") or "").strip().lower()
    if action == "record_functional_area":
        return execute_operation_from_subsystems(
            _subs,
            operation_name="operator.functional_area_record",
            payload={
                "area_slug": str(params.get("area_slug") or ""),
                "title": str(params.get("title") or ""),
                "summary": str(params.get("summary") or ""),
                "area_status": str(params.get("area_status") or "active"),
                "created_at": params.get("created_at"),
                "updated_at": params.get("updated_at"),
            },
        )
    if action == "record_relation":
        return execute_operation_from_subsystems(
            _subs,
            operation_name="operator.object_relation_record",
            payload={
                "relation_kind": str(params.get("relation_kind") or ""),
                "source_kind": str(params.get("source_kind") or ""),
                "source_ref": str(params.get("source_ref") or ""),
                "target_kind": str(params.get("target_kind") or ""),
                "target_ref": str(params.get("target_ref") or ""),
                "relation_status": str(params.get("relation_status") or "active"),
                "relation_metadata": params.get("relation_metadata"),
                "bound_by_decision_id": params.get("bound_by_decision_id"),
                "created_at": params.get("created_at"),
                "updated_at": params.get("updated_at"),
            },
        )
    return {
        "error": (
            "Unknown action. Supported actions: record_functional_area, record_relation"
        )
    }


def tool_praxis_semantic_assertions(params: dict) -> dict:
    """Register predicates, record or retract assertions, and query semantic authority."""

    action = str(params.get("action") or "list").strip().lower()
    if action == "list":
        operation_name = "semantic_assertions.list"
        as_of = params.get("as_of")
        try:
            active_only = _parse_bool(params.get("active_only", True), field_name="active_only")
        except ValueError as exc:
            return _structured_input_error(exc, operation_name=operation_name)
        try:
            limit = _bounded_limit(params, default=100)
        except ValueError as exc:
            return _structured_input_error(exc, operation_name=operation_name)
        return execute_operation_from_subsystems(
            _subs,
            operation_name=operation_name,
            payload={
                "predicate_slug": params.get("predicate_slug"),
                "subject_kind": params.get("subject_kind"),
                "subject_ref": params.get("subject_ref"),
                "object_kind": params.get("object_kind"),
                "object_ref": params.get("object_ref"),
                "source_kind": params.get("source_kind"),
                "source_ref": params.get("source_ref"),
                "active_only": active_only,
                "as_of": (
                    _parse_iso_datetime(as_of, field_name="as_of")
                    if as_of is not None
                    else None
                ),
                "limit": limit,
            },
        )
    if action == "register_predicate":
        created_at = params.get("created_at")
        updated_at = params.get("updated_at")
        return execute_operation_from_subsystems(
            _subs,
            operation_name="semantic_assertions.register_predicate",
            payload={
                "predicate_slug": str(params.get("predicate_slug") or ""),
                "subject_kind_allowlist": params.get("subject_kind_allowlist") or (),
                "object_kind_allowlist": params.get("object_kind_allowlist") or (),
                "cardinality_mode": str(params.get("cardinality_mode") or "many"),
                "predicate_status": str(params.get("predicate_status") or "active"),
                "description": params.get("description"),
                "created_at": (
                    _parse_iso_datetime(created_at, field_name="created_at")
                    if created_at is not None
                    else None
                ),
                "updated_at": (
                    _parse_iso_datetime(updated_at, field_name="updated_at")
                    if updated_at is not None
                    else None
                ),
            },
        )
    if action == "record_assertion":
        valid_from = params.get("valid_from")
        valid_to = params.get("valid_to")
        created_at = params.get("created_at")
        updated_at = params.get("updated_at")
        return execute_operation_from_subsystems(
            _subs,
            operation_name="semantic_assertions.record",
            payload={
                "predicate_slug": str(params.get("predicate_slug") or ""),
                "subject_kind": str(params.get("subject_kind") or ""),
                "subject_ref": str(params.get("subject_ref") or ""),
                "object_kind": str(params.get("object_kind") or ""),
                "object_ref": str(params.get("object_ref") or ""),
                "qualifiers_json": params.get("qualifiers_json"),
                "source_kind": str(params.get("source_kind") or ""),
                "source_ref": str(params.get("source_ref") or ""),
                "evidence_ref": params.get("evidence_ref"),
                "bound_decision_id": params.get("bound_decision_id"),
                "valid_from": (
                    _parse_iso_datetime(valid_from, field_name="valid_from")
                    if valid_from is not None
                    else None
                ),
                "valid_to": (
                    _parse_iso_datetime(valid_to, field_name="valid_to")
                    if valid_to is not None
                    else None
                ),
                "assertion_status": str(params.get("assertion_status") or "active"),
                "semantic_assertion_id": params.get("semantic_assertion_id"),
                "created_at": (
                    _parse_iso_datetime(created_at, field_name="created_at")
                    if created_at is not None
                    else None
                ),
                "updated_at": (
                    _parse_iso_datetime(updated_at, field_name="updated_at")
                    if updated_at is not None
                    else None
                ),
            },
        )
    if action == "retract_assertion":
        retracted_at = params.get("retracted_at")
        updated_at = params.get("updated_at")
        return execute_operation_from_subsystems(
            _subs,
            operation_name="semantic_assertions.retract",
            payload={
                "semantic_assertion_id": str(params.get("semantic_assertion_id") or ""),
                "retracted_at": (
                    _parse_iso_datetime(retracted_at, field_name="retracted_at")
                    if retracted_at is not None
                    else None
                ),
                "updated_at": (
                    _parse_iso_datetime(updated_at, field_name="updated_at")
                    if updated_at is not None
                    else None
                ),
            },
        )
    return {
        "error": (
            "Unknown action. Supported actions: "
            "list, register_predicate, record_assertion, retract_assertion"
        )
    }


def tool_praxis_operator_native_primary_cutover_gate(params: dict) -> dict:
    """Admit one native primary cutover gate through operator-control persistence."""

    return execute_operation_from_subsystems(
        _subs,
        operation_name="operator.native_primary_cutover_gate",
        payload={
            "decided_by": params.get("decided_by", ""),
            "decision_source": params.get("decision_source", ""),
            "rationale": params.get("rationale", ""),
            "roadmap_item_id": params.get("roadmap_item_id"),
            "workflow_class_id": params.get("workflow_class_id"),
            "schedule_definition_id": params.get("schedule_definition_id"),
            "title": params.get("title"),
            "gate_name": params.get("gate_name"),
            "gate_policy": params.get("gate_policy"),
            "required_evidence": params.get("required_evidence"),
            "decided_at": params.get("decided_at"),
            "opened_at": params.get("opened_at"),
            "created_at": params.get("created_at"),
            "updated_at": params.get("updated_at"),
        },
    )


def tool_praxis_operator_architecture_policy(params: dict) -> dict:
    """Record one bounded architecture-policy decision through operator control."""

    effective_from = params.get("effective_from")
    effective_to = params.get("effective_to")
    decided_at = params.get("decided_at")
    created_at = params.get("created_at")
    updated_at = params.get("updated_at")
    return execute_operation_from_subsystems(
        _subs,
        operation_name="operator.architecture_policy_record",
        payload={
            "authority_domain": params.get("authority_domain", ""),
            "policy_slug": params.get("policy_slug", ""),
            "title": params.get("title", ""),
            "rationale": params.get("rationale", ""),
            "decided_by": params.get("decided_by", ""),
            "decision_source": params.get("decision_source", ""),
            "effective_from": (
                _parse_iso_datetime(effective_from, field_name="effective_from")
                if effective_from is not None
                else None
            ),
            "effective_to": (
                _parse_iso_datetime(effective_to, field_name="effective_to")
                if effective_to is not None
                else None
            ),
            "decided_at": (
                _parse_iso_datetime(decided_at, field_name="decided_at")
                if decided_at is not None
                else None
            ),
            "created_at": (
                _parse_iso_datetime(created_at, field_name="created_at")
                if created_at is not None
                else None
            ),
            "updated_at": (
                _parse_iso_datetime(updated_at, field_name="updated_at")
                if updated_at is not None
                else None
            ),
            # Migration 302 — drillability + provenance. Both optional.
            # decision_provenance ∈ {explicit, inferred}; default 'inferred'
            # downstream. decision_why is operator-authored deeper motivation,
            # separate from rationale (which captures the rule).
            "decision_provenance": params.get("decision_provenance"),
            "decision_why": params.get("decision_why"),
        },
    )


def tool_praxis_evolve_operation_field(params: dict) -> dict:
    """Plan-only wizard: introspect the CQRS chain for adding a new field
    to an existing operation. v1 returns the file-by-file edit plan.
    Closes the gap surfaced during P4.2 Pydantic input-model evolution
    where adding one field touched 6-10 files by hand."""

    return execute_operation_from_subsystems(
        _subs,
        operation_name="operation.evolve_field",
        payload={
            "operation_name": params.get("operation_name", ""),
            "field_name": params.get("field_name", ""),
            "field_type_annotation": params.get("field_type_annotation", "str | None"),
            "field_default_repr": params.get("field_default_repr", "None"),
            "field_description": params.get("field_description", ""),
            "db_column": params.get("db_column"),
            "db_table": params.get("db_table"),
        },
    )


def tool_praxis_operator_closeout(params: dict) -> dict:
    """Preview or commit proof-backed bug and roadmap closeout through the shared gate."""

    return execute_operation_from_subsystems(
        _subs,
        operation_name="operator.work_item_closeout",
        payload={
            "action": params.get("action", "preview"),
            "bug_ids": _optional_sequence_payload(params, "bug_ids"),
            "roadmap_item_ids": _optional_sequence_payload(params, "roadmap_item_ids"),
        },
    )


def tool_praxis_operator_roadmap_view(params: dict) -> dict:
    """Read one roadmap subtree, dependency edges, and semantic-first external neighbors."""

    root_roadmap_item_id = str(params.get("root_roadmap_item_id", "")).strip()
    if not root_roadmap_item_id:
        rows = _subs.get_pg_conn().execute(
            """
            SELECT roadmap_item_id
              FROM roadmap_items
             WHERE parent_roadmap_item_id IS NULL
             ORDER BY
                 CASE WHEN status = 'active' THEN 0 ELSE 1 END,
                 updated_at DESC,
                 created_at DESC
             LIMIT 1
            """
        )
        if not rows:
            return {"error": "root_roadmap_item_id is required and no roadmap roots were found"}
        root_roadmap_item_id = str(rows[0].get("roadmap_item_id") or "").strip()
    if not root_roadmap_item_id:
        return {"error": "failed to resolve a default roadmap root"}

    operation_name = "operator.roadmap_tree"
    try:
        semantic_neighbor_limit = _bounded_limit(params, default=5, maximum=200)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)

    return execute_operation_from_subsystems(
        _subs,
        operation_name=operation_name,
        payload={
            "root_roadmap_item_id": root_roadmap_item_id,
            "semantic_neighbor_limit": semantic_neighbor_limit,
        },
    )


def tool_praxis_circuits(params: dict) -> dict:
    """Inspect or override provider circuit breakers through operator-control authority."""

    action = str(params.get("action") or "list").strip().lower()
    if action == "history":
        return execute_operation_from_subsystems(
            _subs,
            operation_name="operator.circuit_history",
            payload={
                "provider_slug": str(params.get("provider_slug") or "").strip().lower() or None,
            },
        )

    if action == "list":
        return execute_operation_from_subsystems(
            _subs,
            operation_name="operator.circuit_states",
            payload={
                "provider_slug": str(params.get("provider_slug") or "").strip().lower() or None,
            },
        )

    provider_slug = str(params.get("provider_slug") or "").strip().lower()
    if not provider_slug:
        return {"error": "provider_slug is required for circuit override actions"}

    if action not in {"open", "close", "reset"}:
        return {"error": "Unknown action. Supported actions: list, history, open, close, reset"}

    effective_to = params.get("effective_to")
    effective_from = params.get("effective_from")
    return execute_operation_from_subsystems(
        _subs,
        operation_name="operator.circuit_override",
        payload={
            "provider_slug": provider_slug,
            "override_state": {
                "open": "open",
                "close": "closed",
                "reset": "reset",
            }[action],
            "effective_to": (
                _parse_iso_datetime(effective_to, field_name="effective_to")
                if effective_to is not None
                else None
            ),
            "reason_code": str(params.get("reason_code") or "operator_control"),
            "rationale": params.get("rationale"),
            "effective_from": (
                _parse_iso_datetime(effective_from, field_name="effective_from")
                if effective_from is not None
                else None
            ),
            "decided_by": params.get("decided_by"),
            "decision_source": params.get("decision_source"),
        },
    )


def tool_praxis_provider_control_plane(params: dict) -> dict:
    """Read the provider/job/model access matrix through CQRS authority."""

    payload = {
        "runtime_profile_ref": str(params.get("runtime_profile_ref") or "praxis").strip(),
        "provider_slug": str(params.get("provider_slug") or "").strip().lower() or None,
        "job_type": str(params.get("job_type") or "").strip() or None,
        "transport_type": str(params.get("transport_type") or "").strip().upper() or None,
        "model_slug": str(params.get("model_slug") or "").strip() or None,
    }
    return _execute_catalog_tool(
        operation_name="operator.provider_control_plane",
        payload=payload,
    )


def _text_sequence_param(params: dict[str, Any], field_name: str) -> list[str]:
    value = params.get(field_name)
    if value is None or value == "":
        return []
    if isinstance(value, str):
        raw_values = value.split(",")
    else:
        raw_values = list(value)
    return [
        text
        for item in raw_values
        if (text := str(item or "").strip())
    ]


def tool_praxis_provider_availability_refresh(params: dict) -> dict:
    """Refresh provider availability through CQRS authority."""

    try:
        timeout_s = _parse_positive_int(params.get("timeout_s", 60), field_name="timeout_s")
        max_concurrency = _parse_positive_int(
            params.get("max_concurrency", 4),
            field_name="max_concurrency",
        )
        refresh_control_plane = (
            _parse_bool(params.get("refresh_control_plane"), field_name="refresh_control_plane")
            if "refresh_control_plane" in params
            else True
        )
        include_snapshots = (
            _parse_bool(params.get("include_snapshots"), field_name="include_snapshots")
            if "include_snapshots" in params
            else True
        )
    except ValueError as exc:
        return _structured_input_error(
            exc,
            operation_name="operator.provider_availability_refresh",
        )

    payload = {
        "provider_slugs": _text_sequence_param(params, "provider_slugs"),
        "adapter_types": _text_sequence_param(params, "adapter_types"),
        "timeout_s": timeout_s,
        "max_concurrency": max_concurrency,
        "refresh_control_plane": refresh_control_plane,
        "runtime_profile_ref": str(params.get("runtime_profile_ref") or "").strip() or None,
        "include_snapshots": include_snapshots,
    }
    return _execute_catalog_tool(
        operation_name="operator.provider_availability_refresh",
        payload=payload,
    )


def tool_praxis_model_access_control_matrix(params: dict) -> dict:
    """Read the live ON/OFF switchboard that drives provider catalog projection."""

    try:
        limit = _bounded_limit(params, default=200, maximum=1000)
    except ValueError as exc:
        return _structured_input_error(
            exc,
            operation_name="operator.model_access_control_matrix",
        )

    payload = {
        "runtime_profile_ref": str(params.get("runtime_profile_ref") or "praxis").strip(),
        "job_type": str(params.get("job_type") or "").strip() or None,
        "transport_type": str(params.get("transport_type") or "").strip().upper() or None,
        "provider_slug": str(params.get("provider_slug") or "").strip().lower() or None,
        "model_slug": str(params.get("model_slug") or "").strip() or None,
        "control_state": str(params.get("control_state") or "").strip().lower() or None,
        "limit": limit,
    }
    return _execute_catalog_tool(
        operation_name="operator.model_access_control_matrix",
        payload=payload,
    )


def tool_praxis_access_control(params: dict) -> dict:
    """Mutate the control-panel denial table.

    Actions: list, disable, enable. Selector tuple is
    (runtime_profile_ref, job_type, transport_type, adapter_type,
    provider_slug, model_slug); '*' is wildcard.
    """

    payload = {
        "action": str(params.get("action") or "list").strip().lower(),
        "runtime_profile_ref": str(params.get("runtime_profile_ref") or "praxis").strip(),
        "job_type": str(params.get("job_type") or "*").strip(),
        "transport_type": str(params.get("transport_type") or "*").strip().upper(),
        "adapter_type": str(params.get("adapter_type") or "*").strip(),
        "provider_slug": str(params.get("provider_slug") or "*").strip().lower(),
        "model_slug": str(params.get("model_slug") or "*").strip(),
        "decision_ref": str(params.get("decision_ref") or "").strip() or None,
        "operator_message": (
            str(params.get("operator_message") or "").strip() or None
        ),
        "reason_code": (
            str(params.get("reason_code") or "control_panel.model_access_method_turned_off").strip()
        ),
    }
    if payload["transport_type"] == "":
        payload["transport_type"] = "*"
    try:
        payload["limit"] = _bounded_limit(params, default=200, maximum=1000)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name="access_control")
    return _execute_catalog_tool(operation_name="access_control", payload=payload)


def tool_praxis_task_route_eligibility(params: dict) -> dict:
    """Write one bounded task-route eligibility window through CQRS authority."""

    operation_name = "operator.task_route_eligibility"
    provider_slug = str(params.get("provider_slug") or "").strip().lower()
    eligibility_status = str(params.get("eligibility_status") or "").strip().lower()
    if not provider_slug:
        return _structured_input_error(
            ValueError("provider_slug is required"),
            operation_name=operation_name,
        )
    if not eligibility_status:
        return _structured_input_error(
            ValueError("eligibility_status is required"),
            operation_name=operation_name,
        )
    if eligibility_status not in {"eligible", "rejected"}:
        return _structured_input_error(
            ValueError("eligibility_status must be one of ['eligible', 'rejected']"),
            operation_name=operation_name,
        )

    effective_from = params.get("effective_from")
    effective_to = params.get("effective_to")
    payload = {
        "provider_slug": provider_slug,
        "eligibility_status": eligibility_status,
        "task_type": str(params.get("task_type") or "").strip() or None,
        "model_slug": str(params.get("model_slug") or "").strip() or None,
        "reason_code": str(params.get("reason_code") or "operator_control").strip(),
        "rationale": str(params.get("rationale") or "").strip() or None,
        "decision_ref": str(params.get("decision_ref") or "").strip() or None,
        "effective_from": (
            _parse_iso_datetime(effective_from, field_name="effective_from")
            if effective_from is not None
            else None
        ),
        "effective_to": (
            _parse_iso_datetime(effective_to, field_name="effective_to")
            if effective_to is not None
            else None
        ),
    }
    return _execute_catalog_tool(operation_name=operation_name, payload=payload)


def tool_praxis_work_assignment_matrix(params: dict) -> dict:
    """Read the model-tier work assignment matrix through CQRS authority."""

    try:
        limit = _bounded_limit(params, default=100, maximum=500)
        open_only = (
            True
            if "open_only" not in params
            else _parse_bool(params.get("open_only"), field_name="open_only")
        )
    except ValueError as exc:
        return _structured_input_error(exc, operation_name="operator.work_assignment_matrix")

    payload = {
        "status": str(params.get("status") or "").strip() or None,
        "audit_group": str(params.get("audit_group") or "").strip() or None,
        "recommended_model_tier": (
            str(params.get("recommended_model_tier") or "").strip() or None
        ),
        "open_only": open_only,
        "limit": limit,
    }
    return _execute_catalog_tool(
        operation_name="operator.work_assignment_matrix",
        payload=payload,
    )


def tool_praxis_execution_truth(params: dict) -> dict:
    """Read a composed proof packet for workflow execution truth."""

    operation_name = "operator.execution_truth"
    try:
        since_hours = min(
            _parse_positive_int(params.get("since_hours", 24), field_name="since_hours"),
            24 * 30,
        )
        include_trace = _parse_bool(
            params.get("include_trace", True),
            field_name="include_trace",
        )
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    payload = {
        "since_hours": since_hours,
        "run_id": str(params.get("run_id") or "").strip() or None,
        "include_trace": include_trace,
    }
    return _execute_catalog_tool(operation_name=operation_name, payload=payload)


def tool_praxis_next_work(params: dict) -> dict:
    """Read the composed next-work packet."""

    operation_name = "operator.next_work"
    try:
        limit = _bounded_limit(params, default=10, maximum=100)
        domain_limit = min(
            _parse_positive_int(
                params.get("domain_limit", 8),
                field_name="domain_limit",
            ),
            100,
        )
        bug_limit = min(
            _parse_positive_int(params.get("bug_limit", 25), field_name="bug_limit"),
            100,
        )
        work_limit = min(
            _parse_positive_int(params.get("work_limit", 25), field_name="work_limit"),
            100,
        )
        since_hours = min(
            _parse_positive_int(params.get("since_hours", 24), field_name="since_hours"),
            24 * 30,
        )
        open_only = _parse_bool(params.get("open_only", True), field_name="open_only")
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    return _execute_catalog_tool(
        operation_name=operation_name,
        payload={
            "limit": limit,
            "since_hours": since_hours,
            "domain_limit": domain_limit,
            "bug_limit": bug_limit,
            "work_limit": work_limit,
            "open_only": open_only,
        },
    )


def tool_praxis_provider_route_truth(params: dict) -> dict:
    """Read composed provider-route legal/runnable truth."""

    operation_name = "operator.provider_route_truth"
    try:
        limit = _bounded_limit(params, default=100, maximum=1000)
    except ValueError as exc:
        return _structured_input_error(exc, operation_name=operation_name)
    payload = {
        "runtime_profile_ref": str(params.get("runtime_profile_ref") or "praxis").strip(),
        "provider_slug": str(params.get("provider_slug") or "").strip().lower() or None,
        "job_type": str(params.get("job_type") or "").strip() or None,
        "transport_type": str(params.get("transport_type") or "").strip().upper() or None,
        "model_slug": str(params.get("model_slug") or "").strip() or None,
        "limit": limit,
    }
    return _execute_catalog_tool(operation_name=operation_name, payload=payload)


def tool_praxis_operation_forge(params: dict) -> dict:
    """Preview the canonical CQRS path for adding or evolving an operation."""

    operation_name = "operator.operation_forge"
    raw_operation_name = str(params.get("operation_name") or "").strip()
    if not raw_operation_name:
        return _structured_input_error(
            ValueError("operation_name is required"),
            operation_name=operation_name,
        )
    operation_kind = str(params.get("operation_kind") or "query").strip().lower()
    posture = str(params.get("posture") or "").strip() or (
        "operate" if operation_kind == "command" else "observe"
    )
    idempotency_policy = str(params.get("idempotency_policy") or "").strip() or (
        "non_idempotent" if operation_kind == "command" else "read_only"
    )
    http_method = str(params.get("http_method") or "").strip() or (
        "POST" if operation_kind == "command" else "GET"
    )
    payload = {
        "operation_name": raw_operation_name,
        "operation_ref": str(params.get("operation_ref") or "").strip() or None,
        "tool_name": str(params.get("tool_name") or "").strip() or None,
        "recommended_alias": str(params.get("recommended_alias") or "").strip() or None,
        "handler_ref": str(params.get("handler_ref") or "").strip() or None,
        "input_model_ref": str(params.get("input_model_ref") or "").strip() or None,
        "authority_domain_ref": (
            str(params.get("authority_domain_ref") or "authority.workflow_runs").strip()
        ),
        "operation_kind": operation_kind,
        "posture": posture,
        "idempotency_policy": idempotency_policy,
        "event_type": str(params.get("event_type") or "").strip() or None,
        "event_required": params.get("event_required"),
        "http_method": http_method,
        "http_path": str(params.get("http_path") or "").strip() or None,
        "summary": str(params.get("summary") or "").strip() or None,
    }
    return _execute_catalog_tool(operation_name=operation_name, payload=payload)


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_orient": (
        tool_praxis_orient,
        {
            "description": (
                "Fresh-agent orientation: returns the canonical orient payload "
                "(standing orders, authority envelope, tool guidance, recent activity, "
                "endpoints, health). The single best first call for any LLM agent or "
                "operator waking up cold against Praxis. Delegates to the same authority "
                "that serves POST /orient so HTTP and MCP consumers see identical shape.\n\n"
                "USE WHEN: starting a new session, onboarding a new agent, or re-anchoring after long idle.\n\n"
                "DO NOT USE: for deep subsystem inspection — use cluster-specific tools instead."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {},
            },
            "cli": {
                "surface": "operations",
                "tier": "curated",
                "recommended_alias": "orient",
                "when_to_use": "Wake up against Praxis and get standing orders, authority envelope, tool guidance, and endpoints in one call.",
                "when_not_to_use": "Do not use it for deep subsystem inspection; call cluster-specific tools instead.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Orient", "input": {}},
                ],
            },
        },
    ),
    "praxis_status_snapshot": (
        tool_praxis_status_snapshot,
        {
            "description": "Read the canonical workflow status snapshot — pass rate, failure mix, queue depth, and in-flight run summaries from receipt authority.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "since_hours": {"type": "integer", "description": "Lookback window in hours.", "default": 24},
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "advanced",
                "when_to_use": "Inspect workflow pass rate, failure mix, and in-flight run summaries from canonical receipts.",
                "when_not_to_use": "Do not use it for deep run inspection or workflow launch.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Show 24h status", "input": {"since_hours": 24}},
                ],
            },
        },
    ),
    "praxis_runtime_truth_snapshot": (
        tool_praxis_runtime_truth_snapshot,
        {
            "kind": "analytics",
            "operation_names": ["operator.runtime_truth_snapshot"],
            "description": (
                "Read actual workflow runtime truth across DB authority, queue state, "
                "worker heartbeats, provider slots, host-resource leases, Docker, "
                "manifest hydration audit, and recent typed failures."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string"},
                    "since_minutes": {"type": "integer", "minimum": 1, "default": 60},
                    "heartbeat_fresh_seconds": {"type": "integer", "minimum": 1, "default": 60},
                    "manifest_audit_limit": {"type": "integer", "minimum": 1, "default": 10},
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "stable",
                "when_to_use": "Inspect the observed runtime truth before launch, retry, or diagnosis.",
                "when_not_to_use": "Do not use it to mutate leases, provider slots, or workflow state.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read runtime truth", "input": {"since_minutes": 60}},
                    {"title": "Read one run truth", "input": {"run_id": "run_abc123"}},
                ],
            },
        },
    ),
    "praxis_firecheck": (
        tool_praxis_firecheck,
        {
            "kind": "analytics",
            "operation_names": ["operator.firecheck"],
            "description": (
                "Preflight whether workflow work can actually fire now. Returns "
                "can_fire, typed blockers, and remediation plans so submitted state "
                "is not mistaken for runtime proof."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string"},
                    "since_minutes": {"type": "integer", "minimum": 1, "default": 60},
                    "heartbeat_fresh_seconds": {"type": "integer", "minimum": 1, "default": 60},
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "stable",
                "recommended_alias": "firecheck",
                "when_to_use": "Run before launching or retrying workflows to prove the platform can build.",
                "when_not_to_use": "Do not use it as a retry command; it is the proof gate before retry.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Check launch readiness", "input": {}},
                    {"title": "Check one run", "input": {"run_id": "run_abc123"}},
                ],
            },
        },
    ),
    "praxis_remediation_plan": (
        tool_praxis_remediation_plan,
        {
            "kind": "analytics",
            "operation_names": ["operator.remediation_plan"],
            "description": (
                "Return the safe remediation tier, evidence requirements, approval "
                "gate, and retry delta for a typed workflow failure."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "failure_type": {"type": "string"},
                    "failure_code": {"type": "string"},
                    "stderr": {"type": "string"},
                    "run_id": {"type": "string"},
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "stable",
                "when_to_use": "Explain what repair is allowed for a failure before retrying.",
                "when_not_to_use": "Do not use it to apply repairs; it is a plan/read surface.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Plan a context repair", "input": {"failure_type": "context_not_hydrated"}},
                    {"title": "Plan from a failure code", "input": {"failure_code": "host_resource_capacity"}},
                ],
            },
        },
    ),
    "praxis_remediation_apply": (
        tool_praxis_remediation_apply,
        {
            "kind": "write",
            "operation_names": ["operator.remediation_apply"],
            "description": (
                "Apply guarded runtime remediation for a typed workflow failure. "
                "It can clean stale provider slot counters or expired host-resource "
                "leases, refuses human-gated repairs, and never retries workflow jobs."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "failure_type": {"type": "string"},
                    "failure_code": {"type": "string"},
                    "blocker_code": {"type": "string"},
                    "stderr": {"type": "string"},
                    "run_id": {"type": "string"},
                    "provider_slug": {"type": "string"},
                    "stale_after_seconds": {"type": "integer", "minimum": 1, "default": 600},
                    "dry_run": {"type": "boolean", "default": True},
                    "confirm": {"type": "boolean", "default": False},
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "stable",
                "recommended_alias": "remediation-apply",
                "when_to_use": (
                    "Use after firecheck or a typed failure to apply only safe local "
                    "authority repairs before one explicit retry."
                ),
                "when_not_to_use": "Do not use it to retry jobs or repair credentials.",
                "risks": {
                    "default": "write",
                    "dry_run": "read",
                },
                "examples": [
                    {"title": "Preview stale slot cleanup", "input": {"failure_type": "provider.capacity"}},
                    {
                        "title": "Apply stale slot cleanup",
                        "input": {
                            "failure_type": "provider.capacity",
                            "dry_run": False,
                            "confirm": True,
                        },
                    },
                ],
            },
        },
    ),
    "praxis_legal_tools": (
        tool_praxis_legal_tools,
        {
            "description": (
                "Deprecated compatibility alias for praxis_next(action='unlock_frontier'). "
                "The legal-tool analysis now lives under the progressive praxis_next "
                "front door so callers do not choose between duplicate next-action surfaces."
            ),
            "kind": "alias",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {
                        "type": "string",
                        "description": "Optional operator intent to match against the compiler and tool catalog.",
                    },
                    "run_id": {
                        "type": "string",
                        "description": "Optional workflow run id already available to the caller.",
                    },
                    "state": {
                        "type": "object",
                        "description": "Typed state already known by the caller; keys satisfy tool required inputs.",
                        "default": {},
                    },
                    "allowed_tools": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional allowlist of tool names to consider.",
                    },
                    "include_blocked": {
                        "type": "boolean",
                        "description": "When true, include blocked tools with typed gaps and repair actions.",
                        "default": True,
                    },
                    "include_mutating": {
                        "type": "boolean",
                        "description": "When false, write, launch, and session tools are blocked unless explicitly allowed.",
                        "default": False,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum legal and blocked rows to return.",
                        "minimum": 1,
                        "default": 20,
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "replacement": "praxis_next",
                "when_to_use": "Legacy alias only; prefer praxis_next(action='unlock_frontier').",
                "when_not_to_use": "Do not build new workflows against this name.",
                "risks": {"default": "read"},
                "examples": [
                    {
                        "title": "Legacy legal-tools call",
                        "input": {
                            "intent": "prove whether this run actually fired",
                            "run_id": "run_123",
                            "limit": 8,
                        },
                    },
                ],
            },
            "type_contract": {
                "default": {
                    "consumes": ["praxis.operator.typed_state"],
                    "produces": ["praxis.operator.legal_tool_actions"],
                },
            },
        },
    ),
    "praxis_execution_proof": (
        tool_praxis_execution_proof,
        {
            "description": (
                "Prove whether a workflow run or trace anchor actually produced runtime "
                "execution evidence. Queued/running labels are treated as weak context, "
                "not proof; the result names the concrete evidence and missing proof."
            ),
            "kind": "walk",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {
                        "type": "string",
                        "description": "Workflow run id anchor.",
                    },
                    "receipt_id": {
                        "type": "string",
                        "description": "Operation receipt UUID anchor.",
                    },
                    "event_id": {
                        "type": "string",
                        "description": "Authority event UUID anchor.",
                    },
                    "correlation_id": {
                        "type": "string",
                        "description": "Correlation UUID anchor.",
                    },
                    "bug_id": {
                        "type": "string",
                        "description": "Bug id anchor.",
                    },
                    "stale_after_seconds": {
                        "type": "integer",
                        "description": "Heartbeat age threshold for current-execution proof.",
                        "minimum": 5,
                        "default": 180,
                    },
                    "include_trace": {
                        "type": "boolean",
                        "description": "When true, compose with trace.walk for receipt/event/correlation evidence.",
                        "default": True,
                    },
                },
                "anyOf": [
                    {"required": ["run_id"]},
                    {"required": ["receipt_id"]},
                    {"required": ["event_id"]},
                    {"required": ["correlation_id"]},
                    {"required": ["bug_id"]},
                ],
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Check whether a run actually fired, is still executing, or only has weak queued/running labels.",
                "when_not_to_use": "Do not use it to launch, retry, cancel, or resolve work; it is proof-only.",
                "risks": {"default": "read"},
                "examples": [
                    {
                        "title": "Prove a run fired",
                        "input": {"run_id": "run_123", "stale_after_seconds": 180},
                    },
                    {
                        "title": "Prove from a receipt",
                        "input": {"receipt_id": "<receipt-uuid>"},
                    },
                ],
            },
            "type_contract": {
                "default": {
                    "consumes": ["praxis.trace.anchor"],
                    "produces": ["praxis.operator.execution_proof"],
                },
            },
        },
    ),
    "praxis_metrics_reset": (
        tool_praxis_metrics_reset,
        {
            "description": "Reset observability metrics through explicit operator maintenance authority.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "confirm": {
                        "type": "boolean",
                        "description": "Required for destructive maintenance actions.",
                        "default": False,
                    },
                    "before_date": {
                        "type": "string",
                        "description": "ISO date for surgical reset (only delete data before this date).",
                    },
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "advanced",
                "when_to_use": "Reset polluted quality metrics or routing counters through one explicit maintenance operation.",
                "when_not_to_use": "Do not use it for ordinary observability reads.",
                "risks": {"default": "write"},
                "examples": [
                    {"title": "Reset metrics with confirmation", "input": {"confirm": True}},
                ],
            },
        },
    ),
    "praxis_bug_replay_provenance_backfill": (
        tool_praxis_bug_replay_provenance_backfill,
        {
            "description": "Backfill replay provenance from canonical bug and receipt authority.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Optional scan limit.",
                        "minimum": 0,
                    },
                    "open_only": {
                        "type": "boolean",
                        "description": (
                            "When true, only scan unresolved bugs. Default is sourced from "
                            "runtime.primitive_contracts.bug_query_default_open_only_backlog() so "
                            "operator-facing bug surfaces share one authority (closes BUG-BAEC85C1)."
                        ),
                        "default": _bug_query_default_open_only_backlog(),
                    },
                    "receipt_limit": {
                        "type": "integer",
                        "description": "Receipt context lookback for replay provenance backfill.",
                        "minimum": 1,
                        "default": 1,
                    },
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "advanced",
                "when_to_use": "Backfill replay provenance without bundling unrelated maintenance actions into one selector tool.",
                "when_not_to_use": "Do not use it for read-only bug backlog inspection.",
                "risks": {"default": "write"},
                "examples": [
                    {"title": "Backfill replay provenance", "input": {"open_only": True}},
                ],
            },
            "type_contract": {
                "default": {"consumes": [], "produces": ["praxis.bug.replay_backfill_result"]},
            },
        },
    ),
    "praxis_semantic_bridges_backfill": (
        tool_praxis_semantic_bridges_backfill,
        {
            "description": "Replay semantic bridges from canonical operator authority into semantic assertions.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "as_of": {
                        "type": "string",
                        "description": "Optional ISO-8601 cutoff for semantic bridge replay.",
                    },
                    "include_object_relations": {
                        "type": "boolean",
                        "description": "Replay operator_object_relations into semantic assertions.",
                        "default": True,
                    },
                    "include_operator_decisions": {
                        "type": "boolean",
                        "description": "Replay operator_decisions into semantic assertions.",
                        "default": True,
                    },
                    "include_roadmap_items": {
                        "type": "boolean",
                        "description": "Replay roadmap semantic fields into semantic assertions.",
                        "default": True,
                    },
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "advanced",
                "when_to_use": "Rebuild semantic bridge authority from canonical operator sources.",
                "when_not_to_use": "Do not use it for semantic reads; use praxis_semantic_assertions instead.",
                "risks": {"default": "write"},
                "examples": [
                    {"title": "Backfill semantic bridges", "input": {"include_object_relations": True}},
                ],
            },
        },
    ),
    "praxis_semantic_projection_refresh": (
        tool_praxis_semantic_projection_refresh,
        {
            "description": "Refresh the semantic projection through explicit operator maintenance authority.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum events to consume in one refresh.",
                        "minimum": 1,
                        "default": 100,
                    },
                    "as_of": {
                        "type": "string",
                        "description": "Optional ISO-8601 cutoff for projection refresh.",
                    },
                },
            },
            "cli": {
                "surface": "operations",
                "tier": "advanced",
                "when_to_use": "Consume semantic projection events through one explicit maintenance operation.",
                "when_not_to_use": "Do not use it for read-only graph inspection.",
                "risks": {"default": "write"},
                "examples": [
                    {"title": "Refresh semantic projection", "input": {"limit": 100}},
                ],
            },
        },
    ),
    "praxis_run_status": (
        tool_praxis_run_status,
        {
            "description": "DEPRECATED ALIAS — use praxis_run(action='status'). Read one run-scoped operator status view.",
            "kind": "alias",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string", "description": "Workflow run id."},
                },
                "required": ["run_id"],
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "replacement": "workflow tools call praxis_run --input-json '{\"run_id\":\"<run_id>\",\"action\":\"status\"}'",
                "when_to_use": "Inspect operator status for one workflow run.",
                "when_not_to_use": "Do not use it for whole-system pass-rate summaries.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read run status", "input": {"run_id": "run_123"}},
                ],
            },
        },
    ),
    "praxis_run_scoreboard": (
        tool_praxis_run_scoreboard,
        {
            "description": "DEPRECATED ALIAS — use praxis_run(action='scoreboard'). Read one run-scoped cutover scoreboard.",
            "kind": "alias",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string", "description": "Workflow run id."},
                },
                "required": ["run_id"],
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "replacement": "workflow tools call praxis_run --input-json '{\"run_id\":\"<run_id>\",\"action\":\"scoreboard\"}'",
                "when_to_use": "Inspect cutover readiness for one workflow run.",
                "when_not_to_use": "Do not use it for workflow launch or global status.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read run scoreboard", "input": {"run_id": "run_123"}},
                ],
            },
        },
    ),
    "praxis_run_graph": (
        tool_praxis_run_graph,
        {
            "description": "DEPRECATED ALIAS — use praxis_run(action='graph'). Read one run-scoped workflow graph.",
            "kind": "alias",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string", "description": "Workflow run id."},
                },
                "required": ["run_id"],
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "replacement": "workflow tools call praxis_run --input-json '{\"run_id\":\"<run_id>\",\"action\":\"graph\"}'",
                "when_to_use": "Inspect workflow topology for one run.",
                "when_not_to_use": "Do not use it for cross-domain operator graph inspection.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read run graph", "input": {"run_id": "run_123"}},
                ],
            },
        },
    ),
    "praxis_graph_projection": (
        tool_praxis_graph_projection,
        {
            "description": "Read the cross-domain operator graph projection.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "as_of": {
                        "type": "string",
                        "description": "Optional ISO-8601 timestamp for the projection snapshot.",
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Inspect the semantic-first operator graph across domains.",
                "when_not_to_use": "Do not use it for run-scoped workflow topology.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read operator graph projection", "input": {"as_of": "2026-04-16T20:05:00+00:00"}},
                ],
            },
        },
    ),
    "praxis_ui_experience_graph": (
        tool_praxis_ui_experience_graph,
        {
            "description": "Read the LLM-facing Praxis app experience graph: surfaces, controls, authority sources, relationships, and source-file anchors.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "focus": {
                        "type": "string",
                        "description": "Optional text filter such as moon, dashboard, run, chat, gate, release, or navigation.",
                    },
                    "surface_name": {
                        "type": "string",
                        "description": "Optional exact surface id/name such as build, dashboard, chat, manifests, atlas, or moon.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum items per section, capped at 250.",
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Inspect the app UI experience before changing React, CSS, or surface catalog behavior.",
                "when_not_to_use": "Do not use it for run-scoped execution topology or raw knowledge-graph traversal.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read Moon UI graph", "input": {"surface_name": "build"}},
                    {"title": "Find release controls", "input": {"focus": "release", "limit": 40}},
                ],
            },
        },
    ),
    "praxis_run": (
        tool_praxis_run,
        {
            "description": (
                "Consolidated run-scoped view. One tool replaces praxis_run_status, "
                "praxis_run_scoreboard, praxis_run_graph, praxis_run_lineage — pick "
                "the view via 'action' or 'view'. The old four remain as aliases "
                "for one window per the no-shims standing order."
            ),
            "kind": "walk",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string", "description": "Workflow run id."},
                    "action": {
                        "type": "string",
                        "description": "Canonical selector for which run view to return.",
                        "enum": ["status", "scoreboard", "graph", "lineage"],
                        "default": "status",
                    },
                    "view": {
                        "type": "string",
                        "description": "Selector alias for HTTP parity. Prefer action; view is accepted so callers can use the same vocabulary across surfaces.",
                        "enum": ["status", "scoreboard", "graph", "lineage"],
                        "default": "status",
                    },
                },
                "required": ["run_id"],
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "One stop for run-scoped status / scoreboard / graph / lineage views. Use action, or view when you are copying the HTTP selector shape.",
                "when_not_to_use": "Do not use it for cross-domain operator graph (use praxis_graph_projection).",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Run status", "input": {"run_id": "run_123", "action": "status"}},
                    {"title": "Run graph", "input": {"run_id": "run_123", "action": "graph"}},
                ],
            },
        },
    ),
    "praxis_run_lineage": (
        tool_praxis_run_lineage,
        {
            "description": "DEPRECATED ALIAS — use praxis_run(action='lineage'). Read one run-scoped lineage view.",
            "kind": "alias",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": {"type": "string", "description": "Workflow run id."},
                },
                "required": ["run_id"],
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "replacement": "workflow tools call praxis_run --input-json '{\"run_id\":\"<run_id>\",\"action\":\"lineage\"}'",
                "when_to_use": "Inspect graph lineage and operator frames for one run.",
                "when_not_to_use": "Do not use it for whole-system summaries.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read run lineage", "input": {"run_id": "run_123"}},
                ],
            },
        },
    ),
    "praxis_trace": (
        tool_praxis_trace,
        {
            "description": (
                "Walk the cause tree for any anchor (receipt_id, event_id, or "
                "correlation_id) and return the rooted DAG of receipts plus the "
                "events they emitted. Phase 1 of causal tracing — links receipts "
                "via cause_receipt_id and groups them by correlation_id. Returns "
                "orphan_count so callers can see when a trace is incomplete "
                "(e.g. when an async-spawned subtree did not propagate context)."
            ),
            "kind": "search",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "receipt_id": {
                        "type": "string",
                        "description": "Receipt UUID anchor.",
                    },
                    "event_id": {
                        "type": "string",
                        "description": "Authority event UUID anchor.",
                    },
                    "correlation_id": {
                        "type": "string",
                        "description": "Correlation UUID anchor — fetches the entire trace.",
                    },
                    "run_id": {
                        "type": "string",
                        "description": "Workflow run id anchor — resolved via authority_events payloads or bugs.discovered_in_run_id fallback.",
                    },
                    "bug_id": {
                        "type": "string",
                        "description": "Bug id anchor — resolved via bugs.discovered_in_receipt_id, falling back to bug_evidence_links.",
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": (
                    "Follow a flow end-to-end across nested gateway calls within "
                    "one entry point. Start from any receipt, event, correlation, "
                    "workflow run, or bug to see the whole tree."
                ),
                "when_not_to_use": (
                    "Do not use this for run-scoped views — praxis_run(action='lineage') "
                    "still walks the evidence_timeline for one workflow run. Use "
                    "praxis_trace when the flow crosses operations, not just stages."
                ),
                "risks": {"default": "read"},
                "examples": [
                    {
                        "title": "Trace from a receipt",
                        "input": {"receipt_id": "<receipt-uuid>"},
                    },
                    {
                        "title": "Trace by correlation",
                        "input": {"correlation_id": "<correlation-uuid>"},
                    },
                ],
            },
            "type_contract": {
                "default": {"consumes": [], "produces": ["praxis.trace.cause_tree"]},
            },
        },
    ),
    "praxis_issue_backlog": (
        tool_praxis_issue_backlog,
        {
            "description": "Read the canonical operator issue backlog.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows to return.",
                        "minimum": 1,
                        "default": 50,
                    },
                    "open_only": {
                        "type": "boolean",
                        "description": (
                            "When true, exclude resolved issues. Default is sourced from "
                            "runtime.primitive_contracts.bug_query_default_open_only_backlog() so "
                            "operator-facing bug surfaces share one authority (closes BUG-BAEC85C1)."
                        ),
                        "default": _bug_query_default_open_only_backlog(),
                    },
                    "status": {
                        "type": "string",
                        "description": "Optional issue status filter.",
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Inspect the canonical upstream issue backlog before bug promotion.",
                "when_not_to_use": "Do not use it to mutate issue or bug state.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read issue backlog", "input": {"limit": 25}},
                ],
            },
        },
    ),
    "praxis_operator_ideas": (
        tool_praxis_operator_ideas,
        {
            "description": (
                "Record, resolve, promote, or list pre-commitment operator ideas. "
                "Ideas are upstream of roadmap commitment: they may be rejected, "
                "superseded, archived, or promoted into existing roadmap items, "
                "but roadmap itself does not gain a canceled state."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "file", "resolve", "promote"],
                        "default": "list",
                    },
                    "idea_id": {"type": "string"},
                    "idea_key": {"type": "string"},
                    "title": {"type": "string"},
                    "summary": {"type": "string"},
                    "source_kind": {
                        "type": "string",
                        "description": "Origin of the idea, such as operator, conversation, receipt, or research.",
                        "default": "operator",
                    },
                    "source_ref": {"type": "string"},
                    "owner_ref": {"type": "string"},
                    "decision_ref": {"type": "string"},
                    "status": {
                        "type": "string",
                        "enum": ["open", "promoted", "rejected", "superseded", "archived"],
                        "description": "Filter for list or terminal status for resolve.",
                    },
                    "resolution_summary": {"type": "string"},
                    "roadmap_item_id": {
                        "type": "string",
                        "description": "Existing roadmap item to link when action='promote'.",
                    },
                    "promoted_by": {"type": "string"},
                    "opened_at": {"type": "string"},
                    "resolved_at": {"type": "string"},
                    "promoted_at": {"type": "string"},
                    "created_at": {"type": "string"},
                    "updated_at": {"type": "string"},
                    "idea_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "open_only": {
                        "type": "boolean",
                        "description": "When true, list only open ideas unless status is supplied.",
                        "default": _bug_query_default_open_only_backlog(),
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum idea rows to return for list.",
                        "minimum": 1,
                        "default": 50,
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Capture pre-commitment ideas or promote them into roadmap without polluting roadmap with canceled items.",
                "when_not_to_use": "Do not use it to create committed roadmap work; use praxis_operator_write after the idea is ready for roadmap.",
                "risks": {
                    "default": "read",
                    "actions": {
                        "list": "read",
                        "file": "write",
                        "resolve": "write",
                        "promote": "write",
                    },
                },
                "examples": [
                    {
                        "title": "List open ideas",
                        "input": {"action": "list", "limit": 25},
                    },
                    {
                        "title": "File an idea",
                        "input": {
                            "action": "file",
                            "title": "First-class ideas authority",
                            "summary": "Pre-commitment intake for roadmap candidates.",
                        },
                    },
                    {
                        "title": "Reject an idea",
                        "input": {
                            "action": "resolve",
                            "idea_id": "operator_idea.example",
                            "status": "rejected",
                            "resolution_summary": "No longer fits the operator model.",
                        },
                    },
                ],
            },
        },
    ),
    "praxis_replay_ready_bugs": (
        tool_praxis_replay_ready_bugs,
        {
            "description": "Read the replay-ready bug backlog from authoritative provenance.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows to return.",
                        "minimum": 1,
                        "default": 50,
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Inspect replayable bugs without bundling that read behind a selector view.",
                "when_not_to_use": "Do not use it to trigger replay backfill.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read replay-ready bugs", "input": {"limit": 25}},
                ],
            },
            "type_contract": {
                "default": {"consumes": [], "produces": ["praxis.bug.replay_ready_list"]},
            },
        },
    ),
    "praxis_bug_triage_packet": (
        tool_praxis_bug_triage_packet,
        {
            "description": (
                "Read a compact LLM-oriented packet that classifies bugs as live defects, "
                "evidence debt, stale projections, platform friction, fixed-pending-verification, "
                "or inactive without mutating bug authority."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum rows to return.",
                        "minimum": 1,
                        "default": 50,
                    },
                    "open_only": {
                        "type": "boolean",
                        "description": "When true, classify only open bug rows by default.",
                        "default": _bug_query_default_open_only_backlog(),
                    },
                    "classification": {
                        "type": "string",
                        "description": "Optional triage classification filter.",
                        "enum": [
                            "live_defect",
                            "evidence_debt",
                            "stale_projection",
                            "platform_friction",
                            "fixed_pending_verification",
                            "inactive",
                        ],
                    },
                    "include_inactive": {
                        "type": "boolean",
                        "description": "When true, include fixed, deferred, closed, and retired rows.",
                        "default": False,
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "when_to_use": "Let an LLM choose bug work using deterministic evidence/provenance classes.",
                "when_not_to_use": "Do not use it to resolve, mutate, or backfill bugs.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read bug triage packet", "input": {"limit": 25}},
                    {
                        "title": "Read evidence-debt bugs",
                        "input": {"classification": "evidence_debt", "limit": 25},
                    },
                ],
            },
            "type_contract": {
                "default": {"consumes": [], "produces": ["praxis.bug.triage_packet"]},
            },
        },
    ),
    "praxis_refactor_heatmap": (
        tool_praxis_refactor_heatmap,
        {
            "description": (
                "Read the ranked refactor heatmap. Combines architecture-bug authority, "
                "source spread, surface coupling, and large-symbol pressure into one "
                "deterministic read model for choosing cleanup work."
            ),
            "kind": "analytics",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "Maximum domains to return.",
                        "minimum": 1,
                        "default": 15,
                    },
                    "include_tests": {
                        "type": "boolean",
                        "description": "When true, include test files in topology metrics.",
                        "default": False,
                    },
                    "include_domains": {
                        "description": "Optional list or comma-separated set of heatmap domain slugs.",
                    },
                    "bug_limit": {
                        "type": "integer",
                        "description": "Maximum architecture bugs to consider.",
                        "minimum": 1,
                        "default": 250,
                    },
                    "long_symbol_threshold": {
                        "type": "integer",
                        "description": "Minimum lines for large function/class pressure.",
                        "minimum": 40,
                        "default": 120,
                    },
                    "open_only": {
                        "type": "boolean",
                        "description": "When true, exclude resolved architecture bugs.",
                        "default": True,
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "stable",
                "when_to_use": (
                    "Rank architecture refactor candidates by authority spread, bugs, "
                    "surface coupling, and large-module pressure."
                ),
                "when_not_to_use": "Do not use it to mutate bugs, roadmap, catalog rows, or source files.",
                "risks": {"default": "read"},
                "examples": [
                    {"title": "Read the refactor heatmap", "input": {"limit": 15}},
                    {
                        "title": "Inspect one domain",
                        "input": {
                            "include_domains": ["provider_routing_admission"],
                            "limit": 1,
                        },
                    },
                ],
            },
            "type_contract": {
                "default": {"consumes": [], "produces": ["praxis.refactor.heatmap"]},
            },
        },
    ),
    "praxis_operator_write": (
        tool_praxis_operator_write,
        {
            "description": (
                "Preview, validate, commit, update, retire, or re-parent roadmap rows through the shared operator-write validation gate.\n\n"
                "USE WHEN: you want to add a roadmap item or a packaged roadmap program without raw SQL. "
                "This gate auto-generates ids, keys, dependency ids, and phase ordering, then returns a preview "
                "before commit. The update/retire/re-parent aliases default to dry-run so callers see the "
                "would-be mutation before writing it.\n\n"
                "EXAMPLE: praxis_operator_write(action='preview', title='Unified operator write gate', "
                "intent_brief='Single preview-first validation gate for roadmap writes', "
                "parent_roadmap_item_id='roadmap_item.authority.cleanup', template='hard_cutover_program')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "preview",
                            "validate",
                            "commit",
                            "update",
                            "retire",
                            "re-parent",
                            "reparent",
                        ],
                        "default": "preview",
                    },
                    "dry_run": {
                        "type": "boolean",
                        "description": (
                            "For action=update|retire|re-parent, true returns the would-be "
                            "change and false commits it. Omitted defaults to true for those "
                            "actions and is ignored for preview|validate|commit."
                        ),
                    },
                    "title": {"type": "string"},
                    "intent_brief": {"type": "string"},
                    "template": {
                        "type": "string",
                        "enum": [
                            "single_capability",
                            "hard_cutover_program",
                            "multi_phase_program",
                            "data_dictionary_impact_program",
                        ],
                        "default": "single_capability",
                        "description": (
                            "Roadmap shape. multi_phase_program produces 5 phase "
                            "placeholder children (foundations / build-out / substrate / "
                            "supervision / release) that the operator customizes via "
                            "action='update'."
                        ),
                    },
                    "priority": {
                        "type": "string",
                        "enum": ["p1", "p2"],
                        "default": "p2",
                    },
                    "parent_roadmap_item_id": {"type": "string"},
                    "slug": {"type": "string"},
                    "depends_on": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "source_bug_id": {"type": "string"},
                    "source_idea_id": {"type": "string"},
                    "registry_paths": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "decision_ref": {"type": "string"},
                    "item_kind": {
                        "type": "string",
                        "enum": ["capability", "initiative"],
                    },
                    "status": {
                        "type": "string",
                        "enum": ["active", "completed", "done"],
                    },
                    "lifecycle": {
                        "type": "string",
                        "enum": ["planned", "claimed", "completed", "retired"],
                        "description": (
                            "Roadmap commitment lifecycle. Use praxis_operator_ideas for pre-commitment ideas. "
                            "Set to 'retired' (with roadmap_item_id) to mark a misfiled/superseded row without "
                            "proof-backed closeout."
                        ),
                    },
                    "tier": {"type": "string"},
                    "phase_ready": {"type": "boolean"},
                    "approval_tag": {"type": "string"},
                    "reference_doc": {"type": "string"},
                    "outcome_gate": {"type": "string"},
                    "proof_kind": {
                        "type": "string",
                        "enum": ["capability_delivered_by_decision_filing"],
                        "description": (
                            "Opt-in proof contract for capability rows whose deliverable IS a "
                            "filed operator_decision (e.g. standing-order policy filings). When "
                            "set, closeout requires only that the decision_ref points at a "
                            "decided operator_decision row, not source_bug + validates_fix proof."
                        ),
                    },
                    "roadmap_item_id": {
                        "type": "string",
                        "description": (
                            "Target an existing roadmap row for update/retire/re-parent. When "
                            "provided, the tool runs in update mode: existing values are preserved "
                            "unless overridden, template children are NOT regenerated, and slug/title/"
                            "intent_brief become optional. Combine with parent_roadmap_item_id to "
                            "re-parent, with lifecycle='retired' to retire, or with phase_order to "
                            "reorder under siblings."
                        ),
                    },
                    "phase_order": {
                        "type": "string",
                        "description": (
                            "Explicit phase_order override (e.g. '33.1'). When omitted, phase_order "
                            "is auto-assigned from sibling insertion order."
                        ),
                    },
                },
                "required": [],
            },
        },
    ),
    "praxis_operator_decisions": (
        tool_praxis_operator_decisions,
        {
            "description": (
                "List or record canonical operator decisions through the shared operator_decisions table.\n\n"
                "USE WHEN: you need durable, queryable operator decisions such as architecture policy rows, "
                "and you want them stored as first-class control authority instead of hidden in prose.\n\n"
                "EXAMPLE: praxis_operator_decisions(action='list', decision_kind='architecture_policy')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "record"],
                        "default": "list",
                    },
                    "decision_key": {"type": "string"},
                    "decision_kind": {"type": "string"},
                    "decision_status": {"type": "string", "default": "decided"},
                    "title": {"type": "string"},
                    "rationale": {"type": "string"},
                    "decided_by": {"type": "string"},
                    "decision_source": {"type": "string"},
                    "decision_scope_kind": {"type": "string"},
                    "decision_scope_ref": {"type": "string"},
                    "effective_from": {"type": "string"},
                    "effective_to": {"type": "string"},
                    "as_of": {"type": "string"},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 100,
                    },
                },
            },
            "cli": {
                "surface": "operator",
                "tier": "advanced",
                "replacement": "praxis_next",
                "when_to_use": "Legacy alias only; prefer praxis_next(action='unlock_frontier').",
                "when_not_to_use": "Do not build new workflows against this name.",
                "risks": {"default": "read"},
                "examples": [
                    {
                        "title": "Legacy legal-tools call",
                        "input": {
                            "intent": "prove whether this run actually fired",
                            "run_id": "run_123",
                            "limit": 8,
                        },
                    },
                ],
            },
        },
    ),
    "praxis_operator_relations": (
        tool_praxis_operator_relations,
        {
            "description": (
                "Record canonical functional areas and cross-object semantic relations.\n\n"
                "USE WHEN: a bug, roadmap item, repo path, document, workflow target, or decision "
                "needs one explicit semantic edge instead of hidden tags or prose.\n\n"
                "ACTIONS:\n"
                "  'record_functional_area' — create or update one functional area row\n"
                "  'record_relation' — create or update one cross-object relation row\n\n"
                "EXAMPLES:\n"
                "  praxis_operator_relations(action='record_functional_area', area_slug='checkout', title='Checkout', summary='Shared checkout semantics')\n"
                "  praxis_operator_relations(action='record_relation', relation_kind='grouped_in', source_kind='roadmap_item', source_ref='roadmap_item.checkout', target_kind='functional_area', target_ref='checkout')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["record_functional_area", "record_relation"],
                    },
                    "area_slug": {"type": "string"},
                    "title": {"type": "string"},
                    "summary": {"type": "string"},
                    "area_status": {
                        "type": "string",
                        "enum": ["active", "inactive"],
                        "default": "active",
                    },
                    "relation_kind": {"type": "string"},
                    "source_kind": {
                        "type": "string",
                        "enum": [
                            "issue",
                            "bug",
                            "roadmap_item",
                            "operator_decision",
                            "cutover_gate",
                            "workflow_class",
                            "schedule_definition",
                            "workflow_run",
                            "document",
                            "repo_path",
                            "functional_area",
                        ],
                    },
                    "source_ref": {"type": "string"},
                    "target_kind": {
                        "type": "string",
                        "enum": [
                            "issue",
                            "bug",
                            "roadmap_item",
                            "operator_decision",
                            "cutover_gate",
                            "workflow_class",
                            "schedule_definition",
                            "workflow_run",
                            "document",
                            "repo_path",
                            "functional_area",
                        ],
                    },
                    "target_ref": {"type": "string"},
                    "relation_status": {
                        "type": "string",
                        "enum": ["active", "inactive"],
                        "default": "active",
                    },
                    "relation_metadata": {
                        "type": "object",
                        "description": "Optional structured context for the relation.",
                    },
                    "bound_by_decision_id": {"type": "string"},
                    "created_at": {"type": "string", "description": "ISO-8601 datetime string"},
                    "updated_at": {"type": "string", "description": "ISO-8601 datetime string"},
                },
                "required": ["action"],
            },
        },
    ),
    "praxis_semantic_assertions": (
        tool_praxis_semantic_assertions,
        {
            "description": (
                "Register semantic predicates, record or retract semantic assertions, and query the canonical semantic substrate.\n\n"
                "USE WHEN: semantics should become typed authority rows with explicit provenance and validity "
                "instead of hidden metadata fields or prose.\n\n"
                "ACTIONS:\n"
                "  'list' — query semantic assertions through the CQRS read path\n"
                "  'register_predicate' — register or update one predicate vocabulary row\n"
                "  'record_assertion' — record one semantic assertion row and emit a semantic bus event\n"
                "  'retract_assertion' — retract one semantic assertion row and emit a semantic bus event\n\n"
                "EXAMPLES:\n"
                "  praxis_semantic_assertions(action='register_predicate', predicate_slug='grouped_in', subject_kind_allowlist=['bug'], object_kind_allowlist=['functional_area'])\n"
                "  praxis_semantic_assertions(action='record_assertion', predicate_slug='grouped_in', subject_kind='bug', subject_ref='bug.checkout.1', object_kind='functional_area', object_ref='functional_area.checkout', source_kind='operator', source_ref='nate')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "list",
                            "register_predicate",
                            "record_assertion",
                            "retract_assertion",
                        ],
                        "default": "list",
                    },
                    "predicate_slug": {"type": "string"},
                    "subject_kind_allowlist": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "object_kind_allowlist": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "cardinality_mode": {
                        "type": "string",
                        "enum": [
                            "many",
                            "single_active_per_subject",
                            "single_active_per_edge",
                        ],
                        "default": "many",
                    },
                    "predicate_status": {
                        "type": "string",
                        "enum": ["active", "inactive"],
                        "default": "active",
                    },
                    "description": {"type": "string"},
                    "subject_kind": {"type": "string"},
                    "subject_ref": {"type": "string"},
                    "object_kind": {"type": "string"},
                    "object_ref": {"type": "string"},
                    "qualifiers_json": {"type": "object"},
                    "source_kind": {"type": "string"},
                    "source_ref": {"type": "string"},
                    "evidence_ref": {"type": "string"},
                    "bound_decision_id": {"type": "string"},
                    "valid_from": {"type": "string"},
                    "valid_to": {"type": "string"},
                    "assertion_status": {
                        "type": "string",
                        "enum": ["active", "superseded", "retracted"],
                        "default": "active",
                    },
                    "semantic_assertion_id": {"type": "string"},
                    "retracted_at": {"type": "string"},
                    "created_at": {"type": "string"},
                    "updated_at": {"type": "string"},
                    "as_of": {"type": "string"},
                    "active_only": {"type": "boolean", "default": True},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 100,
                    },
                },
            },
        },
    ),
    "praxis_operator_native_primary_cutover_gate": (
        tool_praxis_operator_native_primary_cutover_gate,
        {
            "description": (
                "Admit a native primary cutover gate into operator-control decision and gate authority tables.\n\n"
                "USE WHEN: you need a tracked cutover decision for one target (roadmap item, workflow class, or "
                "schedule definition) with optional policy/evidence payloads.\n\n"
                "EXAMPLE: praxis_operator_native_primary_cutover_gate(\n"
                "  decided_by='operator-auto',\n"
                "  decision_source='runbook',\n"
                "  rationale='manual rollout hold ended',\n"
                "  roadmap_item_id='roadmap_item.platform.deploy',\n"
                "  gate_policy={'rollout_window':'canary'},\n"
                "  required_evidence={'checks':['operator-readiness']}\n"
                ")"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "decided_by": {
                        "type": "string",
                        "description": "Operator principal or service taking action.",
                    },
                    "decision_source": {
                        "type": "string",
                        "description": "Source system or artifact for this cutover decision.",
                    },
                    "rationale": {
                        "type": "string",
                        "description": "Human-readable justification for opening the gate.",
                    },
                    "roadmap_item_id": {"type": "string"},
                    "workflow_class_id": {"type": "string"},
                    "schedule_definition_id": {"type": "string"},
                    "title": {"type": "string"},
                    "gate_name": {"type": "string"},
                    "gate_policy": {
                        "type": "object",
                        "description": "Optional policy envelope attached to the gate.",
                    },
                    "required_evidence": {
                        "type": "object",
                        "description": "Optional evidence envelope attached to the gate.",
                    },
                    "decided_at": {"type": "string", "description": "ISO-8601 datetime string"},
                    "opened_at": {"type": "string", "description": "ISO-8601 datetime string"},
                    "created_at": {"type": "string", "description": "ISO-8601 datetime string"},
                    "updated_at": {"type": "string", "description": "ISO-8601 datetime string"},
                },
                "required": ["decided_by", "decision_source", "rationale"],
            },
        },
    ),
    "praxis_operator_architecture_policy": (
        tool_praxis_operator_architecture_policy,
        {
            "description": (
                "Record a durable architecture-policy decision in operator authority.\n\n"
                "USE WHEN: explicit operator or CTO guidance should become a typed, queryable "
                "decision row instead of living only in chat, docs, or migration folklore.\n\n"
                "EXAMPLE: praxis_operator_architecture_policy(\n"
                "  authority_domain='decision_tables',\n"
                "  policy_slug='db-native-authority',\n"
                "  title='Decision tables are DB-native authority',\n"
                "  decided_by='nate',\n"
                "  decision_source='cto.guidance',\n"
                "  rationale='Authority, durable state, and orchestration belong in DB primitives.'\n"
                ")"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "authority_domain": {
                        "type": "string",
                        "description": "Typed authority domain scope, for example decision_tables.",
                    },
                    "policy_slug": {
                        "type": "string",
                        "description": "Stable slug for the architecture policy inside the domain.",
                    },
                    "title": {
                        "type": "string",
                        "description": "Short title for the policy decision.",
                    },
                    "rationale": {
                        "type": "string",
                        "description": "Durable reason for the policy decision.",
                    },
                    "decided_by": {
                        "type": "string",
                        "description": "Principal or operator recording the decision.",
                    },
                    "decision_source": {
                        "type": "string",
                        "description": "Source artifact or authority lane for the decision.",
                    },
                    "effective_from": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime when the policy becomes effective.",
                    },
                    "effective_to": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime when the policy expires.",
                    },
                    "decided_at": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime for the decision timestamp.",
                    },
                    "created_at": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime for the row creation timestamp.",
                    },
                    "updated_at": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime for the row update timestamp.",
                    },
                    "decision_provenance": {
                        "type": "string",
                        "enum": ["explicit", "inferred"],
                        "description": "Optional. 'explicit' = operator unequivocally said so (binding). 'inferred' = model guessed from conversation/debate (advisory). Defaults to 'inferred' when omitted; surfacing layers weight explicit higher.",
                    },
                    "decision_why": {
                        "type": "string",
                        "description": "Optional deeper motivation, separate from rationale (which captures the rule). Drillable surfaces expose this alongside rationale.",
                    },
                },
                "required": [
                    "authority_domain",
                    "policy_slug",
                    "title",
                    "rationale",
                    "decided_by",
                    "decision_source",
                ],
            },
        },
    ),
    "praxis_evolve_operation_field": (
        tool_praxis_evolve_operation_field,
        {
            "operation_names": ["operation.evolve_field"],
            "description": (
                "Plan-only wizard for adding a new field to an existing CQRS operation's input model.\n\n"
                "USE WHEN: you need to evolve an existing operation's input shape (add an optional field). "
                "Returns a complete file-by-file edit checklist instead of grep-hunting through the chain. "
                "v1 is plan-only — operator/agent applies the diffs.\n\n"
                "EXAMPLE: praxis_evolve_operation_field(\n"
                "  operation_name='operator.architecture_policy_record',\n"
                "  field_name='decision_provenance',\n"
                "  field_type_annotation='str | None',\n"
                "  field_default_repr='None',\n"
                "  field_description='explicit | inferred',\n"
                "  db_table='operator_decisions',\n"
                "  db_column='decision_provenance'\n"
                ")\n\n"
                "USE praxis_register_operation for net-new operations; this tool is for evolving existing ones."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "operation_name": {
                        "type": "string",
                        "description": "Existing operation_name (e.g. 'operator.architecture_policy_record').",
                    },
                    "field_name": {
                        "type": "string",
                        "description": "New field name to add to the input shape.",
                    },
                    "field_type_annotation": {
                        "type": "string",
                        "description": "Python type annotation, e.g. 'str | None' or 'int'. Default 'str | None'.",
                    },
                    "field_default_repr": {
                        "type": "string",
                        "description": "Python literal default, e.g. 'None' or \"'inferred'\". Default 'None'.",
                    },
                    "field_description": {
                        "type": "string",
                        "description": "Human description for the MCP tool's inputSchema.",
                    },
                    "db_column": {
                        "type": "string",
                        "description": "Optional matching DB column name when the field is column-backed.",
                    },
                    "db_table": {
                        "type": "string",
                        "description": "Optional table that owns db_column.",
                    },
                },
                "required": ["operation_name", "field_name"],
            },
        },
    ),
    "praxis_operator_closeout": (
        tool_praxis_operator_closeout,
        {
            "description": (
                "Preview or commit proof-backed bug and roadmap closeout through the shared reconciliation gate.\n\n"
                "USE WHEN: you want to safely close bugs and linked roadmap items from explicit validates_fix evidence "
                "without mutating truth from inference alone. Preview returns candidates and skips; commit applies only "
                "the proof-backed subset.\n\n"
                "EXAMPLE: praxis_operator_closeout(action='preview', bug_ids=['bug.operator.fix.123'])"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["preview", "commit"],
                        "default": "preview",
                    },
                    "bug_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "roadmap_item_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
            },
        },
    ),
    "praxis_operator_roadmap_view": (
        tool_praxis_operator_roadmap_view,
        {
            "description": (
                "Read one roadmap subtree and its dependency edges from DB-backed authority.\n\n"
                "USE WHEN: you want the full package view for a roadmap item, including generated child waves, "
                "derived roadmap item clusters, external dependency edges, canonical semantic neighbors, "
                "and a rendered markdown outline.\n\n"
                "EXAMPLES:\n"
                "  praxis_operator_roadmap_view()\n"
                "  praxis_operator_roadmap_view(root_roadmap_item_id='roadmap_item.authority.cleanup.unified.operator.write.validation.gate')\n"
                "  praxis_operator_roadmap_view(root_roadmap_item_id='roadmap_item.authority.cleanup.unified.operator.write.validation.gate', semantic_neighbor_limit=8)"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "root_roadmap_item_id": {"type": "string"},
                    "semantic_neighbor_limit": {
                        "type": "integer",
                        "description": "How many external roadmap neighbors to include from canonical semantic assertions.",
                        "default": 5,
                        "minimum": 0,
                    },
                },
            },
        },
    ),
    "praxis_circuits": (
        tool_praxis_circuits,
        {
            "description": (
                "Inspect effective circuit-breaker state or apply a durable manual override for one provider.\n\n"
                "ACTIONS:\n"
                "  'list'  — show effective state, runtime state, and any active manual override metadata\n"
                "  'history' — show append-only override decision history from operator authority\n"
                "  'open'  — force the breaker open for one provider until reset or effective_to\n"
                "  'close' — force the breaker closed for one provider until reset or effective_to\n"
                "  'reset' — clear the manual override and return to runtime-managed breaker behavior\n\n"
                "USE WHEN: you need operator control over provider traffic without mutating in-memory state by hand."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "history", "open", "close", "reset"],
                        "default": "list",
                    },
                    "provider_slug": {
                        "type": "string",
                        "description": "Provider slug for open, close, reset, or to filter list/history output.",
                    },
                    "effective_to": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime when the manual override expires.",
                    },
                    "effective_from": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime for the decision timestamp.",
                    },
                    "reason_code": {
                        "type": "string",
                        "description": "Operator reason code stored on the decision row.",
                        "default": "operator_control",
                    },
                    "rationale": {
                        "type": "string",
                        "description": "Human-readable rationale for the override.",
                    },
                    "decided_by": {
                        "type": "string",
                        "description": "Principal applying the override.",
                    },
                    "decision_source": {
                        "type": "string",
                        "description": "Source artifact or workflow applying the override.",
                    },
                },
            },
        },
    ),
    "praxis_provider_control_plane": (
        tool_praxis_provider_control_plane,
        {
            "description": (
                "Read the provider/job/model control-plane matrix through CQRS authority.\n\n"
                "USE WHEN: you need the visible matrix of job type, transport type, provider, model, "
                "cost structure, model version, runnable state, credential state, breaker state, "
                "and removal reasons."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "runtime_profile_ref": {
                        "type": "string",
                        "description": "Runtime profile whose private catalog should be read.",
                        "default": "praxis",
                    },
                    "provider_slug": {"type": "string"},
                    "job_type": {"type": "string"},
                    "transport_type": {
                        "type": "string",
                        "enum": ["CLI", "API"],
                    },
                    "model_slug": {"type": "string"},
                },
            },
        },
    ),
    "praxis_provider_availability_refresh": (
        tool_praxis_provider_availability_refresh,
        {
            "description": (
                "Refresh provider availability through CQRS authority.\n\n"
                "Runs the provider_usage heartbeat probe with bounded concurrency, persists "
                "heartbeat_runs + heartbeat_probe_snapshots, refreshes the provider control-plane "
                "projection, and returns the receipt/event-backed evidence handle.\n\n"
                "USE WHEN: provider routing may be stale and you need one explicit availability "
                "refresh before pipeline eval or a proof workflow launch."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "provider_slugs": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional provider slugs to probe. Omit to probe admitted providers.",
                    },
                    "adapter_types": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional adapter types to probe, e.g. cli_llm.",
                    },
                    "timeout_s": {
                        "type": "integer",
                        "default": 60,
                        "description": "Per-provider probe timeout in seconds.",
                    },
                    "max_concurrency": {
                        "type": "integer",
                        "default": 4,
                        "description": "Maximum provider probes to run at once.",
                    },
                    "refresh_control_plane": {
                        "type": "boolean",
                        "default": True,
                    },
                    "runtime_profile_ref": {
                        "type": "string",
                        "description": "Optional runtime profile projection scope.",
                    },
                    "include_snapshots": {
                        "type": "boolean",
                        "default": True,
                    },
                },
            },
        },
    ),
    "praxis_model_access_control_matrix": (
        tool_praxis_model_access_control_matrix,
        {
            "description": (
                "Read the live model-access ON/OFF switchboard that drives the private provider catalog.\n\n"
                "USE WHEN: you need to inspect whether a CLI/API access method is enabled or disabled "
                "for a task type, provider, and model, including scope, reason, and operator instruction."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "runtime_profile_ref": {
                        "type": "string",
                        "description": "Runtime profile whose private control matrix should be read.",
                        "default": "praxis",
                    },
                    "job_type": {"type": "string"},
                    "transport_type": {
                        "type": "string",
                        "enum": ["CLI", "API"],
                    },
                    "provider_slug": {"type": "string"},
                    "model_slug": {"type": "string"},
                    "control_state": {
                        "type": "string",
                        "enum": ["on", "off"],
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 200,
                    },
                },
            },
        },
    ),
    "praxis_access_control": (
        tool_praxis_access_control,
        {
            "description": (
                "Mutate the control-panel model-access denial table — the first-class "
                "checkbox surface for turning a (provider × transport × job_type × model) "
                "tuple on or off.\n\n"
                "USE WHEN: you need to disable or re-enable a provider/model for routing "
                "without writing a migration. Wildcards ('*') broaden the selector — e.g. "
                "(provider_slug='openai', transport_type='CLI') turns OpenAI off for every "
                "CLI job_type/adapter/model in one row.\n\n"
                "ACTIONS:\n"
                "  list    — read existing denial rows (filtered by selector)\n"
                "  disable — upsert a denial row (denied=TRUE) and refresh the projection\n"
                "  enable  — delete the matching denial row and refresh the projection\n\n"
                "Emits access_control.denial.changed on disable/enable."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list", "disable", "enable"],
                        "default": "list",
                    },
                    "runtime_profile_ref": {
                        "type": "string",
                        "description": "Runtime profile whose denial set is being read or written.",
                        "default": "praxis",
                    },
                    "job_type": {
                        "type": "string",
                        "description": "Task type selector or '*' for all.",
                        "default": "*",
                    },
                    "transport_type": {
                        "type": "string",
                        "enum": ["*", "CLI", "API"],
                        "default": "*",
                    },
                    "adapter_type": {
                        "type": "string",
                        "description": "Adapter selector or '*' for all.",
                        "default": "*",
                    },
                    "provider_slug": {
                        "type": "string",
                        "description": "Provider selector or '*' for all.",
                        "default": "*",
                    },
                    "model_slug": {
                        "type": "string",
                        "description": "Model selector or '*' for all.",
                        "default": "*",
                    },
                    "decision_ref": {
                        "type": "string",
                        "description": "Operator decision reference. Required for action='disable'.",
                    },
                    "operator_message": {
                        "type": "string",
                        "description": "Custom operator-message override; defaults to the standard control-panel guidance string.",
                    },
                    "reason_code": {
                        "type": "string",
                        "description": "Override the reason_code stored on the denial row.",
                        "default": "control_panel.model_access_method_turned_off",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 200,
                    },
                },
            },
        },
    ),
    "praxis_task_route_eligibility": (
        tool_praxis_task_route_eligibility,
        {
            "description": (
                "Write one bounded task-route eligibility window for a provider or provider/model "
                "scope through CQRS authority.\n\n"
                "USE WHEN: you need to allow or reject a candidate for one task type without "
                "broadly mutating provider onboarding or model-access control. This is the "
                "canonical by-task routing policy surface.\n\n"
                "STATUSES:\n"
                "  eligible — admit an exception window for this provider/model/task slice\n"
                "  rejected — block this provider/model/task slice until the window expires\n"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "provider_slug": {
                        "type": "string",
                        "description": "Provider slug for the route policy window.",
                    },
                    "eligibility_status": {
                        "type": "string",
                        "enum": ["eligible", "rejected"],
                        "description": "Whether this candidate is allowed or blocked for the scoped slice.",
                    },
                    "task_type": {
                        "type": "string",
                        "description": "Optional task type scope such as build, review, or compile.",
                    },
                    "model_slug": {
                        "type": "string",
                        "description": "Optional model scope; omit to affect the whole provider.",
                    },
                    "reason_code": {
                        "type": "string",
                        "default": "operator_control",
                    },
                    "rationale": {"type": "string"},
                    "decision_ref": {"type": "string"},
                    "effective_from": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime with timezone.",
                    },
                    "effective_to": {
                        "type": "string",
                        "description": "Optional ISO-8601 datetime with timezone.",
                    },
                },
                "required": ["provider_slug", "eligibility_status"],
            },
        },
    ),
    "praxis_work_assignment_matrix": (
        tool_praxis_work_assignment_matrix,
        {
            "description": (
                "Read the model-tier work assignment matrix through CQRS authority.\n\n"
                "USE WHEN: you need to group bugs/work items by audit group, recommended model tier, "
                "task type, suggested sequence, and delegation suitability."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "audit_group": {"type": "string"},
                    "recommended_model_tier": {
                        "type": "string",
                        "description": "Matches either the exact tier or normalized tier group.",
                    },
                    "open_only": {
                        "type": "boolean",
                        "description": (
                            "When true, excludes resolved-terminal work items the same way bug list "
                            "surfaces do. Default is sourced from "
                            "runtime.primitive_contracts.bug_query_default_open_only_list() "
                            "so open_only defaults stay aligned (BUG-BAEC85C1)."
                        ),
                        "default": bug_query_default_open_only_list(),
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 100,
                    },
                },
            },
        },
    ),
    "praxis_execution_truth": (
        tool_praxis_execution_truth,
        {
            "kind": "analytics",
            "description": (
                "Read a composed execution-truth packet. Combines status snapshot, optional "
                "run views, and optional causal trace through gateway-dispatched child "
                "queries so green-looking state is checked against independent proof.\n\n"
                "USE WHEN: you need to know whether work is actually firing, whether a "
                "specific run has observable proof, or whether queue/status state is stale."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "since_hours": {
                        "type": "integer",
                        "description": "Receipt/status lookback window in hours.",
                        "minimum": 1,
                        "default": 24,
                    },
                    "run_id": {
                        "type": "string",
                        "description": "Optional workflow run id to inspect with run views and trace.",
                    },
                    "include_trace": {
                        "type": "boolean",
                        "description": "When true and run_id is supplied, include trace.walk(run_id).",
                        "default": True,
                    },
                },
            },
        },
    ),
    "praxis_next_work": (
        tool_praxis_next_work,
        {
            "kind": "analytics",
            "description": (
                "Read a composed next-work packet. Combines refactor heatmap, bug triage, "
                "work assignment matrix, and runtime status into one ranked operator "
                "view with proof gates and validation paths.\n\n"
                "USE WHEN: you need to choose the next bounded Praxis work item without "
                "manually stitching together bugs, heatmaps, and assignment rows."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 10,
                    },
                    "since_hours": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 24,
                    },
                    "domain_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 8,
                    },
                    "bug_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 25,
                    },
                    "work_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 25,
                    },
                    "open_only": {
                        "type": "boolean",
                        "default": True,
                    },
                },
            },
        },
    ),
    "praxis_provider_route_truth": (
        tool_praxis_provider_route_truth,
        {
            "kind": "analytics",
            "description": (
                "Read composed provider-route truth. Combines provider control plane and "
                "model access control matrix to answer whether a provider/model/job "
                "route is runnable, blocked, mixed, or unknown, with removal reasons.\n\n"
                "USE WHEN: provider availability is ambiguous and you need the exact "
                "authority-backed route state before launching or retrying work."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "runtime_profile_ref": {
                        "type": "string",
                        "default": "praxis",
                    },
                    "provider_slug": {"type": "string"},
                    "job_type": {"type": "string"},
                    "transport_type": {
                        "type": "string",
                        "enum": ["CLI", "API"],
                    },
                    "model_slug": {"type": "string"},
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 100,
                    },
                },
            },
        },
    ),
    "praxis_operation_forge": (
        tool_praxis_operation_forge,
        {
            "kind": "analytics",
            "operation_names": ["operator.operation_forge"],
            "description": (
                "Preview the canonical CQRS path for adding or evolving an operation. "
                "Produces the registration payload, real tool binding + API route when "
                "the operation already exists, and reject paths before anyone hand-builds "
                "catalog drift.\n\n"
                "USE WHEN: you are about to add a tool or operation and need the "
                "operation_catalog/data_dictionary/authority_object path made explicit."
            ),
            "inputSchema": {
                "type": "object",
                "required": ["operation_name"],
                "properties": {
                    "operation_name": {"type": "string"},
                    "operation_ref": {"type": "string"},
                    "tool_name": {"type": "string"},
                    "recommended_alias": {"type": "string"},
                    "handler_ref": {"type": "string"},
                    "input_model_ref": {"type": "string"},
                    "authority_domain_ref": {
                        "type": "string",
                        "default": "authority.workflow_runs",
                    },
                    "operation_kind": {
                        "type": "string",
                        "enum": ["query", "command"],
                        "default": "query",
                    },
                    "posture": {
                        "type": "string",
                        "enum": ["observe", "operate"],
                        "description": "Defaults from operation_kind: observe for query, operate for command.",
                    },
                    "idempotency_policy": {
                        "type": "string",
                        "enum": ["read_only", "idempotent", "non_idempotent"],
                        "description": "Defaults from operation_kind: read_only for query, non_idempotent for command.",
                    },
                    "event_type": {"type": "string"},
                    "event_required": {"type": "boolean"},
                    "http_method": {"type": "string"},
                    "http_path": {"type": "string"},
                    "summary": {"type": "string"},
                },
            },
        },
    ),
}
