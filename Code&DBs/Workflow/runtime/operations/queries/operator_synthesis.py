"""Operator synthesis query handlers.

These handlers deliberately compose existing authorities instead of becoming
another private planner:

* ``operator.legal_tools`` reads the live MCP catalog, optionally folds in the
  compile preview, and returns the legal next tool calls plus typed gaps.
* ``operator.execution_proof`` reads workflow runtime evidence and trace
  authority to distinguish labels from proof that execution actually fired.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import re
from typing import Any, Mapping

from pydantic import BaseModel, Field, field_validator, model_validator


_ANCHOR_FIELDS = ("run_id", "receipt_id", "event_id", "correlation_id", "bug_id")
_MUTATING_RISK_MARKERS = ("write", "launch", "session")
_RUN_TERMINAL_STATES = {
    "canceled",
    "cancelled",
    "completed",
    "failed",
    "succeeded",
    "terminal",
}
_TOOL_NAME_RE = re.compile(r"\b(praxis_[a-zA-Z0-9_]+)\b")
_TOOL_OBJECT_RE = re.compile(r"tool:(praxis_[a-zA-Z0-9_]+)")


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _clean_string_list(value: object) -> list[str] | None:
    if value is None or value == "":
        return None
    if isinstance(value, str):
        items = value.split(",")
    elif isinstance(value, (list, tuple, set)):
        items = list(value)
    else:
        raise ValueError("expected a list or comma-separated string")
    cleaned = [str(item).strip() for item in items if str(item).strip()]
    return cleaned or None


def _bounded_int(value: object, *, default: int, minimum: int, maximum: int) -> int:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        raise ValueError("must be an integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("must be an integer") from exc
    return max(minimum, min(parsed, maximum))


def _row_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "items"):
        return dict(row.items())
    return dict(row)  # type: ignore[arg-type]


def _execute_rows(conn: Any, sql: str, *args: Any) -> list[dict[str, Any]]:
    return [_row_dict(row) for row in conn.execute(sql, *args) or ()]


def _first_row(conn: Any, sql: str, *args: Any) -> dict[str, Any] | None:
    rows = _execute_rows(conn, sql, *args)
    return rows[0] if rows else None


def _json_safe(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, set):
        return sorted(_json_safe(item) for item in value)
    try:
        json.dumps(value)
    except TypeError:
        return str(value)
    return value


def _truthy_value(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _state_has(name: str, state: Mapping[str, Any]) -> bool:
    if _truthy_value(state.get(name)):
        return True
    if name in {"intent", "prose", "question", "query", "text"}:
        return _truthy_value(state.get("intent"))
    if name in {"run_id", "workflow_run_id"}:
        return _truthy_value(state.get("run_id"))
    return False


def _required_any_of_groups(tool_def: Any, action: str) -> list[list[str]]:
    groups: list[list[str]] = []
    schema = getattr(tool_def, "input_schema", {}) or {}
    raw_any_of = schema.get("anyOf")
    if isinstance(raw_any_of, list):
        for option in raw_any_of:
            if not isinstance(option, dict):
                continue
            required = option.get("required")
            if not isinstance(required, list):
                continue
            group = [
                str(item).strip()
                for item in required
                if str(item).strip() and str(item).strip() != "action"
            ]
            if group:
                groups.append(group)

    action_requirements = getattr(tool_def, "action_requirements", {}) or {}
    scoped = action_requirements.get(action, {})
    raw_scoped = scoped.get("anyOf") if isinstance(scoped, dict) else None
    if isinstance(raw_scoped, list):
        for option in raw_scoped:
            if not isinstance(option, list):
                continue
            group = [
                str(item).strip()
                for item in option
                if str(item).strip() and str(item).strip() != "action"
            ]
            if group:
                groups.append(group)
    return groups


def _compile_preview(intent: str | None, subsystems: Any) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not intent:
        return None, None
    try:
        from runtime.compile_cqrs import preview_compile

        conn = subsystems.get_pg_conn() if hasattr(subsystems, "get_pg_conn") else None
        preview = preview_compile(intent, conn=conn, match_limit=8).to_dict()
        return preview, None
    except Exception as exc:
        return None, {
            "source": "runtime.compile_cqrs.preview_compile",
            "error": str(exc),
        }


def _matched_tools_from_preview(
    preview: Mapping[str, Any] | None,
    *,
    catalog_names: set[str],
) -> set[str]:
    if not isinstance(preview, Mapping):
        return set()
    serialized = json.dumps(_json_safe(preview), sort_keys=True)
    matched: set[str] = set()
    for regex in (_TOOL_OBJECT_RE, _TOOL_NAME_RE):
        for candidate in regex.findall(serialized):
            if candidate in catalog_names:
                matched.add(candidate)
    for name in catalog_names:
        if name in serialized:
            matched.add(name)
    return matched


def _risk_requires_explicit_scope(risk: str) -> bool:
    normalized = risk.strip().lower()
    return any(marker in normalized for marker in _MUTATING_RISK_MARKERS)


def _append_unique(items: list[dict[str, Any]], item: dict[str, Any]) -> None:
    key = json.dumps(_json_safe(item), sort_keys=True)
    existing = {json.dumps(_json_safe(value), sort_keys=True) for value in items}
    if key not in existing:
        items.append(item)


def _tool_action_record(tool_def: Any, action: str, risk: str) -> dict[str, Any]:
    return {
        "tool": tool_def.name,
        "action": action,
        "entrypoint": tool_def.cli_entrypoint,
        "describe": tool_def.cli_describe_command,
        "surface": tool_def.cli_surface,
        "tier": tool_def.cli_tier,
        "kind": tool_def.kind,
        "risk": risk,
        "required_args": list(tool_def.required_args_for_action(action)),
    }


class QueryLegalTools(BaseModel):
    """Compute legal next MCP tool calls from current typed state."""

    intent: str | None = None
    run_id: str | None = None
    state: dict[str, Any] = Field(default_factory=dict)
    allowed_tools: list[str] | None = None
    include_blocked: bool = True
    include_mutating: bool = False
    limit: int = 20

    @field_validator("intent", "run_id", mode="before")
    @classmethod
    def _normalize_text(cls, value: object) -> str | None:
        return _clean_text(value)

    @field_validator("state", mode="before")
    @classmethod
    def _normalize_state(cls, value: object) -> dict[str, Any]:
        if value in (None, ""):
            return {}
        if not isinstance(value, dict):
            raise ValueError("state must be an object")
        return dict(value)

    @field_validator("allowed_tools", mode="before")
    @classmethod
    def _normalize_allowed_tools(cls, value: object) -> list[str] | None:
        return _clean_string_list(value)

    @field_validator("limit", mode="before")
    @classmethod
    def _normalize_limit(cls, value: object) -> int:
        return _bounded_int(value, default=20, minimum=1, maximum=100)


class QueryExecutionProof(BaseModel):
    """Prove whether a workflow run or trace anchor produced runtime evidence."""

    run_id: str | None = None
    receipt_id: str | None = None
    event_id: str | None = None
    correlation_id: str | None = None
    bug_id: str | None = None
    stale_after_seconds: int = 180
    include_trace: bool = True

    @field_validator(*_ANCHOR_FIELDS, mode="before")
    @classmethod
    def _normalize_anchor(cls, value: object) -> str | None:
        return _clean_text(value)

    @field_validator("stale_after_seconds", mode="before")
    @classmethod
    def _normalize_stale_after_seconds(cls, value: object) -> int:
        return _bounded_int(value, default=180, minimum=5, maximum=86400)

    @model_validator(mode="after")
    def _exactly_one_anchor(self) -> "QueryExecutionProof":
        provided = [
            getattr(self, field_name)
            for field_name in _ANCHOR_FIELDS
            if getattr(self, field_name)
        ]
        if len(provided) != 1:
            raise ValueError(
                "execution_proof requires exactly one of run_id, receipt_id, "
                "event_id, correlation_id, or bug_id"
            )
        return self


def handle_query_legal_tools(query: QueryLegalTools, subsystems: Any) -> dict[str, Any]:
    from surfaces.mcp.catalog import get_tool_catalog

    catalog = get_tool_catalog()
    allowed_tools = set(query.allowed_tools or catalog.keys())
    state: dict[str, Any] = dict(query.state)
    if query.intent:
        state["intent"] = query.intent
        state.setdefault("query", query.intent)
        state.setdefault("text", query.intent)
    if query.run_id:
        state["run_id"] = query.run_id

    preview, preview_error = _compile_preview(query.intent, subsystems)
    matched_tools = _matched_tools_from_preview(preview, catalog_names=set(catalog))

    legal_actions: list[dict[str, Any]] = []
    blocked_actions: list[dict[str, Any]] = []
    typed_gaps: list[dict[str, Any]] = []
    repair_actions: list[dict[str, Any]] = []

    for tool_name, tool_def in catalog.items():
        action = tool_def.default_action
        risk = tool_def.risk_for_selector(action)
        blocked_reasons: list[str] = []
        repairs: list[dict[str, Any]] = []

        if tool_name not in allowed_tools:
            blocked_reasons.append("outside_allowed_tools")
            repairs.append(
                {
                    "repair_type": "add_to_allowed_tools",
                    "tool": tool_name,
                    "reason": "tool is outside the caller supplied allowlist",
                }
            )

        if tool_def.kind == "alias":
            blocked_reasons.append("deprecated_alias")
            if tool_def.cli_replacement:
                repairs.append(
                    {
                        "repair_type": "use_replacement",
                        "tool": tool_name,
                        "replacement": tool_def.cli_replacement,
                    }
                )

        if _risk_requires_explicit_scope(risk) and not query.include_mutating:
            blocked_reasons.append("requires_mutating_or_session_scope")
            repairs.append(
                {
                    "repair_type": "set_include_mutating",
                    "tool": tool_name,
                    "reason": "tool risk is not read-only",
                }
            )

        missing_required = [
            name
            for name in tool_def.required_args_for_action(action)
            if not _state_has(name, state)
        ]
        for field_name in missing_required:
            blocked_reasons.append(f"missing_required:{field_name}")
            gap = {
                "gap_type": "missing_required_input",
                "tool": tool_name,
                "action": action,
                "field": field_name,
            }
            _append_unique(typed_gaps, gap)
            repairs.append(
                {
                    "repair_type": "provide_input",
                    "tool": tool_name,
                    "action": action,
                    "field": field_name,
                }
            )

        any_of_groups = _required_any_of_groups(tool_def, action)
        if any_of_groups:
            satisfied_groups = [
                group for group in any_of_groups if all(_state_has(name, state) for name in group)
            ]
            if not satisfied_groups:
                blocked_reasons.append("missing_required_anchor")
                gap = {
                    "gap_type": "missing_one_of",
                    "tool": tool_name,
                    "action": action,
                    "fields": any_of_groups,
                }
                _append_unique(typed_gaps, gap)
                repairs.append(
                    {
                        "repair_type": "provide_one_of",
                        "tool": tool_name,
                        "action": action,
                        "fields": any_of_groups,
                    }
                )
            elif len(satisfied_groups) > 1:
                blocked_reasons.append("ambiguous_anchor")
                repairs.append(
                    {
                        "repair_type": "provide_exactly_one_anchor",
                        "tool": tool_name,
                        "action": action,
                        "satisfied_groups": satisfied_groups,
                    }
                )

        base = _tool_action_record(tool_def, action, risk)
        if blocked_reasons:
            blocked_record = {
                **base,
                "blocked_reasons": sorted(set(blocked_reasons)),
                "repair_actions": repairs,
            }
            blocked_actions.append(blocked_record)
            for repair in repairs:
                _append_unique(repair_actions, repair)
            continue

        score = 0
        if tool_name in matched_tools:
            score += 100
        if risk == "read":
            score += 20
        if not base["required_args"]:
            score += 10
        if tool_def.cli_tier == "curated":
            score += 5
        legal_actions.append(
            {
                **base,
                "reason": (
                    "matched_by_compile_preview"
                    if tool_name in matched_tools
                    else "catalog_contract_satisfied"
                ),
                "_score": score,
            }
        )

    legal_actions.sort(key=lambda item: (-int(item["_score"]), item["tool"], item["action"]))
    blocked_actions.sort(key=lambda item: (item["tool"], item["action"]))
    visible_legal = [
        {key: value for key, value in item.items() if key != "_score"}
        for item in legal_actions[: query.limit]
    ]
    visible_blocked = blocked_actions[: query.limit] if query.include_blocked else []

    authority_sources = [
        "surfaces.mcp.catalog",
        "operation_catalog_registry",
        "data_dictionary_objects",
    ]
    if preview is not None or preview_error is not None:
        authority_sources.append("runtime.compile_cqrs.preview_compile")

    payload: dict[str, Any] = {
        "view": "legal_tools",
        "legal_action_count": len(legal_actions),
        "blocked_action_count": len(blocked_actions),
        "legal_actions": _json_safe(visible_legal),
        "blocked_actions": _json_safe(visible_blocked),
        "typed_gaps": _json_safe(typed_gaps[: query.limit]),
        "repair_actions": _json_safe(repair_actions[: query.limit]),
        "state": {
            "provided_fields": sorted(name for name, value in state.items() if _truthy_value(value)),
            "include_mutating": query.include_mutating,
            "allowed_tools_count": len(allowed_tools),
            "matched_tools": sorted(matched_tools),
        },
        "authority_sources": authority_sources,
    }
    if preview_error:
        payload["compile_preview_error"] = preview_error
    if preview is not None:
        payload["compile_preview"] = {
            "input_fingerprint": preview.get("input_fingerprint"),
            "enough_structure": preview.get("enough_structure"),
            "next_actions": preview.get("next_actions", []),
        }
    return payload


def _trace_payload_for_query(query: QueryExecutionProof) -> dict[str, str]:
    for field_name in _ANCHOR_FIELDS:
        value = getattr(query, field_name)
        if value:
            return {field_name: value}
    return {}


def _compact_trace(trace: Any) -> dict[str, Any]:
    if not isinstance(trace, Mapping):
        return {"ok": False, "error": "trace result was not an object"}
    nodes = trace.get("nodes")
    edges = trace.get("edges")
    events = trace.get("events")
    return {
        "ok": bool(trace.get("ok", True)),
        "error_code": trace.get("error_code"),
        "root": _json_safe(trace.get("root")),
        "node_count": len(nodes) if isinstance(nodes, list) else 0,
        "edge_count": len(edges) if isinstance(edges, list) else 0,
        "event_count": len(events) if isinstance(events, list) else 0,
        "orphan_count": int(trace.get("orphan_count") or 0),
    }


def _event_payload(event: Mapping[str, Any]) -> dict[str, Any]:
    payload = event.get("event_payload")
    if isinstance(payload, dict):
        return dict(payload)
    if isinstance(payload, str):
        try:
            parsed = json.loads(payload)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _derive_run_id_from_trace(trace: Any) -> str | None:
    if not isinstance(trace, Mapping):
        return None
    events = trace.get("events")
    if not isinstance(events, list):
        return None
    for event in events:
        if not isinstance(event, Mapping):
            continue
        payload = _event_payload(event)
        for field_name in ("run_id", "workflow_run_id"):
            value = _clean_text(payload.get(field_name))
            if value:
                return value
    return None


def _coerce_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value
    if isinstance(value, str):
        text = value.strip().replace("Z", "+00:00")
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed
    return None


def _age_seconds(value: Any, *, now: datetime) -> float | None:
    parsed = _coerce_datetime(value)
    if parsed is None:
        return None
    return (now - parsed).total_seconds()


def _count(row: Mapping[str, Any] | None, key: str) -> int:
    if not isinstance(row, Mapping):
        return 0
    try:
        return int(row.get(key) or 0)
    except (TypeError, ValueError):
        return 0


def _request_envelope_summary(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = {}
    if not isinstance(value, Mapping):
        return {"present": False}
    return {
        "present": True,
        "name": value.get("name") or value.get("spec_name"),
        "workflow_id": value.get("workflow_id"),
        "phase": value.get("phase"),
        "total_jobs": value.get("total_jobs"),
        "workspace_ref": value.get("workspace_ref"),
        "runtime_profile_ref": value.get("runtime_profile_ref"),
        "parent_run_id": value.get("parent_run_id"),
        "lineage_depth": value.get("lineage_depth"),
    }


def _compact_run_row(run: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "run_id": run.get("run_id"),
        "workflow_id": run.get("workflow_id"),
        "request_id": run.get("request_id"),
        "current_state": run.get("current_state"),
        "requested_at": run.get("requested_at"),
        "admitted_at": run.get("admitted_at"),
        "started_at": run.get("started_at"),
        "finished_at": run.get("finished_at"),
        "last_event_id": run.get("last_event_id"),
        "request_envelope": _request_envelope_summary(run.get("request_envelope")),
    }


def _record_evidence(
    evidence: list[dict[str, Any]],
    *,
    source: str,
    present: bool,
    proof_strength: str,
    details: Mapping[str, Any] | None = None,
) -> None:
    evidence.append(
        {
            "source": source,
            "present": present,
            "proof_strength": proof_strength,
            "details": _json_safe(dict(details or {})),
        }
    )


def _query_trace(query: QueryExecutionProof, subsystems: Any) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    if not query.include_trace:
        return None, None
    try:
        from runtime.operation_catalog_gateway import execute_operation_from_subsystems

        trace = execute_operation_from_subsystems(
            subsystems,
            operation_name="trace.walk",
            payload=_trace_payload_for_query(query),
            requested_mode="query",
        )
        return trace if isinstance(trace, dict) else {"result": trace}, None
    except Exception as exc:
        return None, {
            "source": "trace.walk",
            "error": str(exc),
        }


def _run_row(conn: Any, run_id: str) -> dict[str, Any] | None:
    return _first_row(
        conn,
        """
        SELECT run_id, workflow_id, request_id, current_state, requested_at,
               admitted_at, started_at, finished_at, last_event_id, request_envelope
          FROM workflow_runs
         WHERE run_id = $1
         LIMIT 1
        """,
        run_id,
    )


def _latest_claim(conn: Any, run_id: str) -> dict[str, Any] | None:
    return _first_row(
        conn,
        """
        SELECT run_id, workflow_id, request_id, claim_id, lease_id, proposal_id,
               attempt_no, transition_seq, sandbox_group_id, sandbox_session_id,
               share_mode, reuse_reason_code, created_at, updated_at
          FROM workflow_claim_lease_proposal_runtime
         WHERE run_id = $1
         ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
         LIMIT 1
        """,
        run_id,
    )


def _job_summary(conn: Any, run_id: str) -> dict[str, Any] | None:
    return _first_row(
        conn,
        """
        SELECT COUNT(*) AS total_jobs,
               COUNT(*) FILTER (WHERE started_at IS NOT NULL) AS started_jobs,
               COUNT(*) FILTER (WHERE heartbeat_at IS NOT NULL) AS heartbeat_jobs,
               COUNT(*) FILTER (WHERE status IN ('running', 'succeeded', 'failed', 'completed')) AS observed_jobs,
               MAX(heartbeat_at) AS latest_heartbeat_at,
               MAX(started_at) AS latest_started_at,
               MAX(finished_at) AS latest_finished_at
          FROM workflow_jobs
         WHERE run_id = $1
        """,
        run_id,
    )


def _latest_jobs(conn: Any, run_id: str) -> list[dict[str, Any]]:
    return _execute_rows(
        conn,
        """
        SELECT label, job_type, status, claimed_at, claimed_by, started_at,
               finished_at, heartbeat_at, receipt_id, last_error_code,
               failure_category, failure_zone
          FROM workflow_jobs
         WHERE run_id = $1
         ORDER BY COALESCE(finished_at, heartbeat_at, started_at, claimed_at) DESC NULLS LAST,
                  label ASC
         LIMIT 10
        """,
        run_id,
    )


def _submission_summary(conn: Any, run_id: str) -> dict[str, Any] | None:
    return _first_row(
        conn,
        """
        SELECT COUNT(*) AS submission_count,
               COUNT(*) FILTER (WHERE acceptance_status = 'accepted') AS accepted_submission_count,
               MAX(sealed_at) AS latest_submission_at
          FROM workflow_job_submissions
         WHERE run_id = $1
        """,
        run_id,
    )


def _outbox_summary(conn: Any, run_id: str) -> dict[str, Any] | None:
    return _first_row(
        conn,
        """
        SELECT COUNT(*) AS outbox_count,
               COUNT(*) FILTER (WHERE authority_table = 'receipts') AS receipt_outbox_count,
               MAX(authority_recorded_at) AS latest_outbox_at
          FROM workflow_outbox
         WHERE run_id = $1
        """,
        run_id,
    )


def _authority_event_summary(conn: Any, run_id: str) -> dict[str, Any] | None:
    return _first_row(
        conn,
        """
        SELECT COUNT(*) AS event_count,
               MAX(emitted_at) AS latest_event_at
          FROM authority_events
         WHERE event_payload->>'run_id' = $1
            OR event_payload->>'workflow_run_id' = $1
        """,
        run_id,
    )


def _sandbox_row(conn: Any, sandbox_session_id: str | None) -> dict[str, Any] | None:
    if not sandbox_session_id:
        return None
    return _first_row(
        conn,
        """
        SELECT sandbox_session_id, sandbox_group_id, workspace_ref,
               runtime_profile_ref, sandbox_root, share_mode, opened_at,
               expires_at, closed_at, cleanup_status
          FROM sandbox_sessions
         WHERE sandbox_session_id = $1
         LIMIT 1
        """,
        sandbox_session_id,
    )


def _recommended_next_action(
    *,
    verdict: str,
    run_id: str | None,
    current_state: str | None,
) -> dict[str, Any]:
    if verdict == "executing":
        return {
            "tool": "praxis_run",
            "input": {"run_id": run_id, "action": "status"},
            "reason": "fresh runtime evidence exists; inspect progress next",
        }
    if verdict in {"fired_terminal", "fired_but_stale"}:
        return {
            "tool": "praxis_trace",
            "input": {"run_id": run_id} if run_id else {},
            "reason": "execution fired, but current liveness needs trace or run-status inspection",
        }
    if current_state in {"queued", "pending", "admitted"}:
        return {
            "tool": "praxis_status_snapshot",
            "input": {"since_hours": 24},
            "reason": "run exists without runtime proof; inspect queue and admission health",
        }
    return {
        "tool": "praxis_trace",
        "input": {"run_id": run_id} if run_id else {},
        "reason": "runtime proof is missing; trace authority is the next narrow read",
    }


def handle_query_execution_proof(query: QueryExecutionProof, subsystems: Any) -> dict[str, Any]:
    trace, trace_error = _query_trace(query, subsystems)
    compact_trace = _compact_trace(trace) if trace is not None else None
    run_id = query.run_id or _derive_run_id_from_trace(trace)
    anchor = _trace_payload_for_query(query)
    evidence: list[dict[str, Any]] = []
    missing_evidence: list[str] = []
    query_errors: list[dict[str, Any]] = []
    if trace_error:
        query_errors.append(trace_error)

    if compact_trace is not None:
        trace_present = (
            compact_trace.get("node_count", 0) > 0
            or compact_trace.get("event_count", 0) > 0
        )
        _record_evidence(
            evidence,
            source="trace.walk",
            present=trace_present,
            proof_strength="strong" if trace_present else "missing",
            details=compact_trace,
        )
        if not trace_present:
            missing_evidence.append("trace_nodes_or_events")

    if not run_id:
        trace_present = bool(
            compact_trace
            and (
                compact_trace.get("node_count", 0) > 0
                or compact_trace.get("event_count", 0) > 0
            )
        )
        return {
            "view": "execution_proof",
            "anchor": anchor,
            "run_id": None,
            "fired": trace_present,
            "currently_executing": False,
            "verdict": "trace_only" if trace_present else "not_fired",
            "confidence": "medium" if trace_present else "low",
            "evidence": _json_safe(evidence),
            "missing_evidence": sorted(set(missing_evidence + ["run_id"])),
            "recommended_next_action": _recommended_next_action(
                verdict="trace_only" if trace_present else "not_fired",
                run_id=None,
                current_state=None,
            ),
            "query_errors": query_errors,
            "authority_sources": [
                "trace.walk",
                "authority_operation_receipts",
                "authority_events",
            ],
        }

    conn = subsystems.get_pg_conn()
    run = _run_row(conn, run_id)
    if run is None:
        trace_present = bool(
            compact_trace
            and (
                compact_trace.get("node_count", 0) > 0
                or compact_trace.get("event_count", 0) > 0
            )
        )
        return {
            "view": "execution_proof",
            "anchor": anchor,
            "run_id": run_id,
            "fired": trace_present,
            "currently_executing": False,
            "verdict": "trace_only" if trace_present else "not_fired",
            "confidence": "medium" if trace_present else "low",
            "evidence": _json_safe(evidence),
            "missing_evidence": sorted(set(missing_evidence + ["workflow_runs"])),
            "recommended_next_action": _recommended_next_action(
                verdict="trace_only" if trace_present else "not_fired",
                run_id=run_id,
                current_state=None,
            ),
            "query_errors": query_errors,
            "authority_sources": [
                "workflow_runs",
                "trace.walk",
                "authority_operation_receipts",
                "authority_events",
            ],
        }

    claim = _latest_claim(conn, run_id)
    jobs = _job_summary(conn, run_id) or {}
    latest_jobs = _latest_jobs(conn, run_id)
    submissions = _submission_summary(conn, run_id) or {}
    outbox = _outbox_summary(conn, run_id) or {}
    events = _authority_event_summary(conn, run_id) or {}
    sandbox = _sandbox_row(conn, _clean_text((claim or {}).get("sandbox_session_id")))

    now = datetime.now(timezone.utc)
    latest_heartbeat = jobs.get("latest_heartbeat_at")
    heartbeat_age = _age_seconds(latest_heartbeat, now=now)
    fresh_heartbeat = (
        heartbeat_age is not None and heartbeat_age <= query.stale_after_seconds
    )
    current_state = _clean_text(run.get("current_state"))
    terminal = (current_state or "").lower() in _RUN_TERMINAL_STATES

    has_claim = bool(claim and _truthy_value(claim.get("claim_id")))
    has_sandbox = bool(sandbox)
    started_jobs = _count(jobs, "started_jobs")
    heartbeat_jobs = _count(jobs, "heartbeat_jobs")
    submission_count = _count(submissions, "submission_count")
    outbox_count = _count(outbox, "outbox_count")
    event_count = _count(events, "event_count")
    trace_count = int((compact_trace or {}).get("node_count") or 0) + int(
        (compact_trace or {}).get("event_count") or 0
    )

    _record_evidence(
        evidence,
        source="workflow_runs",
        present=True,
        proof_strength="weak",
        details={
            "current_state": current_state,
            "requested_at": run.get("requested_at"),
            "admitted_at": run.get("admitted_at"),
            "started_at": run.get("started_at"),
            "finished_at": run.get("finished_at"),
        },
    )
    _record_evidence(
        evidence,
        source="workflow_claim_lease_proposal_runtime",
        present=has_claim,
        proof_strength="strong" if has_claim else "missing",
        details=claim,
    )
    _record_evidence(
        evidence,
        source="workflow_jobs",
        present=started_jobs > 0 or heartbeat_jobs > 0,
        proof_strength="strong" if started_jobs > 0 or heartbeat_jobs > 0 else "missing",
        details={
            **jobs,
            "latest_heartbeat_age_seconds": heartbeat_age,
            "fresh_heartbeat": fresh_heartbeat,
            "latest_jobs": latest_jobs,
        },
    )
    _record_evidence(
        evidence,
        source="sandbox_sessions",
        present=has_sandbox,
        proof_strength="strong" if has_sandbox else "missing",
        details=sandbox,
    )
    _record_evidence(
        evidence,
        source="workflow_job_submissions",
        present=submission_count > 0,
        proof_strength="strong" if submission_count > 0 else "missing",
        details=submissions,
    )
    _record_evidence(
        evidence,
        source="workflow_outbox",
        present=outbox_count > 0,
        proof_strength="strong" if outbox_count > 0 else "missing",
        details=outbox,
    )
    _record_evidence(
        evidence,
        source="authority_events",
        present=event_count > 0,
        proof_strength="strong" if event_count > 0 else "missing",
        details=events,
    )

    if not has_claim:
        missing_evidence.append("claim")
    if not fresh_heartbeat:
        missing_evidence.append("fresh_heartbeat")
    if started_jobs <= 0 and heartbeat_jobs <= 0:
        missing_evidence.append("started_or_heartbeat_job")
    if submission_count <= 0:
        missing_evidence.append("job_submission")
    if outbox_count <= 0:
        missing_evidence.append("workflow_outbox")
    if event_count <= 0:
        missing_evidence.append("authority_event")

    runtime_effects = any(
        (
            fresh_heartbeat,
            has_sandbox,
            started_jobs > 0,
            heartbeat_jobs > 0,
            submission_count > 0,
            outbox_count > 0,
            event_count > 0,
            trace_count > 0,
        )
    )
    fired = runtime_effects and (has_claim or started_jobs > 0 or submission_count > 0 or outbox_count > 0 or event_count > 0 or trace_count > 0)
    currently_executing = bool(fired and fresh_heartbeat and not terminal)
    if currently_executing:
        verdict = "executing"
        confidence = "high" if has_claim else "medium"
    elif fired and terminal:
        verdict = "fired_terminal"
        confidence = "high"
    elif fired:
        verdict = "fired_but_stale"
        confidence = "medium" if not fresh_heartbeat else "high"
    else:
        verdict = "not_fired"
        confidence = "low"

    return {
        "view": "execution_proof",
        "anchor": anchor,
        "run_id": run_id,
        "run": _json_safe(_compact_run_row(run)),
        "fired": fired,
        "currently_executing": currently_executing,
        "verdict": verdict,
        "confidence": confidence,
        "stale_after_seconds": query.stale_after_seconds,
        "evidence": _json_safe(evidence),
        "missing_evidence": sorted(set(missing_evidence)),
        "recommended_next_action": _recommended_next_action(
            verdict=verdict,
            run_id=run_id,
            current_state=(current_state or "").lower(),
        ),
        "query_errors": query_errors,
        "authority_sources": [
            "workflow_runs",
            "workflow_claim_lease_proposal_runtime",
            "workflow_jobs",
            "sandbox_sessions",
            "workflow_job_submissions",
            "workflow_outbox",
            "authority_events",
            "trace.walk",
        ],
    }


__all__ = [
    "QueryExecutionProof",
    "QueryLegalTools",
    "handle_query_execution_proof",
    "handle_query_legal_tools",
]
