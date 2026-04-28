from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, Field, field_validator


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if hasattr(value, "items"):
        return dict(value.items())
    return {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _operation_receipt(payload: Mapping[str, Any]) -> dict[str, Any] | None:
    receipt = payload.get("operation_receipt")
    return dict(receipt) if isinstance(receipt, Mapping) else None


def _component(
    subsystems: Any,
    *,
    operation_name: str,
    payload: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    from runtime.operation_catalog_gateway import execute_operation_from_subsystems

    try:
        result = execute_operation_from_subsystems(
            subsystems,
            operation_name=operation_name,
            payload=dict(payload or {}),
            requested_mode="query",
        )
    except Exception as exc:  # noqa: BLE001 - composed read should preserve partial truth
        return {
            "ok": False,
            "operation_name": operation_name,
            "error": str(exc),
            "error_code": getattr(exc, "reason_code", type(exc).__name__),
        }
    body = _as_dict(result)
    if "ok" not in body:
        body["ok"] = True
    return body


def _component_receipts(components: Mapping[str, Any]) -> list[dict[str, Any]]:
    receipts: list[dict[str, Any]] = []
    for name, payload in components.items():
        if isinstance(payload, Mapping):
            receipt = _operation_receipt(payload)
            if receipt:
                receipts.append(
                    {
                        "component": name,
                        "receipt_id": receipt.get("receipt_id"),
                        "operation_name": receipt.get("operation_name"),
                        "execution_status": receipt.get("execution_status"),
                        "correlation_id": receipt.get("correlation_id"),
                        "cause_receipt_id": receipt.get("cause_receipt_id"),
                    }
                )
    return receipts


def _priority_rank(value: object) -> int:
    text = str(value or "").strip().upper()
    if text.startswith("P") and text[1:].isdigit():
        return int(text[1:])
    return 9


def _severity_weight(value: object) -> int:
    rank = _priority_rank(value)
    return max(0, 10 - rank)


def _truth_state_from_status(status: Mapping[str, Any]) -> str:
    if status.get("ok") is False:
        return "degraded"
    if status.get("observability_state") not in (None, "ready", "complete"):
        return "degraded"
    total = int(status.get("total_workflows") or 0)
    queue_total = int(status.get("queue_depth_total") or status.get("queue_depth") or 0)
    if total > 0:
        return "recent_work_observed"
    if queue_total > 0:
        return "queued_not_proven_fired"
    return "no_recent_work_observed"


class QueryExecutionTruth(BaseModel):
    since_hours: int = 24
    run_id: str | None = None
    include_trace: bool = True

    @field_validator("since_hours", mode="before")
    @classmethod
    def _normalize_since_hours(cls, value: object) -> int:
        if value in (None, ""):
            return 24
        if isinstance(value, bool):
            raise ValueError("since_hours must be an integer")
        try:
            return max(1, min(int(value), 24 * 30))
        except (TypeError, ValueError) as exc:
            raise ValueError("since_hours must be an integer") from exc

    @field_validator("run_id", mode="before")
    @classmethod
    def _normalize_run_id(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("run_id must be a non-empty string when provided")
        return value.strip()


class QueryNextWork(BaseModel):
    limit: int = 10
    since_hours: int = 24
    domain_limit: int = 8
    bug_limit: int = 25
    work_limit: int = 25
    open_only: bool = True

    @field_validator("limit", "domain_limit", "bug_limit", "work_limit", mode="before")
    @classmethod
    def _normalize_limit(cls, value: object) -> int:
        if value in (None, ""):
            return 10
        if isinstance(value, bool):
            raise ValueError("limits must be integers")
        try:
            return max(1, min(int(value), 100))
        except (TypeError, ValueError) as exc:
            raise ValueError("limits must be integers") from exc

    @field_validator("since_hours", mode="before")
    @classmethod
    def _normalize_since_hours(cls, value: object) -> int:
        if value in (None, ""):
            return 24
        if isinstance(value, bool):
            raise ValueError("since_hours must be an integer")
        try:
            return max(1, min(int(value), 24 * 30))
        except (TypeError, ValueError) as exc:
            raise ValueError("since_hours must be an integer") from exc


class QueryProviderRouteTruth(BaseModel):
    runtime_profile_ref: str = "praxis"
    provider_slug: str | None = None
    model_slug: str | None = None
    job_type: str | None = None
    transport_type: str | None = None
    limit: int = 100

    @field_validator("runtime_profile_ref", mode="before")
    @classmethod
    def _normalize_runtime_profile_ref(cls, value: object) -> str:
        if value is None:
            return "praxis"
        if not isinstance(value, str) or not value.strip():
            raise ValueError("runtime_profile_ref must be a non-empty string")
        return value.strip()

    @field_validator("provider_slug", "model_slug", "job_type", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("route filters must be non-empty strings when provided")
        return value.strip()

    @field_validator("transport_type", mode="before")
    @classmethod
    def _normalize_transport_type(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("transport_type must be CLI or API when provided")
        normalized = value.strip().upper()
        if normalized not in {"CLI", "API"}:
            raise ValueError("transport_type must be CLI or API")
        return normalized

    @field_validator("limit", mode="before")
    @classmethod
    def _normalize_limit(cls, value: object) -> int:
        if value in (None, ""):
            return 100
        if isinstance(value, bool):
            raise ValueError("limit must be an integer")
        try:
            return max(1, min(int(value), 1000))
        except (TypeError, ValueError) as exc:
            raise ValueError("limit must be an integer") from exc


class QueryOperationForge(BaseModel):
    operation_name: str
    operation_ref: str | None = None
    tool_name: str | None = None
    handler_ref: str | None = None
    input_model_ref: str | None = None
    authority_domain_ref: str = "authority.workflow_runs"
    operation_kind: str = "query"
    posture: str = "observe"
    idempotency_policy: str = "read_only"
    summary: str | None = None

    @field_validator(
        "operation_name",
        "operation_ref",
        "tool_name",
        "handler_ref",
        "input_model_ref",
        "authority_domain_ref",
        "posture",
        "idempotency_policy",
        "summary",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("operation forge text fields must be non-empty when provided")
        return value.strip()

    @field_validator("operation_kind", mode="before")
    @classmethod
    def _normalize_operation_kind(cls, value: object) -> str:
        if value in (None, ""):
            return "query"
        if not isinstance(value, str):
            raise ValueError("operation_kind must be command or query")
        normalized = value.strip().lower()
        if normalized not in {"command", "query"}:
            raise ValueError("operation_kind must be command or query")
        return normalized


def handle_query_execution_truth(
    query: QueryExecutionTruth,
    subsystems: Any,
) -> dict[str, Any]:
    components: dict[str, Any] = {
        "status_snapshot": _component(
            subsystems,
            operation_name="operator.status_snapshot",
            payload={"since_hours": query.since_hours},
        )
    }
    if query.run_id:
        components["run_status"] = _component(
            subsystems,
            operation_name="operator.run_status",
            payload={"run_id": query.run_id},
        )
        components["run_scoreboard"] = _component(
            subsystems,
            operation_name="operator.run_scoreboard",
            payload={"run_id": query.run_id},
        )
        if query.include_trace:
            components["trace"] = _component(
                subsystems,
                operation_name="trace.walk",
                payload={"run_id": query.run_id},
            )

    status = _as_dict(components["status_snapshot"])
    trace = _as_dict(components.get("trace"))
    trace_nodes = _as_list(trace.get("nodes"))
    trace_events = _as_list(trace.get("events"))
    independent_evidence = [
        "operator.status_snapshot.total_workflows"
        if int(status.get("total_workflows") or 0) > 0
        else "",
        "operator.status_snapshot.queue_depth_total"
        if int(status.get("queue_depth_total") or status.get("queue_depth") or 0) > 0
        else "",
        "trace.walk.nodes" if trace_nodes else "",
        "trace.walk.events" if trace_events else "",
    ]
    independent_evidence = [item for item in independent_evidence if item]

    state = _truth_state_from_status(status)
    if query.run_id:
        if trace.get("ok") is False:
            state = "run_trace_unresolved"
        elif trace_nodes or trace_events:
            state = "run_observed"
        elif query.include_trace:
            state = "run_not_observed"

    top_failure_codes = _as_dict(status.get("top_failure_codes"))
    return {
        "operation": "operator.execution_truth",
        "view": "execution_truth",
        "authority": "composed.gateway_receipts",
        "inputs": {
            "since_hours": query.since_hours,
            "run_id": query.run_id,
            "include_trace": query.include_trace,
        },
        "summary": {
            "truth_state": state,
            "actual_work_observed": bool(independent_evidence),
            "queue_depth_status": status.get("queue_depth_status"),
            "queue_depth_total": status.get("queue_depth_total", status.get("queue_depth")),
            "total_workflows": status.get("total_workflows"),
            "pass_rate": status.get("pass_rate"),
            "adjusted_pass_rate": status.get("adjusted_pass_rate"),
            "top_failure_codes": top_failure_codes,
            "primary_failure_code": next(iter(top_failure_codes.keys()), None),
            "independent_evidence": independent_evidence,
            "warnings": [
                "Queue depth is not execution proof; use trace/run evidence for fired/not-fired claims."
            ],
        },
        "component_receipts": _component_receipts(components),
        "components": components,
    }


def _candidate_from_work_row(row: Mapping[str, Any], heatmap_scores: Mapping[str, float]) -> dict[str, Any]:
    audit_group = str(row.get("audit_group") or "")
    heatmap_score = 0.0
    for domain, score in heatmap_scores.items():
        if domain and domain.lower() in audit_group.lower():
            heatmap_score = max(heatmap_score, float(score))
    score = (
        _severity_weight(row.get("priority") or row.get("severity")) * 100
        + heatmap_score
        + max(0, 50 - int(row.get("suggested_sequence") or 50))
    )
    return {
        "candidate_kind": "work_item",
        "score": round(score, 3),
        "item_id": row.get("item_id"),
        "title": row.get("title"),
        "status": row.get("status"),
        "priority": row.get("priority") or row.get("severity"),
        "authority_ref": row.get("source_ref") or "view.work_item_assignment_matrix",
        "audit_group": audit_group,
        "recommended_model_tier": row.get("recommended_model_tier"),
        "task_type": row.get("task_type"),
        "why": row.get("assignment_reason") or "Ranked by work assignment matrix.",
        "validation_path": "Run the item verifier or file FIX_PENDING_VERIFICATION until proof exists.",
        "proof_gate": "One representative proof before fleet execution.",
    }


def _candidate_from_bug(row: Mapping[str, Any]) -> dict[str, Any]:
    score = _severity_weight(row.get("severity")) * 100
    if row.get("classification") == "live_defect":
        score += 75
    if row.get("replay_ready"):
        score += 25
    return {
        "candidate_kind": "bug",
        "score": round(score, 3),
        "item_id": row.get("bug_id"),
        "title": row.get("title"),
        "status": row.get("status"),
        "priority": row.get("severity"),
        "authority_ref": "authority.bugs",
        "classification": row.get("classification"),
        "next_action": row.get("next_action"),
        "why": ", ".join(str(code) for code in _as_list(row.get("reason_codes"))) or "Bug triage packet ranked this row.",
        "validation_path": "Resolve to FIXED only through a registered verifier.",
        "proof_gate": "Evidence-linked replay or verifier before closure.",
    }


def _candidate_from_domain(row: Mapping[str, Any]) -> dict[str, Any]:
    metrics = _as_dict(row.get("metrics"))
    return {
        "candidate_kind": "authority_domain",
        "score": float(row.get("score") or 0),
        "item_id": row.get("domain"),
        "title": row.get("title"),
        "priority": row.get("priority"),
        "authority_ref": row.get("domain"),
        "open_architecture_bugs": metrics.get("open_architecture_bugs"),
        "p1_bugs": metrics.get("p1_bugs"),
        "why": row.get("recommended_change"),
        "validation_path": "Package one bounded build, verify, then re-run refactor heatmap.",
        "proof_gate": "One proof path before broad retry or cleanup fleet.",
    }


def handle_query_next_work(
    query: QueryNextWork,
    subsystems: Any,
) -> dict[str, Any]:
    components = {
        "status_snapshot": _component(
            subsystems,
            operation_name="operator.status_snapshot",
            payload={"since_hours": query.since_hours},
        ),
        "refactor_heatmap": _component(
            subsystems,
            operation_name="operator.refactor_heatmap",
            payload={"limit": query.domain_limit, "open_only": query.open_only},
        ),
        "bug_triage_packet": _component(
            subsystems,
            operation_name="operator.bug_triage_packet",
            payload={"limit": query.bug_limit, "open_only": query.open_only},
        ),
        "work_assignment_matrix": _component(
            subsystems,
            operation_name="operator.work_assignment_matrix",
            payload={"limit": query.work_limit, "open_only": query.open_only},
        ),
    }
    heatmap_rows = [
        _as_dict(row)
        for row in _as_list(_as_dict(components["refactor_heatmap"]).get("heatmap"))
    ]
    heatmap_scores = {
        str(row.get("domain") or ""): float(row.get("score") or 0)
        for row in heatmap_rows
    }
    candidates: list[dict[str, Any]] = []
    candidates.extend(
        _candidate_from_work_row(_as_dict(row), heatmap_scores)
        for row in _as_list(_as_dict(components["work_assignment_matrix"]).get("rows"))
    )
    candidates.extend(
        _candidate_from_bug(_as_dict(row))
        for row in _as_list(_as_dict(components["bug_triage_packet"]).get("bugs"))
    )
    candidates.extend(_candidate_from_domain(row) for row in heatmap_rows)
    candidates.sort(
        key=lambda item: (
            -float(item.get("score") or 0),
            _priority_rank(item.get("priority")),
            str(item.get("item_id") or ""),
        )
    )
    status = _as_dict(components["status_snapshot"])
    return {
        "operation": "operator.next_work",
        "view": "next_work",
        "authority": "composed.operator_read_models",
        "inputs": {
            "limit": query.limit,
            "since_hours": query.since_hours,
            "domain_limit": query.domain_limit,
            "bug_limit": query.bug_limit,
            "work_limit": query.work_limit,
            "open_only": query.open_only,
        },
        "summary": {
            "candidate_count": len(candidates),
            "returned_count": min(query.limit, len(candidates)),
            "runtime_truth_state": _truth_state_from_status(status),
            "top_domain": _as_dict(_as_dict(components["refactor_heatmap"]).get("summary")).get("top_domain"),
            "bug_triage_summary": _as_dict(components["bug_triage_packet"]).get("summary", {}),
            "work_groups": _as_dict(components["work_assignment_matrix"]).get("groups", []),
        },
        "next_actions": candidates[: query.limit],
        "component_receipts": _component_receipts(components),
        "components": components,
    }


def handle_query_provider_route_truth(
    query: QueryProviderRouteTruth,
    subsystems: Any,
) -> dict[str, Any]:
    control_payload = {
        "runtime_profile_ref": query.runtime_profile_ref,
        "provider_slug": query.provider_slug,
        "job_type": query.job_type,
        "transport_type": query.transport_type,
        "model_slug": query.model_slug,
    }
    access_payload = {
        **control_payload,
        "limit": query.limit,
    }
    components = {
        "provider_control_plane": _component(
            subsystems,
            operation_name="operator.provider_control_plane",
            payload=control_payload,
        ),
        "model_access_control_matrix": _component(
            subsystems,
            operation_name="operator.model_access_control_matrix",
            payload=access_payload,
        ),
    }
    control_rows = [
        _as_dict(row)
        for row in _as_list(_as_dict(components["provider_control_plane"]).get("rows"))
    ][: query.limit]
    runnable = [row for row in control_rows if bool(row.get("is_runnable"))]
    blocked = [row for row in control_rows if not bool(row.get("is_runnable"))]
    reason_counts: dict[str, int] = {}
    for row in blocked:
        reason = str(row.get("primary_removal_reason_code") or "unknown")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
    if runnable and blocked:
        route_state = "mixed"
    elif runnable:
        route_state = "runnable"
    elif blocked:
        route_state = "blocked"
    else:
        route_state = "unknown"
    return {
        "operation": "operator.provider_route_truth",
        "view": "provider_route_truth",
        "authority": "composed.provider_control_plane",
        "filters": access_payload,
        "summary": {
            "route_state": route_state,
            "total_routes": len(control_rows),
            "runnable_routes": len(runnable),
            "blocked_routes": len(blocked),
            "reason_counts": dict(sorted(reason_counts.items())),
            "control_counts": _as_dict(_as_dict(components["model_access_control_matrix"]).get("counts")),
            "projection_freshness": _as_dict(components["provider_control_plane"]).get("projection_freshness"),
        },
        "legal_routes": runnable,
        "blocked_routes": blocked,
        "component_receipts": _component_receipts(components),
        "components": components,
    }


def handle_query_operation_forge(
    query: QueryOperationForge,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    operation_ref = query.operation_ref or query.operation_name.replace(".", "-").replace("_", "-")
    tool_name = query.tool_name or "praxis_" + query.operation_name.replace(".", "_")
    existing = conn.fetchrow(
        """
        SELECT operation_ref, operation_name, operation_kind, handler_ref,
               input_model_ref, authority_domain_ref, enabled
        FROM operation_catalog_registry
        WHERE operation_ref = $1 OR operation_name = $2
        LIMIT 1
        """,
        operation_ref,
        query.operation_name,
    )
    existing_payload = _as_dict(existing)
    register_payload = {
        "operation_ref": operation_ref,
        "operation_name": query.operation_name,
        "handler_ref": query.handler_ref,
        "input_model_ref": query.input_model_ref,
        "authority_domain_ref": query.authority_domain_ref,
        "operation_kind": query.operation_kind,
        "posture": query.posture,
        "idempotency_policy": query.idempotency_policy,
        "summary": query.summary,
    }
    missing = [
        key
        for key in ("handler_ref", "input_model_ref", "authority_domain_ref")
        if not register_payload.get(key)
    ]
    return {
        "operation": "operator.operation_forge",
        "view": "operation_forge",
        "authority": "operation_catalog_registry + data_dictionary_objects + authority_object_registry",
        "state": "existing_operation" if existing_payload else "new_operation",
        "existing_operation": existing_payload or None,
        "tool": {
            "tool_name": tool_name,
            "mcp_wrapper": f"tool_{tool_name}",
            "risk": "write" if query.operation_kind == "command" else "read",
        },
        "register_operation_payload": register_payload,
        "missing_inputs": missing,
        "recommended_path": [
            "Add or reuse the handler and Pydantic input model.",
            "Register through praxis_register_operation or a numbered workflow migration using register_operation_atomic.",
            "Add only a thin MCP wrapper that dispatches to the gateway operation.",
            "Describe the tool through TOOLS metadata and regenerate docs.",
            "Verify operation catalog parity and MCP wrapper gateway delegation.",
        ],
        "reject_paths": [
            "Do not dispatch from MCP directly into subsystem code.",
            "Do not hand-write only operation_catalog_registry without the authority/data-dictionary chain.",
            "Do not add action-specific hidden schemas behind one broad selector.",
        ],
        "ok_to_register": not existing_payload and not missing,
    }


__all__ = [
    "QueryExecutionTruth",
    "QueryNextWork",
    "QueryOperationForge",
    "QueryProviderRouteTruth",
    "handle_query_execution_truth",
    "handle_query_next_work",
    "handle_query_operation_forge",
    "handle_query_provider_route_truth",
]
