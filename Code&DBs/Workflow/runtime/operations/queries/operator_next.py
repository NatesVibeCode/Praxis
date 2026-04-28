from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Literal, Mapping, Sequence

from pydantic import BaseModel, Field, field_validator, model_validator

from runtime.queue_admission import (
    DEFAULT_QUEUE_CRITICAL_THRESHOLD,
    DEFAULT_QUEUE_WARNING_THRESHOLD,
    query_queue_depth_snapshot,
)


ActionName = Literal[
    "next",
    "launch_gate",
    "failure_triage",
    "manifest_audit",
    "toolsmith",
    "unlock_frontier",
]
DetailLevel = Literal["brief", "standard", "evidence", "repair"]

_TERMINAL_SUCCESS = {"succeeded", "success", "completed", "complete", "passed", "pass"}
_TERMINAL_FAILURE = {"failed", "failure", "cancelled", "canceled", "error", "blocked"}
_ACTIVE_JOB_STATUSES = {"running", "claimed"}
_QUEUED_JOB_STATUSES = {"pending", "ready"}
_TRANSIENT_RETRY_CODES = {
    "provider.capacity",
    "host_resource_capacity",
    "rate_limit",
    "provider.rate_limit",
    "transient_provider_error",
}


class OperatorNextQuery(BaseModel):
    """Progressive-disclosure operator query over existing Praxis authority."""

    action: ActionName = "next"
    detail: DetailLevel = "brief"
    intent: str | None = None
    run_id: str | None = None
    proof_run_id: str | None = None
    spec_path: str | None = None
    manifest_path: str | None = None
    manifest: dict[str, Any] | None = None
    tool_name: str | None = None
    state: dict[str, Any] = Field(default_factory=dict)
    allowed_tools: list[str] | None = None
    include_blocked: bool = True
    include_mutating: bool = False
    facts: list[str] = Field(default_factory=list)
    fleet_size: int = 1
    limit: int = 8

    @field_validator(
        "intent",
        "run_id",
        "proof_run_id",
        "spec_path",
        "manifest_path",
        "tool_name",
        mode="before",
    )
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None:
            return None
        if not isinstance(value, str):
            raise ValueError("text fields must be strings when provided")
        text = value.strip()
        return text or None

    @field_validator("facts", mode="before")
    @classmethod
    def _normalize_facts(cls, value: object) -> list[str]:
        if value in (None, ""):
            return []
        if isinstance(value, str):
            parts = value.split(",")
        elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
            parts = [str(item) for item in value]
        else:
            raise ValueError("facts must be a list or comma-separated string")
        seen: set[str] = set()
        normalized: list[str] = []
        for part in parts:
            text = str(part or "").strip()
            if text and text not in seen:
                seen.add(text)
                normalized.append(text)
        return normalized

    @field_validator("allowed_tools", mode="before")
    @classmethod
    def _normalize_allowed_tools(cls, value: object) -> list[str] | None:
        if value in (None, ""):
            return None
        if isinstance(value, str):
            parts = value.split(",")
        elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
            parts = [str(item) for item in value]
        else:
            raise ValueError("allowed_tools must be a list or comma-separated string")
        normalized = [
            str(part or "").strip()
            for part in parts
            if str(part or "").strip()
        ]
        return _dedupe_strings(normalized) or None

    @field_validator("state", mode="before")
    @classmethod
    def _normalize_state(cls, value: object) -> dict[str, Any]:
        if value in (None, ""):
            return {}
        if not isinstance(value, dict):
            raise ValueError("state must be an object")
        return dict(value)

    @field_validator("fleet_size", "limit", mode="before")
    @classmethod
    def _normalize_positive_int(cls, value: object, info: Any) -> int:
        default = 1 if info.field_name == "fleet_size" else 8
        if value in (None, ""):
            return default
        if isinstance(value, bool):
            raise ValueError(f"{info.field_name} must be an integer")
        try:
            parsed = int(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{info.field_name} must be an integer") from exc
        upper = 500 if info.field_name == "fleet_size" else 50
        return max(1, min(parsed, upper))

    @model_validator(mode="after")
    def _require_action_anchor(self) -> "OperatorNextQuery":
        if self.action == "failure_triage" and not self.run_id:
            raise ValueError("run_id is required for failure_triage")
        if self.action == "manifest_audit" and not (
            self.manifest or self.manifest_path or self.spec_path
        ):
            raise ValueError("manifest, manifest_path, or spec_path is required for manifest_audit")
        return self


def handle_operator_next(query: OperatorNextQuery, subsystems: Any) -> dict[str, Any]:
    """Return a progressive, read-only decision surface for operator action."""

    context = _context_snapshot(query, subsystems)
    if query.action == "launch_gate":
        payload = _launch_gate(query, subsystems, context)
    elif query.action == "failure_triage":
        payload = _failure_triage(query, subsystems, context)
    elif query.action == "manifest_audit":
        payload = _manifest_audit(query, subsystems, context)
    elif query.action == "toolsmith":
        payload = _toolsmith(query, subsystems, context)
    elif query.action == "unlock_frontier":
        payload = _unlock_frontier(query, subsystems, context)
    else:
        payload = _next_actions(query, subsystems, context)
    return _with_progressive_envelope(query, payload, context)


def _with_progressive_envelope(
    query: OperatorNextQuery,
    payload: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "ok": True,
        "tool": "praxis_next",
        "action": query.action,
        "detail": query.detail,
        "authority": {
            "primary": "operation_catalog_registry",
            "moment_truth": "manifest",
            "state": "workflow_runs/workflow_jobs/provider_concurrency/execution_leases",
        },
        "progressive_disclosure": _progressive_disclosure(query.action),
        **payload,
    }
    if query.detail in {"evidence", "repair"}:
        result["evidence"] = _compact_evidence(context)
    return result


def _compact_evidence(context: Mapping[str, Any]) -> dict[str, Any]:
    catalog = context.get("catalog") if isinstance(context.get("catalog"), Mapping) else {}
    catalog_sample = []
    for row in list(catalog.get("sample") or [])[:8]:
        if not isinstance(row, Mapping):
            continue
        catalog_sample.append(
            {
                "name": row.get("name"),
                "surface": row.get("surface"),
                "tier": row.get("tier"),
                "risk_levels": row.get("risk_levels"),
            }
        )
    return {
        "queue": context.get("queue"),
        "provider_slots": context.get("provider_slots"),
        "host_resources": context.get("host_resources"),
        "run": _compact_run(context.get("run")),
        "proof_run": _compact_run(context.get("proof_run")),
        "catalog": {
            "total_tools": catalog.get("total_tools"),
            "surface_counts": catalog.get("surface_counts"),
            "tier_counts": catalog.get("tier_counts"),
            "sample": catalog_sample,
        },
        "operation_catalog_sample": context.get("operation_catalog"),
    }


def _compact_run(run: object) -> dict[str, Any] | None:
    if not isinstance(run, Mapping):
        return None
    return {
        "run_id": run.get("run_id"),
        "status": run.get("status"),
        "spec_name": run.get("spec_name"),
        "total_jobs": run.get("total_jobs"),
        "completed_jobs": run.get("completed_jobs"),
        "status_counts": run.get("status_counts"),
        "failed_jobs": run.get("failed_jobs"),
        "health": run.get("health"),
    }


def _progressive_disclosure(action: str) -> dict[str, Any]:
    return {
        "brief": "judgment + next best action",
        "standard": "brief plus checks and compact rationale",
        "evidence": "standard plus source snapshots",
        "repair": "evidence plus repair actions and blocked-action reasons",
        "available_actions": [
            "next",
            "launch_gate",
            "failure_triage",
            "manifest_audit",
            "toolsmith",
            "unlock_frontier",
        ],
        "current_action": action,
    }


def _context_snapshot(query: OperatorNextQuery, subsystems: Any) -> dict[str, Any]:
    conn = _conn(subsystems)
    return {
        "queue": _queue_snapshot(conn),
        "provider_slots": _provider_slots(conn),
        "host_resources": _host_resource_leases(conn),
        "catalog": _catalog_snapshot(limit=max(query.limit, 12)),
        "run": _run_snapshot(conn, query.run_id) if query.run_id else None,
        "proof_run": _run_snapshot(conn, query.proof_run_id) if query.proof_run_id else None,
        "operation_catalog": _operation_catalog_snapshot(conn, limit=max(query.limit, 12)),
    }


def _next_actions(
    query: OperatorNextQuery,
    subsystems: Any,
    context: dict[str, Any],
) -> dict[str, Any]:
    intent = (query.intent or "").lower()
    actions: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []

    if query.run_id:
        run = context.get("run") or {}
        status = str(run.get("status") or "").lower()
        if status in _TERMINAL_FAILURE or run.get("failed_jobs"):
            actions.append(
                _action(
                    "failure_triage",
                    "Triage the failed run before retrying",
                    {"run_id": query.run_id, "detail": "repair"},
                    reason="Terminal or failed jobs are present.",
                )
            )
        elif status in _TERMINAL_SUCCESS:
            actions.append(
                _action(
                    "launch_gate",
                    "Use this run as proof before widening execution",
                    {"proof_run_id": query.run_id, "fleet_size": max(query.fleet_size, 2)},
                    reason="The supplied run appears terminal-success.",
                )
            )
        else:
            actions.append(
                _action(
                    "launch_gate",
                    "Check whether this run is actually firing",
                    {"run_id": query.run_id, "detail": "evidence"},
                    reason="Non-terminal workflow state needs independent execution proof.",
                )
            )

    if query.spec_path or query.manifest_path or query.manifest:
        actions.append(
            _action(
                "manifest_audit",
                "Audit the manifest before launch",
                _compact_params(
                    {
                        "spec_path": query.spec_path,
                        "manifest_path": query.manifest_path,
                        "detail": "repair",
                    }
                ),
                reason="Manifest authority should align tools, scope, artifacts, and verifiers before execution.",
            )
        )

    if any(word in intent for word in ("launch", "fleet", "workflow", "fire", "retry")):
        if query.fleet_size > 1 and not query.proof_run_id:
            blocked.append(
                {
                    "action": "workflow_fleet_launch",
                    "reason": "one_proof_before_fleet",
                    "repair_action": {
                        "tool": "praxis_next",
                        "input": {"action": "launch_gate", "proof_run_id": "<canary-run-id>"},
                    },
                }
            )
        actions.append(
            _action(
                "launch_gate",
                "Gate launch/retry against canary proof and live execution evidence",
                _compact_params(
                    {
                        "run_id": query.run_id,
                        "proof_run_id": query.proof_run_id,
                        "fleet_size": query.fleet_size,
                        "detail": "repair",
                    }
                ),
                reason="Submitted is not fired; launch needs runtime evidence.",
            )
        )

    if any(word in intent for word in ("tool", "surface", "catalog", "build", "compose")):
        actions.append(
            _action(
                "toolsmith",
                "Check whether a new tool should compose existing functions",
                {"intent": query.intent, "detail": "standard"},
                reason="Tool growth should prefer composition and dedupe before new leaf tools.",
            )
        )
        actions.append(
            _action(
                "unlock_frontier",
                "Find the smallest capability repair that unlocks the most actions",
                {"intent": query.intent, "detail": "standard"},
                reason="The typed action graph can expose high-leverage blockers.",
            )
        )

    if not actions:
        actions.extend(
            [
                _action(
                    "toolsmith",
                    "Search the catalog before creating or changing a tool",
                    {"intent": query.intent or "operator next action"},
                    reason="Default safe path for ambiguous operator work.",
                ),
                _action(
                    "unlock_frontier",
                    "Compute the action frontier from current facts",
                    {"facts": query.facts, "limit": query.limit},
                    reason="Use math-shaped narrowing when the choice space feels large.",
                ),
            ]
        )

    return {
        "verdict": "inspect_then_act",
        "recommended_actions": _dedupe_actions(actions)[: query.limit],
        "blocked_actions": blocked,
        "summary": "Use the smallest legal composite surface that proves authority before execution.",
    }


def _launch_gate(
    query: OperatorNextQuery,
    subsystems: Any,
    context: dict[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []
    repairs: list[dict[str, Any]] = []
    decision = "allow"

    proof_run = context.get("proof_run") if query.proof_run_id else context.get("run")
    proof_status = str((proof_run or {}).get("status") or "").lower()
    proof_success = proof_status in _TERMINAL_SUCCESS
    if query.fleet_size > 1:
        checks.append(
            _check(
                "one_proof_before_fleet",
                proof_success,
                "Fleet launch requires one terminal-success proof run.",
                observed={"proof_run_id": query.proof_run_id or query.run_id, "status": proof_status},
            )
        )
        if not proof_success:
            decision = "block"
            repairs.append(
                {
                    "action": "run_canary",
                    "reason": "A representative job must pass before fleet launch/retry.",
                    "next_tool": {"tool": "praxis_workflow", "input": {"action": "run", "spec_path": query.spec_path or "<canary-spec>"}},
                }
            )

    run = context.get("run") or proof_run or {}
    has_run_anchor = bool((run or {}).get("run_id"))
    actual_work = has_run_anchor and _has_runtime_work(run, context)
    checks.append(
        _check(
            "submitted_is_not_fired",
            actual_work,
            "Running labels must be backed by run-scoped heartbeat, submission, completed job, or matching lease evidence.",
            observed=_runtime_work_observed(run, context),
        )
    )
    if not actual_work and str(run.get("status") or "").lower() not in _TERMINAL_SUCCESS:
        decision = "block" if decision != "block" else decision
        repair: dict[str, Any] = {
            "action": "inspect_execution_lane",
            "reason": "No independent runtime evidence of building exists.",
        }
        if query.run_id:
            repair["next_tool"] = {
                "tool": "praxis_next",
                "input": {"action": "failure_triage", "run_id": query.run_id},
            }
        else:
            repair["next_tool"] = {
                "tool": "praxis_next",
                "input": {"action": "launch_gate", "proof_run_id": "<canary-run-id>"},
            }
        repairs.append(repair)

    queue = context.get("queue") or {}
    queue_ok = str(queue.get("queue_depth_status") or "unknown") not in {"critical"}
    checks.append(
        _check(
            "queue_pressure",
            queue_ok,
            "Queue depth should not be critical before broad launch.",
            observed=queue,
        )
    )
    if not queue_ok:
        decision = "block"

    provider_blocked = [
        row for row in context.get("provider_slots", [])
        if float(row.get("active_slots") or 0.0) >= float(row.get("max_concurrent") or 0.0)
        and float(row.get("max_concurrent") or 0.0) > 0
    ]
    checks.append(
        _check(
            "provider_capacity",
            not provider_blocked,
            "Provider slots should have room or an explicit throttle.",
            observed={"blocked": provider_blocked, "all": context.get("provider_slots", [])},
        )
    )
    if provider_blocked and decision == "allow":
        decision = "inspect"

    return {
        "verdict": decision,
        "checks": checks,
        "repair_actions": repairs,
        "summary": _launch_summary(decision),
    }


def _failure_triage(
    query: OperatorNextQuery,
    subsystems: Any,
    context: dict[str, Any],
) -> dict[str, Any]:
    run = context.get("run") or {}
    jobs = list(run.get("jobs") or [])
    failed = [
        job for job in jobs
        if str(job.get("status") or "").lower() in _TERMINAL_FAILURE
        or str(job.get("last_error_code") or job.get("failure_category") or "").strip()
    ]
    groups: dict[str, dict[str, Any]] = {}
    for job in failed:
        code = _job_failure_code(job)
        group = groups.setdefault(
            code,
            {
                "failure_code": code,
                "count": 0,
                "job_labels": [],
                "retry_eligible": code in _TRANSIENT_RETRY_CODES,
            },
        )
        group["count"] += 1
        label = str(job.get("label") or job.get("job_label") or "").strip()
        if label:
            group["job_labels"].append(label)

    retryable = [item for item in groups.values() if item["retry_eligible"]]
    non_retryable = [item for item in groups.values() if not item["retry_eligible"]]
    verdict = "fix_before_retry" if non_retryable else ("retry_with_delta" if retryable else "no_failures_found")
    return {
        "verdict": verdict,
        "run_id": query.run_id,
        "failure_groups": sorted(groups.values(), key=lambda item: (-item["count"], item["failure_code"])),
        "retry_contract": {
            "required_fields": ["previous_failure", "retry_delta"],
            "rule": "Retry is allowed only when the prior failure is explicit and the next attempt materially differs.",
        },
        "recommended_actions": _failure_recommendations(retryable, non_retryable, query.run_id),
        "summary": "Grouped by root-looking reason code; capacity failures are retryable only with a declared delta.",
    }


def _manifest_audit(
    query: OperatorNextQuery,
    subsystems: Any,
    context: dict[str, Any],
) -> dict[str, Any]:
    manifest, source = _load_manifest(query, subsystems)
    findings: list[dict[str, Any]] = []
    jobs = _manifest_jobs(manifest)
    if not jobs:
        findings.append(_finding("manifest.no_jobs", "Manifest/spec has no jobs array.", severity="P1"))

    verifier_refs = _verifier_refs(manifest, jobs)
    verifier_state = _known_verifiers(_conn(subsystems), verifier_refs)
    missing_verifiers = sorted(set(verifier_refs) - set(verifier_state.get("known", [])))
    if missing_verifiers:
        findings.append(
            _finding(
                "manifest.unknown_verifiers",
                "Manifest references verifier refs not present in verifier authority.",
                severity="P1",
                evidence={"missing": missing_verifiers},
            )
        )

    for index, job in enumerate(jobs):
        label = str(job.get("label") or job.get("job_label") or f"job_{index + 1}")
        write_scope = _string_list(job.get("write_scope"))
        if _job_requires_submission(job) and not write_scope:
            findings.append(
                _finding(
                    "manifest.write_scope_missing",
                    f"Job {label} appears submission-producing but has no write_scope.",
                    severity="P1",
                    evidence={"job": label},
                )
            )
        execution_manifest = _execution_manifest(job)
        execution_write_scope = _execution_manifest_write_scope(execution_manifest)
        if write_scope and execution_write_scope and set(write_scope) != set(execution_write_scope):
            findings.append(
                _finding(
                    "manifest.write_scope_drift",
                    f"Job {label} has different job and execution-manifest write scopes.",
                    severity="P1",
                    evidence={
                        "job": label,
                        "job_write_scope": write_scope,
                        "execution_manifest_write_scope": execution_write_scope,
                    },
                )
            )
        artifacts = _declared_artifacts(job)
        scope_candidates = [*write_scope, *execution_write_scope]
        if artifacts and any(scope.startswith("scratch/") for scope in scope_candidates):
            findings.append(
                _finding(
                    "manifest.scratch_scope_for_declared_artifact",
                    f"Job {label} declares durable artifacts but writes to scratch fallback scope.",
                    severity="P1",
                    evidence={
                        "job": label,
                        "artifacts": artifacts,
                        "job_write_scope": write_scope,
                        "execution_manifest_write_scope": execution_write_scope,
                    },
                )
            )
        out_of_scope = [
            artifact for artifact in artifacts
            if write_scope and not any(_scope_allows_path(artifact, scope) for scope in write_scope)
        ]
        if out_of_scope:
            findings.append(
                _finding(
                    "manifest.artifact_outside_write_scope",
                    f"Job {label} declares artifacts outside write_scope.",
                    severity="P1",
                    evidence={"job": label, "artifacts": out_of_scope, "write_scope": write_scope},
                )
            )
        allowed_tools = _string_list(job.get("allowed_tools") or job.get("mcp_tools"))
        execution_tools = _execution_manifest_tools(execution_manifest)
        if allowed_tools and execution_tools and set(allowed_tools) != set(execution_tools):
            findings.append(
                _finding(
                    "manifest.tool_allowlist_drift",
                    f"Job {label} has different job and execution-manifest tool allowlists.",
                    severity="P2",
                    evidence={
                        "job": label,
                        "job_allowed_tools": allowed_tools,
                        "execution_manifest_tools": execution_tools,
                    },
                )
            )
        job_verify_refs = _string_list(job.get("verify_refs"))
        execution_verify_refs = _string_list(execution_manifest.get("verify_refs")) if execution_manifest else []
        if job_verify_refs and execution_verify_refs and set(job_verify_refs) != set(execution_verify_refs):
            findings.append(
                _finding(
                    "manifest.verify_ref_drift",
                    f"Job {label} has different job and execution-manifest verifier refs.",
                    severity="P1",
                    evidence={
                        "job": label,
                        "job_verify_refs": job_verify_refs,
                        "execution_manifest_verify_refs": execution_verify_refs,
                    },
                )
            )
        if "praxis_orient" not in allowed_tools:
            findings.append(
                _finding(
                    "manifest.orient_not_admitted",
                    f"Job {label} may not admit praxis_orient for standing-order context.",
                    severity="P3",
                    evidence={"job": label, "allowed_tools_sample": allowed_tools[:10]},
                )
            )

    verdict = "pass" if not any(f["severity"] in {"P0", "P1"} for f in findings) else "block"
    return {
        "verdict": verdict,
        "source": source,
        "job_count": len(jobs),
        "findings": findings,
        "repair_actions": _manifest_repairs(findings),
        "summary": "Manifest audit checks scope, artifact, allowed-tool, and verifier authority alignment.",
    }


def _toolsmith(
    query: OperatorNextQuery,
    subsystems: Any,
    context: dict[str, Any],
) -> dict[str, Any]:
    catalog = _catalog_entries()
    intent_terms = _terms(query.intent or query.tool_name or "")
    scored: list[dict[str, Any]] = []
    for tool in catalog:
        text = " ".join(
            str(tool.get(key) or "")
            for key in ("name", "description", "surface", "tier", "when_to_use", "when_not_to_use")
        ).lower()
        overlap = sum(1 for term in intent_terms if term in text)
        if query.tool_name and query.tool_name in str(tool.get("name") or ""):
            overlap += 3
        if overlap:
            scored.append({"score": overlap, **tool})
    scored.sort(key=lambda item: (-int(item.get("score") or 0), str(item.get("name") or "")))
    similar = scored[: query.limit]
    verdict = "compose_existing" if similar else "register_new_composite"
    return {
        "verdict": verdict,
        "similar_tools": similar,
        "new_tool_contract": {
            "required": [
                "single authority",
                "typed input model",
                "typed output shape",
                "risk level",
                "when_to_use / when_not_to_use",
                "verifier or observable success condition",
                "receipt-backed gateway operation",
            ],
            "preferred_shape": "compose existing tools behind one gateway-backed query/command before adding leaf tools",
        },
        "recommended_actions": [
            {
                "action": "reuse_or_compose",
                "reason": "Dedupe against similar catalog entries before registering a new operation.",
            },
            {
                "action": "register_operation",
                "tool": "praxis_register_operation",
                "reason": "If the handler is genuinely new, register it through the catalog wizard after import checks pass.",
            },
        ],
        "summary": "Toolsmith is a preflight: it narrows, dedupes, and emits the contract for safe tool growth.",
    }


def _unlock_frontier(
    query: OperatorNextQuery,
    subsystems: Any,
    context: dict[str, Any],
) -> dict[str, Any]:
    tool_legality = _legal_tools_payload(query, subsystems)
    facts = set(query.facts)
    facts.update(_facts_from_context(context))
    facts.update(
        f"state:{key}"
        for key, value in query.state.items()
        if value not in (None, "", [], {})
    )
    actions = _catalog_action_contracts()
    legal: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []
    missing_counter: Counter[str] = Counter()

    for action in actions:
        missing = sorted(set(action["consumes"]) - facts)
        if missing:
            blocked.append({**action, "missing": missing})
            for fact in missing:
                missing_counter[fact] += 1
        else:
            legal.append(action)

    repairs: list[dict[str, Any]] = []
    for fact, count in missing_counter.most_common(query.limit):
        unlocked = [
            item["action_ref"]
            for item in blocked
            if fact in item["missing"] and len(item["missing"]) == 1
        ]
        repairs.append(
            {
                "missing_fact": fact,
                "blocked_action_count": count,
                "immediately_unlocked_if_added": unlocked[:10],
                "score": count + len(unlocked),
            }
        )

    return {
        "verdict": "frontier_computed",
        "mathematical_model": {
            "graph": "typed action hypergraph",
            "objective": "choose missing facts that unlock the most actions per repair",
            "approximation": "greedy hitting-set/frontier gain heuristic",
        },
        "known_facts": sorted(facts),
        "legal_actions": legal[: query.limit],
        "blocked_count": len(blocked),
        "metadata_coverage": {
            "catalog_actions_with_type_contracts": len(actions),
            "note": "Coverage improves as tools declare consumes/produces contracts.",
        },
        "tool_legality": tool_legality,
        "best_repairs": repairs,
        "summary": "Finds high-leverage missing authority facts instead of scanning the raw tool pile.",
    }


def _legal_tools_payload(query: OperatorNextQuery, subsystems: Any) -> dict[str, Any]:
    """Embed the legacy legal-tools compiler under the progressive front door."""

    try:
        from runtime.operations.queries.operator_synthesis import (
            QueryLegalTools,
            handle_query_legal_tools,
        )

        payload = handle_query_legal_tools(
            QueryLegalTools(
                intent=query.intent,
                run_id=query.run_id,
                state=query.state,
                allowed_tools=query.allowed_tools,
                include_blocked=query.include_blocked,
                include_mutating=query.include_mutating,
                limit=query.limit,
            ),
            subsystems,
        )
    except Exception as exc:
        return {
            "ok": False,
            "error_code": "operator.next.legal_tools_unavailable",
            "error": str(exc),
        }
    return {
        "ok": True,
        "view": "legal_tools",
        "legal_action_count": payload.get("legal_action_count", 0),
        "blocked_action_count": payload.get("blocked_action_count", 0),
        "legal_actions": payload.get("legal_actions", []),
        "blocked_actions": payload.get("blocked_actions", []),
        "typed_gaps": payload.get("typed_gaps", []),
        "repair_actions": payload.get("repair_actions", []),
        "state": payload.get("state", {}),
        "authority_sources": payload.get("authority_sources", []),
    }


def _conn(subsystems: Any) -> Any:
    getter = getattr(subsystems, "get_pg_conn", None)
    return getter() if callable(getter) else None


def _repo_root(subsystems: Any) -> Path:
    raw = getattr(subsystems, "_repo_root", None) or getattr(subsystems, "repo_root", None)
    if raw is None:
        return Path.cwd()
    return Path(raw)


def _safe_execute(conn: Any, sql: str, *params: Any) -> list[dict[str, Any]]:
    if conn is None or not hasattr(conn, "execute"):
        return []
    try:
        rows = conn.execute(sql, *params)
    except Exception:
        return []
    return [_row_dict(row) for row in (rows or [])]


def _row_dict(row: object) -> dict[str, Any]:
    if isinstance(row, dict):
        return dict(row)
    if hasattr(row, "items"):
        return dict(row.items())
    return dict(row)  # type: ignore[arg-type]


def _queue_snapshot(conn: Any) -> dict[str, Any]:
    if conn is None:
        return {"queue_depth_status": "unknown", "queue_depth_error": "pg connection unavailable"}
    try:
        snapshot = query_queue_depth_snapshot(
            conn,
            warning_threshold=DEFAULT_QUEUE_WARNING_THRESHOLD,
            critical_threshold=DEFAULT_QUEUE_CRITICAL_THRESHOLD,
        )
        return {
            "queue_depth_status": snapshot.status,
            "pending": snapshot.pending,
            "ready": snapshot.ready,
            "claimed": snapshot.claimed,
            "running": snapshot.running,
            "total": snapshot.total_queued,
            "utilization_pct": snapshot.utilization_pct,
        }
    except Exception as exc:
        return {"queue_depth_status": "unknown", "queue_depth_error": str(exc)}


def _provider_slots(conn: Any) -> list[dict[str, Any]]:
    return _safe_execute(
        conn,
        """
        SELECT provider_slug, max_concurrent, active_slots, cost_weight_default, updated_at
        FROM provider_concurrency
        ORDER BY provider_slug
        """,
    )


def _host_resource_leases(conn: Any) -> list[dict[str, Any]]:
    return _safe_execute(
        conn,
        """
        SELECT resource_key, holder_id, expires_at
        FROM execution_leases
        WHERE resource_key LIKE 'host_resource:%'
        ORDER BY expires_at DESC
        LIMIT 20
        """,
    )


def _operation_catalog_snapshot(conn: Any, *, limit: int) -> list[dict[str, Any]]:
    return _safe_execute(
        conn,
        """
        SELECT operation_ref, operation_name, operation_kind, posture, idempotency_policy
        FROM operation_catalog_registry
        WHERE COALESCE(enabled, TRUE) IS TRUE
        ORDER BY operation_name
        LIMIT $1
        """,
        limit,
    )


def _run_snapshot(conn: Any, run_id: str | None) -> dict[str, Any]:
    if not run_id:
        return {}
    try:
        from runtime.workflow.unified import get_run_status, summarize_run_health

        run = get_run_status(conn, run_id)
        if not isinstance(run, dict):
            return {"run_id": run_id, "status": "not_found"}
        now = datetime.now(timezone.utc)
        health = summarize_run_health(run, now)
        jobs = [_row_dict(job) for job in run.get("jobs", [])]
        status_counts = Counter(str(job.get("status") or "unknown") for job in jobs)
        failed_jobs = [
            {
                "label": job.get("label"),
                "status": job.get("status"),
                "failure_code": _job_failure_code(job),
            }
            for job in jobs
            if str(job.get("status") or "").lower() in _TERMINAL_FAILURE
            or str(job.get("last_error_code") or job.get("failure_category") or "").strip()
        ]
        return {
            "run_id": run_id,
            "status": run.get("status"),
            "spec_name": run.get("spec_name"),
            "total_jobs": run.get("total_jobs"),
            "completed_jobs": run.get("completed_jobs"),
            "jobs": jobs,
            "status_counts": dict(status_counts),
            "failed_jobs": failed_jobs,
            "health": health,
        }
    except Exception as exc:
        return {"run_id": run_id, "status": "unknown", "error": str(exc)}


def _catalog_snapshot(*, limit: int) -> dict[str, Any]:
    entries = _catalog_entries()
    surface_counts = Counter(str(row.get("surface") or "unknown") for row in entries)
    tier_counts = Counter(str(row.get("tier") or "unknown") for row in entries)
    return {
        "total_tools": len(entries),
        "surface_counts": dict(surface_counts.most_common()),
        "tier_counts": dict(tier_counts.most_common()),
        "sample": entries[:limit],
    }


def _catalog_entries() -> list[dict[str, Any]]:
    try:
        from surfaces.mcp.catalog import get_tool_catalog

        catalog = get_tool_catalog()
    except Exception:
        return []
    rows: list[dict[str, Any]] = []
    for name, definition in sorted(catalog.items()):
        rows.append(
            {
                "name": name,
                "surface": definition.cli_surface,
                "tier": definition.cli_tier,
                "kind": definition.kind,
                "risk_levels": list(definition.risk_levels),
                "description": definition.description,
                "when_to_use": definition.cli_when_to_use,
                "when_not_to_use": definition.cli_when_not_to_use,
            }
        )
    return rows


def _catalog_action_contracts() -> list[dict[str, Any]]:
    try:
        from surfaces.mcp.catalog import get_tool_catalog

        catalog = get_tool_catalog()
    except Exception:
        return []
    actions: list[dict[str, Any]] = []
    for name, definition in sorted(catalog.items()):
        for action, contract in definition.type_contract.items():
            actions.append(
                {
                    "action_ref": f"{name}:{action}",
                    "tool": name,
                    "action": action,
                    "consumes": list(contract.get("consumes") or []),
                    "produces": list(contract.get("produces") or []),
                    "risk": definition.risk_for_selector(action),
                    "surface": definition.cli_surface,
                }
            )
    return actions


def _action(action: str, label: str, params: dict[str, Any], *, reason: str) -> dict[str, Any]:
    return {
        "tool": "praxis_next",
        "action": action,
        "label": label,
        "input": {"action": action, **params},
        "reason": reason,
    }


def _dedupe_actions(actions: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, Any]] = []
    for action in actions:
        key = (str(action.get("tool") or ""), str(action.get("action") or ""))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(dict(action))
    return deduped


def _compact_params(params: Mapping[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in params.items() if value not in (None, "", [])}


def _check(
    name: str,
    passed: bool,
    message: str,
    *,
    observed: Any,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": "pass" if passed else "fail",
        "message": message,
        "observed": observed,
    }


def _launch_summary(decision: str) -> str:
    if decision == "allow":
        return "Launch gate passed from the available authority snapshots."
    if decision == "inspect":
        return "Launch is not hard-blocked, but capacity or evidence deserves inspection."
    return "Launch blocked until proof and independent runtime evidence exist."


def _has_runtime_work(run: Mapping[str, Any], context: Mapping[str, Any]) -> bool:
    observed = _runtime_work_observed(run, context)
    return bool(
        observed["fresh_heartbeats"]
        or observed["completed_jobs"]
        or observed["submissions"]
        or observed["host_resource_leases"]
        or observed["active_provider_slots"]
    )


def _runtime_work_observed(run: Mapping[str, Any], context: Mapping[str, Any]) -> dict[str, Any]:
    jobs = list(run.get("jobs") or [])
    fresh_heartbeats = 0
    completed_jobs = 0
    submissions = 0
    now = datetime.now(timezone.utc)
    for job in jobs:
        status = str(job.get("status") or "").lower()
        if status in _TERMINAL_SUCCESS:
            completed_jobs += 1
        if isinstance(job.get("submission"), Mapping):
            submissions += 1
        heartbeat_at = job.get("heartbeat_at")
        if status in _ACTIVE_JOB_STATUSES and isinstance(heartbeat_at, datetime):
            age = (now - heartbeat_at).total_seconds()
            if age < 300:
                fresh_heartbeats += 1
    active_provider_slots = sum(
        1 for row in context.get("provider_slots", [])
        if float(row.get("active_slots") or 0.0) > 0.0
    )
    return {
        "fresh_heartbeats": fresh_heartbeats,
        "completed_jobs": completed_jobs,
        "submissions": submissions,
        "host_resource_leases": len(context.get("host_resources") or []),
        "active_provider_slots": active_provider_slots,
    }


def _job_failure_code(job: Mapping[str, Any]) -> str:
    for key in ("last_error_code", "failure_category", "error_code", "reason_code"):
        value = str(job.get(key) or "").strip()
        if value:
            return value
    return str(job.get("status") or "unknown_failure")


def _failure_recommendations(
    retryable: Sequence[dict[str, Any]],
    non_retryable: Sequence[dict[str, Any]],
    run_id: str | None,
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if non_retryable:
        actions.append(
            {
                "action": "fix_contract_first",
                "reason": "At least one failure group is non-transient; retry would duplicate failure.",
                "failure_codes": [item["failure_code"] for item in non_retryable],
            }
        )
    if retryable:
        labels = [label for item in retryable for label in item.get("job_labels", [])]
        actions.append(
            {
                "action": "retry_with_delta",
                "tool": "praxis_workflow",
                "input": {
                    "action": "retry",
                    "run_id": run_id,
                    "label": labels[0] if labels else "<job-label>",
                    "previous_failure": "<receipt-backed failure>",
                    "retry_delta": "<what changed>",
                },
                "reason": "Only retry after declaring prior failure and material delta.",
            }
        )
    return actions


def _load_manifest(
    query: OperatorNextQuery,
    subsystems: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if isinstance(query.manifest, dict):
        return dict(query.manifest), {"kind": "inline"}
    raw_path = query.manifest_path or query.spec_path
    if not raw_path:
        return {}, {"kind": "missing"}
    root = _repo_root(subsystems).resolve()
    path = Path(raw_path)
    if not path.is_absolute():
        path = root / path
    resolved = path.resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        return {}, {"kind": "path", "path": str(resolved), "error": "path outside repo root"}
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except Exception as exc:
        return {}, {"kind": "path", "path": str(resolved), "error": str(exc)}
    return payload if isinstance(payload, dict) else {}, {"kind": "path", "path": str(resolved)}


def _manifest_jobs(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates = [manifest.get("jobs")]
    for key in ("spec", "workflow", "plan", "manifest"):
        nested = manifest.get(key)
        if isinstance(nested, Mapping):
            candidates.append(nested.get("jobs"))
    for candidate in candidates:
        if isinstance(candidate, list):
            return [dict(item) for item in candidate if isinstance(item, Mapping)]
    return []


def _verifier_refs(manifest: Mapping[str, Any], jobs: Sequence[Mapping[str, Any]]) -> list[str]:
    refs: list[str] = []
    refs.extend(_string_list(manifest.get("verify_refs")))
    for job in jobs:
        refs.extend(_string_list(job.get("verify_refs")))
        execution_manifest = job.get("execution_manifest")
        if isinstance(execution_manifest, Mapping):
            refs.extend(_string_list(execution_manifest.get("verify_refs")))
    return _dedupe_strings(refs)


def _execution_manifest(job: Mapping[str, Any]) -> dict[str, Any]:
    for key in ("execution_manifest", "manifest", "execution_bundle"):
        value = job.get(key)
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _execution_manifest_write_scope(execution_manifest: Mapping[str, Any]) -> list[str]:
    if not execution_manifest:
        return []
    access_policy = execution_manifest.get("access_policy")
    values = []
    values.extend(_string_list(execution_manifest.get("write_scope")))
    if isinstance(access_policy, Mapping):
        values.extend(_string_list(access_policy.get("write_scope")))
    return _dedupe_strings(values)


def _execution_manifest_tools(execution_manifest: Mapping[str, Any]) -> list[str]:
    if not execution_manifest:
        return []
    values = []
    values.extend(_string_list(execution_manifest.get("tool_allowlist")))
    values.extend(_string_list(execution_manifest.get("mcp_tools")))
    values.extend(_string_list(execution_manifest.get("allowed_tools")))
    return _dedupe_strings(values)


def _known_verifiers(conn: Any, refs: Sequence[str]) -> dict[str, Any]:
    if not refs:
        return {"known": []}
    rows = _safe_execute(
        conn,
        """
        SELECT verifier_ref
        FROM verifier_registry
        WHERE verifier_ref = ANY($1)
          AND COALESCE(enabled, TRUE) IS TRUE
        """,
        list(refs),
    )
    return {"known": [str(row.get("verifier_ref")) for row in rows if row.get("verifier_ref")]}


def _job_requires_submission(job: Mapping[str, Any]) -> bool:
    if job.get("submission_required") is True:
        return True
    result_kind = str(job.get("result_kind") or "").strip()
    if result_kind:
        return True
    label = str(job.get("label") or "").lower()
    return any(word in label for word in ("plan", "execute", "verify", "resolve"))


def _declared_artifacts(job: Mapping[str, Any]) -> list[str]:
    artifacts: list[str] = []
    for key in ("primary_paths", "artifact_paths", "output_paths", "expected_artifacts"):
        artifacts.extend(_string_list(job.get(key)))
    for key in ("prompt", "instructions", "description"):
        text = str(job.get(key) or "")
        for marker in ("PLAN.md", "EXECUTION.md", "CLOSEOUT.md"):
            if marker in text:
                artifacts.append(marker)
    return _dedupe_strings(artifacts)


def _finding(
    code: str,
    message: str,
    *,
    severity: str,
    evidence: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "code": code,
        "severity": severity,
        "message": message,
        **({"evidence": evidence} if evidence else {}),
    }


def _manifest_repairs(findings: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    repairs: list[dict[str, Any]] = []
    codes = {str(finding.get("code") or "") for finding in findings}
    if "manifest.write_scope_missing" in codes or "manifest.artifact_outside_write_scope" in codes:
        repairs.append(
            {
                "action": "align_artifact_paths_with_write_scope",
                "reason": "The manifest must be the single authority for writable artifacts.",
            }
        )
    if "manifest.write_scope_drift" in codes or "manifest.scratch_scope_for_declared_artifact" in codes:
        repairs.append(
            {
                "action": "recompile_manifest_scope_authority",
                "reason": "Execution manifests should carry the same durable artifact scope the job contract declares.",
            }
        )
    if "manifest.tool_allowlist_drift" in codes:
        repairs.append(
            {
                "action": "choose_one_tool_allowlist_authority",
                "reason": "Tool access should come from one manifest authority, not job text plus execution bundle drift.",
            }
        )
    if "manifest.verify_ref_drift" in codes:
        repairs.append(
            {
                "action": "choose_one_verifier_authority",
                "reason": "Verifier refs must be job-scoped and consistent with the execution manifest.",
            }
        )
    if "manifest.unknown_verifiers" in codes:
        repairs.append(
            {
                "action": "register_or_replace_verifier_refs",
                "reason": "Verifier refs must resolve through verifier authority before launch.",
            }
        )
    if "manifest.orient_not_admitted" in codes:
        repairs.append(
            {
                "action": "admit_praxis_orient_or_remove_instruction",
                "reason": "Do not instruct an agent to call an unavailable authority tool.",
            }
        )
    return repairs


def _facts_from_context(context: Mapping[str, Any]) -> list[str]:
    facts: list[str] = []
    queue = context.get("queue") or {}
    if str(queue.get("queue_depth_status") or "") not in {"critical", "unknown"}:
        facts.append("queue:healthy")
    if context.get("provider_slots"):
        facts.append("providers:observable")
    if context.get("host_resources"):
        facts.append("host_resources:active")
    run = context.get("run") or {}
    if str(run.get("status") or "").lower() in _TERMINAL_SUCCESS:
        facts.append("run:proof_success")
    if run.get("failed_jobs"):
        facts.append("run:failed")
    return facts


def _terms(text: str) -> list[str]:
    normalized = "".join(ch.lower() if ch.isalnum() else " " for ch in text)
    return [part for part in normalized.split() if len(part) > 2]


def _string_list(value: object) -> list[str]:
    if value in (None, ""):
        return []
    if isinstance(value, str):
        parts = [value]
    elif isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        parts = [str(item) for item in value]
    else:
        return []
    return _dedupe_strings(str(part).strip() for part in parts if str(part).strip())


def _dedupe_strings(values: Sequence[str] | Any) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            seen.add(text)
            result.append(text)
    return result


def _scope_allows_path(path: str, scope_path: str) -> bool:
    normalized_path = path.strip().strip("/")
    normalized_scope = scope_path.strip().strip("/")
    if not normalized_path or not normalized_scope:
        return False
    if normalized_path == normalized_scope:
        return True
    if "/" not in normalized_path and normalized_scope.endswith("/" + normalized_path):
        return True
    return normalized_path.startswith(normalized_scope.rstrip("/") + "/")


__all__ = ["OperatorNextQuery", "handle_operator_next"]
