"""Explicit CLI metadata for Praxis MCP tools.

This is the CLI-facing authority for discoverability, safety, and documentation.
It complements the tool-local MCP schema with operator-focused guidance.
"""

from __future__ import annotations

from typing import Any


def _example(title: str, input_payload: dict[str, Any]) -> dict[str, Any]:
    return {"title": title, "input": input_payload}


def _tool(
    *,
    surface: str,
    tier: str,
    recommended_alias: str | None,
    when_to_use: str,
    when_not_to_use: str,
    risks: dict[str, Any],
    examples: list[dict[str, Any]],
    replacement: str | None = None,
) -> dict[str, Any]:
    metadata = {
        "surface": surface,
        "tier": tier,
        "recommended_alias": recommended_alias,
        "when_to_use": when_to_use,
        "when_not_to_use": when_not_to_use,
        "risks": risks,
        "examples": examples,
    }
    if replacement:
        metadata["replacement"] = replacement
    return metadata


CLI_TOOL_METADATA: dict[str, dict[str, Any]] = {
    "praxis_agent_forge": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Preview and validate the CQRS path before registering or changing an agent principal.",
        when_not_to_use="Do not use it to mutate agent state; call praxis_agent_register after forge validation.",
        risks={"default": "read"},
        examples=[
            _example("Preview one agent principal", {"agent_principal_ref": "agent.exec.example"}),
        ],
    ),
    "praxis_agent_register": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Register or update one durable agent principal after praxis_agent_forge validation.",
        when_not_to_use="Do not use it for wake execution, delegation, or status-only reads.",
        risks={"default": "write"},
        examples=[
            _example(
                "Register one agent principal",
                {"agent_principal_ref": "agent.exec.example", "title": "Example Agent"},
            ),
        ],
    ),
    "praxis_agent_list": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="List durable agent principals and their current lifecycle status.",
        when_not_to_use="Do not use it to inspect one agent in depth; use praxis_agent_describe.",
        risks={"default": "read"},
        examples=[_example("List active agents", {"status": "active"})],
    ),
    "praxis_agent_describe": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Inspect one durable agent principal, including trust and scope metadata.",
        when_not_to_use="Do not use it for mutation; use register/status/wake tools for writes.",
        risks={"default": "read"},
        examples=[
            _example("Describe one agent", {"agent_principal_ref": "agent.exec.example"}),
        ],
    ),
    "praxis_agent_status": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Pause, activate, or kill one durable agent principal.",
        when_not_to_use="Do not use it for ordinary run/job status; use praxis_workflow.",
        risks={"default": "write"},
        examples=[
            _example("Pause one agent", {"agent_principal_ref": "agent.exec.example", "status": "paused"}),
        ],
    ),
    "praxis_agent_wake": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Record or request one agent wake through the agent-principal authority.",
        when_not_to_use="Do not use it for direct workflow launches; use praxis_workflow or praxis_solution.",
        risks={"default": "write"},
        examples=[
            _example("Wake one agent", {"agent_principal_ref": "agent.exec.example", "trigger_kind": "manual"}),
        ],
    ),
    "praxis_agent_wake_list": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="List recent agent wake records for inspection and debugging.",
        when_not_to_use="Do not use it to create wakes; use praxis_agent_wake.",
        risks={"default": "read"},
        examples=[
            _example("List recent wakes", {"agent_principal_ref": "agent.exec.example", "limit": 10}),
        ],
    ),
    "praxis_agent_delegate": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Create a governed delegation from one agent principal to a child task.",
        when_not_to_use="Do not use it for human operator workflow launches.",
        risks={"default": "write"},
        examples=[
            _example("Delegate one task", {"parent_agent_ref": "agent.exec.parent", "child_task": "review"}),
        ],
    ),
    "praxis_tool_gap_file": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="File a missing-tool gap as roadmap fuel before improvising around the missing capability.",
        when_not_to_use="Do not use it for ordinary bug reports; use praxis_bugs.",
        risks={"default": "write"},
        examples=[
            _example(
                "File one tool gap",
                {
                    "reporter_agent_ref": "agent.exec.example",
                    "missing_capability": "calendar read",
                    "attempted_task": "schedule prep",
                    "blocked_action": "inspect availability",
                },
            ),
        ],
    ),
    "praxis_tool_gap_list": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="List filed tool gaps and their triage state.",
        when_not_to_use="Do not use it to file a new gap; use praxis_tool_gap_file.",
        risks={"default": "read"},
        examples=[_example("List open tool gaps", {"status": "open", "limit": 10})],
    ),
    "praxis_access_control": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "List, disable, or enable model-access denial rows for a "
            "(provider × transport × job_type × model) selector without a migration."
        ),
        when_not_to_use=(
            "Do not use it for provider smoke tests or onboarding — use praxis_provider_onboard. "
            "Do not use it when you only need search or receipts."
        ),
        risks={
            "default": "read",
            "actions": {"list": "read", "disable": "write", "enable": "write"},
        },
        examples=[
            _example(
                "List denials for one provider",
                {"action": "list", "provider_slug": "openai", "transport_type": "CLI"},
            ),
            _example(
                "Disable a provider on CLI with decision provenance",
                {
                    "action": "disable",
                    "provider_slug": "openai",
                    "transport_type": "CLI",
                    "decision_ref": "architecture-policy::routing::disable-openai-cli",
                },
            ),
        ],
    ),
    "praxis_paid_model_access": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Preview, grant, revoke, bind, or inspect exact one-run leases for paid model access.",
        when_not_to_use="Do not use it for broad provider enables or unpaid route changes; use praxis_access_control for hard-off policy.",
        risks={
            "default": "read",
            "actions": {
                "bind_run": "write",
                "consume": "write",
                "grant_once": "write",
                "preview": "read",
                "revoke": "write",
                "soft_off": "write",
                "soft_on": "write",
                "status": "read",
            },
        },
        examples=[
            _example("Preview paid access state", {"action": "preview", "runtime_profile_ref": "praxis"}),
            _example(
                "Grant one exact paid route",
                {
                    "action": "grant_once",
                    "runtime_profile_ref": "praxis",
                    "job_type": "build",
                    "transport_type": "API",
                    "adapter_type": "llm_task",
                    "provider_slug": "fireworks",
                    "model_slug": "accounts/fireworks/models/kimi-k2p6",
                },
            ),
        ],
    ),
    "praxis_artifacts": _tool(
        surface="evidence",
        tier="stable",
        recommended_alias="artifacts",
        when_to_use="Browse sandbox outputs, search artifact paths, or compare generated files.",
        when_not_to_use="Do not use it for workflow receipt history or knowledge-graph recall.",
        risks={"default": "read", "actions": {"stats": "read", "list": "read", "search": "read", "diff": "read"}},
        examples=[
            _example("List one sandbox", {"action": "list", "sandbox_id": "sandbox_20260423_001"}),
            _example("Search generated outputs", {"action": "search", "query": "migration schema"}),
        ],
    ),
    "praxis_bugs": _tool(
        surface="evidence",
        tier="stable",
        recommended_alias="bugs",
        when_to_use="Inspect the bug tracker, run keyword or hybrid search, file a new bug, or drive replay-ready bug workflows.",
        when_not_to_use="Do not use it for general system status or semantic knowledge search.",
        risks={
            "default": "read",
            "actions": {
                "list": "read",
                "search": "read",
                "stats": "read",
                "packet": "read",
                "history": "read",
                "file": "write",
                "patch_resume": "write",
                "resolve": "write",
                "attach_evidence": "write",
                "replay": "launch",
                "backfill_replay": "launch",
            },
        },
        examples=[
            _example("List open P1 bugs", {"action": "list", "status": "OPEN", "severity": "P1"}),
            _example("Search open routing bugs", {"action": "search", "query": "routing", "status": "OPEN"}),
            _example(
                "File a new bug",
                {
                    "action": "file",
                    "title": "Runner hangs after retry",
                    "severity": "P1",
                    "discovered_in_receipt_id": "receipt-123",
                },
            ),
            _example(
                "Save investigation handoff on a bug",
                {
                    "action": "patch_resume",
                    "bug_id": "BUG-ABCDEF12",
                    "resume_patch": {
                        "hypothesis": "Lease renew races cancel",
                        "next_steps": ["Trace holder at timeout", "Compare with run X"],
                    },
                },
            ),
        ],
    ),
    "praxis_credential_capture": _tool(
        surface="setup",
        tier="stable",
        recommended_alias=None,
        when_to_use=(
            "Request, inspect, or open the secure host API-key entry window "
            "when a wizard/provider/setup flow needs a macOS Keychain-backed credential. "
            "Search terms: api key credential keychain secure window."
        ),
        when_not_to_use=(
            "Do not pass raw API keys to this tool. Do not use it for provider route "
            "onboarding; use praxis_provider_onboard after credentials are present."
        ),
        risks={
            "default": "read",
            "actions": {
                "request": "read",
                "status": "read",
                "capture": "write",
            },
        },
        examples=[
            _example(
                "Show secure-entry descriptor for OpenAI",
                {
                    "action": "request",
                    "env_var_name": "OPENAI_API_KEY",
                    "provider_label": "OpenAI",
                },
            ),
            _example(
                "Check whether the OpenAI key is present in Keychain",
                {
                    "action": "status",
                    "env_var_name": "OPENAI_API_KEY",
                    "provider_label": "OpenAI",
                },
            ),
        ],
    ),
    "praxis_circuits": _tool(
        surface="operations",
        tier="stable",
        recommended_alias="circuits",
        when_to_use="Inspect effective circuit-breaker state or apply a durable manual override for one provider.",
        when_not_to_use="Do not use it for task-route eligibility windows or generic health checks.",
        risks={
            "default": "read",
            "actions": {
                "list": "read",
                "history": "read",
                "open": "write",
                "close": "write",
                "reset": "write",
            },
        },
        examples=[
            _example("List effective circuit states", {"action": "list"}),
            _example("Show override history", {"action": "history", "provider_slug": "openai"}),
            _example("Force a provider open", {"action": "open", "provider_slug": "openai", "rationale": "Provider outage"}),
            _example("Force a provider closed", {"action": "close", "provider_slug": "anthropic", "rationale": "Allow manual recovery probes"}),
            _example("Clear the manual override", {"action": "reset", "provider_slug": "openai"}),
        ],
    ),
    "praxis_provider_control_plane": _tool(
        surface="operations",
        tier="stable",
        recommended_alias="provider-control-plane",
        when_to_use=(
            "Inspect the private provider/job/model matrix, including CLI/API type, cost, version, "
            "runnable state, breaker state, credential state, and removal reasons."
        ),
        when_not_to_use="Do not use it to change provider access; use circuit/control-panel commands for mutations.",
        risks={"default": "read"},
        examples=[
            _example("Read the whole provider matrix", {"runtime_profile_ref": "praxis"}),
            _example(
                "Read plan-generation API rows",
                {
                    "runtime_profile_ref": "praxis",
                    "job_type": "compile",
                    "transport_type": "API",
                },
            ),
        ],
    ),
    "praxis_provider_availability_refresh": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "Refresh provider availability through CQRS before trusting routing or launching "
            "a proof job. The resulting receipt is machine-checkable evidence for proof-launch "
            "approval when route truth is not already fresh. Persists provider_usage probe "
            "snapshots and emits a receipt-backed provider.availability.refreshed event."
        ),
        when_not_to_use=(
            "Do not use this as a dry-run evaluator and do not fire it repeatedly to hope "
            "capacity changes. Use it once when provider availability authority is stale or unknown."
        ),
        risks={"default": "write"},
        examples=[
            _example("Refresh admitted provider availability", {"max_concurrency": 4}),
            _example(
                "Refresh one provider",
                {"provider_slugs": ["openai"], "max_concurrency": 1},
            ),
        ],
    ),
    "praxis_model_access_control_matrix": _tool(
        surface="operations",
        tier="stable",
        recommended_alias=None,
        when_to_use=(
            "Inspect the live ON/OFF model-access switchboard by task type, CLI/API type, "
            "provider, model, scope, reason, and operator instruction."
        ),
        when_not_to_use="Do not use it as a mutation surface; it is the read model that drives provider catalog projection.",
        risks={"default": "read"},
        examples=[
            _example(
                "Read plan-generation API control state",
                {
                    "runtime_profile_ref": "praxis",
                    "job_type": "compile",
                    "transport_type": "API",
                },
            ),
            _example("Read disabled access methods", {"control_state": "off"}),
        ],
    ),
    "praxis_work_assignment_matrix": _tool(
        surface="operations",
        tier="stable",
        recommended_alias=None,
        when_to_use=(
            "Inspect grouped work by audit group, recommended model tier, task type, sequence, "
            "and assignment reason."
        ),
        when_not_to_use="Do not use it as the source of provider availability; use praxis_provider_control_plane for access capability.",
        risks={"default": "read"},
        examples=[
            _example("Read open assignment matrix", {"open_only": True}),
            _example("Read frontier work", {"recommended_model_tier": "frontier"}),
            _example("Read one audit group", {"audit_group": "A_provider_catalog_authority"}),
        ],
    ),
    "praxis_task_route_eligibility": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="task-route-eligibility",
        when_to_use=(
            "Allow or reject one provider/model candidate for one task type through a bounded "
            "eligibility window. Use this for by-task routing policy such as letting "
            "anthropic/claude-sonnet-4-6 participate in build or review without enabling it everywhere."
        ),
        when_not_to_use=(
            "Do not use it for broad provider onboarding or transport-wide ON/OFF control; "
            "use praxis_provider_onboard or praxis_access_control for those."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Allow Sonnet for build only",
                {
                    "provider_slug": "anthropic",
                    "model_slug": "claude-sonnet-4-6",
                    "task_type": "build",
                    "eligibility_status": "eligible",
                    "reason_code": "task_type_exception",
                    "rationale": "Allow sonnet for build high and build mid",
                },
            ),
            _example(
                "Reject one provider for review until tomorrow",
                {
                    "provider_slug": "anthropic",
                    "task_type": "review",
                    "eligibility_status": "rejected",
                    "reason_code": "provider_disabled",
                    "effective_to": "2026-04-30T09:00:00-07:00",
                    "rationale": "Temporary hold during provider investigation",
                },
            ),
        ],
    ),
    "praxis_task_route_request": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="task-route-request",
        when_to_use=(
            "Mutate request-shape knobs for one task route through CQRS authority: "
            "temperature, max_tokens, reasoning_control, request_contract_ref, cache policy, "
            "structured-output policy, or streaming policy."
        ),
        when_not_to_use=(
            "Do not use it to allow, reject, onboard, or admit a route. Eligibility stays "
            "with praxis_task_route_eligibility; admission/access stays with provider control surfaces."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Set compile request shape",
                {
                    "task_type": "materialize",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "temperature": 0.2,
                    "max_tokens": 32768,
                    "reason_code": "request_contract_tuning",
                },
            ),
            _example(
                "Attach request contract",
                {
                    "task_type": "materialize",
                    "provider_slug": "openai",
                    "model_slug": "gpt-5.4",
                    "request_contract_ref": "llm_request_contract.openai.gpt-5-4.api.compile",
                },
            ),
        ],
    ),
    "praxis_execution_truth": _tool(
        surface="operations",
        tier="stable",
        recommended_alias=None,
        when_to_use=(
            "Check whether workflow work is actually firing by combining status, run views, "
            "and causal trace evidence."
        ),
        when_not_to_use="Do not use it to launch, retry, or mutate workflow state.",
        risks={"default": "read"},
        examples=[
            _example("Read platform execution truth", {"since_hours": 24}),
            _example(
                "Read one run with trace proof",
                {"run_id": "run_abc123", "include_trace": True},
            ),
        ],
    ),
    "praxis_runtime_truth_snapshot": _tool(
        surface="operations",
        tier="stable",
        recommended_alias="runtime-truth",
        when_to_use=(
            "Inspect observed workflow runtime truth across DB authority, queue state, "
            "worker heartbeats, provider slots, host-resource leases, Docker, manifest "
            "hydration audit, and recent typed failures."
        ),
        when_not_to_use="Do not use it to repair or retry; it is the evidence packet.",
        risks={"default": "read"},
        examples=[
            _example("Read runtime truth", {"since_minutes": 60}),
            _example("Read one run truth", {"run_id": "run_abc123"}),
        ],
    ),
    "praxis_firecheck": _tool(
        surface="operations",
        tier="stable",
        recommended_alias="firecheck",
        when_to_use=(
            "Run before launching or retrying workflows to prove work can actually fire, "
            "including typed blockers and remediation plans."
        ),
        when_not_to_use="Do not use it as a retry command; it is the proof gate before retry.",
        risks={"default": "read"},
        examples=[
            _example("Check launch readiness", {}),
            _example("Check one run", {"run_id": "run_abc123"}),
        ],
    ),
    "praxis_remediation_plan": _tool(
        surface="operations",
        tier="stable",
        recommended_alias="remediation-plan",
        when_to_use=(
            "Explain the safe remediation tier, evidence requirements, approval gate, "
            "and retry delta for a typed workflow failure."
        ),
        when_not_to_use="Do not use it to apply repairs; it only declares the allowed plan.",
        risks={"default": "read"},
        examples=[
            _example("Plan a context repair", {"failure_type": "context_not_hydrated"}),
            _example("Plan from a failure code", {"failure_code": "host_resource_capacity"}),
        ],
    ),
    "praxis_remediation_apply": _tool(
        surface="operations",
        tier="stable",
        recommended_alias="remediation-apply",
        when_to_use=(
            "Apply only guarded local runtime repairs, such as stale provider slot cleanup "
            "or expired host-resource lease cleanup, before one explicit retry."
        ),
        when_not_to_use="Do not use it to retry jobs, edit code, or repair credentials.",
        risks={"default": "write", "dry_run": "read"},
        examples=[
            _example("Preview stale slot cleanup", {"failure_type": "provider.capacity"}),
            _example(
                "Apply stale slot cleanup",
                {"failure_type": "provider.capacity", "dry_run": False, "confirm": True},
            ),
        ],
    ),
    "praxis_next_work": _tool(
        surface="operator",
        tier="stable",
        recommended_alias=None,
        when_to_use=(
            "Choose the next bounded work item from refactor heatmap, bug triage, "
            "assignment matrix, and runtime status."
        ),
        when_not_to_use="Do not use it to resolve bugs or mutate roadmap authority.",
        risks={"default": "read"},
        examples=[
            _example("Read top next work", {"limit": 10}),
            _example("Read more bug-heavy work", {"bug_limit": 50, "work_limit": 20}),
        ],
    ),
    "praxis_provider_route_truth": _tool(
        surface="operations",
        tier="stable",
        recommended_alias=None,
        when_to_use=(
            "Check whether a provider/model/job route is runnable or blocked, including "
            "control state and removal reasons. Use the returned route truth as proof-launch "
            "evidence when approving a proposed plan."
        ),
        when_not_to_use="Do not use it to change access; use praxis_access_control or praxis_circuits.",
        risks={"default": "read"},
        examples=[
            _example("Read all route truth", {"runtime_profile_ref": "praxis"}),
            _example(
                "Read plan-generation API route truth",
                {"job_type": "compile", "transport_type": "API"},
            ),
        ],
    ),
    "praxis_operation_forge": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="operation-forge",
        when_to_use=(
            "Preview the CQRS operation/tool registration path before adding a new "
            "operation or MCP wrapper. Use it to get the exact register payload, "
            "tool binding, fast-feedback commands, and command/query defaults."
        ),
        when_not_to_use="Do not use it as a mutation surface; it prepares the canonical payload.",
        risks={"default": "read"},
        examples=[
            _example(
                "Preview a query operation",
                {
                    "operation_name": "operator.example_truth",
                    "handler_ref": "runtime.operations.queries.operator_composed.handle_query_example_truth",
                    "input_model_ref": "runtime.operations.queries.operator_composed.QueryExampleTruth",
                    "authority_domain_ref": "authority.workflow_runs",
                },
            ),
            _example(
                "Preview a command operation",
                {
                    "operation_name": "operator.example_apply",
                    "operation_kind": "command",
                    "tool_name": "praxis_example_apply",
                    "recommended_alias": "example-apply",
                    "handler_ref": "runtime.operations.commands.example.handle_example_apply",
                    "input_model_ref": "runtime.operations.commands.example.ExampleApplyCommand",
                    "authority_domain_ref": "authority.workflow_runs",
                    "event_type": "operator.example.applied",
                },
            ),
        ],
    ),
    "praxis_connector": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Build, inspect, register, or verify third-party API connectors.",
        when_not_to_use="Do not use it for invoking an existing integration at runtime.",
        risks={
            "default": "launch",
            "actions": {
                "build": "launch",
                "list": "read",
                "get": "read",
                "register": "write",
                "verify": "launch",
            },
        },
        examples=[
            _example("Build a connector", {"action": "build", "app_name": "Slack"}),
            _example("Verify a built connector", {"action": "verify", "app_slug": "slack"}),
        ],
    ),
    "praxis_data": _tool(
        surface="data",
        tier="stable",
        recommended_alias="data",
        when_to_use="Run deterministic parsing, normalization, validation, mapping, dedupe, or reconcile jobs and optionally launch them through the workflow engine.",
        when_not_to_use="Do not use it for fuzzy inference, free-form classification, or cases where an LLM must invent the transform logic.",
        risks={
            "default": "read",
            "actions": {
                "parse": "read",
                "profile": "read",
                "filter": "read",
                "sort": "read",
                "normalize": "write",
                "repair": "write",
                "repair_loop": "write",
                "backfill": "write",
                "redact": "write",
                "checkpoint": "read",
                "replay": "read",
                "approve": "write",
                "apply": "write",
                "validate": "read",
                "transform": "write",
                "join": "read",
                "merge": "read",
                "aggregate": "read",
                "split": "read",
                "export": "read",
                "dead_letter": "read",
                "dedupe": "write",
                "reconcile": "read",
                "sync": "write",
                "run": "write",
                "workflow_spec": "write",
                "launch": "launch",
            },
        },
        examples=[
            _example("Profile a dataset", {"action": "profile", "input_path": "artifacts/data/users.csv"}),
            _example("Filter active rows", {"action": "filter", "input_path": "artifacts/data/users.csv", "predicates": [{"field": "status", "op": "equals", "value": "active"}]}),
            _example("Join two sources", {"action": "join", "input_path": "artifacts/data/users.json", "secondary_input_path": "artifacts/data/orders.json", "keys": ["user_id"], "right_prefix": "order_"}),
            _example("Aggregate by status", {"action": "aggregate", "input_path": "artifacts/data/orders.json", "group_by": ["status"], "aggregations": [{"op": "count", "as": "row_count"}]}),
            _example("Normalize email addresses", {"action": "normalize", "input_path": "artifacts/data/users.csv", "rules": {"email": ["trim", "lower"]}}),
            _example("Repair pending rows", {"action": "repair", "input_path": "artifacts/data/users.json", "predicates": [{"field": "status", "op": "equals", "value": "pending"}], "repairs": {"status": {"value": "active"}}}),
            _example("Run a repair loop", {"action": "repair_loop", "input_path": "artifacts/data/users.json", "repairs": {"status": {"value": "active"}}, "schema": {"email": {"required": True, "regex": ".+@.+"}}}),
            _example("Backfill missing country", {"action": "backfill", "input_path": "artifacts/data/users.json", "backfill": {"country": {"value": "US"}}}),
            _example("Redact PII fields", {"action": "redact", "input_path": "artifacts/data/users.json", "redactions": {"email": "mask_email", "ssn": "remove"}}),
            _example("Checkpoint a cursor", {"action": "checkpoint", "input_path": "artifacts/data/events.json", "keys": ["id"], "cursor_field": "updated_at"}),
            _example("Replay beyond a checkpoint", {"action": "replay", "input_path": "artifacts/data/events.json", "cursor_field": "updated_at", "checkpoint_path": "artifacts/data/events.checkpoint.json"}),
            _example("Approve a reconcile plan", {"action": "approve", "plan_path": "artifacts/data/reconcile_receipt.json", "approved_by": "ops", "approval_reason": "Reviewed diff and counts"}),
            _example("Apply an approved plan", {"action": "apply", "plan_path": "artifacts/data/reconcile_receipt.json", "approval_path": "artifacts/data/reconcile.approval.json", "secondary_input_path": "artifacts/data/target.json", "keys": ["id"]}),
            _example("Merge two keyed sources", {"action": "merge", "input_path": "artifacts/data/crm.json", "secondary_input_path": "artifacts/data/billing.json", "keys": ["id"], "precedence": "right"}),
            _example("Split rows by status", {"action": "split", "input_path": "artifacts/data/users.json", "split_by_field": "status", "output_path": "artifacts/data/users_by_status"}),
            _example("Export selected fields", {"action": "export", "input_path": "artifacts/data/users.json", "fields": ["id", "email"], "field_map": {"email": "user_email"}}),
            _example("Route invalid rows to dead-letter", {"action": "dead_letter", "input_path": "artifacts/data/users.json", "schema": {"email": {"required": True, "regex": ".+@.+"}}, "output_path": "artifacts/data/users_dead_letter"}),
            _example("Reconcile two sources", {"action": "reconcile", "input_path": "artifacts/data/source.json", "secondary_input_path": "artifacts/data/target.json", "keys": ["id"]}),
            _example("Sync target state", {"action": "sync", "input_path": "artifacts/data/source.json", "secondary_input_path": "artifacts/data/target.json", "keys": ["id"], "sync_mode": "mirror"}),
            _example("Run checkpointed batch sync", {"action": "sync", "input_path": "artifacts/data/source.json", "secondary_input_path": "artifacts/data/target.json", "keys": ["id"], "sync_mode": "upsert", "cursor_field": "updated_at", "checkpoint_path": "artifacts/data/source.checkpoint.json", "batch_size": 500}),
        ],
    ),
    "praxis_constraints": _tool(
        surface="evidence",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Inspect mined constraints and scope-specific guardrails.",
        when_not_to_use="Do not use it for code similarity or bug enumeration.",
        risks={"default": "read", "actions": {"list": "read", "for_scope": "read"}},
        examples=[
            _example("List recent constraints", {"action": "list"}),
            _example("Check scope-specific constraints", {"action": "for_scope", "scope_files": ["runtime/workflow.py"]}),
        ],
    ),
    "praxis_context_shard": _tool(
        surface="session",
        tier="session",
        recommended_alias=None,
        when_to_use="Read the bounded execution shard for the active workflow MCP session.",
        when_not_to_use="Do not use it outside workflow-session execution or as a general repository browser.",
        risks={"default": "session", "views": {"full": "session", "summary": "session", "sections": "session"}},
        examples=[
            _example("Read the shard summary", {"view": "summary"}),
            _example("Read one section", {"view": "sections", "section_name": "TASK BRIEF"}),
        ],
    ),
    "praxis_decompose": _tool(
        surface="planning",
        tier="stable",
        recommended_alias=None,
        when_to_use="Break a large objective into workflow-sized micro-sprints before workflow launch.",
        when_not_to_use="Do not use it to execute work or inspect historical run state.",
        risks={"default": "read"},
        examples=[
            _example("Decompose a platform change", {"objective": "Consolidate operator read and write surfaces"}),
        ],
    ),
    "praxis_discover": _tool(
        surface="code",
        tier="stable",
        recommended_alias="discover",
        when_to_use="Search for existing code by behavior with hybrid retrieval before building something new.",
        when_not_to_use="Do not use it for architectural decisions or receipt analytics.",
        risks={"default": "read", "actions": {"search": "read", "stats": "read", "reindex": "write"}},
        examples=[
            _example("Search by behavior", {"action": "search", "query": "retry logic with exponential backoff"}),
            _example("Search function-level matches", {"action": "search", "query": "parse JSON from stdin", "kind": "function"}),
            _example("Search module-level matches", {"action": "search", "query": "Postgres connection pooling", "kind": "module"}),
            _example("Refresh the index", {"action": "reindex"}),
        ],
    ),
    "praxis_diagnose": _tool(
        surface="operations",
        tier="stable",
        recommended_alias="diagnose",
        when_to_use="Diagnose one workflow run by id and combine receipt, failure, and provider health context.",
        when_not_to_use="Do not use it for broad health checks or generic receipt search.",
        risks={"default": "read"},
        examples=[
            _example("Diagnose a specific run", {"run_id": "run_abc123"}),
        ],
    ),
    "praxis_friction": _tool(
        surface="evidence",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Inspect friction and guardrail events that are slowing workflows down.",
        when_not_to_use="Do not use it for health probes or general bug search.",
        risks={"default": "read", "actions": {"stats": "read", "list": "read", "patterns": "read"}},
        examples=[
            _example("Show friction stats", {"action": "stats"}),
            _example("List recent friction events", {"action": "list", "limit": 20}),
            _example("Show repeated CLI failures", {"action": "patterns", "source": "cli.workflow"}),
        ],
    ),
    "praxis_action_fingerprints": _tool(
        surface="evidence",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Record one raw shell/edit/write/read harness action so recurring patterns can surface as tool opportunities.",
        when_not_to_use="Do not use it for gateway operation receipts or general friction analytics.",
        risks={"default": "write", "actions": {"record": "write"}},
        examples=[
            _example(
                "Record one raw shell action",
                {
                    "action": "record",
                    "tool_name": "local_shell",
                    "source_surface": "codex:host",
                    "tool_input": {"command": ["pytest", "Code&DBs/Workflow/tests/test_x.py", "-q"]},
                },
            ),
            _example(
                "Record one raw file read",
                {
                    "action": "record",
                    "tool_name": "read_file",
                    "source_surface": "gemini:host",
                    "tool_input": {"file_path": "Code&DBs/Workflow/runtime/workflow/_admission.py"},
                },
            ),
        ],
    ),
    "praxis_get_submission": _tool(
        surface="submissions",
        tier="session",
        recommended_alias=None,
        when_to_use="Read a sealed submission in the active workflow MCP session.",
        when_not_to_use="Do not use it outside token-scoped workflow review flows.",
        risks={"default": "session"},
        examples=[
            _example("Read a submission", {"submission_id": "submission_abc123"}),
        ],
    ),
    "praxis_governance": _tool(
        surface="governance",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Scan prompts and scope for policy, secret, or governance violations.",
        when_not_to_use="Do not use it as a general quality dashboard or health probe.",
        risks={"default": "read", "actions": {"scan_prompt": "read", "scan_scope": "read"}},
        examples=[
            _example("Scan a prompt", {"action": "scan_prompt", "text": "Ship the API key in the test fixture"}),
            _example("Scan a scope", {"action": "scan_scope", "write_paths": ["config/runtime_profiles.json"]}),
        ],
    ),
    "praxis_graph": _tool(
        surface="knowledge",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Inspect blast radius and graph neighbors for a known knowledge-graph entity.",
        when_not_to_use="Do not use it for broad knowledge search; use recall first when you need ranked candidates.",
        risks={"default": "read"},
        examples=[
            _example("Inspect blast radius for one entity", {"entity_id": "module:task_assembler", "depth": 1}),
            _example(
                "Inspect blast radius including enrichment edges",
                {"entity_id": "module:task_assembler", "depth": 1, "include_enrichment": True},
            ),
        ],
    ),
    "praxis_story": _tool(
        surface="knowledge",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Compose a short narrative from one entity's graph neighborhood when plain edges are too flat.",
        when_not_to_use="Do not use it for ranked search or blast-radius inspection; use recall or graph first.",
        risks={"default": "read"},
        examples=[
            _example(
                "Compose a story for one entity",
                {"entity_id": "module:task_assembler", "max_lines": 4},
            ),
        ],
    ),
    "praxis_heal": _tool(
        surface="governance",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Diagnose failures and propose healing actions with platform-specific guidance.",
        when_not_to_use="Do not use it as a generic health command or workflow launcher.",
        risks={"default": "read"},
        examples=[
            _example("Classify a failure", {"job_label": "build", "failure_code": "sandbox.timeout", "stderr": "command timed out"}),
            _example("Infer a missing failure code from stderr", {"job_label": "build", "stderr": "failure_code must be a non-empty string"}),
        ],
    ),
    "praxis_health": _tool(
        surface="operations",
        tier="stable",
        recommended_alias="health",
        when_to_use="Run a full preflight before workflow launch or when the platform feels degraded.",
        when_not_to_use="Do not use it to inspect one specific workflow run.",
        risks={"default": "read"},
        examples=[
            _example("Run the full health check", {}),
        ],
    ),
    "praxis_daily_heartbeat": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="heartbeat",
        when_to_use="Run the daily external-health probe across providers, connectors, credentials, and MCP servers.",
        when_not_to_use="Do not use it for knowledge-graph maintenance; use praxis_heartbeat for that cycle.",
        risks={"default": "write"},
        examples=[
            _example("Run the full daily heartbeat", {"scope": "all"}),
            _example("Probe credentials only", {"scope": "credentials"}),
        ],
    ),
    "praxis_heartbeat": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Run or inspect the knowledge-graph maintenance cycle that syncs receipts, bugs, constraints, and memory projections.",
        when_not_to_use="Do not use it for external provider or connector probes; use praxis_daily_heartbeat for that.",
        risks={"default": "read", "actions": {"status": "read", "run": "write"}},
        examples=[
            _example("Show last heartbeat status", {"action": "status"}),
            _example("Run one maintenance cycle", {"action": "run"}),
        ],
    ),
    "praxis_ingest": _tool(
        surface="knowledge",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Persist new documents, build events, or research into the knowledge graph.",
        when_not_to_use="Do not use it for ad hoc questions where nothing should be persisted.",
        risks={"default": "write"},
        examples=[
            _example("Ingest a document", {"kind": "document", "source": "catalog/runtime", "content": "# Runtime catalog"}),
            _example("Ingest a transcript", {"kind": "meeting_transcript", "source": "meeting/2026-04-07", "content": "Alice: TODO review PR"}),
        ],
    ),
    "praxis_integration": _tool(
        surface="integration",
        tier="advanced",
        recommended_alias="integration",
        when_to_use="List integrations, inspect one, validate credentials, or invoke an integration action.",
        when_not_to_use="Do not use it to build connectors or launch workflows.",
        risks={
            "default": "read",
            "actions": {
                "list": "read",
                "describe": "read",
                "test_credentials": "read",
                "health": "read",
                "call": "launch",
                "create": "write",
                "set_secret": "write",
                "reload": "write",
            },
        },
        examples=[
            _example("List integrations", {"action": "list"}),
            _example("Call an integration action", {"action": "call", "integration_id": "stripe", "integration_action": "list_payments", "args": {"limit": 10}}),
        ],
    ),
    "praxis_intent_match": _tool(
        surface="planning",
        tier="stable",
        recommended_alias=None,
        when_to_use="Match a product intent against existing platform components before generating a manifest.",
        when_not_to_use="Do not use it for code search or historical run analysis.",
        risks={"default": "read"},
        examples=[
            _example("Match an app intent", {"intent": "invoice approval workflow with status tracking"}),
        ],
    ),
    "praxis_generate_plan": _tool(
        surface="workflow",
        tier="stable",
        recommended_alias="generate-plan",
        when_to_use=(
            "Shared CQRS plan-generation front door for MCP/CLI/API parity. Use "
            "action='generate_plan' to recognize messy prose without mutation, or "
            "action='materialize_plan' to create or update "
            "draft workflow build state."
        ),
        when_not_to_use=(
            "Do not use it to launch a workflow run. Materialized workflow state still "
            "needs the normal approval and launch path."
        ),
        risks={"default": "read", "actions": {"generate_plan": "read", "materialize_plan": "write"}},
        examples=[
            _example(
                "Generate plan scope",
                {"action": "generate_plan", "intent": "Feed in an app name, search, retrieve, evaluate, then build a custom integration."},
            ),
            _example(
                "Materialize a draft workflow",
                {"action": "materialize_plan", "intent": "Feed in an app name, search, retrieve, evaluate, then build a custom integration.", "title": "Integration builder"},
            ),
        ],
    ),
    "praxis_synthesize_skeleton": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "Synthesize a workflow skeleton from recognized intent atoms before "
            "materializing or launching the workflow."
        ),
        when_not_to_use=(
            "Do not use it as the launch authority; use praxis_generate_plan for draft "
            "state and praxis_workflow for execution."
        ),
        risks={"default": "read"},
        examples=[
            _example("Synthesize a skeleton", {"intent": "Build a connector workflow from app docs and smoke-test it"}),
        ],
    ),
    "praxis_compose_plan_via_llm": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "Compose a bounded plan statement from synthesized workflow atoms "
            "when deterministic skeletons need one LLM planning pass."
        ),
        when_not_to_use=(
            "Do not use it for execution or provider routing; it is a plan-composition helper."
        ),
        risks={"default": "launch"},
        examples=[
            _example("Compose a plan", {"intent": "Build a connector workflow", "plan_name": "connector-build", "concurrency": 4}),
        ],
    ),
    "praxis_compose_experiment": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "Run several praxis_compose_plan_via_llm configurations in parallel on the same intent "
            "and compare outcomes before pinning knobs in task_type_routing."
        ),
        when_not_to_use=(
            "Do not use it for a single compose pass — call praxis_compose_plan_via_llm directly. "
            "Do not use it when you cannot afford multiple LLM-backed compose receipts."
        ),
        risks={"default": "launch"},
        examples=[
            _example(
                "Matrix two temperature overrides on one intent",
                {
                    "intent": "Design a two-step migration to add nullable columns safely.",
                    "configs": [
                        {"model_slug": "openai/gpt-4.1-mini", "temperature": 0.2},
                        {"model_slug": "openai/gpt-4.1-mini", "temperature": 0.7},
                    ],
                    "plan_name": "migration-compose-ab",
                    "concurrency": 2,
                    "max_workers": 4,
                },
            ),
        ],
    ),
    "praxis_promote_experiment_winner": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "Promote the winning compose_experiment leg back into the canonical task_type_routing row "
            "after you have inspected the experiment receipt and picked a winner."
        ),
        when_not_to_use=(
            "Do not use it without a source compose_experiment receipt and config index. "
            "Do not use it to auto-apply provider/model identity changes; those stay visible only in the diff."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Promote a winning experiment leg",
                {
                    "source_experiment_receipt_id": "receipt:compose-experiment:1234",
                    "source_config_index": 0,
                },
            ),
        ],
    ),
    "praxis_manifest_generate": _tool(
        surface="planning",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Generate a new manifest from an intent after you've confirmed the building blocks.",
        when_not_to_use="Do not use it for code execution or connector onboarding.",
        risks={"default": "write"},
        examples=[
            _example("Generate a manifest", {"intent": "customer onboarding pipeline with approval steps"}),
        ],
    ),
    "praxis_manifest_refine": _tool(
        surface="planning",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Iterate on an existing generated manifest based on feedback.",
        when_not_to_use="Do not use it without a manifest id from a prior generation step.",
        risks={"default": "write"},
        examples=[
            _example("Refine a manifest", {"manifest_id": "manifest_abc123", "feedback": "Add weekly trends and remove the status grid"}),
        ],
    ),
    "praxis_canvas": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias="canvas",
        when_to_use=(
            "Read, compose, suggest, mutate, or launch Workflow graphs through "
            "the same CQRS-backed build authority used by the in-app Workflow surface. "
            "The praxis_canvas tool name and canvas alias remain compatibility entrypoints."
        ),
        when_not_to_use=(
            "Do not use it for unrelated roadmap, bug, provider-routing, or direct "
            "database work. Read the graph before mutating fields."
        ),
        risks={
            "default": "write",
            "actions": {
                "get_build": "read",
                "compose": "write",
                "suggest_next": "read",
                "mutate_field": "write",
                "launch": "launch",
            },
        },
        examples=[
            _example("Read a Workflow graph", {"action": "get_build", "workflow_id": "wf_abc"}),
            _example(
                "Compose a Workflow graph",
                {"action": "compose", "intent": "Search GitHub issues and draft a summary"},
            ),
        ],
    ),
    "praxis_operator_closeout": _tool(
        surface="operator",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "Preview or commit operator closeout through the shared gate, including "
            "bug-backed work items and parent initiatives with completed direct children."
        ),
        when_not_to_use="Do not use it for roadmap item creation or read-only status views.",
        risks={"default": "read", "actions": {"preview": "read", "commit": "write"}},
        examples=[
            _example("Preview a closeout", {"action": "preview", "work_item_id": "WI-123"}),
        ],
    ),
    "praxis_operator_roadmap_view": _tool(
        surface="operator",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Read one roadmap subtree, derived clusters, dependency edges, and semantic-first external neighbors without mutating roadmap authority.",
        when_not_to_use="Do not use it to commit roadmap changes.",
        risks={"default": "read"},
        examples=[
            _example("Read the default roadmap root", {}),
            _example(
                "Read one roadmap subtree",
                {"root_roadmap_item_id": "roadmap_item.authority.cleanup.unified.operator.write.validation.gate"},
            ),
            _example(
                "Read one roadmap subtree with more semantic neighbors",
                {
                    "root_roadmap_item_id": "roadmap_item.authority.cleanup.unified.operator.write.validation.gate",
                    "semantic_neighbor_limit": 8,
                },
            ),
        ],
    ),
    "praxis_operator_ideas": _tool(
        surface="operator",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Capture pre-commitment ideas, reject/supersede/archive them, or promote them into committed roadmap items.",
        when_not_to_use="Do not use it as a substitute for committed roadmap work; use praxis_operator_write once scope is committed.",
        risks={"default": "read", "actions": {"list": "read", "file": "write", "resolve": "write", "promote": "write"}},
        examples=[
            _example("List open ideas", {"action": "list", "limit": 25}),
            _example("File an idea", {"action": "file", "title": "First-class ideas authority", "summary": "Pre-commitment intake for roadmap candidates."}),
            _example("Reject an idea", {"action": "resolve", "idea_id": "operator_idea.example", "status": "rejected", "resolution_summary": "No longer fits the operator model."}),
        ],
    ),
    "praxis_operator_write": _tool(
        surface="operator",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Preview, validate, or commit roadmap writes through the operator gate.",
        when_not_to_use="Do not use it for read-only backlog inspection.",
        risks={"default": "read", "actions": {"preview": "read", "validate": "read", "commit": "write"}},
        examples=[
            _example("Preview a roadmap item", {"action": "preview", "title": "Consolidate CLI frontdoors", "intent_brief": "one authority for operator CLI"}),
        ],
    ),
    "praxis_operator_decisions": _tool(
        surface="operator",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "List or record durable operator decisions such as architecture "
            "policy rows in the canonical operator_decisions table. New "
            "records should pass scope_clamp={'applies_to': [...], "
            "'does_not_apply_to': [...]} so downstream surfaces can quote the "
            "clamp verbatim instead of paraphrasing rationale; rows omit it "
            "default to a 'pending_review' placeholder for the operator to "
            "fill in via the Workflow Decisions panel."
        ),
        when_not_to_use="Do not use it for roadmap item authoring or cutover-gate admission.",
        risks={"default": "read", "actions": {"list": "read", "record": "write"}},
        examples=[
            _example(
                "List current architecture policy decisions",
                {"action": "list", "decision_kind": "architecture_policy"},
            ),
            _example(
                "Record one architecture policy decision with scope_clamp",
                {
                    "action": "record",
                    "decision_key": "architecture-policy::decision-tables::db-native-authority",
                    "decision_kind": "architecture_policy",
                    "title": "Decision tables are DB-native authority",
                    "rationale": "Keep control authority in Postgres.",
                    "decided_by": "praxis-admin",
                    "decision_source": "cto.guidance",
                    "decision_scope_kind": "authority_domain",
                    "decision_scope_ref": "decision_tables",
                    "scope_clamp": {
                        "applies_to": [
                            "All architecture-policy decisions about decision authority storage",
                        ],
                        "does_not_apply_to": [
                            "Per-run scratch state",
                            "Ephemeral cache rows",
                        ],
                    },
                },
            ),
        ],
    ),
    "praxis_operator_relations": _tool(
        surface="operator",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Record canonical functional areas and cross-object semantic relations when operator entities need one explicit semantic edge instead of hidden tags or prose.",
        when_not_to_use="Do not use it for read-only operator inspection or generic roadmap authoring.",
        risks={
            "default": "write",
            "actions": {
                "record_functional_area": "write",
                "record_relation": "write",
            },
        },
        examples=[
            _example(
                "Record a functional area",
                {
                    "action": "record_functional_area",
                    "area_slug": "checkout",
                    "title": "Checkout",
                    "summary": "Shared checkout semantics",
                },
            ),
            _example(
                "Record a semantic relation",
                {
                    "action": "record_relation",
                    "relation_kind": "grouped_in",
                    "source_kind": "roadmap_item",
                    "source_ref": "roadmap_item.checkout",
                    "target_kind": "functional_area",
                    "target_ref": "checkout",
                },
            ),
        ],
    ),
    "praxis_semantic_assertions": _tool(
        surface="operator",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Register semantic predicates, record or retract semantic assertions, or query the current semantic substrate when semantics need durable typed authority.",
        when_not_to_use="Do not use it for generic roadmap authoring, issue triage, or workflow telemetry reads.",
        risks={
            "default": "read",
            "actions": {
                "list": "read",
                "register_predicate": "write",
                "record_assertion": "write",
                "retract_assertion": "write",
            },
        },
        examples=[
            _example(
                "List current semantic assertions for one predicate",
                {"action": "list", "predicate_slug": "grouped_in"},
            ),
            _example(
                "Register one semantic predicate",
                {
                    "action": "register_predicate",
                    "predicate_slug": "grouped_in",
                    "subject_kind_allowlist": ["bug"],
                    "object_kind_allowlist": ["functional_area"],
                    "cardinality_mode": "single_active_per_subject",
                },
            ),
            _example(
                "Record one semantic assertion",
                {
                    "action": "record_assertion",
                    "predicate_slug": "grouped_in",
                    "subject_kind": "bug",
                    "subject_ref": "bug.checkout.1",
                    "object_kind": "functional_area",
                    "object_ref": "functional_area.checkout",
                    "source_kind": "operator",
                    "source_ref": "nate",
                },
            ),
        ],
    ),
    "praxis_operator_architecture_policy": _tool(
        surface="operator",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Record one typed architecture policy decision in operator_decisions when explicit guidance should become durable control authority.",
        when_not_to_use="Do not use it for generic decision history reads; use praxis_operator_decisions for that.",
        risks={"default": "write"},
        examples=[
            _example(
                "Record an explicit architecture policy with deeper why",
                {
                    "authority_domain": "decision_tables",
                    "policy_slug": "db-native-authority",
                    "title": "Decision tables are DB-native authority",
                    "rationale": "Keep authority in Postgres.",
                    "decided_by": "nate",
                    "decision_source": "cto.guidance",
                    "decision_provenance": "explicit",
                    "decision_why": "Authority outside the DB cannot be replayed or audited under the gateway-receipt model; surfaces drift from runtime.",
                },
            ),
            _example(
                "Record a model-inferred policy from conversation parsing",
                {
                    "authority_domain": "providers",
                    "policy_slug": "no-some-model-x",
                    "title": "Avoid model X for build tasks",
                    "rationale": "Build tasks regress on model X per recent receipts.",
                    "decided_by": "praxis-agent",
                    "decision_source": "conversation",
                    "decision_provenance": "inferred",
                },
            ),
        ],
    ),
    "praxis_operator_native_primary_cutover_gate": _tool(
        surface="operator",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Admit a native-primary cutover gate with required decision metadata into operator-control.",
        when_not_to_use="Do not use it for read-only operator status views.",
        risks={"default": "write"},
        examples=[
            _example(
                "Admit roadmap-based cutover gate",
                {
                    "decided_by": "operator-auto",
                    "decision_source": "runbook",
                    "rationale": "manual rollout hold ended",
                    "roadmap_item_id": "roadmap_item.platform.deploy",
                },
            ),
        ],
    ),
    "praxis_provider_onboard": _tool(
        surface="integration",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Probe or onboard a new provider/model route into the platform.",
        when_not_to_use="Do not use it for ordinary model selection or workflow launch.",
        risks={"default": "read", "actions": {"probe": "read", "onboard": "write"}},
        examples=[
            _example("Probe a provider", {"action": "probe", "provider_slug": "openrouter", "transport": "api"}),
        ],
    ),
    "praxis_cli_auth_doctor": _tool(
        surface="integration",
        tier="stable",
        recommended_alias=None,
        when_to_use="Diagnose CLI auth state for claude / codex / gemini in one call when a workflow run reported `Not logged in` / 401 / authentication errors, OR proactively before launching CLI-lane work.",
        when_not_to_use="Do not use for general workflow status (use praxis_workflow action='status') or for provider catalog truth (use praxis_provider_control_plane).",
        risks={"default": "read"},
        examples=[
            _example("Check all three CLIs", {}),
            _example("Check just claude", {"providers": ["anthropic"]}),
        ],
    ),
    "praxis_query": _tool(
        surface="query",
        tier="stable",
        recommended_alias="query",
        when_to_use="Route a natural-language question to the right platform subsystem from the terminal when you are not sure which exact tool to use.",
        when_not_to_use="Do not use it when you already know the exact specialist tool you need.",
        risks={"default": "read"},
        examples=[
            _example("Ask for status", {"question": "what is failing right now?"}),
            _example("Ask for schema", {"question": "schema for workflow_runs"}),
            _example("Ask for code discovery via router", {"question": "find retry logic with exponential backoff"}),
        ],
    ),
    "praxis_recall": _tool(
        surface="knowledge",
        tier="stable",
        recommended_alias="recall",
        when_to_use="Search the knowledge graph for decisions, patterns, entities, and prior analysis using ranked text, graph, and vector retrieval.",
        when_not_to_use="Do not use it for code similarity or workflow receipt queries.",
        risks={"default": "read"},
        examples=[
            _example("Recall an architectural decision", {"query": "provider routing", "entity_type": "decision"}),
            _example("Recall a schema entity", {"query": "workflow_runs", "entity_type": "table"}),
            _example("Recall a pattern", {"query": "retry policy", "entity_type": "pattern"}),
        ],
    ),
    "praxis_receipts": _tool(
        surface="evidence",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Search workflow receipts or inspect token burn and execution evidence.",
        when_not_to_use="Do not use it for current health or knowledge-graph recall.",
        risks={"default": "read", "actions": {"search": "read", "token_burn": "read"}},
        examples=[
            _example("Search receipts", {"action": "search", "query": "sandbox timeout"}),
            _example("Inspect token burn", {"action": "token_burn", "since_hours": 24}),
        ],
    ),
    "praxis_reload": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Clear in-process caches after changing runtime config or MCP catalog state.",
        when_not_to_use="Do not use it as a routine health command.",
        risks={"default": "write"},
        examples=[
            _example("Reload process caches", {}),
        ],
    ),
    "praxis_evolve_operation_field": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="evolve-operation-field",
        when_to_use=(
            "Plan how to add one optional field to an existing CQRS operation's input model "
            "(checklist of files and edits). v1 is plan-only — you still apply diffs locally."
        ),
        when_not_to_use=(
            "Do not use it to register a brand-new operation — use praxis_register_operation. "
            "Do not expect the tool to write migrations or apply patches automatically."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Plan a new optional field on an existing op",
                {
                    "operation_name": "operator.architecture_policy_record",
                    "field_name": "decision_provenance",
                    "field_type_annotation": "str | None",
                    "field_default_repr": "None",
                    "field_description": "explicit | inferred provenance",
                    "db_table": "operator_decisions",
                    "db_column": "decision_provenance",
                },
            ),
        ],
    ),
    "praxis_authority_domain_forge": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="authority-domain-forge",
        when_to_use=(
            "Preview authority-domain ownership before creating a new authority boundary "
            "or attaching operations, tables, workflows, or MCP tools to it. Use this "
            "before register-operation when the owning authority is not already explicit."
        ),
        when_not_to_use=(
            "Do not use it as a mutation surface; it only prepares the canonical "
            "authority-domain payload. Use praxis_register_authority_domain to write."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Preview object-truth domain",
                {
                    "authority_domain_ref": "authority.object_truth",
                    "decision_ref": "operator_decision.architecture_policy.product_architecture.object_truth_requires_deterministic_parse_compare_substrate",
                },
            ),
        ],
    ),
    "praxis_register_authority_domain": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="register-authority-domain",
        when_to_use=(
            "Register or update an authority domain after the forge confirms the domain "
            "is the right owner of durable truth. This creates the domain before "
            "operations, tables, workflows, or MCP tools attach to it."
        ),
        when_not_to_use=(
            "Do not use it to attach operations; use praxis_register_operation after "
            "the authority domain exists. Do not use it without a decision_ref."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Register object-truth domain",
                {
                    "authority_domain_ref": "authority.object_truth",
                    "decision_ref": "operator_decision.architecture_policy.product_architecture.object_truth_requires_deterministic_parse_compare_substrate",
                },
            ),
        ],
    ),
    "praxis_object_truth": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="object-truth",
        when_to_use=(
            "Build deterministic object-truth evidence for one inline external record: "
            "identity digest, field observations, value digests, source metadata, "
            "hierarchy signals, and redaction-safe previews."
        ),
        when_not_to_use=(
            "Do not use it for multi-system sampling, durable persistence, or business "
            "truth decisions yet. This is the read-only observe-record slice."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Observe one account record",
                {
                    "system_ref": "salesforce",
                    "object_ref": "account",
                    "record": {
                        "id": "001",
                        "name": "Acme",
                        "billing": {"city": "Denver"},
                    },
                    "identity_fields": ["id"],
                    "source_metadata": {"updated_at": "2026-04-28T10:00:00Z"},
                },
            ),
        ],
    ),
    "praxis_object_truth_store": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="object-truth-store",
        when_to_use=(
            "Persist deterministic object-truth evidence for one inline external "
            "record after the authority domain and evidence tables exist."
        ),
        when_not_to_use=(
            "Do not use for exploratory inspection when no write is intended; use "
            "praxis_object_truth instead. Do not use it to decide business truth."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Store one account record",
                {
                    "system_ref": "salesforce",
                    "object_ref": "account",
                    "record": {
                        "id": "001",
                        "name": "Acme",
                        "billing": {"city": "Denver"},
                    },
                    "identity_fields": ["id"],
                    "source_metadata": {"updated_at": "2026-04-28T10:00:00Z"},
                    "observed_by_ref": "operator:nate",
                    "source_ref": "sample:accounts:001",
                },
            ),
        ],
    ),
    "praxis_object_truth_store_schema_snapshot": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="object-truth-store-schema",
        when_to_use=(
            "Persist normalized schema evidence for one external object before "
            "record sampling or comparison work references a schema digest."
        ),
        when_not_to_use=(
            "Do not use for record payloads; use praxis_object_truth_store for "
            "object-version evidence."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Store account schema",
                {
                    "system_ref": "salesforce",
                    "object_ref": "account",
                    "raw_schema": {
                        "fields": [
                            {"name": "id", "type": "string", "required": True},
                            {"name": "name", "type": "string"},
                        ]
                    },
                    "observed_by_ref": "operator:nate",
                    "source_ref": "schema:salesforce:account",
                },
            ),
        ],
    ),
    "praxis_object_truth_compare_versions": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="object-truth-compare",
        when_to_use=(
            "Compare two persisted object-truth object versions by digest to see "
            "matching, different, missing, and freshness signals."
        ),
        when_not_to_use=(
            "Do not use to decide final business truth by itself; it produces "
            "deterministic evidence for a later decision layer."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Compare two stored versions",
                {
                    "left_object_version_digest": "left-digest",
                    "right_object_version_digest": "right-digest",
                },
            ),
        ],
    ),
    "praxis_object_truth_readiness": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="object-truth-readiness",
        when_to_use=(
            "Inspect whether Object Truth authority is ready for downstream "
            "client-system discovery, ingestion, and Virtual Lab planning. "
            "Returns explicit no-go conditions instead of treating a blocked "
            "state as a tool failure."
        ),
        when_not_to_use=(
            "Do not use it to persist client evidence or compare object "
            "versions; it is the pre-build authority gate only."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Check Object Truth readiness",
                {
                    "client_payload_mode": "redacted_hashes",
                    "planned_fanout": 1,
                    "include_counts": True,
                },
            ),
        ],
    ),
    "praxis_object_truth_latest_version_read": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="object-truth-latest-version",
        when_to_use=(
            "Read the latest trusted Object Truth version for a system/object/"
            "identity/client filter when the caller should not know or manage "
            "exact version digests."
        ),
        when_not_to_use=(
            "Do not use it to ingest new evidence or change Object Truth. "
            "Do not treat stale or conflict no-go states as deployable proof."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Read latest trusted account version",
                {
                    "system_ref": "salesforce",
                    "object_ref": "account",
                    "identity_digest": "identity.digest.account.001",
                    "max_age_seconds": 86400,
                },
            ),
        ],
    ),
    "praxis_object_truth_record_comparison_run": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="object-truth-record-comparison",
        when_to_use=(
            "Persist a comparison result between two stored object versions so "
            "future runs can query the evidence instead of recomputing it."
        ),
        when_not_to_use=(
            "Do not use for ad hoc read-only inspection; use "
            "praxis_object_truth_compare_versions when no write is intended."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Record a comparison run",
                {
                    "left_object_version_digest": "left-digest",
                    "right_object_version_digest": "right-digest",
                    "observed_by_ref": "operator:nate",
                    "source_ref": "comparison:accounts:demo",
                },
            ),
        ],
    ),
    "praxis_object_truth_ingestion_sample_record": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="object-truth-ingestion-sample-record",
        when_to_use=(
            "Persist a receipt-backed Object Truth ingestion sample: system "
            "snapshot, source query, sample capture, raw payload references, "
            "redacted previews, object versions, field observations, and replay "
            "fixture evidence."
        ),
        when_not_to_use=(
            "Do not use it for direct connector execution or final business-truth "
            "decisions. It records observed evidence for later source-authority "
            "and Virtual Lab work."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Record one fixture sample",
                {
                    "client_ref": "client.acme",
                    "system_ref": "salesforce",
                    "integration_id": "integration.salesforce.prod",
                    "connector_ref": "connector.salesforce",
                    "environment_ref": "sandbox",
                    "object_ref": "account",
                    "schema_snapshot_digest": "schema.digest.account",
                    "captured_at": "2026-04-30T16:00:00Z",
                    "capture_receipt_id": "receipt.capture.demo",
                    "identity_fields": ["id"],
                    "sample_strategy": "fixture",
                    "sample_payloads": [{"id": "001", "name": "Acme"}],
                    "privacy_classification": "confidential",
                    "retention_policy_ref": "retention.object_truth.redacted_hashes",
                },
            ),
        ],
    ),
    "praxis_object_truth_ingestion_sample_read": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="object-truth-ingestion-sample-read",
        when_to_use=(
            "Read stored Object Truth ingestion samples, payload references, "
            "object-version refs, and replay fixture evidence."
        ),
        when_not_to_use=(
            "Do not use it to mutate ingestion evidence; use "
            "praxis_object_truth_ingestion_sample_record for writes."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Describe one ingestion sample",
                {
                    "action": "describe",
                    "sample_id": "object_truth_sample.demo",
                    "include_payload_references": True,
                },
            ),
        ],
    ),
    "praxis_object_truth_mdm_resolution_record": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="object-truth-mdm-resolution-record",
        when_to_use=(
            "Persist a receipt-backed Object Truth MDM/source-authority "
            "resolution packet with identity clusters, field comparisons, "
            "normalization rules, authority evidence, hierarchy signals, and "
            "typed gaps."
        ),
        when_not_to_use=(
            "Do not use it to decide source authority implicitly. The input "
            "must already contain explicit MDM evidence built by the domain "
            "layer or an equivalent deterministic authority path."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Record one MDM resolution packet",
                {
                    "client_ref": "client.acme",
                    "entity_type": "organization",
                    "as_of": "2026-04-30T16:00:00Z",
                    "identity_clusters": [{"cluster_id": "object_truth_cluster.organization.demo"}],
                    "field_comparisons": [{"field_comparison_digest": "comparison.digest"}],
                },
            ),
        ],
    ),
    "praxis_object_truth_mdm_resolution_read": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="object-truth-mdm-resolution-read",
        when_to_use=(
            "Read stored Object Truth MDM/source-authority resolution packets "
            "and their decomposed identity, field, authority, hierarchy, and "
            "gap evidence."
        ),
        when_not_to_use=(
            "Do not use it to mutate MDM evidence; use "
            "praxis_object_truth_mdm_resolution_record for writes."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Describe one MDM resolution packet",
                {
                    "action": "describe",
                    "packet_ref": "object_truth_mdm_packet.demo",
                    "include_records": True,
                },
            ),
        ],
    ),
    "praxis_workflow_context_compile": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="workflow-context-compile",
        when_to_use=(
            "Compile a Workflow Context pack from intent and optional graph so "
            "the LLM can infer systems, objects, fields, risks, blockers, and "
            "optional deterministic synthetic worlds before real integrations exist."
        ),
        when_not_to_use=(
            "Do not use it to call live client systems or promote synthetic "
            "evidence. Promotion is a guarded transition, not a compile side effect."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Compile a synthetic renewal-risk context",
                {
                    "intent": "Detect renewal risk from CRM, billing, support, and Slack signals.",
                    "context_mode": "synthetic",
                    "scenario_pack_refs": ["renewal_risk"],
                    "seed": "demo-renewal-risk",
                },
            ),
        ],
    ),
    "praxis_workflow_context_read": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="workflow-context-read",
        when_to_use=(
            "Read persisted Workflow Context packs, entities, bindings, "
            "transition history, blockers, guardrails, synthetic worlds, and "
            "review packets."
        ),
        when_not_to_use=(
            "Do not use it to mutate context state. Use compile, transition, "
            "or bind operations for writes."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Read one context pack",
                {
                    "context_ref": "workflow_context:renewal_risk:demo",
                    "include_entities": True,
                    "include_bindings": True,
                },
            ),
        ],
    ),
    "praxis_workflow_context_transition": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="workflow-context-transition",
        when_to_use=(
            "Move a Workflow Context pack between truth states through backend "
            "policy, for example inferred to schema_bound or verified to promoted."
        ),
        when_not_to_use=(
            "Do not use it to bypass review at real trust boundaries. Synthetic "
            "or inferred context cannot be promoted by supplying nicer labels."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Mark context verified after verifier evidence",
                {
                    "context_ref": "workflow_context:renewal_risk:demo",
                    "to_truth_state": "verified",
                    "transition_reason": "verifier passed against observed Object Truth",
                    "evidence": [{"evidence_ref": "verification.run.123", "evidence_tier": "verified"}],
                },
            ),
        ],
    ),
    "praxis_workflow_context_bind": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="workflow-context-bind",
        when_to_use=(
            "Bind an inferred or synthetic Workflow Context entity to Object "
            "Truth or another explicit authority ref while preserving risk, "
            "review, confidence, and reversibility."
        ),
        when_not_to_use=(
            "Do not use it to decide source authority implicitly or to accept "
            "high-risk bindings without review evidence."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Propose binding synthetic Account to Object Truth",
                {
                    "context_ref": "workflow_context:renewal_risk:demo",
                    "entity_ref": "workflow_context:renewal_risk:demo:entity:object:account",
                    "target_ref": "object_truth_object_version:account.digest",
                    "risk_level": "medium",
                },
            ),
        ],
    ),
    "praxis_workflow_context_guardrail_check": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="workflow-context-guardrails",
        when_to_use=(
            "Ask backend policy what the LLM can safely do next with a "
            "Workflow Context pack, including no-go states and review requirements."
        ),
        when_not_to_use=(
            "Do not use it as a substitute for the transition command when the "
            "state actually needs to change."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Check whether promotion is allowed",
                {
                    "context_ref": "workflow_context:renewal_risk:demo",
                    "target_truth_state": "promoted",
                },
            ),
        ],
    ),
    "praxis_task_environment_contract_record": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="task-environment-contract-record",
        when_to_use=(
            "Persist a receipt-backed task-environment contract head and "
            "revision with its deterministic evaluation result, hierarchy "
            "nodes, typed invalid states, dependency hash, and command event."
        ),
        when_not_to_use=(
            "Do not use it to invent policy during execution. The contract "
            "and evaluation result should come from the task-contract domain "
            "model or another deterministic authority path."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Record one task-environment contract",
                {
                    "contract": {
                        "contract_id": "task_contract.account_sync.1",
                        "task_ref": "task.account_sync",
                        "hierarchy_node_id": "task.account_sync",
                        "revision_id": "rev.contract.1",
                        "status": "active",
                    },
                    "evaluation_result": {
                        "ok": True,
                        "status": "valid",
                        "invalid_states": [],
                        "warnings": [],
                    },
                },
            ),
        ],
    ),
    "praxis_task_environment_contract_read": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="task-environment-contract-read",
        when_to_use=(
            "Read stored task-environment contract heads, revisions, hierarchy "
            "nodes, and typed invalid states before launch or promotion."
        ),
        when_not_to_use=(
            "Do not use it to mutate contract authority; use "
            "praxis_task_environment_contract_record for writes."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Describe one task-environment contract",
                {
                    "action": "describe",
                    "contract_id": "task_contract.account_sync.1",
                    "include_history": True,
                },
            ),
        ],
    ),
    "praxis_integration_action_contract_record": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="integration-action-contract-record",
        when_to_use=(
            "Persist receipt-backed integration action contracts and "
            "automation rule snapshots with deterministic hashes, validation "
            "gaps, linked actions, and a command event before simulation or "
            "sandbox promotion."
        ),
        when_not_to_use=(
            "Do not use it to execute an integration or invent connector "
            "behavior. The contract should describe observed, owner-reviewed, "
            "or explicitly gapped behavior."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Record one integration action contract",
                {
                    "contracts": [
                        {
                            "action_id": "integration_action.hubspot.create_contact",
                            "name": "HubSpot / create contact",
                            "status": "draft",
                        }
                    ],
                    "automation_snapshots": [
                        {
                            "rule_id": "automation.hubspot.contact_sync",
                            "name": "HubSpot contact sync",
                            "status": "active",
                            "linked_action_ids": ["integration_action.hubspot.create_contact"],
                        }
                    ],
                },
            ),
        ],
    ),
    "praxis_integration_action_contract_read": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="integration-action-contract-read",
        when_to_use=(
            "Read stored integration action contracts, revisions, automation "
            "snapshots, linked actions, and typed gaps before Virtual Lab "
            "simulation or live sandbox promotion."
        ),
        when_not_to_use=(
            "Do not use it to mutate contract authority; use "
            "praxis_integration_action_contract_record for writes."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Describe one integration action contract",
                {
                    "action": "describe_contract",
                    "action_contract_id": "integration_action.hubspot.create_contact",
                    "include_history": True,
                    "include_automation": True,
                },
            ),
            _example(
                "List automation snapshots",
                {
                    "action": "list_automation_snapshots",
                    "status": "active",
                },
            ),
        ],
    ),
    "praxis_virtual_lab_state_record": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="virtual-lab-state-record",
        when_to_use=(
            "Persist a receipt-backed Virtual Lab state packet after "
            "deterministic replay validation: environment revision, seeded "
            "object projections, event envelopes, command receipts, and typed "
            "gaps."
        ),
        when_not_to_use=(
            "Do not use it to execute integrations or mutate Object Truth. "
            "Object Truth seeds base state; Virtual Lab records predicted "
            "copy-on-write consequences only."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Record one Virtual Lab revision",
                {
                    "environment_revision": {
                        "environment_id": "virtual_lab.env.account_sync",
                        "revision_id": "virtual_lab_revision.demo",
                        "status": "active",
                    },
                    "object_states": [
                        {
                            "object_id": "account:001",
                            "instance_id": "primary",
                        }
                    ],
                    "events": [],
                    "command_receipts": [],
                    "typed_gaps": [],
                },
            ),
        ],
    ),
    "praxis_verifier_catalog": _tool(
        surface="evidence",
        tier="stable",
        recommended_alias="verifier-catalog",
        when_to_use=(
            "List registered verifier authority refs before picking one for "
            "a bug-resolve, code-change preflight, or workflow-packet review "
            "gate. Returns each verifier's verifier_ref, kind (platform / "
            "receipt / run / path), enabled state, and any bound "
            "suggested-healer refs."
        ),
        when_not_to_use=(
            "Do not use it to actually run a verifier — that path is still "
            "internal to verifier_authority (and reachable via "
            "praxis_bugs action=resolve). This is a read-only catalog query."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "List enabled verifiers",
                {"enabled": True, "limit": 50},
            ),
            _example(
                "Include disabled rows",
                {"enabled": False, "limit": 100},
            ),
        ],
    ),
    "praxis_healer_catalog": _tool(
        surface="evidence",
        tier="stable",
        recommended_alias="healer-catalog",
        when_to_use=(
            "List registered healer authority refs before picking one for "
            "praxis_healer_run, or to inspect what repairs are available "
            "after a verifier fails. Returns each healer's auto_mode, "
            "safety_mode, action_ref, and the verifier_refs it's bound to."
        ),
        when_not_to_use=(
            "Do not use it to actually run a healer — use praxis_healer_run "
            "for that. This is a read-only catalog query."
        ),
        risks={"default": "read"},
        examples=[
            _example("List enabled healers", {"enabled": True, "limit": 50}),
            _example("Include disabled rows", {"enabled": False}),
        ],
    ),
    "praxis_healer_runs_list": _tool(
        surface="evidence",
        tier="stable",
        recommended_alias="healer-runs",
        when_to_use=(
            "List past healing_runs newest-first to inspect repair history. "
            "Filter by healer_ref / verifier_ref (which verifier triggered "
            "the heal) / target / status / trailing-window. Use to confirm "
            "a heal succeeded, audit failure rates, or check whether a "
            "specific target has been auto-repaired recently."
        ),
        when_not_to_use=(
            "Do not use it to RUN a healer — use praxis_healer_run."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Recent runs of one healer",
                {"healer_ref": "healer.platform.schema_bootstrap", "limit": 20},
            ),
            _example(
                "Runs triggered by one verifier",
                {"verifier_ref": "verifier.platform.receipt_provenance"},
            ),
            _example(
                "Failed heals in the last day",
                {"status": "failed", "since_iso": "2026-04-30T00:00:00Z"},
            ),
        ],
    ),
    "praxis_healer_run": _tool(
        surface="evidence",
        tier="stable",
        recommended_alias="healer-run",
        when_to_use=(
            "Manually trigger a healer to repair a verifier failure. "
            "verifier_ref is required; healer_ref is optional (auto-resolves "
            "from verifier bindings when exactly one is bound). The runtime "
            "reruns the bound verifier as post-verification — succeeded "
            "status means BOTH healer action AND post-verification passed."
        ),
        when_not_to_use=(
            "Do not use it for fuzzy LLM-driven repair — healers are "
            "deterministic. The internal scheduler "
            "(run_due_platform_verifications) already runs canonical heals "
            "automatically; use this surface for manual repair gates."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Auto-resolve healer for one verifier",
                {"verifier_ref": "verifier.platform.schema_authority"},
            ),
            _example(
                "Explicit healer + verifier pair",
                {
                    "healer_ref": "healer.platform.schema_bootstrap",
                    "verifier_ref": "verifier.platform.schema_authority",
                },
            ),
            _example(
                "Dry-run (no healing_runs row)",
                {"verifier_ref": "verifier.platform.receipt_provenance", "record_run": False},
            ),
        ],
    ),
    "praxis_verifier_register": _tool(
        surface="evidence",
        tier="stable",
        recommended_alias="verifier-register",
        when_to_use=(
            "Register (or update) a verifier authority ref without authoring "
            "a SQL migration. Use when adding a new verifier — replaces the "
            "old hand-edited verifier_builtins.py + migration pattern. "
            "Optional bind_healer_refs creates verifier_healer_bindings in "
            "the same call."
        ),
        when_not_to_use=(
            "Do not use this to RUN a verifier — that's praxis_verifier_run. "
            "Do not use it to register healers — that's praxis_healer_register."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Register a builtin verifier",
                {
                    "verifier_ref": "verifier.platform.example_check",
                    "display_name": "Example platform check",
                    "verifier_kind": "builtin",
                    "builtin_ref": "verify_schema_authority",
                    "decision_ref": "decision.example.check.20260501",
                },
            ),
            _example(
                "Register a verification_ref-backed verifier with a healer binding",
                {
                    "verifier_ref": "verifier.platform.foo",
                    "display_name": "Foo verifier",
                    "verifier_kind": "verification_ref",
                    "verification_ref": "verification.foo.20260501",
                    "decision_ref": "decision.foo.20260501",
                    "bind_healer_refs": ["healer.platform.foo_repair"],
                },
            ),
        ],
    ),
    "praxis_healer_register": _tool(
        surface="evidence",
        tier="stable",
        recommended_alias="healer-register",
        when_to_use=(
            "Register (or update) a healer authority ref without authoring a "
            "SQL migration. Use when adding a new healer that will be bound "
            "to one or more verifiers. action_ref must name a built-in "
            "handler from runtime.verifier_builtins.run_builtin_healer."
        ),
        when_not_to_use=(
            "Do not use this to RUN a healer — that's praxis_healer_run. "
            "Do not use it to register verifiers — that's praxis_verifier_register."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Register a guarded manual healer",
                {
                    "healer_ref": "healer.platform.example_repair",
                    "display_name": "Example platform repair",
                    "action_ref": "heal_schema_bootstrap",
                    "auto_mode": "manual",
                    "safety_mode": "guarded",
                    "decision_ref": "decision.example.repair.20260501",
                },
            ),
            _example(
                "Register an automatic guarded healer",
                {
                    "healer_ref": "healer.platform.auto_repair",
                    "display_name": "Auto repair",
                    "action_ref": "heal_proof_backfill",
                    "auto_mode": "automatic",
                    "safety_mode": "guarded",
                    "decision_ref": "decision.auto_repair.20260501",
                },
            ),
        ],
    ),
    "praxis_verifier_run": _tool(
        surface="evidence",
        tier="stable",
        recommended_alias="verifier-run",
        when_to_use=(
            "Run a registered verifier against a target as a deterministic "
            "review gate — receipt-backed, replayable, links to a "
            "verification_runs row. Use this from a workflow packet "
            "(integration_id=praxis_verifier_run, integration_action=run) "
            "to express a verify step without going through bug-resolve, "
            "or interactively to confirm a verifier passes against a "
            "specific target."
        ),
        when_not_to_use=(
            "Do not use it for fuzzy LLM-driven review — verifiers are "
            "deterministic. For control-plane scheduler runs that should "
            "auto-file bugs on failure, set promote_bug=True; otherwise "
            "leave the default (False)."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Compile-check a Python file",
                {
                    "verifier_ref": "verifier.job.python.py_compile",
                    "target_kind": "path",
                    "target_ref": "/Users/nate/Praxis/Code&DBs/Workflow/runtime/example.py",
                    "inputs": {"path": "/Users/nate/Praxis/Code&DBs/Workflow/runtime/example.py"},
                },
            ),
            _example(
                "Run pytest on one test file",
                {
                    "verifier_ref": "verifier.job.python.pytest_file",
                    "target_kind": "path",
                    "target_ref": "/Users/nate/Praxis/Code&DBs/Workflow/tests/unit/test_smoke.py",
                    "inputs": {"path": "/Users/nate/Praxis/Code&DBs/Workflow/tests/unit/test_smoke.py"},
                },
            ),
            _example(
                "Platform schema authority check",
                {
                    "verifier_ref": "verifier.platform.schema_authority",
                    "target_kind": "platform",
                    "target_ref": "",
                },
            ),
        ],
    ),
    "praxis_verifier_runs_list": _tool(
        surface="evidence",
        tier="stable",
        recommended_alias="verifier-runs",
        when_to_use=(
            "List past verification_runs newest-first to confirm a "
            "verifier actually ran on a target. Filter by verifier_ref, "
            "target_kind, target_ref, status, or trailing window. Use "
            "before resolving a bug to FIXED to verify the evidence chain, "
            "or to inspect failure rates of a specific verifier."
        ),
        when_not_to_use=(
            "Do not use it to RUN a verifier — that path is still internal "
            "(via praxis_bugs action=resolve). This is read-only history."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Recent runs of one verifier",
                {"verifier_ref": "verifier.job.python.pytest_file", "limit": 20},
            ),
            _example(
                "Failed runs in the last day",
                {"status": "failed", "since_iso": "2026-04-30T00:00:00Z"},
            ),
            _example(
                "Runs against one path-kind target",
                {"target_kind": "path", "target_ref": "/Users/nate/Praxis/Code&DBs/Workflow/tests/unit/test_smoke.py"},
            ),
        ],
    ),
    "praxis_virtual_lab_state_read": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="virtual-lab-state-read",
        when_to_use=(
            "Read stored Virtual Lab revisions, object state projections, event "
            "streams, command receipts, and replay gaps before sandbox "
            "promotion or drift readback."
        ),
        when_not_to_use=(
            "Do not use it to mutate lab authority; use "
            "praxis_virtual_lab_state_record for writes."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Describe one Virtual Lab revision",
                {
                    "action": "describe_revision",
                    "environment_id": "virtual_lab.env.account_sync",
                    "revision_id": "virtual_lab_revision.demo",
                    "include_events": True,
                    "include_receipts": True,
                },
            ),
            _example(
                "Read one event stream",
                {
                    "action": "list_events",
                    "environment_id": "virtual_lab.env.account_sync",
                    "revision_id": "virtual_lab_revision.demo",
                    "stream_id": "virtual_lab.env.account_sync/virtual_lab_revision.demo/objects/account:001/primary",
                },
            ),
        ],
    ),
    "praxis_virtual_lab_simulation_run": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="virtual-lab-simulation-run",
        when_to_use=(
            "Run a deterministic Virtual Lab scenario and persist its trace, "
            "state transitions, automation firings, assertions, verifier "
            "results, typed gaps, and promotion blockers through CQRS."
        ),
        when_not_to_use=(
            "Do not use it to mutate live systems or Object Truth. It records "
            "predicted consequences against a saved Virtual Lab revision."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Run one simulation scenario",
                {
                    "scenario": {
                        "scenario_id": "scenario.account_sync",
                        "initial_state": {
                            "revision": {
                                "environment_id": "virtual_lab.env.account_sync",
                                "revision_id": "virtual_lab_revision.demo",
                            },
                            "object_states": [],
                        },
                        "actions": [],
                        "config": {
                            "seed": "seed.account_sync",
                            "clock_start": "2026-04-30T17:00:00Z",
                        },
                        "verifiers": [
                            {
                                "verifier_id": "verifier.no_blockers",
                                "verifier_kind": "no_blockers",
                            }
                        ],
                    },
                    "task_contract_ref": "task_environment_contract.account_sync",
                    "integration_action_contract_refs": [
                        "integration_action_contract.crm.patch_account"
                    ],
                },
            ),
        ],
    ),
    "praxis_virtual_lab_simulation_read": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="virtual-lab-simulation-read",
        when_to_use=(
            "Inspect persisted Virtual Lab simulation runs, ordered runtime "
            "events, verifier results, typed gaps, and promotion blockers "
            "before live sandbox promotion."
        ),
        when_not_to_use=(
            "Do not use it to run a new scenario; use "
            "praxis_virtual_lab_simulation_run for writes."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Describe one simulation run",
                {
                    "action": "describe_run",
                    "run_id": "virtual_lab_simulation_run.demo",
                    "include_events": True,
                    "include_verifiers": True,
                    "include_blockers": True,
                },
            ),
            _example(
                "List blocked runs for one environment",
                {
                    "action": "list_runs",
                    "environment_id": "virtual_lab.env.account_sync",
                    "status": "blocked",
                },
            ),
        ],
    ),
    "praxis_virtual_lab_sandbox_promotion_record": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="virtual-lab-sandbox-promotion-record",
        when_to_use=(
            "Record a live sandbox promotion window after simulation proof "
            "exists, then persist sandbox execution, readback evidence, "
            "predicted-vs-actual comparison, drift classification, handoff "
            "refs, and stop/continue summary through CQRS."
        ),
        when_not_to_use=(
            "Do not use it to run simulations, call live integrations, or file "
            "bugs directly. It records the evidence and handoff refs after "
            "those actions happen through their own authorities."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Record one sandbox promotion",
                {
                    "manifest": {
                        "manifest_id": "manifest.phase8",
                        "created_at": "2026-04-30T18:00:00Z",
                        "created_by": "agent.phase_08",
                        "candidates": [
                            {
                                "candidate_id": "candidate.phase8.account_sync",
                                "owner": "operator:nate",
                                "build_ref": "build.account_sync.20260430",
                                "sandbox_target": "sandbox.crm.dev",
                                "scope_ref": "scope.client_operating_model.phase_08",
                                "scenario_refs": ["scenario.qualify_account"],
                                "prediction_refs": ["prediction.qualify_account.status"],
                            }
                        ],
                    },
                    "candidate_records": [
                        {
                            "candidate_id": "candidate.phase8.account_sync",
                            "simulation_run_id": "virtual_lab_simulation_run.phase_07_proof",
                            "execution": {"execution_id": "execution.qualify_account.1"},
                            "evidence_package": {"package_id": "evidence_package.qualify_account"},
                            "checks": [{"check_id": "check.status"}],
                        }
                    ],
                },
            ),
        ],
    ),
    "praxis_virtual_lab_sandbox_promotion_read": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="virtual-lab-sandbox-promotion-read",
        when_to_use=(
            "Inspect persisted sandbox promotion records, readback evidence, "
            "drift reason codes, handoff refs, and stop/continue decisions "
            "before any client-live rollout."
        ),
        when_not_to_use=(
            "Do not use it to record new evidence; use "
            "praxis_virtual_lab_sandbox_promotion_record for writes."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Describe one sandbox promotion record",
                {
                    "action": "describe_record",
                    "promotion_record_id": "sandbox_promotion_record.demo",
                    "include_readback": True,
                    "include_drift": True,
                    "include_handoffs": True,
                },
            ),
            _example(
                "List drift needing rerun",
                {
                    "action": "list_drift",
                    "disposition": "rerun_required",
                    "reason_code": "OBSERVABILITY_GAP",
                },
            ),
        ],
    ),
    "praxis_authority_portable_cartridge_record": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="portable-cartridge-record",
        when_to_use=(
            "Record a portable cartridge deployment contract after the manifest "
            "has been assembled. The operation validates the contract, persists "
            "Object Truth dependencies, assets, bindings, verifiers, drift hooks, "
            "runtime assumptions, and readiness through CQRS."
        ),
        when_not_to_use=(
            "Do not use it to execute the cartridge, call customer systems, or "
            "own recurring task runs. It records the portable contract and "
            "readiness evidence only."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Record one staged cartridge contract",
                {
                    "manifest": {
                        "manifest_version": "1.0",
                        "cartridge_id": "phase9-portable-cartridge",
                        "cartridge_version": "2026.04.30",
                        "build_id": "build_2026_04_30_0001",
                    },
                    "deployment_mode": "staged_deployment",
                    "source_ref": "phase_09_live_proof",
                },
            ),
        ],
    ),
    "praxis_authority_portable_cartridge_read": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="portable-cartridge-read",
        when_to_use=(
            "Inspect persisted portable cartridge records, deployment readiness, "
            "Object Truth dependencies, assets, bindings, verifier checks, and "
            "drift hooks before export, mount, or later drift audit."
        ),
        when_not_to_use=(
            "Do not use it to write a contract; use "
            "praxis_authority_portable_cartridge_record for writes."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Describe one cartridge record",
                {
                    "action": "describe_record",
                    "cartridge_record_id": "portable_cartridge_record.phase9.build_2026_04_30_0001.staged_deployment",
                    "include_dependencies": True,
                    "include_bindings": True,
                    "include_verifiers": True,
                    "include_drift_hooks": True,
                },
            ),
            _example(
                "List blocked production cartridges",
                {
                    "action": "list_records",
                    "deployment_mode": "production_deployment",
                    "readiness_status": "blocked",
                },
            ),
        ],
    ),
    "praxis_authority_managed_runtime_record": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="managed-runtime-record",
        when_to_use=(
            "Record an optional managed/exported/hybrid runtime accounting "
            "snapshot with mode policy, metering, run receipt, pricing schedule "
            "reference, heartbeat health, audit context, and customer-safe "
            "observability through CQRS."
        ),
        when_not_to_use=(
            "Do not use it as a scheduler, invoice generator, or hidden required "
            "runtime path. It records the cost and health evidence for a run; "
            "customers may still use exported or hybrid execution."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Record one managed runtime run",
                {
                    "identity": {
                        "run_id": "run.managed.demo",
                        "tenant_ref": "tenant.acme",
                        "environment_ref": "env.prod",
                        "workflow_ref": "workflow.object_truth",
                        "workload_class": "workflow_build",
                    },
                    "policy": {
                        "tenant_ref": "tenant.acme",
                        "environment_ref": "env.prod",
                        "configured_mode": "managed",
                        "managed_workload_classes": ["workflow_build"],
                    },
                    "meter_events": [
                        {"event_kind": "run_started", "occurred_at": "2026-04-30T12:00:00Z"},
                        {
                            "event_kind": "resource_usage",
                            "occurred_at": "2026-04-30T12:00:30Z",
                            "cpu_core_seconds": "120",
                            "memory_gib_seconds": "240",
                        },
                        {"event_kind": "run_finished", "occurred_at": "2026-04-30T12:01:00Z"},
                    ],
                    "terminal_status": "succeeded",
                    "generated_at": "2026-04-30T12:01:01Z",
                    "runtime_version_ref": "runtime.managed.v1",
                },
            ),
        ],
    ),
    "praxis_authority_managed_runtime_read": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="managed-runtime-read",
        when_to_use=(
            "Inspect persisted managed-runtime run receipts, metering, cost "
            "basis, heartbeat health, audit events, pricing schedules, and "
            "customer observability without reading raw tables."
        ),
        when_not_to_use=(
            "Do not use it to record new runtime evidence; use "
            "praxis_authority_managed_runtime_record for writes."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Describe one managed runtime record",
                {
                    "action": "describe_record",
                    "runtime_record_id": "managed_runtime_record.demo",
                    "include_meter_events": True,
                    "include_pool_health": True,
                },
            ),
            _example(
                "List recent managed runs for one tenant",
                {
                    "action": "list_records",
                    "tenant_ref": "tenant.acme",
                    "execution_mode": "managed",
                },
            ),
        ],
    ),
    "praxis_client_operating_model": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="client-operating-model",
        when_to_use=(
            "Build one read-only Client Operating Model operator view from "
            "provided evidence: system census, Object Truth inspection, "
            "identity/source authority, simulation timeline, verifier results, "
            "sandbox drift, cartridge status, managed-runtime accounting, next "
            "safe actions, workflow-builder validation, or Workflow Context "
            "customer composite deployability."
        ),
        when_not_to_use=(
            "Do not use it to persist client evidence, mutate workflows, call live "
            "systems, or claim a source of truth not backed by the supplied evidence. "
            "This is the CQRS read-surface slice; durable projections remain separate."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Build an empty system census view",
                {
                    "view": "system_census",
                    "generated_at": "2026-04-30T12:00:00Z",
                    "permission_scope": {"scope_ref": "tenant.acme", "visibility": "full"},
                    "inputs": {"system_records": []},
                },
            ),
            _example(
                "Validate a bounded workflow-builder graph",
                {
                    "view": "workflow_builder_validation",
                    "inputs": {
                        "graph": {
                            "nodes": [
                                {"node_id": "refresh", "block_ref": "source.refresh"},
                                {"node_id": "verify", "block_ref": "verifier.run"},
                            ],
                            "edges": [{"from": "refresh", "to": "verify"}],
                        },
                        "approved_blocks": {
                            "source.refresh": {"provides": ["fresh_snapshot"]},
                            "verifier.run": {"requires": ["fresh_snapshot"]},
                        },
                        "allowed_edges": [
                            {"from_block": "source.refresh", "to_block": "verifier.run"}
                        ],
                    },
                },
            ),
            _example(
                "Build a Workflow Context composite view",
                {
                    "view": "workflow_context_composite",
                    "generated_at": "2026-04-30T12:00:00Z",
                    "permission_scope": {"scope_ref": "workflow.demo", "visibility": "full"},
                    "inputs": {
                        "workflow_ref": "workflow.demo",
                        "context_pack": {
                            "context_ref": "workflow_context:demo",
                            "context_mode": "synthetic",
                            "truth_state": "synthetic",
                            "confidence_score": 0.42,
                            "entities": [],
                            "blockers": [],
                            "evidence_refs": [],
                        },
                    },
                },
            ),
        ],
    ),
    "praxis_client_operating_model_snapshot_store": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "Persist an already-built Client Operating Model operator_view "
            "snapshot for historical readback and proof receipts."
        ),
        when_not_to_use=(
            "Do not use it to build the view, call client systems, or persist raw "
            "source payloads. Build the read model first with "
            "praxis_client_operating_model, then store the operator_view."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Store an operator-view snapshot",
                {
                    "operator_view": {
                        "kind": "client_operating_model.operator_surface.system_census.v1",
                        "view_id": "system_census.demo",
                        "state": "empty",
                        "freshness": {"status": "unknown"},
                        "permission_scope": {"scope_ref": "tenant.acme"},
                        "evidence_refs": [],
                        "correlation_ids": [],
                        "payload": {"counts": {"systems": 0}},
                    },
                    "observed_by_ref": "operator:nate",
                    "source_ref": "phase_13.http_route_proof",
                },
            ),
        ],
    ),
    "praxis_client_operating_model_snapshots": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "Read stored Client Operating Model operator-view snapshots by "
            "snapshot ref, digest, view, or scope."
        ),
        when_not_to_use=(
            "Do not use it for request-time derivation from fresh evidence; use "
            "praxis_client_operating_model for that."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Read latest stored system census snapshots for a tenant",
                {
                    "view": "system_census",
                    "scope_ref": "tenant.acme",
                    "limit": 5,
                },
            ),
        ],
    ),
    "praxis_client_system_discovery": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="client-system-discovery",
        when_to_use=(
            "Persist or query client-system discovery authority: system census "
            "records, connector surface evidence, credential-health references, "
            "and typed discovery gaps. Use this before designing integrations "
            "from guessed connector behavior."
        ),
        when_not_to_use=(
            "Do not use it for Object Truth field normalization or Virtual Lab "
            "simulation. It owns discovery/census evidence only; downstream "
            "truth and consequence models use their own surfaces."
        ),
        risks={"discover": "write", "record_gap": "write", "list": "read", "search": "read", "describe": "read"},
        examples=[
            _example(
                "List systems for a tenant",
                {"action": "list", "tenant_ref": "tenant.acme"},
            ),
            _example(
                "Record a discovery gap",
                {
                    "action": "record_gap",
                    "gap_kind": "missing_connector",
                    "reason_code": "client_system.connector.missing",
                    "source_ref": "tenant.acme/salesforce",
                    "detail": "Salesforce connector exists but credential-health evidence is absent.",
                    "legal_repair_actions": ["refresh credential health evidence"],
                },
            ),
        ],
    ),
    "praxis_client_system_discovery_census_record": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "Persist one client-system census record and connector evidence "
            "through the CQRS gateway."
        ),
        when_not_to_use=(
            "Do not use it for readback or search; use "
            "praxis_client_system_discovery_census_read for reads."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Record a fixture client-system census",
                {
                    "tenant_ref": "tenant.acme",
                    "workspace_ref": "workspace.acme",
                    "system_slug": "crm",
                    "system_name": "CRM",
                    "discovery_source": "fixture",
                    "captured_at": "2026-04-30T12:00:00Z",
                    "connectors": [],
                },
            ),
        ],
    ),
    "praxis_client_system_discovery_census_read": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "Read client-system census authority by list, search, or describe "
            "through the CQRS gateway."
        ),
        when_not_to_use=(
            "Do not use it to persist discovery results or gaps."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "List systems for a tenant",
                {"action": "list", "tenant_ref": "tenant.acme"},
            ),
            _example(
                "Search connector census",
                {"action": "search", "query": "salesforce", "limit": 10},
            ),
        ],
    ),
    "praxis_client_system_discovery_gap_record": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias=None,
        when_to_use=(
            "Record one typed client-system discovery gap with a gateway "
            "receipt and authority event."
        ),
        when_not_to_use=(
            "Do not use it to persist census rows; use "
            "praxis_client_system_discovery_census_record."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Record missing credential-health evidence",
                {
                    "gap_kind": "credential_health_unknown",
                    "reason_code": "credential.health.unknown",
                    "source_ref": "census:client_system_census.demo",
                    "detail": "Credential check has not run.",
                    "legal_repair_actions": ["run credential health probe"],
                },
            ),
        ],
    ),
    "praxis_register_operation": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="register-operation",
        when_to_use=(
            "Register a net-new CQRS operation (gateway dispatch key + handler + Pydantic input) "
            "through the catalog without hand-authoring a migration for the triple write."
        ),
        when_not_to_use=(
            "Do not use it to tweak an existing operation's input shape — use praxis_evolve_operation_field "
            "for planned field additions. Do not use it to soft-delete an op — use praxis_retire_operation."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Register a hypothetical read-only query op",
                {
                    "operation_ref": "example.query.widget_stats",
                    "operation_name": "example_query_widget_stats",
                    "handler_ref": "runtime.operations.queries.widget_stats.handle_widget_stats",
                    "input_model_ref": "runtime.operations.queries.widget_stats.WidgetStatsQuery",
                    "authority_domain_ref": "authority.example",
                    "operation_kind": "query",
                    "posture": "observe",
                    "idempotency_policy": "read_only",
                },
            ),
        ],
    ),
    "praxis_retire_operation": _tool(
        surface="operations",
        tier="advanced",
        recommended_alias="retire-operation",
        when_to_use=(
            "Soft-retire an operation (disable gateway binding, mark authority object deprecated) "
            "while keeping rows for receipts and audit continuity."
        ),
        when_not_to_use=(
            "Do not use it when you meant to register a replacement op first — retire after the new "
            "path is live. Do not use it for physical deletion; rows are retained by design."
        ),
        risks={"default": "write"},
        examples=[
            _example(
                "Retire an obsolete operation",
                {"operation_ref": "legacy.integration.probe_stale", "reason_code": "superseded"},
            ),
        ],
    ),
    "praxis_research": _tool(
        surface="knowledge",
        tier="stable",
        recommended_alias=None,
        when_to_use="Search prior research findings and analysis results with a lighter-weight surface than recall.",
        when_not_to_use="Do not use it for general knowledge or code search.",
        risks={"default": "read", "actions": {"search": "read"}},
        examples=[
            _example("Search prior research", {"action": "search", "query": "provider routing performance"}),
        ],
    ),
    "praxis_research_workflow": _tool(
        surface="research",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Launch or inspect fan-out research workflows for deeper multi-angle investigations.",
        when_not_to_use="Do not use it for single-shot questions where recall or query is enough.",
        risks={"default": "launch", "actions": {"list": "read", "run": "launch"}},
        examples=[
            _example("Launch a research workflow", {"action": "run", "topic": "best practices for durable MCP transports", "workers": 8}),
        ],
    ),
    "praxis_review_submission": _tool(
        surface="submissions",
        tier="session",
        recommended_alias=None,
        when_to_use="Approve, reject, or request changes on a sealed submission inside a workflow session.",
        when_not_to_use="Do not use it outside token-scoped workflow review flows.",
        risks={"default": "session"},
        examples=[
            _example("Approve a submission", {"submission_id": "submission_abc123", "decision": "approve", "summary": "Looks good"}),
        ],
    ),
    "praxis_session": _tool(
        surface="planning",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Inspect or validate session carry-forward packs between work sessions.",
        when_not_to_use="Do not use it as a live workflow-session context surface.",
        risks={"default": "read", "actions": {"latest": "read", "validate": "read"}},
        examples=[
            _example("Read the latest carry-forward pack", {"action": "latest"}),
        ],
    ),
    "praxis_session_context": _tool(
        surface="session",
        tier="session",
        recommended_alias=None,
        when_to_use="Read or write persistent context owned by the active workflow MCP session.",
        when_not_to_use="Do not use it outside token-scoped workflow execution.",
        risks={"default": "session", "actions": {"read": "session", "write": "session"}},
        examples=[
            _example("Read session context", {"action": "read"}),
            _example("Write session context", {"action": "write", "context": {"step": 3}}),
        ],
    ),
    "praxis_search": _tool(
        surface="knowledge",
        tier="stable",
        recommended_alias="search",
        when_to_use=(
            "Federated search across code, decisions, knowledge, bugs, receipts, and related sources "
            "with semantic, exact, or regex modes — prefer this as the default discovery entry point."
        ),
        when_not_to_use=(
            "Do not use it for writes, workflow launches, or mutating operator state — use the "
            "subsystem-specific tools those actions require."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Semantic search the workflow runtime",
                {
                    "query": "retry logic with exponential backoff",
                    "sources": ["code"],
                    "scope": {"paths": ["Code&DBs/Workflow/runtime/**/*.py"]},
                },
            ),
            _example(
                "Regex search with line context",
                {
                    "query": "/class.*Authority/",
                    "mode": "regex",
                    "sources": ["code"],
                    "scope": {"paths": ["Code&DBs/Workflow/surfaces/**/*.py"]},
                    "shape": "context",
                    "context_lines": 3,
                },
            ),
        ],
    ),
    "praxis_submit_artifact_bundle": _tool(
        surface="submissions",
        tier="session",
        recommended_alias=None,
        when_to_use="Submit an artifact-bundle result owned by the active workflow session.",
        when_not_to_use="Do not use it outside token-scoped workflow execution.",
        risks={"default": "session"},
        examples=[
            _example("Submit an artifact bundle", {"summary": "Generated migration bundle", "primary_paths": ["artifacts/migrations"], "result_kind": "artifact_bundle"}),
        ],
    ),
    "praxis_submit_code_change_candidate": _tool(
        surface="submissions",
        tier="session",
        recommended_alias=None,
        when_to_use="Submit a structured code-change candidate owned by the active workflow session.",
        when_not_to_use="Do not use it outside token-scoped workflow execution; do not submit raw LLM-authored diffs.",
        risks={"default": "session"},
        examples=[
            _example("Submit a candidate", {"bug_id": "BUG-12345678", "review_routing": "human_review"}),
        ],
    ),
    "praxis_code_change_candidate_review": _tool(
        surface="submissions",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Approve, reject, or request changes on a sealed code-change candidate.",
        when_not_to_use="Do not use it to apply source; materialization is a separate operation.",
        risks={"default": "write"},
        examples=[
            _example("Approve a candidate", {"candidate_id": "<uuid>", "reviewer_ref": "human:nate", "decision": "approve"}),
        ],
    ),
    "praxis_code_change_candidate_materialize": _tool(
        surface="submissions",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Apply a reviewed or auto-apply code-change candidate after verifier and gate checks.",
        when_not_to_use="Do not use it to bypass review or verifier evidence.",
        risks={"default": "write"},
        examples=[
            _example("Materialize a candidate", {"candidate_id": "<uuid>", "materialized_by": "human:nate"}),
        ],
    ),
    "praxis_code_change_candidate_preflight": _tool(
        surface="submissions",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Run trusted preflight on a sealed candidate before review. Recomputes the patch from the real base head, runs the temp verifier, and validates agent-declared authority impacts against runtime-derived overlap. Required before code_change_candidate.review approve.",
        when_not_to_use="Do not use it to bypass impact contract validation; preflight is the gate, not a hint.",
        risks={"default": "write"},
        examples=[
            _example("Preflight a candidate", {"candidate_id": "<uuid>", "triggered_by": "human:nate"}),
        ],
    ),
    "praxis_resolve_compose_authority_binding": _tool(
        surface="cqrs",
        tier="advanced",
        recommended_alias=None,
        when_to_use="At plan composition time, resolve the canonical write scope, the read-only predecessor obligation pack, and explicit blocked-compat units for a set of target authority units. Use this so packets bind a workspace where duplicate authority is invisible to the worker.",
        when_not_to_use="Not for live source mutation. This is a read-only resolver; use code_change_candidate.* for write paths.",
        risks={"default": "read"},
        examples=[
            _example(
                "Resolve binding for an operation target",
                {
                    "targets": [
                        {"unit_kind": "operation_ref", "unit_ref": "compose_plan"},
                        {"unit_kind": "source_path", "unit_ref": "Code&DBs/Workflow/runtime/operations/commands/plan_orchestration.py"},
                    ]
                },
            ),
        ],
    ),
    "praxis_audit_authority_impact_contract": _tool(
        surface="cqrs",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Audit a path list (typically `git diff --name-only` over a window) for impact-contract coverage. Surfaces drift where authority-bearing files were edited without a backing candidate impact contract — catches direct commits, scripted edits, and hot-fixes that bypass the gated pipeline.",
        when_not_to_use="Not for the candidate-path enforcement chain (preflight + review + materialize already enforce in-band). This is the orthogonal audit for out-of-band changes.",
        risks={"default": "read"},
        examples=[
            _example(
                "Audit a recent change set",
                {"paths": [
                    "Code&DBs/Databases/migrations/workflow/342_foo.sql",
                    "Code&DBs/Workflow/runtime/operations/commands/foo.py",
                    "docs/notes.md",
                ]},
            ),
        ],
    ),
    "praxis_audit_summary": _tool(
        surface="cqrs",
        tier="stable",
        recommended_alias=None,
        when_to_use="Aggregate audit lens over the gateway dispatch ledger and policy-enforcement ledger. One call returns trailing-window totals (receipts, completed, replayed, failed, untagged_transport), per-transport / per-execution-status / per-operation-kind buckets, top-10 operations with failure counts, and a compliance breakdown (admits, rejects, top tables, top policies). Use it for 'are receipts healthy?' / 'what surface is generating failures?' / 'which policies blocked mutations recently?' questions.",
        when_not_to_use="Not for row-level audit queries — use praxis_search with sources=['authority_receipts'] or ['compliance_receipts'] for individual receipt lookups. Not a real-time monitor; the trailing window is bounded by since_hours.",
        risks={"default": "read"},
        examples=[
            _example("Last 24h aggregates", {"since_hours": 24}),
            _example("Last hour audit pulse", {"since_hours": 1}),
            _example("Trailing week breakdown", {"since_hours": 168}),
        ],
    ),
    "praxis_submit_research_result": _tool(
        surface="submissions",
        tier="session",
        recommended_alias=None,
        when_to_use="Submit a sealed research result owned by the active workflow session.",
        when_not_to_use="Do not use it outside token-scoped workflow execution.",
        risks={"default": "session"},
        examples=[
            _example("Submit a research result", {"summary": "Surveyed MCP CLI exposure patterns", "primary_paths": ["notes/research.md"], "result_kind": "research_result"}),
        ],
    ),
    "praxis_subscribe_events": _tool(
        surface="session",
        tier="session",
        recommended_alias=None,
        when_to_use="Poll workflow-scoped event updates since the last cursor position for the active session.",
        when_not_to_use="Do not use it outside token-scoped workflow execution.",
        risks={"default": "session"},
        examples=[
            _example("Poll build-state events", {"channel": "build_state", "limit": 50}),
        ],
    ),
    "praxis_solution": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias="solution",
        when_to_use="Submit, list, or inspect durable multi-workflow Solutions.",
        when_not_to_use="Do not use it for one workflow run; use praxis_workflow.",
        risks={
            "default": "read",
            "actions": {
                "list": "read",
                "observe": "read",
                "show": "read",
                "status": "read",
                "start": "launch",
                "submit": "launch",
            },
        },
        examples=[
            _example("List recent Solutions", {"action": "list", "limit": 10}),
            _example("Inspect one Solution", {"action": "status", "solution_id": "workflow_chain_123"}),
            _example("Submit a Solution", {"action": "submit", "coordination_path": "artifacts/workflow/solution.json"}),
        ],
    ),
    "praxis_workflow": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Run, preview, inspect, spawn, chain, claim, acknowledge, retry, cancel, repair, or list workflows through the MCP workflow surface.",
        when_not_to_use="Do not use it for natural-language questions or health checks.",
        risks={
            "default": "launch",
            "actions": {
                "claim": "read",
                "acknowledge": "write",
                "status": "read",
                "inspect": "read",
                "list": "read",
                "notifications": "read",
                "preview": "read",
                "run": "launch",
                "spawn": "launch",
                "chain": "launch",
                "retry": "launch",
                "cancel": "launch",
                "repair": "launch",
            },
        },
        examples=[
            _example("List recent workflows", {"action": "list"}),
            _example("Run a spec", {"action": "run", "spec_path": "config/specs/example.queue.json"}),
            _example("Preview execution inputs", {"action": "preview", "spec_path": "config/specs/example.queue.json"}),
            _example(
                "Spawn a child workflow",
                {"action": "spawn", "spec_path": "config/specs/child_workflow.queue.json", "parent_run_id": "workflow_parent_001", "dispatch_reason": "manual.spawn"},
            ),
            _example(
                "Submit a chain",
                {"action": "chain", "coordination_path": "config/chains/example-chain.json", "adopt_active": True},
            ),
            _example(
                "Read claimable worker work",
                {"action": "claim", "subscription_id": "workflow:worker:bridge", "run_id": "workflow_001"},
            ),
            _example(
                "Acknowledge a worker batch",
                {"action": "acknowledge", "work": {"claimable": True}, "through_evidence_seq": 2},
            ),
            _example(
                "Repair a degraded sync state",
                {"action": "repair", "run_id": "workflow_001"},
            ),
        ],
    ),
    "praxis_workflow_validate": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Validate a workflow spec before launching it.",
        when_not_to_use="Do not use it when you need to actually run the workflow.",
        risks={"default": "read"},
        examples=[
            _example("Validate a spec", {"spec_path": "Code&DBs/Workflow/artifacts/workflow/operating_model_paradigm.queue.json"}),
        ],
    ),
    "praxis_plan_lifecycle": _tool(
        surface="workflow",
        tier="stable",
        recommended_alias="plan-history",
        when_to_use=(
            "Read every plan.* event for one workflow_id in chronological "
            "order — composed, approved, launched, or blocked. The Q-side "
            "read of the planning stack's CQRS pattern."
        ),
        when_not_to_use=(
            "Do not use it for workflow_run status; that's a separate "
            "query surfaced by praxis_workflow status/stream actions."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Inspect the lifecycle of a composed plan",
                {"workflow_id": "plan.deadbeef12345678"},
            ),
        ],
    ),
    "praxis_compose_and_launch": _tool(
        surface="workflow",
        tier="stable",
        recommended_alias="ship-intent",
        when_to_use=(
            "End-to-end: prose intent → ProposedPlan → ApprovedPlan → LaunchReceipt "
            "in one call. For trusted automation (CI, scripts, experienced "
            "operators). Fails closed by default on unresolved routes, unbound "
            "pills, or invalid approvals."
        ),
        when_not_to_use=(
            "Do not use it for untrusted input or when the caller needs to inspect "
            "the ProposedPlan first. Use praxis_compose_plan + praxis_approve_proposed_plan "
            "+ praxis_launch_plan(approved_plan=...) for the three-step flow."
        ),
        risks={"default": "launch"},
        examples=[
            _example(
                "Ship an intent through the full pipeline",
                {
                    "intent": (
                        "1. Add a timezone column to users.\n"
                        "2. Backfill existing rows with UTC.\n"
                        "3. Update the profile UI to expose the field."
                    ),
                    "approved_by": "nate@praxis",
                    "plan_name": "timezone_rollout",
                },
            ),
        ],
    ),
    "praxis_compose_plan": _tool(
        surface="workflow",
        tier="stable",
        recommended_alias="compose-plan",
        when_to_use=(
            "Turn prose intent with explicit step markers into a ProposedPlan "
            "in one call — chains Layer 2 (decompose) → Layer 1 (bind) → "
            "Layer 5 (translate + preview). Compose with approve-plan + "
            "launch-plan(approved_plan=...) for the full approval-gated flow."
        ),
        when_not_to_use=(
            "Do not use it for free prose without step markers. Reword the "
            "intent or pass allow_single_step=true explicitly."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Compose a numbered-list intent",
                {
                    "intent": (
                        "1. Add a timezone column to users.\n"
                        "2. Backfill existing rows with UTC.\n"
                        "3. Update the profile UI to expose the field."
                    ),
                    "plan_name": "timezone_rollout",
                    "why": "Operator requested personalization support.",
                },
            ),
            _example(
                "Compose with per-step write scope",
                {
                    "intent": (
                        "1. Update the users schema.\n"
                        "2. Migrate existing rows.\n"
                        "3. Update the UI."
                    ),
                    "write_scope_per_step": [
                        ["Code&DBs/Databases/migrations/"],
                        ["Code&DBs/Workflow/scripts/backfill.py"],
                        ["Code&DBs/Workflow/surfaces/app/src/"]
                    ],
                },
            ),
        ],
    ),
    "praxis_decompose_intent": _tool(
        surface="workflow",
        tier="stable",
        recommended_alias="decompose",
        when_to_use=(
            "Split prose intent into ordered steps by parsing explicit markers "
            "(numbered lists, bulleted lists, or first/then/finally ordering). "
            "Layer 2 (Decompose) of the planning stack — call before turning "
            "steps into PlanPackets."
        ),
        when_not_to_use=(
            "Do not use it to decompose free prose without markers. Reword the "
            "intent, wrap with an LLM extractor, or pass allow_single_step=true "
            "to accept the whole intent as one step."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Decompose a numbered-list intent",
                {
                    "intent": (
                        "1. Add a timezone column to users.\n"
                        "2. Backfill existing rows with UTC.\n"
                        "3. Update the profile UI to expose the field."
                    )
                },
            ),
            _example(
                "Decompose a first/then/finally intent",
                {
                    "intent": (
                        "First investigate the leak, then patch it, finally verify with a run."
                    )
                },
            ),
            _example(
                "Accept a single-step prose intent explicitly",
                {
                    "intent": "Make the dashboard faster by reducing API calls on load.",
                    "allow_single_step": True,
                },
            ),
        ],
    ),
    "praxis_approve_proposed_plan": _tool(
        surface="workflow",
        tier="stable",
        recommended_alias="approve-plan",
        when_to_use=(
            "Approve a ProposedPlan so launch_approved can submit it. Wraps the "
            "proposal with approved_by + timestamp + hash; the hash binds the "
            "approval to the exact spec_dict so tampering between approve and "
            "launch fails closed. The proposed plan must already carry machine-"
            "checkable provider freshness evidence with fresh route truth."
        ),
        when_not_to_use=(
            "Do not use it for no-approval launches — praxis_launch_plan in "
            "submit mode is the direct path."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Approve a proposal returned by praxis_launch_plan(preview_only=true)",
                {
                    "proposed": {
                        "spec_dict": {"name": "...", "jobs": []},
                        "preview": {},
                        "warnings": [],
                        "workflow_id": "plan.deadbeef",
                        "spec_name": "bug_wave_0",
                        "total_jobs": 0,
                        "packet_declarations": [],
                        "binding_summary": {"totals": {"bound": 0, "ambiguous": 0, "unbound": 0}, "unbound_refs": [], "ambiguous_refs": []},
                        "provider_freshness": {
                            "route_truth_ref": "preview:deadbeef",
                            "route_truth_checked_at": "2026-04-28T00:00:00+00:00",
                        },
                    },
                    "approved_by": "nate@praxis",
                    "approval_note": "Looks good; proceed.",
                },
            )
        ],
    ),
    "praxis_bind_data_pills": _tool(
        surface="workflow",
        tier="stable",
        recommended_alias="bind-pills",
        when_to_use=(
            "Suggest likely object.field data-pill candidates from loose prose and "
            "validate explicit references against the data dictionary authority. "
            "Layer 1 (Bind) of the planning stack — call BEFORE decomposing intent "
            "into packets so every field ref is either confirmed or surfaced as a "
            "candidate to confirm."
        ),
        when_not_to_use=(
            "Do not treat suggestions as bound authority. Suggested pills are candidates; "
            "confirmed packet compilation still needs explicit object.field refs or a "
            "caller approval step."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Bind pills in an update-user intent",
                {"intent": "Update users.first_name whenever users.email changes."},
            ),
            _example(
                "Restrict binding to a workspace allowlist",
                {
                    "intent": "Look at users.email and orders.total_cents.",
                    "object_kinds": ["users"],
                },
            ),
        ],
    ),
    "praxis_suggest_plan_atoms": _tool(
        surface="workflow",
        tier="stable",
        recommended_alias="suggest-atoms",
        when_to_use=(
            "Free prose (any length, no markers, no order) should yield candidate "
            "data pills, candidate step types, and candidate input parameters as "
            "three independent suggestion streams. Layer 0 (Suggest) of the "
            "planning stack — call when the prose has no explicit step markers "
            "and the downstream LLM author needs atoms to plan from."
        ),
        when_not_to_use=(
            "Do not use this to launch, order, or commit. It returns suggestions; "
            "an LLM author or operator still has to compose them into a packet "
            "list. For prose that already has explicit markers, call "
            "praxis_decompose_intent for ordered steps instead."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Atoms from a free-prose integration request",
                {
                    "intent": (
                        "A repeatable workflow where we feed in an app name or "
                        "app domain and it gets broken up into multiple steps to "
                        "plan search, retrieve, evaluate and then attempt to "
                        "build a custom integration for an application."
                    )
                },
            ),
        ],
    ),
    "praxis_recognize_intent": _tool(
        surface="workflow",
        tier="stable",
        recommended_alias="recognize-intent",
        when_to_use=(
            "Extract source-ordered user-stated spans, match them to data "
            "dictionary/tool authority, and surface authority-backed prerequisite "
            "suggestions before plan generation or composition turns anything into a workflow."
        ),
        when_not_to_use=(
            "Do not use this as a planner or launcher. It does not reorder user "
            "intent, invent confirmed steps, or create a runnable spec."
        ),
        risks={"default": "read"},
        examples=[
            _example(
                "Recognize a messy integration workflow request",
                {
                    "intent": (
                        "Feed in an app name or app domain, plan search retrieve "
                        "evaluate and build a custom integration."
                    )
                },
            ),
        ],
    ),
    "praxis_launch_plan": _tool(
        surface="workflow",
        tier="stable",
        recommended_alias="launch-plan",
        when_to_use=(
            "Translate an already-planned packet list into a workflow spec and "
            "submit it (or preview first with preview_only=true). This is the "
            "layer-5 translation primitive — caller still owns upstream planning "
            "(extract data pills, decompose prose, reorder by data-flow, author "
            "per-step prompts). Proof launches must carry fresh provider route "
            "truth or a recent provider availability refresh receipt before approval."
        ),
        when_not_to_use=(
            "Do not use it to launch a pre-existing .queue.json spec from disk — "
            "use praxis_workflow action=run for that path. Do not expect it to do "
            "the planning itself (decompose prose, pick fields, reorder steps, "
            "write real prompts) — those layers live with the caller today. If you "
            "intend to approve the launch, first obtain fresh provider route truth "
            "or a recent provider availability refresh receipt."
        ),
        risks={"default": "write", "actions": {"preview": "read", "submit": "write"}},
        examples=[
            _example(
                "Launch a one-packet plan",
                {
                    "plan": {
                        "name": "fix_preview_submit_route_split",
                        "packets": [
                            {
                                "description": "Make preview call TaskTypeRouter so auto/* routes resolve the same way submit does.",
                                "write": ["Code&DBs/Workflow/runtime/workflow/_admission.py"],
                                "stage": "build",
                                "label": "preview-submit-route-parity",
                            }
                        ],
                    }
                },
            ),
            _example(
                "Launch a multi-packet wave with dependencies",
                {
                    "plan": {
                        "name": "bug_wave_0_authority",
                        "why": "Fix bug/evidence authority before burning down dependent bugs.",
                        "packets": [
                            {
                                "description": "Require verifier/evidence link before FIXED transitions.",
                                "write": ["Code&DBs/Workflow/runtime/bugs.py"],
                                "stage": "build",
                                "label": "bug-fixed-requires-evidence",
                                "bug_ref": "BUG-175EB9F3",
                            },
                            {
                                "description": "Disallow silent FIXED -> DEFERRED without superseding evidence.",
                                "write": ["Code&DBs/Workflow/runtime/bugs.py"],
                                "stage": "build",
                                "label": "bug-supersede-rule",
                                "bug_ref": "BUG-9B812B32",
                                "depends_on": ["bug-fixed-requires-evidence"],
                            },
                        ],
                    }
                },
            ),
            _example(
                "Materialize packets from bug IDs (wave deps auto-wired)",
                {
                    "plan": {
                        "name": "bug_burn_p1_authority",
                        "why": "Wave-based burn-down of P1 authority bugs.",
                        "from_bugs": [
                            "BUG-175EB9F3",
                            "BUG-1DBACCD8",
                            "BUG-9B812B32",
                        ],
                    }
                },
            ),
            _example(
                "Materialize packets from roadmap items",
                {
                    "plan": {
                        "name": "q2_roadmap_landing",
                        "why": "Land the two active roadmap items this phase.",
                        "from_roadmap_items": [
                            "roadmap_item.make.canvas.ui.emit.runnable.graph.authority.for.gated.9.step.workflows",
                        ],
                    }
                },
            ),
            _example(
                "Materialize packets from open operator ideas",
                {
                    "plan": {
                        "name": "idea_intake_round",
                        "why": "Explore open operator ideas as bounded build packets.",
                        "from_ideas": [
                            "operator_idea.ingest_shopify_orders",
                            "operator_idea.canvas_inbox_digest",
                        ],
                    }
                },
            ),
            _example(
                "Materialize fix packets from friction events",
                {
                    "plan": {
                        "name": "friction_burn_20260424",
                        "why": "Close out the friction events logged today.",
                        "from_friction": [
                            "friction.workflow_submit_001",
                            "friction.workflow_submit_002",
                        ],
                    }
                },
            ),
        ],
    ),
}


__all__ = ["CLI_TOOL_METADATA"]
