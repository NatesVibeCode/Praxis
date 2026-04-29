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
            _example("Search open routing bugs", {"action": "search", "title": "routing", "status": "OPEN"}),
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
    "praxis_operator_closeout": _tool(
        surface="operator",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Preview or commit operator work-item closeout through the shared gate.",
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
            "fill in via the Moon Decisions panel."
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
    "praxis_next_actions": _tool(
        surface="operator",
        tier="stable",
        recommended_alias="next-actions",
        when_to_use="Legacy alias only; prefer praxis_next(action='next').",
        when_not_to_use="Do not build new workflows against this name.",
        risks={"default": "read"},
        examples=[
            _example(
                "Legacy next-actions call",
                {"intent": "Fix workflow retries so every retry declares the failed receipt and retry delta."},
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
    "praxis_submit_code_change": _tool(
        surface="submissions",
        tier="session",
        recommended_alias=None,
        when_to_use="Submit a sealed code-change result owned by the active workflow session.",
        when_not_to_use="Do not use it outside token-scoped workflow execution.",
        risks={"default": "session"},
        examples=[
            _example("Submit a code change", {"summary": "Fixed MCP transport framing", "primary_paths": ["surfaces/mcp/protocol.py"], "result_kind": "code_change"}),
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
    "praxis_wave": _tool(
        surface="workflow",
        tier="advanced",
        recommended_alias=None,
        when_to_use="Observe or coordinate wave-based execution programs.",
        when_not_to_use="Do not use it for single workflow runs with no wave orchestration.",
        risks={
            "default": "read",
            "actions": {
                "observe": "read",
                "next": "read",
                "start": "launch",
                "record": "write",
            },
        },
        examples=[
            _example("List runnable jobs on one wave", {"action": "next", "wave_id": "wave_1"}),
            _example("Observe current wave state", {"action": "observe"}),
            _example("Record results on one wave", {"action": "record", "wave_id": "wave_1", "jobs": "build:pass,test:fail"}),
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
                            "roadmap_item.make.moon.ui.emit.runnable.graph.authority.for.gated.9.step.workflows",
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
                            "operator_idea.moon_inbox_digest",
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
