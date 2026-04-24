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
) -> dict[str, Any]:
    return {
        "surface": surface,
        "tier": tier,
        "recommended_alias": recommended_alias,
        "when_to_use": when_to_use,
        "when_not_to_use": when_not_to_use,
        "risks": risks,
        "examples": examples,
    }


CLI_TOOL_METADATA: dict[str, dict[str, Any]] = {
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
        risks={"default": "read"},
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
        when_to_use="List or record durable operator decisions such as architecture policy rows in the canonical operator_decisions table.",
        when_not_to_use="Do not use it for roadmap item authoring or cutover-gate admission.",
        risks={"default": "read", "actions": {"list": "read", "record": "write"}},
        examples=[
            _example(
                "List current architecture policy decisions",
                {"action": "list", "decision_kind": "architecture_policy"},
            ),
            _example(
                "Record one architecture policy decision",
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
                "Record a decision-table architecture policy",
                {
                    "authority_domain": "decision_tables",
                    "policy_slug": "db-native-authority",
                    "title": "Decision tables are DB-native authority",
                    "rationale": "Keep authority in Postgres.",
                    "decided_by": "praxis-admin",
                    "decision_source": "cto.guidance",
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
    "praxis_bind_data_pills": _tool(
        surface="workflow",
        tier="stable",
        recommended_alias="bind-pills",
        when_to_use=(
            "Extract and validate object.field data-pill references from prose intent "
            "against the data dictionary authority. Layer 1 (Bind) of the planning stack "
            "— call BEFORE decomposing intent into packets so every field ref is known "
            "to exist."
        ),
        when_not_to_use=(
            "Do not use it to infer missing references. This tool matches explicit "
            "object.field spans only; loose prose like \"the user's name\" returns no "
            "bound pills, and that's honest."
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
    "praxis_launch_plan": _tool(
        surface="workflow",
        tier="stable",
        recommended_alias="launch-plan",
        when_to_use=(
            "Translate an already-planned packet list into a workflow spec and "
            "submit it (or preview first with preview_only=true). This is the "
            "layer-5 translation primitive — caller still owns upstream planning "
            "(extract data pills, decompose prose, reorder by data-flow, author "
            "per-step prompts)."
        ),
        when_not_to_use=(
            "Do not use it to launch a pre-existing .queue.json spec from disk — "
            "use praxis_workflow action=run for that path. Do not expect it to do "
            "the planning itself (decompose prose, pick fields, reorder steps, "
            "write real prompts) — those layers live with the caller today."
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
        ],
    ),
}


__all__ = ["CLI_TOOL_METADATA"]
