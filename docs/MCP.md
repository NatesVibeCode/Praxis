# Praxis MCP Tools

Praxis exposes 128 catalog-backed tools via the [Model Context Protocol](https://modelcontextprotocol.io/).

CLI discovery is generated from the same catalog metadata:

- `workflow tools list`
- `workflow tools search <text> [--exact]`
- `workflow tools describe <tool|alias|entrypoint>`
- `workflow tools call <tool|alias|entrypoint> --input-json '{...}'`
- single-result searches print the direct describe and entrypoint commands
- regenerate docs with `PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m scripts.generate_mcp_docs`

## Catalog Summary

| Tool | Surface | Tier | Alias | Risks | Replacement | Description |
| --- | --- | --- | --- | --- | --- | --- |
| `praxis_discover` | `code` | `stable` | `workflow discover` | `read`, `write` | - | Find existing code that already does what you need — BEFORE writing new code. Uses hybrid retrieval: vector embeddings over AST-extracted behavioral fingerprints plus Postgres full-text search, fused with reciprocal rank fusion so you get both semantic and exact-ish matches even when naming differs. |
| `praxis_data` | `data` | `stable` | `workflow data` | `launch`, `read`, `write` | - | Run deterministic data cleanup and reconciliation jobs: parse datasets, profile fields, filter records, sort rows, normalize values, repair rows, run repair loops, backfill missing values, redact sensitive fields, checkpoint state, replay cursor windows, approve plans, apply approved plans, validate contracts, transform records, join or merge sources, aggregate groups, split partitions, export shaped datasets, dedupe keys, route dead-letter rows, reconcile source vs target state, sync target state deterministically, generate workflow specs, and launch those jobs through Praxis. |
| `praxis_artifacts` | `evidence` | `stable` | `workflow artifacts` | `read` | - | Browse and compare files produced by workflow sandbox runs. Each workflow job can write artifacts (code, logs, reports) — this tool lets you find, search, and diff them. |
| `praxis_bugs` | `evidence` | `stable` | `workflow bugs` | `launch`, `read`, `write` | - | Track bugs in the platform's Postgres-backed bug tracker. List open bugs, file new ones, search by keyword, inspect similar historical fixes, replay a bug from canonical evidence, bulk backfill replay provenance, or resolve existing bugs. |
| `praxis_constraints` | `evidence` | `advanced` | - | `read` | - | View automatically-mined constraints from past workflow failures. The system learns rules like 'files in runtime/ must include imports' from repeated failures. |
| `praxis_friction` | `evidence` | `advanced` | - | `read` | - | View the friction ledger — a record of every time a guardrail blocked or warned about an action (scope violations, secret leaks, policy bounces). |
| `praxis_patterns` | `evidence` | `stable` | - | `read`, `write` | - | Inspect and materialize durable platform patterns: recurring failure shapes clustered from friction events, bugs, and receipts. Patterns sit between raw evidence and bug tickets so repeated platform pain becomes one queryable authority object with evidence links and promotion rules. |
| `praxis_receipts` | `evidence` | `advanced` | - | `read` | - | Search through past workflow results and analyze costs. Every workflow run produces receipts — this tool lets you search them by keyword and analyze token/cost spending. |
| `praxis_audit_primitive` | `general` | `advanced` | - | `read` | - | Generic scan/plan/resolve surface for platform audits (wiring, governance, drift). Call action='playbook' first to read the structured usage guide; then 'registered' to discover audits/patterns, 'plan' to see findings + proposed actions, 'apply' to execute auto-safe patterns. Code-editing patterns are gated behind autorun_ok=False and never fire from 'apply'. |
| `praxis_data_dictionary` | `general` | `advanced` | - | `read` | - | Unified data dictionary authority. Auto-projects field descriptors for every injected object (tables, object_types, integrations, datasets, ingest payloads, operator decisions, receipts, MCP tools). Operator overrides win over projected rows. |
| `praxis_data_dictionary_classifications` | `general` | `advanced` | - | `read` | - | Classification / tag authority for data dictionary objects. Auto-projected from name heuristics (PII detectors, credential tokens, owner columns) and structural type hints. Operator tags take precedence. |
| `praxis_data_dictionary_drift` | `general` | `advanced` | - | `read` | - | Schema-drift detector for the data dictionary. Snapshots the field inventory each heartbeat, diffs successive snapshots, and reports cross-axis impact (PII dropped, downstream consumers affected, quality rules orphaned, stewards to notify). High-severity drift (P0/P1) auto-files dedupe-keyed governance bugs. |
| `praxis_data_dictionary_governance` | `general` | `advanced` | - | `read` | - | Cross-axis governance compliance scan over the data dictionary. Checks three policies: (1) objects carrying a `pii` tag without an owner steward, (2) objects carrying a `sensitive` tag without an owner, (3) enabled rules with severity='error' whose latest run is fail/error. `scan` returns violations only; `enforce` additionally files dedupe-keyed bugs (decision_ref is `governance.<policy>.<object_kind>[.<rule_kind>]`). |
| `praxis_data_dictionary_impact` | `general` | `advanced` | - | `read` | - | Cross-axis impact analysis for a data-dictionary object. Walks lineage in the given direction, then for every reached node reports effective tags, stewards, quality rules, and latest run status. Returns aggregate rollups (PII field count, failing-rule count, distinct owners + publishers) across the blast radius. |
| `praxis_data_dictionary_lineage` | `general` | `advanced` | - | `read` | - | Directed lineage graph over data dictionary objects. Auto-projected from Postgres FK constraints, view dependencies, dataset_promotions, integration manifests, and MCP tool input schemas. Operator-authored edges take precedence. |
| `praxis_data_dictionary_quality` | `general` | `advanced` | - | `read` | - | Declarative data-quality rules + their runs. Auto-projected from Postgres schema (NOT NULL, UNIQUE, FK referential checks) with operator overrides. |
| `praxis_data_dictionary_stewardship` | `general` | `advanced` | - | `read` | - | Stewardship authority for data dictionary objects. Auto-projected from audit-column names, namespace prefix → service owner, and known projector modules. Operator stewards take precedence. |
| `praxis_data_dictionary_wiring_audit` | `general` | `advanced` | - | `read` | - | Wiring + hard-path audit over Praxis. Reports two classes of issue that bloat attention and/or break on VPS migration: (1) hardcoded paths / localhost / ports in source, docs, skills, MCP metadata, CLI surfaces, and queue specs, classified by authority status; (2) unwired authority rows — operator decisions nothing cites, and data-dictionary tables zero code references. No automatic bug filing; the output is a report the operator reviews. |
| `praxis_governance` | `governance` | `advanced` | - | `read` | - | Safety checks before launching a workflow. Scan prompts for leaked secrets (API keys, tokens, passwords) or verify that a set of file paths falls within allowed scope. |
| `praxis_heal` | `governance` | `advanced` | - | `read` | - | Diagnose why a workflow job failed and get a recommended recovery action: retry (transient error), escalate (needs human attention), skip (non-critical), or halt (stop the pipeline). |
| `praxis_cli_auth_doctor` | `integration` | `stable` | - | `read` | - | Diagnose CLI auth state for claude / codex / gemini in one call. Probes each binary with a trivial prompt, parses the output for auth-failure patterns ('Not logged in', '401', 'authentication error'…), and returns a structured per-provider report with concrete host-side remediation commands. |
| `praxis_integration` | `integration` | `advanced` | `workflow integration` | `launch`, `read`, `write` | - | Call, list, or describe registered integrations (API connectors, webhooks, and other external services). |
| `praxis_match_rules_backfill` | `integration` | `advanced` | - | `write` | - | Backfill provider_model_market_match_rules + provider_model_candidates.benchmark_profile for active candidates that lack an enabled rule for the configured benchmark source. |
| `praxis_provider_onboard` | `integration` | `advanced` | - | `read`, `write` | - | Onboard a CLI or API provider into Praxis Engine through one catalog-backed operation. Probes transport, discovers models, writes onboarding authority, and performs the canonical post-onboarding sync. |
| `praxis_graph` | `knowledge` | `advanced` | - | `read` | - | Explore connections from one knowledge-graph entity. Shows what an entity depends on, what depends on it, and the blast radius of changes. |
| `praxis_ingest` | `knowledge` | `advanced` | - | `write` | - | Store new information in the knowledge graph so it can be recalled later via praxis_recall. Content is automatically entity-extracted, deduplicated, and embedded for vector search. |
| `praxis_recall` | `knowledge` | `stable` | `workflow recall` | `read` | - | Search the platform's knowledge graph for information about modules, functions, decisions, patterns, bugs, constraints, people, or any previously ingested content. Returns ranked results with confidence scores and how each result was found (text match, graph traversal, or vector similarity). |
| `praxis_research` | `knowledge` | `stable` | - | `read` | - | Search the knowledge graph specifically for research findings and analysis results. Lighter-weight than praxis_recall — focused on retrieving prior research. |
| `praxis_search` | `knowledge` | `stable` | `workflow search` | `read` | - | Canonical federated search. Returns the data you'd otherwise reach for bash to fetch — line-context code matches, regex/exact/semantic modes, path-glob scoping, time bounds, freshness signal, source-tagged ranked results across code (today) and knowledge/bugs/receipts/git/files/db (rolling out). |
| `praxis_story` | `knowledge` | `advanced` | - | `read` | - | Compose a short narrative from one entity's graph neighborhood. Useful when you want the graph to explain itself in plain language instead of only returning edges. |
| `praxis_access_control` | `operations` | `advanced` | - | `read`, `write` | - | Mutate the control-panel model-access denial table — the first-class checkbox surface for turning a (provider × transport × job_type × model) tuple on or off. |
| `praxis_authority_domain_forge` | `operations` | `advanced` | `workflow authority-domain-forge` | `read` | - | Preview the authority-domain ownership path before creating a new authority boundary or attaching operations, tables, workflows, or tools to it. Returns existing domain state, nearby domains, attached operations, authority objects, missing inputs, reject paths, and the safe register payload. |
| `praxis_authority_memory_refresh` | `operations` | `advanced` | - | `write` | - | Project authority FK data into memory_edges so the knowledge graph reflects real structure. Upserts canonical-class edges for roadmap parent_of/dependencies, roadmap resolves_bug, operator_object_relations, workflow build intent links, bug and issue lineage, bug evidence links, workflow job/chain relationships, and operator decision scopes. Idempotent; safe to re-run. |
| `praxis_bug_replay_provenance_backfill` | `operations` | `advanced` | - | `write` | - | Backfill replay provenance from canonical bug and receipt authority. |
| `praxis_circuits` | `operations` | `stable` | `workflow circuits` | `read`, `write` | - | Inspect effective circuit-breaker state or apply a durable manual override for one provider. |
| `praxis_daily_heartbeat` | `operations` | `advanced` | `workflow heartbeat` | `write` | - | Run one daily-heartbeat probe cycle on demand and persist the results to heartbeat_runs + heartbeat_probe_snapshots through CQRS authority. Probes cover provider CLI usage (claude/codex/gemini latency + token counts), connector liveness (catalog health), credential expiry (keychain/env API keys + OAuth tokens), and MCP server liveness (stdio initialize handshake). |
| `praxis_dataset` | `operations` | `stable` | `workflow dataset` | `read`, `write` | - | Praxis dataset refinery: turn evidence-linked execution receipts into curated, lineage-preserving training and eval data for specialist SLMs (slm/review first). |
| `praxis_diagnose` | `operations` | `stable` | `workflow diagnose` | `read` | - | Diagnose one workflow run by id. Combines the receipt, failure classification, and provider health into a single operator-facing report. |
| `praxis_evolve_operation_field` | `operations` | `advanced` | `workflow evolve-operation-field` | `read` | - | Plan-only wizard for adding a new field to an existing CQRS operation's input model. |
| `praxis_execution_truth` | `operations` | `stable` | - | `read` | - | Read a composed execution-truth packet. Combines status snapshot, optional run views, and optional causal trace through gateway-dispatched child queries so green-looking state is checked against independent proof. |
| `praxis_firecheck` | `operations` | `stable` | `workflow firecheck` | `read` | - | Preflight whether workflow work can actually fire now. Returns can_fire, typed blockers, and remediation plans so submitted state is not mistaken for runtime proof. |
| `praxis_health` | `operations` | `stable` | `workflow health` | `read` | - | Full system health check — Postgres connectivity, disk space, operator panel state, workflow lane recommendations, context cache stats, memory graph health, and projection freshness (event-log cursors + process-cache refresh lag) with SLA alerts and a read-side circuit-breaker verdict. |
| `praxis_heartbeat` | `operations` | `advanced` | - | `read`, `write` | - | Run or check the knowledge graph maintenance cycle. The heartbeat syncs receipts, bugs, constraints, and friction events into the knowledge graph, mines relationships between entities, generates daily/weekly rollups, and archives stale nodes. |
| `praxis_metrics_reset` | `operations` | `advanced` | - | `write` | - | Reset observability metrics through explicit operator maintenance authority. |
| `praxis_model_access_control_matrix` | `operations` | `stable` | - | `read` | - | Read the live model-access ON/OFF switchboard that drives the private provider catalog. |
| `praxis_object_truth` | `operations` | `advanced` | `workflow object-truth` | `read` | - | Build deterministic object-truth evidence for one inline record. This is a thin read-only MCP wrapper over the gateway operation `object_truth_observe_record`; it normalizes identity, field observations, value digests, source metadata, hierarchy signals, and redaction-safe previews without deciding business truth. |
| `praxis_object_truth_compare_versions` | `operations` | `advanced` | `workflow object-truth-compare` | `read` | - | Compare two persisted object-truth object versions by digest. This is a thin read-only MCP wrapper over the gateway query `object_truth_compare_versions`; it compares field observations and freshness hints without deciding business truth. |
| `praxis_object_truth_record_comparison_run` | `operations` | `advanced` | `workflow object-truth-record-comparison` | `write` | - | Compare two persisted object-truth object versions and store the comparison output as durable evidence. This is a thin write MCP wrapper over the gateway command `object_truth_record_comparison_run`. |
| `praxis_object_truth_store` | `operations` | `advanced` | `workflow object-truth-store` | `write` | - | Build and persist deterministic object-truth evidence for one inline record. This is a thin write MCP wrapper over the gateway command `object_truth_store_observed_record`; it creates durable object-version and field-observation evidence, plus the command receipt/event. |
| `praxis_object_truth_store_schema_snapshot` | `operations` | `advanced` | `workflow object-truth-store-schema` | `write` | - | Normalize and persist deterministic schema-snapshot evidence for one external object. This is a thin write MCP wrapper over the gateway command `object_truth_store_schema_snapshot`. |
| `praxis_operation_forge` | `operations` | `advanced` | `workflow operation-forge` | `read` | - | Preview the canonical CQRS path for adding or evolving an operation. Produces the registration payload, real tool binding + API route when the operation already exists, and reject paths before anyone hand-builds catalog drift. |
| `praxis_orient` | `operations` | `curated` | `workflow orient` | `read` | - | Fresh-agent orientation: returns the canonical orient payload (standing orders, authority envelope, tool guidance, recent activity, endpoints, health). The single best first call for any LLM agent or operator waking up cold against Praxis. Delegates to the same authority that serves POST /orient so HTTP and MCP consumers see identical shape. |
| `praxis_provider_availability_refresh` | `operations` | `advanced` | - | `write` | - | Refresh provider availability through CQRS authority. |
| `praxis_provider_control_plane` | `operations` | `stable` | `workflow provider-control-plane` | `read` | - | Read the provider/job/model control-plane matrix through CQRS authority. |
| `praxis_provider_route_truth` | `operations` | `stable` | - | `read` | - | Read composed provider-route truth. Combines provider control plane and model access control matrix to answer whether a provider/model/job route is runnable, blocked, mixed, or unknown, with removal reasons. |
| `praxis_register_authority_domain` | `operations` | `advanced` | `workflow register-authority-domain` | `write` | - | Register or update an authority domain through a receipt-backed CQRS command before operations, tables, workflows, or MCP tools are attached to it. |
| `praxis_register_operation` | `operations` | `advanced` | `workflow register-operation` | `write` | - | Register a new CQRS operation in the catalog from CLI / MCP / HTTP without authoring a migration. Lands the data_dictionary_objects + authority_object_registry + operation_catalog_registry row triple atomically through register_operation_atomic. |
| `praxis_reload` | `operations` | `advanced` | - | `write` | - | Clear in-process caches and optionally importlib.reload runtime modules so DB, config, and code changes take effect without restarting the MCP subprocess. |
| `praxis_remediation_apply` | `operations` | `stable` | `workflow remediation-apply` | `write` | - | Apply guarded runtime remediation for a typed workflow failure. It can clean stale provider slot counters or expired host-resource leases, refuses human-gated repairs, and never retries workflow jobs. |
| `praxis_remediation_plan` | `operations` | `stable` | `workflow remediation-plan` | `read` | - | Return the safe remediation tier, evidence requirements, approval gate, and retry delta for a typed workflow failure. |
| `praxis_retire_operation` | `operations` | `advanced` | `workflow retire-operation` | `write` | - | Soft-retire a CQRS operation. Sets operation_catalog_registry.enabled to FALSE so the gateway stops binding it, and flips the matching authority_object_registry row's lifecycle_status to 'deprecated'. |
| `praxis_runtime_truth_snapshot` | `operations` | `stable` | `workflow runtime-truth` | `read` | - | Read actual workflow runtime truth across DB authority, queue state, worker heartbeats, provider slots, host-resource leases, Docker, manifest hydration audit, and recent typed failures. |
| `praxis_semantic_bridges_backfill` | `operations` | `advanced` | - | `write` | - | Replay semantic bridges from canonical operator authority into semantic assertions. |
| `praxis_semantic_projection_refresh` | `operations` | `advanced` | - | `write` | - | Refresh the semantic projection through explicit operator maintenance authority. |
| `praxis_status_snapshot` | `operations` | `advanced` | - | `read` | - | Read the canonical workflow status snapshot — pass rate, failure mix, queue depth, and in-flight run summaries from receipt authority. |
| `praxis_task_route_eligibility` | `operations` | `advanced` | `workflow task-route-eligibility` | `write` | - | Write one bounded task-route eligibility window for a provider or provider/model scope through CQRS authority. |
| `praxis_work_assignment_matrix` | `operations` | `stable` | - | `read` | - | Read the model-tier work assignment matrix through CQRS authority. |
| `tool_dag_health` | `operations` | `stable` | - | `read` | workflow health | Backwards-compatible alias for praxis_health. |
| `praxis_bug_triage_packet` | `operator` | `advanced` | - | `read` | - | Read a compact LLM-oriented packet that classifies bugs as live defects, evidence debt, stale projections, platform friction, fixed-pending-verification, or inactive without mutating bug authority. |
| `praxis_execution_proof` | `operator` | `advanced` | - | `read` | - | Prove whether a workflow run or trace anchor actually produced runtime execution evidence. Queued/running labels are treated as weak context, not proof; the result names the concrete evidence and missing proof. |
| `praxis_graph_projection` | `operator` | `advanced` | - | `read` | - | Read the cross-domain operator graph projection. |
| `praxis_issue_backlog` | `operator` | `advanced` | - | `read` | - | Read the canonical operator issue backlog. |
| `praxis_next` | `operator` | `stable` | - | `read` | - | Progressive-disclosure operator front door for deciding what to do next. Composes existing Praxis authority instead of exposing the raw tool pile: catalog metadata, manifests, workflow run state, queue state, provider slots, host-resource leases, verifier refs, and retry/launch doctrine. |
| `praxis_next_work` | `operator` | `stable` | - | `read` | - | Read a composed next-work packet. Combines refactor heatmap, bug triage, work assignment matrix, and runtime status into one ranked operator view with proof gates and validation paths. |
| `praxis_operator_architecture_policy` | `operator` | `advanced` | - | `write` | - | Record a durable architecture-policy decision in operator authority. |
| `praxis_operator_closeout` | `operator` | `advanced` | - | `read`, `write` | - | Preview or commit proof-backed bug and roadmap closeout through the shared reconciliation gate. |
| `praxis_operator_decisions` | `operator` | `advanced` | - | `read`, `write` | praxis_next | List or record canonical operator decisions through the shared operator_decisions table. |
| `praxis_operator_ideas` | `operator` | `advanced` | - | `read`, `write` | - | Record, resolve, promote, or list pre-commitment operator ideas. Ideas are upstream of roadmap commitment: they may be rejected, superseded, archived, or promoted into existing roadmap items, but roadmap itself does not gain a canceled state. |
| `praxis_operator_native_primary_cutover_gate` | `operator` | `advanced` | - | `write` | - | Admit a native primary cutover gate into operator-control decision and gate authority tables. |
| `praxis_operator_relations` | `operator` | `advanced` | - | `write` | - | Record canonical functional areas and cross-object semantic relations. |
| `praxis_operator_roadmap_view` | `operator` | `advanced` | - | `read` | - | Read one roadmap subtree and its dependency edges from DB-backed authority. |
| `praxis_operator_write` | `operator` | `advanced` | - | `read`, `write` | - | Preview, validate, commit, update, retire, or re-parent roadmap rows through the shared operator-write validation gate. |
| `praxis_refactor_heatmap` | `operator` | `stable` | - | `read` | - | Read the ranked refactor heatmap. Combines architecture-bug authority, source spread, surface coupling, and large-symbol pressure into one deterministic read model for choosing cleanup work. |
| `praxis_replay_ready_bugs` | `operator` | `advanced` | - | `read` | - | Read the replay-ready bug backlog from authoritative provenance. |
| `praxis_run` | `operator` | `advanced` | - | `read` | - | Consolidated run-scoped view. One tool replaces praxis_run_status, praxis_run_scoreboard, praxis_run_graph, praxis_run_lineage — pick the view via 'action' or 'view'. The old four remain as aliases for one window per the no-shims standing order. |
| `praxis_run_graph` | `operator` | `advanced` | - | `read` | workflow tools call praxis_run --input-json '{"run_id":"<run_id>","action":"graph"}' | DEPRECATED ALIAS — use praxis_run(action='graph'). Read one run-scoped workflow graph. |
| `praxis_run_lineage` | `operator` | `advanced` | - | `read` | workflow tools call praxis_run --input-json '{"run_id":"<run_id>","action":"lineage"}' | DEPRECATED ALIAS — use praxis_run(action='lineage'). Read one run-scoped lineage view. |
| `praxis_run_scoreboard` | `operator` | `advanced` | - | `read` | workflow tools call praxis_run --input-json '{"run_id":"<run_id>","action":"scoreboard"}' | DEPRECATED ALIAS — use praxis_run(action='scoreboard'). Read one run-scoped cutover scoreboard. |
| `praxis_run_status` | `operator` | `advanced` | - | `read` | workflow tools call praxis_run --input-json '{"run_id":"<run_id>","action":"status"}' | DEPRECATED ALIAS — use praxis_run(action='status'). Read one run-scoped operator status view. |
| `praxis_semantic_assertions` | `operator` | `advanced` | - | `read`, `write` | - | Register semantic predicates, record or retract semantic assertions, and query the canonical semantic substrate. |
| `praxis_trace` | `operator` | `advanced` | - | `read` | - | Walk the cause tree for any anchor (receipt_id, event_id, or correlation_id) and return the rooted DAG of receipts plus the events they emitted. Phase 1 of causal tracing — links receipts via cause_receipt_id and groups them by correlation_id. Returns orphan_count so callers can see when a trace is incomplete (e.g. when an async-spawned subtree did not propagate context). |
| `praxis_ui_experience_graph` | `operator` | `advanced` | - | `read` | - | Read the LLM-facing Praxis app experience graph: surfaces, controls, authority sources, relationships, and source-file anchors. |
| `praxis_decompose` | `planning` | `stable` | - | `read` | - | Break down a large objective into small, workflow-ready micro-sprints. Returns each sprint with estimated complexity, dependencies between sprints, and the critical path. |
| `praxis_intent_match` | `planning` | `stable` | - | `read` | - | Find existing UI components, workflows, and integrations that match what you want to build. Searches the registry and proposes how to compose them into an app. |
| `praxis_manifest_generate` | `planning` | `advanced` | - | `write` | - | Generate a complete app manifest (UI layout, data flow, integrations) from a natural language description. Combines intent matching with LLM generation to produce a ready-to-render manifest. |
| `praxis_manifest_refine` | `planning` | `advanced` | - | `write` | - | Iterate on a previously generated app manifest. Apply user feedback to adjust layout, add/remove modules, change data sources, or modify behavior. |
| `praxis_session` | `planning` | `advanced` | - | `read` | - | View or validate session carry-forward packs — compressed context snapshots that help new sessions pick up where previous ones left off. |
| `praxis_query` | `query` | `stable` | `workflow query` | `read` | - | Ask any question about the system in plain English. This is the best starting point when you're unsure which tool to use — it automatically routes your question to the right subsystem. Think of it as a router, not as the deep authority for every domain. |
| `praxis_research_workflow` | `research` | `advanced` | - | `launch`, `read` | - | Run a parallel multi-angle research workflow on any topic. One call generates a workflow spec (seed decomposition, N parallel research workers via replicate, synthesis) and launches it through the service bus. |
| `praxis_context_shard` | `session` | `session` | - | `session` | - | Return the bounded execution shard for the current workflow MCP session. This is only valid inside workflow Docker jobs using the signed MCP bridge. |
| `praxis_session_context` | `session` | `session` | - | `session` | - | Read or write persistent context on your agent session. Context survives across tool calls and is available on retry. |
| `praxis_subscribe_events` | `session` | `session` | - | `session` | - | Pull build state events since the agent's last cursor position. Returns new events and advances the cursor. Call repeatedly to stay in sync with platform state changes. |
| `praxis_credential_capture` | `setup` | `stable` | - | `read`, `write` | - | Request, inspect, or open the host-side secure API-key entry window for macOS Keychain-backed Praxis credentials. This is a thin MCP wrapper over the CQRS operation `credential_capture_keychain`; raw secret values never enter MCP params or tool results. |
| `praxis_setup` | `setup` | `core` | - | `read` | - | Runtime-target setup authority for Praxis. Reports the active runtime_target_ref, substrate kind, API authority, DB authority, native_instance contract, workspace authority, provider-family thin sandbox image contract, and the empty_thin_sandbox_default pass/fail. USE WHEN: moving Praxis between machines, adopting an existing runtime, repointing the package at a DB, or checking that the CLI, MCP, and API are bound to the same repo-local instance. Operations belong to API/MCP; CLI and website are clients. SSH is build/deploy transport only. |
| `praxis_get_submission` | `submissions` | `session` | - | `session` | - | Read a sealed workflow submission within the current workflow MCP session. The session token owns run_id/workflow_id and the tool only accepts submission_id or job_label for the target submission. |
| `praxis_review_submission` | `submissions` | `session` | - | `session` | - | Review a sealed workflow submission within the current workflow MCP session. The session token owns run_id/workflow_id/job_label for the reviewer. The tool only accepts submission_id or job_label for the target submission. |
| `praxis_submit_artifact_bundle` | `submissions` | `session` | - | `session` | - | Submit a sealed artifact bundle result for the current workflow MCP session. The session token owns run_id, workflow_id, and job_label. This tool never accepts those ids as input and returns structured errors instead of stack traces. |
| `praxis_submit_code_change` | `submissions` | `session` | - | `session` | - | Submit a sealed code-change result for the current workflow MCP session. The session token owns run_id, workflow_id, and job_label. This tool never accepts those ids as input and returns structured errors instead of stack traces. |
| `praxis_submit_research_result` | `submissions` | `session` | - | `session` | - | Submit a sealed research result for the current workflow MCP session. The session token owns run_id, workflow_id, and job_label. This tool never accepts those ids as input and returns structured errors instead of stack traces. |
| `praxis_approve_proposed_plan` | `workflow` | `stable` | `workflow approve-plan` | `read` | - | Approve a ProposedPlan so launch_approved can submit it. Takes the ProposedPlan payload from praxis_launch_plan(preview_only=true), wraps it with approved_by + timestamp + hash, and returns an ApprovedPlan. The hash binds the approval to the exact spec_dict — tampering between approve and launch fails closed at launch time. The ProposedPlan must already carry machine-checkable provider freshness evidence. |
| `praxis_bind_data_pills` | `workflow` | `stable` | `workflow bind-pills` | `read` | - | Layer 1 (Bind) of the planning stack: extract and validate ``object.field`` data-pill references from prose intent against the data dictionary authority. Deterministic — matches explicit ``snake_case.field_path`` spans in the prose; does not infer loose references like "the user's name." Returns bound / ambiguous / unbound splits the caller confirms before decomposing intent into packets. |
| `praxis_compose_and_launch` | `workflow` | `stable` | `workflow ship-intent` | `launch` | - | End-to-end: prose intent → compose → approve → launch in one call. Compose the ProposedPlan through Layers 2 → 1 → 5, wrap with an explicit approval record (approved_by + hash), and submit through the CQRS control-command bus. |
| `praxis_compose_experiment` | `workflow` | `advanced` | - | `launch` | - | Parallel matrix runner: fire N compose_plan_via_llm calls side-by-side, each with a different LLM knob configuration. Returns a ranked report (success-first, wall-time-asc). Each child run produces its own compose-plan-via-llm receipt + plan.composed event; the matrix run produces a parent receipt + a compose.experiment.completed event. |
| `praxis_compose_plan` | `workflow` | `stable` | `workflow compose-plan` | `read` | - | Chain Layer 2 (decompose) → Layer 1 (bind) → Layer 5 (translate + preview) in one call. Takes prose intent with explicit step markers, returns a ProposedPlan ready for approval and launch. |
| `praxis_compose_plan_via_llm` | `workflow` | `advanced` | - | `launch` | - | End-to-end LLM plan composition: atoms → skeleton → ONE synthesis LLM call (few-sentence plan statement) → N parallel fork-out author calls (each shares the synthesis as cached prefix) → validate. |
| `praxis_connector` | `workflow` | `advanced` | - | `launch`, `read`, `write` | - | Build API connectors for third-party applications. One call stamps a workflow spec and launches a 4-job pipeline (discover API → map objects → build client → review). |
| `praxis_decompose_intent` | `workflow` | `stable` | `workflow decompose` | `read` | - | Layer 2 (Decompose) of the planning stack: split prose intent into ordered steps by parsing explicit step markers (numbered lists, bulleted lists, or first/then/finally ordering). Deterministic — does NOT do free-prose semantic decomposition. |
| `praxis_generate_plan` | `workflow` | `stable` | `workflow generate-plan` | `read`, `write` | - | Shared CQRS plan-generation front door. action='generate_plan' recognizes messy prose, matches spans to authority, returns suggestions and gaps, and does not mutate state. action='materialize_plan' creates or updates a draft workflow through the canonical workflow build mutation. |
| `praxis_launch_plan` | `workflow` | `stable` | `workflow launch-plan` | `write` | - | Translate a packet list into a workflow spec and submit it — or preview first. This is the layer-5 translation primitive, not a planner. Caller (user or LLM) owns upstream planning: (1) extract data pills from intent, (2) decompose prose into steps, (3) reorder by data-flow, (4) author per-step prompts. This tool translates the already-planned packet list through the capability catalog and submits through the CQRS bus. |
| `praxis_plan_lifecycle` | `workflow` | `stable` | `workflow plan-history` | `read` | - | Q-side of the planning stack: read every plan.* authority_event for one workflow_id in order. Pair with gateway-backed praxis_compose_plan / praxis_launch_plan on the C side. |
| `praxis_promote_experiment_winner` | `workflow` | `advanced` | - | `write` | - | Promote one compose-experiment leg into the canonical task_type_routing row for that task type. The winning leg's temperature and max_tokens are applied; provider/model changes remain visible only in the returned diff. |
| `praxis_suggest_plan_atoms` | `workflow` | `stable` | `workflow suggest-atoms` | `read` | - | Layer 0 (Suggest): free prose → pills + step types + parameters. Deterministic; no LLM call; no order or count produced. |
| `praxis_synthesize_skeleton` | `workflow` | `advanced` | - | `read` | - | Layer 0.5 (Synthesize): atoms + skeleton with deterministic depends_on, consumes/produces/capabilities floors, scaffolded gates from data dictionary. |
| `praxis_wave` | `workflow` | `advanced` | - | `launch`, `read`, `write` | - | Manage execution waves — groups of jobs with dependency ordering. Waves track which jobs are runnable (all dependencies met) and which are blocked. |
| `praxis_workflow` | `workflow` | `advanced` | - | `launch`, `read`, `write` | - | Execute work by launching a workflow for LLM agents. This is the primary way to run tasks — building code, running tests, writing reviews, refactoring, and debates. |
| `praxis_workflow_validate` | `workflow` | `advanced` | - | `read` | - | Dry-run a workflow spec to check for errors before executing it. Returns whether the spec is valid, how many jobs it contains, and which agents each job resolves to. |

## Tool Reference

### Code

#### `praxis_discover`

- Surface: `code`
- Tier: `stable`
- Badges: `stable`, `code`, `alias:discover`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow discover`
- CLI schema help: `workflow tools describe praxis_discover`
- When to use: Search for existing code by behavior with hybrid retrieval before building something new.
- When not to use: Do not use it for architectural decisions or receipt analytics.
- Recommended alias: `workflow discover`
- Selector: `action`; default `search`; values `search`, `reindex`, `stats`, `stale_check`
- Required args: (none)

Example input:

```json
{
  "action": "search",
  "query": "retry logic with exponential backoff"
}
```

### Data

#### `praxis_data`

- Surface: `data`
- Tier: `stable`
- Badges: `stable`, `data`, `alias:data`, `mutates-state`, `launches-work`
- Risks: `launch`, `read`, `write`
- CLI entrypoint: `workflow data`
- CLI schema help: `workflow tools describe praxis_data`
- When to use: Run deterministic parsing, normalization, validation, mapping, dedupe, or reconcile jobs and optionally launch them through the workflow engine.
- When not to use: Do not use it for fuzzy inference, free-form classification, or cases where an LLM must invent the transform logic.
- Recommended alias: `workflow data`
- Selector: `action`; default `profile`; values `parse`, `profile`, `filter`, `sort`, `normalize`, `repair`, `repair_loop`, `backfill`, `redact`, `checkpoint`, `replay`, `approve`, `apply`, `validate`, `transform`, `join`, `merge`, `aggregate`, `split`, `export`, `dead_letter`, `dedupe`, `reconcile`, `sync`, `run`, `workflow_spec`, `launch`
- Required args: (none)

Example input:

```json
{
  "action": "profile",
  "input_path": "artifacts/data/users.csv"
}
```

### Evidence

#### `praxis_artifacts`

- Surface: `evidence`
- Tier: `stable`
- Badges: `stable`, `evidence`, `alias:artifacts`
- Risks: `read`
- CLI entrypoint: `workflow artifacts`
- CLI schema help: `workflow tools describe praxis_artifacts`
- When to use: Browse sandbox outputs, search artifact paths, or compare generated files.
- When not to use: Do not use it for workflow receipt history or knowledge-graph recall.
- Recommended alias: `workflow artifacts`
- Selector: `action`; default `stats`; values `stats`, `list`, `search`, `diff`
- Required args: (none)

Example input:

```json
{
  "action": "list",
  "sandbox_id": "sandbox_20260423_001"
}
```

#### `praxis_bugs`

- Surface: `evidence`
- Tier: `stable`
- Badges: `stable`, `evidence`, `alias:bugs`, `mutates-state`, `launches-work`
- Risks: `launch`, `read`, `write`
- CLI entrypoint: `workflow bugs`
- CLI schema help: `workflow tools describe praxis_bugs`
- When to use: Inspect the bug tracker, run keyword or hybrid search, file a new bug, or drive replay-ready bug workflows.
- When not to use: Do not use it for general system status or semantic knowledge search.
- Recommended alias: `workflow bugs`
- Selector: `action`; default `list`; values `list`, `file`, `search`, `duplicate_check`, `stats`, `show`, `packet`, `history`, `replay`, `backfill_replay`, `attach_evidence`, `patch_resume`, `resolve`
- Required args: (none)

Example input:

```json
{
  "action": "list",
  "status": "OPEN",
  "severity": "P1"
}
```

#### `praxis_constraints`

- Surface: `evidence`
- Tier: `advanced`
- Badges: `advanced`, `evidence`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_constraints`
- CLI schema help: `workflow tools describe praxis_constraints`
- When to use: Inspect mined constraints and scope-specific guardrails.
- When not to use: Do not use it for code similarity or bug enumeration.
- Selector: `action`; default `list`; values `list`, `for_scope`
- Required args: (none)

Example input:

```json
{
  "action": "list"
}
```

#### `praxis_friction`

- Surface: `evidence`
- Tier: `advanced`
- Badges: `advanced`, `evidence`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_friction`
- CLI schema help: `workflow tools describe praxis_friction`
- When to use: Inspect friction and guardrail events that are slowing workflows down.
- When not to use: Do not use it for health probes or general bug search.
- Selector: `action`; default `stats`; values `stats`, `list`, `patterns`
- Required args: (none)

Example input:

```json
{
  "action": "stats"
}
```

#### `praxis_patterns`

- Surface: `evidence`
- Tier: `stable`
- Badges: `stable`, `evidence`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_patterns`
- CLI schema help: `workflow tools describe praxis_patterns`
- When to use: Inspect or materialize recurring platform failure patterns before opening more bug tickets.
- When not to use: Do not use it for one-off defects that already have a concrete fix path; use praxis_bugs instead.
- Selector: `action`; default `list`; values `list`, `candidates`, `evidence`, `materialize`
- Required args: (none)

Example input:

```json
{
  "action": "list"
}
```

#### `praxis_receipts`

- Surface: `evidence`
- Tier: `advanced`
- Badges: `advanced`, `evidence`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_receipts`
- CLI schema help: `workflow tools describe praxis_receipts`
- When to use: Search workflow receipts or inspect token burn and execution evidence.
- When not to use: Do not use it for current health or knowledge-graph recall.
- Selector: `action`; default `search`; values `search`, `token_burn`
- Required args: (none)

Example input:

```json
{
  "action": "search",
  "query": "sandbox timeout"
}
```

### General

#### `praxis_audit_primitive`

- Surface: `general`
- Tier: `advanced`
- Badges: `advanced`, `general`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_audit_primitive`
- CLI schema help: `workflow tools describe praxis_audit_primitive`
- When to use: An audit-remediation job; a scheduled cleanup heartbeat; operator wants to know 'what can be fixed right now with zero risk?'. Always start with `playbook` + `plan` before any `apply`.
- When not to use: Don't use for one-off fact-finding on a specific finding — that's what the individual audit tools (praxis_data_dictionary_wiring_audit, etc.) are for. Don't use for code-edit fixes — the primitive doesn't touch source files.
- Selector: `action`; default `playbook`; values `playbook`, `registered`, `plan`, `apply`, `contracts`, `execute_contract`, `execute_all_contracts`
- Required args: (none)

Example input:

```json
{
  "action": "playbook"
}
```

#### `praxis_data_dictionary`

- Surface: `general`
- Tier: `advanced`
- Badges: `advanced`, `general`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_data_dictionary`
- CLI schema help: `workflow tools describe praxis_data_dictionary`
- When to use: Browse or edit field descriptors for any injected object kind.
- When not to use: Don't use for per-column SQL schema checks — those are covered by praxis_query 'schema for <table>'.
- Selector: `action`; default `list`; values `list`, `describe`, `set_override`, `clear_override`, `reproject`
- Required args: (none)

Example input:

```json
{
  "action": "list"
}
```

#### `praxis_data_dictionary_classifications`

- Surface: `general`
- Tier: `advanced`
- Badges: `advanced`, `general`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_data_dictionary_classifications`
- CLI schema help: `workflow tools describe praxis_data_dictionary_classifications`
- When to use: Identify which fields carry PII / credentials / ownership labels, or override heuristic tags with operator authority.
- When not to use: Not a field descriptor browser — use praxis_data_dictionary for field-level reads and operator overrides.
- Selector: `action`; default `summary`; values `summary`, `describe`, `by_tag`, `tags`, `set`, `clear`, `reproject`
- Required args: (none)

Example input:

```json
{
  "action": "summary"
}
```

#### `praxis_data_dictionary_drift`

- Surface: `general`
- Tier: `advanced`
- Badges: `advanced`, `general`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_data_dictionary_drift`
- CLI schema help: `workflow tools describe praxis_data_dictionary_drift`
- When to use: Before / after a migration; investigating whether a field deletion broke downstream consumers; auditing schema-change cadence.
- When not to use: Don't use to inspect current schema (use praxis_data_dictionary). Don't use to find the blast radius of a *single* known object — use praxis_data_dictionary_impact instead.
- Selector: `action`; default `latest`; values `latest`, `snapshot`, `history`, `diff`
- Required args: (none)

Example input:

```json
{
  "action": "latest"
}
```

#### `praxis_data_dictionary_governance`

- Surface: `general`
- Tier: `advanced`
- Badges: `advanced`, `general`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_data_dictionary_governance`
- CLI schema help: `workflow tools describe praxis_data_dictionary_governance`
- When to use: Governance review: before a release, or when investigating a data-governance complaint, run a dry scan to see which PII/sensitive objects lack owners and which error-severity rules are failing.
- When not to use: Don't use as a substitute for the data-dictionary write tools (set_operator_classification / set_operator_steward) — this only reports, it does not fix the underlying governance gaps.
- Selector: `action`; default `scan`; values `scan`, `enforce`, `scorecard`, `remediate`, `cluster`, `scans`, `scan_detail`, `scans_for_bug`, `pending`, `drain`
- Required args: (none)

Example input:

```json
{
  "action": "scan"
}
```

#### `praxis_data_dictionary_impact`

- Surface: `general`
- Tier: `advanced`
- Badges: `advanced`, `general`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_data_dictionary_impact`
- CLI schema help: `workflow tools describe praxis_data_dictionary_impact`
- When to use: Governance / change-safety review: surface who owns what, which nodes carry PII, which quality rules are currently failing, before making a schema change.
- When not to use: Don't use for simple field-level reads — praxis_data_dictionary is faster. Don't use for pure lineage walks — the existing data-dictionary lineage tool returns just the graph.
- Selector: none
- Required args: `object_kind`

Example input:

```json
{
  "object_kind": "table:workflow_runs",
  "direction": "downstream",
  "max_depth": 3
}
```

#### `praxis_data_dictionary_lineage`

- Surface: `general`
- Tier: `advanced`
- Badges: `advanced`, `general`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_data_dictionary_lineage`
- CLI schema help: `workflow tools describe praxis_data_dictionary_lineage`
- When to use: Trace which objects depend on or derive from a given object_kind.
- When not to use: Not a field-level descriptor browser — use praxis_data_dictionary for field-level reads and operator overrides.
- Selector: `action`; default `summary`; values `summary`, `describe`, `impact`, `set_edge`, `clear_edge`, `reproject`
- Required args: (none)

Example input:

```json
{
  "action": "summary"
}
```

#### `praxis_data_dictionary_quality`

- Surface: `general`
- Tier: `advanced`
- Badges: `advanced`, `general`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_data_dictionary_quality`
- CLI schema help: `workflow tools describe praxis_data_dictionary_quality`
- When to use: Add a declarative check to a field and track pass / fail over time.
- When not to use: Not a generic SQL runner — use praxis_query for ad-hoc data inspection.
- Selector: `action`; default `summary`; values `summary`, `list_rules`, `list_runs`, `run_history`, `set`, `clear`, `evaluate`, `reproject`
- Required args: (none)

Example input:

```json
{
  "action": "summary"
}
```

#### `praxis_data_dictionary_stewardship`

- Surface: `general`
- Tier: `advanced`
- Badges: `advanced`, `general`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_data_dictionary_stewardship`
- CLI schema help: `workflow tools describe praxis_data_dictionary_stewardship`
- When to use: Identify owners / approvers / publishers for data assets, or override heuristic stewardship with operator authority.
- When not to use: Not for assigning work — use the bugs and roadmap tools for that. Stewardship is a labeling authority for data governance.
- Selector: `action`; default `summary`; values `summary`, `describe`, `by_steward`, `set`, `clear`, `reproject`
- Required args: (none)

Example input:

```json
{
  "action": "summary"
}
```

#### `praxis_data_dictionary_wiring_audit`

- Surface: `general`
- Tier: `advanced`
- Badges: `advanced`, `general`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_data_dictionary_wiring_audit`
- CLI schema help: `workflow tools describe praxis_data_dictionary_wiring_audit`
- When to use: Before VPS migration, or any time the platform feels noisy — the report separates 'attention debt' (unwired authority) from 'deployment debt' (hardcoded paths).
- When not to use: Don't use to fix things — this is read-only lint. For fixes, the findings point you at file:line locations to edit or authority rows to retire.
- Selector: `action`; default `all`; values `all`, `hard_paths`, `decisions`, `orphans`, `trend`
- Required args: (none)

Example input:

```json
{
  "action": "all"
}
```

### Governance

#### `praxis_governance`

- Surface: `governance`
- Tier: `advanced`
- Badges: `advanced`, `governance`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_governance`
- CLI schema help: `workflow tools describe praxis_governance`
- When to use: Scan prompts and scope for policy, secret, or governance violations.
- When not to use: Do not use it as a general quality dashboard or health probe.
- Selector: `action`; default `scan_prompt`; values `scan_prompt`, `scan_scope`
- Required args: (none)

Example input:

```json
{
  "action": "scan_prompt",
  "text": "Ship the API key in the test fixture"
}
```

#### `praxis_heal`

- Surface: `governance`
- Tier: `advanced`
- Badges: `advanced`, `governance`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_heal`
- CLI schema help: `workflow tools describe praxis_heal`
- When to use: Diagnose failures and propose healing actions with platform-specific guidance.
- When not to use: Do not use it as a generic health command or workflow launcher.
- Selector: none
- Required args: `job_label`

Example input:

```json
{
  "job_label": "build",
  "failure_code": "sandbox.timeout",
  "stderr": "command timed out"
}
```

### Integration

#### `praxis_cli_auth_doctor`

- Surface: `integration`
- Tier: `stable`
- Badges: `stable`, `integration`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_cli_auth_doctor`
- CLI schema help: `workflow tools describe praxis_cli_auth_doctor`
- When to use: Diagnose CLI auth state for claude / codex / gemini in one call when a workflow run reported `Not logged in` / 401 / authentication errors, OR proactively before launching CLI-lane work.
- When not to use: Do not use for general workflow status (use praxis_workflow action='status') or for provider catalog truth (use praxis_provider_control_plane).
- Selector: none
- Required args: (none)

Example input:

```json
{}
```

#### `praxis_integration`

- Surface: `integration`
- Tier: `advanced`
- Badges: `advanced`, `integration`, `alias:integration`, `mutates-state`, `launches-work`
- Risks: `launch`, `read`, `write`
- CLI entrypoint: `workflow integration`
- CLI schema help: `workflow tools describe praxis_integration`
- When to use: List integrations, inspect one, validate credentials, or invoke an integration action.
- When not to use: Do not use it to build connectors or launch workflows.
- Recommended alias: `workflow integration`
- Selector: `action`; default `list`; values `call`, `list`, `describe`, `test_credentials`, `health`, `create`, `set_secret`, `reload`
- Required args: (none)

Example input:

```json
{
  "action": "list"
}
```

#### `praxis_match_rules_backfill`

- Surface: `integration`
- Tier: `advanced`
- Badges: `advanced`, `integration`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_match_rules_backfill`
- CLI schema help: `workflow tools describe praxis_match_rules_backfill`
- When to use: Backfill benchmark rules for newly added providers or candidates when selection is falling back to capability tags and priority.
- When not to use: Do not use it for ordinary model selection, provider onboarding smoke tests, or read-only route inspection.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "source_slug": "artificial_analysis",
  "dry_run": true
}
```

#### `praxis_provider_onboard`

- Surface: `integration`
- Tier: `advanced`
- Badges: `advanced`, `integration`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_provider_onboard`
- CLI schema help: `workflow tools describe praxis_provider_onboard`
- When to use: Probe or onboard a new provider/model route into the platform.
- When not to use: Do not use it for ordinary model selection or workflow launch.
- Selector: `action`; default `probe`; values `probe`, `onboard`
- Required args: `provider_slug`

Example input:

```json
{
  "action": "probe",
  "provider_slug": "openrouter",
  "transport": "api"
}
```

### Knowledge

#### `praxis_graph`

- Surface: `knowledge`
- Tier: `advanced`
- Badges: `advanced`, `knowledge`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_graph`
- CLI schema help: `workflow tools describe praxis_graph`
- When to use: Inspect blast radius and graph neighbors for a known knowledge-graph entity.
- When not to use: Do not use it for broad knowledge search; use recall first when you need ranked candidates.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "entity_id": "module:task_assembler",
  "depth": 1
}
```

#### `praxis_ingest`

- Surface: `knowledge`
- Tier: `advanced`
- Badges: `advanced`, `knowledge`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_ingest`
- CLI schema help: `workflow tools describe praxis_ingest`
- When to use: Persist new documents, build events, or research into the knowledge graph.
- When not to use: Do not use it for ad hoc questions where nothing should be persisted.
- Selector: none
- Required args: `kind`, `content`, `source`

Example input:

```json
{
  "kind": "document",
  "source": "catalog/runtime",
  "content": "# Runtime catalog"
}
```

#### `praxis_recall`

- Surface: `knowledge`
- Tier: `stable`
- Badges: `stable`, `knowledge`, `alias:recall`
- Risks: `read`
- CLI entrypoint: `workflow recall`
- CLI schema help: `workflow tools describe praxis_recall`
- When to use: Search the knowledge graph for decisions, patterns, entities, and prior analysis using ranked text, graph, and vector retrieval.
- When not to use: Do not use it for code similarity or workflow receipt queries.
- Recommended alias: `workflow recall`
- Selector: none
- Required args: `query`

Example input:

```json
{
  "query": "provider routing",
  "entity_type": "decision"
}
```

#### `praxis_research`

- Surface: `knowledge`
- Tier: `stable`
- Badges: `stable`, `knowledge`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_research`
- CLI schema help: `workflow tools describe praxis_research`
- When to use: Search prior research findings and analysis results with a lighter-weight surface than recall.
- When not to use: Do not use it for general knowledge or code search.
- Selector: `action`; default `search`; values `search`
- Required args: `query`

Example input:

```json
{
  "action": "search",
  "query": "provider routing performance"
}
```

#### `praxis_search`

- Surface: `knowledge`
- Tier: `stable`
- Badges: `stable`, `knowledge`, `alias:search`
- Risks: `read`
- CLI entrypoint: `workflow search`
- CLI schema help: `workflow tools describe praxis_search`
- When to use: Federated search across code, decisions, knowledge, bugs, receipts, and related sources with semantic, exact, or regex modes — prefer this as the default discovery entry point.
- When not to use: Do not use it for writes, workflow launches, or mutating operator state — use the subsystem-specific tools those actions require.
- Recommended alias: `workflow search`
- Selector: none
- Required args: `query`

Example input:

```json
{
  "query": "retry logic with exponential backoff",
  "sources": [
    "code"
  ],
  "scope": {
    "paths": [
      "Code&DBs/Workflow/runtime/**/*.py"
    ]
  }
}
```

#### `praxis_story`

- Surface: `knowledge`
- Tier: `advanced`
- Badges: `advanced`, `knowledge`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_story`
- CLI schema help: `workflow tools describe praxis_story`
- When to use: Compose a short narrative from one entity's graph neighborhood when plain edges are too flat.
- When not to use: Do not use it for ranked search or blast-radius inspection; use recall or graph first.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "entity_id": "module:task_assembler",
  "max_lines": 4
}
```

### Operations

#### `praxis_access_control`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_access_control`
- CLI schema help: `workflow tools describe praxis_access_control`
- When to use: List, disable, or enable model-access denial rows for a (provider × transport × job_type × model) selector without a migration.
- When not to use: Do not use it for provider smoke tests or onboarding — use praxis_provider_onboard. Do not use it when you only need search or receipts.
- Selector: `action`; default `list`; values `list`, `disable`, `enable`
- Required args: (none)

Example input:

```json
{
  "action": "list",
  "provider_slug": "openai",
  "transport_type": "CLI"
}
```

#### `praxis_authority_domain_forge`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:authority-domain-forge`
- Risks: `read`
- CLI entrypoint: `workflow authority-domain-forge`
- CLI schema help: `workflow tools describe praxis_authority_domain_forge`
- When to use: Preview authority-domain ownership before creating a new authority boundary or attaching operations, tables, workflows, or MCP tools to it. Use this before register-operation when the owning authority is not already explicit.
- When not to use: Do not use it as a mutation surface; it only prepares the canonical authority-domain payload. Use praxis_register_authority_domain to write.
- Recommended alias: `workflow authority-domain-forge`
- Selector: none
- Required args: `authority_domain_ref`

Example input:

```json
{
  "authority_domain_ref": "authority.object_truth",
  "decision_ref": "operator_decision.architecture_policy.product_architecture.object_truth_requires_deterministic_parse_compare_substrate"
}
```

#### `praxis_authority_memory_refresh`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_authority_memory_refresh`
- CLI schema help: `workflow tools describe praxis_authority_memory_refresh`
- When to use: Refresh the authority-to-memory projection after bulk authority writes so discover and recall see current structure.
- When not to use: Do not use it for reading the graph; use praxis_discover or praxis_recall.
- Selector: none
- Required args: (none)

Example input:

```json
{}
```

#### `praxis_bug_replay_provenance_backfill`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_bug_replay_provenance_backfill`
- CLI schema help: `workflow tools describe praxis_bug_replay_provenance_backfill`
- When to use: Backfill replay provenance without bundling unrelated maintenance actions into one selector tool.
- When not to use: Do not use it for read-only bug backlog inspection.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "open_only": true
}
```

#### `praxis_circuits`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `alias:circuits`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow circuits`
- CLI schema help: `workflow tools describe praxis_circuits`
- When to use: Inspect effective circuit-breaker state or apply a durable manual override for one provider.
- When not to use: Do not use it for task-route eligibility windows or generic health checks.
- Recommended alias: `workflow circuits`
- Selector: `action`; default `list`; values `list`, `history`, `open`, `close`, `reset`
- Required args: (none)

Example input:

```json
{
  "action": "list"
}
```

#### `praxis_daily_heartbeat`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:heartbeat`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow heartbeat`
- CLI schema help: `workflow tools describe praxis_daily_heartbeat`
- When to use: Run the daily external-health probe across providers, connectors, credentials, and MCP servers.
- When not to use: Do not use it for knowledge-graph maintenance; use praxis_heartbeat for that cycle.
- Recommended alias: `workflow heartbeat`
- Selector: none
- Required args: (none)

Example input:

```json
{
  "scope": "all"
}
```

#### `praxis_dataset`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `alias:dataset`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow dataset`
- CLI schema help: `workflow tools describe praxis_dataset`
- When to use: Curate, score, and promote evidence-linked training/eval data per specialist; export reproducible JSONL with manifest hashes.
- When not to use: Do not use for raw SQL or for writing receipts/decisions directly — those have their own surfaces.
- Recommended alias: `workflow dataset`
- Selector: `action`; default `summary`; values `summary`, `candidates_scan`, `candidates_list`, `candidate_inspect`, `candidate_promote`, `candidate_reject`, `inbox`, `preference_suggest`, `preference_create`, `eval_add`, `promotion_supersede`, `promotions_list`, `policy_list`, `policy_show`, `policy_record`, `lineage`, `manifests_list`, `export`, `stale_reconcile`, `projection_refresh`
- Required args: (none)

Example input:

```json
{
  "action": "candidates_list",
  "candidate_kind": "review",
  "eligibility": "sft_eligible",
  "limit": 10
}
```

#### `praxis_diagnose`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `alias:diagnose`
- Risks: `read`
- CLI entrypoint: `workflow diagnose`
- CLI schema help: `workflow tools describe praxis_diagnose`
- When to use: Diagnose one workflow run by id and combine receipt, failure, and provider health context.
- When not to use: Do not use it for broad health checks or generic receipt search.
- Recommended alias: `workflow diagnose`
- Selector: none
- Required args: `run_id`

Example input:

```json
{
  "run_id": "run_abc123"
}
```

#### `praxis_evolve_operation_field`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:evolve-operation-field`
- Risks: `read`
- CLI entrypoint: `workflow evolve-operation-field`
- CLI schema help: `workflow tools describe praxis_evolve_operation_field`
- When to use: Plan how to add one optional field to an existing CQRS operation's input model (checklist of files and edits). v1 is plan-only — you still apply diffs locally.
- When not to use: Do not use it to register a brand-new operation — use praxis_register_operation. Do not expect the tool to write migrations or apply patches automatically.
- Recommended alias: `workflow evolve-operation-field`
- Selector: none
- Required args: `operation_name`, `field_name`

Example input:

```json
{
  "operation_name": "operator.architecture_policy_record",
  "field_name": "decision_provenance",
  "field_type_annotation": "str | None",
  "field_default_repr": "None",
  "field_description": "explicit | inferred provenance",
  "db_table": "operator_decisions",
  "db_column": "decision_provenance"
}
```

#### `praxis_execution_truth`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_execution_truth`
- CLI schema help: `workflow tools describe praxis_execution_truth`
- When to use: Check whether workflow work is actually firing by combining status, run views, and causal trace evidence.
- When not to use: Do not use it to launch, retry, or mutate workflow state.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "since_hours": 24
}
```

#### `praxis_firecheck`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `alias:firecheck`
- Risks: `read`
- CLI entrypoint: `workflow firecheck`
- CLI schema help: `workflow tools describe praxis_firecheck`
- When to use: Run before launching or retrying workflows to prove work can actually fire, including typed blockers and remediation plans.
- When not to use: Do not use it as a retry command; it is the proof gate before retry.
- Recommended alias: `workflow firecheck`
- Selector: none
- Required args: (none)

Example input:

```json
{}
```

#### `praxis_health`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `alias:health`
- Risks: `read`
- CLI entrypoint: `workflow health`
- CLI schema help: `workflow tools describe praxis_health`
- When to use: Run a full preflight before workflow launch or when the platform feels degraded.
- When not to use: Do not use it to inspect one specific workflow run.
- Recommended alias: `workflow health`
- Selector: none
- Required args: (none)

Example input:

```json
{}
```

#### `praxis_heartbeat`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_heartbeat`
- CLI schema help: `workflow tools describe praxis_heartbeat`
- When to use: Run or inspect the knowledge-graph maintenance cycle that syncs receipts, bugs, constraints, and memory projections.
- When not to use: Do not use it for external provider or connector probes; use praxis_daily_heartbeat for that.
- Selector: `action`; default `status`; values `run`, `status`
- Required args: (none)

Example input:

```json
{
  "action": "status"
}
```

#### `praxis_metrics_reset`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_metrics_reset`
- CLI schema help: `workflow tools describe praxis_metrics_reset`
- When to use: Reset polluted quality metrics or routing counters through one explicit maintenance operation.
- When not to use: Do not use it for ordinary observability reads.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "confirm": true
}
```

#### `praxis_model_access_control_matrix`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_model_access_control_matrix`
- CLI schema help: `workflow tools describe praxis_model_access_control_matrix`
- When to use: Inspect the live ON/OFF model-access switchboard by task type, CLI/API type, provider, model, scope, reason, and operator instruction.
- When not to use: Do not use it as a mutation surface; it is the read model that drives provider catalog projection.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "runtime_profile_ref": "praxis",
  "job_type": "compile",
  "transport_type": "API"
}
```

#### `praxis_object_truth`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:object-truth`
- Risks: `read`
- CLI entrypoint: `workflow object-truth`
- CLI schema help: `workflow tools describe praxis_object_truth`
- When to use: Build deterministic object-truth evidence for one inline external record: identity digest, field observations, value digests, source metadata, hierarchy signals, and redaction-safe previews.
- When not to use: Do not use it for multi-system sampling, durable persistence, or business truth decisions yet. This is the read-only observe-record slice.
- Recommended alias: `workflow object-truth`
- Selector: none
- Required args: `system_ref`, `object_ref`, `record`, `identity_fields`

Example input:

```json
{
  "system_ref": "salesforce",
  "object_ref": "account",
  "record": {
    "id": "001",
    "name": "Acme",
    "billing": {
      "city": "Denver"
    }
  },
  "identity_fields": [
    "id"
  ],
  "source_metadata": {
    "updated_at": "2026-04-28T10:00:00Z"
  }
}
```

#### `praxis_object_truth_compare_versions`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:object-truth-compare`
- Risks: `read`
- CLI entrypoint: `workflow object-truth-compare`
- CLI schema help: `workflow tools describe praxis_object_truth_compare_versions`
- When to use: Compare two persisted object-truth object versions by digest to see matching, different, missing, and freshness signals.
- When not to use: Do not use to decide final business truth by itself; it produces deterministic evidence for a later decision layer.
- Recommended alias: `workflow object-truth-compare`
- Selector: none
- Required args: `left_object_version_digest`, `right_object_version_digest`

Example input:

```json
{
  "left_object_version_digest": "left-digest",
  "right_object_version_digest": "right-digest"
}
```

#### `praxis_object_truth_record_comparison_run`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:object-truth-record-comparison`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow object-truth-record-comparison`
- CLI schema help: `workflow tools describe praxis_object_truth_record_comparison_run`
- When to use: Persist a comparison result between two stored object versions so future runs can query the evidence instead of recomputing it.
- When not to use: Do not use for ad hoc read-only inspection; use praxis_object_truth_compare_versions when no write is intended.
- Recommended alias: `workflow object-truth-record-comparison`
- Selector: none
- Required args: `left_object_version_digest`, `right_object_version_digest`

Example input:

```json
{
  "left_object_version_digest": "left-digest",
  "right_object_version_digest": "right-digest",
  "observed_by_ref": "operator:nate",
  "source_ref": "comparison:accounts:demo"
}
```

#### `praxis_object_truth_store`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:object-truth-store`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow object-truth-store`
- CLI schema help: `workflow tools describe praxis_object_truth_store`
- When to use: Persist deterministic object-truth evidence for one inline external record after the authority domain and evidence tables exist.
- When not to use: Do not use for exploratory inspection when no write is intended; use praxis_object_truth instead. Do not use it to decide business truth.
- Recommended alias: `workflow object-truth-store`
- Selector: none
- Required args: `system_ref`, `object_ref`, `record`, `identity_fields`

Example input:

```json
{
  "system_ref": "salesforce",
  "object_ref": "account",
  "record": {
    "id": "001",
    "name": "Acme",
    "billing": {
      "city": "Denver"
    }
  },
  "identity_fields": [
    "id"
  ],
  "source_metadata": {
    "updated_at": "2026-04-28T10:00:00Z"
  },
  "observed_by_ref": "operator:nate",
  "source_ref": "sample:accounts:001"
}
```

#### `praxis_object_truth_store_schema_snapshot`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:object-truth-store-schema`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow object-truth-store-schema`
- CLI schema help: `workflow tools describe praxis_object_truth_store_schema_snapshot`
- When to use: Persist normalized schema evidence for one external object before record sampling or comparison work references a schema digest.
- When not to use: Do not use for record payloads; use praxis_object_truth_store for object-version evidence.
- Recommended alias: `workflow object-truth-store-schema`
- Selector: none
- Required args: `system_ref`, `object_ref`, `raw_schema`

Example input:

```json
{
  "system_ref": "salesforce",
  "object_ref": "account",
  "raw_schema": {
    "fields": [
      {
        "name": "id",
        "type": "string",
        "required": true
      },
      {
        "name": "name",
        "type": "string"
      }
    ]
  },
  "observed_by_ref": "operator:nate",
  "source_ref": "schema:salesforce:account"
}
```

#### `praxis_operation_forge`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:operation-forge`
- Risks: `read`
- CLI entrypoint: `workflow operation-forge`
- CLI schema help: `workflow tools describe praxis_operation_forge`
- When to use: Preview the CQRS operation/tool registration path before adding a new operation or MCP wrapper. Use it to get the exact register payload, tool binding, fast-feedback commands, and command/query defaults.
- When not to use: Do not use it as a mutation surface; it prepares the canonical payload.
- Recommended alias: `workflow operation-forge`
- Selector: none
- Required args: `operation_name`

Example input:

```json
{
  "operation_name": "operator.example_truth",
  "handler_ref": "runtime.operations.queries.operator_composed.handle_query_example_truth",
  "input_model_ref": "runtime.operations.queries.operator_composed.QueryExampleTruth",
  "authority_domain_ref": "authority.workflow_runs"
}
```

#### `praxis_orient`

- Surface: `operations`
- Tier: `curated`
- Badges: `curated`, `operations`, `alias:orient`
- Risks: `read`
- CLI entrypoint: `workflow orient`
- CLI schema help: `workflow tools describe praxis_orient`
- When to use: Wake up against Praxis and get standing orders, authority envelope, tool guidance, and endpoints in one call.
- When not to use: Do not use it for deep subsystem inspection; call cluster-specific tools instead.
- Recommended alias: `workflow orient`
- Selector: none
- Required args: (none)

Example input:

```json
{}
```

#### `praxis_provider_availability_refresh`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_provider_availability_refresh`
- CLI schema help: `workflow tools describe praxis_provider_availability_refresh`
- When to use: Refresh provider availability through CQRS before trusting routing or launching a proof job. The resulting receipt is machine-checkable evidence for proof-launch approval when route truth is not already fresh. Persists provider_usage probe snapshots and emits a receipt-backed provider.availability.refreshed event.
- When not to use: Do not use this as a dry-run evaluator and do not fire it repeatedly to hope capacity changes. Use it once when provider availability authority is stale or unknown.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "max_concurrency": 4
}
```

#### `praxis_provider_control_plane`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `alias:provider-control-plane`
- Risks: `read`
- CLI entrypoint: `workflow provider-control-plane`
- CLI schema help: `workflow tools describe praxis_provider_control_plane`
- When to use: Inspect the private provider/job/model matrix, including CLI/API type, cost, version, runnable state, breaker state, credential state, and removal reasons.
- When not to use: Do not use it to change provider access; use circuit/control-panel commands for mutations.
- Recommended alias: `workflow provider-control-plane`
- Selector: none
- Required args: (none)

Example input:

```json
{
  "runtime_profile_ref": "praxis"
}
```

#### `praxis_provider_route_truth`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_provider_route_truth`
- CLI schema help: `workflow tools describe praxis_provider_route_truth`
- When to use: Check whether a provider/model/job route is runnable or blocked, including control state and removal reasons. Use the returned route truth as proof-launch evidence when approving a proposed plan.
- When not to use: Do not use it to change access; use praxis_access_control or praxis_circuits.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "runtime_profile_ref": "praxis"
}
```

#### `praxis_register_authority_domain`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:register-authority-domain`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow register-authority-domain`
- CLI schema help: `workflow tools describe praxis_register_authority_domain`
- When to use: Register or update an authority domain after the forge confirms the domain is the right owner of durable truth. This creates the domain before operations, tables, workflows, or MCP tools attach to it.
- When not to use: Do not use it to attach operations; use praxis_register_operation after the authority domain exists. Do not use it without a decision_ref.
- Recommended alias: `workflow register-authority-domain`
- Selector: none
- Required args: `authority_domain_ref`, `decision_ref`

Example input:

```json
{
  "authority_domain_ref": "authority.object_truth",
  "decision_ref": "operator_decision.architecture_policy.product_architecture.object_truth_requires_deterministic_parse_compare_substrate"
}
```

#### `praxis_register_operation`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:register-operation`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow register-operation`
- CLI schema help: `workflow tools describe praxis_register_operation`
- When to use: Register a net-new CQRS operation (gateway dispatch key + handler + Pydantic input) through the catalog without hand-authoring a migration for the triple write.
- When not to use: Do not use it to tweak an existing operation's input shape — use praxis_evolve_operation_field for planned field additions. Do not use it to soft-delete an op — use praxis_retire_operation.
- Recommended alias: `workflow register-operation`
- Selector: none
- Required args: `operation_ref`, `operation_name`, `handler_ref`, `input_model_ref`, `authority_domain_ref`

Example input:

```json
{
  "operation_ref": "example.query.widget_stats",
  "operation_name": "example_query_widget_stats",
  "handler_ref": "runtime.operations.queries.widget_stats.handle_widget_stats",
  "input_model_ref": "runtime.operations.queries.widget_stats.WidgetStatsQuery",
  "authority_domain_ref": "authority.example",
  "operation_kind": "query",
  "posture": "observe",
  "idempotency_policy": "read_only"
}
```

#### `praxis_reload`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_reload`
- CLI schema help: `workflow tools describe praxis_reload`
- When to use: Clear in-process caches after changing runtime config or MCP catalog state.
- When not to use: Do not use it as a routine health command.
- Selector: none
- Required args: (none)

Example input:

```json
{}
```

#### `praxis_remediation_apply`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `alias:remediation-apply`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow remediation-apply`
- CLI schema help: `workflow tools describe praxis_remediation_apply`
- When to use: Apply only guarded local runtime repairs, such as stale provider slot cleanup or expired host-resource lease cleanup, before one explicit retry.
- When not to use: Do not use it to retry jobs, edit code, or repair credentials.
- Recommended alias: `workflow remediation-apply`
- Selector: none
- Required args: (none)

Example input:

```json
{
  "failure_type": "provider.capacity"
}
```

#### `praxis_remediation_plan`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `alias:remediation-plan`
- Risks: `read`
- CLI entrypoint: `workflow remediation-plan`
- CLI schema help: `workflow tools describe praxis_remediation_plan`
- When to use: Explain the safe remediation tier, evidence requirements, approval gate, and retry delta for a typed workflow failure.
- When not to use: Do not use it to apply repairs; it only declares the allowed plan.
- Recommended alias: `workflow remediation-plan`
- Selector: none
- Required args: (none)

Example input:

```json
{
  "failure_type": "context_not_hydrated"
}
```

#### `praxis_retire_operation`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:retire-operation`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow retire-operation`
- CLI schema help: `workflow tools describe praxis_retire_operation`
- When to use: Soft-retire an operation (disable gateway binding, mark authority object deprecated) while keeping rows for receipts and audit continuity.
- When not to use: Do not use it when you meant to register a replacement op first — retire after the new path is live. Do not use it for physical deletion; rows are retained by design.
- Recommended alias: `workflow retire-operation`
- Selector: none
- Required args: `operation_ref`

Example input:

```json
{
  "operation_ref": "legacy.integration.probe_stale",
  "reason_code": "superseded"
}
```

#### `praxis_runtime_truth_snapshot`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `alias:runtime-truth`
- Risks: `read`
- CLI entrypoint: `workflow runtime-truth`
- CLI schema help: `workflow tools describe praxis_runtime_truth_snapshot`
- When to use: Inspect observed workflow runtime truth across DB authority, queue state, worker heartbeats, provider slots, host-resource leases, Docker, manifest hydration audit, and recent typed failures.
- When not to use: Do not use it to repair or retry; it is the evidence packet.
- Recommended alias: `workflow runtime-truth`
- Selector: none
- Required args: (none)

Example input:

```json
{
  "since_minutes": 60
}
```

#### `praxis_semantic_bridges_backfill`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_semantic_bridges_backfill`
- CLI schema help: `workflow tools describe praxis_semantic_bridges_backfill`
- When to use: Rebuild semantic bridge authority from canonical operator sources.
- When not to use: Do not use it for semantic reads; use praxis_semantic_assertions instead.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "include_object_relations": true
}
```

#### `praxis_semantic_projection_refresh`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_semantic_projection_refresh`
- CLI schema help: `workflow tools describe praxis_semantic_projection_refresh`
- When to use: Consume semantic projection events through one explicit maintenance operation.
- When not to use: Do not use it for read-only graph inspection.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "limit": 100
}
```

#### `praxis_status_snapshot`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_status_snapshot`
- CLI schema help: `workflow tools describe praxis_status_snapshot`
- When to use: Inspect workflow pass rate, failure mix, and in-flight run summaries from canonical receipts.
- When not to use: Do not use it for deep run inspection or workflow launch.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "since_hours": 24
}
```

#### `praxis_task_route_eligibility`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `alias:task-route-eligibility`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow task-route-eligibility`
- CLI schema help: `workflow tools describe praxis_task_route_eligibility`
- When to use: Allow or reject one provider/model candidate for one task type through a bounded eligibility window. Use this for by-task routing policy such as letting anthropic/claude-sonnet-4-6 participate in build or review without enabling it everywhere.
- When not to use: Do not use it for broad provider onboarding or transport-wide ON/OFF control; use praxis_provider_onboard or praxis_access_control for those.
- Recommended alias: `workflow task-route-eligibility`
- Selector: none
- Required args: `provider_slug`, `eligibility_status`

Example input:

```json
{
  "provider_slug": "anthropic",
  "model_slug": "claude-sonnet-4-6",
  "task_type": "build",
  "eligibility_status": "eligible",
  "reason_code": "task_type_exception",
  "rationale": "Allow sonnet for build high and build mid"
}
```

#### `praxis_work_assignment_matrix`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_work_assignment_matrix`
- CLI schema help: `workflow tools describe praxis_work_assignment_matrix`
- When to use: Inspect grouped work by audit group, recommended model tier, task type, sequence, and assignment reason.
- When not to use: Do not use it as the source of provider availability; use praxis_provider_control_plane for access capability.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "open_only": true
}
```

#### `tool_dag_health`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `deprecated-alias`
- Risks: `read`
- CLI entrypoint: `workflow tools call tool_dag_health`
- CLI schema help: `workflow tools describe tool_dag_health`
- Replacement: `workflow health`
- When to use: Run a full preflight before workflow launch or when the platform feels degraded.
- When not to use: Do not use it to inspect one specific workflow run.
- Selector: none
- Required args: (none)

Example input:

```json
{}
```

### Operator

#### `praxis_bug_triage_packet`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_bug_triage_packet`
- CLI schema help: `workflow tools describe praxis_bug_triage_packet`
- When to use: Let an LLM choose bug work using deterministic evidence/provenance classes.
- When not to use: Do not use it to resolve, mutate, or backfill bugs.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "limit": 25
}
```

#### `praxis_execution_proof`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_execution_proof`
- CLI schema help: `workflow tools describe praxis_execution_proof`
- When to use: Check whether a run actually fired, is still executing, or only has weak queued/running labels.
- When not to use: Do not use it to launch, retry, cancel, or resolve work; it is proof-only.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "run_id": "run_123",
  "stale_after_seconds": 180
}
```

#### `praxis_graph_projection`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_graph_projection`
- CLI schema help: `workflow tools describe praxis_graph_projection`
- When to use: Inspect the semantic-first operator graph across domains.
- When not to use: Do not use it for run-scoped workflow topology.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "as_of": "2026-04-16T20:05:00+00:00"
}
```

#### `praxis_issue_backlog`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_issue_backlog`
- CLI schema help: `workflow tools describe praxis_issue_backlog`
- When to use: Inspect the canonical upstream issue backlog before bug promotion.
- When not to use: Do not use it to mutate issue or bug state.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "limit": 25
}
```

#### `praxis_next`

- Surface: `operator`
- Tier: `stable`
- Badges: `stable`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_next`
- CLI schema help: `workflow tools describe praxis_next`
- When to use: Ask what the next legal operator move is, gate launches/retries, triage failures, audit manifests, dedupe tool ideas, or compute the unlock frontier.
- When not to use: Do not use it to mutate workflow state or launch work directly; it is a read-only decision surface.
- Selector: `action`; default `next`; values `next`, `launch_gate`, `failure_triage`, `manifest_audit`, `toolsmith`, `unlock_frontier`
- Required args: (none)

Example input:

```json
{
  "action": "next",
  "intent": "fire workflow fleet safely",
  "fleet_size": 12
}
```

#### `praxis_next_work`

- Surface: `operator`
- Tier: `stable`
- Badges: `stable`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_next_work`
- CLI schema help: `workflow tools describe praxis_next_work`
- When to use: Choose the next bounded work item from refactor heatmap, bug triage, assignment matrix, and runtime status.
- When not to use: Do not use it to resolve bugs or mutate roadmap authority.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "limit": 10
}
```

#### `praxis_operator_architecture_policy`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_operator_architecture_policy`
- CLI schema help: `workflow tools describe praxis_operator_architecture_policy`
- When to use: Record one typed architecture policy decision in operator_decisions when explicit guidance should become durable control authority.
- When not to use: Do not use it for generic decision history reads; use praxis_operator_decisions for that.
- Selector: none
- Required args: `authority_domain`, `policy_slug`, `title`, `rationale`, `decided_by`, `decision_source`

Example input:

```json
{
  "authority_domain": "decision_tables",
  "policy_slug": "db-native-authority",
  "title": "Decision tables are DB-native authority",
  "rationale": "Keep authority in Postgres.",
  "decided_by": "nate",
  "decision_source": "cto.guidance",
  "decision_provenance": "explicit",
  "decision_why": "Authority outside the DB cannot be replayed or audited under the gateway-receipt model; surfaces drift from runtime."
}
```

#### `praxis_operator_closeout`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_operator_closeout`
- CLI schema help: `workflow tools describe praxis_operator_closeout`
- When to use: Preview or commit operator work-item closeout through the shared gate.
- When not to use: Do not use it for roadmap item creation or read-only status views.
- Selector: `action`; default `preview`; values `preview`, `commit`
- Required args: (none)

Example input:

```json
{
  "action": "preview",
  "work_item_id": "WI-123"
}
```

#### `praxis_operator_decisions`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_operator_decisions`
- CLI schema help: `workflow tools describe praxis_operator_decisions`
- Replacement: `praxis_next`
- When to use: List or record durable operator decisions such as architecture policy rows in the canonical operator_decisions table. New records should pass scope_clamp={'applies_to': [...], 'does_not_apply_to': [...]} so downstream surfaces can quote the clamp verbatim instead of paraphrasing rationale; rows omit it default to a 'pending_review' placeholder for the operator to fill in via the Moon Decisions panel.
- When not to use: Do not use it for roadmap item authoring or cutover-gate admission.
- Selector: `action`; default `list`; values `list`, `record`
- Required args: (none)

Example input:

```json
{
  "action": "list",
  "decision_kind": "architecture_policy"
}
```

#### `praxis_operator_ideas`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_operator_ideas`
- CLI schema help: `workflow tools describe praxis_operator_ideas`
- When to use: Capture pre-commitment ideas, reject/supersede/archive them, or promote them into committed roadmap items.
- When not to use: Do not use it as a substitute for committed roadmap work; use praxis_operator_write once scope is committed.
- Selector: `action`; default `list`; values `list`, `file`, `resolve`, `promote`
- Required args: (none)

Example input:

```json
{
  "action": "list",
  "limit": 25
}
```

#### `praxis_operator_native_primary_cutover_gate`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_operator_native_primary_cutover_gate`
- CLI schema help: `workflow tools describe praxis_operator_native_primary_cutover_gate`
- When to use: Admit a native-primary cutover gate with required decision metadata into operator-control.
- When not to use: Do not use it for read-only operator status views.
- Selector: none
- Required args: `decided_by`, `decision_source`, `rationale`

Example input:

```json
{
  "decided_by": "operator-auto",
  "decision_source": "runbook",
  "rationale": "manual rollout hold ended",
  "roadmap_item_id": "roadmap_item.platform.deploy"
}
```

#### `praxis_operator_relations`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_operator_relations`
- CLI schema help: `workflow tools describe praxis_operator_relations`
- When to use: Record canonical functional areas and cross-object semantic relations when operator entities need one explicit semantic edge instead of hidden tags or prose.
- When not to use: Do not use it for read-only operator inspection or generic roadmap authoring.
- Selector: `action`; default `record_functional_area`; values `record_functional_area`, `record_relation`
- Required args: (none)

Example input:

```json
{
  "action": "record_functional_area",
  "area_slug": "checkout",
  "title": "Checkout",
  "summary": "Shared checkout semantics"
}
```

#### `praxis_operator_roadmap_view`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_operator_roadmap_view`
- CLI schema help: `workflow tools describe praxis_operator_roadmap_view`
- When to use: Read one roadmap subtree, derived clusters, dependency edges, and semantic-first external neighbors without mutating roadmap authority.
- When not to use: Do not use it to commit roadmap changes.
- Selector: none
- Required args: (none)

Example input:

```json
{}
```

#### `praxis_operator_write`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_operator_write`
- CLI schema help: `workflow tools describe praxis_operator_write`
- When to use: Preview, validate, or commit roadmap writes through the operator gate.
- When not to use: Do not use it for read-only backlog inspection.
- Selector: `action`; default `preview`; values `preview`, `validate`, `commit`, `update`, `retire`, `re_parent`, `reparent`
- Required args: (none)

Example input:

```json
{
  "action": "preview",
  "title": "Consolidate CLI frontdoors",
  "intent_brief": "one authority for operator CLI"
}
```

#### `praxis_refactor_heatmap`

- Surface: `operator`
- Tier: `stable`
- Badges: `stable`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_refactor_heatmap`
- CLI schema help: `workflow tools describe praxis_refactor_heatmap`
- When to use: Rank architecture refactor candidates by authority spread, bugs, surface coupling, and large-module pressure.
- When not to use: Do not use it to mutate bugs, roadmap, catalog rows, or source files.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "limit": 15
}
```

#### `praxis_replay_ready_bugs`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_replay_ready_bugs`
- CLI schema help: `workflow tools describe praxis_replay_ready_bugs`
- When to use: Inspect replayable bugs without bundling that read behind a selector view.
- When not to use: Do not use it to trigger replay backfill.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "limit": 25
}
```

#### `praxis_run`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_run`
- CLI schema help: `workflow tools describe praxis_run`
- When to use: One stop for run-scoped status / scoreboard / graph / lineage views. Use action, or view when you are copying the HTTP selector shape.
- When not to use: Do not use it for cross-domain operator graph (use praxis_graph_projection).
- Selector: `action`; default `status`; values `status`, `scoreboard`, `graph`, `lineage`
- Required args: `run_id`

Example input:

```json
{
  "run_id": "run_123",
  "action": "status"
}
```

#### `praxis_run_graph`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `deprecated-alias`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_run_graph`
- CLI schema help: `workflow tools describe praxis_run_graph`
- Replacement: `workflow tools call praxis_run --input-json '{"run_id":"<run_id>","action":"graph"}'`
- When to use: Inspect workflow topology for one run.
- When not to use: Do not use it for cross-domain operator graph inspection.
- Selector: none
- Required args: `run_id`

Example input:

```json
{
  "run_id": "run_123"
}
```

#### `praxis_run_lineage`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `deprecated-alias`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_run_lineage`
- CLI schema help: `workflow tools describe praxis_run_lineage`
- Replacement: `workflow tools call praxis_run --input-json '{"run_id":"<run_id>","action":"lineage"}'`
- When to use: Inspect graph lineage and operator frames for one run.
- When not to use: Do not use it for whole-system summaries.
- Selector: none
- Required args: `run_id`

Example input:

```json
{
  "run_id": "run_123"
}
```

#### `praxis_run_scoreboard`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `deprecated-alias`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_run_scoreboard`
- CLI schema help: `workflow tools describe praxis_run_scoreboard`
- Replacement: `workflow tools call praxis_run --input-json '{"run_id":"<run_id>","action":"scoreboard"}'`
- When to use: Inspect cutover readiness for one workflow run.
- When not to use: Do not use it for workflow launch or global status.
- Selector: none
- Required args: `run_id`

Example input:

```json
{
  "run_id": "run_123"
}
```

#### `praxis_run_status`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `deprecated-alias`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_run_status`
- CLI schema help: `workflow tools describe praxis_run_status`
- Replacement: `workflow tools call praxis_run --input-json '{"run_id":"<run_id>","action":"status"}'`
- When to use: Inspect operator status for one workflow run.
- When not to use: Do not use it for whole-system pass-rate summaries.
- Selector: none
- Required args: `run_id`

Example input:

```json
{
  "run_id": "run_123"
}
```

#### `praxis_semantic_assertions`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_semantic_assertions`
- CLI schema help: `workflow tools describe praxis_semantic_assertions`
- When to use: Register semantic predicates, record or retract semantic assertions, or query the current semantic substrate when semantics need durable typed authority.
- When not to use: Do not use it for generic roadmap authoring, issue triage, or workflow telemetry reads.
- Selector: `action`; default `list`; values `list`, `register_predicate`, `record_assertion`, `retract_assertion`
- Required args: (none)

Example input:

```json
{
  "action": "list",
  "predicate_slug": "grouped_in"
}
```

#### `praxis_trace`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_trace`
- CLI schema help: `workflow tools describe praxis_trace`
- When to use: Follow a flow end-to-end across nested gateway calls within one entry point. Start from any receipt, event, correlation, workflow run, or bug to see the whole tree.
- When not to use: Do not use this for run-scoped views — praxis_run(action='lineage') still walks the evidence_timeline for one workflow run. Use praxis_trace when the flow crosses operations, not just stages.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "receipt_id": "<receipt-uuid>"
}
```

#### `praxis_ui_experience_graph`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_ui_experience_graph`
- CLI schema help: `workflow tools describe praxis_ui_experience_graph`
- When to use: Inspect the app UI experience before changing React, CSS, or surface catalog behavior.
- When not to use: Do not use it for run-scoped execution topology or raw knowledge-graph traversal.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "surface_name": "build"
}
```

### Planning

#### `praxis_decompose`

- Surface: `planning`
- Tier: `stable`
- Badges: `stable`, `planning`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_decompose`
- CLI schema help: `workflow tools describe praxis_decompose`
- When to use: Break a large objective into workflow-sized micro-sprints before workflow launch.
- When not to use: Do not use it to execute work or inspect historical run state.
- Selector: none
- Required args: `objective`

Example input:

```json
{
  "objective": "Consolidate operator read and write surfaces"
}
```

#### `praxis_intent_match`

- Surface: `planning`
- Tier: `stable`
- Badges: `stable`, `planning`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_intent_match`
- CLI schema help: `workflow tools describe praxis_intent_match`
- When to use: Match a product intent against existing platform components before generating a manifest.
- When not to use: Do not use it for code search or historical run analysis.
- Selector: none
- Required args: `intent`

Example input:

```json
{
  "intent": "invoice approval workflow with status tracking"
}
```

#### `praxis_manifest_generate`

- Surface: `planning`
- Tier: `advanced`
- Badges: `advanced`, `planning`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_manifest_generate`
- CLI schema help: `workflow tools describe praxis_manifest_generate`
- When to use: Generate a new manifest from an intent after you've confirmed the building blocks.
- When not to use: Do not use it for code execution or connector onboarding.
- Selector: none
- Required args: `intent`

Example input:

```json
{
  "intent": "customer onboarding pipeline with approval steps"
}
```

#### `praxis_manifest_refine`

- Surface: `planning`
- Tier: `advanced`
- Badges: `advanced`, `planning`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_manifest_refine`
- CLI schema help: `workflow tools describe praxis_manifest_refine`
- When to use: Iterate on an existing generated manifest based on feedback.
- When not to use: Do not use it without a manifest id from a prior generation step.
- Selector: none
- Required args: `manifest_id`, `feedback`

Example input:

```json
{
  "manifest_id": "manifest_abc123",
  "feedback": "Add weekly trends and remove the status grid"
}
```

#### `praxis_session`

- Surface: `planning`
- Tier: `advanced`
- Badges: `advanced`, `planning`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_session`
- CLI schema help: `workflow tools describe praxis_session`
- When to use: Inspect or validate session carry-forward packs between work sessions.
- When not to use: Do not use it as a live workflow-session context surface.
- Selector: `action`; default `latest`; values `latest`, `validate`
- Required args: (none)

Example input:

```json
{
  "action": "latest"
}
```

### Query

#### `praxis_query`

- Surface: `query`
- Tier: `stable`
- Badges: `stable`, `query`, `alias:query`
- Risks: `read`
- CLI entrypoint: `workflow query`
- CLI schema help: `workflow tools describe praxis_query`
- When to use: Route a natural-language question to the right platform subsystem from the terminal when you are not sure which exact tool to use.
- When not to use: Do not use it when you already know the exact specialist tool you need.
- Recommended alias: `workflow query`
- Selector: none
- Required args: `question`

Example input:

```json
{
  "question": "what is failing right now?"
}
```

### Research

#### `praxis_research_workflow`

- Surface: `research`
- Tier: `advanced`
- Badges: `advanced`, `research`, `launches-work`
- Risks: `launch`, `read`
- CLI entrypoint: `workflow tools call praxis_research_workflow`
- CLI schema help: `workflow tools describe praxis_research_workflow`
- When to use: Launch or inspect fan-out research workflows for deeper multi-angle investigations.
- When not to use: Do not use it for single-shot questions where recall or query is enough.
- Selector: `action`; default `run`; values `run`, `list`
- Required args: (none)

Example input:

```json
{
  "action": "run",
  "topic": "best practices for durable MCP transports",
  "workers": 8
}
```

### Session

#### `praxis_context_shard`

- Surface: `session`
- Tier: `session`
- Badges: `session`, `session`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_context_shard`
- CLI schema help: `workflow tools describe praxis_context_shard`
- When to use: Read the bounded execution shard for the active workflow MCP session.
- When not to use: Do not use it outside workflow-session execution or as a general repository browser.
- Selector: `view`; default `full`; values `full`, `summary`, `sections`
- Required args: (none)

Example input:

```json
{
  "view": "summary"
}
```

#### `praxis_session_context`

- Surface: `session`
- Tier: `session`
- Badges: `session`, `session`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_session_context`
- CLI schema help: `workflow tools describe praxis_session_context`
- When to use: Read or write persistent context owned by the active workflow MCP session.
- When not to use: Do not use it outside token-scoped workflow execution.
- Selector: `action`; default `read`; values `read`, `write`
- Required args: (none)

Example input:

```json
{
  "action": "read"
}
```

#### `praxis_subscribe_events`

- Surface: `session`
- Tier: `session`
- Badges: `session`, `session`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_subscribe_events`
- CLI schema help: `workflow tools describe praxis_subscribe_events`
- When to use: Poll workflow-scoped event updates since the last cursor position for the active session.
- When not to use: Do not use it outside token-scoped workflow execution.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "channel": "build_state",
  "limit": 50
}
```

### Setup

#### `praxis_credential_capture`

- Surface: `setup`
- Tier: `stable`
- Badges: `stable`, `setup`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_credential_capture`
- CLI schema help: `workflow tools describe praxis_credential_capture`
- When to use: Request, inspect, or open the secure host API-key entry window when a wizard/provider/setup flow needs a macOS Keychain-backed credential. Search terms: api key credential keychain secure window.
- When not to use: Do not pass raw API keys to this tool. Do not use it for provider route onboarding; use praxis_provider_onboard after credentials are present.
- Selector: `action`; default `request`; values `request`, `status`, `capture`
- Required args: `env_var_name`

Example input:

```json
{
  "action": "request",
  "env_var_name": "OPENAI_API_KEY",
  "provider_label": "OpenAI"
}
```

#### `praxis_setup`

- Surface: `setup`
- Tier: `core`
- Badges: `core`, `setup`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_setup`
- CLI schema help: `workflow tools describe praxis_setup`
- When to use: Inspect or plan runtime-target setup through the same authority as `praxis setup doctor|plan|apply`, including the native_instance contract.
- When not to use: Do not use as a workflow launch/status tool.
- Selector: `action`; default `doctor`; values `doctor`, `plan`, `apply`, `graph`
- Required args: (none)

Example input:

```json
{
  "action": "doctor"
}
```

### Submissions

#### `praxis_get_submission`

- Surface: `submissions`
- Tier: `session`
- Badges: `session`, `submissions`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_get_submission`
- CLI schema help: `workflow tools describe praxis_get_submission`
- When to use: Read a sealed submission in the active workflow MCP session.
- When not to use: Do not use it outside token-scoped workflow review flows.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "submission_id": "submission_abc123"
}
```

#### `praxis_review_submission`

- Surface: `submissions`
- Tier: `session`
- Badges: `session`, `submissions`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_review_submission`
- CLI schema help: `workflow tools describe praxis_review_submission`
- When to use: Approve, reject, or request changes on a sealed submission inside a workflow session.
- When not to use: Do not use it outside token-scoped workflow review flows.
- Selector: none
- Required args: `decision`, `summary`

Example input:

```json
{
  "submission_id": "submission_abc123",
  "decision": "approve",
  "summary": "Looks good"
}
```

#### `praxis_submit_artifact_bundle`

- Surface: `submissions`
- Tier: `session`
- Badges: `session`, `submissions`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_submit_artifact_bundle`
- CLI schema help: `workflow tools describe praxis_submit_artifact_bundle`
- When to use: Submit an artifact-bundle result owned by the active workflow session.
- When not to use: Do not use it outside token-scoped workflow execution.
- Selector: none
- Required args: `summary`, `primary_paths`, `result_kind`

Example input:

```json
{
  "summary": "Generated migration bundle",
  "primary_paths": [
    "artifacts/migrations"
  ],
  "result_kind": "artifact_bundle"
}
```

#### `praxis_submit_code_change`

- Surface: `submissions`
- Tier: `session`
- Badges: `session`, `submissions`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_submit_code_change`
- CLI schema help: `workflow tools describe praxis_submit_code_change`
- When to use: Submit a sealed code-change result owned by the active workflow session.
- When not to use: Do not use it outside token-scoped workflow execution.
- Selector: none
- Required args: `summary`, `primary_paths`, `result_kind`

Example input:

```json
{
  "summary": "Fixed MCP transport framing",
  "primary_paths": [
    "surfaces/mcp/protocol.py"
  ],
  "result_kind": "code_change"
}
```

#### `praxis_submit_research_result`

- Surface: `submissions`
- Tier: `session`
- Badges: `session`, `submissions`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_submit_research_result`
- CLI schema help: `workflow tools describe praxis_submit_research_result`
- When to use: Submit a sealed research result owned by the active workflow session.
- When not to use: Do not use it outside token-scoped workflow execution.
- Selector: none
- Required args: `summary`, `primary_paths`, `result_kind`

Example input:

```json
{
  "summary": "Surveyed MCP CLI exposure patterns",
  "primary_paths": [
    "notes/research.md"
  ],
  "result_kind": "research_result"
}
```

### Workflow

#### `praxis_approve_proposed_plan`

- Surface: `workflow`
- Tier: `stable`
- Badges: `stable`, `workflow`, `alias:approve-plan`
- Risks: `read`
- CLI entrypoint: `workflow approve-plan`
- CLI schema help: `workflow tools describe praxis_approve_proposed_plan`
- When to use: Approve a ProposedPlan so launch_approved can submit it. Wraps the proposal with approved_by + timestamp + hash; the hash binds the approval to the exact spec_dict so tampering between approve and launch fails closed. The proposed plan must already carry machine-checkable provider freshness evidence with fresh route truth.
- When not to use: Do not use it for no-approval launches — praxis_launch_plan in submit mode is the direct path.
- Recommended alias: `workflow approve-plan`
- Selector: none
- Required args: `proposed`, `approved_by`

Example input:

```json
{
  "proposed": {
    "spec_dict": {
      "name": "...",
      "jobs": []
    },
    "preview": {},
    "warnings": [],
    "workflow_id": "plan.deadbeef",
    "spec_name": "bug_wave_0",
    "total_jobs": 0,
    "packet_declarations": [],
    "binding_summary": {
      "totals": {
        "bound": 0,
        "ambiguous": 0,
        "unbound": 0
      },
      "unbound_refs": [],
      "ambiguous_refs": []
    },
    "provider_freshness": {
      "route_truth_ref": "preview:deadbeef",
      "route_truth_checked_at": "2026-04-28T00:00:00+00:00"
    }
  },
  "approved_by": "nate@praxis",
  "approval_note": "Looks good; proceed."
}
```

#### `praxis_bind_data_pills`

- Surface: `workflow`
- Tier: `stable`
- Badges: `stable`, `workflow`, `alias:bind-pills`
- Risks: `read`
- CLI entrypoint: `workflow bind-pills`
- CLI schema help: `workflow tools describe praxis_bind_data_pills`
- When to use: Suggest likely object.field data-pill candidates from loose prose and validate explicit references against the data dictionary authority. Layer 1 (Bind) of the planning stack — call BEFORE decomposing intent into packets so every field ref is either confirmed or surfaced as a candidate to confirm.
- When not to use: Do not treat suggestions as bound authority. Suggested pills are candidates; confirmed packet compilation still needs explicit object.field refs or a caller approval step.
- Recommended alias: `workflow bind-pills`
- Selector: none
- Required args: `intent`

Example input:

```json
{
  "intent": "Update users.first_name whenever users.email changes."
}
```

#### `praxis_compose_and_launch`

- Surface: `workflow`
- Tier: `stable`
- Badges: `stable`, `workflow`, `alias:ship-intent`, `launches-work`
- Risks: `launch`
- CLI entrypoint: `workflow ship-intent`
- CLI schema help: `workflow tools describe praxis_compose_and_launch`
- When to use: End-to-end: prose intent → ProposedPlan → ApprovedPlan → LaunchReceipt in one call. For trusted automation (CI, scripts, experienced operators). Fails closed by default on unresolved routes, unbound pills, or invalid approvals.
- When not to use: Do not use it for untrusted input or when the caller needs to inspect the ProposedPlan first. Use praxis_compose_plan + praxis_approve_proposed_plan + praxis_launch_plan(approved_plan=...) for the three-step flow.
- Recommended alias: `workflow ship-intent`
- Selector: none
- Required args: `intent`, `approved_by`

Example input:

```json
{
  "intent": "1. Add a timezone column to users.\n2. Backfill existing rows with UTC.\n3. Update the profile UI to expose the field.",
  "approved_by": "nate@praxis",
  "plan_name": "timezone_rollout"
}
```

#### `praxis_compose_experiment`

- Surface: `workflow`
- Tier: `advanced`
- Badges: `advanced`, `workflow`, `launches-work`
- Risks: `launch`
- CLI entrypoint: `workflow tools call praxis_compose_experiment`
- CLI schema help: `workflow tools describe praxis_compose_experiment`
- When to use: Run several praxis_compose_plan_via_llm configurations in parallel on the same intent and compare outcomes before pinning knobs in task_type_routing.
- When not to use: Do not use it for a single compose pass — call praxis_compose_plan_via_llm directly. Do not use it when you cannot afford multiple LLM-backed compose receipts.
- Selector: none
- Required args: `intent`, `configs`

Example input:

```json
{
  "intent": "Design a two-step migration to add nullable columns safely.",
  "configs": [
    {
      "model_slug": "openai/gpt-4.1-mini",
      "temperature": 0.2
    },
    {
      "model_slug": "openai/gpt-4.1-mini",
      "temperature": 0.7
    }
  ],
  "plan_name": "migration-compose-ab",
  "concurrency": 2,
  "max_workers": 4
}
```

#### `praxis_compose_plan`

- Surface: `workflow`
- Tier: `stable`
- Badges: `stable`, `workflow`, `alias:compose-plan`
- Risks: `read`
- CLI entrypoint: `workflow compose-plan`
- CLI schema help: `workflow tools describe praxis_compose_plan`
- When to use: Turn prose intent with explicit step markers into a ProposedPlan in one call — chains Layer 2 (decompose) → Layer 1 (bind) → Layer 5 (translate + preview). Compose with approve-plan + launch-plan(approved_plan=...) for the full approval-gated flow.
- When not to use: Do not use it for free prose without step markers. Reword the intent or pass allow_single_step=true explicitly.
- Recommended alias: `workflow compose-plan`
- Selector: none
- Required args: `intent`

Example input:

```json
{
  "intent": "1. Add a timezone column to users.\n2. Backfill existing rows with UTC.\n3. Update the profile UI to expose the field.",
  "plan_name": "timezone_rollout",
  "why": "Operator requested personalization support."
}
```

#### `praxis_compose_plan_via_llm`

- Surface: `workflow`
- Tier: `advanced`
- Badges: `advanced`, `workflow`, `launches-work`
- Risks: `launch`
- CLI entrypoint: `workflow tools call praxis_compose_plan_via_llm`
- CLI schema help: `workflow tools describe praxis_compose_plan_via_llm`
- When to use: Compose a bounded plan statement from synthesized workflow atoms when deterministic skeletons need one LLM planning pass.
- When not to use: Do not use it for execution or provider routing; it is a plan-composition helper.
- Selector: none
- Required args: `intent`

Example input:

```json
{
  "intent": "Build a connector workflow",
  "plan_name": "connector-build",
  "concurrency": 4
}
```

#### `praxis_connector`

- Surface: `workflow`
- Tier: `advanced`
- Badges: `advanced`, `workflow`, `mutates-state`, `launches-work`
- Risks: `launch`, `read`, `write`
- CLI entrypoint: `workflow tools call praxis_connector`
- CLI schema help: `workflow tools describe praxis_connector`
- When to use: Build, inspect, register, or verify third-party API connectors.
- When not to use: Do not use it for invoking an existing integration at runtime.
- Selector: `action`; default `build`; values `build`, `list`, `get`, `register`, `verify`
- Required args: (none)

Example input:

```json
{
  "action": "build",
  "app_name": "Slack"
}
```

#### `praxis_decompose_intent`

- Surface: `workflow`
- Tier: `stable`
- Badges: `stable`, `workflow`, `alias:decompose`
- Risks: `read`
- CLI entrypoint: `workflow decompose`
- CLI schema help: `workflow tools describe praxis_decompose_intent`
- When to use: Split prose intent into ordered steps by parsing explicit markers (numbered lists, bulleted lists, or first/then/finally ordering). Layer 2 (Decompose) of the planning stack — call before turning steps into PlanPackets.
- When not to use: Do not use it to decompose free prose without markers. Reword the intent, wrap with an LLM extractor, or pass allow_single_step=true to accept the whole intent as one step.
- Recommended alias: `workflow decompose`
- Selector: none
- Required args: `intent`

Example input:

```json
{
  "intent": "1. Add a timezone column to users.\n2. Backfill existing rows with UTC.\n3. Update the profile UI to expose the field."
}
```

#### `praxis_generate_plan`

- Surface: `workflow`
- Tier: `stable`
- Badges: `stable`, `workflow`, `alias:generate-plan`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow generate-plan`
- CLI schema help: `workflow tools describe praxis_generate_plan`
- When to use: Shared CQRS plan-generation front door for MCP/CLI/API parity. Use action='generate_plan' to recognize messy prose without mutation, or action='materialize_plan' to create or update draft workflow build state.
- When not to use: Do not use it to launch a workflow run. Materialized workflow state still needs the normal approval and launch path.
- Recommended alias: `workflow generate-plan`
- Selector: `action`; default `generate_plan`; values `generate_plan`, `materialize_plan`
- Required args: `intent`

Example input:

```json
{
  "action": "generate_plan",
  "intent": "Feed in an app name, search, retrieve, evaluate, then build a custom integration."
}
```

#### `praxis_launch_plan`

- Surface: `workflow`
- Tier: `stable`
- Badges: `stable`, `workflow`, `alias:launch-plan`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow launch-plan`
- CLI schema help: `workflow tools describe praxis_launch_plan`
- When to use: Translate an already-planned packet list into a workflow spec and submit it (or preview first with preview_only=true). This is the layer-5 translation primitive — caller still owns upstream planning (extract data pills, decompose prose, reorder by data-flow, author per-step prompts). Proof launches must carry fresh provider route truth or a recent provider availability refresh receipt before approval.
- When not to use: Do not use it to launch a pre-existing .queue.json spec from disk — use praxis_workflow action=run for that path. Do not expect it to do the planning itself (decompose prose, pick fields, reorder steps, write real prompts) — those layers live with the caller today. If you intend to approve the launch, first obtain fresh provider route truth or a recent provider availability refresh receipt.
- Recommended alias: `workflow launch-plan`
- Selector: none
- Required args: (none)

Example input:

```json
{
  "plan": {
    "name": "fix_preview_submit_route_split",
    "packets": [
      {
        "description": "Make preview call TaskTypeRouter so auto/* routes resolve the same way submit does.",
        "write": [
          "Code&DBs/Workflow/runtime/workflow/_admission.py"
        ],
        "stage": "build",
        "label": "preview-submit-route-parity"
      }
    ]
  }
}
```

#### `praxis_plan_lifecycle`

- Surface: `workflow`
- Tier: `stable`
- Badges: `stable`, `workflow`, `alias:plan-history`
- Risks: `read`
- CLI entrypoint: `workflow plan-history`
- CLI schema help: `workflow tools describe praxis_plan_lifecycle`
- When to use: Read every plan.* event for one workflow_id in chronological order — composed, approved, launched, or blocked. The Q-side read of the planning stack's CQRS pattern.
- When not to use: Do not use it for workflow_run status; that's a separate query surfaced by praxis_workflow status/stream actions.
- Recommended alias: `workflow plan-history`
- Selector: none
- Required args: `workflow_id`

Example input:

```json
{
  "workflow_id": "plan.deadbeef12345678"
}
```

#### `praxis_promote_experiment_winner`

- Surface: `workflow`
- Tier: `advanced`
- Badges: `advanced`, `workflow`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_promote_experiment_winner`
- CLI schema help: `workflow tools describe praxis_promote_experiment_winner`
- When to use: Promote the winning compose_experiment leg back into the canonical task_type_routing row after you have inspected the experiment receipt and picked a winner.
- When not to use: Do not use it without a source compose_experiment receipt and config index. Do not use it to auto-apply provider/model identity changes; those stay visible only in the diff.
- Selector: none
- Required args: `source_experiment_receipt_id`, `source_config_index`

Example input:

```json
{
  "source_experiment_receipt_id": "receipt:compose-experiment:1234",
  "source_config_index": 0
}
```

#### `praxis_suggest_plan_atoms`

- Surface: `workflow`
- Tier: `stable`
- Badges: `stable`, `workflow`, `alias:suggest-atoms`
- Risks: `read`
- CLI entrypoint: `workflow suggest-atoms`
- CLI schema help: `workflow tools describe praxis_suggest_plan_atoms`
- When to use: Free prose (any length, no markers, no order) should yield candidate data pills, candidate step types, and candidate input parameters as three independent suggestion streams. Layer 0 (Suggest) of the planning stack — call when the prose has no explicit step markers and the downstream LLM author needs atoms to plan from.
- When not to use: Do not use this to launch, order, or commit. It returns suggestions; an LLM author or operator still has to compose them into a packet list. For prose that already has explicit markers, call praxis_decompose_intent for ordered steps instead.
- Recommended alias: `workflow suggest-atoms`
- Selector: none
- Required args: `intent`

Example input:

```json
{
  "intent": "A repeatable workflow where we feed in an app name or app domain and it gets broken up into multiple steps to plan search, retrieve, evaluate and then attempt to build a custom integration for an application."
}
```

#### `praxis_synthesize_skeleton`

- Surface: `workflow`
- Tier: `advanced`
- Badges: `advanced`, `workflow`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_synthesize_skeleton`
- CLI schema help: `workflow tools describe praxis_synthesize_skeleton`
- When to use: Synthesize a workflow skeleton from recognized intent atoms before materializing or launching the workflow.
- When not to use: Do not use it as the launch authority; use praxis_generate_plan for draft state and praxis_workflow for execution.
- Selector: none
- Required args: `intent`

Example input:

```json
{
  "intent": "Build a connector workflow from app docs and smoke-test it"
}
```

#### `praxis_wave`

- Surface: `workflow`
- Tier: `advanced`
- Badges: `advanced`, `workflow`, `mutates-state`, `launches-work`
- Risks: `launch`, `read`, `write`
- CLI entrypoint: `workflow tools call praxis_wave`
- CLI schema help: `workflow tools describe praxis_wave`
- When to use: Observe or coordinate wave-based execution programs.
- When not to use: Do not use it for single workflow runs with no wave orchestration.
- Selector: `action`; default `observe`; values `observe`, `start`, `next`, `record`
- Required args: (none)

Example input:

```json
{
  "action": "next",
  "wave_id": "wave_1"
}
```

#### `praxis_workflow`

- Surface: `workflow`
- Tier: `advanced`
- Badges: `advanced`, `workflow`, `mutates-state`, `launches-work`
- Risks: `launch`, `read`, `write`
- CLI entrypoint: `workflow tools call praxis_workflow`
- CLI schema help: `workflow tools describe praxis_workflow`
- When to use: Run, preview, inspect, spawn, chain, claim, acknowledge, retry, cancel, repair, or list workflows through the MCP workflow surface.
- When not to use: Do not use it for natural-language questions or health checks.
- Selector: `action`; default `run`; values `run`, `spawn`, `preview`, `status`, `inspect`, `claim`, `acknowledge`, `cancel`, `list`, `notifications`, `retry`, `repair`, `chain`
- Required args: (none)

Example input:

```json
{
  "action": "list"
}
```

#### `praxis_workflow_validate`

- Surface: `workflow`
- Tier: `advanced`
- Badges: `advanced`, `workflow`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_workflow_validate`
- CLI schema help: `workflow tools describe praxis_workflow_validate`
- When to use: Validate a workflow spec before launching it.
- When not to use: Do not use it when you need to actually run the workflow.
- Selector: none
- Required args: `spec_path`

Example input:

```json
{
  "spec_path": "Code&DBs/Workflow/artifacts/workflow/operating_model_paradigm.queue.json"
}
```
