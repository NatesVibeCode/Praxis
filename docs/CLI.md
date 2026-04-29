# Praxis CLI Surface

The authoritative operator front door is `praxis workflow`.

This file is generated from the MCP/catalog metadata used by `workflow tools`.
If it disagrees with runtime output, trust the runtime and regenerate this file.
Canonical regeneration command: `PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m scripts.generate_mcp_docs`.

## CQRS Gateway

The operation catalog gateway is the CQRS write/read front door when you already know the operation name.

- `praxis workflow operate catalog`
- `praxis workflow operate call <operation_name> --input-json '{...}'`
- `praxis workflow operate query <operation_name> --input-json '{...}'`
- `praxis workflow operate command <operation_name> --input-json '{...}'`

## Discovery Commands

- `praxis workflow tools list`
- `praxis workflow tools search <text> [--exact]`
- `praxis workflow tools describe <tool|alias|entrypoint>`
- `praxis workflow tools call <tool|alias|entrypoint> --input-json '{...}'`
- `praxis workflow routes --json` for the live HTTP API route catalog
- `praxis workflow instances --include-routes` to validate `/api/operate` and `/api/catalog/operations` wiring

## Stable Aliases

| Command | Tool | Surface | Risk | When To Use |
| --- | --- | --- | --- | --- |
| `praxis workflow discover` | `praxis_discover` | `code` | `read`, `write` | Search for existing code by behavior with hybrid retrieval before building something new. |
| `praxis workflow data` | `praxis_data` | `data` | `launch`, `read`, `write` | Run deterministic parsing, normalization, validation, mapping, dedupe, or reconcile jobs and optionally launch them through the workflow engine. |
| `praxis workflow artifacts` | `praxis_artifacts` | `evidence` | `read` | Browse sandbox outputs, search artifact paths, or compare generated files. |
| `praxis workflow bugs` | `praxis_bugs` | `evidence` | `launch`, `read`, `write` | Inspect the bug tracker, run keyword or hybrid search, file a new bug, or drive replay-ready bug workflows. |
| `praxis workflow integration` | `praxis_integration` | `integration` | `launch`, `read`, `write` | List integrations, inspect one, validate credentials, or invoke an integration action. |
| `praxis workflow recall` | `praxis_recall` | `knowledge` | `read` | Search the knowledge graph for decisions, patterns, entities, and prior analysis using ranked text, graph, and vector retrieval. |
| `praxis workflow search` | `praxis_search` | `knowledge` | `read` | Federated search across code, decisions, knowledge, bugs, receipts, and related sources with semantic, exact, or regex modes — prefer this as the default discovery entry point. |
| `praxis workflow authority-domain-forge` | `praxis_authority_domain_forge` | `operations` | `read` | Preview authority-domain ownership before creating a new authority boundary or attaching operations, tables, workflows, or MCP tools to it. Use this before register-operation when the owning authority is not already explicit. |
| `praxis workflow evolve-operation-field` | `praxis_evolve_operation_field` | `operations` | `read` | Plan how to add one optional field to an existing CQRS operation's input model (checklist of files and edits). v1 is plan-only — you still apply diffs locally. |
| `praxis workflow heartbeat` | `praxis_daily_heartbeat` | `operations` | `write` | Run the daily external-health probe across providers, connectors, credentials, and MCP servers. |
| `praxis workflow object-truth` | `praxis_object_truth` | `operations` | `read` | Build deterministic object-truth evidence for one inline external record: identity digest, field observations, value digests, source metadata, hierarchy signals, and redaction-safe previews. |
| `praxis workflow object-truth-compare` | `praxis_object_truth_compare_versions` | `operations` | `read` | Compare two persisted object-truth object versions by digest to see matching, different, missing, and freshness signals. |
| `praxis workflow object-truth-record-comparison` | `praxis_object_truth_record_comparison_run` | `operations` | `write` | Persist a comparison result between two stored object versions so future runs can query the evidence instead of recomputing it. |
| `praxis workflow object-truth-store` | `praxis_object_truth_store` | `operations` | `write` | Persist deterministic object-truth evidence for one inline external record after the authority domain and evidence tables exist. |
| `praxis workflow object-truth-store-schema` | `praxis_object_truth_store_schema_snapshot` | `operations` | `write` | Persist normalized schema evidence for one external object before record sampling or comparison work references a schema digest. |
| `praxis workflow operation-forge` | `praxis_operation_forge` | `operations` | `read` | Preview the CQRS operation/tool registration path before adding a new operation or MCP wrapper. Use it to get the exact register payload, tool binding, fast-feedback commands, and command/query defaults. |
| `praxis workflow register-authority-domain` | `praxis_register_authority_domain` | `operations` | `write` | Register or update an authority domain after the forge confirms the domain is the right owner of durable truth. This creates the domain before operations, tables, workflows, or MCP tools attach to it. |
| `praxis workflow register-operation` | `praxis_register_operation` | `operations` | `write` | Register a net-new CQRS operation (gateway dispatch key + handler + Pydantic input) through the catalog without hand-authoring a migration for the triple write. |
| `praxis workflow retire-operation` | `praxis_retire_operation` | `operations` | `write` | Soft-retire an operation (disable gateway binding, mark authority object deprecated) while keeping rows for receipts and audit continuity. |
| `praxis workflow task-route-eligibility` | `praxis_task_route_eligibility` | `operations` | `write` | Allow or reject one provider/model candidate for one task type through a bounded eligibility window. Use this for by-task routing policy such as letting anthropic/claude-sonnet-4-6 participate in build or review without enabling it everywhere. |
| `praxis workflow task-route-request` | `praxis_task_route_request` | `operations` | `write` | Mutate request-shape knobs for one task route through CQRS authority: temperature, max_tokens, reasoning_control, request_contract_ref, cache policy, structured-output policy, or streaming policy. |
| `praxis workflow orient` | `praxis_orient` | `operations` | `read` | Wake up against Praxis and get standing orders, authority envelope, tool guidance, and endpoints in one call. |
| `praxis workflow circuits` | `praxis_circuits` | `operations` | `read`, `write` | Inspect effective circuit-breaker state or apply a durable manual override for one provider. |
| `praxis workflow dataset` | `praxis_dataset` | `operations` | `read`, `write` | Curate, score, and promote evidence-linked training/eval data per specialist; export reproducible JSONL with manifest hashes. |
| `praxis workflow diagnose` | `praxis_diagnose` | `operations` | `read` | Diagnose one workflow run by id and combine receipt, failure, and provider health context. |
| `praxis workflow firecheck` | `praxis_firecheck` | `operations` | `read` | Run before launching or retrying workflows to prove work can actually fire, including typed blockers and remediation plans. |
| `praxis workflow health` | `praxis_health` | `operations` | `read` | Run a full preflight before workflow launch or when the platform feels degraded. |
| `praxis workflow provider-control-plane` | `praxis_provider_control_plane` | `operations` | `read` | Inspect the private provider/job/model matrix, including CLI/API type, cost, version, runnable state, breaker state, credential state, and removal reasons. |
| `praxis workflow remediation-apply` | `praxis_remediation_apply` | `operations` | `write` | Apply only guarded local runtime repairs, such as stale provider slot cleanup or expired host-resource lease cleanup, before one explicit retry. |
| `praxis workflow remediation-plan` | `praxis_remediation_plan` | `operations` | `read` | Explain the safe remediation tier, evidence requirements, approval gate, and retry delta for a typed workflow failure. |
| `praxis workflow runtime-truth` | `praxis_runtime_truth_snapshot` | `operations` | `read` | Inspect observed workflow runtime truth across DB authority, queue state, worker heartbeats, provider slots, host-resource leases, Docker, manifest hydration audit, and recent typed failures. |
| `praxis workflow query` | `praxis_query` | `query` | `read` | Route a natural-language question to the right platform subsystem from the terminal when you are not sure which exact tool to use. |
| `praxis workflow moon` | `praxis_moon` | `workflow` | `launch`, `read`, `write` | Read, compose, suggest, mutate, or launch Moon workflow graphs through the same CQRS-backed build authority used by the in-app Moon surface. |
| `praxis workflow approve-plan` | `praxis_approve_proposed_plan` | `workflow` | `read` | Approve a ProposedPlan so launch_approved can submit it. Wraps the proposal with approved_by + timestamp + hash; the hash binds the approval to the exact spec_dict so tampering between approve and launch fails closed. The proposed plan must already carry machine-checkable provider freshness evidence with fresh route truth. |
| `praxis workflow bind-pills` | `praxis_bind_data_pills` | `workflow` | `read` | Suggest likely object.field data-pill candidates from loose prose and validate explicit references against the data dictionary authority. Layer 1 (Bind) of the planning stack — call BEFORE decomposing intent into packets so every field ref is either confirmed or surfaced as a candidate to confirm. |
| `praxis workflow compose-plan` | `praxis_compose_plan` | `workflow` | `read` | Turn prose intent with explicit step markers into a ProposedPlan in one call — chains Layer 2 (decompose) → Layer 1 (bind) → Layer 5 (translate + preview). Compose with approve-plan + launch-plan(approved_plan=...) for the full approval-gated flow. |
| `praxis workflow decompose` | `praxis_decompose_intent` | `workflow` | `read` | Split prose intent into ordered steps by parsing explicit markers (numbered lists, bulleted lists, or first/then/finally ordering). Layer 2 (Decompose) of the planning stack — call before turning steps into PlanPackets. |
| `praxis workflow generate-plan` | `praxis_generate_plan` | `workflow` | `read`, `write` | Shared CQRS plan-generation front door for MCP/CLI/API parity. Use action='generate_plan' to recognize messy prose without mutation, or action='materialize_plan' to create or update draft workflow build state. |
| `praxis workflow launch-plan` | `praxis_launch_plan` | `workflow` | `write` | Translate an already-planned packet list into a workflow spec and submit it (or preview first with preview_only=true). This is the layer-5 translation primitive — caller still owns upstream planning (extract data pills, decompose prose, reorder by data-flow, author per-step prompts). Proof launches must carry fresh provider route truth or a recent provider availability refresh receipt before approval. |
| `praxis workflow plan-history` | `praxis_plan_lifecycle` | `workflow` | `read` | Read every plan.* event for one workflow_id in chronological order — composed, approved, launched, or blocked. The Q-side read of the planning stack's CQRS pattern. |
| `praxis workflow ship-intent` | `praxis_compose_and_launch` | `workflow` | `launch` | End-to-end: prose intent → ProposedPlan → ApprovedPlan → LaunchReceipt in one call. For trusted automation (CI, scripts, experienced operators). Fails closed by default on unresolved routes, unbound pills, or invalid approvals. |
| `praxis workflow suggest-atoms` | `praxis_suggest_plan_atoms` | `workflow` | `read` | Free prose (any length, no markers, no order) should yield candidate data pills, candidate step types, and candidate input parameters as three independent suggestion streams. Layer 0 (Suggest) of the planning stack — call when the prose has no explicit step markers and the downstream LLM author needs atoms to plan from. |

## Full Catalog Entrypoints

### Code

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow discover` | `praxis_discover` | `stable` | action: search, reindex, stats, stale_check | `read`, `write` | - |

### Data

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow data` | `praxis_data` | `stable` | action: parse, profile, filter, sort, normalize, repair, repair_loop, backfill, redact, checkpoint, replay, approve, apply, validate, transform, join, merge, aggregate, split, export, dead_letter, dedupe, reconcile, sync, run, workflow_spec, launch | `launch`, `read`, `write` | - |

### Evidence

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_constraints` | `praxis_constraints` | `advanced` | action: list, for_scope | `read` | - |
| `praxis workflow tools call praxis_friction` | `praxis_friction` | `advanced` | action: stats, list, patterns, record | `read` | - |
| `praxis workflow tools call praxis_receipts` | `praxis_receipts` | `advanced` | action: search, token_burn | `read` | - |
| `praxis workflow artifacts` | `praxis_artifacts` | `stable` | action: stats, list, search, diff | `read` | - |
| `praxis workflow bugs` | `praxis_bugs` | `stable` | action: list, file, search, duplicate_check, stats, show, packet, history, replay, backfill_replay, attach_evidence, patch_resume, resolve | `launch`, `read`, `write` | - |
| `praxis workflow tools call praxis_patterns` | `praxis_patterns` | `stable` | action: list, candidates, evidence, materialize | `read`, `write` | - |

### General

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_audit_primitive` | `praxis_audit_primitive` | `advanced` | action: playbook, registered, plan, apply, contracts, execute_contract, execute_all_contracts | `read` | - |
| `praxis workflow tools call praxis_data_dictionary` | `praxis_data_dictionary` | `advanced` | action: list, describe, set_override, clear_override, reproject | `read` | - |
| `praxis workflow tools call praxis_data_dictionary_classifications` | `praxis_data_dictionary_classifications` | `advanced` | action: summary, describe, by_tag, tags, set, clear, reproject | `read` | - |
| `praxis workflow tools call praxis_data_dictionary_drift` | `praxis_data_dictionary_drift` | `advanced` | action: latest, snapshot, history, diff | `read` | - |
| `praxis workflow tools call praxis_data_dictionary_governance` | `praxis_data_dictionary_governance` | `advanced` | action: scan, enforce, scorecard, remediate, cluster, scans, scan_detail, scans_for_bug, pending, drain | `read` | - |
| `praxis workflow tools call praxis_data_dictionary_impact` | `praxis_data_dictionary_impact` | `advanced` | - | `read` | - |
| `praxis workflow tools call praxis_data_dictionary_lineage` | `praxis_data_dictionary_lineage` | `advanced` | action: summary, describe, impact, set_edge, clear_edge, reproject | `read` | - |
| `praxis workflow tools call praxis_data_dictionary_quality` | `praxis_data_dictionary_quality` | `advanced` | action: summary, list_rules, list_runs, run_history, set, clear, evaluate, reproject | `read` | - |
| `praxis workflow tools call praxis_data_dictionary_stewardship` | `praxis_data_dictionary_stewardship` | `advanced` | action: summary, describe, by_steward, set, clear, reproject | `read` | - |
| `praxis workflow tools call praxis_data_dictionary_wiring_audit` | `praxis_data_dictionary_wiring_audit` | `advanced` | action: all, hard_paths, decisions, orphans, trend | `read` | - |

### Governance

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_governance` | `praxis_governance` | `advanced` | action: scan_prompt, scan_scope | `read` | - |
| `praxis workflow tools call praxis_heal` | `praxis_heal` | `advanced` | - | `read` | - |

### Integration

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow integration` | `praxis_integration` | `advanced` | action: call, list, describe, test_credentials, health, create, set_secret, reload | `launch`, `read`, `write` | - |
| `praxis workflow tools call praxis_match_rules_backfill` | `praxis_match_rules_backfill` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_provider_onboard` | `praxis_provider_onboard` | `advanced` | action: probe, onboard | `read`, `write` | - |
| `praxis workflow tools call praxis_cli_auth_doctor` | `praxis_cli_auth_doctor` | `stable` | - | `read` | - |

### Knowledge

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_graph` | `praxis_graph` | `advanced` | - | `read` | - |
| `praxis workflow tools call praxis_ingest` | `praxis_ingest` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_story` | `praxis_story` | `advanced` | - | `read` | - |
| `praxis workflow recall` | `praxis_recall` | `stable` | - | `read` | - |
| `praxis workflow search` | `praxis_search` | `stable` | - | `read` | - |
| `praxis workflow tools call praxis_research` | `praxis_research` | `stable` | action: search | `read` | - |

### Operations

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow authority-domain-forge` | `praxis_authority_domain_forge` | `advanced` | - | `read` | - |
| `praxis workflow evolve-operation-field` | `praxis_evolve_operation_field` | `advanced` | - | `read` | - |
| `praxis workflow heartbeat` | `praxis_daily_heartbeat` | `advanced` | - | `write` | - |
| `praxis workflow object-truth` | `praxis_object_truth` | `advanced` | - | `read` | - |
| `praxis workflow object-truth-compare` | `praxis_object_truth_compare_versions` | `advanced` | - | `read` | - |
| `praxis workflow object-truth-record-comparison` | `praxis_object_truth_record_comparison_run` | `advanced` | - | `write` | - |
| `praxis workflow object-truth-store` | `praxis_object_truth_store` | `advanced` | - | `write` | - |
| `praxis workflow object-truth-store-schema` | `praxis_object_truth_store_schema_snapshot` | `advanced` | - | `write` | - |
| `praxis workflow operation-forge` | `praxis_operation_forge` | `advanced` | - | `read` | - |
| `praxis workflow register-authority-domain` | `praxis_register_authority_domain` | `advanced` | - | `write` | - |
| `praxis workflow register-operation` | `praxis_register_operation` | `advanced` | - | `write` | - |
| `praxis workflow retire-operation` | `praxis_retire_operation` | `advanced` | - | `write` | - |
| `praxis workflow task-route-eligibility` | `praxis_task_route_eligibility` | `advanced` | - | `write` | - |
| `praxis workflow task-route-request` | `praxis_task_route_request` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_access_control` | `praxis_access_control` | `advanced` | action: list, disable, enable | `read`, `write` | - |
| `praxis workflow tools call praxis_authority_memory_refresh` | `praxis_authority_memory_refresh` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_bug_replay_provenance_backfill` | `praxis_bug_replay_provenance_backfill` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_heartbeat` | `praxis_heartbeat` | `advanced` | action: run, status | `read`, `write` | - |
| `praxis workflow tools call praxis_metrics_reset` | `praxis_metrics_reset` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_provider_availability_refresh` | `praxis_provider_availability_refresh` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_reload` | `praxis_reload` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_semantic_bridges_backfill` | `praxis_semantic_bridges_backfill` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_semantic_projection_refresh` | `praxis_semantic_projection_refresh` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_status_snapshot` | `praxis_status_snapshot` | `advanced` | - | `read` | - |
| `praxis workflow orient` | `praxis_orient` | `curated` | - | `read` | - |
| `praxis workflow circuits` | `praxis_circuits` | `stable` | action: list, history, open, close, reset | `read`, `write` | - |
| `praxis workflow dataset` | `praxis_dataset` | `stable` | action: summary, candidates_scan, candidates_list, candidate_inspect, candidate_promote, candidate_reject, inbox, preference_suggest, preference_create, eval_add, promotion_supersede, promotions_list, policy_list, policy_show, policy_record, lineage, manifests_list, export, stale_reconcile, projection_refresh | `read`, `write` | - |
| `praxis workflow diagnose` | `praxis_diagnose` | `stable` | - | `read` | - |
| `praxis workflow firecheck` | `praxis_firecheck` | `stable` | - | `read` | - |
| `praxis workflow health` | `praxis_health` | `stable` | - | `read` | - |
| `praxis workflow provider-control-plane` | `praxis_provider_control_plane` | `stable` | - | `read` | - |
| `praxis workflow remediation-apply` | `praxis_remediation_apply` | `stable` | - | `write` | - |
| `praxis workflow remediation-plan` | `praxis_remediation_plan` | `stable` | - | `read` | - |
| `praxis workflow runtime-truth` | `praxis_runtime_truth_snapshot` | `stable` | - | `read` | - |
| `praxis workflow tools call praxis_execution_truth` | `praxis_execution_truth` | `stable` | - | `read` | - |
| `praxis workflow tools call praxis_model_access_control_matrix` | `praxis_model_access_control_matrix` | `stable` | - | `read` | - |
| `praxis workflow tools call praxis_provider_route_truth` | `praxis_provider_route_truth` | `stable` | - | `read` | - |
| `praxis workflow tools call praxis_work_assignment_matrix` | `praxis_work_assignment_matrix` | `stable` | - | `read` | - |
| `praxis workflow tools call tool_dag_health` | `tool_dag_health` | `stable` | - | `read` | workflow health |

### Operator

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_bug_triage_packet` | `praxis_bug_triage_packet` | `advanced` | - | `read` | - |
| `praxis workflow tools call praxis_execution_proof` | `praxis_execution_proof` | `advanced` | - | `read` | - |
| `praxis workflow tools call praxis_graph_projection` | `praxis_graph_projection` | `advanced` | - | `read` | - |
| `praxis workflow tools call praxis_issue_backlog` | `praxis_issue_backlog` | `advanced` | - | `read` | - |
| `praxis workflow tools call praxis_operator_architecture_policy` | `praxis_operator_architecture_policy` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_operator_closeout` | `praxis_operator_closeout` | `advanced` | action: preview, commit | `read`, `write` | - |
| `praxis workflow tools call praxis_operator_decisions` | `praxis_operator_decisions` | `advanced` | action: list, record | `read`, `write` | praxis_next |
| `praxis workflow tools call praxis_operator_ideas` | `praxis_operator_ideas` | `advanced` | action: list, file, resolve, promote | `read`, `write` | - |
| `praxis workflow tools call praxis_operator_native_primary_cutover_gate` | `praxis_operator_native_primary_cutover_gate` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_operator_relations` | `praxis_operator_relations` | `advanced` | action: record_functional_area, record_relation | `write` | - |
| `praxis workflow tools call praxis_operator_roadmap_view` | `praxis_operator_roadmap_view` | `advanced` | - | `read` | - |
| `praxis workflow tools call praxis_operator_write` | `praxis_operator_write` | `advanced` | action: preview, validate, commit, update, retire, re_parent, reparent | `read`, `write` | - |
| `praxis workflow tools call praxis_replay_ready_bugs` | `praxis_replay_ready_bugs` | `advanced` | - | `read` | - |
| `praxis workflow tools call praxis_run` | `praxis_run` | `advanced` | action: status, scoreboard, graph, lineage | `read` | - |
| `praxis workflow tools call praxis_run_graph` | `praxis_run_graph` | `advanced` | - | `read` | workflow tools call praxis_run --input-json '{"run_id":"<run_id>","action":"graph"}' |
| `praxis workflow tools call praxis_run_lineage` | `praxis_run_lineage` | `advanced` | - | `read` | workflow tools call praxis_run --input-json '{"run_id":"<run_id>","action":"lineage"}' |
| `praxis workflow tools call praxis_run_scoreboard` | `praxis_run_scoreboard` | `advanced` | - | `read` | workflow tools call praxis_run --input-json '{"run_id":"<run_id>","action":"scoreboard"}' |
| `praxis workflow tools call praxis_run_status` | `praxis_run_status` | `advanced` | - | `read` | workflow tools call praxis_run --input-json '{"run_id":"<run_id>","action":"status"}' |
| `praxis workflow tools call praxis_semantic_assertions` | `praxis_semantic_assertions` | `advanced` | action: list, register_predicate, record_assertion, retract_assertion | `read`, `write` | - |
| `praxis workflow tools call praxis_trace` | `praxis_trace` | `advanced` | - | `read` | - |
| `praxis workflow tools call praxis_ui_experience_graph` | `praxis_ui_experience_graph` | `advanced` | - | `read` | - |
| `praxis workflow tools call praxis_next` | `praxis_next` | `stable` | action: next, launch_gate, failure_triage, manifest_audit, toolsmith, unlock_frontier | `read` | - |
| `praxis workflow tools call praxis_next_work` | `praxis_next_work` | `stable` | - | `read` | - |
| `praxis workflow tools call praxis_refactor_heatmap` | `praxis_refactor_heatmap` | `stable` | - | `read` | - |

### Planning

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_manifest_generate` | `praxis_manifest_generate` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_manifest_refine` | `praxis_manifest_refine` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_session` | `praxis_session` | `advanced` | action: latest, validate | `read` | - |
| `praxis workflow tools call praxis_decompose` | `praxis_decompose` | `stable` | - | `read` | - |
| `praxis workflow tools call praxis_intent_match` | `praxis_intent_match` | `stable` | - | `read` | - |

### Query

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow query` | `praxis_query` | `stable` | - | `read` | - |

### Research

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_research_workflow` | `praxis_research_workflow` | `advanced` | action: run, list | `launch`, `read` | - |

### Session

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_context_shard` | `praxis_context_shard` | `session` | view: full, summary, sections | `session` | - |
| `praxis workflow tools call praxis_session_context` | `praxis_session_context` | `session` | action: read, write | `session` | - |
| `praxis workflow tools call praxis_subscribe_events` | `praxis_subscribe_events` | `session` | - | `session` | - |

### Setup

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_setup` | `praxis_setup` | `core` | action: doctor, plan, apply, graph | `read` | - |
| `praxis workflow tools call praxis_credential_capture` | `praxis_credential_capture` | `stable` | action: request, status, capture | `read`, `write` | - |

### Submissions

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_code_change_candidate_materialize` | `praxis_code_change_candidate_materialize` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_code_change_candidate_review` | `praxis_code_change_candidate_review` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_get_submission` | `praxis_get_submission` | `session` | - | `session` | - |
| `praxis workflow tools call praxis_review_submission` | `praxis_review_submission` | `session` | - | `session` | - |
| `praxis workflow tools call praxis_submit_artifact_bundle` | `praxis_submit_artifact_bundle` | `session` | - | `session` | - |
| `praxis workflow tools call praxis_submit_code_change_candidate` | `praxis_submit_code_change_candidate` | `session` | - | `session` | - |
| `praxis workflow tools call praxis_submit_research_result` | `praxis_submit_research_result` | `session` | - | `session` | - |

### Workflow

| Entrypoint | Tool | Tier | Selector | Risks | Replacement |
| --- | --- | --- | --- | --- | --- |
| `praxis workflow moon` | `praxis_moon` | `advanced` | action: get_build, compose, suggest_next, mutate_field, launch | `launch`, `read`, `write` | - |
| `praxis workflow tools call praxis_compose_experiment` | `praxis_compose_experiment` | `advanced` | - | `launch` | - |
| `praxis workflow tools call praxis_compose_plan_via_llm` | `praxis_compose_plan_via_llm` | `advanced` | - | `launch` | - |
| `praxis workflow tools call praxis_connector` | `praxis_connector` | `advanced` | action: build, list, get, register, verify | `launch`, `read`, `write` | - |
| `praxis workflow tools call praxis_promote_experiment_winner` | `praxis_promote_experiment_winner` | `advanced` | - | `write` | - |
| `praxis workflow tools call praxis_synthesize_skeleton` | `praxis_synthesize_skeleton` | `advanced` | - | `read` | - |
| `praxis workflow tools call praxis_wave` | `praxis_wave` | `advanced` | action: observe, start, next, record | `launch`, `read`, `write` | - |
| `praxis workflow tools call praxis_workflow` | `praxis_workflow` | `advanced` | action: run, spawn, preview, status, inspect, claim, acknowledge, cancel, list, notifications, retry, repair, chain | `launch`, `read`, `write` | - |
| `praxis workflow tools call praxis_workflow_validate` | `praxis_workflow_validate` | `advanced` | - | `read` | - |
| `praxis workflow approve-plan` | `praxis_approve_proposed_plan` | `stable` | - | `read` | - |
| `praxis workflow bind-pills` | `praxis_bind_data_pills` | `stable` | - | `read` | - |
| `praxis workflow compose-plan` | `praxis_compose_plan` | `stable` | - | `read` | - |
| `praxis workflow decompose` | `praxis_decompose_intent` | `stable` | - | `read` | - |
| `praxis workflow generate-plan` | `praxis_generate_plan` | `stable` | action: generate_plan, materialize_plan | `read`, `write` | - |
| `praxis workflow launch-plan` | `praxis_launch_plan` | `stable` | - | `write` | - |
| `praxis workflow plan-history` | `praxis_plan_lifecycle` | `stable` | - | `read` | - |
| `praxis workflow ship-intent` | `praxis_compose_and_launch` | `stable` | - | `launch` | - |
| `praxis workflow suggest-atoms` | `praxis_suggest_plan_atoms` | `stable` | - | `read` | - |
