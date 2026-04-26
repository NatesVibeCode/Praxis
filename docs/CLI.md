# Praxis CLI Surface

The authoritative operator front door is `praxis workflow`.

This file is generated from the MCP/catalog metadata used by `workflow tools`.
If it disagrees with runtime output, trust the runtime and regenerate this file.

## Discovery Commands

- `praxis workflow tools list`
- `praxis workflow tools search <text> [--exact]`
- `praxis workflow tools describe <tool|alias|entrypoint>`
- `praxis workflow tools call <tool|alias|entrypoint> --input-json '{...}'`
- `praxis workflow routes --json` for the live HTTP API route catalog

## Stable Aliases

| Command | Tool | Surface | Risk | When To Use |
| --- | --- | --- | --- | --- |
| `praxis workflow discover` | `praxis_discover` | `code` | `read`, `write` | Search for existing code by behavior with hybrid retrieval before building something new. |
| `praxis workflow data` | `praxis_data` | `data` | `launch`, `read`, `write` | Run deterministic parsing, normalization, validation, mapping, dedupe, or reconcile jobs and optionally launch them through the workflow engine. |
| `praxis workflow artifacts` | `praxis_artifacts` | `evidence` | `read` | Browse sandbox outputs, search artifact paths, or compare generated files. |
| `praxis workflow bugs` | `praxis_bugs` | `evidence` | `launch`, `read`, `write` | Inspect the bug tracker, run keyword or hybrid search, file a new bug, or drive replay-ready bug workflows. |
| `praxis workflow integration` | `praxis_integration` | `integration` | `launch`, `read`, `write` | List integrations, inspect one, validate credentials, or invoke an integration action. |
| `praxis workflow recall` | `praxis_recall` | `knowledge` | `read` | Search the knowledge graph for decisions, patterns, entities, and prior analysis using ranked text, graph, and vector retrieval. |
| `praxis workflow heartbeat` | `praxis_daily_heartbeat` | `operations` | `read` | Run the daily external-health probe across providers, connectors, credentials, and MCP servers. |
| `praxis workflow orient` | `praxis_orient` | `operations` | `read` | Wake up against Praxis and get standing orders, authority envelope, tool guidance, and endpoints in one call. |
| `praxis workflow circuits` | `praxis_circuits` | `operations` | `read`, `write` | Inspect effective circuit-breaker state or apply a durable manual override for one provider. |
| `praxis workflow dataset` | `praxis_dataset` | `operations` | `read`, `write` | Curate, score, and promote evidence-linked training/eval data per specialist; export reproducible JSONL with manifest hashes. |
| `praxis workflow diagnose` | `praxis_diagnose` | `operations` | `read` | Diagnose one workflow run by id and combine receipt, failure, and provider health context. |
| `praxis workflow health` | `praxis_health` | `operations` | `read` | Run a full preflight before workflow launch or when the platform feels degraded. |
| `praxis workflow query` | `praxis_query` | `query` | `read` | Route a natural-language question to the right platform subsystem from the terminal when you are not sure which exact tool to use. |
| `praxis workflow approve-plan` | `praxis_approve_proposed_plan` | `workflow` | `read` | Approve a ProposedPlan so launch_approved can submit it. Wraps the proposal with approved_by + timestamp + hash; the hash binds the approval to the exact spec_dict so tampering between approve and launch fails closed. |
| `praxis workflow bind-pills` | `praxis_bind_data_pills` | `workflow` | `read` | Suggest likely object.field data-pill candidates from loose prose and validate explicit references against the data dictionary authority. Layer 1 (Bind) of the planning stack — call BEFORE decomposing intent into packets so every field ref is either confirmed or surfaced as a candidate to confirm. |
| `praxis workflow compile` | `praxis_compile` | `workflow` | `read`, `write` | Shared CQRS compile front door for MCP/CLI/API parity. Use action='preview' to recognize messy prose without mutation, or action='materialize' to create or update draft workflow build state. |
| `praxis workflow compose-plan` | `praxis_compose_plan` | `workflow` | `read` | Turn prose intent with explicit step markers into a ProposedPlan in one call — chains Layer 2 (decompose) → Layer 1 (bind) → Layer 5 (translate + preview). Compose with approve-plan + launch-plan(approved_plan=...) for the full approval-gated flow. |
| `praxis workflow decompose` | `praxis_decompose_intent` | `workflow` | `read` | Split prose intent into ordered steps by parsing explicit markers (numbered lists, bulleted lists, or first/then/finally ordering). Layer 2 (Decompose) of the planning stack — call before turning steps into PlanPackets. |
| `praxis workflow launch-plan` | `praxis_launch_plan` | `workflow` | `write` | Translate an already-planned packet list into a workflow spec and submit it (or preview first with preview_only=true). This is the layer-5 translation primitive — caller still owns upstream planning (extract data pills, decompose prose, reorder by data-flow, author per-step prompts). |
| `praxis workflow plan-history` | `praxis_plan_lifecycle` | `workflow` | `read` | Read every plan.* event for one workflow_id in chronological order — composed, approved, launched, or blocked. The Q-side read of the planning stack's CQRS pattern. |
| `praxis workflow project-budget` | `praxis_project_plan_budget` | `workflow` | `read` | Estimate token budgets for a ProposedPlan before approving. Honest projection — prompt tokens are char-based, output tokens are a per-stage upper bound, no USD cost. |
| `praxis workflow ship-intent` | `praxis_compose_and_launch` | `workflow` | `launch` | End-to-end: prose intent → ProposedPlan → ApprovedPlan → LaunchReceipt in one call. For trusted automation (CI, scripts, experienced operators). Fails closed by default on unresolved routes, unbound pills, or budget-cap overrun. |
| `praxis workflow suggest-atoms` | `praxis_suggest_plan_atoms` | `workflow` | `read` | Free prose (any length, no markers, no order) should yield candidate data pills, candidate step types, and candidate input parameters as three independent suggestion streams. Layer 0 (Suggest) of the planning stack — call when the prose has no explicit step markers and the downstream LLM author needs atoms to plan from. |

## Full Catalog Entrypoints

### Code

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow discover` | `praxis_discover` | `stable` | action: search, reindex, stats, stale_check | `read`, `write` |

### Data

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow data` | `praxis_data` | `stable` | action: parse, profile, filter, sort, normalize, repair, repair_loop, backfill, redact, checkpoint, replay, approve, apply, validate, transform, join, merge, aggregate, split, export, dead_letter, dedupe, reconcile, sync, run, workflow_spec, launch | `launch`, `read`, `write` |

### Evidence

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_constraints` | `praxis_constraints` | `advanced` | action: list, for_scope | `read` |
| `praxis workflow tools call praxis_friction` | `praxis_friction` | `advanced` | action: stats, list, patterns | `read` |
| `praxis workflow tools call praxis_receipts` | `praxis_receipts` | `advanced` | action: search, token_burn | `read` |
| `praxis workflow artifacts` | `praxis_artifacts` | `stable` | action: stats, list, search, diff | `read` |
| `praxis workflow bugs` | `praxis_bugs` | `stable` | action: list, file, search, duplicate_check, stats, packet, history, replay, backfill_replay, attach_evidence, patch_resume, resolve | `launch`, `read`, `write` |

### General

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_audit_primitive` | `praxis_audit_primitive` | `advanced` | action: playbook, registered, plan, apply, contracts, execute_contract, execute_all_contracts | `read` |
| `praxis workflow tools call praxis_data_dictionary` | `praxis_data_dictionary` | `advanced` | action: list, describe, set_override, clear_override, reproject | `read` |
| `praxis workflow tools call praxis_data_dictionary_classifications` | `praxis_data_dictionary_classifications` | `advanced` | action: summary, describe, by_tag, tags, set, clear, reproject | `read` |
| `praxis workflow tools call praxis_data_dictionary_drift` | `praxis_data_dictionary_drift` | `advanced` | action: latest, snapshot, history, diff | `read` |
| `praxis workflow tools call praxis_data_dictionary_governance` | `praxis_data_dictionary_governance` | `advanced` | action: scan, enforce, scorecard, remediate, cluster, scans, scan_detail, scans_for_bug, pending, drain | `read` |
| `praxis workflow tools call praxis_data_dictionary_impact` | `praxis_data_dictionary_impact` | `advanced` | - | `read` |
| `praxis workflow tools call praxis_data_dictionary_lineage` | `praxis_data_dictionary_lineage` | `advanced` | action: summary, describe, impact, set_edge, clear_edge, reproject | `read` |
| `praxis workflow tools call praxis_data_dictionary_quality` | `praxis_data_dictionary_quality` | `advanced` | action: summary, list_rules, list_runs, run_history, set, clear, evaluate, reproject | `read` |
| `praxis workflow tools call praxis_data_dictionary_stewardship` | `praxis_data_dictionary_stewardship` | `advanced` | action: summary, describe, by_steward, set, clear, reproject | `read` |
| `praxis workflow tools call praxis_data_dictionary_wiring_audit` | `praxis_data_dictionary_wiring_audit` | `advanced` | action: all, hard_paths, decisions, orphans, trend | `read` |

### Governance

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_governance` | `praxis_governance` | `advanced` | action: scan_prompt, scan_scope | `read` |
| `praxis workflow tools call praxis_heal` | `praxis_heal` | `advanced` | - | `read` |

### Integration

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow integration` | `praxis_integration` | `advanced` | action: call, list, describe, test_credentials, health, create, set_secret, reload | `launch`, `read`, `write` |
| `praxis workflow tools call praxis_provider_onboard` | `praxis_provider_onboard` | `advanced` | action: probe, onboard | `read`, `write` |

### Knowledge

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_graph` | `praxis_graph` | `advanced` | - | `read` |
| `praxis workflow tools call praxis_ingest` | `praxis_ingest` | `advanced` | - | `write` |
| `praxis workflow tools call praxis_story` | `praxis_story` | `advanced` | - | `read` |
| `praxis workflow recall` | `praxis_recall` | `stable` | - | `read` |
| `praxis workflow tools call praxis_research` | `praxis_research` | `stable` | action: search | `read` |

### Operations

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow heartbeat` | `praxis_daily_heartbeat` | `advanced` | - | `read` |
| `praxis workflow tools call praxis_authority_memory_refresh` | `praxis_authority_memory_refresh` | `advanced` | - | `write` |
| `praxis workflow tools call praxis_bug_replay_provenance_backfill` | `praxis_bug_replay_provenance_backfill` | `advanced` | - | `write` |
| `praxis workflow tools call praxis_heartbeat` | `praxis_heartbeat` | `advanced` | action: run, status | `read`, `write` |
| `praxis workflow tools call praxis_metrics_reset` | `praxis_metrics_reset` | `advanced` | - | `write` |
| `praxis workflow tools call praxis_reload` | `praxis_reload` | `advanced` | - | `write` |
| `praxis workflow tools call praxis_semantic_bridges_backfill` | `praxis_semantic_bridges_backfill` | `advanced` | - | `write` |
| `praxis workflow tools call praxis_semantic_projection_refresh` | `praxis_semantic_projection_refresh` | `advanced` | - | `write` |
| `praxis workflow tools call praxis_status_snapshot` | `praxis_status_snapshot` | `advanced` | - | `read` |
| `praxis workflow orient` | `praxis_orient` | `curated` | - | `read` |
| `praxis workflow circuits` | `praxis_circuits` | `stable` | action: list, history, open, close, reset | `read`, `write` |
| `praxis workflow dataset` | `praxis_dataset` | `stable` | action: summary, candidates_scan, candidates_list, candidate_inspect, candidate_promote, candidate_reject, inbox, preference_suggest, preference_create, eval_add, promotion_supersede, promotions_list, policy_list, policy_show, policy_record, lineage, manifests_list, export, stale_reconcile, projection_refresh | `read`, `write` |
| `praxis workflow diagnose` | `praxis_diagnose` | `stable` | - | `read` |
| `praxis workflow health` | `praxis_health` | `stable` | - | `read` |

### Operator

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_graph_projection` | `praxis_graph_projection` | `advanced` | - | `read` |
| `praxis workflow tools call praxis_issue_backlog` | `praxis_issue_backlog` | `advanced` | - | `read` |
| `praxis workflow tools call praxis_operator_architecture_policy` | `praxis_operator_architecture_policy` | `advanced` | - | `write` |
| `praxis workflow tools call praxis_operator_closeout` | `praxis_operator_closeout` | `advanced` | action: preview, commit | `read`, `write` |
| `praxis workflow tools call praxis_operator_decisions` | `praxis_operator_decisions` | `advanced` | action: list, record | `read`, `write` |
| `praxis workflow tools call praxis_operator_ideas` | `praxis_operator_ideas` | `advanced` | action: list, file, resolve, promote | `read`, `write` |
| `praxis workflow tools call praxis_operator_native_primary_cutover_gate` | `praxis_operator_native_primary_cutover_gate` | `advanced` | - | `write` |
| `praxis workflow tools call praxis_operator_relations` | `praxis_operator_relations` | `advanced` | action: record_functional_area, record_relation | `write` |
| `praxis workflow tools call praxis_operator_roadmap_view` | `praxis_operator_roadmap_view` | `advanced` | - | `read` |
| `praxis workflow tools call praxis_operator_write` | `praxis_operator_write` | `advanced` | action: preview, validate, commit, update, retire, re_parent, reparent | `read`, `write` |
| `praxis workflow tools call praxis_replay_ready_bugs` | `praxis_replay_ready_bugs` | `advanced` | - | `read` |
| `praxis workflow tools call praxis_run_graph` | `praxis_run_graph` | `advanced` | - | `read` |
| `praxis workflow tools call praxis_run_lineage` | `praxis_run_lineage` | `advanced` | - | `read` |
| `praxis workflow tools call praxis_run_scoreboard` | `praxis_run_scoreboard` | `advanced` | - | `read` |
| `praxis workflow tools call praxis_run_status` | `praxis_run_status` | `advanced` | - | `read` |
| `praxis workflow tools call praxis_semantic_assertions` | `praxis_semantic_assertions` | `advanced` | action: list, register_predicate, record_assertion, retract_assertion | `read`, `write` |
| `praxis workflow tools call praxis_ui_experience_graph` | `praxis_ui_experience_graph` | `advanced` | - | `read` |

### Planning

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_manifest_generate` | `praxis_manifest_generate` | `advanced` | - | `write` |
| `praxis workflow tools call praxis_manifest_refine` | `praxis_manifest_refine` | `advanced` | - | `write` |
| `praxis workflow tools call praxis_session` | `praxis_session` | `advanced` | action: latest, validate | `read` |
| `praxis workflow tools call praxis_decompose` | `praxis_decompose` | `stable` | - | `read` |
| `praxis workflow tools call praxis_intent_match` | `praxis_intent_match` | `stable` | - | `read` |

### Query

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow query` | `praxis_query` | `stable` | - | `read` |

### Research

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_research_workflow` | `praxis_research_workflow` | `advanced` | action: run, list | `launch`, `read` |

### Session

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_context_shard` | `praxis_context_shard` | `session` | view: full, summary, sections | `session` |
| `praxis workflow tools call praxis_session_context` | `praxis_session_context` | `session` | action: read, write | `session` |
| `praxis workflow tools call praxis_subscribe_events` | `praxis_subscribe_events` | `session` | - | `session` |

### Setup

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_setup` | `praxis_setup` | `core` | action: doctor, plan, apply, graph | `read` |

### Submissions

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_get_submission` | `praxis_get_submission` | `session` | - | `session` |
| `praxis workflow tools call praxis_review_submission` | `praxis_review_submission` | `session` | - | `session` |
| `praxis workflow tools call praxis_submit_artifact_bundle` | `praxis_submit_artifact_bundle` | `session` | - | `session` |
| `praxis workflow tools call praxis_submit_code_change` | `praxis_submit_code_change` | `session` | - | `session` |
| `praxis workflow tools call praxis_submit_research_result` | `praxis_submit_research_result` | `session` | - | `session` |

### Workflow

| Entrypoint | Tool | Tier | Selector | Risks |
| --- | --- | --- | --- | --- |
| `praxis workflow tools call praxis_compose_plan_via_llm` | `praxis_compose_plan_via_llm` | `advanced` | - | `launch` |
| `praxis workflow tools call praxis_connector` | `praxis_connector` | `advanced` | action: build, list, get, register, verify | `launch`, `read`, `write` |
| `praxis workflow tools call praxis_synthesize_skeleton` | `praxis_synthesize_skeleton` | `advanced` | - | `read` |
| `praxis workflow tools call praxis_wave` | `praxis_wave` | `advanced` | action: observe, start, next, record | `launch`, `read`, `write` |
| `praxis workflow tools call praxis_workflow` | `praxis_workflow` | `advanced` | action: run, spawn, preview, status, inspect, claim, acknowledge, cancel, list, notifications, retry, repair, chain | `launch`, `read`, `write` |
| `praxis workflow tools call praxis_workflow_validate` | `praxis_workflow_validate` | `advanced` | - | `read` |
| `praxis workflow approve-plan` | `praxis_approve_proposed_plan` | `stable` | - | `read` |
| `praxis workflow bind-pills` | `praxis_bind_data_pills` | `stable` | - | `read` |
| `praxis workflow compile` | `praxis_compile` | `stable` | action: preview, materialize | `read`, `write` |
| `praxis workflow compose-plan` | `praxis_compose_plan` | `stable` | - | `read` |
| `praxis workflow decompose` | `praxis_decompose_intent` | `stable` | - | `read` |
| `praxis workflow launch-plan` | `praxis_launch_plan` | `stable` | - | `write` |
| `praxis workflow plan-history` | `praxis_plan_lifecycle` | `stable` | - | `read` |
| `praxis workflow project-budget` | `praxis_project_plan_budget` | `stable` | - | `read` |
| `praxis workflow ship-intent` | `praxis_compose_and_launch` | `stable` | - | `launch` |
| `praxis workflow suggest-atoms` | `praxis_suggest_plan_atoms` | `stable` | - | `read` |
