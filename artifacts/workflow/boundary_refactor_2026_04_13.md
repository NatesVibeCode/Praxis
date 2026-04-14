# Boundary Refactor Roadmap — 2026-04-13

## Purpose

Close the 60 DEFERRED `boundary` / `boundary_leak` bugs that Praxis self-filed on 2026-04-09 (`decision_ref: bug_boundary_mining_2026_04_09`) by migrating canonical DB writes out of `runtime/` and `surfaces/` into `storage/postgres/*_repository.py` modules, and by moving policy/assembly out of API surfaces into `authority/` modules.

After the 2026-04-12 sweep, 33 of the original 93 bugs were closed as already-fixed-but-not-resolved. The remaining 60 represent real migration work and are grouped below into coherent waves. Each wave = one `config/cascade/specs/W_boundary_refactor_wave_*_20260413.queue.json` file the Praxis Engine can launch independently.

## Layering target

```
surfaces/api/  ──►  authority/  ──►  runtime/  ──►  storage/postgres/*_repository.py
                                                        │
                                                        ▼
                                            Code&DBs/Databases/migrations/workflow/*.sql
```

Runtime modules remain thin orchestration over repository calls. DDL lives only in migrations. Surfaces never carry SQL or policy assembly.

## Waves

| Wave | Target | Bugs | Lead file(s) to create/extend |
|---|---|---|---|
| A | Memory authority seam | 2 | `storage/postgres/memory_telemetry_repository.py`, extend `memory_graph_repository.py`, migration for `retrieval_metrics` DDL |
| B | Observability & metrics | 5 | `storage/postgres/observability_repository.py` |
| C | Receipt & evidence persistence | 6 | `storage/postgres/receipt_repository.py`, `storage/postgres/evidence_repository.py` |
| D | Worker & execution lifecycle | 7 | extend `workflow_runtime_repository.py`, new `storage/postgres/execution_repository.py` |
| E | Trigger & subscription state | 3 | `storage/postgres/subscription_repository.py`, `storage/postgres/command_repository.py` |
| F | Verifier & friction | 4 | `storage/postgres/verification_repository.py`, `storage/postgres/friction_repository.py` |
| G | Artifact & file storage | 4 | `storage/postgres/artifact_repository.py` |
| H | Catalog & routing | 3 | `storage/postgres/catalog_repository.py` |
| I | Caches & review queue | 3 | `storage/postgres/cache_repository.py`, `storage/postgres/review_repository.py` |
| J | Chat persistence | 1 | `storage/postgres/chat_repository.py` |
| K | Surfaces: operator_write policy | 3 | extract into `authority/operator_control.py` |
| L | Surfaces: frontdoor submission | 3 | runtime `workflow_admission` / authority `instruction_packets` |
| M | Surfaces: handler writes | 3 | extend `storage/postgres/object_lifecycle_repository.py` |
| N | Surfaces: operator_read authority | 3 | authority `operator_read` + `config` + repository |
| O | Client/browser authority leaks | 6 | move to server-side API contracts |
| P | MCP surfaces | 1 | split destructive `reset_metrics` out of observability tool |
| Q | Admin handler policy | 1 | move transport-matrix logic to authority |

## Wave-to-bug mapping

### Wave A — Memory authority (2 bugs)
- `BUG-B981792C01F4` — `memory/retrieval_telemetry.py` creates `retrieval_metrics` and writes rows
- `BUG-EB861DA883EF` — `memory/crud.py` owns `memory_entities` and `memory_edges` writes

### Wave B — Observability & metrics (5 bugs)
- `BUG-B73E47948CC1` — `runtime/observability.py` writes `workflow_metrics`
- `BUG-0C9449F404EB` — `runtime/event_log.py` writes `platform_events` / `event_log_cursors`
- `BUG-789A0F27385F` — `runtime/quality_views.py` writes `quality_rollups`, `agent_profiles`, `failure_catalog`
- `BUG-011038E343EB` — `runtime/debate_metrics.py` writes `debate_round_metrics`, `debate_consensus`
- `BUG-5EFC81856C2C` — `runtime/cost_tracker.py` upserts `workflow_cost_ledger`

### Wave C — Receipt & evidence persistence (6 bugs)
- `BUG-CEC41B042F4F` — `runtime/receipt_store.py` writes `receipts`
- `BUG-A3538875D2D9` — `runtime/workflow/receipt_writer.py` writes `receipts` + `workflow_notifications`
- `BUG-1DEAE0B81178` — `runtime/persistent_evidence.py` writes canonical workflow persistence
- `BUG-41F70113DFC3` / `BUG-07C1BFD40B03` — `runtime/compile_artifacts.py` writes `compile_artifacts` + `execution_packets` (dup bug IDs on same file)
- `BUG-6D489F9683CF` — `runtime/workflow/job_runtime_context.py` writes `workflow_job_runtime_context`

### Wave D — Worker & execution lifecycle (7 bugs)
- `BUG-50ACCE7BAE38` — `runtime/workflow/worker.py` claims and updates `run_nodes`
- `BUG-4502C997A314` — `runtime/workflow/unified.py` writes `workflow_definitions`, `admission_decisions`, `workflow_runs`
- `BUG-3AB2B41B1F6A` — `runtime/workflow/unified.py` owns `workflow_jobs` lifecycle + terminal `system_events`
- `BUG-65BB03A64946` — `runtime/model_executor.py` writes 8 canonical tables
- `BUG-D4A871354DFE` — `runtime/claims.py` writes `workflow_claim_lease_proposal_runtime` + sandbox tables
- `BUG-2EB83612CCB2` — `runtime/execution_leases.py` writes `execution_leases`

### Wave E — Trigger & subscription state (3 bugs)
- `BUG-612F9A07E104` — `runtime/triggers.py` writes subscription + checkpoint + fire-count state
- `BUG-D77FFA613FBA` — `runtime/subscription_repository.py` persists `event_subscriptions`, `subscription_checkpoints`
- `BUG-6760FE9BE1E0` — `runtime/control_commands.py` writes `control_commands` + `system_events`

### Wave F — Verifier & friction (4 bugs)
- `BUG-6FF35361BF49` — `runtime/verification.py` upserts `verify_refs`
- `BUG-DC213265758A` — `runtime/verifier_authority.py` writes `verification_runs`, `healing_runs`
- `BUG-8EE7514C0983` — `runtime/friction_ledger.py` writes friction rows
- `BUG-56E479A6E104` — `runtime/capability_feedback.py` writes `capability_outcomes`

### Wave G — Artifact & file storage (4 bugs)
- `BUG-0E7511AA44D6` — `runtime/sandbox_artifacts.py` writes `sandbox_artifacts`
- `BUG-0AE3B74D83CD` — `runtime/file_storage.py` writes `uploaded_files`
- `BUG-BF1D05739C13` — `runtime/compile_index.py` writes `compile_index_snapshots`
- `BUG-8E762C12E824` — `runtime/repo_snapshot_store.py` writes `repo_snapshots`

### Wave H — Catalog & routing (3 bugs)
- `BUG-3EA4D30C037E` — `runtime/capability_catalog.py` writes `capability_catalog`
- `BUG-58FEF4AC0B5D` — `runtime/task_type_router.py` writes `task_type_routing`
- `BUG-49DE406B89CA` — `runtime/module_indexer.py` writes `module_embeddings`

### Wave I — Caches & review queue (3 bugs)
- `BUG-010099E3219F` — `runtime/result_cache.py` writes `workflow_result_cache`
- `BUG-2DB22C395592` — `runtime/auto_review.py` writes `review_queue`
- `BUG-D45A73DABE47` — `runtime/post_workflow_sync.py` writes `workflow_run_sync_status`

### Wave J — Chat persistence (1 bug)
- `BUG-AD0DB73979AC` — `runtime/chat_orchestrator.py` writes conversations / messages / traces

### Wave K — Surfaces: operator_write policy extraction (3 bugs)
- `BUG-4011A03A0B1F` — `_record_work_item_workflow_binding` at `operator_write.py:588`
- `BUG-B6D5E1EC557F` — `_admit_native_primary_cutover_gate` at `operator_write.py:624`
- `BUG-931EA1E523A9` — `_reconcile_work_item_closeout` at `operator_write.py:1302`

### Wave L — Surfaces: frontdoor submission & inspection (3 bugs)
- `BUG-118A3F229AC1` — `_submission_from_outcome` at `frontdoor.py:483`
- `BUG-0015036EEFF2` — packet inspection re-derivation at `frontdoor.py:828`
- `BUG-433129C7156B` — hardcoded instruction packet at `workflow_admin.py:132`

### Wave M — Surfaces: handler writes (3 bugs)
- `BUG-73068592A574` — `workflow_run.py:1004-1043` app_manifests upserts in surface
- `BUG-24232B4DA970` — `workflow_run.py:375-403` object_types inserts in surface
- `BUG-7949DED00CB4` — `workflow_run.py:732-788` authority_checkpoints writes in surface

### Wave N — Surfaces: operator_read authority overreach (3 bugs)
- `BUG-E585032448ED` — `operator_read.py:1616-1795` smoke contract resolution (config authority)
- `BUG-691C6690094D` — `operator_read.py:675-761` instruction authority packets
- `BUG-63C0AB529C8D` — `operator_read.py:960-1009` raw SQL in read surface

### Wave O — Client/browser authority leaks (6 bugs)
- `BUG-D0633FD644AA` — `BuildWorkspace.tsx:866-964` client control plane
- `BUG-5001FA81C604` — `OperatingModelEditor.tsx` freshness/commit eligibility
- `BUG-73E7EB966B45` — `commitmentMapShared.ts:131-204` fallback subflow
- `BUG-C6B368E8C549` — `queueSpecExport.ts:123-175` client spec assembly
- `BUG-DD9D1DF37451` — `taskRouting.ts:21-101` client task authority
- `BUG-C66E4C997F15` — `intentParser.ts:521-1091` client compilation
- `BUG-87242329957C` — `pipelineConvert.ts:21-145` client topology

### Wave P — MCP surfaces (1 bug)
- `BUG-75A9C871941E` — `surfaces/mcp/tools/operator.py` reset_metrics hides destructive writes

### Wave Q — Admin handler policy (1 bug)
- `BUG-F792909C6665` — `workflow_admin.py:283-339` transport eligibility in admin handler

## Wave ordering

Waves are mostly independent except:
- Wave C depends on D for `workflow_events` / `workflow_runs` repository (build D first if both fire)
- Wave E depends on Wave D's `system_events` seam
- Wave L depends on Wave D's admission repository
- Wave M depends on Wave N for `app_manifests` repository consolidation

Recommended launch order for max parallelism: **A, B, G, H, J, P** → **D** → **C, E, F, I** → **K, L, M, N** → **O, Q**.

## Verification contract

Each wave's workflow spec requires:
1. A `read_scope` naming the runtime file(s) and the `storage/postgres/*` target
2. A `write_scope` naming the new/edited repository + migration + runtime module
3. A `verify_command` running `python3 -m py_compile` on touched modules + the targeted test suite
4. A final `review` job that confirms no SQL remains in the source file
5. A closing `workflow bugs` call marking each bug as `FIXED` with a `decision_ref` pointing to the queue name

## Closing the bugs

After each wave lands, the builder must run:

```bash
export WORKFLOW_DATABASE_URL="postgresql://localhost:5432/praxis"
"Code&DBs/Workflow/workflow" tools call praxis_bugs --input-json '{
  "command": "resolve",
  "bug_id": "BUG-XXXXXXXX",
  "status": "FIXED",
  "resolution": "Writes moved to storage/postgres/<name>_repository.py in <wave_queue_id>. Verified: no INSERT/UPDATE/DELETE remains in <source_file>."
}' --yes
```
