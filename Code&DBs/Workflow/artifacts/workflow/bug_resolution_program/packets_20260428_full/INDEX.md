# Backlog workflow specs — bug_resolution_program_20260428_full

Generated: 2026-04-28  
Coordination: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260428_full.json`  

## Totals

- **Spec files:** 225
- **Bug packets:** 224
- **Roadmap items:** 1
- **Bugs in scope:** 260

## Tiers

- **high** (83): P0/P1 — ship next; authority repair, runtime stability, compile blockers
- **medium** (125): P2 + roadmap items — durable cleanups, wiring fixes, normal-priority capabilities
- **low** (17): P3 — polish, docs, low-blast-radius hygiene

## By severity

- **P0:** 0
- **P1:** 83
- **P2:** 125
- **P3:** 17

## By wave (execution order)

- **wave_0_authority_repair:** 166
- **wave_1_evidence_normalization:** 53
- **wave_2_execute:** 5

## By lane

- **authority_bug_system:** 166
- **workflow_runtime:** 44
- **app_wiring_frontend:** 13
- **data_projector:** 1

## HIGH tier (83 specs)

### wave_0_authority_repair (60)

- `P1` `authority_bug_system` `VERIFY` **Route and catalog discovery omit capability-mount degradation and can serve partial API as canonical**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-api-capability-mount-visibility-2026-04-28.queue.json`  
  bugs: BUG-9D09F47D  

- `P1` `authority_bug_system` `ARCHITECTURE` **Fallback-based compatibility paths mask authority failures across runtime evidence, impact analysis, and MCP transport**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-authority-fallback-masking-2026-04-28.queue.json`  
  bugs: BUG-293B874A  

- `P1` `authority_bug_system` `RUNTIME` **praxis_wave start reports a running wave but observe from a fresh process shows no waves**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-cluster-authority-bug-system.queue.json`  
  bugs: BUG-AF7C1773  

- `P1` `authority_bug_system` `ARCHITECTURE` **workflow_chain bootstrap and run status soften missing control_commands authority instead of failing closed**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-control-commands-optionality-2026-04-28.queue.json`  
  bugs: BUG-B5F3106D  

- `P1` `authority_bug_system` `ARCHITECTURE/TEST` **Dataset candidate subscriber outer loops are unproven while tests only cover pure helpers**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-dataset-scan-split.queue.json`  
  bugs: BUG-7378056B, BUG-415FC105  

- `P1` `authority_bug_system` `TEST/WIRING` **Historical-spec guard only tests synthetic fixtures, so live queue artifacts keep retired DB authority**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-historical-spec-guard-regression-2026-04-28.queue.json`  
  bugs: BUG-4E1FD606, BUG-A4CE07C5  

- `P1` `authority_bug_system` `ARCHITECTURE/WIRING` **[hygiene-2026-04-22/secret-authority] Sandbox env assembly copies host env and dotenv before secret allowlist**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-22-secret-authority.queue.json`  
  bugs: BUG-2CF335E3, BUG-25224975  

- `P1` `authority_bug_system` `ARCHITECTURE` **[hygiene-2026-04-23/connector-builder-authority] Connector registrar auto-imports filesystem clients and writes multiple…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-23-connector-builder-authority.queue.json`  
  bugs: BUG-A63D9317, BUG-0AB8A780  

- `P1` `authority_bug_system` `ARCHITECTURE` **[hygiene-2026-04-23/operation-receipts] Operation catalog execution receipts are response decoration instead of atomic d…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-23-operation-receipts.queue.json`  
  bugs: BUG-9B812B32  

- `P1` `authority_bug_system` `ARCHITECTURE` **[hygiene-2026-04-23/runtime-target-setup] Runtime-target setup authority is split across setup wizard, service lifecycle…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-23-runtime-target-setup.queue.json`  
  bugs: BUG-F9FD6AC3  

- `P1` `authority_bug_system` `ARCHITECTURE` **praxis_wave and workflow_chain split wave authority between in-memory state and Postgres**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-23-wave-authority-split.queue.json`  
  bugs: BUG-4C7E2290  

- `P1` `authority_bug_system` `ARCHITECTURE/WIRING` **MCP data surface imports a stale control-plane manifest callable copy through runtime wrapper**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-manifest-authority-split.queue.json`  
  bugs: BUG-4A2DABE0, BUG-92FC7D60  

- `P1` `authority_bug_system` `ARCHITECTURE/WIRING` **Registry manifest upserts bypass helm normalization and object-type persistence**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-manifest-authority-split-2026-04-28.queue.json`  
  bugs: BUG-5CF9B049, BUG-542C4A1F  

- `P1` `authority_bug_system` `ARCHITECTURE/RUNTIME/TEST` **Gateway bug filing tests use fake trackers so correlation and receipt guarantees can regress silently**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-pattern-authority-bypass-2026-04-28.queue.json`  
  bugs: BUG-D698012D, BUG-CE7534B9, BUG-C0E31FA1, BUG-9A25C4A4  

- `P1` `authority_bug_system` `ARCHITECTURE/WIRING` **MemorySync only projects verified_by proof edges from receipt outputs, leaving verification_runs invisible to the proof …**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-proof-metrics-authority-mix-2026-04-28.queue.json`  
  bugs: BUG-21D6F42E, BUG-AEB9D066  

- `P1` `authority_bug_system` `ARCHITECTURE/RUNTIME` **authority memory projects synthetic source_issue_id values as issue lineage**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-source-issue-authority-split-2026-04-28.queue.json`  
  bugs: BUG-65F29AC5, BUG-797BF6FA, BUG-99E2C3C8  

- `P1` `authority_bug_system` `ARCHITECTURE/VERIFY/WIRING` **Verifier control-plane auto bugs file through BugTracker.file_bug with no discovery receipt or run provenance**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-spiderweb-2026-04-28-bug-provenance-bypass.queue.json`  
  bugs: BUG-9C8BE592, BUG-3AFA5EF4, BUG-14EBA691  

- `P1` `authority_bug_system` `ARCHITECTURE/WIRING` **Wiring audit reports archived mobile tables as live unwired code orphans**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-spiderweb-mobile-archive-authority-drift-2026-04-28.queue.json`  
  bugs: BUG-0C6A41A4, BUG-DFA908C8  

- `P1` `authority_bug_system` `ARCHITECTURE/WIRING` **Frontdoor and operator repository duplicate packet-inspection fallback derivation and error handling**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-spiderweb-run-status-read-split-2026-04-28.queue.json`  
  bugs: BUG-9798CDF4, BUG-98228556  

- `P1` `authority_bug_system` `ARCHITECTURE/WIRING` **bug_resolution_program freeze path bypasses invoke_tool and standing-order surfacing**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-standing-order-authority-bypass-2026-04-28.queue.json`  
  bugs: BUG-630CBBE0, BUG-1D54FE81  

- `P1` `authority_bug_system` `ARCHITECTURE` **[architecture] Build per-sandbox per-provider credential authority to replace launch-context credential forwarding**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-tag-anthropic.queue.json`  
  bugs: BUG-25829630  

- `P1` `authority_bug_system` `ARCHITECTURE` **Atlas default view is not outcome-oriented**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-atlas-default-view.queue.json`  
  bugs: BUG-71AE925E  

- `P1` `authority_bug_system` `RUNTIME` **Atlas graph is too heavy to inspect reliably**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-atlas-graph-too.queue.json`  
  bugs: BUG-A274AEBC  

- `P1` `authority_bug_system` `ARCHITECTURE` **Atlas missing graph-table toggle and data-graph hierarchy-aware node metadata**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-atlas-missing-graph-table.queue.json`  
  bugs: BUG-37D18B71  

- `P1` `authority_bug_system` `SCOPE` **Bug authority lacks structured execution scope fields**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-authority-lacks-structured.queue.json`  
  bugs: BUG-38C264B5  

- `P1` `authority_bug_system` `ARCHITECTURE` **compile/submission path calls api_llm despite operator decision + registry binding cli_llm — MCP timeouts on multi-packe…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-compile-submission-path-calls.queue.json`  
  bugs: BUG-FD42FE1D  

- `P1` `authority_bug_system` `ARCHITECTURE` **Database environment authority is split across shell bootstrap, runtime, surface resolvers, and tests**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-database-environment-authority.queue.json`  
  bugs: BUG-9BB04947  

- `P1` `authority_bug_system` `ARCHITECTURE` **Docker and sandbox setup install and mount provider CLIs outside catalog authority**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-docker-sandbox-setup.queue.json`  
  bugs: BUG-023252F7  

- `P1` `authority_bug_system` `ARCHITECTURE` **Event and receipt projection authority is spread across runtime, storage, observability, subscriptions, and surfaces**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-event-receipt-projection.queue.json`  
  bugs: BUG-32E01522  

- `P1` `authority_bug_system` `WIRING` **execution transport authority never reaches remote lanes**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-execution-transport-authority.queue.json`  
  bugs: BUG-D3C6352B  

- `P1` `authority_bug_system` `WIRING` **integration credential set_secret mutates integration_registry directly in surface layer**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-integration-credential-set-secret.queue.json`  
  bugs: BUG-0EFDAB8E  

- `P1` `authority_bug_system` `ARCHITECTURE` **integrations_admin static HTTP routes bypass operation_catalog_gateway and receipt-backed CQRS**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-integrations-admin-static-http.queue.json`  
  bugs: BUG-DF32D694  

- `P1` `authority_bug_system` `RUNTIME` **Manual circuit breaker override query failure erases force-open decisions**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-manual-circuit-breaker.queue.json`  
  bugs: BUG-2D9A6DED  

- `P1` `authority_bug_system` `WIRING` **Moon generated nodes leave data dictionary, bindings, imports, and DB object contracts empty**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-moon-generated-nodes.queue.json`  
  bugs: BUG-5DD67C2A  

- `P1` `authority_bug_system` `RUNTIME` **Native cutover can leave workspace and database authority split across old mount and local runtime**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-native-cutover-can.queue.json`  
  bugs: BUG-A5FE235C  

- `P1` `authority_bug_system` `ARCHITECTURE` **Native service lifecycle is spread across launch scripts, runtime CQRS, surfaces, heartbeat, and setup checks**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-native-service-lifecycle.queue.json`  
  bugs: BUG-DFC8607C  

- `P1` `authority_bug_system` `RUNTIME` **Operator console replays previous turn's reply from stale SSE queue**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-operator-console-replays.queue.json`  
  bugs: BUG-534E7290  

- `P1` `authority_bug_system` `RUNTIME` **OrbStack migration can leave Docker authority unavailable with root-owned or corrupted VM data**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-orbstack-migration-can.queue.json`  
  bugs: BUG-90E70AA6  

- `P1` `authority_bug_system` `WIRING` **Permission matrix hardcodes provider allowlists outside provider catalog**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-permission-matrix-hardcodes.queue.json`  
  bugs: BUG-ADAEB359  

- `P1` `authority_bug_system` `WIRING` **praxis_graph_projection fails on empty decision_ref row**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-praxis-graph-projection-empty-decision-ref.queue.json`  
  bugs: BUG-026AB2E7  

- `P1` `authority_bug_system` `WIRING` **praxis_next_actions direct_fallback bypasses operation_catalog_gateway**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-praxis-next-actions-direct-fallback-bypasses.queue.json`  
  bugs: BUG-99E0CBC9  

- `P1` `authority_bug_system` `ARCHITECTURE` **Provider routing and admission authority is duplicated across registry, adapters, runtime profiles, and transport code**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-provider-routing-admission.queue.json`  
  bugs: BUG-EEE3E88E  

- `P1` `authority_bug_system` `ARCHITECTURE` **provider_transport built-in defaults remain executable fallback authority**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-provider-transport-built-in-defaults.queue.json`  
  bugs: BUG-D4CC68A9  

- `P1` `authority_bug_system` `ARCHITECTURE` **Recall operator-decision search bypasses the operation catalog gateway**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-recall-operator-decision-search.queue.json`  
  bugs: BUG-FA0A5B0B  

- `P1` `authority_bug_system` `ARCHITECTURE` **Refactor secrets OAuth and credential resolution spread across adapters registry runtime and memory**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-refactor-secrets-oauth.queue.json`  
  bugs: BUG-BF734C00  

- `P1` `authority_bug_system` `ARCHITECTURE` **Registry/catalog/tool authority is spread across registry, runtime, surfaces, storage, and tests**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-registry-catalog-tool-authority-spread.queue.json`  
  bugs: BUG-268ECD3F  

- `P1` `authority_bug_system` `ARCHITECTURE` **Runtime profiles hardcode provider env vars and model allowlists outside effective catalog**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-runtime-profiles-hardcode.queue.json`  
  bugs: BUG-1B959922  

- `P1` `authority_bug_system` `ARCHITECTURE` **Secret and credential resolution is spread across keychain, OAuth, env forwarding, provider transport, and execution bac…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-secret-credential-resolution.queue.json`  
  bugs: BUG-2337DB51  

- `P1` `authority_bug_system` `ARCHITECTURE` **Service lifecycle authority is spread across setup, health, scripts, runtime, and target-specific launch paths**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-service-lifecycle-authority.queue.json`  
  bugs: BUG-080157FE  

- `P1` `authority_bug_system` `RUNTIME` **submission_gate Stage 4 fails build jobs without explicit verify_refs even when in-scope file landed and auto-seal seale…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-submission-gate-stage-build.queue.json`  
  bugs: BUG-31E77A5E  

- `P1` `authority_bug_system` `ARCHITECTURE` **task_type_routing admits Together and OpenRouter-DeepSeek for non-research/non-compile task_types — violates DeepSeek sc…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-task-type-routing-admits-together.queue.json`  
  bugs: BUG-2F793EE7  

- `P1` `authority_bug_system` `RUNTIME` **Together API decoder corruption in V4-Pro responses**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-together-api-decoder.queue.json`  
  bugs: BUG-F65A9A98  

- `P1` `authority_bug_system` `VERIFY` **Verifier selection is Python-only instead of catalog-backed by scope**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-verifier-selection-python-only.queue.json`  
  bugs: BUG-8F6A612A  

- `P1` `authority_bug_system` `WIRING` **Wire execution transport authority through runtime adapters**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-wire-execution-transport.queue.json`  
  bugs: BUG-B0BCB286  

- `P1` `authority_bug_system` `WIRING` **Wire oversight telemetry tables (policy_drift_events/session_blast_radius) into lifecycle runtime**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-wire-oversight-telemetry.queue.json`  
  bugs: BUG-1E77345E  

- `P1` `authority_bug_system` `WIRING` **Worker pipeline crashes on null bytes in LLM output: PG rejects with "invalid byte sequence for encoding UTF8: 0x00"**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-worker-pipeline-crashes.queue.json`  
  bugs: BUG-097AB98E  

- `P1` `authority_bug_system` `ARCHITECTURE` **_workflow_expected_object_exists row-key parser uses '|' separator but recent migration authority entries (280, 282, 283…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-workflow-expected-object-exists-row-key-parse.queue.json`  
  bugs: BUG-911BCE24  

- `P1` `authority_bug_system` `VERIFY` **workflow health reports healthy while projections are critical and routes are unhealthy**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-workflow-health-reports.queue.json`  
  bugs: BUG-D39EBC3F  

- `P1` `authority_bug_system` `ARCHITECTURE` **Workspace and scope boundary authority is spread across runtime, registry, adapters, shell wrappers, and tests**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-workspace-scope-boundary.queue.json`  
  bugs: BUG-46A6C7F2  

- `P1` `authority_bug_system` `ARCHITECTURE/WIRING` **Graph-run worker failure path hardcodes empty lineage and job counts instead of using the shared workflow_state terminal…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-workflow-terminal-event-authority-split-2026-04-28.queue.json`  
  bugs: BUG-98382303, BUG-0DEF0D0A  


### wave_1_evidence_normalization (22)

- `P1` `app_wiring_frontend` `WIRING` **praxis_search canonical front door can return unreceipted direct_fallback success**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-authority-fallback-masking-2026-04-28-read-sur.queue.json`  
  bugs: BUG-239AE106  

- `P1` `app_wiring_frontend` `WIRING` **Route explanation CQRS is missing for provider/model removal reasons**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-title-route-explanation-cqrs.queue.json`  
  bugs: BUG-5444AA3C  

- `P1` `workflow_runtime` `WIRING` **Execution transport authority is split across resolver, worker loop, core executor, and CLI runner**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-execution-transport-split.queue.json`  
  bugs: BUG-C0E107A6  

- `P1` `workflow_runtime` `TEST` **Semantic invariant scanner misses module-alias unified dispatch calls, so CQRS boundary tests can pass with live bypasse…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-spiderweb-2026-04-28-command-bus-invariant-blinds.queue.json`  
  bugs: BUG-791E3C52  

- `P1` `workflow_runtime` `RUNTIME` **asyncpg pools accumulate idle connections — Postgres saturates at 100/100, blocks compile and every other handler**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-tag-postgres.queue.json`  
  bugs: BUG-D7A939CA  

- `P1` `workflow_runtime` `RUNTIME` **Submission gate blocks audit jobs with workflow_submission.out_of_scope on baseline-only paths that never mount into /wo…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-tag-workspace-hydration.queue.json`  
  bugs: BUG-E68A6013  

- `P1` `workflow_runtime` `RUNTIME` **Circuit breaker authority unavailable skips provider preflight**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-circuit-breaker-authority.queue.json`  
  bugs: BUG-724759AE  

- `P1` `workflow_runtime` `RUNTIME` **Connector auto-builder aborts the full run instead of emitting downstream mock artifacts after early discovery failure**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-connector-auto-builder-aborts.queue.json`  
  bugs: BUG-40494726  

- `P1` `workflow_runtime` `WIRING` **CQRS provider capability matrix is missing for effective provider job catalog**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-cqrs-provider-capability.queue.json`  
  bugs: BUG-EBE27625  

- `P1` `workflow_runtime` `RUNTIME` **Moon dev API crash loop blocks compose engine handoff**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-moon-dev-api.queue.json`  
  bugs: BUG-3CC5D3AD  

- `P1` `workflow_runtime` `WIRING` **Moon generated workflow has no release gates or typed gate contracts**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-moon-generated-workflow.queue.json`  
  bugs: BUG-2729F8B7  

- `P1` `workflow_runtime` `RUNTIME` **praxis_submit_code_change marks empty-workspace baseline drift as out_of_scope, blocking seal even when only declared wr…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-praxis-submit-code-change-marks-empty-works.queue.json`  
  bugs: BUG-F78C0477  

- `P1` `workflow_runtime` `RUNTIME` **Provider freshness is not encoded as a pre-proof launch gate**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-provider-freshness-encoded.queue.json`  
  bugs: BUG-72420B56  

- `P1` `workflow_runtime` `RUNTIME` **Provider transport admission filter fail-opens when admissions table is missing**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-provider-transport-admission.queue.json`  
  bugs: BUG-5DFF1C68  

- `P1` `workflow_runtime` `RUNTIME` **refresh_private_provider_job_catalog ON CONFLICT clause not aware of routing's transport_type split — cardinality violat…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-refresh-private-provider-job-catalog-confli.queue.json`  
  bugs: BUG-72695EF3  

- `P1` `workflow_runtime` `WIRING` **Sandbox auth seeding hardcodes provider credential homes outside credential CQRS**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-sandbox-auth-seeding.queue.json`  
  bugs: BUG-70706DC9  

- `P1` `workflow_runtime` `RUNTIME` **Stale workflow packet fleets remain retryable without quarantine authority**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-stale-workflow-packet.queue.json`  
  bugs: BUG-62F78235  

- `P1` `workflow_runtime` `RUNTIME` **Worker force-fails every admitted run with workflow.execution_crash; effective workflow-runtime outage**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-worker-force-fails-every.queue.json`  
  bugs: BUG-0C0C55B1  

- `P1` `workflow_runtime` `RUNTIME` **Workflow docker_packet_only sandbox fails with too many open files**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-workflow-docker-packet-only-sandbox.queue.json`  
  bugs: BUG-AA7CA63D  

- `P1` `workflow_runtime` `ARCHITECTURE` **Workflow execution lifecycle is spread across admission, claims, leases, workers, receipts, and status surfaces**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-workflow-execution-lifecycle.queue.json`  
  bugs: BUG-F2EA854B  

- `P1` `workflow_runtime` `RUNTIME` **Workflow review sandbox advertises repo snapshot but does not mount readable repo files; bundled praxis CLI shim also fa…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-workflow-review-sandbox.queue.json`  
  bugs: BUG-632E6F45  

- `P1` `workflow_runtime` `RUNTIME` **Workflow review step_3 sandbox missing target repo and broken praxis CLI runtime**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-workflow-review-step-3.queue.json`  
  bugs: BUG-17CC1088  


### wave_2_execute (1)

- `P1` `workflow_runtime` `RUNTIME` **praxis_submit_code_change rejects sandbox-written files as phantom_ship in empty workflow workspace**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-2-execute-workflow-runtime-title-praxis-submit-code-change-rejects-sandbox-written.queue.json`  
  bugs: BUG-D2363EB8  


## MEDIUM tier (125 specs)

### roadmap_item_packet (1)

- `P2` `roadmap_route_authority_write_surface` `ARCHITECTURE` **roadmap_item.add.canonical.route.authority.write.surface.for.task.type.routing**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/roadmap-item-add-canonical-route-authority-write-surface-for-task-type-routing.queue.json`  
  bugs: (roadmap)  
  decision: `decision.2026-04-29.add-canonical-route-authority-write-surface-for-task-type-routing`  


### wave_0_authority_repair (90)

- `P2` `authority_bug_system` `ARCHITECTURE` **Bug failure packet assembly is duplicated between BugTracker and bug_evidence**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-bug-failure-packet-duplication-2026-04-28.queue.json`  
  bugs: BUG-CEA99453  

- `P2` `authority_bug_system` `ARCHITECTURE` **bug triage packet bypasses bug_candidates projection and re-reads the raw bug tracker**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-bug-triage-projection-split-2026-04-28.queue.json`  
  bugs: BUG-52B7A1C3  

- `P2` `authority_bug_system` `TEST/WIRING` **surfaces.web_dashboard is an orphaned dashboard path with its own receipt schema**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-dashboard-orphan-drift-2026-04-28.queue.json`  
  bugs: BUG-C2ECF2F0, BUG-D7281A3D  

- `P2` `authority_bug_system` `WIRING` **[hygiene-2026-04-22/agent-skills] Agent skill authority policy has no DB-backed registry or operator surface**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-22-agent-skills.queue.json`  
  bugs: BUG-1FB4A132  

- `P2` `authority_bug_system` `VERIFY` **[hygiene-2026-04-22/wiring-audit] Active standing orders are reported as unwired decisions**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-22-wiring-audit.queue.json`  
  bugs: BUG-08E4B65C  

- `P2` `authority_bug_system` `VERIFY` **[hygiene-2026-04-23/graph-authority] Authority-free graph validation accepts unbound workspace and runtime refs**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-23-graph-authority.queue.json`  
  bugs: BUG-1A4029F2  

- `P2` `authority_bug_system` `WIRING` **[hygiene-2026-04-23/integration-secret-authority] API and MCP duplicate integration secret writes and preserve env fallb…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-23-integration-secret-authority.queue.json`  
  bugs: BUG-6AB79F3B  

- `P2` `authority_bug_system` `RUNTIME` **[hygiene-2026-04-23/reload-workaround-authority] praxis_reload mutates live process state without durable operational re…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-23-reload-workaround-authority.queue.json`  
  bugs: BUG-A84383D1  

- `P2` `authority_bug_system` `WIRING` **[hygiene-2026-04-23/workflow-cli-authority] Modern workflow front door still routes core commands through legacy workflo…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-23-workflow-cli-authority.queue.json`  
  bugs: BUG-D92E6B38  

- `P2` `authority_bug_system` `VERIFY` **[hygiene-2026-04-23/workflow-history-authority] Workflow history merges process-local fallback into durable status summa…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-23-workflow-history-authority.queue.json`  
  bugs: BUG-5AA2CCF3  

- `P2` `authority_bug_system` `WIRING` **[hygiene-2026-04-24/db-authority-orphans] Data dictionary still exposes DB authority tables with no production runtime o…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-24-db-authority-orphans.queue.json`  
  bugs: BUG-F8C9F5B5  

- `P2` `authority_bug_system` `ARCHITECTURE` **[hygiene-2026-04-24/mobile-agent-sessions-authority] Agent sessions API owns provider execution, workflow launch, approv…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-24-mobile-agent-sessions-authority.queue.json`  
  bugs: BUG-F8B4EDE7  

- `P2` `authority_bug_system` `TEST/VERIFY` **Operation catalog parity tests use seeded rows, so live schema-authority drift goes untested**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-schema-authority-false-negative-2026-04-28.queue.json`  
  bugs: BUG-CD421070, BUG-9CDDC59B  

- `P2` `authority_bug_system` `ARCHITECTURE` **typed_gap.created helper writes authority_events directly with synthetic operation_ref and no receipt linkage**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-spiderweb-2026-04-28-typed-gap-event-bypass.queue.json`  
  bugs: BUG-19F7672E  

- `P2` `authority_bug_system` `ARCHITECTURE` **Interactive agent sessions still mirror DB session state into compatibility files and ignore write failures**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-spiderweb-agent-session-compat-mirror-2026-04-28.queue.json`  
  bugs: BUG-7097822D  

- `P2` `authority_bug_system` `ARCHITECTURE` **Task contract maintain_wiring_health unresolved after 1 iter(s)**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-tag-audit-contract.queue.json`  
  bugs: BUG-121B5049  

- `P2` `authority_bug_system` `RUNTIME` **JIT trigger-check hooks lose every friction event — praxis_friction has no record action**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-tag-cqrs.queue.json`  
  bugs: BUG-6873C9C6, BUG-8D8C5256  

- `P2` `authority_bug_system` `RUNTIME/WIRING` **Bug duplicate/search surfaces hang during operator filing**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-tag-deployment.queue.json`  
  bugs: BUG-07771B2A, BUG-8B3AED45  

- `P2` `authority_bug_system` `OTHER` **Activity truth evidence drift requires explicit review**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-activity-truth-evidence.queue.json`  
  bugs: bug.780c33a110.activity-truth.cockpit, bug.227b891bbf.activity-truth.cockpit, bug.7e0f249aa7.activity-truth.cockpit, bug.3bab0a06e3.activity-truth.cockpit, bug.f2d2213feb.activity-truth.cockpit, bug.d48079a039.activity-truth.cockpit, bug.4379d5d19c.activity-truth.cockpit  

- `P2` `authority_bug_system` `WIRING` **Add setup apply as mutating state authority command path**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-add-setup-apply.queue.json`  
  bugs: BUG-25EF566B  

- `P2` `authority_bug_system` `WIRING` **Add time-window support to workflow status surface**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-add-time-window-support.queue.json`  
  bugs: BUG-FEC8E8E3  

- `P2` `authority_bug_system` `WIRING` **API docs parity test is non-deterministic when capability mount authority varies**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-api-docs-parity.queue.json`  
  bugs: BUG-ABD9DD69  

- `P2` `authority_bug_system` `WIRING` **Atlas detail panel does not provide authoritative drilldown**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-atlas-detail-panel.queue.json`  
  bugs: BUG-363F16A6  

- `P2` `authority_bug_system` `ARCHITECTURE` **Atlas read model leaves most nodes unowned**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-atlas-read-model.queue.json`  
  bugs: BUG-E6D338F6  

- `P2` `authority_bug_system` `WIRING` **Atlas search behaves differently between graph and table**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-atlas-search-behaves.queue.json`  
  bugs: BUG-AF43D300  

- `P2` `authority_bug_system` `VERIFY` **AtlasPage test imports missing buildSemanticModel export**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-atlaspage-test-imports.queue.json`  
  bugs: BUG-99E5B2C7  

- `P2` `authority_bug_system` `WIRING` **authoring scaffold commands (page, hierarchy) missing from praxis workflow front door**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-authoring-scaffold-commands.queue.json`  
  bugs: BUG-32A7845E  

- `P2` `authority_bug_system` `ARCHITECTURE` **Centralize environment and configuration resolution**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-centralize-environment-configuration.queue.json`  
  bugs: BUG-50389B7D  

- `P2` `authority_bug_system` `OTHER` **Compiler build_graph renders template stubs instead of semantic-retrieval capability nodes**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-compiler-build-graph-renders.queue.json`  
  bugs: BUG-3330D2CD  

- `P2` `authority_bug_system` `WIRING` **compose_and_launch data-pill binder marks Praxis.db as unbound, blocking intents that name our own infra**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-compose-and-launch-data-pill-binder.queue.json`  
  bugs: BUG-EAB5110B  

- `P2` `authority_bug_system` `WIRING` **config/cascade/specs queue JSON embeds host-local paths in verify_command and prompts (breaks portable rerun)**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-config-cascade-specs-queue-json.queue.json`  
  bugs: BUG-ACF1F41A  

- `P2` `authority_bug_system` `ARCHITECTURE` **CREATE OR REPLACE VIEW with renamed columns jams bootstrap on subsequent restarts**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-create-replace-view.queue.json`  
  bugs: BUG-B4D18A71  

- `P2` `authority_bug_system` `WIRING` **Credential availability is not projected into provider capability CQRS**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-credential-availability-projected.queue.json`  
  bugs: BUG-DF343C7D  

- `P2` `authority_bug_system` `ARCHITECTURE` **Direct internal imports remain in surfaces/api**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-direct-internal-imports.queue.json`  
  bugs: BUG-54EEB8B5  

- `P2` `authority_bug_system` `ARCHITECTURE` **enforce_operation_catalog_cqrs_contract trigger AOR-row check hardcoded object_kind=command, blocks query ops**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-enforce-operation-catalog-cqrs-contract-trigg.queue.json`  
  bugs: BUG-ECD0E5B3  

- `P2` `authority_bug_system` `VERIFY` **No existing workflow receipts for ui_atlas_refinement wave IDs before edit**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-existing-workflow-receipts.queue.json`  
  bugs: BUG-D7801F5D  

- `P2` `authority_bug_system` `WIRING` **Extract shared client-core library for CLI and API**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-extract-shared-client-core.queue.json`  
  bugs: BUG-0FB23DDF  

- `P2` `authority_bug_system` `VERIFY` **Generated Atlas artifact is stale against live Atlas graph**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-generated-atlas-artifact.queue.json`  
  bugs: BUG-759D70A2  

- `P2` `authority_bug_system` `WIRING` **Hardcoded repo paths bypass workspace layout registry**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-hardcoded-repo-paths.queue.json`  
  bugs: BUG-AEEC855B  

- `P2` `authority_bug_system` `RUNTIME` **Hosted app has no canonical rebuild command**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-hosted-app-has.queue.json`  
  bugs: BUG-22B49805  

- `P2` `authority_bug_system` `WIRING` **Implement provider onboarding capacity probing for all protocol families**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-implement-provider-onboarding.queue.json`  
  bugs: BUG-1DEEBF2D  

- `P2` `authority_bug_system` `OTHER` **input-validation::mcp::operator::action_enum_dispatch — multi-action operator tools normalize action without enum check**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-input-validation-mcp-operator.queue.json`  
  bugs: BUG-34C2F2DA  

- `P2` `authority_bug_system` `ARCHITECTURE` **input-validation::mcp::workflow::spec_path — no workspace containment check**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-input-validation-mcp-workflow.queue.json`  
  bugs: BUG-FF008A47  

- `P2` `authority_bug_system` `ARCHITECTURE` **input-validation::rest::cors — PRAXIS_API_ALLOWED_ORIGINS defaults to wildcard**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-input-validation-rest-cors.queue.json`  
  bugs: BUG-76A0519D  

- `P2` `authority_bug_system` `OTHER` **input-validation::rest::handler_catchall_path — rest_of_path:path routes accept arbitrary path segments with no traversa…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-input-validation-rest-handler-catchall-path.queue.json`  
  bugs: BUG-24F87D0F  

- `P2` `authority_bug_system` `OTHER` **input-validation::rest::observability_roots — /api/observability/code-hotspots roots and path_prefix are unsanitized**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-input-validation-rest-observability-roots.queue.json`  
  bugs: BUG-419EDA6D  

- `P2` `authority_bug_system` `ARCHITECTURE` **input-validation::rest::workflow_run_request — pydantic models lack field constraints**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-input-validation-rest-workflow-run-request.queue.json`  
  bugs: BUG-AD855F0C  

- `P2` `authority_bug_system` `ARCHITECTURE` **Launch-plan inline execution manifests are not persisted into manifest authority**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-launch-plan-inline-execution.queue.json`  
  bugs: BUG-8641B0B5  

- `P2` `authority_bug_system` `WIRING` **launcher command missing from praxis workflow front door**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-launcher-command-missing.queue.json`  
  bugs: BUG-AE3BE4E0  

- `P2` `authority_bug_system` `ARCHITECTURE` **Live runtime still embeds operator-local authority markers and mixed CQRS bug-surface reads**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-live-runtime-still.queue.json`  
  bugs: BUG-7B683C6C  

- `P2` `authority_bug_system` `OTHER` **Moon node state styling uses filled alarm circles as primary graph language**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-moon-node-state.queue.json`  
  bugs: BUG-6F69A6DF  

- `P2` `authority_bug_system` `TEST` **MoonBuildPage dock authority tests fail because node-popout is absent**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-moonbuildpage-dock-authority.queue.json`  
  bugs: BUG-8742C732  

- `P2` `authority_bug_system` `RUNTIME` **Operator console misses assistant reply when SSE stream tears down before final frame**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-operator-console-misses.queue.json`  
  bugs: BUG-D358869B  

- `P2` `authority_bug_system` `ARCHITECTURE` **operator-decision-triggers.json: user-specific absolute file_glob breaks standing-order surfacing on other machines**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-operator-decision-triggers-json-user-specific.queue.json`  
  bugs: BUG-6553F5ED  

- `P2` `authority_bug_system` `WIRING` **operator_write commit succeeds without roadmap item readback**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-operator-write-commit-succeeds.queue.json`  
  bugs: BUG-507AB442  

- `P2` `authority_bug_system` `OTHER` **persona_profiles is empty — Moon compose path has no response_contract / system_prompt to teach the LLM the build shape**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-persona-profiles-empty-moon.queue.json`  
  bugs: BUG-B7A075E2  

- `P2` `authority_bug_system` `WIRING` **Policy verification scripts and policy artifact renderer hardcode praxis-api-server-1 instead of resolving api-server se…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-policy-verification-scripts.queue.json`  
  bugs: BUG-C12BC0A0  

- `P2` `authority_bug_system` `ARCHITECTURE` **praxis_cli_auth_doctor hardcodes /tmp/_codex_authprobe.txt for Codex auth probing**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-praxis-cli-auth-doctor-hardcodes-tmp-codex-au.queue.json`  
  bugs: BUG-A37C4715  

- `P2` `authority_bug_system` `WIRING` **Praxis native scheduler has no operator-facing registration surface — schedule_definitions writes only via SQL/migration**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-praxis-native-scheduler.queue.json`  
  bugs: BUG-DE971274  

- `P2` `authority_bug_system` `OTHER` **praxis_test DB drift blocks pytest integration tests (migration 024 column mismatch)**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-praxis-test-drift-blocks.queue.json`  
  bugs: BUG-2BBCC370  

- `P2` `authority_bug_system` `OTHER` **praxis-up recreate reverts policy_authority_reject_delete trigger function (BUG-193C9A50 detail)**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-praxis-up-recreate-reverts.queue.json`  
  bugs: BUG-7CBE7A3B, BUG-193C9A50  

- `P2` `authority_bug_system` `ARCHITECTURE` **provider guidance surfaces still bypass the effective provider catalog and carry hardcoded provider/model examples**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-provider-guidance-surfaces.queue.json`  
  bugs: BUG-0B793F90  

- `P2` `authority_bug_system` `ARCHITECTURE` **Refactor discovery recall query and memory retrieval authority spread across modules**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-refactor-discovery-recall.queue.json`  
  bugs: BUG-68256068  

- `P2` `authority_bug_system` `ARCHITECTURE` **Refactor provider routing and runtime-profile admission spread across registry runtime and adapters**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-refactor-provider-routing.queue.json`  
  bugs: BUG-91A91284  

- `P2` `authority_bug_system` `ARCHITECTURE` **Refactor receipts evidence and verification proof pipeline spread across runtime storage and observability**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-refactor-receipts-evidence.queue.json`  
  bugs: BUG-79695B46  

- `P2` `authority_bug_system` `ARCHITECTURE` **Refactor storage layer to reduce repository sprawl**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-refactor-storage-layer.queue.json`  
  bugs: BUG-644C5F2D  

- `P2` `authority_bug_system` `ARCHITECTURE` **Refactor workflow submission and run lifecycle spread across front doors and runtime modules**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-refactor-workflow-submission.queue.json`  
  bugs: BUG-8DB03A36  

- `P2` `authority_bug_system` `ARCHITECTURE` **refresh_private_provider_job_catalog redefinitions in 267/269 use unscoped JOIN economics, break under task_type_routing…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-refresh-private-provider-job-catalog-redefini.queue.json`  
  bugs: BUG-F7D535EF  

- `P2` `authority_bug_system` `ARCHITECTURE` **register_operation_atomic helper writes query_model into write_model_kind for query ops, fails CHECK constraint**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-register-operation-atomic-helper-writes.queue.json`  
  bugs: BUG-110F4EA3  

- `P2` `authority_bug_system` `ARCHITECTURE` **Remove autonomous_objective_proof import shim**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-remove-autonomous-objective-proof-import.queue.json`  
  bugs: BUG-D2357C35  

- `P2` `authority_bug_system` `RUNTIME` **Repo .env contains BOM/CRLF that breaks shell sourcing**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-repo-env-contains.queue.json`  
  bugs: BUG-A9A42870  

- `P2` `authority_bug_system` `WIRING` **Retire scripts/praxis-ctl compatibility wrapper**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-retire-scripts-praxis-ctl-compatibility.queue.json`  
  bugs: BUG-65A0399D  

- `P2` `authority_bug_system` `RUNTIME` **roadmap tree view omits direct child roadmap items**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-roadmap-tree-view.queue.json`  
  bugs: BUG-247C050D  

- `P2` `authority_bug_system` `ARCHITECTURE` **scripts/verify-policy-authority-machinery.py rewrites DB authority to localhost**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-scripts-verify-policy-authority-machinery-py-.queue.json`  
  bugs: BUG-48014630  

- `P2` `authority_bug_system` `ARCHITECTURE` **security-review-2026-04-22-cors: REST API defaults to wildcard CORS origin**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-security-review-2026-04-22-cors-rest-api.queue.json`  
  bugs: BUG-EBE6B0E1  

- `P2` `authority_bug_system` `WIRING` **Setup apply flow should execute through mutating state authority**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-setup-apply-flow.queue.json`  
  bugs: BUG-1350534F  

- `P2` `authority_bug_system` `ARCHITECTURE` **[setup] apply mode still uses a stubbed mutation path**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-setup-apply-mode.queue.json`  
  bugs: BUG-2E33CBEB  

- `P2` `authority_bug_system` `WIRING` **setup apply surface is wired but still only simulates mutation instead of emitting durable authority state**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-setup-apply-surface.queue.json`  
  bugs: BUG-5B7D2647  

- `P2` `authority_bug_system` `ARCHITECTURE` **setup-dblink-for-policy-authority hardcodes a loopback superuser DSN**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-setup-dblink-for-policy-authority-hardcodes-l.queue.json`  
  bugs: BUG-4E4AFFF3  

- `P2` `authority_bug_system` `RUNTIME` **Support Intake proof run blocked by missing OpenAI credential**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-support-intake-proof.queue.json`  
  bugs: BUG-DDFA8F9E  

- `P2` `authority_bug_system` `ARCHITECTURE` **surfaces/mcp/tools/agent_events.py bypasses the operation gateway and writes session state directly**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-surfaces-mcp-tools-agent-events-py-bypasses-o.queue.json`  
  bugs: BUG-14C9AA02  

- `P2` `authority_bug_system` `ARCHITECTURE` **Unified tool factory for MCP tool surfaces**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-unified-tool-factory.queue.json`  
  bugs: BUG-C3582EAA  

- `P2` `authority_bug_system` `VERIFY` **Validation and verification contract is spread across test shell, verifier runtime, workflow specs, and queue artifacts**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-validation-verification-contract.queue.json`  
  bugs: BUG-5D0140CD  

- `P2` `authority_bug_system` `WIRING` **Wire api_models table into runtime model resolution authority**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-wire-api-models-table.queue.json`  
  bugs: BUG-2529D366  

- `P2` `authority_bug_system` `WIRING` **Wire setup wizard apply mode to catalog-backed mutating authority**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-wire-setup-wizard.queue.json`  
  bugs: BUG-010C811D  

- `P2` `authority_bug_system` `WIRING` **[WIRING] plan.launched / plan.composed emit to system_events in spec_compiler and intent_composition while CQRS gateway …**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-wiring-plan-launched-plan-composed.queue.json`  
  bugs: BUG-214C9380  

- `P2` `authority_bug_system` `ARCHITECTURE` **Workflow schema readiness authority omits row-key tables and allows empty non-archive migration contracts**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-workflow-schema-readiness.queue.json`  
  bugs: BUG-40967584  

- `P2` `authority_bug_system` `ARCHITECTURE` **workflow_spec embeds operator-local retired authority markers as hardcoded absolute paths**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-workflow-spec-embeds-operator-local.queue.json`  
  bugs: BUG-A687A7FB  

- `P2` `authority_bug_system` `ARCHITECTURE` **workflow_spec encodes retired authority as literal localhost and /Users/nate denylist**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-workflow-spec-encodes-retired.queue.json`  
  bugs: BUG-372EDD42  

- `P2` `authority_bug_system` `ARCHITECTURE` **WorkflowMigrationError on praxis-api-server boot**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-workflowmigrationerror-praxis-api-server-boot.queue.json`  
  bugs: BUG-D2ED53B4  


### wave_1_evidence_normalization (30)

- `P2` `app_wiring_frontend` `WIRING` **Global praxis launcher can route outside the active workspace and hide repo-local fixes**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-tag-workspace-boundary.queue.json`  
  bugs: BUG-96F12329  

- `P2` `app_wiring_frontend` `WIRING` **bin/praxis-compose-env dump-keychain silently skips TOGETHER_API_KEY**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-title-bin-praxis-compose-env-dump-keychain-sil.queue.json`  
  bugs: BUG-1879A498  

- `P2` `app_wiring_frontend` `WIRING` **Regression: capability route conflict still degrades API startup for service lifecycle desired-state**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-title-capability-route-conflict.queue.json`  
  bugs: BUG-65964DC8  

- `P2` `app_wiring_frontend` `WIRING` **compose_and_launch parallel callers race attach_route_plans, leaving auto/* slugs unresolved**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-title-compose-and-launch-parallel-callers.queue.json`  
  bugs: BUG-C2895CCB  

- `P2` `app_wiring_frontend` `WIRING` **Dev UI API cannot import generated connector artifacts from artifacts.connectors**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-title-dev-api-cannot.queue.json`  
  bugs: BUG-F758C3D4  

- `P2` `app_wiring_frontend` `WIRING` **Missing UI workflow spec directory for ui_atlas_refinement_wave**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-title-missing-workflow-spec.queue.json`  
  bugs: BUG-AD8C5792  

- `P2` `app_wiring_frontend` `WIRING` **Remove legacy compile_materialize alias route and catalog repair**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-title-remove-legacy-compile-materialize.queue.json`  
  bugs: BUG-9CC1EE7E  

- `P2` `app_wiring_frontend` `WIRING` **submission_required forced TRUE for non-mutating task_types whenever write_scope is non-empty**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-title-submission-required-forced-true.queue.json`  
  bugs: BUG-9D24097A  

- `P2` `app_wiring_frontend` `OTHER` **task_type_router resolves no llm_task adapters for any auto/X key — compile_prose silently falls through to deterministi…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-title-task-type-router-resolves-llm-task.queue.json`  
  bugs: BUG-D4D0A348  

- `P2` `app_wiring_frontend` `WIRING` **Use ui_shell_route_registry as authoritative source for app shell routing**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-title-use-ui-shell-route-registry-authoritativ.queue.json`  
  bugs: BUG-1D781196  

- `P2` `data_projector` `RUNTIME` **Regression: data_dictionary_stewardship projector still writes missing object_type contact owner**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-data-projector-title-data-dictionary-stewardship-projector-still.queue.json`  
  bugs: BUG-FF7424CC  

- `P2` `workflow_runtime` `RUNTIME` **dep-audit::anthropic minor bump 0.86.0 -> 0.87.0 required (CVE-2026-34450, CVE-2026-34452)**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-dep-audit-python-anthropic.queue.json`  
  bugs: BUG-AAE46E22  

- `P2` `workflow_runtime` `TEST` **Startup boot reports success while pytest disables auto wiring and tests assert only booted true**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-startup-wiring-proof-gap.queue.json`  
  bugs: BUG-123C17AC  

- `P2` `workflow_runtime` `RUNTIME` **dep-audit::vitest — moderate advisory via vite chain; major bump to 4.1.5**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-tag-dep-audit.queue.json`  
  bugs: BUG-EE65C154, BUG-ABBEFC6F  

- `P2` `workflow_runtime` `WIRING` **Compiler route catalog bypasses effective provider job catalog**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-compiler-route-catalog.queue.json`  
  bugs: BUG-80CF188A  

- `P2` `workflow_runtime` `RUNTIME` **contract-drift::runtime/build_planning_contract.py**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-contract-drift-runtime-build-planning-contr.queue.json`  
  bugs: BUG-AA6AC4E0  

- `P2` `workflow_runtime` `RUNTIME` **Launcher /app returns 503 until the SPA bundle is built locally**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-launcher-app-returns.queue.json`  
  bugs: BUG-8E1C52AE  

- `P2` `workflow_runtime` `TEST` **Linter rollback can revert command-bus bypass cleanup**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-linter-rollback-can.queue.json`  
  bugs: BUG-52408D6E  

- `P2` `workflow_runtime` `RUNTIME` **Manual migration promotion has no safe schema_migrations receipt helper**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-manual-migration-promotion.queue.json`  
  bugs: BUG-DF4CAB94  

- `P2` `workflow_runtime` `WIRING` **Moon compose creates local drafts with no trigger or jobs, leaving release dispatch disabled**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-moon-compose-creates.queue.json`  
  bugs: BUG-8DADDD64  

- `P2` `workflow_runtime` `RUNTIME` **Moon trigger picker hangs while loading available triggers because catalog API does not return**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-moon-trigger-picker.queue.json`  
  bugs: BUG-5861F7F9  

- `P2` `workflow_runtime` `RUNTIME` **praxis_ingest accepts conversation memory that recall cannot retrieve**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-praxis-ingest-accepts-conversation.queue.json`  
  bugs: BUG-C4091203  

- `P2` `workflow_runtime` `RUNTIME` **praxis query routes readback questions to empty quality_views rollup**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-praxis-query-routes.queue.json`  
  bugs: BUG-91D41A89  

- `P2` `workflow_runtime` `WIRING` **praxis workflow query "is the workflow runtime healthy" returns overall=healthy while worker force-fails every run**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-praxis-workflow-query.queue.json`  
  bugs: BUG-17601F93  

- `P2` `workflow_runtime` `WIRING` **Provider cost and budget posture is not exposed through effective catalog CQRS**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-provider-cost-budget.queue.json`  
  bugs: BUG-C7B9D6F2  

- `P2` `workflow_runtime` `ARCHITECTURE` **runtime.default_path_pilot.resolve_default_path_pilot is never reached from a live path**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-runtime-default-path-pilot-resolve-default-.queue.json`  
  bugs: BUG-11DCF6AC  

- `P2` `workflow_runtime` `RUNTIME` **Strengthen ProviderAdapterContract for cross-cutting concerns**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-strengthen-provideradaptercontract-cross-cu.queue.json`  
  bugs: BUG-C094B483  

- `P2` `workflow_runtime` `WIRING` **Wire workflow_spec_ready staging lifecycle into runtime service**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-wire-workflow-spec-ready-staging.queue.json`  
  bugs: BUG-9229A265  

- `P2` `workflow_runtime` `RUNTIME` **Worker background-consumer loop dies with NameError: _evaluate_ready_specs not defined**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-worker-background-consumer-loop.queue.json`  
  bugs: BUG-2907B68C  

- `P2` `workflow_runtime` `WIRING` **workflow CLI runner has transport implementation dead-end**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-workflow-runtime-title-workflow-cli-runner.queue.json`  
  bugs: BUG-22C9EC03  


### wave_2_execute (4)

- `P2` `workflow_runtime` `RUNTIME` **Repeated receipt failure: provider.capacity**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-2-execute-workflow-runtime-failure-code-provider-capacity.queue.json`  
  bugs: BUG-BC412660  

- `P2` `workflow_runtime` `RUNTIME` **Repeated receipt failure: route.unhealthy**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-2-execute-workflow-runtime-failure-code-route-unhealthy.queue.json`  
  bugs: BUG-EAD36E8A  

- `P2` `workflow_runtime` `RUNTIME` **Repeated receipt failure: sandbox_error**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-2-execute-workflow-runtime-failure-code-sandbox-error.queue.json`  
  bugs: BUG-825B9588, BUG-F28BF7B6, BUG-3DF8F51B, BUG-B8066B82, BUG-9CA0CB78, BUG-7C5D8AE4, BUG-39D02693  

- `P2` `workflow_runtime` `RUNTIME` **Repeated receipt failure: workflow.timeout**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-2-execute-workflow-runtime-failure-code-workflow-timeout.queue.json`  
  bugs: BUG-A3B51E8D  


## LOW tier (17 specs)

### wave_0_authority_repair (16)

- `P3` `authority_bug_system` `OTHER` **Authority memory projection (runtime/authority_memory_projection.py) is not wired into data-dictionary reproject — schem…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-authority-memory-projection.queue.json`  
  bugs: BUG-02EE0886  

- `P3` `authority_bug_system` `WIRING` **Correction + narrowed scope for BUG-DE971274 — Praxis workflow_triggers CLI write surface exists; MCP and schedule_defin…**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-correction-narrowed-scope.queue.json`  
  bugs: BUG-2971285E  

- `P3` `authority_bug_system` `VERIFY` **Dashboard quick reference contains static provider circuit and model values**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-dashboard-quick-reference.queue.json`  
  bugs: BUG-C3800386  

- `P3` `authority_bug_system` `OTHER` **Fresh-clone bootstrap chain idempotency gaps in 091+ (ON CONFLICT failures)**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-fresh-clone-bootstrap-chain.queue.json`  
  bugs: BUG-D0471336  

- `P3` `authority_bug_system` `VERIFY` **input-validation::contracts::domain — validate_workflow_request collapses distinct errors to request.graph_invalid**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-input-validation-contracts-domain.queue.json`  
  bugs: BUG-616A0D2E  

- `P3` `authority_bug_system` `WIRING` **[intent binding] proposed-pill commit helper is not connected to any production path**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-intent-binding-proposed-pill.queue.json`  
  bugs: BUG-DB844CB4  

- `P3` `authority_bug_system` `WIRING` **intent_decomposition verb classifier missing review-tier verbs (audit/confirm/validate/check/inspect/...)**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-intent-decomposition-verb-classifier.queue.json`  
  bugs: BUG-FB870B40  

- `P3` `authority_bug_system` `RUNTIME` **Operator console gives no live signal that the agent is thinking**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-operator-console-gives.queue.json`  
  bugs: BUG-65B18ED6  

- `P3` `authority_bug_system` `OTHER` **Per-harness hooks (Codex / Gemini) have no unit tests — only end-to-end smoke**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-per-harness-hooks-codex.queue.json`  
  bugs: BUG-5ECC795A  

- `P3` `authority_bug_system` `VERIFY` **Python dependency audit tool unavailable from repo quality surface**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-python-dependency-audit.queue.json`  
  bugs: BUG-50ACD990  

- `P3` `authority_bug_system` `WIRING` **Remove deprecated next-actions and legal-tools aliases**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-remove-deprecated-next-actions.queue.json`  
  bugs: BUG-439F6746  

- `P3` `authority_bug_system` `RUNTIME` **Retire scripts/praxis_ctl_local_alpha.py after consumer audit**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-retire-scripts-praxis-ctl-local-alpha-py-afte.queue.json`  
  bugs: BUG-94FF822F  

- `P3` `authority_bug_system` `OTHER` **Snapshot full-clone exercise unverified — bootstrap import path never end-to-end tested**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-snapshot-full-clone-exercise.queue.json`  
  bugs: BUG-B9CD8B04  

- `P3` `authority_bug_system` `OTHER` **Standing-order trigger fires on every consecutive edit to same self-authored file (no session cooldown)**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-standing-order-trigger-fires.queue.json`  
  bugs: BUG-3E9820C4  

- `P3` `authority_bug_system` `OTHER` **Test bug for tracing**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-test-tracing.queue.json`  
  bugs: BUG-BCCFB6A6  

- `P3` `authority_bug_system` `WIRING` **workflow bugs resolve CLI cannot pass promote_to_pattern**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-0-authority-repair-authority-bug-system-title-workflow-resolve-cli.queue.json`  
  bugs: BUG-04568800  


### wave_1_evidence_normalization (1)

- `P3` `app_wiring_frontend` `WIRING` **Bug file schema exposes source_issue_id without FK prerequisite guidance**  
  spec: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets_20260428_full/wave-1-evidence-normalization-app-wiring-frontend-title-file-schema-exposes.queue.json`  
  bugs: BUG-FA9571E0  

