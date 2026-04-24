# Bugs in scope

- Packet: `bug_resolution_program_20260424.wave-0-bug-evidence-authority-bug-evidence-1`
- Packet kind: `authority_repair`
- Lane: `Authority / bug system (authority_bug_system)`
- Authority owner: `lane:authority_bug_system`
- Wave: `wave_0_authority_repair`
- Depends on wave: `none`
- Bug IDs:
  - `BUG-175EB9F3`
  - `BUG-1D9FAF57`
  - `BUG-1DBACCD8`
  - `BUG-9B812B32`
  - `BUG-A84383D1`

# Titles in scope

- `BUG-175EB9F3`: Bug tracking and resolution lifecycle is spread across runtime, surfaces, scripts, evidence, and packet artifacts
- `BUG-1D9FAF57`: `[hygiene-2026-04-22/bug-evidence] Bug file action accepts underlinked rows despite evidence authority requirement`
- `BUG-1DBACCD8`: `[hygiene-2026-04-22/db-authority] Bug surface and documented Postgres fallback read different bug tables`
- `BUG-9B812B32`: `[hygiene-2026-04-23/operation-receipts] Operation catalog execution receipts are response decoration instead of atomic durable proof`
- `BUG-A84383D1`: `[hygiene-2026-04-23/reload-workaround-authority] praxis_reload mutates live process state without durable operational receipt`

# Authority model

- Praxis.db standing orders are the session authority. Treat the active `operator_decisions` rows as durable instructions until explicitly retired.
- Bug truth must come from the canonical bug/receipt/evidence runtime surfaces, not from packet prose, queue files, local assumptions, or legacy fallback paths.
- `workflow orient`, `bug stats`, `bug list`, `bug search`, and the replay-ready view are verification surfaces only. They must reflect the same canonical bug authority and DB identity.
- `praxis_bugs(action='file'|'attach_evidence'|'resolve'|...)`, `bug_tracker`, `bug_evidence`, `receipt_store`, `operation_catalog`, and the `praxis_reload` audit/receipt path are the authoritative mutation/read surfaces for this packet.
- Packet artifacts are derived coordination state. They may describe the workflow, but they are not a separate source of bug authority.
- Do not treat documented fallback tables, ad hoc SQL, or local runtime state as an alternate authority.

# Files to read first

- Packet and program context:
  - `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/coordination.json`
  - `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/packets/wave-0-bug-evidence-authority-bug-evidence-1.queue.json`
  - `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260423.json`
  - `Code&DBs/Workflow/runtime/bug_resolution_program.py`
- Canonical bug/evidence/receipt authority:
  - `Code&DBs/Workflow/runtime/bug_tracker.py`
  - `Code&DBs/Workflow/runtime/bug_evidence.py`
  - `Code&DBs/Workflow/runtime/receipt_store.py`
  - `Code&DBs/Workflow/runtime/workflow/receipt_writer.py`
  - `Code&DBs/Workflow/storage/postgres/bug_evidence_repository.py`
- Bug surfaces and read-path shaping:
  - `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
  - `Code&DBs/Workflow/surfaces/api/handlers/_query_bugs.py`
  - `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py`
  - `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py`
  - `Code&DBs/Workflow/surfaces/mcp/tools/operator.py`
  - `Code&DBs/Workflow/surfaces/mcp/tools/health.py`
  - `Code&DBs/Workflow/surfaces/mcp/tools/workflow.py`
  - `Code&DBs/Workflow/surfaces/mcp/cli_metadata.py`
  - `Code&DBs/Workflow/surfaces/api/operation_catalog_authority.py`
- Operation catalog / durable proof path:
  - `Code&DBs/Workflow/runtime/operation_catalog.py`
  - `Code&DBs/Workflow/runtime/operation_catalog_gateway.py`
  - `Code&DBs/Workflow/runtime/operations/queries/operator_observability.py`
  - `Code&DBs/Workflow/surfaces/api/rest.py`
  - `Code&DBs/Workflow/surfaces/api/handlers/workflow_query_core.py`
- Focused tests that already pin the affected paths:
  - `Code&DBs/Workflow/tests/unit/test_bug_resolution_program.py`
  - `Code&DBs/Workflow/tests/unit/test_spec_compiler_launch_plan.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker_bug_evidence_wiring.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker_read_only.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_surface_default_open_only_unified.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_surface_db_isolation_guardrail.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_authority_status_queries.py`
  - `Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py`
  - `Code&DBs/Workflow/tests/unit/test_operator_observability_queries.py`
  - `Code&DBs/Workflow/tests/unit/test_operation_catalog_authority.py`
  - `Code&DBs/Workflow/tests/unit/test_operation_catalog_gateway.py`
  - `Code&DBs/Workflow/tests/unit/test_operation_catalog_runtime.py`
  - `Code&DBs/Workflow/tests/unit/test_receipt_store_payloads.py`
  - `Code&DBs/Workflow/tests/unit/test_self_healing_and_receipts.py`
  - `Code&DBs/Workflow/tests/unit/test_praxis_reload_runtime_modules.py`
  - `Code&DBs/Workflow/tests/unit/test_cli_authority_surface.py`
  - `Code&DBs/Workflow/tests/integration/test_mcp_workflow_server.py`

# Files allowed to change

- Bug-resolution program and packet-authority alignment:
  - `Code&DBs/Workflow/runtime/bug_resolution_program.py`
  - `Code&DBs/Workflow/surfaces/mcp/tools/workflow.py`
  - `Code&DBs/Workflow/surfaces/mcp/cli_metadata.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_resolution_program.py`
  - `Code&DBs/Workflow/tests/unit/test_spec_compiler_launch_plan.py`
- Bug/evidence/receipt authority and bug read surfaces:
  - `Code&DBs/Workflow/runtime/bug_tracker.py`
  - `Code&DBs/Workflow/runtime/bug_evidence.py`
  - `Code&DBs/Workflow/runtime/receipt_store.py`
  - `Code&DBs/Workflow/runtime/workflow/receipt_writer.py`
  - `Code&DBs/Workflow/storage/postgres/bug_evidence_repository.py`
  - `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
  - `Code&DBs/Workflow/surfaces/api/handlers/_query_bugs.py`
  - `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py`
  - `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py`
  - `Code&DBs/Workflow/surfaces/mcp/tools/operator.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker_bug_evidence_wiring.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker_read_only.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_surface_default_open_only_unified.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_surface_db_isolation_guardrail.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_authority_status_queries.py`
  - `Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py`
  - `Code&DBs/Workflow/tests/unit/test_operator_observability_queries.py`
  - `Code&DBs/Workflow/tests/unit/test_receipt_store_payloads.py`
- Operation catalog durable-proof path:
  - `Code&DBs/Workflow/runtime/operation_catalog.py`
  - `Code&DBs/Workflow/runtime/operation_catalog_gateway.py`
  - `Code&DBs/Workflow/runtime/operations/queries/operator_observability.py`
  - `Code&DBs/Workflow/surfaces/api/operation_catalog_authority.py`
  - `Code&DBs/Workflow/surfaces/api/rest.py`
  - `Code&DBs/Workflow/surfaces/api/handlers/workflow_query_core.py`
  - `Code&DBs/Workflow/tests/unit/test_operation_catalog_authority.py`
  - `Code&DBs/Workflow/tests/unit/test_operation_catalog_gateway.py`
  - `Code&DBs/Workflow/tests/unit/test_operation_catalog_runtime.py`
  - `Code&DBs/Workflow/tests/unit/test_self_healing_and_receipts.py`
  - `Code&DBs/Workflow/tests/unit/test_receipt_repository_emit.py`
- `praxis_reload` durable operational proof path:
  - `Code&DBs/Workflow/surfaces/mcp/tools/health.py`
  - `Code&DBs/Workflow/surfaces/cli/commands/authority.py`
  - `Code&DBs/Workflow/surfaces/mcp/cli_metadata.py`
  - `Code&DBs/Workflow/tests/unit/test_praxis_reload_runtime_modules.py`
  - `Code&DBs/Workflow/tests/unit/test_cli_authority_surface.py`

# Verification path

- First, confirm the active authority path with `workflow orient` and the bug read surfaces:
  - `workflow orient`
  - `workflow bugs stats`
  - `workflow bugs list`
  - `workflow bugs search`
  - `workflow tools call praxis_replay_ready_bugs`
- Then verify the packet-specific invariants:
  - `BUG-1D9FAF57`: a targeted `praxis_bugs(action='file', ...)` attempt without required evidence must fail closed or route to an explicit remediation state; it must not silently create an underlinked bug row.
  - `BUG-1DBACCD8`: `bug stats`, `bug list`, `bug search`, and replay-ready must all agree on the same bug-table authority / DB identity.
  - `BUG-9B812B32`: operation-catalog execution must emit durable proof that is queryable through the receipt path, not only a decorated response body.
  - `BUG-A84383D1`: `praxis_reload` must produce a durable operational receipt or explicit refusal path; live process mutation without proof is not acceptable.
  - `BUG-175EB9F3`: the bug-resolution lifecycle must be derivable from one canonical runtime authority path, with packet artifacts acting only as projections.
- Minimum test slice before closure:
  - `Code&DBs/Workflow/tests/unit/test_bug_resolution_program.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker_bug_evidence_wiring.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker_read_only.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_surface_default_open_only_unified.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_surface_db_isolation_guardrail.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_authority_status_queries.py`
  - `Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py`
  - `Code&DBs/Workflow/tests/unit/test_operator_observability_queries.py`
  - `Code&DBs/Workflow/tests/unit/test_operation_catalog_authority.py`
  - `Code&DBs/Workflow/tests/unit/test_operation_catalog_gateway.py`
  - `Code&DBs/Workflow/tests/unit/test_operation_catalog_runtime.py`
  - `Code&DBs/Workflow/tests/unit/test_receipt_store_payloads.py`
  - `Code&DBs/Workflow/tests/unit/test_self_healing_and_receipts.py`
  - `Code&DBs/Workflow/tests/unit/test_praxis_reload_runtime_modules.py`
  - `Code&DBs/Workflow/tests/integration/test_mcp_workflow_server.py`
- Closure evidence must include the exact commands run, the pass/fail result for each verification surface, and the mapping from each bug ID to either a fix or a documented blocker.

# Stop boundary

- Do not edit code in this planning job.
- Do not widen beyond the five bug IDs above or the authority surfaces named in this packet.
- Do not add ad hoc SQL, local Postgres assumptions, or a second bug-table authority.
- Do not re-scope into unrelated runtime, UI, provider-routing, or setup work unless a touched verifier proves that the canonical authority path cannot be repaired without the adjacent helper.
- Do not close any bug on prose alone; each terminal outcome needs verifier-backed evidence or an explicit blocker.

# Per-bug intended outcome

- `BUG-175EB9F3`: converge bug resolution lifecycle state onto one canonical runtime authority path so bug filing, evidence, receipts, packet generation, and closure all point at the same truth source.
- `BUG-1D9FAF57`: reject underlinked bug filing when evidence authority is required, or surface the underlinked row as an explicit remediation queue item rather than a healthy bug record.
- `BUG-1DBACCD8`: make the bug surface and documented Postgres fallback resolve to one bug table / DB identity with no split-brain read path.
- `BUG-9B812B32`: make operation-catalog execution receipts atomic durable proof, not response decoration, and prove them through the canonical receipt path.
- `BUG-A84383D1`: require a durable operational receipt or an explicit denied path for `praxis_reload`, so live process mutation never happens without proof.
