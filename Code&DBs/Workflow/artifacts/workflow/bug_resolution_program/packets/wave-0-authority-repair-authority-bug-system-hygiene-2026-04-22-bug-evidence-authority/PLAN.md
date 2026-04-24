# Bugs in scope

- Packet: `bug_resolution_program_20260423.wave-0-authority-repair-authority-bug-system-hygiene-2026-04-22-bug-evidence-authority`
- Packet kind: `authority_repair`
- Lane: `Authority / bug system (authority_bug_system)`
- Authority owner: `lane:authority_bug_system`
- Wave: `wave_0_authority_repair`
- Cluster: `hygiene-2026-04-22-bug-evidence-authority`
- Depends on wave: `none`
- Bug IDs:
  - `BUG-A75BC81E`
  - `BUG-69870BA5`
  - `BUG-1D9FAF57`
- Repo-local packet authority available in this workspace:
  - [artifacts/workflow/root_bug_table_remediation_20260423.md](/workspace/artifacts/workflow/root_bug_table_remediation_20260423.md)
  - [artifacts/workflow/root_bug_table_remediation_20260423.queue.json](/workspace/artifacts/workflow/root_bug_table_remediation_20260423.queue.json)
- Note:
  - The external kickoff JSON path provided in the job input is not mounted in this workspace, so execution should treat the repo-local remediation queue plus this packet contract as the available authority snapshot for planning.

# Titles in scope

- [hygiene-2026-04-22/bug-evidence] Post-receipt hooks swallow ledger and evidence-link failures
- [hygiene-2026-04-22/bug-evidence] Receipt failure auto-filer uses different identities for counting and dedupe
- [hygiene-2026-04-22/bug-evidence] Bug file action accepts underlinked rows despite evidence authority requirement

# Files to read first

- Packet and wave context:
  - [artifacts/workflow/root_bug_table_remediation_20260423.md](/workspace/artifacts/workflow/root_bug_table_remediation_20260423.md)
  - [artifacts/workflow/root_bug_table_remediation_20260423.queue.json](/workspace/artifacts/workflow/root_bug_table_remediation_20260423.queue.json)
- Primary runtime authority for the three bugs:
  - [Code&DBs/Workflow/runtime/receipt_store.py](/workspace/Code&DBs/Workflow/runtime/receipt_store.py)
  - [Code&DBs/Workflow/runtime/bug_tracker.py](/workspace/Code&DBs/Workflow/runtime/bug_tracker.py)
  - [Code&DBs/Workflow/runtime/bug_evidence.py](/workspace/Code&DBs/Workflow/runtime/bug_evidence.py)
- Shared bug surface contracts and read paths that must stay aligned:
  - [Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py)
  - [Code&DBs/Workflow/surfaces/mcp/tools/bugs.py](/workspace/Code&DBs/Workflow/surfaces/mcp/tools/bugs.py)
  - [Code&DBs/Workflow/surfaces/api/handlers/_query_bugs.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/_query_bugs.py)
  - [Code&DBs/Workflow/surfaces/api/handlers/workflow_query_core.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_query_core.py)
  - [Code&DBs/Workflow/runtime/operations/queries/operator_observability.py](/workspace/Code&DBs/Workflow/runtime/operations/queries/operator_observability.py)
- Orient/runtime-binding surfaces because packet verification explicitly includes `workflow orient`:
  - [Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py)
  - [Code&DBs/Workflow/runtime/primitive_contracts.py](/workspace/Code&DBs/Workflow/runtime/primitive_contracts.py)
  - [Code&DBs/Workflow/runtime/_workflow_database.py](/workspace/Code&DBs/Workflow/runtime/_workflow_database.py)
- Focused tests that already cover the affected path:
  - [Code&DBs/Workflow/tests/unit/test_receipt_store_payloads.py](/workspace/Code&DBs/Workflow/tests/unit/test_receipt_store_payloads.py)
  - [Code&DBs/Workflow/tests/unit/test_bug_tracker.py](/workspace/Code&DBs/Workflow/tests/unit/test_bug_tracker.py)
  - [Code&DBs/Workflow/tests/unit/test_bug_tracker_bug_evidence_wiring.py](/workspace/Code&DBs/Workflow/tests/unit/test_bug_tracker_bug_evidence_wiring.py)
  - [Code&DBs/Workflow/tests/unit/test_bug_surface_default_open_only_unified.py](/workspace/Code&DBs/Workflow/tests/unit/test_bug_surface_default_open_only_unified.py)
  - [Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py)
  - [Code&DBs/Workflow/tests/unit/test_operator_observability_queries.py](/workspace/Code&DBs/Workflow/tests/unit/test_operator_observability_queries.py)
  - [Code&DBs/Workflow/tests/unit/test_dependency_truth_surfaces.py](/workspace/Code&DBs/Workflow/tests/unit/test_dependency_truth_surfaces.py)
  - [Code&DBs/Workflow/tests/integration/test_mcp_workflow_server.py](/workspace/Code&DBs/Workflow/tests/integration/test_mcp_workflow_server.py)

# Files allowed to change

- Runtime bug/evidence authority:
  - `Code&DBs/Workflow/runtime/receipt_store.py`
  - `Code&DBs/Workflow/runtime/bug_tracker.py`
  - `Code&DBs/Workflow/runtime/bug_evidence.py`
- Bug read surfaces and shared response shaping:
  - `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
  - `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py`
  - `Code&DBs/Workflow/surfaces/api/handlers/_query_bugs.py`
  - `Code&DBs/Workflow/surfaces/api/handlers/workflow_query_core.py`
  - `Code&DBs/Workflow/runtime/operations/queries/operator_observability.py`
- Orient/runtime-binding projection only if needed for the verification surface and only to repair the declared authority-bug-system path:
  - `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py`
  - `Code&DBs/Workflow/runtime/primitive_contracts.py`
  - `Code&DBs/Workflow/runtime/_workflow_database.py`
- Tests covering the touched path:
  - `Code&DBs/Workflow/tests/unit/test_receipt_store_payloads.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker_bug_evidence_wiring.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_surface_default_open_only_unified.py`
  - `Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py`
  - `Code&DBs/Workflow/tests/unit/test_operator_observability_queries.py`
  - `Code&DBs/Workflow/tests/unit/test_dependency_truth_surfaces.py`
  - `Code&DBs/Workflow/tests/integration/test_mcp_workflow_server.py`
- Do not widen beyond this packet into unrelated bug clusters, queue specs, or general bug-search performance work unless the failing proof shows the bug-system authority path above cannot be repaired without a tightly-coupled contract update in one of these files.

# Verification or closure proof required

- Required verification surface from the packet contract:
  - `workflow orient`
  - bug `stats`
  - bug `list`
  - bug `search`
  - replay-ready view
- Proof expectation:
  - all four surfaces return cleanly for the affected path
  - no swallowed hook failures when friction-ledger or evidence-link writes fail
  - receipt-failure aggregation uses one canonical identity between threshold counting and dedupe
  - bug filing does not admit new underlinked rows when evidence authority is required
  - underlinked bugs remain observable as a remediation queue/count, not silently accepted as healthy
- Minimum test slice before closure:
  - `Code&DBs/Workflow/tests/unit/test_receipt_store_payloads.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker.py`
  - `Code&DBs/Workflow/tests/unit/test_bug_tracker_bug_evidence_wiring.py`
  - `Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py`
  - `Code&DBs/Workflow/tests/unit/test_operator_observability_queries.py`
  - `Code&DBs/Workflow/tests/unit/test_dependency_truth_surfaces.py`
- Integration confirmation if runtime is available:
  - `Code&DBs/Workflow/tests/integration/test_mcp_workflow_server.py`
- Closure evidence should include:
  - exact commands run
  - pass/fail output summary
  - the touched verification surfaces and the clean result for each
  - explicit mapping from each of the three bug IDs to the repair or blocker

# Stop boundary

- Stop after authority-bug-system repairs required for:
  - post-receipt hook failure visibility
  - canonical receipt-failure dedupe/count identity
  - underlinked bug-file rejection or hard gating
  - clean orient + bug stats/list/search + replay-ready read surfaces for this path
- Do not:
  - edit queue specs or unrelated packet artifacts
  - widen into unrelated replay bugs, verifier bugs, or general search/ranking work
  - introduce new authority paths outside the listed runtime/surface files
  - close any bug on prose alone without proof from the required verification surface
