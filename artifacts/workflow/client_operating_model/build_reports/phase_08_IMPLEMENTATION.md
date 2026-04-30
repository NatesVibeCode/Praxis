# Phase 08 Implementation Report

Date: 2026-04-30

## Summary

Promoted live sandbox promotion and drift feedback into DB-backed CQRS
authority.

Phase 7 owns deterministic Virtual Lab simulation proof. Phase 8 now owns the
next gate: a candidate cannot be recorded as sandbox-promoted unless it points
at a passed simulation run with verifier proof and no promotion blockers. After
the sandbox execution, Praxis persists readback evidence, predicted-vs-actual
comparison rows, drift ledgers, handoff refs, and one stop/continue summary.

## Authority Model

- Authority domain: `authority.virtual_lab_sandbox_promotion`
- Event stream: `stream.authority.virtual_lab_sandbox_promotion`
- Command operation: `virtual_lab_sandbox_promotion_record`
- Query operation: `virtual_lab_sandbox_promotion_read`
- Event contract: `virtual_lab_sandbox_promotion.recorded`
- HTTP route: `/api/virtual-lab/sandbox-promotions`
- MCP tools:
  - `praxis_virtual_lab_sandbox_promotion_record`
  - `praxis_virtual_lab_sandbox_promotion_read`

## Changed Files

- `Code&DBs/Databases/migrations/workflow/374_virtual_lab_sandbox_promotion_authority.sql`
- `Code&DBs/Workflow/runtime/operations/commands/virtual_lab_sandbox_promotion.py`
- `Code&DBs/Workflow/runtime/operations/queries/virtual_lab_sandbox_promotion.py`
- `Code&DBs/Workflow/storage/postgres/virtual_lab_sandbox_promotion_repository.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/virtual_lab_sandbox_promotion.py`
- `Code&DBs/Workflow/surfaces/mcp/cli_metadata.py`
- `Code&DBs/Workflow/system_authority/workflow_migration_authority.json`
- `Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py`
- `Code&DBs/Workflow/tests/unit/test_virtual_lab_sandbox_promotion_operations.py`
- `Code&DBs/Workflow/tests/unit/test_virtual_lab_sandbox_promotion_repository.py`
- `Code&DBs/Workflow/tests/unit/test_virtual_lab_sandbox_promotion_mcp_tool.py`
- `Code&DBs/Workflow/tests/unit/test_operation_catalog_bindings.py`
- `Code&DBs/Workflow/tests/unit/test_operation_catalog_mounting.py`
- `Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py`
- `docs/MCP.md`
- `docs/CLI.md`
- `docs/API.md`

## Implemented Contracts

- Manifest-level sandbox promotion records with manifest and summary digests.
- Per-candidate records bound to verified `virtual_lab_simulation_runs`.
- Controlled sandbox execution records with environment/config/seed refs.
- Readback evidence storage with available/trusted flags and immutable refs.
- Predicted-vs-actual comparison reports and rows.
- Drift ledgers and queryable drift classifications by reason code, severity,
  layer, disposition, owner, and candidate.
- Handoff refs for bug, gap, contract note, evidence, and receipt follow-up.
- Stop/continue summaries with candidate decisions.
- Thin MCP wrappers that dispatch only through the CQRS gateway.

## Guardrails

- `virtual_lab_sandbox_promotion_record` requires one candidate evidence record
  for every manifest candidate.
- By default, each candidate must reference a persisted simulation run with:
  - `status = passed`
  - at least one verifier result
  - all verifier results passed
  - zero promotion blockers
- Missing or untrusted required readback evidence becomes a blocked comparison
  row and must be classified before a drift ledger can close.
- Implementation-defect classification still requires environment, contract,
  and harness causes to be explicitly excluded in the pure domain model.

## Live Proof

- CQRS wizard command forge receipt:
  `4ebba2ab-0bfa-4388-addf-2c7f09ccedee`
- CQRS wizard query forge receipt:
  `d1787b0a-f6ec-4d3b-ae98-ad1603b95fbd`
- Simulation proof read receipt:
  `a396a96b-5b9a-4beb-871a-ba1ea0c6ed70`
- Sandbox promotion record receipt:
  `ad4332bf-4e71-40c3-9842-65f2fb317d03`
- Sandbox promotion event:
  `8e5a5bc6-60fd-473b-814f-2ba60e4e75cc`
- Sandbox promotion list read receipt:
  `649fc9ac-b99b-42d4-ae32-c81b6c04ee20`
- Sandbox promotion describe read receipt:
  `21dfd393-0941-4cd9-8e80-9c75108506d1`
- Roadmap closeout preview receipt:
  `0294a9fa-3f39-4d10-89c2-26fdc1bda2d3`
- Roadmap closeout command receipt:
  `5a70709b-9f03-4023-8b81-5187b51d90fd`
- Roadmap closeout event:
  `970fa8c3-3eae-49e2-b75b-112a3b9aa6c5`
- Roadmap readback receipt:
  `8a1e760e-eb73-48cb-bb87-9982ad21aa62`

Live record:

- Promotion record: `sandbox_promotion_record.phase_08_live_proof`
- Manifest: `manifest.phase_08_live_proof`
- Candidate: `candidate.phase8.account_sync`
- Simulation proof: `virtual_lab_simulation_run.phase_07_proof`
- Recommendation: `continue`
- Candidate decision: `validated`
- Comparison status: `match`
- Drift classifications: `0`
- Handoffs: `0`

Operation catalog readback confirmed:

- `virtual_lab_sandbox_promotion_record` -> `POST /api/virtual-lab/sandbox-promotions`,
  interactive command, event required, event type
  `virtual_lab_sandbox_promotion.recorded`
- `virtual_lab_sandbox_promotion_read` -> `GET /api/virtual-lab/sandbox-promotions`,
  interactive read-only query

## Validation

```text
py_compile passed
6 passed in 0.40s
69 passed in 0.69s
77 passed in 0.03s
9 passed in 0.62s
scoped git diff/trailing-whitespace checks passed for Phase 8 files
live CQRS write/read smoke passed
roadmap closeout and readback passed
```

The combined docs-plus-integration pytest invocation still has the existing
route-count false negative when `tests/integration/conftest.py` repoints
`WORKFLOW_DATABASE_URL` at `praxis_test`. The split checks above are the valid
authority lanes: docs against the active generated catalog, integration
migration contracts against the test database.

## Boundary

This phase records live sandbox promotion evidence and drift feedback. It does
not itself call live integrations, mutate client-live systems, file bugs, open
gaps, or update Object Truth. Those remain explicit downstream authority
actions triggered by the handoff refs and stop/continue recommendation.
