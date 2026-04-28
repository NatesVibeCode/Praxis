# Wave 0 Plan: Bug Evidence Authority

## Authority Model

- Source of truth for this packet is Praxis.db standing orders, the active packet queue, and the current repository snapshot plus artifact state.
- This job is planning only. It may inspect code and artifacts, but it must not change runtime code, DB state, bug rows, or receipts.
- The execute packet must treat bug records, evidence links, and operation receipts as authoritative only when they come from the canonical workflow surfaces, not from ad hoc scripts or local fallback tables.
- `BUG-1D9FAF57` is about bug filing authority: the filing path must fail closed when discovery evidence is underlinked.
- `BUG-1DBACCD8` is about read authority: the bug surface and the documented PostgreSQL fallback must read the same canonical bug table with the same semantics.
- `BUG-9B812B32` is about proof authority: operation receipts must be durable execution proof, not response decoration.
- `BUG-A84383D1` is about reload authority: `praxis_reload` must be auditable and receipt-backed before any live process mutation is accepted as complete.
- `BUG-175EB9F3` is the umbrella lifecycle bug: bug tracking and resolution must collapse into one canonical chain across runtime, surfaces, scripts, evidence, and packet artifacts.
- If `praxis workflow discover` and `praxis workflow recall` are available in the execute packet, use them before inventing new implementation patterns; this planning job falls back to direct source inspection only because the local CLI bridge is unavailable here.

## Files To Read

Read the smallest set that proves the authority seams and the verification targets:

- `AGENTS.md`
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/coordination.json`
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/packets/wave-0-bug-evidence-authority-bug-evidence-1.queue.json`
- `Code&DBs/Workflow/runtime/bug_tracker.py`
- `Code&DBs/Workflow/runtime/bug_evidence.py`
- `Code&DBs/Workflow/runtime/bug_resolution_program.py`
- `Code&DBs/Workflow/runtime/operation_catalog_gateway.py`
- `Code&DBs/Workflow/runtime/receipt_store.py`
- `Code&DBs/Workflow/runtime/operations/commands/bug_actions.py`
- `Code&DBs/Workflow/surfaces/cli/commands/authority.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/health.py`
- `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
- `Code&DBs/Workflow/surfaces/api/handlers/_query_bugs.py`
- `Code&DBs/Workflow/surfaces/api/handlers/workflow_query_core.py`
- `Code&DBs/Workflow/storage/postgres/bug_evidence_repository.py`
- `Code&DBs/Workflow/storage/postgres/receipt_repository.py`
- `Code&DBs/Workflow/tests/unit/test_bug_tracker_filing_evidence.py`
- `Code&DBs/Workflow/tests/unit/test_bug_tracker_bug_evidence_wiring.py`
- `Code&DBs/Workflow/tests/unit/test_bug_surface_default_open_only_unified.py`
- `Code&DBs/Workflow/tests/unit/test_bug_surface_db_isolation_guardrail.py`
- `Code&DBs/Workflow/tests/unit/test_operation_catalog_gateway.py`
- `Code&DBs/Workflow/tests/unit/test_cli_authority_surface.py`
- `Code&DBs/Workflow/tests/unit/test_receipt_repository_emit.py`
- `Code&DBs/Workflow/tests/unit/test_praxis_reload_runtime_modules.py`

If the execute packet needs more context, read sibling wave-0 artifacts that describe the same authority split or proof requirement, but do not widen beyond this bug set.

## Files Allowed To Change

- In this job: only `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-0-bug-evidence-authority-bug-evidence-1/PLAN.md`.
- In the follow-on execute packet, keep writes constrained to the exact authority seam under review:
  - bug filing and evidence validation code
  - bug query and fallback read paths
  - receipt persistence and gateway proof emission
  - reload authority and audit emission
  - tests and fixtures needed to prove the bug is closed
- Do not change DB schema, bug rows, or unrelated runtime cleanup paths in this packet family.

## Verification Path

Use the narrowest checks that prove each authority boundary or leave a precise blocker:

1. Inspect the current code path for the bug filing, bug query, receipt, and reload surfaces named above.
2. For `BUG-1D9FAF57`, verify the bug-file action rejects an underlinked request instead of creating an authoritative bug row.
3. For `BUG-1DBACCD8`, verify the bug surface and the documented PostgreSQL fallback read the same canonical bug authority and expose the same list/search/read semantics.
4. For `BUG-9B812B32`, verify the operation catalog gateway persists proof as an authoritative receipt, not as response-only decoration.
5. For `BUG-A84383D1`, verify `praxis_reload` emits durable audit proof and cannot be treated as complete unless the live mutation is backed by persisted receipt evidence.
6. For `BUG-175EB9F3`, verify the bug lifecycle has one reviewable authority chain from filing through evidence, replay, resolution, and packet materialization.
7. Run only the smallest regression tests that cover the affected seam for the bug under inspection.

## Stop Boundary

- Stop after the plan is written and the authority boundaries are explicit.
- Do not implement code in this job.
- Do not change bug state, DB contents, or receipt rows in this job.
- Do not widen scope to unrelated runtime cleanup, unrelated bugs, or speculative refactors.
- If required evidence is missing or contradictory, stop and record the gap rather than inventing a source of truth.

## Per-Bug Intended Outcome

- `BUG-175EB9F3 [P1/ARCHITECTURE]`
  - Intended outcome: bug tracking and resolution authority converges on one canonical lifecycle across runtime, surfaces, scripts, evidence, and packet artifacts.
  - Success means every path points to the same bug authority model and no parallel lifecycle source remains.

- `BUG-1D9FAF57 [P1/ARCHITECTURE]`
  - Intended outcome: bug-file actions reject or fail closed when the evidence chain is underlinked relative to the authority requirement.
  - Success means no accepted bug row can remain underlinked.

- `BUG-1DBACCD8 [P1/ARCHITECTURE]`
  - Intended outcome: the bug surface and the documented PostgreSQL fallback resolve against the same canonical bug table and the same read semantics.
  - Success means there is no split-brain read path or documentation mismatch.

- `BUG-9B812B32 [P1/ARCHITECTURE]`
  - Intended outcome: operation receipts become atomic durable proof emitted by the catalog gateway, not response decoration.
  - Success means the receipt is the authoritative execution record and survives as proof even when the response payload is reshaped.

- `BUG-A84383D1 [P2/RUNTIME]`
  - Intended outcome: `praxis_reload` becomes a durable, auditable operation whose live state mutation is only accepted when backed by persisted operational proof.
  - Success means reload has receipt-backed authority instead of an ephemeral process-only side effect.
