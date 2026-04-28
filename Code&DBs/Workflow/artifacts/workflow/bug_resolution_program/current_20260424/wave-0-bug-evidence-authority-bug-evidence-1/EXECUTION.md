# Execute Packet: `wave-0-bug-evidence-authority-bug-evidence-1`

## Summary

Implemented the smallest durable fix for `BUG-1D9FAF57`: the bug-file action now fails closed unless the caller supplies `discovered_in_run_id` or `discovered_in_receipt_id`, so the public filing path cannot create a new underlinked bug row.

I did not resolve any bug row in this job. The broader architecture bugs remain deferred in this packet because they span larger authority seams than this contained action-layer fix.

## Changed files

- `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
- `Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py`
- `Code&DBs/Workflow/tests/integration/test_mcp_workflow_server.py`
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-0-bug-evidence-authority-bug-evidence-1/EXECUTION.md`

## Discovery and orientation evidence

1. Read the packet plan first:
   - `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-0-bug-evidence-authority-bug-evidence-1/PLAN.md`

2. Attempted required orientation and discovery surfaces:
   - `praxis workflow tools call praxis_operator_decisions ...`
   - `curl --max-time 5 -X POST http://host.docker.internal:8420/orient ...`
   - repo-local CLI bootstrap through `python3` and `surfaces.cli.main`

3. Environment proof for the blocked tool path:
   - the packaged `praxis` CLI fails with `ModuleNotFoundError: No module named 'json'`
   - direct `python3` imports of stdlib `json` fail with the same error
   - `psql`, `rg`, and `git` are not installed in this container
   - `/orient` timed out from the shell

4. Because the mandated tool path was not executable in this container, I used direct source inspection of the scoped runtime/surface/test files to keep the change grounded in the local authority code.

## Code evidence collected

### `BUG-1D9FAF57`

- `runtime/bug_tracker.py` shows the current tracker still allows direct `file_bug(...)` calls with no discovery anchor, and `stats()` explicitly counts rows with no `discovered_in_run_id`, no `discovered_in_receipt_id`, and no `bug_evidence_links` as `underlinked_count`.
- `surfaces/api/handlers/_bug_surface_contract.py` is the public bug-file action seam used by `workflow_query_core.handle_bugs(...)` and by the gateway command wrapper in `runtime/operations/commands/bug_actions.py`.
- The smallest durable fix is therefore to reject underlinked payloads in `file_bug_payload(...)` before the tracker inserts the bug row.

### `BUG-9B812B32`

- `runtime/operation_catalog_gateway.py` persists the receipt through `_persist_operation_outcome(...)` after the handler returns.
- That is a larger architectural receipt-timing issue than this packet's smallest fix and was not changed here.

### `BUG-175EB9F3`, `BUG-1DBACCD8`, `BUG-A84383D1`

- Source inspection confirms these remain cross-cutting:
  - lifecycle spread touches runtime, API handlers, MCP tools, tests, and packet artifacts
  - bug read authority alignment spans query surfaces and documented fallback behavior
  - reload authority is covered in dedicated reload runtime code/tests
- None of those can be closed responsibly as a side effect of the filing gate fix.

## Implementation details

1. Added `_require_discovery_authority(body)` to `surfaces/api/handlers/_bug_surface_contract.py`.
2. Called that guard at the top of `file_bug_payload(...)`, after title validation and before dry-run or persistence.
3. Added a unit regression in `tests/unit/test_workflow_query_handlers.py` proving the bug-file action now rejects an underlinked request and never reaches `BugTracker.file_bug(...)`.
4. Updated MCP integration tests in `tests/integration/test_mcp_workflow_server.py` so valid bug-file calls include `discovered_in_receipt_id: "receipt-123"`, and added a regression that the MCP bug-file action rejects a request with no discovery anchor.

## Verification status

- Intended verifier scope:
  - `tests/unit/test_workflow_query_handlers.py`
  - `tests/integration/test_mcp_workflow_server.py`
- I could not execute Python-based verification in this container because `python3` cannot import stdlib `json`, which also breaks the local `praxis` CLI.
- I therefore have code-level proof by source inspection and targeted regression additions, but not an executed test run from this environment.
- Required workflow verify ref remains pending downstream:
  - `verify.bug_resolution_current_20260424.wave-0-bug-evidence-authority-bug-evidence-1.execute_packet`

## Intended terminal status per bug

### `BUG-1D9FAF57` [P1/ARCHITECTURE]

- Intended terminal status: `FIX_PENDING_VERIFICATION`
- Basis:
  - public bug-file action now rejects the underlinked shape that `stats().underlinked_count` classifies as non-authoritative
  - unit and MCP integration regressions were added for the failure-closed behavior
  - runtime verification is still required in a container with a working Python toolchain

### `BUG-175EB9F3` [P1/ARCHITECTURE]

- Intended terminal status: `DEFERRED`
- Basis:
  - lifecycle convergence is broader than the action-layer fix and still spans runtime, surfaces, scripts, evidence, and artifacts

### `BUG-1DBACCD8` [P1/ARCHITECTURE]

- Intended terminal status: `DEFERRED`
- Basis:
  - this packet did not change the bug-surface read path or the documented Postgres fallback; canonical table alignment still needs its own proof pass

### `BUG-9B812B32` [P1/ARCHITECTURE]

- Intended terminal status: `DEFERRED`
- Basis:
  - receipt atomicity is centered in `runtime/operation_catalog_gateway.py` and requires a broader architectural change than the smallest contained fix

### `BUG-A84383D1` [P2/RUNTIME]

- Intended terminal status: `DEFERRED`
- Basis:
  - reload receipt-backing lives on a separate runtime surface and was not changed in this packet

## Submission note

- The execution bundle requires a sealed submission through `praxis submit_code_change` / `praxis_submit_code_change`.
- The packaged CLI path is broken in this container, so I called `praxis_submit_code_change` directly over `/mcp`.
- That submit attempt returned `workflow_submission.service_error` with a server-side `workflow_outbox.transition_seq` null-constraint failure.
- A follow-up `praxis_get_submission` call still returned a sealed submission for this job label, but its measured `changed_paths` point at unrelated files (`storage/_generated_workflow_migration_authority.py`, `tests/unit/test_workflow_migration_idempotence.py`) rather than the files changed in this run.
- Treat the sealed-submission state as inconsistent until the workflow submission service is repaired or the harness re-seals this job cleanly.
