# Wave 6 Contract Deps Cleanup: Workflow Runtime Packet Plan

## Authority Model
- Primary authority is the Praxis.db-backed workflow contract surface and the bug tracker record for each bug.
- The execution bundle and packet artifacts are read-side authority for this job; they define the run boundary, allowed tools, and stop conditions.
- `contracts/domain.py` and `runtime/workflow/submission_contract.py` are the canonical code authorities for the workflow request/submission contract shape.
- Verifier runtime, shell-based validation, workflow specs, and queue artifacts are downstream consumers. If they disagree, the contract source and its tests win.
- For this packet, treat queue artifacts as derived outputs, not as a source of truth.

## Files To Read
- `contracts/domain.py`
- `runtime/workflow/submission_contract.py`
- The tests that cover those modules, especially round-trip and submission-contract coverage.
- The verifier runtime path that validates workflow requests and submissions.
- The workflow specs and queue artifact definitions for the wave-6 contract-deps cleanup packet.
- The current packet directory under `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/` for existing conventions and neighboring packet structure.

## Files Allowed To Change
- None in this planning job.
- In the follow-on implementation packet, the expected change set is limited to:
  - `contracts/domain.py`
  - `runtime/workflow/submission_contract.py`
  - the focused unit tests that exercise those contracts
  - verifier/runtime tests or helpers only if they are required to keep the contract boundary consistent
  - the queue artifact/spec files only if they are derived from the same canonical contract and need regeneration
- Do not widen the change set to unrelated workflow or bug-tracker modules.

## Verification Path
- Add or update focused tests for JSON round-trip behavior on `WorkflowRequest`.
- Add dedicated tests for `runtime/workflow/submission_contract.py`.
- Add a regression test for DB-backed authority resolution when `WORKFLOW_DATABASE_URL` host resolution fails or is unavailable.
- Run the smallest test slice that proves the contract boundary:
  - contract/domain round-trip tests
  - submission contract tests
  - the validator/runtime test that covers DB authority resolution
- If the packet needs queue artifact regeneration, verify the regenerated artifact still matches the canonical contract shape.

## Stop Boundary
- Stop after the plan is approved and the implementation packet is scoped.
- Do not edit code in this job.
- Do not broaden the scope beyond the four listed bugs.
- Do not introduce new contract shapes, new workflow semantics, or unrelated refactors while addressing these bugs.

## Per-Bug Intended Outcome
- `BUG-5D0140CD`: Collapse the validation/verification contract onto a single contract authority so the shell, verifier runtime, workflow specs, and queue artifacts all derive from the same source of truth.
- `BUG-7C80AB3F`: Restore a `WorkflowRequest` JSON round-trip path in `contracts/domain.py` so the request object can be serialized and deserialized without losing contract fidelity.
- `BUG-E162C5F1`: Add dedicated, targeted tests for `runtime/workflow/submission_contract.py` so the submission contract has explicit regression coverage instead of implicit coverage from neighboring tests.
- `BUG-505104EE`: Make workflow validation resolve DB-backed agent authority deterministically even when the `WORKFLOW_DATABASE_URL` host is unavailable, instead of failing before authority resolution completes.
