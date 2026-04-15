# Phase 4 Gate and Promotion Policy

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `4` (`Gate and Promotion Policy`), status `historical_foundation`, predecessor `3`, required closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is grounded in the current checked-out repo at `/workspace`. The platform context names `/Users/nate/Praxis`, but that path is not present in this execution environment, so all repo evidence and verification commands below use the live checkout path and the supplied database target `postgresql://nate@127.0.0.1:5432/praxis`.

## 1. Objective in repo terms

- Prove that the live workflow submission review seam projects canonical Phase 4 policy truth into the database, not just the isolated policy engine.
- The bounded repo target is [Code&DBs/Workflow/runtime/workflow/submission_capture.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_capture.py) `review_submission(...)` calling [Code&DBs/Workflow/runtime/workflow/submission_policy.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_policy.py) `evaluate_publish_policy(...)` and writing rows that satisfy the Phase 4 schema contract in [Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql](/workspace/Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql).
- Keep the sprint on one runtime seam only: publish-style review approval with verification evidence and explicit finalization inputs. Do not widen into repo-wide policy cleanup.

## 2. Current evidence in the repo

- The authority map declares phase `4` as `Gate and Promotion Policy` with the mandatory closeout sequence `review -> healer -> human_approval` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json).
- The Phase 4 schema authority already exists in [Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql](/workspace/Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql):
- it creates `gate_evaluations`
- it constrains `promotion_decisions` to final `accept` or `reject`
- it binds promotion rows back to gate truth with `promotion_decisions_gate_truth_fkey`
- it requires finalization evidence for accepted promotion rows with `promotion_decisions_accept_evidence_check`
- The Phase 4 policy engine already exists in [Code&DBs/Workflow/policy/gate.py](/workspace/Code&DBs/Workflow/policy/gate.py):
- `evaluate_gate(...)` returns `accept`, `reject`, or `block`
- `decide_promotion(...)` refuses blocked gate evaluations and only emits final promotion truth
- accepted promotion requires a current head, promotion intent, finalization timestamp, and canonical commit ref
- The live runtime projection seam already exists:
- [Code&DBs/Workflow/runtime/workflow/submission_capture.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_capture.py) `review_submission(...)` normalizes review inputs, reads `verification_artifact_refs`, and calls `evaluate_publish_policy(...)`
- [Code&DBs/Workflow/runtime/workflow/submission_policy.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_policy.py) evaluates the gate, inserts `gate_evaluations`, and conditionally inserts `promotion_decisions`
- The current tests prove parts of the contract, but not the end-to-end DB-backed runtime seam:
- [Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py](/workspace/Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py) proves the schema and policy authority directly against Postgres
- [Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py) proves `review_submission(...)` issues the expected insert statements through a fake connection
- There is no current integration test that drives `review_submission(...)` through a real database-backed submission record and then asserts the stored Phase 4 rows.

## 3. Gap or ambiguity still remaining

- Phase 4 is implemented, but the runtime proof is not converged.
- The unresolved gap is whether the actual submission review path stores Phase 4 truth correctly when fed a real sealed submission and real database state.
- The current repo still leaves room for drift between:
- input normalization in `submission_capture.review_submission(...)`
- projection logic in `submission_policy.evaluate_publish_policy(...)`
- the actual constraints enforced by migration `003_gate_and_promotion_policy.sql`
- The first sprint should not widen into:
- admission policy
- retry policy
- publish UI or MCP redesign
- new migration work
- review scheduling or recurring repair orchestration

## 4. One bounded first sprint only

- Add one focused integration proof that uses the real submission review path against Postgres.
- Seed the minimum authority needed for one sealed submission review:
- `workflow_runs`
- any required workflow/job rows for `review_submission(...)`
- one stored submission carrying verification artifact refs
- Exercise `review_submission(...)` with:
- decision `approve`
- verification evidence present on the submission
- explicit `promotion_intent_at`
- explicit `finalized_at`
- explicit `canonical_commit_ref`
- Assert that the runtime path creates:
- one `gate_evaluations` row with decision `accept`
- one `promotion_decisions` row with decision `accept`
- Assert that those stored rows satisfy the live Phase 4 contract:
- the promotion row points at the gate row
- `current_head_ref` equals `validated_head_ref`
- finalization evidence is present
- `target_kind` is canonical and `target_ref` stays stable
- If the proof exposes drift, repair only the review-to-policy projection needed to make this one contract pass.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/runtime/workflow/submission_capture.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_capture.py)
- [Code&DBs/Workflow/runtime/workflow/submission_policy.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_policy.py)
- Primary regression scope:
- one new integration test under [Code&DBs/Workflow/tests/integration](/workspace/Code&DBs/Workflow/tests/integration)
- or an adjacent extension to an existing workflow submission integration test if that keeps the proof focused
- Read-only authority references:
- [Code&DBs/Workflow/policy/gate.py](/workspace/Code&DBs/Workflow/policy/gate.py)
- [Code&DBs/Workflow/policy/domain.py](/workspace/Code&DBs/Workflow/policy/domain.py)
- [Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql](/workspace/Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql)
- [Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py](/workspace/Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py)
- [Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py)
- [Code&DBs/Workflow/storage/postgres/workflow_submission_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/workflow_submission_repository.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/workflow/submission_gate.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_gate.py)
- [Code&DBs/Workflow/runtime/recurring_review_repair_flow.py](/workspace/Code&DBs/Workflow/runtime/recurring_review_repair_flow.py)
- [Code&DBs/Workflow/surfaces/api/workflow_submission.py](/workspace/Code&DBs/Workflow/surfaces/api/workflow_submission.py)
- [Code&DBs/Workflow/surfaces/mcp/tools/submission.py](/workspace/Code&DBs/Workflow/surfaces/mcp/tools/submission.py)
- any new migration or migration-authority regeneration
- any payload redesign outside the one review approval path

## 6. Done criteria

- A Postgres-backed integration test drives `review_submission(...)` from a real stored submission to stored Phase 4 policy rows.
- The proof demonstrates one accepted gate evaluation and one accepted promotion decision for the same proposal.
- The stored rows satisfy the active schema contract from migration `003_gate_and_promotion_policy.sql`, including gate-truth linkage and accepted-promotion evidence requirements.
- Existing Phase 4 policy integration proof still passes in [Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py](/workspace/Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py).
- Existing unit proof still passes in [Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py).
- No unrelated admission, retry, scheduler, UI, or migration work lands in the sprint.

## 7. Verification commands

- `cd /workspace`
- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='/workspace/Code&DBs/Workflow'`
- `python -m pytest '/workspace/Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py' -q`
- `python -m pytest '/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py' -q`
- `rg -n "def review_submission|evaluate_publish_policy|promotion_intent_at|finalized_at|canonical_commit_ref|verification_artifact_refs" '/workspace/Code&DBs/Workflow/runtime/workflow/submission_capture.py'`
- `rg -n "INSERT INTO gate_evaluations|INSERT INTO promotion_decisions|PolicyEngine|CANONICAL_TARGET_KIND" '/workspace/Code&DBs/Workflow/runtime/workflow/submission_policy.py'`
- `rg -n "gate_evaluations|promotion_decisions_gate_truth_fkey|promotion_decisions_accept_evidence_check" '/workspace/Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql'`

Expected verification outcome:

- the policy engine and schema authority still pass unchanged
- the runtime review path is visibly anchored to the canonical Phase 4 helper
- the sprint adds one DB-backed review-to-policy proof instead of relying only on raw policy tests or fake-connection unit tests

## 8. Review -> healer -> human approval gate

- Review:
- confirm the change set stays on the `review_submission(...)` to `evaluate_publish_policy(...)` seam
- confirm the new proof uses a real Postgres-backed submission path rather than direct inserts into `gate_evaluations` or `promotion_decisions`
- confirm accepted-promotion proof includes explicit `promotion_intent_at`, `finalized_at`, and `canonical_commit_ref`
- confirm no out-of-scope scheduler, admission, retry, API redesign, or migration edits leaked in
- Healer:
- if review finds drift, repair only the scoped runtime projection or the focused integration proof
- do not widen healer work into submission frontdoor redesign, recurring review/repair flow, or new schema authority
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 4 sprint
- if a later sprint is approved, take exactly one adjacent seam next, such as a reject-path runtime proof, rather than broad policy refactoring
