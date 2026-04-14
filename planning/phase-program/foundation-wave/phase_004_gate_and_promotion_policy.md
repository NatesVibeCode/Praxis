# Phase 4 Gate and Promotion Policy

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `4` (`Gate and Promotion Policy`), status `historical_foundation`, predecessor phase `3`, required closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is grounded in the checked-out repo state under `/workspace`. The supplied platform execution target is `/Users/nate/Praxis` with `postgresql://nate@127.0.0.1:5432/praxis`; use that root when executing later outside this session. The execution shard still shows proof coverage at `0.0` and write-manifest coverage at `0.1716`, so the first sprint must add one narrow runnable proof instead of widening policy scope.

## 1. Objective in repo terms

- Converge the live Phase 4 review seam so publish approval projects canonical gate and promotion truth through the actual submission review path, not only through isolated policy-engine tests.
- The repo-level target is the current call chain:
- [Code&DBs/Workflow/surfaces/api/workflow_submission.py](/workspace/Code&DBs/Workflow/surfaces/api/workflow_submission.py) `review_submission(...)`
- [Code&DBs/Workflow/runtime/workflow/submission_capture.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_capture.py) `review_submission(...)`
- [Code&DBs/Workflow/runtime/workflow/submission_policy.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_policy.py) `evaluate_publish_policy(...)`
- backed by the schema contract in [Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql](/workspace/Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql).
- Keep the sprint bounded to one approval path only: a publish-capable review that records one gate evaluation and one promotion decision when explicit promotion evidence is present.

## 2. Current evidence in the repo

- The authority map registers phase `4` as `Gate and Promotion Policy` and requires `review -> healer -> human_approval` closeout in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json).
- The Phase 4 schema authority is live in [Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql](/workspace/Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql):
- `gate_evaluations` is the pre-promotion authority table
- `promotion_decisions` is constrained to final `accept` or `reject`
- `promotion_decisions_gate_truth_fkey` binds promotion rows to exact gate truth
- `promotion_decisions_accept_evidence_check` requires `current_head_ref`, `promotion_intent_at`, `finalized_at`, `canonical_commit_ref`, and head equality for accepted promotions
- The canonical policy engine already exists in [Code&DBs/Workflow/policy/gate.py](/workspace/Code&DBs/Workflow/policy/gate.py):
- `evaluate_gate(...)` emits `accept`, `reject`, or `block`
- `decide_promotion(...)` refuses blocked gates and enforces explicit finalization evidence
- `CANONICAL_TARGET_KIND` is fixed as `canonical_repo`
- The live workflow projection path is already wired:
- [Code&DBs/Workflow/runtime/workflow/submission_capture.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_capture.py) records the review, derives verification refs and effective head state, and calls `evaluate_publish_policy(...)` for publish reviewers
- [Code&DBs/Workflow/runtime/workflow/submission_policy.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_policy.py) inserts `gate_evaluations` and conditionally inserts `promotion_decisions`
- [Code&DBs/Workflow/surfaces/api/workflow_submission.py](/workspace/Code&DBs/Workflow/surfaces/api/workflow_submission.py) is the actual frontdoor that forwards optional publish-policy fields into the service layer
- Existing proof is partial:
- [Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py](/workspace/Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py) proves the schema and policy engine directly against Postgres
- [Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py) proves the runtime review service issues gate and promotion inserts through a fake connection
- [Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py) proves the frontdoor forwards optional publish-policy fields
- There is no current integration test that drives the frontdoor or runtime review service against a real stored submission and then asserts the persisted Phase 4 rows.

## 3. Gap or ambiguity still remaining

- Phase 4 policy exists, but the repo does not yet prove the full DB-backed review path.
- The unresolved ambiguity is whether the live review flow stores canonical Phase 4 truth correctly when all layers participate:
- frontdoor payload normalization in `surfaces/api/workflow_submission.py`
- submission targeting and reviewer-role routing in `runtime/workflow/submission_capture.py`
- gate/promotion projection in `runtime/workflow/submission_policy.py`
- foreign-key and check constraints from migration `003_gate_and_promotion_policy.sql`
- The first sprint should not widen into:
- recurring review repair
- generic submission acceptance redesign
- MCP tool redesign
- new migration work
- non-publish review roles
- repo-wide promotion workflow cleanup

## 4. One bounded first sprint only

- Add one focused integration proof for the live publish review path against Postgres.
- Seed only the minimum required authority:
- a `workflow_runs` row and required workflow/job rows for the reviewer and target submission
- one stored submission row with `verification_artifact_refs`
- runtime context needed for the target job if the review path reads execution bundle state
- Exercise one review through the live callable seam:
- preferred: `runtime.workflow.submission_capture.review_submission(...)`
- optional if practical without widening: the public frontdoor `surfaces.api.workflow_submission.review_submission(...)`
- Use one `approve` decision with explicit:
- `policy_snapshot_ref`
- `target_ref`
- `current_head_ref`
- `promotion_intent_at`
- `finalized_at`
- `canonical_commit_ref`
- Assert that the real path stores:
- one `gate_evaluations` row with decision `accept`
- one `promotion_decisions` row with decision `accept`
- Assert that stored rows satisfy live Phase 4 policy truth:
- promotion row links back to the gate row through the full gate-truth foreign key
- `current_head_ref` equals `validated_head_ref`
- `target_kind` is `canonical_repo`
- `target_ref` is the requested canonical target
- accepted-promotion evidence fields are present and ordered correctly
- If the proof exposes drift, repair only the scoped review-to-policy projection or the test setup needed to make this one contract pass.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/runtime/workflow/submission_capture.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_capture.py)
- [Code&DBs/Workflow/runtime/workflow/submission_policy.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_policy.py)
- [Code&DBs/Workflow/surfaces/api/workflow_submission.py](/workspace/Code&DBs/Workflow/surfaces/api/workflow_submission.py) only if the chosen integration proof needs frontdoor normalization to be part of the contract
- Primary regression scope:
- one new integration test under [Code&DBs/Workflow/tests/integration](/workspace/Code&DBs/Workflow/tests/integration)
- or one focused extension to [Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py](/workspace/Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py) if that keeps the proof isolated
- Supporting read-only references:
- [Code&DBs/Workflow/policy/gate.py](/workspace/Code&DBs/Workflow/policy/gate.py)
- [Code&DBs/Workflow/policy/domain.py](/workspace/Code&DBs/Workflow/policy/domain.py)
- [Code&DBs/Workflow/storage/postgres/workflow_submission_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/workflow_submission_repository.py)
- [Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql](/workspace/Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql)
- [Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py)
- [Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/workflow/submission_gate.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_gate.py)
- [Code&DBs/Workflow/runtime/recurring_review_repair_flow.py](/workspace/Code&DBs/Workflow/runtime/recurring_review_repair_flow.py)
- [Code&DBs/Workflow/tests/integration/test_recurring_review_repair_flow.py](/workspace/Code&DBs/Workflow/tests/integration/test_recurring_review_repair_flow.py)
- any new migration, schema expansion, or migration-authority regeneration
- any redesign of submission tool admission, execution bundle structure, or publish scheduling

## 6. Done criteria

- A Postgres-backed integration test drives the live publish review path from a real stored submission to stored Phase 4 rows.
- The proof demonstrates one accepted gate evaluation and one accepted promotion decision for the same proposal derived from that submission.
- The stored rows satisfy the active schema contract from migration `003_gate_and_promotion_policy.sql`, including gate-truth linkage and accepted-promotion evidence requirements.
- Existing Phase 4 integration proof still passes in [Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py](/workspace/Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py).
- Existing unit proofs still pass in [Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py) and [Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py).
- No unrelated recurring-review, admission, MCP, or migration work lands in the sprint.

## 7. Verification commands

```bash
cd /Users/nate/Praxis
export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'
export PYTHONPATH='Code&DBs/Workflow'
python -m pytest 'Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py' -q
python -m pytest 'Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py' -q
python -m pytest 'Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py' -q
rg -n "def review_submission\\(|evaluate_publish_policy\\(|promotion_intent_at|finalized_at|canonical_commit_ref" \
  'Code&DBs/Workflow/surfaces/api/workflow_submission.py' \
  'Code&DBs/Workflow/runtime/workflow/submission_capture.py' \
  'Code&DBs/Workflow/runtime/workflow/submission_policy.py'
rg -n "gate_evaluations|promotion_decisions_gate_truth_fkey|promotion_decisions_accept_evidence_check" \
  'Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql'
```

Expected verification outcome:

- the live review path is visibly anchored to the canonical Phase 4 helper
- the repo retains direct schema-and-policy coverage
- the sprint adds one DB-backed review-path proof instead of relying only on raw policy tests and fake-connection unit tests

## 8. Review -> healer -> human approval gate

- Review:
- confirm the change set stayed on the live publish review seam and did not widen into general submission or repair architecture
- confirm the new proof executes through the actual review path rather than inserting directly into `gate_evaluations` or `promotion_decisions`
- confirm accepted-promotion proof includes explicit `promotion_intent_at`, `finalized_at`, and `canonical_commit_ref`
- confirm no new migration, recurring-repair work, or tool-surface redesign leaked into scope
- Healer:
- if review finds drift, repair only the scoped review-to-policy projection or the focused integration proof
- do not widen healer work into submission gating, repair fanout, scheduler behavior, or schema redesign
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 4 sprint
- any later Phase 4 sprint must take exactly one adjacent seam next, such as a reject-path or non-promotion-path runtime proof, rather than broad policy refactoring
