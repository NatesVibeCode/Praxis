# Phase 4 Gate and Promotion Policy

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `4` (`Gate and Promotion Policy`), predecessor phase `3`, closeout sequence `review -> healer -> human_approval`.

Grounding note:
- Repo evidence in this packet was read from the mounted checkout at `/workspace`.
- The supplied execution root for later work is `/Users/nate/Praxis` with database `postgresql://nate@127.0.0.1:5432/praxis`.
- The execution context shard says `execution_packets_ready=true`, `repo_snapshots_ready=true`, `verification_registry_ready=true`, and `verify_refs_ready=true`, while `fully_proved_verification_coverage=0.0` and `verification_coverage=0.0`. Phase 4 therefore needs one real proof on the live review seam, not more policy description.

## 1. Objective in repo terms

- Prove that the current workflow submission review path can persist canonical Phase 4 truth into `gate_evaluations` and `promotion_decisions` through the live repo code, not only through isolated policy-engine or raw-SQL tests.
- Keep the sprint pinned to the existing publish-review path:
- [Code&DBs/Workflow/surfaces/api/workflow_submission.py](/workspace/Code&DBs/Workflow/surfaces/api/workflow_submission.py)
- [Code&DBs/Workflow/runtime/workflow/submission_capture.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_capture.py)
- [Code&DBs/Workflow/runtime/workflow/submission_policy.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_policy.py)
- with schema authority from [Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql](/workspace/Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql).
- First-sprint target: one accepted publish review, starting from a stored submission and ending in durable gate and promotion rows that satisfy the active database constraints.

## 2. Current evidence in the repo

- Phase `4` is declared in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) as `Gate and Promotion Policy` with predecessor `3` and mandatory closeout `review -> healer -> human_approval`.
- The canonical schema already exists in [Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql](/workspace/Code&DBs/Databases/migrations/workflow/003_gate_and_promotion_policy.sql):
- `gate_evaluations` is the canonical gate table.
- `promotion_decisions` is restricted to final promotion truth.
- `promotion_decisions_gate_truth_fkey` binds promotion rows to the full gate truth tuple.
- `promotion_decisions_accept_evidence_check` requires accepted promotions to carry `promotion_intent_at`, `finalized_at`, `canonical_commit_ref`, and matching head evidence.
- The policy engine already exists in [Code&DBs/Workflow/policy/gate.py](/workspace/Code&DBs/Workflow/policy/gate.py):
- `evaluate_gate(...)` emits `accept`, `reject`, or `block`.
- `decide_promotion(...)` refuses blocked gates and enforces finalization evidence.
- `CANONICAL_TARGET_KIND` is fixed for this seam.
- The runtime projection path is present today:
- [Code&DBs/Workflow/runtime/workflow/submission_capture.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_capture.py) `review_submission(...)` detects publish reviewers and calls `evaluate_publish_policy(...)`.
- [Code&DBs/Workflow/runtime/workflow/submission_policy.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_policy.py) inserts `gate_evaluations` and conditionally inserts `promotion_decisions`.
- [Code&DBs/Workflow/surfaces/api/workflow_submission.py](/workspace/Code&DBs/Workflow/surfaces/api/workflow_submission.py) forwards optional publish-policy fields including `policy_snapshot_ref`, `target_ref`, `current_head_ref`, `promotion_intent_at`, `finalized_at`, and `canonical_commit_ref`.
- Existing proof is still split by layer:
- [Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py](/workspace/Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py) proves schema and policy invariants directly against Postgres.
- [Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py) proves the review service emits gate and promotion inserts against a fake connection.
- [Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py) proves frontdoor forwarding of publish-policy fields.
- There is no current Postgres-backed integration proof that drives the actual review path from a stored submission and then asserts the persisted Phase 4 rows.

## 3. Gap or ambiguity still remaining

- The repo already has schema authority, policy-engine logic, review-path code, and unit/frontdoor coverage.
- The missing proof is whether those pieces converge correctly when one real review flows through the live runtime seam.
- The unresolved Phase 4 ambiguity is the end-to-end contract across:
- frontdoor request shaping in `surfaces/api/workflow_submission.py`
- reviewer-role routing and submission lookup in `runtime/workflow/submission_capture.py`
- gate/promotion projection in `runtime/workflow/submission_policy.py`
- constraint enforcement in migration `003_gate_and_promotion_policy.sql`
- This packet resolves only that live-path proof gap.
- Do not widen into:
- submission redesign
- recurring review repair
- new migrations
- general publish workflow architecture
- MCP tool redesign
- repo-wide fixture cleanup

## 4. One bounded first sprint only

- Add one focused Postgres-backed integration proof for an accepted publish review.
- Seed only the minimum runtime authority needed to review a real submission:
- one `workflow_runs` row
- the required `workflow_jobs` rows for the target submission job and the publish reviewer job
- one stored submission row with non-empty `verification_artifact_refs`
- only the runtime-context rows needed if the review path requires workspace/head derivation
- Exercise one live review through the narrowest real seam that still proves Phase 4:
- preferred seam: `runtime.workflow.submission_capture.review_submission(...)`
- optional seam: `surfaces.api.workflow_submission.review_submission(...)` if the test can keep context setup small and still stay on one seam
- Use one `approve` review with explicit:
- `policy_snapshot_ref`
- `target_ref`
- `current_head_ref`
- `promotion_intent_at`
- `finalized_at`
- `canonical_commit_ref`
- Assert that the live path stores:
- one `gate_evaluations` row with decision `accept`
- one `promotion_decisions` row with decision `accept`
- Assert that stored rows satisfy current schema truth:
- the promotion row links to the gate row through the full gate-truth foreign key
- `current_head_ref` equals `validated_head_ref`
- `target_kind` is the canonical target kind from the active policy code
- accepted-promotion evidence fields are present and temporally valid
- If the proof exposes drift, fix only the review-to-policy projection or the minimum test setup needed to make this one contract pass.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/runtime/workflow/submission_capture.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_capture.py)
- [Code&DBs/Workflow/runtime/workflow/submission_policy.py](/workspace/Code&DBs/Workflow/runtime/workflow/submission_policy.py)
- [Code&DBs/Workflow/surfaces/api/workflow_submission.py](/workspace/Code&DBs/Workflow/surfaces/api/workflow_submission.py) only if the chosen integration proof includes the frontdoor
- Primary regression scope:
- one new focused integration test under [Code&DBs/Workflow/tests/integration](/workspace/Code&DBs/Workflow/tests/integration)
- or one narrow extension to [Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py](/workspace/Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py) if it stays isolated from raw-SQL-only coverage
- Read-only grounding references:
- [Code&DBs/Workflow/policy/gate.py](/workspace/Code&DBs/Workflow/policy/gate.py)
- [Code&DBs/Workflow/policy/domain.py](/workspace/Code&DBs/Workflow/policy/domain.py)
- [Code&DBs/Workflow/storage/postgres/workflow_submission_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/workflow_submission_repository.py)
- [Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py)
- [Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/recurring_review_repair_flow.py](/workspace/Code&DBs/Workflow/runtime/recurring_review_repair_flow.py)
- [Code&DBs/Workflow/tests/integration/test_recurring_review_repair_flow.py](/workspace/Code&DBs/Workflow/tests/integration/test_recurring_review_repair_flow.py)
- any migration, schema expansion, or migration-authority regeneration
- any redesign of submission admission, review orchestration, or publish scheduling

## 6. Done criteria

- One Postgres-backed integration test drives the live publish review path from a real stored submission to durable Phase 4 rows.
- The proof demonstrates one accepted gate evaluation and one accepted promotion decision for the same proposal derived from that review.
- The stored rows satisfy the active Phase 4 schema contract in migration `003_gate_and_promotion_policy.sql`, including gate-truth linkage and accepted-promotion evidence requirements.
- Existing coverage remains green in:
- [Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py](/workspace/Code&DBs/Workflow/tests/integration/test_gate_and_promotion_policy.py)
- [Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py)
- [Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_submission_frontdoor.py)
- No recurring-review, migration, or broad submission-architecture work lands in the sprint.

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

- the repo still has direct Phase 4 schema and policy coverage
- the live review path is visibly anchored to the publish-policy projection helper
- the sprint adds one DB-backed review-path proof instead of relying only on raw-SQL integration tests and fake-connection unit tests

## 8. Review -> healer -> human approval gate

- Review:
- confirm the sprint stayed on the live publish review seam only
- confirm the new proof executes through the actual review path and does not insert directly into `gate_evaluations` or `promotion_decisions`
- confirm accepted-promotion proof includes explicit `promotion_intent_at`, `finalized_at`, and `canonical_commit_ref`
- confirm no migration, recurring-review, or broad submission redesign leaked into scope
- Healer:
- if review finds drift, repair only the scoped review-to-policy projection or the one focused integration proof
- do not widen healer work into admission policy, recurring repair, scheduler behavior, or schema redesign
- Human approval gate:
- require explicit human approval after review and any healer pass before opening another Phase 4 sprint
- any later Phase 4 sprint must take exactly one adjacent seam next, such as a reject-path or non-promotion-path live proof, not broad refactoring
