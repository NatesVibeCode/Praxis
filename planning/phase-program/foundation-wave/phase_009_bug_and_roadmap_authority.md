# Phase 9 Bug and Roadmap Authority

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `9` (`Bug and Roadmap Authority`), predecessor phase `8`, with mandatory closeout sequence `review -> healer -> human_approval`.

## 1. Objective in repo terms

- Re-establish one explicit Phase 9 authority seam in the current repo by making proof-backed bug and roadmap closeout flow through the existing Postgres closeout repository instead of duplicating commit SQL inside the operator API surface.
- Keep the sprint bounded to the closeout path exposed by [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py), not the full bug tracker or all roadmap mutation paths.
- Repo-level target for this sprint: one write owner for proof-backed status transitions from `bugs` to `roadmap_items` when `/api/operator/work-item-closeout` commits.

## 2. Current evidence in the repo

- The canonical Phase 9 schema origin is [Code&DBs/Databases/migrations/workflow/009_bug_and_roadmap_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/009_bug_and_roadmap_authority.sql), which creates `bugs`, `bug_evidence_links`, `roadmap_items`, and `roadmap_item_dependencies`.
- The live bug and roadmap row shape is broader than migration `009` alone. Later migrations such as [014_fts_and_bug_columns.sql](/workspace/Code&DBs/Databases/migrations/workflow/014_fts_and_bug_columns.sql) extend `bugs` and `roadmap_items`, so any Phase 9 packet must treat `009` as schema origin, not complete runtime shape.
- A canonical closeout write surface already exists in [Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py) with `PostgresWorkItemCloseoutRepository.mark_bugs_fixed(...)` and `mark_roadmap_items_completed(...)`.
- The operator frontdoor still duplicates those writes inline in [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py): `reconcile_work_item_closeout_async(...)` contains direct `UPDATE bugs` and `UPDATE roadmap_items` commit SQL.
- The closeout path is a real exposed operator surface, not dead code. [Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py) advertises `/api/operator/work-item-closeout` as the proof-backed bug and roadmap closeout route.
- Integration coverage already anchors the intended contract in [Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py), which proves preview and commit behavior from explicit fix evidence.
- Direct Phase 9 writes still exist elsewhere, which means the broader authority problem is real but not appropriate for one sprint:
- [Code&DBs/Workflow/runtime/post_workflow_sync.py](/workspace/Code&DBs/Workflow/runtime/post_workflow_sync.py) directly updates `public.roadmap_items`.
- [Code&DBs/Workflow/runtime/bug_tracker.py](/workspace/Code&DBs/Workflow/runtime/bug_tracker.py) directly updates `bugs`.
- [Skills/praxis-bug-logging/SKILL.md](/workspace/Skills/praxis-bug-logging/SKILL.md) and [Skills/praxis-bug-logging/references/bug-authority.md](/workspace/Skills/praxis-bug-logging/references/bug-authority.md) already document the intended authority model: bug writes should go through an explicit bug-tracker surface rather than ad hoc SQL.

## 3. Gap or ambiguity still remaining

- The repo already has an explicit closeout repository, but the main operator closeout frontdoor bypasses it during commit.
- That leaves two owners for the same mutation semantics: one in `operator_write.py` and one in `work_item_closeout_repository.py`.
- Because both layers define the same `FIXED` bug closeout and roadmap completion updates, they can drift in timestamp behavior, result shape, or resolution-summary rules without an obvious contract break.
- The wider Phase 9 space is still unresolved after this sprint because other direct writers remain in `post_workflow_sync.py` and `bug_tracker.py`.
- The first sprint should remove one duplication seam, not attempt to solve all bug and roadmap authority in one pass.

## 4. One bounded first sprint only

- Replace the inline commit-time closeout SQL inside `OperatorControlFrontdoor.reconcile_work_item_closeout_async(...)` with calls to `PostgresWorkItemCloseoutRepository`.
- Preserve the current preview contract exactly:
- bug candidate selection still depends on `validates_fix` evidence
- roadmap candidate selection still depends on source-bug proof
- preview payload fields, skipped reasons, and non-commit output stay unchanged
- Preserve the current commit payload shape returned from the operator surface even if the mutation implementation moves behind the repository boundary.
- Stop after the operator closeout seam is converged. Do not widen into bug filing, evidence linking, roadmap authoring, or workflow-run sync cleanup.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py)
- [Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py)
- Primary contract test scope:
- [Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py)
- Read-only context:
- [Code&DBs/Databases/migrations/workflow/009_bug_and_roadmap_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/009_bug_and_roadmap_authority.sql)
- [Code&DBs/Databases/migrations/workflow/014_fts_and_bug_columns.sql](/workspace/Code&DBs/Databases/migrations/workflow/014_fts_and_bug_columns.sql)
- [Code&DBs/Workflow/runtime/post_workflow_sync.py](/workspace/Code&DBs/Workflow/runtime/post_workflow_sync.py)
- [Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/bug_tracker.py](/workspace/Code&DBs/Workflow/runtime/bug_tracker.py)
- [Code&DBs/Workflow/runtime/verifier_bug_bridge.py](/workspace/Code&DBs/Workflow/runtime/verifier_bug_bridge.py)
- [Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py)
- new migrations
- repo-wide direct-SQL cleanup
- changing the public payload contract of `/api/operator/work-item-closeout`

## 6. Done criteria

- `reconcile_work_item_closeout_async(...)` no longer contains inline `UPDATE bugs` or `UPDATE roadmap_items` statements for commit behavior.
- The commit path in `operator_write.py` delegates proof-backed bug and roadmap closeout writes to `PostgresWorkItemCloseoutRepository`.
- Preview behavior remains contract-compatible: same candidate ids, same skipped reason codes, same proof-threshold payload, same `committed: false` shape.
- Commit behavior remains contract-compatible for caller-visible fields in `applied.bugs` and `applied.roadmap_items`.
- [test_work_item_closeout_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py) passes without weakening its assertions.
- No other Phase 9 writer is refactored as part of this sprint.

## 7. Verification commands

- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py -q`
- `python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_roadmap_write_gate.py /workspace/Code\&DBs/Workflow/tests/integration/test_roadmap_tree_view.py -q`
- `rg -n "UPDATE bugs AS bug|UPDATE roadmap_items" /workspace/Code\\&DBs/Workflow/surfaces/api/operator_write.py /workspace/Code\\&DBs/Workflow/storage/postgres/work_item_closeout_repository.py`

Expected verification outcome:
- `operator_write.py` still owns preview reconciliation logic but no longer owns the commit-time SQL for bug and roadmap closeout
- `work_item_closeout_repository.py` remains the single explicit closeout mutation owner for this seam

## 8. Review -> healer -> human approval gate

- Review:
- confirm the operator closeout frontdoor delegates commit mutations to the repository
- confirm preview payload shape and skip-reason semantics did not drift
- confirm no new direct write path to `bugs` or `roadmap_items` was introduced in surface code
- Healer:
- if review finds payload drift, timestamp drift, or status regression, repair only the operator closeout seam inside the scoped files above
- do not widen healer work into `bug_tracker.py`, `post_workflow_sync.py`, or unrelated roadmap authoring surfaces
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 9 sprint
- the next Phase 9 sprint, if approved later, should target exactly one additional write seam rather than “finish bug and roadmap authority”
