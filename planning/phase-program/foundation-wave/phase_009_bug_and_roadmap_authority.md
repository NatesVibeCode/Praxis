# Phase 9 Bug and Roadmap Authority

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `9` (`Bug and Roadmap Authority`), status `historical_foundation`, predecessor phase `8`, with mandatory closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is based on the current checked-out repo snapshot under `/workspace`. The database target supplied for verification is `postgresql://nate@127.0.0.1:5432/praxis`. The current execution shard also shows Phase proof coverage is still immature (`fully_proved_verification_coverage=0.0`, `verification_coverage=0.0`, `write_manifest_coverage=0.085`), so this sprint must stay narrow and produce one explicit authority convergence with runnable proof.

## 1. Objective in repo terms

- Converge one real Phase 9 write seam in the current repo by making proof-backed bug and roadmap closeout commit through the dedicated Postgres closeout repository instead of keeping duplicate mutation SQL inside the operator write frontdoor.
- Keep the sprint bounded to the existing public closeout path exposed by `/api/operator/work-item-closeout` and `native-operator work-item-closeout`.
- Repo-level target for this sprint: one owner for commit-time status transitions on `bugs` and `roadmap_items` when the closeout gate moves from preview to commit.

## 2. Current evidence in the repo

- The authority map defines phase `9` as `Bug and Roadmap Authority` with predecessor `8` and required closeout sequence `review -> healer -> human_approval` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json).
- The schema origin already exists in [Code&DBs/Databases/migrations/workflow/009_bug_and_roadmap_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/009_bug_and_roadmap_authority.sql), which creates `bugs`, `bug_evidence_links`, `roadmap_items`, and `roadmap_item_dependencies`.
- Later migrations prove the live Phase 9 surface is broader than the original schema cut:
- [Code&DBs/Databases/migrations/workflow/014_fts_and_bug_columns.sql](/workspace/Code&DBs/Databases/migrations/workflow/014_fts_and_bug_columns.sql) extends bug columns and search support.
- [Code&DBs/Databases/migrations/workflow/042_roadmap_item_registry_paths.sql](/workspace/Code&DBs/Databases/migrations/workflow/042_roadmap_item_registry_paths.sql) extends `roadmap_items` with registry-path support.
- Explicit write-side repositories already exist in the Postgres storage layer and are exported from [Code&DBs/Workflow/storage/postgres/__init__.py](/workspace/Code&DBs/Workflow/storage/postgres/__init__.py):
- [Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py) owns roadmap package writes.
- [Code&DBs/Workflow/storage/postgres/bug_evidence_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/bug_evidence_repository.py) owns explicit bug-evidence upserts.
- [Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py) already owns `mark_bugs_fixed(...)` and `mark_roadmap_items_completed(...)`.
- The public closeout seam is already live:
- [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py) implements `OperatorControlFrontdoor._reconcile_work_item_closeout(...)`.
- [Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py) exposes `/api/operator/work-item-closeout`.
- [Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py](/workspace/Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py) proves the CLI routes through the shared gate.
- The proof contract for the closeout gate already exists in [Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py):
- preview requires `validates_fix` evidence
- preview returns `committed: false` and empty `applied`
- commit marks the bug `FIXED`
- commit closes the linked roadmap item
- The operator frontdoor already uses the repository-boundary pattern elsewhere. [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py) routes roadmap package commits through `PostgresRoadmapAuthoringRepository`, which is the exact Phase 9 pattern this sprint should mirror for closeout.
- The current duplication is concrete, not hypothetical. `OperatorControlFrontdoor._reconcile_work_item_closeout(...)` in [operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py) still executes inline `UPDATE bugs` and `UPDATE roadmap_items` SQL for `action="commit"` even though [work_item_closeout_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py) already defines the same mutations.
- Additional direct Phase 9 writers still exist outside the closeout seam:
- [Code&DBs/Workflow/runtime/bug_tracker.py](/workspace/Code&DBs/Workflow/runtime/bug_tracker.py) writes `bugs` and `bug_evidence_links`.
- [Code&DBs/Workflow/runtime/verifier_bug_bridge.py](/workspace/Code&DBs/Workflow/runtime/verifier_bug_bridge.py) directly inserts into `bug_evidence_links`.
- [Code&DBs/Workflow/runtime/post_workflow_sync.py](/workspace/Code&DBs/Workflow/runtime/post_workflow_sync.py) directly updates `public.roadmap_items`.

## 3. Gap or ambiguity still remaining

- Phase 9 already has canonical schema and several dedicated repositories, but the main public closeout seam still has two mutation owners for the same behavior:
- `surfaces/api/operator_write.py` owns preview logic and also owns commit-time `UPDATE` SQL
- `storage/postgres/work_item_closeout_repository.py` owns the same closeout mutations as a repository contract
- That split makes commit semantics easy to drift on timestamps, returned fields, and future rule changes while still leaving tests green if only one path is exercised.
- The bigger Phase 9 authority problem is still open because bug filing, evidence linking, and workflow-run roadmap completion each keep their own direct SQL mutation paths.
- The first sprint must remove exactly one duplicate owner at one exposed frontdoor. It must not claim Phase 9 is fully solved.

## 4. One bounded first sprint only

- Replace the inline commit-time closeout SQL inside `OperatorControlFrontdoor._reconcile_work_item_closeout(...)` with calls to `PostgresWorkItemCloseoutRepository`.
- Keep preview logic in the frontdoor. Candidate selection, proof-threshold computation, and skipped-reason assembly remain surface concerns for this sprint.
- Preserve the current closeout contract exactly:
- `preview` stays non-mutating and returns `committed: false`
- bug closeout still requires `validates_fix` evidence from `bug_evidence_links`
- roadmap closeout still requires a linked `source_bug_id` with explicit fix proof
- candidate ids, skip reason codes, and proof-threshold payload stay stable
- commit payload shape for `applied.bugs` and `applied.roadmap_items` stays stable for callers
- Stop once the frontdoor delegates commit mutations and the focused tests prove it. Do not widen into bug filing, evidence-link creation, roadmap authoring, roadmap export, workflow-run sync cleanup, or schema changes.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py)
- [Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py)
- Primary regression scope:
- [Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py)
- [Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py](/workspace/Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py)
- Read-only authority references:
- [Code&DBs/Databases/migrations/workflow/009_bug_and_roadmap_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/009_bug_and_roadmap_authority.sql)
- [Code&DBs/Databases/migrations/workflow/014_fts_and_bug_columns.sql](/workspace/Code&DBs/Databases/migrations/workflow/014_fts_and_bug_columns.sql)
- [Code&DBs/Databases/migrations/workflow/042_roadmap_item_registry_paths.sql](/workspace/Code&DBs/Databases/migrations/workflow/042_roadmap_item_registry_paths.sql)
- [Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py)
- [Code&DBs/Workflow/storage/postgres/bug_evidence_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/bug_evidence_repository.py)
- [Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/bug_tracker.py](/workspace/Code&DBs/Workflow/runtime/bug_tracker.py)
- [Code&DBs/Workflow/runtime/verifier_bug_bridge.py](/workspace/Code&DBs/Workflow/runtime/verifier_bug_bridge.py)
- [Code&DBs/Workflow/runtime/post_workflow_sync.py](/workspace/Code&DBs/Workflow/runtime/post_workflow_sync.py)
- behavior changes in [Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py)
- new migrations
- repo-wide direct-SQL cleanup
- any public payload redesign for `/api/operator/work-item-closeout`

## 6. Done criteria

- `OperatorControlFrontdoor._reconcile_work_item_closeout(...)` no longer contains inline `UPDATE bugs` or `UPDATE roadmap_items` commit logic.
- The commit path delegates bug and roadmap closeout writes to `PostgresWorkItemCloseoutRepository`.
- Preview behavior remains caller-compatible:
- same evaluated ids
- same candidate ids
- same skip reason codes
- same proof-threshold structure
- same `committed: false` and empty `applied` behavior in preview mode
- Commit behavior remains caller-compatible for the current `applied.bugs` and `applied.roadmap_items` payload fields.
- [test_work_item_closeout_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py) passes without weakening the proof-backed closeout assertions.
- [test_native_operator_work_item_closeout_cli.py](/workspace/Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py) still passes, proving the CLI seam remains stable.
- No out-of-scope Phase 9 writer is refactored in this sprint.

## 7. Verification commands

- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='/workspace/Code&DBs/Workflow'`
- `python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py -q`
- `python -m pytest /workspace/Code\&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py -q`
- `python -m pytest /workspace/Code\&DBs/Workflow/tests/integration/test_roadmap_write_gate.py -q`
- `rg -n "UPDATE bugs AS bug|UPDATE roadmap_items|PostgresWorkItemCloseoutRepository" /workspace/Code\\&DBs/Workflow/surfaces/api/operator_write.py /workspace/Code\\&DBs/Workflow/storage/postgres/work_item_closeout_repository.py`
- `rg -n "reconcile_work_item_closeout|/api/operator/work-item-closeout|native-operator.*work-item-closeout" /workspace/Code\\&DBs/Workflow/surfaces/api/operator_write.py /workspace/Code\\&DBs/Workflow/surfaces/api/handlers/workflow_admin.py /workspace/Code\\&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py`

Expected verification outcome:

- `operator_write.py` still owns preview and candidate assembly
- `work_item_closeout_repository.py` becomes the only commit-time mutation owner for this closeout seam
- the closeout gate still proves explicit-fix evidence before mutating bug or roadmap status
- the roadmap package write path remains unchanged and continues to demonstrate the same repository-boundary pattern

## 8. Review -> healer -> human approval gate

- Review:
- confirm the operator closeout frontdoor delegates commit mutations to the repository and only retains preview, candidate selection, and payload assembly
- confirm preview payload shape, skip-reason semantics, and commit payload shape did not drift
- confirm no new direct write path to `bugs`, `bug_evidence_links`, or `roadmap_items` was introduced in surface code
- confirm the sprint did not widen into `bug_tracker.py`, `verifier_bug_bridge.py`, `post_workflow_sync.py`, or roadmap authoring behavior
- Healer:
- if review finds payload drift, timestamp drift, or status regression, repair only the operator closeout seam inside the scoped files above
- do not widen healer work into bug filing, evidence-link authoring, roadmap package authoring, or workflow-run sync logic
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 9 sprint
- the next Phase 9 sprint, if approved later, should target exactly one additional writer seam, likely `post_workflow_sync.py` or one bug-evidence writer, not “finish bug and roadmap authority” in one pass
