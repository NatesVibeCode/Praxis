# Phase 9 Bug and Roadmap Authority

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `9` (`Bug and Roadmap Authority`), arc `0-9 define the machine`, status `historical_foundation`, predecessor phase `8`, required closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is grounded in the current repo snapshot mounted at `/workspace`. The supplied platform root for later execution is `/Users/nate/Praxis`, and the supplied database target is `postgresql://nate@127.0.0.1:5432/praxis`. The execution shard says compile-authority inputs are ready and receipts are not yet proved: `execution_packets_ready=true`, `repo_snapshots_ready=true`, `verification_registry_ready=true`, `verify_refs_ready=true`, `fully_proved_verification_coverage=0.0`, `verification_coverage=0.0`, `write_manifest_coverage=0.25`, total receipts `188`.

## 1. Objective in repo terms

- Make one Phase 9 mutation seam have exactly one write owner in the current repo: proof-backed bug and roadmap closeout through the shared operator frontdoor.
- Move commit-time bug and roadmap status mutation ownership to [Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py).
- Keep preview, candidate selection, and payload assembly in [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py).
- Preserve the current external contract for both `/api/operator/work-item-closeout` and `native-operator work-item-closeout`.
- Do not attempt repo-wide Phase 9 convergence in this sprint.

## 2. Current evidence in the repo

- The authority map declares phase `9` as `Bug and Roadmap Authority`, predecessor `8`, with mandatory closeout sequence `review -> healer -> human_approval` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json).
- Canonical Phase 9 schema authority already exists in [Code&DBs/Databases/migrations/workflow/009_bug_and_roadmap_authority.sql](/workspace/Code&DBs/Databases/migrations/workflow/009_bug_and_roadmap_authority.sql). It creates `bugs`, `bug_evidence_links`, `roadmap_items`, and `roadmap_item_dependencies`.
- Later migrations show that the live Phase 9 surface expanded after migration `009`:
- [Code&DBs/Databases/migrations/workflow/014_fts_and_bug_columns.sql](/workspace/Code&DBs/Databases/migrations/workflow/014_fts_and_bug_columns.sql) adds bug metadata and FTS columns for `bugs` and `roadmap_items`.
- [Code&DBs/Databases/migrations/workflow/042_roadmap_item_registry_paths.sql](/workspace/Code&DBs/Databases/migrations/workflow/042_roadmap_item_registry_paths.sql) adds `roadmap_items.registry_paths`.
- Dedicated Postgres authority surfaces already exist and are exported from [Code&DBs/Workflow/storage/postgres/__init__.py](/workspace/Code&DBs/Workflow/storage/postgres/__init__.py):
- [Code&DBs/Workflow/storage/postgres/bug_evidence_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/bug_evidence_repository.py)
- [Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py)
- [Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py)
- The closeout repository already owns explicit commit mutations:
- `PostgresWorkItemCloseoutRepository.mark_bugs_fixed(...)`
- `PostgresWorkItemCloseoutRepository.mark_roadmap_items_completed(...)`
- The public closeout seam already exists:
- [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py) implements `OperatorControlFrontdoor._reconcile_work_item_closeout(...)`.
- [Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py) exposes `/api/operator/work-item-closeout`.
- [Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py](/workspace/Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py) covers the CLI path.
- The focused proof contract already exists in [Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py):
- preview is non-mutating
- bug closeout requires `bug_evidence_links.evidence_role = 'validates_fix'`
- commit marks a bug `FIXED`
- commit closes the linked roadmap item
- The duplicate write seam is still present in the repo snapshot:
- [Code&DBs/Workflow/surfaces/api/operator_write.py](/workspace/Code&DBs/Workflow/surfaces/api/operator_write.py) still contains inline `UPDATE bugs AS bug` and `UPDATE roadmap_items` SQL inside `_reconcile_work_item_closeout(...)`.
- [Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py) already contains the same mutation intent as repository methods.
- Additional active Phase 9 direct writers still exist elsewhere in the repo, but they are neighboring evidence rather than first-sprint scope:
- [Code&DBs/Workflow/runtime/bug_tracker.py](/workspace/Code&DBs/Workflow/runtime/bug_tracker.py) inserts into `bugs`, updates `bugs`, and inserts into `bug_evidence_links`.
- [Code&DBs/Workflow/runtime/verifier_bug_bridge.py](/workspace/Code&DBs/Workflow/runtime/verifier_bug_bridge.py) inserts into `bug_evidence_links`.
- [Code&DBs/Workflow/runtime/post_workflow_sync.py](/workspace/Code&DBs/Workflow/runtime/post_workflow_sync.py) directly updates `public.roadmap_items`.
- The repo already shows the target boundary pattern elsewhere: roadmap package writes route through [Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py) instead of keeping write SQL in the surface.

## 3. Gap or ambiguity still remaining

- Phase 9 already has schema authority and repository authority, but the main public closeout seam still has two write owners for the same commit behavior.
- Today the split is concrete:
- `surfaces/api/operator_write.py` owns preview logic, candidate assembly, skip-reason generation, payload assembly, and also commit-time write SQL.
- `storage/postgres/work_item_closeout_repository.py` owns repository methods for those same closeout writes.
- That duplication creates drift risk on timestamps, returned fields, and resolution-summary semantics while leaving adjacent tests able to pass.
- Broader Phase 9 authority convergence is still unfinished because bug filing, bug evidence authoring, and workflow-run roadmap closeout each keep separate direct-SQL writers.
- This packet must not claim to solve all of that. It should remove exactly one duplicated write owner at one exposed frontdoor.

## 4. One bounded first sprint only

- Replace the inline commit-time closeout SQL inside `OperatorControlFrontdoor._reconcile_work_item_closeout(...)` with calls to `PostgresWorkItemCloseoutRepository`.
- Keep preview behavior in the frontdoor for this sprint:
- bug and roadmap candidate discovery
- proof-threshold calculation
- skip-reason generation
- response payload assembly
- Preserve the current public closeout contract:
- `action="preview"` remains non-mutating and returns `committed: false`
- bug closeout still requires `bug_evidence_links.evidence_role = 'validates_fix'`
- roadmap closeout still requires a linked `source_bug_id` with explicit fix proof
- evaluated ids, candidate ids, skip reason codes, and proof-threshold payload stay stable
- `applied.bugs` and `applied.roadmap_items` shapes stay stable on commit
- Stop after the frontdoor delegates commit mutations and the focused proofs pass.
- Do not widen into bug filing, bug evidence authoring, roadmap authoring, workflow-run sync cleanup, schema work, or payload redesign.

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
- changes to [Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/roadmap_authoring_repository.py) behavior
- new migrations
- repo-wide Phase 9 direct-SQL cleanup
- any redesign of `/api/operator/work-item-closeout` or CLI payloads

## 6. Done criteria

- `OperatorControlFrontdoor._reconcile_work_item_closeout(...)` no longer contains inline commit-time `UPDATE bugs AS bug` or `UPDATE roadmap_items` SQL.
- The commit path delegates bug and roadmap closeout writes to `PostgresWorkItemCloseoutRepository`.
- Preview behavior remains caller-compatible:
- same evaluated ids
- same candidate ids
- same skip reason codes
- same proof-threshold structure
- same `committed: false`
- same empty `applied` payload in preview mode
- Commit behavior remains caller-compatible for current `applied.bugs` and `applied.roadmap_items` fields.
- [Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py) passes without weakening the explicit-fix-proof assertions.
- [Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py](/workspace/Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py) still passes.
- [Code&DBs/Workflow/tests/integration/test_roadmap_write_gate.py](/workspace/Code&DBs/Workflow/tests/integration/test_roadmap_write_gate.py) still passes unchanged as an adjacent regression check.
- No out-of-scope Phase 9 writer seam is refactored in this sprint.

## 7. Verification commands

- `cd /Users/nate/Praxis`
- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='Code&DBs/Workflow'`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_roadmap_write_gate.py' -q`
- `rg -n "UPDATE bugs AS bug|UPDATE roadmap_items|PostgresWorkItemCloseoutRepository|mark_bugs_fixed|mark_roadmap_items_completed" 'Code&DBs/Workflow/surfaces/api/operator_write.py' 'Code&DBs/Workflow/storage/postgres/work_item_closeout_repository.py'`
- `rg -n "reconcile_work_item_closeout|/api/operator/work-item-closeout|work-item-closeout" 'Code&DBs/Workflow/surfaces/api/operator_write.py' 'Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py' 'Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py'`

Expected verification outcome:

- `operator_write.py` still owns preview and payload assembly
- `work_item_closeout_repository.py` becomes the only commit-time mutation owner for this closeout seam
- explicit fix proof is still required before bug or roadmap status mutates
- adjacent roadmap write proof still passes unchanged

## 8. Review -> healer -> human approval gate

- Review:
- confirm the operator closeout frontdoor delegates commit mutations to the repository and retains only preview, candidate selection, and payload assembly
- confirm preview payload shape, skip-reason semantics, and commit payload shape did not drift
- confirm no new direct write path to `bugs`, `bug_evidence_links`, or `roadmap_items` was introduced in surface code
- confirm the sprint did not widen into `bug_tracker.py`, `verifier_bug_bridge.py`, `post_workflow_sync.py`, or roadmap authoring behavior
- Healer:
- if review finds payload drift, timestamp drift, or status regression, repair only the scoped closeout seam in the files listed above
- do not widen healer work into bug filing, evidence-link authoring, roadmap package authoring, workflow-run sync logic, or migration edits
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 9 sprint
- if a later Phase 9 sprint is approved, target exactly one additional writer seam, most likely `runtime/post_workflow_sync.py` or one bug-evidence writer, instead of claiming global Phase 9 convergence
