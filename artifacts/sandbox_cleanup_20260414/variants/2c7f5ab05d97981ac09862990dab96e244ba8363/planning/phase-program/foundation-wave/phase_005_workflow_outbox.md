# Phase 5 Workflow Outbox

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `5` (`Workflow Outbox`), status `historical_foundation`, predecessor phase `4`, with mandatory closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is based on the current checked-out repo snapshot. The core outbox seam exists and is exercised, but its bootstrap helper is still narrower than the repo's declared migration authority for outbox-owned trigger behavior.

## 1. Objective in repo terms

- Reassert one canonical Phase 5 bootstrap seam for the workflow outbox in the current repo.
- Keep the sprint bounded to outbox schema/bootstrap authority, not to every downstream notification or worker consumer.
- Repo-level target for this sprint: when runtime code bootstraps the workflow outbox through [Code&DBs/Workflow/runtime/outbox.py](/workspace/Code&DBs/Workflow/runtime/outbox.py), it must apply the canonical bootstrap-authority statements for the outbox seam rather than hardcoding only migration `005_workflow_outbox.sql`.

## 2. Current evidence in the repo

- Phase `5` is declared as `Workflow Outbox` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json), and the registry requires `review -> healer -> human_approval` before moving forward.
- The schema origin for this phase is [Code&DBs/Databases/migrations/workflow/005_workflow_outbox.sql](/workspace/Code&DBs/Databases/migrations/workflow/005_workflow_outbox.sql). It creates `workflow_outbox`, its indexes, and the trigger functions `workflow_outbox_capture_event()` and `workflow_outbox_capture_receipt()`.
- The outbox is explicitly derived rather than authoritative. Migration `005` documents that `workflow_events` and `receipts` remain the source of truth and that subscribers consume the derived outbox seam.
- The current runtime seam is [Code&DBs/Workflow/runtime/outbox.py](/workspace/Code&DBs/Workflow/runtime/outbox.py). It exposes:
- `bootstrap_workflow_outbox_schema(...)`
- `fetch_workflow_outbox_batch(...)`
- `PostgresWorkflowOutboxSubscriber`
- `WorkflowOutboxCursor` / `WorkflowOutboxRecord` / `WorkflowOutboxBatch`
- The outbox subscriber path is already used and proven in the current repo:
- [Code&DBs/Workflow/tests/integration/test_workflow_outbox.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_outbox.py) proves replay-order reads from committed authority rows
- [Code&DBs/Workflow/tests/integration/test_subscription_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_subscription_repository.py) proves worker checkpoint resume over outbox facts
- [Code&DBs/Workflow/tests/integration/test_workflow_bridge.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_bridge.py) proves the dispatch bridge consumes `runtime.outbox` without inventing a second lifecycle state machine
- Later repo history already amends the outbox seam. [Code&DBs/Databases/migrations/workflow/023_dispatch_notifications.sql](/workspace/Code&DBs/Databases/migrations/workflow/023_dispatch_notifications.sql) replaces `workflow_outbox_capture_receipt()` so dispatch-job receipts also write durable `dispatch_notifications` rows and emit `pg_notify('dispatch_complete', ...)`.
- The generated migration authority already classifies `023_dispatch_notifications.sql` as `bootstrap_only` in [Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py](/workspace/Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py), meaning it is part of the repo's declared full bootstrap order even though it is not in the canonical manifest.
- [Code&DBs/Workflow/storage/migrations.py](/workspace/Code&DBs/Workflow/storage/migrations.py) already provides the policy-aware helpers `workflow_bootstrap_migration_path(...)`, `workflow_bootstrap_migration_sql_text(...)`, and `workflow_bootstrap_migration_statements(...)` for exactly this kind of bootstrap-eligible migration loading.
- The live drift is in `runtime.outbox.py`: it hardcodes `_OUTBOX_SCHEMA_FILENAME = "005_workflow_outbox.sql"` and reads that file directly, so `bootstrap_workflow_outbox_schema(...)` bypasses the declared migration-authority path and ignores later bootstrap-only amendments to the same outbox trigger family.
- No current outbox test proves that bootstrapping via `runtime.outbox` also brings in the later dispatch-notification behavior from migration `023`.

## 3. Gap or ambiguity still remaining

- The main ambiguity is not whether the outbox exists. It does.
- The unresolved question is which code path owns bootstrap truth for the outbox seam:
- the generated migration-authority path in `storage/migrations.py`
- or the file-local hardcoded `005` reader in `runtime.outbox.py`
- Because `023_dispatch_notifications.sql` mutates the same receipt trigger family, the current helper can bootstrap an outbox shape that is older than the repo's declared bootstrap authority.
- The first sprint should not widen into replacing all notification consumers, deleting `workflow_notifications`, or redesigning SSE wakeup channels. Those are adjacent seams, but they are not required to restore one trustworthy Phase 5 bootstrap boundary.

## 4. One bounded first sprint only

- Replace the hardcoded single-file outbox bootstrap in `runtime.outbox.py` with a canonical bootstrap-authority path that applies:
- `005_workflow_outbox.sql`
- any later bootstrap-eligible migrations that explicitly amend Phase 5 outbox-owned objects, starting with `023_dispatch_notifications.sql`
- Add one focused integration contract that bootstraps through `bootstrap_workflow_outbox_schema(...)`, inserts a dispatch-job receipt, and proves the bootstrapped database now reflects both parts of the current repo contract:
- the receipt still lands in `workflow_outbox`
- the amended receipt trigger also creates the durable `dispatch_notifications` row
- Keep the read contract stable:
- `fetch_workflow_outbox_batch(...)`
- `PostgresWorkflowOutboxSubscriber`
- `WorkflowOutboxBatch`
- Stop once runtime bootstrap is authority-aware and the new contract test passes. Do not widen into runtime consumer migration, LISTEN loop rewrites, or notification-surface unification.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/runtime/outbox.py](/workspace/Code&DBs/Workflow/runtime/outbox.py)
- Primary contract-test scope:
- [Code&DBs/Workflow/tests/integration/test_workflow_outbox.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_outbox.py) or one new focused integration test file beside it
- Read-only authority references:
- [Code&DBs/Workflow/storage/migrations.py](/workspace/Code&DBs/Workflow/storage/migrations.py)
- [Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py](/workspace/Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py)
- [Code&DBs/Databases/migrations/workflow/005_workflow_outbox.sql](/workspace/Code&DBs/Databases/migrations/workflow/005_workflow_outbox.sql)
- [Code&DBs/Databases/migrations/workflow/023_dispatch_notifications.sql](/workspace/Code&DBs/Databases/migrations/workflow/023_dispatch_notifications.sql)
- Existing regression context to keep green:
- [Code&DBs/Workflow/tests/integration/test_subscription_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_subscription_repository.py)
- [Code&DBs/Workflow/tests/integration/test_workflow_bridge.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_bridge.py)
- [Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/workflow_notifications.py](/workspace/Code&DBs/Workflow/runtime/workflow_notifications.py)
- [Code&DBs/Workflow/storage/postgres/receipt_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/receipt_repository.py)
- [Code&DBs/Workflow/runtime/workflow/receipt_writer.py](/workspace/Code&DBs/Workflow/runtime/workflow/receipt_writer.py)
- [Code&DBs/Workflow/surfaces/api/handlers/workflow_run.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_run.py)
- any migration renumbering or authority-manifest redesign
- any repo-wide notification transport consolidation

## 6. Done criteria

- `runtime.outbox.bootstrap_workflow_outbox_schema(...)` no longer depends on a file-local hardcoded `005_workflow_outbox.sql` read path as its only source of bootstrap truth.
- The bootstrap helper applies the canonical bootstrap-authority statements needed for the outbox seam, including the current repo amendment in `023_dispatch_notifications.sql`.
- A focused integration test proves that bootstrapping through `runtime.outbox` produces the current repo behavior for dispatch-job receipts:
- one durable `workflow_outbox` row
- one durable `dispatch_notifications` row
- Existing outbox replay/checkpoint regressions still pass without payload-shape drift.
- No notification-consumer redesign, no new schema numbering, and no broader runtime bus unification lands in this sprint.

## 7. Verification commands

- `cd /Users/nate/Praxis`
- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='Code&DBs/Workflow'`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_workflow_outbox.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_subscription_repository.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_workflow_migration_policy_boundaries.py -q`
- `rg -n "_OUTBOX_SCHEMA_FILENAME|workflow_bootstrap_migration_statements|023_dispatch_notifications.sql|dispatch_notifications" Code\&DBs/Workflow/runtime/outbox.py Code\&DBs/Workflow/tests/integration/test_workflow_outbox.py`

Expected verification outcome:

- the outbox bootstrap path is visibly tied to bootstrap-aware migration authority instead of a raw `005` file read
- the focused outbox integration contract proves the current receipt-trigger behavior after bootstrap
- existing outbox consumer regressions remain green

## 8. Review -> healer -> human approval gate

- Review:
- confirm the sprint stayed inside Phase 5 bootstrap authority for the outbox seam
- confirm the new proof uses `bootstrap_workflow_outbox_schema(...)` rather than manually executing `023`
- confirm the proof would have failed under the old hardcoded-`005` bootstrap path
- confirm no out-of-scope notification consumer or SSE rewrites landed
- Healer:
- if review finds bootstrap-authority drift or a weak proof, repair only the scoped outbox bootstrap and test files
- do not widen healer work into `workflow_notifications`, `receipt_writer`, or UI streaming code
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 5 sprint
- the next Phase 5 sprint, if approved later, should take one adjacent seam only, most likely a consumer convergence seam, not a repo-wide notification rewrite
