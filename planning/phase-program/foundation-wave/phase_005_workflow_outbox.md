# Phase 5 Workflow Outbox

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `5` (`Workflow Outbox`), status `historical_foundation`, predecessor phase `4`, required closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is grounded in the checked-out repo at `/workspace` and intended for execution in the declared platform root `/Users/nate/Praxis` with `WORKFLOW_DATABASE_URL=postgresql://nate@127.0.0.1:5432/praxis`. Repo evidence below reflects the live files present in this checkout.

## 1. Objective in repo terms

- Reassert one canonical bootstrap authority for the workflow outbox seam.
- Make [Code&DBs/Workflow/runtime/outbox.py](/workspace/Code&DBs/Workflow/runtime/outbox.py) stop treating `005_workflow_outbox.sql` as the only bootstrap truth and instead load Phase 5 bootstrap statements through the shared workflow migration authority helpers in [Code&DBs/Workflow/storage/migrations.py](/workspace/Code&DBs/Workflow/storage/migrations.py).
- Prove that `bootstrap_workflow_outbox_schema(...)` materializes the current outbox receipt-trigger behavior that the repo already classifies as bootstrap-relevant, without widening into notification-consumer redesign.

## 2. Current evidence in the repo

- The authority map declares phase `5` as `Workflow Outbox` with predecessor `4` and closeout sequence `review -> healer -> human_approval` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json).
- [Code&DBs/Databases/migrations/workflow/005_workflow_outbox.sql](/workspace/Code&DBs/Databases/migrations/workflow/005_workflow_outbox.sql) is the base Phase 5 schema authority. It creates `workflow_outbox`, replay indexes, and trigger functions that mirror workflow events and receipts into the outbox.
- [Code&DBs/Workflow/runtime/outbox.py](/workspace/Code&DBs/Workflow/runtime/outbox.py) is the active runtime seam. It exposes `bootstrap_workflow_outbox_schema(...)`, `fetch_workflow_outbox_batch(...)`, and `PostgresWorkflowOutboxSubscriber`.
- The current runtime bootstrap implementation is file-local and narrow:
- it hardcodes `_OUTBOX_SCHEMA_FILENAME = "005_workflow_outbox.sql"`
- it reads that file directly from `workflow_migrations_root()`
- it executes the full SQL text in one transaction
- [Code&DBs/Workflow/storage/migrations.py](/workspace/Code&DBs/Workflow/storage/migrations.py) already exposes canonical bootstrap-aware helpers:
- `workflow_bootstrap_migration_path(...)`
- `workflow_bootstrap_migration_sql_text(...)`
- `workflow_bootstrap_migration_statements(...)`
- [Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py](/workspace/Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py) classifies:
- `005_workflow_outbox.sql` as `canonical`
- `023_dispatch_notifications.sql` as `bootstrap_only`
- and includes both in `WORKFLOW_FULL_BOOTSTRAP_SEQUENCE`
- [Code&DBs/Databases/migrations/workflow/023_dispatch_notifications.sql](/workspace/Code&DBs/Databases/migrations/workflow/023_dispatch_notifications.sql) mutates the same seam by:
- creating `dispatch_notifications`
- replacing `workflow_outbox_capture_receipt()`
- preserving the `workflow_outbox` insert
- adding durable notification rows for `dispatch_job` receipts
- Existing tests prove the seam is active but do not currently prove the bootstrap-authority convergence target:
- [Code&DBs/Workflow/tests/integration/test_workflow_outbox.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_outbox.py) proves replay ordering and committed-read behavior through `bootstrap_workflow_outbox_schema(...)`
- [Code&DBs/Workflow/tests/integration/test_subscription_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_subscription_repository.py) depends on the same bootstrap helper for checkpoint/resume paths
- [Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py) bootstraps the seam in broader smoke coverage
- [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py) already proves the shared migration helper can parse bootstrap-aware statements, but it does not prove `runtime.outbox.bootstrap_workflow_outbox_schema(...)` uses that authority surface

## 3. Gap or ambiguity still remaining

- The unresolved issue is not whether an outbox exists. It does.
- The unresolved issue is which authority defines the outbox bootstrap contract for runtime callers.
- Today the repo has two competing truths for the same seam:
- `storage/migrations.py` says full bootstrap authority includes bootstrap-only amendments such as `023_dispatch_notifications.sql`
- `runtime/outbox.py` only executes `005_workflow_outbox.sql`
- That leaves one concrete ambiguity for future execution agents:
- after calling `bootstrap_workflow_outbox_schema(...)`, should the database include the receipt-trigger behavior from `023_dispatch_notifications.sql` or not
- This packet resolves only that bootstrap-authority ambiguity. It does not attempt to settle LISTEN/NOTIFY architecture, dispatch-consumer ownership, or repo-wide notification transport design.

## 4. One bounded first sprint only

- Replace the direct one-file bootstrap read in `runtime/outbox.py` with canonical bootstrap-aware statement loading from `storage/migrations.py`.
- Apply only the outbox-relevant bootstrap sequence needed to keep Phase 5 truthful in repo terms:
- the base outbox migration `005_workflow_outbox.sql`
- the later bootstrap amendment `023_dispatch_notifications.sql`
- Add one focused integration proof adjacent to [Code&DBs/Workflow/tests/integration/test_workflow_outbox.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_outbox.py) that:
- bootstraps through `bootstrap_workflow_outbox_schema(...)`
- inserts a `dispatch_job` receipt through the existing workflow authority tables
- proves the receipt still lands in `workflow_outbox`
- proves a `dispatch_notifications` row is also created
- Stop after that proof passes. Do not widen into subscriber API changes, new runtime notification abstractions, migration regeneration, or dispatch loop rewrites.

## 5. Exact file or subsystem scope

- In scope:
- [Code&DBs/Workflow/runtime/outbox.py](/workspace/Code&DBs/Workflow/runtime/outbox.py)
- [Code&DBs/Workflow/tests/integration/test_workflow_outbox.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_outbox.py) or one new adjacent focused integration test
- read-only use of [Code&DBs/Workflow/storage/migrations.py](/workspace/Code&DBs/Workflow/storage/migrations.py)
- read-only use of [Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py](/workspace/Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py)
- read-only use of [Code&DBs/Databases/migrations/workflow/005_workflow_outbox.sql](/workspace/Code&DBs/Databases/migrations/workflow/005_workflow_outbox.sql)
- read-only use of [Code&DBs/Databases/migrations/workflow/023_dispatch_notifications.sql](/workspace/Code&DBs/Databases/migrations/workflow/023_dispatch_notifications.sql)
- Keep green while working:
- [Code&DBs/Workflow/tests/integration/test_subscription_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_subscription_repository.py)
- [Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py)
- [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py)
- Out of scope:
- dispatch listener loops
- `pg_notify` consumer orchestration
- SSE or API surfaces
- receipt writer redesign outside what is minimally needed to exercise the trigger contract
- migration renumbering, manifest regeneration, or broader policy-bucket cleanup
- any repo-wide notification architecture convergence beyond this one bootstrap seam

## 6. Done criteria

- `bootstrap_workflow_outbox_schema(...)` no longer depends on a raw direct read of only `005_workflow_outbox.sql`.
- The implementation routes through canonical bootstrap-aware helpers already present in `storage/migrations.py`.
- Bootstrapping the outbox seam includes the current receipt-trigger amendment from `023_dispatch_notifications.sql`.
- One focused integration test proves that a bootstrapped database produces both:
- a durable `workflow_outbox` row for the inserted receipt
- a durable `dispatch_notifications` row for a `dispatch_job` receipt
- Existing outbox replay and checkpoint/resume tests remain green.
- No unrelated notification architecture or consumer-surface work lands in the sprint.

## 7. Verification commands

```bash
cd /Users/nate/Praxis
export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'
export PYTHONPATH='Code&DBs/Workflow'
python -m pytest 'Code&DBs/Workflow/tests/integration/test_workflow_outbox.py' -q
python -m pytest 'Code&DBs/Workflow/tests/integration/test_subscription_repository.py' -q
python -m pytest 'Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py' -q
python -m pytest 'Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py' -q
rg -n "_OUTBOX_SCHEMA_FILENAME|workflow_bootstrap_migration_statements|023_dispatch_notifications.sql|dispatch_notifications" \
  'Code&DBs/Workflow/runtime/outbox.py' \
  'Code&DBs/Workflow/tests/integration/test_workflow_outbox.py' \
  'Code&DBs/Workflow/storage/migrations.py'
```

Expected verification outcome:

- `runtime/outbox.py` is visibly bound to canonical bootstrap-aware loading rather than only `_OUTBOX_SCHEMA_FILENAME`
- the outbox integration proof covers `dispatch_notifications`
- existing replay and subscription proofs still pass

## 8. Review -> healer -> human approval gate

- Review:
- confirm the sprint stayed on the bootstrap authority seam for Phase 5
- confirm the proof executes through `bootstrap_workflow_outbox_schema(...)` rather than by manually applying `023_dispatch_notifications.sql`
- confirm the new test would have failed under the old hardcoded `005` bootstrap path
- confirm no consumer-topology, API, or dispatch-loop redesign leaked into scope
- Healer:
- if review finds drift, repair only the scoped bootstrap helper and the focused outbox integration proof
- do not widen healer work into listeners, API handlers, runtime notification abstractions, or migration-policy regeneration
- Human approval gate:
- require explicit human approval after review and any healer pass before opening another Phase 5 sprint
- any follow-on Phase 5 work must take one adjacent seam only, not a repo-wide notification rewrite
