# Phase 5 Workflow Outbox

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `5` (`Workflow Outbox`), status `historical_foundation`, predecessor phase `4`, required closeout sequence `review -> healer -> human_approval`.

Grounding note:
- This packet is grounded in the live checkout at `/workspace`.
- The supplied platform root for later execution is the Praxis repository root; run `source scripts/_workflow_env.sh && workflow_load_repo_env` at that root so `WORKFLOW_DATABASE_URL` is set. Repo evidence below is taken from `/workspace` while verification commands target the declared platform root.
- The execution shard says execution packets, repo snapshots, verification registry, and verify refs are ready, while verification coverage is still `0.0`. This sprint therefore needs one narrow proof on the real outbox bootstrap seam, not a broad notification redesign.

## 1. Objective in repo terms

- Reassert one canonical bootstrap authority for the workflow outbox seam in the current repo.
- Make [Code&DBs/Workflow/runtime/outbox.py](/workspace/Code&DBs/Workflow/runtime/outbox.py) stop treating `005_workflow_outbox.sql` as the only runtime bootstrap truth and instead bind it to the shared bootstrap-aware migration helpers already exposed by [Code&DBs/Workflow/storage/migrations.py](/workspace/Code&DBs/Workflow/storage/migrations.py).
- Prove one concrete Phase 5 truth through the live bootstrap seam: a database prepared by `bootstrap_workflow_outbox_schema(...)` should materialize the current receipt-trigger behavior that the repo classifies as bootstrap-relevant for outbox capture.
- Keep the sprint strictly on bootstrap authority convergence for the outbox seam. Do not turn this into a dispatch subsystem rewrite or notification architecture sweep.

## 2. Current evidence in the repo

- The authority map declares phase `5` as `Workflow Outbox` with predecessor `4` and mandatory closeout `review -> healer -> human_approval` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json).
- [Code&DBs/Databases/migrations/workflow/005_workflow_outbox.sql](/workspace/Code&DBs/Databases/migrations/workflow/005_workflow_outbox.sql) is the base Phase 5 schema authority:
- it creates `workflow_outbox`
- it defines `workflow_outbox_capture_event()`
- it defines `workflow_outbox_capture_receipt()`
- it installs insert triggers on `workflow_events` and `receipts`
- it backfills committed authority rows into `workflow_outbox`
- [Code&DBs/Workflow/runtime/outbox.py](/workspace/Code&DBs/Workflow/runtime/outbox.py) is the active runtime seam:
- it exposes `bootstrap_workflow_outbox_schema(...)`
- it exposes `fetch_workflow_outbox_batch(...)`
- it exposes `PostgresWorkflowOutboxSubscriber`
- it currently hardcodes `_OUTBOX_SCHEMA_FILENAME = "005_workflow_outbox.sql"`
- it reads the SQL file directly from `workflow_migrations_root()`
- it executes the raw file text in one transaction through `_schema_sql_text()`
- [Code&DBs/Workflow/storage/migrations.py](/workspace/Code&DBs/Workflow/storage/migrations.py) already provides the canonical bootstrap-aware helpers for this seam:
- `workflow_bootstrap_migration_path(...)`
- `workflow_bootstrap_migration_sql_text(...)`
- `workflow_bootstrap_migration_statements(...)`
- [Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py](/workspace/Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py) classifies:
- `005_workflow_outbox.sql` as `canonical`
- `023_dispatch_notifications.sql` as `bootstrap_only`
- both files inside `WORKFLOW_FULL_BOOTSTRAP_SEQUENCE`
- [Code&DBs/Databases/migrations/workflow/023_dispatch_notifications.sql](/workspace/Code&DBs/Databases/migrations/workflow/023_dispatch_notifications.sql) mutates the same Phase 5 seam:
- it creates `dispatch_notifications`
- it replaces `workflow_outbox_capture_receipt()`
- it preserves the `workflow_outbox` insert for every receipt
- it adds durable notification rows for `dispatch_job` receipts
- it also fires `pg_notify('dispatch_complete', ...)` for live listeners
- Existing integration coverage proves the outbox seam is live but does not prove the bootstrap authority convergence target:
- [Code&DBs/Workflow/tests/integration/test_workflow_outbox.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_outbox.py) bootstraps control-plane plus outbox schema and proves replay ordering plus committed-read behavior
- [Code&DBs/Workflow/tests/integration/test_subscription_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_subscription_repository.py) depends on the same outbox bootstrap helper for checkpoint and resume flows
- [Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py) uses the same helper in broader smoke coverage
- [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py) already proves the shared migration helper can parse bootstrap-aware statements, but it does not prove `runtime.outbox.bootstrap_workflow_outbox_schema(...)` actually routes through that authority surface

## 3. Gap or ambiguity still remaining

- The repo does not lack an outbox. The unresolved issue is bootstrap authority.
- The current repo state says two different things about the same seam:
- `storage/migrations.py` and generated migration authority say full bootstrap truth includes both canonical and bootstrap-only files, including `023_dispatch_notifications.sql`
- `runtime/outbox.py` only applies `005_workflow_outbox.sql`
- That leaves one concrete ambiguity for future execution agents:
- after calling `bootstrap_workflow_outbox_schema(...)`, should the resulting database include the current `dispatch_notifications` table and the amended receipt trigger behavior, or not
- This packet resolves only that ambiguity.
- Explicitly not resolved here:
- LISTEN/NOTIFY consumer topology
- dispatch worker or listener orchestration
- SSE or API notification surfaces
- receipt-writer redesign beyond what is minimally needed to exercise the trigger contract
- repo-wide notification convergence

## 4. One bounded first sprint only

- Replace the direct one-file bootstrap path in [Code&DBs/Workflow/runtime/outbox.py](/workspace/Code&DBs/Workflow/runtime/outbox.py) with canonical bootstrap-aware statement loading from [Code&DBs/Workflow/storage/migrations.py](/workspace/Code&DBs/Workflow/storage/migrations.py).
- Keep the runtime scope minimal:
- apply the base outbox migration authority from `005_workflow_outbox.sql`
- apply the bootstrap amendment from `023_dispatch_notifications.sql`
- do not widen beyond those outbox-relevant bootstrap statements
- Add one focused integration proof adjacent to [Code&DBs/Workflow/tests/integration/test_workflow_outbox.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_outbox.py) that:
- bootstraps through `bootstrap_workflow_outbox_schema(...)`
- inserts or persists a `dispatch_job` receipt through the existing authority tables already used by the outbox test harness
- proves the receipt still lands in `workflow_outbox`
- proves a `dispatch_notifications` row is also created for that `dispatch_job` receipt
- proves the new expectation would fail under the old `005`-only bootstrap path
- Stop after that proof passes and existing dependent tests stay green.
- Do not include:
- subscriber API changes
- new runtime notification abstractions
- migration regeneration or renumbering
- dispatch loop rewrites
- listener implementations

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/runtime/outbox.py](/workspace/Code&DBs/Workflow/runtime/outbox.py)
- Primary proof scope:
- [Code&DBs/Workflow/tests/integration/test_workflow_outbox.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_outbox.py)
- or one new adjacent focused integration test under [Code&DBs/Workflow/tests/integration](/workspace/Code&DBs/Workflow/tests/integration)
- Read-only authority references:
- [Code&DBs/Workflow/storage/migrations.py](/workspace/Code&DBs/Workflow/storage/migrations.py)
- [Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py](/workspace/Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py)
- [Code&DBs/Databases/migrations/workflow/005_workflow_outbox.sql](/workspace/Code&DBs/Databases/migrations/workflow/005_workflow_outbox.sql)
- [Code&DBs/Databases/migrations/workflow/023_dispatch_notifications.sql](/workspace/Code&DBs/Databases/migrations/workflow/023_dispatch_notifications.sql)
- Keep green while working:
- [Code&DBs/Workflow/tests/integration/test_subscription_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_subscription_repository.py)
- [Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py)
- [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py)
- Explicitly out of scope:
- dispatch listener loops
- `pg_notify` consumer orchestration
- SSE, MCP, or HTTP API surfaces
- broader receipt writer or evidence writer redesign
- migration manifest regeneration or authority-bucket cleanup
- any repo-wide notification architecture convergence beyond this bootstrap seam

## 6. Done criteria

- `bootstrap_workflow_outbox_schema(...)` no longer depends on a raw direct read of only `005_workflow_outbox.sql`.
- The implementation routes through canonical bootstrap-aware helpers already present in [Code&DBs/Workflow/storage/migrations.py](/workspace/Code&DBs/Workflow/storage/migrations.py).
- Bootstrapping the outbox seam includes the current receipt-trigger amendment from `023_dispatch_notifications.sql`.
- One focused integration test proves that a database bootstrapped through `bootstrap_workflow_outbox_schema(...)` produces both:
- a durable `workflow_outbox` row for the inserted `dispatch_job` receipt
- a durable `dispatch_notifications` row for that same receipt
- The new proof is specific enough that it would have failed before the bootstrap change.
- Existing outbox replay and dependent checkpoint or smoke tests remain green.
- No unrelated notification architecture or dispatch-consumer work lands in the sprint.

## 7. Verification commands

```bash
cd <Praxis repository root>  # contains ./scripts/_workflow_env.sh
. ./scripts/_workflow_env.sh && workflow_load_repo_env
export PYTHONPATH='Code&DBs/Workflow'
python -m pytest 'Code&DBs/Workflow/tests/integration/test_workflow_outbox.py' -q
python -m pytest 'Code&DBs/Workflow/tests/integration/test_subscription_repository.py' -q
python -m pytest 'Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py' -q
python -m pytest 'Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py' -q
rg -n "_OUTBOX_SCHEMA_FILENAME|workflow_bootstrap_migration_statements|workflow_bootstrap_migration_sql_text|023_dispatch_notifications\\.sql|dispatch_notifications" \
  'Code&DBs/Workflow/runtime/outbox.py' \
  'Code&DBs/Workflow/tests/integration/test_workflow_outbox.py' \
  'Code&DBs/Workflow/storage/migrations.py' \
  'Code&DBs/Workflow/storage/_generated_workflow_migration_authority.py'
```

Expected verification outcome:

- `runtime/outbox.py` is visibly bound to bootstrap-aware migration helpers rather than only `_OUTBOX_SCHEMA_FILENAME`
- the focused integration proof covers `dispatch_job` receipt capture into both `workflow_outbox` and `dispatch_notifications`
- existing replay, subscription, smoke, and migration-contract proofs still pass

## 8. Review -> healer -> human approval gate

- Review:
- confirm the sprint stayed on the Phase 5 bootstrap-authority seam only
- confirm the new proof executes through `bootstrap_workflow_outbox_schema(...)` rather than by manually applying `023_dispatch_notifications.sql`
- confirm the new proof would have failed under the old `005`-only runtime bootstrap
- confirm no listener, API, dispatch-loop, or broader notification-architecture redesign leaked into scope
- Healer:
- if review finds drift, repair only the scoped outbox bootstrap helper and the focused outbox integration proof
- do not widen healer work into dispatch listeners, API handlers, runtime notification abstractions, or migration-policy regeneration
- Human approval gate:
- require explicit human approval after review and any healer pass before opening another Phase 5 sprint
- any follow-on Phase 5 work must take one adjacent seam only, not a repo-wide notification rewrite
