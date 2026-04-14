# Phase 6 Platform Authority Schema

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `6` (`Platform Authority Schema`), status `historical_foundation`, predecessor phase `5`, with mandatory closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is grounded in the current checked-out repo at `/workspace`. The platform context names `/Users/nate/Praxis`, but that path is not present in this execution environment, so repo evidence below is taken from the live workspace snapshot while preserving the supplied database target `postgresql://nate@127.0.0.1:5432/praxis`. The execution shard also shows compile-authority scaffolding is present while proof coverage is still effectively zero, so this sprint must produce one narrow authority convergence with explicit verification.

## 1. Objective in repo terms

- Re-establish one bounded Phase 6 bootstrap boundary in the current repo instead of treating the whole `006_platform_authority_schema.sql` packet as one undifferentiated runtime dependency.
- Keep the first sprint focused on the subscription authority seam only:
- `event_subscriptions`
- `subscription_checkpoints`
- Repo-level target for this sprint: [Code&DBs/Workflow/runtime/subscription_repository.py](/workspace/Code&DBs/Workflow/runtime/subscription_repository.py) should bootstrap only the subscription-owned subset of Phase 6 authority, while continuing to route actual writes through the explicit Postgres writer helpers in [Code&DBs/Workflow/storage/postgres/subscription_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/subscription_repository.py).

## 2. Current evidence in the repo

- Phase `6` is declared as `Platform Authority Schema` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json), and the registry requires `review -> healer -> human_approval` before later phases.
- The schema origin already exists in [Code&DBs/Databases/migrations/workflow/006_platform_authority_schema.sql](/workspace/Code&DBs/Databases/migrations/workflow/006_platform_authority_schema.sql). It creates eight cross-cutting authority tables:
- `context_bundles`
- `context_bundle_anchors`
- `provider_model_candidates`
- `model_profile_candidate_bindings`
- `event_subscriptions`
- `subscription_checkpoints`
- `workflow_lanes`
- `workflow_lane_policies`
- Phase 6 already has dedicated repo seams for several of those families:
- [Code&DBs/Workflow/registry/context_bundle_repository.py](/workspace/Code&DBs/Workflow/registry/context_bundle_repository.py) bootstraps only the context-bundle subset by filtering `006_platform_authority_schema.sql` down to `context_bundles` and `context_bundle_anchors`.
- [Code&DBs/Workflow/policy/workflow_lanes.py](/workspace/Code&DBs/Workflow/policy/workflow_lanes.py) bootstraps only the workflow-lane subset by filtering the same migration down to `workflow_lanes` and `workflow_lane_policies`.
- [Code&DBs/Workflow/registry/route_catalog_repository.py](/workspace/Code&DBs/Workflow/registry/route_catalog_repository.py) reads the route-catalog half of Phase 6 together with later augmenting migrations `046` and `074`.
- The subscription authority seam is also real and already partially converged:
- [Code&DBs/Workflow/runtime/subscription_repository.py](/workspace/Code&DBs/Workflow/runtime/subscription_repository.py) exposes the durable `EventSubscriptionDefinition`, `EventSubscriptionCheckpoint`, `PostgresEventSubscriptionRepository`, and `bootstrap_subscription_repository_schema(...)` surface.
- The same module already routes persistence through `upsert_event_subscription_record(...)` and `upsert_subscription_checkpoint_record(...)` from [Code&DBs/Workflow/storage/postgres/subscription_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/subscription_repository.py) instead of inlining write SQL.
- [Code&DBs/Workflow/tests/unit/test_wave_e_no_inline_sql.py](/workspace/Code&DBs/Workflow/tests/unit/test_wave_e_no_inline_sql.py) explicitly guards that `runtime/subscription_repository.py` does not carry inline `INSERT`, `UPDATE`, `DELETE`, or `CREATE TABLE` write SQL.
- [Code&DBs/Workflow/tests/integration/test_subscription_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_subscription_repository.py) already proves the user-facing runtime path:
- bootstrap control plane, outbox, and subscription schema
- persist one event-subscription definition
- consume outbox facts
- persist and reload the checkpoint across a restart
- [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py) already proves `006_platform_authority_schema.sql` is in the manifest and declares the expected Phase 6 objects.
- The live drift is in the bootstrap boundary for subscriptions. In [runtime/subscription_repository.py](/workspace/Code&DBs/Workflow/runtime/subscription_repository.py), `_schema_statements()` currently returns the full result of `workflow_migration_statements("006_platform_authority_schema.sql")` with no table-family filter, unlike the scoped bootstrap helpers in `context_bundle_repository.py` and `policy/workflow_lanes.py`.
- That means `bootstrap_subscription_repository_schema(...)` currently applies all Phase 6 tables, not just `event_subscriptions` and `subscription_checkpoints`.
- No current subscription test proves that the subscription bootstrap helper is scoped to its own authority family or that unrelated Phase 6 tables remain absent when only the subscription seam is bootstrapped.

## 3. Gap or ambiguity still remaining

- Phase 6 is not missing. The schema packet exists and multiple consumers already depend on it.
- The unresolved question is which boundary owns subscription bootstrap truth:
- the subscription repository should bootstrap only its own two tables
- or the subscription repository is allowed to bootstrap the entire Phase 6 packet as a side effect
- Today the code says both things at once:
- the module name and repository contract are narrowly about subscriptions
- the actual bootstrap helper executes every statement in `006_platform_authority_schema.sql`
- That ambiguity matters because a caller can accidentally create unrelated authority surfaces such as `context_bundles`, `provider_model_candidates`, or `workflow_lanes` just by bootstrapping subscriptions, which masks missing bootstrap calls elsewhere and weakens phase-level authority proofs.
- This first sprint should not widen into:
- trigger/system-event redesign
- chat/event persistence cleanup
- provider onboarding or route-catalog refactors
- lane-policy redesign
- a whole-repo “split 006 into many migrations” rewrite

## 4. One bounded first sprint only

- Converge the subscription bootstrap helper onto the same scoped-bootstrap pattern already used by the context-bundle and workflow-lane repositories.
- In this sprint, do three things only:
- change [runtime/subscription_repository.py](/workspace/Code&DBs/Workflow/runtime/subscription_repository.py) so `_schema_statements()` filters `006_platform_authority_schema.sql` down to the subscription-owned objects and indexes for:
- `event_subscriptions`
- `subscription_checkpoints`
- keep `upsert_event_subscription_record(...)` and `upsert_subscription_checkpoint_record(...)` in [storage/postgres/subscription_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/subscription_repository.py) as the mutation owner
- add one focused integration proof that bootstraps only the subscription seam from an empty database state and asserts:
- `event_subscriptions` exists
- `subscription_checkpoints` exists
- at least one unrelated Phase 6 table that should remain outside this bootstrap path, such as `workflow_lanes` or `context_bundles`, does not exist until its own bootstrap helper runs
- Keep the existing restart/resume proof intact. Stop once the subscription bootstrap boundary is explicit and proven. Do not widen into broader runtime eventing or cross-phase schema decomposition.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/runtime/subscription_repository.py](/workspace/Code&DBs/Workflow/runtime/subscription_repository.py)
- Primary proof scope:
- [Code&DBs/Workflow/tests/integration/test_subscription_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_subscription_repository.py) or one new focused integration test beside it
- Read-only authority references:
- [Code&DBs/Databases/migrations/workflow/006_platform_authority_schema.sql](/workspace/Code&DBs/Databases/migrations/workflow/006_platform_authority_schema.sql)
- [Code&DBs/Workflow/storage/postgres/subscription_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/subscription_repository.py)
- [Code&DBs/Workflow/registry/context_bundle_repository.py](/workspace/Code&DBs/Workflow/registry/context_bundle_repository.py)
- [Code&DBs/Workflow/policy/workflow_lanes.py](/workspace/Code&DBs/Workflow/policy/workflow_lanes.py)
- [Code&DBs/Workflow/tests/unit/test_wave_e_no_inline_sql.py](/workspace/Code&DBs/Workflow/tests/unit/test_wave_e_no_inline_sql.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/triggers.py](/workspace/Code&DBs/Workflow/runtime/triggers.py)
- [Code&DBs/Workflow/runtime/subscriptions.py](/workspace/Code&DBs/Workflow/runtime/subscriptions.py), except for compatibility fallout from the scoped bootstrap
- [Code&DBs/Workflow/registry/context_bundle_repository.py](/workspace/Code&DBs/Workflow/registry/context_bundle_repository.py)
- [Code&DBs/Workflow/registry/provider_onboarding_repository.py](/workspace/Code&DBs/Workflow/registry/provider_onboarding_repository.py)
- [Code&DBs/Workflow/policy/workflow_lanes.py](/workspace/Code&DBs/Workflow/policy/workflow_lanes.py)
- migration renumbering or a redesign of the full Phase 6 packet layout
- trigger, event-log, or chat authority work

## 6. Done criteria

- `bootstrap_subscription_repository_schema(...)` no longer uses the full unfiltered statement set from `006_platform_authority_schema.sql`.
- The subscription bootstrap path applies only the schema objects required for:
- `event_subscriptions`
- `subscription_checkpoints`
- their supporting indexes and constraints
- `runtime/subscription_repository.py` continues to delegate row mutations through [storage/postgres/subscription_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/subscription_repository.py); no inline write SQL is introduced.
- A focused integration proof demonstrates that the subscription bootstrap helper creates subscription authority tables without also bootstrapping unrelated Phase 6 surfaces.
- The existing restart/resume path in [test_subscription_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_subscription_repository.py) still passes.
- No broader eventing, trigger, or packet-splitting refactor lands in this sprint.

## 7. Verification commands

- `cd /Users/nate/Praxis`
- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='Code&DBs/Workflow'`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_subscription_repository.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/unit/test_wave_e_no_inline_sql.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/unit/test_wave_e_repository_roundtrip.py -q`
- `rg -n "workflow_migration_statements\\(_SCHEMA_FILENAME\\)|event_subscriptions|subscription_checkpoints|workflow_lanes|context_bundles" Code\&DBs/Workflow/runtime/subscription_repository.py Code\&DBs/Workflow/tests/integration/test_subscription_repository.py`

Expected verification outcome:

- the subscription bootstrap helper visibly filters Phase 6 schema statements to the subscription-owned subset
- the focused proof shows the subscription seam can bootstrap independently
- the runtime repository still has no inline mutation SQL
- existing resume/checkpoint behavior remains green

## 8. Review -> healer -> human approval gate

- Review:
- confirm the sprint stayed inside the subscription bootstrap boundary of Phase 6
- confirm the proof exercises `bootstrap_subscription_repository_schema(...)` directly instead of manually creating subscription tables with ad hoc SQL
- confirm the proof demonstrates absence of at least one unrelated Phase 6 surface before its own bootstrap helper runs
- confirm no out-of-scope trigger, event-log, route-catalog, or lane refactor slipped in
- Healer:
- if review finds bootstrap drift or weak proof, repair only `runtime/subscription_repository.py` and the focused subscription tests
- do not widen healer work into `runtime/triggers.py`, `runtime/subscriptions.py`, provider onboarding, or lane-policy code
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 6 sprint
- the next Phase 6 sprint, if approved later, should take one adjacent family only, likely context-bundle bootstrap hardening or route-catalog authority convergence, not “finish all platform authority schema” in one pass
