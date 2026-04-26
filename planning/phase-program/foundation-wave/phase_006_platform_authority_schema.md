# Phase 6 Platform Authority Schema

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `6` (`Platform Authority Schema`), status `historical_foundation`, predecessor phase `5`, required closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is grounded in the mounted repo snapshot at `/workspace`. The platform context names the Praxis repository root as the canonical execution root, so evidence below cites the current checkout and verification commands are written for that root. The execution shard says execution packets, repo snapshots, verification registry, and verify refs are ready, while verification coverage is still `0.0`, so this packet keeps Phase 6 to one proofable seam only.

## 1. Objective in repo terms

- Reassert one explicit authority boundary inside Phase 6 instead of letting `006_platform_authority_schema.sql` behave like an undifferentiated bootstrap packet for every consumer.
- Keep the first sprint on the runtime-owned subscription family only:
- `event_subscriptions`
- `subscription_checkpoints`
- Repo outcome for this sprint:
- [Code&DBs/Workflow/runtime/subscription_repository.py](/workspace/Code&DBs/Workflow/runtime/subscription_repository.py) must bootstrap only the subscription-owned subset of Phase 6
- durable row mutation must remain owned by [Code&DBs/Workflow/storage/postgres/subscription_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/subscription_repository.py)
- the boundary must be proved against the current migration authority and live repo seams, not by introducing a new migration or ad hoc SQL bootstrap path

## 2. Current evidence in the repo

- Phase `6` is declared in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) as `Platform Authority Schema`, with predecessor `5` and mandatory closeout `review -> healer -> human_approval`.
- The canonical Phase 6 schema packet is [Code&DBs/Databases/migrations/workflow/006_platform_authority_schema.sql](/workspace/Code&DBs/Databases/migrations/workflow/006_platform_authority_schema.sql). In the current repo snapshot it creates exactly eight table families:
- `context_bundles`
- `context_bundle_anchors`
- `provider_model_candidates`
- `model_profile_candidate_bindings`
- `event_subscriptions`
- `subscription_checkpoints`
- `workflow_lanes`
- `workflow_lane_policies`
- The same objects are registered in [Code&DBs/Workflow/system_authority/workflow_migration_authority.json](/workspace/Code&DBs/Workflow/system_authority/workflow_migration_authority.json), and [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py) already proves that `006_platform_authority_schema.sql` declares the expected Phase 6 objects.
- Two other Phase 6 consumers already narrow the migration packet to their own owned families:
- [Code&DBs/Workflow/registry/context_bundle_repository.py](/workspace/Code&DBs/Workflow/registry/context_bundle_repository.py) filters the Phase 6 statements to `context_bundles` and `context_bundle_anchors`
- [Code&DBs/Workflow/policy/workflow_lanes.py](/workspace/Code&DBs/Workflow/policy/workflow_lanes.py) filters the same packet to `workflow_lanes` and `workflow_lane_policies`
- The route-catalog seam is broader but still Phase 6-aware:
- [Code&DBs/Workflow/registry/route_catalog_repository.py](/workspace/Code&DBs/Workflow/registry/route_catalog_repository.py) loads `006_platform_authority_schema.sql` plus `046_provider_model_candidate_profiles.sql` and `074_provider_policy_multi_provider_refs.sql`
- The subscription family already has a real runtime repository seam:
- [Code&DBs/Workflow/runtime/subscription_repository.py](/workspace/Code&DBs/Workflow/runtime/subscription_repository.py) defines `EventSubscriptionDefinition`, `EventSubscriptionCheckpoint`, `PostgresEventSubscriptionRepository`, and `bootstrap_subscription_repository_schema(...)`
- the same module delegates mutation writes through `upsert_event_subscription_record(...)` and `upsert_subscription_checkpoint_record(...)` from [Code&DBs/Workflow/storage/postgres/subscription_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/subscription_repository.py)
- [Code&DBs/Workflow/tests/unit/test_wave_e_no_inline_sql.py](/workspace/Code&DBs/Workflow/tests/unit/test_wave_e_no_inline_sql.py) explicitly guards that `runtime/subscription_repository.py` does not inline `INSERT`, `UPDATE`, `DELETE FROM`, or `CREATE TABLE` write SQL
- [Code&DBs/Workflow/tests/integration/test_subscription_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_subscription_repository.py) already proves the durable runtime path:
- bootstrap control-plane, outbox, and subscription schema
- persist a durable subscription definition
- consume workflow outbox facts
- persist and reload checkpoints across restart
- The active ambiguity is visible in code:
- `_schema_statements()` in [Code&DBs/Workflow/runtime/subscription_repository.py](/workspace/Code&DBs/Workflow/runtime/subscription_repository.py) currently returns `workflow_migration_statements("006_platform_authority_schema.sql")` with no marker-based filtering
- unlike the context-bundle and workflow-lane seams, the subscription bootstrap helper therefore executes the full Phase 6 statement set
- no existing test currently proves that bootstrapping subscriptions leaves unrelated Phase 6 families absent

## 3. Gap or ambiguity still remaining

- Phase 6 exists; the unresolved problem is authority scope, not missing schema.
- The current repo still allows two conflicting interpretations:
- the subscription repository is named and structured as the owner of the runtime subscription family only
- its bootstrap path executes every executable statement in `006_platform_authority_schema.sql`
- That means a caller can currently create unrelated Phase 6 surfaces such as:
- `context_bundles`
- `provider_model_candidates`
- `workflow_lanes`
- merely by bootstrapping subscriptions
- That weakens future authority proofs because bootstrap ownership becomes broader than read ownership and broader than write ownership.
- This first sprint must not widen into:
- splitting `006_platform_authority_schema.sql` into multiple migrations
- redesigning trigger or outbox behavior
- route-catalog convergence
- workflow-lane policy changes
- generic cleanup of every Phase 6 consumer

## 4. One bounded first sprint only

- Converge the subscription bootstrap helper onto the same scoped-bootstrap pattern already used by the context-bundle and workflow-lane seams.
- In this sprint, do only these things:
- change [Code&DBs/Workflow/runtime/subscription_repository.py](/workspace/Code&DBs/Workflow/runtime/subscription_repository.py) so `_schema_statements()` filters `006_platform_authority_schema.sql` to the subscription-owned tables, indexes, and constraints for:
- `event_subscriptions`
- `subscription_checkpoints`
- keep all mutation writes in [Code&DBs/Workflow/storage/postgres/subscription_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/subscription_repository.py)
- add one focused proof that bootstraps only the subscription seam from an empty Phase 6 state and asserts:
- `event_subscriptions` exists
- `subscription_checkpoints` exists
- at least one unrelated Phase 6 table such as `context_bundles` or `workflow_lanes` does not exist until its own authority bootstrap runs
- keep the existing subscription round-trip and restart behavior green
- Stop as soon as the subscription bootstrap boundary is explicit and proved. Do not widen into route-catalog, provider-onboarding, trigger, or lane-family work.

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/runtime/subscription_repository.py](/workspace/Code&DBs/Workflow/runtime/subscription_repository.py)
- Primary proof scope:
- [Code&DBs/Workflow/tests/integration/test_subscription_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_subscription_repository.py) or one new adjacent focused test under [Code&DBs/Workflow/tests/integration](/workspace/Code&DBs/Workflow/tests/integration)
- Read-only authority references:
- [Code&DBs/Databases/migrations/workflow/006_platform_authority_schema.sql](/workspace/Code&DBs/Databases/migrations/workflow/006_platform_authority_schema.sql)
- [Code&DBs/Workflow/system_authority/workflow_migration_authority.json](/workspace/Code&DBs/Workflow/system_authority/workflow_migration_authority.json)
- [Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py)
- [Code&DBs/Workflow/storage/postgres/subscription_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/subscription_repository.py)
- [Code&DBs/Workflow/registry/context_bundle_repository.py](/workspace/Code&DBs/Workflow/registry/context_bundle_repository.py)
- [Code&DBs/Workflow/policy/workflow_lanes.py](/workspace/Code&DBs/Workflow/policy/workflow_lanes.py)
- [Code&DBs/Workflow/tests/unit/test_wave_e_no_inline_sql.py](/workspace/Code&DBs/Workflow/tests/unit/test_wave_e_no_inline_sql.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/triggers.py](/workspace/Code&DBs/Workflow/runtime/triggers.py), except for compatibility fallout if the narrower bootstrap reveals a real defect
- [Code&DBs/Workflow/runtime/subscriptions.py](/workspace/Code&DBs/Workflow/runtime/subscriptions.py), except for compatibility fallout from the narrower bootstrap
- [Code&DBs/Workflow/registry/context_bundle_repository.py](/workspace/Code&DBs/Workflow/registry/context_bundle_repository.py)
- [Code&DBs/Workflow/registry/route_catalog_repository.py](/workspace/Code&DBs/Workflow/registry/route_catalog_repository.py)
- [Code&DBs/Workflow/registry/provider_onboarding_repository.py](/workspace/Code&DBs/Workflow/registry/provider_onboarding_repository.py)
- [Code&DBs/Workflow/policy/workflow_lanes.py](/workspace/Code&DBs/Workflow/policy/workflow_lanes.py)
- migration renumbering or packet decomposition
- Phase 6-wide cleanup beyond the subscription bootstrap seam

## 6. Done criteria

- `bootstrap_subscription_repository_schema(...)` no longer executes the full unfiltered statement set from `006_platform_authority_schema.sql`.
- The subscription bootstrap path applies only the Phase 6 objects required for:
- `event_subscriptions`
- `subscription_checkpoints`
- their supporting indexes and constraints
- `runtime/subscription_repository.py` still delegates row mutation through [Code&DBs/Workflow/storage/postgres/subscription_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/subscription_repository.py); no inline mutation SQL is introduced.
- One focused integration proof demonstrates that the subscription seam can bootstrap independently without materializing unrelated Phase 6 surfaces.
- Existing restart and resume behavior in [Code&DBs/Workflow/tests/integration/test_subscription_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_subscription_repository.py) still passes.
- Existing migration-contract and generated-authority proofs for Phase 6 remain green.
- No new migration is added and no out-of-scope Phase 6 family is modified.

## 7. Verification commands

- `cd` to the Praxis repository root (the directory that contains `scripts/_workflow_env.sh`)
- `. ./scripts/_workflow_env.sh && workflow_load_repo_env`
- `export PYTHONPATH='Code&DBs/Workflow'`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_subscription_repository.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_workflow_schema_authority_artifacts.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/unit/test_wave_e_no_inline_sql.py' -q`
- `rg -n "workflow_migration_statements\\(|event_subscriptions|subscription_checkpoints|context_bundles|workflow_lanes" 'Code&DBs/Workflow/runtime/subscription_repository.py' 'Code&DBs/Workflow/tests/integration/test_subscription_repository.py' 'Code&DBs/Workflow/registry/context_bundle_repository.py' 'Code&DBs/Workflow/policy/workflow_lanes.py'`
- `rg -n '"006_platform_authority_schema.sql"|event_subscriptions|subscription_checkpoints|workflow_lanes|context_bundles' 'Code&DBs/Workflow/system_authority/workflow_migration_authority.json' 'Code&DBs/Workflow/tests/integration/test_workflow_migration_contracts.py'`

Expected verification outcome:

- the subscription bootstrap helper visibly filters Phase 6 statements to the runtime-owned subscription family
- the focused proof shows `event_subscriptions` and `subscription_checkpoints` can bootstrap without also creating unrelated Phase 6 tables
- runtime mutation ownership stays in the Postgres writer helper
- existing restart/resume behavior and migration-authority coverage remain green

## 8. Review -> healer -> human approval gate

- Review:
- confirm the sprint stayed inside the subscription bootstrap seam of Phase 6
- confirm the new proof executes through `bootstrap_subscription_repository_schema(...)` rather than direct table-creation SQL
- confirm the proof checks absence of at least one unrelated Phase 6 table before its own bootstrap path runs
- confirm no route-catalog, provider-onboarding, workflow-lane, trigger, or outbox redesign leaked in
- Healer:
- if review finds drift or weak proof, repair only:
- [Code&DBs/Workflow/runtime/subscription_repository.py](/workspace/Code&DBs/Workflow/runtime/subscription_repository.py)
- the focused subscription proof under [Code&DBs/Workflow/tests/integration](/workspace/Code&DBs/Workflow/tests/integration)
- do not widen healer work into `runtime/triggers.py`, `runtime/subscriptions.py`, route-catalog code, provider onboarding, or lane-policy code
- Human approval gate:
- require explicit human approval after review and any healer pass before a second Phase 6 sprint opens
- if a later Phase 6 sprint is approved, take exactly one adjacent family next, such as route-catalog bootstrap hardening, not a generic Phase 6 cleanup sweep
