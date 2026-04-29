# Wave 7 Manifest Contract Readiness Plan

## Authority Model
- Top authority is Praxis.db standing orders surfaced by `/orient`; those rules outrank this plan and any sidecar notes.
- Bug scope authority is the four packet bugs named in the queue and nothing else:
  - `BUG-62F78235 [P1/RUNTIME]`
  - `BUG-8F6A612A [P1/VERIFY]`
  - `BUG-123C17AC [P2/TEST]`
  - `BUG-9D09F47D [P1/VERIFY]`
- The manifest contract is the DB-backed workflow launch contract, not a filesystem convention. It ties the numbered migration manifest, the generated workflow migration authority, and the expected-object contract together so startup and health can fail closed when the manifest, catalog, or runtime truth drift.
- The quarantine gate is the authoritative fence between stale or unsafe packet fleets and any retry/relaunch path. If a packet fleet is stale, incomplete, or backed by degraded authority, the gate must quarantine it or force a fresh recompile/reissue path instead of letting the old fleet keep retrying.
- Readiness truth must come from authoritative runtime/surface state, not from a single `booted: true` flag or a successful route-discovery probe. Before any relaunch, the plan requires the system to surface:
  - manifest authority status,
  - quarantine eligibility or quarantine failure,
  - capability-mount degradation,
  - startup wiring status,
  - and launcher readiness derived from the real DB/runtime checks.
- This job is planning only. No code edits, bug-state mutation, or relaunch work belongs here.

## Files To Read
- `AGENTS.md`
- `docs/ARCHITECTURE.md`
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/packets/wave-7-manifest-contract-readiness-1.queue.json`
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/coordination.json`
- `Code&DBs/Workflow/runtime/primitive_contracts.py`
- `Code&DBs/Workflow/runtime/spec_compiler.py`
- `Code&DBs/Workflow/runtime/admission_repair.py`
- `Code&DBs/Workflow/runtime/workflow/pipeline_eval.py`
- `Code&DBs/Workflow/runtime/workflow/_claiming.py`
- `Code&DBs/Workflow/runtime/workflow/_status.py`
- `Code&DBs/Workflow/runtime/operation_catalog.py`
- `Code&DBs/Workflow/runtime/route_outcomes.py`
- `Code&DBs/Workflow/runtime/setup_wizard.py`
- `Code&DBs/Workflow/surfaces/api/rest.py`
- `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py`
- `Code&DBs/Workflow/surfaces/api/handlers/workflow_query_core.py`
- `Code&DBs/Workflow/surfaces/api/handlers/workflow_launcher.py`
- `Code&DBs/Workflow/tests/unit/test_startup_wiring.py`
- `Code&DBs/Workflow/tests/unit/test_api_rest_startup.py`
- `Code&DBs/Workflow/tests/unit/test_workflow_staleness_query_authority.py`
- `Code&DBs/Workflow/tests/unit/test_verifier_authority.py`
- `Code&DBs/Workflow/tests/unit/test_claim_route_block_fail_closed.py`
- `Code&DBs/Workflow/tests/unit/test_operation_catalog_mounting.py`
- `Code&DBs/Workflow/tests/integration/test_route_catalog_repository.py`
- `Code&DBs/Workflow/tests/integration/test_provider_route_health_budget_schema.py`
- `Code&DBs/Workflow/tests/integration/test_native_operator_query_surface.py`

## Files Allowed To Change
- Only this file:
  - `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-7-manifest-contract-readiness-1/PLAN.md`
- No runtime, surface, test, schema, or workflow metadata files may be changed in this job.

## Verification Path
- This packet does not implement behavior. Its job is to define the narrow proof path for the later packet.
- The later execution packet should prove the manifest contract by reading the DB-backed authority chain and checking that startup/health no longer treat the manifest as “good” unless the generated authority and expected-object contract agree.
- The later execution packet should prove the quarantine gate by showing stale packet fleets are either quarantined or explicitly prevented from retrying without fresh authority, and that the status surface names the quarantine reason rather than silently reusing the old fleet.
- The later execution packet should prove verifier selection is scope-backed by catalog rows, not Python-extension heuristics, and that unsupported scope is surfaced as a controlled failure or explicit gap.
- The later execution packet should prove startup truth is not reduced to `booted: true`; it must expose whether auto wiring ran, whether any registry/catalog step was skipped, and whether the system is actually ready for a relaunch.
- The later execution packet should prove route/catalog discovery reports capability-mount degradation and partial-mount state explicitly, so a partially mounted API cannot be treated as canonical.
- Narrow verifier candidates for the later packet should include:
  - `Code&DBs/Workflow/tests/unit/test_startup_wiring.py`
  - `Code&DBs/Workflow/tests/unit/test_api_rest_startup.py`
  - `Code&DBs/Workflow/tests/unit/test_workflow_staleness_query_authority.py`
  - `Code&DBs/Workflow/tests/unit/test_verifier_authority.py`
  - `Code&DBs/Workflow/tests/unit/test_claim_route_block_fail_closed.py`
  - `Code&DBs/Workflow/tests/unit/test_operation_catalog_mounting.py`
  - `Code&DBs/Workflow/tests/integration/test_route_catalog_repository.py`

## Stop Boundary
- Stop after writing this plan.
- Do not edit runtime, surfaces, tests, schema, or workflow queue files in this job.
- Do not resolve any bug, attach evidence, or claim readiness from this packet.
- If the manifest authority, quarantine gate, or readiness truth cannot be described from the files above, stop and reopen discovery rather than inventing policy.
- If the later implementation needs a broader refactor than the files above, halt before crossing into unrelated surfaces.

## Per-Bug Intended Outcome
- `BUG-62F78235`: stale workflow packet fleets must not remain retryable by default. They should be quarantined or forced back through a fresh authority path so relaunch never reuses an unsafe fleet.
- `BUG-8F6A612A`: verifier selection must be catalog-backed by scope and authority rows, not Python-only extension rules. The packet should end with a scope-aware verifier decision that is explainable from the catalog.
- `BUG-123C17AC`: startup boot must not claim success just because the object says `booted: true`. Readiness must include whether auto wiring was actually enabled, which registry/catalog steps ran or skipped, and whether the surface is genuinely relaunch-safe.
- `BUG-9D09F47D`: route and catalog discovery must surface capability-mount degradation and partial API state explicitly, and partial mount must be treated as degraded authority rather than canonical readiness.
