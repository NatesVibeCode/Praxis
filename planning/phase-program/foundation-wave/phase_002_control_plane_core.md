# Phase 2 Control Plane Core

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `2` (`Control Plane Core`), status `historical_foundation`, predecessor phase `1`, with mandatory closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is based on the current checked-out repo snapshot in this workspace. In current repo terms, the most defensible Phase 2 seam is the native frontdoor submit/status spine that sits directly on top of the original control-plane schema from migration `001_v1_control_plane.sql`.

## 1. Objective in repo terms

- Reassert one explicit Phase 2 control-plane core seam in the current repo: the repo-local native frontdoor that admits a workflow request into canonical control-plane tables and reads the same run back through the frontdoor status surface.
- Keep the sprint bounded to the initial durable control-plane spine:
- `workflow_definitions`
- `admission_decisions`
- `workflow_runs`
- First-sprint target: prove that [Code&DBs/Workflow/surfaces/api/frontdoor.py](/workspace/Code&DBs/Workflow/surfaces/api/frontdoor.py) can perform one real submit -> persist -> status round-trip against Postgres using the canonical native instance contract, without relying on stubbed persistence or direct SQL fixture seeding in the test itself.

## 2. Current evidence in the repo

- Phase `2` is declared in the registry as `Control Plane Core` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json), with predecessor `1` and required closeout sequence `review -> healer -> human_approval`.
- The original control-plane schema for this phase already exists in [Code&DBs/Databases/migrations/workflow/001_v1_control_plane.sql](/workspace/Code&DBs/Databases/migrations/workflow/001_v1_control_plane.sql). It creates the foundational tables:
- `workflow_definitions`
- `workflow_definition_nodes`
- `workflow_definition_edges`
- `admission_decisions`
- `workflow_runs`
- `run_nodes`
- `run_edges`
- The checked-in storage layer already exposes explicit Phase 2 write authority through [Code&DBs/Workflow/storage/postgres/admission.py](/workspace/Code&DBs/Workflow/storage/postgres/admission.py):
- `WorkflowAdmissionDecisionWrite`
- `WorkflowRunWrite`
- `WorkflowAdmissionSubmission`
- `persist_workflow_admission(...)`
- The storage package already treats schema bootstrap as an explicit control-plane concern in [Code&DBs/Workflow/storage/postgres/__init__.py](/workspace/Code&DBs/Workflow/storage/postgres/__init__.py) by exporting `bootstrap_control_plane_schema`, `connect_workflow_database`, and `persist_workflow_admission`.
- The native frontdoor surface already exists in [Code&DBs/Workflow/surfaces/api/frontdoor.py](/workspace/Code&DBs/Workflow/surfaces/api/frontdoor.py) and is intentionally narrow:
- `submit(...)` resolves the native repo-local instance, plans intake, builds `WorkflowAdmissionSubmission`, bootstraps schema, and persists the submission
- `status(...)` reads the durable `workflow_runs` row and derives compact observability payloads
- `health(...)` resolves repo-local instance plus Postgres health/bootstrap status
- Repo-local native boundary enforcement already exists in [Code&DBs/Workflow/runtime/instance.py](/workspace/Code&DBs/Workflow/runtime/instance.py) and [config/runtime_profiles.json](/workspace/config/runtime_profiles.json), which means the Phase 2 seam can be exercised without inventing a new workspace or deployment model.
- Integration coverage already proves the persistence layer and frontdoor in pieces, but not yet as one durable seam:
- [Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py) proves `persist_workflow_admission(...)` writes canonical control-plane rows and rejects malformed or conflicting inputs
- [Code&DBs/Workflow/tests/integration/test_native_frontdoor.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_frontdoor.py) proves frontdoor request shaping, repo-local native-instance resolution, status serialization, packet-inspection fallback behavior, and observability shaping mostly through fake/stub connections
- Unit/frontdoor-adjacent surfaces already depend on this core behaving coherently:
- [Code&DBs/Workflow/tests/unit/test_rest_queue_submit.py](/workspace/Code&DBs/Workflow/tests/unit/test_rest_queue_submit.py)
- [Code&DBs/Workflow/tests/unit/test_cli_workflow_frontdoors.py](/workspace/Code&DBs/Workflow/tests/unit/test_cli_workflow_frontdoors.py)
- [Code&DBs/Workflow/tests/unit/test_workflow_command_bus_authority.py](/workspace/Code&DBs/Workflow/tests/unit/test_workflow_command_bus_authority.py)
- Current repo evidence also shows that many broader integration tests still seed `workflow_definitions`, `admission_decisions`, and `workflow_runs` directly with SQL fixtures instead of proving the public Phase 2 frontdoor is the seam that creates the initial durable run identity.

## 3. Gap or ambiguity still remaining

- The repo already has a canonical Phase 2 schema and a canonical Phase 2 write primitive, but it does not yet have one focused integration proof that the public native frontdoor itself can round-trip a real request through that control-plane core.
- Today the proof splits are separate:
- storage integration proves `persist_workflow_admission(...)`
- frontdoor integration proves request shaping and status payloads through stubs/fakes
- That leaves an ambiguity about the true live seam for Phase 2:
- Is `NativeWorkflowFrontdoor.submit(...)` actually the durable owner of initial run creation in repo-local native mode?
- Or is the current confidence mostly assembled from lower-level persistence tests plus higher-level mocked frontdoor tests?
- The first sprint should remove that ambiguity with one real repo-grounded round-trip contract, not broaden into command-bus policy, operator read surfaces, workflow submission review, or observability redesign.
- `status(...)` currently includes packet-inspection and derived observability behavior, but those are adjacent layers, not the core Phase 2 objective. The first sprint should prove the frontdoor can read back the canonical run row and stable identity fields; it should not try to solve every later read-model concern.

## 4. One bounded first sprint only

- Add one integration contract that uses the real native frontdoor over a real Postgres connection and proves:
- `submit(...)` bootstraps the control-plane schema if needed
- `submit(...)` persists one canonical admission decision and one canonical workflow run
- `status(...)` can read back the same run through the frontdoor surface with stable identity fields from durable storage
- Prefer reusing the existing request-builder patterns from [Code&DBs/Workflow/tests/integration/test_native_frontdoor.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_frontdoor.py) and the Postgres helpers from [Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py).
- If the new integration test exposes a defect, fix only the narrow handoff between:
- frontdoor request/intake shaping
- `WorkflowAdmissionSubmission` construction
- schema bootstrap / persistence invocation
- frontdoor status row loading / stable field serialization
- Stop after one end-to-end Phase 2 proof exists and passes.
- Do not widen into:
- service-bus command routing
- CLI or MCP submit/retry/cancel unification
- packet-inspection redesign
- observability taxonomy redesign
- operator-control tables
- submission review policy

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/surfaces/api/frontdoor.py](/workspace/Code&DBs/Workflow/surfaces/api/frontdoor.py)
- [Code&DBs/Workflow/tests/integration/test_native_frontdoor.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_frontdoor.py)
- Primary supporting authority scope:
- [Code&DBs/Workflow/storage/postgres/admission.py](/workspace/Code&DBs/Workflow/storage/postgres/admission.py)
- [Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py](/workspace/Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py)
- Read-only grounding references:
- [Code&DBs/Databases/migrations/workflow/001_v1_control_plane.sql](/workspace/Code&DBs/Databases/migrations/workflow/001_v1_control_plane.sql)
- [Code&DBs/Workflow/runtime/instance.py](/workspace/Code&DBs/Workflow/runtime/instance.py)
- [config/runtime_profiles.json](/workspace/config/runtime_profiles.json)
- [Code&DBs/Workflow/observability/status_observability.py](/workspace/Code&DBs/Workflow/observability/status_observability.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/control_commands.py](/workspace/Code&DBs/Workflow/runtime/control_commands.py)
- [Code&DBs/Workflow/surfaces/api/rest.py](/workspace/Code&DBs/Workflow/surfaces/api/rest.py)
- [Code&DBs/Workflow/surfaces/api/handlers/workflow_run.py](/workspace/Code&DBs/Workflow/surfaces/api/handlers/workflow_run.py)
- [Code&DBs/Workflow/surfaces/api/workflow_submission.py](/workspace/Code&DBs/Workflow/surfaces/api/workflow_submission.py)
- [Code&DBs/Workflow/storage/postgres/workflow_submission_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/workflow_submission_repository.py)
- command-bus retry/cancel policy
- packet-inspection schema evolution
- broad removal of direct SQL fixture seeding from unrelated tests

## 6. Done criteria

- A focused integration test proves one real `NativeWorkflowFrontdoor.submit(...)` -> Postgres persistence -> `NativeWorkflowFrontdoor.status(...)` round-trip using the repo-local native instance contract.
- The test asserts stable control-plane identity fields at minimum:
- `run_id`
- `workflow_id`
- `request_id`
- `workflow_definition_id`
- `admitted_definition_hash`
- `current_state`
- `admission_decision_id`
- The new proof depends on canonical storage/bootstrap helpers rather than a fake connection for the submit path.
- Existing frontdoor integration tests still pass, including legacy packet-inspection fallback behavior.
- Existing persistence integration tests still pass, including malformed-submission and conflict rejection.
- No new migration is added and migration `001_v1_control_plane.sql` is not edited in this sprint.
- No command-bus, operator-control, or submission-review surfaces are changed as collateral work.

## 7. Verification commands

- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='/Users/nate/Praxis/Code&DBs/Workflow'`
- `cd /Users/nate/Praxis`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_native_frontdoor.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_postgres_runtime_path.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/unit/test_cli_workflow_frontdoors.py Code\&DBs/Workflow/tests/unit/test_rest_queue_submit.py -q`
- `rg -n "persist_workflow_admission|bootstrap_control_plane_schema|FROM workflow_runs" Code\&DBs/Workflow/surfaces/api/frontdoor.py`

Expected verification outcome:

- the native frontdoor passes with one real Postgres-backed admission/status proof
- the canonical persistence layer still enforces control-plane invariants
- frontdoor code still clearly depends on explicit control-plane bootstrap and persistence seams

## 8. Review -> healer -> human approval gate

- Review:
- confirm the packet stays on the Phase 2 control-plane core seam and does not drift into later command-bus or operator-control phases
- confirm the new proof is genuinely end-to-end for the native frontdoor submit/status path and does not reintroduce a fake connection for the write path
- confirm the asserted fields are core control-plane identity fields, not a grab bag of later observability extras
- confirm no out-of-scope files were changed
- Healer:
- if review finds frontdoor drift, brittle proof shape, or accidental expansion into later surfaces, repair only:
- [Code&DBs/Workflow/surfaces/api/frontdoor.py](/workspace/Code&DBs/Workflow/surfaces/api/frontdoor.py)
- [Code&DBs/Workflow/tests/integration/test_native_frontdoor.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_frontdoor.py)
- rerun all verification commands
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 2 sprint
- the next Phase 2 sprint, if approved later, should take exactly one adjacent seam only, most likely command-surface convergence or status-read authority cleanup, not “finish the whole control plane” in one pass
