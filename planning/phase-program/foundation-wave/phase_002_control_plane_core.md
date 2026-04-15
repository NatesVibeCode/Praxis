# Phase 2 Control Plane Core

Status: execution_ready

Authority map:
- `planning/phase-program/praxis_0_100_registry.json` declares phase `2` as `Control Plane Core`.
- The registry marks phase `2` as predecessor-bound to phase `1`.
- Registry governance requires `one_phase_one_thing = true`.
- Registry governance requires the closeout sequence `review -> healer -> human_approval`.
- `config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json` requires one bounded execution packet for `phase_002_control_plane_core`.

Grounding note:
- This packet is grounded in the mounted repo snapshot at `/workspace`.
- The declared execution root is `/Users/nate/Praxis`; verification commands target that root exactly.
- The execution shard says compile inputs are ready, but proof coverage is still effectively unproven: `verification_coverage=0.0`, `fully_proved_verification_coverage=0.0`, `write_manifest_coverage=0.2337`.
- The repo already contains the schema, storage writer, native frontdoor, and split tests for this phase. The first sprint is therefore a proof-and-repair sprint, not a redesign sprint.

## 1. Objective in repo terms

- Prove that the repo-local native control-plane frontdoor is a thin wrapper over the canonical Postgres control-plane core.
- The bounded seam is the real `submit(...) -> durable Postgres control-plane write -> status(...)` path in:
- `Code&DBs/Workflow/surfaces/api/frontdoor.py`
- `Code&DBs/Workflow/storage/postgres/admission.py`
- `Code&DBs/Workflow/storage/postgres/__init__.py`
- `Code&DBs/Workflow/runtime/intake.py`
- Repo-level target: a real native submit call persists canonical `workflow_definitions`, `admission_decisions`, and `workflow_runs` rows through `persist_workflow_admission(...)`, and a follow-up native status call reads the durable run back from the same control-plane path.
- Stop at Phase 2 control-plane admission/status truth. Do not widen into command-bus convergence, later lifecycle orchestration, operator-control surfaces, or schema expansion.

## 2. Current evidence in the repo

- `planning/phase-program/praxis_0_100_registry.json` defines phase `2` as `Control Plane Core` and requires closeout sequence `review -> healer -> human_approval`.
- `config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json` assigns this exact packet path to job label `phase_002_control_plane_core`.
- `Code&DBs/Databases/migrations/workflow/001_v1_control_plane.sql` already defines the core Phase 2 tables, including:
- `workflow_definitions`
- `workflow_definition_nodes`
- `workflow_definition_edges`
- `admission_decisions`
- `workflow_runs`
- `Code&DBs/Workflow/storage/postgres/__init__.py` exports the canonical control-plane storage surface used by the native frontdoor:
- `connect_workflow_database`
- `bootstrap_control_plane_schema`
- `persist_workflow_admission`
- `Code&DBs/Workflow/storage/postgres/admission.py` already owns the transactional write contract through:
- `WorkflowAdmissionDecisionWrite`
- `WorkflowRunWrite`
- `WorkflowAdmissionSubmission`
- `WorkflowAdmissionWriteResult`
- `persist_workflow_admission(...)`
- `Code&DBs/Workflow/surfaces/api/frontdoor.py` already implements the intended shape of the seam:
- `NativeWorkflowFrontdoor.submit(...)` resolves the native instance, plans intake with `WorkflowIntakePlanner`, builds `WorkflowAdmissionSubmission`, and delegates to `_submit_submission(...)`
- `_submit_submission(...)` uses `connect_workflow_database`, `bootstrap_control_plane_schema`, and `persist_workflow_admission`
- `NativeWorkflowFrontdoor.status(...)` reads `workflow_runs` through the same connection surface and serializes the durable run payload
- The public module-level `submit(...)` and `status` exports in `frontdoor.py` point to the real native frontdoor rather than a compatibility wrapper
- `Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py` already proves the storage-owned write seam directly:
- `test_postgres_control_plane_path_writes_a_run_and_decision`
- `test_postgres_control_plane_path_rejects_conflicting_preseeded_definition_rows`
- `test_postgres_control_plane_path_rejects_malformed_child_rows`
- `Code&DBs/Workflow/tests/integration/test_native_frontdoor.py` already proves frontdoor behavior such as status serialization and native instance enforcement, but its submit-path tests use injected fakes for database connection, schema bootstrap, and persistence instead of the real Postgres path.
- `Code&DBs/Workflow/runtime/instance.py` already provides the repo-local native instance boundary that Phase 2 submit/status depends on.

## 3. Gap or ambiguity still remaining

- The repo currently proves the storage writer in one suite and the frontdoor surface shape in another suite, but it does not yet prove the exact Phase 2 vertical slice end to end.
- The unresolved ambiguity is narrow:
- whether the public native `submit(...)` path actually drives the canonical Postgres control-plane writer in live repo terms
- whether `status(...)` round-trips the same durable run identity from the same real persisted row rather than from fake test seams
- Because the frontdoor tests use injected fakes, the current test matrix could still pass while the real `submit -> bootstrap -> persist -> status` handoff is broken.
- Phase 2 does not need a new architecture. It needs one live proof that removes this ambiguity and any minimal repair needed to make that proof true.

## 4. One bounded first sprint only

- Add one focused Postgres-backed integration proof for the native frontdoor vertical slice.
- Preferred implementation shape:
- extend `Code&DBs/Workflow/tests/integration/test_native_frontdoor.py`
- reuse request-payload builders already present there
- reuse durable-row assertion patterns already present in `Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py`
- The sprint proves exactly these behaviors:
- `submit(...)` through `Code&DBs/Workflow/surfaces/api/frontdoor.py` persists one admitted workflow definition, one admission decision, and one workflow run through the real Postgres writer
- `status(...)` for the resulting `run_id` returns the same durable identity fields from storage
- the proof uses a real `WORKFLOW_DATABASE_URL`, not fake persistence hooks or in-memory stand-ins
- Allow repair only if the proof exposes a real defect in:
- request-to-submission mapping
- `NativeWorkflowFrontdoor._submit_submission(...)`
- durable run-row loading inside `NativeWorkflowFrontdoor.status(...)`
- Stop after one vertical slice is proved. Explicitly out of scope:
- `rest.py` command-bus surfaces
- workflow job lifecycle or execution packet lifecycle behavior
- operator-control or promotion/gate tables
- new migrations
- broad fixture cleanup
- replacing unrelated frontdoor tests

## 5. Exact file or subsystem scope

- Primary implementation scope:
- `Code&DBs/Workflow/tests/integration/test_native_frontdoor.py`
- Read-only authority and proof references:
- `planning/phase-program/praxis_0_100_registry.json`
- `config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json`
- `Code&DBs/Databases/migrations/workflow/001_v1_control_plane.sql`
- `Code&DBs/Workflow/surfaces/api/frontdoor.py`
- `Code&DBs/Workflow/storage/postgres/__init__.py`
- `Code&DBs/Workflow/storage/postgres/admission.py`
- `Code&DBs/Workflow/runtime/intake.py`
- `Code&DBs/Workflow/runtime/instance.py`
- `Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py`
- Repair-only scope if the new proof finds a defect:
- `Code&DBs/Workflow/surfaces/api/frontdoor.py`
- `Code&DBs/Workflow/storage/postgres/admission.py`
- `Code&DBs/Workflow/storage/postgres/__init__.py` only if the defect is at the exported control-plane seam
- Subsystem boundary:
- native repo-local submit/status surface over the canonical Postgres control-plane admission core
- Explicitly out of scope:
- `Code&DBs/Workflow/surfaces/api/rest.py`
- `Code&DBs/Workflow/surfaces/api/handlers/`
- `Code&DBs/Workflow/runtime/command_handlers.py`
- `Code&DBs/Workflow/storage/postgres/workflow_submission_repository.py`
- any migration after `001_v1_control_plane.sql`
- any packet outside `planning/phase-program/foundation-wave/phase_002_control_plane_core.md`

## 6. Done criteria

- One real Postgres-backed integration proof exists for `submit(...) -> durable write -> status(...)` through `Code&DBs/Workflow/surfaces/api/frontdoor.py`.
- The proof uses the actual `connect_workflow_database`, `bootstrap_control_plane_schema`, and `persist_workflow_admission` path, not test doubles.
- The proof confirms durable rows exist for:
- `workflow_definitions`
- `admission_decisions`
- `workflow_runs`
- The proof asserts stable persisted identity fields across submit and status, at minimum:
- `run_id`
- `workflow_id`
- `request_id`
- `workflow_definition_id`
- `admitted_definition_hash`
- `admission_decision_id`
- `current_state`
- Existing storage-path proofs in `Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py` still pass.
- Existing native frontdoor proofs unrelated to this vertical slice still pass.
- No migration file is added or modified.
- No later-phase control-plane concerns are pulled into the sprint.

## 7. Verification commands

- `cd /Users/nate/Praxis`
- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='Code&DBs/Workflow'`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_native_frontdoor.py' -q`
- `python -m pytest 'Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py' -q`
- `rg -n "class NativeWorkflowFrontdoor|def submit\\(|def status\\(|connect_workflow_database|bootstrap_control_plane_schema|persist_workflow_admission" 'Code&DBs/Workflow/surfaces/api/frontdoor.py'`
- `rg -n "test_postgres_control_plane_path_writes_a_run_and_decision|test_postgres_control_plane_path_rejects_conflicting_preseeded_definition_rows|test_postgres_control_plane_path_rejects_malformed_child_rows" 'Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py'`

Expected verification outcome:

- the native frontdoor has one direct Postgres-backed proof instead of only fake-hook submit tests
- the control-plane transaction boundary remains owned by storage
- status reads back the same durable run identity that submit wrote

## 8. Review -> healer -> human approval gate

- Review:
- confirm the new proof hits the real native `frontdoor.submit(...)` and `frontdoor.status(...)` seam
- confirm the test uses a real Postgres connection and does not smuggle fake persistence through monkeypatched helpers
- confirm assertions cover durable row existence and identity round-trip, not only returned JSON shape
- confirm the sprint stayed inside the Phase 2 seam and did not widen into command-bus, handler, or migration work
- Healer:
- if review finds handoff drift, persistence drift, or brittle proof structure, repair only the scoped files above
- do not widen healer work into schema redesign, workflow lifecycle work, or unrelated frontdoor cleanup
- rerun the full verification command set after any repair
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 2 sprint
- if a later Phase 2 sprint is approved, it must target one adjacent seam only, not “finish the control plane”
