# Phase 2 Control Plane Core

Status: execution_ready

Authority map:
- `planning/phase-program/praxis_0_100_registry.json` phase `2` title = `Control Plane Core`
- `planning/phase-program/praxis_0_100_registry.json` phase `2` status = `historical_foundation`
- `planning/phase-program/praxis_0_100_registry.json` phase `2` predecessor = `1`
- `planning/phase-program/praxis_0_100_registry.json` governance requires closeout sequence `review -> healer -> human_approval`
- `config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json` job `phase_002_control_plane_core` requires one bounded execution packet with explicit files and verification

Execution environment note:
- repo evidence was gathered from the mounted checkout at `/workspace`
- the platform-context repo path `/Users/nate/Praxis` is not present in this execution environment, so verification commands below use `/workspace`

## 1. Objective in repo terms

Pin Phase 2 to one durable repo seam only:
- native frontdoor submit/status over the canonical Postgres control-plane schema

In current repo terms, the control-plane core is the minimum path that:
- accepts a `WorkflowRequest` through `Code&DBs/Workflow/surfaces/api/frontdoor.py`
- persists the admitted definition, admission decision, and run identity through `Code&DBs/Workflow/storage/postgres/admission.py`
- reads the same run back from `workflow_runs` through the frontdoor status surface

This phase is about one bounded spine only:
- `workflow_definitions`
- `admission_decisions`
- `workflow_runs`

This phase is not:
- execution evidence completion
- workflow outbox rollout
- registry authority storage design
- operator cockpit read models
- packet-inspection evolution
- command-bus convergence

## 2. Current evidence in the repo

- `planning/phase-program/praxis_0_100_registry.json` declares Phase `2` as `Control Plane Core` with mandatory closeout `review -> healer -> human_approval`
- `Code&DBs/Databases/migrations/workflow/001_v1_control_plane.sql` already defines the foundational control-plane tables:
- `workflow_definitions`
- `workflow_definition_nodes`
- `workflow_definition_edges`
- `admission_decisions`
- `workflow_runs`
- `run_nodes`
- `run_edges`
- `Code&DBs/Workflow/storage/postgres/admission.py` already implements the canonical Phase 2 write primitives:
- `WorkflowAdmissionDecisionWrite`
- `WorkflowRunWrite`
- `WorkflowAdmissionSubmission`
- `persist_workflow_admission(...)`
- `Code&DBs/Workflow/storage/postgres/__init__.py` exports `bootstrap_control_plane_schema`, `connect_workflow_database`, and `persist_workflow_admission`, which means Phase 2 already has an explicit bootstrap and persistence boundary
- `Code&DBs/Workflow/surfaces/api/frontdoor.py` already exposes the narrow native control-plane surface:
- `submit(...)` resolves the native instance, plans intake, bootstraps the schema, and persists a `WorkflowAdmissionSubmission`
- `status(...)` reads the durable `workflow_runs` row and shapes a compact response
- `health(...)` reports repo-local Postgres reachability and schema bootstrap state
- `Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py` already proves the storage layer can write canonical decision, run, and definition rows against real Postgres
- `Code&DBs/Workflow/tests/integration/test_native_frontdoor.py` currently proves frontdoor request shaping and status behavior mostly through fake connections and monkeypatched persistence seams
- `Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py` already proves a real Postgres-backed `NativeWorkflowFrontdoor.submit(...)` and `status(...)` path, but it is bundled with later-phase concerns:
- registry authority bootstrap
- workflow outbox bootstrap
- runtime execution
- evidence writing and inspection
- broader self-hosted smoke assertions
- broader integration coverage still frequently seeds `workflow_definitions`, `admission_decisions`, and `workflow_runs` directly in test fixtures, which means the dedicated public Phase 2 seam is still under-specified as a standalone contract

## 3. Gap or ambiguity still remaining

The repo no longer has a total absence of live Phase 2 proof. The remaining gap is narrower:
- there is no focused Phase 2-only integration contract for `NativeWorkflowFrontdoor.submit(...) -> durable control-plane rows -> NativeWorkflowFrontdoor.status(...)`

Current proof shape is split three ways:
- low-level persistence is covered in `test_postgres_runtime_path.py`
- frontdoor behavior is covered with fakes in `test_native_frontdoor.py`
- one real frontdoor proof exists in `test_native_self_hosted_smoke.py`, but it is mixed with registry, outbox, execution, and evidence concerns that belong to later phases

That leaves one execution ambiguity for future LLM work:
- what is the smallest authoritative contract that proves Phase 2 itself is intact, without borrowing confidence from broader smoke coverage?

The first sprint should remove that ambiguity by extracting one focused Phase 2 contract, not by redesigning the control plane.

## 4. One bounded first sprint only

Sprint label:
- add a dedicated real-Postgres native frontdoor control-plane round-trip contract

Sprint outcome:
- one focused integration proof shows the public frontdoor can create and read back the canonical Phase 2 run identity without depending on later-phase bootstrap or execution surfaces

Sprint tasks:
1. Add one real Postgres-backed integration case to `Code&DBs/Workflow/tests/integration/test_native_frontdoor.py`
2. Reuse an in-memory `RegistryResolver` for workspace/runtime-profile authority so the test does not need Postgres registry bootstrap
3. Use the real `connect_workflow_database`, `bootstrap_control_plane_schema`, and `persist_workflow_admission` path through `NativeWorkflowFrontdoor.submit(...)`
4. Call `NativeWorkflowFrontdoor.status(...)` for the created run
5. Assert the minimum durable identity fields from the frontdoor response and from direct row reads:
- `run_id`
- `workflow_id`
- `request_id`
- `workflow_definition_id`
- `admitted_definition_hash`
- `current_state`
- `admission_decision_id`
6. If the test exposes a defect, fix only the narrow handoff between frontdoor intake shaping, schema bootstrap, persistence invocation, and status row serialization

Stop boundary:
- stop once there is one focused Phase 2 contract that passes alongside the existing storage and smoke proofs

Explicitly not in this sprint:
- adding migrations
- changing `001_v1_control_plane.sql`
- bootstrapping registry authority tables in the new test
- running deterministic execution after submit
- asserting outbox, receipts, or inspection completeness
- packet-inspection redesign
- command-bus, operator, or review-surface work

## 5. Exact file or subsystem scope

Read scope:
- `planning/phase-program/praxis_0_100_registry.json`
- `config/cascade/specs/W_phase_001_010_foundation_wave_20260414.queue.json`
- `Code&DBs/Databases/migrations/workflow/001_v1_control_plane.sql`
- `Code&DBs/Workflow/surfaces/api/frontdoor.py`
- `Code&DBs/Workflow/storage/postgres/admission.py`
- `Code&DBs/Workflow/storage/postgres/__init__.py`
- `Code&DBs/Workflow/runtime/instance.py`
- `config/runtime_profiles.json`
- `Code&DBs/Workflow/tests/integration/test_native_frontdoor.py`
- `Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py`
- `Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py`

Write scope:
- `Code&DBs/Workflow/tests/integration/test_native_frontdoor.py`
- `Code&DBs/Workflow/surfaces/api/frontdoor.py` only if required to make the focused contract pass

Subsystem boundary:
- native frontdoor submit/status
- Postgres control-plane bootstrap and admission persistence
- canonical run identity row load from `workflow_runs`

Out of scope:
- `Code&DBs/Workflow/registry/**`
- `Code&DBs/Workflow/runtime/outbox.py`
- `Code&DBs/Workflow/runtime/persistent_evidence.py`
- `Code&DBs/Workflow/runtime/execution.py`
- `Code&DBs/Workflow/surfaces/api/rest.py`
- `Code&DBs/Workflow/surfaces/api/handlers/**`
- any migration beyond read-only grounding

## 6. Done criteria

- `test_native_frontdoor.py` contains one real Postgres-backed contract for `submit(...) -> status(...)`
- the new test uses real control-plane bootstrap and persistence, not a fake connection or fake persist function for the write path
- the new test keeps registry authority in-memory so the assertion stays Phase 2-scoped
- the test proves the durable identity fields match across:
- frontdoor submit response
- frontdoor status response
- direct `workflow_runs` row read
- direct `admission_decisions` row read
- existing `test_postgres_runtime_path.py` still passes
- existing `test_native_self_hosted_smoke.py` still passes
- no new migration, schema redesign, or later-phase bootstrap requirement is introduced

## 7. Verification commands

Run from the mounted repo root:

```bash
cd /workspace
export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'
export PYTHONPATH='/workspace/Code&DBs/Workflow'
python -m pytest '/workspace/Code&DBs/Workflow/tests/integration/test_native_frontdoor.py' -q
python -m pytest '/workspace/Code&DBs/Workflow/tests/integration/test_postgres_runtime_path.py' -q
python -m pytest '/workspace/Code&DBs/Workflow/tests/integration/test_native_self_hosted_smoke.py' -q
rg -n 'bootstrap_control_plane_schema|persist_workflow_admission|FROM workflow_runs' '/workspace/Code&DBs/Workflow/surfaces/api/frontdoor.py'
```

Expected verification result:
- the focused frontdoor integration proves the Phase 2 durable submit/status seam without later-phase scaffolding
- the storage-layer contract still proves canonical row persistence
- the broader self-hosted smoke still passes as a higher-level superset proof

## 8. Review -> healer -> human approval gate

Review:
- confirm the packet stays inside the Phase 2 control-plane core seam
- confirm the proposed sprint extracts a smaller authoritative contract instead of duplicating the broad smoke test
- confirm the new proof uses real Postgres for the write path
- confirm registry, outbox, execution, and evidence concerns stay out of the new test except for minimal read-only context
- confirm write scope remains limited to the frontdoor test and frontdoor surface only if needed

Healer:
- if review finds drift or undercoverage, repair only:
- `Code&DBs/Workflow/tests/integration/test_native_frontdoor.py`
- `Code&DBs/Workflow/surfaces/api/frontdoor.py`
- rerun all verification commands

Human approval gate:
- require explicit human approval after review and any healer pass
- do not open a second Phase 2 sprint before approval is recorded
- after approval, the next adjacent seam can move into command surfaces or richer status/read authority, but not as part of this first sprint
