# Phase 8 Workflow Class and Schedule Schema

Status: execution_ready

Registry authority: `planning/phase-program/praxis_0_100_registry.json` phase `8` (`Workflow Class and Schedule Schema`)

## 1. Objective in repo terms

Make Phase 8 read authority converge on one repo-native path for active workflow classes and recurring schedules that come from:

- `workflow_classes`
- `schedule_definitions`
- `recurring_run_windows`

In the current repo, those tables already exist and already have a canonical composed authority layer. The execution objective is not to redesign the schema. It is to stop adding or keeping parallel read models that reinterpret the same Phase 8 tables in runtime code.

For the first bounded sprint, the concrete repo goal is narrower:

- move `Code&DBs/Workflow/runtime/native_scheduler.py` off its private direct-SQL read model
- make `Code&DBs/Workflow/surfaces/api/native_scheduler.py` resolve schedule inspection through the canonical Phase 8 authority stack
- preserve the current frontdoor payload contract unless a test-backed change is required

## 2. Current evidence in the repo

- `Code&DBs/Databases/migrations/workflow/008_workflow_class_and_schedule_schema.sql` is the canonical schema origin for:
- `workflow_classes`
- `schedule_definitions`
- `recurring_run_windows`
- `planning/phase-program/praxis_0_100_registry.json` marks Phase 8 as `historical_foundation` and requires the closeout sequence `review -> healer -> human_approval`.
- `Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py` proves migration `008_workflow_class_and_schedule_schema.sql` is still present in the canonical manifest and that expected objects are declared.
- `Code&DBs/Workflow/policy/workflow_classes.py` already defines the canonical workflow-class authority records, catalog, and Postgres repository.
- `Code&DBs/Workflow/authority/workflow_schedule.py` already defines the composed schedule authority that resolves a schedule plus its workflow class plus its active recurring run window.
- `Code&DBs/Workflow/storage/postgres/workflow_schedule_repository.py` already loads that composed authority from Postgres in one repository.
- `Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py` already proves the composed repository can load and resolve workflow classes, schedules, and recurring windows together in a deterministic, fail-closed way.
- `Code&DBs/Workflow/tests/integration/test_scheduler_window_authority_repository.py` already proves active schedule/window authority against a real Postgres database.
- `Code&DBs/Workflow/runtime/native_scheduler.py` still keeps its own local `_SCHEDULE_QUERY`, `_WORKFLOW_CLASS_QUERY`, row validators, record types, and `PostgresNativeSchedulerRepository`.
- `Code&DBs/Workflow/surfaces/api/native_scheduler.py` still defaults to that duplicate repository path through `PostgresNativeSchedulerRepository`.
- `Code&DBs/Workflow/runtime/scheduler_window_repository.py` is another live duplicate schedule/window read model in the repo, and `Code&DBs/Workflow/runtime/default_path_pilot.py` currently reports schedule authority as `runtime.scheduler_window_repository`.

## 3. Gap or ambiguity still remaining

- The Phase 8 schema is not missing. The ambiguity is ownership of read semantics over that schema.
- The repo currently has one canonical composed path:
- `policy.workflow_classes`
- `authority.workflow_schedule`
- `storage.postgres.workflow_schedule_repository`
- The repo also has at least two parallel runtime read paths:
- `runtime.native_scheduler`
- `runtime.scheduler_window_repository`
- Because those runtime paths redefine SQL, row parsing, ambiguity handling, and authority labels locally, they can drift from the canonical path without any migration change.
- The broader repo-wide ambiguity is larger than one sprint. `runtime.scheduler_window_repository.py` and its `default_path_pilot.py` consumers should not be silently treated as solved if the first sprint only converges `native_scheduler.py`.

## 4. One bounded first sprint only

Replace the `native_scheduler` direct-SQL inspection path with a thin adapter over the existing Phase 8 canonical authority stack.

Sprint target:

- `Code&DBs/Workflow/runtime/native_scheduler.py` stops owning its own queries against `schedule_definitions` and `workflow_classes`
- the runtime seam reads through `PostgresWorkflowScheduleRepository.load_catalog(...)` or a very thin adapter built on that repository
- `Code&DBs/Workflow/surfaces/api/native_scheduler.py` keeps its current external response shape: `native_instance` plus `schedule`
- one focused contract test proves the frontdoor now depends on the canonical repository/catalog path rather than a private duplicated SQL repository

Stop boundary for this sprint:

- do not rewrite `runtime/scheduler_window_repository.py`
- do not redesign `default_path_pilot.py`
- do not alter Phase 8 migration SQL
- do not widen into schedule mutation paths or window-capacity write logic

## 5. Exact file or subsystem scope

Read and modify only:

- `Code&DBs/Workflow/runtime/native_scheduler.py`
- `Code&DBs/Workflow/surfaces/api/native_scheduler.py`
- `Code&DBs/Workflow/tests/integration/test_native_scheduler_runtime.py`

Read for reuse and contract anchoring:

- `Code&DBs/Workflow/storage/postgres/workflow_schedule_repository.py`
- `Code&DBs/Workflow/authority/workflow_schedule.py`
- `Code&DBs/Workflow/policy/workflow_classes.py`
- `Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py`
- `Code&DBs/Workflow/tests/integration/test_scheduler_window_authority_repository.py`

Explicitly out of scope:

- `Code&DBs/Workflow/runtime/scheduler_window_repository.py`
- `Code&DBs/Workflow/runtime/default_path_pilot.py`
- `Code&DBs/Databases/migrations/workflow/008_workflow_class_and_schedule_schema.sql`
- any new migration, schema object, or generated authority manifest change

## 6. Done criteria

- `runtime/native_scheduler.py` no longer contains a private authoritative SQL read model for Phase 8 schedule and workflow-class inspection.
- `surfaces/api/native_scheduler.py` resolves through the canonical repository/catalog path, directly or via a thin adapter with no duplicated SQL.
- The native scheduler frontdoor still returns the same top-level payload shape for existing callers unless a test explicitly captures a required contract correction.
- A focused test fails before the change and passes after it, proving the frontdoor/runtime path no longer depends on `PostgresNativeSchedulerRepository`-style private table queries.
- Existing Phase 8 manifest and repository tests still pass.
- No changes are made to migration `008_workflow_class_and_schedule_schema.sql`, registry metadata, or generated migration authority artifacts.

## 7. Verification commands

Run from the repo root with the workflow database configured when needed:

```bash
export WORKFLOW_DATABASE_URL="postgresql://nate@127.0.0.1:5432/praxis"
pytest Code\&DBs/Workflow/tests/integration/test_native_scheduler_runtime.py -q
pytest Code\&DBs/Workflow/tests/integration/test_workflow_class_repository.py -q
pytest Code\&DBs/Workflow/tests/integration/test_scheduler_window_authority_repository.py -q
pytest Code\&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py -q
```

Optional grep to confirm the duplication was removed from the first sprint seam:

```bash
rg -n "_SCHEDULE_QUERY|_WORKFLOW_CLASS_QUERY|FROM schedule_definitions|FROM workflow_classes" \
  Code\&DBs/Workflow/runtime/native_scheduler.py \
  Code\&DBs/Workflow/surfaces/api/native_scheduler.py
```

Expected post-sprint intent:

- `native_scheduler.py` should no longer be a second SQL owner for Phase 8 inspection
- `workflow_schedule_repository.py` should remain the reusable owner for the canonical read path used by this sprint

## 8. Review -> healer -> human approval gate

Review:

- confirm `runtime/native_scheduler.py` no longer duplicates Phase 8 schedule/class SQL
- confirm `surfaces/api/native_scheduler.py` still returns the expected payload contract
- confirm no migration or manifest drift was introduced
- confirm the sprint did not silently widen into `runtime/scheduler_window_repository.py` or `default_path_pilot.py`

Healer:

- if review finds contract drift or accidental duplication reintroduced inside the `native_scheduler` seam, repair only that seam
- do not let healer widen scope into the separate scheduler-window runtime path

Human approval gate:

- require explicit human approval after review and any healer pass before starting a second Phase 8 sprint
- the next Phase 8 sprint, if approved later, should target the separate `runtime/scheduler_window_repository.py` convergence problem as its own bounded packet rather than bundling both seams together
