# Phase 8 Workflow Class and Schedule Schema

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `8` (`Workflow Class and Schedule Schema`), arc `0-9 define the machine`, status `historical_foundation`, predecessor `7`, closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is grounded in the current checkout at `/workspace`. The declared platform execution root is `/Users/nate/Praxis`, so repo evidence cites `/workspace` and verification commands target that declared root. The execution shard says compile-authority inputs are ready, while verification coverage is still immature, so this packet stays narrow and proof-first.

## 1. Objective in repo terms

- Converge one repo-owned Phase 8 inspection seam for workflow classes and recurring schedules.
- Keep the sprint bounded to the native scheduler frontdoor used by [Code&DBs/Workflow/surfaces/api/native_scheduler.py](/workspace/Code&DBs/Workflow/surfaces/api/native_scheduler.py).
- Replace duplicate direct SQL in [Code&DBs/Workflow/runtime/native_scheduler.py](/workspace/Code&DBs/Workflow/runtime/native_scheduler.py) with the existing canonical Phase 8 authority stack:
- [Code&DBs/Workflow/policy/workflow_classes.py](/workspace/Code&DBs/Workflow/policy/workflow_classes.py)
- [Code&DBs/Workflow/authority/workflow_schedule.py](/workspace/Code&DBs/Workflow/authority/workflow_schedule.py)
- [Code&DBs/Workflow/storage/postgres/workflow_schedule_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/workflow_schedule_repository.py)
- Preserve the current caller-facing contract from the native scheduler frontdoor:
- top-level `native_instance`
- top-level `schedule`
- `schedule.schedule_definition`
- `schedule.workflow_class`
- Do not redesign the Phase 8 schema, add write behavior, or claim repo-wide Phase 8 convergence.

## 2. Current evidence in the repo

- The authority map defines phase `8` as `Workflow Class and Schedule Schema` with predecessor `7` and required sequence `review -> healer -> human_approval` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json).
- Canonical schema authority already exists in [Code&DBs/Databases/migrations/workflow/008_workflow_class_and_schedule_schema.sql](/workspace/Code&DBs/Databases/migrations/workflow/008_workflow_class_and_schedule_schema.sql). It creates:
- `workflow_classes`
- `schedule_definitions`
- `recurring_run_windows`
- The migration comments already split ownership:
- `workflow_classes` owned by `policy/`
- `schedule_definitions` owned by `runtime/`
- `recurring_run_windows` owned by `runtime/`
- [Code&DBs/Workflow/system_authority/workflow_migration_authority.json](/workspace/Code&DBs/Workflow/system_authority/workflow_migration_authority.json) includes `008_workflow_class_and_schedule_schema.sql` in the canonical manifest and expects the Phase 8 objects.
- [Code&DBs/Workflow/policy/workflow_classes.py](/workspace/Code&DBs/Workflow/policy/workflow_classes.py) already provides the canonical workflow-class repository and catalog.
- [Code&DBs/Workflow/authority/workflow_schedule.py](/workspace/Code&DBs/Workflow/authority/workflow_schedule.py) already provides `NativeWorkflowScheduleCatalog`, which resolves:
- one active `schedule_definition`
- one active `workflow_class`
- one active `recurring_run_window`
- fail-closed ambiguity behavior for duplicate active rows
- [Code&DBs/Workflow/storage/postgres/workflow_schedule_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/workflow_schedule_repository.py) already loads workflow classes, schedule definitions, and recurring run windows inside one transaction through `load_catalog(...)`.
- [Code&DBs/Workflow/runtime/native_scheduler.py](/workspace/Code&DBs/Workflow/runtime/native_scheduler.py) still owns duplicate read authority through:
- `_SCHEDULE_QUERY`
- `_WORKFLOW_CLASS_QUERY`
- `PostgresNativeSchedulerRepository`
- file-local `NativeScheduleDefinitionRecord`
- file-local `NativeWorkflowClassRecord`
- [Code&DBs/Workflow/surfaces/api/native_scheduler.py](/workspace/Code&DBs/Workflow/surfaces/api/native_scheduler.py) still defaults its repository factory to `PostgresNativeSchedulerRepository`, so the public frontdoor is wired to the duplicate path today.
- [Code&DBs/Workflow/runtime/scheduler_window_repository.py](/workspace/Code&DBs/Workflow/runtime/scheduler_window_repository.py) is another active direct reader of `schedule_definitions` and `recurring_run_windows`, which means the repo is not globally converged even if the native scheduler seam is fixed.
- Existing proofs already cover the schema and canonical authority path:
- [Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py)
- [Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py)
- Existing native scheduler proofs still encode the duplicate read path:
- [Code&DBs/Workflow/tests/integration/test_native_scheduler_runtime.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_scheduler_runtime.py) asserts query order against `schedule_definitions` then `workflow_classes`
- [Code&DBs/Workflow/tests/integration/test_native_default_parallel_proof.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_default_parallel_proof.py) proves payload shape but still uses the duplicate scheduler seam

## 3. Gap or ambiguity still remaining

- The gap is not missing schema. Phase 8 tables and manifest authority already exist.
- The gap is duplicate read ownership for the native scheduler inspection path.
- Today the repo has two meanings for the same authority:
- canonical composed resolution in `policy.workflow_classes` plus `authority.workflow_schedule` plus `storage.postgres.workflow_schedule_repository`
- duplicate direct-SQL resolution in `runtime.native_scheduler`
- There is a second bounded ambiguity inside the native scheduler seam: the canonical catalog requires an active `recurring_run_window`, while the current frontdoor payload only exposes `schedule_definition` and `workflow_class`.
- This sprint should resolve that ambiguity by using the canonical catalog internally and adapting the resolved result back into the current payload shape without exposing recurring-window fields yet.
- Even after this sprint, [Code&DBs/Workflow/runtime/scheduler_window_repository.py](/workspace/Code&DBs/Workflow/runtime/scheduler_window_repository.py) will remain another direct Phase 8 reader. Do not describe the result as repo-wide Phase 8 convergence.

## 4. One bounded first sprint only

- Replace the native scheduler frontdoor dependency on `PostgresNativeSchedulerRepository` with a thin adapter over `PostgresWorkflowScheduleRepository.load_catalog(...)` and `NativeWorkflowScheduleCatalog.resolve(...)`.
- Preserve the current external payload from [Code&DBs/Workflow/surfaces/api/native_scheduler.py](/workspace/Code&DBs/Workflow/surfaces/api/native_scheduler.py). This sprint should not require callers to consume recurring-window fields.
- Keep current authority labels stable unless a scoped proof requires a deliberate update:
- `schedule_authority == "runtime.schedule_definitions"`
- `workflow_class_authority == "policy.workflow_classes"`
- Rewrite native-scheduler-facing tests to prove:
- deterministic output
- canonical-path behavior
- fail-closed ambiguity behavior
- frontdoor contract stability
- Stop boundary for this sprint:
- do not refactor [Code&DBs/Workflow/runtime/scheduler_window_repository.py](/workspace/Code&DBs/Workflow/runtime/scheduler_window_repository.py)
- do not refactor [Code&DBs/Workflow/runtime/default_path_pilot.py](/workspace/Code&DBs/Workflow/runtime/default_path_pilot.py)
- do not change [Code&DBs/Databases/migrations/workflow/008_workflow_class_and_schedule_schema.sql](/workspace/Code&DBs/Databases/migrations/workflow/008_workflow_class_and_schedule_schema.sql)
- do not add Phase 8 writes
- do not widen into cleanup of every remaining Phase 8 reader

## 5. Exact file or subsystem scope

- Primary implementation scope:
- [Code&DBs/Workflow/runtime/native_scheduler.py](/workspace/Code&DBs/Workflow/runtime/native_scheduler.py)
- [Code&DBs/Workflow/surfaces/api/native_scheduler.py](/workspace/Code&DBs/Workflow/surfaces/api/native_scheduler.py)
- Primary regression scope:
- [Code&DBs/Workflow/tests/integration/test_native_scheduler_runtime.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_scheduler_runtime.py)
- [Code&DBs/Workflow/tests/integration/test_native_default_parallel_proof.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_default_parallel_proof.py)
- Read-only authority references:
- [Code&DBs/Workflow/storage/postgres/workflow_schedule_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/workflow_schedule_repository.py)
- [Code&DBs/Workflow/authority/workflow_schedule.py](/workspace/Code&DBs/Workflow/authority/workflow_schedule.py)
- [Code&DBs/Workflow/policy/workflow_classes.py](/workspace/Code&DBs/Workflow/policy/workflow_classes.py)
- [Code&DBs/Databases/migrations/workflow/008_workflow_class_and_schedule_schema.sql](/workspace/Code&DBs/Databases/migrations/workflow/008_workflow_class_and_schedule_schema.sql)
- [Code&DBs/Workflow/system_authority/workflow_migration_authority.json](/workspace/Code&DBs/Workflow/system_authority/workflow_migration_authority.json)
- [Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py)
- [Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/scheduler_window_repository.py](/workspace/Code&DBs/Workflow/runtime/scheduler_window_repository.py)
- [Code&DBs/Workflow/runtime/default_path_pilot.py](/workspace/Code&DBs/Workflow/runtime/default_path_pilot.py)
- [Code&DBs/Workflow/tests/integration/test_scheduler_window_authority_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_scheduler_window_authority_repository.py)
- any new migration
- any payload expansion that makes callers consume recurring-window fields
- any repo-wide consolidation of all remaining Phase 8 readers

## 6. Done criteria

- [Code&DBs/Workflow/runtime/native_scheduler.py](/workspace/Code&DBs/Workflow/runtime/native_scheduler.py) no longer owns authoritative private SQL for `schedule_definitions` or `workflow_classes`.
- The native scheduler inspection seam reads through the canonical Phase 8 repository and catalog path, either directly or through a thin adapter, without recreating local duplicate SQL.
- [Code&DBs/Workflow/surfaces/api/native_scheduler.py](/workspace/Code&DBs/Workflow/surfaces/api/native_scheduler.py) remains caller-compatible:
- top-level `native_instance` remains
- top-level `schedule` remains
- `schedule.schedule_definition` remains present
- `schedule.workflow_class` remains present
- `schedule.schedule_authority` remains stable unless the scoped proofs intentionally update it
- `schedule.workflow_class_authority` remains stable unless the scoped proofs intentionally update it
- Native scheduler tests stop asserting low-level query-order ownership in `runtime.native_scheduler` and instead assert canonical-path behavior plus fail-closed ambiguity outcomes.
- Canonical repository and migration proofs still pass in:
- [Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py)
- [Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py)
- No changes land in `runtime/scheduler_window_repository.py`, `runtime/default_path_pilot.py`, or migration `008_workflow_class_and_schedule_schema.sql`.

## 7. Verification commands

- `cd /Users/nate/Praxis`
- `export WORKFLOW_DATABASE_URL='postgresql://nate@127.0.0.1:5432/praxis'`
- `export PYTHONPATH='Code&DBs/Workflow'`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_native_scheduler_runtime.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_native_default_parallel_proof.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_workflow_class_repository.py -q`
- `python -m pytest Code\&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py -q`
- `rg -n "_SCHEDULE_QUERY|_WORKFLOW_CLASS_QUERY|PostgresNativeSchedulerRepository|NativeWorkflowScheduleCatalog|PostgresWorkflowScheduleRepository" Code\&DBs/Workflow/runtime/native_scheduler.py Code\&DBs/Workflow/surfaces/api/native_scheduler.py`
- `rg -n "008_workflow_class_and_schedule_schema\\.sql|workflow_classes|schedule_definitions|recurring_run_windows" Code\&DBs/Workflow/system_authority/workflow_migration_authority.json Code\&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py`

Expected verification outcome:

- native scheduler inspection remains deterministic and fail-closed
- the public frontdoor no longer depends on duplicate direct SQL in `runtime/native_scheduler.py`
- the public frontdoor adapts the canonical catalog result back into the existing `schedule_definition` plus `workflow_class` payload shape without surfacing recurring-window fields
- canonical Phase 8 repository and migration proofs still pass
- out-of-scope scheduler-window and default-path seams remain unchanged

## 8. Review -> healer -> human approval gate

- Review:
- confirm `runtime/native_scheduler.py` no longer duplicates `schedule_definitions` or `workflow_classes` SQL
- confirm `surfaces/api/native_scheduler.py` now depends on the canonical Phase 8 repository and catalog path
- confirm the refreshed native scheduler proofs assert contract behavior and fail-closed ambiguity rather than raw query-order expectations
- confirm no changes landed in `runtime/scheduler_window_repository.py`, `runtime/default_path_pilot.py`, or migration `008_workflow_class_and_schedule_schema.sql`
- Healer:
- if review finds payload drift, authority-label drift, duplicate SQL reintroduced, or accidental widening into scheduler-window work, repair only the files listed in this packet's in-scope set
- do not widen healer work into `runtime.scheduler_window_repository`, `runtime.default_path_pilot`, migration edits, or new Phase 8 write behavior
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 8 sprint
- any later second sprint should target exactly one remaining reader seam, most likely `runtime/scheduler_window_repository.py`, instead of claiming that Phase 8 authority convergence is globally complete
