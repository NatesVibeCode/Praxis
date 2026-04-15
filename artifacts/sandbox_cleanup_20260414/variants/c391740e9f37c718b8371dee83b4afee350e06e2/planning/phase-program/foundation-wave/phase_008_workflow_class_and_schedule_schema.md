# Phase 8 Workflow Class and Schedule Schema

Status: execution_ready

Registry authority: [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json) phase `8` (`Workflow Class and Schedule Schema`), status `historical_foundation`, predecessor phase `7`, required closeout sequence `review -> healer -> human_approval`.

Grounding note: this packet is grounded in the current checked-out repo snapshot under `/workspace` and mapped to the declared execution root `/Users/nate/Praxis` for future command execution. The repo state still contains both the canonical Phase 8 catalog path and a duplicate native-scheduler SQL reader, so the sprint is a read-authority convergence sprint, not a schema-design sprint.

## 1. Objective in repo terms

- Reassert one canonical Phase 8 read-authority seam for workflow classes and recurring schedule inspection in the current repo.
- Make the native scheduler inspection frontdoor read Phase 8 authority through the existing composed catalog path instead of through file-local direct SQL in [Code&DBs/Workflow/runtime/native_scheduler.py](/workspace/Code&DBs/Workflow/runtime/native_scheduler.py).
- Keep the public frontdoor contract stable in [Code&DBs/Workflow/surfaces/api/native_scheduler.py](/workspace/Code&DBs/Workflow/surfaces/api/native_scheduler.py): `native_instance`, `schedule`, `schedule.schedule_definition`, and `schedule.workflow_class` must remain present for this sprint.
- Do not change Phase 8 table shape, write behavior, migration history, or repo-wide schedule-window ownership in this packet.

## 2. Current evidence in the repo

- The authority map declares phase `8` as `Workflow Class and Schedule Schema`, predecessor `7`, with mandatory closeout sequence `review -> healer -> human_approval` in [planning/phase-program/praxis_0_100_registry.json](/workspace/planning/phase-program/praxis_0_100_registry.json).
- The schema authority for this phase is [Code&DBs/Databases/migrations/workflow/008_workflow_class_and_schedule_schema.sql](/workspace/Code&DBs/Databases/migrations/workflow/008_workflow_class_and_schedule_schema.sql). It creates `workflow_classes`, `schedule_definitions`, and `recurring_run_windows`.
- Migration `008` explicitly documents split ownership:
- `workflow_classes` is owned by `policy/`
- `schedule_definitions` and `recurring_run_windows` are owned by `runtime/`
- [Code&DBs/Workflow/policy/workflow_classes.py](/workspace/Code&DBs/Workflow/policy/workflow_classes.py) already provides the canonical workflow-class catalog and fail-closed resolution model.
- [Code&DBs/Workflow/authority/workflow_schedule.py](/workspace/Code&DBs/Workflow/authority/workflow_schedule.py) already provides the canonical composed Phase 8 read model through `NativeWorkflowScheduleCatalog`.
- [Code&DBs/Workflow/storage/postgres/workflow_schedule_repository.py](/workspace/Code&DBs/Workflow/storage/postgres/workflow_schedule_repository.py) already loads workflow classes, schedule definitions, and recurring run windows together in one transaction through `load_catalog(...)`.
- [Code&DBs/Workflow/runtime/native_scheduler.py](/workspace/Code&DBs/Workflow/runtime/native_scheduler.py) still owns `_SCHEDULE_QUERY`, `_WORKFLOW_CLASS_QUERY`, `NativeScheduleDefinitionRecord`, `NativeWorkflowClassRecord`, and `PostgresNativeSchedulerRepository`, so it still defines a second Phase 8 read path.
- [Code&DBs/Workflow/surfaces/api/native_scheduler.py](/workspace/Code&DBs/Workflow/surfaces/api/native_scheduler.py) still defaults its repository factory to `PostgresNativeSchedulerRepository`, so the public inspection seam is still wired to the duplicate path.
- [Code&DBs/Workflow/tests/integration/test_native_scheduler_runtime.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_scheduler_runtime.py) still proves behavior by asserting query order against `FROM schedule_definitions` and `FROM workflow_classes`.
- [Code&DBs/Workflow/tests/integration/test_native_default_parallel_proof.py](/workspace/Code&DBs/Workflow/tests/integration/test_native_default_parallel_proof.py) still asserts the current payload shape and authority labels exposed by the frontdoor.
- [Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py) and [Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py) already provide current proof coverage for the canonical class catalog and migration authority.
- [Code&DBs/Workflow/runtime/scheduler_window_repository.py](/workspace/Code&DBs/Workflow/runtime/scheduler_window_repository.py) remains an additional active reader of `recurring_run_windows`. That is real repo evidence but remains out of scope for this first sprint.

## 3. Gap or ambiguity still remaining

- The unresolved question is not what Phase 8 tables exist. The migration and canonical catalog already answer that.
- The unresolved question is which code path is allowed to define scheduler inspection meaning for `workflow_classes` and `schedule_definitions`.
- Today the repo has competing read authorities:
- the canonical composed path in `policy.workflow_classes` + `authority.workflow_schedule` + `storage.postgres.workflow_schedule_repository`
- the duplicate direct-SQL path in `runtime.native_scheduler`
- There is a second ambiguity inside the sprint boundary: the canonical schedule catalog understands active `recurring_run_windows`, while the current native scheduler frontdoor returns only `schedule_definition` and `workflow_class`.
- For this sprint, preserve the existing frontdoor payload and adapt canonical catalog results back into the current contract instead of widening the surface to expose window data.
- Do not describe this sprint as global Phase 8 convergence. After this sprint, `runtime/scheduler_window_repository.py` will still remain as a separate Phase 8 reader seam.

## 4. One bounded first sprint only

- Replace the native scheduler frontdoor dependency on `PostgresNativeSchedulerRepository` with a thin adapter that reads through `PostgresWorkflowScheduleRepository.load_catalog(...)` and resolves through `NativeWorkflowScheduleCatalog`.
- Preserve the existing external payload from [Code&DBs/Workflow/surfaces/api/native_scheduler.py](/workspace/Code&DBs/Workflow/surfaces/api/native_scheduler.py). Existing callers should not need to consume recurring window fields in this sprint.
- Preserve current authority labels unless the scoped tests are intentionally updated in the same sprint. Current repo evidence expects:
- `schedule_authority == "runtime.schedule_definitions"`
- `workflow_class_authority == "policy.workflow_classes"`
- Rewrite native-scheduler-facing tests so they prove canonical-path behavior, deterministic results, and fail-closed ambiguity handling rather than raw SQL/query-order ownership in `runtime.native_scheduler`.
- Stop boundary for this sprint:
- do not refactor [Code&DBs/Workflow/runtime/scheduler_window_repository.py](/workspace/Code&DBs/Workflow/runtime/scheduler_window_repository.py)
- do not refactor [Code&DBs/Workflow/runtime/default_path_pilot.py](/workspace/Code&DBs/Workflow/runtime/default_path_pilot.py)
- do not change [Code&DBs/Databases/migrations/workflow/008_workflow_class_and_schedule_schema.sql](/workspace/Code&DBs/Databases/migrations/workflow/008_workflow_class_and_schedule_schema.sql)
- do not add Phase 8 write behavior
- do not widen into repo-wide cleanup of every remaining Phase 8 reader

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
- [Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py)
- [Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py)
- Explicitly out of scope:
- [Code&DBs/Workflow/runtime/scheduler_window_repository.py](/workspace/Code&DBs/Workflow/runtime/scheduler_window_repository.py)
- [Code&DBs/Workflow/runtime/default_path_pilot.py](/workspace/Code&DBs/Workflow/runtime/default_path_pilot.py)
- [Code&DBs/Workflow/tests/integration/test_scheduler_window_authority_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_scheduler_window_authority_repository.py)
- any new migration
- any payload expansion that makes callers consume recurring window fields
- any repo-wide consolidation of all remaining Phase 8 readers

## 6. Done criteria

- [Code&DBs/Workflow/runtime/native_scheduler.py](/workspace/Code&DBs/Workflow/runtime/native_scheduler.py) no longer owns authoritative private SQL for `schedule_definitions` or `workflow_classes`.
- The native scheduler inspection seam reads through the canonical Phase 8 repository/catalog path, either directly or through a thin adapter, without reintroducing local duplicate SQL.
- [Code&DBs/Workflow/surfaces/api/native_scheduler.py](/workspace/Code&DBs/Workflow/surfaces/api/native_scheduler.py) remains contract-compatible for existing callers:
- top-level `native_instance` and `schedule` remain
- `schedule.schedule_definition` remains present
- `schedule.workflow_class` remains present
- Native scheduler tests stop asserting direct query-order ownership in `runtime.native_scheduler` and instead assert canonical-path behavior plus fail-closed ambiguity outcomes.
- Canonical repository and migration proofs still pass in [Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_class_repository.py) and [Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py](/workspace/Code&DBs/Workflow/tests/integration/test_workflow_class_schedule_schema.py).
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

Expected verification outcome:

- native scheduler inspection remains deterministic and fail-closed
- the frontdoor no longer depends on duplicate direct SQL in `runtime/native_scheduler.py`
- canonical Phase 8 repository and migration proofs still pass
- out-of-scope scheduler-window and default-path seams remain unchanged

## 8. Review -> healer -> human approval gate

- Review:
- confirm `runtime/native_scheduler.py` no longer duplicates `schedule_definitions` or `workflow_classes` SQL
- confirm `surfaces/api/native_scheduler.py` now depends on the canonical Phase 8 repository/catalog path
- confirm the refreshed native scheduler proofs assert contract behavior and fail-closed ambiguity rather than raw query-order expectations
- confirm no changes landed in `runtime/scheduler_window_repository.py`, `runtime/default_path_pilot.py`, or migration `008_workflow_class_and_schedule_schema.sql`
- Healer:
- if review finds payload drift, authority-label drift, duplicate SQL reintroduced, or accidental widening into scheduler-window work, repair only the files listed in this packet's in-scope set
- do not widen healer work into `runtime.scheduler_window_repository`, `runtime.default_path_pilot`, migration edits, or new Phase 8 write behavior
- Human approval gate:
- require explicit human approval after review and any healer pass before opening a second Phase 8 sprint
- any later second sprint should target exactly one remaining reader seam, most likely `runtime/scheduler_window_repository.py`, instead of claiming Phase 8 authority convergence is globally complete
