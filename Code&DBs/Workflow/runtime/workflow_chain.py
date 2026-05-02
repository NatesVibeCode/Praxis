"""Durable workflow chain authority over multi-wave coordination specs."""

from __future__ import annotations

from dataclasses import dataclass
from functools import lru_cache
import json
from pathlib import Path
import uuid
from typing import Any

from runtime.system_events import emit_system_event
from runtime.workspace_paths import repo_root as workspace_repo_root
from storage.migrations import WorkflowMigrationError, workflow_migration_statements

_SCHEMA_FILENAMES = (
    "087_workflow_chain_authority.sql",
    "088_workflow_chain_dependency_and_adoption_authority.sql",
    "090_workflow_chain_cancellation_and_alignment.sql",
)
_WORKFLOW_TERMINAL_STATUSES = {"succeeded", "failed", "dead_letter", "cancelled", "missing"}
_CHAIN_TERMINAL_STATUSES = {"succeeded", "failed", "cancelled"}
_WAVE_TERMINAL_STATUSES = {"succeeded", "failed", "blocked", "cancelled"}
_RUN_FAILURE_STATUSES = {"failed", "dead_letter", "missing"}
_RUN_CANCELLATION_STATUSES = {"cancelled"}
_RUN_ACTIVE_STATUSES = {"queued", "running"}


class WorkflowChainError(ValueError):
    """Raised when a workflow chain file is missing, invalid, or cannot execute."""

    def __init__(
        self,
        message: str,
        *,
        reason_code: str = "workflow.chain.invalid",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


@dataclass(frozen=True)
class WorkflowChainWave:
    """One chain wave composed of one or more workflow specs."""

    wave_id: str
    spec_paths: tuple[str, ...]
    depends_on: tuple[str, ...] = ()


@dataclass(frozen=True)
class WorkflowChainProgram:
    """Parsed chain program built from a coordination JSON file."""

    program: str
    coordination_path: str
    mode: str | None
    why: str | None
    validate_order: tuple[str, ...]
    waves: tuple[WorkflowChainWave, ...]


def _normalize_relative_path(path_value: str, *, repo_root: Path) -> str:
    candidate = Path(path_value)
    full_path = candidate if candidate.is_absolute() else (repo_root / candidate)
    resolved_repo_root = repo_root.resolve()
    resolved_path = full_path.resolve()
    try:
        relative = resolved_path.relative_to(resolved_repo_root)
    except ValueError as exc:
        raise WorkflowChainError(f"path escapes repo root: {path_value}") from exc
    return str(relative)


def _require_string(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise WorkflowChainError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_string_list(value: object, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or any(not isinstance(item, str) or not item.strip() for item in value):
        raise WorkflowChainError(f"{field_name} must be a list of non-empty strings")
    return [item.strip() for item in value]


def _normalize_dependency_ids(
    value: object,
    *,
    field_name: str,
    wave_id: str,
) -> tuple[str, ...]:
    dependencies = _require_string_list(value, field_name=field_name)
    ordered: list[str] = []
    seen: set[str] = set()
    for dependency in dependencies:
        if dependency == wave_id:
            raise WorkflowChainError(f"wave {wave_id} cannot depend on itself")
        if dependency in seen:
            continue
        seen.add(dependency)
        ordered.append(dependency)
    return tuple(ordered)


def _default_repo_root() -> Path:
    return workspace_repo_root()


def _topologically_order_waves(waves: list[WorkflowChainWave]) -> tuple[WorkflowChainWave, ...]:
    ordered: list[WorkflowChainWave] = []
    completed: set[str] = set()

    while len(ordered) < len(waves):
        ready = [
            wave
            for wave in waves
            if wave.wave_id not in completed and set(wave.depends_on).issubset(completed)
        ]
        if not ready:
            raise WorkflowChainError("wave dependency cycle detected")
        for wave in ready:
            ordered.append(wave)
            completed.add(wave.wave_id)
    return tuple(ordered)


def _validate_validate_order(
    validate_order: tuple[str, ...],
    *,
    wave_specs: tuple[str, ...],
) -> None:
    if not validate_order:
        return

    validate_set = set(validate_order)
    wave_spec_set = set(wave_specs)
    missing_specs = sorted(wave_spec_set - validate_set)
    extra_specs = sorted(validate_set - wave_spec_set)
    if not missing_specs and not extra_specs:
        return

    messages: list[str] = []
    if missing_specs:
        messages.append(f"missing {missing_specs}")
    if extra_specs:
        messages.append(f"extra {extra_specs}")
    raise WorkflowChainError(
        "validate_order must match the exact set of specs referenced by waves: "
        + "; ".join(messages),
        reason_code="workflow.chain.validate_order_drift",
        details={"missing_specs": missing_specs, "extra_specs": extra_specs},
    )


def _dedupe_ordered(values: list[str]) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _raise_missing_specs(missing_specs: list[str]) -> None:
    unique_missing = _dedupe_ordered(missing_specs)
    if not unique_missing:
        return
    raise WorkflowChainError(
        "workflow chain preflight failed: missing referenced spec(s): "
        + ", ".join(unique_missing),
        reason_code="workflow.chain.preflight_missing_specs",
        details={"missing_specs": unique_missing},
    )


def load_workflow_chain(
    path: str,
    *,
    repo_root: str | None = None,
) -> WorkflowChainProgram:
    """Load one workflow chain program from a coordination JSON file."""

    repo_root_path = Path(repo_root).resolve() if repo_root else _default_repo_root().resolve()
    chain_path = Path(path)
    if not chain_path.is_absolute():
        chain_path = repo_root_path / chain_path
    if not chain_path.exists():
        raise WorkflowChainError(f"workflow chain file not found: {path}")

    try:
        raw = json.loads(chain_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise WorkflowChainError(f"workflow chain file is not valid JSON: {path}") from exc

    if not isinstance(raw, dict):
        raise WorkflowChainError("workflow chain file must contain a JSON object")

    program = _require_string(raw.get("program"), field_name="program")
    validate_order = tuple(
        _normalize_relative_path(item, repo_root=repo_root_path)
        for item in _require_string_list(raw.get("validate_order"), field_name="validate_order")
    )
    missing_specs: list[str] = []
    for spec_path in validate_order:
        if not (repo_root_path / spec_path).exists():
            missing_specs.append(spec_path)

    raw_waves = raw.get("waves")
    if not isinstance(raw_waves, list) or not raw_waves:
        raise WorkflowChainError("waves must be a non-empty list")

    seen_wave_ids: set[str] = set()
    parsed_waves: list[WorkflowChainWave] = []
    for index, raw_wave in enumerate(raw_waves):
        if not isinstance(raw_wave, dict):
            raise WorkflowChainError(f"wave[{index}] must be an object")
        wave_id = _require_string(raw_wave.get("wave_id"), field_name=f"waves[{index}].wave_id")
        if wave_id in seen_wave_ids:
            raise WorkflowChainError(f"duplicate wave_id: {wave_id}")
        seen_wave_ids.add(wave_id)

        spec_paths = tuple(
            _normalize_relative_path(item, repo_root=repo_root_path)
            for item in _require_string_list(raw_wave.get("specs"), field_name=f"waves[{index}].specs")
        )
        if not spec_paths:
            raise WorkflowChainError(f"waves[{index}].specs must not be empty")
        for spec_path in spec_paths:
            if not (repo_root_path / spec_path).exists():
                missing_specs.append(spec_path)

        depends_on = _normalize_dependency_ids(
            raw_wave.get("depends_on", []),
            field_name=f"waves[{index}].depends_on",
            wave_id=wave_id,
        )
        parsed_waves.append(
            WorkflowChainWave(
                wave_id=wave_id,
                spec_paths=spec_paths,
                depends_on=depends_on,
            )
        )

    known_waves = {wave.wave_id for wave in parsed_waves}
    for wave in parsed_waves:
        for dependency in wave.depends_on:
            if dependency not in known_waves:
                raise WorkflowChainError(
                    f"wave {wave.wave_id} depends on unknown wave {dependency}",
                )

    _raise_missing_specs(missing_specs)

    all_wave_specs = tuple(spec_path for wave in parsed_waves for spec_path in wave.spec_paths)
    _validate_validate_order(validate_order, wave_specs=all_wave_specs)

    ordered_waves = _topologically_order_waves(parsed_waves)
    coordination_relative = _normalize_relative_path(str(chain_path), repo_root=repo_root_path)
    return WorkflowChainProgram(
        program=program,
        coordination_path=coordination_relative,
        mode=str(raw.get("mode")).strip() or None if raw.get("mode") is not None else None,
        why=str(raw.get("why")).strip() or None if raw.get("why") is not None else None,
        validate_order=validate_order,
        waves=ordered_waves,
    )


def iter_chain_spec_paths(program: WorkflowChainProgram) -> tuple[str, ...]:
    """Return the unique spec paths for validation/execution in stable order."""

    ordered: list[str] = []
    seen: set[str] = set()
    for spec_path in program.validate_order:
        if spec_path not in seen:
            ordered.append(spec_path)
            seen.add(spec_path)
    for wave in program.waves:
        for spec_path in wave.spec_paths:
            if spec_path not in seen:
                ordered.append(spec_path)
                seen.add(spec_path)
    return tuple(ordered)


@lru_cache(maxsize=len(_SCHEMA_FILENAMES))
def _schema_statements(filename: str) -> tuple[str, ...]:
    try:
        return workflow_migration_statements(filename)
    except WorkflowMigrationError as exc:
        reason_code = (
            "workflow.chain.schema_empty"
            if exc.reason_code == "workflow.migration_empty"
            else "workflow.chain.schema_missing"
        )
        message = (
            "workflow-chain schema file did not contain executable statements"
            if reason_code == "workflow.chain.schema_empty"
            else "workflow-chain schema file could not be resolved from the canonical workflow migration root"
        )
        raise WorkflowChainError(f"{message}: {exc.details}") from exc


def bootstrap_workflow_chain_schema(conn: Any) -> None:
    """Apply the durable workflow-chain schema."""

    control_commands_exists = True
    try:
        rows = conn.execute("SELECT to_regclass('control_commands') AS table_name")
        control_commands_exists = bool(rows and rows[0].get("table_name"))
    except Exception:
        control_commands_exists = True

    if not control_commands_exists:
        from runtime.control_commands import bootstrap_control_commands_schema

        bootstrap_control_commands_schema(conn)
    sql_statements: list[str] = []
    for filename in _SCHEMA_FILENAMES:
        sql_statements.extend(_schema_statements(filename))
    sql_text = ";\n".join(sql_statements) + ";"
    if hasattr(conn, "execute_script"):
        conn.execute_script(sql_text)
        return
    for statement in sql_statements:
        conn.execute(statement)


def _validated_specs_for_program(
    program: WorkflowChainProgram,
    *,
    repo_root: str,
    pg_conn: Any,
) -> list[dict[str, Any]]:
    """Validate every referenced workflow spec through the live authority."""

    from runtime.workflow_spec import WorkflowSpec
    from runtime.workflow_validation import validate_workflow_spec

    repo_root_path = Path(repo_root).resolve()
    results: list[dict[str, Any]] = []
    for spec_path in iter_chain_spec_paths(program):
        spec = WorkflowSpec.load(str(repo_root_path / spec_path))
        result = validate_workflow_spec(spec, pg_conn=pg_conn)
        result["spec_path"] = spec_path
        result["_workflow_spec"] = spec
        results.append(result)
    return results


def validate_workflow_chain(program: WorkflowChainProgram, *, repo_root: str, pg_conn: Any) -> list[dict[str, Any]]:
    """Validate every referenced workflow spec through the live authority."""

    validation = _validated_specs_for_program(program, repo_root=repo_root, pg_conn=pg_conn)
    public_results: list[dict[str, Any]] = []
    for item in validation:
        public_item = dict(item)
        public_item.pop("_workflow_spec", None)
        public_results.append(public_item)
    return public_results


def _workflow_chain_result_ref(chain_id: str) -> str:
    return f"workflow_chain:{chain_id}"


def workflow_chain_id_from_result_ref(result_ref: str | None) -> str | None:
    if not result_ref or not isinstance(result_ref, str):
        return None
    if not result_ref.startswith("workflow_chain:"):
        return None
    chain_id = result_ref.split(":", 1)[1].strip()
    return chain_id or None


def _queue_id_for_spec(spec: Any) -> str | None:
    raw = getattr(spec, "_raw", {}) or {}
    queue_id = raw.get("queue_id")
    return str(queue_id).strip() if isinstance(queue_id, str) and queue_id.strip() else None


def _json_loads_maybe(value: object, default: object) -> object:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return default
    return value


def _normalized_definition(program: WorkflowChainProgram) -> dict[str, Any]:
    return {
        "program": program.program,
        "coordination_path": program.coordination_path,
        "mode": program.mode,
        "why": program.why,
        "validate_order": list(program.validate_order),
        "waves": [
            {
                "wave_id": wave.wave_id,
                "depends_on": list(wave.depends_on),
                "specs": list(wave.spec_paths),
            }
            for wave in program.waves
        ],
    }


def _emit_chain_event(conn: Any, *, event_type: str, chain_id: str, payload: dict[str, Any]) -> None:
    emit_system_event(
        conn,
        event_type=event_type,
        source_id=chain_id,
        source_type="workflow_chain",
        payload=payload,
    )
    conn.execute("SELECT pg_notify('system_event', $1)", chain_id)


def _spec_rows_for_program(
    program: WorkflowChainProgram,
    *,
    repo_root: Path,
    validated_specs: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    from runtime.workflow._shared import _workflow_id_for_spec
    from runtime.workflow_spec import WorkflowSpec

    specs_by_path = {
        str(item.get("spec_path")): item.get("_workflow_spec")
        for item in (validated_specs or [])
        if item.get("_workflow_spec") is not None
    }
    rows: list[dict[str, Any]] = []
    for wave in program.waves:
        for ordinal, spec_path in enumerate(wave.spec_paths, start=1):
            spec = specs_by_path.get(spec_path)
            if spec is None:
                spec = WorkflowSpec.load(str(repo_root / spec_path))
            rows.append(
                {
                    "wave_id": wave.wave_id,
                    "ordinal": ordinal,
                    "spec_path": spec_path,
                    "spec_name": spec.name,
                    "workflow_id": _workflow_id_for_spec(spec),
                    "spec_workflow_id": spec.workflow_id,
                    "queue_id": _queue_id_for_spec(spec),
                    "total_jobs": len(spec.jobs),
                }
            )
    return rows


def _assert_unique_adoption_targets(rows: list[dict[str, Any]]) -> None:
    seen: dict[tuple[str, str], str] = {}
    for row in rows:
        workflow_id = str(row.get("workflow_id") or "").strip()
        queue_id = str(row.get("queue_id") or "").strip()
        spec_path = str(row.get("spec_path") or "").strip()
        if not workflow_id or not spec_path:
            continue
        adoption_target = (workflow_id, queue_id)
        duplicate_spec_path = seen.get(adoption_target)
        if duplicate_spec_path is None:
            seen[adoption_target] = spec_path
            continue
        qualifier = f" and queue_id '{queue_id}'" if queue_id else " without a queue_id"
        raise WorkflowChainError(
            "workflow chain contains ambiguous adoption targets for "
            f"workflow_id '{workflow_id}'{qualifier}: {duplicate_spec_path} and {spec_path}",
        )


def find_active_run_for_workflow_id(
    conn: Any,
    workflow_id: str,
    *,
    queue_id: str | None = None,
) -> dict[str, Any] | None:
    """Return the newest queued/running run for a workflow_id and optional queue_id."""

    if queue_id:
        rows = conn.execute(
            """SELECT run_id, workflow_id, current_state, requested_at
               FROM workflow_runs
               WHERE workflow_id = $1
                 AND current_state IN ('queued', 'running')
                 AND adoption_key = $2
               ORDER BY requested_at DESC
               LIMIT 1""",
            workflow_id,
            queue_id,
        )
    else:
        rows = conn.execute(
            """SELECT run_id, workflow_id, current_state, requested_at
               FROM workflow_runs
               WHERE workflow_id = $1
                 AND current_state IN ('queued', 'running')
               ORDER BY requested_at DESC
               LIMIT 1""",
            workflow_id,
        )
    if not rows:
        return None
    return dict(rows[0])


def submit_workflow_chain(
    conn: Any,
    *,
    coordination_path: str,
    repo_root: str,
    requested_by_kind: str,
    requested_by_ref: str,
    adopt_active: bool = True,
    chain_id: str | None = None,
    command_id: str | None = None,
) -> str:
    """Persist one durable workflow chain and queue its first runnable wave."""

    repo_root_path = Path(repo_root).resolve()
    normalized_chain_id = chain_id or f"workflow_chain_{uuid.uuid4().hex[:12]}"

    def _queue_preflight_repair(
        *,
        error_code: str,
        error_detail: str,
        missing_specs: list[str] | None = None,
        validation_errors: list[dict[str, Any]] | None = None,
    ) -> None:
        from runtime.workflow.repair_queue import enqueue_solution_preflight_repair

        enqueue_solution_preflight_repair(
            conn,
            solution_id=normalized_chain_id,
            coordination_path=coordination_path,
            repo_root=str(repo_root_path),
            command_id=command_id,
            requested_by_kind=requested_by_kind,
            requested_by_ref=requested_by_ref,
            error_code=error_code,
            error_detail=error_detail,
            missing_specs=missing_specs or [],
            validation_errors=validation_errors or [],
        )

    try:
        program = load_workflow_chain(coordination_path, repo_root=str(repo_root_path))
    except WorkflowChainError as exc:
        try:
            _queue_preflight_repair(
                error_code=exc.reason_code,
                error_detail=str(exc),
                missing_specs=list(exc.details.get("missing_specs") or []),
            )
        except Exception as repair_exc:  # pragma: no cover - defensive authority surfacing.
            raise WorkflowChainError(
                f"{exc}; repair queue enqueue failed: {repair_exc}",
                reason_code="workflow.chain.preflight_repair_enqueue_failed",
                details={
                    "original_reason_code": exc.reason_code,
                    "original_error": str(exc),
                    "repair_error": str(repair_exc),
                },
            ) from repair_exc
        raise

    validation = _validated_specs_for_program(program, repo_root=str(repo_root_path), pg_conn=conn)
    invalid = [item for item in validation if not item.get("valid", False)]
    if invalid:
        first_invalid = invalid[0]
        validation_errors = [
            {
                "spec_path": str(item.get("spec_path") or ""),
                "error": str(item.get("error") or "invalid spec"),
            }
            for item in invalid
        ]
        error_detail = (
            f"workflow chain validation failed for {first_invalid['spec_path']}: "
            f"{first_invalid.get('error') or 'invalid spec'}"
        )
        try:
            _queue_preflight_repair(
                error_code="workflow.chain.validation_failed",
                error_detail=error_detail,
                validation_errors=validation_errors,
            )
        except Exception as repair_exc:  # pragma: no cover - defensive authority surfacing.
            raise WorkflowChainError(
                f"{error_detail}; repair queue enqueue failed: {repair_exc}",
                reason_code="workflow.chain.preflight_repair_enqueue_failed",
                details={
                    "original_reason_code": "workflow.chain.validation_failed",
                    "original_error": error_detail,
                    "repair_error": str(repair_exc),
                },
            ) from repair_exc
        raise WorkflowChainError(
            error_detail,
            reason_code="workflow.chain.validation_failed",
            details={"validation_errors": validation_errors},
        )

    spec_rows = _spec_rows_for_program(
        program,
        repo_root=repo_root_path,
        validated_specs=validation,
    )
    if adopt_active:
        _assert_unique_adoption_targets(spec_rows)

    chain_definition = _normalized_definition(program)
    conn.execute(
        """INSERT INTO workflow_chains (
               chain_id,
               command_id,
               coordination_path,
               repo_root,
               program,
               mode,
               why,
               definition,
               adopt_active,
               status,
               requested_by_kind,
               requested_by_ref
           ) VALUES (
               $1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9, 'queued', $10, $11
           )""",
        normalized_chain_id,
        command_id,
        program.coordination_path,
        str(repo_root_path),
        program.program,
        program.mode,
        program.why,
        json.dumps(chain_definition, sort_keys=True),
        adopt_active,
        requested_by_kind,
        requested_by_ref,
    )

    for ordinal, wave in enumerate(program.waves, start=1):
        depends_on_wave_id = wave.depends_on[0] if len(wave.depends_on) == 1 else None
        conn.execute(
            """INSERT INTO workflow_chain_waves (
                   chain_id,
                   wave_id,
                   ordinal,
                   depends_on_wave_id,
                   status
               ) VALUES ($1, $2, $3, $4, 'pending')""",
            normalized_chain_id,
            wave.wave_id,
            ordinal,
            depends_on_wave_id,
        )
        for dependency_wave_id in wave.depends_on:
            conn.execute(
                """INSERT INTO workflow_chain_wave_dependencies (
                       chain_id,
                       wave_id,
                       depends_on_wave_id
                   ) VALUES ($1, $2, $3)
                   ON CONFLICT DO NOTHING""",
                normalized_chain_id,
                wave.wave_id,
                dependency_wave_id,
            )

    for row in spec_rows:
        conn.execute(
            """INSERT INTO workflow_chain_wave_runs (
                   chain_id,
                   wave_id,
                   ordinal,
                   spec_path,
                   spec_name,
                   workflow_id,
                   spec_workflow_id,
                   queue_id,
                   total_jobs,
                   submission_status,
                   run_status
               ) VALUES (
                   $1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending', 'pending'
               )""",
            normalized_chain_id,
            row["wave_id"],
            row["ordinal"],
            row["spec_path"],
            row["spec_name"],
            row["workflow_id"],
            row["spec_workflow_id"],
            row["queue_id"],
            row["total_jobs"],
        )

    _emit_chain_event(
        conn,
        event_type="workflow_chain.submitted",
        chain_id=normalized_chain_id,
        payload={
            "chain_id": normalized_chain_id,
            "program": program.program,
            "coordination_path": program.coordination_path,
            "wave_count": len(program.waves),
            "spec_count": len(iter_chain_spec_paths(program)),
        },
    )
    advance_workflow_chains(conn, chain_id=normalized_chain_id)
    return normalized_chain_id


def get_workflow_chain_status(conn: Any, chain_id: str) -> dict[str, Any] | None:
    """Return full chain status with waves and wave runs."""

    chain_rows = conn.execute(
        """SELECT chain_id, command_id, coordination_path, repo_root, program, mode, why,
                  definition, adopt_active, status, current_wave_id, requested_by_kind,
                  requested_by_ref, last_error_code, last_error_detail, created_at,
                  updated_at, started_at, finished_at
           FROM workflow_chains
           WHERE chain_id = $1
           LIMIT 1""",
        chain_id,
    )
    if not chain_rows:
        return None
    chain_row = dict(chain_rows[0])
    waves = conn.execute(
        """SELECT chain_id, wave_id, ordinal, depends_on_wave_id, blocked_by_wave_id,
                  status, created_at, updated_at, started_at, completed_at
           FROM workflow_chain_waves
           WHERE chain_id = $1
           ORDER BY ordinal ASC""",
        chain_id,
    )
    dependencies = conn.execute(
        """SELECT wave_id, depends_on_wave_id
           FROM workflow_chain_wave_dependencies
           WHERE chain_id = $1
           ORDER BY wave_id ASC, depends_on_wave_id ASC""",
        chain_id,
    )
    runs = conn.execute(
        """SELECT chain_id, wave_id, ordinal, spec_path, spec_name, workflow_id,
                  spec_workflow_id, queue_id, command_id, run_id, submission_status,
                  run_status, completed_jobs, total_jobs, created_at, updated_at,
                  started_at, completed_at
           FROM workflow_chain_wave_runs
           WHERE chain_id = $1
           ORDER BY wave_id ASC, ordinal ASC""",
        chain_id,
    )
    definition = _json_loads_maybe(chain_row.get("definition"), {}) or {}
    depends_on_map: dict[str, list[str]] = {}
    for dependency_row in dependencies or []:
        wave_id = str(dependency_row.get("wave_id") or "").strip()
        depends_on_wave_id = str(dependency_row.get("depends_on_wave_id") or "").strip()
        if not wave_id or not depends_on_wave_id:
            continue
        depends_on_map.setdefault(wave_id, []).append(depends_on_wave_id)
    runs_by_wave: dict[str, list[dict[str, Any]]] = {}
    for run_row in runs or []:
        record = dict(run_row)
        runs_by_wave.setdefault(str(record["wave_id"]), []).append(record)

    wave_payloads: list[dict[str, Any]] = []
    for wave_row in waves or []:
        record = dict(wave_row)
        wave_id = str(record.get("wave_id") or "")
        normalized_depends_on = depends_on_map.get(wave_id, [])
        record["depends_on"] = normalized_depends_on
        record["depends_on_wave_id"] = normalized_depends_on[0] if len(normalized_depends_on) == 1 else None
        record["blocked_by"] = record.get("blocked_by_wave_id")
        record["runs"] = runs_by_wave.get(wave_id, [])
        wave_payloads.append(record)

    return {
        "chain_id": chain_row["chain_id"],
        "command_id": chain_row.get("command_id"),
        "coordination_path": chain_row["coordination_path"],
        "repo_root": chain_row["repo_root"],
        "program": chain_row["program"],
        "mode": chain_row.get("mode"),
        "why": chain_row.get("why"),
        "definition": definition,
        "adopt_active": bool(chain_row.get("adopt_active")),
        "status": chain_row["status"],
        "current_wave": chain_row.get("current_wave_id"),
        "requested_by_kind": chain_row.get("requested_by_kind"),
        "requested_by_ref": chain_row.get("requested_by_ref"),
        "last_error_code": chain_row.get("last_error_code"),
        "last_error_detail": chain_row.get("last_error_detail"),
        "created_at": chain_row.get("created_at"),
        "updated_at": chain_row.get("updated_at"),
        "started_at": chain_row.get("started_at"),
        "finished_at": chain_row.get("finished_at"),
        "waves": wave_payloads,
    }


def list_workflow_chains(conn: Any, *, limit: int = 20) -> list[dict[str, Any]]:
    """Return recent workflow-chain summaries."""

    rows = conn.execute(
        """SELECT chain_id, coordination_path, program, status, current_wave_id,
                  created_at, updated_at, started_at, finished_at
           FROM workflow_chains
           ORDER BY created_at DESC
           LIMIT $1""",
        limit,
    )
    return [dict(row) for row in (rows or [])]


def _recover_stale_dispatch_rows(conn: Any, *, chain_id: str | None = None) -> None:
    if chain_id:
        conn.execute(
            """UPDATE workflow_chain_wave_runs
               SET submission_status = 'pending',
                   updated_at = now()
               WHERE chain_id = $1
                 AND submission_status = 'dispatching'
                 AND run_id IS NULL
                 AND updated_at < now() - interval '60 seconds'""",
            chain_id,
        )
        return
    conn.execute(
        """UPDATE workflow_chain_wave_runs
           SET submission_status = 'pending',
               updated_at = now()
           WHERE submission_status = 'dispatching'
             AND run_id IS NULL
             AND updated_at < now() - interval '60 seconds'"""
    )


def _update_wave_run_status(
    conn: Any,
    *,
    chain_id: str,
    wave_id: str,
    spec_path: str,
    run_status: str,
    completed_jobs: int | None,
    total_jobs: int | None,
) -> None:
    next_submission_status = run_status if run_status in _WORKFLOW_TERMINAL_STATUSES else run_status
    conn.execute(
        """UPDATE workflow_chain_wave_runs
           SET run_status = $4,
               submission_status = $5,
               completed_jobs = COALESCE($6, completed_jobs),
               total_jobs = COALESCE($7, total_jobs),
               completed_at = CASE
                   WHEN $4 IN ('succeeded', 'failed', 'dead_letter', 'cancelled', 'missing')
                   THEN COALESCE(completed_at, now())
                   ELSE completed_at
               END,
               updated_at = now()
           WHERE chain_id = $1
             AND wave_id = $2
             AND spec_path = $3""",
        chain_id,
        wave_id,
        spec_path,
        run_status,
        next_submission_status,
        completed_jobs,
        total_jobs,
    )


def _refresh_running_wave_runs(conn: Any, state: dict[str, Any], wave: dict[str, Any]) -> int:
    from runtime.workflow._status import get_run_status

    actions = 0
    for run_row in wave.get("runs", []):
        run_id = str(run_row.get("run_id") or "").strip()
        if not run_id:
            continue
        run_status = get_run_status(conn, run_id)
        status_value = "missing" if run_status is None else str(run_status.get("status") or "missing")
        completed_jobs = None if run_status is None else int(run_status.get("completed_jobs") or 0)
        total_jobs = None if run_status is None else int(run_status.get("total_jobs") or 0)
        if (
            status_value != str(run_row.get("run_status") or "")
            or completed_jobs != run_row.get("completed_jobs")
            or total_jobs != run_row.get("total_jobs")
        ):
            _update_wave_run_status(
                conn,
                chain_id=state["chain_id"],
                wave_id=wave["wave_id"],
                spec_path=str(run_row["spec_path"]),
                run_status=status_value,
                completed_jobs=completed_jobs,
                total_jobs=total_jobs,
            )
            actions += 1
    return actions


def _cancel_active_wave_runs(
    conn: Any,
    state: dict[str, Any],
    wave: dict[str, Any],
    *,
    skip_spec_path: str | None = None,
) -> list[dict[str, Any]]:
    from runtime.control_commands import (
        ControlCommandType,
        ControlIntent,
        execute_control_intent,
        render_control_command_response,
    )
    from runtime.workflow._status import get_run_status

    cleanup_results: list[dict[str, Any]] = []
    chain_id = str(state["chain_id"])
    wave_id = str(wave["wave_id"])
    for run_row in wave.get("runs", []):
        spec_path = str(run_row.get("spec_path") or "").strip()
        if skip_spec_path and spec_path == skip_spec_path:
            continue
        run_id = str(run_row.get("run_id") or "").strip()
        run_status = str(run_row.get("run_status") or "").strip()
        if not run_id or run_status not in _RUN_ACTIVE_STATUSES:
            continue

        result_payload: dict[str, Any]
        try:
            command = execute_control_intent(
                conn,
                ControlIntent(
                    command_type=ControlCommandType.WORKFLOW_CANCEL,
                    requested_by_kind="system",
                    requested_by_ref=f"workflow_chain:{chain_id}",
                    idempotency_key=f"workflow.chain.cancel.{chain_id}.{wave_id}.{run_id}",
                    payload={"run_id": run_id, "include_running": True},
                ),
                approved_by="workflow.chain.cleanup",
            )
            result_payload = render_control_command_response(
                conn,
                command,
                action="cancel",
                run_id=run_id,
            )
        except Exception as exc:
            result_payload = {
                "status": "failed",
                "command_status": "failed",
                "run_id": run_id,
                "error_code": getattr(exc, "reason_code", None) or getattr(exc, "error_code", None),
                "error_detail": str(exc),
            }

        run_state = get_run_status(conn, run_id)
        status_value = "missing" if run_state is None else str(run_state.get("status") or "missing")
        completed_jobs = None if run_state is None else int(run_state.get("completed_jobs") or 0)
        total_jobs = None if run_state is None else int(run_state.get("total_jobs") or 0)
        _update_wave_run_status(
            conn,
            chain_id=chain_id,
            wave_id=wave_id,
            spec_path=spec_path,
            run_status=status_value,
            completed_jobs=completed_jobs,
            total_jobs=total_jobs,
        )
        cleanup_results.append(
            {
                "spec_path": spec_path,
                "run_id": run_id,
                "status": result_payload.get("status"),
                "command_status": result_payload.get("command_status"),
                "command_id": result_payload.get("command_id"),
                "error_code": result_payload.get("error_code"),
                "error_detail": result_payload.get("error_detail"),
                "run_status": status_value,
            }
        )
    return cleanup_results


def _mark_wave_failed_and_chain_failed(
    conn: Any,
    state: dict[str, Any],
    wave: dict[str, Any],
    *,
    error_code: str,
    error_detail: str,
    cleanup_results: list[dict[str, Any]] | None = None,
) -> None:
    chain_id = state["chain_id"]
    wave_id = wave["wave_id"]
    conn.execute(
        """UPDATE workflow_chain_waves
           SET status = 'failed',
               updated_at = now(),
               completed_at = COALESCE(completed_at, now())
           WHERE chain_id = $1
             AND wave_id = $2""",
        chain_id,
        wave_id,
    )
    conn.execute(
        """UPDATE workflow_chain_waves
           SET status = 'blocked',
               blocked_by_wave_id = $3,
               updated_at = now()
           WHERE chain_id = $1
             AND status = 'pending'
             AND ordinal > $2""",
        chain_id,
        int(wave["ordinal"]),
        wave_id,
    )
    conn.execute(
        """UPDATE workflow_chains
           SET status = 'failed',
               current_wave_id = $2,
               last_error_code = $3,
               last_error_detail = $4,
               updated_at = now(),
               finished_at = COALESCE(finished_at, now())
           WHERE chain_id = $1""",
        chain_id,
        wave_id,
        error_code,
        error_detail,
    )
    payload = {
        "chain_id": chain_id,
        "wave_id": wave_id,
        "error_code": error_code,
        "error_detail": error_detail,
    }
    if cleanup_results:
        payload["cleanup"] = cleanup_results
    _emit_chain_event(
        conn,
        event_type="workflow_chain.failed",
        chain_id=chain_id,
        payload=payload,
    )


def _mark_wave_cancelled_and_chain_cancelled(
    conn: Any,
    state: dict[str, Any],
    wave: dict[str, Any],
    *,
    error_code: str,
    error_detail: str,
    cleanup_results: list[dict[str, Any]] | None = None,
) -> None:
    chain_id = state["chain_id"]
    wave_id = wave["wave_id"]
    conn.execute(
        """UPDATE workflow_chain_waves
           SET status = 'cancelled',
               updated_at = now(),
               completed_at = COALESCE(completed_at, now())
           WHERE chain_id = $1
             AND wave_id = $2""",
        chain_id,
        wave_id,
    )
    conn.execute(
        """UPDATE workflow_chain_waves
           SET status = 'cancelled',
               blocked_by_wave_id = $3,
               updated_at = now(),
               completed_at = COALESCE(completed_at, now())
           WHERE chain_id = $1
             AND status IN ('pending', 'blocked')
             AND ordinal > $2""",
        chain_id,
        int(wave["ordinal"]),
        wave_id,
    )
    conn.execute(
        """UPDATE workflow_chains
           SET status = 'cancelled',
               current_wave_id = $2,
               last_error_code = $3,
               last_error_detail = $4,
               updated_at = now(),
               finished_at = COALESCE(finished_at, now())
           WHERE chain_id = $1""",
        chain_id,
        wave_id,
        error_code,
        error_detail,
    )
    payload = {
        "chain_id": chain_id,
        "wave_id": wave_id,
        "error_code": error_code,
        "error_detail": error_detail,
    }
    if cleanup_results:
        payload["cleanup"] = cleanup_results
    _emit_chain_event(
        conn,
        event_type="workflow_chain.cancelled",
        chain_id=chain_id,
        payload=payload,
    )


def _mark_wave_succeeded(conn: Any, chain_id: str, wave_id: str) -> None:
    conn.execute(
        """UPDATE workflow_chain_waves
           SET status = 'succeeded',
               blocked_by_wave_id = NULL,
               updated_at = now(),
               completed_at = COALESCE(completed_at, now())
           WHERE chain_id = $1
             AND wave_id = $2""",
        chain_id,
        wave_id,
    )
    conn.execute(
        """UPDATE workflow_chains
           SET current_wave_id = NULL,
               updated_at = now(),
               started_at = COALESCE(started_at, now())
           WHERE chain_id = $1""",
        chain_id,
    )
    _emit_chain_event(
        conn,
        event_type="workflow_chain.wave_succeeded",
        chain_id=chain_id,
        payload={"chain_id": chain_id, "wave_id": wave_id},
    )


def _mark_chain_succeeded(conn: Any, chain_id: str) -> None:
    conn.execute(
        """UPDATE workflow_chains
           SET status = 'succeeded',
               current_wave_id = NULL,
               updated_at = now(),
               finished_at = COALESCE(finished_at, now())
           WHERE chain_id = $1""",
        chain_id,
    )
    _emit_chain_event(
        conn,
        event_type="workflow_chain.succeeded",
        chain_id=chain_id,
        payload={"chain_id": chain_id},
    )


def _try_start_next_wave(conn: Any, state: dict[str, Any]) -> str | None:
    succeeded_waves = {
        str(wave["wave_id"])
        for wave in state["waves"]
        if str(wave.get("status") or "") == "succeeded"
    }
    for wave in state["waves"]:
        if str(wave.get("status") or "") != "pending":
            continue
        depends_on = [item for item in wave.get("depends_on", []) if item]
        if not set(depends_on).issubset(succeeded_waves):
            continue
        rows = conn.execute(
            """UPDATE workflow_chain_waves
               SET status = 'running',
                   blocked_by_wave_id = NULL,
                   updated_at = now(),
                   started_at = COALESCE(started_at, now())
               WHERE chain_id = $1
                 AND wave_id = $2
                 AND status = 'pending'
               RETURNING wave_id""",
            state["chain_id"],
            wave["wave_id"],
        )
        if not rows:
            return None
        conn.execute(
            """UPDATE workflow_chains
               SET status = 'running',
                   current_wave_id = $2,
                   updated_at = now(),
                   started_at = COALESCE(started_at, now())
               WHERE chain_id = $1""",
            state["chain_id"],
            wave["wave_id"],
        )
        _emit_chain_event(
            conn,
            event_type="workflow_chain.wave_started",
            chain_id=state["chain_id"],
            payload={"chain_id": state["chain_id"], "wave_id": wave["wave_id"]},
        )
        return str(rows[0]["wave_id"])
    return None


def _dispatch_run_id(chain_id: str, wave_id: str, ordinal: int) -> str:
    from runtime.workflow._shared import _slugify

    return f"workflow.chain.{chain_id[-12:]}.{_slugify(wave_id)}.{ordinal:03d}"


def _submit_wave_runs(conn: Any, state: dict[str, Any], wave: dict[str, Any]) -> int:
    from runtime.control_commands import (
        render_workflow_submit_response,
        request_workflow_submit_command,
    )

    actions = 0
    chain_id = state["chain_id"]
    adopt_active = bool(state.get("adopt_active"))
    for run_row in wave.get("runs", []):
        spec_path = str(run_row["spec_path"])
        if str(run_row.get("run_id") or "").strip():
            continue
        claim_rows = conn.execute(
            """UPDATE workflow_chain_wave_runs
               SET submission_status = 'dispatching',
                   started_at = COALESCE(started_at, now()),
                   updated_at = now()
               WHERE chain_id = $1
                 AND wave_id = $2
                 AND spec_path = $3
                 AND submission_status = 'pending'
               RETURNING ordinal, spec_name, workflow_id, spec_workflow_id, queue_id, total_jobs""",
            chain_id,
            wave["wave_id"],
            spec_path,
        )
        if not claim_rows:
            continue
        claimed = dict(claim_rows[0])
        workflow_id = str(claimed["workflow_id"])
        queue_id = claimed.get("queue_id")
        if adopt_active:
            adopted = find_active_run_for_workflow_id(
                conn,
                workflow_id,
                queue_id=str(queue_id) if queue_id else None,
            )
            if adopted is not None:
                conn.execute(
                    """UPDATE workflow_chain_wave_runs
                       SET command_id = NULL,
                           run_id = $4,
                           submission_status = 'adopted_active',
                           run_status = $5,
                           updated_at = now()
                       WHERE chain_id = $1
                         AND wave_id = $2
                         AND spec_path = $3""",
                    chain_id,
                    wave["wave_id"],
                    spec_path,
                    adopted["run_id"],
                    adopted.get("current_state") or "queued",
                )
                actions += 1
                continue

        submission = render_workflow_submit_response(
            request_workflow_submit_command(
                conn,
                requested_by_kind="system",
                requested_by_ref=f"workflow_chain:{chain_id}",
                spec_path=spec_path,
                repo_root=state["repo_root"],
                run_id=_dispatch_run_id(chain_id, str(wave["wave_id"]), int(claimed["ordinal"])),
                idempotency_key=f"workflow.chain.submit.{chain_id}.{wave['wave_id']}.{claimed['ordinal']}",
            ),
            spec_name=str(claimed["spec_name"]),
            total_jobs=int(claimed["total_jobs"] or 0),
        )
        run_id = submission.get("run_id")
        status_value = str(submission.get("status") or "failed")
        if not isinstance(run_id, str) or not run_id.strip():
            conn.execute(
                """UPDATE workflow_chain_wave_runs
                   SET submission_status = 'failed',
                       run_status = 'failed',
                       updated_at = now(),
                       completed_at = COALESCE(completed_at, now())
                   WHERE chain_id = $1
                     AND wave_id = $2
                     AND spec_path = $3""",
                chain_id,
                wave["wave_id"],
                spec_path,
            )
            _mark_wave_failed_and_chain_failed(
                conn,
                state,
                wave,
                error_code=str(submission.get("error_code") or "workflow.chain.submit_failed"),
                error_detail=str(submission.get("error_detail") or "workflow chain submit failed"),
            )
            return actions + 1

        conn.execute(
            """UPDATE workflow_chain_wave_runs
               SET command_id = $4,
                   run_id = $5,
                   submission_status = $6,
                   run_status = $7,
                   updated_at = now()
               WHERE chain_id = $1
                 AND wave_id = $2
                 AND spec_path = $3""",
            chain_id,
            wave["wave_id"],
            spec_path,
            submission.get("command_id"),
            run_id,
            status_value,
            status_value if status_value in _WORKFLOW_TERMINAL_STATUSES else "queued",
        )
        actions += 1
    return actions


def advance_workflow_chains(conn: Any, *, chain_id: str | None = None) -> int:
    """Advance durable workflow chains from queued/running state."""

    _recover_stale_dispatch_rows(conn, chain_id=chain_id)
    if chain_id:
        target_ids = [chain_id]
    else:
        rows = conn.execute(
            """SELECT chain_id
               FROM workflow_chains
               WHERE status IN ('queued', 'running')
               ORDER BY created_at ASC
               LIMIT 20"""
        )
        target_ids = [str(row["chain_id"]) for row in (rows or [])]

    actions = 0
    for target_chain_id in target_ids:
        while True:
            state = get_workflow_chain_status(conn, target_chain_id)
            if state is None or state["status"] in _CHAIN_TERMINAL_STATUSES:
                break

            running_wave = next((wave for wave in state["waves"] if wave["status"] == "running"), None)
            if running_wave is not None:
                actions += _refresh_running_wave_runs(conn, state, running_wave)
                state = get_workflow_chain_status(conn, target_chain_id) or state
                running_wave = next((wave for wave in state["waves"] if wave["status"] == "running"), None)
                if running_wave is None:
                    continue

                cancelled_runs = [
                    run_row
                    for run_row in running_wave.get("runs", [])
                    if str(run_row.get("run_status") or "") in _RUN_CANCELLATION_STATUSES
                ]
                if cancelled_runs:
                    cancelled_run = cancelled_runs[0]
                    cleanup_results = _cancel_active_wave_runs(
                        conn,
                        state,
                        running_wave,
                        skip_spec_path=str(cancelled_run["spec_path"]),
                    )
                    actions += len(cleanup_results)
                    _mark_wave_cancelled_and_chain_cancelled(
                        conn,
                        state,
                        running_wave,
                        error_code="workflow.chain.wave_cancelled",
                        error_detail=(
                            f"wave {running_wave['wave_id']} cancelled because "
                            f"{cancelled_run['spec_path']} reached {cancelled_run['run_status']}"
                        ),
                        cleanup_results=cleanup_results,
                    )
                    actions += 1
                    break

                failing_runs = [
                    run_row
                    for run_row in running_wave.get("runs", [])
                    if str(run_row.get("run_status") or "") in _RUN_FAILURE_STATUSES
                ]
                if failing_runs:
                    failed_run = failing_runs[0]
                    cleanup_results = _cancel_active_wave_runs(
                        conn,
                        state,
                        running_wave,
                        skip_spec_path=str(failed_run["spec_path"]),
                    )
                    actions += len(cleanup_results)
                    _mark_wave_failed_and_chain_failed(
                        conn,
                        state,
                        running_wave,
                        error_code="workflow.chain.wave_failed",
                        error_detail=(
                            f"wave {running_wave['wave_id']} failed because "
                            f"{failed_run['spec_path']} reached {failed_run['run_status']}"
                        ),
                        cleanup_results=cleanup_results,
                    )
                    actions += 1
                    break

                run_rows = running_wave.get("runs", [])
                if run_rows and all(str(run_row.get("run_status") or "") == "succeeded" for run_row in run_rows):
                    _mark_wave_succeeded(conn, state["chain_id"], running_wave["wave_id"])
                    actions += 1
                    continue

                actions += _submit_wave_runs(conn, state, running_wave)
                break

            started_wave_id = _try_start_next_wave(conn, state)
            if started_wave_id is None:
                if state["waves"] and all(wave["status"] == "succeeded" for wave in state["waves"]):
                    _mark_chain_succeeded(conn, state["chain_id"])
                    actions += 1
                break

            actions += 1
            state = get_workflow_chain_status(conn, target_chain_id) or state
            running_wave = next((wave for wave in state["waves"] if wave["status"] == "running"), None)
            if running_wave is None:
                break
            actions += _submit_wave_runs(conn, state, running_wave)
            break
    return actions


__all__ = [
    "WorkflowChainError",
    "WorkflowChainProgram",
    "WorkflowChainWave",
    "advance_workflow_chains",
    "bootstrap_workflow_chain_schema",
    "find_active_run_for_workflow_id",
    "get_workflow_chain_status",
    "iter_chain_spec_paths",
    "list_workflow_chains",
    "load_workflow_chain",
    "submit_workflow_chain",
    "validate_workflow_chain",
    "workflow_chain_id_from_result_ref",
]
