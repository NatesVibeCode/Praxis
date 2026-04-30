"""Postgres persistence for Virtual Lab simulation authority."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from typing import Any

from .validators import (
    PostgresWriteError,
    _encode_jsonb,
    _optional_text,
    _require_mapping,
    _require_text,
)


def _normalize_row(row: Any, *, operation: str) -> dict[str, Any]:
    if row is None:
        raise PostgresWriteError(
            "virtual_lab_simulation.write_failed",
            f"{operation} returned no row",
        )
    payload = dict(row)
    for key, value in list(payload.items()):
        if isinstance(value, str) and (key.endswith("_json") or key.endswith("_refs_json")):
            try:
                payload[key] = json.loads(value)
            except (TypeError, json.JSONDecodeError):
                continue
    return payload


def _normalize_optional_row(row: Any, *, operation: str) -> dict[str, Any] | None:
    if row is None:
        return None
    return _normalize_row(row, operation=operation)


def _normalize_rows(rows: Any, *, operation: str) -> list[dict[str, Any]]:
    return [_normalize_row(row, operation=operation) for row in (rows or [])]


def _timestamp(value: Any, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError as exc:
            raise PostgresWriteError(
                "virtual_lab_simulation.invalid_timestamp",
                f"{field_name} must be an ISO timestamp",
                details={"field_name": field_name, "value": value},
            ) from exc
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    raise PostgresWriteError(
        "virtual_lab_simulation.invalid_timestamp",
        f"{field_name} must be an ISO timestamp",
        details={"field_name": field_name, "value": value},
    )


def _optional_clean_text(value: object, *, field_name: str) -> str | None:
    if value is None or value == "":
        return None
    return _optional_text(value, field_name=field_name)


def _list_payloads(value: object, *, field_name: str) -> list[dict[str, Any]]:
    if value is None:
        return []
    if not isinstance(value, list) or not all(isinstance(item, dict) for item in value):
        raise PostgresWriteError(
            "virtual_lab_simulation.invalid_payload",
            f"{field_name} must be a list of JSON objects",
            details={"field_name": field_name},
        )
    return [dict(item) for item in value]


def _list_text(value: object, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise PostgresWriteError(
            "virtual_lab_simulation.invalid_refs",
            f"{field_name} must be a list of strings",
            details={"field_name": field_name},
        )
    return [str(item).strip() for item in value if str(item).strip()]


def persist_virtual_lab_simulation_run(
    conn: Any,
    *,
    scenario: dict[str, Any],
    result: dict[str, Any],
    task_contract_ref: str | None = None,
    integration_action_contract_refs: list[str] | None = None,
    automation_snapshot_refs: list[str] | None = None,
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
) -> dict[str, Any]:
    scenario_payload = dict(_require_mapping(scenario, field_name="scenario"))
    result_payload = dict(_require_mapping(result, field_name="result"))
    initial_state = dict(_require_mapping(scenario_payload.get("initial_state"), field_name="scenario.initial_state"))
    revision = dict(_require_mapping(initial_state.get("revision"), field_name="scenario.initial_state.revision"))
    trace = dict(_require_mapping(result_payload.get("trace"), field_name="result.trace"))

    environment_id = _require_text(revision.get("environment_id"), field_name="revision.environment_id")
    revision_id = _require_text(revision.get("revision_id"), field_name="revision.revision_id")
    run_id = _require_text(result_payload.get("run_id"), field_name="result.run_id")
    scenario_id = _require_text(result_payload.get("scenario_id"), field_name="result.scenario_id")

    runtime_events = _list_payloads(trace.get("events"), field_name="trace.events")
    state_events = _list_payloads(trace.get("state_events"), field_name="trace.state_events")
    transitions = _list_payloads(trace.get("transitions"), field_name="trace.transitions")
    automation_evaluations = _list_payloads(
        trace.get("automation_evaluations"),
        field_name="trace.automation_evaluations",
    )
    automation_firings = _list_payloads(trace.get("automation_firings"), field_name="trace.automation_firings")
    action_results = _list_payloads(result_payload.get("action_results"), field_name="result.action_results")
    assertion_results = _list_payloads(result_payload.get("assertion_results"), field_name="result.assertion_results")
    verifier_results = _list_payloads(result_payload.get("verifier_results"), field_name="result.verifier_results")
    gaps = _list_payloads(result_payload.get("gaps"), field_name="result.gaps")
    blockers = _list_payloads(result_payload.get("blockers"), field_name="result.blockers")

    run_row = conn.fetchrow(
        """
        INSERT INTO virtual_lab_simulation_runs (
            run_id,
            scenario_id,
            scenario_digest,
            config_digest,
            environment_id,
            revision_id,
            revision_digest,
            status,
            stop_reason,
            trace_digest,
            result_digest,
            runtime_version,
            action_count,
            runtime_event_count,
            state_event_count,
            transition_count,
            automation_evaluation_count,
            automation_firing_count,
            assertion_count,
            verifier_count,
            typed_gap_count,
            blocker_count,
            task_contract_ref,
            integration_action_contract_refs_json,
            automation_snapshot_refs_json,
            scenario_json,
            result_json,
            observed_by_ref,
            source_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
            $11, $12, $13, $14, $15, $16, $17, $18, $19,
            $20, $21, $22, $23, $24::jsonb, $25::jsonb, $26::jsonb,
            $27::jsonb, $28, $29
        )
        ON CONFLICT (run_id) DO UPDATE SET
            scenario_id = EXCLUDED.scenario_id,
            scenario_digest = EXCLUDED.scenario_digest,
            config_digest = EXCLUDED.config_digest,
            environment_id = EXCLUDED.environment_id,
            revision_id = EXCLUDED.revision_id,
            revision_digest = EXCLUDED.revision_digest,
            status = EXCLUDED.status,
            stop_reason = EXCLUDED.stop_reason,
            trace_digest = EXCLUDED.trace_digest,
            result_digest = EXCLUDED.result_digest,
            runtime_version = EXCLUDED.runtime_version,
            action_count = EXCLUDED.action_count,
            runtime_event_count = EXCLUDED.runtime_event_count,
            state_event_count = EXCLUDED.state_event_count,
            transition_count = EXCLUDED.transition_count,
            automation_evaluation_count = EXCLUDED.automation_evaluation_count,
            automation_firing_count = EXCLUDED.automation_firing_count,
            assertion_count = EXCLUDED.assertion_count,
            verifier_count = EXCLUDED.verifier_count,
            typed_gap_count = EXCLUDED.typed_gap_count,
            blocker_count = EXCLUDED.blocker_count,
            task_contract_ref = EXCLUDED.task_contract_ref,
            integration_action_contract_refs_json = EXCLUDED.integration_action_contract_refs_json,
            automation_snapshot_refs_json = EXCLUDED.automation_snapshot_refs_json,
            scenario_json = EXCLUDED.scenario_json,
            result_json = EXCLUDED.result_json,
            observed_by_ref = EXCLUDED.observed_by_ref,
            source_ref = EXCLUDED.source_ref,
            updated_at = now()
        RETURNING *
        """,
        run_id,
        scenario_id,
        _require_text(scenario_payload.get("scenario_digest"), field_name="scenario.scenario_digest"),
        _require_text(scenario_payload.get("config", {}).get("config_digest"), field_name="scenario.config.config_digest"),
        environment_id,
        revision_id,
        _require_text(revision.get("revision_digest"), field_name="revision.revision_digest"),
        _require_text(result_payload.get("status"), field_name="result.status"),
        _require_text(result_payload.get("stop_reason"), field_name="result.stop_reason"),
        _require_text(trace.get("trace_digest"), field_name="trace.trace_digest"),
        _require_text(result_payload.get("result_digest"), field_name="result.result_digest"),
        _require_text(result_payload.get("runtime_version"), field_name="result.runtime_version"),
        len(action_results),
        len(runtime_events),
        len(state_events),
        len(transitions),
        len(automation_evaluations),
        len(automation_firings),
        len(assertion_results),
        len(verifier_results),
        len(gaps),
        len(blockers),
        _optional_clean_text(task_contract_ref, field_name="task_contract_ref"),
        _encode_jsonb(_list_text(integration_action_contract_refs, field_name="integration_action_contract_refs"), field_name="integration_action_contract_refs"),
        _encode_jsonb(_list_text(automation_snapshot_refs, field_name="automation_snapshot_refs"), field_name="automation_snapshot_refs"),
        _encode_jsonb(scenario_payload, field_name="scenario"),
        _encode_jsonb(result_payload, field_name="result"),
        _optional_clean_text(observed_by_ref, field_name="observed_by_ref"),
        _optional_clean_text(source_ref, field_name="source_ref"),
    )

    _replace_child_rows(conn, "virtual_lab_simulation_runtime_events", run_id, runtime_events, _runtime_event_args)
    _replace_child_rows(conn, "virtual_lab_simulation_state_events", run_id, state_events, _state_event_args)
    _replace_child_rows(conn, "virtual_lab_simulation_transitions", run_id, transitions, _transition_args)
    _replace_child_rows(conn, "virtual_lab_simulation_action_results", run_id, action_results, _action_result_args)
    _replace_child_rows(
        conn,
        "virtual_lab_simulation_automation_evaluations",
        run_id,
        automation_evaluations,
        _automation_evaluation_args,
    )
    _replace_child_rows(conn, "virtual_lab_simulation_automation_firings", run_id, automation_firings, _automation_firing_args)
    _replace_child_rows(conn, "virtual_lab_simulation_assertion_results", run_id, assertion_results, _assertion_result_args)
    _replace_child_rows(conn, "virtual_lab_simulation_verifier_results", run_id, verifier_results, _verifier_result_args)
    _replace_child_rows(conn, "virtual_lab_simulation_typed_gaps", run_id, gaps, _gap_args)
    _replace_child_rows(conn, "virtual_lab_simulation_promotion_blockers", run_id, blockers, _blocker_args)

    return {
        **_normalize_row(run_row, operation="persist_virtual_lab_simulation_run"),
        "runtime_event_count": len(runtime_events),
        "state_event_count": len(state_events),
        "transition_count": len(transitions),
        "action_count": len(action_results),
        "automation_evaluation_count": len(automation_evaluations),
        "automation_firing_count": len(automation_firings),
        "assertion_count": len(assertion_results),
        "verifier_count": len(verifier_results),
        "typed_gap_count": len(gaps),
        "blocker_count": len(blockers),
    }


def _replace_child_rows(conn: Any, table: str, run_id: str, rows: list[dict[str, Any]], arg_builder) -> None:
    conn.execute(f"DELETE FROM {table} WHERE run_id = $1", run_id)
    if not rows:
        return
    sql, batch = arg_builder(run_id, rows)
    conn.execute_many(sql, batch)


def _runtime_event_args(run_id: str, rows: list[dict[str, Any]]) -> tuple[str, list[tuple[object, ...]]]:
    return (
        """
        INSERT INTO virtual_lab_simulation_runtime_events (
            run_id, event_id, sequence_number, event_type, occurred_at,
            source_area, causation_id, correlation_id, event_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
        """,
        [
            (
                run_id,
                _require_text(row.get("event_id"), field_name="runtime_event.event_id"),
                int(row.get("sequence_number")),
                _require_text(row.get("event_type"), field_name="runtime_event.event_type"),
                _timestamp(row.get("occurred_at"), field_name="runtime_event.occurred_at"),
                _require_text(row.get("source_area"), field_name="runtime_event.source_area"),
                _optional_clean_text(row.get("causation_id"), field_name="runtime_event.causation_id"),
                _require_text(row.get("correlation_id"), field_name="runtime_event.correlation_id"),
                _encode_jsonb(row, field_name="runtime_event"),
            )
            for row in rows
        ],
    )


def _state_event_args(run_id: str, rows: list[dict[str, Any]]) -> tuple[str, list[tuple[object, ...]]]:
    return (
        """
        INSERT INTO virtual_lab_simulation_state_events (
            run_id, event_id, environment_id, revision_id, stream_id,
            event_type, sequence_number, command_id, pre_state_digest,
            post_state_digest, event_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::jsonb)
        """,
        [
            (
                run_id,
                _require_text(row.get("event_id"), field_name="state_event.event_id"),
                _require_text(row.get("environment_id"), field_name="state_event.environment_id"),
                _require_text(row.get("revision_id"), field_name="state_event.revision_id"),
                _require_text(row.get("stream_id"), field_name="state_event.stream_id"),
                _require_text(row.get("event_type"), field_name="state_event.event_type"),
                int(row.get("sequence_number")),
                _require_text(row.get("command_id"), field_name="state_event.command_id"),
                _require_text(row.get("pre_state_digest"), field_name="state_event.pre_state_digest"),
                _require_text(row.get("post_state_digest"), field_name="state_event.post_state_digest"),
                _encode_jsonb(row, field_name="state_event"),
            )
            for row in rows
        ],
    )


def _transition_args(run_id: str, rows: list[dict[str, Any]]) -> tuple[str, list[tuple[object, ...]]]:
    return (
        """
        INSERT INTO virtual_lab_simulation_transitions (
            run_id, transition_id, object_id, instance_id, event_id, event_type,
            sequence_number, pre_state_digest, post_state_digest, causation_id,
            action_id, transition_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12::jsonb)
        """,
        [
            (
                run_id,
                _require_text(row.get("transition_id"), field_name="transition.transition_id"),
                _require_text(row.get("object_id"), field_name="transition.object_id"),
                _require_text(row.get("instance_id"), field_name="transition.instance_id"),
                _require_text(row.get("event_id"), field_name="transition.event_id"),
                _require_text(row.get("event_type"), field_name="transition.event_type"),
                int(row.get("sequence_number")),
                _require_text(row.get("pre_state_digest"), field_name="transition.pre_state_digest"),
                _require_text(row.get("post_state_digest"), field_name="transition.post_state_digest"),
                _optional_clean_text(row.get("causation_id"), field_name="transition.causation_id"),
                _require_text(row.get("action_id"), field_name="transition.action_id"),
                _encode_jsonb(row, field_name="transition"),
            )
            for row in rows
        ],
    )


def _action_result_args(run_id: str, rows: list[dict[str, Any]]) -> tuple[str, list[tuple[object, ...]]]:
    return (
        """
        INSERT INTO virtual_lab_simulation_action_results (
            run_id, action_id, action_kind, status, command_id,
            receipt_status, result_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        """,
        [
            (
                run_id,
                _require_text(row.get("action_id"), field_name="action_result.action_id"),
                _require_text(row.get("action_kind"), field_name="action_result.action_kind"),
                _require_text(row.get("status"), field_name="action_result.status"),
                _require_text(row.get("command_id"), field_name="action_result.command_id"),
                _optional_clean_text(row.get("receipt_status"), field_name="action_result.receipt_status"),
                _encode_jsonb(row, field_name="action_result"),
            )
            for row in rows
        ],
    )


def _automation_evaluation_args(run_id: str, rows: list[dict[str, Any]]) -> tuple[str, list[tuple[object, ...]]]:
    return (
        """
        INSERT INTO virtual_lab_simulation_automation_evaluations (
            run_id, rule_id, triggering_event_id, eligible, reason_code,
            evaluation_json
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        """,
        [
            (
                run_id,
                _require_text(row.get("rule_id"), field_name="automation_evaluation.rule_id"),
                _require_text(row.get("triggering_event_id"), field_name="automation_evaluation.triggering_event_id"),
                bool(row.get("eligible")),
                _require_text(row.get("reason_code"), field_name="automation_evaluation.reason_code"),
                _encode_jsonb(row, field_name="automation_evaluation"),
            )
            for row in rows
        ],
    )


def _automation_firing_args(run_id: str, rows: list[dict[str, Any]]) -> tuple[str, list[tuple[object, ...]]]:
    return (
        """
        INSERT INTO virtual_lab_simulation_automation_firings (
            run_id, firing_id, rule_id, triggering_event_id,
            recursion_depth, firing_json
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        """,
        [
            (
                run_id,
                _require_text(row.get("firing_id"), field_name="automation_firing.firing_id"),
                _require_text(row.get("rule_id"), field_name="automation_firing.rule_id"),
                _require_text(row.get("triggering_event_id"), field_name="automation_firing.triggering_event_id"),
                int(row.get("recursion_depth")),
                _encode_jsonb(row, field_name="automation_firing"),
            )
            for row in rows
        ],
    )


def _assertion_result_args(run_id: str, rows: list[dict[str, Any]]) -> tuple[str, list[tuple[object, ...]]]:
    return (
        """
        INSERT INTO virtual_lab_simulation_assertion_results (
            run_id, assertion_id, assertion_kind, passed, severity,
            result_json
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        """,
        [
            (
                run_id,
                _require_text(row.get("assertion_id"), field_name="assertion_result.assertion_id"),
                _require_text(row.get("assertion_kind"), field_name="assertion_result.assertion_kind"),
                bool(row.get("passed")),
                _require_text(row.get("severity"), field_name="assertion_result.severity"),
                _encode_jsonb(row, field_name="assertion_result"),
            )
            for row in rows
        ],
    )


def _verifier_result_args(run_id: str, rows: list[dict[str, Any]]) -> tuple[str, list[tuple[object, ...]]]:
    return (
        """
        INSERT INTO virtual_lab_simulation_verifier_results (
            run_id, verifier_id, verifier_kind, status, severity,
            result_json
        ) VALUES ($1, $2, $3, $4, $5, $6::jsonb)
        """,
        [
            (
                run_id,
                _require_text(row.get("verifier_id"), field_name="verifier_result.verifier_id"),
                _require_text(row.get("verifier_kind"), field_name="verifier_result.verifier_kind"),
                _require_text(row.get("status"), field_name="verifier_result.status"),
                _require_text(row.get("severity"), field_name="verifier_result.severity"),
                _encode_jsonb(row, field_name="verifier_result"),
            )
            for row in rows
        ],
    )


def _gap_args(run_id: str, rows: list[dict[str, Any]]) -> tuple[str, list[tuple[object, ...]]]:
    return (
        """
        INSERT INTO virtual_lab_simulation_typed_gaps (
            run_id, gap_id, code, severity, source_area, trace_event_id,
            gap_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        """,
        [
            (
                run_id,
                _require_text(row.get("gap_id"), field_name="gap.gap_id"),
                _require_text(row.get("code"), field_name="gap.code"),
                _require_text(row.get("severity"), field_name="gap.severity"),
                _require_text(row.get("source_area"), field_name="gap.source_area"),
                _optional_clean_text(row.get("trace_event_id"), field_name="gap.trace_event_id"),
                _encode_jsonb(row, field_name="gap"),
            )
            for row in rows
        ],
    )


def _blocker_args(run_id: str, rows: list[dict[str, Any]]) -> tuple[str, list[tuple[object, ...]]]:
    return (
        """
        INSERT INTO virtual_lab_simulation_promotion_blockers (
            run_id, blocker_id, code, source_area, gap_id, trace_event_id,
            blocker_json
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb)
        """,
        [
            (
                run_id,
                _require_text(row.get("blocker_id"), field_name="blocker.blocker_id"),
                _require_text(row.get("code"), field_name="blocker.code"),
                _require_text(row.get("source_area"), field_name="blocker.source_area"),
                _optional_clean_text(row.get("gap_id"), field_name="blocker.gap_id"),
                _optional_clean_text(row.get("trace_event_id"), field_name="blocker.trace_event_id"),
                _encode_jsonb(row, field_name="blocker"),
            )
            for row in rows
        ],
    )


def list_virtual_lab_simulation_runs(
    conn: Any,
    *,
    status: str | None = None,
    scenario_id: str | None = None,
    environment_id: str | None = None,
    revision_id: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses = ["TRUE"]
    args: list[Any] = []
    if status:
        args.append(status)
        clauses.append(f"status = ${len(args)}")
    if scenario_id:
        args.append(scenario_id)
        clauses.append(f"scenario_id = ${len(args)}")
    if environment_id:
        args.append(environment_id)
        clauses.append(f"environment_id = ${len(args)}")
    if revision_id:
        args.append(revision_id)
        clauses.append(f"revision_id = ${len(args)}")
    args.append(max(1, min(int(limit), 500)))
    rows = conn.fetch(
        f"""
        SELECT *
          FROM virtual_lab_simulation_runs
         WHERE {' AND '.join(clauses)}
         ORDER BY updated_at DESC, run_id
         LIMIT ${len(args)}
        """,
        *args,
    )
    return _normalize_rows(rows, operation="list_virtual_lab_simulation_runs")


def load_virtual_lab_simulation_run(
    conn: Any,
    *,
    run_id: str,
    include_events: bool = True,
    include_state_events: bool = True,
    include_transitions: bool = True,
    include_actions: bool = True,
    include_automation: bool = True,
    include_assertions: bool = True,
    include_verifiers: bool = True,
    include_gaps: bool = True,
    include_blockers: bool = True,
) -> dict[str, Any] | None:
    run = _normalize_optional_row(
        conn.fetchrow("SELECT * FROM virtual_lab_simulation_runs WHERE run_id = $1", run_id),
        operation="load_virtual_lab_simulation_run",
    )
    if run is None:
        return None
    if include_events:
        run["runtime_events"] = list_virtual_lab_simulation_events(conn, run_id=run_id, limit=500)
    if include_state_events:
        run["state_events"] = _fetch_child_json(
            conn,
            table="virtual_lab_simulation_state_events",
            json_column="event_json",
            run_id=run_id,
            order_by="event_id",
        )
    if include_transitions:
        run["transitions"] = _fetch_child_json(
            conn,
            table="virtual_lab_simulation_transitions",
            json_column="transition_json",
            run_id=run_id,
            order_by="sequence_number",
        )
    if include_actions:
        run["action_results"] = _fetch_child_json(
            conn,
            table="virtual_lab_simulation_action_results",
            json_column="result_json",
            run_id=run_id,
            order_by="created_at",
        )
    if include_automation:
        run["automation_evaluations"] = _fetch_child_json(
            conn,
            table="virtual_lab_simulation_automation_evaluations",
            json_column="evaluation_json",
            run_id=run_id,
            order_by="created_at",
        )
        run["automation_firings"] = _fetch_child_json(
            conn,
            table="virtual_lab_simulation_automation_firings",
            json_column="firing_json",
            run_id=run_id,
            order_by="created_at",
        )
    if include_assertions:
        run["assertion_results"] = _fetch_child_json(
            conn,
            table="virtual_lab_simulation_assertion_results",
            json_column="result_json",
            run_id=run_id,
            order_by="created_at",
        )
    if include_verifiers:
        run["verifier_results"] = list_virtual_lab_simulation_verifiers(conn, run_id=run_id, limit=500)
    if include_gaps:
        run["typed_gaps"] = _fetch_child_json(
            conn,
            table="virtual_lab_simulation_typed_gaps",
            json_column="gap_json",
            run_id=run_id,
            order_by="created_at",
        )
    if include_blockers:
        run["promotion_blockers"] = list_virtual_lab_simulation_blockers(conn, run_id=run_id, limit=500)
    return run


def list_virtual_lab_simulation_events(
    conn: Any,
    *,
    run_id: str,
    event_type: str | None = None,
    source_area: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses = ["run_id = $1"]
    args: list[Any] = [run_id]
    if event_type:
        args.append(event_type)
        clauses.append(f"event_type = ${len(args)}")
    if source_area:
        args.append(source_area)
        clauses.append(f"source_area = ${len(args)}")
    args.append(max(1, min(int(limit), 500)))
    rows = conn.fetch(
        f"""
        SELECT event_json
          FROM virtual_lab_simulation_runtime_events
         WHERE {' AND '.join(clauses)}
         ORDER BY sequence_number ASC
         LIMIT ${len(args)}
        """,
        *args,
    )
    return [row["event_json"] for row in _normalize_rows(rows, operation="list_virtual_lab_simulation_events")]


def list_virtual_lab_simulation_verifiers(
    conn: Any,
    *,
    run_id: str,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses = ["run_id = $1"]
    args: list[Any] = [run_id]
    if status:
        args.append(status)
        clauses.append(f"status = ${len(args)}")
    args.append(max(1, min(int(limit), 500)))
    rows = conn.fetch(
        f"""
        SELECT result_json
          FROM virtual_lab_simulation_verifier_results
         WHERE {' AND '.join(clauses)}
         ORDER BY verifier_id
         LIMIT ${len(args)}
        """,
        *args,
    )
    return [row["result_json"] for row in _normalize_rows(rows, operation="list_virtual_lab_simulation_verifiers")]


def list_virtual_lab_simulation_blockers(
    conn: Any,
    *,
    run_id: str,
    code: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    clauses = ["run_id = $1"]
    args: list[Any] = [run_id]
    if code:
        args.append(code)
        clauses.append(f"code = ${len(args)}")
    args.append(max(1, min(int(limit), 500)))
    rows = conn.fetch(
        f"""
        SELECT blocker_json
          FROM virtual_lab_simulation_promotion_blockers
         WHERE {' AND '.join(clauses)}
         ORDER BY blocker_id
         LIMIT ${len(args)}
        """,
        *args,
    )
    return [row["blocker_json"] for row in _normalize_rows(rows, operation="list_virtual_lab_simulation_blockers")]


def _fetch_child_json(
    conn: Any,
    *,
    table: str,
    json_column: str,
    run_id: str,
    order_by: str,
) -> list[dict[str, Any]]:
    rows = conn.fetch(
        f"SELECT {json_column} FROM {table} WHERE run_id = $1 ORDER BY {order_by}",
        run_id,
    )
    return [row[json_column] for row in _normalize_rows(rows, operation=f"fetch_{table}")]


__all__ = [
    "persist_virtual_lab_simulation_run",
    "list_virtual_lab_simulation_runs",
    "load_virtual_lab_simulation_run",
    "list_virtual_lab_simulation_events",
    "list_virtual_lab_simulation_verifiers",
    "list_virtual_lab_simulation_blockers",
]
