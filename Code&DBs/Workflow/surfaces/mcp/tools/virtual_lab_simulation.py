"""Tools: praxis_virtual_lab_simulation_*."""

from __future__ import annotations

from typing import Any

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_virtual_lab_simulation_run(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Run a Virtual Lab simulation through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Running Virtual Lab simulation",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="virtual_lab_simulation_run",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - Virtual Lab simulation run {status}",
        )
    return result


def tool_praxis_virtual_lab_simulation_read(
    params: dict,
    _progress_emitter=None,
) -> dict:
    """Read Virtual Lab simulations through CQRS."""

    payload = {key: value for key, value in dict(params or {}).items() if value is not None}
    if _progress_emitter:
        _progress_emitter.emit(
            progress=0,
            total=1,
            message="Reading Virtual Lab simulations",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="virtual_lab_simulation_read",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done - Virtual Lab simulation read {status}",
        )
    return result


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_virtual_lab_simulation_run": (
        tool_praxis_virtual_lab_simulation_run,
        {
            "kind": "write",
            "operation_names": ["virtual_lab_simulation_run"],
            "description": (
                "Run and persist a deterministic Virtual Lab simulation through "
                "the CQRS gateway. The run stores scenario/result digests, "
                "runtime events, predicted state events, transitions, action "
                "results, automation firings, assertions, verifier results, "
                "typed gaps, promotion blockers, and contract refs."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "scenario": {
                        "type": "object",
                        "description": "SimulationScenario JSON packet from runtime.virtual_lab.simulation.",
                    },
                    "run_id": {"type": "string"},
                    "task_contract_ref": {"type": "string"},
                    "integration_action_contract_refs": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "automation_snapshot_refs": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "observed_by_ref": {"type": "string"},
                    "source_ref": {"type": "string"},
                },
                "required": ["scenario"],
            },
            "type_contract": {
                "run_virtual_lab_simulation": {
                    "consumes": [
                        "virtual_lab.simulation_scenario",
                        "virtual_lab.environment_revision",
                        "task_environment.contract_ref",
                        "integration_action.contract_ref",
                        "integration_automation.snapshot_ref",
                    ],
                    "produces": [
                        "virtual_lab.simulation_run",
                        "virtual_lab.simulation_trace",
                        "virtual_lab.state_transition",
                        "virtual_lab.automation_firing",
                        "virtual_lab.assertion_result",
                        "virtual_lab.verifier_result",
                        "virtual_lab.typed_gap",
                        "virtual_lab.promotion_blocker",
                        "authority_operation_receipt",
                        "authority_event.virtual_lab_simulation.completed",
                    ],
                }
            },
        },
    ),
    "praxis_virtual_lab_simulation_read": (
        tool_praxis_virtual_lab_simulation_read,
        {
            "kind": "analytics",
            "operation_names": ["virtual_lab_simulation_read"],
            "description": (
                "Read persisted Virtual Lab simulation runs, traces, verifier "
                "results, typed gaps, and promotion blockers through the CQRS "
                "gateway."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "list_runs",
                            "describe_run",
                            "list_events",
                            "list_verifiers",
                            "list_blockers",
                        ],
                    },
                    "run_id": {"type": "string"},
                    "scenario_id": {"type": "string"},
                    "environment_id": {"type": "string"},
                    "revision_id": {"type": "string"},
                    "status": {"type": "string"},
                    "event_type": {"type": "string"},
                    "source_area": {"type": "string"},
                    "blocker_code": {"type": "string"},
                    "include_events": {"type": "boolean"},
                    "include_state_events": {"type": "boolean"},
                    "include_transitions": {"type": "boolean"},
                    "include_actions": {"type": "boolean"},
                    "include_automation": {"type": "boolean"},
                    "include_assertions": {"type": "boolean"},
                    "include_verifiers": {"type": "boolean"},
                    "include_gaps": {"type": "boolean"},
                    "include_blockers": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 500},
                },
            },
            "type_contract": {
                "read_virtual_lab_simulation": {
                    "consumes": [
                        "virtual_lab.simulation_run_id",
                        "virtual_lab.scenario_id",
                        "virtual_lab.environment_id",
                    ],
                    "produces": [
                        "virtual_lab.simulation_runs",
                        "virtual_lab.simulation_runtime_events",
                        "virtual_lab.simulation_transitions",
                        "virtual_lab.simulation_verifier_results",
                        "virtual_lab.simulation_promotion_blockers",
                    ],
                }
            },
        },
    ),
}
