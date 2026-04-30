"""CQRS commands for Virtual Lab simulation authority."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field, field_validator

from runtime.virtual_lab.simulation import (
    run_simulation_scenario,
    simulation_scenario_from_dict,
)
from storage.postgres.virtual_lab_simulation_repository import persist_virtual_lab_simulation_run


class RunVirtualLabSimulationCommand(BaseModel):
    """Run and persist a deterministic Virtual Lab simulation scenario."""

    scenario: dict[str, Any]
    run_id: str | None = None
    task_contract_ref: str | None = None
    integration_action_contract_refs: list[str] = Field(default_factory=list)
    automation_snapshot_refs: list[str] = Field(default_factory=list)
    observed_by_ref: str | None = None
    source_ref: str | None = None

    @field_validator("run_id", "task_contract_ref", "observed_by_ref", "source_ref", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: object) -> str | None:
        if value is None or value == "":
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError("optional refs must be non-empty strings when provided")
        return value.strip()

    @field_validator("integration_action_contract_refs", "automation_snapshot_refs", mode="before")
    @classmethod
    def _normalize_ref_list(cls, value: object) -> list[str]:
        if value is None:
            return []
        if isinstance(value, str):
            text = value.strip()
            return [text] if text else []
        if not isinstance(value, list):
            raise ValueError("contract refs must be strings or lists of strings")
        refs: list[str] = []
        for item in value:
            if not isinstance(item, str) or not item.strip():
                raise ValueError("contract refs must be non-empty strings")
            refs.append(item.strip())
        return refs


def handle_virtual_lab_simulation_run(
    command: RunVirtualLabSimulationCommand,
    subsystems: Any,
) -> dict[str, Any]:
    conn = subsystems.get_pg_conn()
    scenario = simulation_scenario_from_dict(dict(command.scenario))
    result = run_simulation_scenario(scenario, run_id=command.run_id)
    scenario_json = scenario.to_json()
    result_json = result.to_json()
    persisted = persist_virtual_lab_simulation_run(
        conn,
        scenario=scenario_json,
        result=result_json,
        task_contract_ref=command.task_contract_ref,
        integration_action_contract_refs=command.integration_action_contract_refs,
        automation_snapshot_refs=command.automation_snapshot_refs,
        observed_by_ref=command.observed_by_ref,
        source_ref=command.source_ref,
    )
    revision = scenario.initial_state.revision
    event_payload = {
        "run_id": result.run_id,
        "scenario_id": scenario.scenario_id,
        "status": result.status,
        "stop_reason": result.stop_reason,
        "scenario_digest": scenario.scenario_digest,
        "trace_digest": result.trace.trace_digest,
        "result_digest": result.result_digest,
        "action_count": len(result.action_results),
        "verifier_count": len(result.verifier_results),
        "blocker_count": len(result.blockers),
        "environment_id": revision.environment_id,
        "revision_id": revision.revision_id,
    }
    return {
        "ok": True,
        "operation": "virtual_lab_simulation_run",
        "run_id": result.run_id,
        "status": result.status,
        "stop_reason": result.stop_reason,
        "scenario": scenario_json,
        "result": result_json,
        "persisted": persisted,
        "event_payload": event_payload,
    }


__all__ = [
    "RunVirtualLabSimulationCommand",
    "handle_virtual_lab_simulation_run",
]
