"""Pass 4 tests: authority_binding persistence through packet -> job -> worker."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from runtime.spec_compiler import CompiledSpec, PlanPacket, _packet_to_job
from runtime.workflow.job_runtime_context import (
    load_workflow_job_authority_binding,
    load_workflow_job_runtime_context,
)


_SAMPLE_BINDING = {
    "canonical_write_scope": [
        {
            "unit_kind": "operation_ref",
            "unit_ref": "compose_plan",
            "requested_target": {"unit_kind": "operation_ref", "unit_ref": "old_compose"},
            "was_redirected": True,
        }
    ],
    "predecessor_obligations": [
        {
            "predecessor_unit_kind": "operation_ref",
            "predecessor_unit_ref": "old_compose",
            "successor_unit_kind": "operation_ref",
            "successor_unit_ref": "compose_plan",
            "supersession_status": "compat",
            "obligation_summary": "preserve legacy intent shape",
            "obligation_evidence": {},
            "source_candidate_id": None,
            "source_impact_id": None,
            "source_decision_ref": None,
        }
    ],
    "blocked_compat_units": [],
    "unresolved_targets": [],
    "notes": [],
}


def _compiled() -> CompiledSpec:
    return CompiledSpec(
        prompt="do the thing",
        scope_write=[],
        label="step_1",
        verify_refs=[],
        capabilities=[],
        tier=None,
    )


def test_packet_to_job_preserves_authority_binding() -> None:
    packet = PlanPacket(
        description="patch compose path",
        write=["Code&DBs/Workflow/runtime/operations/commands/compose_plan.py"],
        stage="build",
        label="step_1",
        authority_binding=_SAMPLE_BINDING,
    )
    job = _packet_to_job(packet, compiled=_compiled(), workdir="/repo", index=0)
    assert "authority_binding" in job
    assert job["authority_binding"]["canonical_write_scope"][0]["unit_ref"] == "compose_plan"
    assert (
        job["authority_binding"]["predecessor_obligations"][0]["obligation_summary"]
        == "preserve legacy intent shape"
    )


def test_packet_to_job_omits_authority_binding_when_unset() -> None:
    packet = PlanPacket(
        description="docs only",
        write=["docs/notes.md"],
        stage="build",
        label="step_1",
    )
    job = _packet_to_job(packet, compiled=_compiled(), workdir="/repo", index=0)
    assert "authority_binding" not in job


def test_packet_to_job_does_not_share_binding_dict_with_packet() -> None:
    packet = PlanPacket(
        description="x",
        write=["docs/x.md"],
        stage="build",
        label="step_1",
        authority_binding=_SAMPLE_BINDING,
    )
    job = _packet_to_job(packet, compiled=_compiled(), workdir="/repo", index=0)
    job["authority_binding"]["mutated"] = True
    assert "mutated" not in (packet.authority_binding or {})


@dataclass
class _FakeConn:
    binding_for_label: dict[str, Any] = field(default_factory=dict)
    rows: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        self.rows.append((query, args))
        if "FROM workflow_jobs" in query:
            run_id, label = args
            value = self.binding_for_label.get(label)
            if value is None:
                return {"authority_binding": None}
            return {"authority_binding": value}
        return None


def test_load_workflow_job_authority_binding_returns_none_when_unset() -> None:
    conn = _FakeConn(binding_for_label={"step_1": None})
    result = load_workflow_job_authority_binding(
        conn, run_id="run_1", job_label="step_1"
    )
    assert result is None


def test_load_workflow_job_authority_binding_returns_dict_payload() -> None:
    conn = _FakeConn(binding_for_label={"step_1": _SAMPLE_BINDING})
    result = load_workflow_job_authority_binding(
        conn, run_id="run_1", job_label="step_1"
    )
    assert result == _SAMPLE_BINDING


def test_load_workflow_job_authority_binding_decodes_json_string() -> None:
    encoded = json.dumps(_SAMPLE_BINDING, sort_keys=True)
    conn = _FakeConn(binding_for_label={"step_1": encoded})
    result = load_workflow_job_authority_binding(
        conn, run_id="run_1", job_label="step_1"
    )
    assert isinstance(result, dict)
    assert result["canonical_write_scope"][0]["unit_ref"] == "compose_plan"


def test_load_workflow_job_authority_binding_returns_none_for_blank_inputs() -> None:
    conn = _FakeConn()
    assert load_workflow_job_authority_binding(conn, run_id="", job_label="step_1") is None
    assert load_workflow_job_authority_binding(conn, run_id="run_1", job_label="") is None


@dataclass
class _ContextFakeConn(_FakeConn):
    runtime_row: dict[str, Any] | None = None

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        # Only the workflow_jobs path goes through this conn; runtime context
        # uses the receipt repository injection below.
        return super().fetchrow(query, *args)


class _StubReceiptRepo:
    def __init__(self, runtime_row: dict[str, Any] | None) -> None:
        self.runtime_row = runtime_row

    def load_workflow_job_runtime_context(self, *, run_id: str, job_label: str):
        return self.runtime_row


def test_load_workflow_job_runtime_context_includes_authority_binding(monkeypatch) -> None:
    runtime_row = {
        "execution_context_shard": json.dumps({"baseline": {"foo": "bar"}}),
        "execution_bundle": {"manifest": {}},
    }
    conn = _ContextFakeConn(
        binding_for_label={"step_1": _SAMPLE_BINDING},
        runtime_row=runtime_row,
    )

    import runtime.workflow.job_runtime_context as job_runtime_context

    monkeypatch.setattr(
        job_runtime_context,
        "PostgresReceiptRepository",
        lambda _conn: _StubReceiptRepo(runtime_row),
    )
    result = load_workflow_job_runtime_context(
        conn, run_id="run_1", job_label="step_1"
    )
    assert result is not None
    assert result["execution_context_shard"] == {"baseline": {"foo": "bar"}}
    assert result["execution_bundle"] == {"manifest": {}}
    assert result["authority_binding"] == _SAMPLE_BINDING


def test_load_workflow_job_runtime_context_authority_binding_none_when_unbound(monkeypatch) -> None:
    runtime_row = {
        "execution_context_shard": {},
        "execution_bundle": {},
    }
    conn = _ContextFakeConn(binding_for_label={"step_1": None}, runtime_row=runtime_row)

    import runtime.workflow.job_runtime_context as job_runtime_context

    monkeypatch.setattr(
        job_runtime_context,
        "PostgresReceiptRepository",
        lambda _conn: _StubReceiptRepo(runtime_row),
    )
    result = load_workflow_job_runtime_context(
        conn, run_id="run_1", job_label="step_1"
    )
    assert result is not None
    assert result["authority_binding"] is None
