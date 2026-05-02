from __future__ import annotations

import asyncio
from datetime import datetime, timezone

from surfaces.api import _operator_repository
from surfaces.api.handlers import workflow_query


def test_operator_repository_uses_shared_packet_inspection_resolver(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _Conn:
        async def fetch(self, _query: str, _run_ids: list[str]):
            return [
                {
                    "run_id": "run-1",
                    "workflow_id": "workflow.alpha",
                    "request_id": "request-1",
                    "workflow_definition_id": "definition-1",
                    "current_state": "running",
                    "terminal_reason_code": None,
                    "request_digest": None,
                    "admitted_definition_hash": None,
                    "run_idempotency_key": None,
                    "packet_inspection": None,
                    "failure_category": None,
                    "request_envelope": {},
                    "operator_frames": [],
                    "packets": [{"packet_version": 1, "workflow_id": "workflow.alpha"}],
                }
            ]

    def _fake_resolve_packet_inspection(*, run_row, packets):
        captured["run_id"] = run_row["run_id"]
        captured["packets"] = packets
        return {"kind": "inspection"}, "derived"

    monkeypatch.setattr(
        _operator_repository,
        "resolve_packet_inspection",
        _fake_resolve_packet_inspection,
    )

    records, summary = asyncio.run(
        _operator_repository.NativeOperatorQueryFrontdoor()._fetch_workflow_run_packet_inspections(
            conn=_Conn(),
            workflow_run_ids=("run-1",),
        )
    )

    assert captured == {
        "run_id": "run-1",
        "packets": [{"packet_version": 1, "workflow_id": "workflow.alpha"}],
    }
    assert len(records) == 1
    assert records[0].packet_inspection_source == "derived"
    assert records[0].packet_inspection == {"kind": "inspection"}
    assert summary is not None


def test_workflow_query_uses_shared_packet_inspection_resolver(monkeypatch) -> None:
    captured: dict[str, object] = {}

    class _Pg:
        def execute(self, _query: str, run_id: str):
            return [
                {
                    "run_id": run_id,
                    "workflow_id": "workflow.alpha",
                    "request_id": "request-1",
                    "workflow_definition_id": "definition-1",
                    "current_state": "running",
                    "packet_inspection": None,
                    "request_envelope": {},
                    "requested_at": datetime.now(timezone.utc),
                    "admitted_at": None,
                    "started_at": None,
                    "finished_at": None,
                    "last_event_id": None,
                    "packets": [{"packet_version": 1}],
                }
            ]

    def _fake_resolve_packet_inspection(*, run_row, packets):
        captured["run_id"] = run_row["run_id"]
        captured["packets"] = packets
        return {"kind": "workflow-query-inspection"}, "derived"

    monkeypatch.setattr(
        "runtime.execution_packet_authority.resolve_packet_inspection",
        _fake_resolve_packet_inspection,
    )

    assert workflow_query._fetch_run_packet_inspection(_Pg(), "run-1") == {
        "kind": "workflow-query-inspection"
    }
    assert captured == {"run_id": "run-1", "packets": [{"packet_version": 1}]}
