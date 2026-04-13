from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

os.environ.setdefault("WORKFLOW_DATABASE_URL", "postgresql://test@localhost:5432/praxis_test")

from runtime.domain import RuntimeBoundaryError
from runtime.workflow import orchestrator


def _workflow_result() -> orchestrator.WorkflowResult:
    now = datetime.now(timezone.utc)
    return orchestrator.WorkflowResult(
        run_id="run.orchestrator.sync",
        status="succeeded",
        reason_code="workflow.execution_succeeded",
        completion="done",
        outputs={},
        evidence_count=1,
        started_at=now,
        finished_at=now,
        latency_ms=10,
        provider_slug="openai",
        model_slug="gpt-5.4",
        adapter_type="cli_llm",
    )


def test_finalize_workflow_result_degrades_sync_failures(monkeypatch) -> None:
    result = _workflow_result()

    monkeypatch.setattr(orchestrator, "_result_is_persisted", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        "runtime.post_workflow_sync.run_post_workflow_sync",
        lambda _run_id: (_ for _ in ()).throw(RuntimeError("sync boom")),
    )
    monkeypatch.setattr(
        "runtime.post_workflow_sync.record_workflow_run_sync_status",
        lambda run_id, **_kwargs: SimpleNamespace(
            run_id=run_id,
            sync_status="degraded",
            sync_cycle_id=None,
            sync_error_count=1,
        ),
    )

    finalized = orchestrator._finalize_workflow_result(
        result,
        evidence_writer=object(),
    )

    assert finalized.persisted is True
    assert finalized.sync_status == "degraded"
    assert finalized.sync_cycle_id is None
    assert finalized.sync_error_count == 1


def test_finalize_workflow_result_raises_when_degraded_status_cannot_be_persisted(monkeypatch) -> None:
    result = _workflow_result()

    monkeypatch.setattr(orchestrator, "_result_is_persisted", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(
        "runtime.post_workflow_sync.run_post_workflow_sync",
        lambda _run_id: (_ for _ in ()).throw(RuntimeError("sync boom")),
    )
    monkeypatch.setattr(
        "runtime.post_workflow_sync.record_workflow_run_sync_status",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("record boom")),
    )

    with pytest.raises(
        RuntimeBoundaryError,
        match="degraded status could not be persisted",
    ):
        orchestrator._finalize_workflow_result(
            result,
            evidence_writer=object(),
        )
