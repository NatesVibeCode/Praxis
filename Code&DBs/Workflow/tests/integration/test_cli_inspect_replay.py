from __future__ import annotations

from dataclasses import dataclass, field
from io import StringIO

from observability.read_models import (
    InspectionReadModel,
    ProjectionCompleteness,
    ProjectionWatermark,
    ReplayReadModel,
)
from surfaces.cli.main import main


@dataclass
class _StubInspectReplayService:
    inspect_calls: list[str] = field(default_factory=list)
    replay_calls: list[str] = field(default_factory=list)

    def inspect_run(self, *, run_id: str) -> InspectionReadModel:
        self.inspect_calls.append(run_id)
        return InspectionReadModel(
            run_id=run_id,
            request_id="request.inspect",
            completeness=ProjectionCompleteness(is_complete=True),
            watermark=ProjectionWatermark(evidence_seq=7),
            evidence_refs=("row.inspect.1", "row.inspect.2"),
            current_state="succeeded",
            node_timeline=("node_0:running", "node_0:succeeded"),
            terminal_reason="runtime.workflow_succeeded",
        )

    def replay_run(self, *, run_id: str) -> ReplayReadModel:
        self.replay_calls.append(run_id)
        return ReplayReadModel(
            run_id=run_id,
            request_id="request.replay",
            completeness=ProjectionCompleteness(is_complete=True),
            watermark=ProjectionWatermark(evidence_seq=11),
            evidence_refs=("row.replay.1", "row.replay.2"),
            dependency_order=("node_0", "node_1"),
            node_outcomes=("node_0:succeeded", "node_1:succeeded"),
            admitted_definition_ref="workflow_definition.alpha.v1",
            terminal_reason="runtime.workflow_succeeded",
        )


def test_cli_inspect_frontdoor_uses_explicit_reader_service() -> None:
    service = _StubInspectReplayService()
    stdout = StringIO()

    exit_code = main(
        ["inspect", "run.inspect"],
        inspect_replay_service=service,
        stdout=stdout,
    )

    assert exit_code == 0
    assert service.inspect_calls == ["run.inspect"]
    assert service.replay_calls == []
    rendered = stdout.getvalue()
    assert "kind: inspection" in rendered
    assert "run_id: run.inspect" in rendered
    assert "current_state: succeeded" in rendered
    assert "watermark_seq: 7" in rendered


def test_cli_replay_frontdoor_accepts_runtime_alias_for_service() -> None:
    service = _StubInspectReplayService()
    stdout = StringIO()

    exit_code = main(
        ["replay", "run.replay"],
        runtime_orchestrator=service,
        stdout=stdout,
    )

    assert exit_code == 0
    assert service.inspect_calls == []
    assert service.replay_calls == ["run.replay"]
    rendered = stdout.getvalue()
    assert "kind: replay" in rendered
    assert "run_id: run.replay" in rendered
    assert "dependency_order: node_0, node_1" in rendered
    assert "terminal_reason: runtime.workflow_succeeded" in rendered
