from __future__ import annotations

from dataclasses import dataclass
from io import StringIO
from types import SimpleNamespace

import runtime.debate_metrics as debate_metrics
import runtime.debate_workflow as debate_workflow
from runtime.debate_workflow import DebateConfig, PersonaDefinition, run_debate
from surfaces.cli.commands import workflow as workflow_commands


@dataclass(frozen=True)
class _FakeWorkflowResult:
    status: str
    completion: str
    latency_ms: int


class _FakeConn:
    def __init__(self) -> None:
        self.scripts: list[str] = []
        self.statements: list[tuple[str, tuple[object, ...]]] = []

    def execute_script(self, sql: str) -> None:
        self.scripts.append(sql)

    def execute(self, sql: str, *params):
        self.statements.append((sql, params))
        return []


def test_collector_persists_round_and_consensus_without_losing_memory() -> None:
    conn = _FakeConn()
    collector = debate_metrics.DebateMetricsCollector(conn=conn)

    first = collector.record_round(
        debate_id="debate-123",
        persona="Pragmatist",
        text="I recommend this path because it uses `safe` code and [1] evidence.",
        duration_seconds=1.25,
        round_number=1,
        persona_position=0,
    )
    second = collector.record_round(
        debate_id="debate-123",
        persona="Skeptic",
        text="This will probably fail unless we add explicit checks.",
        duration_seconds=2.5,
        round_number=1,
        persona_position=1,
    )
    consensus = collector.record_synthesis(
        debate_id="debate-123",
        consensus_points=["keep the code path explicit"],
        disagreements=["risk tolerance differs"],
        synthesis_text="We should keep the code path explicit and reduce hidden state.",
    )

    round_metrics, persisted_consensus = collector.get_debate("debate-123")

    assert round_metrics == [first, second]
    assert persisted_consensus == consensus
    assert len(conn.scripts) == 1
    assert any("INSERT INTO debate_round_metrics" in stmt for stmt, _ in conn.statements)
    assert any("INSERT INTO debate_consensus" in stmt for stmt, _ in conn.statements)


def test_run_debate_threads_metrics_connection_through_persistence_path(monkeypatch) -> None:
    conn = _FakeConn()
    config = DebateConfig(
        topic="Should we persist debate metrics?",
        personas=[PersonaDefinition(name="Pragmatist", perspective="Be practical.")],
        rounds=2,
    )

    call_count = {"value": 0}

    def _fake_run_workflow_parallel(specs, max_workers):
        call_count["value"] += 1
        if call_count["value"] in (1, 2):
            return [_FakeWorkflowResult("succeeded", f"round-{call_count['value']}", 100)]
        return [_FakeWorkflowResult("succeeded", "synthesis", 75)]

    monkeypatch.setattr(debate_workflow, "run_workflow_parallel", _fake_run_workflow_parallel)

    result = run_debate(config, metrics_conn=conn)

    assert result.status == "succeeded"
    assert result.metrics is not None
    assert len(result.metrics["rounds"]) == 2
    assert result.metrics["synthesis"] is not None
    assert len(conn.scripts) == 1
    round_inserts = [sql for sql, _ in conn.statements if "INSERT INTO debate_round_metrics" in sql]
    consensus_inserts = [sql for sql, _ in conn.statements if "INSERT INTO debate_consensus" in sql]
    assert len(round_inserts) == 2
    assert len(consensus_inserts) == 1


def test_workflow_debate_command_passes_metrics_connection(monkeypatch) -> None:
    conn = object()
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        "storage.postgres.connection.ensure_postgres_available",
        lambda env=None: conn,
    )

    def _fake_run_debate(config, *, metrics_conn=None):
        captured["topic"] = config.topic
        captured["metrics_conn"] = metrics_conn
        return SimpleNamespace(
            status="succeeded",
            topic=config.topic,
            persona_responses={"Pragmatist": "Ship it."},
            synthesis="Keep it explicit.",
        )

    monkeypatch.setattr(debate_workflow, "run_debate", _fake_run_debate)

    stdout = StringIO()
    exit_code = workflow_commands._debate_command(
        ["Should we persist debate metrics?"],
        stdout=stdout,
    )

    assert exit_code == 0
    assert captured == {
        "topic": "Should we persist debate metrics?",
        "metrics_conn": conn,
    }
