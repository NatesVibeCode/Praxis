"""Unit tests for auto-promotion in runtime/dataset_candidate_subscriber.py."""

from __future__ import annotations

from contracts.dataset import (
    CandidateScore,
    DatasetScoringPolicy,
    RawDatasetCandidate,
)
from runtime.dataset_candidate_subscriber import _maybe_auto_promote


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, tuple]] = []
        self.decision_inserts: list[tuple[str, tuple]] = []
        self.emitted: list[dict] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.executed.append((sql, args))
        return "INSERT 0 1"

    async def fetchrow(self, sql: str, *args: object):
        if "INSERT INTO operator_decisions" in sql:
            self.decision_inserts.append((sql, args))
            return {
                "operator_decision_id": args[0],
                "decision_key": args[1],
                "decision_kind": args[2],
                "decision_status": args[3],
                "title": args[4],
                "rationale": args[5],
                "decided_by": args[6],
                "decision_source": args[7],
                "effective_from": args[8],
                "effective_to": args[9],
                "decided_at": args[10],
                "created_at": args[11],
                "updated_at": args[12],
                "decision_scope_kind": args[13],
                "decision_scope_ref": args[14],
            }
        return None

    async def fetch(self, sql: str, *args: object):
        return []


async def _emit_stub(conn, *, channel, event_type, entity_id, entity_kind, payload, emitted_by):
    conn.emitted.append(
        {
            "channel": channel,
            "event_type": event_type,
            "entity_id": entity_id,
            "payload": payload,
        }
    )
    return 9999


def _policy(*, auto_promote: bool, slug: str = "review.v1") -> DatasetScoringPolicy:
    return DatasetScoringPolicy(
        policy_id="pol_x",
        policy_slug=slug,
        specialist_target="slm/review",
        rubric={"factors": {}, "thresholds": {}},
        decided_by="nathan",
        rationale="test",
        auto_promote=auto_promote,
    )


def _candidate(**overrides) -> RawDatasetCandidate:
    defaults = dict(
        candidate_id="c_1",
        candidate_kind="review",
        source_receipt_id="r_1",
        source_run_id="run_1",
        source_node_id="review_step",
        raw_input_ref={"receipt_id": "r_1", "path": "$.inputs"},
        raw_output_ref={"receipt_id": "r_1", "path": "$.outputs"},
        dedupe_signature="sha256:aaaa",
        route_slug="slm/review",
        redaction_status="clean",
        staleness_status="fresh",
        admitted_definition_hash="sha256:def",
    )
    defaults.update(overrides)
    return RawDatasetCandidate(**defaults)


def _score(eligibility: str = "sft_eligible", confidence: float = 0.83) -> CandidateScore:
    return CandidateScore(
        candidate_id="c_1",
        policy_id="pol_x",
        eligibility=eligibility,
        confidence=confidence,
        factors={},
        rationale="ok",
    )


def test_auto_promote_skipped_when_policy_disabled(monkeypatch) -> None:
    import asyncio

    from runtime import dataset_candidate_subscriber as mod

    monkeypatch.setattr(mod, "aemit", _emit_stub)
    conn = _FakeConn()
    pid = asyncio.run(
        _maybe_auto_promote(conn, candidate=_candidate(), policy=_policy(auto_promote=False), score=_score())
    )
    assert pid is None
    assert conn.executed == []


def test_auto_promote_skipped_when_score_below_threshold(monkeypatch) -> None:
    import asyncio

    from runtime import dataset_candidate_subscriber as mod

    monkeypatch.setattr(mod, "aemit", _emit_stub)
    conn = _FakeConn()
    pid = asyncio.run(
        _maybe_auto_promote(
            conn,
            candidate=_candidate(),
            policy=_policy(auto_promote=True),
            score=_score(eligibility="manual_review", confidence=0.55),
        )
    )
    assert pid is None
    assert conn.executed == []


def test_auto_promote_skipped_when_not_redaction_clean(monkeypatch) -> None:
    import asyncio

    from runtime import dataset_candidate_subscriber as mod

    monkeypatch.setattr(mod, "aemit", _emit_stub)
    conn = _FakeConn()
    pid = asyncio.run(
        _maybe_auto_promote(
            conn,
            candidate=_candidate(redaction_status="redaction_required"),
            policy=_policy(auto_promote=True),
            score=_score(),
        )
    )
    assert pid is None
    assert conn.executed == []


def test_auto_promote_skipped_when_stale(monkeypatch) -> None:
    import asyncio

    from runtime import dataset_candidate_subscriber as mod

    monkeypatch.setattr(mod, "aemit", _emit_stub)
    conn = _FakeConn()
    pid = asyncio.run(
        _maybe_auto_promote(
            conn,
            candidate=_candidate(staleness_status="definition_stale"),
            policy=_policy(auto_promote=True),
            score=_score(),
        )
    )
    assert pid is None


def test_auto_promote_skipped_for_preference_family(monkeypatch) -> None:
    import asyncio

    from runtime import dataset_candidate_subscriber as mod

    monkeypatch.setattr(mod, "aemit", _emit_stub)
    conn = _FakeConn()
    policy = DatasetScoringPolicy(
        policy_id="pol_x",
        policy_slug="review.v1",
        specialist_target="slm/review",
        rubric={"factors": {}, "thresholds": {}, "auto_promote_family": "preference"},
        decided_by="nathan",
        rationale="test",
        auto_promote=True,
    )
    pid = asyncio.run(_maybe_auto_promote(conn, candidate=_candidate(), policy=policy, score=_score()))
    assert pid is None


def test_auto_promote_inserts_and_emits_when_eligible(monkeypatch) -> None:
    import asyncio

    from runtime import dataset_candidate_subscriber as mod

    monkeypatch.setattr(mod, "aemit", _emit_stub)
    conn = _FakeConn()
    pid = asyncio.run(
        _maybe_auto_promote(
            conn,
            candidate=_candidate(),
            policy=_policy(auto_promote=True),
            score=_score(eligibility="sft_eligible", confidence=0.83),
        )
    )
    assert pid is not None and pid.startswith("prom_")
    assert len(conn.executed) == 1
    sql, args = conn.executed[0]
    assert "INSERT INTO dataset_promotions" in sql
    assert "sft" in args
    assert "slm/review" in args
    assert "auto" in args
    assert "system:review.v1" in args
    # decision_ref (args[10]) must be populated by the bridge
    assert args[10] is not None and str(args[10]).startswith("operator_decision.dataset_promotion.")
    assert len(conn.decision_inserts) == 1
    _, decision_args = conn.decision_inserts[0]
    assert decision_args[2] == "dataset_promotion"
    assert decision_args[13] == "dataset_specialist"
    assert decision_args[14] == "slm/review"
    assert len(conn.emitted) == 1
    assert conn.emitted[0]["event_type"] == "dataset_promotion_recorded"
    assert conn.emitted[0]["payload"]["promotion_kind"] == "auto"
    assert conn.emitted[0]["payload"]["decision_ref"].startswith("operator_decision.dataset_promotion.")
