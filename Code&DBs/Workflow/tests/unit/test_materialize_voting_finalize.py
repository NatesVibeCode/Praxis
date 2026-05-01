"""Tests for the voting helper + compile_finalize stage in materializer_llm.

Covers:
  - resolve_top_k_voters: scoring (affinity + benchmark + priority), provider diversity
  - _call_voting_sub_task: unanimous early-stop, round-1 majority, round-2 majority, tiebreaker
  - call_compile_finalize: per-binding resolution, accepted_target picking, error tolerance

All offline — mocks the postgres connection module + the per-route LLM call helper.
No LLM calls fire in these tests.
"""

from __future__ import annotations

import sys
from types import SimpleNamespace

import pytest


def _install_pg_fake(monkeypatch, *, profile_row, candidate_rows):
    """Install fake storage.postgres.connection + native_runtime_profile_sync modules.

    `profile_row` is the dict returned by the task_type_route_profiles fetchrow.
    `candidate_rows` is the list returned by the matrix-gated candidate query.
    """

    class _FakeSyncConn:
        def __init__(self, pool):
            pass

        def fetchrow(self, query, *args):
            return profile_row

        def fetch(self, query, *args):
            return candidate_rows

    monkeypatch.setitem(
        sys.modules,
        "storage.postgres.connection",
        SimpleNamespace(
            SyncPostgresConnection=_FakeSyncConn,
            get_workflow_pool=lambda: object(),
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "registry.native_runtime_profile_sync",
        SimpleNamespace(default_native_runtime_profile_ref=lambda _pg: "praxis"),
    )


# ──────────────────────────────────────────────────────────────────────────
# resolve_top_k_voters
# ──────────────────────────────────────────────────────────────────────────


def test_resolve_top_k_voters_picks_best_affinity_match(monkeypatch) -> None:
    """Highest affinity_score wins (3×primary + 2×secondary + 1×specialized − 5×avoid)."""
    import runtime.materializer_llm as materializer_llm

    profile_row = {
        "affinity_labels": {
            "primary": ["classification"],
            "secondary": ["json-mode"],
            "specialized": ["fast"],
            "avoid": ["audio"],
        },
        "benchmark_metric_weights": {},
    }
    candidate_rows = [
        {
            "provider_slug": "openrouter",
            "model_slug": "vendor/strong",
            "temperature": 0.0,
            "max_tokens": 4096,
            "capability_tags": ["classification", "json-mode", "fast"],
            "task_affinities": {"primary": ["classification"], "avoid": []},
            "benchmark_profile": {},
            "priority": 1,
        },
        {
            "provider_slug": "openrouter",
            "model_slug": "vendor/weak",
            "temperature": 0.0,
            "max_tokens": 4096,
            "capability_tags": ["audio"],  # hits avoid
            "task_affinities": {"primary": ["audio"]},
            "benchmark_profile": {},
            "priority": 2,
        },
    ]
    _install_pg_fake(monkeypatch, profile_row=profile_row, candidate_rows=candidate_rows)

    voters = materializer_llm.resolve_top_k_voters(
        "materialize_finalize", k=2, diverse_providers=False
    )
    assert len(voters) == 2
    assert voters[0]["model_slug"] == "vendor/strong"
    assert voters[0]["score"] > voters[1]["score"]
    # affinity score: 3 (primary) + 2 (secondary) + 1 (specialized) = 6
    assert voters[0]["score_breakdown"]["affinity"] == 6.0


def test_resolve_top_k_voters_prefers_provider_diversity(monkeypatch) -> None:
    """With diverse_providers=True, K voters come from K different providers when possible."""
    import runtime.materializer_llm as materializer_llm

    profile_row = {
        "affinity_labels": {"primary": ["classification"]},
        "benchmark_metric_weights": {},
    }
    candidate_rows = [
        {
            "provider_slug": "openrouter",
            "model_slug": "a/best",
            "capability_tags": ["classification"],
            "task_affinities": {"primary": ["classification"]},
            "benchmark_profile": {},
            "priority": 1,
            "temperature": None, "max_tokens": None,
        },
        {
            "provider_slug": "openrouter",
            "model_slug": "a/second-best",
            "capability_tags": ["classification"],
            "task_affinities": {"primary": ["classification"]},
            "benchmark_profile": {},
            "priority": 2,
            "temperature": None, "max_tokens": None,
        },
        {
            "provider_slug": "together",
            "model_slug": "b/different-provider",
            "capability_tags": ["classification"],
            "task_affinities": {"primary": ["classification"]},
            "benchmark_profile": {},
            "priority": 5,
            "temperature": None, "max_tokens": None,
        },
    ]
    _install_pg_fake(monkeypatch, profile_row=profile_row, candidate_rows=candidate_rows)

    voters = materializer_llm.resolve_top_k_voters(
        "materialize_finalize", k=2, diverse_providers=True
    )
    providers = sorted(v["provider_slug"] for v in voters)
    assert providers == ["openrouter", "together"], (
        "diverse_providers should pick across providers even when one provider has the top scores"
    )


def test_resolve_top_k_voters_benchmark_weights_break_ties(monkeypatch) -> None:
    """When affinity is tied, benchmark_metric_weights × benchmark_profile values rank-order voters."""
    import runtime.materializer_llm as materializer_llm

    profile_row = {
        "affinity_labels": {"primary": ["classification"]},
        "benchmark_metric_weights": {"intelligence_index": 1.0, "speed": 0.5},
    }
    candidate_rows = [
        {
            "provider_slug": "openrouter",
            "model_slug": "a/smarter",
            "capability_tags": ["classification"],
            "task_affinities": {"primary": ["classification"]},
            "benchmark_profile": {"intelligence_index": 60.0, "speed": 50.0},
            "priority": 1,
            "temperature": None, "max_tokens": None,
        },
        {
            "provider_slug": "together",
            "model_slug": "b/dumber",
            "capability_tags": ["classification"],
            "task_affinities": {"primary": ["classification"]},
            "benchmark_profile": {"intelligence_index": 30.0, "speed": 200.0},
            "priority": 1,
            "temperature": None, "max_tokens": None,
        },
    ]
    _install_pg_fake(monkeypatch, profile_row=profile_row, candidate_rows=candidate_rows)

    voters = materializer_llm.resolve_top_k_voters("materialize_finalize", k=2)
    # smarter: 60×1.0 + 50×0.5 = 85
    # dumber:  30×1.0 + 200×0.5 = 130
    # dumber wins on benchmark score
    assert voters[0]["model_slug"] == "b/dumber"
    assert voters[0]["score_breakdown"]["benchmark"] == 130.0
    assert voters[1]["score_breakdown"]["benchmark"] == 85.0


# ──────────────────────────────────────────────────────────────────────────
# _call_voting_sub_task — adaptive cascade
# ──────────────────────────────────────────────────────────────────────────


def _install_voter_pool(monkeypatch, *, voter_models: list[str]):
    """Stub resolve_top_k_voters to return a fixed pool, regardless of args."""
    import runtime.materializer_llm as materializer_llm

    pool = [
        {
            "provider_slug": "openrouter",
            "model_slug": m,
            "score": 100.0 - i,
            "score_breakdown": {"affinity": 0.0, "benchmark": 0.0, "priority": 0.0},
            "temperature": 0.0,
            "max_tokens": 1024,
        }
        for i, m in enumerate(voter_models)
    ]
    monkeypatch.setattr(
        materializer_llm, "resolve_top_k_voters", lambda *a, **kw: pool[: kw.get("k", len(pool))]
    )


def test_voting_unanimous_early_stops_after_min_votes(monkeypatch) -> None:
    import runtime.materializer_llm as materializer_llm

    _install_voter_pool(monkeypatch, voter_models=["m/1", "m/2", "m/3", "m/4", "m/5"])
    call_count = {"n": 0}

    def _fake_route(*, provider_slug, model_slug, prompt, temperature=None, max_tokens=None):
        call_count["n"] += 1
        return '{"answer": "X"}'

    monkeypatch.setattr(materializer_llm, "_call_specific_route", _fake_route)

    def _parse(raw):
        import json
        return json.loads(raw)["answer"]

    result = materializer_llm._call_voting_sub_task(
        task_type="materialize_finalize",
        prompt="...",
        parser=_parse,
        min_votes=3,
        max_votes=5,
    )
    assert result["decision_path"] == "unanimous"
    assert result["answer"] == "X"
    # Early-stop must NOT have fired the round-2 voters
    assert call_count["n"] == 3


def test_voting_round1_majority_short_circuits(monkeypatch) -> None:
    import runtime.materializer_llm as materializer_llm

    _install_voter_pool(monkeypatch, voter_models=["m/1", "m/2", "m/3", "m/4", "m/5"])
    answers = ["X", "X", "Y"]
    idx = {"i": 0}

    def _fake_route(*, provider_slug, model_slug, prompt, temperature=None, max_tokens=None):
        a = answers[idx["i"] % len(answers)]
        idx["i"] += 1
        import json
        return json.dumps({"answer": a})

    monkeypatch.setattr(materializer_llm, "_call_specific_route", _fake_route)

    result = materializer_llm._call_voting_sub_task(
        task_type="materialize_finalize",
        prompt="...",
        parser=lambda r: __import__("json").loads(r)["answer"],
        min_votes=3,
        max_votes=5,
    )
    assert result["decision_path"] == "majority_round1"
    assert result["answer"] == "X"
    assert result["vote_count"] == 2


def test_voting_expands_to_round2_then_tiebreaks(monkeypatch) -> None:
    """Round 1 has 1-1-1 split; round 2 (5 total) still 2-2-1 split → tiebreaker fires."""
    import runtime.materializer_llm as materializer_llm

    _install_voter_pool(monkeypatch, voter_models=["m/1", "m/2", "m/3", "m/4", "m/5"])
    # 5 split votes followed by tiebreaker that picks "Z"
    answers = ["X", "Y", "Z", "X", "Y", "Z"]
    idx = {"i": 0}

    def _fake_route(*, provider_slug, model_slug, prompt, temperature=None, max_tokens=None):
        a = answers[idx["i"]]
        idx["i"] += 1
        import json
        return json.dumps({"answer": a})

    monkeypatch.setattr(materializer_llm, "_call_specific_route", _fake_route)

    result = materializer_llm._call_voting_sub_task(
        task_type="materialize_finalize",
        prompt="...",
        parser=lambda r: __import__("json").loads(r)["answer"],
        min_votes=3,
        max_votes=5,
    )
    assert result["decision_path"] == "tiebreaker"
    assert result["answer"] == "Z"


def test_voting_failed_voter_does_not_break_tally(monkeypatch) -> None:
    """A voter raising an exception is recorded as a failed vote but doesn't crash voting."""
    import runtime.materializer_llm as materializer_llm

    _install_voter_pool(monkeypatch, voter_models=["m/1", "m/2", "m/3", "m/4", "m/5"])
    sequence = [None, "X", "X"]  # first call raises, next two return "X" → unanimous of remaining 2
    idx = {"i": 0}

    def _fake_route(*, provider_slug, model_slug, prompt, temperature=None, max_tokens=None):
        a = sequence[idx["i"]]
        idx["i"] += 1
        if a is None:
            raise RuntimeError("simulated provider failure")
        import json
        return json.dumps({"answer": a})

    monkeypatch.setattr(materializer_llm, "_call_specific_route", _fake_route)

    result = materializer_llm._call_voting_sub_task(
        task_type="materialize_finalize",
        prompt="...",
        parser=lambda r: __import__("json").loads(r)["answer"],
        min_votes=3,
        max_votes=5,
    )
    # 2/3 voters returned X unanimously → majority_round1 (2 of 2 ok votes)
    assert result["answer"] == "X"
    assert result["total_ok"] == 2


# ──────────────────────────────────────────────────────────────────────────
# call_compile_finalize — per-binding resolution
# ──────────────────────────────────────────────────────────────────────────


def test_finalize_skips_already_accepted_bindings(monkeypatch) -> None:
    import runtime.materializer_llm as materializer_llm

    ledger = [
        {
            "binding_id": "binding:already-done",
            "state": "accepted",
            "candidate_targets": [{"target_ref": "@x/y", "label": "X", "kind": "integration"}],
            "accepted_target": {"target_ref": "@x/y", "label": "X", "kind": "integration"},
        },
    ]

    # Voting helper should never be called when nothing is blocking
    def _explode(*args, **kwargs):
        raise AssertionError("voting helper should not run when no blockers")

    monkeypatch.setattr(materializer_llm, "_call_voting_sub_task", _explode)

    result = materializer_llm.call_compile_finalize(
        binding_ledger=ledger, original_prose="x"
    )
    assert result["blocking_count"] == 0
    assert result["resolved_count"] == 0
    assert result["updated_ledger"] == ledger


def test_finalize_resolves_blocking_binding_to_chosen_candidate(monkeypatch) -> None:
    import runtime.materializer_llm as materializer_llm

    ledger = [
        {
            "binding_id": "binding:to-resolve",
            "state": "suggested",
            "source_label": "@?notifier",
            "source_node_ids": ["step-001"],
            "candidate_targets": [
                {"target_ref": "@notifications/send", "label": "Notify Hub", "kind": "integration"},
                {"target_ref": "@gmail/send", "label": "Gmail", "kind": "integration"},
            ],
            "accepted_target": None,
        },
    ]

    def _fake_voting(*, task_type, prompt, parser, **kwargs):
        return {
            "answer": "@notifications/send",
            "votes": [],
            "decision_path": "unanimous",
            "vote_count": 3,
            "total_ok": 3,
        }

    monkeypatch.setattr(materializer_llm, "_call_voting_sub_task", _fake_voting)

    result = materializer_llm.call_compile_finalize(
        binding_ledger=ledger,
        original_prose="When customer reports issue we notify on-call.",
    )
    assert result["blocking_count"] == 1
    assert result["resolved_count"] == 1
    new_binding = result["updated_ledger"][0]
    assert new_binding["state"] == "accepted"
    assert new_binding["accepted_target"]["target_ref"] == "@notifications/send"


def test_finalize_does_not_fabricate_target_refs_outside_candidate_list(monkeypatch) -> None:
    """If voting picks a target_ref that's NOT in candidate_targets, finalize must NOT accept it."""
    import runtime.materializer_llm as materializer_llm

    ledger = [
        {
            "binding_id": "binding:hallucination-test",
            "state": "suggested",
            "source_label": "@?x",
            "candidate_targets": [
                {"target_ref": "@valid/option", "label": "Valid", "kind": "integration"},
            ],
            "accepted_target": None,
        },
    ]

    def _fake_voting(*, task_type, prompt, parser, **kwargs):
        return {
            "answer": "@made/up",  # not in candidate_targets
            "votes": [], "decision_path": "unanimous", "vote_count": 3, "total_ok": 3,
        }

    monkeypatch.setattr(materializer_llm, "_call_voting_sub_task", _fake_voting)

    result = materializer_llm.call_compile_finalize(
        binding_ledger=ledger, original_prose="x"
    )
    assert result["resolved_count"] == 0, "voter hallucination should NOT be applied"
    assert result["updated_ledger"][0]["state"] == "suggested"  # unchanged


def test_finalize_voter_exception_keeps_binding_unchanged(monkeypatch) -> None:
    import runtime.materializer_llm as materializer_llm

    ledger = [
        {
            "binding_id": "binding:exception-test",
            "state": "suggested",
            "candidate_targets": [
                {"target_ref": "@x/y", "label": "X", "kind": "integration"},
            ],
        },
    ]

    def _explode(**kwargs):
        raise RuntimeError("voting infra crashed")

    monkeypatch.setattr(materializer_llm, "_call_voting_sub_task", _explode)

    result = materializer_llm.call_compile_finalize(
        binding_ledger=ledger, original_prose="x"
    )
    assert result["resolved_count"] == 0
    assert result["updated_ledger"][0]["state"] == "suggested"
    assert result["decisions"][0].get("error", "").startswith("RuntimeError")


def test_finalize_parser_handles_null_resolved_target(monkeypatch) -> None:
    """When voters return resolved=null (no good match), parser returns None → no binding flipped."""
    import runtime.materializer_llm as materializer_llm

    raw_null = '{"chosen_target_ref": null, "confidence": 0.0, "rationale": "no match"}'
    raw_explicit_null = '{"chosen_target_ref": "null", "confidence": 0.0}'  # string "null"
    raw_empty = '{"chosen_target_ref": "", "confidence": 0.0}'
    raw_real = '{"chosen_target_ref": "@notifications/send", "confidence": 0.9}'

    assert materializer_llm._parse_finalize_response(raw_null) is None
    assert materializer_llm._parse_finalize_response(raw_explicit_null) is None
    assert materializer_llm._parse_finalize_response(raw_empty) is None
    assert materializer_llm._parse_finalize_response(raw_real) == "@notifications/send"
    assert materializer_llm._parse_finalize_response("not-json") is None
