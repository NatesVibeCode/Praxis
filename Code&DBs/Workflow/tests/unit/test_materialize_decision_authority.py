"""Foundation slice tests for the materialize + Review front door.

Covers the three additive seams introduced by migration 396 + runtime
``runtime/materialize/review_payload.py``:

1. Pydantic contracts (MaterializeAlternative / MaterializeDecision /
   PacketDecisionRecord / MaterializeReviewPayload) import cleanly and
   round-trip through model_dump/model_validate without field loss.
2. ``_build_packet_map_entry`` surfaces the per-packet decisions list
   under ``alternatives_considered``, sourced from
   ``job['materialize_decisions']`` via the same job-dict mirror pattern
   as ``data_pills``.
3. ``UnresolvedSourceRefError`` / ``UnresolvedWriteScopeError`` /
   ``UnresolvedStageError`` carry per-entry ``available_options`` for the
   failure-mode equivalent of the success-mode ``alternatives_considered``.

See the lunchbox at /Users/nate/.claude/plans/and-praxis-phase-to-plan-
enchanted-hanrahan.md for the full Foundation contract.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.materialize.review_payload import (
    MaterializeAlternative,
    MaterializeDecision,
    MaterializeReviewPayload,
    PacketDecisionRecord,
)
from runtime.spec_materializer import (
    Plan,
    PlanPacket,
    UnresolvedSourceRefError,
    UnresolvedStageError,
    UnresolvedWriteScopeError,
    _STAGE_TEMPLATES,
    _build_packet_map_entry,
    materialize_plan,
)


class _StubConn:
    """Same minimal stub used in test_spec_compiler_fail_closed —
    materialize_plan's pre-validation paths don't touch the DB."""

    def execute(self, sql: str, *args):
        return []


def _job(**overrides):
    base = {
        "label": "step-1",
        "agent": "auto/build",
        "task_type": "build",
        "produces": [],
        "consumes": [],
        "consumes_any": [],
        "write_scope": ["src/foo.py"],
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# 1. Pydantic contracts import + round-trip
# ---------------------------------------------------------------------------


def test_materialize_decision_round_trips_with_alternatives():
    decision = MaterializeDecision(
        decision_kind="stage_resolution",
        chosen="build",
        alternatives=[
            MaterializeAlternative(
                ref="fix",
                reason_not_picked="admitted but not packet.stage",
            )
        ],
        confidence=None,
        notes="",
    )
    dumped = decision.model_dump()
    rehydrated = MaterializeDecision.model_validate(dumped)
    assert rehydrated == decision
    assert rehydrated.alternatives[0].ref == "fix"


def test_materialize_decision_chosen_may_be_none_for_ambiguous_binding():
    decision = MaterializeDecision(
        decision_kind="data_pill_binding",
        chosen=None,
        alternatives=[
            MaterializeAlternative(ref="users.first_name", reason_not_picked="ambiguous"),
            MaterializeAlternative(ref="customers.first_name", reason_not_picked="ambiguous"),
        ],
    )
    assert decision.chosen is None
    assert len(decision.alternatives) == 2


def test_packet_decision_record_defaults_are_empty_lists():
    record = PacketDecisionRecord(packet_label="p")
    assert record.decisions == []
    assert record.unresolved_options == []


def test_materialize_review_payload_full_round_trip():
    payload = MaterializeReviewPayload(
        lane="auto",
        workflow_id="wf-foundation-smoke",
        run_id=None,
        packets=[
            PacketDecisionRecord(
                packet_label="p1",
                decisions=[
                    MaterializeDecision(
                        decision_kind="agent_selection",
                        chosen="auto/build",
                        alternatives=[],
                    )
                ],
            )
        ],
        warnings=[],
    )
    dumped = payload.model_dump()
    rehydrated = MaterializeReviewPayload.model_validate(dumped)
    assert rehydrated == payload
    assert rehydrated.packets[0].decisions[0].chosen == "auto/build"


def test_materialize_review_payload_lane_rejects_unknown_token():
    """The lane literal collapsed from three values to two — only ``auto`` and
    ``manifest`` are accepted. The legacy ``compile`` token must fail
    validation so consumers don't accidentally rely on it."""
    with pytest.raises(Exception):
        MaterializeReviewPayload(lane="compile", workflow_id="wf")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# 2. _build_packet_map_entry surfaces alternatives_considered
# ---------------------------------------------------------------------------


def test_packet_map_entry_alternatives_empty_when_job_has_no_decisions():
    entry = _build_packet_map_entry(job=_job())
    assert entry["alternatives_considered"] == []


def test_packet_map_entry_alternatives_mirrors_job_materialize_decisions():
    decisions = [
        {
            "decision_kind": "stage_resolution",
            "chosen": "build",
            "alternatives": [
                {"ref": "fix", "reason_not_picked": "admitted but not packet.stage"}
            ],
            "confidence": None,
            "notes": "",
        }
    ]
    entry = _build_packet_map_entry(job=_job(materialize_decisions=decisions))
    assert entry["alternatives_considered"] == decisions
    # And each one is shape-valid against the Pydantic contract.
    validated = MaterializeDecision.model_validate(entry["alternatives_considered"][0])
    assert validated.decision_kind == "stage_resolution"
    assert validated.alternatives[0].ref == "fix"


def test_packet_map_entry_alternatives_is_a_copy_not_reference():
    """The job's materialize_decisions list must not be mutated by consumers
    inspecting the packet_map entry."""
    decisions: list[dict] = []
    job = _job(materialize_decisions=decisions)
    entry = _build_packet_map_entry(job=job)
    entry["alternatives_considered"].append({"sentinel": True})
    assert decisions == []  # original list untouched


# ---------------------------------------------------------------------------
# 3. Structured-error available_options on the failure path
# ---------------------------------------------------------------------------


def test_unresolved_stage_error_carries_available_options_per_entry():
    err = UnresolvedStageError(
        [{"index": 0, "label": "p", "stage": "made_up_stage"}]
    )
    entry = err.unresolved_stages[0]
    assert "available_options" in entry
    refs = [opt["ref"] for opt in entry["available_options"]]
    assert refs == sorted(_STAGE_TEMPLATES.keys())
    for opt in entry["available_options"]:
        assert opt["reason_not_picked"] == "admitted in _STAGE_TEMPLATES but not chosen"


def test_unresolved_stage_error_back_compat_legacy_keys_preserved():
    """typed_gap_events.py reads entry['index'], entry['label'], entry['stage'] —
    those keys must survive the structural enrichment."""
    err = UnresolvedStageError(
        [{"index": 7, "label": "alpha", "stage": "rumble"}]
    )
    entry = err.unresolved_stages[0]
    assert entry["index"] == 7
    assert entry["label"] == "alpha"
    assert entry["stage"] == "rumble"


def test_unresolved_source_ref_error_back_compat_unresolved_refs_stays_strings():
    """Legacy consumers (typed_gap_events.py:329, test_spec_compiler_source_refs.py)
    iterate ``unresolved_refs`` as bare strings. That contract is preserved."""
    refs = ["decision.X", "review.Y", "discovery.Z"]
    err = UnresolvedSourceRefError(refs)
    assert err.unresolved_refs == refs


def test_unresolved_source_ref_error_unresolved_entries_carries_options():
    err = UnresolvedSourceRefError(["xyz:nope"])
    assert len(err.unresolved_entries) == 1
    entry = err.unresolved_entries[0]
    assert entry["ref"] == "xyz:nope"
    assert len(entry["available_options"]) > 0
    # Known prefixes from _SOURCE_REF_DISPATCH must be present.
    refs_in_options = {opt["ref"] for opt in entry["available_options"]}
    assert "BUG-" in refs_in_options
    assert "roadmap_item." in refs_in_options


def test_unresolved_write_scope_error_carries_empty_available_options_stub():
    """Foundation stub: write-scope has no resolver offering runners-up
    today. The field exists so the future scope_resolver lane fills it
    without a shape change at consumers."""
    err = UnresolvedWriteScopeError(
        [{"index": 0, "label": "p", "description_preview": "do it"}]
    )
    entry = err.unresolved_writes[0]
    assert entry["available_options"] == []
    # Legacy keys preserved.
    assert entry["index"] == 0
    assert entry["label"] == "p"
    assert entry["description_preview"] == "do it"


# ---------------------------------------------------------------------------
# 4. End-to-end through materialize_plan: stage failure surfaces options
# ---------------------------------------------------------------------------


def test_compile_plan_failure_path_surfaces_available_options_via_unresolved_stage():
    """The failure-mode seam: when materialize_plan raises UnresolvedStageError,
    each entry's available_options gives the chat-model Review handler the
    runner-ups to explain (the success-mode seam is alternatives_considered
    on packet_map, exercised by test 2 above).

    Note: the umbrella function is still named ``materialize_plan`` in this
    slice — the wider Compile→Materialize identifier rename is a deferred
    follow-on. The lane discriminator ``MaterializeLane`` and the runtime
    ``runtime.materialize`` package are renamed; the substrate function
    names will follow when their 123 callers migrate.
    """
    plan = Plan(
        name="foundation_failure_smoke",
        packets=[
            PlanPacket(
                description="This stage will not resolve",
                write=["src/foundation_smoke.py"],
                stage="made_up_stage_that_does_not_exist",
            ),
        ],
    )
    with pytest.raises(UnresolvedStageError) as exc_info:
        materialize_plan(plan, conn=_StubConn())
    err = exc_info.value
    assert len(err.unresolved_stages) == 1
    entry = err.unresolved_stages[0]
    assert entry["stage"] == "made_up_stage_that_does_not_exist"
    assert len(entry["available_options"]) == len(_STAGE_TEMPLATES)
