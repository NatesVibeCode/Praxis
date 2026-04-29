"""Phase 2.1.E regression: packet_map entries carry typed release gates.

Before this change, the packet_map entry exposed ``expected_gates``
(verify_ref list) but no typed gate contracts. Moon-generated workflows
shipped without release gates that could auto-satisfy from typed state,
forcing gate evaluation to fall back to human review (BUG-2729F8B7).

The fix: ``_build_packet_map_entry`` now emits ``expected_typed_gates``
derived from the job's ``produces`` types. Each produced type becomes a
gate dict with ``auto_satisfies_when_produced=True``, so gate
satisfaction is computable from typed state — no human in the loop, per
the autonomous-first directive.

The packet map entry also surfaces ``consumes`` / ``consumes_any`` /
``produces`` directly so downstream consumers don't have to go re-resolve
the contract.
"""
from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.spec_compiler import _build_packet_map_entry, PlanPacket


def _job(**overrides):
    base = {
        "label": "step-1",
        "agent": "auto/build",
        "task_type": "build",
        "produces": ["code_change_candidate", "execution_receipt"],
        "consumes": [],
        "consumes_any": ["research_findings", "input_text"],
        "write_scope": ["src/foo.py"],
    }
    base.update(overrides)
    return base


def test_packet_map_entry_carries_expected_typed_gates_from_produces():
    entry = _build_packet_map_entry(job=_job())
    gates = entry["expected_typed_gates"]
    assert isinstance(gates, list)
    assert len(gates) == 2
    assert {g["type"] for g in gates} == {"code_change_candidate", "execution_receipt"}
    for gate in gates:
        assert gate["kind"] == "typed_produce"
        assert gate["auto_satisfies_when_produced"] is True


def test_packet_map_entry_surfaces_consumes_and_produces_directly():
    entry = _build_packet_map_entry(job=_job())
    assert entry["consumes"] == []
    assert entry["consumes_any"] == ["research_findings", "input_text"]
    assert entry["produces"] == ["code_change_candidate", "execution_receipt"]


def test_packet_map_entry_with_no_produces_emits_empty_typed_gates():
    entry = _build_packet_map_entry(job=_job(produces=[]))
    assert entry["expected_typed_gates"] == []
    assert entry["produces"] == []


def test_packet_map_entry_legacy_expected_gates_field_unchanged():
    """Adding typed gates must NOT remove or modify the legacy
    ``expected_gates`` (verify_ref list) field."""
    entry = _build_packet_map_entry(
        job=_job(verify_refs=["verify.foo", "verify.bar"])
    )
    assert entry["expected_gates"] == ["verify.foo", "verify.bar"]
    # And typed gates still emitted alongside
    assert len(entry["expected_typed_gates"]) == 2


def test_packet_map_entry_typed_gates_independent_of_verify_refs():
    """A job with no verify_refs but with produces still gets typed gates.
    Autonomous gate satisfaction does not require verify_refs to exist."""
    entry = _build_packet_map_entry(job=_job(verify_refs=[]))
    assert entry["expected_gates"] == []
    assert len(entry["expected_typed_gates"]) == 2
    assert {g["type"] for g in entry["expected_typed_gates"]} == {
        "code_change_candidate",
        "execution_receipt",
    }


def test_packet_map_entry_with_packet_supplies_typed_gates_from_job():
    """When a packet is also supplied, typed gates still come from the
    job's ``produces`` (the post-compile typed state), not the raw
    packet."""
    packet = PlanPacket(
        description="Implement the feature",
        write=["src/foo.py"],
        stage="build",
        label="step-1",
    )
    entry = _build_packet_map_entry(packet=packet, job=_job())
    # Typed gates come from the job, not from the packet (packet has no
    # produces field; the job is the post-compile shape that does).
    assert {g["type"] for g in entry["expected_typed_gates"]} == {
        "code_change_candidate",
        "execution_receipt",
    }
