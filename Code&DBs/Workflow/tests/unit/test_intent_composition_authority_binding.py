"""Unit tests for compose-time authority binding wired into intent_composition."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from runtime.intent_composition import (
    _binding_for_packet,
    _packet_write_paths_to_targets,
    attach_authority_bindings_to_packets,
)
from runtime.spec_compiler import PlanPacket


@dataclass
class _FakeConn:
    successor_map: dict[tuple[str, str], dict[str, Any]] = field(default_factory=dict)
    obligations_for_successor: dict[tuple[str, str], list[dict[str, Any]]] = field(
        default_factory=dict
    )

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        assert "FROM authority_canonical_successor_for" in query
        unit_kind, unit_ref = args
        return self.successor_map.get((unit_kind, unit_ref))

    def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        assert "FROM authority_active_predecessor_obligations" in query
        kinds, refs = args
        out: list[dict[str, Any]] = []
        for kind, ref in zip(kinds, refs, strict=False):
            out.extend(self.obligations_for_successor.get((kind, ref), []))
        return out


def _ob_row(
    *,
    successor_kind: str,
    successor_ref: str,
    predecessor_kind: str,
    predecessor_ref: str,
    summary: str = "preserve invariants",
) -> dict[str, Any]:
    return {
        "successor_unit_kind": successor_kind,
        "successor_unit_ref": successor_ref,
        "predecessor_unit_kind": predecessor_kind,
        "predecessor_unit_ref": predecessor_ref,
        "supersession_status": "compat",
        "obligation_summary": summary,
        "obligation_evidence": {},
        "source_candidate_id": "cand:1",
        "source_impact_id": "imp:1",
        "source_decision_ref": "decision.test",
    }


def test_packet_write_paths_to_targets_skips_workspace_root() -> None:
    targets = _packet_write_paths_to_targets(["."])
    assert targets == []


def test_packet_write_paths_to_targets_skips_empty_entries() -> None:
    targets = _packet_write_paths_to_targets(["", "  ", "."])
    assert targets == []


def test_packet_write_paths_classifies_known_authority_paths() -> None:
    targets = _packet_write_paths_to_targets(
        [
            "Code&DBs/Databases/migrations/workflow/342_foo.sql",
            "Code&DBs/Workflow/runtime/operations/commands/foo.py",
            "docs/notes.md",
        ]
    )
    by_kind = {t["unit_kind"]: t["unit_ref"] for t in targets}
    assert by_kind["migration_ref"] == "Code&DBs/Databases/migrations/workflow/342_foo.sql"
    assert by_kind["handler_ref"] == "Code&DBs/Workflow/runtime/operations/commands/foo.py"
    assert by_kind["source_path"] == "docs/notes.md"


def test_packet_write_paths_dedupes_repeats() -> None:
    targets = _packet_write_paths_to_targets(["docs/x.md", "docs/x.md"])
    assert len(targets) == 1


def test_binding_for_packet_returns_none_when_no_conn() -> None:
    packet = PlanPacket(description="x", write=["docs/x.md"], stage="build", label="step_1")
    assert _binding_for_packet(None, packet) is None


def test_binding_for_packet_returns_none_when_only_workspace_root() -> None:
    packet = PlanPacket(description="x", write=["."], stage="build", label="step_1")
    assert _binding_for_packet(_FakeConn(), packet) is None


def test_binding_for_packet_returns_resolved_binding() -> None:
    foo_v1 = "Code&DBs/Workflow/runtime/operations/commands/foo_v1.py"
    foo_v2 = "Code&DBs/Workflow/runtime/operations/commands/foo_v2.py"
    conn = _FakeConn(
        successor_map={
            ("handler_ref", foo_v1): {
                "successor_unit_kind": "handler_ref",
                "successor_unit_ref": foo_v2,
                "supersession_status": "compat",
                "supersession_id": "ss:1",
                "updated_at": "2026-04-29T00:00:00+00:00",
            }
        },
        obligations_for_successor={
            ("handler_ref", foo_v2): [
                _ob_row(
                    successor_kind="handler_ref",
                    successor_ref=foo_v2,
                    predecessor_kind="handler_ref",
                    predecessor_ref=foo_v1,
                    summary="legacy callers expect dict shape",
                )
            ]
        },
    )
    packet = PlanPacket(
        description="patch foo",
        write=[foo_v1],
        stage="build",
        label="step_1",
    )
    binding = _binding_for_packet(conn, packet)
    assert binding is not None
    assert len(binding["canonical_write_scope"]) == 1
    assert binding["canonical_write_scope"][0]["unit_ref"] == foo_v2
    assert binding["canonical_write_scope"][0]["was_redirected"] is True
    assert len(binding["predecessor_obligations"]) == 1
    assert (
        binding["predecessor_obligations"][0]["obligation_summary"]
        == "legacy callers expect dict shape"
    )


def test_attach_authority_bindings_to_packets_attaches_per_packet() -> None:
    x_path = "Code&DBs/Workflow/runtime/operations/commands/x.py"
    old_x = "Code&DBs/Workflow/runtime/operations/commands/old_x.py"
    conn = _FakeConn(
        obligations_for_successor={
            ("handler_ref", x_path): [
                _ob_row(
                    successor_kind="handler_ref",
                    successor_ref=x_path,
                    predecessor_kind="handler_ref",
                    predecessor_ref=old_x,
                )
            ]
        }
    )
    packets = [
        PlanPacket(description="touches authority", write=[x_path], stage="build", label="step_1"),
        PlanPacket(description="touches root", write=["."], stage="build", label="step_2"),
    ]
    rebound = attach_authority_bindings_to_packets(conn, packets)
    assert rebound[0].authority_binding is not None
    assert len(rebound[0].authority_binding["predecessor_obligations"]) == 1
    assert rebound[1].authority_binding is None


def test_attach_authority_bindings_no_op_when_conn_missing() -> None:
    packets = [
        PlanPacket(description="x", write=["docs/x.md"], stage="build", label="step_1"),
    ]
    rebound = attach_authority_bindings_to_packets(None, packets)
    assert rebound[0].authority_binding is None
    assert rebound is not packets  # returns a fresh list, leaves originals untouched
