from __future__ import annotations

import json
from pathlib import Path

from runtime.bug_resolution_program import (
    build_workflow_chain_payload,
    build_coordination_payload,
    classify_bug_lane,
    materialize_packet_specs,
)


def _bug(
    bug_id: str,
    *,
    title: str,
    category: str,
    replay_ready: bool,
    replay_reason_code: str = "bug.replay_not_ready",
    owner_ref: str | None = None,
    severity: str = "P2",
    tags: tuple[str, ...] = (),
) -> dict[str, object]:
    return {
        "bug_id": bug_id,
        "title": title,
        "category": category,
        "severity": severity,
        "status": "OPEN",
        "summary": title,
        "description": title,
        "owner_ref": owner_ref,
        "tags": tags,
        "replay_ready": replay_ready,
        "replay_reason_code": replay_reason_code,
    }


def _ok(command: str, payload: dict[str, object]) -> dict[str, object]:
    return {
        "ok": True,
        "command": command,
        "exit_code": 0,
        "stderr": "",
        "payload": payload,
        "error": "",
    }


def _failed(command: str) -> dict[str, object]:
    return {
        "ok": False,
        "command": command,
        "exit_code": 1,
        "stderr": "authority unavailable",
        "payload": {},
        "error": "authority unavailable",
    }


def test_classify_bug_lane_prefers_authority_signals() -> None:
    lane = classify_bug_lane(
        _bug(
            "BUG-1",
            title="workflow bug search can hang silently",
            category="RUNTIME",
            replay_ready=False,
            owner_ref="authority.bugs",
        )
    )
    assert lane == "authority_bug_system"


def test_classify_bug_lane_routes_frontend_manifest_failures() -> None:
    lane = classify_bug_lane(
        _bug(
            "BUG-2",
            title="App manifest route returns SPA HTML instead of a web manifest",
            category="WIRING",
            replay_ready=True,
        )
    )
    assert lane == "app_wiring_frontend"


def test_build_coordination_payload_uses_wave_zero_for_authority_and_wave_one_for_blocked() -> None:
    bugs = [
        _bug(
            "BUG-A",
            title="workflow CLI discover and bug search can hang silently",
            category="RUNTIME",
            replay_ready=False,
            owner_ref="authority.bugs",
        ),
        _bug(
            "BUG-B",
            title="scripts/praxis restart api fails when Compose env is absent",
            category="RUNTIME",
            replay_ready=False,
            replay_reason_code="bug.replay_missing_run_context",
        ),
        _bug(
            "BUG-C",
            title="App registers /sw.js but API server returns 404",
            category="WIRING",
            replay_ready=True,
            replay_reason_code="bug.replay_ready",
        ),
    ]
    clusters = [
        {
            "cluster_key": "bug.owner:authority.bugs",
            "label": "owner: authority.bugs",
            "reason_code": "bug_cluster.owner_ref",
            "bug_ids": ["BUG-A"],
        },
        {
            "cluster_key": "bug.singleton:BUG-B",
            "label": "setup bug",
            "reason_code": "bug_cluster.singleton",
            "bug_ids": ["BUG-B"],
        },
        {
            "cluster_key": "bug.singleton:BUG-C",
            "label": "frontend bug",
            "reason_code": "bug_cluster.singleton",
            "bug_ids": ["BUG-C"],
        },
    ]
    coordination = build_coordination_payload(
        program_id="bug_resolution_program_20260423",
        orient_result=_ok("./scripts/praxis workflow orient --json", {"standing_orders": []}),
        stats_result=_ok(
            "./scripts/praxis workflow bugs stats --json",
            {"stats": {"open_count": 3, "replay_ready_count": 1}},
        ),
        list_result=_ok(
            "./scripts/praxis workflow bugs list --json",
            {"bugs": bugs, "clusters": clusters, "count": 3, "returned_count": 3},
        ),
        search_result=_ok(
            "./scripts/praxis workflow bugs search timeout --json",
            {"bugs": [], "count": 0, "returned_count": 0},
        ),
        replay_ready_result=_ok(
            "./scripts/praxis workflow tools call praxis_replay_ready_bugs",
            {"view": "replay_ready_bugs", "bugs": [{"bug_id": "BUG-C", "replay_ready": True}]},
        ),
        generated_at="2026-04-23T12:00:00+00:00",
    )

    assert coordination["coordination_state"] == "frozen"
    packets = coordination["packets"]
    assert len(packets) == 3
    authority_packet = next(packet for packet in packets if packet["bug_ids"] == ["BUG-A"])
    blocked_packet = next(packet for packet in packets if packet["bug_ids"] == ["BUG-B"])
    ready_packet = next(packet for packet in packets if packet["bug_ids"] == ["BUG-C"])
    assert authority_packet["wave_id"] == "wave_0_authority_repair"
    assert blocked_packet["wave_id"] == "wave_1_evidence_normalization"
    assert ready_packet["wave_id"] == "wave_2_execute"


def test_build_coordination_payload_blocks_when_authority_fails() -> None:
    coordination = build_coordination_payload(
        program_id="bug_resolution_program_20260423",
        orient_result=_failed("./scripts/praxis workflow orient --json"),
        stats_result=_ok("./scripts/praxis workflow bugs stats --json", {"stats": {"open_count": 0}}),
        list_result=_ok("./scripts/praxis workflow bugs list --json", {"bugs": [], "count": 0}),
        search_result=_ok("./scripts/praxis workflow bugs search timeout --json", {"bugs": [], "count": 0}),
        replay_ready_result=_ok(
            "./scripts/praxis workflow tools call praxis_replay_ready_bugs",
            {"view": "replay_ready_bugs", "bugs": []},
        ),
    )
    assert coordination["coordination_state"] == "blocked_authority"
    assert coordination["packets"] == []
    assert coordination["errors"]


def test_materialize_packet_specs_renders_valid_json(tmp_path: Path) -> None:
    coordination = {
        "coordination_state": "frozen",
        "packets": [
            {
                "packet_id": "bug_resolution_program.packet-a",
                "packet_slug": "packet-a",
                "wave_id": "wave_2_execute",
                "lane_id": "workflow_runtime",
                "lane_label": "Workflow / runtime",
                "packet_kind": "build_or_close",
                "bug_ids": ["BUG-A"],
                "bug_titles": ["Example runtime failure"],
                "authority_owner": "lane:workflow_runtime",
                "cluster": {
                    "cluster_key": "bug.singleton:BUG-A",
                    "label": "Example runtime failure",
                },
                "verification_surface": "Focused runtime verification",
                "done_criteria": ["Close the bug with proof."],
                "stop_boundary": "Do not widen scope.",
                "depends_on_wave": ["wave_1_evidence_normalization"],
            }
        ],
    }
    template = json.dumps(
        {
            "name": "{{PACKET_NAME}}",
            "workflow_id": "{{PACKET_SLUG}}",
            "queue_id": "{{PACKET_SLUG}}",
            "phase": "bug_resolution_packet",
            "jobs": [
                {
                    "label": "plan",
                    "agent": "openai/gpt-5.4",
                    "prompt": "Packet {{PACKET_ID}} bugs {{BUG_IDS_JSON}} stop {{STOP_BOUNDARY}}",
                }
            ],
        }
    )

    rendered = materialize_packet_specs(
        coordination=coordination,
        template_text=template,
        coordination_path="/tmp/coordination.json",
        output_dir=tmp_path,
    )

    assert len(rendered) == 1
    payload = json.loads((tmp_path / "packet-a.queue.json").read_text(encoding="utf-8"))
    assert payload["workflow_id"] == "packet-a"
    assert "BUG-A" in payload["jobs"][0]["prompt"]


def test_build_workflow_chain_payload_batches_packets_in_wave_order() -> None:
    coordination = {
        "program_id": "bug_resolution_program_20260423",
        "coordination_state": "frozen",
        "max_parallel_lanes": 2,
        "packets": [
            {"packet_slug": "wave0-a", "wave_id": "wave_0_authority_repair"},
            {"packet_slug": "wave0-b", "wave_id": "wave_0_authority_repair"},
            {"packet_slug": "wave0-c", "wave_id": "wave_0_authority_repair"},
            {"packet_slug": "wave1-a", "wave_id": "wave_1_evidence_normalization"},
        ],
    }
    packet_specs = [
        {"packet_slug": "wave0-a", "spec_path": "packets/wave0-a.queue.json"},
        {"packet_slug": "wave0-b", "spec_path": "packets/wave0-b.queue.json"},
        {"packet_slug": "wave0-c", "spec_path": "packets/wave0-c.queue.json"},
        {"packet_slug": "wave1-a", "spec_path": "packets/wave1-a.queue.json"},
    ]

    chain = build_workflow_chain_payload(
        coordination=coordination,
        packet_specs=packet_specs,
        max_parallel=2,
    )

    assert chain["program"] == "bug_resolution_program_20260423"
    assert chain["validate_order"] == [
        "packets/wave0-a.queue.json",
        "packets/wave0-b.queue.json",
        "packets/wave0-c.queue.json",
        "packets/wave1-a.queue.json",
    ]
    assert chain["waves"] == [
        {
            "wave_id": "wave_0_authority_repair_batch_001",
            "specs": ["packets/wave0-a.queue.json", "packets/wave0-b.queue.json"],
        },
        {
            "wave_id": "wave_0_authority_repair_batch_002",
            "specs": ["packets/wave0-c.queue.json"],
            "depends_on": ["wave_0_authority_repair_batch_001"],
        },
        {
            "wave_id": "wave_1_evidence_normalization_batch_001",
            "specs": ["packets/wave1-a.queue.json"],
            "depends_on": ["wave_0_authority_repair_batch_002"],
        },
    ]
