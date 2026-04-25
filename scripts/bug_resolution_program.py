#!/usr/bin/env python3
"""Freeze and materialize the bug-resolution workflow program."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any
from collections.abc import Mapping


REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
DEFAULT_COORDINATION_OUTPUT = (
    REPO_ROOT
    / "Code&DBs"
    / "Workflow"
    / "artifacts"
    / "workflow"
    / "bug_resolution_program"
    / "bug_resolution_program_kickoff_20260423.json"
)
DEFAULT_PACKET_TEMPLATE = (
    REPO_ROOT
    / "Code&DBs"
    / "Workflow"
    / "artifacts"
    / "workflow"
    / "bug_resolution_program_packet_template_20260423.queue.json"
)
DEFAULT_PACKET_OUTPUT_DIR = (
    REPO_ROOT
    / "Code&DBs"
    / "Workflow"
    / "artifacts"
    / "workflow"
    / "bug_resolution_program"
    / "packets"
)
DEFAULT_CHAIN_OUTPUT = (
    REPO_ROOT
    / "Code&DBs"
    / "Workflow"
    / "artifacts"
    / "workflow"
    / "bug_resolution_program"
    / "bug_resolution_program_chain_20260423.json"
)

if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))


def _call_tool(command: str, handler, params: Mapping[str, Any]) -> dict[str, Any]:
    try:
        payload = handler(dict(params))
    except Exception as exc:  # noqa: BLE001 - freeze must report authority truthfully
        payload = {}
        error = f"{type(exc).__name__}: {exc}"
    else:
        error = ""
        if isinstance(payload, dict) and payload.get("error"):
            error = str(payload.get("error"))
        elif not isinstance(payload, dict):
            error = "tool returned non-dict payload"
    return {
        "ok": error == "" and isinstance(payload, dict),
        "command": command,
        "exit_code": 0 if error == "" else 1,
        "stdout": "",
        "stderr": "",
        "payload": payload if isinstance(payload, dict) else {},
        "error": error,
    }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _default_program_id() -> str:
    from runtime.bug_resolution_program import utc_now_iso
    return f"bug_resolution_program_{utc_now_iso()[:10].replace('-', '')}"


def _freeze_command(args: argparse.Namespace) -> int:
    from runtime.bug_resolution_program import (
        build_coordination_payload,
        utc_now_iso,
    )
    from surfaces.mcp.tools.bugs import tool_praxis_bugs
    from surfaces.mcp.tools.operator import (
        tool_praxis_orient,
        tool_praxis_replay_ready_bugs,
    )

    program_id = args.program_id or _default_program_id()
    output_path = Path(args.output).resolve()

    orient = _call_tool("praxis_orient", tool_praxis_orient, {})
    stats = _call_tool("praxis_bugs(action=stats)", tool_praxis_bugs, {"action": "stats"})
    open_count = int(
        ((stats.get("payload") or {}).get("stats") or {}).get("open_count") or args.default_limit
    )
    open_count = max(1, open_count)
    listing = _call_tool(
        "praxis_bugs(action=list,include_replay_state=true)",
        tool_praxis_bugs,
        {
            "action": "list",
            "limit": open_count,
            "open_only": True,
            "include_replay_state": True,
        },
    )
    search = _call_tool(
        "praxis_bugs(action=search)",
        tool_praxis_bugs,
        {
            "action": "search",
            "title": args.search_probe,
            "limit": 5,
            "open_only": True,
        },
    )
    replay_ready = _call_tool(
        "praxis_replay_ready_bugs",
        tool_praxis_replay_ready_bugs,
        {"limit": open_count},
    )

    coordination = build_coordination_payload(
        program_id=program_id,
        orient_result=orient,
        stats_result=stats,
        list_result=listing,
        search_result=search,
        replay_ready_result=replay_ready,
        generated_at=utc_now_iso(),
    )
    _write_json(output_path, coordination)
    summary = {
        "program_id": coordination["program_id"],
        "coordination_state": coordination["coordination_state"],
        "output_path": str(output_path),
        "packet_count": len(coordination.get("packets") or ()),
        "bug_count": int(((coordination.get("snapshot") or {}).get("count")) or 0),
    }
    print(json.dumps(summary, indent=2))
    return 0 if coordination["coordination_state"] == "frozen" else 1


def _materialize_packets_command(args: argparse.Namespace) -> int:
    from runtime.bug_resolution_program import materialize_packet_specs

    coordination_path = Path(args.coordination).resolve()
    template_path = Path(args.template).resolve()
    output_dir = Path(args.output_dir).resolve()

    coordination = json.loads(coordination_path.read_text(encoding="utf-8"))
    if coordination.get("coordination_state") != "frozen":
        print(
            json.dumps(
                {
                    "error": "coordination is not frozen",
                    "coordination_state": coordination.get("coordination_state"),
                    "coordination_path": str(coordination_path),
                },
                indent=2,
            )
        )
        return 1

    template_text = template_path.read_text(encoding="utf-8")
    materialized = materialize_packet_specs(
        coordination=coordination,
        template_text=template_text,
        coordination_path=str(coordination_path),
        output_dir=output_dir,
    )
    print(
        json.dumps(
            {
                "coordination_path": str(coordination_path),
                "packet_count": len(materialized),
                "output_dir": str(output_dir),
                "packets": materialized,
            },
            indent=2,
        )
    )
    return 0 if materialized else 1


def _materialize_chain_command(args: argparse.Namespace) -> int:
    from runtime.bug_resolution_program import (
        build_workflow_chain_payload,
        materialize_packet_specs,
    )

    coordination_path = Path(args.coordination).resolve()
    template_path = Path(args.template).resolve()
    packet_output_dir = Path(args.packet_output_dir).resolve()
    output_path = Path(args.output).resolve()

    coordination = json.loads(coordination_path.read_text(encoding="utf-8"))
    if coordination.get("coordination_state") != "frozen":
        print(
            json.dumps(
                {
                    "error": "coordination is not frozen",
                    "coordination_state": coordination.get("coordination_state"),
                    "coordination_path": str(coordination_path),
                },
                indent=2,
            )
        )
        return 1

    template_text = template_path.read_text(encoding="utf-8")
    packet_specs = materialize_packet_specs(
        coordination=coordination,
        template_text=template_text,
        coordination_path=str(coordination_path),
        output_dir=packet_output_dir,
    )
    for packet_spec in packet_specs:
        spec_path = Path(str(packet_spec.get("spec_path") or ""))
        if spec_path.is_absolute():
            try:
                packet_spec["spec_path"] = str(spec_path.relative_to(REPO_ROOT))
            except ValueError:
                packet_spec["spec_path"] = str(spec_path)
    chain = build_workflow_chain_payload(
        coordination=coordination,
        packet_specs=packet_specs,
        max_parallel=args.max_parallel,
    )
    _write_json(output_path, chain)
    print(
        json.dumps(
            {
                "chain_path": str(output_path),
                "program": chain["program"],
                "wave_count": len(chain["waves"]),
                "spec_count": len(chain["validate_order"]),
                "max_parallel": args.max_parallel,
            },
            indent=2,
        )
    )
    return 0


def _open_wave_command(args: argparse.Namespace) -> int:
    from surfaces.mcp.tools.wave import tool_praxis_wave

    coordination_path = Path(args.coordination).resolve()
    coordination = json.loads(coordination_path.read_text(encoding="utf-8"))
    packet_wave_id = str(args.wave_id).rsplit(".", 1)[-1]
    jobs = [
        str(packet.get("packet_slug") or "").strip()
        for packet in coordination.get("packets") or ()
        if str(packet.get("wave_id") or "").strip() == packet_wave_id
        and str(packet.get("packet_slug") or "").strip()
    ]
    if not jobs:
        print(
            json.dumps(
                {
                    "error": "no packets found for wave",
                    "wave_id": args.wave_id,
                    "coordination_path": str(coordination_path),
                    "packet_wave_id": packet_wave_id,
                },
                indent=2,
            )
        )
        return 1

    result = _call_tool(
        f"praxis_wave(action=start,wave_id={args.wave_id})",
        tool_praxis_wave,
        {
            "action": "start",
            "wave_id": args.wave_id,
            "jobs": ",".join(jobs),
        },
    )
    response = result["payload"] if result.get("payload") else result
    if isinstance(response, dict):
        response.setdefault("job_count", len(jobs))
        response.setdefault("jobs", jobs)
        response.setdefault("coordination_path", str(coordination_path))
    print(json.dumps(response, indent=2))
    return 0 if result.get("ok") else 1


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Freeze and materialize the bug-resolution workflow program."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    freeze = subparsers.add_parser("freeze", help="capture the kickoff backlog and lane mapping")
    freeze.add_argument("--program-id", default="", help="durable program id")
    freeze.add_argument(
        "--output",
        default=str(DEFAULT_COORDINATION_OUTPUT),
        help="path to the generated coordination JSON",
    )
    freeze.add_argument(
        "--default-limit",
        type=int,
        default=200,
        help="fallback list limit when bug stats are unavailable",
    )
    freeze.add_argument(
        "--search-probe",
        default="timeout",
        help="search query used to prove bug search works during kickoff capture",
    )
    freeze.set_defaults(_handler=_freeze_command)

    materialize = subparsers.add_parser(
        "materialize-packets",
        help="render packet specs from a frozen coordination file",
    )
    materialize.add_argument("--coordination", required=True, help="frozen coordination JSON path")
    materialize.add_argument(
        "--template",
        default=str(DEFAULT_PACKET_TEMPLATE),
        help="packet template queue JSON",
    )
    materialize.add_argument(
        "--output-dir",
        default=str(DEFAULT_PACKET_OUTPUT_DIR),
        help="directory for rendered packet specs",
    )
    materialize.set_defaults(_handler=_materialize_packets_command)

    chain = subparsers.add_parser(
        "materialize-chain",
        help="render a durable workflow-chain coordination JSON from frozen packets",
    )
    chain.add_argument("--coordination", required=True, help="frozen coordination JSON path")
    chain.add_argument(
        "--template",
        default=str(DEFAULT_PACKET_TEMPLATE),
        help="packet template queue JSON",
    )
    chain.add_argument(
        "--packet-output-dir",
        default=str(DEFAULT_PACKET_OUTPUT_DIR),
        help="directory for rendered packet specs",
    )
    chain.add_argument(
        "--output",
        default=str(DEFAULT_CHAIN_OUTPUT),
        help="path to the generated workflow-chain coordination JSON",
    )
    chain.add_argument(
        "--max-parallel",
        type=int,
        default=5,
        help="maximum packet specs per durable chain wave",
    )
    chain.set_defaults(_handler=_materialize_chain_command)

    open_wave = subparsers.add_parser(
        "open-wave",
        help="start one wave through the canonical praxis_wave tool surface",
    )
    open_wave.add_argument("--wave-id", required=True, help="wave id to start")
    open_wave.add_argument(
        "--coordination",
        default=str(DEFAULT_COORDINATION_OUTPUT),
        help="frozen coordination JSON used to derive jobs for the wave",
    )
    open_wave.set_defaults(_handler=_open_wave_command)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return args._handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
