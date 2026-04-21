#!/usr/bin/env python3
"""Launch the mobile_oversight 8-phase workflow chain.

Standalone CLI entry point so the chain can be kicked off from a Terminal
without Claude Code attached (the Mac needs the RAM).

Usage:
  scripts/launch_mobile_oversight_chain.py
  scripts/launch_mobile_oversight_chain.py --dry-run
  scripts/launch_mobile_oversight_chain.py --coordination-path config/cascade/chain/mobile_oversight_program.json

Once submitted, the already-loaded launchd agent com.praxis.agent-sessions
drives the chain forward via _advance_background_workflow_chains on every
worker-loop tick. No additional cron is required until Phase 7's healer lands.

Observe progress from any terminal (no CC needed):
  praxis workflow run-status <wave_run_id>
  praxis workflow query "chain status"

NOTE: this wraps the same service-bus path the MCP tool praxis_workflow(
action='chain') uses internally. A proper praxis workflow chain CLI frontdoor
is tracked as BUG-61881910; until that ships, this script is the CLI.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
sys.path.insert(0, str(WORKFLOW_ROOT))

DEFAULT_COORDINATION_PATH = "config/cascade/chain/mobile_oversight_program.json"


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--coordination-path",
        default=DEFAULT_COORDINATION_PATH,
        help=f"Path to the chain coordination JSON (default: {DEFAULT_COORDINATION_PATH})",
    )
    parser.add_argument(
        "--no-adopt-active",
        action="store_true",
        help=(
            "Do NOT adopt already-active workflow runs that target the same specs. "
            "Default adopt=True lets an existing run satisfy a wave."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse + validate the chain and specs only. Do not submit.",
    )
    parser.add_argument(
        "--requested-by-ref",
        default="scripts.launch_mobile_oversight_chain",
        help="Provenance tag for the command row.",
    )
    return parser.parse_args()


def _dry_run(coord_path: Path) -> int:
    from runtime.workflow_chain import (
        load_workflow_chain,
        validate_workflow_chain,
    )
    from surfaces.mcp.subsystems import _subs

    pg = _subs.get_pg_conn()
    program = load_workflow_chain(str(coord_path), repo_root=str(REPO_ROOT))
    print(f"program:        {program.program}")
    print(f"mode:           {program.mode}")
    print(f"validate_order: {len(program.validate_order)} specs")
    print(f"waves:          {len(program.waves)}")
    for wave in program.waves:
        deps = list(wave.depends_on) or ["-"]
        print(f"  {wave.wave_id:<30s}  deps={deps}  specs={len(wave.spec_paths)}")
    print()

    validations = validate_workflow_chain(
        program, repo_root=str(REPO_ROOT), pg_conn=pg
    )
    all_valid = all(item.get("valid", False) for item in validations)
    for item in validations:
        mark = "OK" if item.get("valid") else "FAIL"
        print(f"  [{mark:4s}] {item.get('spec_path')}")
        if not item.get("valid") and item.get("error"):
            print(f"          error: {item['error'][:200]}")
    print()
    print("DRY-RUN:", "all valid" if all_valid else "INVALID — submission would reject")
    return 0 if all_valid else 1


def _submit(coord_path: Path, *, adopt_active: bool, requested_by_ref: str) -> int:
    from runtime.control_commands import submit_workflow_chain_command
    from surfaces.mcp.subsystems import _subs

    pg = _subs.get_pg_conn()
    try:
        response = submit_workflow_chain_command(
            pg,
            requested_by_kind="cli",
            requested_by_ref=requested_by_ref,
            coordination_path=str(coord_path),
            repo_root=str(REPO_ROOT),
            adopt_active=adopt_active,
        )
    except Exception as exc:
        print(f"SUBMIT FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        # Surface the __cause__ chain if present (BUG-90681A62 mitigation).
        cause = exc.__cause__
        depth = 1
        while cause is not None and depth <= 5:
            print(
                f"  caused by [{depth}]: {type(cause).__name__}: {cause}",
                file=sys.stderr,
            )
            cause = cause.__cause__
            depth += 1
        return 1

    print(json.dumps(response, indent=2, default=str))
    chain_id = response.get("chain_id") or response.get("workflow_chain_id")
    if chain_id:
        print()
        print(f"CHAIN LAUNCHED: {chain_id}")
        print(
            "Observe:  praxis workflow query 'chain status'  "
            "(or praxis workflow run-status <wave_run_id>)"
        )
    return 0


def main() -> int:
    args = _parse_args()
    coord_path = REPO_ROOT / args.coordination_path
    if not coord_path.is_file():
        print(f"coordination path not found: {coord_path}", file=sys.stderr)
        return 2

    if args.dry_run:
        return _dry_run(coord_path)

    return _submit(
        coord_path,
        adopt_active=not args.no_adopt_active,
        requested_by_ref=args.requested_by_ref,
    )


if __name__ == "__main__":
    sys.exit(main())
