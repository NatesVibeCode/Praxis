"""Shell-callable entrypoint to the onboarding gate-probe graph.

scripts/bootstrap and other shell frontdoors delegate prereq checks here so
error wording lives in one place (the probe's remediation_hint) instead of
being duplicated between shell ``die`` strings and Python remediation.

Usage:
    python3.14 -m runtime.onboarding.bootstrap_cli check <gate_ref>
        Evaluate one gate. Prints the full GateResult as JSON on stdout.
        Exit 0 if status=='ok', 1 otherwise.

    python3.14 -m runtime.onboarding.bootstrap_cli require <gate_ref>
        Evaluate one gate. On success prints "ok <gate_ref>" to stdout and
        exits 0. On failure prints the gate's title + remediation_hint to
        stderr and exits 1. Designed for shell ``|| exit 1`` patterns.

    python3.14 -m runtime.onboarding.bootstrap_cli graph [--json]
        Evaluate the whole graph. --json emits the full payload; otherwise
        a one-line-per-gate summary. Exit 0 if all gates ok, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path

from . import ONBOARDING_GRAPH
from .graph import GateResult


_DEFAULT_REPO_ROOT_ENV = "PRAXIS_ONBOARDING_REPO_ROOT"


def _resolve_repo_root() -> Path:
    override = os.environ.get(_DEFAULT_REPO_ROOT_ENV)
    if override:
        return Path(override).resolve()
    # Two parents up from this file: runtime/onboarding/bootstrap_cli.py
    # -> runtime/onboarding -> runtime -> Code&DBs/Workflow -> Code&DBs -> repo_root
    return Path(__file__).resolve().parents[4]


def _result_to_dict(result: GateResult) -> dict:
    payload = asdict(result)
    payload["observed_state"] = dict(result.observed_state)
    payload["evaluated_at"] = result.evaluated_at.isoformat()
    return payload


def _cmd_check(args: argparse.Namespace) -> int:
    repo_root = _resolve_repo_root()
    env = dict(os.environ)
    results = ONBOARDING_GRAPH.evaluate(env, repo_root)
    target = None
    for result in results:
        if result.gate_ref == args.gate_ref:
            target = result
            break
    if target is None:
        print(
            json.dumps(
                {
                    "ok": False,
                    "error_code": "onboarding.gate_unknown",
                    "gate_ref": args.gate_ref,
                    "message": (
                        f"Unknown or non-applicable gate_ref: {args.gate_ref!r}. "
                        "Use the 'graph' subcommand to list applicable gates."
                    ),
                }
            ),
            file=sys.stdout,
        )
        return 1
    print(json.dumps({"ok": target.status == "ok", **_result_to_dict(target)}, sort_keys=True))
    return 0 if target.status == "ok" else 1


def _cmd_require(args: argparse.Namespace) -> int:
    repo_root = _resolve_repo_root()
    env = dict(os.environ)
    results = ONBOARDING_GRAPH.evaluate(env, repo_root)
    target = None
    for result in results:
        if result.gate_ref == args.gate_ref:
            target = result
            break
    if target is None:
        print(
            f"onboarding: unknown or non-applicable gate_ref {args.gate_ref!r}",
            file=sys.stderr,
        )
        return 1
    if target.status == "ok":
        print(f"ok {target.gate_ref}")
        return 0
    probe = ONBOARDING_GRAPH.probe(target.gate_ref)
    print(f"{probe.title} [{target.status}]", file=sys.stderr)
    if target.remediation_hint:
        print(target.remediation_hint, file=sys.stderr)
    if target.remediation_doc_url:
        print(f"See: {target.remediation_doc_url}", file=sys.stderr)
    return 1


def _cmd_graph(args: argparse.Namespace) -> int:
    repo_root = _resolve_repo_root()
    env = dict(os.environ)
    results = ONBOARDING_GRAPH.evaluate(env, repo_root)
    all_ok = all(r.status == "ok" for r in results)
    if args.json:
        payload = {
            "ok": all_ok,
            "repo_root": str(repo_root),
            "gates": [_result_to_dict(r) for r in results],
            "summary": {
                "total": len(results),
                "ok": sum(1 for r in results if r.status == "ok"),
                "missing": sum(1 for r in results if r.status == "missing"),
                "blocked": sum(1 for r in results if r.status == "blocked"),
                "unknown": sum(1 for r in results if r.status == "unknown"),
            },
        }
        print(json.dumps(payload, sort_keys=True))
    else:
        for r in sorted(results, key=lambda x: (x.gate_ref,)):
            print(f"  [{r.status:8s}] {r.gate_ref}")
    return 0 if all_ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python3.14 -m runtime.onboarding.bootstrap_cli",
        description="Shell-callable probe authority for Praxis onboarding gates.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    check = sub.add_parser("check", help="Evaluate one gate and print JSON result.")
    check.add_argument("gate_ref", help="Canonical gate_ref, e.g. platform.python3_14")
    check.set_defaults(handler=_cmd_check)

    require = sub.add_parser(
        "require",
        help="Evaluate one gate; exit 1 with stderr remediation if not ok.",
    )
    require.add_argument("gate_ref")
    require.set_defaults(handler=_cmd_require)

    graph = sub.add_parser("graph", help="Evaluate the whole graph.")
    graph.add_argument("--json", action="store_true", help="Emit full JSON payload.")
    graph.set_defaults(handler=_cmd_graph)

    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
