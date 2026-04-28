#!/usr/bin/env python3
"""Backfill provider_model_market_match_rules through the CQRS gateway.

Thin operator-facing wrapper around the registered ``match_rules.backfill``
operation. The operation handler runs the same `_plan_benchmark_rules`
planner the onboarding wizard uses and writes via `_apply_benchmark_plan`,
so each call records a receipt and emits a `match_rules.backfilled` event.

Default is a dry-run preview. Re-run with ``--apply`` to write.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import sys

_WORKFLOW_ROOT = Path(__file__).resolve().parent.parent
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.operation_catalog_gateway import execute_operation_from_env


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Backfill provider_model_market_match_rules via the registered "
            "match_rules.backfill CQRS operation. Each call records an "
            "authority_operation_receipts row and emits a "
            "match_rules.backfilled event."
        )
    )
    parser.add_argument(
        "--source",
        "--source-slug",
        default="artificial_analysis",
        help="market_benchmark_source_registry.source_slug to backfill against.",
    )
    parser.add_argument(
        "--provider",
        "--provider-slug",
        action="append",
        default=[],
        help=(
            "Restrict to specific provider_slugs. Repeat for multiple. "
            "Default: all providers with active candidates missing rules."
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write rules + benchmark_profile. Without this flag the script previews.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload: dict = {
        "source_slug": str(args.source).strip() or "artificial_analysis",
        "dry_run": not bool(args.apply),
    }
    if args.provider:
        payload["provider_slugs"] = tuple(p.strip() for p in args.provider if p.strip())

    result = execute_operation_from_env(
        env=dict(os.environ),
        operation_name="match_rules.backfill",
        payload=payload,
    )
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0 if result.get("ok") is not False else 1


if __name__ == "__main__":
    raise SystemExit(main())
