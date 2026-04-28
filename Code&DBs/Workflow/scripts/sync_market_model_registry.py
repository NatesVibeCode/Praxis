#!/usr/bin/env python3
"""Sync external market benchmark data into registry tables.

Keeps cross-vendor benchmark/comparison data in a dedicated registry surface.
Matched rows are also copied into provider_model_candidates.benchmark_profile
so runtime and API surfaces can expose common metrics for executable models.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import hashlib
import json
import os
from pathlib import Path
import sys
from typing import Any

import asyncpg

_WORKFLOW_ROOT = Path(__file__).resolve().parent.parent
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from scripts.sync_framework import (
    add_database_url_arg,
    add_dry_run_arg,
    db_connect,
    decision_ref as make_decision_ref,
    http_json,
    jsonb,
    require_database_url,
    run_and_print,
    utc_now,
)

DEFAULT_SOURCE_SLUG = "artificial_analysis"
DEFAULT_MODALITY = "llm"

_UPSERT_MARKET_ROW_SQL = """
    INSERT INTO market_model_registry (
        market_model_ref, source_slug, modality, source_model_id,
        source_model_slug, model_name, creator_id, creator_slug, creator_name,
        evaluations, pricing, speed_metrics, prompt_options, raw_payload,
        decision_ref, first_seen_at, last_synced_at
    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,$13::jsonb,$14::jsonb,$15,$16,$17)
    ON CONFLICT (market_model_ref) DO UPDATE SET
        source_slug=EXCLUDED.source_slug, modality=EXCLUDED.modality,
        source_model_id=EXCLUDED.source_model_id, source_model_slug=EXCLUDED.source_model_slug,
        model_name=EXCLUDED.model_name, creator_id=EXCLUDED.creator_id,
        creator_slug=EXCLUDED.creator_slug, creator_name=EXCLUDED.creator_name,
        evaluations=EXCLUDED.evaluations, pricing=EXCLUDED.pricing,
        speed_metrics=EXCLUDED.speed_metrics, prompt_options=EXCLUDED.prompt_options,
        raw_payload=EXCLUDED.raw_payload, decision_ref=EXCLUDED.decision_ref,
        last_synced_at=EXCLUDED.last_synced_at
"""

_UPSERT_BINDING_SQL = """
    INSERT INTO provider_model_market_bindings
        (provider_model_market_binding_id, candidate_ref, market_model_ref,
         binding_kind, binding_confidence, decision_ref, bound_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7)
    ON CONFLICT (candidate_ref, market_model_ref) DO UPDATE SET
        decision_ref=EXCLUDED.decision_ref, bound_at=EXCLUDED.bound_at,
        binding_kind=EXCLUDED.binding_kind, binding_confidence=EXCLUDED.binding_confidence
"""

_CLEAR_BINDINGS_SQL = """
    DELETE FROM provider_model_market_bindings AS b
    USING market_model_registry AS m
    WHERE b.candidate_ref=$1 AND b.market_model_ref=m.market_model_ref AND m.source_slug=$2
"""


def _market_model_ref(source_slug: str, modality: str, source_model_id: str) -> str:
    return f"market_model.{source_slug}.{modality}.{source_model_id}"

def _binding_ref(candidate_ref: str, market_model_ref: str) -> str:
    digest = hashlib.sha1(f"{candidate_ref}|{market_model_ref}".encode()).hexdigest()[:20]
    return f"provider_model_market_binding.{digest}"

def _json_object(value: object, *, field_name: str) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise RuntimeError(f"{field_name} must be a JSON object")
    return dict(value)


async def _load_source_config(conn: asyncpg.Connection, source_slug: str) -> dict[str, Any]:
    row = await conn.fetchrow(
        "SELECT source_slug,display_name,api_url,api_key_env_var,modality,"
        "request_headers,common_metric_paths,creator_slug_aliases,enabled "
        "FROM market_benchmark_source_registry WHERE source_slug=$1 LIMIT 1",
        source_slug,
    )
    if row is None:
        raise RuntimeError(f"market_benchmark_source_registry missing source_slug={source_slug}")
    if not bool(row["enabled"]):
        raise RuntimeError(f"market benchmark source {source_slug} is disabled")
    return {
        "source_slug": str(row["source_slug"]),
        "display_name": str(row["display_name"]),
        "api_url": str(row["api_url"]),
        "api_key_env_var": str(row["api_key_env_var"]),
        "modality": str(row["modality"] or DEFAULT_MODALITY),
        "request_headers": _json_object(row["request_headers"], field_name="request_headers"),
        "common_metric_paths": {str(k): str(v) for k, v in _json_object(row["common_metric_paths"], field_name="common_metric_paths").items()},
        "creator_slug_aliases": {str(k).strip().lower(): str(v).strip().lower() for k, v in _json_object(row["creator_slug_aliases"], field_name="creator_slug_aliases").items()},
    }


async def _load_match_rules(conn: asyncpg.Connection, *, source_slug: str) -> dict[tuple[str, str], dict[str, Any]]:
    rows = await conn.fetch(
        "SELECT provider_model_market_match_rule_id,source_slug,provider_slug,candidate_model_slug,"
        "target_creator_slug,target_source_model_slug,match_kind,binding_confidence,"
        "selection_metadata,decision_ref,enabled FROM provider_model_market_match_rules WHERE source_slug=$1",
        source_slug,
    )
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["provider_slug"]), str(row["candidate_model_slug"]))
        if key in lookup:
            raise RuntimeError(f"duplicate candidate keys in match_rules for source_slug={source_slug}: {key[0]}/{key[1]}")
        sel = row["selection_metadata"]
        if isinstance(sel, str):
            sel = json.loads(sel)
        if not isinstance(sel, dict):
            raise RuntimeError(f"selection_metadata must be a JSON object for {key[0]}/{key[1]}")
        tsm = row["target_source_model_slug"]
        lookup[key] = {
            "provider_model_market_match_rule_id": str(row["provider_model_market_match_rule_id"]),
            "source_slug": str(row["source_slug"]),
            "provider_slug": key[0], "candidate_model_slug": key[1],
            "target_creator_slug": str(row["target_creator_slug"]),
            "target_source_model_slug": str(tsm) if isinstance(tsm, str) and tsm.strip() else None,
            "match_kind": str(row["match_kind"]),
            "binding_confidence": float(row["binding_confidence"]),
            "selection_metadata": dict(sel),
            "decision_ref": str(row["decision_ref"]),
            "enabled": bool(row["enabled"]),
        }
    return lookup


async def _candidate_lookup(conn: asyncpg.Connection) -> dict[tuple[str, str], tuple[str, dict[str, Any]]]:
    rows = await conn.fetch(
        "SELECT DISTINCT ON (provider_slug,model_slug) candidate_ref,provider_slug,model_slug,benchmark_profile "
        "FROM provider_model_candidates WHERE status='active' "
        "ORDER BY provider_slug,model_slug,priority ASC,created_at DESC"
    )
    out: dict[tuple[str, str], tuple[str, dict[str, Any]]] = {}
    for row in rows:
        bp = row["benchmark_profile"]
        if isinstance(bp, str):
            bp = json.loads(bp)
        out[(str(row["provider_slug"]), str(row["model_slug"]))] = (str(row["candidate_ref"]), dict(bp) if isinstance(bp, dict) else {})
    return out


def _resolve_api_key(source_config: dict[str, Any], explicit: str | None) -> str:
    if isinstance(explicit, str) and explicit.strip():
        return explicit.strip()
    env_var = str(source_config.get("api_key_env_var") or "").strip()
    if not env_var:
        raise RuntimeError("market benchmark source missing api_key_env_var")
    key = os.environ.get(env_var, "").strip()
    if not key:
        raise RuntimeError(f"--api-key is required (or set {env_var})")
    return key

def _request_headers(source_config: dict[str, Any], *, api_key: str) -> dict[str, str]:
    h = {str(k): str(v) for k, v in dict(source_config.get("request_headers") or {}).items()}
    return {k: v.replace("{api_key}", api_key) for k, v in h.items()}

def _lookup_path(item: dict[str, Any], path: str) -> Any:
    cur: Any = item
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur

def _normalize_market_row(
    item: dict[str, Any], *, source_config: dict[str, Any],
    prompt_options: dict[str, Any], dec_ref: str, synced_at: datetime,
) -> dict[str, Any]:
    creator = item.get("model_creator")
    if not isinstance(creator, dict):
        raise RuntimeError(f"market row missing model_creator: {item!r}")
    source_slug = str(source_config.get("source_slug") or DEFAULT_SOURCE_SLUG)
    modality = str(source_config.get("modality") or DEFAULT_MODALITY)
    source_model_id = str(item.get("id") or "").strip()
    source_model_slug = str(item.get("slug") or "").strip()
    model_name = str(item.get("name") or "").strip()
    aliases = dict(source_config.get("creator_slug_aliases") or {})
    raw_cs = str(creator.get("slug") or "").strip().lower()
    creator_slug = aliases.get(raw_cs, raw_cs)
    creator_name = str(creator.get("name") or "").strip()
    if not all((source_model_id, source_model_slug, model_name, creator_slug, creator_name)):
        raise RuntimeError(f"market row missing stable identity fields: {item!r}")
    return {
        "market_model_ref": _market_model_ref(source_slug, modality, source_model_id),
        "source_slug": source_slug, "modality": modality,
        "source_model_id": source_model_id, "source_model_slug": source_model_slug,
        "model_name": model_name, "creator_id": str(creator.get("id") or "").strip() or None,
        "creator_slug": creator_slug, "creator_name": creator_name,
        "evaluations": item.get("evaluations") if isinstance(item.get("evaluations"), dict) else {},
        "pricing": item.get("pricing") if isinstance(item.get("pricing"), dict) else {},
        "speed_metrics": {
            "median_output_tokens_per_second": item.get("median_output_tokens_per_second"),
            "median_time_to_first_token_seconds": item.get("median_time_to_first_token_seconds"),
            "median_time_to_first_answer_token": item.get("median_time_to_first_answer_token"),
        },
        "prompt_options": dict(prompt_options), "raw_payload": dict(item),
        "decision_ref": dec_ref, "synced_at": synced_at,
    }

def load_market_models(source_config: dict[str, Any], *, api_key: str) -> tuple[dict[str, Any], ...]:
    payload = http_json(str(source_config["api_url"]), headers=_request_headers(source_config, api_key=api_key))
    data = payload.get("data")
    if not isinstance(data, list):
        raise RuntimeError("Artificial Analysis response did not contain a data list")
    prompt_options = payload.get("prompt_options") or {}
    if not isinstance(prompt_options, dict):
        prompt_options = {}
    dec_ref = make_decision_ref("market-model-sync")
    synced_at = utc_now()
    return tuple(
        _normalize_market_row(item, source_config=source_config, prompt_options=prompt_options,
                              dec_ref=dec_ref, synced_at=synced_at)
        for item in data if isinstance(item, dict)
    )

def _market_row_lookup(rows: tuple[dict[str, Any], ...]) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["creator_slug"]), str(row["source_model_slug"]))
        if key in lookup:
            raise RuntimeError(f"market feed returned duplicate creator/slug rows for {key[0]}/{key[1]}")
        lookup[key] = row
    return lookup


def _validate_coverage(
    *, candidate_lookup: dict, match_rules: dict, source_slug: str
) -> None:
    rule_keys = {k for k, v in match_rules.items() if bool(v.get("enabled"))}
    missing = sorted(set(candidate_lookup) - rule_keys)
    if missing:
        raise RuntimeError(
            f"provider_model_market_match_rules missing enabled rows for source_slug={source_slug}: "
            + ", ".join(f"{p}/{m}" for p, m in missing)
            + ". Re-run with --auto-seed to plan+write rules from the onboarding "
            "planner, or preview first via "
            "`python3 scripts/seed_market_match_rules.py --source "
            f"{source_slug}` (then re-run with --apply)."
        )

def _resolve_rule_row(
    *, rule: dict[str, Any], market_lookup: dict[tuple[str, str], dict[str, Any]]
) -> dict[str, Any] | None:
    if str(rule.get("match_kind")) == "source_unavailable":
        return None
    slug = rule.get("target_source_model_slug")
    if not isinstance(slug, str) or not slug.strip():
        return None
    key = (str(rule["target_creator_slug"]), slug.strip())
    row = market_lookup.get(key)
    if row is None:
        raise RuntimeError(f"match_rules references market row not in feed: {key[0]}/{key[1]}")
    return row


async def _upsert_market_row(conn: asyncpg.Connection, row: dict[str, Any]) -> None:
    await conn.execute(
        _UPSERT_MARKET_ROW_SQL,
        row["market_model_ref"], row["source_slug"], row["modality"],
        row["source_model_id"], row["source_model_slug"], row["model_name"],
        row["creator_id"], row["creator_slug"], row["creator_name"],
        jsonb(row["evaluations"]), jsonb(row["pricing"]), jsonb(row["speed_metrics"]),
        jsonb(row["prompt_options"]), jsonb(row["raw_payload"]),
        row["decision_ref"], row["synced_at"], row["synced_at"],
    )

async def _upsert_binding(conn: asyncpg.Connection, *, candidate_ref: str, market_model_ref: str,
                          binding_kind: str, binding_confidence: float, dec_ref: str, bound_at: datetime) -> None:
    await conn.execute(_UPSERT_BINDING_SQL,
        _binding_ref(candidate_ref, market_model_ref), candidate_ref, market_model_ref,
        binding_kind, binding_confidence, dec_ref, bound_at)

async def _clear_bindings(conn: asyncpg.Connection, *, candidate_ref: str, source_slug: str) -> None:
    await conn.execute(_CLEAR_BINDINGS_SQL, candidate_ref, source_slug)

async def _write_benchmark_profile(conn: asyncpg.Connection, *, candidate_ref: str,
                                   existing_profile: dict[str, Any], market_benchmark: dict[str, Any]) -> None:
    merged = {**existing_profile, "market_benchmark": dict(market_benchmark)}
    await conn.execute(
        "UPDATE provider_model_candidates SET benchmark_profile=$2::jsonb WHERE candidate_ref=$1",
        candidate_ref, jsonb(merged),
    )

def _benchmark_payload(*, market_row: dict[str, Any], source_config: dict[str, Any], rule: dict[str, Any]) -> dict[str, Any]:
    metric_paths = dict(source_config.get("common_metric_paths") or {})
    return {
        "coverage_status": "bound",
        "source_slug": market_row["source_slug"],
        "market_model_ref": market_row["market_model_ref"],
        "source_model_id": market_row["source_model_id"],
        "source_model_slug": market_row["source_model_slug"],
        "model_name": market_row["model_name"],
        "creator_slug": market_row["creator_slug"],
        "creator_name": market_row["creator_name"],
        "evaluations": market_row["evaluations"],
        "pricing": market_row["pricing"],
        "speed_metrics": market_row["speed_metrics"],
        "prompt_options": market_row["prompt_options"],
        "common_metrics": {name: _lookup_path(market_row["raw_payload"], path) for name, path in metric_paths.items()},
        "binding_kind": rule["match_kind"],
        "binding_confidence": rule["binding_confidence"],
        "binding_rule_ref": rule["provider_model_market_match_rule_id"],
        "binding_decision_ref": rule["decision_ref"],
        "selection_metadata": dict(rule["selection_metadata"]),
        "last_synced_at": market_row["synced_at"].isoformat(),
    }

def _benchmark_gap_payload(*, source_slug: str, rule: dict[str, Any], synced_at: datetime) -> dict[str, Any]:
    return {
        "coverage_status": "source_unavailable",
        "source_slug": source_slug,
        "binding_kind": rule["match_kind"],
        "binding_confidence": rule["binding_confidence"],
        "binding_rule_ref": rule["provider_model_market_match_rule_id"],
        "binding_decision_ref": rule["decision_ref"],
        "selection_metadata": dict(rule["selection_metadata"]),
        "last_synced_at": synced_at.isoformat(),
    }


async def apply_benchmark_decisions(
    conn: asyncpg.Connection,
    *,
    decisions: list[dict[str, Any]],
    source_slug: str,
    source_config: dict[str, Any],
    decision_ref: str,
) -> tuple[int, int]:
    """Apply pre-computed benchmark match decisions for a single provider.

    Used by the provider onboarding wizard, which has already produced a
    plan via `_plan_benchmark_rules` and just needs the market row upsert
    + binding + benchmark_profile write per decision. Mirrors the inner
    loop of `run_sync` but skipped the feed/match-rule/coverage steps the
    wizard does itself. Returns ``(bound_count, gap_count)``.
    """
    bound = 0
    gaps = 0
    fallback_synced_at = utc_now()
    for decision in decisions:
        candidate_ref = str(decision["candidate_ref"])
        match_kind = str(decision["match_kind"])
        binding_confidence = float(decision["binding_confidence"])
        existing_profile = dict(decision.get("existing_benchmark_profile") or {})
        rule = {
            "match_kind": match_kind,
            "binding_confidence": binding_confidence,
            "provider_model_market_match_rule_id": str(decision["rule_ref"]),
            "decision_ref": decision_ref,
            "selection_metadata": dict(decision.get("selection_metadata") or {}),
        }
        market_row = decision.get("market_row")

        await _clear_bindings(conn, candidate_ref=candidate_ref, source_slug=source_slug)

        if not isinstance(market_row, dict):
            gaps += 1
            await _write_benchmark_profile(
                conn,
                candidate_ref=candidate_ref,
                existing_profile=existing_profile,
                market_benchmark=_benchmark_gap_payload(
                    source_slug=source_slug, rule=rule, synced_at=fallback_synced_at,
                ),
            )
            continue

        mrow = dict(market_row)
        await _upsert_market_row(conn, mrow)
        await _upsert_binding(
            conn,
            candidate_ref=candidate_ref,
            market_model_ref=str(mrow["market_model_ref"]),
            binding_kind=match_kind,
            binding_confidence=binding_confidence,
            dec_ref=decision_ref,
            bound_at=mrow.get("synced_at") or fallback_synced_at,
        )
        await _write_benchmark_profile(
            conn,
            candidate_ref=candidate_ref,
            existing_profile=existing_profile,
            market_benchmark=_benchmark_payload(
                market_row=mrow, source_config=source_config, rule=rule,
            ),
        )
        bound += 1
    return bound, gaps


# ---------------------------------------------------------------------------
# Sync entrypoint
# ---------------------------------------------------------------------------

def _autoseed_via_gateway(*, source_slug: str) -> dict[str, Any]:
    """Dispatch the registered match_rules.backfill operation through the gateway.

    Each invocation records an authority_operation_receipts row and emits a
    `match_rules.backfilled` event. Imports are deferred so this script
    stays usable without a fully-imported runtime when gateway dispatch is
    not desired.
    """
    import os
    from runtime.operation_catalog_gateway import execute_operation_from_env

    return execute_operation_from_env(
        env=dict(os.environ),
        operation_name="match_rules.backfill",
        payload={"source_slug": source_slug, "dry_run": False},
    )


async def run_sync(*, database_url: str, source_slug: str = DEFAULT_SOURCE_SLUG,
                   api_key: str | None = None, dry_run: bool,
                   auto_seed: bool = False) -> dict[str, Any]:
    conn = await db_connect(database_url)
    try:
        source_config = await _load_source_config(conn, source_slug)
        slug = str(source_config["source_slug"])
        autoseed_summary: dict[str, Any] | None = None
        if auto_seed and not dry_run:
            autoseed_summary = _autoseed_via_gateway(source_slug=slug)
        match_rules = await _load_match_rules(conn, source_slug=slug)
        rows = load_market_models(source_config, api_key=_resolve_api_key(source_config, api_key))
        market_lookup = _market_row_lookup(rows)
        candidate_lookup = await _candidate_lookup(conn)
        _validate_coverage(candidate_lookup=candidate_lookup, match_rules=match_rules, source_slug=slug)

        matched = 0
        unavailable = 0
        targeted: set[tuple[str, str]] = set()

        def _tally(candidate_key: tuple) -> dict[str, Any] | None:
            nonlocal matched, unavailable
            mrow = _resolve_rule_row(rule=match_rules[candidate_key], market_lookup=market_lookup)
            if mrow is None:
                unavailable += 1
            else:
                matched += 1
                targeted.add((str(mrow["creator_slug"]), str(mrow["source_model_slug"])))
            return mrow

        def _summary(extra: dict | None = None) -> dict[str, Any]:
            base = {
                "ok": True, "dry_run": dry_run, "source": slug,
                "market_models": len(rows), "matched_candidate_rows": matched,
                "bound_market_models": len(targeted), "coverage_gap_candidate_rows": unavailable,
                "unmatched_market_models": len(rows) - len(targeted),
            }
            if autoseed_summary is not None:
                base["autoseed"] = autoseed_summary
            return {**base, **(extra or {})}

        if dry_run:
            for ck in candidate_lookup:
                _tally(ck)
            return _summary()

        async with conn.transaction():
            for row in rows:
                await _upsert_market_row(conn, row)
            for ck in sorted(candidate_lookup):
                candidate_ref, benchmark_profile = candidate_lookup[ck]
                rule = match_rules[ck]
                mrow = _tally(ck)
                await _clear_bindings(conn, candidate_ref=candidate_ref, source_slug=slug)
                if mrow is None:
                    await _write_benchmark_profile(conn, candidate_ref=candidate_ref,
                        existing_profile=benchmark_profile,
                        market_benchmark=_benchmark_gap_payload(
                            source_slug=slug, rule=rule,
                            synced_at=rows[0]["synced_at"] if rows else utc_now()))
                else:
                    await _upsert_binding(conn, candidate_ref=candidate_ref,
                        market_model_ref=mrow["market_model_ref"], binding_kind=str(rule["match_kind"]),
                        binding_confidence=float(rule["binding_confidence"]),
                        dec_ref=str(rule["decision_ref"]), bound_at=mrow["synced_at"])
                    await _write_benchmark_profile(conn, candidate_ref=candidate_ref,
                        existing_profile=benchmark_profile,
                        market_benchmark=_benchmark_payload(market_row=mrow, source_config=source_config, rule=rule))

        total_rows = await conn.fetchval("SELECT COUNT(*) FROM market_model_registry")
        total_bindings = await conn.fetchval("SELECT COUNT(*) FROM provider_model_market_bindings")
        return _summary({"registry_rows": int(total_rows or 0), "binding_rows": int(total_bindings or 0)})
    finally:
        await conn.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync external market benchmark data into market_model_registry.")
    add_database_url_arg(parser)
    parser.add_argument("--source", "--source-slug", default=DEFAULT_SOURCE_SLUG,
                        help="Registry source_slug from market_benchmark_source_registry.")
    parser.add_argument("--api-key", default=None,
                        help="API key override (else uses source row's api_key_env_var).")
    add_dry_run_arg(parser)
    parser.add_argument(
        "--auto-seed",
        action="store_true",
        help=(
            "Plan and write missing provider_model_market_match_rules in the "
            "same transaction before validating coverage. Uses the onboarding "
            "planner; safe defaults (exact / normalized / family / unavailable)."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    source_slug = str(args.source).strip() or DEFAULT_SOURCE_SLUG
    api_key = args.api_key.strip() if isinstance(args.api_key, str) and args.api_key.strip() else None
    return run_and_print(run_sync(database_url=require_database_url(args),
                                  source_slug=source_slug, api_key=api_key, dry_run=args.dry_run,
                                  auto_seed=bool(args.auto_seed)))


if __name__ == "__main__":
    raise SystemExit(main())
