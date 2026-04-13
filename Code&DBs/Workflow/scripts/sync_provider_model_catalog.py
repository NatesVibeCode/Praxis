#!/usr/bin/env python3
"""Sync provider model inventories into provider_model_candidates.

- OpenAI: Codex CLI's local models cache.
- Google: Vertex publisher models endpoint using the local Gemini OAuth token.
- Anthropic: current direct API model IDs from Anthropic's official docs.
  We intentionally avoid live Anthropic probing when account usage is exhausted.
"""

from __future__ import annotations

import argparse
from collections.abc import Iterable
import json
from pathlib import Path
import sys
from typing import Any
from urllib.parse import urlencode

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

OPENAI_MODELS_CACHE_PATH = Path.home() / ".codex" / "models_cache.json"
GOOGLE_OAUTH_PATH = Path.home() / ".gemini" / "oauth_creds.json"
GOOGLE_PUBLISHER_MODELS_URL = "https://aiplatform.googleapis.com/v1beta1/publishers/google/models"
MODEL_PROFILE_AUTHORITY_PATH = _WORKFLOW_ROOT / "docs" / "model_catalog_classification_2026-04-08.json"

# https://platform.claude.com/docs/en/about-claude/models/overview — observed 2026-04-08
ANTHROPIC_DOC_MODELS = ("claude-opus-4-6", "claude-sonnet-4-6", "claude-haiku-4-5-20251001")

ACTIVE_MODEL_MIGRATIONS: dict[str, dict[str, str]] = {
    "anthropic": {
        "claude-sonnet-4-5": "claude-sonnet-4-6",
        "claude-haiku-4-5": "claude-haiku-4-5-20251001",
    },
}

_LEGACY_TIER = {"high": "frontier", "medium": "mid", "low": "economy"}
_PRIORITY_BASE = {"high": 500, "medium": 700, "low": 900}
_BALANCE_WEIGHT = {"high": 1, "medium": 2, "low": 3}
_PROFILE_AUTHORITY_CACHE: dict[str, Any] | None = None


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError as exc:
        raise RuntimeError(f"required file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"invalid JSON in {path}: {exc}") from exc

def _load_profile_authority() -> dict[str, Any]:
    global _PROFILE_AUTHORITY_CACHE
    if _PROFILE_AUTHORITY_CACHE is not None:
        return _PROFILE_AUTHORITY_CACHE
    payload = _load_json(MODEL_PROFILE_AUTHORITY_PATH)
    source_index, profiles = payload.get("source_index"), payload.get("profiles")
    if not isinstance(source_index, dict) or not isinstance(profiles, list):
        raise RuntimeError(f"{MODEL_PROFILE_AUTHORITY_PATH} did not contain source_index + profiles")
    index: dict[tuple[str, str], dict[str, Any]] = {}
    for raw in profiles:
        if not isinstance(raw, dict):
            raise RuntimeError(f"{MODEL_PROFILE_AUTHORITY_PATH} contained a non-object profile entry")
        p, m = raw.get("provider"), raw.get("models")
        if not isinstance(p, str) or not p.strip():
            raise RuntimeError(f"{MODEL_PROFILE_AUTHORITY_PATH} profile missing provider")
        if not isinstance(m, list) or not m:
            raise RuntimeError(f"{MODEL_PROFILE_AUTHORITY_PATH} profile {raw!r} missing models")
        for slug in m:
            if not isinstance(slug, str) or not slug.strip():
                raise RuntimeError(f"{MODEL_PROFILE_AUTHORITY_PATH} profile {p!r} blank model slug")
            key = (p.strip(), slug.strip())
            if key in index:
                raise RuntimeError(f"{MODEL_PROFILE_AUTHORITY_PATH} duplicated profile for {p}/{slug}")
            index[key] = dict(raw)
    _PROFILE_AUTHORITY_CACHE = {"source_index": dict(source_index), "profiles": profiles, "index": index}
    return _PROFILE_AUTHORITY_CACHE

def _model_profile(provider_slug: str, model_slug: str) -> dict[str, Any]:
    profile = _load_profile_authority()["index"].get((provider_slug, model_slug))
    if profile is None:
        raise RuntimeError(
            f"missing model classification authority for {provider_slug}/{model_slug}. "
            f"Update {MODEL_PROFILE_AUTHORITY_PATH} before syncing new inventory."
        )
    return profile

def _expanded_benchmark_profile(profile: dict[str, Any]) -> dict[str, Any]:
    bp = profile.get("benchmark_profile")
    if not isinstance(bp, dict):
        raise RuntimeError(f"invalid benchmark_profile for {profile.get('profile_id')}")
    refs = bp.get("source_refs") or []
    if not isinstance(refs, list):
        raise RuntimeError(f"invalid source_refs for {profile.get('profile_id')}")
    idx = _load_profile_authority()["source_index"]
    pid = profile.get("profile_id")
    urls = []
    for ref in refs:
        if not isinstance(ref, str) or not ref.strip():
            raise RuntimeError(f"blank source ref for {pid}")
        url = idx.get(ref)
        if not isinstance(url, str) or not url.strip():
            raise RuntimeError(f"missing source_index entry {ref!r} for {pid}")
        urls.append(url.strip())
    return {**bp, "source_urls": urls}


def _normalize_unique(items: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    out: list[str] = []
    for item in items:
        s = item.strip()
        if s and s not in seen:
            seen.add(s)
            out.append(s)
    return tuple(out)

def _capability_tags(profile: dict[str, Any], *, source_tag: str) -> tuple[str, ...]:
    tier = str(profile["route_tier"])
    legacy = _LEGACY_TIER.get(tier)
    if legacy is None:
        raise RuntimeError(f"unsupported route_tier {tier!r}")
    aff = profile.get("task_affinities") or {}
    if not isinstance(aff, dict):
        raise RuntimeError(f"invalid task_affinities for {profile.get('profile_id')}")
    tags: list[str] = [legacy, str(profile["latency_class"]), "inventory-sync", source_tag]
    for bucket in ("primary", "secondary", "specialized"):
        vals = aff.get(bucket) or []
        if not isinstance(vals, list):
            raise RuntimeError(f"invalid task_affinities.{bucket} for {profile.get('profile_id')}")
        tags.extend(str(v).strip() for v in vals if str(v).strip())
    return _normalize_unique(tags)

def _default_params(provider_slug: str, model_slug: str, profile: dict[str, Any], *, source: str, synced_at: str) -> dict[str, Any]:
    return {"inventory_source": source, "inventory_synced_at": synced_at,
            "provider_slug": provider_slug, "model_slug": model_slug,
            "classification_profile_id": profile["profile_id"]}


def load_openai_inventory(cache_path: Path = OPENAI_MODELS_CACHE_PATH) -> tuple[str, ...]:
    payload = _load_json(cache_path)
    models = payload.get("models")
    if not isinstance(models, list):
        raise RuntimeError(f"{cache_path} did not contain a models list")
    slugs = [i["slug"] for i in models if isinstance(i, dict) and isinstance(i.get("slug"), str) and i["slug"].strip()]
    inventory = _normalize_unique(slugs)
    if not inventory:
        raise RuntimeError(f"{cache_path} did not contain any model slugs")
    return inventory


def load_google_inventory() -> tuple[str, ...]:
    payload = _load_json(GOOGLE_OAUTH_PATH)
    token = payload.get("access_token")
    if not isinstance(token, str) or not token.strip():
        raise RuntimeError(f"{GOOGLE_OAUTH_PATH} did not contain an access_token")
    headers = {"Authorization": f"Bearer {token.strip()}", "Accept": "application/json"}
    models: list[str] = []
    page_token: str | None = None
    while True:
        query: dict[str, str] = {"pageSize": "300"}
        if page_token:
            query["pageToken"] = page_token
        resp = http_json(f"{GOOGLE_PUBLISHER_MODELS_URL}?{urlencode(query)}", headers=headers)
        raw = resp.get("publisherModels") or resp.get("models")
        if not isinstance(raw, list):
            break
        for item in raw:
            name = item.get("name") if isinstance(item, dict) else None
            if isinstance(name, str) and "/" in name:
                models.append(name.rsplit("/", 1)[-1])
        next_token = resp.get("nextPageToken")
        if not isinstance(next_token, str) or not next_token.strip():
            break
        page_token = next_token.strip()
    inventory = _normalize_unique(models)
    if not inventory:
        raise RuntimeError("google inventory probe returned no model slugs")
    return inventory


def load_anthropic_inventory() -> tuple[str, ...]:
    return ANTHROPIC_DOC_MODELS


def _build_cli_config(profile: asyncpg.Record) -> dict[str, Any]:
    """Build CLI config from provider_cli_profiles — no hardcoded provider branches."""
    base_flags = profile["base_flags"]
    if isinstance(base_flags, str):
        base_flags = json.loads(base_flags)
    if not isinstance(base_flags, list):
        base_flags = []
    cmd = [profile["binary_name"], *[str(f) for f in base_flags]]
    flag = profile["model_flag"]
    if isinstance(flag, str) and flag.strip():
        cmd.extend([flag, "{model}"])
    return {
        "prompt_mode": str(profile.get("prompt_mode") or "stdin"),
        "cmd_template": cmd,
        "envelope_key": profile["output_envelope_key"],
        "output_format": profile["output_format"],
    }


def _count_from_status(status: str) -> int:
    tail = status.rsplit(" ", 1)[-1]
    return int(tail) if tail.isdigit() else 0


async def _migrate_active_aliases(
    conn: asyncpg.Connection,
    *,
    provider_slug: str,
    decision_ref: str,
    cli_config: dict[str, Any],
    migrations: dict[str, str],
) -> int:
    moved = 0
    cli_json = jsonb(cli_config)
    for old_slug, new_slug in migrations.items():
        status = await conn.execute(
            "UPDATE provider_model_candidates SET model_slug=$1, cli_config=$2::jsonb, decision_ref=$3 "
            "WHERE provider_slug=$4 AND model_slug=$5 AND status='active'",
            new_slug, cli_json, decision_ref, provider_slug, old_slug,
        )
        moved += _count_from_status(status)
    return moved


async def _active_model_exists(conn: asyncpg.Connection, *, provider_slug: str, model_slug: str) -> bool:
    return await conn.fetchval(
        "SELECT 1 FROM provider_model_candidates "
        "WHERE provider_slug=$1 AND model_slug=$2 AND status='active' LIMIT 1",
        provider_slug, model_slug,
    ) == 1


def _profile_params(
    profile: dict[str, Any],
    *,
    provider_slug: str,
    model_slug: str,
    cli_config: dict[str, Any],
    source: str,
    source_tag: str,
    synced_at: Any,
    decision_ref: str,
) -> tuple[Any, ...]:
    """Build the 13 shared column values used by both UPDATE and INSERT."""
    return (
        jsonb(cli_config),
        decision_ref,
        jsonb(_capability_tags(profile, source_tag=source_tag)),
        jsonb(_default_params(provider_slug, model_slug, profile, source=source, synced_at=synced_at.isoformat())),
        str(profile["route_tier"]),
        int(profile["route_tier_rank"]),
        str(profile["latency_class"]),
        int(profile["latency_rank"]),
        jsonb(profile["reasoning_control"]),
        jsonb(profile["task_affinities"]),
        jsonb(_expanded_benchmark_profile(profile)),
    )


_UPDATE_ACTIVE_SQL = """
    UPDATE provider_model_candidates
    SET cli_config=$1::jsonb, decision_ref=$2,
        capability_tags=$5::jsonb, default_parameters=$6::jsonb,
        route_tier=$7, route_tier_rank=$8, latency_class=$9, latency_rank=$10,
        reasoning_control=$11::jsonb, task_affinities=$12::jsonb, benchmark_profile=$13::jsonb
    WHERE provider_slug=$3 AND model_slug=$4 AND status='active'
      AND (cli_config IS DISTINCT FROM $1::jsonb OR decision_ref IS DISTINCT FROM $2
           OR capability_tags IS DISTINCT FROM $5::jsonb OR default_parameters IS DISTINCT FROM $6::jsonb
           OR route_tier IS DISTINCT FROM $7 OR route_tier_rank IS DISTINCT FROM $8
           OR latency_class IS DISTINCT FROM $9 OR latency_rank IS DISTINCT FROM $10
           OR reasoning_control IS DISTINCT FROM $11::jsonb OR task_affinities IS DISTINCT FROM $12::jsonb
           OR benchmark_profile IS DISTINCT FROM $13::jsonb)
"""

_UPDATE_FULL_SQL = """
    UPDATE provider_model_candidates
    SET provider_ref=$2, provider_name=$3, provider_slug=$4, model_slug=$5, status=$6,
        priority=$7, balance_weight=$8, capability_tags=$9::jsonb, default_parameters=$10::jsonb,
        effective_from=$11, effective_to=NULL, decision_ref=$12,
        cli_config=$14::jsonb, route_tier=$15, route_tier_rank=$16, latency_class=$17,
        latency_rank=$18, reasoning_control=$19::jsonb, task_affinities=$20::jsonb, benchmark_profile=$21::jsonb
    WHERE candidate_ref=$1
"""

_INSERT_SQL = """
    INSERT INTO provider_model_candidates (
        candidate_ref, provider_ref, provider_name, provider_slug,
        model_slug, status, priority, balance_weight,
        capability_tags, default_parameters, effective_from, effective_to,
        decision_ref, created_at, cli_config, route_tier, route_tier_rank,
        latency_class, latency_rank, reasoning_control, task_affinities, benchmark_profile
    ) VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11,NULL,$12,$13,$14::jsonb,
        $15,$16,$17,$18,$19::jsonb,$20::jsonb,$21::jsonb
    )
"""


async def _ensure_safe_cli_config(
    conn: asyncpg.Connection,
    *,
    provider_slug: str,
    model_slug: str,
    decision_ref: str,
    cli_config: dict[str, Any],
    source: str,
    source_tag: str,
    synced_at: Any,
) -> int:
    profile = _model_profile(provider_slug, model_slug)
    pp = _profile_params(profile, provider_slug=provider_slug, model_slug=model_slug,
                         cli_config=cli_config, source=source, source_tag=source_tag,
                         synced_at=synced_at, decision_ref=decision_ref)
    status = await conn.execute(_UPDATE_ACTIVE_SQL, *pp[:2], provider_slug, model_slug, *pp[2:])
    return _count_from_status(status)


async def _upsert_candidate(
    conn: asyncpg.Connection,
    *,
    provider_slug: str,
    model_slug: str,
    position: int,
    decision_ref: str,
    cli_config: dict[str, Any],
    source: str,
    source_tag: str,
    synced_at: Any,
) -> str:
    candidate_ref = f"candidate.{provider_slug}.{model_slug}"
    existing = await conn.fetchval(
        "SELECT candidate_ref FROM provider_model_candidates WHERE candidate_ref=$1", candidate_ref
    )
    profile = _model_profile(provider_slug, model_slug)
    tier = str(profile["route_tier"])
    pp = _profile_params(profile, provider_slug=provider_slug, model_slug=model_slug,
                         cli_config=cli_config, source=source, source_tag=source_tag,
                         synced_at=synced_at, decision_ref=decision_ref)
    # params: $1=candidate_ref, $2=provider_ref, $3=provider_name, $4=provider_slug,
    #         $5=model_slug, $6=status, $7=priority, $8=balance_weight,
    #         $9=capability_tags, $10=default_parameters, $11=effective_from,
    #         $12=decision_ref, $13=created_at, $14=cli_config,
    #         $15=route_tier, $16=route_tier_rank, $17=latency_class, $18=latency_rank,
    #         $19=reasoning_control, $20=task_affinities, $21=benchmark_profile
    params = (
        candidate_ref, f"provider.{provider_slug}", provider_slug, provider_slug, model_slug,
        "active", _PRIORITY_BASE.get(tier, 1000) + position, _BALANCE_WEIGHT.get(tier, 1),
        pp[2], pp[3], synced_at, decision_ref, synced_at,
        pp[0], tier, int(profile["route_tier_rank"]), str(profile["latency_class"]),
        int(profile["latency_rank"]), pp[8], pp[9], pp[10],
    )
    if existing:
        await conn.execute(_UPDATE_FULL_SQL, *params)
        return "reactivated"
    await conn.execute(_INSERT_SQL, *params)
    return "inserted"


async def sync_provider_inventory(
    conn: asyncpg.Connection,
    *,
    provider_slug: str,
    models: tuple[str, ...],
    source: str,
    source_tag: str,
    decision_ref: str,
) -> dict[str, Any]:
    row = await conn.fetchrow(
        "SELECT * FROM provider_cli_profiles WHERE provider_slug=$1 AND status='active'", provider_slug
    )
    if row is None:
        raise RuntimeError(f"missing active provider_cli_profiles row for {provider_slug}")
    cli_config = _build_cli_config(row)
    summary: dict[str, Any] = {
        "inventory_models": list(models), "inventory_count": len(models),
        "migrated_alias_rows": 0, "updated_active_rows": 0,
        "inserted_models": 0, "reactivated_models": 0, "source": source,
    }
    migrations = ACTIVE_MODEL_MIGRATIONS.get(provider_slug, {})
    if migrations:
        summary["migrated_alias_rows"] = await _migrate_active_aliases(
            conn, provider_slug=provider_slug, decision_ref=decision_ref,
            cli_config=cli_config, migrations=migrations,
        )
    synced_at = utc_now()
    for position, model_slug in enumerate(models):
        summary["updated_active_rows"] += await _ensure_safe_cli_config(
            conn, provider_slug=provider_slug, model_slug=model_slug, decision_ref=decision_ref,
            cli_config=cli_config, source=source, source_tag=source_tag, synced_at=synced_at,
        )
        if await _active_model_exists(conn, provider_slug=provider_slug, model_slug=model_slug):
            continue
        action = await _upsert_candidate(
            conn, provider_slug=provider_slug, model_slug=model_slug, position=position,
            decision_ref=decision_ref, cli_config=cli_config,
            source=source, source_tag=source_tag, synced_at=synced_at,
        )
        summary["inserted_models" if action == "inserted" else "reactivated_models"] += 1
    return summary


async def _model_counts(conn: asyncpg.Connection, *, distinct: bool) -> dict[str, int]:
    col = "COUNT(DISTINCT model_slug)" if distinct else "COUNT(*)"
    rows = await conn.fetch(
        f"SELECT provider_slug, {col} AS n FROM provider_model_candidates "
        "WHERE status='active' GROUP BY provider_slug ORDER BY provider_slug"
    )
    return {row["provider_slug"]: int(row["n"]) for row in rows}


async def run_sync(*, database_url: str, dry_run: bool) -> dict[str, Any]:
    inventories = {
        "anthropic": {"models": load_anthropic_inventory(), "source": "anthropic-docs-models-overview", "source_tag": "anthropic-docs"},
        "google":    {"models": load_google_inventory(),    "source": "google-vertex-publisher-models",  "source_tag": "google-vertex"},
        "openai":    {"models": load_openai_inventory(),    "source": "codex-models-cache",              "source_tag": "codex-cache"},
    }
    dec_ref = make_decision_ref("provider-model-sync")
    conn = await db_connect(database_url)
    try:
        before = await _model_counts(conn, distinct=True)
        if dry_run:
            return {
                "ok": True, "dry_run": True, "decision_ref": dec_ref,
                "before_distinct_active_models": before,
                "providers": {
                    p: {"inventory_count": len(c["models"]), "inventory_models": list(c["models"]), "source": c["source"]}
                    for p, c in inventories.items()
                },
            }
        async with conn.transaction():
            summaries = {
                p: await sync_provider_inventory(
                    conn, provider_slug=p, models=c["models"],
                    source=c["source"], source_tag=c["source_tag"], decision_ref=dec_ref,
                )
                for p, c in inventories.items()
            }
        return {
            "ok": True, "dry_run": False, "decision_ref": dec_ref,
            "before_distinct_active_models": before,
            "after_distinct_active_models": await _model_counts(conn, distinct=True),
            "after_active_rows": await _model_counts(conn, distinct=False),
            "providers": summaries,
        }
    finally:
        await conn.close()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync provider model inventories into provider_model_candidates.")
    add_database_url_arg(parser)
    add_dry_run_arg(parser)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return run_and_print(run_sync(database_url=require_database_url(args), dry_run=args.dry_run))


if __name__ == "__main__":
    raise SystemExit(main())
