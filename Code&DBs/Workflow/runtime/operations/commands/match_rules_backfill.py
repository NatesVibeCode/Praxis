"""CQRS handler: backfill provider_model_market_match_rules from existing candidates.

Per-record decision flow:

  1. Plan via the existing slug-based planner (`_plan_benchmark_rules`).
  2. High-confidence match (exact / normalized / dated_release) → write the rule
     and bind the market row in its own per-record transaction.
  3. Low-confidence (`family_proxy` or `source_unavailable`) → run a
     similarity-matching pass (token-set Jaccard) over the creator's market
     rows. If we find a strong suggestion, write it as `family_proxy` with
     the score in selection_metadata. Otherwise file a `praxis_bugs` row
     capturing the candidate + suggestion + reasoning, and continue.
  4. Anything that throws → record as `failed` for this record but never
     abort the batch.

The external benchmark API is fetched **once per backfill** (NOT per
provider) — free APIs are not for hammering. Each per-record write goes
through its own transaction so a single bad row never rolls back work
done for other candidates.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
import os
from typing import Any

import asyncpg
from pydantic import BaseModel, ConfigDict, Field

from runtime._workflow_database import resolve_runtime_database_url


class MatchRulesBackfillCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_slug: str = Field(
        default="artificial_analysis",
        description=(
            "market_benchmark_source_registry.source_slug to backfill against."
        ),
    )
    provider_slugs: tuple[str, ...] = Field(
        default=(),
        description=(
            "Optional restriction to specific provider_slugs. Empty tuple = "
            "all providers with active candidates missing enabled rules."
        ),
    )
    similarity_threshold: float = Field(
        default=0.75,
        ge=0.0,
        le=1.0,
        description=(
            "Minimum Jaccard token-set score for the similarity pass to "
            "accept a suggested match. Below this threshold we file a bug "
            "instead of writing a rule."
        ),
    )
    dry_run: bool = Field(
        default=False,
        description=(
            "When true, plans rules and reports the would-be writes without "
            "touching `provider_model_market_match_rules`, "
            "`provider_model_market_bindings`, `benchmark_profile`, or filing bugs."
        ),
    )


def _resolved_database_url(subsystems: Any) -> str:
    env = getattr(subsystems, "_postgres_env", None)
    if callable(env):
        source = env()
        resolved = resolve_runtime_database_url(env=source, required=False)
        if resolved:
            return resolved
    raise RuntimeError("WORKFLOW_DATABASE_URL is required for match-rules backfill")


def _token_set(slug: str) -> set[str]:
    from registry.provider_onboarding._spec import _normalized_slug

    return {tok for tok in _normalized_slug(slug).split("-") if tok}


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a or not b:
        return 0.0
    return len(a & b) / len(a | b)


def _similarity_suggest(
    *,
    target_slug: str,
    creator_rows: list[dict[str, Any]],
    threshold: float,
) -> tuple[dict[str, Any], float] | None:
    """Token-set Jaccard suggestion over a creator's market rows."""
    target_tokens = _token_set(target_slug)
    if not target_tokens or not creator_rows:
        return None
    scored = [
        (row, _jaccard(target_tokens, _token_set(str(row["source_model_slug"]))))
        for row in creator_rows
    ]
    scored.sort(key=lambda pair: -pair[1])
    best_row, best_score = scored[0]
    if best_score >= threshold:
        return best_row, best_score
    return None


async def _missing_candidates(
    conn: asyncpg.Connection,
    *,
    source_slug: str,
    provider_filter: tuple[str, ...],
) -> list[dict[str, Any]]:
    """Return missing candidates with their actual candidate_ref from DB.

    Reading the canonical candidate_ref (instead of reconstructing it from
    `_candidate_ref(provider_slug, model_slug)`) avoids FK violations
    against legacy candidate rows whose refs were normalized differently
    at onboarding time (BUG observed 2026-04-27: openrouter+anthropic/...
    had inconsistent slash→dash vs slash→dot normalizations across rows).
    """
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (c.provider_slug, c.model_slug)
               c.candidate_ref, c.provider_slug, c.model_slug, c.benchmark_profile
          FROM provider_model_candidates c
         WHERE c.status = 'active'
           AND ($2::text[] IS NULL OR c.provider_slug = ANY($2))
           AND NOT EXISTS (
                SELECT 1
                  FROM provider_model_market_match_rules r
                 WHERE r.source_slug = $1
                   AND r.provider_slug = c.provider_slug
                   AND r.candidate_model_slug = c.model_slug
                   AND r.enabled = true
           )
         ORDER BY c.provider_slug, c.model_slug, c.priority ASC, c.created_at DESC
        """,
        source_slug,
        list(provider_filter) if provider_filter else None,
    )
    import json

    out: list[dict[str, Any]] = []
    for row in rows:
        bp = row["benchmark_profile"]
        if isinstance(bp, str):
            bp = json.loads(bp)
        out.append(
            {
                "candidate_ref": str(row["candidate_ref"]),
                "provider_slug": str(row["provider_slug"]),
                "model_slug": str(row["model_slug"]),
                "benchmark_profile": dict(bp) if isinstance(bp, dict) else {},
            }
        )
    return out


def _rule_id(source_slug: str, provider_slug: str, model_slug: str) -> str:
    return (
        f"provider_model_market_match_rule.{source_slug}.{provider_slug}.{model_slug}"
    )


async def _write_match_rule(
    conn: asyncpg.Connection,
    *,
    source_slug: str,
    provider_slug: str,
    model_slug: str,
    plan_row: dict[str, Any],
    decision_ref: str,
) -> None:
    from registry.provider_onboarding_repository import _UPSERT_MATCH_RULE_SQL
    from scripts.sync_framework import jsonb

    target_slug = plan_row["target_source_model_slug"]
    await conn.execute(
        _UPSERT_MATCH_RULE_SQL,
        _rule_id(source_slug, provider_slug, model_slug),
        source_slug,
        provider_slug,
        model_slug,
        str(plan_row["target_creator_slug"]),
        str(target_slug) if isinstance(target_slug, str) else None,
        str(plan_row["match_kind"]),
        float(plan_row["binding_confidence"]),
        jsonb(dict(plan_row.get("selection_metadata") or {})),
        decision_ref,
    )


async def _write_binding_and_profile(
    conn: asyncpg.Connection,
    *,
    candidate_ref: str,
    existing_profile: dict[str, Any],
    source_slug: str,
    source_config: dict[str, Any],
    market_row: dict[str, Any],
    plan_row: dict[str, Any],
    decision_ref: str,
) -> None:
    """Write `provider_model_market_bindings` row + `benchmark_profile.market_benchmark`.

    Reuses the sync script's helpers so the on-disk shape matches what
    the runtime reader (`runtime/routing_scorer.py:343 candidate_common_metrics`)
    expects. Caller is responsible for the surrounding transaction.
    """
    from scripts.sync_market_model_registry import (
        _benchmark_payload,
        _clear_bindings,
        _upsert_binding,
        _upsert_market_row,
        _write_benchmark_profile,
        utc_now,
    )

    rule_for_payload = {
        "match_kind": str(plan_row["match_kind"]),
        "binding_confidence": float(plan_row["binding_confidence"]),
        "provider_model_market_match_rule_id": _rule_id(
            source_slug, str(plan_row["provider_slug"]), str(plan_row["model_slug"])
        ),
        "decision_ref": decision_ref,
        "selection_metadata": dict(plan_row.get("selection_metadata") or {}),
    }
    if "synced_at" not in market_row:
        market_row = {**market_row, "synced_at": utc_now()}
    await _upsert_market_row(conn, market_row)
    await _clear_bindings(conn, candidate_ref=candidate_ref, source_slug=source_slug)
    await _upsert_binding(
        conn,
        candidate_ref=candidate_ref,
        market_model_ref=str(market_row["market_model_ref"]),
        binding_kind=str(plan_row["match_kind"]),
        binding_confidence=float(plan_row["binding_confidence"]),
        dec_ref=decision_ref,
        bound_at=market_row["synced_at"],
    )
    await _write_benchmark_profile(
        conn,
        candidate_ref=candidate_ref,
        existing_profile=existing_profile,
        market_benchmark=_benchmark_payload(
            market_row=market_row,
            source_config=source_config,
            rule=rule_for_payload,
        ),
    )


def _file_uncertain_bug(
    *,
    provider_slug: str,
    model_slug: str,
    plan_row: dict[str, Any],
    suggestion: tuple[dict[str, Any], float] | None,
    source_slug: str,
) -> tuple[str | None, str | None]:
    """File a bug for an uncertain candidate via the canonical bug tracker.

    Returns (bug_id, error). Uses `_bug_surface_contract.file_bug_payload`
    — the same call path the `praxis_bugs` MCP tool uses — so dedupe
    (vector similarity floor BUG-9475EEB0) and provenance defaults stay
    consistent with manually-filed bugs.
    """
    from runtime import bug_tracker as bt_mod
    from surfaces.api.handlers import _bug_surface_contract as _bug_contract
    from surfaces.mcp.helpers import _bug_to_dict
    from surfaces.mcp.subsystems import _subs

    suggestion_block = ""
    if suggestion is not None:
        s_row, s_score = suggestion
        suggestion_block = (
            f"\n\nClosest similarity suggestion (Jaccard token-set): "
            f"{s_row['creator_slug']}/{s_row['source_model_slug']} "
            f"(score {s_score:.3f}, below operator threshold)."
        )
    desc = (
        f"match_rules.backfill could not confidently bind "
        f"{provider_slug}/{model_slug} to a {source_slug} benchmark row.\n\n"
        f"Planner verdict: {plan_row['match_kind']} "
        f"(confidence {plan_row['binding_confidence']:.2f})."
        f"{suggestion_block}\n\n"
        f"Resolve by either: (a) operator-confirmed manual rule, "
        f"(b) a creator_slug_aliases extension on "
        f"market_benchmark_source_registry, or (c) marking as legitimately "
        f"source_unavailable when the benchmark source publishes nothing "
        f"comparable."
    )
    body = {
        "title": (
            f"match_rules.backfill: uncertain {provider_slug}/{model_slug} "
            f"vs {source_slug}"
        ),
        "severity": "P3",
        "category": "WIRING",
        "description": desc,
        "filed_by": "match_rules.backfill",
        "source_kind": "operation_command",
    }
    try:
        bt = _subs.get_bug_tracker()
        result = _bug_contract.file_bug_payload(
            bt=bt,
            bt_mod=bt_mod,
            body=body,
            serialize_bug=_bug_to_dict,
            filed_by_default="match_rules.backfill",
            source_kind_default="operation_command",
            include_similar_bugs=False,
        )
        bug_id = (
            result.get("bug_id")
            or (result.get("bug") or {}).get("bug_id")
            or (result.get("filed") or {}).get("bug_id")
        )
        return (str(bug_id) if bug_id else None, None)
    except Exception as exc:  # noqa: BLE001
        return (None, f"{type(exc).__name__}: {exc}")


async def _backfill_one_record(
    conn: asyncpg.Connection,
    *,
    candidate: dict[str, Any],
    source_slug: str,
    source_config: dict[str, Any],
    market_rows: tuple[dict[str, Any], ...],
    market_by_creator: dict[str, list[dict[str, Any]]],
    similarity_threshold: float,
    decision_ref: str,
    dry_run: bool,
) -> dict[str, Any]:
    from registry.provider_onboarding._benchmark import _plan_benchmark_rules
    from registry.provider_onboarding._spec import ProviderOnboardingModelSpec

    provider_slug = candidate["provider_slug"]
    model_slug = candidate["model_slug"]
    candidate_ref = candidate["candidate_ref"]

    plan = _plan_benchmark_rules(
        provider_slug=provider_slug,
        models=[ProviderOnboardingModelSpec(model_slug=model_slug)],
        source_config=source_config,
        market_rows=market_rows,
    )
    plan_row = dict(plan[0])
    plan_row["provider_slug"] = provider_slug
    match_kind = str(plan_row["match_kind"])
    high_confidence_kinds = {
        "exact_source_slug",
        "normalized_slug_alias",
        "dated_release_alias",
    }

    record: dict[str, Any] = {
        "provider_slug": provider_slug,
        "model_slug": model_slug,
        "candidate_ref": candidate_ref,
        "planner_match_kind": match_kind,
        "planner_confidence": float(plan_row["binding_confidence"]),
    }

    if match_kind in high_confidence_kinds:
        if dry_run:
            record["status"] = "would_apply_high_confidence"
            return record
        try:
            async with conn.transaction():
                await _write_match_rule(
                    conn,
                    source_slug=source_slug,
                    provider_slug=provider_slug,
                    model_slug=model_slug,
                    plan_row=plan_row,
                    decision_ref=decision_ref,
                )
                if plan_row.get("market_row"):
                    await _write_binding_and_profile(
                        conn,
                        candidate_ref=candidate_ref,
                        existing_profile=candidate["benchmark_profile"],
                        source_slug=source_slug,
                        source_config=source_config,
                        market_row=dict(plan_row["market_row"]),
                        plan_row=plan_row,
                        decision_ref=decision_ref,
                    )
            record["status"] = "applied"
        except Exception as exc:  # noqa: BLE001 — per-record never aborts the batch
            record["status"] = "failed"
            record["error"] = str(exc)
        return record

    creator_slug = str(plan_row["target_creator_slug"])
    suggestion = _similarity_suggest(
        target_slug=model_slug,
        creator_rows=market_by_creator.get(creator_slug, []),
        threshold=similarity_threshold,
    )
    if suggestion is not None:
        s_row, s_score = suggestion
        promoted = {
            **plan_row,
            "match_kind": "family_proxy",
            "binding_confidence": round(s_score, 3),
            "target_source_model_slug": str(s_row["source_model_slug"]),
            "target_creator_slug": str(s_row["creator_slug"]),
            "selection_metadata": {
                "reason": (
                    "Slug-based planner returned no high-confidence match; "
                    "token-set Jaccard similarity over the creator's market "
                    "rows surfaced this candidate."
                ),
                "coverage_scope": "text_benchmark",
                "match_strategy": "token_set_jaccard",
                "similarity_score": s_score,
            },
            "market_row": dict(s_row),
        }
        if dry_run:
            record["status"] = "would_apply_via_similarity"
            record["similarity_score"] = s_score
            record["similarity_target"] = f"{s_row['creator_slug']}/{s_row['source_model_slug']}"
            return record
        try:
            async with conn.transaction():
                await _write_match_rule(
                    conn,
                    source_slug=source_slug,
                    provider_slug=provider_slug,
                    model_slug=model_slug,
                    plan_row=promoted,
                    decision_ref=decision_ref,
                )
                await _write_binding_and_profile(
                    conn,
                    candidate_ref=candidate_ref,
                    existing_profile=candidate["benchmark_profile"],
                    source_slug=source_slug,
                    source_config=source_config,
                    market_row=dict(s_row),
                    plan_row=promoted,
                    decision_ref=decision_ref,
                )
            record["status"] = "applied_via_similarity"
            record["similarity_score"] = s_score
            record["similarity_target"] = f"{s_row['creator_slug']}/{s_row['source_model_slug']}"
        except Exception as exc:  # noqa: BLE001
            record["status"] = "failed"
            record["error"] = str(exc)
        return record

    if dry_run:
        record["status"] = "would_file_bug"
        return record

    bug_id, bug_err = _file_uncertain_bug(
        provider_slug=provider_slug,
        model_slug=model_slug,
        plan_row=plan_row,
        suggestion=suggestion,
        source_slug=source_slug,
    )
    if bug_id:
        record["status"] = "bug_filed"
        record["bug_id"] = bug_id
    else:
        record["status"] = "bug_file_failed"
        if bug_err:
            record["bug_file_error"] = bug_err
    return record


async def _run_backfill(
    *,
    database_url: str,
    source_slug: str,
    provider_filter: tuple[str, ...],
    similarity_threshold: float,
    dry_run: bool,
) -> dict[str, Any]:
    from scripts.sync_framework import decision_ref as make_decision_ref
    from scripts.sync_market_model_registry import (
        _load_source_config,
        _resolve_api_key,
        load_market_models,
    )

    conn = await asyncpg.connect(database_url)
    try:
        candidates = await _missing_candidates(
            conn,
            source_slug=source_slug,
            provider_filter=provider_filter,
        )
        if not candidates:
            return {
                "ok": True,
                "source_slug": source_slug,
                "candidates_examined": 0,
                "applied": 0,
                "applied_via_similarity": 0,
                "bugs_filed": 0,
                "failed": 0,
                "dry_run": dry_run,
                "message": (
                    f"no active candidates missing enabled match rules for "
                    f"source_slug={source_slug}"
                ),
                "records": [],
            }

        # ONE fetch of the external source per backfill — reused across all records.
        source_config = await _load_source_config(conn, source_slug)
        api_key = os.environ.get("ARTIFICIAL_ANALYSIS_API_KEY") or None
        market_rows = load_market_models(
            source_config, api_key=_resolve_api_key(source_config, api_key)
        )
        market_by_creator: defaultdict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in market_rows:
            market_by_creator[str(row["creator_slug"])].append(dict(row))

        decision_ref = make_decision_ref("market-match-rules-backfill")

        records: list[dict[str, Any]] = []
        for candidate in candidates:
            records.append(
                await _backfill_one_record(
                    conn,
                    candidate=candidate,
                    source_slug=source_slug,
                    source_config=source_config,
                    market_rows=market_rows,
                    market_by_creator=market_by_creator,
                    similarity_threshold=similarity_threshold,
                    decision_ref=decision_ref,
                    dry_run=dry_run,
                )
            )

        counts: defaultdict[str, int] = defaultdict(int)
        for record in records:
            counts[record["status"]] += 1
        return {
            "ok": True,
            "source_slug": source_slug,
            "decision_ref": decision_ref,
            "candidates_examined": len(records),
            "applied": counts.get("applied", 0),
            "applied_via_similarity": counts.get("applied_via_similarity", 0),
            "bugs_filed": counts.get("bug_filed", 0),
            "bug_file_failed": counts.get("bug_file_failed", 0),
            "would_apply_high_confidence": counts.get("would_apply_high_confidence", 0),
            "would_apply_via_similarity": counts.get("would_apply_via_similarity", 0),
            "would_file_bug": counts.get("would_file_bug", 0),
            "failed": counts.get("failed", 0),
            "dry_run": dry_run,
            "records": records,
        }
    finally:
        await conn.close()


def handle_match_rules_backfill(
    command: MatchRulesBackfillCommand,
    subsystems: Any,
) -> dict[str, Any]:
    return asyncio.run(
        _run_backfill(
            database_url=_resolved_database_url(subsystems),
            source_slug=command.source_slug,
            provider_filter=command.provider_slugs,
            similarity_threshold=command.similarity_threshold,
            dry_run=command.dry_run,
        )
    )


__all__ = ["MatchRulesBackfillCommand", "handle_match_rules_backfill"]
