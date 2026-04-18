"""Read API for the dataset refinery.

Pure read paths — no writes, no event emission. The MCP tool and CLI both
go through here so there is one canonical place to filter and serialize
candidate / score / promotion / lineage rows.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from storage.postgres import connect_workflow_database


def _decode_jsonb(value: Any) -> Any:
    if value is None or isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


def _row(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    data = dict(row)
    for k, v in list(data.items()):
        if isinstance(v, str) and v and v[0] in "{[":
            data[k] = _decode_jsonb(v)
    return data


@dataclass(frozen=True, slots=True)
class CandidateFilter:
    candidate_kind: str | None = None
    route_slug: str | None = None
    eligibility: str | None = None
    policy_id: str | None = None
    redaction_status: str | None = None
    staleness_status: str | None = None
    limit: int = 50
    offset: int = 0


async def alist_candidates(
    *,
    filters: CandidateFilter,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Filtered, paginated list of raw candidates with their best score."""

    where: list[str] = []
    args: list[Any] = []
    if filters.candidate_kind:
        args.append(filters.candidate_kind)
        where.append(f"c.candidate_kind = ${len(args)}")
    if filters.route_slug:
        args.append(filters.route_slug)
        where.append(f"c.route_slug = ${len(args)}")
    if filters.redaction_status:
        args.append(filters.redaction_status)
        where.append(f"c.redaction_status = ${len(args)}")
    if filters.staleness_status:
        args.append(filters.staleness_status)
        where.append(f"c.staleness_status = ${len(args)}")
    score_join = ""
    if filters.eligibility or filters.policy_id:
        score_join = "LEFT JOIN dataset_candidate_scores s ON s.candidate_id = c.candidate_id"
        if filters.policy_id:
            args.append(filters.policy_id)
            where.append(f"s.policy_id = ${len(args)}")
        if filters.eligibility:
            args.append(filters.eligibility)
            where.append(f"s.eligibility = ${len(args)}")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""

    args.append(max(1, min(int(filters.limit or 50), 500)))
    args.append(max(0, int(filters.offset or 0)))
    limit_idx = len(args) - 1
    offset_idx = len(args)

    sql = f"""
        SELECT c.candidate_id, c.candidate_kind, c.source_receipt_id,
               c.source_run_id, c.route_slug, c.redaction_status,
               c.staleness_status, c.dedupe_signature, c.ingested_at,
               c.workflow_definition_id, c.admitted_definition_hash,
               c.linked_bug_ids,
               c.verifier_summary, c.review_summary, c.operator_decision_summary
          FROM dataset_raw_candidates c
          {score_join}
          {where_sql}
         ORDER BY c.ingested_at DESC
         LIMIT ${limit_idx} OFFSET ${offset_idx}
    """
    conn = await connect_workflow_database(env)
    try:
        rows = await conn.fetch(sql, *args)
    finally:
        await conn.close()
    return {
        "candidates": [_row(r) for r in rows],
        "limit": filters.limit,
        "offset": filters.offset,
    }


async def ainspect_candidate(
    *, candidate_id: str, env: Mapping[str, str] | None = None
) -> dict[str, Any]:
    """One-call lineage view: candidate + scores + evidence links + active promotions."""

    conn = await connect_workflow_database(env)
    try:
        candidate = _row(
            await conn.fetchrow(
                """SELECT * FROM dataset_raw_candidates WHERE candidate_id = $1""",
                candidate_id,
            )
        )
        if not candidate:
            return {"candidate": None}
        scores = [
            _row(r)
            for r in await conn.fetch(
                """SELECT s.candidate_id, s.policy_id, s.eligibility, s.confidence,
                          s.factors, s.rationale, s.scored_at,
                          s.scored_against_definition_hash,
                          p.policy_slug, p.specialist_target, p.auto_promote
                     FROM dataset_candidate_scores s
                     JOIN dataset_scoring_policies p ON p.policy_id = s.policy_id
                    WHERE s.candidate_id = $1
                    ORDER BY s.scored_at DESC""",
                candidate_id,
            )
        ]
        score_history = [
            _row(r)
            for r in await conn.fetch(
                """SELECT h.history_id, h.candidate_id, h.policy_id,
                          h.eligibility, h.confidence,
                          h.previous_eligibility, h.previous_confidence,
                          h.change_reason, h.rationale, h.scored_at,
                          h.scored_against_definition_hash, h.recorded_at,
                          p.policy_slug, p.specialist_target
                     FROM dataset_candidate_score_history h
                     JOIN dataset_scoring_policies p ON p.policy_id = h.policy_id
                    WHERE h.candidate_id = $1
                    ORDER BY h.recorded_at DESC""",
                candidate_id,
            )
        ]
        links = [
            _row(r)
            for r in await conn.fetch(
                """SELECT candidate_id, evidence_kind, evidence_ref, evidence_role, recorded_at
                     FROM dataset_candidate_evidence_links
                    WHERE candidate_id = $1
                    ORDER BY recorded_at""",
                candidate_id,
            )
        ]
        promotions = [
            _row(r)
            for r in await conn.fetch(
                """SELECT promotion_id, dataset_family, specialist_target, policy_id,
                          split_tag, promoted_by, promotion_kind, rationale,
                          decision_ref, superseded_by, superseded_reason, promoted_at
                     FROM dataset_promotions
                    WHERE $1 = ANY(candidate_ids)
                    ORDER BY promoted_at DESC""",
                candidate_id,
            )
        ]
    finally:
        await conn.close()
    return {
        "candidate": candidate,
        "scores": scores,
        "score_history": score_history,
        "evidence_links": links,
        "promotions": promotions,
    }


async def alist_policies(
    *,
    specialist_target: str | None = None,
    active_only: bool = True,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    where: list[str] = []
    args: list[Any] = []
    if specialist_target:
        args.append(specialist_target)
        where.append(f"specialist_target = ${len(args)}")
    if active_only:
        where.append("superseded_by IS NULL")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    sql = f"""
        SELECT policy_id, policy_slug, specialist_target, rubric, auto_promote,
               decided_by, rationale, created_at, superseded_by
          FROM dataset_scoring_policies
          {where_sql}
         ORDER BY created_at DESC
    """
    conn = await connect_workflow_database(env)
    try:
        rows = await conn.fetch(sql, *args)
    finally:
        await conn.close()
    return {"policies": [_row(r) for r in rows]}


async def ainspect_policy(
    *, policy_id_or_slug: str, env: Mapping[str, str] | None = None
) -> dict[str, Any]:
    conn = await connect_workflow_database(env)
    try:
        row = await conn.fetchrow(
            """SELECT * FROM dataset_scoring_policies
                WHERE policy_id = $1 OR policy_slug = $1""",
            policy_id_or_slug,
        )
    finally:
        await conn.close()
    return {"policy": _row(row) if row else None}


async def alist_promotions(
    *,
    specialist_target: str | None = None,
    dataset_family: str | None = None,
    split_tag: str | None = None,
    active_only: bool = True,
    limit: int = 50,
    offset: int = 0,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    where: list[str] = []
    args: list[Any] = []
    if specialist_target:
        args.append(specialist_target)
        where.append(f"specialist_target = ${len(args)}")
    if dataset_family:
        args.append(dataset_family)
        where.append(f"dataset_family = ${len(args)}")
    if split_tag:
        args.append(split_tag)
        where.append(f"split_tag = ${len(args)}")
    if active_only:
        where.append("superseded_by IS NULL")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    args.append(max(1, min(int(limit or 50), 500)))
    args.append(max(0, int(offset or 0)))
    sql = f"""
        SELECT promotion_id, candidate_ids, dataset_family, specialist_target,
               policy_id, split_tag, promoted_by, promotion_kind, rationale,
               decision_ref, superseded_by, superseded_reason, promoted_at
          FROM dataset_promotions
          {where_sql}
         ORDER BY promoted_at DESC
         LIMIT ${len(args) - 1} OFFSET ${len(args)}
    """
    conn = await connect_workflow_database(env)
    try:
        rows = await conn.fetch(sql, *args)
    finally:
        await conn.close()
    return {
        "promotions": [_row(r) for r in rows],
        "limit": limit,
        "offset": offset,
    }


async def afetch_lineage(
    *,
    promotion_id: str | None = None,
    candidate_id: str | None = None,
    specialist_target: str | None = None,
    limit: int = 200,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Read from ``dataset_lineage_v`` for an audit-friendly flattened view."""

    where: list[str] = []
    args: list[Any] = []
    if promotion_id:
        args.append(promotion_id)
        where.append(f"promotion_id = ${len(args)}")
    if candidate_id:
        args.append(candidate_id)
        where.append(f"candidate_id = ${len(args)}")
    if specialist_target:
        args.append(specialist_target)
        where.append(f"specialist_target = ${len(args)}")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    args.append(max(1, min(int(limit or 200), 1000)))
    sql = f"""
        SELECT promotion_id, dataset_family, specialist_target, split_tag,
               promotion_kind, policy_id, decision_ref, superseded_by, promoted_at,
               candidate_id, candidate_kind, source_receipt_id, source_run_id,
               workflow_definition_id, admitted_definition_hash,
               staleness_status, redaction_status,
               evidence_kind, evidence_ref, evidence_role
          FROM dataset_lineage_v
          {where_sql}
         ORDER BY promoted_at DESC
         LIMIT ${len(args)}
    """
    conn = await connect_workflow_database(env)
    try:
        rows = await conn.fetch(sql, *args)
    finally:
        await conn.close()
    return {"lineage": [_row(r) for r in rows]}


async def alist_export_manifests(
    *,
    specialist_target: str | None = None,
    dataset_family: str | None = None,
    limit: int = 50,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    where: list[str] = []
    args: list[Any] = []
    if specialist_target:
        args.append(specialist_target)
        where.append(f"specialist_target = ${len(args)}")
    if dataset_family:
        args.append(dataset_family)
        where.append(f"dataset_family = ${len(args)}")
    where_sql = ("WHERE " + " AND ".join(where)) if where else ""
    args.append(max(1, min(int(limit or 50), 500)))
    sql = f"""
        SELECT manifest_id, dataset_family, specialist_target, split_tag,
               promotion_ids, output_path, output_sha256, row_count,
               exported_by, exported_at
          FROM dataset_export_manifests
          {where_sql}
         ORDER BY exported_at DESC
         LIMIT ${len(args)}
    """
    conn = await connect_workflow_database(env)
    try:
        rows = await conn.fetch(sql, *args)
    finally:
        await conn.close()
    return {"manifests": [_row(r) for r in rows]}


async def alist_manual_review_inbox(
    *,
    candidate_kind: str | None = None,
    specialist_target: str | None = None,
    limit: int = 25,
    offset: int = 0,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Active-learning inbox: candidates whose best score tier is ``manual_review``.

    These are the rows the operator should look at next — the scorer
    thinks they are borderline (some signal but not enough to auto).
    Ordered by score confidence DESC then recency DESC so the strongest
    near-misses surface first.
    """

    conn = await connect_workflow_database(env)
    params: list[Any] = []
    where = [
        "s.eligibility = 'manual_review'",
        "c.staleness_status = 'fresh'",
        "c.redaction_status <> 'sensitive_blocked'",
        "NOT EXISTS (SELECT 1 FROM dataset_promotions p "
        " WHERE c.candidate_id = ANY(p.candidate_ids) AND p.superseded_by IS NULL)",
    ]
    if candidate_kind:
        params.append(candidate_kind)
        where.append(f"c.candidate_kind = ${len(params)}")
    if specialist_target:
        params.append(specialist_target)
        where.append(f"c.route_slug = ${len(params)}")
    params.append(int(max(1, min(200, limit))))
    params.append(int(max(0, offset)))
    sql = f"""
        SELECT c.candidate_id, c.candidate_kind, c.route_slug, c.task_type,
               c.source_receipt_id, c.source_run_id, c.ingested_at,
               c.redaction_status, c.staleness_status,
               s.policy_id, s.eligibility, s.confidence, s.rationale, s.scored_at
          FROM dataset_raw_candidates c
          JOIN dataset_candidate_scores s ON s.candidate_id = c.candidate_id
         WHERE {' AND '.join(where)}
         ORDER BY s.confidence DESC, c.ingested_at DESC
         LIMIT ${len(params) - 1} OFFSET ${len(params)}
    """
    try:
        rows = await conn.fetch(sql, *params)
    finally:
        await conn.close()
    items = [_row(r) for r in rows]
    return {"items": items, "count": len(items), "limit": limit, "offset": offset}


async def asuggest_preference_pairs(
    *,
    candidate_kind: str | None = None,
    specialist_target: str | None = None,
    limit: int = 20,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Scan for candidate pairs that disagree on observable signal.

    A pair is eligible when two fresh, redaction-clean candidates share
    candidate_kind + specialist_target + task_type and differ on
    verifier_summary.status OR review_summary.status. The subscriber
    already stamped those summaries, so this is a pure-SQL filter.

    Returns ``{pairs: [...]}``. Each pair names chosen/rejected
    candidates and the divergence signal. The operator or a later
    preference-specific policy still decides whether to promote.
    """

    conn = await connect_workflow_database(env)
    params: list[Any] = [int(max(1, min(200, limit)))]
    where = ["c1.staleness_status = 'fresh'", "c2.staleness_status = 'fresh'",
             "c1.redaction_status = 'clean'", "c2.redaction_status = 'clean'",
             "c1.candidate_kind = c2.candidate_kind",
             "coalesce(c1.route_slug,'') = coalesce(c2.route_slug,'')",
             "coalesce(c1.task_type,'') = coalesce(c2.task_type,'')",
             "c1.candidate_id < c2.candidate_id"]
    if candidate_kind:
        params.append(candidate_kind)
        where.append(f"c1.candidate_kind = ${len(params)}")
    if specialist_target:
        params.append(specialist_target)
        where.append(f"c1.route_slug = ${len(params)}")
    sql = f"""
        WITH divergent AS (
            SELECT
                c1.candidate_id AS chosen_id,
                c2.candidate_id AS rejected_id,
                c1.candidate_kind,
                c1.route_slug,
                c1.task_type,
                (c1.verifier_summary->>'status') AS chosen_verifier_status,
                (c2.verifier_summary->>'status') AS rejected_verifier_status,
                (c1.review_summary->>'status') AS chosen_review_status,
                (c2.review_summary->>'status') AS rejected_review_status
            FROM dataset_raw_candidates c1
            JOIN dataset_raw_candidates c2 ON ({' AND '.join(where)})
            WHERE (
                  (c1.verifier_summary->>'status') = 'passed'
              AND (c2.verifier_summary->>'status') IN ('failed','error')
            ) OR (
                  (c1.review_summary->>'status') = 'active'
              AND (c2.review_summary->>'status') = 'retracted'
            )
        )
        SELECT * FROM divergent LIMIT $1
    """
    try:
        rows = await conn.fetch(sql, *params)
    finally:
        await conn.close()
    pairs = []
    for r in rows:
        d = _row(r)
        signals: list[str] = []
        if d.get("chosen_verifier_status") and d.get("rejected_verifier_status"):
            signals.append(
                f"verifier: {d['chosen_verifier_status']} vs {d['rejected_verifier_status']}"
            )
        if d.get("chosen_review_status") and d.get("rejected_review_status"):
            signals.append(
                f"review: {d['chosen_review_status']} vs {d['rejected_review_status']}"
            )
        pairs.append(
            {
                "chosen_candidate_id": d["chosen_id"],
                "rejected_candidate_id": d["rejected_id"],
                "candidate_kind": d["candidate_kind"],
                "route_slug": d["route_slug"],
                "task_type": d["task_type"],
                "divergence_signals": signals,
            }
        )
    return {"pairs": pairs, "count": len(pairs)}


async def asummarize_refinery(
    *, env: Mapping[str, str] | None = None
) -> dict[str, Any]:
    """One-call counts for an at-a-glance health view."""

    conn = await connect_workflow_database(env)
    try:
        row = await conn.fetchrow(
            """SELECT
                  (SELECT count(*) FROM dataset_raw_candidates) AS candidates_total,
                  (SELECT count(*) FROM dataset_raw_candidates
                    WHERE staleness_status = 'fresh') AS candidates_fresh,
                  (SELECT count(*) FROM dataset_raw_candidates
                    WHERE redaction_status = 'sensitive_blocked') AS candidates_blocked,
                  (SELECT count(*) FROM dataset_scoring_policies
                    WHERE superseded_by IS NULL) AS policies_active,
                  (SELECT count(*) FROM dataset_promotions
                    WHERE superseded_by IS NULL) AS promotions_active,
                  (SELECT count(*) FROM dataset_curated_examples
                    WHERE is_active) AS curated_sft_active,
                  (SELECT count(*) FROM dataset_curated_preference_pairs
                    WHERE is_active) AS curated_preference_active,
                  (SELECT count(*) FROM dataset_curated_eval_cases
                    WHERE is_active) AS curated_eval_active,
                  (SELECT count(*) FROM dataset_export_manifests) AS exports_total
            """
        )
    finally:
        await conn.close()
    return _row(row)


__all__ = [
    "CandidateFilter",
    "afetch_lineage",
    "ainspect_candidate",
    "ainspect_policy",
    "alist_candidates",
    "alist_export_manifests",
    "alist_manual_review_inbox",
    "alist_policies",
    "asuggest_preference_pairs",
    "alist_promotions",
    "asummarize_refinery",
]
