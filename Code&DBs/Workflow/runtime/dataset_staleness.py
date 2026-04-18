"""Definition + evidence staleness reconciler for dataset candidates.

Two passes per tick:

1. **Definition staleness.** A candidate is ``definition_stale`` when a
   newer ``workflow_definitions`` row exists for the same ``workflow_id``
   with a different ``definition_hash`` than the candidate was scored
   against (``admitted_definition_hash``). The exporter refuses to ship
   ``definition_stale`` rows.

2. **Evidence staleness.** A candidate is ``evidence_stale`` when any of
   its evidence links point at:

   - a ``semantic_assertion`` whose ``assertion_status = 'retracted'``,
     or
   - a ``bug`` whose ``status = 'wont_fix'``.

   For each candidate flipped to ``evidence_stale``, every active
   promotion referencing it gets superseded by a tombstone promotion.
   The tombstone preserves the original family/specialist/candidate
   shape (so CHECKs hold) and records the supersession reason; readers
   filter on ``superseded_by IS NULL`` and projections flip
   ``is_active = false`` via :mod:`dataset_curation_projection_subscriber`
   on the emitted event.

The reconciler is idempotent: re-running on a clean DB is a no-op.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from storage.postgres import connect_workflow_database

from .cache_invalidation import aemit_cache_invalidation
from .dataset_curation_projection_subscriber import EVENT_PROMOTION_SUPERSEDED
from .event_log import CHANNEL_DATASET, aemit


EVENT_CANDIDATE_STALENESS_CHANGED = "dataset_candidate_staleness_changed"
EVENT_STALENESS_RECONCILED = "dataset_staleness_reconciled"
DEFAULT_RECONCILER_ID = "dataset_staleness_reconciler"
SUPERSEDE_REASON_DEFINITION = "candidate_definition_stale"
SUPERSEDE_REASON_EVIDENCE = "candidate_evidence_stale"


class _Connection(Protocol):
    async def execute(self, query: str, *args: object) -> str: ...

    async def fetch(self, query: str, *args: object) -> list[Any]: ...

    async def fetchrow(self, query: str, *args: object) -> Any: ...

    def transaction(self) -> object: ...

    async def close(self) -> None: ...


def _now() -> datetime:
    return datetime.now(timezone.utc)


# --------------------------------------------------------------------------
# Pass 1: definition staleness.
# --------------------------------------------------------------------------


async def reconcile_definition_staleness(conn: _Connection) -> list[str]:
    """Flip candidates to ``definition_stale``. Returns affected candidate_ids."""

    rows = await conn.fetch(
        """WITH stale AS (
                SELECT c.candidate_id
                  FROM dataset_raw_candidates c
                  JOIN workflow_definitions cur
                    ON cur.workflow_definition_id = c.workflow_definition_id
                 WHERE c.staleness_status = 'fresh'
                   AND c.workflow_definition_id IS NOT NULL
                   AND c.admitted_definition_hash IS NOT NULL
                   AND EXISTS (
                       SELECT 1 FROM workflow_definitions newer
                        WHERE newer.workflow_id = cur.workflow_id
                          AND newer.definition_hash <> c.admitted_definition_hash
                          AND newer.created_at > cur.created_at
                   )
           )
           UPDATE dataset_raw_candidates c
              SET staleness_status = 'definition_stale'
             FROM stale
            WHERE c.candidate_id = stale.candidate_id
        RETURNING c.candidate_id"""
    )
    return [str(r["candidate_id"]) for r in rows]


# --------------------------------------------------------------------------
# Pass 2: evidence staleness.
# --------------------------------------------------------------------------


async def reconcile_evidence_staleness(conn: _Connection) -> list[str]:
    """Flip candidates to ``evidence_stale``. Returns affected candidate_ids."""

    rows = await conn.fetch(
        """WITH retracted_assertions AS (
                SELECT semantic_assertion_id::text AS evidence_ref
                  FROM semantic_assertions
                 WHERE assertion_status = 'retracted'
            ),
            wontfix_bugs AS (
                SELECT bug_id::text AS evidence_ref
                  FROM bugs
                 WHERE status = 'wont_fix'
            ),
            stale_links AS (
                SELECT DISTINCT l.candidate_id
                  FROM dataset_candidate_evidence_links l
                  JOIN dataset_raw_candidates c ON c.candidate_id = l.candidate_id
                 WHERE c.staleness_status = 'fresh'
                   AND (
                        (l.evidence_kind = 'semantic_assertion'
                         AND l.evidence_ref IN (SELECT evidence_ref FROM retracted_assertions))
                     OR (l.evidence_kind = 'bug'
                         AND l.evidence_ref IN (SELECT evidence_ref FROM wontfix_bugs))
                   )
           )
           UPDATE dataset_raw_candidates c
              SET staleness_status = 'evidence_stale'
             FROM stale_links
            WHERE c.candidate_id = stale_links.candidate_id
        RETURNING c.candidate_id"""
    )
    return [str(r["candidate_id"]) for r in rows]


# --------------------------------------------------------------------------
# Active promotion supersession.
# --------------------------------------------------------------------------


async def _active_promotions_referencing(
    conn: _Connection, *, candidate_ids: list[str]
) -> list[Mapping[str, Any]]:
    if not candidate_ids:
        return []
    rows = await conn.fetch(
        """SELECT promotion_id, candidate_ids, dataset_family, specialist_target,
                  policy_id, payload, split_tag, staleness_status_seen
             FROM (
                 SELECT p.promotion_id, p.candidate_ids, p.dataset_family,
                        p.specialist_target, p.policy_id, p.payload, p.split_tag,
                        c.staleness_status AS staleness_status_seen
                   FROM dataset_promotions p
                   JOIN LATERAL unnest(p.candidate_ids) AS cand(candidate_id) ON TRUE
                   JOIN dataset_raw_candidates c ON c.candidate_id = cand.candidate_id
                  WHERE p.superseded_by IS NULL
                    AND cand.candidate_id = ANY($1::text[])
             ) sub""",
        candidate_ids,
    )
    seen: dict[str, dict[str, Any]] = {}
    for r in rows:
        d = dict(r)
        seen.setdefault(str(d["promotion_id"]), d)
    return list(seen.values())


async def _insert_tombstone_and_supersede(
    conn: _Connection,
    *,
    original: Mapping[str, Any],
    reason: str,
    reconciled_by: str,
) -> str:
    tombstone_id = f"prom_tomb_{uuid.uuid4().hex[:16]}"
    payload = {
        "tombstone": True,
        "supersedes_promotion_id": original["promotion_id"],
        "reason": reason,
    }
    # Tombstones use auto kind so decision_ref is not required.
    await conn.execute(
        """INSERT INTO dataset_promotions (
                promotion_id, candidate_ids, dataset_family, specialist_target,
                policy_id, payload, split_tag, promoted_by, promotion_kind,
                rationale
            ) VALUES (
                $1, $2::text[], $3, $4, $5, $6::jsonb, $7, $8, 'auto', $9
            )""",
        tombstone_id,
        list(original["candidate_ids"]),
        original["dataset_family"],
        original["specialist_target"],
        original["policy_id"],
        json.dumps(payload),
        original.get("split_tag"),
        f"system:{reconciled_by}",
        f"superseded by staleness reconciler: {reason}",
    )
    await conn.execute(
        """UPDATE dataset_promotions
              SET superseded_by = $1, superseded_reason = $2
            WHERE promotion_id = $3
              AND superseded_by IS NULL""",
        tombstone_id,
        reason,
        original["promotion_id"],
    )
    return tombstone_id


async def supersede_stale_active_promotions(
    conn: _Connection,
    *,
    affected_candidate_ids: list[str],
    reconciled_by: str = DEFAULT_RECONCILER_ID,
) -> list[tuple[str, str]]:
    """Tombstone every active promotion that references a stale candidate.

    Returns a list of ``(superseded_promotion_id, tombstone_id)`` tuples.
    Emits ``dataset_promotion_superseded`` events on ``CHANNEL_DATASET``
    so the curation projection subscriber can flip ``is_active``.
    """

    promotions = await _active_promotions_referencing(
        conn, candidate_ids=affected_candidate_ids
    )
    superseded: list[tuple[str, str]] = []
    for p in promotions:
        reason = (
            SUPERSEDE_REASON_DEFINITION
            if p.get("staleness_status_seen") == "definition_stale"
            else SUPERSEDE_REASON_EVIDENCE
        )
        tombstone_id = await _insert_tombstone_and_supersede(
            conn,
            original=p,
            reason=reason,
            reconciled_by=reconciled_by,
        )
        await aemit(
            conn,
            channel=CHANNEL_DATASET,
            event_type=EVENT_PROMOTION_SUPERSEDED,
            entity_id=str(p["promotion_id"]),
            entity_kind="dataset_promotion",
            payload={
                "promotion_id": str(p["promotion_id"]),
                "tombstone_promotion_id": tombstone_id,
                "reason": reason,
                "superseded_by_subsystem": reconciled_by,
            },
            emitted_by=f"dataset_staleness.{reconciled_by}",
        )
        superseded.append((str(p["promotion_id"]), tombstone_id))
    return superseded


# --------------------------------------------------------------------------
# Top-level orchestration.
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StalenessReconcileResult:
    reconciler_id: str
    definition_stale_candidates: tuple[str, ...]
    evidence_stale_candidates: tuple[str, ...]
    superseded_promotions: tuple[tuple[str, str], ...]
    refresh_event_id: int | None

    def to_json(self) -> dict[str, Any]:
        return {
            "reconciler_id": self.reconciler_id,
            "definition_stale_candidates": list(self.definition_stale_candidates),
            "evidence_stale_candidates": list(self.evidence_stale_candidates),
            "superseded_promotions": [
                {"promotion_id": p, "tombstone_id": t}
                for (p, t) in self.superseded_promotions
            ],
            "refresh_event_id": self.refresh_event_id,
        }


async def areconcile_dataset_staleness(
    *,
    reconciled_by: str = DEFAULT_RECONCILER_ID,
    env: Mapping[str, str] | None = None,
    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    ),
) -> dict[str, Any]:
    """Run both staleness passes plus supersession in one transaction."""

    conn = await connect_database(env)
    refresh_event_id: int | None = None
    try:
        async with conn.transaction():
            definition_stale = await reconcile_definition_staleness(conn)
            evidence_stale = await reconcile_evidence_staleness(conn)
            affected = list({*definition_stale, *evidence_stale})
            superseded = await supersede_stale_active_promotions(
                conn, affected_candidate_ids=affected, reconciled_by=reconciled_by
            )
            for cid in affected:
                await aemit(
                    conn,
                    channel=CHANNEL_DATASET,
                    event_type=EVENT_CANDIDATE_STALENESS_CHANGED,
                    entity_id=cid,
                    entity_kind="dataset_raw_candidate",
                    payload={
                        "candidate_id": cid,
                        "new_status": (
                            "definition_stale" if cid in definition_stale else "evidence_stale"
                        ),
                    },
                    emitted_by=f"dataset_staleness.{reconciled_by}",
                )
            if affected or superseded:
                refresh_event_id = await aemit(
                    conn,
                    channel=CHANNEL_DATASET,
                    event_type=EVENT_STALENESS_RECONCILED,
                    entity_id="dataset_staleness",
                    entity_kind="dataset_staleness_reconciler",
                    payload={
                        "reconciler_id": reconciled_by,
                        "definition_stale_count": len(definition_stale),
                        "evidence_stale_count": len(evidence_stale),
                        "superseded_count": len(superseded),
                        "reconciled_at": _now().isoformat(),
                    },
                    emitted_by=f"dataset_staleness.{reconciled_by}",
                )
                await aemit_cache_invalidation(
                    conn,
                    cache_kind="dataset_curated_projection",
                    cache_key="all",
                    reason=f"staleness reconcile: {len(superseded)} superseded",
                    invalidated_by=f"dataset_staleness.{reconciled_by}",
                )
    finally:
        await conn.close()
    return StalenessReconcileResult(
        reconciler_id=reconciled_by,
        definition_stale_candidates=tuple(definition_stale),
        evidence_stale_candidates=tuple(evidence_stale),
        superseded_promotions=tuple(superseded),
        refresh_event_id=refresh_event_id,
    ).to_json()


def reconcile_dataset_staleness(
    *,
    reconciled_by: str = DEFAULT_RECONCILER_ID,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            areconcile_dataset_staleness(reconciled_by=reconciled_by, env=env)
        )
    raise RuntimeError("staleness reconciler requires a non-async call boundary")


__all__ = [
    "DEFAULT_RECONCILER_ID",
    "EVENT_CANDIDATE_STALENESS_CHANGED",
    "EVENT_STALENESS_RECONCILED",
    "SUPERSEDE_REASON_DEFINITION",
    "SUPERSEDE_REASON_EVIDENCE",
    "StalenessReconcileResult",
    "areconcile_dataset_staleness",
    "reconcile_dataset_staleness",
    "reconcile_definition_staleness",
    "reconcile_evidence_staleness",
    "supersede_stale_active_promotions",
]
