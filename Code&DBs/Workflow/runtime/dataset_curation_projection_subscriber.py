"""Subscriber: ``CHANNEL_DATASET`` -> ``dataset_curated_*`` projections.

Authority lives in :mod:`dataset_promotions` (and friends). This subscriber
turns each promotion event into a row in the right read-model table:

* ``dataset_promotion_recorded`` with ``dataset_family='sft'``        -> ``dataset_curated_examples``
* ``dataset_promotion_recorded`` with ``dataset_family='preference'`` -> ``dataset_curated_preference_pairs``
* ``dataset_promotion_recorded`` with ``dataset_family='eval'``       -> ``dataset_curated_eval_cases``
* ``dataset_promotion_superseded``                                    -> flips ``is_active = false`` on the matching projection row

Routing-family promotions are accepted but Phase 1 has no projection table
for them; they are recorded silently.

Cursor semantics mirror :mod:`runtime.dataset_candidate_subscriber`:
one cursor per ``(subscriber_id, channel)``, advance to the last seen
event id (relevant or not) so the subscriber doesn't re-scan forever.
"""

from __future__ import annotations

import asyncio
from runtime.async_bridge import run_sync_safe
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from storage.postgres import connect_workflow_database

from .event_log import (
    CHANNEL_DATASET,
    aadvance_cursor,
    aemit,
    aget_cursor,
    aread_since,
)


_DEFAULT_SUBSCRIBER_ID = "dataset_curation_projection_refresher"
DEFAULT_SUBSCRIBER_ID = _DEFAULT_SUBSCRIBER_ID
DATASET_CURATION_PROJECTION_ID = "dataset_curated_projections"

EVENT_PROMOTION_RECORDED = "dataset_promotion_recorded"
EVENT_PROMOTION_SUPERSEDED = "dataset_promotion_superseded"
EVENT_CURATION_PROJECTION_REFRESHED = "curation_projection_refreshed"

_RELEVANT_EVENTS = frozenset({EVENT_PROMOTION_RECORDED, EVENT_PROMOTION_SUPERSEDED})


class _Connection(Protocol):
    async def execute(self, query: str, *args: object) -> str: ...

    async def fetch(self, query: str, *args: object) -> list[Any]: ...

    async def fetchrow(self, query: str, *args: object) -> Any: ...

    def transaction(self) -> object: ...

    async def close(self) -> None: ...


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _decode_jsonb(value: Any) -> Any:
    if value is None or isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return value
    return value


# --------------------------------------------------------------------------
# Per-family materializers. Each takes the loaded promotion row and writes
# one upsert into its projection table.
# --------------------------------------------------------------------------


async def _materialize_sft(conn: _Connection, promotion: Mapping[str, Any]) -> None:
    payload = _decode_jsonb(promotion["payload"]) or {}
    prompt = payload.get("prompt")
    target_output = payload.get("target_output") or payload.get("completion")
    if not isinstance(prompt, dict) or not isinstance(target_output, dict):
        raise ValueError(
            f"sft promotion {promotion['promotion_id']} payload must include "
            f"object 'prompt' and 'target_output' (or 'completion')"
        )
    candidate_ids = list(promotion["candidate_ids"] or ())
    if not candidate_ids:
        raise ValueError(f"promotion {promotion['promotion_id']} has no candidate_ids")
    await conn.execute(
        """INSERT INTO dataset_curated_examples (
                promotion_id, specialist_target, split_tag,
                prompt, target_output, candidate_id, is_active, refreshed_at
            ) VALUES ($1, $2, $3, $4::jsonb, $5::jsonb, $6, true, now())
            ON CONFLICT (promotion_id) DO UPDATE SET
                specialist_target = EXCLUDED.specialist_target,
                split_tag = EXCLUDED.split_tag,
                prompt = EXCLUDED.prompt,
                target_output = EXCLUDED.target_output,
                candidate_id = EXCLUDED.candidate_id,
                is_active = true,
                refreshed_at = now()""",
        promotion["promotion_id"],
        promotion["specialist_target"],
        promotion.get("split_tag"),
        json.dumps(prompt),
        json.dumps(target_output),
        candidate_ids[0],
    )


async def _materialize_preference(
    conn: _Connection, promotion: Mapping[str, Any]
) -> None:
    payload = _decode_jsonb(promotion["payload"]) or {}
    prompt = payload.get("prompt")
    chosen = payload.get("chosen_output") or payload.get("chosen")
    rejected = payload.get("rejected_output") or payload.get("rejected")
    pair_evidence = payload.get("pair_evidence") or {}
    if not isinstance(prompt, dict) or not isinstance(chosen, dict) or not isinstance(rejected, dict):
        raise ValueError(
            f"preference promotion {promotion['promotion_id']} payload must include "
            f"object 'prompt', 'chosen_output', 'rejected_output'"
        )
    if not isinstance(pair_evidence, dict):
        raise ValueError(
            f"preference promotion {promotion['promotion_id']} 'pair_evidence' must be an object"
        )
    candidate_ids = list(promotion["candidate_ids"] or ())
    if len(candidate_ids) != 2:
        raise ValueError(
            f"preference promotion {promotion['promotion_id']} requires exactly 2 candidate_ids"
        )
    chosen_id, rejected_id = candidate_ids[0], candidate_ids[1]
    await conn.execute(
        """INSERT INTO dataset_curated_preference_pairs (
                promotion_id, specialist_target, split_tag, prompt,
                chosen_output, rejected_output,
                chosen_candidate_id, rejected_candidate_id,
                pair_evidence, is_active, refreshed_at
            ) VALUES (
                $1, $2, $3, $4::jsonb, $5::jsonb, $6::jsonb,
                $7, $8, $9::jsonb, true, now()
            )
            ON CONFLICT (promotion_id) DO UPDATE SET
                specialist_target = EXCLUDED.specialist_target,
                split_tag = EXCLUDED.split_tag,
                prompt = EXCLUDED.prompt,
                chosen_output = EXCLUDED.chosen_output,
                rejected_output = EXCLUDED.rejected_output,
                chosen_candidate_id = EXCLUDED.chosen_candidate_id,
                rejected_candidate_id = EXCLUDED.rejected_candidate_id,
                pair_evidence = EXCLUDED.pair_evidence,
                is_active = true,
                refreshed_at = now()""",
        promotion["promotion_id"],
        promotion["specialist_target"],
        promotion.get("split_tag"),
        json.dumps(prompt),
        json.dumps(chosen),
        json.dumps(rejected),
        chosen_id,
        rejected_id,
        json.dumps(pair_evidence),
    )


async def _materialize_eval(conn: _Connection, promotion: Mapping[str, Any]) -> None:
    payload = _decode_jsonb(promotion["payload"]) or {}
    case_input = payload.get("case_input") or payload.get("input")
    if not isinstance(case_input, dict):
        raise ValueError(
            f"eval promotion {promotion['promotion_id']} payload must include object 'case_input'"
        )
    revision_scope = payload.get("revision_scope") or {}
    if not isinstance(revision_scope, dict):
        raise ValueError(
            f"eval promotion {promotion['promotion_id']} 'revision_scope' must be an object"
        )
    expected_output = payload.get("expected_output")
    rubric = payload.get("rubric")
    difficulty_tags = list(payload.get("difficulty_tags") or ())
    domain_tags = list(payload.get("domain_tags") or ())
    excluded_from_training = bool(payload.get("excluded_from_training", True))
    await conn.execute(
        """INSERT INTO dataset_curated_eval_cases (
                promotion_id, specialist_target, case_input,
                expected_output, rubric, difficulty_tags, domain_tags,
                revision_scope, excluded_from_training, is_active, refreshed_at
            ) VALUES (
                $1, $2, $3::jsonb, $4::jsonb, $5::jsonb,
                $6::text[], $7::text[], $8::jsonb, $9, true, now()
            )
            ON CONFLICT (promotion_id) DO UPDATE SET
                specialist_target = EXCLUDED.specialist_target,
                case_input = EXCLUDED.case_input,
                expected_output = EXCLUDED.expected_output,
                rubric = EXCLUDED.rubric,
                difficulty_tags = EXCLUDED.difficulty_tags,
                domain_tags = EXCLUDED.domain_tags,
                revision_scope = EXCLUDED.revision_scope,
                excluded_from_training = EXCLUDED.excluded_from_training,
                is_active = true,
                refreshed_at = now()""",
        promotion["promotion_id"],
        promotion["specialist_target"],
        json.dumps(case_input),
        json.dumps(expected_output) if expected_output is not None else None,
        json.dumps(rubric) if rubric is not None else None,
        difficulty_tags,
        domain_tags,
        json.dumps(revision_scope),
        excluded_from_training,
    )


_FAMILY_DISPATCH: Mapping[str, Callable[[_Connection, Mapping[str, Any]], Awaitable[None]]] = {
    "sft": _materialize_sft,
    "preference": _materialize_preference,
    "eval": _materialize_eval,
}


async def _load_promotion(
    conn: _Connection, *, promotion_id: str
) -> Mapping[str, Any] | None:
    row = await conn.fetchrow(
        """SELECT promotion_id, candidate_ids, dataset_family, specialist_target,
                  policy_id, payload, split_tag, promoted_by, promotion_kind,
                  rationale, decision_ref, superseded_by, superseded_reason
             FROM dataset_promotions
            WHERE promotion_id = $1""",
        promotion_id,
    )
    if row is None:
        return None
    data = dict(row)
    data["payload"] = _decode_jsonb(data.get("payload"))
    return data


async def _deactivate_projection_rows(
    conn: _Connection, *, promotion_id: str
) -> None:
    """Flip is_active=false in whichever projection table holds the row."""

    for table in (
        "dataset_curated_examples",
        "dataset_curated_preference_pairs",
        "dataset_curated_eval_cases",
    ):
        await conn.execute(
            f"UPDATE {table} SET is_active = false, refreshed_at = now() "
            f"WHERE promotion_id = $1",
            promotion_id,
        )


# --------------------------------------------------------------------------
# Subscriber.
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CurationProjectionResult:
    subscriber_id: str
    starting_cursor: int
    ending_cursor: int
    scanned_count: int
    promotions_materialized: int
    promotions_superseded: int
    promotions_skipped: int
    refresh_event_id: int | None

    def to_json(self) -> dict[str, Any]:
        return {
            "subscriber_id": self.subscriber_id,
            "starting_cursor": self.starting_cursor,
            "ending_cursor": self.ending_cursor,
            "scanned_count": self.scanned_count,
            "promotions_materialized": self.promotions_materialized,
            "promotions_superseded": self.promotions_superseded,
            "promotions_skipped": self.promotions_skipped,
            "refresh_event_id": self.refresh_event_id,
        }


@dataclass(slots=True)
class DatasetCurationProjectionSubscriber:
    """Durable subscriber: dataset events -> dataset_curated_* projections."""

    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    )

    async def consume_available_async(
        self,
        *,
        limit: int = 100,
        subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        normalized_limit = max(1, int(limit or 100))
        conn = await self.connect_database(env)
        materialized = 0
        superseded = 0
        skipped = 0
        refresh_event_id: int | None = None
        try:
            async with conn.transaction():
                starting_cursor = await aget_cursor(
                    conn, subscriber_id=subscriber_id, channel=CHANNEL_DATASET
                )
                events = await aread_since(
                    conn,
                    channel=CHANNEL_DATASET,
                    cursor=starting_cursor,
                    limit=normalized_limit,
                )
                for event in events:
                    if event.event_type not in _RELEVANT_EVENTS:
                        continue
                    payload = event.payload or {}
                    promotion_id = payload.get("promotion_id") or event.entity_id
                    if not promotion_id:
                        skipped += 1
                        continue
                    if event.event_type == EVENT_PROMOTION_SUPERSEDED:
                        await _deactivate_projection_rows(conn, promotion_id=promotion_id)
                        superseded += 1
                        continue
                    promotion = await _load_promotion(conn, promotion_id=promotion_id)
                    if promotion is None:
                        skipped += 1
                        continue
                    family = str(promotion.get("dataset_family") or "")
                    materializer = _FAMILY_DISPATCH.get(family)
                    if materializer is None:
                        # routing-family or unknown: not projected in Phase 1.
                        skipped += 1
                        continue
                    await materializer(conn, promotion)
                    materialized += 1
                ending_cursor = starting_cursor
                if events:
                    ending_cursor = events[-1].id
                    await aadvance_cursor(
                        conn,
                        subscriber_id=subscriber_id,
                        channel=CHANNEL_DATASET,
                        event_id=ending_cursor,
                    )
                if materialized or superseded:
                    refresh_event_id = await aemit(
                        conn,
                        channel=CHANNEL_DATASET,
                        event_type=EVENT_CURATION_PROJECTION_REFRESHED,
                        entity_id=DATASET_CURATION_PROJECTION_ID,
                        entity_kind="dataset_curation_projection",
                        payload={
                            "subscriber_id": subscriber_id,
                            "scanned_count": len(events),
                            "promotions_materialized": materialized,
                            "promotions_superseded": superseded,
                            "promotions_skipped": skipped,
                            "refreshed_at": _now().isoformat(),
                        },
                        emitted_by="dataset_curation_projection_subscriber.consume",
                    )
        finally:
            await conn.close()
        return CurationProjectionResult(
            subscriber_id=subscriber_id,
            starting_cursor=starting_cursor,
            ending_cursor=ending_cursor,
            scanned_count=len(events),
            promotions_materialized=materialized,
            promotions_superseded=superseded,
            promotions_skipped=skipped,
            refresh_event_id=refresh_event_id,
        ).to_json()

    def consume_available(
        self,
        *,
        limit: int = 100,
        subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return run_sync_safe(
            self.consume_available_async(
                limit=limit, subscriber_id=subscriber_id, env=env
            )
        )


async def aconsume_dataset_curation_events(
    *,
    limit: int = 100,
    subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return await DatasetCurationProjectionSubscriber().consume_available_async(
        limit=limit, subscriber_id=subscriber_id, env=env
    )


__all__ = [
    "DATASET_CURATION_PROJECTION_ID",
    "DEFAULT_SUBSCRIBER_ID",
    "EVENT_CURATION_PROJECTION_REFRESHED",
    "EVENT_PROMOTION_RECORDED",
    "EVENT_PROMOTION_SUPERSEDED",
    "CurationProjectionResult",
    "DatasetCurationProjectionSubscriber",
    "aconsume_dataset_curation_events",
]
