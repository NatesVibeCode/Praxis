"""JSONL exporter for the dataset refinery.

Reads from the curated projections and writes a deterministic JSONL file
plus a ``dataset_export_manifests`` row that pins the file's content hash,
row count, and the exact set of promotion ids that went into it.

JSONL row shapes:

* ``sft``        — ``{"prompt": ..., "completion": ..., "meta": {...}}``
* ``preference`` — ``{"prompt": ..., "chosen": ..., "rejected": ..., "meta": {...}}``
* ``eval``       — ``{"input": ..., "expected": ..., "rubric": ..., "tags": [...], "revision_scope": {...}, "meta": {...}}``

Hard rules enforced here:

* Only ``is_active = TRUE`` projection rows are exported.
* Promotions whose underlying candidates are ``definition_stale`` or
  ``evidence_stale`` are excluded — except eval cases, which carry their
  own ``revision_scope`` and are intentionally pinned.
* ``train`` and ``eval`` exports for the same specialist must not share
  any ``dedupe_signature``; the exporter raises ``DatasetExportError``
  before writing if they do.
* ``eval``-family promotions can only be exported with ``split_tag='eval'``
  (or ``holdout``).
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import uuid
from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from contracts.dataset import DATASET_FAMILIES, SPLIT_TAGS
from storage.postgres import connect_workflow_database

from .cache_invalidation import aemit_cache_invalidation
from .event_log import CHANNEL_DATASET, aemit


EVENT_DATASET_EXPORTED = "dataset_exported"


class DatasetExportError(RuntimeError):
    """Raised when an export request would violate refinery invariants."""


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
# Per-family row loaders. Each yields canonical export dicts in
# promotion_id order so the resulting file is deterministic.
# --------------------------------------------------------------------------


async def _load_sft_rows(
    conn: _Connection, *, specialist_target: str, split_tag: str
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """SELECT e.promotion_id, e.prompt, e.target_output, e.candidate_id,
                  c.dedupe_signature, c.staleness_status, c.redaction_status,
                  c.admitted_definition_hash, c.workflow_definition_id,
                  c.source_receipt_id, c.route_slug, p.policy_id
             FROM dataset_curated_examples e
             JOIN dataset_raw_candidates c ON c.candidate_id = e.candidate_id
             JOIN dataset_promotions p ON p.promotion_id = e.promotion_id
            WHERE e.specialist_target = $1
              AND e.split_tag = $2
              AND e.is_active = true
              AND p.superseded_by IS NULL
              AND c.staleness_status = 'fresh'
              AND c.redaction_status = 'clean'
            ORDER BY e.promotion_id""",
        specialist_target,
        split_tag,
    )
    return [
        {
            "prompt": _decode_jsonb(r["prompt"]),
            "completion": _decode_jsonb(r["target_output"]),
            "meta": {
                "promotion_id": r["promotion_id"],
                "candidate_id": r["candidate_id"],
                "policy_id": r["policy_id"],
                "specialist": specialist_target,
                "split": split_tag,
                "source_receipt_id": r["source_receipt_id"],
                "route_slug": r["route_slug"],
                "workflow_definition_id": r["workflow_definition_id"],
                "admitted_definition_hash": r["admitted_definition_hash"],
                "dedupe_signature": r["dedupe_signature"],
            },
        }
        for r in rows
    ]


async def _load_preference_rows(
    conn: _Connection, *, specialist_target: str, split_tag: str
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """SELECT pp.promotion_id, pp.prompt, pp.chosen_output, pp.rejected_output,
                  pp.chosen_candidate_id, pp.rejected_candidate_id, pp.pair_evidence,
                  cc.dedupe_signature AS chosen_dedupe,
                  cc.staleness_status AS chosen_stale,
                  cc.redaction_status AS chosen_redaction,
                  cr.dedupe_signature AS rejected_dedupe,
                  cr.staleness_status AS rejected_stale,
                  cr.redaction_status AS rejected_redaction,
                  p.policy_id
             FROM dataset_curated_preference_pairs pp
             JOIN dataset_raw_candidates cc ON cc.candidate_id = pp.chosen_candidate_id
             JOIN dataset_raw_candidates cr ON cr.candidate_id = pp.rejected_candidate_id
             JOIN dataset_promotions p ON p.promotion_id = pp.promotion_id
            WHERE pp.specialist_target = $1
              AND pp.split_tag = $2
              AND pp.is_active = true
              AND p.superseded_by IS NULL
              AND cc.staleness_status = 'fresh'
              AND cr.staleness_status = 'fresh'
              AND cc.redaction_status = 'clean'
              AND cr.redaction_status = 'clean'
            ORDER BY pp.promotion_id""",
        specialist_target,
        split_tag,
    )
    return [
        {
            "prompt": _decode_jsonb(r["prompt"]),
            "chosen": _decode_jsonb(r["chosen_output"]),
            "rejected": _decode_jsonb(r["rejected_output"]),
            "meta": {
                "promotion_id": r["promotion_id"],
                "chosen_candidate_id": r["chosen_candidate_id"],
                "rejected_candidate_id": r["rejected_candidate_id"],
                "policy_id": r["policy_id"],
                "specialist": specialist_target,
                "split": split_tag,
                "pair_evidence": _decode_jsonb(r["pair_evidence"]),
                "dedupe_signatures": [r["chosen_dedupe"], r["rejected_dedupe"]],
            },
        }
        for r in rows
    ]


async def _load_eval_rows(
    conn: _Connection, *, specialist_target: str, split_tag: str
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """SELECT ec.promotion_id, ec.case_input, ec.expected_output, ec.rubric,
                  ec.difficulty_tags, ec.domain_tags, ec.revision_scope,
                  ec.excluded_from_training, p.policy_id, p.candidate_ids,
                  p.split_tag
             FROM dataset_curated_eval_cases ec
             JOIN dataset_promotions p ON p.promotion_id = ec.promotion_id
            WHERE ec.specialist_target = $1
              AND COALESCE(p.split_tag, 'eval') = $2
              AND ec.is_active = true
              AND p.superseded_by IS NULL
            ORDER BY ec.promotion_id""",
        specialist_target,
        split_tag,
    )
    return [
        {
            "input": _decode_jsonb(r["case_input"]),
            "expected": _decode_jsonb(r["expected_output"]),
            "rubric": _decode_jsonb(r["rubric"]),
            "tags": list((r.get("difficulty_tags") or [])) + list((r.get("domain_tags") or [])),
            "revision_scope": _decode_jsonb(r["revision_scope"]),
            "meta": {
                "promotion_id": r["promotion_id"],
                "policy_id": r["policy_id"],
                "candidate_ids": list(r["candidate_ids"] or ()),
                "specialist": specialist_target,
                "split": split_tag,
                "excluded_from_training": bool(r["excluded_from_training"]),
                "difficulty_tags": list(r.get("difficulty_tags") or ()),
                "domain_tags": list(r.get("domain_tags") or ()),
            },
        }
        for r in rows
    ]


_FAMILY_LOADERS: Mapping[str, Callable[..., Awaitable[list[dict[str, Any]]]]] = {
    "sft": _load_sft_rows,
    "preference": _load_preference_rows,
    "eval": _load_eval_rows,
}


# --------------------------------------------------------------------------
# Cross-split leakage check.
# --------------------------------------------------------------------------


async def _assert_no_train_eval_leakage(
    conn: _Connection,
    *,
    dataset_family: str,
    specialist_target: str,
    split_tag: str,
) -> None:
    """A dedupe_signature must not appear in both train and eval for the same specialist."""

    if split_tag not in {"train", "eval"} or dataset_family != "sft":
        return
    other = "eval" if split_tag == "train" else "train"
    row = await conn.fetchrow(
        """SELECT count(*) AS n
             FROM dataset_curated_examples a
             JOIN dataset_raw_candidates ca ON ca.candidate_id = a.candidate_id
             JOIN dataset_curated_examples b
               ON b.specialist_target = a.specialist_target
             JOIN dataset_raw_candidates cb ON cb.candidate_id = b.candidate_id
            WHERE a.specialist_target = $1
              AND a.split_tag = $2
              AND b.split_tag = $3
              AND a.is_active = true
              AND b.is_active = true
              AND ca.dedupe_signature = cb.dedupe_signature""",
        specialist_target,
        split_tag,
        other,
    )
    leaks = int(row["n"] or 0) if row else 0
    if leaks > 0:
        raise DatasetExportError(
            f"train/eval leakage: {leaks} dedupe_signature(s) appear in both "
            f"{split_tag!r} and {other!r} for specialist {specialist_target!r}"
        )


# --------------------------------------------------------------------------
# JSONL writer + manifest.
# --------------------------------------------------------------------------


def _write_jsonl(rows: Iterable[Mapping[str, Any]], output_path: str) -> tuple[int, str]:
    """Write rows as JSONL, return ``(row_count, sha256)``. Atomic via temp + rename."""

    parent = os.path.dirname(os.path.abspath(output_path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp_path = f"{output_path}.{uuid.uuid4().hex[:8]}.tmp"
    hasher = hashlib.sha256()
    row_count = 0
    with open(tmp_path, "w", encoding="utf-8") as fh:
        for row in rows:
            line = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)
            payload = line + "\n"
            fh.write(payload)
            hasher.update(payload.encode("utf-8"))
            row_count += 1
    os.replace(tmp_path, output_path)
    return row_count, "sha256:" + hasher.hexdigest()


async def _record_manifest(
    conn: _Connection,
    *,
    dataset_family: str,
    specialist_target: str,
    split_tag: str,
    promotion_ids: list[str],
    output_path: str,
    output_sha256: str,
    row_count: int,
    exported_by: str,
) -> str:
    manifest_id = f"man_{uuid.uuid4().hex[:20]}"
    await conn.execute(
        """INSERT INTO dataset_export_manifests (
                manifest_id, dataset_family, specialist_target, split_tag,
                promotion_ids, output_path, output_sha256, row_count, exported_by
            ) VALUES ($1, $2, $3, $4, $5::text[], $6, $7, $8, $9)""",
        manifest_id,
        dataset_family,
        specialist_target,
        split_tag,
        promotion_ids,
        output_path,
        output_sha256,
        row_count,
        exported_by,
    )
    return manifest_id


# --------------------------------------------------------------------------
# Public entry point.
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DatasetExportResult:
    manifest_id: str
    dataset_family: str
    specialist_target: str
    split_tag: str
    output_path: str
    output_sha256: str
    row_count: int
    promotion_ids: tuple[str, ...]
    exported_by: str
    exported_at: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "manifest_id": self.manifest_id,
            "dataset_family": self.dataset_family,
            "specialist_target": self.specialist_target,
            "split_tag": self.split_tag,
            "output_path": self.output_path,
            "output_sha256": self.output_sha256,
            "row_count": self.row_count,
            "promotion_ids": list(self.promotion_ids),
            "exported_by": self.exported_by,
            "exported_at": self.exported_at.isoformat(),
        }


async def aexport_dataset(
    *,
    dataset_family: str,
    specialist_target: str,
    split_tag: str,
    output_path: str,
    exported_by: str,
    env: Mapping[str, str] | None = None,
    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    ),
) -> dict[str, Any]:
    if dataset_family not in DATASET_FAMILIES:
        raise DatasetExportError(
            f"unknown dataset_family {dataset_family!r}; expected one of {sorted(DATASET_FAMILIES)}"
        )
    if split_tag not in SPLIT_TAGS:
        raise DatasetExportError(
            f"unknown split_tag {split_tag!r}; expected one of {sorted(SPLIT_TAGS)}"
        )
    loader = _FAMILY_LOADERS.get(dataset_family)
    if loader is None:
        raise DatasetExportError(
            f"dataset_family {dataset_family!r} has no exporter (Phase 1 covers sft/preference/eval)"
        )
    if dataset_family == "eval" and split_tag == "train":
        raise DatasetExportError("eval-family promotions cannot be exported as 'train' split")
    if not (exported_by or "").strip():
        raise DatasetExportError("exported_by must be non-blank")

    conn = await connect_database(env)
    try:
        async with conn.transaction():
            await _assert_no_train_eval_leakage(
                conn,
                dataset_family=dataset_family,
                specialist_target=specialist_target,
                split_tag=split_tag,
            )
            rows = await loader(
                conn,
                specialist_target=specialist_target,
                split_tag=split_tag,
            )
            promotion_ids = [str(r["meta"]["promotion_id"]) for r in rows]
            row_count, output_sha256 = _write_jsonl(rows, output_path)
            manifest_id = await _record_manifest(
                conn,
                dataset_family=dataset_family,
                specialist_target=specialist_target,
                split_tag=split_tag,
                promotion_ids=promotion_ids,
                output_path=output_path,
                output_sha256=output_sha256,
                row_count=row_count,
                exported_by=exported_by,
            )
            await aemit(
                conn,
                channel=CHANNEL_DATASET,
                event_type=EVENT_DATASET_EXPORTED,
                entity_id=manifest_id,
                entity_kind="dataset_export_manifest",
                payload={
                    "manifest_id": manifest_id,
                    "dataset_family": dataset_family,
                    "specialist_target": specialist_target,
                    "split_tag": split_tag,
                    "row_count": row_count,
                    "output_path": output_path,
                    "output_sha256": output_sha256,
                    "exported_by": exported_by,
                },
                emitted_by="dataset_exporter.aexport_dataset",
            )
            await aemit_cache_invalidation(
                conn,
                cache_kind="dataset_export_manifests",
                cache_key=f"{specialist_target}:{dataset_family}:{split_tag}",
                reason=f"new export {manifest_id}",
                invalidated_by="dataset_exporter",
            )
    finally:
        await conn.close()
    return DatasetExportResult(
        manifest_id=manifest_id,
        dataset_family=dataset_family,
        specialist_target=specialist_target,
        split_tag=split_tag,
        output_path=output_path,
        output_sha256=output_sha256,
        row_count=row_count,
        promotion_ids=tuple(promotion_ids),
        exported_by=exported_by,
        exported_at=_now(),
    ).to_json()


def export_dataset(
    *,
    dataset_family: str,
    specialist_target: str,
    split_tag: str,
    output_path: str,
    exported_by: str,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            aexport_dataset(
                dataset_family=dataset_family,
                specialist_target=specialist_target,
                split_tag=split_tag,
                output_path=output_path,
                exported_by=exported_by,
                env=env,
            )
        )
    raise RuntimeError("dataset exporter requires a non-async call boundary")


async def abuild_eval_set(
    *,
    specialist_target: str,
    output_path: str,
    exported_by: str,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Convenience wrapper: export the eval split for a specialist."""

    return await aexport_dataset(
        dataset_family="eval",
        specialist_target=specialist_target,
        split_tag="eval",
        output_path=output_path,
        exported_by=exported_by,
        env=env,
    )


__all__ = [
    "EVENT_DATASET_EXPORTED",
    "DatasetExportError",
    "DatasetExportResult",
    "abuild_eval_set",
    "aexport_dataset",
    "export_dataset",
]
