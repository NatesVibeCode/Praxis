"""Persistent runtime context authority for workflow Docker jobs."""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from typing import Any

logger = logging.getLogger(__name__)


def persist_workflow_job_runtime_contexts(
    conn,
    *,
    run_id: str,
    workflow_id: str | None,
    execution_context_shards: Mapping[str, Mapping[str, Any]] | None,
    execution_bundles: Mapping[str, Mapping[str, Any]] | None,
) -> None:
    normalized_run_id = str(run_id or "").strip()
    if not normalized_run_id:
        return

    shard_map = dict(execution_context_shards or {})
    bundle_map = dict(execution_bundles or {})
    labels = sorted({*shard_map.keys(), *bundle_map.keys()})
    for label in labels:
        normalized_label = str(label or "").strip()
        if not normalized_label:
            continue
        execution_context_shard = dict(shard_map.get(label) or {})
        execution_bundle = dict(bundle_map.get(label) or {})
        try:
            conn.execute(
                """INSERT INTO workflow_job_runtime_context
                   (run_id, job_label, workflow_id, execution_context_shard, execution_bundle)
                   VALUES ($1, $2, $3, $4::jsonb, $5::jsonb)
                   ON CONFLICT (run_id, job_label) DO UPDATE SET
                       workflow_id = EXCLUDED.workflow_id,
                       execution_context_shard = EXCLUDED.execution_context_shard,
                       execution_bundle = EXCLUDED.execution_bundle,
                       updated_at = now()""",
                normalized_run_id,
                normalized_label,
                str(workflow_id or "").strip() or None,
                json.dumps(execution_context_shard, sort_keys=True, default=str),
                json.dumps(execution_bundle, sort_keys=True, default=str),
            )
        except Exception as exc:
            logger.warning(
                "workflow runtime context persist failed for %s/%s: %s",
                normalized_run_id,
                normalized_label,
                exc,
            )


def load_workflow_job_runtime_context(
    conn,
    *,
    run_id: str,
    job_label: str,
) -> dict[str, Any] | None:
    normalized_run_id = str(run_id or "").strip()
    normalized_job_label = str(job_label or "").strip()
    if not normalized_run_id or not normalized_job_label:
        return None
    rows = conn.execute(
        """SELECT run_id, job_label, workflow_id, execution_context_shard, execution_bundle,
                  created_at, updated_at
           FROM workflow_job_runtime_context
           WHERE run_id = $1 AND job_label = $2""",
        normalized_run_id,
        normalized_job_label,
    )
    if not rows:
        return None
    row = dict(rows[0])
    for key in ("execution_context_shard", "execution_bundle"):
        value = row.get(key)
        if isinstance(value, str):
            try:
                row[key] = json.loads(value)
            except json.JSONDecodeError:
                row[key] = {}
        elif not isinstance(value, Mapping):
            row[key] = {}
        else:
            row[key] = dict(value)
    return row

