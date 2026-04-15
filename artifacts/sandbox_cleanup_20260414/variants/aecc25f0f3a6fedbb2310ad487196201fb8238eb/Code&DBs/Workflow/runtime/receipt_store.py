"""Canonical Postgres-backed receipt store.

All receipt reads and writes go through the ``receipts`` table. Legacy receipt
index tables are no longer part of runtime authority.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from runtime.receipt_provenance import (
    build_git_provenance,
    build_mutation_provenance,
    build_write_manifest,
    extract_write_paths,
)

_log = logging.getLogger("receipt_store")
_COMPACT_GIT_PROVENANCE_KEYS = frozenset(
    {
        "available",
        "captured_at",
        "repo_snapshot_ref",
        "repo_fingerprint",
        "git_dirty",
        "reason_code",
        "error",
    }
)
_ATTEMPTED_VERIFICATION_STATUSES = frozenset({"passed", "failed", "error"})


@dataclass(frozen=True)
class ReceiptRecord:
    """A canonical workflow receipt from Postgres."""

    id: str
    label: str
    agent: str
    status: str
    failure_code: str
    timestamp: Optional[datetime]
    raw: dict[str, Any]

    @property
    def run_id(self) -> str:
        return str(self.raw.get("run_id") or "")

    @property
    def provider_slug(self) -> str:
        return str(self.raw.get("provider_slug") or "unknown")

    @property
    def model_slug(self) -> str:
        return str(self.raw.get("model_slug") or "unknown")

    @property
    def latency_ms(self) -> int:
        return int(self.raw.get("latency_ms", 0) or 0)

    @property
    def outputs(self) -> dict[str, Any]:
        outputs = self.raw.get("outputs")
        return outputs if isinstance(outputs, dict) else {}

    @property
    def capabilities(self) -> list[str]:
        caps = self.raw.get("capabilities")
        return caps if isinstance(caps, list) else []

    @property
    def author_model(self) -> Optional[str]:
        value = self.raw.get("author_model")
        return str(value) if value else None

    def to_dict(self) -> dict[str, Any]:
        return dict(self.raw)

    def to_search_result(self) -> dict[str, Any]:
        """Return a clean, minimal dict for MCP/API search surfaces.

        Rules: no empty strings, no zero numeric fields, no duplicated fields,
        no internal blobs (inputs/outputs). Only what a caller needs to act on.
        """
        r = self.raw
        outputs = r.get("outputs") if isinstance(r.get("outputs"), dict) else {}
        artifacts = r.get("artifacts") if isinstance(r.get("artifacts"), dict) else {}

        out: dict[str, Any] = {}

        # Identity
        for key in ("receipt_id", "run_id", "workflow_id", "job_label"):
            v = str(r.get(key) or "").strip()
            if v:
                out[key] = v

        # Attempt only if retried
        attempt = int(r.get("attempt_no") or 1)
        if attempt > 1:
            out["attempt_no"] = attempt

        # Single agent field
        agent = str(r.get("agent") or "").strip()
        if agent:
            out["agent"] = agent

        # Outcome
        status = str(r.get("status") or "").strip()
        if status:
            out["status"] = status
        failure_code = str(r.get("failure_code") or "").strip()
        if failure_code:
            out["failure_code"] = failure_code

        # Timing
        latency = int(r.get("latency_ms") or 0)
        if latency > 0:
            out["latency_ms"] = latency
        finished = r.get("finished_at")
        if finished:
            out["finished_at"] = finished

        # Cost/tokens — only if non-zero
        tokens_in = int(r.get("input_tokens") or 0)
        tokens_out = int(r.get("output_tokens") or 0)
        cost = float(r.get("cost_usd") or 0.0)
        if tokens_in > 0:
            out["input_tokens"] = tokens_in
        if tokens_out > 0:
            out["output_tokens"] = tokens_out
        if cost > 0.0:
            out["cost_usd"] = round(cost, 6)

        # Verification — only if actionable (not "skipped")
        vstatus = str(outputs.get("verification_status") or "").strip()
        if vstatus and vstatus != "skipped":
            out["verification_status"] = vstatus
        verr = str(outputs.get("verification_error") or "").strip()
        if verr:
            out["verification_error"] = verr[:300]

        # Output artifact path (more useful than raw transcript preview)
        output_path = str(artifacts.get("output_path") or "").strip()
        if output_path:
            out["output_path"] = output_path

        # stdout_preview only if it's plain text, not a CLI event transcript
        preview = str(r.get("stdout_preview") or "").strip()
        if preview and "thread.started" not in preview and "item.completed" not in preview:
            out["stdout_preview"] = preview[:400]

        return out



def _conn():
    from storage.postgres import ensure_postgres_available

    return ensure_postgres_available()



def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    if value is None:
        return {}
    try:
        return dict(value)
    except Exception:
        return {}



def _json_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return []
        return list(parsed) if isinstance(parsed, list) else []
    return []



def _derive_agent(inputs: dict[str, Any], outputs: dict[str, Any], executor_type: str) -> str:
    for candidate in (
        inputs.get("agent_slug"),
        inputs.get("agent"),
        outputs.get("agent_slug"),
        outputs.get("agent"),
        outputs.get("author_model"),
        executor_type,
    ):
        text = str(candidate or "").strip()
        if text:
            return text
    return "unknown"



def normalize_receipt_payload(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(payload)

    agent_slug = (
        normalized.get("agent_slug")
        or normalized.get("agent")
        or normalized.get("provider_slug")
        or ""
    )
    provider_slug = normalized.get("provider_slug")
    model_slug = normalized.get("model_slug")
    if "/" in str(agent_slug):
        derived_provider, derived_model = str(agent_slug).split("/", 1)
        provider_slug = provider_slug or derived_provider
        model_slug = model_slug or derived_model
    elif agent_slug:
        provider_slug = provider_slug or agent_slug

    if provider_slug:
        normalized["provider_slug"] = provider_slug
    if model_slug:
        normalized["model_slug"] = model_slug
    if agent_slug and "agent_slug" not in normalized:
        normalized["agent_slug"] = agent_slug
    if agent_slug and "agent" not in normalized:
        normalized["agent"] = agent_slug

    if not normalized.get("finished_at") and normalized.get("timestamp"):
        normalized["finished_at"] = normalized["timestamp"]

    if not normalized.get("latency_ms") and normalized.get("duration_seconds") is not None:
        try:
            normalized["latency_ms"] = int(float(normalized["duration_seconds"]) * 1000)
        except (TypeError, ValueError):
            pass

    if not normalized.get("total_cost_usd") and normalized.get("cost_usd") is not None:
        normalized["total_cost_usd"] = normalized["cost_usd"]

    return normalized


def _json_merge_object(base: Any, extra: dict[str, Any]) -> dict[str, Any]:
    merged = _json_object(base)
    merged.update(extra)
    return merged


def _candidate_write_paths(payload: dict[str, Any], outputs: dict[str, Any]) -> list[str]:
    write_manifest = outputs.get("write_manifest")
    manifest_results = write_manifest.get("results") if isinstance(write_manifest, dict) else None
    manifest_paths = []
    if isinstance(manifest_results, list):
        manifest_paths = [
            str(row.get("file_path") or "").strip()
            for row in manifest_results
            if isinstance(row, dict) and str(row.get("file_path") or "").strip()
        ]
    mutation_provenance = outputs.get("mutation_provenance")
    mutation_paths = []
    if isinstance(mutation_provenance, dict):
        mutation_paths = mutation_provenance.get("write_paths") or []
    return extract_write_paths(
        payload.get("write_scope"),
        payload.get("scope_write"),
        payload.get("verified_paths"),
        payload.get("file_paths"),
        payload.get("touch_keys"),
        manifest_paths,
        mutation_paths,
    )


def _workspace_root_for_payload(payload: dict[str, Any]) -> str | None:
    for candidate in (
        payload.get("workspace_root"),
        payload.get("workdir"),
    ):
        text = str(candidate or "").strip()
        if text:
            return str(Path(text).resolve())
    return None


def _verification_status(outputs: dict[str, Any]) -> str:
    return str(outputs.get("verification_status") or "").strip().lower()


def _extract_verification_binding_paths(bindings: Any) -> list[str]:
    if not isinstance(bindings, list):
        return []
    singular_keys = ("path", "file", "target", "module")
    plural_keys = ("paths", "files", "targets", "write_scope", "file_paths", "modules")
    paths: list[str] = []
    seen: set[str] = set()
    for binding in bindings:
        if not isinstance(binding, dict):
            continue
        inputs = binding.get("inputs")
        if not isinstance(inputs, dict):
            continue
        for key in singular_keys:
            for path in extract_write_paths(inputs.get(key)):
                if path not in seen:
                    seen.add(path)
                    paths.append(path)
        for key in plural_keys:
            for path in extract_write_paths(inputs.get(key)):
                if path not in seen:
                    seen.add(path)
                    paths.append(path)
    return paths


def _derive_verified_paths(
    *,
    inputs: dict[str, Any],
    outputs: dict[str, Any],
    payload: dict[str, Any],
) -> list[str]:
    status = _verification_status(outputs)
    if status not in _ATTEMPTED_VERIFICATION_STATUSES:
        return []
    existing = extract_write_paths(outputs.get("verified_paths"))
    if existing:
        return existing
    binding_paths = _extract_verification_binding_paths(outputs.get("verification_bindings"))
    if binding_paths:
        return binding_paths
    return extract_write_paths(payload.get("verified_paths"))


def _git_provenance_needs_refresh(existing_git: Any, *, conn=None) -> bool:
    if not isinstance(existing_git, dict):
        return True
    if conn is None:
        return False
    if not bool(existing_git.get("available", True)):
        return True
    if not str(existing_git.get("repo_snapshot_ref") or "").strip():
        return True
    existing_keys = set(existing_git.keys())
    return bool(existing_keys - _COMPACT_GIT_PROVENANCE_KEYS)


def _apply_receipt_provenance(
    *,
    payload: dict[str, Any],
    inputs: dict[str, Any],
    outputs: dict[str, Any],
    conn=None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    workspace_root = _workspace_root_for_payload(payload)
    write_paths = _candidate_write_paths(payload, outputs)
    touch_entries = payload.get("touch_keys")
    workspace_ref = str(payload.get("workspace_ref") or "")
    runtime_profile_ref = str(payload.get("runtime_profile_ref") or "")
    packet_provenance = _json_object(payload.get("packet_provenance"))

    if workspace_root:
        existing_git = outputs.get("git_provenance")
        if _git_provenance_needs_refresh(existing_git, conn=conn):
            outputs["git_provenance"] = build_git_provenance(
                workspace_root=workspace_root,
                workspace_ref=workspace_ref,
                runtime_profile_ref=runtime_profile_ref,
                packet_provenance=packet_provenance,
                conn=conn,
            )
        outputs.setdefault(
            "workspace_provenance",
            {
                "workspace_root": workspace_root,
                "workspace_ref": workspace_ref,
                "runtime_profile_ref": runtime_profile_ref,
            },
        )
    if write_paths:
        outputs.setdefault(
            "write_manifest",
            build_write_manifest(
                workspace_root=workspace_root,
                write_paths=write_paths,
                source="receipt_enrichment",
            ),
        )
        outputs.setdefault(
            "mutation_provenance",
            build_mutation_provenance(
                workspace_root=workspace_root,
                write_paths=write_paths,
                touch_entries=touch_entries if isinstance(touch_entries, list) else None,
                source="receipt_enrichment",
            ),
        )
    verify_passed = payload.get("verify_passed")
    if isinstance(verify_passed, bool) and "verification_status" not in outputs:
        outputs["verification_status"] = "passed" if verify_passed else "failed"
    derived_verified_paths = _derive_verified_paths(
        inputs=inputs,
        outputs=outputs,
        payload=payload,
    )
    if derived_verified_paths:
        outputs["verified_paths"] = derived_verified_paths
    if write_paths:
        inputs.setdefault("write_scope", write_paths)
    if isinstance(touch_entries, list) and touch_entries:
        inputs.setdefault("touch_keys", touch_entries)
    if workspace_root:
        inputs.setdefault("workspace_root", workspace_root)
    if workspace_ref:
        inputs.setdefault("workspace_ref", workspace_ref)
    if runtime_profile_ref:
        inputs.setdefault("runtime_profile_ref", runtime_profile_ref)
    if packet_provenance:
        inputs.setdefault("packet_provenance", packet_provenance)
    return inputs, outputs



def _row_to_record(row: Any) -> ReceiptRecord:
    inputs = _json_object(row.get("inputs"))
    outputs = _json_object(row.get("outputs"))
    artifacts = _json_object(row.get("artifacts"))
    decision_refs = _json_list(row.get("decision_refs"))
    agent = _derive_agent(inputs, outputs, str(row.get("executor_type") or ""))

    payload: dict[str, Any] = {
        "receipt_id": str(row.get("receipt_id") or ""),
        "workflow_id": str(row.get("workflow_id") or ""),
        "run_id": str(row.get("run_id") or ""),
        "request_id": str(row.get("request_id") or ""),
        "label": str(row.get("node_id") or ""),
        "job_label": str(row.get("node_id") or ""),
        "node_id": str(row.get("node_id") or ""),
        "attempt_no": int(row.get("attempt_no") or 1),
        "started_at": row.get("started_at").isoformat() if row.get("started_at") else None,
        "finished_at": row.get("finished_at").isoformat() if row.get("finished_at") else None,
        "timestamp": row.get("finished_at").isoformat() if row.get("finished_at") else None,
        "executor_type": str(row.get("executor_type") or ""),
        "status": str(row.get("status") or ""),
        "failure_code": str(row.get("failure_code") or ""),
        "inputs": inputs,
        "outputs": outputs,
        "artifacts": artifacts,
        "decision_refs": decision_refs,
        "agent": agent,
        "agent_slug": agent,
        "author_model": outputs.get("author_model") or inputs.get("author_model") or agent,
        "latency_ms": int(outputs.get("duration_ms") or 0),
        "cost_usd": float(outputs.get("cost_usd") or 0.0),
        "input_tokens": int(outputs.get("token_input") or 0),
        "output_tokens": int(outputs.get("token_output") or 0),
        "stdout_preview": outputs.get("stdout_preview"),
    }
    if outputs.get("failure_classification") is not None:
        payload["failure_classification"] = outputs.get("failure_classification")

    payload = normalize_receipt_payload(payload)
    timestamp = row.get("finished_at") or row.get("started_at")
    return ReceiptRecord(
        id=str(row.get("receipt_id") or ""),
        label=str(row.get("node_id") or ""),
        agent=agent,
        status=str(row.get("status") or ""),
        failure_code=str(row.get("failure_code") or ""),
        timestamp=timestamp,
        raw=payload,
    )



def list_receipts(
    *,
    limit: int = 100,
    since_hours: int = 0,
    status: Optional[str] = None,
    agent: Optional[str] = None,
) -> list[ReceiptRecord]:
    clauses: list[str] = []
    params: list[Any] = []
    idx = 1

    if since_hours > 0:
        clauses.append(f"COALESCE(finished_at, started_at) >= ${idx}")
        params.append(datetime.now(timezone.utc) - timedelta(hours=since_hours))
        idx += 1

    if status:
        clauses.append(f"status = ${idx}")
        params.append(status)
        idx += 1

    if agent:
        clauses.append(
            f"COALESCE(inputs->>'agent_slug', inputs->>'agent', outputs->>'author_model', executor_type, '') = ${idx}"
        )
        params.append(agent)
        idx += 1

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)

    sql = (
        "SELECT receipt_id, workflow_id, run_id, request_id, node_id, attempt_no, started_at, finished_at, "
        "executor_type, status, inputs, outputs, artifacts, failure_code, decision_refs "
        f"FROM receipts {where} "
        f"ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST, receipt_id DESC LIMIT ${idx}"
    )

    rows = _conn().execute(sql, *params)
    return [_row_to_record(row) for row in rows]



def load_receipt(receipt_id: int | str) -> Optional[ReceiptRecord]:
    row = _conn().fetchrow(
        "SELECT receipt_id, workflow_id, run_id, request_id, node_id, attempt_no, started_at, finished_at, "
        "executor_type, status, inputs, outputs, artifacts, failure_code, decision_refs "
        "FROM receipts WHERE receipt_id = $1 LIMIT 1",
        str(receipt_id),
    )
    return _row_to_record(row) if row else None



def find_receipt_by_run_id(run_id: str) -> Optional[ReceiptRecord]:
    row = _conn().fetchrow(
        "SELECT receipt_id, workflow_id, run_id, request_id, node_id, attempt_no, started_at, finished_at, "
        "executor_type, status, inputs, outputs, artifacts, failure_code, decision_refs "
        "FROM receipts WHERE run_id = $1 ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST LIMIT 1",
        run_id,
    )
    return _row_to_record(row) if row else None



def search_receipts(
    query: str,
    *,
    limit: int = 50,
    status: Optional[str] = None,
    agent: Optional[str] = None,
    workflow_id: Optional[str] = None,
) -> list[ReceiptRecord]:
    params: list[Any] = [query]
    idx = 2
    clauses = [
        "(to_tsvector('english', COALESCE(node_id, '') || ' ' || COALESCE(status, '') || ' ' || COALESCE(failure_code, '') || ' ' || COALESCE(inputs::text, '') || ' ' || COALESCE(outputs::text, '')) @@ plainto_tsquery('english', $1) "
        "OR COALESCE(node_id, '') ILIKE '%' || $1 || '%' "
        "OR COALESCE(inputs::text, '') ILIKE '%' || $1 || '%' "
        "OR COALESCE(outputs::text, '') ILIKE '%' || $1 || '%')"
    ]
    if status:
        clauses.append(f"status = ${idx}")
        params.append(status)
        idx += 1
    if agent:
        clauses.append(
            f"COALESCE(inputs->>'agent_slug', inputs->>'agent', outputs->>'author_model', executor_type, '') = ${idx}"
        )
        params.append(agent)
        idx += 1
    if workflow_id:
        clauses.append(f"workflow_id = ${idx}")
        params.append(workflow_id)
        idx += 1
    params.append(limit)
    sql = (
        "SELECT receipt_id, workflow_id, run_id, request_id, node_id, attempt_no, started_at, finished_at, "
        "executor_type, status, inputs, outputs, artifacts, failure_code, decision_refs "
        f"FROM receipts WHERE {' AND '.join(clauses)} "
        f"ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST LIMIT ${idx}"
    )
    rows = _conn().execute(sql, *params)
    return [_row_to_record(row) for row in rows]



def list_receipt_payloads(
    *,
    limit: int = 100,
    since_hours: int = 0,
    status: Optional[str] = None,
    agent: Optional[str] = None,
) -> list[dict[str, Any]]:
    return [
        normalize_receipt_payload(record.to_dict())
        for record in list_receipts(limit=limit, since_hours=since_hours, status=status, agent=agent)
    ]


def receipt_stats(*, since_hours: int = 24) -> dict[str, Any]:
    since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
    rows = _conn().execute(
        """
        SELECT COALESCE(inputs->>'agent_slug', inputs->>'agent', outputs->>'author_model', executor_type, 'unknown') AS agent,
               COALESCE(SUM(COALESCE(NULLIF(outputs->>'token_input', '')::bigint, 0)), 0) AS total_input,
               COALESCE(SUM(COALESCE(NULLIF(outputs->>'token_output', '')::bigint, 0)), 0) AS total_output,
               COALESCE(SUM(COALESCE(NULLIF(outputs->>'cost_usd', '')::double precision, 0)), 0) AS total_cost,
               COUNT(*) AS receipt_count
          FROM receipts
         WHERE COALESCE(finished_at, started_at) >= $1
         GROUP BY 1
        """,
        since,
    )

    by_agent: dict[str, dict[str, Any]] = {}
    totals = {"input_tokens": 0, "output_tokens": 0, "cost": 0.0, "receipts": 0}
    for row in rows:
        agent = str(row.get("agent") or "unknown")
        by_agent[agent] = {
            "input_tokens": int(row.get("total_input") or 0),
            "output_tokens": int(row.get("total_output") or 0),
            "cost": round(float(row.get("total_cost") or 0.0), 4),
            "receipts": int(row.get("receipt_count") or 0),
        }
        totals["input_tokens"] += by_agent[agent]["input_tokens"]
        totals["output_tokens"] += by_agent[agent]["output_tokens"]
        totals["cost"] += by_agent[agent]["cost"]
        totals["receipts"] += by_agent[agent]["receipts"]

    totals["cost"] = round(totals["cost"], 4)
    return {"by_agent": by_agent, "totals": totals}


def proof_metrics(*, since_hours: int = 0, conn=None) -> dict[str, Any]:
    """Return proof completeness metrics from receipts and the memory graph."""

    conn = conn or _conn()
    clauses: list[str] = []
    params: list[Any] = []
    if since_hours > 0:
        clauses.append("COALESCE(finished_at, started_at) >= $1")
        params.append(datetime.now(timezone.utc) - timedelta(hours=since_hours))
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    rows = conn.execute(
        f"""
        SELECT
            COUNT(*) AS receipts_total,
            COUNT(*) FILTER (
                WHERE COALESCE(outputs->>'verification_status', '') <> ''
            ) AS receipts_with_verification_status,
            COUNT(*) FILTER (
                WHERE COALESCE(outputs->>'verification_status', '') IN ('passed', 'failed', 'error')
            ) AS receipts_with_attempted_verification,
            COUNT(*) FILTER (
                WHERE COALESCE(outputs->>'verification_status', '') = 'configured'
            ) AS receipts_with_configured_verification,
            COUNT(*) FILTER (
                WHERE COALESCE(outputs->>'verification_status', '') = 'skipped'
            ) AS receipts_with_skipped_verification,
            COUNT(*) FILTER (
                WHERE jsonb_typeof(outputs->'verification') = 'object'
                  AND outputs->'verification' <> '{{}}'::jsonb
            ) AS receipts_with_verification,
            COUNT(*) FILTER (
                WHERE jsonb_typeof(outputs->'verified_paths') = 'array'
                  AND jsonb_array_length(outputs->'verified_paths') > 0
            ) AS receipts_with_verified_paths,
            COUNT(*) FILTER (
                WHERE COALESCE(outputs->>'verification_status', '') IN ('passed', 'failed', 'error')
                  AND NOT COALESCE((
                      jsonb_typeof(outputs->'verification') = 'object'
                      AND outputs->'verification' <> '{{}}'::jsonb
                  ), FALSE)
                  AND NOT COALESCE((
                      jsonb_typeof(outputs->'verified_paths') = 'array'
                      AND jsonb_array_length(outputs->'verified_paths') > 0
                  ), FALSE)
            ) AS receipts_with_status_only_verification,
            COUNT(*) FILTER (
                WHERE COALESCE(outputs->>'verification_status', '') IN ('passed', 'failed', 'error')
                  AND jsonb_typeof(outputs->'verified_paths') = 'array'
                  AND jsonb_array_length(outputs->'verified_paths') > 0
            ) AS receipts_with_path_backed_verification,
            COUNT(*) FILTER (
                WHERE COALESCE(outputs->>'verification_status', '') IN ('passed', 'failed', 'error')
                  AND jsonb_typeof(outputs->'verification') = 'object'
                  AND outputs->'verification' <> '{{}}'::jsonb
                  AND jsonb_typeof(outputs->'verified_paths') = 'array'
                  AND jsonb_array_length(outputs->'verified_paths') > 0
            ) AS receipts_with_fully_proved_verification,
            COUNT(*) FILTER (WHERE outputs ? 'write_manifest') AS receipts_with_write_manifest,
            COUNT(*) FILTER (WHERE outputs ? 'mutation_provenance') AS receipts_with_mutation_provenance,
            COUNT(*) FILTER (WHERE outputs ? 'git_provenance') AS receipts_with_git_provenance,
            COUNT(*) FILTER (
                WHERE COALESCE(outputs->'git_provenance'->>'repo_snapshot_ref', '') <> ''
            ) AS receipts_with_repo_snapshot_ref
        FROM receipts
        {where}
        """,
        *params,
    )
    row = rows[0] if rows else {}
    memory = conn.fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE entity_type = 'code_unit') AS code_units,
            COUNT(*) FILTER (WHERE entity_type = 'table') AS tables,
            COUNT(*) FILTER (
                WHERE entity_type = 'fact' AND COALESCE(metadata->>'entity_subtype', '') = 'verification_result'
            ) AS verification_results,
            COUNT(*) FILTER (
                WHERE entity_type = 'fact' AND COALESCE(metadata->>'entity_subtype', '') = 'failure_result'
            ) AS failure_results
        FROM memory_entities
        WHERE archived = false
        """
    ) or {}
    edges = conn.fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE relation_type = 'verified_by' AND active = true) AS verified_by_edges,
            COUNT(*) FILTER (WHERE relation_type = 'recorded_in' AND active = true) AS recorded_in_edges,
            COUNT(*) FILTER (WHERE relation_type = 'produced' AND active = true) AS produced_edges,
            COUNT(*) FILTER (WHERE relation_type = 'related_to' AND active = true) AS related_edges
        FROM memory_edges
        """
    ) or {}
    compile_row = conn.fetchrow(
        """
        SELECT
            to_regclass('public.compile_artifacts') IS NOT NULL AS compile_artifacts_ready,
            to_regclass('public.capability_catalog') IS NOT NULL AS capability_catalog_ready,
            to_regclass('public.verify_refs') IS NOT NULL AS verify_refs_ready,
            to_regclass('public.verification_registry') IS NOT NULL AS verification_registry_ready,
            to_regclass('public.compile_index_snapshots') IS NOT NULL AS compile_index_snapshots_ready,
            to_regclass('public.execution_packets') IS NOT NULL AS execution_packets_ready,
            to_regclass('public.repo_snapshots') IS NOT NULL AS repo_snapshots_ready,
            to_regclass('public.verifier_registry') IS NOT NULL AS verifier_registry_ready,
            to_regclass('public.healer_registry') IS NOT NULL AS healer_registry_ready,
            to_regclass('public.verifier_healer_bindings') IS NOT NULL AS verifier_healer_bindings_ready,
            to_regclass('public.verification_runs') IS NOT NULL AS verification_runs_ready,
            to_regclass('public.healing_runs') IS NOT NULL AS healing_runs_ready
        """
    ) or {}
    repo_snapshot_row = (
        conn.fetchrow(
            """
            SELECT COUNT(*) AS repo_snapshots
            FROM repo_snapshots
            """
        )
        or {}
    ) if bool(compile_row.get("repo_snapshots_ready")) else {"repo_snapshots": 0}
    verifier_healer_row = (
        conn.fetchrow(
            """
            SELECT
                (SELECT COUNT(*) FROM verifier_registry) AS verifiers,
                (SELECT COUNT(*) FROM healer_registry) AS healers,
                (SELECT COUNT(*) FROM verifier_healer_bindings WHERE enabled = TRUE) AS verifier_healer_bindings,
                (SELECT COUNT(*) FROM verification_runs) AS verification_runs,
                (SELECT COUNT(*) FROM healing_runs) AS healing_runs
            """
        )
        or {}
    ) if all(
        bool(compile_row.get(key))
        for key in (
            "verifier_registry_ready",
            "healer_registry_ready",
            "verifier_healer_bindings_ready",
            "verification_runs_ready",
            "healing_runs_ready",
        )
    ) else {
        "verifiers": 0,
        "healers": 0,
        "verifier_healer_bindings": 0,
        "verification_runs": 0,
        "healing_runs": 0,
    }

    receipts_total = int(row.get("receipts_total") or 0)

    def _ratio(value: Any) -> float:
        if receipts_total <= 0:
            return 0.0
        return round(int(value or 0) / receipts_total, 4)

    return {
        "receipts": {
            "total": receipts_total,
            "with_verification_status": int(row.get("receipts_with_verification_status") or 0),
            "with_attempted_verification": int(row.get("receipts_with_attempted_verification") or 0),
            "with_configured_verification": int(row.get("receipts_with_configured_verification") or 0),
            "with_skipped_verification": int(row.get("receipts_with_skipped_verification") or 0),
            "with_verification": int(row.get("receipts_with_verification") or 0),
            "with_verified_paths": int(row.get("receipts_with_verified_paths") or 0),
            "with_status_only_verification": int(row.get("receipts_with_status_only_verification") or 0),
            "with_path_backed_verification": int(row.get("receipts_with_path_backed_verification") or 0),
            "with_fully_proved_verification": int(row.get("receipts_with_fully_proved_verification") or 0),
            "with_write_manifest": int(row.get("receipts_with_write_manifest") or 0),
            "with_mutation_provenance": int(row.get("receipts_with_mutation_provenance") or 0),
            "with_git_provenance": int(row.get("receipts_with_git_provenance") or 0),
            "with_repo_snapshot_ref": int(row.get("receipts_with_repo_snapshot_ref") or 0),
            "verification_status_coverage": _ratio(row.get("receipts_with_verification_status")),
            "attempted_verification_coverage": _ratio(row.get("receipts_with_attempted_verification")),
            "configured_verification_coverage": _ratio(row.get("receipts_with_configured_verification")),
            "skipped_verification_coverage": _ratio(row.get("receipts_with_skipped_verification")),
            "verification_coverage": _ratio(row.get("receipts_with_verification")),
            "status_only_verification_coverage": _ratio(
                row.get("receipts_with_status_only_verification")
            ),
            "path_backed_verification_coverage": _ratio(
                row.get("receipts_with_path_backed_verification")
            ),
            "fully_proved_verification_coverage": _ratio(
                row.get("receipts_with_fully_proved_verification")
            ),
            "write_manifest_coverage": _ratio(row.get("receipts_with_write_manifest")),
            "mutation_provenance_coverage": _ratio(row.get("receipts_with_mutation_provenance")),
            "git_provenance_coverage": _ratio(row.get("receipts_with_git_provenance")),
            "repo_snapshot_ref_coverage": _ratio(row.get("receipts_with_repo_snapshot_ref")),
        },
        "memory_graph": {
            "code_units": int(memory.get("code_units") or 0),
            "tables": int(memory.get("tables") or 0),
            "verification_results": int(memory.get("verification_results") or 0),
            "failure_results": int(memory.get("failure_results") or 0),
            "verified_by_edges": int(edges.get("verified_by_edges") or 0),
            "recorded_in_edges": int(edges.get("recorded_in_edges") or 0),
            "produced_edges": int(edges.get("produced_edges") or 0),
            "related_edges": int(edges.get("related_edges") or 0),
        },
        "compile_authority": {
            "compile_artifacts_ready": bool(compile_row.get("compile_artifacts_ready")),
            "capability_catalog_ready": bool(compile_row.get("capability_catalog_ready")),
            "verify_refs_ready": bool(compile_row.get("verify_refs_ready")),
            "verification_registry_ready": bool(compile_row.get("verification_registry_ready")),
            "compile_spine_ready": all(
                bool(compile_row.get(key))
                for key in (
                    "compile_artifacts_ready",
                    "capability_catalog_ready",
                    "verify_refs_ready",
                    "verification_registry_ready",
                )
            ),
            "compile_index_snapshots_ready": bool(
                compile_row.get("compile_index_snapshots_ready")
            ),
            "execution_packets_ready": bool(compile_row.get("execution_packets_ready")),
            "repo_snapshots_ready": bool(compile_row.get("repo_snapshots_ready")),
            "repo_snapshots": int(repo_snapshot_row.get("repo_snapshots") or 0),
        },
        "recovery_authority": {
            "verifier_registry_ready": bool(compile_row.get("verifier_registry_ready")),
            "healer_registry_ready": bool(compile_row.get("healer_registry_ready")),
            "verifier_healer_bindings_ready": bool(
                compile_row.get("verifier_healer_bindings_ready")
            ),
            "verification_runs_ready": bool(compile_row.get("verification_runs_ready")),
            "healing_runs_ready": bool(compile_row.get("healing_runs_ready")),
            "authority_ready": all(
                bool(compile_row.get(key))
                for key in (
                    "verifier_registry_ready",
                    "healer_registry_ready",
                    "verifier_healer_bindings_ready",
                    "verification_runs_ready",
                    "healing_runs_ready",
                )
            ),
            "verifiers": int(verifier_healer_row.get("verifiers") or 0),
            "healers": int(verifier_healer_row.get("healers") or 0),
            "verifier_healer_bindings": int(
                verifier_healer_row.get("verifier_healer_bindings") or 0
            ),
            "verification_runs": int(verifier_healer_row.get("verification_runs") or 0),
            "healing_runs": int(verifier_healer_row.get("healing_runs") or 0),
        },
    }


def backfill_receipt_provenance(
    *,
    run_id: str | None = None,
    limit: int | None = None,
    repo_root: str | None = None,
    conn=None,
) -> dict[str, Any]:
    """Enrich historical receipts with mutation and git provenance when derivable."""

    conn = conn or _conn()
    params: list[Any] = []
    where_clauses: list[str] = []
    if run_id:
        params.append(run_id)
        where_clauses.append(f"r.run_id = ${len(params)}")
    where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    limit_sql = ""
    if limit is not None:
        params.append(max(limit, 0))
        limit_sql = f" LIMIT ${len(params)}"
    rows = conn.execute(
        f"""
        SELECT
            r.receipt_id,
            r.inputs,
            r.outputs,
            j.touch_keys,
            wr.request_envelope
        FROM receipts AS r
        LEFT JOIN workflow_jobs AS j
            ON j.receipt_id = r.receipt_id
        LEFT JOIN workflow_runs AS wr
            ON wr.run_id = r.run_id
        {where}
        ORDER BY r.evidence_seq ASC
        {limit_sql}
        """,
        *params,
    )
    updated = 0
    for row in rows or []:
        inputs = _json_object(row.get("inputs"))
        outputs = _json_object(row.get("outputs"))
        envelope = _json_object(row.get("request_envelope"))
        spec_snapshot = _json_object(envelope.get("spec_snapshot"))
        workspace_provenance = _json_object(outputs.get("workspace_provenance"))
        existing_git = _json_object(outputs.get("git_provenance"))
        payload = {
            "workspace_root": (
                repo_root
                or inputs.get("workspace_root")
                or workspace_provenance.get("workspace_root")
                or existing_git.get("workspace_root")
                or spec_snapshot.get("workdir")
            ),
            "write_scope": inputs.get("write_scope"),
            "touch_keys": _json_list(row.get("touch_keys")),
            "workspace_ref": (
                envelope.get("workspace_ref")
                or inputs.get("workspace_ref")
                or workspace_provenance.get("workspace_ref")
                or existing_git.get("workspace_ref")
            ),
            "runtime_profile_ref": (
                envelope.get("runtime_profile_ref")
                or inputs.get("runtime_profile_ref")
                or workspace_provenance.get("runtime_profile_ref")
                or existing_git.get("runtime_profile_ref")
            ),
        }
        inputs_before = json.dumps(inputs, sort_keys=True, default=str)
        outputs_before = json.dumps(outputs, sort_keys=True, default=str)
        inputs, outputs = _apply_receipt_provenance(
            payload=payload,
            inputs=inputs,
            outputs=outputs,
            conn=conn,
        )
        if (
            json.dumps(inputs, sort_keys=True, default=str) == inputs_before
            and json.dumps(outputs, sort_keys=True, default=str) == outputs_before
        ):
            continue
        conn.execute(
            """
            UPDATE receipts
            SET inputs = $2::jsonb,
                outputs = $3::jsonb
            WHERE receipt_id = $1
            """,
            str(row.get("receipt_id") or ""),
            json.dumps(inputs, sort_keys=True, default=str),
            json.dumps(outputs, sort_keys=True, default=str),
        )
        updated += 1
    return {
        "run_id": run_id,
        "requested_limit": limit,
        "updated_receipts": updated,
    }


def write_receipt(receipt_dict: dict[str, Any], *, conn=None) -> None:
    """Persist a receipt payload to the canonical ``receipts`` table."""
    normalized = normalize_receipt_payload(dict(receipt_dict))
    conn = conn or _conn()
    now = datetime.now(timezone.utc)

    run_id = str(normalized.get("run_id") or "")
    label = str(normalized.get("label") or normalized.get("job_label") or "workflow")
    attempt_no = max(1, int(normalized.get("attempt_no") or normalized.get("attempts") or 1))
    receipt_id = str(normalized.get("receipt_id") or f"receipt:{run_id}:{label}:{attempt_no}")
    workflow_id = str(normalized.get("workflow_id") or run_id)
    request_id = str(normalized.get("request_id") or f"req_{run_id}")
    started_at_raw = normalized.get("started_at") or normalized.get("timestamp")
    finished_at_raw = normalized.get("finished_at") or normalized.get("timestamp")
    started_at = datetime.fromisoformat(started_at_raw) if isinstance(started_at_raw, str) and started_at_raw else started_at_raw
    finished_at = datetime.fromisoformat(finished_at_raw) if isinstance(finished_at_raw, str) and finished_at_raw else finished_at_raw
    if started_at is None:
        started_at = now
    if finished_at is None:
        finished_at = now

    agent = str(normalized.get("agent_slug") or normalized.get("agent") or normalized.get("author_model") or "")
    outputs = dict(normalized.get("outputs") or {})
    outputs.setdefault("status", normalized.get("status", ""))
    outputs.setdefault("error_code", normalized.get("failure_code") or normalized.get("error_code") or "")
    outputs.setdefault("duration_ms", int(normalized.get("latency_ms") or outputs.get("duration_ms") or 0))
    outputs.setdefault("token_input", int(normalized.get("input_tokens") or outputs.get("token_input") or 0))
    outputs.setdefault("token_output", int(normalized.get("output_tokens") or outputs.get("token_output") or 0))
    outputs.setdefault("cost_usd", float(normalized.get("cost_usd") or normalized.get("total_cost_usd") or outputs.get("cost_usd") or 0.0))
    if normalized.get("stdout_preview") is not None and "stdout_preview" not in outputs:
        outputs["stdout_preview"] = normalized.get("stdout_preview")
    if normalized.get("failure_classification") is not None and "failure_classification" not in outputs:
        outputs["failure_classification"] = normalized.get("failure_classification")
    if normalized.get("author_model") is not None and "author_model" not in outputs:
        outputs["author_model"] = normalized.get("author_model")
    for key in (
        "verification",
        "verification_bindings",
        "verification_status",
        "verified_paths",
        "write_manifest",
        "mutation_provenance",
        "git_provenance",
        "workspace_provenance",
    ):
        if normalized.get(key) is not None and key not in outputs:
            outputs[key] = normalized.get(key)

    inputs = {
        "job_label": label,
        "agent_slug": agent,
        "provider_slug": normalized.get("provider_slug"),
        "model_slug": normalized.get("model_slug"),
        "capabilities": normalized.get("capabilities") or [],
        "author_model": normalized.get("author_model"),
        "verify_refs": normalized.get("verify_refs"),
    }
    for key in (
        "write_scope",
        "touch_keys",
        "workspace_ref",
        "runtime_profile_ref",
        "packet_provenance",
        "workspace_root",
    ):
        if normalized.get(key) is not None:
            inputs[key] = normalized.get(key)
    inputs, outputs = _apply_receipt_provenance(
        payload=normalized,
        inputs=inputs,
        outputs=outputs,
        conn=conn,
    )
    artifacts = normalized.get("artifacts") if isinstance(normalized.get("artifacts"), dict) else {}
    decision_refs = normalized.get("decision_refs") if isinstance(normalized.get("decision_refs"), list) else []

    conn.execute(
        """
        INSERT INTO receipts (
            receipt_id, receipt_type, schema_version,
            workflow_id, run_id, request_id,
            causation_id, node_id, attempt_no, supersedes_receipt_id,
            started_at, finished_at, evidence_seq,
            executor_type, status, inputs, outputs, artifacts,
            failure_code, decision_refs
        ) VALUES (
            $1, $2, $3,
            $4, $5, $6,
            NULL, $7, $8, NULL,
            $9, $10, $11,
            $12, $13, $14::jsonb, $15::jsonb, $16::jsonb,
            $17, $18::jsonb
        )
        ON CONFLICT (receipt_id) DO UPDATE SET
            workflow_id = EXCLUDED.workflow_id,
            run_id = EXCLUDED.run_id,
            request_id = EXCLUDED.request_id,
            node_id = EXCLUDED.node_id,
            attempt_no = EXCLUDED.attempt_no,
            started_at = EXCLUDED.started_at,
            finished_at = EXCLUDED.finished_at,
            evidence_seq = EXCLUDED.evidence_seq,
            executor_type = EXCLUDED.executor_type,
            status = EXCLUDED.status,
            inputs = EXCLUDED.inputs,
            outputs = EXCLUDED.outputs,
            artifacts = EXCLUDED.artifacts,
            failure_code = EXCLUDED.failure_code,
            decision_refs = EXCLUDED.decision_refs
        """,
        receipt_id,
        "workflow_result",
        1,
        workflow_id,
        run_id,
        request_id,
        label,
        attempt_no,
        started_at,
        finished_at,
        int(normalized.get("evidence_count") or attempt_no),
        str(normalized.get("adapter_type") or normalized.get("executor_type") or "workflow"),
        str(normalized.get("status") or ""),
        json.dumps(inputs, sort_keys=True, default=str),
        json.dumps(outputs, sort_keys=True, default=str),
        json.dumps(artifacts, sort_keys=True, default=str),
        str(normalized.get("failure_code") or normalized.get("error_code") or ""),
        json.dumps(decision_refs, sort_keys=True, default=str),
    )
