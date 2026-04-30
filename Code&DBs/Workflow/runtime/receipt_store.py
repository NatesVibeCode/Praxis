"""Canonical Postgres-backed receipt store.

All receipt reads and writes go through the ``receipts`` table. Legacy receipt
index tables are no longer part of runtime authority.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from runtime.receipt_provenance import (
    build_git_provenance,
    build_mutation_provenance,
    build_write_manifest,
    extract_write_paths,
)
from runtime.bug_evidence import EVIDENCE_ROLE_OBSERVED_IN
from runtime.failure_classifier import classify_failure
from runtime.friction_ledger import FrictionLedger, FrictionType
from runtime.bug_tracker import BugTracker, BugCategory, BugSeverity, build_failure_signature
from runtime._workflow_database import resolve_runtime_database_url
from runtime.system_events import emit_system_event
from storage.postgres.receipt_repository import PostgresReceiptRepository
from storage.postgres import ensure_postgres_available
from runtime.workflow.evidence_sequence_allocator import (
    insert_receipt_if_absent_with_deterministic_seq,
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
_AUTO_BUG_THRESHOLD = 3


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
    database_url = resolve_runtime_database_url()
    return ensure_postgres_available(env={"WORKFLOW_DATABASE_URL": database_url})


def _repository(conn=None) -> PostgresReceiptRepository:
    return PostgresReceiptRepository(conn or _conn())



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


def _normalize_tag_value(value: object) -> str:
    text = str(value or "").strip().lower()
    chars = []
    for ch in text:
        if ch.isalnum() or ch in "._:/-":
            chars.append(ch)
        else:
            chars.append("-")
    normalized = "".join(chars).strip("-")
    return normalized or "none"


def _auto_bug_source_issue_id(*, failure_code: str, node_id: str) -> str:
    """Return the stable issue authority for receipt-failure auto bugs."""

    return (
        "receipt.failure:"
        f"{_normalize_tag_value(failure_code)}:"
        f"{_normalize_tag_value(node_id or 'unknown')}"
    )


def _receipt_failure_category(payload: dict[str, Any]) -> str:
    direct = str(payload.get("failure_category") or "").strip().lower()
    if direct:
        return direct
    outputs = payload.get("outputs")
    if isinstance(outputs, dict):
        failure_classification = outputs.get("failure_classification")
        if isinstance(failure_classification, dict):
            category = str(failure_classification.get("category") or "").strip().lower()
            if category:
                return category
    failure_code = str(payload.get("failure_code") or "").strip()
    if not failure_code:
        return ""
    classification = classify_failure(failure_code, outputs=payload.get("outputs"))
    return classification.category.value


def _source_slug(payload: dict[str, Any]) -> str:
    provider = str(payload.get("provider_slug") or "").strip()
    model = str(payload.get("model_slug") or "").strip()
    if provider and model:
        return f"{provider}/{model}"
    if provider:
        return provider
    return str(payload.get("agent") or "unknown")


def _report_post_receipt_hook_failure(
    conn,
    *,
    hook: str,
    payload: dict[str, Any],
    error: Exception,
) -> None:
    failure_code = str(payload.get("failure_code") or "").strip()
    receipt_id = str(payload.get("receipt_id") or "").strip()
    run_id = str(payload.get("run_id") or "").strip()
    _log.warning(
        "post-receipt hook %s failed for receipt_id=%s run_id=%s failure_code=%s: %s",
        hook,
        receipt_id or "unknown",
        run_id or "unknown",
        failure_code or "unknown",
        error,
        exc_info=True,
    )
    try:
        emit_system_event(
            conn,
            event_type="post_receipt_hook.failed",
            source_id=receipt_id or run_id or "unknown",
            source_type="receipt" if receipt_id else "workflow_run",
            payload={
                "hook": hook,
                "receipt_id": receipt_id or None,
                "run_id": run_id or None,
                "failure_code": failure_code or None,
                "error_type": type(error).__name__,
                "error": str(error),
            },
        )
    except Exception as emit_error:
        _log.warning(
            "could not emit post-receipt hook failure event for hook %s: %s",
            hook,
            emit_error,
            exc_info=True,
        )


def _run_post_receipt_hooks(payload: dict[str, Any], *, conn) -> None:
    status = str(payload.get("status") or "").strip().lower()
    failure_code = str(payload.get("failure_code") or "").strip()
    if status in {"succeeded", "success", "passed"} or not failure_code:
        return

    failure_category = _receipt_failure_category(payload)
    source_kind = _source_slug(payload)
    label = str(payload.get("job_label") or payload.get("label") or payload.get("node_id") or "").strip()
    node_id = str(payload.get("node_id") or label).strip()
    identity = node_id or "unknown"
    run_id = str(payload.get("run_id") or "").strip()
    receipt_id = str(payload.get("receipt_id") or "").strip()

    # 1) Project failures into friction ledger for every persisted failure receipt.
    try:
        classification = classify_failure(failure_code, outputs=payload.get("outputs"))
        friction_type = (
            FrictionType.GUARDRAIL_BOUNCE
            if classification.is_retryable
            else FrictionType.HARD_FAILURE
        )
        FrictionLedger(conn).record(
            friction_type=friction_type,
            source=source_kind,
            job_label=identity,
            message=f"{failure_category or classification.category.value}: {failure_code}",
        )
    except Exception as error:
        _report_post_receipt_hook_failure(
            conn,
            hook="friction_ledger.record",
            payload=payload,
            error=error,
        )

    # 2) Run canonical bug-threshold aggregation from receipts table.
    try:
        signature = build_failure_signature(
            failure_code=failure_code,
            job_label=identity if identity != "unknown" else None,
            node_id=node_id or None,
            failure_category=failure_category or None,
            agent=str(payload.get("agent") or "").strip() or None,
            provider_slug=str(payload.get("provider_slug") or "").strip() or None,
            model_slug=str(payload.get("model_slug") or "").strip() or None,
            source_kind="receipt_store",
        )
        tracker = BugTracker(conn)
        source_issue_id = _auto_bug_source_issue_id(
            failure_code=failure_code,
            node_id=node_id or identity,
        )
        signature_tags = (
            "auto-filed",
            f"failure_code:{_normalize_tag_value(failure_code)}",
            f"job_label:{_normalize_tag_value(identity)}",
            f"node_id:{_normalize_tag_value(node_id or identity)}",
            f"failure_category:{_normalize_tag_value(failure_category or 'unknown')}",
            f"provider:{_normalize_tag_value(str(payload.get('provider_slug') or 'unknown'))}",
            f"model:{_normalize_tag_value(str(payload.get('model_slug') or 'unknown'))}",
        )
        dedupe_tags = (
            "auto-filed",
            f"failure_code:{_normalize_tag_value(failure_code)}",
            f"job_label:{_normalize_tag_value(identity)}",
        )
        existing = tracker.list_bugs(
            open_only=True,
            source_issue_id=source_issue_id,
            limit=1,
        )
        if not existing:
            # Backward compatibility for bugs filed before source_issue_id became
            # the auto-filer authority. This prevents one more duplicate row
            # while old open bugs are still carrying only aggregation tags.
            existing = tracker.list_bugs(open_only=True, tags=dedupe_tags, limit=1)
        failure_count = int(
            conn.fetchval(
                """
                SELECT COUNT(*) AS total
                FROM receipts
                WHERE lower(status) IN ('failed', 'error')
                AND COALESCE(failure_code, '') = $1
                  AND COALESCE(node_id, '') = $2
                """,
                failure_code,
                node_id,
            )
            or 0
        )
        bug_id = existing[0].bug_id if existing else ""
        if not bug_id and failure_count >= _AUTO_BUG_THRESHOLD:
            bug, _similar = tracker.file_bug(
                title=f"Repeated receipt failure: {failure_code}",
                severity=BugSeverity.P2,
                category=BugCategory.RUNTIME,
                description=(
                    f"Failure code '{failure_code}' occurred {failure_count} times"
                    f" for node '{identity}' in persisted receipts."
                ),
                filed_by="receipt_store",
                source_kind="receipt_store",
                discovered_in_run_id=run_id or None,
                discovered_in_receipt_id=receipt_id or None,
                source_issue_id=source_issue_id,
                tags=signature_tags,
                resume_context={
                    "auto_bug_identity": {
                        "source_issue_id": source_issue_id,
                        "authority": "receipts.failure_code+node_id",
                        "aggregation_fields": ["failure_code", "node_id"],
                        "failure_count": failure_count,
                        "threshold": _AUTO_BUG_THRESHOLD,
                        "signature_fingerprint": signature.get("fingerprint"),
                    }
                },
            )
            bug_id = bug.bug_id
        if bug_id:
            if receipt_id:
                try:
                    tracker.link_evidence(
                        bug_id,
                        evidence_kind="receipt",
                        evidence_ref=receipt_id,
                        evidence_role=EVIDENCE_ROLE_OBSERVED_IN,
                        created_by="receipt_store",
                        notes=f"Observed failure {failure_code}.",
                    )
                except Exception as error:
                    _report_post_receipt_hook_failure(
                        conn,
                        hook="bug_evidence.receipt",
                        payload=payload,
                        error=error,
                    )
            if run_id:
                try:
                    tracker.link_evidence(
                        bug_id,
                        evidence_kind="run",
                        evidence_ref=run_id,
                        evidence_role=EVIDENCE_ROLE_OBSERVED_IN,
                        created_by="receipt_store",
                        notes=f"Observed failing run {run_id}.",
                    )
                except Exception as error:
                    _report_post_receipt_hook_failure(
                        conn,
                        hook="bug_evidence.run",
                        payload=payload,
                        error=error,
                    )
    except Exception as error:
        _report_post_receipt_hook_failure(
            conn,
            hook="auto_bug_threshold",
            payload=payload,
            error=error,
        )



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


def _attach_receipt_structural_proof(
    *,
    receipt_id: str,
    outputs: dict[str, Any],
) -> dict[str, Any]:
    """Attach a per-receipt verification block by running the structural-proof
    builtin inline.

    Pure-function check (no DB I/O). Preserves any pre-existing
    ``verification_status`` if a caller has already attempted real verification
    on this receipt.
    """
    try:
        from runtime.verifier_builtins import (
            RECEIPT_STRUCTURAL_PROOF_VERIFIER_REF,
            builtin_verify_receipt_structural_proof,
        )
    except Exception:
        return outputs

    receipt_view = {
        "receipt_id": receipt_id,
        "outputs": outputs,
    }
    try:
        status, builtin_outputs = builtin_verify_receipt_structural_proof(
            inputs={"receipt": receipt_view},
        )
    except Exception:
        return outputs

    verification_block = {
        "verifier_ref": RECEIPT_STRUCTURAL_PROOF_VERIFIER_REF,
        "status": status,
        "checks": builtin_outputs.get("checks", {}),
        "missing": builtin_outputs.get("missing", []),
        "kind": "structural_proof",
    }
    outputs["verification"] = verification_block
    outputs.setdefault("verification_status", status)
    return outputs


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
    workspace_snapshot_ref = str(payload.get("workspace_snapshot_ref") or "").strip()
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
                **(
                    {"workspace_snapshot_ref": workspace_snapshot_ref}
                    if workspace_snapshot_ref
                    else {}
                ),
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
    manifest_id: Optional[str] = None,
) -> list[ReceiptRecord]:
    rows = _repository().list_receipts(
        limit=max(limit, 1),
        since_hours=since_hours,
        status=status,
        agent=agent,
        manifest_id=manifest_id,
    )
    return [_row_to_record(row) for row in rows]



def load_receipt(receipt_id: int | str) -> Optional[ReceiptRecord]:
    row = _repository().load_receipt(receipt_id=str(receipt_id))
    return _row_to_record(row) if row else None



def find_receipt_by_run_id(run_id: str) -> Optional[ReceiptRecord]:
    row = _repository().load_latest_receipt_for_run(run_id=run_id)
    return _row_to_record(row) if row else None



def search_receipts(
    query: str,
    *,
    limit: int = 50,
    status: Optional[str] = None,
    agent: Optional[str] = None,
    workflow_id: Optional[str] = None,
    conn=None,
) -> list[ReceiptRecord]:
    rows = _repository(conn).search_receipts(
        query=query,
        limit=max(limit, 1),
        status=status,
        agent=agent,
        workflow_id=workflow_id,
    )
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


def load_receipt_payload(receipt_id: int | str) -> Optional[dict[str, Any]]:
    record = load_receipt(receipt_id)
    return normalize_receipt_payload(record.to_dict()) if record is not None else None


def receipt_stats(*, since_hours: int = 24, conn=None) -> dict[str, Any]:
    rows = _repository(conn).receipt_stats(since_hours=since_hours)

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
    snapshot = _repository(conn).proof_metrics_snapshot(since_hours=since_hours)
    row = snapshot.get("receipts") or {}
    memory = snapshot.get("memory_graph") or {}
    edges = snapshot.get("edges") or {}
    compile_row = snapshot.get("compile_authority") or {}
    repo_snapshot_row = snapshot.get("repo_snapshots") or {}
    verifier_healer_row = snapshot.get("recovery_authority") or {}

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
    repository = _repository(conn)
    rows = repository.list_receipts_for_provenance_backfill(run_id=run_id, limit=limit)
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
            "workspace_snapshot_ref": workspace_provenance.get("workspace_snapshot_ref"),
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
        repository.update_receipt_payloads(
            receipt_id=str(row.get("receipt_id") or ""),
            inputs=inputs,
            outputs=outputs,
        )
        updated += 1
    return {
        "run_id": run_id,
        "requested_limit": limit,
        "updated_receipts": updated,
    }


def write_receipt(receipt_dict: dict[str, Any], *, conn=None) -> dict[str, Any]:
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
    outputs.setdefault(
        "cache_read_tokens",
        int(normalized.get("cache_read_tokens") or outputs.get("cache_read_tokens") or 0),
    )
    outputs.setdefault(
        "cache_creation_tokens",
        int(
            normalized.get("cache_creation_tokens")
            or outputs.get("cache_creation_tokens")
            or 0
        ),
    )
    outputs.setdefault("cost_usd", float(normalized.get("cost_usd") or normalized.get("total_cost_usd") or outputs.get("cost_usd") or 0.0))
    outputs.setdefault(
        "duration_api_ms",
        int(normalized.get("duration_api_ms") or outputs.get("duration_api_ms") or 0),
    )
    outputs.setdefault("num_turns", int(normalized.get("num_turns") or outputs.get("num_turns") or 0))
    if normalized.get("tool_use") is not None and "tool_use" not in outputs:
        outputs["tool_use"] = normalized.get("tool_use")
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
        "workspace_snapshot_cache_hit",
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
    if "verification" not in outputs:
        outputs = _attach_receipt_structural_proof(receipt_id=receipt_id, outputs=outputs)
    artifacts = normalized.get("artifacts") if isinstance(normalized.get("artifacts"), dict) else {}
    decision_refs = normalized.get("decision_refs") if isinstance(normalized.get("decision_refs"), list) else []

    transition_seq = insert_receipt_if_absent_with_deterministic_seq(
        conn=conn,
        receipt_id=receipt_id,
        receipt_type="workflow_result",
        schema_version=1,
        workflow_id=workflow_id,
        run_id=run_id,
        request_id=request_id,
        node_id=label,
        attempt_no=attempt_no,
        started_at=started_at,
        finished_at=finished_at,
        status=str(normalized.get("status") or ""),
        inputs=inputs,
        outputs=outputs,
        artifacts=artifacts,
        failure_code=str(normalized.get("failure_code") or normalized.get("error_code") or "").strip() or None,
    )
    normalized["evidence_count"] = transition_seq
    _run_post_receipt_hooks(
        {
            **normalized,
            "receipt_id": receipt_id,
            "job_label": label,
            "label": label,
            "node_id": label,
            "agent": agent or _derive_agent(inputs, outputs, ""),
            "failure_category": _receipt_failure_category(
                {
                    **normalized,
                    "outputs": outputs,
                    "failure_code": str(normalized.get("failure_code") or ""),
                }
            ),
        },
        conn=conn,
    )
    return {
        "receipt_id": receipt_id,
        "transition_seq": transition_seq,
    }
