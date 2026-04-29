"""Subscriber: ``CHANNEL_RECEIPT`` -> ``dataset_raw_candidates``.

For each receipt event the subscriber:

1. Pulls the full receipt row plus the evidence already linked to it
   (verification_runs, semantic_assertions, operator_decisions,
   bug_evidence_links, dispatch_runs).
2. Classifies what *kind* of candidate this receipt represents (Phase 1
   only emits ``review`` candidates).
3. Builds a :class:`~contracts.dataset.RawDatasetCandidate` with summary
   blobs, computes a dedupe signature, classifies redaction.
4. Inserts the candidate + evidence links idempotently
   (``UNIQUE (source_receipt_id, candidate_kind)``).
5. If active policies exist for the candidate's specialist target,
   computes scores via :mod:`runtime.dataset_scorer` and upserts them.

Subscriber semantics follow :mod:`runtime.operator_decision_projection_subscriber`
(cursor per (subscriber_id, channel), explicit refresh event after
processing). Cursor advances on the *last seen* event id, not the last
relevant one, so we don't reprocess unrelated receipts forever.
"""

from __future__ import annotations

import asyncio
from runtime.async_bridge import run_sync_safe
import hashlib
import json
import re
import uuid
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from contracts.dataset import (
    CandidateEvidenceLink,
    DatasetScoringPolicy,
    RawDatasetCandidate,
)
from storage.postgres import connect_workflow_database

from .dataset_redactor import classify_redaction
from .dataset_scorer import score_candidate
from .event_log import (
    CHANNEL_DATASET,
    CHANNEL_RECEIPT,
    aadvance_cursor,
    aemit,
    aget_cursor,
    aread_since,
)


_DEFAULT_SUBSCRIBER_ID = "dataset_candidate_ingester"
DEFAULT_SUBSCRIBER_ID = _DEFAULT_SUBSCRIBER_ID
DATASET_CANDIDATES_PROJECTION_ID = "dataset_raw_candidates"
EVENT_RAW_CANDIDATE_INGESTED = "raw_candidate_ingested"
EVENT_CANDIDATE_INGESTION_REFRESHED = "candidate_ingestion_refreshed"

_RELEVANT_RECEIPT_EVENTS = frozenset({"receipt_recorded"})


class _Connection(Protocol):
    async def execute(self, query: str, *args: object) -> str: ...

    async def fetch(self, query: str, *args: object) -> list[Any]: ...

    async def fetchrow(self, query: str, *args: object) -> Any: ...

    def transaction(self) -> object: ...

    async def close(self) -> None: ...


# --------------------------------------------------------------------------
# Classification: receipt -> candidate_kind. Phase 1 emits only 'review'.
# --------------------------------------------------------------------------

_REVIEW_OUTPUT_KEYS = ("review_verdict", "verdict", "review")
_REVIEW_NODE_HINTS = ("review", "reviewer")
_TRIAGE_OUTPUT_KEYS = ("triage_verdict", "triage", "bug_severity", "triage_decision")
_TRIAGE_NODE_HINTS = ("triage", "bug_triage", "classify_failure")
_TRIAGE_TASK_TYPES = ("triage", "bug_triage", "failure_triage")
_EXPLAIN_OUTPUT_KEYS = ("operator_explanation", "explanation", "explain_answer", "answer")
_EXPLAIN_NODE_HINTS = ("operator_explain", "explain", "operator_answer")
_EXPLAIN_TASK_TYPES = ("operator_explain", "explain", "operator_answer")


def classify_candidate_kinds(receipt: Mapping[str, Any]) -> tuple[str, ...]:
    """Decide which candidate kinds this receipt should produce.

    Returns ``'triage'``, ``'review'``, ``'operator_explain'`` or
    nothing. The classifier is deliberately conservative: it requires
    positive output or node/task-type signal. Triage wins over review
    when both signals are present; explain only fires when no other
    candidate kind matches.
    """

    outputs = receipt.get("outputs") or {}
    inputs = receipt.get("inputs") or {}
    node_id = str(receipt.get("node_id") or "").lower()
    task_type = str(inputs.get("task_type") or "").lower()

    has_triage_output = any(k in outputs for k in _TRIAGE_OUTPUT_KEYS)
    looks_like_triage_node = any(hint in node_id for hint in _TRIAGE_NODE_HINTS)
    is_triage_task = any(t == task_type for t in _TRIAGE_TASK_TYPES)
    if has_triage_output or looks_like_triage_node or is_triage_task:
        return ("triage",)

    has_review_output = any(k in outputs for k in _REVIEW_OUTPUT_KEYS)
    looks_like_review_node = any(hint in node_id for hint in _REVIEW_NODE_HINTS)
    if has_review_output or looks_like_review_node:
        return ("review",)

    has_explain_output = any(k in outputs for k in _EXPLAIN_OUTPUT_KEYS)
    looks_like_explain_node = any(hint in node_id for hint in _EXPLAIN_NODE_HINTS)
    is_explain_task = any(t == task_type for t in _EXPLAIN_TASK_TYPES)
    if has_explain_output or looks_like_explain_node or is_explain_task:
        return ("operator_explain",)
    return ()


# --------------------------------------------------------------------------
# Dedupe signature.
# --------------------------------------------------------------------------

_ULID_RE = re.compile(r"\b[0-9A-HJKMNP-TV-Z]{26}\b")
_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_ISO_TS_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+\-]\d{2}:?\d{2})?\b")
_ABS_PATH_RE = re.compile(r"/(?:Users|home|var|tmp|private)/[A-Za-z0-9._\-/]+")
_HEX_HASH_RE = re.compile(r"\b[0-9a-f]{32,64}\b")


def _normalize_for_dedupe(value: Any) -> str:
    if value is None:
        text = ""
    elif isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, default=str, sort_keys=True, ensure_ascii=False)
        except (TypeError, ValueError):
            text = str(value)
    text = _ULID_RE.sub("<ULID>", text)
    text = _UUID_RE.sub("<UUID>", text)
    text = _ISO_TS_RE.sub("<TS>", text)
    text = _ABS_PATH_RE.sub("<PATH>", text)
    text = _HEX_HASH_RE.sub("<HASH>", text)
    return text


def compute_dedupe_signature(
    *, candidate_kind: str, route_slug: str | None, raw_input: Any
) -> str:
    """Stable sha256 over normalized (input, kind, route)."""

    payload = "\u0000".join(
        [
            candidate_kind,
            route_slug or "",
            _normalize_for_dedupe(raw_input),
        ]
    )
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------
# Evidence assembly.
# --------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _ReceiptEvidenceBundle:
    """All authority rows the ingestion subscriber needs for one receipt."""

    receipt: Mapping[str, Any]
    verifications: tuple[Mapping[str, Any], ...]
    assertions: tuple[Mapping[str, Any], ...]
    operator_decisions: tuple[Mapping[str, Any], ...]
    bug_links: tuple[Mapping[str, Any], ...]
    dispatch_run: Mapping[str, Any] | None
    workflow_run: Mapping[str, Any] | None


_EVIDENCE_TABLES = (
    "verification_runs",
    "semantic_assertions",
    "operator_decisions",
    "bug_evidence_links",
    "workflow_runs",
    "dispatch_runs",
)


async def _available_tables(
    conn: _Connection, *, candidates: Sequence[str] = _EVIDENCE_TABLES
) -> frozenset[str]:
    """Return the subset of ``candidates`` that exist in the current DB."""

    rows = await conn.fetch(
        """SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = ANY($1::text[])""",
        list(candidates),
    )
    return frozenset(str(r["table_name"]) for r in rows)


async def _load_receipt_bundle(
    conn: _Connection,
    *,
    receipt_id: str,
    available_tables: frozenset[str] | None = None,
) -> _ReceiptEvidenceBundle | None:
    receipt_row = await conn.fetchrow(
        """SELECT receipt_id, receipt_type, workflow_id, run_id, request_id,
                  node_id, attempt_no, started_at, finished_at, evidence_seq,
                  status, inputs, outputs, failure_code, decision_refs
             FROM receipts WHERE receipt_id = $1""",
        receipt_id,
    )
    if receipt_row is None:
        return None
    receipt = _row_to_dict(receipt_row)

    if available_tables is None:
        available_tables = await _available_tables(conn)

    verifications: list[Any] = []
    if "verification_runs" in available_tables:
        verifications = await conn.fetch(
            """SELECT verification_run_id, verifier_ref, target_kind, target_ref,
                      status, healing_candidate, attempted_at
                 FROM verification_runs
                WHERE target_kind = 'receipt' AND target_ref = $1
                ORDER BY attempted_at DESC""",
            receipt_id,
        )

    assertions: list[Any] = []
    if "semantic_assertions" in available_tables:
        assertions = await conn.fetch(
            """SELECT semantic_assertion_id, predicate_slug, assertion_status,
                      subject_kind, subject_ref, object_kind, object_ref,
                      source_kind, source_ref, evidence_ref, bound_decision_id,
                      valid_from, valid_to
                 FROM semantic_assertions
                WHERE evidence_ref = $1
                   OR (subject_kind = 'receipt' AND subject_ref = $1)
                   OR (object_kind = 'receipt' AND object_ref = $1)
                ORDER BY valid_from DESC""",
            receipt_id,
        )

    operator_decisions: list[Any] = []
    decision_refs = receipt.get("decision_refs") or []
    if (
        isinstance(decision_refs, list)
        and decision_refs
        and "operator_decisions" in available_tables
    ):
        decision_ids = [
            str(d.get("decision_id"))
            for d in decision_refs
            if isinstance(d, dict) and d.get("decision_id")
        ]
        if decision_ids:
            operator_decisions = await conn.fetch(
                """SELECT operator_decision_id, decision_kind, decision_status,
                          decided_by, decided_at, effective_from, effective_to
                     FROM operator_decisions
                    WHERE operator_decision_id = ANY($1::text[])""",
                decision_ids,
            )

    bug_links: list[Any] = []
    if "bug_evidence_links" in available_tables:
        bug_links = await conn.fetch(
            """SELECT bug_id, evidence_kind, evidence_ref, evidence_role
                 FROM bug_evidence_links
                WHERE evidence_kind = 'receipt' AND evidence_ref = $1""",
            receipt_id,
        )

    dispatch_run: Mapping[str, Any] | None = None
    workflow_run: Mapping[str, Any] | None = None
    run_id = receipt.get("run_id")
    if run_id:
        if "workflow_runs" in available_tables:
            wf_run = await conn.fetchrow(
                """SELECT run_id, workflow_id, workflow_definition_id,
                          admitted_definition_hash, current_state, terminal_reason_code
                     FROM workflow_runs WHERE run_id = $1""",
                run_id,
            )
            if wf_run is not None:
                workflow_run = _row_to_dict(wf_run)
        if "dispatch_runs" in available_tables:
            dr_row = await conn.fetchrow(
                """SELECT run_id, spec_name, phase, status, outcome_goal
                     FROM dispatch_runs WHERE run_id = $1""",
                run_id,
            )
            if dr_row is not None:
                dispatch_run = _row_to_dict(dr_row)

    return _ReceiptEvidenceBundle(
        receipt=receipt,
        verifications=tuple(_row_to_dict(r) for r in verifications),
        assertions=tuple(_row_to_dict(r) for r in assertions),
        operator_decisions=tuple(_row_to_dict(r) for r in operator_decisions),
        bug_links=tuple(_row_to_dict(r) for r in bug_links),
        dispatch_run=dispatch_run,
        workflow_run=workflow_run,
    )


def _row_to_dict(row: Any) -> dict[str, Any]:
    """asyncpg ``Record`` or ``dict`` → plain dict, JSON-decoding stringy fields."""

    data = dict(row) if not isinstance(row, dict) else dict(row)
    for k, v in list(data.items()):
        if isinstance(v, str) and v and v[0] in "{[":
            try:
                data[k] = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                pass
    return data


# --------------------------------------------------------------------------
# Candidate construction.
# --------------------------------------------------------------------------


def _verifier_summary(verifications: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    if not verifications:
        return None
    primary = verifications[0]
    statuses = [str(v.get("status", "")).lower() for v in verifications]
    overall = "passed" if statuses and all(s == "passed" for s in statuses) else (
        "failed" if any(s in {"failed", "error"} for s in statuses) else "unknown"
    )
    return {
        "status": overall,
        "verifier_ref": primary.get("verifier_ref"),
        "verification_run_id": primary.get("verification_run_id"),
        "all_runs": [
            {
                "verification_run_id": v.get("verification_run_id"),
                "verifier_ref": v.get("verifier_ref"),
                "status": v.get("status"),
            }
            for v in verifications
        ],
    }


def _review_summary(assertions: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    review_assertions = [
        a for a in assertions if "review" in str(a.get("predicate_slug", "")).lower()
    ]
    if not review_assertions:
        return None
    primary = review_assertions[0]
    return {
        "assertion_ids": [str(a.get("semantic_assertion_id")) for a in review_assertions],
        "predicate": primary.get("predicate_slug"),
        "status": primary.get("assertion_status"),
    }


def _operator_summary(decisions: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    if not decisions:
        return None
    primary = decisions[0]
    return {
        "decision_ids": [str(d.get("operator_decision_id")) for d in decisions],
        "status": primary.get("decision_status"),
        "decided_by": primary.get("decided_by"),
    }


def _downstream_summary(
    workflow_run: Mapping[str, Any] | None,
    bug_links: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    if workflow_run is None and not bug_links:
        return None
    summary: dict[str, Any] = {}
    if workflow_run is not None:
        summary["workflow_run_state"] = workflow_run.get("current_state")
        summary["terminal_reason_code"] = workflow_run.get("terminal_reason_code")
    if bug_links:
        summary["linked_bug_ids"] = [str(b.get("bug_id")) for b in bug_links]
    return summary


def _route_slug(receipt: Mapping[str, Any], candidate_kind: str) -> str:
    """Curation target for this candidate (e.g. ``slm/review``).

    Always derived from ``candidate_kind`` — it names the *specialist the
    data is curated for*, not the model that happened to produce the
    receipt. The producing model is captured separately on
    ``provider_ref`` / ``model_ref``.
    """

    return f"slm/{candidate_kind}"


def build_candidate_from_bundle(
    bundle: _ReceiptEvidenceBundle, *, candidate_kind: str
) -> RawDatasetCandidate:
    """Pure assembly: bundle -> RawDatasetCandidate."""

    receipt = bundle.receipt
    inputs = receipt.get("inputs") or {}
    outputs = receipt.get("outputs") or {}
    parsed = outputs.get("parsed") if isinstance(outputs, dict) else None

    raw_input_ref = {"receipt_id": receipt.get("receipt_id"), "path": "$.inputs"}
    raw_output_ref = {"receipt_id": receipt.get("receipt_id"), "path": "$.outputs"}
    parsed_output_ref = (
        {"receipt_id": receipt.get("receipt_id"), "path": "$.outputs.parsed"}
        if parsed
        else None
    )

    redaction = classify_redaction(inputs, outputs, parsed)

    route_slug = _route_slug(receipt, candidate_kind)
    dedupe_signature = compute_dedupe_signature(
        candidate_kind=candidate_kind,
        route_slug=route_slug,
        raw_input=inputs,
    )

    candidate_id = str(uuid.uuid4())

    workflow_run = bundle.workflow_run or {}
    linked_bug_ids = tuple(str(b.get("bug_id")) for b in bundle.bug_links if b.get("bug_id"))

    links: list[CandidateEvidenceLink] = [
        CandidateEvidenceLink(
            candidate_id=candidate_id,
            evidence_kind="receipt",
            evidence_ref=str(receipt.get("receipt_id")),
            evidence_role="source_input",
        )
    ]
    for v in bundle.verifications:
        links.append(
            CandidateEvidenceLink(
                candidate_id=candidate_id,
                evidence_kind="verification_run",
                evidence_ref=str(v.get("verification_run_id")),
                evidence_role="verifier_signal",
            )
        )
    for a in bundle.assertions:
        links.append(
            CandidateEvidenceLink(
                candidate_id=candidate_id,
                evidence_kind="semantic_assertion",
                evidence_ref=str(a.get("semantic_assertion_id")),
                evidence_role="reviewer_signal",
            )
        )
    for d in bundle.operator_decisions:
        links.append(
            CandidateEvidenceLink(
                candidate_id=candidate_id,
                evidence_kind="operator_decision",
                evidence_ref=str(d.get("operator_decision_id")),
                evidence_role="operator_signal",
            )
        )
    for b in bundle.bug_links:
        links.append(
            CandidateEvidenceLink(
                candidate_id=candidate_id,
                evidence_kind="bug",
                evidence_ref=str(b.get("bug_id")),
                evidence_role="failure_signature",
            )
        )
    if bundle.dispatch_run is not None:
        links.append(
            CandidateEvidenceLink(
                candidate_id=candidate_id,
                evidence_kind="dispatch_run",
                evidence_ref=str(bundle.dispatch_run.get("run_id")),
                evidence_role="downstream_outcome",
            )
        )
    if workflow_run:
        links.append(
            CandidateEvidenceLink(
                candidate_id=candidate_id,
                evidence_kind="workflow_run",
                evidence_ref=str(workflow_run.get("run_id")),
                evidence_role="downstream_outcome",
            )
        )

    return RawDatasetCandidate(
        candidate_id=candidate_id,
        candidate_kind=candidate_kind,
        source_receipt_id=str(receipt.get("receipt_id")),
        source_run_id=str(receipt.get("run_id")),
        source_node_id=str(receipt.get("node_id") or ""),
        source_workflow_id=str(receipt.get("workflow_id") or "") or None,
        task_type=str(inputs.get("task_type") or "") or None,
        route_slug=route_slug,
        persona=str(inputs.get("persona") or "") or None,
        provider_ref=str(inputs.get("provider_ref") or outputs.get("provider_ref") or "") or None,
        model_ref=str(
            inputs.get("model_ref")
            or outputs.get("author_model")
            or inputs.get("route_slug")
            or inputs.get("agent_slug")
            or ""
        )
        or None,
        workflow_definition_id=str(workflow_run.get("workflow_definition_id") or "") or None,
        admitted_definition_hash=str(workflow_run.get("admitted_definition_hash") or "") or None,
        repo_snapshot_ref=str((outputs.get("git_provenance") or {}).get("commit") or "") or None,
        raw_input_ref=raw_input_ref,
        raw_output_ref=raw_output_ref,
        parsed_output_ref=parsed_output_ref,
        verifier_summary=_verifier_summary(bundle.verifications),
        review_summary=_review_summary(bundle.assertions),
        operator_decision_summary=_operator_summary(bundle.operator_decisions),
        downstream_summary=_downstream_summary(bundle.workflow_run, bundle.bug_links),
        linked_bug_ids=linked_bug_ids,
        redaction_status=redaction.status,
        staleness_status="fresh",
        dedupe_signature=dedupe_signature,
        evidence_links=tuple(links),
    )


# --------------------------------------------------------------------------
# DB writes.
# --------------------------------------------------------------------------


async def _insert_candidate(
    conn: _Connection, candidate: RawDatasetCandidate
) -> bool:
    """Insert candidate + links. Returns True if inserted, False if duplicate."""

    row = await conn.fetchrow(
        """INSERT INTO dataset_raw_candidates (
                candidate_id, candidate_kind, source_receipt_id, source_run_id,
                source_node_id, source_workflow_id, task_type, route_slug, persona,
                provider_ref, model_ref, workflow_definition_id,
                admitted_definition_hash, repo_snapshot_ref,
                raw_input_ref, raw_output_ref, parsed_output_ref,
                verifier_summary, review_summary, operator_decision_summary,
                downstream_summary, linked_bug_ids, linked_roadmap_ids,
                redaction_status, staleness_status, dedupe_signature
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
                $15::jsonb, $16::jsonb, $17::jsonb,
                $18::jsonb, $19::jsonb, $20::jsonb, $21::jsonb,
                $22::text[], $23::text[], $24, $25, $26
            )
            ON CONFLICT (source_receipt_id, candidate_kind) DO NOTHING
            RETURNING candidate_id""",
        candidate.candidate_id,
        candidate.candidate_kind,
        candidate.source_receipt_id,
        candidate.source_run_id,
        candidate.source_node_id,
        candidate.source_workflow_id,
        candidate.task_type,
        candidate.route_slug,
        candidate.persona,
        candidate.provider_ref,
        candidate.model_ref,
        candidate.workflow_definition_id,
        candidate.admitted_definition_hash,
        candidate.repo_snapshot_ref,
        json.dumps(candidate.raw_input_ref),
        json.dumps(candidate.raw_output_ref),
        json.dumps(candidate.parsed_output_ref) if candidate.parsed_output_ref else None,
        json.dumps(candidate.verifier_summary) if candidate.verifier_summary else None,
        json.dumps(candidate.review_summary) if candidate.review_summary else None,
        json.dumps(candidate.operator_decision_summary)
        if candidate.operator_decision_summary
        else None,
        json.dumps(candidate.downstream_summary) if candidate.downstream_summary else None,
        list(candidate.linked_bug_ids),
        list(candidate.linked_roadmap_ids),
        candidate.redaction_status,
        candidate.staleness_status,
        candidate.dedupe_signature,
    )
    if row is None:
        return False
    for link in candidate.evidence_links:
        await conn.execute(
            """INSERT INTO dataset_candidate_evidence_links (
                    candidate_id, evidence_kind, evidence_ref, evidence_role
                ) VALUES ($1, $2, $3, $4)
                ON CONFLICT DO NOTHING""",
            link.candidate_id,
            link.evidence_kind,
            link.evidence_ref,
            link.evidence_role,
        )
    return True


async def _load_active_policies(
    conn: _Connection, *, specialist_target: str
) -> tuple[DatasetScoringPolicy, ...]:
    rows = await conn.fetch(
        """SELECT policy_id, policy_slug, specialist_target, rubric, auto_promote,
                  decided_by, rationale, created_at, superseded_by
             FROM dataset_scoring_policies
            WHERE specialist_target = $1 AND superseded_by IS NULL""",
        specialist_target,
    )
    policies: list[DatasetScoringPolicy] = []
    for r in rows:
        d = _row_to_dict(r)
        rubric = d.get("rubric") or {}
        policies.append(
            DatasetScoringPolicy(
                policy_id=str(d["policy_id"]),
                policy_slug=str(d["policy_slug"]),
                specialist_target=str(d["specialist_target"]),
                rubric=rubric,
                auto_promote=bool(d.get("auto_promote", False)),
                decided_by=str(d["decided_by"]),
                rationale=str(d["rationale"]),
                created_at=d.get("created_at"),
                superseded_by=d.get("superseded_by"),
            )
        )
    return tuple(policies)


_AUTO_PROMOTE_ELIGIBLE_TIERS: frozenset[str] = frozenset(
    {"sft_eligible", "preference_eligible", "eval_eligible", "routing_eligible"}
)
_EVENT_DATASET_PROMOTION_RECORDED = "dataset_promotion_recorded"
_CACHE_KIND_DATASET_CURATED_PROJECTION = "dataset_curated_projection"


def _default_family_for_kind(candidate_kind: str) -> str:
    if candidate_kind in {"review", "triage", "operator_explain", "repair"}:
        return "sft"
    if candidate_kind == "route_choice":
        return "routing"
    return "sft"


def _build_auto_promotion_payload(
    candidate: RawDatasetCandidate, score: Any
) -> dict[str, Any]:
    return {
        "prompt": {
            "raw_input_ref": dict(candidate.raw_input_ref),
            "task_type": candidate.task_type,
            "persona": candidate.persona,
            "route_slug": candidate.route_slug,
        },
        "target_output": {
            "raw_output_ref": dict(candidate.raw_output_ref),
            "parsed_output_ref": (
                dict(candidate.parsed_output_ref)
                if candidate.parsed_output_ref
                else None
            ),
        },
        "meta": {
            "auto_promoted": True,
            "score_confidence": float(score.confidence),
            "score_eligibility": score.eligibility,
            "admitted_definition_hash": candidate.admitted_definition_hash,
            "repo_snapshot_ref": candidate.repo_snapshot_ref,
            "source_receipt_id": candidate.source_receipt_id,
        },
    }


async def _maybe_auto_promote(
    conn: _Connection,
    *,
    candidate: RawDatasetCandidate,
    policy: DatasetScoringPolicy,
    score: Any,
) -> str | None:
    """If the policy opts into auto-promotion and the score clears an eligible
    tier, insert an auto-promotion row in the same transaction.

    Returns the new ``promotion_id`` when a row was inserted, else ``None``.
    """

    if not policy.auto_promote:
        return None
    if score.eligibility not in _AUTO_PROMOTE_ELIGIBLE_TIERS:
        return None
    if candidate.redaction_status != "clean":
        return None
    if candidate.staleness_status != "fresh":
        return None

    family = str(policy.rubric.get("auto_promote_family") or _default_family_for_kind(candidate.candidate_kind))
    if family not in {"sft", "preference", "eval", "routing"}:
        family = _default_family_for_kind(candidate.candidate_kind)
    if family == "preference":
        # Preference pairs require two candidates; auto-promote cannot
        # synthesize a pair on its own.
        return None

    split_tag = str(policy.rubric.get("auto_promote_split") or "train")
    if split_tag not in {"train", "eval", "holdout"}:
        split_tag = "train"

    # Contract check (raises on misuse)
    promotion_id = f"prom_{uuid.uuid4().hex[:20]}"
    payload = _build_auto_promotion_payload(candidate, score)
    promoted_by = f"system:{policy.policy_slug}"
    rationale = (
        f"auto-promoted by policy {policy.policy_slug}: "
        f"{score.eligibility} @ confidence={float(score.confidence):.3f}"
    )

    # Bridge: every auto-promotion writes a typed dataset_promotion decision
    # row so the refinery has decision-table authority alongside the event.
    from surfaces.api.operator_write import _arecord_dataset_decision
    decision_ref = await _arecord_dataset_decision(
        conn,
        decision_kind="dataset_promotion",
        decision_key=f"dataset-promotion::{promotion_id}",
        decision_scope_kind="dataset_specialist",
        decision_scope_ref=policy.specialist_target,
        title=f"Dataset auto-promotion {promotion_id}",
        rationale=rationale,
        decided_by=promoted_by,
        decision_source=f"dataset_candidate_subscriber.auto_promote:{policy.policy_slug}",
    )

    # Serialize for Postgres
    await conn.execute(
        """INSERT INTO dataset_promotions (
                promotion_id, candidate_ids, dataset_family, specialist_target,
                policy_id, payload, split_tag, promoted_by, promotion_kind,
                rationale, decision_ref
            ) VALUES ($1, $2::text[], $3, $4, $5, $6::jsonb, $7, $8, $9, $10, $11)""",
        promotion_id,
        [candidate.candidate_id],
        family,
        policy.specialist_target,
        policy.policy_id,
        json.dumps(payload),
        split_tag,
        promoted_by,
        "auto",
        rationale,
        decision_ref,
    )
    event_payload = {
        "promotion_id": promotion_id,
        "dataset_family": family,
        "specialist_target": policy.specialist_target,
        "policy_id": policy.policy_id,
        "promoted_by": promoted_by,
        "promotion_kind": "auto",
        "split_tag": split_tag,
        "candidate_ids": [candidate.candidate_id],
        "decision_ref": decision_ref,
    }
    await aemit(
        conn,
        channel=CHANNEL_DATASET,
        event_type=_EVENT_DATASET_PROMOTION_RECORDED,
        entity_id=promotion_id,
        entity_kind="dataset_promotion",
        payload=event_payload,
        emitted_by="dataset_candidate_subscriber.auto_promote",
    )
    # Causal side effects (cache invalidation, etc) flow through the
    # semantic_predicate_catalog so this auto path shares the same
    # declared propagation as the manual operator_write path.  Predicate:
    # ``dataset_promotion.invalidates_curated_projection_cache``.
    from runtime.semantic_propagation_engine import fire_causal_propagations

    await fire_causal_propagations(
        conn,
        event_type=_EVENT_DATASET_PROMOTION_RECORDED,
        event_payload=event_payload,
        emitted_by="dataset_candidate_subscriber.auto_promote",
    )
    return promotion_id


async def _upsert_score(conn: _Connection, score: Any) -> None:
    await conn.execute(
        """INSERT INTO dataset_candidate_scores (
                candidate_id, policy_id, eligibility, confidence,
                factors, rationale, scored_against_definition_hash
            ) VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7)
            ON CONFLICT (candidate_id, policy_id) DO UPDATE SET
                eligibility = EXCLUDED.eligibility,
                confidence = EXCLUDED.confidence,
                factors = EXCLUDED.factors,
                rationale = EXCLUDED.rationale,
                scored_at = now(),
                scored_against_definition_hash = EXCLUDED.scored_against_definition_hash""",
        score.candidate_id,
        score.policy_id,
        score.eligibility,
        float(score.confidence),
        json.dumps(score.factors),
        score.rationale,
        score.scored_against_definition_hash,
    )


# --------------------------------------------------------------------------
# Subscriber.
# --------------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class CandidateIngestionResult:
    subscriber_id: str
    starting_cursor: int
    ending_cursor: int
    scanned_count: int
    candidates_inserted: int
    candidates_skipped_duplicate: int
    scores_recorded: int
    auto_promotions: int
    refresh_event_id: int | None

    def to_json(self) -> dict[str, Any]:
        return {
            "subscriber_id": self.subscriber_id,
            "starting_cursor": self.starting_cursor,
            "ending_cursor": self.ending_cursor,
            "scanned_count": self.scanned_count,
            "candidates_inserted": self.candidates_inserted,
            "candidates_skipped_duplicate": self.candidates_skipped_duplicate,
            "scores_recorded": self.scores_recorded,
            "auto_promotions": self.auto_promotions,
            "refresh_event_id": self.refresh_event_id,
        }


@dataclass(slots=True)
class DatasetCandidateSubscriber:
    """Durable subscriber: receipt events -> dataset_raw_candidates."""

    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    )
    load_receipt_bundle: Callable[..., Awaitable[_ReceiptEvidenceBundle | None]] = (
        _load_receipt_bundle
    )
    classify_kinds: Callable[[Mapping[str, Any]], tuple[str, ...]] = classify_candidate_kinds

    async def consume_available_async(
        self,
        *,
        limit: int = 100,
        subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        normalized_limit = max(1, int(limit or 100))
        conn = await self.connect_database(env)
        candidates_inserted = 0
        candidates_skipped = 0
        scores_recorded = 0
        auto_promotions = 0
        refresh_event_id: int | None = None
        try:
            async with conn.transaction():
                starting_cursor = await aget_cursor(
                    conn, subscriber_id=subscriber_id, channel=CHANNEL_RECEIPT
                )
                events = await aread_since(
                    conn,
                    channel=CHANNEL_RECEIPT,
                    cursor=starting_cursor,
                    limit=normalized_limit,
                )
                relevant_events = [
                    e for e in events if e.event_type in _RELEVANT_RECEIPT_EVENTS
                ]
                available = await _available_tables(conn) if relevant_events else frozenset()
                for event in relevant_events:
                    receipt_id = (event.payload or {}).get("receipt_id") or event.entity_id
                    if not receipt_id:
                        continue
                    bundle = await self.load_receipt_bundle(
                        conn, receipt_id=receipt_id, available_tables=available
                    )
                    if bundle is None:
                        continue
                    kinds = self.classify_kinds(bundle.receipt)
                    for kind in kinds:
                        candidate = build_candidate_from_bundle(bundle, candidate_kind=kind)
                        inserted = await _insert_candidate(conn, candidate)
                        if not inserted:
                            candidates_skipped += 1
                            continue
                        candidates_inserted += 1
                        await aemit(
                            conn,
                            channel=CHANNEL_DATASET,
                            event_type=EVENT_RAW_CANDIDATE_INGESTED,
                            entity_id=candidate.candidate_id,
                            entity_kind="dataset_raw_candidate",
                            payload={
                                "candidate_id": candidate.candidate_id,
                                "candidate_kind": candidate.candidate_kind,
                                "source_receipt_id": candidate.source_receipt_id,
                                "route_slug": candidate.route_slug,
                                "redaction_status": candidate.redaction_status,
                            },
                            emitted_by="dataset_candidate_subscriber.consume",
                        )
                        if candidate.route_slug:
                            policies = await _load_active_policies(
                                conn, specialist_target=candidate.route_slug
                            )
                            for policy in policies:
                                score = score_candidate(candidate, policy)
                                await _upsert_score(conn, score)
                                scores_recorded += 1
                                promo_id = await _maybe_auto_promote(
                                    conn,
                                    candidate=candidate,
                                    policy=policy,
                                    score=score,
                                )
                                if promo_id:
                                    auto_promotions += 1
                ending_cursor = starting_cursor
                if events:
                    ending_cursor = events[-1].id
                    await aadvance_cursor(
                        conn,
                        subscriber_id=subscriber_id,
                        channel=CHANNEL_RECEIPT,
                        event_id=ending_cursor,
                    )
                if relevant_events or candidates_inserted:
                    refresh_event_id = await aemit(
                        conn,
                        channel=CHANNEL_DATASET,
                        event_type=EVENT_CANDIDATE_INGESTION_REFRESHED,
                        entity_id=DATASET_CANDIDATES_PROJECTION_ID,
                        entity_kind="dataset_candidate_projection",
                        payload={
                            "subscriber_id": subscriber_id,
                            "scanned_count": len(events),
                            "candidates_inserted": candidates_inserted,
                            "candidates_skipped_duplicate": candidates_skipped,
                            "scores_recorded": scores_recorded,
                            "refreshed_at": _now().isoformat(),
                        },
                        emitted_by="dataset_candidate_subscriber.consume",
                    )
        finally:
            await conn.close()
        return CandidateIngestionResult(
            subscriber_id=subscriber_id,
            starting_cursor=starting_cursor,
            ending_cursor=ending_cursor,
            scanned_count=len(events),
            candidates_inserted=candidates_inserted,
            candidates_skipped_duplicate=candidates_skipped,
            scores_recorded=scores_recorded,
            auto_promotions=auto_promotions,
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


async def aconsume_dataset_candidate_events(
    *,
    limit: int = 100,
    subscriber_id: str = _DEFAULT_SUBSCRIBER_ID,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return await DatasetCandidateSubscriber().consume_available_async(
        limit=limit, subscriber_id=subscriber_id, env=env
    )


async def aingest_receipts_backfill(
    *,
    since_days: int | None = None,
    receipt_ids: Sequence[str] | None = None,
    limit: int = 500,
    env: Mapping[str, str] | None = None,
    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    ),
) -> dict[str, Any]:
    """Direct-from-receipts backfill for candidates.

    Bypasses the event_log cursor and reads the ``receipts`` table directly.
    Useful for seeding the refinery with receipts that predate the
    dataset-refinery event emission path, or for re-scanning a specific
    receipt set after policy/rubric changes.

    Selection precedence: ``receipt_ids`` > ``since_days`` > "most recent".
    """

    conn = await connect_database(env)
    inserted = 0
    skipped = 0
    scored = 0
    auto_promotions = 0
    scanned = 0
    try:
        async with conn.transaction():
            available = await _available_tables(conn)
            if receipt_ids:
                rows = await conn.fetch(
                    """SELECT receipt_id FROM receipts
                        WHERE receipt_id = ANY($1::text[])""",
                    list(receipt_ids),
                )
            elif since_days is not None and since_days > 0:
                rows = await conn.fetch(
                    """SELECT receipt_id FROM receipts
                        WHERE finished_at >= now() - ($1::int * interval '1 day')
                        ORDER BY finished_at DESC NULLS LAST
                        LIMIT $2""",
                    int(since_days),
                    int(limit),
                )
            else:
                rows = await conn.fetch(
                    """SELECT receipt_id FROM receipts
                        ORDER BY finished_at DESC NULLS LAST
                        LIMIT $1""",
                    int(limit),
                )
            for row in rows:
                scanned += 1
                receipt_id = str(row["receipt_id"])
                bundle = await _load_receipt_bundle(
                    conn, receipt_id=receipt_id, available_tables=available
                )
                if bundle is None:
                    continue
                kinds = classify_candidate_kinds(bundle.receipt)
                for kind in kinds:
                    candidate = build_candidate_from_bundle(bundle, candidate_kind=kind)
                    did_insert = await _insert_candidate(conn, candidate)
                    if not did_insert:
                        skipped += 1
                        continue
                    inserted += 1
                    await aemit(
                        conn,
                        channel=CHANNEL_DATASET,
                        event_type=EVENT_RAW_CANDIDATE_INGESTED,
                        entity_id=candidate.candidate_id,
                        entity_kind="dataset_raw_candidate",
                        payload={
                            "candidate_id": candidate.candidate_id,
                            "candidate_kind": candidate.candidate_kind,
                            "source_receipt_id": candidate.source_receipt_id,
                            "route_slug": candidate.route_slug,
                            "redaction_status": candidate.redaction_status,
                            "backfill": True,
                        },
                        emitted_by="dataset_candidate_subscriber.backfill",
                    )
                    if candidate.route_slug:
                        policies = await _load_active_policies(
                            conn, specialist_target=candidate.route_slug
                        )
                        for policy in policies:
                            score = score_candidate(candidate, policy)
                            await _upsert_score(conn, score)
                            scored += 1
                            promo_id = await _maybe_auto_promote(
                                conn,
                                candidate=candidate,
                                policy=policy,
                                score=score,
                            )
                            if promo_id:
                                auto_promotions += 1
    finally:
        await conn.close()
    return {
        "mode": "backfill",
        "scanned_receipts": scanned,
        "candidates_inserted": inserted,
        "candidates_skipped_duplicate": skipped,
        "scores_recorded": scored,
        "auto_promotions": auto_promotions,
    }


__all__ = [
    "DATASET_CANDIDATES_PROJECTION_ID",
    "DEFAULT_SUBSCRIBER_ID",
    "EVENT_CANDIDATE_INGESTION_REFRESHED",
    "EVENT_RAW_CANDIDATE_INGESTED",
    "CandidateIngestionResult",
    "DatasetCandidateSubscriber",
    "aconsume_dataset_candidate_events",
    "aingest_receipts_backfill",
    "build_candidate_from_bundle",
    "classify_candidate_kinds",
    "compute_dedupe_signature",
]
