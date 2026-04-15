"""Bug tracker backed by Postgres with tsvector FTS."""

from __future__ import annotations

import enum
import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Optional, Tuple

if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection
    from runtime.embedding_service import EmbeddingService

from runtime import bug_evidence as _bug_evidence
from storage.postgres.vector_store import PostgresVectorStore
from runtime.payload_coercion import json_object as _json_object, json_list as _json_list, coerce_datetime as _coerce_datetime


_BUG_BLAST_RADIUS_WINDOW_SQL = "7 days"
_TAG_VALUE_PATTERN = re.compile(r"[^a-z0-9._:/-]+")
_ALLOWED_EVIDENCE_KINDS = frozenset({"receipt", "run", "verification_run", "healing_run"})
_ALLOWED_EVIDENCE_ROLES = frozenset({"observed_in", "attempted_fix", "validates_fix"})
_VERIFICATION_SUCCESS_STATUSES = frozenset({"passed", "succeeded", "success", "ok"})


def _normalize_tag_value(value: object) -> str:
    text = _TAG_VALUE_PATTERN.sub("-", str(value or "").strip().lower())
    return text.strip("-") or "none"


def _extract_tag_value(tags: Tuple[str, ...], prefix: str) -> str | None:
    normalized_prefix = f"{prefix.lower()}:"
    for raw_tag in tags:
        tag = str(raw_tag or "").strip()
        if tag.lower().startswith(normalized_prefix):
            return tag.split(":", 1)[1].strip() or None
    return None


def _stable_fingerprint(payload: dict[str, Any]) -> str:
    return hashlib.sha1(
        json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    ).hexdigest()[:20]


def _ordered_unique(values: list[Any]) -> tuple[Any, ...]:
    deduped: list[Any] = []
    seen: set[str] = set()
    for value in values:
        if value in (None, "", [], (), {}):
            continue
        try:
            key = json.dumps(value, sort_keys=True, default=str)
        except TypeError:
            key = str(value)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return tuple(deduped)


def _attempted_at_sort_key(item: Any) -> datetime:
    return _bug_evidence.attempted_at_sort_key(item)


def _payload_keys(payload: Any) -> tuple[str, ...]:
    if isinstance(payload, dict):
        return tuple(sorted(str(key) for key in payload.keys()))
    return ()


def _verification_passed(status: object) -> bool:
    return _bug_evidence.verification_passed(status)


def _packet_summary(packet: dict[str, Any]) -> dict[str, Any]:
    return _bug_evidence.packet_summary(packet)


def build_failure_signature(
    *,
    failure_code: str | None,
    job_label: str | None = None,
    node_id: str | None = None,
    failure_category: str | None = None,
    agent: str | None = None,
    provider_slug: str | None = None,
    model_slug: str | None = None,
    source_kind: str | None = None,
) -> dict[str, Any]:
    return _bug_evidence.build_failure_signature(
        failure_code=failure_code,
        job_label=job_label,
        node_id=node_id,
        failure_category=failure_category,
        agent=agent,
        provider_slug=provider_slug,
        model_slug=model_slug,
        source_kind=source_kind,
    )


def _extract_receipt_paths(payload: dict[str, Any], *, key: str) -> tuple[str, ...]:
    return _bug_evidence.extract_receipt_paths(payload, key=key)


def _extract_write_paths(inputs: dict[str, Any], outputs: dict[str, Any]) -> tuple[str, ...]:
    return _bug_evidence.extract_write_paths(inputs, outputs)


# -- Enums ------------------------------------------------------------------


class BugSeverity(enum.Enum):
    P0 = "P0"
    P1 = "P1"
    P2 = "P2"
    P3 = "P3"


class BugStatus(enum.Enum):
    OPEN = "OPEN"
    IN_PROGRESS = "IN_PROGRESS"
    FIXED = "FIXED"
    WONT_FIX = "WONT_FIX"
    DEFERRED = "DEFERRED"


class BugCategory(enum.Enum):
    SCOPE = "SCOPE"
    VERIFY = "VERIFY"
    IMPORT = "IMPORT"
    WIRING = "WIRING"
    ARCHITECTURE = "ARCHITECTURE"
    RUNTIME = "RUNTIME"
    TEST = "TEST"
    OTHER = "OTHER"


# -- Data classes -----------------------------------------------------------


@dataclass(frozen=True)
class Bug:
    bug_id: str
    bug_key: str
    title: str
    severity: BugSeverity
    status: BugStatus
    priority: str
    category: BugCategory
    description: str
    summary: str
    filed_at: datetime
    updated_at: datetime
    resolved_at: Optional[datetime]
    created_at: datetime
    filed_by: str
    assigned_to: Optional[str]
    tags: Tuple[str, ...]
    source_kind: str
    discovered_in_run_id: Optional[str]
    discovered_in_receipt_id: Optional[str]
    owner_ref: Optional[str]
    decision_ref: str
    resolution_summary: Optional[str]


@dataclass(frozen=True)
class BugStats:
    total: int
    by_status: Dict[str, int]
    by_severity: Dict[str, int]
    by_category: Dict[str, int]
    open_count: int
    mttr_hours: Optional[float]
    packet_ready_count: Optional[int] = 0
    replay_ready_count: Optional[int] = 0
    replay_blocked_count: Optional[int] = 0
    fix_verified_count: Optional[int] = 0
    underlinked_count: Optional[int] = 0
    observability_state: str = "complete"
    errors: Tuple[str, ...] = ()


# -- Tracker ----------------------------------------------------------------

_RESOLVED_STATUSES = {BugStatus.FIXED, BugStatus.WONT_FIX, BugStatus.DEFERRED}


class BugTracker:
    """Postgres-backed bug tracker with tsvector FTS."""

    def __init__(self, conn: "SyncPostgresConnection", embedder: "EmbeddingService | None" = None) -> None:
        self._conn = conn
        self._embedder = embedder
        self._vector_store = (
            PostgresVectorStore(conn, embedder) if embedder is not None else None
        )

    @staticmethod
    def _normalize_status(value: object, default: BugStatus = BugStatus.OPEN) -> BugStatus:
        value_text = str(value).strip().upper().replace("-", "_") if value is not None else ""
        if value_text in {"RESOLVED", "DONE", "CLOSED"}:
            value_text = BugStatus.FIXED.value
        return BugTracker._safe_enum(BugStatus, value_text, default)

    @staticmethod
    def _normalize_category(value: object, default: BugCategory = BugCategory.OTHER) -> BugCategory:
        value_text = str(value).strip().upper().replace("-", "_") if value is not None else ""
        return BugTracker._safe_enum(BugCategory, value_text, default)

    @staticmethod
    def _normalize_severity(value: object, default: BugSeverity = BugSeverity.P2) -> BugSeverity:
        value_text = str(value).strip().upper().replace("-", "_") if value is not None else ""
        alias_map = {
            "LOW": BugSeverity.P3.value,
            "MEDIUM": BugSeverity.P2.value,
            "HIGH": BugSeverity.P1.value,
            "CRITICAL": BugSeverity.P0.value,
        }
        value_text = alias_map.get(value_text, value_text)
        return BugTracker._safe_enum(BugSeverity, value_text, default)

    @staticmethod
    def _status_filter_values(status: BugStatus) -> list[str]:
        if status == BugStatus.FIXED:
            return [
                BugStatus.FIXED.value,
                "RESOLVED",
                "DONE",
                "CLOSED",
            ]
        return [status.value]

    @staticmethod
    def _severity_filter_values(severity: BugSeverity) -> list[str]:
        alias_map = {
            BugSeverity.P0: ["P0", "CRITICAL"],
            BugSeverity.P1: ["P1", "HIGH"],
            BugSeverity.P2: ["P2", "MEDIUM"],
            BugSeverity.P3: ["P3", "LOW"],
        }
        return alias_map.get(severity, [severity.value])

    @staticmethod
    def _safe_enum(enum_cls, value, default):
        try:
            return enum_cls(value)
        except (ValueError, KeyError):
            return default

    def _row_to_bug(self, row) -> Bug:
        resolved = row["resolved_at"]
        filed_at = row.get("filed_at", None) or row.get("opened_at", None) or row["created_at"]
        created_at = _coerce_datetime(row.get("created_at")) or (
            filed_at if isinstance(filed_at, datetime) else datetime.fromisoformat(filed_at)
        )
        return Bug(
            bug_id=row["bug_id"],
            bug_key=str(row.get("bug_key") or row["bug_id"].lower().replace("-", "_")),
            title=row["title"],
            severity=self._normalize_severity(row["severity"], default=BugSeverity.P2),
            status=self._normalize_status(row["status"], default=BugStatus.OPEN),
            priority=str(row.get("priority") or self._normalize_severity(row["severity"], default=BugSeverity.P2).value),
            category=self._normalize_category(row.get("category"), default=BugCategory.OTHER),
            description=row.get("description") or row.get("summary") or "",
            summary=str(row.get("summary") or row.get("description") or ""),
            filed_at=filed_at if isinstance(filed_at, datetime) else datetime.fromisoformat(filed_at),
            updated_at=row["updated_at"] if isinstance(row["updated_at"], datetime) else datetime.fromisoformat(row["updated_at"]),
            resolved_at=resolved if isinstance(resolved, datetime) else (datetime.fromisoformat(resolved) if resolved else None),
            created_at=created_at,
            filed_by=row.get("filed_by") or row.get("owner_ref") or "system",
            assigned_to=row.get("assigned_to"),
            tags=tuple(row["tags"].split(",")) if row.get("tags") else (),
            source_kind=str(row.get("source_kind") or "manual"),
            discovered_in_run_id=row.get("discovered_in_run_id"),
            discovered_in_receipt_id=row.get("discovered_in_receipt_id"),
            owner_ref=row.get("owner_ref"),
            decision_ref=str(row.get("decision_ref") or ""),
            resolution_summary=str(row.get("resolution_summary") or "").strip() or None,
        )

    def _row_to_evidence_link(self, row: Any) -> dict[str, Any]:
        return {
            "bug_evidence_link_id": str(row.get("bug_evidence_link_id") or ""),
            "bug_id": str(row.get("bug_id") or ""),
            "evidence_kind": str(row.get("evidence_kind") or ""),
            "evidence_ref": str(row.get("evidence_ref") or ""),
            "evidence_role": str(row.get("evidence_role") or ""),
            "created_at": _coerce_datetime(row.get("created_at")),
            "created_by": str(row.get("created_by") or ""),
            "notes": str(row.get("notes") or "").strip() or None,
        }

    def _row_to_receipt_summary(self, row: Any) -> dict[str, Any]:
        inputs = _json_object(row.get("inputs"))
        outputs = _json_object(row.get("outputs"))
        artifacts = _json_object(row.get("artifacts"))
        decision_refs = _json_list(row.get("decision_refs"))
        agent = (
            str(
                inputs.get("agent_slug")
                or inputs.get("agent")
                or outputs.get("author_model")
                or row.get("executor_type")
                or ""
            ).strip()
            or "unknown"
        )
        provider_slug = str(
            outputs.get("provider_slug") or inputs.get("provider_slug") or ""
        ).strip()
        model_slug = str(outputs.get("model_slug") or inputs.get("model_slug") or "").strip()
        if not provider_slug and "/" in agent:
            provider_slug, _, model_slug = agent.partition("/")
        git_provenance = _json_object(outputs.get("git_provenance"))
        workspace_provenance = _json_object(outputs.get("workspace_provenance"))
        failure_classification = _json_object(outputs.get("failure_classification"))
        write_paths = _extract_write_paths(inputs, outputs)
        verified_paths = _extract_receipt_paths(outputs, key="verified_paths")
        timestamp = _coerce_datetime(row.get("finished_at")) or _coerce_datetime(row.get("started_at"))
        return {
            "receipt_id": str(row.get("receipt_id") or ""),
            "workflow_id": str(row.get("workflow_id") or ""),
            "run_id": str(row.get("run_id") or ""),
            "request_id": str(row.get("request_id") or ""),
            "node_id": str(row.get("node_id") or ""),
            "status": str(row.get("status") or ""),
            "failure_code": str(row.get("failure_code") or ""),
            "timestamp": timestamp,
            "started_at": _coerce_datetime(row.get("started_at")),
            "finished_at": _coerce_datetime(row.get("finished_at")),
            "executor_type": str(row.get("executor_type") or ""),
            "agent": agent,
            "provider_slug": provider_slug or None,
            "model_slug": model_slug or None,
            "latency_ms": int(outputs.get("duration_ms") or 0),
            "verification_status": str(outputs.get("verification_status") or "").strip() or None,
            "failure_category": str(failure_classification.get("category") or "").strip() or None,
            "failure_classification": failure_classification or None,
            "inputs": inputs,
            "outputs": outputs,
            "artifacts": artifacts,
            "decision_refs": decision_refs,
            "git_provenance": git_provenance,
            "workspace_provenance": workspace_provenance,
            "write_paths": write_paths,
            "verified_paths": verified_paths,
        }

    def _query_rows_with_error(self, query: str, *params: object) -> tuple[list[Any], str | None]:
        try:
            return list(self._conn.execute(query, *params) or []), None
        except Exception as exc:
            return [], f"{type(exc).__name__}: {exc}"

    def _query_optional_rows(self, query: str, *params: object) -> list[Any]:
        rows, _error = self._query_rows_with_error(query, *params)
        return rows

    def _query_scalar_with_error(self, query: str, *params: object) -> tuple[int | None, str | None]:
        try:
            return int(self._conn.fetchval(query, *params) or 0), None
        except Exception as exc:
            return None, f"{type(exc).__name__}: {exc}"

    def _query_optional_scalar(self, query: str, *params: object) -> int:
        value, _error = self._query_scalar_with_error(query, *params)
        return int(value or 0)

    def _record_exists(self, query: str, *params: object) -> bool:
        try:
            return bool(self._conn.fetchval(query, *params))
        except Exception:
            return False

    def _validate_bug_provenance(
        self,
        *,
        discovered_in_run_id: str | None,
        discovered_in_receipt_id: str | None,
    ) -> None:
        if discovered_in_run_id and not self._record_exists(
            "SELECT 1 FROM workflow_runs WHERE run_id = $1",
            discovered_in_run_id,
        ):
            raise ValueError(f"unknown discovered_in_run_id: {discovered_in_run_id}")
        if discovered_in_receipt_id and not self._record_exists(
            "SELECT 1 FROM receipts WHERE receipt_id = $1",
            discovered_in_receipt_id,
        ):
            raise ValueError(
                f"unknown discovered_in_receipt_id: {discovered_in_receipt_id}"
            )

    def _validate_evidence_reference(
        self,
        *,
        evidence_kind: str,
        evidence_ref: str,
    ) -> None:
        if evidence_kind not in _ALLOWED_EVIDENCE_KINDS:
            allowed = ", ".join(sorted(_ALLOWED_EVIDENCE_KINDS))
            raise ValueError(f"evidence_kind must be one of {allowed}")
        if evidence_kind == "receipt":
            exists = self._record_exists(
                "SELECT 1 FROM receipts WHERE receipt_id = $1",
                evidence_ref,
            )
        elif evidence_kind == "run":
            exists = self._record_exists(
                "SELECT 1 FROM workflow_runs WHERE run_id = $1",
                evidence_ref,
            )
        elif evidence_kind == "verification_run":
            exists = self._record_exists(
                "SELECT 1 FROM verification_runs WHERE verification_run_id = $1",
                evidence_ref,
            )
        else:
            exists = self._record_exists(
                "SELECT 1 FROM healing_runs WHERE healing_run_id = $1",
                evidence_ref,
            )
        if not exists:
            raise ValueError(f"unknown {evidence_kind} reference: {evidence_ref}")

    def _public_receipt_summary(self, receipt: dict[str, Any] | None) -> dict[str, Any] | None:
        return _bug_evidence.public_receipt_summary(receipt)

    def _replay_action(
        self,
        *,
        bug_id: str,
        replay_context: dict[str, Any],
    ) -> dict[str, Any]:
        return _bug_evidence.replay_action(bug_id=bug_id, replay_context=replay_context)

    def _replay_run_view(self, run_id: str):
        from runtime.execution.orchestrator import RuntimeOrchestrator
        from storage.postgres import PostgresEvidenceReader, resolve_workflow_database_url

        database_url = resolve_workflow_database_url()
        orchestrator = RuntimeOrchestrator(
            evidence_reader=PostgresEvidenceReader(database_url=database_url),
        )
        return orchestrator.replay_run(run_id=run_id)

    def _find_signature_receipts(
        self,
        *,
        failure_code: str | None,
        node_id: str | None,
        limit: int,
    ) -> tuple[list[dict[str, Any]], str | None]:
        return _bug_evidence.find_signature_receipts(
            self._conn,
            failure_code=failure_code,
            node_id=node_id,
            limit=limit,
        )

    def _compare_write_sets(self, latest_receipt: dict[str, Any] | None) -> dict[str, Any]:
        return _bug_evidence.compare_write_sets(self._conn, latest_receipt)

    def _load_verification_rows(
        self,
        table_name: str,
        id_field: str,
        refs: tuple[str, ...],
    ) -> tuple[dict[str, dict[str, Any]], str | None]:
        return _bug_evidence.load_verification_rows(
            self._conn,
            table_name,
            id_field,
            refs,
        )

    def _build_observability_gaps(
        self,
        *,
        bug: Bug,
        evidence_links: list[dict[str, Any]],
        latest_receipt: dict[str, Any] | None,
        fix_validation_count: int,
    ) -> tuple[str, ...]:
        return _bug_evidence.build_observability_gaps(
            bug=bug,
            bug_status_fixed=BugStatus.FIXED,
            evidence_links=evidence_links,
            latest_receipt=latest_receipt,
            fix_validation_count=fix_validation_count,
        )

    def _build_counterfactual_axes(self, latest_receipt: dict[str, Any] | None) -> tuple[dict[str, Any], ...]:
        return _bug_evidence.build_counterfactual_axes(latest_receipt)

    def _build_blast_radius(
        self,
        *,
        failure_code: str | None,
        node_id: str | None,
    ) -> dict[str, Any]:
        return _bug_evidence.build_blast_radius(
            self._conn,
            failure_code=failure_code,
            node_id=node_id,
        )

    def _bug_signature_from_tags(self, bug: Bug) -> dict[str, Any]:
        return _bug_evidence.bug_signature_from_tags(bug)

    def _shared_signature_fields(
        self,
        current_signature: dict[str, Any],
        candidate_signature: dict[str, Any],
    ) -> tuple[str, ...]:
        return _bug_evidence.shared_signature_fields(current_signature, candidate_signature)

    def _historical_fix_evidence(self, bug_id: str) -> dict[str, Any]:
        evidence_links = self.list_evidence(bug_id)
        return _bug_evidence.historical_fix_evidence(
            self._conn,
            bug_id,
            evidence_links,
        )

    def _build_historical_fixes(
        self,
        *,
        bug: Bug,
        signature: dict[str, Any],
        limit: int = 5,
    ) -> dict[str, Any]:
        failure_code = str(signature.get("failure_code") or "").strip()
        node_anchor = str(signature.get("node_id") or signature.get("job_label") or "").strip()
        if not failure_code and not node_anchor:
            return {
                "count": 0,
                "items": (),
                "reason_code": "bug.historical_fixes.insufficient_signature",
                "errors": (),
            }

        clauses = [
            "bug_id <> $1",
            "UPPER(status) IN ('FIXED', 'RESOLVED', 'DONE', 'CLOSED')",
        ]
        params: list[object] = [bug.bug_id]
        next_idx = 2
        if failure_code:
            clauses.append(f"tags ILIKE ${next_idx}")
            params.append(f"%failure_code:{_normalize_tag_value(failure_code)}%")
            next_idx += 1
        if node_anchor:
            clauses.append(
                f"(tags ILIKE ${next_idx} OR tags ILIKE ${next_idx + 1})"
            )
            params.append(f"%node_id:{_normalize_tag_value(node_anchor)}%")
            params.append(f"%job_label:{_normalize_tag_value(node_anchor)}%")
            next_idx += 2
        params.append(max(limit * 4, 20))
        try:
            rows = self._conn.execute(
                f"""
                SELECT *
                  FROM bugs
                 WHERE {' AND '.join(clauses)}
                 ORDER BY resolved_at DESC NULLS LAST, updated_at DESC
                 LIMIT ${next_idx}
                """,
                *params,
            )
        except Exception as exc:
            return {
                "count": 0,
                "items": (),
                "reason_code": "bug.historical_fixes.query_failed",
                "errors": (f"{type(exc).__name__}: {exc}",),
            }

        ranked: list[tuple[int, datetime | None, dict[str, Any]]] = []
        errors: list[str] = []
        for row in rows:
            candidate = self._row_to_bug(row)
            candidate_signature = self._bug_signature_from_tags(candidate)
            shared_fields = self._shared_signature_fields(signature, candidate_signature)
            if not shared_fields:
                continue
            fix_evidence = self._historical_fix_evidence(candidate.bug_id)
            errors.extend(str(item) for item in fix_evidence.get("errors") or ())
            ranked.append(
                (
                    len(shared_fields),
                    candidate.resolved_at,
                    {
                        "bug_id": candidate.bug_id,
                        "bug_key": candidate.bug_key,
                        "title": candidate.title,
                        "status": candidate.status.value,
                        "severity": candidate.severity.value,
                        "resolved_at": candidate.resolved_at,
                        "decision_ref": candidate.decision_ref or None,
                        "resolution_summary": candidate.resolution_summary,
                        "shared_signature_fields": shared_fields,
                        "signature": candidate_signature,
                        "fix_verification": {
                            "fix_verified": bool(fix_evidence.get("fix_verified")),
                            "linked_validation_count": int(fix_evidence.get("linked_validation_count") or 0),
                            "verified_validation_count": int(fix_evidence.get("verified_validation_count") or 0),
                            "last_validation": fix_evidence.get("last_validation"),
                            "attempted_fix_count": int(fix_evidence.get("attempted_fix_count") or 0),
                            "last_attempted_fix": fix_evidence.get("last_attempted_fix"),
                        },
                    },
                )
            )

        ranked.sort(
            key=lambda item: (
                item[0],
                item[1] or datetime.min.replace(tzinfo=timezone.utc),
            ),
            reverse=True,
        )
        items = tuple(item[2] for item in ranked[:limit])
        return {
            "count": len(items),
            "items": items,
            "reason_code": "bug.historical_fixes.found" if items else "bug.historical_fixes.none",
            "errors": tuple(dict.fromkeys(errors)),
        }

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    # -- public API ---------------------------------------------------------

    def file_bug(
        self,
        title: str,
        severity: BugSeverity,
        category: BugCategory,
        description: str,
        filed_by: str,
        *,
        source_kind: str = "dispatch",
        decision_ref: str = "",
        discovered_in_run_id: str | None = None,
        discovered_in_receipt_id: str | None = None,
        owner_ref: str | None = None,
        tags: Tuple[str, ...] = (),
    ) -> tuple["Bug", list[dict]]:
        """File a new bug. Returns (bug, similar_bugs) where similar_bugs may be empty."""
        bug_id = f"BUG-{uuid.uuid4().hex[:8].upper()}"
        bug_key = bug_id.lower().replace("-", "_")
        now = self._now()
        normalized_tags = tuple(str(tag).strip() for tag in tags if str(tag).strip())
        tags_str = ",".join(normalized_tags)
        normalized_source_kind = source_kind.strip() or "dispatch"
        normalized_decision_ref = decision_ref.strip()
        self._validate_bug_provenance(
            discovered_in_run_id=discovered_in_run_id,
            discovered_in_receipt_id=discovered_in_receipt_id,
        )

        vec = None
        similar_bugs: list[dict] = []
        vector_query = None
        if self._vector_store is not None:
            embed_text = title + " " + description
            vector_query = self._vector_store.prepare(embed_text)
            dup_rows = vector_query.search(
                "bugs",
                select_columns=("bug_id", "title", "status", "severity"),
                limit=5,
                min_similarity=0.85,
                score_alias="similarity",
            )
            for r in dup_rows:
                similar_bugs.append({
                    "bug_id": r["bug_id"],
                    "title": r["title"],
                    "status": r["status"],
                    "severity": r["severity"],
                    "similarity": round(float(r["similarity"]), 4),
                })

        self._conn.execute(
            """INSERT INTO bugs
                (bug_id, bug_key, title, severity, status, priority, category, description,
                 summary, source_kind, discovered_in_run_id, discovered_in_receipt_id,
                 owner_ref, decision_ref, opened_at, resolved_at, created_at, updated_at,
                 filed_by, tags)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, NULL, $16, $17, $18, $19)""",
            bug_id, bug_key, title, severity.value, BugStatus.OPEN.value,
            severity.value, category.value, description,
            description[:200], normalized_source_kind, discovered_in_run_id, discovered_in_receipt_id,
            owner_ref, normalized_decision_ref, now, now, now, filed_by, tags_str,
        )
        if vector_query is not None:
            vector_query.set_embedding("bugs", "bug_id", bug_id)

        row = self._conn.fetchrow("SELECT * FROM bugs WHERE bug_id = $1", bug_id)
        return self._row_to_bug(row), similar_bugs

    def get(self, bug_id: str) -> Bug | None:
        row = self._conn.fetchrow("SELECT * FROM bugs WHERE bug_id = $1", bug_id)
        return self._row_to_bug(row) if row else None

    def update_status(self, bug_id: str, new_status: BugStatus) -> Bug | None:
        now = self._now()
        rows = self._conn.execute(
            "UPDATE bugs SET status = $1, updated_at = $2 WHERE bug_id = $3 RETURNING *",
            new_status.value, now, bug_id,
        )
        return self._row_to_bug(rows[0]) if rows else None

    def assign(self, bug_id: str, assigned_to: str) -> Bug | None:
        now = self._now()
        rows = self._conn.execute(
            "UPDATE bugs SET assigned_to = $1, updated_at = $2 WHERE bug_id = $3 RETURNING *",
            assigned_to, now, bug_id,
        )
        return self._row_to_bug(rows[0]) if rows else None

    def link_evidence(
        self,
        bug_id: str,
        *,
        evidence_kind: str,
        evidence_ref: str,
        evidence_role: str,
        created_by: str = "bug_tracker",
        notes: str | None = None,
    ) -> dict[str, Any] | None:
        bug_id = str(bug_id or "").strip()
        evidence_kind = str(evidence_kind or "").strip()
        evidence_ref = str(evidence_ref or "").strip()
        evidence_role = str(evidence_role or "").strip()
        created_by = str(created_by or "bug_tracker").strip() or "bug_tracker"
        if not bug_id or not evidence_kind or not evidence_ref or not evidence_role:
            return None
        if evidence_role not in _ALLOWED_EVIDENCE_ROLES:
            allowed = ", ".join(sorted(_ALLOWED_EVIDENCE_ROLES))
            raise ValueError(f"evidence_role must be one of {allowed}")
        if self.get(bug_id) is None:
            raise ValueError(f"bug not found: {bug_id}")
        self._validate_evidence_reference(
            evidence_kind=evidence_kind,
            evidence_ref=evidence_ref,
        )
        rows = self._query_optional_rows(
            """
            INSERT INTO bug_evidence_links (
                bug_evidence_link_id,
                bug_id,
                evidence_kind,
                evidence_ref,
                evidence_role,
                created_at,
                created_by,
                notes
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8
            )
            ON CONFLICT (bug_id, evidence_kind, evidence_ref, evidence_role)
            DO UPDATE SET
                notes = COALESCE(bug_evidence_links.notes, EXCLUDED.notes)
            RETURNING *
            """,
            f"bug_evidence_link:{uuid.uuid4().hex}",
            bug_id,
            evidence_kind,
            evidence_ref,
            evidence_role,
            self._now(),
            created_by,
            notes,
        )
        return self._row_to_evidence_link(rows[0]) if rows else None

    def list_evidence(
        self,
        bug_id: str,
        *,
        evidence_role: str | None = None,
    ) -> list[dict[str, Any]]:
        bug_id = str(bug_id or "").strip()
        if not bug_id:
            return []
        if evidence_role:
            rows = self._query_optional_rows(
                """
                SELECT *
                  FROM bug_evidence_links
                 WHERE bug_id = $1
                   AND evidence_role = $2
                 ORDER BY created_at ASC, bug_evidence_link_id ASC
                """,
                bug_id,
                evidence_role,
            )
        else:
            rows = self._query_optional_rows(
                """
                SELECT *
                  FROM bug_evidence_links
                 WHERE bug_id = $1
                 ORDER BY created_at ASC, bug_evidence_link_id ASC
                """,
                bug_id,
            )
        return [self._row_to_evidence_link(row) for row in rows]

    def _link_evidence_if_missing(
        self,
        *,
        bug_id: str,
        evidence_links: list[dict[str, Any]],
        evidence_kind: str,
        evidence_ref: str | None,
        evidence_role: str = "observed_in",
        created_by: str = "bug_tracker.auto_backfill",
        notes: str | None = None,
    ) -> dict[str, Any] | None:
        reference = str(evidence_ref or "").strip()
        if not reference:
            return None
        for link in evidence_links:
            if (
                str(link.get("evidence_kind") or "") == evidence_kind
                and str(link.get("evidence_ref") or "") == reference
                and str(link.get("evidence_role") or "") == evidence_role
            ):
                return None
        try:
            link = self.link_evidence(
                bug_id,
                evidence_kind=evidence_kind,
                evidence_ref=reference,
                evidence_role=evidence_role,
                created_by=created_by,
                notes=notes,
            )
        except ValueError:
            return None
        if link is not None:
            evidence_links.append(link)
        return link

    def _signature_expectation_from_bug(self, bug: Bug) -> dict[str, str | None]:
        return _bug_evidence.signature_expectation_from_bug(bug)

    def _receipt_matches_backfill_signature(
        self,
        *,
        receipt: dict[str, Any],
        expected: dict[str, str | None],
    ) -> bool:
        return _bug_evidence.receipt_matches_backfill_signature(
            receipt=receipt,
            expected=expected,
        )

    def _unique_signature_receipt_for_backfill(
        self,
        bug: Bug,
    ) -> tuple[dict[str, Any] | None, str | None]:
        expected = self._signature_expectation_from_bug(bug)
        failure_code = str(expected.get("failure_code") or "").strip()
        node_anchor = str(expected.get("node_id") or expected.get("job_label") or "").strip()
        if not failure_code or not node_anchor:
            return None, "bug.replay_backfill.insufficient_signature"
        receipts, error = self._find_signature_receipts(
            failure_code=failure_code,
            node_id=node_anchor,
            limit=25,
        )
        if error:
            return None, f"bug.replay_backfill.query_failed:{error}"
        matches = [
            receipt
            for receipt in receipts
            if self._receipt_matches_backfill_signature(
                receipt=receipt,
                expected=expected,
            )
        ]
        if len(matches) == 1:
            return matches[0], None
        if not matches:
            return None, "bug.replay_backfill.no_unique_match"
        return None, "bug.replay_backfill.ambiguous_match"

    def backfill_replay_provenance(self, bug_id: str) -> dict[str, Any] | None:
        bug = self.get(bug_id)
        if bug is None:
            return None
        evidence_links = self.list_evidence(bug_id)
        added_links: list[dict[str, Any]] = []

        discovered_receipt = self._link_evidence_if_missing(
            bug_id=bug_id,
            evidence_links=evidence_links,
            evidence_kind="receipt",
            evidence_ref=bug.discovered_in_receipt_id,
            notes="Auto-backfilled observed receipt provenance from bug discovery fields.",
        )
        if discovered_receipt is not None:
            added_links.append(discovered_receipt)
        discovered_run = self._link_evidence_if_missing(
            bug_id=bug_id,
            evidence_links=evidence_links,
            evidence_kind="run",
            evidence_ref=bug.discovered_in_run_id,
            notes="Auto-backfilled observed run provenance from bug discovery fields.",
        )
        if discovered_run is not None:
            added_links.append(discovered_run)

        has_observed_link = any(
            str(link.get("evidence_role") or "") == "observed_in"
            and str(link.get("evidence_kind") or "") in {"receipt", "run"}
            for link in evidence_links
        )
        reason_code = "bug.replay_backfill.authoritative_fields"
        if not has_observed_link:
            matched_receipt, match_reason = self._unique_signature_receipt_for_backfill(bug)
            reason_code = match_reason or "bug.replay_backfill.signature_match"
            if matched_receipt is not None:
                receipt_link = self._link_evidence_if_missing(
                    bug_id=bug_id,
                    evidence_links=evidence_links,
                    evidence_kind="receipt",
                    evidence_ref=str(matched_receipt.get("receipt_id") or "").strip(),
                    notes="Auto-backfilled observed receipt provenance from a unique signature match.",
                )
                if receipt_link is not None:
                    added_links.append(receipt_link)
                run_link = self._link_evidence_if_missing(
                    bug_id=bug_id,
                    evidence_links=evidence_links,
                    evidence_kind="run",
                    evidence_ref=str(matched_receipt.get("run_id") or "").strip(),
                    notes="Auto-backfilled observed run provenance from a unique signature receipt match.",
                )
                if run_link is not None:
                    added_links.append(run_link)

        return {
            "bug_id": bug_id,
            "linked_count": len(added_links),
            "linked_refs": tuple(
                {
                    "evidence_kind": str(link.get("evidence_kind") or ""),
                    "evidence_ref": str(link.get("evidence_ref") or ""),
                }
                for link in added_links
            ),
            "reason_code": reason_code,
        }

    def bulk_backfill_replay_provenance(
        self,
        *,
        limit: int | None = None,
        open_only: bool = True,
        receipt_limit: int = 1,
    ) -> dict[str, Any]:
        scan_limit = limit
        if scan_limit is None:
            scan_limit = self.count_bugs(open_only=open_only)
        scan_limit = max(0, int(scan_limit or 0))

        bugs = self.list_bugs(
            open_only=open_only,
            limit=scan_limit,
        )
        bug_results: list[dict[str, Any]] = []
        linked_total = 0
        backfilled_count = 0
        replay_ready_count = 0
        replay_blocked_count = 0

        for bug in bugs:
            backfill = self.backfill_replay_provenance(bug.bug_id) or {
                "bug_id": bug.bug_id,
                "linked_count": 0,
                "linked_refs": (),
                "reason_code": "bug.replay_backfill.missing_bug",
            }
            hint = self.replay_hint(bug.bug_id, receipt_limit=receipt_limit) or {
                "available": False,
                "reason_code": "bug.replay_not_ready",
                "run_id": None,
                "receipt_id": None,
                "automatic": False,
            }
            linked_count = int(backfill.get("linked_count") or 0)
            linked_total += linked_count
            if linked_count > 0:
                backfilled_count += 1
            replay_ready = bool(hint.get("available"))
            if replay_ready:
                replay_ready_count += 1
            else:
                replay_blocked_count += 1
            bug_results.append(
                {
                    "bug_id": bug.bug_id,
                    "linked_count": linked_count,
                    "linked_refs": tuple(backfill.get("linked_refs") or ()),
                    "backfill_reason_code": str(
                        backfill.get("reason_code") or "bug.replay_backfill.none"
                    ),
                    "replay_ready": replay_ready,
                    "replay_reason_code": str(
                        hint.get("reason_code") or "bug.replay_not_ready"
                    ),
                    "replay_run_id": hint.get("run_id"),
                    "replay_receipt_id": hint.get("receipt_id"),
                }
            )

        return {
            "scanned_count": len(bugs),
            "backfilled_count": backfilled_count,
            "linked_count": linked_total,
            "replay_ready_count": replay_ready_count,
            "replay_blocked_count": replay_blocked_count,
            "open_only": bool(open_only),
            "limit": scan_limit,
            "bugs": tuple(bug_results),
        }

    def replay_hint(
        self,
        bug_id: str,
        *,
        receipt_limit: int = 1,
    ) -> dict[str, Any] | None:
        packet = self.failure_packet(bug_id, receipt_limit=receipt_limit)
        if packet is None:
            return None
        replay = _json_object(_json_object(packet.get("agent_actions")).get("replay"))
        return {
            "available": bool(replay.get("available")),
            "reason_code": str(replay.get("reason_code") or "bug.replay_not_ready"),
            "run_id": replay.get("run_id"),
            "receipt_id": replay.get("receipt_id"),
            "automatic": bool(replay.get("automatic")),
        }

    def failure_packet(
        self,
        bug_id: str,
        *,
        receipt_limit: int = 5,
    ) -> dict[str, Any] | None:
        bug = self.get(bug_id)
        if bug is None:
            return None
        backfill = self.backfill_replay_provenance(bug.bug_id)

        evidence_links = self.list_evidence(bug_id)
        receipt_refs: set[str] = set()
        run_refs: set[str] = set()
        verification_run_refs: set[str] = set()
        healing_run_refs: set[str] = set()
        for evidence in evidence_links:
            evidence_kind = str(evidence.get("evidence_kind") or "")
            evidence_ref = str(evidence.get("evidence_ref") or "")
            if evidence_kind == "receipt":
                receipt_refs.add(evidence_ref)
            elif evidence_kind == "run":
                run_refs.add(evidence_ref)
            elif evidence_kind == "verification_run":
                verification_run_refs.add(evidence_ref)
            elif evidence_kind == "healing_run":
                healing_run_refs.add(evidence_ref)
        if bug.discovered_in_receipt_id:
            receipt_refs.add(bug.discovered_in_receipt_id)
        if bug.discovered_in_run_id:
            run_refs.add(bug.discovered_in_run_id)

        query_errors: list[str] = []
        rows: list[Any] = []
        clauses: list[str] = []
        params: list[object] = []
        idx = 1
        if receipt_refs:
            clauses.append(f"receipt_id = ANY(${idx}::text[])")
            params.append(sorted(receipt_refs))
            idx += 1
        if run_refs:
            clauses.append(f"run_id = ANY(${idx}::text[])")
            params.append(sorted(run_refs))
            idx += 1
        if clauses:
            params.append(receipt_limit)
            rows, receipt_error = self._query_rows_with_error(
                (
                    "SELECT receipt_id, workflow_id, run_id, request_id, node_id, attempt_no, started_at, finished_at, "
                    "executor_type, status, inputs, outputs, artifacts, failure_code, decision_refs "
                    f"FROM receipts WHERE {' OR '.join(clauses)} "
                    f"ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST LIMIT ${idx}"
                ),
                *params,
            )
            if receipt_error:
                query_errors.append(f"receipts.query_failed:{receipt_error}")
        explicit_receipts = [self._row_to_receipt_summary(row) for row in rows]

        failure_code = _extract_tag_value(bug.tags, "failure_code")
        node_id = _extract_tag_value(bug.tags, "node_id")
        job_label = _extract_tag_value(bug.tags, "job_label")
        fallback_receipts: list[dict[str, Any]] = []
        if not explicit_receipts:
            fallback_receipts, fallback_error = self._find_signature_receipts(
                failure_code=failure_code,
                node_id=node_id or job_label,
                limit=receipt_limit,
            )
            if fallback_error:
                query_errors.append(f"fallback_receipts.query_failed:{fallback_error}")

        latest_receipt = explicit_receipts[0] if explicit_receipts else None
        inferred_receipt = fallback_receipts[0] if fallback_receipts else None
        signature_receipt = latest_receipt or inferred_receipt
        failure_code = (
            str(signature_receipt.get("failure_code") or "").strip()
            if signature_receipt is not None
            else failure_code
        ) or None
        node_id = (
            str(signature_receipt.get("node_id") or "").strip()
            if signature_receipt is not None
            else node_id
        ) or None
        job_label = job_label or (
            _extract_tag_value(bug.tags, "job_label")
            or (
                str(signature_receipt.get("node_id") or "").strip()
                if signature_receipt is not None
                else None
            )
        )
        failure_category = (
            str(signature_receipt.get("failure_category") or "").strip()
            if signature_receipt is not None
            else _extract_tag_value(bug.tags, "failure_category")
        ) or None
        agent = (
            str(signature_receipt.get("agent") or "").strip()
            if signature_receipt is not None
            else _extract_tag_value(bug.tags, "agent")
        ) or None
        provider_slug = (
            str(signature_receipt.get("provider_slug") or "").strip()
            if signature_receipt is not None
            else _extract_tag_value(bug.tags, "provider")
        ) or None
        model_slug = (
            str(signature_receipt.get("model_slug") or "").strip()
            if signature_receipt is not None
            else _extract_tag_value(bug.tags, "model")
        ) or None
        signature = build_failure_signature(
            failure_code=failure_code,
            job_label=job_label,
            node_id=node_id,
            failure_category=failure_category,
            agent=agent,
            provider_slug=provider_slug,
            model_slug=model_slug,
            source_kind=bug.source_kind,
        )
        verification_rows, verification_error = self._load_verification_rows(
            "verification_runs",
            "verification_run_id",
            tuple(sorted(verification_run_refs)),
        )
        if verification_error:
            query_errors.append(f"verification_runs.query_failed:{verification_error}")
        healing_rows, healing_error = self._load_verification_rows(
            "healing_runs",
            "healing_run_id",
            tuple(sorted(healing_run_refs)),
        )
        if healing_error:
            query_errors.append(f"healing_runs.query_failed:{healing_error}")
        validate_fix_links = [
            evidence
            for evidence in evidence_links
            if evidence.get("evidence_role") == "validates_fix"
        ]
        attempted_fix_links = [
            evidence
            for evidence in evidence_links
            if evidence.get("evidence_role") == "attempted_fix"
        ]
        observed_links = [
            evidence
            for evidence in evidence_links
            if evidence.get("evidence_role") == "observed_in"
        ]
        verified_validation_rows = [
            verification_rows.get(str(item.get("evidence_ref") or ""))
            for item in validate_fix_links
            if _verification_passed(
                verification_rows.get(str(item.get("evidence_ref") or ""), {}).get("status")
            )
        ]
        validation_times = [
            row.get("attempted_at")
            for row in verified_validation_rows
            if isinstance(row, dict) and isinstance(row.get("attempted_at"), datetime)
        ]
        observed_receipt_ids = {
            str(item.get("evidence_ref") or "")
            for item in observed_links
            if str(item.get("evidence_kind") or "") == "receipt"
        }
        observed_run_ids = {
            str(item.get("evidence_ref") or "")
            for item in observed_links
            if str(item.get("evidence_kind") or "") == "run"
        }
        if bug.discovered_in_receipt_id:
            observed_receipt_ids.add(bug.discovered_in_receipt_id)
        if bug.discovered_in_run_id:
            observed_run_ids.add(bug.discovered_in_run_id)
        first_seen_candidates = [bug.filed_at]
        last_seen_candidates = [bug.updated_at]
        for evidence in evidence_links:
            created_at = evidence.get("created_at")
            if isinstance(created_at, datetime):
                first_seen_candidates.append(created_at)
                last_seen_candidates.append(created_at)
        for receipt in explicit_receipts or fallback_receipts:
            timestamp = receipt.get("timestamp")
            if isinstance(timestamp, datetime):
                first_seen_candidates.append(timestamp)
                last_seen_candidates.append(timestamp)
        fix_verified_at = max(validation_times) if validation_times else None
        last_seen_at = max(last_seen_candidates)
        lifecycle = {
            "first_seen_at": min(first_seen_candidates),
            "last_seen_at": last_seen_at,
            "recurrence_count": max(len(observed_links), len(observed_receipt_ids), 1),
            "impacted_run_count": len(observed_run_ids),
            "impacted_receipt_count": len(observed_receipt_ids),
            "attempted_fix_count": len(attempted_fix_links),
            "fix_validation_count": len(validate_fix_links),
            "verified_validation_count": len(verified_validation_rows),
            "fix_verified_at": fix_verified_at,
            "has_regression_after_fix": bool(
                fix_verified_at is not None
                and isinstance(last_seen_at, datetime)
                and last_seen_at > fix_verified_at
                and len(observed_links) > len(verified_validation_rows)
            ),
        }
        observability_gaps = list(
            self._build_observability_gaps(
                bug=bug,
                evidence_links=evidence_links,
                latest_receipt=latest_receipt,
                fix_validation_count=len(verified_validation_rows),
            )
        )
        if fallback_receipts and not explicit_receipts:
            observability_gaps.append("receipt.inferred_only")
        observability_gaps = list(dict.fromkeys(observability_gaps))
        initial_decision_refs: list[Any] = [bug.decision_ref]
        if latest_receipt:
            initial_decision_refs.extend(latest_receipt.get("decision_refs") or ())
        decision_refs = _ordered_unique(initial_decision_refs)
        latest_validation = None
        if validate_fix_links:
            latest_validation = max(
                (
                    verification_rows.get(str(link.get("evidence_ref") or ""))
                    for link in validate_fix_links
                ),
                key=_attempted_at_sort_key,
                default=None,
            )
        latest_healing = None
        if attempted_fix_links:
            latest_healing = max(
                (
                    healing_rows.get(str(link.get("evidence_ref") or ""))
                    for link in attempted_fix_links
                ),
                key=_attempted_at_sort_key,
                default=None,
            )
        decision_refs = _ordered_unique(
            [
                *decision_refs,
                latest_validation.get("decision_ref") if isinstance(latest_validation, dict) else None,
                latest_healing.get("decision_ref") if isinstance(latest_healing, dict) else None,
            ]
        )
        replay_context = {
            "ready": bool(
                latest_receipt
                and latest_receipt.get("run_id")
                and latest_receipt.get("receipt_id")
            ),
            "source": "evidence" if latest_receipt else ("fallback" if fallback_receipts else "missing"),
            "workflow_id": latest_receipt.get("workflow_id") if latest_receipt else None,
            "run_id": latest_receipt.get("run_id") if latest_receipt else bug.discovered_in_run_id,
            "receipt_id": latest_receipt.get("receipt_id") if latest_receipt else bug.discovered_in_receipt_id,
            "request_id": latest_receipt.get("request_id") if latest_receipt else None,
            "node_id": latest_receipt.get("node_id") if latest_receipt else None,
            "failure_code": failure_code,
            "repo_snapshot_ref": (
                _json_object(latest_receipt.get("git_provenance")).get("repo_snapshot_ref")
                if latest_receipt is not None
                else None
            ),
            "workspace_ref": (
                latest_receipt.get("inputs", {}).get("workspace_ref")
                if latest_receipt is not None
                else None
            ),
            "runtime_profile_ref": (
                latest_receipt.get("inputs", {}).get("runtime_profile_ref")
                if latest_receipt is not None
                else None
            ),
            "decision_refs": decision_refs,
            "verification_status": latest_receipt.get("verification_status") if latest_receipt else None,
        }
        historical_fixes = self._build_historical_fixes(
            bug=bug,
            signature=signature,
        )
        return _bug_evidence.assemble_failure_packet(
            bug=bug,
            bug_status_fixed=BugStatus.FIXED,
            evidence_links=evidence_links,
            explicit_receipts=explicit_receipts,
            fallback_receipts=fallback_receipts,
            verification_rows=verification_rows,
            healing_rows=healing_rows,
            verification_run_refs=verification_run_refs,
            healing_run_refs=healing_run_refs,
            query_errors=query_errors,
            signature=signature,
            failure_code=failure_code,
            node_id=node_id,
            replay_action_result=self._replay_action(
                bug_id=bug.bug_id,
                replay_context=replay_context,
            ),
            write_set_diff=self._compare_write_sets(latest_receipt),
            blast_radius=self._build_blast_radius(
                failure_code=failure_code,
                node_id=node_id,
            ),
            historical_fixes=historical_fixes,
            backfill=backfill,
        )

    def replay_bug(
        self,
        bug_id: str,
        *,
        receipt_limit: int = 5,
    ) -> dict[str, Any] | None:
        packet = self.failure_packet(bug_id, receipt_limit=receipt_limit)
        if packet is None:
            return None

        replay_context = dict(packet.get("replay_context") or {})
        replay_action = _json_object(_json_object(packet.get("agent_actions")).get("replay"))
        response = {
            "bug_id": str(bug_id),
            "packet_ready": bool(replay_context.get("ready")),
            "replay_context": replay_context,
            "packet_summary": _packet_summary(packet),
            "historical_fixes": packet.get("historical_fixes"),
            "tooling": {"replay": replay_action},
        }
        run_id = str(replay_context.get("run_id") or "").strip()
        if not replay_context.get("ready") or not run_id:
            response.update(
                {
                    "ready": False,
                    "reason_code": str(
                        replay_action.get("reason_code") or "bug.replay_not_ready"
                    ),
                    "replay": None,
                }
            )
            return response

        try:
            replay_view = self._replay_run_view(run_id)
        except Exception as exc:
            response.update(
                {
                    "ready": False,
                    "reason_code": "bug.replay_failed",
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "replay": None,
                }
            )
            return response

        response.update(
            {
                "ready": True,
                "reason_code": "bug.replay_loaded",
                "replay": replay_view,
            }
        )
        return response

    def resolve(self, bug_id: str, status: BugStatus) -> Bug | None:
        if status not in _RESOLVED_STATUSES:
            raise ValueError(
                f"resolve() status must be one of {[s.value for s in _RESOLVED_STATUSES]}, got {status.value}"
            )
        now = self._now()
        rows = self._conn.execute(
            "UPDATE bugs SET status = $1, resolved_at = $2, updated_at = $3 WHERE bug_id = $4 RETURNING *",
            status.value, now, now, bug_id,
        )
        return self._row_to_bug(rows[0]) if rows else None

    def list_bugs(
        self,
        status: BugStatus | None = None,
        severity: BugSeverity | None = None,
        category: BugCategory | None = None,
        title_like: str | None = None,
        tags: Tuple[str, ...] | None = None,
        exclude_tags: Tuple[str, ...] | None = None,
        open_only: bool = False,
        limit: int = 50,
    ) -> list[Bug]:
        where, params, next_idx = self._build_list_bugs_where_clause(
            status=status,
            severity=severity,
            category=category,
            title_like=title_like,
            tags=tags,
            exclude_tags=exclude_tags,
            open_only=open_only,
        )
        query = f"SELECT * FROM bugs{where} ORDER BY created_at DESC LIMIT ${next_idx}"
        params.append(limit)

        rows = self._conn.execute(query, *params)
        return [self._row_to_bug(r) for r in rows]

    def count_bugs(
        self,
        status: BugStatus | None = None,
        severity: BugSeverity | None = None,
        category: BugCategory | None = None,
        title_like: str | None = None,
        tags: Tuple[str, ...] | None = None,
        exclude_tags: Tuple[str, ...] | None = None,
        open_only: bool = False,
    ) -> int:
        where, params, _next_idx = self._build_list_bugs_where_clause(
            status=status,
            severity=severity,
            category=category,
            title_like=title_like,
            tags=tags,
            exclude_tags=exclude_tags,
            open_only=open_only,
        )
        return int(self._conn.fetchval(f"SELECT COUNT(*) FROM bugs{where}", *params) or 0)

    def _build_list_bugs_where_clause(
        self,
        *,
        status: BugStatus | None,
        severity: BugSeverity | None,
        category: BugCategory | None,
        title_like: str | None,
        tags: Tuple[str, ...] | None,
        exclude_tags: Tuple[str, ...] | None,
        open_only: bool,
    ) -> tuple[str, list[object], int]:
        clauses: list[str] = []
        params: list[object] = []
        idx = 1
        if status is not None:
            status_values = self._status_filter_values(status)
            placeholders = ", ".join(f"${idx + i}" for i in range(len(status_values)))
            clauses.append(f"UPPER(status) IN ({placeholders})")
            params.extend(status_values)
            idx += len(status_values)
        elif open_only:
            excluded_status_values = [
                BugStatus.FIXED.value,
                BugStatus.WONT_FIX.value,
                BugStatus.DEFERRED.value,
                "RESOLVED",
                "DONE",
                "CLOSED",
            ]
            placeholders = ", ".join(f"${idx + i}" for i in range(len(excluded_status_values)))
            clauses.append(
                f"UPPER(status) NOT IN ({placeholders})"
            )
            params.extend(excluded_status_values)
            idx += len(excluded_status_values)
        if severity is not None:
            severity_values = self._severity_filter_values(severity)
            placeholders = ", ".join(f"${idx + i}" for i in range(len(severity_values)))
            clauses.append(f"UPPER(severity) IN ({placeholders})")
            params.extend(severity_values)
            idx += len(severity_values)
        if category is not None:
            clauses.append(f"UPPER(category) = ${idx}")
            params.append(category.value)
            idx += 1
        if title_like is not None:
            title_pattern = f"%{title_like.strip()}%"
            clauses.append(
                f"(title ILIKE ${idx} OR description ILIKE ${idx} OR summary ILIKE ${idx})"
            )
            params.append(title_pattern)
            idx += 1
        if tags:
            for raw_tag in tags:
                tag = str(raw_tag).strip().lower()
                if not tag:
                    continue
                clauses.append(
                    f"(LOWER(',' || COALESCE(tags, '') || ',') LIKE ${idx})"
                )
                params.append(f"%,{tag},%")
                idx += 1
        if exclude_tags:
            for raw_tag in exclude_tags:
                tag = str(raw_tag).strip().lower()
                if not tag:
                    continue
                clauses.append(
                    f"NOT (LOWER(',' || COALESCE(tags, '') || ',') LIKE ${idx})"
                )
                params.append(f"%,{tag},%")
                idx += 1

        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        return where, params, idx

    def search(self, query: str, limit: int = 20) -> list[Bug]:
        if self._vector_store is None:
            rows = self._conn.execute(
                "SELECT * FROM bugs WHERE search_vector @@ plainto_tsquery('english', $1) LIMIT $2",
                query, limit,
            )
            return [self._row_to_bug(r) for r in rows]

        vector_query = self._vector_store.prepare(query)

        fts_rows = self._conn.execute(
            """SELECT bug_id,
                      row_number() OVER (ORDER BY ts_rank(search_vector, plainto_tsquery('english', $1)) DESC) AS rank
               FROM bugs
               WHERE search_vector @@ plainto_tsquery('english', $1)
               LIMIT $2""",
            query, limit * 2,
        )
        vec_rows = vector_query.search(
            "bugs",
            select_columns=("bug_id",),
            limit=limit * 2,
            min_similarity=None,
            score_alias="similarity",
        )

        K = 60
        scores: dict[str, float] = {}
        for r in fts_rows:
            bid = r["bug_id"]
            scores[bid] = scores.get(bid, 0.0) + 1.0 / (K + int(r["rank"]))
        for i, r in enumerate(vec_rows, start=1):
            bid = r["bug_id"]
            scores[bid] = scores.get(bid, 0.0) + 1.0 / (K + i)

        top_ids = sorted(scores, key=lambda x: scores[x], reverse=True)[:limit]
        if not top_ids:
            return []

        placeholders = ", ".join(f"${i+1}" for i in range(len(top_ids)))
        rows = self._conn.execute(
            f"SELECT * FROM bugs WHERE bug_id IN ({placeholders})",
            *top_ids,
        )
        id_order = {bid: i for i, bid in enumerate(top_ids)}
        rows_sorted = sorted(rows, key=lambda r: id_order.get(r["bug_id"], 999))
        return [self._row_to_bug(r) for r in rows_sorted]

    def stats(self) -> BugStats:
        total = self._conn.fetchval("SELECT COUNT(*) FROM bugs") or 0

        by_status: dict[str, int] = {}
        for row in self._conn.execute(
            "SELECT status, COUNT(*) as cnt FROM bugs GROUP BY status"
        ):
            normalized = self._normalize_status(row["status"]).value
            by_status[normalized] = by_status.get(normalized, 0) + int(row["cnt"])

        by_severity: dict[str, int] = {}
        for row in self._conn.execute(
            "SELECT severity, COUNT(*) as cnt FROM bugs GROUP BY severity"
        ):
            normalized = self._normalize_severity(row["severity"], default=BugSeverity.P2)
            by_severity[normalized.value] = by_severity.get(normalized.value, 0) + int(row["cnt"])

        by_category: dict[str, int] = {}
        for row in self._conn.execute(
            "SELECT category, COUNT(*) as cnt FROM bugs GROUP BY category"
        ):
            normalized = self._normalize_category(row.get("category")).value
            by_category[normalized] = by_category.get(normalized, 0) + int(row["cnt"])

        open_count = by_status.get(BugStatus.OPEN.value, 0) + by_status.get(
            BugStatus.IN_PROGRESS.value, 0
        )

        resolved_rows = self._conn.execute(
            "SELECT opened_at, resolved_at FROM bugs WHERE resolved_at IS NOT NULL"
        )

        mttr_hours: float | None = None
        if resolved_rows:
            total_hours = 0.0
            for r in resolved_rows:
                filed = r["opened_at"] if isinstance(r["opened_at"], datetime) else datetime.fromisoformat(r["opened_at"])
                resolved = r["resolved_at"] if isinstance(r["resolved_at"], datetime) else datetime.fromisoformat(r["resolved_at"])
                total_hours += (resolved - filed).total_seconds() / 3600.0
            mttr_hours = total_hours / len(resolved_rows)

        stats_errors: list[str] = []
        packet_ready_count, packet_ready_error = self._query_scalar_with_error(
            """
            SELECT COUNT(*)
              FROM bugs AS b
             WHERE b.discovered_in_run_id IS NOT NULL
                OR b.discovered_in_receipt_id IS NOT NULL
                OR EXISTS (
                    SELECT 1
                      FROM bug_evidence_links AS bel
                     WHERE bel.bug_id = b.bug_id
                       AND bel.evidence_role = 'observed_in'
                       AND bel.evidence_kind IN ('receipt', 'run')
                )
            """
        )
        if packet_ready_error:
            stats_errors.append(f"packet_ready_count.query_failed:{packet_ready_error}")
        fix_verified_count, fix_verified_error = self._query_scalar_with_error(
            """
            SELECT COUNT(DISTINCT bel.bug_id)
              FROM bug_evidence_links AS bel
              JOIN verification_runs AS vr
                ON vr.verification_run_id = bel.evidence_ref
             WHERE bel.evidence_role = 'validates_fix'
               AND bel.evidence_kind = 'verification_run'
               AND vr.status = 'passed'
            """
        )
        if fix_verified_error:
            stats_errors.append(f"fix_verified_count.query_failed:{fix_verified_error}")
        underlinked_count, underlinked_error = self._query_scalar_with_error(
            """
            SELECT COUNT(*)
              FROM bugs AS b
             WHERE b.discovered_in_run_id IS NULL
               AND b.discovered_in_receipt_id IS NULL
               AND NOT EXISTS (
                    SELECT 1
                      FROM bug_evidence_links AS bel
                     WHERE bel.bug_id = b.bug_id
               )
            """
        )
        if underlinked_error:
            stats_errors.append(f"underlinked_count.query_failed:{underlinked_error}")

        return BugStats(
            total=total,
            by_status=by_status,
            by_severity=by_severity,
            by_category=by_category,
            open_count=open_count,
            mttr_hours=mttr_hours,
            packet_ready_count=packet_ready_count,
            replay_ready_count=packet_ready_count,
            replay_blocked_count=(
                None
                if packet_ready_count is None
                else max(open_count - int(packet_ready_count), 0)
            ),
            fix_verified_count=fix_verified_count,
            underlinked_count=underlinked_count,
            observability_state="degraded" if stats_errors else "complete",
            errors=tuple(stats_errors),
        )
