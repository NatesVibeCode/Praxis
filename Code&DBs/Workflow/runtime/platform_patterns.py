"""Durable platform pattern authority.

Patterns are the middle layer between raw evidence and bugs. They answer:
"what recurring system shape keeps becoming possible?" Bugs still own concrete
fix work; patterns own recurrence, evidence links, promotion rules, and
intervention state.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import json
from typing import Any, Iterable, Mapping, Sequence

from runtime.idempotency import canonical_hash


PATTERN_IDENTITY_PURPOSE = "platform_pattern.identity"
PATTERN_IDENTITY_ALGORITHM = "sha256"
PATTERN_IDENTITY_CANONICALIZATION = "platform_pattern_identity_v1"
PATTERN_HYDRATION_PURPOSE = "platform_pattern.hydration"
PATTERN_HYDRATION_ALGORITHM = "sha256"
PATTERN_HYDRATION_CANONICALIZATION = "platform_pattern_hydration_v1"
PATTERN_HYDRATION_CONTRACT = (
    "retrieval_suggests_semantics_hydrates_primitives_cqrs_writes_authority"
)
PATTERN_POLICY_DECISION_REF = (
    "operator_decision.architecture_policy.pattern_authority."
    "failure_patterns_between_evidence_and_bugs"
)

PATTERN_KINDS = frozenset(
    {
        "architecture_smell",
        "runtime_failure_pattern",
        "operator_friction",
        "missing_authority",
        "weak_observability",
    }
)
PATTERN_STATUSES = frozenset(
    {"observing", "confirmed", "intervention_planned", "mitigated", "rejected"}
)
PATTERN_SEVERITIES = frozenset({"P0", "P1", "P2", "P3"})


@dataclass(frozen=True, slots=True)
class PatternEvidence:
    evidence_kind: str
    evidence_ref: str
    evidence_role: str = "observed_in"
    observed_at: datetime | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "evidence_kind": self.evidence_kind,
            "evidence_ref": self.evidence_ref,
            "evidence_role": self.evidence_role,
            "observed_at": _iso(self.observed_at),
            "details": dict(self.details),
        }


@dataclass(frozen=True, slots=True)
class PatternCandidate:
    pattern_key: str
    pattern_kind: str
    title: str
    failure_mode: str
    evidence_count: int
    first_seen_at: datetime | None
    last_seen_at: datetime | None
    promotion_candidate: bool
    promotion_rule: Mapping[str, Any]
    evidence: tuple[PatternEvidence, ...] = ()
    severity: str = "P2"
    status: str = "observing"
    owner_surface: str = "praxis_patterns"
    verifier_ref: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)

    @property
    def identity_digest(self) -> str:
        return pattern_identity_digest(self.pattern_key)

    @property
    def pattern_ref(self) -> str:
        return pattern_ref_for_key(self.pattern_key)

    @property
    def hydration_digest(self) -> str:
        return pattern_hydration_digest(self)

    def to_json(self, *, include_hydration: bool = False) -> dict[str, Any]:
        payload = {
            "pattern_ref": self.pattern_ref,
            "pattern_key": self.pattern_key,
            "identity_digest": self.identity_digest,
            "identity_digest_purpose": PATTERN_IDENTITY_PURPOSE,
            "identity_digest_algorithm": PATTERN_IDENTITY_ALGORITHM,
            "identity_digest_canonicalization": PATTERN_IDENTITY_CANONICALIZATION,
            "pattern_kind": self.pattern_kind,
            "title": self.title,
            "failure_mode": self.failure_mode,
            "evidence_count": self.evidence_count,
            "first_seen_at": _iso(self.first_seen_at),
            "last_seen_at": _iso(self.last_seen_at),
            "promotion_candidate": self.promotion_candidate,
            "promotion_rule": dict(self.promotion_rule),
            "severity": self.severity,
            "status": self.status,
            "owner_surface": self.owner_surface,
            "verifier_ref": self.verifier_ref,
            "metadata": dict(self.metadata),
            "evidence": [item.to_json() for item in self.evidence],
        }
        if include_hydration:
            payload["hydration"] = pattern_hydration_manifest(self)
        return payload


def pattern_identity_digest(pattern_key: str) -> str:
    """Return the canonical identity digest for one pattern key.

    This intentionally records purpose and canonicalization in the hashed
    material. The DB stores the same metadata as columns so a future crypto
    registry can verify or migrate the digest without treating it as a mystery
    hex string.
    """

    return canonical_hash(
        {
            "purpose": PATTERN_IDENTITY_PURPOSE,
            "algorithm": PATTERN_IDENTITY_ALGORITHM,
            "canonicalization": PATTERN_IDENTITY_CANONICALIZATION,
            "pattern_key": _require_text(pattern_key, field_name="pattern_key"),
        }
    )


def pattern_ref_for_key(pattern_key: str) -> str:
    return f"PATTERN-{pattern_identity_digest(pattern_key)[:10].upper()}"


def pattern_hydration_digest(pattern: PatternCandidate | Mapping[str, Any]) -> str:
    """Return the canonical digest for one pattern hydration manifest."""

    base = _pattern_hydration_base(pattern)
    return canonical_hash(
        {
            "purpose": PATTERN_HYDRATION_PURPOSE,
            "algorithm": PATTERN_HYDRATION_ALGORITHM,
            "canonicalization": PATTERN_HYDRATION_CANONICALIZATION,
            "contract": PATTERN_HYDRATION_CONTRACT,
            **base,
        }
    )


def pattern_hydration_manifest(
    pattern: PatternCandidate | Mapping[str, Any],
    *,
    materialized: bool = False,
) -> dict[str, Any]:
    """Hydrate a pattern into semantic and primitive-action candidates."""

    base = _pattern_hydration_base(pattern)
    evidence_refs = _pattern_hydration_evidence_refs(pattern)
    retrieval_sources = _pattern_retrieval_sources(
        pattern,
        evidence_refs=evidence_refs,
    )
    semantic_bindings = _pattern_semantic_binding_suggestions(
        base,
        evidence_refs=evidence_refs,
    )
    typed_gaps = _pattern_typed_gaps(
        base=base,
        materialized=materialized,
        semantic_bindings=semantic_bindings,
    )
    primitive_hydration = _pattern_primitive_hydration(
        pattern,
        base=base,
        materialized=materialized,
        typed_gaps=typed_gaps,
    )
    return {
        "hydration_digest": pattern_hydration_digest(pattern),
        "hydration_digest_purpose": PATTERN_HYDRATION_PURPOSE,
        "hydration_digest_algorithm": PATTERN_HYDRATION_ALGORITHM,
        "hydration_digest_canonicalization": PATTERN_HYDRATION_CANONICALIZATION,
        "manifest_contract": PATTERN_HYDRATION_CONTRACT,
        "record_state": "materialized" if materialized else "candidate",
        "authority_model": {
            "immutable_truth": [
                "authority_events",
                "authority_operation_receipts",
                "semantic_assertions",
                "platform_pattern_evidence_links",
            ],
            "projection_indexes": [
                "fts",
                "vector",
                "graph",
                "semantic_current_assertions",
            ],
            "boundary": (
                "retrieval ranks candidates; hydration proposes typed objects; "
                "CQRS commands write authority"
            ),
        },
        "retrieval_plane": retrieval_sources,
        "semantic_binding_suggestions": semantic_bindings,
        "primitive_hydration": primitive_hydration,
        "typed_gaps": typed_gaps,
        "repair_actions": _pattern_repair_actions(typed_gaps, base=base),
    }


class PlatformPatternAuthority:
    """DB-backed authority over recurring platform patterns."""

    def __init__(self, conn: Any, *, friction_ledger: Any | None = None) -> None:
        self._conn = conn
        self._friction_ledger = friction_ledger

    def candidate_bundle(
        self,
        *,
        sources: Sequence[str] | None = None,
        limit: int = 20,
        threshold: int = 3,
        since_hours: float | None = None,
        include_test: bool = False,
        include_hydration: bool = False,
    ) -> dict[str, Any]:
        candidates, errors = self.derive_candidates(
            sources=sources,
            limit=limit,
            threshold=threshold,
            since_hours=since_hours,
            include_test=include_test,
        )
        return {
            "ok": not errors,
            "count": len(candidates),
            "threshold": threshold,
            "sources": list(_normalize_sources(sources)),
            "view": "pattern_candidates_hydrated" if include_hydration else "pattern_candidates",
            "candidates": [
                candidate.to_json(include_hydration=include_hydration)
                for candidate in candidates
            ],
            "errors": errors,
        }

    def derive_candidates(
        self,
        *,
        sources: Sequence[str] | None = None,
        limit: int = 20,
        threshold: int = 3,
        since_hours: float | None = None,
        include_test: bool = False,
    ) -> tuple[list[PatternCandidate], list[dict[str, Any]]]:
        normalized_sources = _normalize_sources(sources)
        normalized_limit = _bounded_int(limit, default=20, minimum=1, maximum=200)
        normalized_threshold = _bounded_int(threshold, default=3, minimum=1, maximum=100)
        since = _since(since_hours)
        candidates: list[PatternCandidate] = []
        errors: list[dict[str, Any]] = []

        def collect(source: str, loader) -> None:
            try:
                candidates.extend(loader())
            except Exception as exc:  # noqa: BLE001 - one evidence source should not blank all pattern truth
                errors.append(
                    {
                        "source": source,
                        "reason_code": f"platform_patterns.{source}.query_failed",
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )

        if "friction" in normalized_sources:
            collect(
                "friction",
                lambda: self._friction_candidates(
                    limit=max(normalized_limit, 50),
                    threshold=normalized_threshold,
                    since=since,
                    include_test=include_test,
                ),
            )
        if "bugs" in normalized_sources:
            collect(
                "bugs",
                lambda: self._bug_candidates(
                    limit=max(normalized_limit * 5, 100),
                    threshold=normalized_threshold,
                    since=since,
                ),
            )
        if "receipts" in normalized_sources:
            collect(
                "receipts",
                lambda: self._receipt_candidates(
                    limit=max(normalized_limit * 10, 250),
                    threshold=normalized_threshold,
                    since=since,
                ),
            )

        merged = _merge_candidates(candidates, threshold=normalized_threshold)
        merged.sort(
            key=lambda item: (
                item.promotion_candidate,
                item.evidence_count,
                item.last_seen_at or datetime.min.replace(tzinfo=timezone.utc),
            ),
            reverse=True,
        )
        return merged[:normalized_limit], errors

    def list_patterns(
        self,
        *,
        pattern_kind: str | None = None,
        status: str | None = None,
        limit: int = 50,
        include_evidence: bool = False,
        include_hydration: bool = False,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = []
        index = 1
        if pattern_kind:
            clauses.append(f"pattern_kind = ${index}")
            params.append(_normalize_pattern_kind(pattern_kind))
            index += 1
        if status:
            clauses.append(f"status = ${index}")
            params.append(_normalize_status(status))
            index += 1
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(_bounded_int(limit, default=50, minimum=1, maximum=500))
        rows = self._conn.execute(
            f"""
            SELECT pattern_ref,
                   pattern_key,
                   identity_digest,
                   identity_digest_purpose,
                   identity_digest_algorithm,
                   identity_digest_canonicalization,
                   pattern_kind,
                   title,
                   failure_mode,
                   status,
                   severity,
                   promotion_rule,
                   owner_surface,
                   verifier_ref,
                   decision_ref,
                   first_seen_at,
                   last_seen_at,
                   evidence_count,
                   metadata,
                   created_at,
                   updated_at
              FROM platform_patterns
            {where}
             ORDER BY last_seen_at DESC NULLS LAST, updated_at DESC
             LIMIT ${index}
            """,
            *params,
        )
        patterns = [_pattern_row_to_json(row) for row in rows or []]
        if include_evidence:
            for pattern in patterns:
                pattern["evidence"] = self.list_evidence(
                    str(pattern["pattern_ref"]),
                    limit=100,
                )
        if include_hydration:
            for pattern in patterns:
                pattern["hydration"] = pattern_hydration_manifest(
                    pattern,
                    materialized=True,
                )
        return patterns

    def list_evidence(self, pattern_ref: str, *, limit: int = 100) -> list[dict[str, Any]]:
        normalized_ref = _require_text(pattern_ref, field_name="pattern_ref")
        rows = self._conn.execute(
            """
            SELECT pattern_evidence_link_id,
                   pattern_ref,
                   evidence_kind,
                   evidence_ref,
                   evidence_role,
                   observed_at,
                   details,
                   created_at,
                   created_by
              FROM platform_pattern_evidence_links
             WHERE pattern_ref = $1
             ORDER BY COALESCE(observed_at, created_at) DESC
             LIMIT $2
            """,
            normalized_ref,
            _bounded_int(limit, default=100, minimum=1, maximum=1000),
        )
        return [_evidence_row_to_json(row) for row in rows or []]

    def materialize_candidates(
        self,
        *,
        sources: Sequence[str] | None = None,
        limit: int = 20,
        threshold: int = 3,
        since_hours: float | None = None,
        include_test: bool = False,
        candidate_keys: Sequence[str] | None = None,
        promotion_only: bool = True,
        status: str = "confirmed",
        created_by: str = "praxis_patterns",
    ) -> dict[str, Any]:
        candidates, errors = self.derive_candidates(
            sources=sources,
            limit=limit,
            threshold=threshold,
            since_hours=since_hours,
            include_test=include_test,
        )
        key_filter = {
            _require_text(key, field_name="candidate_key")
            for key in (candidate_keys or ())
        }
        target_status = _normalize_status(status)
        materialized: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for candidate in candidates:
            if key_filter and candidate.pattern_key not in key_filter:
                skipped.append({"pattern_key": candidate.pattern_key, "reason": "not_requested"})
                continue
            if promotion_only and not candidate.promotion_candidate:
                skipped.append({"pattern_key": candidate.pattern_key, "reason": "below_threshold"})
                continue
            materialized.append(
                self._upsert_candidate(
                    candidate,
                    status=target_status,
                    created_by=created_by,
                )
            )
        return {
            "ok": not errors,
            "status": "completed",
            "materialized_count": len(materialized),
            "skipped_count": len(skipped),
            "candidates_scanned": len(candidates),
            "threshold": threshold,
            "patterns": materialized,
            "skipped": skipped,
            "errors": errors,
            "event_payload": {
                "materialized_count": len(materialized),
                "skipped_count": len(skipped),
                "sources": list(_normalize_sources(sources)),
                "threshold": threshold,
                "pattern_refs": [str(item.get("pattern_ref") or "") for item in materialized],
            },
        }

    def _upsert_candidate(
        self,
        candidate: PatternCandidate,
        *,
        status: str,
        created_by: str,
    ) -> dict[str, Any]:
        row = self._conn.fetchrow(
            """
            INSERT INTO platform_patterns (
                pattern_ref,
                pattern_key,
                identity_digest,
                identity_digest_purpose,
                identity_digest_algorithm,
                identity_digest_canonicalization,
                pattern_kind,
                title,
                failure_mode,
                status,
                severity,
                promotion_rule,
                owner_surface,
                verifier_ref,
                decision_ref,
                first_seen_at,
                last_seen_at,
                evidence_count,
                metadata
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                $11, $12::jsonb, $13, $14, $15, $16, $17, $18, $19::jsonb
            )
            ON CONFLICT (pattern_key) DO UPDATE SET
                identity_digest = EXCLUDED.identity_digest,
                identity_digest_purpose = EXCLUDED.identity_digest_purpose,
                identity_digest_algorithm = EXCLUDED.identity_digest_algorithm,
                identity_digest_canonicalization = EXCLUDED.identity_digest_canonicalization,
                pattern_kind = EXCLUDED.pattern_kind,
                title = EXCLUDED.title,
                failure_mode = EXCLUDED.failure_mode,
                status = EXCLUDED.status,
                severity = EXCLUDED.severity,
                promotion_rule = EXCLUDED.promotion_rule,
                owner_surface = EXCLUDED.owner_surface,
                verifier_ref = EXCLUDED.verifier_ref,
                decision_ref = EXCLUDED.decision_ref,
                first_seen_at = LEAST(
                    COALESCE(platform_patterns.first_seen_at, EXCLUDED.first_seen_at),
                    COALESCE(EXCLUDED.first_seen_at, platform_patterns.first_seen_at)
                ),
                last_seen_at = GREATEST(
                    COALESCE(platform_patterns.last_seen_at, EXCLUDED.last_seen_at),
                    COALESCE(EXCLUDED.last_seen_at, platform_patterns.last_seen_at)
                ),
                evidence_count = GREATEST(platform_patterns.evidence_count, EXCLUDED.evidence_count),
                metadata = EXCLUDED.metadata,
                updated_at = now()
            RETURNING *
            """,
            candidate.pattern_ref,
            candidate.pattern_key,
            candidate.identity_digest,
            PATTERN_IDENTITY_PURPOSE,
            PATTERN_IDENTITY_ALGORITHM,
            PATTERN_IDENTITY_CANONICALIZATION,
            _normalize_pattern_kind(candidate.pattern_kind),
            _require_text(candidate.title, field_name="title"),
            _require_text(candidate.failure_mode, field_name="failure_mode"),
            status,
            _normalize_severity(candidate.severity),
            _json_dumps(dict(candidate.promotion_rule)),
            _require_text(candidate.owner_surface, field_name="owner_surface"),
            candidate.verifier_ref,
            PATTERN_POLICY_DECISION_REF,
            candidate.first_seen_at,
            candidate.last_seen_at,
            int(candidate.evidence_count),
            _json_dumps(dict(candidate.metadata)),
        )
        pattern_ref = str(_row_dict(row).get("pattern_ref") or candidate.pattern_ref)
        for evidence in candidate.evidence:
            self._link_evidence(pattern_ref, evidence, created_by=created_by)
        refreshed = self._refresh_evidence_rollup(pattern_ref)
        return _pattern_row_to_json(refreshed or row)

    def _link_evidence(
        self,
        pattern_ref: str,
        evidence: PatternEvidence,
        *,
        created_by: str,
    ) -> None:
        link_id = _evidence_link_id(pattern_ref, evidence)
        self._conn.execute(
            """
            INSERT INTO platform_pattern_evidence_links (
                pattern_evidence_link_id,
                pattern_ref,
                evidence_kind,
                evidence_ref,
                evidence_role,
                observed_at,
                details,
                created_by
            ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8)
            ON CONFLICT (pattern_ref, evidence_kind, evidence_ref, evidence_role) DO UPDATE SET
                observed_at = COALESCE(EXCLUDED.observed_at, platform_pattern_evidence_links.observed_at),
                details = EXCLUDED.details,
                created_by = EXCLUDED.created_by
            """,
            link_id,
            pattern_ref,
            _require_text(evidence.evidence_kind, field_name="evidence_kind"),
            _require_text(evidence.evidence_ref, field_name="evidence_ref"),
            _require_text(evidence.evidence_role, field_name="evidence_role"),
            evidence.observed_at,
            _json_dumps(dict(evidence.details)),
            _require_text(created_by, field_name="created_by"),
        )

    def _refresh_evidence_rollup(self, pattern_ref: str) -> Any:
        return self._conn.fetchrow(
            """
            UPDATE platform_patterns
               SET evidence_count = (
                       SELECT COUNT(*)
                         FROM platform_pattern_evidence_links
                        WHERE pattern_ref = $1
                   ),
                   first_seen_at = COALESCE(
                       (
                           SELECT MIN(COALESCE(observed_at, created_at))
                             FROM platform_pattern_evidence_links
                            WHERE pattern_ref = $1
                       ),
                       first_seen_at
                   ),
                   last_seen_at = COALESCE(
                       (
                           SELECT MAX(COALESCE(observed_at, created_at))
                             FROM platform_pattern_evidence_links
                            WHERE pattern_ref = $1
                       ),
                       last_seen_at
                   )
             WHERE pattern_ref = $1
             RETURNING *
            """,
            pattern_ref,
        )

    def _friction_candidates(
        self,
        *,
        limit: int,
        threshold: int,
        since: datetime | None,
        include_test: bool,
    ) -> list[PatternCandidate]:
        ledger = self._friction_ledger
        if ledger is None:
            from runtime.friction_ledger import FrictionLedger

            ledger = FrictionLedger(self._conn)
        patterns = ledger.patterns(
            since=since,
            limit=limit,
            scan_limit=max(limit * 5, 500),
            include_test=include_test,
            promotion_threshold=threshold,
        )
        candidates: list[PatternCandidate] = []
        for pattern in patterns:
            reason_code = _clean_token(getattr(pattern, "reason_code", "friction"))
            source_key = "failure_code" if "receipt_store" in getattr(pattern, "sources", ()) else "friction"
            pattern_key = (
                f"failure_code:{reason_code}"
                if source_key == "failure_code"
                else f"friction:{getattr(pattern, 'fingerprint')}"
            )
            evidence = tuple(
                PatternEvidence(
                    evidence_kind="friction_event",
                    evidence_ref=str(event_id),
                    evidence_role="observed_in",
                    observed_at=getattr(pattern, "last_seen", None),
                    details={
                        "source": "friction",
                        "reason_code": reason_code,
                        "command": getattr(pattern, "command", ""),
                    },
                )
                for event_id in tuple(getattr(pattern, "event_ids", ()) or ())
            )
            candidates.append(
                PatternCandidate(
                    pattern_key=pattern_key,
                    pattern_kind=_kind_for_friction(reason_code, getattr(pattern, "sources", ())),
                    title=(
                        f"Recurring failure code: {reason_code}"
                        if source_key == "failure_code"
                        else f"Recurring friction: {reason_code}"
                    ),
                    failure_mode=str(getattr(pattern, "sample", "") or reason_code)[:1200],
                    evidence_count=int(getattr(pattern, "count", 0) or 0),
                    first_seen_at=getattr(pattern, "first_seen", None),
                    last_seen_at=getattr(pattern, "last_seen", None),
                    promotion_candidate=bool(getattr(pattern, "promotion_candidate", False)),
                    promotion_rule={
                        "source": "friction_events",
                        "threshold": threshold,
                        "promotion": "file_or_dedupe_bug_when_fixable",
                    },
                    evidence=evidence,
                    metadata={
                        "sources": list(getattr(pattern, "sources", ()) or ()),
                        "job_labels": list(getattr(pattern, "job_labels", ()) or ()),
                    },
                )
            )
        return candidates

    def _bug_candidates(
        self,
        *,
        limit: int,
        threshold: int,
        since: datetime | None,
    ) -> list[PatternCandidate]:
        clauses = []
        params: list[Any] = []
        if since is not None:
            clauses.append("updated_at >= $1")
            params.append(since)
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        params.append(limit)
        rows = self._conn.execute(
            f"""
            SELECT bug_id,
                   title,
                   status,
                   severity,
                   category,
                   tags,
                   source_kind,
                   source_issue_id,
                   summary,
                   COALESCE(description, summary) AS description,
                   opened_at,
                   created_at,
                   updated_at
              FROM bugs
            {where}
             ORDER BY updated_at DESC NULLS LAST
             LIMIT ${len(params)}
            """,
            *params,
        )
        groups: dict[str, dict[str, Any]] = {}
        for raw in rows or []:
            row = _row_dict(raw)
            tags = _split_tags(row.get("tags"))
            failure_code = _tag_value(tags, "failure_code")
            if failure_code:
                key = f"failure_code:{_clean_token(failure_code)}"
                title = f"Recurring failure code: {_clean_token(failure_code)}"
                kind = _kind_for_bug(row)
            else:
                source_issue_id = str(row.get("source_issue_id") or "").strip()
                if source_issue_id.startswith("cli.workflow-friction:"):
                    key = f"friction:{source_issue_id.rsplit(':', 1)[-1]}"
                    title = "Recurring CLI workflow friction"
                    kind = "operator_friction"
                else:
                    continue
            observed_at = _coerce_datetime(row.get("updated_at")) or _coerce_datetime(row.get("created_at"))
            group = groups.setdefault(
                key,
                {
                    "pattern_key": key,
                    "pattern_kind": kind,
                    "title": title,
                    "failure_mode": str(row.get("description") or row.get("summary") or row.get("title") or "")[:1200],
                    "severity": _normalize_severity(row.get("severity") or "P2"),
                    "first_seen_at": observed_at,
                    "last_seen_at": observed_at,
                    "evidence": [],
                    "categories": set(),
                    "statuses": set(),
                },
            )
            group["evidence"].append(
                PatternEvidence(
                    evidence_kind="bug",
                    evidence_ref=str(row.get("bug_id") or ""),
                    evidence_role="symptom_bug",
                    observed_at=observed_at,
                    details={
                        "source": "bugs",
                        "status": str(row.get("status") or ""),
                        "severity": str(row.get("severity") or ""),
                        "category": str(row.get("category") or ""),
                    },
                )
            )
            group["categories"].add(str(row.get("category") or "OTHER"))
            group["statuses"].add(str(row.get("status") or "OPEN"))
            group["first_seen_at"] = _min_datetime(group["first_seen_at"], observed_at)
            group["last_seen_at"] = _max_datetime(group["last_seen_at"], observed_at)
        return [
            PatternCandidate(
                pattern_key=str(group["pattern_key"]),
                pattern_kind=str(group["pattern_kind"]),
                title=str(group["title"]),
                failure_mode=str(group["failure_mode"]),
                evidence_count=len(group["evidence"]),
                first_seen_at=group["first_seen_at"],
                last_seen_at=group["last_seen_at"],
                promotion_candidate=len(group["evidence"]) >= threshold,
                promotion_rule={
                    "source": "bugs",
                    "threshold": threshold,
                    "promotion": "dedupe_bug_cluster_or_create_roadmap_intervention",
                },
                evidence=tuple(group["evidence"]),
                severity=str(group["severity"]),
                metadata={
                    "bug_categories": sorted(group["categories"]),
                    "bug_statuses": sorted(group["statuses"]),
                },
            )
            for group in groups.values()
        ]

    def _receipt_candidates(
        self,
        *,
        limit: int,
        threshold: int,
        since: datetime | None,
    ) -> list[PatternCandidate]:
        clauses = ["lower(status) IN ('failed', 'error')", "COALESCE(failure_code, '') <> ''"]
        params: list[Any] = []
        if since is not None:
            params.append(since)
            clauses.append(f"COALESCE(finished_at, started_at) >= ${len(params)}")
        params.append(limit)
        rows = self._conn.execute(
            f"""
            SELECT receipt_id,
                   run_id,
                   node_id,
                   status,
                   failure_code,
                   started_at,
                   finished_at,
                   executor_type,
                   inputs,
                   outputs
              FROM receipts
             WHERE {' AND '.join(clauses)}
             ORDER BY COALESCE(finished_at, started_at) DESC NULLS LAST
             LIMIT ${len(params)}
            """,
            *params,
        )
        groups: dict[str, dict[str, Any]] = {}
        for raw in rows or []:
            row = _row_dict(raw)
            failure_code = _clean_token(row.get("failure_code") or "unknown")
            key = f"failure_code:{failure_code}"
            observed_at = _coerce_datetime(row.get("finished_at")) or _coerce_datetime(row.get("started_at"))
            group = groups.setdefault(
                key,
                {
                    "pattern_key": key,
                    "title": f"Recurring failure code: {failure_code}",
                    "failure_mode": f"Receipts repeatedly fail with failure_code={failure_code}.",
                    "first_seen_at": observed_at,
                    "last_seen_at": observed_at,
                    "evidence": [],
                    "nodes": set(),
                    "executors": set(),
                },
            )
            node_id = str(row.get("node_id") or "").strip()
            executor = str(row.get("executor_type") or "").strip()
            if node_id:
                group["nodes"].add(node_id)
            if executor:
                group["executors"].add(executor)
            group["evidence"].append(
                PatternEvidence(
                    evidence_kind="receipt",
                    evidence_ref=str(row.get("receipt_id") or ""),
                    evidence_role="observed_in",
                    observed_at=observed_at,
                    details={
                        "source": "receipts",
                        "failure_code": failure_code,
                        "node_id": node_id,
                        "run_id": str(row.get("run_id") or ""),
                    },
                )
            )
            group["first_seen_at"] = _min_datetime(group["first_seen_at"], observed_at)
            group["last_seen_at"] = _max_datetime(group["last_seen_at"], observed_at)
        return [
            PatternCandidate(
                pattern_key=str(group["pattern_key"]),
                pattern_kind="runtime_failure_pattern",
                title=str(group["title"]),
                failure_mode=str(group["failure_mode"]),
                evidence_count=len(group["evidence"]),
                first_seen_at=group["first_seen_at"],
                last_seen_at=group["last_seen_at"],
                promotion_candidate=len(group["evidence"]) >= threshold,
                promotion_rule={
                    "source": "receipts",
                    "threshold": threshold,
                    "promotion": "dedupe_receipt_failures_before_bug_filing",
                },
                evidence=tuple(group["evidence"][:25]),
                metadata={
                    "node_ids": sorted(group["nodes"])[:20],
                    "executor_types": sorted(group["executors"])[:20],
                },
            )
            for group in groups.values()
        ]


def _pattern_hydration_base(pattern: PatternCandidate | Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(pattern, PatternCandidate):
        base: dict[str, Any] = {
            "pattern_ref": pattern.pattern_ref,
            "pattern_key": pattern.pattern_key,
            "identity_digest": pattern.identity_digest,
            "pattern_kind": pattern.pattern_kind,
            "status": pattern.status,
            "severity": pattern.severity,
            "owner_surface": pattern.owner_surface,
            "verifier_ref": pattern.verifier_ref,
            "evidence_count": pattern.evidence_count,
            "promotion_candidate": pattern.promotion_candidate,
            "promotion_rule": dict(pattern.promotion_rule),
            "metadata": dict(pattern.metadata),
        }
    else:
        mapping = dict(pattern)
        pattern_key = _require_text(mapping.get("pattern_key"), field_name="pattern_key")
        base = {
            "pattern_ref": _require_text(
                mapping.get("pattern_ref") or pattern_ref_for_key(pattern_key),
                field_name="pattern_ref",
            ),
            "pattern_key": pattern_key,
            "identity_digest": str(
                mapping.get("identity_digest") or pattern_identity_digest(pattern_key)
            ),
            "pattern_kind": str(mapping.get("pattern_kind") or "runtime_failure_pattern"),
            "status": str(mapping.get("status") or "observing"),
            "severity": str(mapping.get("severity") or "P2"),
            "owner_surface": str(mapping.get("owner_surface") or "praxis_patterns"),
            "verifier_ref": mapping.get("verifier_ref"),
            "evidence_count": int(mapping.get("evidence_count") or 0),
            "promotion_candidate": bool(mapping.get("promotion_candidate", True)),
            "promotion_rule": _json_object(mapping.get("promotion_rule")),
            "metadata": _json_object(mapping.get("metadata")),
        }
    base["evidence_refs"] = [
        f"{item['evidence_kind']}:{item['evidence_ref']}:{item['evidence_role']}"
        for item in _pattern_hydration_evidence_refs(pattern)
    ]
    return base


def _pattern_hydration_evidence_refs(
    pattern: PatternCandidate | Mapping[str, Any],
) -> list[dict[str, Any]]:
    if isinstance(pattern, PatternCandidate):
        items = [item.to_json() for item in pattern.evidence]
    else:
        raw_items = dict(pattern).get("evidence") or ()
        items = [dict(item) for item in raw_items if isinstance(item, Mapping)]
    refs: list[dict[str, Any]] = []
    for item in items:
        evidence_kind = str(item.get("evidence_kind") or "").strip()
        evidence_ref = str(item.get("evidence_ref") or "").strip()
        evidence_role = str(item.get("evidence_role") or "observed_in").strip()
        if not evidence_kind or not evidence_ref:
            continue
        refs.append(
            {
                "evidence_kind": evidence_kind,
                "evidence_ref": evidence_ref,
                "evidence_role": evidence_role,
                "observed_at": item.get("observed_at"),
                "details": _json_object(item.get("details")),
            }
        )
    refs.sort(
        key=lambda item: (
            str(item["evidence_kind"]),
            str(item["evidence_ref"]),
            str(item["evidence_role"]),
        )
    )
    return refs


def _pattern_retrieval_sources(
    pattern: PatternCandidate | Mapping[str, Any],
    *,
    evidence_refs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    base = _pattern_hydration_base_without_evidence(pattern)
    sources: list[str] = []
    promotion_rule = _json_object(base.get("promotion_rule"))
    metadata = _json_object(base.get("metadata"))
    for value in (
        *(_iter_text_values(promotion_rule.get("source"))),
        *(_iter_text_values(promotion_rule.get("merged_sources"))),
        *(_iter_text_values(metadata.get("sources"))),
    ):
        text = str(value or "").strip()
        if text and text not in sources:
            sources.append(text)
    for evidence in evidence_refs:
        text = str(evidence.get("evidence_kind") or "").strip()
        if text and text not in sources:
            sources.append(text)
    return {
        "authority": "candidate_evidence_only",
        "candidate_sources": sources,
        "compatible_indexes": ["fts", "vector", "graph", "hybrid"],
        "evidence_refs": list(evidence_refs),
        "admission_rule": (
            "indexes and semantic search can select candidates, but cannot admit "
            "truth or perform mutation"
        ),
    }


def _pattern_semantic_binding_suggestions(
    base: Mapping[str, Any],
    *,
    evidence_refs: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    pattern_ref = str(base["pattern_ref"])
    bindings: list[dict[str, Any]] = [
        {
            "status": "suggested_unpersisted",
            "predicate_slug": "pattern_has_kind",
            "subject_kind": "platform_pattern",
            "subject_ref": pattern_ref,
            "object_kind": "pattern_kind",
            "object_ref": str(base.get("pattern_kind") or ""),
            "qualifiers_json": {"status": str(base.get("status") or "observing")},
            "source_kind": "platform_pattern_hydration",
            "source_ref": pattern_ref,
            "requires_predicate_registration": True,
        },
        {
            "status": "suggested_unpersisted",
            "predicate_slug": "pattern_owner_surface",
            "subject_kind": "platform_pattern",
            "subject_ref": pattern_ref,
            "object_kind": "tool",
            "object_ref": str(base.get("owner_surface") or "praxis_patterns"),
            "qualifiers_json": {"severity": str(base.get("severity") or "P2")},
            "source_kind": "platform_pattern_hydration",
            "source_ref": pattern_ref,
            "requires_predicate_registration": True,
        },
    ]
    for evidence in evidence_refs[:25]:
        bindings.append(
            {
                "status": "suggested_unpersisted",
                "predicate_slug": "pattern_evidenced_by",
                "subject_kind": "platform_pattern",
                "subject_ref": pattern_ref,
                "object_kind": str(evidence.get("evidence_kind") or "evidence"),
                "object_ref": str(evidence.get("evidence_ref") or ""),
                "qualifiers_json": {
                    "evidence_role": str(evidence.get("evidence_role") or "observed_in"),
                    "observed_at": evidence.get("observed_at"),
                },
                "source_kind": "platform_pattern_hydration",
                "source_ref": pattern_ref,
                "evidence_ref": str(evidence.get("evidence_ref") or ""),
                "requires_predicate_registration": True,
            }
        )
    return bindings


def _pattern_typed_gaps(
    *,
    base: Mapping[str, Any],
    materialized: bool,
    semantic_bindings: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    gaps: list[dict[str, Any]] = []
    if int(base.get("evidence_count") or 0) <= 0:
        gaps.append(
            {
                "gap_type": "pattern.evidence_missing",
                "tool": "praxis_patterns",
                "action": "candidates",
                "field": "evidence_refs",
                "blocks": ["materialize_pattern", "file_or_dedupe_bug"],
            }
        )
    if not materialized and not bool(base.get("promotion_candidate")):
        threshold = _json_object(base.get("promotion_rule")).get("threshold")
        gaps.append(
            {
                "gap_type": "pattern.evidence_threshold_unmet",
                "tool": "praxis_patterns",
                "action": "materialize",
                "field": "threshold",
                "required": threshold,
                "actual": int(base.get("evidence_count") or 0),
                "blocks": ["materialize_pattern"],
            }
        )
    if not base.get("verifier_ref"):
        gaps.append(
            {
                "gap_type": "pattern.verifier_missing",
                "tool": "praxis_patterns",
                "action": "list",
                "field": "verifier_ref",
                "blocks": ["resolve_pattern_as_verified"],
            }
        )
    if semantic_bindings:
        gaps.append(
            {
                "gap_type": "pattern.semantic_bindings_unmaterialized",
                "tool": "praxis_semantic_assertions",
                "action": "record_assertion",
                "field": "predicate_slug",
                "suggested_predicates": sorted(
                    {
                        str(binding.get("predicate_slug") or "")
                        for binding in semantic_bindings
                        if binding.get("predicate_slug")
                    }
                ),
                "blocks": ["semantic_projection_authority"],
            }
        )
    return gaps


def _pattern_primitive_hydration(
    pattern: PatternCandidate | Mapping[str, Any],
    *,
    base: Mapping[str, Any],
    materialized: bool,
    typed_gaps: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    pattern_ref = str(base["pattern_ref"])
    pattern_key = str(base["pattern_key"])
    blocked_gap_types = {str(gap.get("gap_type") or "") for gap in typed_gaps}
    materialize_blockers = sorted(
        gap
        for gap in blocked_gap_types
        if gap in {"pattern.evidence_missing", "pattern.evidence_threshold_unmet"}
    )
    actions: list[dict[str, Any]] = [
        {
            "id": "inspect_evidence",
            "tool": "praxis_patterns",
            "action": "evidence",
            "risk": "read",
            "legal_status": "legal_read",
            "input": {"action": "evidence", "pattern_ref": pattern_ref},
            "produces": ["praxis.pattern.evidence_list"],
        },
        {
            "id": "inspect_semantic_bindings",
            "tool": "praxis_semantic_assertions",
            "action": "list",
            "risk": "read",
            "legal_status": "legal_read",
            "input": {
                "action": "list",
                "subject_kind": "platform_pattern",
                "subject_ref": pattern_ref,
            },
            "produces": ["praxis.semantic_assertion.list"],
        },
        {
            "id": "record_semantic_assertions",
            "tool": "praxis_semantic_assertions",
            "action": "record_assertion",
            "risk": "write",
            "legal_status": "blocked_until_operator_confirms_predicates",
            "blocked_by": ["pattern.semantic_bindings_unmaterialized"],
            "consumes": ["pattern.semantic_binding_suggestions"],
            "produces": ["praxis.semantic_assertion.recorded"],
        },
        {
            "id": "file_or_dedupe_bug",
            "tool": "praxis_bugs",
            "action": "duplicate_check_then_file",
            "risk": "write",
            "legal_status": (
                "requires_mutating_scope_and_dedupe"
                if materialized
                else "blocked_until_pattern_materialized"
            ),
            "blocked_by": [] if materialized else ["pattern_not_materialized"],
            "input_template": {
                "action": "file",
                "source_kind": "platform_pattern",
                "source_issue_id": pattern_ref,
                "title": str(_pattern_title(pattern)),
            },
            "consumes": ["praxis.pattern.record"],
            "produces": ["praxis.bug.record"],
        },
    ]
    if not materialized:
        actions.insert(
            2,
            {
                "id": "materialize_pattern",
                "tool": "praxis_patterns",
                "action": "materialize",
                "risk": "write",
                "legal_status": (
                    "requires_mutating_scope"
                    if not materialize_blockers
                    else "blocked"
                ),
                "blocked_by": materialize_blockers,
                "input": {
                    "action": "materialize",
                    "candidate_keys": [pattern_key],
                    "promotion_only": True,
                },
                "consumes": ["praxis.pattern.candidate"],
                "produces": ["praxis.pattern.record"],
            },
        )
    return {
        "object_kind": "platform_pattern",
        "object_ref": pattern_ref,
        "state": "materialized" if materialized else "candidate",
        "legal_actions": [
            action
            for action in actions
            if action["legal_status"].startswith("legal_")
        ],
        "blocked_or_mutating_actions": [
            action for action in actions if not action["legal_status"].startswith("legal_")
        ],
        "write_policy": "mutation requires explicit Praxis tool execution through the CQRS gateway",
    }


def _pattern_repair_actions(
    typed_gaps: Sequence[Mapping[str, Any]],
    *,
    base: Mapping[str, Any],
) -> list[dict[str, Any]]:
    repairs: list[dict[str, Any]] = []
    pattern_ref = str(base["pattern_ref"])
    pattern_key = str(base["pattern_key"])
    for gap in typed_gaps:
        gap_type = str(gap.get("gap_type") or "")
        if gap_type == "pattern.evidence_missing":
            repairs.append(
                {
                    "repair_type": "collect_pattern_evidence",
                    "tool": "praxis_patterns",
                    "action": "candidates",
                    "input": {"action": "candidates", "include_hydration": True},
                }
            )
        elif gap_type == "pattern.evidence_threshold_unmet":
            repairs.append(
                {
                    "repair_type": "wait_or_override_materialization",
                    "tool": "praxis_patterns",
                    "action": "materialize",
                    "input": {
                        "action": "materialize",
                        "candidate_keys": [pattern_key],
                        "promotion_only": False,
                    },
                }
            )
        elif gap_type == "pattern.verifier_missing":
            repairs.append(
                {
                    "repair_type": "attach_or_declare_verifier",
                    "target": f"{pattern_ref}.verifier_ref",
                    "reason": "verified mitigation needs an explicit verifier authority",
                }
            )
        elif gap_type == "pattern.semantic_bindings_unmaterialized":
            repairs.append(
                {
                    "repair_type": "register_or_record_semantic_assertions",
                    "tool": "praxis_semantic_assertions",
                    "action": "record_assertion",
                    "input_source": "hydration.semantic_binding_suggestions",
                }
            )
    return repairs


def _pattern_title(pattern: PatternCandidate | Mapping[str, Any]) -> str:
    if isinstance(pattern, PatternCandidate):
        return pattern.title
    mapping = dict(pattern)
    return str(mapping.get("title") or mapping.get("pattern_key") or "Platform pattern")


def _pattern_hydration_base_without_evidence(
    pattern: PatternCandidate | Mapping[str, Any],
) -> dict[str, Any]:
    if isinstance(pattern, PatternCandidate):
        return {
            "promotion_rule": dict(pattern.promotion_rule),
            "metadata": dict(pattern.metadata),
        }
    mapping = dict(pattern)
    return {
        "promotion_rule": _json_object(mapping.get("promotion_rule")),
        "metadata": _json_object(mapping.get("metadata")),
    }


def _merge_candidates(
    candidates: Iterable[PatternCandidate],
    *,
    threshold: int,
) -> list[PatternCandidate]:
    groups: dict[str, list[PatternCandidate]] = {}
    for candidate in candidates:
        groups.setdefault(candidate.pattern_key, []).append(candidate)
    merged: list[PatternCandidate] = []
    for pattern_key, items in groups.items():
        primary = items[0]
        evidence: list[PatternEvidence] = []
        seen_evidence: set[tuple[str, str, str]] = set()
        metadata_sources: list[str] = []
        for item in items:
            metadata_sources.append(str(item.promotion_rule.get("source") or "unknown"))
            for link in item.evidence:
                key = (link.evidence_kind, link.evidence_ref, link.evidence_role)
                if key in seen_evidence:
                    continue
                seen_evidence.add(key)
                evidence.append(link)
        evidence_count = max(sum(item.evidence_count for item in items), len(evidence))
        first_seen = None
        last_seen = None
        for item in items:
            first_seen = _min_datetime(first_seen, item.first_seen_at)
            last_seen = _max_datetime(last_seen, item.last_seen_at)
        promotion_rule = dict(primary.promotion_rule)
        promotion_rule["merged_sources"] = sorted(set(metadata_sources))
        merged.append(
            PatternCandidate(
                pattern_key=pattern_key,
                pattern_kind=primary.pattern_kind,
                title=primary.title,
                failure_mode=primary.failure_mode,
                evidence_count=evidence_count,
                first_seen_at=first_seen,
                last_seen_at=last_seen,
                promotion_candidate=evidence_count >= threshold or any(item.promotion_candidate for item in items),
                promotion_rule=promotion_rule,
                evidence=tuple(evidence[:50]),
                severity=primary.severity,
                status=primary.status,
                owner_surface=primary.owner_surface,
                verifier_ref=primary.verifier_ref,
                metadata={
                    "merged_candidate_count": len(items),
                    "sources": sorted(set(metadata_sources)),
                    **dict(primary.metadata),
                },
            )
        )
    return merged


def _normalize_sources(sources: Sequence[str] | None) -> tuple[str, ...]:
    allowed = ("friction", "bugs", "receipts")
    if not sources:
        return allowed
    normalized: list[str] = []
    for source in sources:
        text = str(source or "").strip().lower()
        if text not in allowed:
            raise ValueError(f"unknown pattern source: {source}")
        if text not in normalized:
            normalized.append(text)
    return tuple(normalized) or allowed


def _normalize_pattern_kind(value: object) -> str:
    text = str(value or "").strip().lower()
    if text not in PATTERN_KINDS:
        raise ValueError(f"pattern_kind must be one of {sorted(PATTERN_KINDS)}")
    return text


def _normalize_status(value: object) -> str:
    text = str(value or "").strip().lower()
    if text not in PATTERN_STATUSES:
        raise ValueError(f"status must be one of {sorted(PATTERN_STATUSES)}")
    return text


def _normalize_severity(value: object) -> str:
    text = str(value or "P2").strip().upper()
    return text if text in PATTERN_SEVERITIES else "P2"


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _bounded_int(value: object, *, default: int, minimum: int, maximum: int) -> int:
    if value in (None, ""):
        return default
    if isinstance(value, bool):
        raise ValueError("integer fields cannot be booleans")
    try:
        return max(minimum, min(int(value), maximum))
    except (TypeError, ValueError) as exc:
        raise ValueError("expected an integer") from exc


def _since(since_hours: float | None) -> datetime | None:
    if since_hours in (None, ""):
        return None
    hours = float(since_hours)
    if hours <= 0:
        return None
    return datetime.now(timezone.utc) - timedelta(hours=min(hours, 24 * 365))


def _kind_for_friction(reason_code: str, sources: Sequence[str]) -> str:
    if any(str(source).startswith("receipt") for source in sources):
        return "runtime_failure_pattern"
    if reason_code.startswith("cli."):
        return "operator_friction"
    if "missing" in reason_code or "required" in reason_code:
        return "missing_authority"
    if "observ" in reason_code or "stale" in reason_code:
        return "weak_observability"
    return "operator_friction"


def _kind_for_bug(row: Mapping[str, Any]) -> str:
    category = str(row.get("category") or "").strip().upper()
    title = str(row.get("title") or "").lower()
    if category == "ARCHITECTURE":
        return "architecture_smell"
    if category == "WIRING" or "missing" in title or "dead route" in title:
        return "missing_authority"
    if "observability" in title or "green-looking" in title:
        return "weak_observability"
    return "runtime_failure_pattern"


def _evidence_link_id(pattern_ref: str, evidence: PatternEvidence) -> str:
    digest = canonical_hash(
        {
            "purpose": "platform_pattern.evidence_link",
            "canonicalization": "platform_pattern_evidence_link_v1",
            "pattern_ref": pattern_ref,
            "evidence_kind": evidence.evidence_kind,
            "evidence_ref": evidence.evidence_ref,
            "evidence_role": evidence.evidence_role,
        }
    )
    return f"pattern_evidence.{digest[:24]}"


def _pattern_row_to_json(row: Any) -> dict[str, Any]:
    payload = _row_dict(row)
    for key in ("promotion_rule", "metadata"):
        payload[key] = _json_object(payload.get(key))
    for key in ("first_seen_at", "last_seen_at", "created_at", "updated_at"):
        payload[key] = _iso(_coerce_datetime(payload.get(key)))
    return payload


def _evidence_row_to_json(row: Any) -> dict[str, Any]:
    payload = _row_dict(row)
    payload["details"] = _json_object(payload.get("details"))
    for key in ("observed_at", "created_at"):
        payload[key] = _iso(_coerce_datetime(payload.get(key)))
    return payload


def _row_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return {
            key: getattr(row, key)
            for key in dir(row)
            if not key.startswith("_") and not callable(getattr(row, key))
        }


def _split_tags(raw: object) -> tuple[str, ...]:
    if raw is None:
        return ()
    if isinstance(raw, (list, tuple)):
        return tuple(str(item).strip() for item in raw if str(item).strip())
    return tuple(part.strip() for part in str(raw).split(",") if part.strip())


def _tag_value(tags: Sequence[str], prefix: str) -> str | None:
    needle = f"{prefix.lower()}:"
    for tag in tags:
        text = str(tag or "").strip()
        if text.lower().startswith(needle):
            return text.split(":", 1)[1].strip() or None
    return None


def _clean_token(value: object) -> str:
    return " ".join(str(value or "").strip().split()) or "unknown"


def _coerce_datetime(value: object) -> datetime | None:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    return None


def _min_datetime(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return left if left <= right else right


def _max_datetime(left: datetime | None, right: datetime | None) -> datetime | None:
    if left is None:
        return right
    if right is None:
        return left
    return left if left >= right else right


def _json_dumps(value: Mapping[str, Any]) -> str:
    return json.dumps(dict(value), sort_keys=True, default=str)


def _json_object(value: object) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


def _iter_text_values(value: object) -> tuple[str, ...]:
    if value in (None, ""):
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple, set)):
        return tuple(str(item) for item in value if str(item or "").strip())
    return (str(value),)


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else None


__all__ = [
    "PATTERN_HYDRATION_ALGORITHM",
    "PATTERN_HYDRATION_CANONICALIZATION",
    "PATTERN_HYDRATION_CONTRACT",
    "PATTERN_HYDRATION_PURPOSE",
    "PATTERN_IDENTITY_ALGORITHM",
    "PATTERN_IDENTITY_CANONICALIZATION",
    "PATTERN_IDENTITY_PURPOSE",
    "PATTERN_POLICY_DECISION_REF",
    "PatternCandidate",
    "PatternEvidence",
    "PlatformPatternAuthority",
    "pattern_hydration_digest",
    "pattern_hydration_manifest",
    "pattern_identity_digest",
    "pattern_ref_for_key",
]
