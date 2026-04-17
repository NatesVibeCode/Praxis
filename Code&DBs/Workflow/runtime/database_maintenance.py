"""Deterministic database maintenance processor.

Claims queued maintenance intents from Postgres and executes exact, policy-backed
repair verbs without any model-in-the-loop decision step.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile
from typing import TYPE_CHECKING, Any, Optional

from runtime.embedding_service import (
    EmbeddingRuntimeAuthority,
    EmbeddingService,
    resolve_embedding_runtime_authority,
)
from runtime.system_events import emit_system_event
from storage.postgres.memory_graph_repository import PostgresMemoryGraphRepository
from storage.postgres.vector_store import PostgresVectorStore, VectorFilter

if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection


_DEFAULT_REVIEW_THRESHOLDS = {
    "pending_total": 10,
    "failed_total": 1,
    "oldest_pending_seconds": 3600,
    "review_queue_pending_total": 5,
    "entities_needing_reembed": 25,
    "exact_duplicate_entities": 25,
}

_DEFAULT_REPAIR_THRESHOLDS = {
    "pending_total": 50,
    "failed_total": 1,
    "oldest_pending_seconds": 21600,
    "review_queue_pending_total": 20,
    "entities_needing_reembed": 100,
    "exact_duplicate_entities": 100,
}

_DEFAULT_SANDBOX_CLEANUP_BATCH_LIMIT = 25
_DEFAULT_SANDBOX_CLEANUP_CLAIM_TIMEOUT_SECONDS = 15 * 60
_LOCAL_EPHEMERAL_SANDBOX_ROOT_PREFIXES = (
    "praxis-docker-sandbox-",
    "praxis-cloudflare-sandbox-",
)


@dataclass(frozen=True)
class MaintenanceIntent:
    intent_id: int
    intent_kind: str
    subject_kind: str
    subject_id: str | None
    policy_key: str | None
    fingerprint: str
    priority: int
    payload: dict[str, Any]
    attempt_count: int
    max_attempts: int


@dataclass(frozen=True)
class MaintenanceRunResult:
    claimed: int
    completed: int
    skipped: int
    failed: int
    enqueued: int
    findings: tuple[str, ...]
    errors: tuple[str, ...]


@dataclass(frozen=True)
class SandboxCleanupTarget:
    sandbox_session_id: str
    sandbox_root: str
    cleanup_attempt_count: int
    closed_at: datetime | None
    expires_at: datetime | None


@dataclass(frozen=True)
class SandboxCleanupResult:
    status: str
    reason: str
    cleanup_root: str | None
    error_message: str | None = None


class DatabaseMaintenanceProcessor:
    """Execute maintenance_intents deterministically against DB-backed state."""

    def __init__(
        self,
        conn: "SyncPostgresConnection",
        embedder: Optional[Any] = None,
    ) -> None:
        self._conn = conn
        self._embedder = embedder
        self._memory_graph_repository = PostgresMemoryGraphRepository(conn)
        self._vector_store = (
            PostgresVectorStore(conn, embedder) if embedder is not None else None
        )
        self._embedding_authority = self._resolve_embedding_authority(embedder)
        self._validate_embedder_contract()

    # ------------------------------------------------------------------
    # Availability / plumbing
    # ------------------------------------------------------------------

    def is_available(self) -> bool:
        try:
            row = self._conn.fetchrow(
                "SELECT to_regclass('public.maintenance_intents') AS regclass_name"
            )
        except Exception:
            return False
        return bool(row and row["regclass_name"])

    def run_once(self, limit: int = 25) -> MaintenanceRunResult:
        if not self.is_available():
            return MaintenanceRunResult(
                claimed=0,
                completed=0,
                skipped=0,
                failed=0,
                enqueued=0,
                findings=(),
                errors=(),
            )

        findings: list[str] = []
        errors: list[str] = []
        completed = 0
        skipped = 0
        failed = 0

        enqueued = self.enqueue_due_policies()
        claimed_rows = self.claim_pending(limit=limit)
        claimed_total = len(claimed_rows)
        completed_delta, skipped_delta, failed_delta = self._process_claimed_rows(
            claimed_rows,
            findings=findings,
            errors=errors,
        )
        completed += completed_delta
        skipped += skipped_delta
        failed += failed_delta

        # The embedding authority owns the refresh policy. Drain the declared
        # follow-on refresh batch in the same cycle when its trigger intents
        # were claimed, so vector maintenance does not wait for another loop.
        if self._embedding_authority.should_drain_follow_on_refresh(
            intent.intent_kind for intent in claimed_rows
        ):
            follow_on_rows = self.claim_pending_by_kind(
                self._embedding_authority.refresh_follow_on_intent_kind,
                limit=min(limit, self._embedding_authority.refresh_follow_on_batch_limit),
            )
            claimed_total += len(follow_on_rows)
            follow_on_completed, follow_on_skipped, follow_on_failed = self._process_claimed_rows(
                follow_on_rows,
                findings=findings,
                errors=errors,
            )
            completed += follow_on_completed
            skipped += follow_on_skipped
            failed += follow_on_failed

        return MaintenanceRunResult(
            claimed=claimed_total,
            completed=completed,
            skipped=skipped,
            failed=failed,
            enqueued=enqueued,
            findings=tuple(findings),
            errors=tuple(errors),
        )

    def enqueue_due_policies(self) -> int:
        rows = self._conn.execute(
            """
            SELECT policy_key, subject_kind, intent_kind, priority,
                   cadence_seconds, max_attempts, config, last_enqueued_at
            FROM maintenance_policies
            WHERE enabled = true
              AND cadence_seconds IS NOT NULL
            ORDER BY priority DESC, policy_key
            """
        )
        now = datetime.now(timezone.utc)
        enqueued = 0
        for row in rows:
            cadence_seconds = int(row["cadence_seconds"] or 0)
            if cadence_seconds <= 0:
                continue
            last_enqueued_at = row["last_enqueued_at"]
            if last_enqueued_at is not None and (now - last_enqueued_at).total_seconds() < cadence_seconds:
                continue
            bucket = int(now.timestamp() // cadence_seconds)
            payload = self._payload_dict(row["config"])
            self._enqueue_intent(
                intent_kind=str(row["intent_kind"]),
                subject_kind=str(row["subject_kind"]),
                subject_id=f"policy:{row['policy_key']}",
                policy_key=str(row["policy_key"]),
                fingerprint=f"{row['intent_kind']}:{row['policy_key']}:{bucket}",
                priority=int(row["priority"] or 100),
                payload=payload,
                max_attempts=int(row["max_attempts"] or 5),
            )
            self._touch_policy(str(row["policy_key"]), field_name="last_enqueued_at")
            enqueued += 1
        return enqueued

    def claim_pending(self, limit: int = 25) -> list[MaintenanceIntent]:
        rows = self._conn.execute(
            """
            WITH picked AS (
                SELECT intent_id
                FROM maintenance_intents
                WHERE status = 'pending'
                  AND available_at <= now()
                ORDER BY priority DESC, created_at ASC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE maintenance_intents AS intents
            SET status = 'claimed',
                claimed_at = now(),
                attempt_count = intent_count.attempt_count + 1,
                updated_at = now()
            FROM (
                SELECT mi.intent_id, mi.attempt_count
                FROM maintenance_intents AS mi
                JOIN picked USING (intent_id)
            ) AS intent_count
            WHERE intents.intent_id = intent_count.intent_id
            RETURNING intents.intent_id, intents.intent_kind, intents.subject_kind,
                      intents.subject_id, intents.policy_key, intents.fingerprint,
                      intents.priority, intents.payload, intents.attempt_count,
                      intents.max_attempts
            """,
            limit,
        )
        return [self._row_to_intent(row) for row in rows]

    def claim_pending_by_kind(self, intent_kind: str, *, limit: int = 25) -> list[MaintenanceIntent]:
        rows = self._conn.execute(
            """
            WITH picked AS (
                SELECT intent_id
                FROM maintenance_intents
                WHERE status = 'pending'
                  AND available_at <= now()
                  AND intent_kind = $1
                ORDER BY priority DESC, created_at ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            UPDATE maintenance_intents AS intents
            SET status = 'claimed',
                claimed_at = now(),
                attempt_count = intent_count.attempt_count + 1,
                updated_at = now()
            FROM (
                SELECT mi.intent_id, mi.attempt_count
                FROM maintenance_intents AS mi
                JOIN picked USING (intent_id)
            ) AS intent_count
            WHERE intents.intent_id = intent_count.intent_id
            RETURNING intents.intent_id, intents.intent_kind, intents.subject_kind,
                      intents.subject_id, intents.policy_key, intents.fingerprint,
                      intents.priority, intents.payload, intents.attempt_count,
                      intents.max_attempts
            """,
            intent_kind,
            limit,
        )
        return [self._row_to_intent(row) for row in rows]

    # ------------------------------------------------------------------
    # Intent execution
    # ------------------------------------------------------------------

    def _process_claimed_rows(
        self,
        claimed_rows: list[MaintenanceIntent],
        *,
        findings: list[str],
        errors: list[str],
    ) -> tuple[int, int, int]:
        completed = 0
        skipped = 0
        failed = 0
        for intent in claimed_rows:
            try:
                outcome = self._execute_intent(intent)
                status = str(outcome.get("status", "completed"))
                outcome_payload = outcome.get("outcome", {})
                message = str(outcome.get("message", "") or "").strip()
                if status == "skipped":
                    self._mark_skipped(intent.intent_id, outcome_payload)
                    skipped += 1
                else:
                    self._mark_completed(intent.intent_id, outcome_payload)
                    completed += 1
                if intent.policy_key:
                    self._touch_policy(intent.policy_key, field_name="last_run_at")
                if message:
                    findings.append(f"{intent.intent_kind}:{message}")
            except Exception as exc:
                self._retry_or_fail(intent, str(exc))
                errors.append(f"{intent.intent_kind}:{exc}")
                failed += 1
        return completed, skipped, failed

    def _execute_intent(self, intent: MaintenanceIntent) -> dict[str, Any]:
        if intent.intent_kind in {
            "start_maintenance_repair",
            "dispatch_maintenance_repair",
        }:
            return self._process_start_maintenance_repair(intent)
        if intent.intent_kind in {
            "start_maintenance_review",
            "dispatch_maintenance_review",
        }:
            return self._process_start_maintenance_review(intent)
        if intent.intent_kind == "embed_entity":
            return self._process_embed_entity(intent)
        if intent.intent_kind == "refresh_vector_neighbors":
            return self._process_refresh_vector_neighbors(intent)
        if intent.intent_kind == "archive_stale_entities":
            return self._process_archive_stale_entities(intent)
        if intent.intent_kind == "archive_exact_duplicate_entities":
            return self._process_archive_exact_duplicate_entities(intent)
        if intent.intent_kind == "embed_constraint":
            return self._process_embed_constraint(intent)
        if intent.intent_kind == "embed_friction_event":
            return self._process_embed_friction_event(intent)
        if intent.intent_kind == "reconcile_sandbox_session_cleanup":
            return self._process_reconcile_sandbox_session_cleanup(intent)
        return {
            "status": "skipped",
            "message": f"unsupported_intent:{intent.intent_kind}",
            "outcome": {"reason": "unsupported_intent"},
        }

    def _process_start_maintenance_review(self, intent: MaintenanceIntent) -> dict[str, Any]:
        return self._process_start_maintenance_workflow(
            intent,
            workflow_kind="review",
            default_policy_key="system.maintenance_review.daily",
        )

    def _process_start_maintenance_repair(self, intent: MaintenanceIntent) -> dict[str, Any]:
        return self._process_start_maintenance_workflow(
            intent,
            workflow_kind="repair",
            default_policy_key="system.maintenance_repair.auto",
        )

    def _process_start_maintenance_workflow(
        self,
        intent: MaintenanceIntent,
        *,
        workflow_kind: str,
        default_policy_key: str,
    ) -> dict[str, Any]:
        policy_key = intent.policy_key or default_policy_key
        policy = self._fetch_policy(policy_key)
        config = policy.get("config", {})
        summary = self._collect_maintenance_review_summary(intent_id=intent.intent_id)
        evaluation = self._evaluate_maintenance_state(summary=summary, config=config)
        self._record_policy_evaluation(
            policy_key,
            policy=policy,
            evaluation=evaluation,
        )
        decision = self._maintenance_start_decision(
            policy=policy,
            evaluation=evaluation,
            config=config,
            workflow_kind=workflow_kind,
        )
        event_prefix = f"maintenance.{workflow_kind}"
        if not decision["should_start"]:
            outcome = {
                "reason": decision["reason"],
                "policy_key": policy_key,
                "workflow_kind": workflow_kind,
                "summary": summary,
                "evaluation": evaluation["public"],
            }
            self._emit_system_event(
                event_type=f"{event_prefix}.skipped",
                source_id=intent.subject_id or str(intent.intent_id),
                source_type="maintenance_intent",
                payload=outcome,
            )
            return {
                "status": "skipped",
                "message": decision["reason"],
                "outcome": outcome,
            }

        if workflow_kind == "repair":
            spec_dict = self._build_maintenance_repair_spec(
                summary=summary,
                evaluation=evaluation["public"],
                config=config,
                policy_key=policy_key,
            )
        else:
            spec_dict = self._build_maintenance_review_spec(
                summary=summary,
                evaluation=evaluation["public"],
                config=config,
                policy_key=policy_key,
            )

        from runtime.workflow.unified import submit_workflow_inline

        workflow_start_result = submit_workflow_inline(
            self._conn,
            spec_dict,
            packet_provenance={
                "source_kind": "database_maintenance",
                "maintenance_policy": policy_key,
                "maintenance_intent_id": intent.intent_id,
                "file_inputs": {
                    "summary": summary,
                    "evaluation": evaluation["public"],
                    "spec_name": spec_dict.get("name"),
                    "job_labels": [job.get("label") for job in spec_dict.get("jobs", [])],
                },
            },
        )
        workflow_run_id = str(workflow_start_result.get("run_id") or "")
        self._record_policy_evaluation(
            policy_key,
            policy=policy,
            evaluation=evaluation,
            workflow_run_id=workflow_run_id,
        )
        outcome = {
            "policy_key": policy_key,
            "workflow_run_id": workflow_run_id,
            "start_status": workflow_start_result.get("status"),
            "start_reason": decision["reason"],
            "workflow_kind": workflow_kind,
            "summary": summary,
            "evaluation": evaluation["public"],
        }
        self._emit_system_event(
            event_type=f"{event_prefix}.started",
            source_id=workflow_run_id or (intent.subject_id or str(intent.intent_id)),
            source_type="workflow_run" if workflow_run_id else "maintenance_intent",
            payload=outcome,
        )
        return {
            "status": "completed",
            "message": f"maintenance_{workflow_kind}_started:{workflow_run_id or 'unknown'}",
            "outcome": outcome,
        }

    def _process_embed_entity(self, intent: MaintenanceIntent) -> dict[str, Any]:
        if self._embedder is None:
            return self._embedding_unavailable_response(intent)
        row = self._conn.fetchrow(
            """
            SELECT id, name, content, archived, source_hash, embedding_version
            FROM memory_entities
            WHERE id = $1
            """,
            intent.subject_id,
        )
        if row is None:
            return {
                "status": "skipped",
                "message": f"entity_missing:{intent.subject_id}",
                "outcome": {"reason": "entity_missing"},
            }
        if row["archived"]:
            return {
                "status": "skipped",
                "message": f"entity_archived:{intent.subject_id}",
                "outcome": {"reason": "entity_archived"},
            }

        text = f"{row['name'] or ''} {row['content'] or ''}".strip()
        if not text:
            self._memory_graph_repository.mark_entity_embedding_failed(
                entity_id=str(intent.subject_id),
            )
            return {
                "status": "skipped",
                "message": f"entity_empty:{intent.subject_id}",
                "outcome": {"reason": "entity_empty"},
            }

        if self._vector_store is not None:
            self._vector_store.set_embedding(
                "memory_entities",
                "id",
                intent.subject_id,
                text=text,
            )
        self._memory_graph_repository.mark_entity_embedding_ready(
            entity_id=str(intent.subject_id),
            embedding_model=self._embedding_model_name(),
        )
        source_hash = str(row["source_hash"] or "")
        embedding_version = int(row["embedding_version"] or 1)
        self._enqueue_intent(
            intent_kind="refresh_vector_neighbors",
            subject_kind="memory_entity",
            subject_id=str(intent.subject_id),
            policy_key="memory_entity.vector_neighbors",
            fingerprint=f"refresh_vector_neighbors:{intent.subject_id}:{source_hash}:{embedding_version}",
            priority=80,
            payload={
                "source_hash": source_hash,
                "embedding_version": embedding_version,
            },
            max_attempts=5,
        )
        return {
            "status": "completed",
            "message": f"embedded_entity:{intent.subject_id}",
            "outcome": {
                "entity_id": intent.subject_id,
                "embedding_model": self._embedding_model_name(),
                "embedding_version": embedding_version,
            },
        }

    def _process_refresh_vector_neighbors(self, intent: MaintenanceIntent) -> dict[str, Any]:
        row = self._conn.fetchrow(
            """
            SELECT id,
                   name,
                   content,
                   embedding_version,
                   archived
            FROM memory_entities
            WHERE id = $1
            """,
            intent.subject_id,
        )
        if row is None:
            return {
                "status": "skipped",
                "message": f"vector_source_missing:{intent.subject_id}",
                "outcome": {"reason": "source_missing"},
            }
        if row["archived"]:
            return {
                "status": "skipped",
                "message": f"vector_source_unavailable:{intent.subject_id}",
                "outcome": {"reason": "source_unavailable"},
            }

        policy = self._fetch_policy("memory_entity.vector_neighbors")
        top_k = int(policy.get("config", {}).get("top_k", 8))
        min_similarity = float(policy.get("config", {}).get("min_similarity", 0.8))
        embedding_version = int(row["embedding_version"] or 1)
        source_text = f"{row['name'] or ''} {row['content'] or ''}".strip()
        if not source_text or self._vector_store is None:
            return {
                "status": "skipped",
                "message": f"vector_source_unavailable:{intent.subject_id}",
                "outcome": {"reason": "source_unavailable"},
            }

        vector_query = self._vector_store.prepare(source_text)
        neighbors = vector_query.search(
            "memory_entities",
            select_columns=("id",),
            filters=(
                VectorFilter("archived", False),
                VectorFilter("id", intent.subject_id, operator="<>"),
            ),
            limit=top_k,
            min_similarity=min_similarity,
            score_alias="similarity",
        )

        self._conn.execute(
            "DELETE FROM memory_vector_neighbors WHERE source_entity_id = $1",
            intent.subject_id,
        )
        self._conn.execute(
            """
            DELETE FROM memory_inferred_edges
            WHERE source_id = $1
              AND inference_kind = 'vector_neighbor'
            """,
            intent.subject_id,
        )

        if neighbors:
            vector_rows = []
            inferred_rows = []
            for rank, neighbor in enumerate(neighbors, start=1):
                similarity = float(neighbor["similarity"] or 0.0)
                target_id = str(neighbor["id"])
                vector_rows.append(
                    (
                        str(intent.subject_id),
                        target_id,
                        "memory_entity.vector_neighbors",
                        similarity,
                        rank,
                        embedding_version,
                    )
                )
                inferred_rows.append(
                    (
                        str(intent.subject_id),
                        target_id,
                        "semantic_neighbor",
                        "vector_neighbor",
                        similarity,
                        json.dumps(
                            {
                                "policy_key": "memory_entity.vector_neighbors",
                                "rank": rank,
                            }
                        ),
                        1,
                        embedding_version,
                    )
                )
            self._conn.execute_many(
                """
                INSERT INTO memory_vector_neighbors (
                    source_entity_id,
                    target_entity_id,
                    policy_key,
                    similarity,
                    rank,
                    embedding_version,
                    refreshed_at,
                    active
                )
                VALUES ($1, $2, $3, $4, $5, $6, now(), true)
                ON CONFLICT (source_entity_id, target_entity_id, policy_key) DO UPDATE
                SET similarity = EXCLUDED.similarity,
                    rank = EXCLUDED.rank,
                    embedding_version = EXCLUDED.embedding_version,
                    refreshed_at = now(),
                    active = true
                """,
                vector_rows,
            )
            self._conn.execute_many(
                """
                INSERT INTO memory_inferred_edges (
                    source_id,
                    target_id,
                    relation_type,
                    inference_kind,
                    confidence,
                    metadata,
                    evidence_count,
                    embedding_version,
                    created_at,
                    refreshed_at,
                    active
                )
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7, $8, now(), now(), true)
                ON CONFLICT (source_id, target_id, relation_type, inference_kind) DO UPDATE
                SET confidence = EXCLUDED.confidence,
                    metadata = EXCLUDED.metadata,
                    embedding_version = EXCLUDED.embedding_version,
                    evidence_count = EXCLUDED.evidence_count,
                    refreshed_at = now(),
                    active = true
                """,
                inferred_rows,
            )

        self._memory_graph_repository.touch_entity_maintenance(
            entity_id=str(intent.subject_id),
        )
        return {
            "status": "completed",
            "message": f"vector_neighbors:{intent.subject_id}:{len(neighbors)}",
            "outcome": {
                "entity_id": intent.subject_id,
                "neighbor_count": len(neighbors),
                "embedding_version": embedding_version,
            },
        }

    def _process_archive_stale_entities(self, intent: MaintenanceIntent) -> dict[str, Any]:
        policy = self._fetch_policy(intent.policy_key or "memory_entity.archive_stale")
        max_age_days = int(policy.get("config", {}).get("max_age_days", 90))
        rows = self._conn.execute(
            """
            SELECT id
            FROM memory_entities
            WHERE archived = false
              AND updated_at < now() - make_interval(days => $1)
            """,
            max_age_days,
        )
        archived_ids = self._memory_graph_repository.archive_entities(
            entity_ids=[str(row["id"]) for row in rows],
        )
        return {
            "status": "completed",
            "message": f"archived_stale:{len(archived_ids)}",
            "outcome": {
                "max_age_days": max_age_days,
                "archived_ids": list(archived_ids),
            },
        }

    def _process_archive_exact_duplicate_entities(self, intent: MaintenanceIntent) -> dict[str, Any]:
        policy = self._fetch_policy(
            intent.policy_key or "memory_entity.archive_exact_duplicates"
        )
        config = policy.get("config", {})
        payload = self._payload_dict(intent.payload)
        group_limit = int(payload.get("group_limit") or config.get("group_limit") or 25)
        group_limit = max(group_limit, 1)
        groups = self._select_exact_duplicate_groups(
            entity_type=payload.get("entity_type"),
            source_hash=payload.get("source_hash"),
            group_limit=group_limit,
        )
        if not groups:
            return {
                "status": "skipped",
                "message": "exact_duplicates_not_found",
                "outcome": {
                    "reason": "exact_duplicates_not_found",
                    "group_limit": group_limit,
                },
            }

        archived_total = 0
        rehomed_edge_rows = 0
        deleted_edge_rows = 0
        deleted_inferred_rows = 0
        deleted_neighbor_rows = 0
        deleted_pending_intents = 0
        repaired_groups: list[dict[str, Any]] = []

        for group in groups:
            entity_ids = [str(entity_id) for entity_id in list(group["entity_ids"] or [])]
            if len(entity_ids) < 2:
                continue
            canonical_id = entity_ids[0]
            duplicate_ids = entity_ids[1:]
            outcome = self._memory_graph_repository.absorb_exact_duplicate_entities(
                canonical_entity_id=canonical_id,
                duplicate_entity_ids=duplicate_ids,
            )
            archived_total += int(outcome.get("archived_count") or 0)
            rehomed_edge_rows += int(outcome.get("rehomed_edge_rows") or 0)
            deleted_edge_rows += int(outcome.get("deleted_edge_rows") or 0)
            deleted_inferred_rows += int(outcome.get("deleted_inferred_rows") or 0)
            deleted_neighbor_rows += int(outcome.get("deleted_neighbor_rows") or 0)
            deleted_pending_intents += int(outcome.get("deleted_pending_intents") or 0)
            repaired_groups.append(
                {
                    "entity_type": str(group["entity_type"]),
                    "source_hash": str(group["source_hash"]),
                    "canonical_entity_id": canonical_id,
                    "archived_ids": list(outcome.get("archived_ids") or []),
                }
            )

        if archived_total <= 0:
            return {
                "status": "skipped",
                "message": "exact_duplicates_already_clean",
                "outcome": {
                    "reason": "exact_duplicates_already_clean",
                    "groups": repaired_groups,
                },
            }

        return {
            "status": "completed",
            "message": f"archived_exact_duplicates:{archived_total}",
            "outcome": {
                "group_count": len(repaired_groups),
                "archived_count": archived_total,
                "rehomed_edge_rows": rehomed_edge_rows,
                "deleted_edge_rows": deleted_edge_rows,
                "deleted_inferred_rows": deleted_inferred_rows,
                "deleted_neighbor_rows": deleted_neighbor_rows,
                "deleted_pending_intents": deleted_pending_intents,
                "groups": repaired_groups,
            },
        }

    def _process_reconcile_sandbox_session_cleanup(
        self,
        intent: MaintenanceIntent,
    ) -> dict[str, Any]:
        if not self._table_exists("sandbox_sessions"):
            return {
                "status": "skipped",
                "message": "sandbox_cleanup_unavailable",
                "outcome": {"reason": "sandbox_sessions_missing"},
            }

        policy = self._fetch_policy(
            intent.policy_key or "sandbox_session.cleanup_reconcile"
        )
        config = policy.get("config", {})
        batch_limit = max(
            1,
            int(config.get("batch_limit") or _DEFAULT_SANDBOX_CLEANUP_BATCH_LIMIT),
        )
        claim_timeout_seconds = max(
            60,
            int(
                config.get("claim_timeout_seconds")
                or _DEFAULT_SANDBOX_CLEANUP_CLAIM_TIMEOUT_SECONDS
            ),
        )

        expired_closed = self._mark_expired_sandbox_sessions_closed()
        targets = self._claim_due_sandbox_cleanup_sessions(
            limit=batch_limit,
            claim_timeout_seconds=claim_timeout_seconds,
        )
        if not targets:
            outcome = {
                "reason": "sandbox_cleanup_none_due",
                "expired_closed": expired_closed,
                "claimed": 0,
                "batch_limit": batch_limit,
                "claim_timeout_seconds": claim_timeout_seconds,
            }
            self._emit_system_event(
                event_type="maintenance.sandbox_cleanup.skipped",
                source_id=intent.subject_id or str(intent.intent_id),
                source_type="maintenance_intent",
                payload=outcome,
            )
            return {
                "status": "skipped",
                "message": "sandbox_cleanup_none_due",
                "outcome": outcome,
            }

        removed_count = 0
        absent_count = 0
        skipped_count = 0
        failed_count = 0
        processed_ids: list[str] = []

        for target in targets:
            processed_ids.append(target.sandbox_session_id)
            result = self._cleanup_local_sandbox_root(target.sandbox_root)
            outcome = {
                "reason": result.reason,
                "cleanup_root": result.cleanup_root,
                "sandbox_root": target.sandbox_root,
                "cleanup_attempt_count": target.cleanup_attempt_count,
            }
            if result.status == "completed":
                if result.reason == "removed":
                    removed_count += 1
                else:
                    absent_count += 1
                self._record_sandbox_cleanup_result(
                    target.sandbox_session_id,
                    status="completed",
                    outcome=outcome,
                )
                continue
            if result.status == "skipped":
                skipped_count += 1
                self._record_sandbox_cleanup_result(
                    target.sandbox_session_id,
                    status="skipped",
                    outcome=outcome,
                )
                continue

            failed_count += 1
            assert result.error_message is not None
            self._record_sandbox_cleanup_result(
                target.sandbox_session_id,
                status="failed",
                outcome=outcome,
                error_message=result.error_message,
            )

        outcome = {
            "expired_closed": expired_closed,
            "claimed": len(targets),
            "removed_count": removed_count,
            "already_absent_count": absent_count,
            "skipped_count": skipped_count,
            "failed_count": failed_count,
            "batch_limit": batch_limit,
            "claim_timeout_seconds": claim_timeout_seconds,
            "sandbox_session_ids": processed_ids,
        }
        self._emit_system_event(
            event_type="maintenance.sandbox_cleanup.reconciled",
            source_id=intent.subject_id or str(intent.intent_id),
            source_type="maintenance_intent",
            payload=outcome,
        )
        return {
            "status": "completed",
            "message": (
                "sandbox_cleanup:"
                f"removed={removed_count}:"
                f"already_absent={absent_count}:"
                f"skipped={skipped_count}:"
                f"failed={failed_count}"
            ),
            "outcome": outcome,
        }

    def _process_embed_constraint(self, intent: MaintenanceIntent) -> dict[str, Any]:
        if self._embedder is None:
            return self._embedding_unavailable_response(intent)
        row = self._conn.fetchrow(
            """
            SELECT constraint_id, pattern, constraint_text
            FROM workflow_constraints
            WHERE constraint_id = $1
            """,
            intent.subject_id,
        )
        if row is None:
            return {
                "status": "skipped",
                "message": f"constraint_missing:{intent.subject_id}",
                "outcome": {"reason": "constraint_missing"},
            }
        if self._vector_store is not None:
            self._vector_store.set_embedding(
                "workflow_constraints",
                "constraint_id",
                intent.subject_id,
                text=f"pattern: {row['pattern']}\ndescription: {row['constraint_text']}",
            )
        return {
            "status": "completed",
            "message": f"embedded_constraint:{intent.subject_id}",
            "outcome": {"constraint_id": intent.subject_id},
        }

    def _process_embed_friction_event(self, intent: MaintenanceIntent) -> dict[str, Any]:
        if self._embedder is None:
            return self._embedding_unavailable_response(intent)
        row = self._conn.fetchrow(
            """
            SELECT event_id, friction_type, message
            FROM friction_events
            WHERE event_id = $1
            """,
            intent.subject_id,
        )
        if row is None:
            return {
                "status": "skipped",
                "message": f"friction_missing:{intent.subject_id}",
                "outcome": {"reason": "friction_missing"},
            }
        if self._vector_store is not None:
            self._vector_store.set_embedding(
                "friction_events",
                "event_id",
                intent.subject_id,
                text=f"{row['friction_type']} {row['message']}",
            )
        return {
            "status": "completed",
            "message": f"embedded_friction:{intent.subject_id}",
            "outcome": {"event_id": intent.subject_id},
        }

    def _select_exact_duplicate_groups(
        self,
        *,
        entity_type: Any | None = None,
        source_hash: Any | None = None,
        group_limit: int = 25,
    ) -> list[dict[str, Any]]:
        clauses = [
            "archived = false",
            "COALESCE(source_hash, '') <> ''",
        ]
        args: list[Any] = []
        if isinstance(entity_type, str) and entity_type.strip():
            args.append(entity_type.strip())
            clauses.append(f"entity_type = ${len(args)}")
        if isinstance(source_hash, str) and source_hash.strip():
            args.append(source_hash.strip())
            clauses.append(f"source_hash = ${len(args)}")
        args.append(max(int(group_limit), 1))
        rows = self._conn.execute(
            f"""
            SELECT entity_type,
                   source_hash,
                   array_agg(id ORDER BY created_at ASC, id ASC) AS entity_ids,
                   COUNT(*) AS duplicate_total
            FROM memory_entities
            WHERE {' AND '.join(clauses)}
            GROUP BY entity_type, source_hash
            HAVING COUNT(*) > 1
            ORDER BY COUNT(*) DESC, MIN(created_at) ASC, entity_type ASC, source_hash ASC
            LIMIT ${len(args)}
            """,
            *args,
        )
        return [dict(row) for row in rows]

    def _collect_maintenance_review_summary(self, *, intent_id: int) -> dict[str, Any]:
        pending_by_kind_rows = self._conn.execute(
            """
            SELECT intent_kind, COUNT(*) AS total
            FROM maintenance_intents
            WHERE status = 'pending'
              AND intent_id <> $1
            GROUP BY intent_kind
            ORDER BY total DESC, intent_kind ASC
            """,
            intent_id,
        )
        failed_by_kind_rows = self._conn.execute(
            """
            SELECT intent_kind, COUNT(*) AS total
            FROM maintenance_intents
            WHERE status = 'failed'
              AND intent_id <> $1
            GROUP BY intent_kind
            ORDER BY total DESC, intent_kind ASC
            """,
            intent_id,
        )
        queue_pending = 0
        if self._table_exists("review_queue"):
            queue_pending = self._fetch_int(
                """
                SELECT COUNT(*) AS total
                FROM review_queue
                WHERE processed_at IS NULL
                """
            )
        needs_reembed = self._fetch_int(
            """
            SELECT COUNT(*) AS total
            FROM memory_entities
            WHERE archived = false
              AND (
                COALESCE(needs_reembed, false) = true
                OR COALESCE(embedding_status, 'pending') IN ('pending', 'failed')
              )
            """
        )
        vector_neighbor_rows = self._fetch_int(
            "SELECT COUNT(*) AS total FROM memory_vector_neighbors WHERE active = true"
        )
        duplicate_metrics_row = self._conn.fetchrow(
            """
            SELECT COUNT(*) AS duplicate_groups,
                   COALESCE(SUM(group_size - 1), 0) AS duplicate_entities
            FROM (
                SELECT COUNT(*) AS group_size
                FROM memory_entities
                WHERE archived = false
                  AND COALESCE(source_hash, '') <> ''
                GROUP BY entity_type, source_hash
                HAVING COUNT(*) > 1
            ) AS duplicate_groups
            """
        )
        oldest_pending_seconds = self._fetch_float(
            """
            SELECT EXTRACT(EPOCH FROM (now() - MIN(created_at))) AS age_seconds
            FROM maintenance_intents
            WHERE status = 'pending'
              AND intent_id <> $1
            """,
            intent_id,
        )

        pending_by_kind = {
            str(row["intent_kind"]): int(row["total"] or 0)
            for row in pending_by_kind_rows or []
        }
        failed_by_kind = {
            str(row["intent_kind"]): int(row["total"] or 0)
            for row in failed_by_kind_rows or []
        }
        pending_total = sum(pending_by_kind.values())
        failed_total = sum(failed_by_kind.values())
        exact_duplicate_groups = 0
        exact_duplicate_entities = 0
        if duplicate_metrics_row is not None:
            exact_duplicate_groups = int(duplicate_metrics_row["duplicate_groups"] or 0)
            exact_duplicate_entities = int(duplicate_metrics_row["duplicate_entities"] or 0)
        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "maintenance_intents": {
                "pending_total": pending_total,
                "failed_total": failed_total,
                "oldest_pending_seconds": int(oldest_pending_seconds or 0),
                "pending_by_kind": pending_by_kind,
                "failed_by_kind": failed_by_kind,
            },
            "review_queue": {
                "pending_total": queue_pending,
            },
            "memory": {
                "entities_needing_reembed": needs_reembed,
                "active_vector_neighbors": vector_neighbor_rows,
                "exact_duplicate_groups": exact_duplicate_groups,
                "exact_duplicate_entities": exact_duplicate_entities,
            },
        }

    def _evaluate_maintenance_state(
        self,
        *,
        summary: dict[str, Any],
        config: dict[str, Any],
    ) -> dict[str, Any]:
        thresholds = config.get("thresholds") if isinstance(config.get("thresholds"), dict) else {}
        review_thresholds = self._normalize_metric_thresholds(
            thresholds.get("review"),
            defaults=_DEFAULT_REVIEW_THRESHOLDS,
        )
        repair_thresholds = self._normalize_metric_thresholds(
            thresholds.get("repair"),
            defaults=_DEFAULT_REPAIR_THRESHOLDS,
        )
        metrics = {
            "pending_total": int(summary.get("maintenance_intents", {}).get("pending_total") or 0),
            "failed_total": int(summary.get("maintenance_intents", {}).get("failed_total") or 0),
            "oldest_pending_seconds": int(
                summary.get("maintenance_intents", {}).get("oldest_pending_seconds") or 0
            ),
            "review_queue_pending_total": int(summary.get("review_queue", {}).get("pending_total") or 0),
            "entities_needing_reembed": int(
                summary.get("memory", {}).get("entities_needing_reembed") or 0
            ),
            "exact_duplicate_entities": int(
                summary.get("memory", {}).get("exact_duplicate_entities") or 0
            ),
        }
        signals: list[dict[str, Any]] = []
        for metric_name, value in metrics.items():
            repair_threshold = repair_thresholds.get(metric_name)
            review_threshold = review_thresholds.get(metric_name)
            severity = None
            threshold_value = None
            if repair_threshold is not None and value >= repair_threshold:
                severity = "repair"
                threshold_value = repair_threshold
            elif review_threshold is not None and value >= review_threshold:
                severity = "review"
                threshold_value = review_threshold
            if severity is None:
                continue
            signals.append(
                {
                    "metric": metric_name,
                    "value": value,
                    "threshold": threshold_value,
                    "severity": severity,
                }
            )

        severity = "clean"
        if any(signal["severity"] == "repair" for signal in signals):
            severity = "repair"
        elif signals:
            severity = "review"

        fingerprint_payload = {
            "severity": severity,
            "signals": signals,
            "metrics": metrics,
            "pending_by_kind": summary.get("maintenance_intents", {}).get("pending_by_kind", {}),
            "failed_by_kind": summary.get("maintenance_intents", {}).get("failed_by_kind", {}),
        }
        state_fingerprint = self._stable_fingerprint(fingerprint_payload)
        public_evaluation = {
            "severity": severity,
            "review_needed": bool(signals),
            "repair_needed": any(signal["severity"] == "repair" for signal in signals),
            "signals": signals,
            "metrics": metrics,
            "state_fingerprint": state_fingerprint,
        }
        return {
            "is_dirty": bool(signals),
            "severity": severity,
            "review_needed": public_evaluation["review_needed"],
            "repair_needed": public_evaluation["repair_needed"],
            "signals": signals,
            "metrics": metrics,
            "state_fingerprint": state_fingerprint,
            "public": public_evaluation,
            "stored": {
                "summary": summary,
                "evaluation": public_evaluation,
                "pending_by_kind": fingerprint_payload["pending_by_kind"],
                "failed_by_kind": fingerprint_payload["failed_by_kind"],
            },
        }

    def _maintenance_start_decision(
        self,
        *,
        policy: dict[str, Any],
        evaluation: dict[str, Any],
        config: dict[str, Any],
        workflow_kind: str,
    ) -> dict[str, Any]:
        start_if_clean = bool(config.get("start_if_clean", config.get("dispatch_if_clean", False)))
        if workflow_kind == "repair":
            if not evaluation["repair_needed"]:
                return {"should_start": False, "reason": "maintenance_repair_not_needed"}
        else:
            if not evaluation["review_needed"] and not start_if_clean:
                return {"should_start": False, "reason": "maintenance_review_not_needed"}

        state_fingerprint = str(evaluation["state_fingerprint"])
        last_start_fingerprint = str(policy.get("last_start_fingerprint") or "")
        last_started_at = policy.get("last_started_at")
        repeat_seconds_default = 43200 if workflow_kind == "repair" else 259200
        repeat_seconds = int(
            config.get("repeat_start_seconds", config.get("repeat_dispatch_seconds"))
            or repeat_seconds_default
        )
        if last_start_fingerprint != state_fingerprint:
            return {"should_start": True, "reason": "dirty_state_changed"}
        if last_started_at is None:
            return {"should_start": True, "reason": "dirty_state_never_started"}
        elapsed_seconds = (datetime.now(timezone.utc) - last_started_at).total_seconds()
        if elapsed_seconds >= repeat_seconds:
            return {"should_start": True, "reason": "dirty_state_repeat_interval_elapsed"}
        return {"should_start": False, "reason": "dirty_state_unchanged_within_repeat_window"}

    def _build_maintenance_review_spec(
        self,
        *,
        summary: dict[str, Any],
        evaluation: dict[str, Any],
        config: dict[str, Any],
        policy_key: str,
    ) -> dict[str, Any]:
        agent_slug = str(config.get("agent_slug") or "openai/gpt-5.4-mini").strip()
        workflow_name = str(config.get("workflow_name") or "maintenance_daily_review").strip()
        job_label = str(config.get("job_label") or "maintenance_review").strip()
        workspace_ref = str(config.get("workspace_ref") or "praxis").strip()
        runtime_profile_ref = str(config.get("runtime_profile_ref") or "praxis").strip()
        task_type = str(config.get("task_type") or "ops_review").strip()
        max_attempts = int(config.get("start_max_attempts", config.get("dispatch_max_attempts")) or 2)
        prompt = (
            "You are the daily maintenance reviewer for the Praxis control plane.\n\n"
            "Use only the repo-local state below. Do not invent missing facts.\n"
            "Return JSON with this exact shape:\n"
            "{\n"
            '  "status": "healthy|degraded|stalled",\n'
            '  "summary": "one short paragraph",\n'
            '  "must_do": ["..."],\n'
            '  "should_do": ["..."],\n'
            '  "watch_items": ["..."],\n'
            '  "repair_start_recommended": {"value": true, "reason": "..."}\n'
            "}\n\n"
            f"Policy: {policy_key}\n"
            "Dirty-state evaluation:\n"
            f"{json.dumps(evaluation, indent=2, sort_keys=True)}\n\n"
            "State:\n"
            f"{json.dumps(summary, indent=2, sort_keys=True)}"
        )
        return {
            "name": workflow_name,
            "phase": "review",
            "outcome_goal": "Produce one authoritative maintenance review for the current dirty state.",
            "workspace_ref": workspace_ref,
            "runtime_profile_ref": runtime_profile_ref,
            "jobs": [
                {
                    "label": job_label,
                    "agent": agent_slug,
                    "prompt": prompt,
                    "task_type": task_type,
                    "max_attempts": max_attempts,
                }
            ],
        }

    def _build_maintenance_repair_spec(
        self,
        *,
        summary: dict[str, Any],
        evaluation: dict[str, Any],
        config: dict[str, Any],
        policy_key: str,
    ) -> dict[str, Any]:
        agent_slug = str(config.get("agent_slug") or "openai/gpt-5.4").strip()
        workflow_name = str(config.get("workflow_name") or "maintenance_auto_repair").strip()
        job_label = str(config.get("job_label") or "maintenance_repair").strip()
        workspace_ref = str(config.get("workspace_ref") or "praxis").strip()
        runtime_profile_ref = str(config.get("runtime_profile_ref") or "praxis").strip()
        task_type = str(config.get("task_type") or "ops_repair").strip()
        max_attempts = int(config.get("start_max_attempts", config.get("dispatch_max_attempts")) or 2)
        prompt = (
            "You are the automatic maintenance repair worker for the Praxis control plane.\n\n"
            "Use only the repo-local state below. Do not invent missing facts.\n"
            "Operate only inside this workspace.\n"
            "Prefer direct fixes to the maintenance path itself: policy/config issues, stuck deterministic"
            " maintenance flows, embedding/vector refresh defects, and queue-drain regressions.\n"
            "If no safe repair is justified, return a no-op result and make no changes.\n"
            "Return JSON with this exact shape:\n"
            "{\n"
            '  "status": "repaired|partial|no_safe_repair",\n'
            '  "summary": "one short paragraph",\n'
            '  "actions_taken": ["..."],\n'
            '  "remaining_risks": ["..."],\n'
            '  "follow_up_review_needed": {"value": true, "reason": "..."}\n'
            "}\n\n"
            f"Policy: {policy_key}\n"
            "Dirty-state evaluation:\n"
            f"{json.dumps(evaluation, indent=2, sort_keys=True)}\n\n"
            "State:\n"
            f"{json.dumps(summary, indent=2, sort_keys=True)}"
        )
        return {
            "name": workflow_name,
            "phase": "repair",
            "outcome_goal": "Reduce or eliminate the current maintenance dirty state.",
            "workspace_ref": workspace_ref,
            "runtime_profile_ref": runtime_profile_ref,
            "jobs": [
                {
                    "label": job_label,
                    "agent": agent_slug,
                    "prompt": prompt,
                    "task_type": task_type,
                    "max_attempts": max_attempts,
                }
            ],
        }

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def _mark_completed(self, intent_id: int, outcome: dict[str, Any]) -> None:
        self._conn.execute(
            """
            UPDATE maintenance_intents
            SET status = 'completed',
                completed_at = now(),
                outcome = $2::jsonb,
                updated_at = now()
            WHERE intent_id = $1
            """,
            intent_id,
            json.dumps(outcome or {}),
        )

    def _mark_skipped(self, intent_id: int, outcome: dict[str, Any]) -> None:
        self._conn.execute(
            """
            UPDATE maintenance_intents
            SET status = 'skipped',
                completed_at = now(),
                outcome = $2::jsonb,
                updated_at = now()
            WHERE intent_id = $1
            """,
            intent_id,
            json.dumps(outcome or {}),
        )

    def _mark_expired_sandbox_sessions_closed(self) -> int:
        row = self._conn.fetchrow(
            """
            WITH updated AS (
                UPDATE sandbox_sessions
                SET closed_at = expires_at,
                    closed_reason_code = COALESCE(closed_reason_code, 'sandbox.expired')
                WHERE closed_at IS NULL
                  AND expires_at <= now()
                RETURNING 1
            )
            SELECT COUNT(*) AS total
            FROM updated
            """
        )
        return 0 if row is None else int(row["total"] or 0)

    def _claim_due_sandbox_cleanup_sessions(
        self,
        *,
        limit: int,
        claim_timeout_seconds: int,
    ) -> list[SandboxCleanupTarget]:
        rows = self._conn.execute(
            """
            WITH picked AS (
                SELECT sandbox_session_id
                FROM sandbox_sessions
                WHERE closed_at IS NOT NULL
                  AND cleanup_completed_at IS NULL
                  AND (
                        cleanup_status IS NULL
                     OR cleanup_status IN ('pending', 'failed')
                     OR (
                            cleanup_status = 'in_progress'
                        AND cleanup_attempted_at <= now() - make_interval(secs => $1)
                     )
                  )
                ORDER BY COALESCE(closed_at, expires_at) ASC, sandbox_session_id ASC
                LIMIT $2
                FOR UPDATE SKIP LOCKED
            )
            UPDATE sandbox_sessions AS sessions
            SET cleanup_status = 'in_progress',
                cleanup_requested_at = COALESCE(sessions.cleanup_requested_at, now()),
                cleanup_attempted_at = now(),
                cleanup_attempt_count = COALESCE(sessions.cleanup_attempt_count, 0) + 1,
                cleanup_last_error = NULL,
                cleanup_outcome = '{}'::jsonb
            FROM picked
            WHERE sessions.sandbox_session_id = picked.sandbox_session_id
            RETURNING sessions.sandbox_session_id,
                      sessions.sandbox_root,
                      sessions.cleanup_attempt_count,
                      sessions.closed_at,
                      sessions.expires_at
            """,
            claim_timeout_seconds,
            limit,
        )
        return [self._row_to_sandbox_cleanup_target(row) for row in rows]

    def _record_sandbox_cleanup_result(
        self,
        sandbox_session_id: str,
        *,
        status: str,
        outcome: dict[str, Any],
        error_message: str | None = None,
    ) -> None:
        if status not in {"completed", "failed", "skipped"}:
            raise RuntimeError(f"invalid sandbox cleanup status: {status}")
        completed = status in {"completed", "skipped"}
        self._conn.execute(
            """
            UPDATE sandbox_sessions
            SET cleanup_status = $2,
                cleanup_completed_at = CASE WHEN $3 THEN now() ELSE NULL END,
                cleanup_last_error = $4,
                cleanup_outcome = $5::jsonb
            WHERE sandbox_session_id = $1
            """,
            sandbox_session_id,
            status,
            completed,
            error_message,
            json.dumps(outcome or {}),
        )

    def _retry_or_fail(self, intent: MaintenanceIntent, error_message: str) -> None:
        if intent.attempt_count >= intent.max_attempts:
            self._conn.execute(
                """
                UPDATE maintenance_intents
                SET status = 'failed',
                    completed_at = now(),
                    last_error = $2,
                    updated_at = now()
                WHERE intent_id = $1
                """,
                intent.intent_id,
                error_message,
            )
            return
        delay_seconds = min(300 * intent.attempt_count, 3600)
        available_at = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)
        self._conn.execute(
            """
            UPDATE maintenance_intents
            SET status = 'pending',
                claimed_at = NULL,
                available_at = $2,
                last_error = $3,
                updated_at = now()
            WHERE intent_id = $1
            """,
            intent.intent_id,
            available_at,
            error_message,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _enqueue_intent(
        self,
        *,
        intent_kind: str,
        subject_kind: str,
        subject_id: str | None,
        fingerprint: str,
        priority: int,
        payload: dict[str, Any],
        max_attempts: int,
        policy_key: str | None = None,
    ) -> None:
        self._conn.execute(
            """
            SELECT enqueue_maintenance_intent(
                $1, $2, $3, $4, $5, $6::jsonb, now(), $7, $8
            ) AS intent_id
            """,
            intent_kind,
            subject_kind,
            subject_id,
            fingerprint,
            priority,
            json.dumps(payload or {}),
            max_attempts,
            policy_key,
        )

    def _row_to_sandbox_cleanup_target(self, row: Any) -> SandboxCleanupTarget:
        return SandboxCleanupTarget(
            sandbox_session_id=str(row["sandbox_session_id"]),
            sandbox_root=str(row["sandbox_root"] or ""),
            cleanup_attempt_count=int(row["cleanup_attempt_count"] or 0),
            closed_at=row.get("closed_at"),
            expires_at=row.get("expires_at"),
        )

    def _cleanup_local_sandbox_root(self, sandbox_root: str) -> SandboxCleanupResult:
        cleanup_root = self._resolve_local_ephemeral_cleanup_root(sandbox_root)
        if cleanup_root is None:
            return SandboxCleanupResult(
                status="skipped",
                reason="unsupported_sandbox_root",
                cleanup_root=None,
            )

        path = Path(cleanup_root)
        if not path.exists():
            return SandboxCleanupResult(
                status="completed",
                reason="already_absent",
                cleanup_root=cleanup_root,
            )
        if not path.is_dir():
            return SandboxCleanupResult(
                status="skipped",
                reason="unsupported_sandbox_root",
                cleanup_root=cleanup_root,
            )
        try:
            shutil.rmtree(path)
        except FileNotFoundError:
            return SandboxCleanupResult(
                status="completed",
                reason="already_absent",
                cleanup_root=cleanup_root,
            )
        except OSError as exc:
            return SandboxCleanupResult(
                status="failed",
                reason="delete_failed",
                cleanup_root=cleanup_root,
                error_message=str(exc),
            )
        return SandboxCleanupResult(
            status="completed",
            reason="removed",
            cleanup_root=cleanup_root,
        )

    def _resolve_local_ephemeral_cleanup_root(self, sandbox_root: str) -> str | None:
        raw_root = str(sandbox_root or "").strip()
        if not raw_root:
            return None
        resolved_root = Path(os.path.realpath(raw_root))
        temp_root = Path(os.path.realpath(tempfile.gettempdir()))
        candidates = [resolved_root]
        if resolved_root.name == "workspace":
            candidates.append(resolved_root.parent)
        for candidate in candidates:
            if candidate.parent != temp_root:
                continue
            if any(
                candidate.name.startswith(prefix)
                for prefix in _LOCAL_EPHEMERAL_SANDBOX_ROOT_PREFIXES
            ):
                return str(candidate)
        return None

    def _fetch_policy(self, policy_key: str) -> dict[str, Any]:
        row = self._conn.fetchrow(
            """
            SELECT *
            FROM maintenance_policies
            WHERE policy_key = $1
            """,
            policy_key,
        )
        if row is None:
            return {"config": {}}
        payload = dict(row)
        return {
            "policy_key": payload.get("policy_key"),
            "priority": payload.get("priority"),
            "cadence_seconds": payload.get("cadence_seconds"),
            "max_attempts": payload.get("max_attempts"),
            "config": self._payload_dict(payload.get("config")),
            "last_enqueued_at": payload.get("last_enqueued_at"),
            "last_run_at": payload.get("last_run_at"),
            "last_state_fingerprint": payload.get("last_state_fingerprint"),
            "last_start_fingerprint": payload.get("last_start_fingerprint"),
            "last_started_at": payload.get("last_started_at"),
            "last_dirty_at": payload.get("last_dirty_at"),
            "last_clean_at": payload.get("last_clean_at"),
            "last_workflow_run_id": payload.get("last_workflow_run_id"),
            "last_evaluation": self._payload_dict(payload.get("last_evaluation")),
        }

    def _record_policy_evaluation(
        self,
        policy_key: str,
        *,
        policy: dict[str, Any],
        evaluation: dict[str, Any],
        workflow_run_id: str | None = None,
    ) -> None:
        updates: dict[str, Any] = {
            "last_state_fingerprint": evaluation["state_fingerprint"],
            "last_evaluation": evaluation["stored"],
        }
        previous_state_fingerprint = str(policy.get("last_state_fingerprint") or "")
        if evaluation["is_dirty"]:
            if previous_state_fingerprint != evaluation["state_fingerprint"] or policy.get("last_dirty_at") is None:
                updates["last_dirty_at"] = datetime.now(timezone.utc)
        else:
            updates["last_clean_at"] = datetime.now(timezone.utc)
        if workflow_run_id:
            updates["last_start_fingerprint"] = evaluation["state_fingerprint"]
            updates["last_started_at"] = datetime.now(timezone.utc)
            updates["last_workflow_run_id"] = workflow_run_id
        self._update_policy_fields(policy_key, updates)

    def _update_policy_fields(self, policy_key: str, updates: dict[str, Any]) -> None:
        if not updates:
            return
        allowed_fields = {
            "last_state_fingerprint",
            "last_start_fingerprint",
            "last_started_at",
            "last_dirty_at",
            "last_clean_at",
            "last_workflow_run_id",
            "last_evaluation",
        }
        assignments: list[str] = []
        values: list[Any] = [policy_key]
        parameter_index = 2
        for field_name, value in updates.items():
            if field_name not in allowed_fields:
                raise ValueError(f"unsupported policy update field: {field_name}")
            if field_name == "last_evaluation":
                assignments.append(f"{field_name} = ${parameter_index}::jsonb")
                values.append(json.dumps(value or {}))
            else:
                assignments.append(f"{field_name} = ${parameter_index}")
                values.append(value)
            parameter_index += 1
        assignments.append("updated_at = now()")
        self._conn.execute(
            f"""
            UPDATE maintenance_policies
            SET {', '.join(assignments)}
            WHERE policy_key = $1
            """,
            *values,
        )

    def _emit_system_event(
        self,
        *,
        event_type: str,
        source_id: str,
        source_type: str,
        payload: dict[str, Any],
    ) -> None:
        try:
            emit_system_event(
                self._conn,
                event_type=event_type,
                source_id=source_id,
                source_type=source_type,
                payload=payload or {},
            )
        except Exception:
            return

    def _table_exists(self, table_name: str) -> bool:
        row = self._conn.fetchrow(
            "SELECT to_regclass($1) AS regclass_name",
            f"public.{table_name}",
        )
        return bool(row and row["regclass_name"])

    def _fetch_int(self, query: str, *args: Any) -> int:
        row = self._conn.fetchrow(query, *args)
        if row is None:
            return 0
        return int(row["total"] or 0)

    def _fetch_float(self, query: str, *args: Any) -> float:
        row = self._conn.fetchrow(query, *args)
        if row is None:
            return 0.0
        return float(row["age_seconds"] or 0.0)

    @staticmethod
    def _normalize_metric_thresholds(
        raw: Any,
        *,
        defaults: dict[str, int],
    ) -> dict[str, int]:
        thresholds: dict[str, int] = {}
        source = raw if isinstance(raw, dict) else {}
        for metric_name, default_value in defaults.items():
            raw_value = source.get(metric_name, default_value)
            try:
                thresholds[metric_name] = int(raw_value)
            except (TypeError, ValueError):
                thresholds[metric_name] = int(default_value)
        return thresholds

    @staticmethod
    def _stable_fingerprint(payload: dict[str, Any]) -> str:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()

    def _touch_policy(self, policy_key: str, *, field_name: str) -> None:
        if field_name not in {"last_enqueued_at", "last_run_at"}:
            raise ValueError(f"unsupported policy field: {field_name}")
        self._conn.execute(
            f"""
            UPDATE maintenance_policies
            SET {field_name} = now(),
                updated_at = now()
            WHERE policy_key = $1
            """,
            policy_key,
        )

    def _embedding_model_name(self) -> str:
        return self._embedding_authority.model_name

    def _embedding_unavailable_response(self, intent: MaintenanceIntent) -> dict[str, Any]:
        return self._embedding_authority.missing_embedder_outcome(
            subject_id=str(intent.subject_id) if intent.subject_id is not None else None,
        )

    def _resolve_embedding_authority(
        self,
        embedder: Optional[Any],
    ) -> EmbeddingRuntimeAuthority:
        authority = getattr(embedder, "authority", None)
        required_attrs = (
            "model_name",
            "dimensions",
            "should_drain_follow_on_refresh",
            "missing_embedder_outcome",
            "validate_embedding_vector",
            "validate_embedder_model",
            "validate_embedder_dimensions",
        )
        if authority is not None and all(hasattr(authority, attr) for attr in required_attrs):
            return authority
        return resolve_embedding_runtime_authority()

    def _validate_embedder_contract(self) -> None:
        if self._embedder is None:
            return
        model_name = self._candidate_model_name()
        self._embedding_authority.validate_embedder_model(model_name)
        candidate_dimensions = self._candidate_dimensions()
        self._embedding_authority.validate_embedder_dimensions(candidate_dimensions)

    def _candidate_model_name(self) -> str | None:
        model_name = getattr(self._embedder, "model_name", None)
        if isinstance(model_name, str) and model_name.strip():
            return model_name.strip()
        private_name = getattr(self._embedder, "_model_name", None)
        if isinstance(private_name, str) and private_name.strip():
            return private_name.strip()
        return None

    def _candidate_dimensions(self) -> int | None:
        dimensions = getattr(self._embedder, "dimensions", None)
        if isinstance(dimensions, int) and not isinstance(dimensions, bool):
            return dimensions
        private_dimensions = getattr(self._embedder, "DIMENSIONS", None)
        if isinstance(private_dimensions, int) and not isinstance(private_dimensions, bool):
            return private_dimensions
        return None

    @staticmethod
    def _payload_dict(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return value
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                return {}
            return parsed if isinstance(parsed, dict) else {}
        return {}

    def _row_to_intent(self, row: Any) -> MaintenanceIntent:
        return MaintenanceIntent(
            intent_id=int(row["intent_id"]),
            intent_kind=str(row["intent_kind"]),
            subject_kind=str(row["subject_kind"]),
            subject_id=str(row["subject_id"]) if row["subject_id"] is not None else None,
            policy_key=str(row["policy_key"]) if row["policy_key"] is not None else None,
            fingerprint=str(row["fingerprint"]),
            priority=int(row["priority"]),
            payload=self._payload_dict(row["payload"]),
            attempt_count=int(row["attempt_count"]),
            max_attempts=int(row["max_attempts"]),
        )
