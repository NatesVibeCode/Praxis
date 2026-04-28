"""Focused tests for deterministic database maintenance."""
from __future__ import annotations

import importlib.util
import hashlib
import json
import sys as _sys
import uuid
from pathlib import Path

import pytest

from _pg_test_conn import get_test_env
from storage.postgres import SyncPostgresConnection, get_workflow_pool
from storage.postgres.vector_store import PostgresVectorStore
from runtime.embedding_service import (
    EmbeddingService,
    resolve_embedding_runtime_authority,
)


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
_RUNTIME_DIR = _WORKFLOW_ROOT / "runtime"
_MIGRATION_PATH = (
    _WORKFLOW_ROOT.parent
    / "Databases"
    / "migrations"
    / "workflow"
    / "064_self_maintaining_memory_authority.sql"
)
_DAILY_REVIEW_MIGRATION_PATH = (
    _WORKFLOW_ROOT.parent
    / "Databases"
    / "migrations"
    / "workflow"
    / "065_daily_maintenance_review_dispatch.sql"
)
_SIGNAL_CONTROL_MIGRATION_PATH = (
    _WORKFLOW_ROOT.parent
    / "Databases"
    / "migrations"
    / "workflow"
    / "066_maintenance_signal_control.sql"
)
_RENAME_START_TERMS_MIGRATION_PATH = (
    _WORKFLOW_ROOT.parent
    / "Databases"
    / "migrations"
    / "workflow"
    / "067_rename_maintenance_start_terms.sql"
)
_RENAME_START_CONFIG_MIGRATION_PATH = (
    _WORKFLOW_ROOT.parent
    / "Databases"
    / "migrations"
    / "workflow"
    / "068_rename_maintenance_start_config_keys.sql"
)
_EXACT_DUPLICATE_REPAIR_MIGRATION_PATH = (
    _WORKFLOW_ROOT.parent
    / "Databases"
    / "migrations"
    / "workflow"
    / "093_memory_exact_duplicate_repair.sql"
)

if str(_WORKFLOW_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_WORKFLOW_ROOT))

_TEST_CONN: SyncPostgresConnection | None = None
_EMBEDDING_AUTHORITY = resolve_embedding_runtime_authority()


def _direct_load(module_name: str, filename: str):
    path = _RUNTIME_DIR / filename
    spec = importlib.util.spec_from_file_location(module_name, str(path))
    mod = importlib.util.module_from_spec(spec)
    _sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def _shared_test_conn() -> SyncPostgresConnection:
    global _TEST_CONN
    if _TEST_CONN is None:
        _TEST_CONN = SyncPostgresConnection(
            get_workflow_pool(env=get_test_env())
        )
    return _TEST_CONN


_maintenance = _direct_load("runtime.database_maintenance", "database_maintenance.py")
DatabaseMaintenanceProcessor = _maintenance.DatabaseMaintenanceProcessor
MaintenanceIntent = _maintenance.MaintenanceIntent
SandboxCleanupTarget = _maintenance.SandboxCleanupTarget


class _FakeEmbedder:
    model_name = _EMBEDDING_AUTHORITY.model_name

    def embed_one(self, text: str) -> list[float]:
        tokens = [token for token in text.lower().split() if token]
        seed = next((token for token in tokens if token.startswith("pairtoken_")), text.lower())
        values: list[float] = []
        digest = hashlib.sha256(seed.encode("utf-8")).digest()
        while len(values) < 384:
            for byte in digest:
                values.append((byte / 127.5) - 1.0)
                if len(values) == 384:
                    break
            digest = hashlib.sha256(digest).digest()
        return values


class _MismatchedEmbedder:
    authority = _EMBEDDING_AUTHORITY
    model_name = "not-the-authority-model"
    dimensions = _EMBEDDING_AUTHORITY.dimensions

    def embed_one(self, text: str) -> list[float]:
        return [0.0] * self.dimensions


class _AvailabilityConn:
    def fetchrow(self, query: str, *args):
        if "to_regclass('public.maintenance_intents')" in query:
            return {"regclass_name": "maintenance_intents"}
        raise AssertionError(f"unexpected fetchrow query: {query}")

    def execute(self, query: str, *args):
        return []


def test_embedding_runtime_authority_controls_service_model_dimensions_refresh_and_failure_policy() -> None:
    service = EmbeddingService()
    authority = resolve_embedding_runtime_authority()

    assert service.authority == authority
    assert service.model_name == authority.model_name
    assert service.dimensions == authority.dimensions == 384
    assert authority.refresh_follow_on_enabled is True
    assert authority.refresh_trigger_intent_kinds == ("embed_entity",)
    assert authority.refresh_follow_on_intent_kind == "refresh_vector_neighbors"
    assert authority.refresh_follow_on_batch_limit == 25
    assert authority.should_drain_follow_on_refresh(["embed_entity"]) is True
    assert authority.should_drain_follow_on_refresh(["archive_stale_entities"]) is False
    assert authority.missing_embedder_mode == "skip"
    assert authority.missing_embedder_reason == "embedder_unavailable"


def test_embedding_service_constructor_can_override_model_and_dimensions() -> None:
    service = EmbeddingService(model_name="custom-embedder", dimensions=512)

    assert service.model_name == "custom-embedder"
    assert service.dimensions == 512
    assert service.authority.model_name == "custom-embedder"
    assert service.authority.dimensions == 512


def test_vector_store_rejects_embedder_contract_mismatch() -> None:
    with pytest.raises(RuntimeError, match="embedding_model_mismatch"):
        PostgresVectorStore(_AvailabilityConn(), _MismatchedEmbedder())


def test_execute_intent_routes_start_and_dispatch_maintenance_intents(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor = DatabaseMaintenanceProcessor(_AvailabilityConn(), embedder=_FakeEmbedder())
    seen: list[str] = []

    def _fake_review(intent: MaintenanceIntent) -> dict[str, object]:
        seen.append(intent.intent_kind)
        return {"status": "completed", "message": "review-handler", "outcome": {}}

    def _fake_repair(intent: MaintenanceIntent) -> dict[str, object]:
        seen.append(intent.intent_kind)
        return {"status": "completed", "message": "repair-handler", "outcome": {}}

    def _fake_sandbox_cleanup(intent: MaintenanceIntent) -> dict[str, object]:
        seen.append(intent.intent_kind)
        return {"status": "completed", "message": "sandbox-cleanup-handler", "outcome": {}}

    monkeypatch.setattr(processor, "_process_start_maintenance_review", _fake_review)
    monkeypatch.setattr(processor, "_process_start_maintenance_repair", _fake_repair)
    monkeypatch.setattr(
        processor,
        "_process_reconcile_sandbox_session_cleanup",
        _fake_sandbox_cleanup,
    )

    start_review = MaintenanceIntent(1, "start_maintenance_review", "system", "policy:review", None, "f1", 100, {}, 0, 3)
    dispatch_review = MaintenanceIntent(2, "dispatch_maintenance_review", "system", "policy:review", None, "f2", 100, {}, 0, 3)
    start_repair = MaintenanceIntent(3, "start_maintenance_repair", "system", "policy:repair", None, "f3", 100, {}, 0, 3)
    dispatch_repair = MaintenanceIntent(4, "dispatch_maintenance_repair", "system", "policy:repair", None, "f4", 100, {}, 0, 3)
    sandbox_cleanup = MaintenanceIntent(5, "reconcile_sandbox_session_cleanup", "sandbox_session", "policy:sandbox", None, "f5", 100, {}, 0, 3)

    assert processor._execute_intent(start_review)["message"] == "review-handler"
    assert processor._execute_intent(dispatch_review)["message"] == "review-handler"
    assert processor._execute_intent(start_repair)["message"] == "repair-handler"
    assert processor._execute_intent(dispatch_repair)["message"] == "repair-handler"
    assert processor._execute_intent(sandbox_cleanup)["message"] == "sandbox-cleanup-handler"
    assert seen == [
        "start_maintenance_review",
        "dispatch_maintenance_review",
        "start_maintenance_repair",
        "dispatch_maintenance_repair",
        "reconcile_sandbox_session_cleanup",
    ]


def test_reconcile_sandbox_session_cleanup_removes_claimed_local_root(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    processor = DatabaseMaintenanceProcessor(_AvailabilityConn(), embedder=None)
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    sandbox_root = temp_root / "praxis-docker-sandbox-alpha"
    (sandbox_root / "workspace").mkdir(parents=True)
    (sandbox_root / "workspace" / "stale.txt").write_text("stale", encoding="utf-8")
    recorded: list[tuple[str, str, dict[str, object], str | None]] = []
    events: list[dict[str, object]] = []

    monkeypatch.setattr(_maintenance.tempfile, "gettempdir", lambda: str(temp_root))
    monkeypatch.setattr(processor, "_table_exists", lambda table_name: table_name == "sandbox_sessions")
    monkeypatch.setattr(
        processor,
        "_fetch_policy",
        lambda policy_key: {
            "policy_key": policy_key,
            "config": {"batch_limit": 10, "claim_timeout_seconds": 600},
        },
    )
    monkeypatch.setattr(processor, "_mark_expired_sandbox_sessions_closed", lambda: 2)
    monkeypatch.setattr(
        processor,
        "_claim_due_sandbox_cleanup_sessions",
        lambda *, limit, claim_timeout_seconds: [
            SandboxCleanupTarget(
                sandbox_session_id="sandbox-session-1",
                sandbox_root=str(sandbox_root / "workspace"),
                cleanup_attempt_count=1,
                closed_at=None,
                expires_at=None,
            )
        ],
    )
    monkeypatch.setattr(
        processor,
        "_record_sandbox_cleanup_result",
        lambda sandbox_session_id, *, status, outcome, error_message=None: recorded.append(
            (sandbox_session_id, status, outcome, error_message)
        ),
    )
    monkeypatch.setattr(
        processor,
        "_emit_system_event",
        lambda **kwargs: events.append(kwargs),
    )

    outcome = processor._process_reconcile_sandbox_session_cleanup(
        MaintenanceIntent(
            intent_id=50,
            intent_kind="reconcile_sandbox_session_cleanup",
            subject_kind="sandbox_session",
            subject_id="policy:sandbox",
            policy_key="sandbox_session.cleanup_reconcile",
            fingerprint="sandbox-cleanup:test",
            priority=100,
            payload={},
            attempt_count=0,
            max_attempts=5,
        )
    )

    assert outcome["status"] == "completed"
    assert outcome["message"] == "sandbox_cleanup:removed=1:already_absent=0:skipped=0:failed=0"
    assert not sandbox_root.exists()
    assert recorded == [
        (
            "sandbox-session-1",
            "completed",
            {
                "reason": "removed",
                "cleanup_root": str(sandbox_root),
                "sandbox_root": str(sandbox_root / "workspace"),
                "cleanup_attempt_count": 1,
            },
            None,
        )
    ]
    assert events and events[-1]["event_type"] == "maintenance.sandbox_cleanup.reconciled"
    assert events[-1]["payload"]["removed_count"] == 1
    assert events[-1]["payload"]["expired_closed"] == 2


def test_reconcile_sandbox_session_cleanup_records_skips_and_failures(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    processor = DatabaseMaintenanceProcessor(_AvailabilityConn(), embedder=None)
    temp_root = tmp_path / "temp"
    temp_root.mkdir()
    stubborn_root = temp_root / "praxis-docker-sandbox-stubborn"
    stubborn_root.mkdir()
    recorded: list[tuple[str, str, dict[str, object], str | None]] = []

    monkeypatch.setattr(_maintenance.tempfile, "gettempdir", lambda: str(temp_root))
    monkeypatch.setattr(processor, "_table_exists", lambda table_name: table_name == "sandbox_sessions")
    monkeypatch.setattr(
        processor,
        "_fetch_policy",
        lambda policy_key: {
            "policy_key": policy_key,
            "config": {"batch_limit": 10, "claim_timeout_seconds": 600},
        },
    )
    monkeypatch.setattr(processor, "_mark_expired_sandbox_sessions_closed", lambda: 0)
    monkeypatch.setattr(
        processor,
        "_claim_due_sandbox_cleanup_sessions",
        lambda *, limit, claim_timeout_seconds: [
            SandboxCleanupTarget(
                sandbox_session_id="sandbox-session-skip",
                sandbox_root=str(tmp_path / "outside-sandbox-root"),
                cleanup_attempt_count=2,
                closed_at=None,
                expires_at=None,
            ),
            SandboxCleanupTarget(
                sandbox_session_id="sandbox-session-fail",
                sandbox_root=str(stubborn_root),
                cleanup_attempt_count=3,
                closed_at=None,
                expires_at=None,
            ),
        ],
    )
    monkeypatch.setattr(
        processor,
        "_record_sandbox_cleanup_result",
        lambda sandbox_session_id, *, status, outcome, error_message=None: recorded.append(
            (sandbox_session_id, status, outcome, error_message)
        ),
    )
    real_rmtree = _maintenance.shutil.rmtree

    def _fake_rmtree(path, *args, **kwargs):
        target = Path(path)
        if target == stubborn_root:
            raise OSError("permission denied")
        return real_rmtree(path, *args, **kwargs)

    monkeypatch.setattr(_maintenance.shutil, "rmtree", _fake_rmtree)

    outcome = processor._process_reconcile_sandbox_session_cleanup(
        MaintenanceIntent(
            intent_id=51,
            intent_kind="reconcile_sandbox_session_cleanup",
            subject_kind="sandbox_session",
            subject_id="policy:sandbox",
            policy_key="sandbox_session.cleanup_reconcile",
            fingerprint="sandbox-cleanup:test-failure",
            priority=100,
            payload={},
            attempt_count=0,
            max_attempts=5,
        )
    )

    assert outcome["status"] == "completed"
    assert outcome["message"] == "sandbox_cleanup:removed=0:already_absent=0:skipped=1:failed=1"
    assert stubborn_root.exists()
    assert recorded == [
        (
            "sandbox-session-skip",
            "skipped",
            {
                "reason": "unsupported_sandbox_root",
                "cleanup_root": None,
                "sandbox_root": str(tmp_path / "outside-sandbox-root"),
                "cleanup_attempt_count": 2,
            },
            None,
        ),
        (
            "sandbox-session-fail",
            "failed",
            {
                "reason": "delete_failed",
                "cleanup_root": str(stubborn_root),
                "sandbox_root": str(stubborn_root),
                "cleanup_attempt_count": 3,
            },
            "permission denied",
        ),
    ]


def test_run_once_drains_follow_on_vector_refreshes_after_embedding(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor = DatabaseMaintenanceProcessor(_AvailabilityConn(), embedder=_FakeEmbedder())
    assert processor._embedding_authority.should_drain_follow_on_refresh(["embed_entity"]) is True
    embed_intent = MaintenanceIntent(
        101,
        "embed_entity",
        "memory_entity",
        "entity-1",
        None,
        "embed:entity-1",
        100,
        {},
        0,
        5,
    )
    refresh_intent = MaintenanceIntent(
        102,
        "refresh_vector_neighbors",
        "memory_entity",
        "entity-1",
        None,
        "refresh:entity-1",
        80,
        {},
        0,
        5,
    )
    claim_calls: list[tuple[str, int]] = []
    completed_ids: list[int] = []

    monkeypatch.setattr(processor, "enqueue_due_policies", lambda: 0)
    monkeypatch.setattr(
        processor,
        "claim_pending",
        lambda limit=25: claim_calls.append(("pending", limit)) or [embed_intent],
    )
    monkeypatch.setattr(
        processor,
        "claim_pending_by_kind",
        lambda intent_kind, *, limit=25: claim_calls.append((intent_kind, limit)) or (
            [refresh_intent] if intent_kind == "refresh_vector_neighbors" else []
        ),
    )
    monkeypatch.setattr(
        processor,
        "_execute_intent",
        lambda intent: {
            101: {"status": "completed", "message": "embedded_entity:entity-1", "outcome": {}},
            102: {"status": "completed", "message": "vector_neighbors:entity-1:3", "outcome": {}},
        }[intent.intent_id],
    )
    monkeypatch.setattr(
        processor,
        "_mark_completed",
        lambda intent_id, outcome: completed_ids.append(intent_id),
    )
    monkeypatch.setattr(
        processor,
        "_mark_skipped",
        lambda intent_id, outcome: pytest.fail(f"unexpected skipped intent {intent_id}"),
    )
    monkeypatch.setattr(
        processor,
        "_retry_or_fail",
        lambda intent, error_message: pytest.fail(f"unexpected retry for {intent.intent_id}: {error_message}"),
    )

    run = processor.run_once(limit=7)

    assert run.claimed == 2
    assert run.completed == 2
    assert run.failed == 0
    assert run.errors == ()
    assert run.findings == (
        "embed_entity:embedded_entity:entity-1",
        "refresh_vector_neighbors:vector_neighbors:entity-1:3",
    )
    assert claim_calls == [("pending", 7), ("refresh_vector_neighbors", 7)]
    assert completed_ids == [101, 102]


def test_run_once_skips_embedding_intents_when_embedder_is_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    processor = DatabaseMaintenanceProcessor(_AvailabilityConn(), embedder=None)
    intent = MaintenanceIntent(
        201,
        "embed_entity",
        "memory_entity",
        "entity-2",
        None,
        "embed:entity-2",
        100,
        {},
        0,
        5,
    )

    monkeypatch.setattr(processor, "enqueue_due_policies", lambda: 0)
    monkeypatch.setattr(
        processor,
        "claim_pending",
        lambda limit=25: [intent],
    )

    run = processor.run_once(limit=1)

    assert processor._embedding_authority.missing_embedder_mode == "skip"
    assert run.claimed == 1
    assert run.completed == 0
    assert run.skipped == 1
    assert run.failed == 0
    assert run.errors == ()
    assert run.findings == ("embed_entity:embedder_unavailable:entity-2",)


_MIGRATION_PATHS = (
    _MIGRATION_PATH,
    _DAILY_REVIEW_MIGRATION_PATH,
    _SIGNAL_CONTROL_MIGRATION_PATH,
    _RENAME_START_TERMS_MIGRATION_PATH,
    _RENAME_START_CONFIG_MIGRATION_PATH,
    _EXACT_DUPLICATE_REPAIR_MIGRATION_PATH,
)


def _apply_migration(conn) -> None:
    # Migrations 064-068 ship as a set. On a populated dev database the bulk
    # UPDATEs (6k+ entities, 14k+ intents) exceed _run_sync's 30s timeout per
    # call.  Check the final schema marker and skip entirely when present;
    # otherwise concatenate all five into a single execute_script call so there
    # is one async round-trip instead of five.
    base_applied = conn.fetchval(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns "
        "WHERE table_name='memory_entities' AND column_name='source_hash')"
    )
    if base_applied:
        exact_duplicate_repair_applied = conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM maintenance_policies "
            "WHERE policy_key = 'memory_entity.archive_exact_duplicates')"
        )
        if exact_duplicate_repair_applied:
            return
        conn.execute_script(_EXACT_DUPLICATE_REPAIR_MIGRATION_PATH.read_text(encoding="utf-8"))
        return
    combined = "\n".join(p.read_text(encoding="utf-8") for p in _MIGRATION_PATHS)
    conn.execute_script(combined)


def _cleanup_subject(conn, entity_ids: list[str], constraint_ids: list[str]) -> None:
    if entity_ids:
        conn.execute(
            "DELETE FROM memory_vector_neighbors WHERE source_entity_id = ANY($1::text[]) OR target_entity_id = ANY($1::text[])",
            entity_ids,
        )
        conn.execute(
            "DELETE FROM memory_inferred_edges WHERE source_id = ANY($1::text[]) OR target_id = ANY($1::text[])",
            entity_ids,
        )
        conn.execute(
            "DELETE FROM memory_edges WHERE source_id = ANY($1::text[]) OR target_id = ANY($1::text[])",
            entity_ids,
        )
        conn.execute(
            "DELETE FROM maintenance_intents WHERE subject_id = ANY($1::text[])",
            entity_ids,
        )
        conn.execute(
            "DELETE FROM memory_entities WHERE id = ANY($1::text[])",
            entity_ids,
        )
    if constraint_ids:
        conn.execute(
            "DELETE FROM maintenance_intents WHERE subject_id = ANY($1::text[])",
            constraint_ids,
        )
        conn.execute(
            "DELETE FROM workflow_constraints WHERE constraint_id = ANY($1::text[])",
            constraint_ids,
        )
    conn.execute(
        """
        DELETE FROM maintenance_intents
        WHERE intent_kind IN (
                'start_maintenance_review',
                'start_maintenance_repair',
                'archive_exact_duplicate_entities'
            )
           OR subject_id = 'policy:system.maintenance_review.daily'
           OR subject_id = 'policy:system.maintenance_repair.auto'
        """
    )
    conn.execute(
        """
        DELETE FROM system_events
        WHERE event_type IN (
            'maintenance.review.started',
            'maintenance.review.skipped',
            'maintenance.repair.started',
            'maintenance.repair.skipped'
        )
        """
    )
    conn.execute(
        """
        UPDATE maintenance_policies
        SET config = jsonb_build_object(
                'agent_slug', 'openai/gpt-5.4-mini',
                'workflow_name', 'maintenance_daily_review',
                'job_label', 'maintenance_review',
                'workspace_ref', 'praxis',
                'runtime_profile_ref', 'praxis',
                'task_type', 'ops_review',
                'start_if_clean', false,
                'start_max_attempts', 2,
                'repeat_start_seconds', 259200,
                'thresholds', jsonb_build_object(
                    'review', jsonb_build_object(
                        'pending_total', 10,
                        'failed_total', 1,
                        'oldest_pending_seconds', 3600,
                        'review_queue_pending_total', 5,
                        'entities_needing_reembed', 25
                    ),
                    'repair', jsonb_build_object(
                        'pending_total', 50,
                        'failed_total', 1,
                        'oldest_pending_seconds', 21600,
                        'review_queue_pending_total', 20,
                        'entities_needing_reembed', 100
                    )
                )
            ),
            priority = 120,
            last_enqueued_at = NULL,
            last_run_at = NULL,
            last_state_fingerprint = NULL,
            last_start_fingerprint = NULL,
            last_started_at = NULL,
            last_dirty_at = NULL,
            last_clean_at = NULL,
            last_workflow_run_id = NULL,
            last_evaluation = '{}'::jsonb,
            updated_at = now()
        WHERE policy_key = 'system.maintenance_review.daily'
        """
    )
    conn.execute(
        """
        UPDATE maintenance_policies
        SET config = jsonb_build_object(
                'agent_slug', 'openai/gpt-5.4',
                'workflow_name', 'maintenance_auto_repair',
                'job_label', 'maintenance_repair',
                'workspace_ref', 'praxis',
                'runtime_profile_ref', 'praxis',
                'task_type', 'ops_repair',
                'start_if_clean', false,
                'start_max_attempts', 2,
                'repeat_start_seconds', 43200,
                'thresholds', jsonb_build_object(
                    'review', jsonb_build_object(
                        'pending_total', 10,
                        'failed_total', 1,
                        'oldest_pending_seconds', 3600,
                        'review_queue_pending_total', 5,
                        'entities_needing_reembed', 25
                    ),
                    'repair', jsonb_build_object(
                        'pending_total', 50,
                        'failed_total', 1,
                        'oldest_pending_seconds', 21600,
                        'review_queue_pending_total', 20,
                        'entities_needing_reembed', 100
                    )
                )
            ),
            priority = 130,
            last_enqueued_at = NULL,
            last_run_at = NULL,
            last_state_fingerprint = NULL,
            last_start_fingerprint = NULL,
            last_started_at = NULL,
            last_dirty_at = NULL,
            last_clean_at = NULL,
            last_workflow_run_id = NULL,
            last_evaluation = '{}'::jsonb,
            updated_at = now()
        WHERE policy_key = 'system.maintenance_repair.auto'
        """
    )
    conn.execute(
        """
        UPDATE maintenance_policies
        SET last_enqueued_at = NULL,
            last_run_at = NULL,
            updated_at = now()
        WHERE policy_key = 'memory_entity.archive_exact_duplicates'
        """
    )


def _claim_only(processor, *intent_kinds: str, enqueue: bool = False) -> None:
    if not enqueue:
        processor.enqueue_due_policies = lambda: 0

    def _claim(limit: int = 25):
        remaining = limit
        claimed = []
        for intent_kind in intent_kinds:
            if remaining <= 0:
                break
            rows = processor.claim_pending_by_kind(intent_kind, limit=remaining)
            claimed.extend(rows)
            remaining -= len(rows)
        return claimed

    processor.claim_pending = _claim


def test_entity_embedding_intents_materialize_vector_neighbors() -> None:
    conn = _shared_test_conn()
    _apply_migration(conn)

    suffix = uuid.uuid4().hex[:8]
    pair_token = f"pairtoken_{suffix}"
    entity_ids = [f"maintenance_entity_alpha_{suffix}", f"maintenance_entity_beta_{suffix}"]

    try:
        for idx, entity_id in enumerate(entity_ids, start=1):
            conn.execute(
                """
                INSERT INTO memory_entities (
                    id, entity_type, name, content, metadata, source, confidence,
                    archived, created_at, updated_at
                ) VALUES (
                    $1, 'fact', $2, $3, $4::jsonb, 'test_suite', 0.9, false, now(), now()
                )
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    content = EXCLUDED.content,
                    metadata = EXCLUDED.metadata,
                    archived = false,
                    updated_at = now()
                """,
                entity_id,
                f"{pair_token} alpha node {idx}",
                f"{pair_token} alpha content {idx}",
                json.dumps({"test_case": suffix}),
            )

        pending_rows = conn.execute(
            """
            SELECT subject_id
            FROM maintenance_intents
            WHERE intent_kind = 'embed_entity'
              AND subject_id = ANY($1::text[])
              AND status = 'pending'
            """,
            entity_ids,
        )
        assert {row["subject_id"] for row in pending_rows} == set(entity_ids)
        conn.execute(
            """
            UPDATE maintenance_intents
            SET priority = 10000,
                updated_at = now()
            WHERE intent_kind = 'embed_entity'
              AND subject_id = ANY($1::text[])
              AND status = 'pending'
            """,
            entity_ids,
        )

        processor = DatabaseMaintenanceProcessor(conn, embedder=_FakeEmbedder())
        _claim_only(processor, "embed_entity")
        first_run = processor.run_once(limit=10)
        # The follow-on drain may already process refresh intents in the
        # first cycle.  Boost any remaining pending refresh intents and
        # run a second cycle to cover both paths.
        conn.execute(
            """
            UPDATE maintenance_intents
            SET priority = 10000,
                updated_at = now()
            WHERE intent_kind = 'refresh_vector_neighbors'
              AND subject_id = ANY($1::text[])
              AND status = 'pending'
            """,
            entity_ids,
        )
        second_run = processor.run_once(limit=10)

        total_completed = first_run.completed + second_run.completed
        assert total_completed >= 4, (
            f"expected >= 4 completions (2 embed + 2 refresh), got {total_completed} "
            f"(first={first_run.completed}, second={second_run.completed})"
        )

        embedded_rows = conn.execute(
            """
            SELECT id, embedding_status, embedding IS NOT NULL AS has_embedding
            FROM memory_entities
            WHERE id = ANY($1::text[])
            ORDER BY id
            """,
            entity_ids,
        )
        assert all(row["has_embedding"] for row in embedded_rows)
        assert all(row["embedding_status"] == "ready" for row in embedded_rows)

        neighbor_rows = conn.execute(
            """
            SELECT source_entity_id, target_entity_id, similarity
            FROM memory_vector_neighbors
            WHERE source_entity_id = ANY($1::text[])
            ORDER BY source_entity_id, rank
            """,
            entity_ids,
        )
        assert neighbor_rows
        assert {row["source_entity_id"] for row in neighbor_rows} == set(entity_ids)
        assert all(float(row["similarity"]) >= 0.8 for row in neighbor_rows)

        inferred_rows = conn.execute(
            """
            SELECT relation_type, inference_kind
            FROM memory_inferred_edges
            WHERE source_id = ANY($1::text[])
            """,
            entity_ids,
        )
        assert inferred_rows
        assert {row["relation_type"] for row in inferred_rows} == {"semantic_neighbor"}
        assert {row["inference_kind"] for row in inferred_rows} == {"vector_neighbor"}
    finally:
        _cleanup_subject(conn, entity_ids=entity_ids, constraint_ids=[])


def test_constraint_embedding_intent_backfills_vector() -> None:
    conn = _shared_test_conn()
    _apply_migration(conn)

    constraint_id = f"maintenance_constraint_{uuid.uuid4().hex[:8]}"
    try:
        conn.execute(
            """
            INSERT INTO workflow_constraints (
                constraint_id, pattern, constraint_text, confidence,
                mined_from_jobs, scope_prefix, created_at
            ) VALUES (
                $1, 'ImportError', 'repair imports before execution', 0.9,
                'job_a,job_b', 'tests/', now()
            )
            ON CONFLICT (constraint_id) DO UPDATE SET
                pattern = EXCLUDED.pattern,
                constraint_text = EXCLUDED.constraint_text,
                confidence = EXCLUDED.confidence
            """,
            constraint_id,
        )

        pending_rows = conn.execute(
            """
            SELECT subject_id
            FROM maintenance_intents
            WHERE intent_kind = 'embed_constraint'
              AND subject_id = $1
              AND status = 'pending'
            """,
            constraint_id,
        )
        assert pending_rows
        conn.execute(
            """
            UPDATE maintenance_intents
            SET priority = 10000,
                updated_at = now()
            WHERE intent_kind = 'embed_constraint'
              AND subject_id = $1
              AND status = 'pending'
            """,
            constraint_id,
        )

        processor = DatabaseMaintenanceProcessor(conn, embedder=_FakeEmbedder())
        _claim_only(processor, "embed_constraint")
        run = processor.run_once(limit=10)
        assert run.completed >= 1

        constraint_rows = conn.execute(
            """
            SELECT embedding IS NOT NULL AS has_embedding
            FROM workflow_constraints
            WHERE constraint_id = $1
            """,
            constraint_id,
        )
        assert constraint_rows and constraint_rows[0]["has_embedding"]
    finally:
        _cleanup_subject(conn, entity_ids=[], constraint_ids=[constraint_id])


def test_exact_duplicate_intent_archives_redundant_entities_and_rehomes_edges() -> None:
    conn = _shared_test_conn()
    _apply_migration(conn)

    suffix = uuid.uuid4().hex[:8]
    canonical_id = f"dup_canonical_{suffix}"
    duplicate_id = f"dup_shadow_{suffix}"
    source_id = f"dup_source_{suffix}"
    target_id = f"dup_target_{suffix}"
    intent_id = None

    try:
        for entity_id in (canonical_id, duplicate_id):
            conn.execute(
                """
                INSERT INTO memory_entities (
                    id, entity_type, name, content, metadata, source, confidence,
                    archived, created_at, updated_at
                ) VALUES (
                    $1, 'fact', $2, $3, $4::jsonb, 'test_suite', 0.9, false, now(), now()
                )
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    content = EXCLUDED.content,
                    metadata = EXCLUDED.metadata,
                    archived = false,
                    updated_at = now()
                """,
                entity_id,
                f"exact duplicate {suffix}",
                f"same content {suffix}",
                json.dumps({"test_case": suffix}),
            )
        for entity_id, name in ((source_id, "dup source"), (target_id, "dup target")):
            conn.execute(
                """
                INSERT INTO memory_entities (
                    id, entity_type, name, content, metadata, source, confidence,
                    archived, created_at, updated_at
                ) VALUES (
                    $1, 'fact', $2, $3, '{}'::jsonb, 'test_suite', 0.9, false, now(), now()
                )
                ON CONFLICT (id) DO UPDATE SET
                    name = EXCLUDED.name,
                    content = EXCLUDED.content,
                    archived = false,
                    updated_at = now()
                """,
                entity_id,
                name,
                f"{name} content {suffix}",
            )

        conn.execute(
            """
            INSERT INTO memory_edges (
                source_id, target_id, relation_type, weight, metadata, created_at
            ) VALUES (
                $1, $2, 'related_to', 0.8, '{}'::jsonb, now()
            )
            ON CONFLICT (source_id, target_id, relation_type) DO UPDATE
            SET weight = EXCLUDED.weight,
                metadata = EXCLUDED.metadata
            """,
            duplicate_id,
            target_id,
        )
        conn.execute(
            """
            INSERT INTO memory_edges (
                source_id, target_id, relation_type, weight, metadata, created_at
            ) VALUES (
                $1, $2, 'related_to', 0.7, '{}'::jsonb, now()
            )
            ON CONFLICT (source_id, target_id, relation_type) DO UPDATE
            SET weight = EXCLUDED.weight,
                metadata = EXCLUDED.metadata
            """,
            source_id,
            duplicate_id,
        )
        conn.execute(
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
            ) VALUES (
                $1, $2, 'semantic_neighbor', 'vector_neighbor', 0.91, '{}'::jsonb, 1, 1, now(), now(), true
            )
            ON CONFLICT (source_id, target_id, relation_type, inference_kind) DO UPDATE
            SET confidence = EXCLUDED.confidence,
                metadata = EXCLUDED.metadata,
                active = true,
                refreshed_at = now()
            """,
            source_id,
            duplicate_id,
        )
        conn.execute(
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
            ) VALUES (
                $1, $2, 'memory_entity.vector_neighbors', 0.92, 1, 1, now(), true
            )
            ON CONFLICT (source_entity_id, target_entity_id, policy_key) DO UPDATE
            SET similarity = EXCLUDED.similarity,
                rank = EXCLUDED.rank,
                embedding_version = EXCLUDED.embedding_version,
                refreshed_at = now(),
                active = true
            """,
            source_id,
            duplicate_id,
        )

        duplicate_intents = conn.execute(
            """
            SELECT intent_id, payload
            FROM maintenance_intents
            WHERE intent_kind = 'archive_exact_duplicate_entities'
              AND status = 'pending'
              AND payload->>'source_hash' = (
                    SELECT source_hash FROM memory_entities WHERE id = $1
              )
            ORDER BY intent_id DESC
            LIMIT 1
            """,
            canonical_id,
        )
        assert duplicate_intents
        intent_id = int(duplicate_intents[0]["intent_id"])
        payload = duplicate_intents[0]["payload"]

        processor = DatabaseMaintenanceProcessor(conn, embedder=_FakeEmbedder())
        before_summary = processor._collect_maintenance_review_summary(intent_id=-1)
        outcome = processor._process_archive_exact_duplicate_entities(
            MaintenanceIntent(
                intent_id=intent_id,
                intent_kind="archive_exact_duplicate_entities",
                subject_kind="memory_entity_duplicate_group",
                subject_id=f"exact_duplicate:fact:{suffix}",
                policy_key="memory_entity.archive_exact_duplicates",
                fingerprint=f"archive_exact_duplicate_entities:{suffix}",
                priority=10000,
                payload=payload if isinstance(payload, dict) else json.loads(payload),
                attempt_count=0,
                max_attempts=5,
            )
        )

        assert outcome["status"] == "completed"
        assert outcome["outcome"]["archived_count"] == 1

        entity_rows = conn.execute(
            """
            SELECT id, archived
            FROM memory_entities
            WHERE id = ANY($1::text[])
            ORDER BY id
            """,
            [canonical_id, duplicate_id],
        )
        archived_by_id = {row["id"]: bool(row["archived"]) for row in entity_rows}
        assert archived_by_id[canonical_id] is False
        assert archived_by_id[duplicate_id] is True

        edge_rows = conn.execute(
            """
            SELECT source_id, target_id
            FROM memory_edges
            WHERE source_id = ANY($1::text[])
               OR target_id = ANY($1::text[])
            ORDER BY source_id, target_id
            """,
            [canonical_id, duplicate_id, source_id, target_id],
        )
        assert {"source_id": canonical_id, "target_id": target_id} in [
            {"source_id": row["source_id"], "target_id": row["target_id"]}
            for row in edge_rows
        ]
        assert {"source_id": source_id, "target_id": canonical_id} in [
            {"source_id": row["source_id"], "target_id": row["target_id"]}
            for row in edge_rows
        ]
        assert all(duplicate_id not in (row["source_id"], row["target_id"]) for row in edge_rows)

        inferred_count = conn.fetchval(
            """
            SELECT COUNT(*)
            FROM memory_inferred_edges
            WHERE source_id = $1 OR target_id = $1
            """,
            duplicate_id,
        )
        neighbor_count = conn.fetchval(
            """
            SELECT COUNT(*)
            FROM memory_vector_neighbors
            WHERE source_entity_id = $1 OR target_entity_id = $1
            """,
            duplicate_id,
        )
        assert inferred_count == 0
        assert neighbor_count == 0

        after_summary = processor._collect_maintenance_review_summary(intent_id=-1)
        assert (
            after_summary["memory"]["exact_duplicate_entities"]
            <= before_summary["memory"]["exact_duplicate_entities"] - 1
        )
    finally:
        if intent_id is not None:
            conn.execute(
                "DELETE FROM maintenance_intents WHERE intent_id = $1",
                intent_id,
            )
        _cleanup_subject(
            conn,
            entity_ids=[canonical_id, duplicate_id, source_id, target_id],
            constraint_ids=[],
        )


def _reset_review_policy(conn) -> None:
    """Clear stale start_maintenance_review intents and reset the policy state.

    The enqueue_maintenance_intent ON CONFLICT path does not reset status back
    to 'pending', so a same-day re-run would skip the intent.  Delete existing
    intents and reset policy tracking columns to ensure a clean slate.
    """
    conn.execute(
        "DELETE FROM maintenance_intents "
        "WHERE intent_kind = 'archive_exact_duplicate_entities' "
        "   OR ("
        "        intent_kind IN ('start_maintenance_review', 'start_maintenance_repair') "
        "    AND subject_id LIKE 'policy:system.maintenance_%'"
        "   )"
    )
    conn.execute(
        """
        UPDATE maintenance_policies
        SET last_enqueued_at = NULL,
            last_run_at = NULL,
            last_state_fingerprint = NULL,
            last_start_fingerprint = NULL,
            last_started_at = NULL,
            last_dirty_at = NULL,
            last_clean_at = NULL,
            last_workflow_run_id = NULL,
            last_evaluation = '{}'::jsonb,
            updated_at = now()
        WHERE policy_key LIKE 'system.maintenance_%'
           OR policy_key = 'memory_entity.archive_exact_duplicates'
        """
    )


def test_daily_maintenance_review_starts_backlog_summary(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _shared_test_conn()
    _apply_migration(conn)
    _reset_review_policy(conn)

    suffix = uuid.uuid4().hex[:8]
    entity_id = f"maintenance_review_entity_{suffix}"
    captured: dict[str, object] = {}

    import runtime.control_commands as control_commands_mod

    def _fake_submit(
        conn_arg,
        *,
        requested_by_kind,
        requested_by_ref,
        inline_spec=None,
        packet_provenance=None,
        dispatch_reason=None,
        spec_name=None,
        total_jobs=None,
        **_kwargs,
    ):
        captured["conn"] = conn_arg
        captured["requested_by_kind"] = requested_by_kind
        captured["requested_by_ref"] = requested_by_ref
        captured["spec_dict"] = inline_spec
        captured["packet_provenance"] = packet_provenance
        captured["dispatch_reason"] = dispatch_reason
        captured["spec_name"] = spec_name
        captured["total_jobs"] = total_jobs
        return {
            "run_id": f"workflow_maintenance_review_{suffix}",
            "status": "queued",
            "total_jobs": total_jobs,
            "spec_name": spec_name,
            "workflow_id": "workflow.maintenance.daily.review",
            "replayed_jobs": [],
        }

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _fake_submit)

    try:
        conn.execute(
            """
            UPDATE maintenance_policies
            SET priority = 10000,
                config = jsonb_build_object(
                    'agent_slug', 'openai/gpt-5.4-mini',
                    'workflow_name', 'maintenance_daily_review',
                    'job_label', 'maintenance_review',
                    'workspace_ref', 'praxis',
                    'runtime_profile_ref', 'praxis',
                    'task_type', 'ops_review',
                    'start_if_clean', false,
                    'start_max_attempts', 2,
                    'repeat_start_seconds', 259200,
                    'thresholds', jsonb_build_object(
                        'review', jsonb_build_object(
                            'pending_total', 1,
                            'failed_total', 1,
                            'oldest_pending_seconds', 1,
                            'review_queue_pending_total', 1,
                            'entities_needing_reembed', 1
                        ),
                        'repair', jsonb_build_object(
                            'pending_total', 100,
                            'failed_total', 10,
                            'oldest_pending_seconds', 100000,
                            'review_queue_pending_total', 100,
                            'entities_needing_reembed', 100
                        )
                    )
                ),
                updated_at = now()
            WHERE policy_key = 'system.maintenance_review.daily'
            """
        )
        conn.execute(
            """
            INSERT INTO memory_entities (
                id, entity_type, name, content, metadata, source, confidence,
                archived, created_at, updated_at
            ) VALUES (
                $1, 'fact', $2, $3, $4::jsonb, 'test_suite', 0.9, false, now(), now()
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                content = EXCLUDED.content,
                metadata = EXCLUDED.metadata,
                archived = false,
                updated_at = now()
            """,
            entity_id,
            f"daily review {suffix}",
            f"daily review backlog {suffix}",
            json.dumps({"test_case": "daily_review_start"}),
        )

        processor = DatabaseMaintenanceProcessor(conn, embedder=_FakeEmbedder())
        _claim_only(processor, "start_maintenance_review", enqueue=True)
        run = processor.run_once(limit=1)

        assert run.completed == 1
        assert any(
            finding.startswith(f"start_maintenance_review:maintenance_review_started:workflow_maintenance_review_{suffix}")
            for finding in run.findings
        )
        assert captured["conn"] is conn
        assert captured["requested_by_kind"] == "system"
        assert captured["requested_by_ref"] == "database_maintenance.review"
        spec_dict = captured["spec_dict"]
        assert spec_dict["name"] == "maintenance_daily_review"
        assert spec_dict["jobs"][0]["label"] == "maintenance_review"
        assert spec_dict["jobs"][0]["agent"] == "openai/gpt-5.4-mini"
        assert '"pending_by_kind": {' in spec_dict["jobs"][0]["prompt"]
        assert '"review_needed": true' in spec_dict["jobs"][0]["prompt"]
        assert captured["dispatch_reason"] == "maintenance.review.auto"
        assert captured["spec_name"] == "maintenance_daily_review"
        assert captured["total_jobs"] == 1

        intent_rows = conn.execute(
            """
            SELECT status, outcome
            FROM maintenance_intents
            WHERE intent_kind = 'start_maintenance_review'
            ORDER BY intent_id DESC
            LIMIT 1
            """
        )
        assert intent_rows and intent_rows[0]["status"] == "completed"
        outcome = intent_rows[0]["outcome"]
        if isinstance(outcome, str):
            outcome = json.loads(outcome)
        assert outcome["workflow_run_id"] == f"workflow_maintenance_review_{suffix}"

        event_rows = conn.execute(
            """
            SELECT event_type, source_id
            FROM system_events
            WHERE event_type = 'maintenance.review.started'
              AND source_id = $1
            ORDER BY id DESC
            LIMIT 1
            """,
            f"workflow_maintenance_review_{suffix}",
        )
        assert event_rows and event_rows[0]["event_type"] == "maintenance.review.started"

        policy_rows = conn.execute(
            """
            SELECT last_start_fingerprint, last_workflow_run_id, last_evaluation
            FROM maintenance_policies
            WHERE policy_key = 'system.maintenance_review.daily'
            """
        )
        assert policy_rows and policy_rows[0]["last_start_fingerprint"]
        assert policy_rows[0]["last_workflow_run_id"] == f"workflow_maintenance_review_{suffix}"
    finally:
        _cleanup_subject(conn, entity_ids=[entity_id], constraint_ids=[])


def test_daily_maintenance_review_can_skip_when_policy_is_clean_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _shared_test_conn()
    _apply_migration(conn)

    processor = DatabaseMaintenanceProcessor(conn, embedder=_FakeEmbedder())
    intent = _maintenance.MaintenanceIntent(
        intent_id=999999,
        intent_kind="start_maintenance_review",
        subject_kind="system",
        subject_id="policy:system.maintenance_review.daily",
        policy_key="system.maintenance_review.daily",
        fingerprint="start_maintenance_review:test",
        priority=10000,
        payload={},
        attempt_count=0,
        max_attempts=3,
    )

    import runtime.control_commands as control_commands_mod

    def _unexpected_submit(*args, **kwargs):
        raise AssertionError("start should not run for a clean-only skip")

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _unexpected_submit)

    monkeypatch.setattr(
        processor,
        "_collect_maintenance_review_summary",
        lambda *, intent_id: {
            "generated_at": "2026-04-09T00:00:00+00:00",
            "maintenance_intents": {
                "pending_total": 0,
                "failed_total": 0,
                "oldest_pending_seconds": 0,
                "pending_by_kind": {},
                "failed_by_kind": {},
            },
            "review_queue": {"pending_total": 0},
            "memory": {
                "entities_needing_reembed": 0,
                "active_vector_neighbors": 0,
            },
        },
    )
    monkeypatch.setattr(
        processor,
        "_fetch_policy",
        lambda policy_key: {
            "policy_key": policy_key,
            "priority": 120,
            "cadence_seconds": 86400,
            "max_attempts": 3,
            "config": {
                "agent_slug": "openai/gpt-5.4-mini",
                "start_if_clean": False,
            },
        },
    )

    outcome = processor._process_start_maintenance_review(intent)
    assert outcome["status"] == "skipped"
    assert outcome["outcome"]["reason"] == "maintenance_review_not_needed"


def test_evaluate_maintenance_state_marks_exact_duplicate_entities_dirty() -> None:
    processor = DatabaseMaintenanceProcessor(_AvailabilityConn(), embedder=None)
    summary = {
        "generated_at": "2026-04-09T00:00:00+00:00",
        "maintenance_intents": {
            "pending_total": 0,
            "failed_total": 0,
            "oldest_pending_seconds": 0,
            "pending_by_kind": {},
            "failed_by_kind": {},
        },
        "review_queue": {"pending_total": 0},
        "memory": {
            "entities_needing_reembed": 0,
            "active_vector_neighbors": 0,
            "exact_duplicate_groups": 2,
            "exact_duplicate_entities": 7,
        },
    }
    config = {
        "thresholds": {
            "review": {"exact_duplicate_entities": 5},
            "repair": {"exact_duplicate_entities": 10},
        }
    }

    evaluation = processor._evaluate_maintenance_state(summary=summary, config=config)

    assert evaluation["review_needed"] is True
    assert evaluation["repair_needed"] is False
    assert evaluation["metrics"]["exact_duplicate_entities"] == 7
    assert evaluation["signals"] == [
        {
            "metric": "exact_duplicate_entities",
            "value": 7,
            "threshold": 5,
            "severity": "review",
        }
    ]


def test_daily_maintenance_review_skips_same_dirty_fingerprint_within_repeat_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _shared_test_conn()
    _apply_migration(conn)

    processor = DatabaseMaintenanceProcessor(conn, embedder=_FakeEmbedder())
    intent = _maintenance.MaintenanceIntent(
        intent_id=999998,
        intent_kind="start_maintenance_review",
        subject_kind="system",
        subject_id="policy:system.maintenance_review.daily",
        policy_key="system.maintenance_review.daily",
        fingerprint="start_maintenance_review:repeat",
        priority=10000,
        payload={},
        attempt_count=0,
        max_attempts=3,
    )
    summary = {
        "generated_at": "2026-04-09T00:00:00+00:00",
        "maintenance_intents": {
            "pending_total": 12,
            "failed_total": 0,
            "oldest_pending_seconds": 7200,
            "pending_by_kind": {"embed_entity": 12},
            "failed_by_kind": {},
        },
        "review_queue": {"pending_total": 0},
        "memory": {
            "entities_needing_reembed": 0,
            "active_vector_neighbors": 0,
        },
    }
    config = {
        "agent_slug": "openai/gpt-5.4-mini",
        "start_if_clean": False,
        "repeat_start_seconds": 259200,
        "thresholds": {
            "review": {
                "pending_total": 10,
                "failed_total": 1,
                "oldest_pending_seconds": 3600,
                "review_queue_pending_total": 5,
                "entities_needing_reembed": 25,
            },
            "repair": {
                "pending_total": 50,
                "failed_total": 1,
                "oldest_pending_seconds": 21600,
                "review_queue_pending_total": 20,
                "entities_needing_reembed": 100,
            },
        },
    }
    evaluation = processor._evaluate_maintenance_state(summary=summary, config=config)
    current_time = conn.fetchrow("SELECT now() AS current_time")["current_time"]

    import runtime.control_commands as control_commands_mod

    def _unexpected_submit(*args, **kwargs):
        raise AssertionError("start should not run when the dirty fingerprint is unchanged")

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _unexpected_submit)
    monkeypatch.setattr(processor, "_collect_maintenance_review_summary", lambda *, intent_id: summary)
    monkeypatch.setattr(
        processor,
        "_fetch_policy",
        lambda policy_key: {
            "policy_key": policy_key,
            "priority": 120,
            "cadence_seconds": 86400,
            "max_attempts": 3,
            "config": config,
            "last_state_fingerprint": evaluation["state_fingerprint"],
            "last_start_fingerprint": evaluation["state_fingerprint"],
            "last_started_at": current_time,
        },
    )

    outcome = processor._process_start_maintenance_review(intent)
    assert outcome["status"] == "skipped"
    assert outcome["outcome"]["reason"] == "dirty_state_unchanged_within_repeat_window"


def test_auto_repair_starts_when_severe_dirty_state_is_detected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    conn = _shared_test_conn()
    _apply_migration(conn)
    _reset_review_policy(conn)

    suffix = uuid.uuid4().hex[:8]
    entity_id = f"maintenance_repair_entity_{suffix}"
    captured: dict[str, object] = {}

    import runtime.control_commands as control_commands_mod

    def _fake_submit(
        conn_arg,
        *,
        requested_by_kind,
        requested_by_ref,
        inline_spec=None,
        packet_provenance=None,
        dispatch_reason=None,
        spec_name=None,
        total_jobs=None,
        **_kwargs,
    ):
        captured["conn"] = conn_arg
        captured["requested_by_kind"] = requested_by_kind
        captured["requested_by_ref"] = requested_by_ref
        captured["spec_dict"] = inline_spec
        captured["packet_provenance"] = packet_provenance
        captured["dispatch_reason"] = dispatch_reason
        captured["spec_name"] = spec_name
        captured["total_jobs"] = total_jobs
        return {
            "run_id": f"workflow_maintenance_repair_{suffix}",
            "status": "queued",
            "total_jobs": total_jobs,
            "spec_name": spec_name,
            "workflow_id": "workflow.maintenance.auto.repair",
            "replayed_jobs": [],
        }

    monkeypatch.setattr(control_commands_mod, "submit_workflow_command", _fake_submit)

    try:
        conn.execute(
            """
            UPDATE maintenance_policies
            SET priority = 10000,
                config = jsonb_build_object(
                    'agent_slug', 'openai/gpt-5.4',
                    'workflow_name', 'maintenance_auto_repair',
                    'job_label', 'maintenance_repair',
                    'workspace_ref', 'praxis',
                    'runtime_profile_ref', 'praxis',
                    'task_type', 'ops_repair',
                    'start_if_clean', false,
                    'start_max_attempts', 2,
                    'repeat_start_seconds', 43200,
                    'thresholds', jsonb_build_object(
                        'review', jsonb_build_object(
                            'pending_total', 1,
                            'failed_total', 1,
                            'oldest_pending_seconds', 1,
                            'review_queue_pending_total', 1,
                            'entities_needing_reembed', 1
                        ),
                        'repair', jsonb_build_object(
                            'pending_total', 1,
                            'failed_total', 1,
                            'oldest_pending_seconds', 1,
                            'review_queue_pending_total', 1,
                            'entities_needing_reembed', 1
                        )
                    )
                ),
                updated_at = now()
            WHERE policy_key = 'system.maintenance_repair.auto'
            """
        )
        conn.execute(
            """
            INSERT INTO memory_entities (
                id, entity_type, name, content, metadata, source, confidence,
                archived, created_at, updated_at
            ) VALUES (
                $1, 'fact', $2, $3, $4::jsonb, 'test_suite', 0.9, false, now(), now()
            )
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                content = EXCLUDED.content,
                metadata = EXCLUDED.metadata,
                archived = false,
                updated_at = now()
            """,
            entity_id,
            f"daily repair {suffix}",
            f"daily repair backlog {suffix}",
            json.dumps({"test_case": "daily_repair_start"}),
        )

        processor = DatabaseMaintenanceProcessor(conn, embedder=_FakeEmbedder())
        _claim_only(processor, "start_maintenance_repair", enqueue=True)
        run = processor.run_once(limit=1)

        assert run.completed == 1
        assert any(
            finding.startswith(f"start_maintenance_repair:maintenance_repair_started:workflow_maintenance_repair_{suffix}")
            for finding in run.findings
        )
        spec_dict = captured["spec_dict"]
        assert spec_dict["name"] == "maintenance_auto_repair"
        assert spec_dict["jobs"][0]["label"] == "maintenance_repair"
        assert spec_dict["jobs"][0]["task_type"] == "ops_repair"
        assert '"severity": "repair"' in spec_dict["jobs"][0]["prompt"]

        event_rows = conn.execute(
            """
            SELECT event_type, source_id
            FROM system_events
            WHERE event_type = 'maintenance.repair.started'
              AND source_id = $1
            ORDER BY id DESC
            LIMIT 1
            """,
            f"workflow_maintenance_repair_{suffix}",
        )
        assert event_rows and event_rows[0]["event_type"] == "maintenance.repair.started"
    finally:
        _cleanup_subject(conn, entity_ids=[entity_id], constraint_ids=[])
