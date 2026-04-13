from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
import json
import uuid

import asyncpg
import pytest

from policy.domain import (
    GateDecisionKind,
    PolicyDecisionError,
    PolicyEngine,
    PromotionDecisionKind,
    PromotionDecisionRecord,
)
from policy.gate import (
    CANONICAL_TARGET_KIND,
    PROMOTION_ACCEPT_REASON,
    PROMOTION_GATE_BLOCKED,
    PROMOTION_REJECT_MISSING_PROMOTION_INTENT,
)
from runtime import RunState
from storage import migrations as workflow_migrations
from storage.migrations import WorkflowMigrationError, workflow_migration_statements
from storage.postgres import (
    PostgresConfigurationError,
    bootstrap_control_plane_schema,
    connect_workflow_database,
)

_POLICY_MIGRATION_FILENAME = "003_gate_and_promotion_policy.sql"


def _unique_suffix() -> str:
    return uuid.uuid4().hex[:10]


def _clear_workflow_migration_caches() -> None:
    workflow_migrations.workflow_migrations_root.cache_clear()
    workflow_migrations.workflow_migration_manifest.cache_clear()
    workflow_migrations.workflow_migration_sql_text.cache_clear()
    workflow_migrations.workflow_migration_statements.cache_clear()


def test_blocked_gate_has_no_promotion_bypass_path() -> None:
    asyncio.run(_exercise_blocked_gate_has_no_promotion_bypass_path())


def test_promotion_row_cannot_drift_from_gate_authority() -> None:
    asyncio.run(_exercise_promotion_row_cannot_drift_from_gate_authority())


def test_accepted_promotion_requires_explicit_freshness_and_finalization_evidence() -> None:
    asyncio.run(
        _exercise_accepted_promotion_requires_explicit_freshness_and_finalization_evidence()
    )


def test_gate_policy_migration_resolution_fails_closed_when_canonical_file_is_missing_on_cold_start(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    canonical_root = tmp_path / "workflow"
    canonical_root.mkdir()
    # Seed all canonical migrations except 003 (the one we want to detect as missing).
    for filename in workflow_migrations._WORKFLOW_MIGRATION_SEQUENCE:
        if filename != _POLICY_MIGRATION_FILENAME:
            (canonical_root / filename).write_text("SELECT 1;\n", encoding="utf-8")

    _clear_workflow_migration_caches()
    monkeypatch.setattr(
        workflow_migrations,
        "_workflow_migrations_root_path",
        lambda: canonical_root,
    )
    try:
        with pytest.raises(WorkflowMigrationError) as exc_info:
            workflow_migration_statements(_POLICY_MIGRATION_FILENAME)
    finally:
        _clear_workflow_migration_caches()

    assert exc_info.value.reason_code == "workflow.migration_manifest_incomplete"
    assert exc_info.value.details["filename"] == _POLICY_MIGRATION_FILENAME
    assert exc_info.value.details["missing_filenames"] == _POLICY_MIGRATION_FILENAME


async def _exercise_blocked_gate_has_no_promotion_bypass_path() -> None:
    suffix = _unique_suffix()
    try:
        conn = await connect_workflow_database()
    except PostgresConfigurationError as exc:
        pytest.skip(f"WORKFLOW_DATABASE_URL is required for gate policy integration test: {exc.reason_code}")
    try:
        await bootstrap_control_plane_schema(conn)
        await _apply_migration_statements(conn)
        authority = await _seed_run_authority(conn, suffix=suffix)

        decided_at = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
        engine = PolicyEngine()
        gate_evaluation = engine.evaluate_gate(
            proposal_id=f"proposal:{suffix}",
            workflow_id=authority["workflow_id"],
            run_id=authority["run_id"],
            validation_receipt_ref="",
            proposal_manifest_hash=f"sha256:proposal:{suffix}",
            validated_head_ref=f"head:{suffix}",
            target_kind=CANONICAL_TARGET_KIND,
            target_ref="repo:canonical",
            policy_snapshot_ref="policy_snapshot:proposal_gate_v1",
            decided_by="policy.gate",
            validation_passed=True,
            proposal_receipt_present=True,
            decided_at=decided_at,
        )

        assert gate_evaluation.decision is GateDecisionKind.BLOCK
        assert gate_evaluation.reason_code == "policy.gate_missing_validation_receipt"

        await _insert_gate_evaluation(conn, gate_evaluation=gate_evaluation)

        stored_gate_row = await conn.fetchrow(
            """
            SELECT decision, reason_code
            FROM gate_evaluations
            WHERE gate_evaluation_id = $1
            """,
            gate_evaluation.gate_evaluation_id,
        )
        assert stored_gate_row is not None
        assert stored_gate_row["decision"] == "block"
        assert stored_gate_row["reason_code"] == gate_evaluation.reason_code

        with pytest.raises(PolicyDecisionError) as exc_info:
            engine.decide_promotion(
                gate_evaluation=gate_evaluation,
                policy_snapshot_ref=gate_evaluation.policy_snapshot_ref,
                decided_by="policy.promote",
                current_head_ref=gate_evaluation.validated_head_ref,
                decided_at=decided_at + timedelta(seconds=1),
            )

        assert exc_info.value.reason_code == PROMOTION_GATE_BLOCKED

        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                """
                INSERT INTO promotion_decisions (
                    promotion_decision_id,
                    gate_evaluation_id,
                    proposal_id,
                    workflow_id,
                    run_id,
                    decision,
                    reason_code,
                    decided_at,
                    decided_by,
                    policy_snapshot_ref,
                    validation_receipt_ref,
                    proposal_manifest_hash,
                    validated_head_ref,
                    promotion_intent_at,
                    finalized_at,
                    canonical_commit_ref,
                    target_kind,
                    target_ref
                ) VALUES (
                    $1, $2, $3, $4, $5, 'block', $6, $7, $8, $9, $10, $11, $12,
                    NULL, NULL, NULL, $13, $14
                )
                """,
                f"promotion:{suffix}:blocked",
                gate_evaluation.gate_evaluation_id,
                gate_evaluation.proposal_id,
                gate_evaluation.workflow_id,
                gate_evaluation.run_id,
                gate_evaluation.reason_code,
                decided_at + timedelta(seconds=2),
                "policy.bypass_attempt",
                gate_evaluation.policy_snapshot_ref,
                gate_evaluation.validation_receipt_ref,
                gate_evaluation.proposal_manifest_hash,
                gate_evaluation.validated_head_ref,
                gate_evaluation.target_kind,
                gate_evaluation.target_ref,
            )

        promotion_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM promotion_decisions
            WHERE proposal_id = $1
            """,
            gate_evaluation.proposal_id,
        )
        assert promotion_count == 0
    finally:
        await conn.close()


async def _exercise_promotion_row_cannot_drift_from_gate_authority() -> None:
    suffix = _unique_suffix()
    try:
        conn = await connect_workflow_database()
    except PostgresConfigurationError as exc:
        pytest.skip(f"WORKFLOW_DATABASE_URL is required for gate policy integration test: {exc.reason_code}")
    try:
        await bootstrap_control_plane_schema(conn)
        await _apply_migration_statements(conn)
        authority = await _seed_run_authority(conn, suffix=suffix)

        decided_at = datetime(2026, 4, 1, 12, 5, tzinfo=timezone.utc)
        engine = PolicyEngine()
        gate_evaluation = engine.evaluate_gate(
            proposal_id=f"proposal:{suffix}",
            workflow_id=authority["workflow_id"],
            run_id=authority["run_id"],
            validation_receipt_ref=f"validation:{suffix}",
            proposal_manifest_hash=f"sha256:proposal:{suffix}",
            validated_head_ref=f"head:{suffix}",
            target_kind=CANONICAL_TARGET_KIND,
            target_ref="repo:canonical",
            policy_snapshot_ref="policy_snapshot:proposal_gate_v1",
            decided_by="policy.gate",
            validation_passed=True,
            proposal_receipt_present=True,
            validated_manifest_hash=f"sha256:proposal:{suffix}",
            current_head_ref=f"head:{suffix}",
            decided_at=decided_at,
        )

        assert gate_evaluation.decision is GateDecisionKind.ACCEPT
        await _insert_gate_evaluation(conn, gate_evaluation=gate_evaluation)

        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await conn.execute(
                """
                INSERT INTO promotion_decisions (
                    promotion_decision_id,
                    gate_evaluation_id,
                    proposal_id,
                    workflow_id,
                    run_id,
                    decision,
                    reason_code,
                    decided_at,
                    decided_by,
                    policy_snapshot_ref,
                    validation_receipt_ref,
                    proposal_manifest_hash,
                    validated_head_ref,
                    current_head_ref,
                    promotion_intent_at,
                    finalized_at,
                    canonical_commit_ref,
                    target_kind,
                    target_ref
                ) VALUES (
                    $1, $2, $3, $4, $5, 'reject', $6, $7, $8, $9, $10, $11, $12,
                    $13, NULL, NULL, NULL, $14, $15
                )
                """,
                f"promotion:{suffix}:drift",
                gate_evaluation.gate_evaluation_id,
                f"{gate_evaluation.proposal_id}:drift",
                gate_evaluation.workflow_id,
                gate_evaluation.run_id,
                "policy.promotion_target_mismatch",
                decided_at + timedelta(seconds=1),
                "policy.bypass_attempt",
                gate_evaluation.policy_snapshot_ref,
                gate_evaluation.validation_receipt_ref,
                gate_evaluation.proposal_manifest_hash,
                gate_evaluation.validated_head_ref,
                gate_evaluation.validated_head_ref,
                gate_evaluation.target_kind,
                "repo:drifted",
            )

        promotion_count = await conn.fetchval(
            """
            SELECT COUNT(*)
            FROM promotion_decisions
            WHERE gate_evaluation_id = $1
            """,
            gate_evaluation.gate_evaluation_id,
        )
        assert promotion_count == 0
    finally:
        await conn.close()


async def _exercise_accepted_promotion_requires_explicit_freshness_and_finalization_evidence() -> None:
    suffix = _unique_suffix()
    try:
        conn = await connect_workflow_database()
    except PostgresConfigurationError as exc:
        pytest.skip(f"WORKFLOW_DATABASE_URL is required for gate policy integration test: {exc.reason_code}")
    try:
        await bootstrap_control_plane_schema(conn)
        await _apply_migration_statements(conn)
        authority = await _seed_run_authority(conn, suffix=suffix)

        decided_at = datetime(2026, 4, 1, 12, 10, tzinfo=timezone.utc)
        engine = PolicyEngine()
        gate_evaluation = engine.evaluate_gate(
            proposal_id=f"proposal:{suffix}",
            workflow_id=authority["workflow_id"],
            run_id=authority["run_id"],
            validation_receipt_ref=f"validation:{suffix}",
            proposal_manifest_hash=f"sha256:proposal:{suffix}",
            validated_head_ref=f"head:{suffix}",
            target_kind=CANONICAL_TARGET_KIND,
            target_ref="repo:canonical",
            policy_snapshot_ref="policy_snapshot:proposal_gate_v1",
            decided_by="policy.gate",
            validation_passed=True,
            proposal_receipt_present=True,
            validated_manifest_hash=f"sha256:proposal:{suffix}",
            current_head_ref=f"head:{suffix}",
            decided_at=decided_at,
        )

        assert gate_evaluation.decision is GateDecisionKind.ACCEPT
        await _insert_gate_evaluation(conn, gate_evaluation=gate_evaluation)

        rejected_promotion = engine.decide_promotion(
            gate_evaluation=gate_evaluation,
            policy_snapshot_ref=gate_evaluation.policy_snapshot_ref,
            decided_by="policy.promote",
            current_head_ref=gate_evaluation.validated_head_ref,
            decided_at=decided_at + timedelta(seconds=1),
        )
        assert rejected_promotion.decision is PromotionDecisionKind.REJECT
        assert rejected_promotion.reason_code == PROMOTION_REJECT_MISSING_PROMOTION_INTENT
        assert rejected_promotion.current_head_ref == gate_evaluation.validated_head_ref
        assert rejected_promotion.promotion_intent_at is None
        assert rejected_promotion.finalized_at is None
        assert rejected_promotion.canonical_commit_ref is None

        with pytest.raises(asyncpg.CheckViolationError):
            await conn.execute(
                """
                INSERT INTO promotion_decisions (
                    promotion_decision_id,
                    gate_evaluation_id,
                    proposal_id,
                    workflow_id,
                    run_id,
                    decision,
                    reason_code,
                    decided_at,
                    decided_by,
                    policy_snapshot_ref,
                    validation_receipt_ref,
                    proposal_manifest_hash,
                    validated_head_ref,
                    current_head_ref,
                    promotion_intent_at,
                    finalized_at,
                    canonical_commit_ref,
                    target_kind,
                    target_ref
                ) VALUES (
                    $1, $2, $3, $4, $5, 'accept', $6, $7, $8, $9, $10, $11, $12,
                    $13, NULL, NULL, NULL, $14, $15
                )
                """,
                f"promotion:{suffix}:missing-evidence",
                gate_evaluation.gate_evaluation_id,
                gate_evaluation.proposal_id,
                gate_evaluation.workflow_id,
                gate_evaluation.run_id,
                PROMOTION_ACCEPT_REASON,
                decided_at + timedelta(seconds=2),
                "policy.bypass_attempt",
                gate_evaluation.policy_snapshot_ref,
                gate_evaluation.validation_receipt_ref,
                gate_evaluation.proposal_manifest_hash,
                gate_evaluation.validated_head_ref,
                gate_evaluation.validated_head_ref,
                gate_evaluation.target_kind,
                gate_evaluation.target_ref,
            )

        promotion_intent_at = decided_at + timedelta(seconds=3)
        finalized_at = promotion_intent_at + timedelta(seconds=1)
        accepted_promotion = engine.decide_promotion(
            gate_evaluation=gate_evaluation,
            policy_snapshot_ref=gate_evaluation.policy_snapshot_ref,
            decided_by="policy.promote",
            current_head_ref=gate_evaluation.validated_head_ref,
            promotion_intent_at=promotion_intent_at,
            finalized_at=finalized_at,
            canonical_commit_ref=f"commit:{suffix}",
            decided_at=finalized_at,
        )
        assert accepted_promotion.decision is PromotionDecisionKind.ACCEPT
        assert accepted_promotion.reason_code == PROMOTION_ACCEPT_REASON
        assert accepted_promotion.current_head_ref == gate_evaluation.validated_head_ref

        await _insert_promotion_decision(conn, promotion_decision=accepted_promotion)

        stored_promotion = await conn.fetchrow(
            """
            SELECT
                decision,
                current_head_ref,
                promotion_intent_at,
                finalized_at,
                canonical_commit_ref,
                target_ref
            FROM promotion_decisions
            WHERE promotion_decision_id = $1
            """,
            accepted_promotion.promotion_decision_id,
        )
        assert stored_promotion is not None
        assert stored_promotion["decision"] == "accept"
        assert stored_promotion["current_head_ref"] == gate_evaluation.validated_head_ref
        assert stored_promotion["promotion_intent_at"] == promotion_intent_at
        assert stored_promotion["finalized_at"] == finalized_at
        assert stored_promotion["canonical_commit_ref"] == f"commit:{suffix}"
        assert stored_promotion["target_ref"] == gate_evaluation.target_ref
    finally:
        await conn.close()


async def _apply_migration_statements(conn: asyncpg.Connection) -> None:
    async with conn.transaction():
        for statement in workflow_migration_statements(_POLICY_MIGRATION_FILENAME):
            await conn.execute(statement)


async def _insert_gate_evaluation(
    conn: asyncpg.Connection,
    *,
    gate_evaluation,
) -> None:
    await conn.execute(
        """
        INSERT INTO gate_evaluations (
            gate_evaluation_id,
            proposal_id,
            workflow_id,
            run_id,
            decision,
            reason_code,
            decided_at,
            decided_by,
            policy_snapshot_ref,
            validation_receipt_ref,
            proposal_manifest_hash,
            validated_head_ref,
            target_kind,
            target_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
        )
        """,
        gate_evaluation.gate_evaluation_id,
        gate_evaluation.proposal_id,
        gate_evaluation.workflow_id,
        gate_evaluation.run_id,
        gate_evaluation.decision.value,
        gate_evaluation.reason_code,
        gate_evaluation.decided_at,
        gate_evaluation.decided_by,
        gate_evaluation.policy_snapshot_ref,
        gate_evaluation.validation_receipt_ref,
        gate_evaluation.proposal_manifest_hash,
        gate_evaluation.validated_head_ref,
        gate_evaluation.target_kind,
        gate_evaluation.target_ref,
    )


async def _insert_promotion_decision(
    conn: asyncpg.Connection,
    *,
    promotion_decision: PromotionDecisionRecord,
) -> None:
    await conn.execute(
        """
        INSERT INTO promotion_decisions (
            promotion_decision_id,
            gate_evaluation_id,
            proposal_id,
            workflow_id,
            run_id,
            decision,
            reason_code,
            decided_at,
            decided_by,
            policy_snapshot_ref,
            validation_receipt_ref,
            proposal_manifest_hash,
            validated_head_ref,
            current_head_ref,
            promotion_intent_at,
            finalized_at,
            canonical_commit_ref,
            target_kind,
            target_ref
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $19
        )
        """,
        promotion_decision.promotion_decision_id,
        promotion_decision.gate_evaluation_id,
        promotion_decision.proposal_id,
        promotion_decision.workflow_id,
        promotion_decision.run_id,
        promotion_decision.decision.value,
        promotion_decision.reason_code,
        promotion_decision.decided_at,
        promotion_decision.decided_by,
        promotion_decision.policy_snapshot_ref,
        promotion_decision.validation_receipt_ref,
        promotion_decision.proposal_manifest_hash,
        promotion_decision.validated_head_ref,
        promotion_decision.current_head_ref,
        promotion_decision.promotion_intent_at,
        promotion_decision.finalized_at,
        promotion_decision.canonical_commit_ref,
        promotion_decision.target_kind,
        promotion_decision.target_ref,
    )


async def _seed_run_authority(
    conn: asyncpg.Connection,
    *,
    suffix: str,
) -> dict[str, str]:
    requested_at = datetime(2026, 4, 1, 11, 59, tzinfo=timezone.utc)
    admitted_at = requested_at + timedelta(seconds=1)
    workflow_id = f"workflow:{suffix}"
    request_id = f"request:{suffix}"
    workflow_definition_id = f"workflow_definition:{suffix}:v1"
    admission_decision_id = f"admission:{suffix}"
    run_id = f"run:{suffix}"
    request_envelope = {
        "schema_version": 1,
        "workflow_id": workflow_id,
        "request_id": request_id,
        "workflow_definition_id": workflow_definition_id,
        "definition_version": 1,
        "definition_hash": f"sha256:{suffix}",
        "workspace_ref": f"workspace:{suffix}",
        "runtime_profile_ref": f"runtime_profile:{suffix}",
        "nodes": [],
        "edges": [],
    }

    await conn.execute(
        """
        INSERT INTO workflow_definitions (
            workflow_definition_id,
            workflow_id,
            schema_version,
            definition_version,
            definition_hash,
            status,
            request_envelope,
            normalized_definition,
            created_at,
            supersedes_workflow_definition_id
        ) VALUES ($1, $2, 1, 1, $3, 'admitted', $4::jsonb, $4::jsonb, $5, NULL)
        ON CONFLICT (workflow_definition_id) DO NOTHING
        """,
        workflow_definition_id,
        workflow_id,
        request_envelope["definition_hash"],
        json.dumps(request_envelope, sort_keys=True, separators=(",", ":")),
        requested_at,
    )
    await conn.execute(
        """
        INSERT INTO admission_decisions (
            admission_decision_id,
            workflow_id,
            request_id,
            decision,
            reason_code,
            decided_at,
            decided_by,
            policy_snapshot_ref,
            validation_result_ref,
            authority_context_ref
        ) VALUES ($1, $2, $3, 'admit', 'policy.admission_allowed', $4, $5, $6, $7, $8)
        ON CONFLICT (admission_decision_id) DO NOTHING
        """,
        admission_decision_id,
        workflow_id,
        request_id,
        admitted_at,
        "policy.intake",
        "policy_snapshot:workflow_intake_v1",
        f"validation:{suffix}",
        f"authority:bundle:{suffix}",
    )
    await conn.execute(
        """
        INSERT INTO workflow_runs (
            run_id,
            workflow_id,
            request_id,
            request_digest,
            authority_context_digest,
            workflow_definition_id,
            admitted_definition_hash,
            run_idempotency_key,
            schema_version,
            request_envelope,
            context_bundle_id,
            admission_decision_id,
            current_state,
            terminal_reason_code,
            requested_at,
            admitted_at,
            started_at,
            finished_at,
            last_event_id
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $3, 1, $8::jsonb, $9, $10, $11, NULL,
            $12, $13, NULL, NULL, NULL
        )
        ON CONFLICT (run_id) DO NOTHING
        """,
        run_id,
        workflow_id,
        request_id,
        f"digest:{suffix}",
        f"authority-digest:{suffix}",
        workflow_definition_id,
        request_envelope["definition_hash"],
        json.dumps(request_envelope, sort_keys=True, separators=(",", ":")),
        f"authority:bundle:{suffix}",
        admission_decision_id,
        RunState.PROPOSAL_SUBMITTED.value,
        requested_at,
        admitted_at,
    )
    return {
        "workflow_id": workflow_id,
        "run_id": run_id,
    }
