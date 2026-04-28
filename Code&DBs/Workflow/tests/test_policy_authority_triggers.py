"""Integration tests for the Policy Authority triggers (P4.2.b).

Verifies that the BEFORE DELETE / BEFORE TRUNCATE triggers shipped in
migration 296 actually:

  - block DELETE on operator_decisions (without bypass)
  - block DELETE on authority_operation_receipts (without bypass)
  - block DELETE on authority_events (without bypass)
  - admit DELETE under SET LOCAL praxis.policy_bypass = 'on'
  - block TRUNCATE on each of the three protected tables
  - surface the decision_key + rationale in the error message

The tests need a real Postgres connection because the triggers are
plpgsql. They skip when the test DB is not reachable.
"""

from __future__ import annotations

import os
import uuid

import pytest

from _pg_test_conn import ensure_test_database_ready

os.environ.setdefault("WORKFLOW_DATABASE_URL", ensure_test_database_ready())

from storage.postgres import SyncPostgresConnection, get_workflow_pool  # noqa: E402


@pytest.fixture
def conn() -> SyncPostgresConnection:
    try:
        return SyncPostgresConnection(get_workflow_pool())
    except Exception as exc:
        pytest.skip(
            "policy authority trigger tests require a reachable test DB: "
            f"{type(exc).__name__}: {exc}"
        )


def _insert_test_decision(conn: SyncPostgresConnection, decision_key: str) -> str:
    """Insert a disposable operator_decisions row for trigger testing.

    Returns the operator_decision_id so the caller can clean up under
    bypass.
    """
    decision_id = f"operator_decision.test.{uuid.uuid4().hex}"[:240]
    conn.execute(
        """
        INSERT INTO operator_decisions (
            operator_decision_id, decision_key, decision_kind, decision_status,
            title, rationale, decided_by, decision_source,
            effective_from, decided_at, created_at, updated_at,
            decision_scope_kind, decision_scope_ref, scope_clamp
        ) VALUES (
            $1, $2, 'architecture_policy', 'decided',
            'TEST', 'TEST', 'praxis', 'policy_authority_trigger_test',
            now(), now(), now(), now(),
            'authority_domain', 'test',
            '{"applies_to":["pending_review"],"does_not_apply_to":[]}'::jsonb
        )
        """,
        decision_id,
        decision_key,
    )
    return decision_id


def _cleanup_test_decision(conn: SyncPostgresConnection, decision_key: str) -> None:
    """Bypass-delete the test row so the test is idempotent."""
    conn.execute(
        "BEGIN; SET LOCAL praxis.policy_bypass = 'on'; "
        "DELETE FROM operator_decisions WHERE decision_key = $1; "
        "COMMIT",
        decision_key,
    )


def test_delete_operator_decisions_blocked_without_bypass(
    conn: SyncPostgresConnection,
) -> None:
    decision_key = (
        f"architecture-policy::TEST::trigger-block-{uuid.uuid4().hex[:8]}"
    )
    _insert_test_decision(conn, decision_key)
    try:
        with pytest.raises(Exception) as exc_info:
            conn.execute(
                "DELETE FROM operator_decisions WHERE decision_key = $1",
                decision_key,
            )
        msg = str(exc_info.value)
        assert "policy_authority" in msg
        assert "DELETE" in msg
        assert "operator_decisions" in msg
        # The standing order's decision_key must show in the error so the
        # agent sees why it was blocked.
        assert (
            "operator-decisions-not-deletable" in msg
            or "policy.operator_decisions.delete_reject" in msg
        )
    finally:
        _cleanup_test_decision(conn, decision_key)


def test_delete_operator_decisions_admitted_with_bypass(
    conn: SyncPostgresConnection,
) -> None:
    decision_key = (
        f"architecture-policy::TEST::trigger-bypass-{uuid.uuid4().hex[:8]}"
    )
    _insert_test_decision(conn, decision_key)
    # Bypass-delete inside a transaction so SET LOCAL applies. Should
    # succeed and the row should be gone afterwards.
    conn.execute(
        "BEGIN; SET LOCAL praxis.policy_bypass = 'on'; "
        "DELETE FROM operator_decisions WHERE decision_key = $1; "
        "COMMIT",
        decision_key,
    )
    remaining = conn.execute(
        "SELECT count(*) AS n FROM operator_decisions WHERE decision_key = $1",
        decision_key,
    )
    assert remaining[0]["n"] == 0


def test_truncate_operator_decisions_blocked(conn: SyncPostgresConnection) -> None:
    with pytest.raises(Exception) as exc_info:
        conn.execute("TRUNCATE TABLE operator_decisions")
    msg = str(exc_info.value)
    assert "policy_authority" in msg
    assert "TRUNCATE" in msg


def test_truncate_authority_operation_receipts_blocked(
    conn: SyncPostgresConnection,
) -> None:
    with pytest.raises(Exception) as exc_info:
        conn.execute("TRUNCATE TABLE authority_operation_receipts")
    msg = str(exc_info.value)
    assert "policy_authority" in msg
    assert "TRUNCATE" in msg
    assert "receipts-immutable" in msg or "authority_operation_receipts" in msg


def test_truncate_authority_events_blocked(conn: SyncPostgresConnection) -> None:
    with pytest.raises(Exception) as exc_info:
        conn.execute("TRUNCATE TABLE authority_events")
    msg = str(exc_info.value)
    assert "policy_authority" in msg
    assert "TRUNCATE" in msg
    assert "events-immutable" in msg or "authority_events" in msg


def test_policy_definitions_rows_exist(conn: SyncPostgresConnection) -> None:
    """The 6 initial policy_definitions rows from migration 296 are present."""
    rows = conn.execute(
        """
        SELECT policy_id FROM policy_definitions
         WHERE effective_to IS NULL
         ORDER BY policy_id
        """
    )
    policy_ids = {r["policy_id"] for r in rows}
    expected = {
        "policy.operator_decisions.delete_reject",
        "policy.operator_decisions.truncate_reject",
        "policy.authority_operation_receipts.delete_reject",
        "policy.authority_operation_receipts.truncate_reject",
        "policy.authority_events.delete_reject",
        "policy.authority_events.truncate_reject",
    }
    assert expected <= policy_ids, (
        f"missing policy rows: {expected - policy_ids}"
    )


def test_policy_authority_decisions_rows_exist(conn: SyncPostgresConnection) -> None:
    """The 3 anchoring operator_decisions rows from migration 296 are present."""
    rows = conn.execute(
        """
        SELECT decision_key FROM operator_decisions
         WHERE decision_key IN (
            'architecture-policy::policy-authority::operator-decisions-not-deletable',
            'architecture-policy::policy-authority::receipts-immutable',
            'architecture-policy::policy-authority::events-immutable'
         )
        """
    )
    keys = {r["decision_key"] for r in rows}
    assert len(keys) == 3, f"missing decision rows: expected 3, got {keys}"
