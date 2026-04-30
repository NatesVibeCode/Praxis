"""Structural test for the ``register_operation_atomic`` SQL function.

This test exists to enforce the singleton invariant on the
``register_operation_atomic`` overload set. The Python caller in
``runtime.operations.commands.catalog_operation_register`` invokes the
function with named arguments. Postgres' named-argument resolution
matches by NAME first, then by argument-type compatibility. When two
overloads exist that share identical types at every supplied named
position (e.g. the 23-param signature from migration 240 and the
26-param signature from migration 350), no caller-side type cast can
disambiguate them — Postgres raises ``AmbiguousFunctionError``.

The structural fix is to keep exactly one overload alive at any point
after migrations apply. Migration 350 enforces this on its own apply
via a ``DROP FUNCTION`` loop in pg_proc before the canonical
``CREATE OR REPLACE FUNCTION``. Any future migration that redefines
this helper MUST follow the same drop-all-then-create pattern. This
test catches regressions automatically.

Skips cleanly when the test DB is not reachable so the unit-suite
remains runnable in environments without Postgres.

Anchor:
  BUG-8DC8A3BA — diagnosis recorded on the bug.

  architecture-policy::policy-authority::receipts-immutable already
  governs the broader audit-ledger invariants; this is a sibling
  contract for the registration helper.
"""

from __future__ import annotations

import os

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
            "register_operation_atomic singleton test requires a reachable "
            f"test DB: {type(exc).__name__}: {exc}"
        )


def _overload_signatures(conn: SyncPostgresConnection) -> list[str]:
    rows = conn.execute(
        """
        SELECT pg_get_function_identity_arguments(p.oid) AS args
          FROM pg_proc p
          JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = 'public'
           AND p.proname = 'register_operation_atomic'
         ORDER BY p.oid
        """,
    )
    return [str(row["args"]) for row in rows or ()]


def test_register_operation_atomic_has_exactly_one_overload(
    conn: SyncPostgresConnection,
) -> None:
    """Exactly one ``register_operation_atomic`` overload may exist.

    Two or more overloads where the named parameters share types at
    every position produce ``AmbiguousFunctionError`` at named-argument
    resolution time, breaking the wizard tool ``praxis_register_operation``
    (BUG-8DC8A3BA). Migration 350 drops all overloads before re-creating
    the canonical 26-param signature; any future migration that
    redefines this helper must follow the same pattern.
    """

    signatures = _overload_signatures(conn)
    assert len(signatures) == 1, (
        f"expected exactly 1 register_operation_atomic overload, "
        f"got {len(signatures)}: {signatures}"
    )


def test_register_operation_atomic_has_canonical_param_count(
    conn: SyncPostgresConnection,
) -> None:
    """The single overload must be the canonical 26-param signature.

    Migration 350 introduced ``p_timeout_ms``, ``p_execution_lane``, and
    ``p_kickoff_required``. If the singleton check passes but the
    surviving overload has fewer params, a future migration regressed
    the lane-classification capability.
    """

    signatures = _overload_signatures(conn)
    assert signatures, "no register_operation_atomic overload found"
    [args] = signatures
    # Each comma in the identity-arguments list separates one param.
    param_count = args.count(",") + 1
    assert param_count == 26, (
        "expected the 26-param register_operation_atomic signature "
        f"(post-migration 350); got {param_count}-param: {args}"
    )
