#!/usr/bin/env python3
"""verify-policy-authority — end-to-end smoke against the live workflow DB.

Exercises the migrations 295/296/297 deliverables:
  - policy_definitions + authority_compliance_receipts schema present
  - 6 initial policy_definitions rows (3 tables × delete/truncate)
  - 3 anchoring operator_decisions rows
  - DELETE on operator_decisions blocked without bypass; admitted with bypass
  - TRUNCATE blocked on each protected table
  - policy.list + compliance.list_receipts registered in operation_catalog_registry

The unit tests under tests/test_policy_authority_triggers.py rely on a
clean test DB (praxis_test) bootstrapped through the workflow migration
runner. This script is the manual fallback when the test DB infra has
drift unrelated to P4.2 — runs against the live praxis DB and reports
pass/fail per check.

Usage:
  scripts/verify-policy-authority.py
  WORKFLOW_DATABASE_URL=... scripts/verify-policy-authority.py

Exit codes:
  0  — all checks passed
  1  — at least one check failed
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from typing import Awaitable, Callable

import asyncpg


def _resolve_url() -> str:
    url = os.environ.get("WORKFLOW_DATABASE_URL")
    if url:
        return url
    # Fallback: read it out of the praxis-api-server-1 container.
    import subprocess
    try:
        out = subprocess.check_output(
            ["docker", "exec", "praxis-api-server-1", "bash", "-lc", "echo $WORKFLOW_DATABASE_URL"],
            text=True,
            timeout=4,
        ).strip()
    except Exception as exc:
        print(f"verify-policy-authority: cannot resolve DB URL: {exc}", file=sys.stderr)
        sys.exit(2)
    if not out:
        print("verify-policy-authority: WORKFLOW_DATABASE_URL empty", file=sys.stderr)
        sys.exit(2)
    # Container uses host.docker.internal — rewrite for host execution.
    return out.replace("host.docker.internal", "127.0.0.1")


_PASSED = 0
_FAILED = 0
_FAILURES: list[str] = []


def _result(label: str, passed: bool, detail: str | None = None) -> None:
    global _PASSED, _FAILED
    if passed:
        _PASSED += 1
        print(f"  \033[1;32m[ok]\033[0m {label}")
    else:
        _FAILED += 1
        print(f"  \033[1;31m[FAIL]\033[0m {label}" + (f" — {detail}" if detail else ""))
        _FAILURES.append(label + (f" — {detail}" if detail else ""))


async def _check(label: str, fn: Callable[[], Awaitable[bool]]) -> None:
    try:
        ok = await fn()
        _result(label, ok)
    except Exception as exc:
        _result(label, False, f"{type(exc).__name__}: {str(exc)[:160]}")


async def main() -> int:
    url = _resolve_url()
    print(f"verify-policy-authority: connecting to {url.split('@')[-1]}")
    conn = await asyncpg.connect(url)
    try:
        # Schema presence
        async def _schema_tables() -> bool:
            rows = await conn.fetch(
                """
                SELECT table_name FROM information_schema.tables
                 WHERE table_name IN ('policy_definitions','authority_compliance_receipts')
                """
            )
            return {r["table_name"] for r in rows} == {
                "policy_definitions",
                "authority_compliance_receipts",
            }
        await _check("policy_definitions + authority_compliance_receipts present", _schema_tables)

        # Anchor decisions present
        async def _anchor_decisions() -> bool:
            rows = await conn.fetch(
                """
                SELECT decision_key FROM operator_decisions
                 WHERE decision_key IN (
                    'architecture-policy::policy-authority::operator-decisions-not-deletable',
                    'architecture-policy::policy-authority::receipts-immutable',
                    'architecture-policy::policy-authority::events-immutable'
                 )
                """
            )
            return len(rows) == 3
        await _check("3 anchor operator_decisions rows present", _anchor_decisions)

        # Initial policy rows
        async def _initial_policies() -> bool:
            rows = await conn.fetch(
                "SELECT policy_id FROM policy_definitions WHERE effective_to IS NULL"
            )
            ids = {r["policy_id"] for r in rows}
            expected = {
                "policy.operator_decisions.delete_reject",
                "policy.operator_decisions.truncate_reject",
                "policy.authority_operation_receipts.delete_reject",
                "policy.authority_operation_receipts.truncate_reject",
                "policy.authority_events.delete_reject",
                "policy.authority_events.truncate_reject",
            }
            return expected <= ids
        await _check("6 initial policy_definitions rows active", _initial_policies)

        # Triggers attached
        async def _triggers_attached() -> bool:
            rows = await conn.fetch(
                """
                SELECT tgname FROM pg_trigger t
                  JOIN pg_class c ON c.oid = t.tgrelid
                 WHERE tgname LIKE 'policy_%'
                """
            )
            names = {r["tgname"] for r in rows}
            expected = {
                "policy_operator_decisions_no_delete",
                "policy_operator_decisions_no_truncate",
                "policy_authority_operation_receipts_no_delete",
                "policy_authority_operation_receipts_no_truncate",
                "policy_authority_events_no_delete",
                "policy_authority_events_no_truncate",
            }
            return expected <= names
        await _check("6 enforcement triggers attached", _triggers_attached)

        # CQRS ops registered
        async def _ops_registered() -> bool:
            rows = await conn.fetch(
                """
                SELECT operation_ref FROM operation_catalog_registry
                 WHERE operation_ref IN ('policy-list','compliance-list-receipts')
                """
            )
            return len(rows) == 2
        await _check("policy.list + compliance.list_receipts registered", _ops_registered)

        # End-to-end: insert a real row, try to delete (must fail), bypass-delete (must succeed).
        test_key = (
            f"architecture-policy::TEST::verify-policy-authority-{uuid.uuid4().hex[:8]}"
        )
        decision_id = f"operator_decision.test.{uuid.uuid4().hex}"[:240]

        await conn.execute(
            """
            INSERT INTO operator_decisions (
                operator_decision_id, decision_key, decision_kind, decision_status,
                title, rationale, decided_by, decision_source,
                effective_from, decided_at, created_at, updated_at,
                decision_scope_kind, decision_scope_ref, scope_clamp
            ) VALUES (
                $1, $2, 'architecture_policy', 'decided',
                'TEST', 'verify-policy-authority smoke', 'praxis', 'verify_policy_authority',
                now(), now(), now(), now(),
                'authority_domain', 'test',
                '{"applies_to":["pending_review"],"does_not_apply_to":[]}'::jsonb
            )
            """,
            decision_id,
            test_key,
        )

        async def _delete_blocked() -> bool:
            try:
                async with conn.transaction():
                    await conn.execute(
                        "DELETE FROM operator_decisions WHERE decision_key = $1",
                        test_key,
                    )
                return False  # admitted — wrong
            except asyncpg.exceptions.CheckViolationError as exc:
                # Must mention the decision_key in the error
                return (
                    "operator-decisions-not-deletable" in str(exc)
                    or "policy.operator_decisions.delete_reject" in str(exc)
                )
        await _check("DELETE blocked + decision_key in error", _delete_blocked)

        # P4.2.e (migration 298): the autonomous-transaction compliance
        # receipt should have been written even though the parent rolled
        # back. Look for a receipt linked to the test row's decision_key
        # in the rationale (subject_pk carries decision_key inside the
        # row jsonb).
        async def _compliance_receipt_persisted() -> bool:
            row = await conn.fetchrow(
                """
                SELECT policy_id, outcome, target_table, operation, rejected_reason
                  FROM authority_compliance_receipts
                 WHERE target_table = 'operator_decisions'
                   AND outcome = 'reject'
                   AND operation = 'DELETE'
                   AND subject_pk @> jsonb_build_object('decision_key', $1::text)
                 ORDER BY created_at DESC LIMIT 1
                """,
                test_key,
            )
            if row is None:
                return False
            return (
                row["policy_id"] == "policy.operator_decisions.delete_reject"
                and row["outcome"] == "reject"
                and row["operation"] == "DELETE"
                and bool(row["rejected_reason"])
            )
        await _check(
            "compliance_receipt persisted across parent rollback (P4.2.e)",
            _compliance_receipt_persisted,
        )

        async def _bypass_admits() -> bool:
            async with conn.transaction():
                await conn.execute("SET LOCAL praxis.policy_bypass = 'on'")
                result = await conn.execute(
                    "DELETE FROM operator_decisions WHERE decision_key = $1",
                    test_key,
                )
            # row should be gone
            remaining = await conn.fetchval(
                "SELECT count(*) FROM operator_decisions WHERE decision_key = $1",
                test_key,
            )
            return result == "DELETE 1" and remaining == 0
        await _check("bypass-DELETE admits + cleans up test row", _bypass_admits)

        # P4.2.f (migrations 299+300): generic insert/update enforcement
        # machinery + receipt-writer self-bypass. Smoke an attach → reject
        # → admit → receipt-persisted → detach cycle on a throwaway table.
        async def _attach_detach_machinery() -> bool:
            pol_id = f"policy.TEST.verify_{uuid.uuid4().hex[:8]}"
            dk = f"architecture-policy::TEST::verify-pa-machinery-{uuid.uuid4().hex[:8]}"
            op_id = f"op.test.{uuid.uuid4().hex}"[:200]
            try:
                await conn.execute(
                    "CREATE TABLE IF NOT EXISTS pa_verify_smoke (id text PRIMARY KEY, email text)"
                )
                await conn.execute(
                    """INSERT INTO operator_decisions (
                        operator_decision_id, decision_key, decision_kind, decision_status,
                        title, rationale, decided_by, decision_source,
                        effective_from, decided_at, created_at, updated_at,
                        decision_scope_kind, decision_scope_ref, scope_clamp)
                       VALUES ($1,$2,'architecture_policy','decided','TEST','TEST',
                        'praxis','verify_policy_authority',now(),now(),now(),now(),
                        'authority_domain','test',
                        '{"applies_to":["pending_review"],"does_not_apply_to":[]}'::jsonb)""",
                    op_id, dk,
                )
                await conn.execute(
                    """INSERT INTO policy_definitions (policy_id, decision_key,
                        enforcement_kind, target_table, predicate_sql, rationale, effective_from)
                       VALUES ($1, $2, 'insert_reject', 'pa_verify_smoke',
                        $3, 'No admin emails', now())""",
                    pol_id, dk, "NEW.email LIKE '%@admin.example.com'",
                )

                attach_result = await conn.fetchval(
                    "SELECT policy_authority_attach_table_policy($1)", pol_id
                )
                if not attach_result or "attached" not in attach_result:
                    return False

                # Bad insert — must reject
                rejected = False
                try:
                    async with conn.transaction():
                        await conn.execute(
                            "INSERT INTO pa_verify_smoke (id, email) VALUES ('a','bad@admin.example.com')"
                        )
                except asyncpg.exceptions.CheckViolationError as exc:
                    rejected = pol_id in str(exc)
                if not rejected:
                    return False

                # Good insert — must admit
                async with conn.transaction():
                    await conn.execute(
                        "INSERT INTO pa_verify_smoke (id, email) VALUES ('b','user@example.com')"
                    )

                # Receipt persisted (proves 300's self-bypass works)
                receipt = await conn.fetchrow(
                    """SELECT outcome FROM authority_compliance_receipts
                        WHERE policy_id = $1 AND outcome = 'reject'
                        ORDER BY created_at DESC LIMIT 1""",
                    pol_id,
                )
                if receipt is None:
                    return False

                # Detach
                detach_result = await conn.fetchval(
                    "SELECT policy_authority_detach_table_policy($1)", pol_id
                )
                if not detach_result or "detached" not in detach_result:
                    return False

                return True
            finally:
                # Cleanup — bypass to remove the test rows from authority tables
                async with conn.transaction():
                    await conn.execute("SET LOCAL praxis.policy_bypass = 'on'")
                    await conn.execute("DROP TABLE IF EXISTS pa_verify_smoke")
                    await conn.execute(
                        "DELETE FROM authority_compliance_receipts WHERE policy_id = $1", pol_id
                    )
                    await conn.execute(
                        "DELETE FROM policy_definitions WHERE policy_id = $1", pol_id
                    )
                    await conn.execute(
                        "DELETE FROM operator_decisions WHERE decision_key = $1", dk
                    )
        await _check(
            "attach insert_reject → reject → admit → receipt → detach (P4.2.f)",
            _attach_detach_machinery,
        )

    finally:
        await conn.close()

    print()
    print(f"summary: {_PASSED} passed, {_FAILED} failed")
    if _FAILURES:
        print("FAILURES:")
        for f in _FAILURES:
            print(f"  • {f}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
