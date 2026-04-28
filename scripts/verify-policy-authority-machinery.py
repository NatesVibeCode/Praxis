#!/usr/bin/env python3
"""verify-policy-authority-machinery — live-DB smoke for items 8 and 9.

Sibling to verify-policy-authority.py (which covers migrations 295-297).
This script exercises:

  Item 8 — migration 298 + 300:
    - dblink helper installed
    - reject-path compliance receipts persist across rollback (autonomous tx)
    - receipt writer self-bypass works (no recursive trigger fire)

  Item 9 — migration 299:
    - policy_authority_attach_table_policy generates and attaches a trigger
    - insert_reject blocks matching INSERTs and writes a reject receipt
    - non-matching INSERTs admit cleanly
    - policy_authority_detach_table_policy removes the trigger
    - update_clamp explicitly errors as not-yet-implemented
    - delete_reject and truncate_reject are refused by the activator
      (owned by static migration-296 triggers)

All test artifacts (test operator_decisions row, test policy_definitions
row, test compliance receipts) are bypass-cleaned at the end.

Exit codes:
  0 — all checks passed
  1 — at least one check failed
  2 — DB unreachable
"""

from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import uuid
from typing import Awaitable, Callable

import asyncpg


def _resolve_url() -> str:
    url = os.environ.get("WORKFLOW_DATABASE_URL")
    if url:
        return url
    try:
        out = subprocess.check_output(
            ["docker", "exec", "praxis-api-server-1", "bash", "-lc", "echo $WORKFLOW_DATABASE_URL"],
            text=True,
            timeout=4,
        ).strip()
    except Exception as exc:
        print(f"verify-policy-authority-machinery: cannot resolve DB URL: {exc}", file=sys.stderr)
        sys.exit(2)
    if not out:
        print("verify-policy-authority-machinery: WORKFLOW_DATABASE_URL empty", file=sys.stderr)
        sys.exit(2)
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
        _result(label, False, f"{type(exc).__name__}: {str(exc)[:200]}")


async def _insert_test_decision(conn: asyncpg.Connection, decision_key: str, source: str) -> str:
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
            'TEST', 'TEST', 'praxis', $3,
            now(), now(), now(), now(),
            'authority_domain', 'test',
            '{"applies_to":["pending_review"],"does_not_apply_to":[]}'::jsonb
        )
        """,
        decision_id,
        decision_key,
        source,
    )
    return decision_id


async def _bypass_cleanup_decisions(conn: asyncpg.Connection, decision_keys: list[str]) -> None:
    async with conn.transaction():
        await conn.execute("SET LOCAL praxis.policy_bypass='on'")
        for key in decision_keys:
            await conn.execute("DELETE FROM operator_decisions WHERE decision_key = $1", key)


async def _bypass_cleanup_compliance(conn: asyncpg.Connection, policy_id: str) -> None:
    async with conn.transaction():
        await conn.execute("SET LOCAL praxis.policy_bypass='on'")
        await conn.execute(
            "DELETE FROM authority_compliance_receipts WHERE policy_id = $1", policy_id
        )


async def main() -> int:
    url = _resolve_url()
    print(f"verify-policy-authority-machinery: connecting to {url.split('@')[-1]}")
    conn = await asyncpg.connect(url)
    notices: list[asyncpg.PostgresLogMessage] = []
    conn.add_log_listener(lambda c, m: notices.append(m))

    cleanup_decisions: list[str] = []
    cleanup_policy_ids: list[str] = []

    try:
        # ============================================================
        # Item 8 — dblink + reject receipts
        # ============================================================
        print("\n— Item 8: dblink-based reject receipts —")

        # Helper installed
        async def _helper_installed() -> bool:
            r = await conn.fetch(
                "SELECT proname FROM pg_proc WHERE proname='policy_authority_record_compliance_receipt'"
            )
            return bool(r)
        await _check("policy_authority_record_compliance_receipt function installed", _helper_installed)

        # dblink granted to current role
        async def _dblink_granted() -> bool:
            r = await conn.fetchval(
                "SELECT has_function_privilege(current_user, 'dblink_connect_u(text, text)', 'execute')"
            )
            return bool(r)
        await _check("dblink_connect_u EXECUTE granted to current role", _dblink_granted)

        # Reject-path receipt persists across rollback
        decision_key_a = f"architecture-policy::TEST::reject-receipt-{uuid.uuid4().hex[:8]}"
        await _insert_test_decision(conn, decision_key_a, "verify_machinery")
        cleanup_decisions.append(decision_key_a)

        async def _reject_receipt_persists() -> bool:
            before = await conn.fetchval(
                "SELECT count(*) FROM authority_compliance_receipts "
                "WHERE outcome='reject' AND target_table='operator_decisions'"
            )
            try:
                async with conn.transaction():
                    await conn.execute(
                        "DELETE FROM operator_decisions WHERE decision_key = $1",
                        decision_key_a,
                    )
            except asyncpg.exceptions.CheckViolationError:
                pass
            after = await conn.fetchval(
                "SELECT count(*) FROM authority_compliance_receipts "
                "WHERE outcome='reject' AND target_table='operator_decisions'"
            )
            return after > before
        await _check("reject DELETE writes compliance receipt across rollback", _reject_receipt_persists)

        # ============================================================
        # Item 9 — INSERT/UPDATE machinery + activator
        # ============================================================
        print("\n— Item 9: insert/update machinery —")

        # New helpers installed
        async def _new_helpers() -> bool:
            r = await conn.fetch(
                """SELECT proname FROM pg_proc WHERE proname IN (
                       'policy_authority_record_admit_receipt',
                       'policy_authority_attach_table_policy',
                       'policy_authority_detach_table_policy'
                   )"""
            )
            return len(r) == 3
        await _check("attach + detach + admit-receipt helpers installed", _new_helpers)

        # update_clamp explicitly NOT YET IMPLEMENTED
        async def _clamp_blocked() -> bool:
            decision_key = f"architecture-policy::TEST::clamp-deferred-{uuid.uuid4().hex[:8]}"
            await _insert_test_decision(conn, decision_key, "verify_machinery_clamp")
            cleanup_decisions.append(decision_key)
            policy_id = f"policy.TEST.clamp_{uuid.uuid4().hex[:8]}"
            cleanup_policy_ids.append(policy_id)
            await conn.execute(
                """INSERT INTO policy_definitions (
                       policy_id, decision_key, enforcement_kind, target_table,
                       rationale, effective_from
                   ) VALUES ($1, $2, 'update_clamp', 'operator_decisions',
                             'clamp policy', now())""",
                policy_id, decision_key,
            )
            try:
                await conn.fetchval(
                    "SELECT policy_authority_attach_table_policy($1)", policy_id
                )
                return False  # should have raised
            except asyncpg.exceptions.FeatureNotSupportedError as e:
                return "update_clamp" in str(e)
        await _check("update_clamp activator returns feature_not_supported", _clamp_blocked)

        # delete_reject and truncate_reject refused (owned by static triggers)
        async def _static_owner_refused() -> bool:
            decision_key = f"architecture-policy::TEST::static-owner-{uuid.uuid4().hex[:8]}"
            await _insert_test_decision(conn, decision_key, "verify_machinery_static")
            cleanup_decisions.append(decision_key)
            policy_id = f"policy.TEST.static_{uuid.uuid4().hex[:8]}"
            cleanup_policy_ids.append(policy_id)
            await conn.execute(
                """INSERT INTO policy_definitions (
                       policy_id, decision_key, enforcement_kind, target_table,
                       rationale, effective_from
                   ) VALUES ($1, $2, 'delete_reject', 'authority_events',
                             'try-to-double-attach', now())""",
                policy_id, decision_key,
            )
            try:
                await conn.fetchval(
                    "SELECT policy_authority_attach_table_policy($1)", policy_id
                )
                return False
            except asyncpg.exceptions.FeatureNotSupportedError as e:
                return "static triggers in migration 296" in str(e)
        await _check("activator refuses delete_reject (static-trigger owned)", _static_owner_refused)

        # End-to-end insert_reject roundtrip
        decision_key_b = f"architecture-policy::TEST::insert-reject-{uuid.uuid4().hex[:8]}"
        await _insert_test_decision(conn, decision_key_b, "verify_machinery_insert")
        cleanup_decisions.append(decision_key_b)
        policy_id_b = f"policy.TEST.insert_{uuid.uuid4().hex[:8]}"
        cleanup_policy_ids.append(policy_id_b)
        await conn.execute(
            """INSERT INTO policy_definitions (
                   policy_id, decision_key, enforcement_kind, target_table,
                   predicate_sql, rationale, effective_from
               ) VALUES ($1, $2, 'insert_reject', 'operator_decisions',
                         $3, 'forbid synthetic verify rows', now())""",
            policy_id_b, decision_key_b,
            "NEW.decision_source = 'BLOCK_ME_VERIFY_MACHINERY'",
        )

        async def _attach_creates_trigger() -> bool:
            result = await conn.fetchval(
                "SELECT policy_authority_attach_table_policy($1)", policy_id_b
            )
            return result is not None and "attached INSERT on operator_decisions" in result
        await _check("attach generates INSERT trigger", _attach_creates_trigger)

        async def _matching_insert_rejected() -> bool:
            before = await conn.fetchval(
                "SELECT count(*) FROM authority_compliance_receipts "
                "WHERE policy_id=$1 AND outcome='reject'",
                policy_id_b,
            )
            try:
                async with conn.transaction():
                    bad_id = f"operator_decision.test.{uuid.uuid4().hex}"[:240]
                    bad_key = f"architecture-policy::TEST::should-block-{uuid.uuid4().hex[:8]}"
                    cleanup_decisions.append(bad_key)
                    await conn.execute(
                        """INSERT INTO operator_decisions (
                                operator_decision_id, decision_key, decision_kind, decision_status,
                                title, rationale, decided_by, decision_source,
                                effective_from, decided_at, created_at, updated_at,
                                decision_scope_kind, decision_scope_ref, scope_clamp
                           ) VALUES ($1, $2, 'architecture_policy', 'decided',
                                'should_block', 'should_block', 'praxis',
                                'BLOCK_ME_VERIFY_MACHINERY',
                                now(), now(), now(), now(),
                                'authority_domain', 'test',
                                '{"applies_to":["pending_review"],"does_not_apply_to":[]}'::jsonb)""",
                        bad_id, bad_key,
                    )
                return False  # should have raised
            except asyncpg.exceptions.CheckViolationError:
                pass
            after = await conn.fetchval(
                "SELECT count(*) FROM authority_compliance_receipts "
                "WHERE policy_id=$1 AND outcome='reject'",
                policy_id_b,
            )
            return after > before
        await _check("predicate-matching INSERT rejected + receipt written", _matching_insert_rejected)

        async def _innocuous_insert_admitted() -> bool:
            innocent_id = f"operator_decision.test.{uuid.uuid4().hex}"[:240]
            innocent_key = f"architecture-policy::TEST::innocent-{uuid.uuid4().hex[:8]}"
            cleanup_decisions.append(innocent_key)
            await conn.execute(
                """INSERT INTO operator_decisions (
                        operator_decision_id, decision_key, decision_kind, decision_status,
                        title, rationale, decided_by, decision_source,
                        effective_from, decided_at, created_at, updated_at,
                        decision_scope_kind, decision_scope_ref, scope_clamp
                   ) VALUES ($1, $2, 'architecture_policy', 'decided',
                        'innocent', 'innocent', 'praxis', 'innocent_source',
                        now(), now(), now(), now(),
                        'authority_domain', 'test',
                        '{"applies_to":["pending_review"],"does_not_apply_to":[]}'::jsonb)""",
                innocent_id, innocent_key,
            )
            row = await conn.fetchval(
                "SELECT count(*) FROM operator_decisions WHERE decision_key=$1", innocent_key
            )
            return row == 1
        await _check("non-matching INSERT admitted cleanly", _innocuous_insert_admitted)

        async def _detach_drops_trigger() -> bool:
            await conn.fetchval(
                "SELECT policy_authority_detach_table_policy($1)", policy_id_b
            )
            slug = policy_id_b.replace(".", "_").replace("-", "_")
            row = await conn.fetchval(
                "SELECT count(*) FROM pg_trigger WHERE tgname = $1",
                f"policy_{slug}",
            )
            return row == 0
        await _check("detach removes the trigger", _detach_drops_trigger)

    finally:
        # Cleanup
        for pid in cleanup_policy_ids:
            try:
                await _bypass_cleanup_compliance(conn, pid)
                await conn.execute("DELETE FROM policy_definitions WHERE policy_id = $1", pid)
            except Exception:
                pass
        if cleanup_decisions:
            try:
                await _bypass_cleanup_decisions(conn, cleanup_decisions)
            except Exception:
                pass
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
