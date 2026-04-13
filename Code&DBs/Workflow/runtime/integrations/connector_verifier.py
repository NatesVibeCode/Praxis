"""Connector capability verification.

Runs each declared capability through the production execute_integration() path,
checks expectations, records results in verification_runs, and updates
connector_registry.verification_status.
"""

from __future__ import annotations

import json
import logging
import time
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

logger = logging.getLogger(__name__)


def verify_connector(
    slug: str,
    pg: "SyncPostgresConnection",
    *,
    actions: list[str] | None = None,
) -> dict[str, Any]:
    """Verify a registered connector's capabilities against a live API.

    Returns a summary with per-action results and overall coverage.
    """
    from runtime.integrations.connector_registry import (
        get_connector,
        get_verification_spec,
        update_verification_status,
    )

    connector = get_connector(pg, slug)
    if connector is None:
        return {"error": f"Connector '{slug}' not found in connector_registry"}

    spec = get_verification_spec(pg, slug)
    if not spec:
        return {
            "error": f"No verification_spec for connector '{slug}'. "
            "Register the connector first to auto-generate specs.",
        }

    # Filter to requested actions if provided
    test_cases = [
        tc for tc in spec
        if isinstance(tc, dict) and tc.get("action")
    ]
    if actions:
        test_cases = [tc for tc in test_cases if tc["action"] in actions]

    if not test_cases:
        return {"error": "No matching test cases found", "spec_count": len(spec)}

    # Load verifier definition for recording
    verifier = _load_connector_verifier(pg)

    results: list[dict[str, Any]] = []
    passed = 0
    failed = 0
    skipped = 0

    for tc in test_cases:
        action = tc["action"]
        if tc.get("skip"):
            results.append({"action": action, "status": "skipped", "reason": "skip=true"})
            skipped += 1
            continue

        t0 = time.monotonic()
        result = _run_one(slug, action, tc.get("args") or {}, pg)
        duration_ms = int((time.monotonic() - t0) * 1000)

        expect = tc.get("expect") or {"status": "succeeded"}
        check_status, check_detail = _check_expectations(result, expect)

        if check_status == "passed":
            passed += 1
        else:
            failed += 1

        # Record in verification_runs
        _record(
            verifier=verifier,
            slug=slug,
            action=action,
            status=check_status,
            inputs={"action": action, "args": tc.get("args") or {}, "expect": expect},
            outputs={"result_status": result.get("status"), "detail": check_detail},
            duration_ms=duration_ms,
            conn=pg,
        )

        results.append({
            "action": action,
            "status": check_status,
            "detail": check_detail,
            "duration_ms": duration_ms,
        })

    total_testable = passed + failed
    coverage = passed / total_testable if total_testable > 0 else 0.0

    if failed == 0 and passed > 0:
        overall_status = "verified"
    elif passed > 0:
        overall_status = "partial"
    else:
        overall_status = "failed"

    update_verification_status(pg, slug, overall_status)

    return {
        "slug": slug,
        "verification_status": overall_status,
        "coverage": round(coverage, 3),
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "total": len(test_cases),
        "results": results,
    }


def verification_coverage(pg: "SyncPostgresConnection") -> dict[str, Any]:
    """Aggregate verification coverage across all active connectors."""
    rows = pg.execute(
        """SELECT slug, verification_status, last_verified_at,
                  jsonb_array_length(verification_spec) AS spec_count
             FROM connector_registry
            WHERE status = 'active'
            ORDER BY slug""",
    )
    connectors = []
    total_verified = 0
    total_connectors = 0
    for row in rows or []:
        row = dict(row)
        total_connectors += 1
        if row.get("verification_status") in ("verified", "partial"):
            total_verified += 1
        connectors.append({
            "slug": row["slug"],
            "status": row.get("verification_status", "unverified"),
            "last_verified": str(row["last_verified_at"]) if row.get("last_verified_at") else None,
            "spec_count": row.get("spec_count", 0),
        })
    return {
        "coverage": round(total_verified / total_connectors, 3) if total_connectors > 0 else 0.0,
        "verified": total_verified,
        "total": total_connectors,
        "connectors": connectors,
    }


# ── Internal helpers ────────────────────────────────────────────────


def _run_one(slug: str, action: str, args: dict, pg: Any) -> dict[str, Any]:
    """Execute a single capability through the production path."""
    from runtime.integrations import execute_integration

    try:
        return dict(execute_integration(slug, action, args, pg))
    except Exception as exc:
        logger.warning("Verification call %s/%s failed: %s", slug, action, exc)
        return {"status": "failed", "error": str(exc), "data": None}


def _check_expectations(result: dict, expect: dict) -> tuple[str, str]:
    """Check result against expect vocabulary. Returns (status, detail)."""
    if not expect:
        return ("passed", "no expectations") if result.get("status") == "succeeded" else ("failed", f"status={result.get('status')}")

    # Check status
    expected_status = expect.get("status")
    if expected_status and result.get("status") != expected_status:
        return "failed", f"expected status={expected_status}, got {result.get('status')}: {result.get('error', '')}"

    # Check error_absent
    if expect.get("error_absent") and result.get("error"):
        return "failed", f"expected no error, got: {result.get('error')}"

    data = result.get("data")

    # Check data_type
    expected_type = expect.get("data_type")
    if expected_type == "list" and not isinstance(data, list):
        return "failed", f"expected list, got {type(data).__name__}"
    if expected_type == "dict" and not isinstance(data, dict):
        return "failed", f"expected dict, got {type(data).__name__}"

    # Check data_keys
    expected_keys = expect.get("data_keys")
    if expected_keys and isinstance(data, dict):
        missing = [k for k in expected_keys if k not in data]
        if missing:
            return "failed", f"missing keys: {missing}"

    # Check data_min_length
    min_len = expect.get("data_min_length")
    if min_len is not None and isinstance(data, (list, tuple)):
        if len(data) < min_len:
            return "failed", f"expected min length {min_len}, got {len(data)}"

    return "passed", "all expectations met"


def _load_connector_verifier(pg: "SyncPostgresConnection"):
    """Load the verifier.connector.capability definition."""
    from runtime.verifier_authority import _load_verifier
    return _load_verifier("verifier.connector.capability", conn=pg)


def _record(
    *,
    verifier,
    slug: str,
    action: str,
    status: str,
    inputs: dict,
    outputs: dict,
    duration_ms: int,
    conn: "SyncPostgresConnection",
) -> None:
    """Record a verification run result."""
    from runtime.verifier_authority import _record_verification_run
    _record_verification_run(
        verifier=verifier,
        target_kind="connector",
        target_ref=f"{slug}:{action}",
        status=status,
        inputs=inputs,
        outputs=outputs,
        suggested_healer_ref=None,
        healing_candidate=False,
        duration_ms=duration_ms,
        conn=conn,
    )
