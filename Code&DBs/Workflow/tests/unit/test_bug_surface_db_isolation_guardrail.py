"""Regression pin for BUG-E569CC49.

The repo supplies two test DB helpers in ``_pg_test_conn``:

* ``get_test_conn()`` — a long-lived shared auto-commit connection. Writes
  are durable. Fast, but any row it inserts stays in the test database and
  can leak across tests.
* ``get_isolated_conn()`` / ``transactional_test_conn()`` — dedicated
  connection inside a transaction that rolls back on close. Writes do not
  persist between tests.

The bug-surface authority tables (``bugs``, ``bug_evidence_links``,
``bug_recurrences``, etc.) are especially sensitive: a test that inserts a
bug row through ``get_test_conn()`` pollutes the bug-query result set for
every later test in the same run, silently masking real bug-query behavior
behind a background of stale test rows.

BUG-E569CC49 flagged three specific test files for mixing bug writes with
the shared connection. Those files have been migrated (two removed the
usage, one moved to ``get_isolated_conn``). This test turns that one-time
cleanup into a standing invariant: **no test file may import a bug-surface
writer (``BugTracker``, ``bug_tracker``) AND call the shared auto-commit
``get_test_conn()``**. Any such pairing fails in CI. Bug-surface writes in
tests must go through the rollback-isolated helpers.

Pins:

1. The ``get_test_conn`` docstring still announces the durability warning
   — this is the operator-facing contract and must not silently drop.
2. ``_pg_test_conn`` exports both the shared and isolated constructors.
3. No test file under ``tests/`` imports ``BugTracker`` (or references
   ``bug_tracker``) AND calls ``get_test_conn()`` in the same file.
4. The three originally flagged files (``test_api_rest_startup.py``,
   ``test_observability_hub.py``, ``test_mcp_workflow_server.py``) remain
   clean — none of them calls ``get_test_conn()``.
"""
from __future__ import annotations

from pathlib import Path

import pytest

import _pg_test_conn


_REPO_ROOT = Path(__file__).resolve().parents[2]
_TEST_ROOTS = (_REPO_ROOT / "tests" / "unit", _REPO_ROOT / "tests" / "integration")


def _iter_test_files() -> list[Path]:
    files: list[Path] = []
    for root in _TEST_ROOTS:
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("test_*.py")):
            files.append(path)
    return files


# -- 1. docstring contract on the shared helper -------------------------


def test_shared_test_conn_docstring_warns_about_durable_writes() -> None:
    """``get_test_conn`` must keep its durability warning so any caller
    reading the source can't miss the isolation trade-off. If someone
    ever simplifies the docstring and drops the warning, this test
    fails — the contract is pinned in source form, not just in behavior.
    """
    doc = (_pg_test_conn.get_test_conn.__doc__ or "").lower()
    assert "writes are durable" in doc, (
        "get_test_conn() must document that writes are durable — operators "
        "pick between shared and isolated based on this warning."
    )
    assert "isolated" in doc or "transactional" in doc, (
        "get_test_conn() must steer callers toward the isolated/transactional "
        "helpers for write tests."
    )


# -- 2. both helpers still exported -------------------------------------


def test_pg_test_conn_exports_both_shared_and_isolated_helpers() -> None:
    """The isolation contract relies on both helpers being available.
    If the isolated helper ever disappears, every test that needs
    rollback semantics has no escape hatch and silently regresses onto
    the shared helper. Pin the export surface."""
    assert callable(getattr(_pg_test_conn, "get_test_conn", None))
    assert callable(getattr(_pg_test_conn, "get_isolated_conn", None))
    assert callable(getattr(_pg_test_conn, "transactional_test_conn", None))


# -- 3. the core guardrail ---------------------------------------------


def test_no_test_file_mixes_bug_writer_and_shared_conn() -> None:
    """The core BUG-E569CC49 pin.

    Any test file that both (a) imports the bug-surface writer and
    (b) calls the shared ``get_test_conn()`` almost certainly inserts
    bug rows that will outlive the test. This is the exact shape the
    bug was filed against. Catch it in CI.

    Allowlist: this very test file is permitted to reference both
    identifiers (by inspection), as is ``test_pg_test_conn_helpers.py``
    which tests the helper's own behavior.
    """
    allowlist = {
        Path(__file__).resolve(),
        _REPO_ROOT / "tests" / "unit" / "test_pg_test_conn_helpers.py",
    }
    offenders: list[str] = []
    for path in _iter_test_files():
        if path.resolve() in allowlist:
            continue
        text = path.read_text(encoding="utf-8")
        if "get_test_conn(" not in text:
            continue
        # Writer shapes: the BugTracker class, the bug_tracker module,
        # or the MCP tool's bug endpoint. Any of these plus the shared
        # helper is a leak risk.
        touches_bug_writer = (
            "BugTracker" in text
            or "from runtime.bug_tracker" in text
            or "import runtime.bug_tracker" in text
            or "tool_praxis_bugs" in text
        )
        if touches_bug_writer:
            offenders.append(str(path.relative_to(_REPO_ROOT)))
    assert not offenders, (
        f"Test files mix bug-surface writers with the shared auto-commit "
        f"connection (BUG-E569CC49): {offenders}. Switch to "
        f"get_isolated_conn() / transactional_test_conn() for bug writes."
    )


# -- 4. the originally flagged files remain clean ----------------------


@pytest.mark.parametrize(
    "rel",
    [
        "tests/unit/test_api_rest_startup.py",
        "tests/integration/test_observability_hub.py",
        "tests/integration/test_mcp_workflow_server.py",
    ],
)
def test_originally_flagged_files_do_not_use_shared_conn(rel: str) -> None:
    """The three files named in the BUG-E569CC49 report have been cleaned
    up. Pin that cleanup so a future refactor cannot silently reintroduce
    ``get_test_conn()`` in any of them."""
    path = _REPO_ROOT / rel
    if not path.exists():
        pytest.skip(f"{rel} no longer present; cleanup still holds")
    text = path.read_text(encoding="utf-8")
    assert "get_test_conn(" not in text, (
        f"{rel} re-introduced get_test_conn() — the file was flagged in "
        f"BUG-E569CC49 and must stay on the isolated helper."
    )
