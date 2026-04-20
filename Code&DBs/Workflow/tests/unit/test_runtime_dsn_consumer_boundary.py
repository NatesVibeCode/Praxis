"""Anti-bypass tests for the runtime DSN consumer boundary.

Consumers of ``WORKFLOW_DATABASE_URL`` in the ``runtime/`` and ``surfaces/``
trees must resolve the DSN through the authoritative helpers rather than
reading the environment variable directly. The authorities are:

  * ``runtime._workflow_database`` — ``resolve_runtime_database_authority``
    / ``resolve_runtime_database_url`` / ``workflow_database_url_is_configured``
  * ``surfaces._workflow_database`` — thin surface wrappers that delegate
    to the runtime authority
  * ``runtime.primitive_contracts`` — projects a redacted DSN binding for
    ``/orient`` consumers (never emits the raw DSN)

This test walks ``runtime/**/*.py`` and ``surfaces/**/*.py`` and fails when
a non-authority module reads ``WORKFLOW_DATABASE_URL`` directly off
``os.environ`` / ``os.getenv``, or embeds a literal ``postgres://`` /
``postgresql://`` DSN.

It enforces:
``decision.2026-04-19.runtime-dsn-consumer-boundary``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]


# Modules allowed to read WORKFLOW_DATABASE_URL directly off os.environ /
# os.getenv because they ARE the authority that consumers should route
# through. Paths are relative to _WORKFLOW_ROOT.
_DSN_AUTHORITY_ALLOWLIST: frozenset[str] = frozenset(
    {
        "runtime/_workflow_database.py",
        "surfaces/_workflow_database.py",
        "runtime/primitive_contracts.py",
    }
)


# Modules allowed to embed a literal postgres://... DSN because they own
# a documented authority fallback (docker compose port discovery).
_DSN_LITERAL_ALLOWLIST: frozenset[str] = frozenset(
    {
        "runtime/_workflow_database.py",
    }
)


# Forbidden raw env-read patterns. These are code-level constructs, not
# documentation — they cannot legitimately appear in a consumer module.
_RAW_ENV_READ_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"""os\.environ\.get\(\s*["']WORKFLOW_DATABASE_URL["']"""),
    re.compile(r"""os\.environ\[\s*["']WORKFLOW_DATABASE_URL["']\s*\]"""),
    re.compile(r"""os\.getenv\(\s*["']WORKFLOW_DATABASE_URL["']"""),
)


# Literal DSN prefixes. Hardcoded DSNs bypass the authority resolver.
_DSN_LITERAL_PATTERN: re.Pattern[str] = re.compile(
    r"""["']postgres(?:ql)?://"""
)


def _relative_python_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.py") if p.is_file())


def _as_relative_key(path: Path) -> str:
    return path.relative_to(_WORKFLOW_ROOT).as_posix()


@pytest.mark.parametrize(
    "scanned_root",
    [
        _WORKFLOW_ROOT / "runtime",
        _WORKFLOW_ROOT / "surfaces",
    ],
    ids=["runtime", "surfaces"],
)
def test_no_raw_workflow_database_url_env_reads(scanned_root: Path) -> None:
    """No module outside the DSN authority may read WORKFLOW_DATABASE_URL directly."""

    offenders: list[str] = []
    for path in _relative_python_files(scanned_root):
        key = _as_relative_key(path)
        if key in _DSN_AUTHORITY_ALLOWLIST:
            continue
        source = path.read_text(encoding="utf-8")
        for pattern in _RAW_ENV_READ_PATTERNS:
            if pattern.search(source):
                offenders.append(f"{key}: matches {pattern.pattern!r}")
                break
    assert not offenders, (
        "Raw WORKFLOW_DATABASE_URL env reads detected — route through "
        "runtime._workflow_database (resolve_runtime_database_url / "
        "workflow_database_url_is_configured) or surfaces._workflow_database:\n  - "
        + "\n  - ".join(offenders)
    )


@pytest.mark.parametrize(
    "scanned_root",
    [
        _WORKFLOW_ROOT / "runtime",
        _WORKFLOW_ROOT / "surfaces",
    ],
    ids=["runtime", "surfaces"],
)
def test_no_literal_postgres_dsn(scanned_root: Path) -> None:
    """No module outside the DSN authority may embed a literal postgres:// DSN."""

    offenders: list[str] = []
    for path in _relative_python_files(scanned_root):
        key = _as_relative_key(path)
        if key in _DSN_LITERAL_ALLOWLIST:
            continue
        source = path.read_text(encoding="utf-8")
        if _DSN_LITERAL_PATTERN.search(source):
            offenders.append(key)
    assert not offenders, (
        "Literal postgres://... DSNs detected — consumers must resolve the DSN "
        "through runtime._workflow_database authority helpers, never embed one:\n  - "
        + "\n  - ".join(offenders)
    )


def test_workflow_database_url_is_configured_honors_explicit_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The is_configured helper honors the explicit env mapping over process env."""
    from runtime._workflow_database import workflow_database_url_is_configured

    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    assert workflow_database_url_is_configured(
        {"WORKFLOW_DATABASE_URL": "postgresql://x@y/z"}
    ) is True
    assert workflow_database_url_is_configured({}) is False
    assert workflow_database_url_is_configured() is False

    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://proc@env/db")
    assert workflow_database_url_is_configured() is True
    # Explicit empty env still falls through to process env per helper contract.
    assert workflow_database_url_is_configured({}) is True


def test_dsn_authority_allowlist_paths_exist() -> None:
    """Fail loudly if an allowlist entry drifts out of sync with the filesystem."""
    for relative in _DSN_AUTHORITY_ALLOWLIST | _DSN_LITERAL_ALLOWLIST:
        path = _WORKFLOW_ROOT / relative
        assert path.is_file(), (
            f"allowlisted DSN authority path does not exist: {relative}"
        )
