"""Anti-bypass tests for the runtime HTTP endpoints consumer boundary.

Consumers of HTTP endpoints in the ``runtime/`` and ``surfaces/`` trees
must resolve ``api_base_url`` / ``launch_url`` / ``dashboard_url`` /
``api_docs_url`` through the authoritative helpers rather than
hand-rolling URL literals or reading ``PRAXIS_API_BASE_URL`` directly.

The authority is:

  * ``runtime.primitive_contracts`` — ``build_runtime_binding_contract``
    and ``resolve_runtime_http_endpoints`` project the endpoint block
    from the native runtime binding; the defaults and env resolution
    live in one place so changes to how we discover the local API base
    propagate to every consumer at once.

This test walks ``runtime/**/*.py`` and ``surfaces/**/*.py`` and fails
when a non-authority module:

  * embeds a literal ``http://127.0.0.1:<port>`` URL, or
  * reads ``PRAXIS_API_BASE_URL`` directly off ``os.environ`` / ``os.getenv``, or
  * embeds a literal ``https?://<host>/app`` or ``https?://<host>/docs`` URL
    (these are the documented launch_url / api_docs_url suffixes and must
    come from the contract).

It enforces:
``decision.2026-04-19.runtime-http-endpoints-consumer-boundary``.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]


# Modules allowed to emit the endpoint literals / env reads because they
# ARE the authority consumers should route through.
_HTTP_ENDPOINTS_AUTHORITY_ALLOWLIST: frozenset[str] = frozenset(
    {
        "runtime/primitive_contracts.py",
    }
)


# Forbidden raw env reads for PRAXIS_API_BASE_URL.
_RAW_API_BASE_URL_READ_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"""os\.environ\.get\(\s*["']PRAXIS_API_BASE_URL["']"""),
    re.compile(r"""os\.environ\[\s*["']PRAXIS_API_BASE_URL["']\s*\]"""),
    re.compile(r"""os\.getenv\(\s*["']PRAXIS_API_BASE_URL["']"""),
)


# Forbidden URL literal patterns. 127.0.0.1 with a port is the documented
# default the contract synthesizes; nobody else should recreate it.
# /app and /docs are the launch_url / api_docs_url suffixes.
_LOCAL_API_URL_LITERAL: re.Pattern[str] = re.compile(
    r"""["']http://127\.0\.0\.1:"""
)
_LAUNCH_OR_DOCS_URL_LITERAL: re.Pattern[str] = re.compile(
    r"""["']https?://[^"'\s]+/(?:app|docs)["']"""
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
def test_no_raw_praxis_api_base_url_env_reads(scanned_root: Path) -> None:
    """No module outside the contract may read PRAXIS_API_BASE_URL directly."""

    offenders: list[str] = []
    for path in _relative_python_files(scanned_root):
        key = _as_relative_key(path)
        if key in _HTTP_ENDPOINTS_AUTHORITY_ALLOWLIST:
            continue
        source = path.read_text(encoding="utf-8")
        for pattern in _RAW_API_BASE_URL_READ_PATTERNS:
            if pattern.search(source):
                offenders.append(f"{key}: matches {pattern.pattern!r}")
                break
    assert not offenders, (
        "Raw PRAXIS_API_BASE_URL env reads detected — route through "
        "runtime.primitive_contracts.resolve_runtime_http_endpoints():\n  - "
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
def test_no_local_api_url_literal(scanned_root: Path) -> None:
    """No module outside the contract may embed http://127.0.0.1:<port> URLs."""

    offenders: list[str] = []
    for path in _relative_python_files(scanned_root):
        key = _as_relative_key(path)
        if key in _HTTP_ENDPOINTS_AUTHORITY_ALLOWLIST:
            continue
        source = path.read_text(encoding="utf-8")
        if _LOCAL_API_URL_LITERAL.search(source):
            offenders.append(key)
    assert not offenders, (
        "Literal http://127.0.0.1:<port> URLs detected — consumers must resolve "
        "api_base_url through runtime.primitive_contracts.resolve_runtime_http_endpoints():"
        "\n  - " + "\n  - ".join(offenders)
    )


@pytest.mark.parametrize(
    "scanned_root",
    [
        _WORKFLOW_ROOT / "runtime",
        _WORKFLOW_ROOT / "surfaces",
    ],
    ids=["runtime", "surfaces"],
)
def test_no_launch_or_docs_url_literal(scanned_root: Path) -> None:
    """No module outside the contract may embed /app or /docs URL literals."""

    offenders: list[str] = []
    for path in _relative_python_files(scanned_root):
        key = _as_relative_key(path)
        if key in _HTTP_ENDPOINTS_AUTHORITY_ALLOWLIST:
            continue
        source = path.read_text(encoding="utf-8")
        if _LAUNCH_OR_DOCS_URL_LITERAL.search(source):
            offenders.append(key)
    assert not offenders, (
        "Literal https?://<host>/{app,docs} URLs detected — consumers must resolve "
        "launch_url / api_docs_url through "
        "runtime.primitive_contracts.resolve_runtime_http_endpoints():\n  - "
        + "\n  - ".join(offenders)
    )


def test_http_endpoints_authority_allowlist_paths_exist() -> None:
    """Fail loudly if an allowlist entry drifts out of sync with the filesystem."""
    for relative in _HTTP_ENDPOINTS_AUTHORITY_ALLOWLIST:
        path = _WORKFLOW_ROOT / relative
        assert path.is_file(), (
            f"allowlisted HTTP endpoints authority path does not exist: {relative}"
        )


def test_contract_resolves_api_base_url_from_env() -> None:
    """Smoke check: the contract actually projects the endpoints we enforce."""
    from runtime.primitive_contracts import resolve_runtime_http_endpoints

    endpoints = resolve_runtime_http_endpoints(
        workflow_env={"PRAXIS_API_BASE_URL": "http://api.test:9010"},
        native_instance={},
    )
    assert endpoints["api_base_url"] == "http://api.test:9010"
    assert endpoints["workflow_api_base_url"] == "http://api.test:9010"
    assert endpoints["launch_url"] == "http://api.test:9010/app"
    assert endpoints["api_docs_url"] == "http://api.test:9010/docs"


def test_contract_synthesizes_default_when_env_missing() -> None:
    """Smoke check: the contract owns the 127.0.0.1:<port> fallback."""
    from runtime.primitive_contracts import resolve_runtime_http_endpoints

    endpoints = resolve_runtime_http_endpoints(
        workflow_env={"PRAXIS_API_PORT": "9042"},
        native_instance={},
    )
    assert endpoints["api_base_url"] == "http://127.0.0.1:9042"
    assert endpoints["workflow_api_base_url"] == "http://127.0.0.1:9042"
    assert endpoints["launch_url"] == "http://127.0.0.1:9042/app"
    assert endpoints["api_docs_url"] == "http://127.0.0.1:9042/docs"


def test_contract_projects_separate_workflow_api_base_when_configured() -> None:
    from runtime.primitive_contracts import resolve_runtime_http_endpoints

    endpoints = resolve_runtime_http_endpoints(
        workflow_env={
            "PRAXIS_API_BASE_URL": "http://api.test:9010",
            "PRAXIS_WORKFLOW_API_BASE_URL": "http://workflow.test:9555",
        },
        native_instance={},
    )
    assert endpoints["api_base_url"] == "http://api.test:9010"
    assert endpoints["workflow_api_base_url"] == "http://workflow.test:9555"
    assert endpoints["workflow_api_authority_source"] == "env:PRAXIS_WORKFLOW_API_BASE_URL"
