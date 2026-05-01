"""Regression tests for praxis_reload runtime_modules scope.

BUG-FE3A8255: before the fix, praxis_reload only cleared DB/config caches.
A hot-fix to a runtime/*.py module was invisible through the MCP surface
until the entire subprocess was restarted. The fix adds a
runtime_modules scope that importlib.reloads an allowlisted set of pure
runtime modules, so the operator hot-fix flow no longer requires a
disruptive MCP subprocess restart.

State-bearing modules (connection pools, registries bound to handler
closures) are excluded from the allowlist and rejected with a structured
reason_code, so nothing silently half-reloads.
"""

from __future__ import annotations

import json
import sys

from surfaces.mcp.tools import health as health_tools
from surfaces.mcp.tools.health import (
    _RUNTIME_RELOAD_ALLOWLIST_PREFIXES,
    _module_is_reload_allowed,
    tool_praxis_reload,
)


def test_allowlist_covers_modules_commonly_hot_fixed():
    """The modules we recently had to fix during live debugging — the ones
    that prompted this bug — must all be on the allowlist. If someone later
    narrows the allowlist without grep'ing for this test, it'll fire."""
    must_be_allowed = (
        "runtime.workflow_spec",
        "runtime.workflow_validation",
        "runtime.workflow_chain",
        "runtime.workflow._context_building",
        "runtime.capability.resolver",
        "runtime.materialize_reuse",
    )
    for name in must_be_allowed:
        assert _module_is_reload_allowed(name), (
            f"{name} must be allowlisted — fixing it was the motivating "
            f"scenario for BUG-FE3A8255."
        )


def test_state_bearing_modules_are_not_allowlisted():
    """Connection pools, MCP handler registration modules, and supervisor
    threads must stay excluded — reloading them mid-process would corrupt
    state that has no safe re-initialization path inside a live subprocess."""
    must_be_rejected = (
        "storage.postgres.connection",
        "surfaces.mcp.tools.health",
        "surfaces.mcp.tools.reload",
        "surfaces.mcp.catalog",
        "runtime.praxis_supervisor",
        "runtime.daily_heartbeat",
        "registry.provider_execution_registry",
        "runtime.verification",
        "runtime.workflow._admission",
        "runtime.workflow.execution_backends",
        "runtime.self_healing",
    )
    for name in must_be_rejected:
        assert not _module_is_reload_allowed(name), (
            f"{name} is state-bearing and must NOT be allowlisted for "
            f"importlib.reload; doing so corrupts handler closures / pools."
        )


def test_default_scope_back_compat_caches_only():
    """Existing callers of praxis_reload() with no params must keep getting
    the old behavior: caches cleared, no runtime-module reload. The
    'runtime_modules' key must be absent from the default-scope result."""
    result = tool_praxis_reload({})
    assert result["scope"] == "caches"
    assert "runtime_modules" not in result
    # Cache keys from the pre-fix path still show up.
    cleared = " ".join(result.get("reloaded", []))
    assert "model_context_limits" in cleared or "FAILED" in cleared


def test_invalid_scope_rejected_with_reason_code():
    result = tool_praxis_reload({"scope": "bogus"})
    assert result.get("reason_code") == "runtime.reload.invalid_scope"
    assert "error" in result


def test_invalid_modules_param_type_rejected():
    result = tool_praxis_reload(
        {"scope": "runtime_modules", "modules": "runtime.workflow_spec"}
    )
    assert result.get("reason_code") == "runtime.reload.invalid_modules_param"


def test_non_allowlisted_module_rejected_structured_error():
    """Operator asks to reload a state-bearing module → structured reject,
    no silent half-reload. Allowlist prefixes surfaced in the response so
    the operator knows what's eligible."""
    result = tool_praxis_reload(
        {
            "scope": "runtime_modules",
            "modules": ["runtime.workflow_spec", "storage.postgres.connection"],
        }
    )
    rt = result["runtime_modules"]
    assert rt["reason_code"] == "runtime.reload.module_not_allowlisted"
    assert "storage.postgres.connection" in rt["rejected"]
    assert rt["reloaded"] == []  # all-or-nothing on validation failure
    assert "runtime.workflow_spec" in " ".join(rt["allowlist_prefixes"]) or any(
        "runtime.workflow_spec" in p for p in rt["allowlist_prefixes"]
    )


def test_explicit_module_list_reloads_requested_allowlisted_module():
    """Happy path: caller passes an allowlisted module that's already
    imported → it gets importlib.reload'd and returned in the reloaded list."""
    # Guarantee the module is in sys.modules before we ask for reload.
    import runtime.workflow_spec  # noqa: F401

    assert "runtime.workflow_spec" in sys.modules

    result = tool_praxis_reload(
        {"scope": "runtime_modules", "modules": ["runtime.workflow_spec"]}
    )
    rt = result["runtime_modules"]
    assert "runtime.workflow_spec" in rt["reloaded"]
    assert rt["failed"] == []
    assert rt["count"] == 1


def test_default_module_list_reloads_every_imported_allowlisted_module():
    """scope=runtime_modules with no modules list → reloads whichever of
    the allowlisted modules are currently in sys.modules. We import a
    known-allowlisted one and assert it shows up."""
    import runtime.materialize_reuse  # noqa: F401

    result = tool_praxis_reload({"scope": "runtime_modules"})
    rt = result["runtime_modules"]
    assert "runtime.materialize_reuse" in rt["reloaded"]
    assert rt["failed"] == []


def test_scope_all_clears_caches_and_reloads_modules_in_one_call():
    """scope=all is the operator-friendly 'full local refresh' — caches +
    runtime module reload. Both sections must be populated in the result."""
    import runtime.workflow_spec  # noqa: F401

    result = tool_praxis_reload({"scope": "all"})
    assert result["scope"] == "all"
    # Caches cleared (at least the model_context_limits / mcp_catalog /
    # context_cache entries appear in result["reloaded"]).
    assert len(result["reloaded"]) >= 1
    # Runtime section present.
    assert "runtime_modules" in result
    assert "runtime.workflow_spec" in result["runtime_modules"]["reloaded"]


class _AuditConn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *args):
        self.calls.append((query, args))


def test_reload_audit_records_runtime_module_result(monkeypatch):
    import runtime.workflow_spec  # noqa: F401

    conn = _AuditConn()
    monkeypatch.setattr(health_tools._subs, "get_pg_conn", lambda: conn)

    result = tool_praxis_reload(
        {"scope": "runtime_modules", "modules": ["runtime.workflow_spec"]}
    )

    assert result["audit"]["system_event_recorded"] is True
    audit_calls = [
        args
        for query, args in conn.calls
        if "INSERT INTO system_events" in query and args and args[0] == "runtime.reload"
    ]
    assert len(audit_calls) == 1
    payload = json.loads(audit_calls[0][3])
    assert payload["requested_modules"] == ["runtime.workflow_spec"]
    assert "runtime.workflow_spec" in payload["runtime_modules_reloaded"]
    assert payload["runtime_modules_failed"] == []
    assert payload["runtime_modules"]["count"] == 1
    assert isinstance(payload["process_id"], int)


def test_allowlist_prefixes_exposed_as_module_level_constant():
    """The allowlist is a module-level constant so other tests and future
    operator UIs can read it programmatically instead of string-matching
    the description. Declared positional contract: tuple of str."""
    assert isinstance(_RUNTIME_RELOAD_ALLOWLIST_PREFIXES, tuple)
    for p in _RUNTIME_RELOAD_ALLOWLIST_PREFIXES:
        assert isinstance(p, str) and p
