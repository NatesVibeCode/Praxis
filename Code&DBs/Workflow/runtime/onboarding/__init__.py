"""Praxis onboarding gate-probe graph authority.

One probe authority, many surfaces. Every onboarding gate — platform prereqs,
runtime readiness, provider credentials, MCP integration — is a
``GateProbe`` registered in ``ONBOARDING_GRAPH``. CLI, HTTP, and future Moon
renderers all read from the same graph.

Public API:
    from runtime.onboarding import ONBOARDING_GRAPH
    results = ONBOARDING_GRAPH.evaluate(env, repo_root)
"""

from __future__ import annotations

from . import applies, probes_mcp, probes_platform, probes_provider, probes_runtime
from .graph import (
    GateApply,
    GateGraph,
    GateGraphError,
    GateProbe,
    GateResult,
    GateStatus,
    ONBOARDING_GRAPH,
)
from .persistence import (
    read_all_gate_states,
    read_gate_state,
    write_gate_state,
)

__all__ = [
    "GateApply",
    "GateGraph",
    "GateGraphError",
    "GateProbe",
    "GateResult",
    "GateStatus",
    "ONBOARDING_GRAPH",
    "read_all_gate_states",
    "read_gate_state",
    "write_gate_state",
]


def _register_all() -> None:
    probes_platform.register(ONBOARDING_GRAPH)
    probes_runtime.register(ONBOARDING_GRAPH)
    probes_provider.register(ONBOARDING_GRAPH)
    probes_mcp.register(ONBOARDING_GRAPH)
    applies.register(ONBOARDING_GRAPH)


_register_all()
