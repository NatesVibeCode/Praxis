"""compile_runs trace recorder — single source of truth for what just happened.

The Canvas "Describe it" / compile_prose path can degrade silently in five
distinct ways:

  1. LLM was requested but routing missed (no llm_task primary for the task
     type). compile_prose falls through to the deterministic path.
  2. LLM fired but no persona was loaded — the prompt is ad-hoc inline text
     with no response_contract — output comes back empty / prose-shaped.
  3. Artifact cache replayed a stale result — caller cannot tell whether the
     LLM ran this turn.
  4. LLM fired correctly but the build_authority_bundle pass produced 0
     bindings / 0 pills / 0 release_gates because the LLM emitted prose
     without typed structure.
  5. compile_prose raised before persisting — caller sees the exception but
     no durable record of what was attempted.

This module records each compile_prose invocation as one ``compile_runs``
row before, during, and after the call. The row is the single source of
truth for "what just happened in compile?"  Use ``praxis_compile_trace`` to
read recent rows; use ``compile_health`` view for aggregate degradation
signals.
"""

from __future__ import annotations

import hashlib
import logging
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class MaterializeRunTrace:
    """Mutable accumulator for one compile_prose invocation's provenance.

    Caller threads the trace object through the compile pipeline, mutating
    fields as each lane resolves (LLM routing, persona load, cache hit,
    build_authority output). At the end of the call ``record`` writes one
    durable row to ``compile_runs``.
    """

    compile_run_id: str = field(default_factory=lambda: str(uuid4()))
    workflow_id: str | None = None
    title: str | None = None
    prose_sha256: str = ""
    prose_preview: str = ""
    task_type_requested: str = "build"

    llm_requested: bool = False
    llm_fired: bool = False
    llm_skip_reason: str | None = None
    provider_slug: str | None = None
    model_slug: str | None = None
    persona_profile_id: str | None = None
    persona_resolved: bool = False

    cache_hit: bool = False
    cache_reason: str | None = None

    node_count: int = 0
    edge_count: int = 0
    pill_count: int = 0
    gate_count: int = 0
    binding_count: int = 0
    deterministic_fallback: bool = False

    started_monotonic_ns: int = field(default_factory=time.monotonic_ns)
    duration_ms: int = 0
    status: str = "completed"
    error_code: str | None = None
    error_detail: str | None = None

    def mark_failed(self, exc: BaseException) -> None:
        self.status = "failed"
        self.error_code = type(exc).__name__
        self.error_detail = str(exc)[:500]

    def measure_definition(self, definition: dict[str, Any] | None) -> None:
        """Walk the compiled definition and count structural pieces.

        Empty counts are exactly what we want to surface — a fresh LLM-fired
        run that returned 0 nodes / 0 pills / 0 gates is the silent failure
        mode the trace exists to expose.
        """
        if not isinstance(definition, dict):
            return
        graph = (
            definition.get("build_graph")
            or definition.get("definition_graph")
            or {}
        )
        if isinstance(graph, dict):
            nodes = graph.get("nodes") or definition.get("nodes") or []
            edges = graph.get("edges") or definition.get("edges") or []
        else:
            nodes = definition.get("nodes") or []
            edges = definition.get("edges") or []
        self.node_count = len(nodes) if isinstance(nodes, list) else 0
        self.edge_count = len(edges) if isinstance(edges, list) else 0
        binding_ledger = definition.get("binding_ledger") or []
        if isinstance(binding_ledger, list):
            self.binding_count = len(binding_ledger)
        pills = 0
        gates = 0
        if isinstance(nodes, list):
            for node in nodes:
                if not isinstance(node, dict):
                    continue
                node_pills = node.get("data_pills") or node.get("bindings") or []
                if isinstance(node_pills, list):
                    pills += len(node_pills)
                node_gates = node.get("release_gates") or node.get("gates") or []
                if isinstance(node_gates, list):
                    gates += len(node_gates)
        self.pill_count = pills
        self.gate_count = gates


def begin_compile_trace(
    *,
    prose: str,
    title: str | None = None,
    workflow_id: str | None = None,
    task_type: str = "build",
    llm_requested: bool = False,
) -> MaterializeRunTrace:
    """Open a new trace at the start of compile_prose.

    Records prose hash + preview so a later trace read can identify which
    input was being compiled without storing the full prose.
    """
    trace = MaterializeRunTrace()
    trace.workflow_id = (workflow_id or None)
    trace.title = (title or None)
    trace.task_type_requested = task_type or "build"
    trace.llm_requested = bool(llm_requested)
    sha = hashlib.sha256((prose or "").encode("utf-8")).hexdigest()
    trace.prose_sha256 = sha
    trace.prose_preview = (prose or "")[:240]
    return trace


def record_compile_trace(conn: Any, trace: MaterializeRunTrace) -> None:
    """Persist one compile_runs row.

    Best-effort: a failure to record the trace must never block the compile
    flow. The whole point of the trace is to expose silent degradation, not
    introduce a new one.
    """
    try:
        if conn is None:
            return
        if trace.duration_ms == 0 and trace.started_monotonic_ns:
            trace.duration_ms = int(
                (time.monotonic_ns() - trace.started_monotonic_ns) / 1_000_000
            )
        execute = getattr(conn, "execute", None)
        if execute is None:
            return
        execute(
            """
            INSERT INTO compile_runs (
                compile_run_id,
                workflow_id,
                title,
                prose_sha256,
                prose_preview,
                task_type_requested,
                llm_requested,
                llm_fired,
                llm_skip_reason,
                provider_slug,
                model_slug,
                persona_profile_id,
                persona_resolved,
                cache_hit,
                cache_reason,
                node_count,
                edge_count,
                pill_count,
                gate_count,
                binding_count,
                deterministic_fallback,
                duration_ms,
                status,
                error_code,
                error_detail,
                started_at,
                finished_at
            ) VALUES (
                $1::uuid,
                $2, $3, $4, $5, $6,
                $7, $8, $9,
                $10, $11, $12, $13,
                $14, $15,
                $16, $17, $18, $19, $20,
                $21,
                $22::INTEGER, $23, $24, $25,
                now() - make_interval(secs => $22::INTEGER / 1000.0),
                now()
            )
            """,
            trace.compile_run_id,
            trace.workflow_id,
            trace.title,
            trace.prose_sha256,
            trace.prose_preview,
            trace.task_type_requested,
            trace.llm_requested,
            trace.llm_fired,
            trace.llm_skip_reason,
            trace.provider_slug,
            trace.model_slug,
            trace.persona_profile_id,
            trace.persona_resolved,
            trace.cache_hit,
            trace.cache_reason,
            trace.node_count,
            trace.edge_count,
            trace.pill_count,
            trace.gate_count,
            trace.binding_count,
            trace.deterministic_fallback,
            trace.duration_ms,
            trace.status,
            trace.error_code,
            trace.error_detail,
        )
    except Exception as exc:  # noqa: BLE001 — observability must never block
        logger.warning("compile_runs trace record failed: %s", exc)


@contextmanager
def compile_trace_scope(
    conn: Any,
    *,
    prose: str,
    title: str | None = None,
    workflow_id: str | None = None,
    task_type: str = "build",
    llm_requested: bool = False,
):
    """Context manager: opens a trace, yields it, persists at exit.

    Caller mutates fields on the yielded trace as the compile lane resolves.
    On exception, marks the trace as failed and persists with error details
    before re-raising.
    """
    trace = begin_compile_trace(
        prose=prose,
        title=title,
        workflow_id=workflow_id,
        task_type=task_type,
        llm_requested=llm_requested,
    )
    try:
        yield trace
    except BaseException as exc:
        trace.mark_failed(exc)
        record_compile_trace(conn, trace)
        raise
    else:
        record_compile_trace(conn, trace)


__all__ = [
    "MaterializeRunTrace",
    "begin_compile_trace",
    "compile_trace_scope",
    "record_compile_trace",
]
