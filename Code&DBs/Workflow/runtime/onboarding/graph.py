"""Gate-probe graph authority for Praxis onboarding.

One module owns: probe contracts, probe registration, DAG traversal, and the
module-level ``ONBOARDING_GRAPH`` singleton every surface consumes. Probes are
pure reads — they return a ``GateResult`` describing current state plus a
copy-pasteable ``remediation_hint``. Mutations are separate ``GateApply``
handlers; Packet 1 registers probes only.
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

GateStatus = Literal["ok", "missing", "blocked", "unknown"]


@dataclass(frozen=True, slots=True)
class GateProbe:
    gate_ref: str
    domain: str
    title: str
    purpose: str
    depends_on: tuple[str, ...] = ()
    platforms: tuple[str, ...] = ()
    ok_cache_ttl_s: int = 300


@dataclass(frozen=True, slots=True)
class GateResult:
    gate_ref: str
    status: GateStatus
    observed_state: Mapping[str, Any]
    remediation_hint: str | None
    remediation_doc_url: str | None
    apply_ref: str | None
    evaluated_at: datetime


@dataclass(frozen=True, slots=True)
class GateApply:
    apply_ref: str
    gate_ref: str
    description: str
    handler: Callable[..., GateResult]
    mutates: tuple[str, ...]
    requires_approval: bool = True


ProbeFn = Callable[[Mapping[str, str], Path], GateResult]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def gate_result(
    probe: GateProbe,
    *,
    status: GateStatus,
    observed_state: Mapping[str, Any] | None = None,
    remediation_hint: str | None = None,
    remediation_doc_url: str | None = None,
    apply_ref: str | None = None,
) -> GateResult:
    return GateResult(
        gate_ref=probe.gate_ref,
        status=status,
        observed_state=dict(observed_state or {}),
        remediation_hint=remediation_hint,
        remediation_doc_url=remediation_doc_url,
        apply_ref=apply_ref,
        evaluated_at=_now(),
    )


def _platform_matches(probe: GateProbe, platform: str) -> bool:
    if not probe.platforms:
        return True
    return any(platform == p or platform.startswith(p) for p in probe.platforms)


class GateGraphError(RuntimeError):
    """Raised when the gate graph is misconfigured (duplicate ref, cycle)."""


class GateGraph:
    def __init__(self) -> None:
        self._probes: dict[str, tuple[GateProbe, ProbeFn]] = {}
        self._applies: dict[str, GateApply] = {}

    def register(self, probe: GateProbe, fn: ProbeFn) -> None:
        if probe.gate_ref in self._probes:
            raise GateGraphError(f"duplicate gate_ref: {probe.gate_ref}")
        self._probes[probe.gate_ref] = (probe, fn)

    def register_apply(self, apply: GateApply) -> None:
        if apply.apply_ref in self._applies:
            raise GateGraphError(f"duplicate apply_ref: {apply.apply_ref}")
        if apply.gate_ref not in self._probes:
            raise GateGraphError(
                f"apply {apply.apply_ref} targets unregistered gate {apply.gate_ref}"
            )
        self._applies[apply.apply_ref] = apply

    def probe(self, gate_ref: str) -> GateProbe:
        if gate_ref not in self._probes:
            raise GateGraphError(f"unknown gate_ref: {gate_ref}")
        return self._probes[gate_ref][0]

    def probes(self) -> tuple[GateProbe, ...]:
        return tuple(probe for probe, _ in self._probes.values())

    def applies(self) -> tuple[GateApply, ...]:
        return tuple(self._applies.values())

    def evaluate(
        self,
        env: Mapping[str, str],
        repo_root: Path,
        *,
        platform: str | None = None,
        conn: Any = None,
        use_cache: bool = True,
    ) -> list[GateResult]:
        platform_ref = platform or sys.platform
        order = self._topological_order()
        results: dict[str, GateResult] = {}

        cached: dict[str, GateResult] = {}
        if conn is not None and use_cache:
            try:
                from .persistence import read_all_gate_states

                cached = read_all_gate_states(conn)
            except Exception:
                cached = {}

        for gate_ref in order:
            probe, fn = self._probes[gate_ref]
            if not _platform_matches(probe, platform_ref):
                continue
            blocking = [
                dep
                for dep in probe.depends_on
                if dep in results and results[dep].status != "ok"
            ]
            if blocking:
                results[gate_ref] = gate_result(
                    probe,
                    status="blocked",
                    observed_state={"blocking_gates": blocking},
                    remediation_hint=(
                        f"Resolve prerequisite gate(s) first: {', '.join(blocking)}"
                    ),
                )
                continue
            if gate_ref in cached:
                results[gate_ref] = cached[gate_ref]
                continue
            try:
                fresh = fn(env, repo_root)
            except Exception as exc:
                fresh = gate_result(
                    probe,
                    status="unknown",
                    observed_state={
                        "error": str(exc),
                        "error_type": type(exc).__name__,
                    },
                )
            results[gate_ref] = fresh
            if conn is not None:
                try:
                    from .persistence import write_gate_state

                    write_gate_state(conn, fresh, probe, platform=platform_ref)
                except Exception:
                    pass
        return list(results.values())

    def apply_gate(
        self,
        apply_ref: str,
        env: Mapping[str, str],
        repo_root: Path,
        *,
        applied_by: str = "onboarding_apply",
        conn: Any = None,
        **kwargs: Any,
    ) -> GateResult:
        if apply_ref not in self._applies:
            raise GateGraphError(f"unknown apply_ref: {apply_ref}")
        apply = self._applies[apply_ref]
        probe, _fn = self._probes[apply.gate_ref]
        result = apply.handler(env, repo_root, **kwargs)
        if conn is not None:
            try:
                from .persistence import write_gate_state

                write_gate_state(
                    conn,
                    result,
                    probe,
                    applied_by=applied_by,
                    applied_at=_now(),
                )
            except Exception:
                pass
        return result

    def apply_for_gate(self, gate_ref: str) -> GateApply | None:
        for apply in self._applies.values():
            if apply.gate_ref == gate_ref:
                return apply
        return None

    def _topological_order(self) -> list[str]:
        in_degree: dict[str, int] = {ref: 0 for ref in self._probes}
        for probe, _ in self._probes.values():
            for dep in probe.depends_on:
                if dep not in self._probes:
                    raise GateGraphError(
                        f"{probe.gate_ref} depends on unregistered gate {dep}"
                    )
                in_degree[probe.gate_ref] += 1
        queue = sorted(ref for ref, deg in in_degree.items() if deg == 0)
        order: list[str] = []
        while queue:
            ref = queue.pop(0)
            order.append(ref)
            for other_ref, (other_probe, _) in self._probes.items():
                if ref in other_probe.depends_on:
                    in_degree[other_ref] -= 1
                    if in_degree[other_ref] == 0:
                        queue.append(other_ref)
                        queue.sort()
        if len(order) != len(self._probes):
            remaining = sorted(set(self._probes) - set(order))
            raise GateGraphError(
                f"cycle detected in gate graph; cannot order: {remaining}"
            )
        return order


ONBOARDING_GRAPH = GateGraph()
