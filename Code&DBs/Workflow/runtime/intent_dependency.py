"""Layer 0.5 (Synthesize): deterministic dependency skeleton from atoms."""

from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from runtime.intent_suggestion import (
    ParameterSuggestion,
    StepTypeSuggestion,
    SuggestedAtoms,
)


_PILL_WRITE_VERBS = frozenset({
    "add", "create", "implement", "build", "write", "wire", "update",
    "set", "change", "patch", "fix", "repair", "resolve", "rewrite",
    "refactor", "migrate", "backfill", "seed", "insert", "register",
    "delete", "drop", "remove", "rename", "rollback", "revert",
})

_PILL_READ_VERBS = frozenset({
    "look", "read", "list", "describe", "examine", "expose",
    "consume", "reference", "use", "fetch", "retrieve", "search",
    "find", "investigate", "analyze", "review", "audit", "verify",
    "evaluate", "assess", "score", "compare", "show", "render",
})

_EXPLICIT_ORDER_CUES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bfirst\b", re.IGNORECASE), "first"),
    (re.compile(r"\bthen\b", re.IGNORECASE), "then"),
    (re.compile(r"\bnext\b", re.IGNORECASE), "next"),
    (re.compile(r"\bfinally\b|\blastly\b", re.IGNORECASE), "finally"),
    (re.compile(r"\bafter\s+that\b", re.IGNORECASE), "after_that"),
]


_STAGE_CONFIDENCE_THRESHOLD = 0.5


@dataclass(frozen=True)
class GateScaffold:
    gate_id: str
    gate_kind: str
    runtime_check: bool
    scaffold_priority: int
    params: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "gate_id": self.gate_id, "gate_kind": self.gate_kind,
            "runtime_check": self.runtime_check,
            "scaffold_priority": self.scaffold_priority, "params": dict(self.params),
        }


@dataclass(frozen=True)
class SkeletalPacket:
    label: str
    stage: str
    description: str
    clause_span: str
    clause_offset: int
    consumes_floor: list[str]
    produces_floor: list[str]
    capabilities_floor: list[str]
    gates_scaffold: list[GateScaffold]
    depends_on: list[str]
    pill_writes: list[str]
    pill_reads: list[str]
    confidence: float
    edge_reasons: dict[str, list[str]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "label": self.label, "stage": self.stage, "description": self.description,
            "clause_span": self.clause_span, "clause_offset": self.clause_offset,
            "consumes_floor": list(self.consumes_floor),
            "produces_floor": list(self.produces_floor),
            "capabilities_floor": list(self.capabilities_floor),
            "gates_scaffold": [g.to_dict() for g in self.gates_scaffold],
            "depends_on": list(self.depends_on),
            "pill_writes": list(self.pill_writes),
            "pill_reads": list(self.pill_reads),
            "confidence": round(self.confidence, 3),
            "edge_reasons": {k: list(v) for k, v in self.edge_reasons.items()},
        }


@dataclass(frozen=True)
class SkeletalPlan:
    parameters: list[ParameterSuggestion]
    packets: list[SkeletalPacket]
    notes: list[str]
    stage_contracts: dict[str, dict[str, Any]]
    gate_contracts: dict[str, dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "parameters": [p.to_dict() for p in self.parameters],
            "packets": [p.to_dict() for p in self.packets],
            "notes": list(self.notes),
            "stage_contracts": dict(self.stage_contracts),
            "gate_contracts": dict(self.gate_contracts),
        }


def _load_stage_contracts(conn: Any) -> dict[str, dict[str, Any]]:
    from runtime.data_dictionary import DataDictionaryBoundaryError, list_object_kinds
    out: dict[str, dict[str, Any]] = {}
    try:
        rows = list_object_kinds(conn, category="stage")
    except DataDictionaryBoundaryError:
        return out
    for row in rows:
        object_kind = str(row.get("object_kind") or "")
        stage_name = object_kind.split(":", 1)[1] if ":" in object_kind else object_kind
        out[stage_name] = {
            "object_kind": object_kind,
            "label": row.get("label"),
            "summary": row.get("summary"),
            "metadata": row.get("metadata") or {},
        }
    return out


def _load_gate_contracts(conn: Any) -> dict[str, dict[str, Any]]:
    from runtime.data_dictionary import DataDictionaryBoundaryError, list_object_kinds
    out: dict[str, dict[str, Any]] = {}
    try:
        rows = list_object_kinds(conn, category="gate")
    except DataDictionaryBoundaryError:
        return out
    for row in rows:
        object_kind = str(row.get("object_kind") or "")
        out[object_kind] = {
            "object_kind": object_kind,
            "label": row.get("label"),
            "summary": row.get("summary"),
            "metadata": row.get("metadata") or {},
        }
    return out


def _label_for_clause(stage: str, offset: int, span: str) -> str:
    span_token = re.sub(r"[^a-zA-Z]+", "_", span.lower()).strip("_")
    span_token = "_".join(span_token.split("_")[:3])[:24] or "step"
    return f"{stage}_{offset:04d}_{span_token}".strip("_")


def _classify_pill_use(clause: str, pill_ref: str) -> str:
    clause_lower = clause.lower()
    pill_position = clause_lower.find(pill_ref.lower())
    if pill_position < 0:
        return "read"
    window = clause_lower[max(0, pill_position - 60) : pill_position + len(pill_ref) + 60]
    tokens = re.findall(r"[a-z]+", window)
    if any(token in _PILL_WRITE_VERBS for token in tokens):
        return "write"
    if any(token in _PILL_READ_VERBS for token in tokens):
        return "read"
    return "read"


def _pill_ref(pill: Any) -> str | None:
    """Compose ``object_kind.field_path`` from any pill record shape."""
    object_kind = getattr(pill, "object_kind", None)
    field_path = getattr(pill, "field_path", None)
    if object_kind and field_path:
        return f"{object_kind}.{field_path}"
    return getattr(pill, "ref", None)


def _extract_clause_pill_uses(clause: str, pills: list[Any]) -> tuple[list[str], list[str]]:
    writes: list[str] = []
    reads: list[str] = []
    clause_lower = clause.lower()
    for pill in pills:
        ref = _pill_ref(pill)
        if not ref or ref.lower() not in clause_lower:
            continue
        if _classify_pill_use(clause, ref) == "write":
            writes.append(ref)
        else:
            reads.append(ref)
    return writes, reads


def _pick_step_per_clause(step_types: list[StepTypeSuggestion]) -> list[StepTypeSuggestion]:
    grouped: dict[str, list[StepTypeSuggestion]] = defaultdict(list)
    for s in step_types:
        if s.confidence >= _STAGE_CONFIDENCE_THRESHOLD:
            grouped[s.phrase_span].append(s)
    picks: list[StepTypeSuggestion] = []
    for span, group in grouped.items():
        group.sort(key=lambda s: -s.confidence)
        picks.append(group[0])
    return picks


def _scaffold_gates_for_stage(
    stage_name: str,
    stage_contracts: dict[str, dict[str, Any]],
    gate_contracts: dict[str, dict[str, Any]],
) -> list[GateScaffold]:
    contract = stage_contracts.get(stage_name)
    if not contract:
        return []
    required = list(contract.get("metadata", {}).get("required_gates") or [])
    scaffolds: list[GateScaffold] = []
    for gate_id in required:
        gate = gate_contracts.get(gate_id)
        if not gate:
            scaffolds.append(GateScaffold(
                gate_id=gate_id, gate_kind="unknown",
                runtime_check=False, scaffold_priority=99,
            ))
            continue
        meta = gate.get("metadata", {})
        scaffolds.append(GateScaffold(
            gate_id=gate_id, gate_kind=gate_id,
            runtime_check=bool(meta.get("runtime_check")),
            scaffold_priority=int(meta.get("scaffold_priority") or 50),
        ))
    scaffolds.sort(key=lambda g: g.scaffold_priority)
    return scaffolds


def _stage_floor(stage_name: str, contracts: dict[str, dict[str, Any]], key: str) -> list[str]:
    contract = contracts.get(stage_name)
    if not contract:
        return []
    return list(contract.get("metadata", {}).get(key) or [])


def _explicit_order_index(clause: str) -> int | None:
    rank = {"first": 0, "then": 2, "next": 3, "after_that": 4, "finally": 9}
    matched: list[int] = []
    for pattern, name in _EXPLICIT_ORDER_CUES:
        if pattern.search(clause):
            matched.append(rank[name])
    if not matched:
        return None
    return min(matched)


def _resolve_depends_on(packets: list[SkeletalPacket]) -> list[SkeletalPacket]:
    label_order = [p.label for p in packets]
    edges: dict[str, set[str]] = {p.label: set() for p in packets}
    edge_reasons: dict[str, dict[str, list[str]]] = {p.label: {} for p in packets}

    for i, packet in enumerate(packets):
        upstream = packets[:i]

        for read_ref in packet.pill_reads:
            for prior in reversed(upstream):
                if read_ref in prior.pill_writes:
                    edges[packet.label].add(prior.label)
                    edge_reasons[packet.label].setdefault(prior.label, []).append(
                        f"pill:read-after-write({read_ref})"
                    )
                    break

        for need in packet.consumes_floor:
            for prior in reversed(upstream):
                if need in prior.produces_floor:
                    edges[packet.label].add(prior.label)
                    edge_reasons[packet.label].setdefault(prior.label, []).append(
                        f"stage_io:consumes({need})"
                    )
                    break

        explicit = _explicit_order_index(packet.clause_span)
        if explicit is not None and explicit >= 9:
            for prior in upstream:
                edges[packet.label].add(prior.label)
                edge_reasons[packet.label].setdefault(prior.label, []).append("explicit:finally")

    out: list[SkeletalPacket] = []
    for packet in packets:
        deps = sorted(edges[packet.label], key=lambda label: label_order.index(label))
        out.append(SkeletalPacket(
            label=packet.label, stage=packet.stage,
            description=packet.description, clause_span=packet.clause_span,
            clause_offset=packet.clause_offset,
            consumes_floor=list(packet.consumes_floor),
            produces_floor=list(packet.produces_floor),
            capabilities_floor=list(packet.capabilities_floor),
            gates_scaffold=list(packet.gates_scaffold),
            depends_on=deps, pill_writes=list(packet.pill_writes),
            pill_reads=list(packet.pill_reads), confidence=packet.confidence,
            edge_reasons={k: list(v) for k, v in edge_reasons[packet.label].items()},
        ))
    return out


def skeleton_from_seeds(
    *,
    seeds: list[Any],
    atoms: SuggestedAtoms,
    conn: Any,
) -> SkeletalPlan:
    """Wrap LLM-emitted packet seeds with deterministic floors / gates / depends_on.

    seeds is a list of objects with .label / .stage / .description / .depends_on
    (typically ``PacketSeed`` from runtime.plan_synthesis). Each seed becomes a
    ``SkeletalPacket`` with the stage's contracted produces/consumes/capabilities
    floors and required-gate scaffolds applied. depends_on edges from the LLM
    are preserved as-is (the LLM owns work-volume decomposition); the
    deterministic synthesizer adds NO additional cross-stage edges here so we
    don't fight the LLM's chosen structure.
    """
    notes: list[str] = list(atoms.notes)
    stage_contracts = _load_stage_contracts(conn)
    gate_contracts = _load_gate_contracts(conn)

    if not stage_contracts:
        notes.append("no stage rows registered (data_dictionary category='stage')")

    packets: list[SkeletalPacket] = []
    for index, seed in enumerate(seeds):
        stage = getattr(seed, "stage", None) or "build"
        label = getattr(seed, "label", None) or f"{stage}_{index:04d}"
        description = getattr(seed, "description", None) or ""
        depends_on = list(getattr(seed, "depends_on", []) or [])
        packets.append(SkeletalPacket(
            label=label, stage=stage,
            description=description, clause_span=description,
            clause_offset=index * 100,
            consumes_floor=_stage_floor(stage, stage_contracts, "consumes"),
            produces_floor=_stage_floor(stage, stage_contracts, "produces"),
            capabilities_floor=_stage_floor(stage, stage_contracts, "capabilities"),
            gates_scaffold=_scaffold_gates_for_stage(stage, stage_contracts, gate_contracts),
            depends_on=depends_on,
            pill_writes=[], pill_reads=[],
            confidence=1.0,
        ))

    return SkeletalPlan(
        parameters=list(atoms.parameters), packets=packets, notes=notes,
        stage_contracts=stage_contracts, gate_contracts=gate_contracts,
    )


def synthesize_skeleton(atoms: SuggestedAtoms, *, conn: Any) -> SkeletalPlan:
    notes: list[str] = list(atoms.notes)
    stage_contracts = _load_stage_contracts(conn)
    gate_contracts = _load_gate_contracts(conn)

    if not stage_contracts:
        notes.append("no stage rows registered (data_dictionary category='stage')")

    picks = _pick_step_per_clause(atoms.step_types)
    picks.sort(key=lambda s: atoms.intent.find(s.phrase_span))

    suggested_pills = list(atoms.pills.bound)

    packets: list[SkeletalPacket] = []
    for index, pick in enumerate(picks):
        offset = atoms.intent.find(pick.phrase_span)
        if offset < 0:
            offset = index * 1000
        label = _label_for_clause(pick.suggested_stage, offset, pick.phrase_span)
        writes, reads = _extract_clause_pill_uses(pick.phrase_span, suggested_pills)
        packets.append(SkeletalPacket(
            label=label, stage=pick.suggested_stage,
            description=pick.phrase_span, clause_span=pick.phrase_span,
            clause_offset=offset,
            consumes_floor=_stage_floor(pick.suggested_stage, stage_contracts, "consumes"),
            produces_floor=_stage_floor(pick.suggested_stage, stage_contracts, "produces"),
            capabilities_floor=_stage_floor(pick.suggested_stage, stage_contracts, "capabilities"),
            gates_scaffold=_scaffold_gates_for_stage(pick.suggested_stage, stage_contracts, gate_contracts),
            depends_on=[], pill_writes=writes, pill_reads=reads,
            confidence=pick.confidence,
        ))

    packets = _resolve_depends_on(packets)

    if not packets:
        notes.append("no clauses scored above the stage-confidence threshold")

    return SkeletalPlan(
        parameters=list(atoms.parameters), packets=packets, notes=notes,
        stage_contracts=stage_contracts, gate_contracts=gate_contracts,
    )
