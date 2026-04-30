"""Compose-time canonical authority resolver.

Given a set of target authority units, return the canonical write scope, the
read-only predecessor obligation pack, and explicit blocked-compat units.

The resolver is the engine behind the active prevention thesis: the agent
should not be able to write duplicate authority because the workspace it is
handed contains only one canonical owner per type-flow. Predecessors stay
visible as a *read-only* obligation pack — "do not imitate; preserve these
tested invariants" — never invisible (invisibility hides obligations the
successor must honor).

Today the resolver reads `authority_supersession_registry` rows populated
by candidate materialization (see candidate_materialization.materialize_candidate
which writes one row per validated `intent IN ('replace','retire')` impact).
Operators can also insert rows manually for legacy supersessions that
predate the impact contract.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any


_VALID_UNIT_KINDS = frozenset(
    {
        "operation_ref",
        "authority_object_ref",
        "data_dictionary_object_kind",
        "http_route",
        "mcp_tool",
        "cli_alias",
        "migration_ref",
        "database_object",
        "handler_ref",
        "verifier_ref",
        "event_type",
        "provider_route_ref",
        "source_path",
    }
)


@dataclass(frozen=True, slots=True)
class TargetUnit:
    """One requested target the caller wants to compose work against."""

    unit_kind: str
    unit_ref: str


@dataclass(frozen=True, slots=True)
class CanonicalUnit:
    """A unit the caller may write through (the live canonical authority)."""

    unit_kind: str
    unit_ref: str
    requested_target: TargetUnit
    was_redirected: bool


@dataclass(slots=True)
class PredecessorObligation:
    """One predecessor that must be read but not extended."""

    predecessor_unit_kind: str
    predecessor_unit_ref: str
    successor_unit_kind: str
    successor_unit_ref: str
    supersession_status: str
    obligation_summary: str | None
    obligation_evidence: dict[str, Any]
    source_candidate_id: str | None
    source_impact_id: str | None
    source_decision_ref: str | None


@dataclass(slots=True)
class ComposeAuthorityBinding:
    """Resolved canonical authority binding for a compose-time request."""

    canonical_write_scope: list[CanonicalUnit] = field(default_factory=list)
    predecessor_obligations: list[PredecessorObligation] = field(default_factory=list)
    blocked_compat_units: list[PredecessorObligation] = field(default_factory=list)
    unresolved_targets: list[TargetUnit] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "canonical_write_scope": [asdict(unit) for unit in self.canonical_write_scope],
            "predecessor_obligations": [asdict(ob) for ob in self.predecessor_obligations],
            "blocked_compat_units": [asdict(ob) for ob in self.blocked_compat_units],
            "unresolved_targets": [asdict(t) for t in self.unresolved_targets],
            "notes": list(self.notes),
        }


def _normalize_targets(raw_targets: Iterable[Mapping[str, Any]] | None) -> list[TargetUnit]:
    if not raw_targets:
        return []
    normalized: list[TargetUnit] = []
    for index, raw in enumerate(raw_targets):
        if not isinstance(raw, Mapping):
            raise ValueError(f"targets[{index}] must be an object")
        unit_kind = str(raw.get("unit_kind") or "").strip().lower()
        unit_ref = str(raw.get("unit_ref") or "").strip()
        if unit_kind not in _VALID_UNIT_KINDS:
            raise ValueError(
                f"targets[{index}].unit_kind={unit_kind!r} is not a valid authority unit kind"
            )
        if not unit_ref:
            raise ValueError(f"targets[{index}].unit_ref is required")
        normalized.append(TargetUnit(unit_kind=unit_kind, unit_ref=unit_ref))
    return normalized


def _query_canonical_successor(
    conn: Any, *, unit_kind: str, unit_ref: str
) -> dict[str, Any] | None:
    row = conn.fetchrow(
        """
        SELECT successor_unit_kind::text AS successor_unit_kind,
               successor_unit_ref,
               supersession_status::text AS supersession_status,
               supersession_id::text AS supersession_id,
               updated_at
          FROM authority_canonical_successor_for
         WHERE predecessor_unit_kind = $1::candidate_authority_unit_kind
           AND predecessor_unit_ref = $2
        """,
        unit_kind,
        unit_ref,
    )
    return None if row is None else dict(row)


def _query_active_predecessors(
    conn: Any,
    *,
    successors: Sequence[tuple[str, str]],
) -> list[PredecessorObligation]:
    if not successors:
        return []
    successor_kinds = [pair[0] for pair in successors]
    successor_refs = [pair[1] for pair in successors]
    rows = conn.fetch(
        """
        SELECT successor_unit_kind::text AS successor_unit_kind,
               successor_unit_ref,
               predecessor_unit_kind::text AS predecessor_unit_kind,
               predecessor_unit_ref,
               supersession_status::text AS supersession_status,
               obligation_summary,
               obligation_evidence,
               source_candidate_id::text AS source_candidate_id,
               source_impact_id::text AS source_impact_id,
               source_decision_ref
          FROM authority_active_predecessor_obligations
         WHERE (successor_unit_kind::text, successor_unit_ref) IN (
                 SELECT unnest($1::text[]),
                        unnest($2::text[])
             )
        """,
        successor_kinds,
        successor_refs,
    )
    obligations: list[PredecessorObligation] = []
    for row in rows or ():
        evidence = row.get("obligation_evidence")
        if isinstance(evidence, str):
            try:
                import json as _json

                evidence = _json.loads(evidence)
            except Exception:  # noqa: BLE001
                evidence = {}
        if not isinstance(evidence, dict):
            evidence = {}
        obligations.append(
            PredecessorObligation(
                predecessor_unit_kind=str(row["predecessor_unit_kind"]),
                predecessor_unit_ref=str(row["predecessor_unit_ref"]),
                successor_unit_kind=str(row["successor_unit_kind"]),
                successor_unit_ref=str(row["successor_unit_ref"]),
                supersession_status=str(row["supersession_status"]),
                obligation_summary=row.get("obligation_summary"),
                obligation_evidence=evidence,
                source_candidate_id=row.get("source_candidate_id"),
                source_impact_id=row.get("source_impact_id"),
                source_decision_ref=row.get("source_decision_ref"),
            )
        )
    return obligations


def resolve_compose_authority_binding(
    conn: Any,
    *,
    raw_targets: Iterable[Mapping[str, Any]] | None,
) -> ComposeAuthorityBinding:
    """Resolve the canonical write scope + predecessor obligation pack."""

    targets = _normalize_targets(raw_targets)
    binding = ComposeAuthorityBinding()

    if not targets:
        binding.notes.append("no_targets_supplied")
        return binding

    seen_canonical: set[tuple[str, str]] = set()
    for target in targets:
        successor_row = _query_canonical_successor(
            conn,
            unit_kind=target.unit_kind,
            unit_ref=target.unit_ref,
        )
        if successor_row is not None:
            canonical_kind = successor_row["successor_unit_kind"]
            canonical_ref = successor_row["successor_unit_ref"]
            was_redirected = (
                canonical_kind != target.unit_kind or canonical_ref != target.unit_ref
            )
        else:
            canonical_kind = target.unit_kind
            canonical_ref = target.unit_ref
            was_redirected = False

        canonical_key = (canonical_kind, canonical_ref)
        if canonical_key not in seen_canonical:
            seen_canonical.add(canonical_key)
            binding.canonical_write_scope.append(
                CanonicalUnit(
                    unit_kind=canonical_kind,
                    unit_ref=canonical_ref,
                    requested_target=target,
                    was_redirected=was_redirected,
                )
            )
        elif was_redirected:
            binding.notes.append(
                f"target_{target.unit_kind}:{target.unit_ref}_redirected_to_existing_canonical"
            )

        if was_redirected:
            binding.notes.append(
                f"target_{target.unit_kind}:{target.unit_ref}_superseded_by_{canonical_kind}:{canonical_ref}"
            )

    obligations = _query_active_predecessors(
        conn,
        successors=sorted(seen_canonical),
    )

    obligation_keys: set[tuple[str, str]] = set()
    for ob in obligations:
        key = (ob.predecessor_unit_kind, ob.predecessor_unit_ref)
        if key in obligation_keys:
            continue
        obligation_keys.add(key)
        binding.predecessor_obligations.append(ob)
        if ob.predecessor_unit_kind == "source_path":
            binding.blocked_compat_units.append(ob)

    return binding


__all__ = [
    "TargetUnit",
    "CanonicalUnit",
    "PredecessorObligation",
    "ComposeAuthorityBinding",
    "resolve_compose_authority_binding",
]
