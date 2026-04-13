"""Intent matching: search registries and compose matched primitives.

Searches registry_ui_components, registry_calculations, and
registry_workflows using Postgres full-text search plus semantic
similarity, then proposes how matched pieces wire together into an app.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any

from runtime.intent_lexicon import expand_query_terms
from storage.postgres.vector_store import PostgresVectorStore


# ---------------------------------------------------------------------------
# Domain models
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class RegistryMatch:
    """Single matched registry entry with relevance score."""

    id: str
    name: str
    description: str
    category: str
    rank: float
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Binding:
    """Proposed binding between a component and a calculation or workflow."""

    source_id: str
    source_type: str  # "ui_component", "calculation", "workflow"
    target_id: str
    target_type: str
    rationale: str


@dataclass(frozen=True, slots=True)
class MatchResult:
    """Result of searching all three registries for an intent."""

    intent: str
    ui_components: tuple[RegistryMatch, ...] = ()
    calculations: tuple[RegistryMatch, ...] = ()
    workflows: tuple[RegistryMatch, ...] = ()
    coverage_score: float = 0.0
    gaps: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CompositionPlan:
    """How matched pieces wire together into an app."""

    components: tuple[str, ...] = ()
    calculations: tuple[str, ...] = ()
    workflows: tuple[str, ...] = ()
    bindings: tuple[Binding, ...] = ()
    layout_suggestion: str = ""
    confidence: float = 0.0


# ---------------------------------------------------------------------------
# Intent Matcher
# ---------------------------------------------------------------------------

# Postgres FTS query template — plainto_tsquery handles unquoted user input.
_SEARCH_SQL = """
SELECT id, name, description, category,
       ts_rank(search_vector, to_tsquery('english', $1)) AS rank
  FROM {table}
 WHERE search_vector @@ to_tsquery('english', $1)
 ORDER BY rank DESC
 LIMIT $2
"""

# Extra column sets per registry for metadata enrichment.
_EXTRA_COLS: dict[str, str] = {
    "registry_ui_components": "props_schema, emits_events, accepts_slots, default_size",
    "registry_calculations": "input_schema, output_schema, execution_type, resource_ref",
    "registry_workflows": "trigger_type, input_schema, output_schema, steps, mcp_tool_refs",
}

_SEARCH_SQL_EXTRA = """
SELECT id, name, description, category,
       ts_rank(search_vector, to_tsquery('english', $1)) AS rank,
       {extra}
  FROM {table}
 WHERE search_vector @@ to_tsquery('english', $1)
 ORDER BY rank DESC
 LIMIT $2
"""

_LAYOUT_HEURISTICS: dict[str, str] = {
    "display": "main",
    "input": "sidebar",
    "action": "toolbar",
    "layout": "main",
}

class IntentMatcher:
    """Search registries by intent and compose matched primitives."""

    def __init__(self, conn, embedder=None) -> None:
        self._conn = conn
        self._embedder = embedder
        self._vector_store = (
            PostgresVectorStore(conn, embedder) if embedder is not None else None
        )

    # -- Public API ---------------------------------------------------------

    def match(self, intent: str, limit: int = 10) -> MatchResult:
        """Search all three registries for primitives relevant to *intent*."""
        if not intent or not intent.strip():
            return MatchResult(intent=intent, coverage_score=0.0, gaps=("empty intent",))

        ui = self._search("registry_ui_components", intent, limit)
        calcs = self._search("registry_calculations", intent, limit)
        wfs = self._search("registry_workflows", intent, limit)

        if self._vector_store is not None:
            vector_query = self._vector_store.prepare(intent)
            ui_vec = self._vector_search(vector_query, "registry_ui_components", limit)
            calcs_vec = self._vector_search(vector_query, "registry_calculations", limit)
            wfs_vec = self._vector_search(vector_query, "registry_workflows", limit)
            ui = self._merge_rrf(ui, ui_vec)
            calcs = self._merge_rrf(calcs, calcs_vec)
            wfs = self._merge_rrf(wfs, wfs_vec)

        coverage = self._compute_coverage(intent, ui, calcs, wfs)
        gaps = self._identify_gaps(intent, ui, calcs, wfs)

        return MatchResult(
            intent=intent,
            ui_components=tuple(ui),
            calculations=tuple(calcs),
            workflows=tuple(wfs),
            coverage_score=coverage,
            gaps=tuple(gaps),
        )

    def compose(self, intent: str, matches: MatchResult) -> CompositionPlan:
        """Propose how matched pieces wire together."""
        comp_ids = tuple(m.id for m in matches.ui_components)
        calc_ids = tuple(m.id for m in matches.calculations)
        wf_ids = tuple(m.id for m in matches.workflows)

        bindings = self._propose_bindings(matches)
        layout = self._suggest_layout(matches)
        confidence = self._compute_confidence(matches, bindings)

        return CompositionPlan(
            components=comp_ids,
            calculations=calc_ids,
            workflows=wf_ids,
            bindings=tuple(bindings),
            layout_suggestion=layout,
            confidence=confidence,
        )

    # -- Internal -----------------------------------------------------------

    @staticmethod
    def _to_or_query(intent: str) -> str:
        """Convert user intent to OR-joined tsquery: 'table chart stats' -> 'table | chart | stats'."""
        words = list(expand_query_terms(intent))
        if not words:
            return intent
        return " | ".join(words)

    def _search(self, table: str, intent: str, limit: int) -> list[RegistryMatch]:
        extra = _EXTRA_COLS.get(table, "")
        if extra:
            sql = _SEARCH_SQL_EXTRA.format(table=table, extra=extra)
        else:
            sql = _SEARCH_SQL.format(table=table)

        query = self._to_or_query(intent)
        rows = self._conn.execute(sql, query, limit)
        results: list[RegistryMatch] = []
        for row in rows:
            meta: dict[str, Any] = {}
            # asyncpg rows support dict() or key access
            row_dict = dict(row) if hasattr(row, "keys") else {}
            for col in ("props_schema", "emits_events", "accepts_slots",
                        "default_size", "input_schema", "output_schema",
                        "execution_type", "resource_ref", "trigger_type",
                        "steps", "mcp_tool_refs"):
                if col in row_dict and row_dict[col] is not None:
                    val = row_dict[col]
                    # jsonb comes back as str or dict depending on driver
                    if isinstance(val, str):
                        try:
                            val = json.loads(val)
                        except (json.JSONDecodeError, TypeError):
                            pass
                    meta[col] = val

            results.append(RegistryMatch(
                id=row["id"],
                name=row["name"],
                description=row["description"],
                category=row["category"],
                rank=float(row["rank"]),
                metadata=meta,
            ))
        return results

    def _vector_search(self, vector_query, table: str, limit: int = 10) -> list[RegistryMatch]:
        """Search *table* using semantic similarity against *intent*."""
        extra = _EXTRA_COLS.get(table, "")
        select_columns = ["id", "name", "description", "category"]
        if extra:
            select_columns.extend(col.strip() for col in extra.split(","))

        rows = vector_query.search(
            table,
            select_columns=tuple(select_columns),
            limit=limit,
            score_alias="rank",
        )
        results: list[RegistryMatch] = []
        for row in rows:
            meta: dict[str, Any] = {}
            row_dict = dict(row) if hasattr(row, "keys") else {}
            for col in ("props_schema", "emits_events", "accepts_slots",
                        "default_size", "input_schema", "output_schema",
                        "execution_type", "resource_ref", "trigger_type",
                        "steps", "mcp_tool_refs"):
                if col in row_dict and row_dict[col] is not None:
                    val = row_dict[col]
                    if isinstance(val, str):
                        try:
                            val = json.loads(val)
                        except (json.JSONDecodeError, TypeError):
                            pass
                    meta[col] = val
            results.append(RegistryMatch(
                id=row["id"],
                name=row["name"],
                description=row["description"],
                category=row["category"],
                rank=float(row["rank"]),
                metadata=meta,
            ))
        return results

    @staticmethod
    def _merge_rrf(
        fts: list[RegistryMatch],
        vec: list[RegistryMatch],
        k: int = 60,
    ) -> list[RegistryMatch]:
        """Merge FTS and vector results using Reciprocal Rank Fusion (k=60).

        Each result's fused score = sum(1 / (k + rank_position)) across
        the lists it appears in. Deduplicates by id, preserving the best
        RegistryMatch metadata seen first.
        """
        scores: dict[str, float] = {}
        best: dict[str, RegistryMatch] = {}

        for i, m in enumerate(fts):
            scores[m.id] = scores.get(m.id, 0.0) + 1.0 / (k + i + 1)
            if m.id not in best:
                best[m.id] = m

        for i, m in enumerate(vec):
            scores[m.id] = scores.get(m.id, 0.0) + 1.0 / (k + i + 1)
            if m.id not in best:
                best[m.id] = m

        return [
            RegistryMatch(
                id=best[mid].id,
                name=best[mid].name,
                description=best[mid].description,
                category=best[mid].category,
                rank=scores[mid],
                metadata=best[mid].metadata,
            )
            for mid in sorted(scores, key=lambda x: scores[x], reverse=True)
        ]

    def _compute_coverage(
        self,
        intent: str,
        ui: list[RegistryMatch],
        calcs: list[RegistryMatch],
        wfs: list[RegistryMatch],
    ) -> float:
        """Estimate how well existing primitives cover the intent.

        Uses a simple heuristic: each category with matches contributes
        up to 1/3 coverage, weighted by best rank in that category.
        """
        scores: list[float] = []
        for group in (ui, calcs, wfs):
            if group:
                best_rank = max(m.rank for m in group)
                # ts_rank is small; normalize loosely — cap at 1.0
                scores.append(min(best_rank / 0.1, 1.0))
            else:
                scores.append(0.0)
        return round(sum(scores) / 3.0, 4)

    def _identify_gaps(
        self,
        intent: str,
        ui: list[RegistryMatch],
        calcs: list[RegistryMatch],
        wfs: list[RegistryMatch],
    ) -> list[str]:
        gaps: list[str] = []
        if not ui:
            gaps.append("no matching UI components")
        if not calcs:
            gaps.append("no matching calculations")
        if not wfs:
            gaps.append("no matching workflows")
        return gaps

    def _propose_bindings(self, matches: MatchResult) -> list[Binding]:
        """Propose bindings by matching input/output schemas between pieces."""
        bindings: list[Binding] = []

        # Build a map of output schemas from calculations
        calc_outputs: dict[str, dict] = {}
        for calc in matches.calculations:
            out_schema = calc.metadata.get("output_schema")
            if out_schema and isinstance(out_schema, dict) and out_schema:
                calc_outputs[calc.id] = out_schema

        # Build a map of input schemas from calculations
        calc_inputs: dict[str, dict] = {}
        for calc in matches.calculations:
            in_schema = calc.metadata.get("input_schema")
            if in_schema and isinstance(in_schema, dict) and in_schema:
                calc_inputs[calc.id] = in_schema

        # Bind UI components to calculations that produce data they could display
        for comp in matches.ui_components:
            cat = comp.category
            if cat == "display" and calc_outputs:
                # Display components consume calculation outputs
                for calc_id in calc_outputs:
                    bindings.append(Binding(
                        source_id=calc_id,
                        source_type="calculation",
                        target_id=comp.id,
                        target_type="ui_component",
                        rationale=f"calculation '{calc_id}' output feeds display component '{comp.id}'",
                    ))
            elif cat == "input" and calc_inputs:
                # Input components feed calculation inputs
                for calc_id in calc_inputs:
                    bindings.append(Binding(
                        source_id=comp.id,
                        source_type="ui_component",
                        target_id=calc_id,
                        target_type="calculation",
                        rationale=f"input component '{comp.id}' feeds calculation '{calc_id}'",
                    ))

        # Bind workflows to calculations they might trigger
        for wf in matches.workflows:
            wf_inputs = wf.metadata.get("input_schema")
            if wf_inputs and isinstance(wf_inputs, dict) and wf_inputs:
                for calc_id, out in calc_outputs.items():
                    bindings.append(Binding(
                        source_id=calc_id,
                        source_type="calculation",
                        target_id=wf.id,
                        target_type="workflow",
                        rationale=f"calculation '{calc_id}' output triggers workflow '{wf.id}'",
                    ))

        return bindings

    def _suggest_layout(self, matches: MatchResult) -> str:
        """Suggest a SlotLayout arrangement based on component categories."""
        slots: dict[str, list[str]] = {"main": [], "sidebar": [], "toolbar": []}
        for comp in matches.ui_components:
            slot = _LAYOUT_HEURISTICS.get(comp.category, "main")
            slots[slot].append(comp.id)

        parts: list[str] = []
        for slot_name, ids in slots.items():
            if ids:
                parts.append(f"{slot_name}=[{', '.join(ids)}]")

        return " | ".join(parts) if parts else "main=[*]"

    def _compute_confidence(
        self,
        matches: MatchResult,
        bindings: list[Binding],
    ) -> float:
        """Confidence: weighted combination of coverage and binding quality."""
        coverage_weight = 0.6
        binding_weight = 0.4

        total_pieces = (
            len(matches.ui_components)
            + len(matches.calculations)
            + len(matches.workflows)
        )
        if total_pieces == 0:
            return 0.0

        # Binding ratio: how many pieces are connected
        bound_ids = set()
        for b in bindings:
            bound_ids.add(b.source_id)
            bound_ids.add(b.target_id)
        binding_ratio = min(len(bound_ids) / max(total_pieces, 1), 1.0)

        conf = (matches.coverage_score * coverage_weight) + (binding_ratio * binding_weight)
        return round(min(conf, 1.0), 4)
