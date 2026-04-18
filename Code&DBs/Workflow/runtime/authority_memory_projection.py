"""Project authoritative FK relationships from authority tables into memory_edges.

Today ``memory_edges`` is ``authority_class='enrichment'`` by default, so the
knowledge graph is shallow even though the authority tables hold real
structure (roadmap parent/child, roadmap->source_bug, operator_object_relations,
workflow_build_intents, etc). This projection mirrors those FK columns into
``memory_edges`` under ``authority_class='authoritative'``, so discover, recall,
and the atlas see the full structure.

Design notes
------------
The authority write paths do not emit outbox events for roadmap_items or
operator_object_relations today, so a pure subscriber would have nothing to
consume. Instead this module is a **refresher**: one idempotent pass that
re-derives the authoritative memory_edges from the authority tables. Run it on
a schedule (praxis_heartbeat-like) or manually via ``praxis workflow ...``
until write paths emit events and we upgrade to a true subscriber.

The refresher is upsert-only; it never retracts enrichment edges. When an FK is
cleared on the authority side, the corresponding authoritative edge is set to
``active=false`` but the row is preserved for audit.

Projections wired here (initial set)
------------------------------------
- ``roadmap_items.parent_roadmap_item_id``  -> ``parent_of`` (104 rows today)
- ``roadmap_items.source_bug_id``           -> ``resolves_bug``
  (0 rows today; populating this closes the 7 P2 "closeout blocked:
  missing source_bug_id" bugs the moment projection runs)
- ``operator_object_relations`` (active)    -> mirror of source_kind/target_kind
- ``workflow_build_intents.workflow_id``    -> ``implements_build``

Extend ``_PROJECTIONS`` with additional FK mappings as new authorities land.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Protocol

from storage.postgres import connect_workflow_database

DEFAULT_PROJECTION_ID = "authority_memory_projection"
# memory_edges check constraints (ck_memory_edges_*) limit these to specific enums:
#   authority_class: canonical | enrichment
#   provenance_kind: schema_projection is the closest fit for FK-derived edges
#   relation_type:   depends_on, constrains, implements, supersedes, related_to,
#                    derived_from, caused_by, correlates_with, produced, triggered,
#                    covers, verified_by, recorded_in, regressed_from,
#                    semantic_neighbor
AUTHORITY_CLASS = "canonical"
PROVENANCE_KIND = "schema_projection"


@dataclass(frozen=True, slots=True)
class FkProjection:
    """One FK column or junction table -> memory_edge projection.

    ``relation_type`` must be one of the memory_edges check-constraint values.
    ``semantic_kind`` carries the finer-grained meaning (e.g. ``parent_of``,
    ``resolves_bug``) and is stored in the ``metadata`` jsonb column so we
    don't lose fidelity to the bounded vocabulary.
    """

    name: str
    # SELECT returning (source_id, target_id, active) tuples.
    # May additionally return ``metadata_override`` jsonb for per-row detail.
    select_sql: str
    relation_type: str
    semantic_kind: str
    provenance_ref: str  # e.g. "roadmap_items.parent_roadmap_item_id"


_PROJECTIONS: tuple[FkProjection, ...] = (
    FkProjection(
        name="roadmap_parent_of",
        select_sql=(
            """
            SELECT
              'roadmap_item::' || roadmap_item_id          AS source_id,
              'roadmap_item'                                AS source_kind,
              COALESCE(NULLIF(title, ''), roadmap_item_id)  AS source_name,
              'roadmap_item::' || parent_roadmap_item_id   AS target_id,
              'roadmap_item'                                AS target_kind,
              parent_roadmap_item_id                        AS target_name,
              TRUE                                          AS active
            FROM roadmap_items
            WHERE parent_roadmap_item_id IS NOT NULL
            """
        ),
        relation_type="parent_of",
        semantic_kind="parent_of",
        provenance_ref="roadmap_items.parent_roadmap_item_id",
    ),
    FkProjection(
        name="roadmap_resolves_bug",
        select_sql=(
            """
            SELECT
              'roadmap_item::' || roadmap_item_id          AS source_id,
              'roadmap_item'                                AS source_kind,
              COALESCE(NULLIF(title, ''), roadmap_item_id)  AS source_name,
              'bug::' || source_bug_id                      AS target_id,
              'bug'                                          AS target_kind,
              source_bug_id                                  AS target_name,
              TRUE                                           AS active
            FROM roadmap_items
            WHERE source_bug_id IS NOT NULL
            """
        ),
        relation_type="resolves_bug",
        semantic_kind="resolves_bug",
        provenance_ref="roadmap_items.source_bug_id",
    ),
    FkProjection(
        name="operator_object_relations_mirror",
        select_sql=(
            """
            SELECT
              source_kind || '::' || source_ref                           AS source_id,
              source_kind                                                  AS source_kind,
              source_ref                                                   AS source_name,
              target_kind || '::' || target_ref                           AS target_id,
              target_kind                                                  AS target_kind,
              target_ref                                                   AS target_name,
              (relation_status = 'active')                                 AS active,
              jsonb_build_object('operator_relation_kind', relation_kind)  AS metadata_override
            FROM operator_object_relations
            WHERE source_kind IN ('bug', 'roadmap_item', 'repo_path',
                                  'operator_decision', 'functional_area')
              AND target_kind IN ('bug', 'roadmap_item', 'repo_path',
                                  'operator_decision', 'functional_area')
            """
        ),
        relation_type="belongs_to_area",
        semantic_kind="operator_relation",
        provenance_ref="operator_object_relations",
    ),
    FkProjection(
        name="workflow_build_intent_implements_build",
        select_sql=(
            """
            SELECT
              'workflow_build_intent::' || intent_ref   AS source_id,
              'workflow_build_intent'                    AS source_kind,
              intent_ref                                 AS source_name,
              'workflow::' || workflow_id                AS target_id,
              'workflow'                                  AS target_kind,
              workflow_id                                 AS target_name,
              TRUE                                        AS active
            FROM workflow_build_intents
            WHERE workflow_id IS NOT NULL
            """
        ),
        relation_type="implements_build",
        semantic_kind="implements_build",
        provenance_ref="workflow_build_intents.workflow_id",
    ),
)


class _Connection(Protocol):
    async def execute(self, query: str, *args: object) -> str: ...

    async def fetch(self, query: str, *args: object) -> list[Any]: ...

    async def fetchrow(self, query: str, *args: object) -> Any: ...

    def transaction(self) -> object: ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ProjectionResult:
    projection_id: str
    total_upserted: int
    total_deactivated: int
    by_projection: dict[str, dict[str, int]]
    as_of: datetime

    def to_json(self) -> dict[str, Any]:
        return {
            "projection_id": self.projection_id,
            "total_upserted": self.total_upserted,
            "total_deactivated": self.total_deactivated,
            "by_projection": dict(self.by_projection),
            "as_of": self.as_of.isoformat(),
        }


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def _ensure_entity(
    conn: _Connection, entity_id: str, entity_type: str, name: str | None
) -> None:
    """Idempotent upsert of a memory_entities stub so edge FKs resolve."""
    await conn.execute(
        """
        INSERT INTO memory_entities (
          id, entity_type, name, confidence, source,
          created_at, updated_at, last_maintained_at
        ) VALUES ($1, $2, $3, 1.0, 'authority_projection', now(), now(), now())
        ON CONFLICT (id) DO NOTHING
        """,
        entity_id, entity_type, name or entity_id,
    )


async def _apply_projection(conn: _Connection, proj: FkProjection) -> dict[str, int]:
    """Upsert rows for one projection. Returns {upserted, deactivated, entities}.

    Ensures memory_entities stubs exist for both endpoints before writing the
    edge (FK constraint). Deactivation is scoped by ``provenance_ref`` so
    sibling projections sharing the same ``relation_type`` don't collide.
    """
    import json as _json

    rows = await conn.fetch(proj.select_sql)
    expected: set[tuple[str, str]] = set()
    upserted = 0
    entities = 0
    seen_entities: set[str] = set()
    for row in rows:
        src = row["source_id"]
        tgt = row["target_id"]
        if src == tgt:
            continue
        active = bool(row["active"])
        # Ensure both endpoints exist as memory_entities.
        for nid, kind, name in (
            (src, row.get("source_kind"), row.get("source_name")),
            (tgt, row.get("target_kind"), row.get("target_name")),
        ):
            if nid in seen_entities or not kind:
                continue
            await _ensure_entity(conn, nid, str(kind), name)
            seen_entities.add(nid)
            entities += 1

        extra_meta: dict[str, Any] = {}
        if "metadata_override" in row and row["metadata_override"] is not None:
            raw = row["metadata_override"]
            extra_meta = raw if isinstance(raw, dict) else _json.loads(raw)
        metadata = {"semantic_kind": proj.semantic_kind, **extra_meta}
        expected.add((src, tgt))
        await conn.execute(
            """
            INSERT INTO memory_edges (
              source_id, target_id, relation_type, weight, active,
              authority_class, provenance_kind, provenance_ref,
              metadata, last_validated_at
            ) VALUES ($1, $2, $3, 1.0, $4, $5, $6, $7, $8::jsonb, now())
            ON CONFLICT (source_id, target_id, relation_type)
            DO UPDATE SET
              active = EXCLUDED.active,
              authority_class = EXCLUDED.authority_class,
              provenance_kind = EXCLUDED.provenance_kind,
              provenance_ref = EXCLUDED.provenance_ref,
              metadata = EXCLUDED.metadata,
              last_validated_at = now()
            """,
            src, tgt, proj.relation_type, active,
            AUTHORITY_CLASS, PROVENANCE_KIND, proj.provenance_ref,
            _json.dumps(metadata),
        )
        upserted += 1

    # Scope deactivation to rows previously written by this projection,
    # identified by provenance_ref. Prevents cross-projection collisions.
    deactivated_rows = await conn.fetch(
        """
        SELECT source_id, target_id
          FROM memory_edges
         WHERE relation_type = $1
           AND authority_class = $2
           AND provenance_ref = $3
           AND active = true
        """,
        proj.relation_type, AUTHORITY_CLASS, proj.provenance_ref,
    )
    deactivated = 0
    for row in deactivated_rows:
        if (row["source_id"], row["target_id"]) not in expected:
            await conn.execute(
                """
                UPDATE memory_edges
                   SET active = false, last_validated_at = now()
                 WHERE source_id = $1
                   AND target_id = $2
                   AND relation_type = $3
                   AND authority_class = $4
                   AND provenance_ref = $5
                """,
                row["source_id"], row["target_id"],
                proj.relation_type, AUTHORITY_CLASS, proj.provenance_ref,
            )
            deactivated += 1

    return {"upserted": upserted, "deactivated": deactivated, "entities": entities}


@dataclass(slots=True)
class AuthorityMemoryProjection:
    """Refreshable projection of authority FKs into memory_edges."""

    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    )
    projections: tuple[FkProjection, ...] = _PROJECTIONS

    async def refresh_async(
        self,
        *,
        as_of: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> ProjectionResult:
        ts = _now() if as_of is None else as_of
        conn = await self.connect_database(env)
        by: dict[str, dict[str, int]] = {}
        total_up = 0
        total_de = 0
        try:
            async with conn.transaction():
                for proj in self.projections:
                    result = await _apply_projection(conn, proj)
                    by[proj.name] = result
                    total_up += result["upserted"]
                    total_de += result["deactivated"]
        finally:
            await conn.close()

        return ProjectionResult(
            projection_id=DEFAULT_PROJECTION_ID,
            total_upserted=total_up,
            total_deactivated=total_de,
            by_projection=by,
            as_of=ts,
        )


async def refresh_authority_memory_projection(
    *,
    env: Mapping[str, str] | None = None,
    as_of: datetime | None = None,
) -> ProjectionResult:
    """Top-level entry point. Safe to call on a schedule or via CLI."""
    return await AuthorityMemoryProjection().refresh_async(as_of=as_of, env=env)


# --- Follow-ups for a full implementation (intentional TODOs) -----------
# 1. Wire into the CLI: `praxis workflow tools call praxis_authority_memory_refresh`.
#    Register a handler in surfaces/mcp/tools/ that calls refresh_authority_memory_projection.
# 2. Schedule via recurring_run_windows or heartbeat — run every N minutes.
# 3. When authority write paths emit outbox events (roadmap_items,
#    operator_object_relations, workflow_build_intents), swap the refresher
#    for a true cursor-based subscriber. The FkProjection rows already carry
#    enough metadata to drive per-event projection.
# 4. Add memory_entity rows for any source_id/target_id that doesn't yet exist
#    in memory_entities so discover/recall can resolve the edge endpoints.
# 5. Extend _PROJECTIONS with: bug evidence refs, workflow_chains,
#    workflow_job_submissions -> workflow_runs, operator_decisions -> scope.
