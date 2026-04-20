"""Project authoritative FK relationships from authority tables into memory_edges.

Today ``memory_edges`` is ``authority_class='enrichment'`` by default, so the
knowledge graph is shallow even though the authority tables hold real
structure (roadmap parent/child, roadmap->source_bug, operator_object_relations,
workflow_build_intents, etc). This projection mirrors those FK columns into
``memory_edges`` under ``authority_class='canonical'``, so discover, recall,
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

Projections wired here
---------------------
- ``roadmap_items.parent_roadmap_item_id``              -> ``parent_of``
- ``roadmap_items.source_bug_id``                       -> ``resolves_bug``
- ``roadmap_item_dependencies``                         -> ``depends_on``
- ``operator_object_relations`` (active)                -> mirror of source/target kinds + relation metadata
- ``bugs.discovered_in_run_id``                         -> ``recorded_in``
- ``bugs.discovered_in_receipt_id``                     -> ``recorded_in``
- ``bugs.source_issue_id``                              -> ``derived_from``
- ``workflow_build_intents.workflow_id``                -> ``implements_build``
- ``bug_evidence_links``                               -> ``related_to`` by evidence kind
- ``workflow_job_submissions.run_id``                   -> ``parent_of`` to submission IDs
- ``workflow_job_submissions.workflow_id``               -> ``related_to`` to workflow IDs
- ``workflow_chains``                                   -> ``parent_of`` to chain waves
- ``workflow_chain_waves``                              -> ``parent_of`` to wave runs
- ``workflow_chain_wave_runs.run_id``                   -> ``parent_of`` to workflow runs
- ``issues.discovered_in_run_id``                       -> ``recorded_in``
- ``issues.discovered_in_receipt_id``                   -> ``recorded_in``
- ``operator_decisions`` with typed scope                -> ``related_to``
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
#   relation_type:   depends_on, parent_of, resolves_bug, implements_build,
#                    belongs_to_area, related_to, recorded_in, derived_from
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
        name="bug_discovered_in_run",
        select_sql=(
            """
            SELECT
              'bug::' || bug_id                      AS source_id,
              'bug'                                  AS source_kind,
              bug_id                                 AS source_name,
              'workflow_run::' || discovered_in_run_id AS target_id,
              'workflow_run'                         AS target_kind,
              discovered_in_run_id                    AS target_name,
              TRUE                                   AS active
            FROM bugs
            WHERE discovered_in_run_id IS NOT NULL
            """
        ),
        relation_type="recorded_in",
        semantic_kind="bug_discovered_in_run",
        provenance_ref="bugs.discovered_in_run_id",
    ),
    FkProjection(
        name="bug_discovered_in_receipt",
        select_sql=(
            """
            SELECT
              'bug::' || bug_id                             AS source_id,
              'bug'                                        AS source_kind,
              bug_id                                       AS source_name,
              'receipt::' || discovered_in_receipt_id        AS target_id,
              'receipt'                                    AS target_kind,
              discovered_in_receipt_id                      AS target_name,
              TRUE                                         AS active
            FROM bugs
            WHERE discovered_in_receipt_id IS NOT NULL
            """
        ),
        relation_type="recorded_in",
        semantic_kind="bug_discovered_in_receipt",
        provenance_ref="bugs.discovered_in_receipt_id",
    ),
    FkProjection(
        name="bug_source_issue",
        select_sql=(
            """
            SELECT
              'issue::' || source_issue_id                AS source_id,
              'issue'                                     AS source_kind,
              source_issue_id                              AS source_name,
              'bug::' || bug_id                           AS target_id,
              'bug'                                       AS target_kind,
              bug_id                                      AS target_name,
              TRUE                                        AS active
            FROM bugs
            WHERE source_issue_id IS NOT NULL
            """
        ),
        relation_type="derived_from",
        semantic_kind="bug_source_issue",
        provenance_ref="bugs.source_issue_id",
    ),
    FkProjection(
        name="roadmap_item_dependencies",
        select_sql=(
            """
            SELECT
              'roadmap_item::' || roadmap_item_id          AS source_id,
              'roadmap_item'                                AS source_kind,
              roadmap_item_id                                AS source_name,
              'roadmap_item::' || depends_on_roadmap_item_id AS target_id,
              'roadmap_item'                                AS target_kind,
              depends_on_roadmap_item_id                     AS target_name,
              TRUE                                          AS active
            FROM roadmap_item_dependencies
            """
        ),
        relation_type="depends_on",
        semantic_kind="roadmap_dependency",
        provenance_ref="roadmap_item_dependencies",
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
                                  'operator_decision', 'functional_area', 'issue',
                                  'workflow_class', 'schedule_definition',
                                  'workflow_run', 'document', 'cutover_gate')
              AND target_kind IN ('bug', 'roadmap_item', 'repo_path',
                                  'operator_decision', 'functional_area', 'issue',
                                  'workflow_class', 'schedule_definition',
                                  'workflow_run', 'document', 'cutover_gate')
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
    FkProjection(
        name="workflow_job_submission_workflow",
        select_sql=(
            """
            SELECT
              'workflow_job_submission::' || submission_id   AS source_id,
              'workflow_job_submission'                     AS source_kind,
              submission_id                                 AS source_name,
              'workflow::' || workflow_id                    AS target_id,
              'workflow'                                    AS target_kind,
              workflow_id                                   AS target_name,
              TRUE                                          AS active
            FROM workflow_job_submissions
            WHERE workflow_id IS NOT NULL
            """
        ),
        relation_type="related_to",
        semantic_kind="submission_workflow",
        provenance_ref="workflow_job_submissions.workflow_id",
    ),
    FkProjection(
        name="bug_evidence_links_receipt",
        select_sql=(
            """
            SELECT
              'bug::' || bug_id                           AS source_id,
              'bug'                                       AS source_kind,
              bug_id                                      AS source_name,
              'receipt::' || evidence_ref                  AS target_id,
              'receipt'                                   AS target_kind,
              evidence_ref                                 AS target_name,
              TRUE                                         AS active,
              jsonb_build_object(
                'evidence_kind', evidence_kind,
                'evidence_role', evidence_role
              )                                           AS metadata_override
            FROM bug_evidence_links
            WHERE evidence_kind = 'receipt'
            """
        ),
        relation_type="related_to",
        semantic_kind="bug_evidence",
        provenance_ref="bug_evidence_links.receipt",
    ),
    FkProjection(
        name="bug_evidence_links_run",
        select_sql=(
            """
            SELECT
              'bug::' || bug_id                           AS source_id,
              'bug'                                       AS source_kind,
              bug_id                                      AS source_name,
              'workflow_run::' || evidence_ref             AS target_id,
              'workflow_run'                              AS target_kind,
              evidence_ref                                 AS target_name,
              TRUE                                         AS active,
              jsonb_build_object(
                'evidence_kind', evidence_kind,
                'evidence_role', evidence_role
              )                                           AS metadata_override
            FROM bug_evidence_links
            WHERE evidence_kind = 'run'
            """
        ),
        relation_type="related_to",
        semantic_kind="bug_evidence",
        provenance_ref="bug_evidence_links.run",
    ),
    FkProjection(
        name="bug_evidence_links_verification_run",
        select_sql=(
            """
            SELECT
              'bug::' || bug_id                           AS source_id,
              'bug'                                       AS source_kind,
              bug_id                                      AS source_name,
              'verification_run::' || evidence_ref         AS target_id,
              'verification_run'                          AS target_kind,
              evidence_ref                                 AS target_name,
              TRUE                                         AS active,
              jsonb_build_object(
                'evidence_kind', evidence_kind,
                'evidence_role', evidence_role
              )                                           AS metadata_override
            FROM bug_evidence_links
            WHERE evidence_kind = 'verification_run'
            """
        ),
        relation_type="related_to",
        semantic_kind="bug_evidence",
        provenance_ref="bug_evidence_links.verification_run",
    ),
    FkProjection(
        name="bug_evidence_links_healing_run",
        select_sql=(
            """
            SELECT
              'bug::' || bug_id                           AS source_id,
              'bug'                                       AS source_kind,
              bug_id                                      AS source_name,
              'healing_run::' || evidence_ref              AS target_id,
              'healing_run'                               AS target_kind,
              evidence_ref                                 AS target_name,
              TRUE                                         AS active,
              jsonb_build_object(
                'evidence_kind', evidence_kind,
                'evidence_role', evidence_role
              )                                           AS metadata_override
            FROM bug_evidence_links
            WHERE evidence_kind = 'healing_run'
            """
        ),
        relation_type="related_to",
        semantic_kind="bug_evidence",
        provenance_ref="bug_evidence_links.healing_run",
    ),
    FkProjection(
        name="workflow_job_submissions_to_workflow_runs",
        select_sql=(
            """
            SELECT
              'workflow_run::' || run_id               AS source_id,
              'workflow_run'                           AS source_kind,
              run_id                                   AS source_name,
              'workflow_job_submission::' || submission_id AS target_id,
              'workflow_job_submission'                AS target_kind,
              submission_id                            AS target_name,
              TRUE                                     AS active
            FROM workflow_job_submissions
            WHERE run_id IS NOT NULL
            """
        ),
        relation_type="parent_of",
        semantic_kind="run_submissions",
        provenance_ref="workflow_job_submissions.run_id",
    ),
    FkProjection(
        name="workflow_chains_to_waves",
        select_sql=(
            """
            SELECT
              'workflow_chain::' || chain_id                     AS source_id,
              'workflow_chain'                                   AS source_kind,
              chain_id                                           AS source_name,
              'workflow_chain_wave::' || chain_id || '::' || wave_id AS target_id,
              'workflow_chain_wave'                              AS target_kind,
              wave_id                                            AS target_name,
              TRUE                                               AS active
            FROM workflow_chain_waves
            WHERE chain_id IS NOT NULL
              AND wave_id IS NOT NULL
            """
        ),
        relation_type="parent_of",
        semantic_kind="chain_contains_wave",
        provenance_ref="workflow_chain_waves",
    ),
    FkProjection(
        name="workflow_chain_waves_to_wave_runs",
        select_sql=(
            """
            SELECT
              'workflow_chain_wave::' || chain_id || '::' || wave_id  AS source_id,
              'workflow_chain_wave'                                   AS source_kind,
              chain_id || '|' || wave_id                               AS source_name,
              'workflow_chain_wave_run::' || chain_id || '::' || wave_id || '::' || spec_path
                                                                      AS target_id,
              'workflow_chain_wave_run'                                AS target_kind,
              spec_path                                               AS target_name,
              TRUE                                                    AS active
            FROM workflow_chain_wave_runs
            WHERE chain_id IS NOT NULL
              AND wave_id IS NOT NULL
              AND spec_path IS NOT NULL
            """
        ),
        relation_type="parent_of",
        semantic_kind="wave_contains_run_spec",
        provenance_ref="workflow_chain_wave_runs",
    ),
    FkProjection(
        name="workflow_chain_wave_runs_to_workflow_runs",
        select_sql=(
            """
            SELECT
              'workflow_chain_wave_run::' || chain_id || '::' || wave_id || '::' || spec_path
                                                          AS source_id,
              'workflow_chain_wave_run'                          AS source_kind,
              chain_id || '|' || wave_id || '|' || spec_path        AS source_name,
              'workflow_run::' || run_id                           AS target_id,
              'workflow_run'                                      AS target_kind,
              run_id                                              AS target_name,
              TRUE                                                AS active
            FROM workflow_chain_wave_runs
            WHERE run_id IS NOT NULL
            """
        ),
        relation_type="parent_of",
        semantic_kind="wave_run_executes_run",
        provenance_ref="workflow_chain_wave_runs.run_id",
    ),
    FkProjection(
        name="operator_decisions_to_scope",
        select_sql=(
            """
            SELECT
              'operator_decision::' || operator_decision_id AS source_id,
              'operator_decision'                          AS source_kind,
              operator_decision_id                         AS source_name,
              CASE lower(decision_scope_kind)
                WHEN 'bug' THEN 'bug::' || decision_scope_ref
                WHEN 'issue' THEN 'issue::' || decision_scope_ref
                WHEN 'roadmap_item' THEN 'roadmap_item::' || decision_scope_ref
                WHEN 'workflow' THEN 'workflow::' || decision_scope_ref
                WHEN 'workflow_run' THEN 'workflow_run::' || decision_scope_ref
                WHEN 'workflow_chain' THEN 'workflow_chain::' || decision_scope_ref
                WHEN 'workflow_build_intent' THEN 'workflow_build_intent::' || decision_scope_ref
                WHEN 'functional_area' THEN 'functional_area::' || decision_scope_ref
                WHEN 'repo_path' THEN 'repo_path::' || decision_scope_ref
                WHEN 'operator_decision' THEN 'operator_decision::' || decision_scope_ref
                WHEN 'workflow_class' THEN 'workflow_class::' || decision_scope_ref
                WHEN 'schedule_definition' THEN 'schedule_definition::' || decision_scope_ref
                WHEN 'document' THEN 'document::' || decision_scope_ref
                WHEN 'cutover_gate' THEN 'cutover_gate::' || decision_scope_ref
                WHEN 'provider' THEN 'provider::' || decision_scope_ref
                WHEN 'authority_domain' THEN 'authority_domain::' || decision_scope_ref
                ELSE NULL
              END                                          AS target_id,
              CASE lower(decision_scope_kind)
                WHEN 'bug' THEN 'bug'
                WHEN 'issue' THEN 'issue'
                WHEN 'roadmap_item' THEN 'roadmap_item'
                WHEN 'workflow' THEN 'workflow'
                WHEN 'workflow_run' THEN 'workflow_run'
                WHEN 'workflow_chain' THEN 'workflow_chain'
                WHEN 'workflow_build_intent' THEN 'workflow_build_intent'
                WHEN 'functional_area' THEN 'functional_area'
                WHEN 'repo_path' THEN 'repo_path'
                WHEN 'operator_decision' THEN 'operator_decision'
                WHEN 'workflow_class' THEN 'workflow_class'
                WHEN 'schedule_definition' THEN 'schedule_definition'
                WHEN 'document' THEN 'document'
                WHEN 'cutover_gate' THEN 'cutover_gate'
                WHEN 'provider' THEN 'provider'
                WHEN 'authority_domain' THEN 'authority_domain'
                ELSE NULL
              END                                          AS target_kind,
              decision_scope_ref                            AS target_name,
              TRUE                                         AS active,
              jsonb_build_object(
                'decision_scope_kind', decision_scope_kind,
                'decision_scope_ref', decision_scope_ref
              )                                           AS metadata_override
            FROM operator_decisions
            WHERE decision_scope_kind IS NOT NULL
              AND decision_scope_ref IS NOT NULL
              AND lower(decision_scope_kind) IN (
                'bug', 'issue', 'roadmap_item', 'workflow', 'workflow_run',
                'workflow_chain', 'workflow_build_intent', 'functional_area',
                'repo_path', 'operator_decision', 'workflow_class',
                'schedule_definition', 'document', 'cutover_gate',
                'provider', 'authority_domain'
              )
            """
        ),
        relation_type="related_to",
        semantic_kind="decision_scope",
        provenance_ref="operator_decisions.decision_scope",
    ),
    FkProjection(
        name="issue_discovered_in_run",
        select_sql=(
            """
            SELECT
              'issue::' || issue_id                      AS source_id,
              'issue'                                    AS source_kind,
              issue_id                                   AS source_name,
              'workflow_run::' || discovered_in_run_id    AS target_id,
              'workflow_run'                             AS target_kind,
              discovered_in_run_id                        AS target_name,
              TRUE                                       AS active
            FROM issues
            WHERE discovered_in_run_id IS NOT NULL
            """
        ),
        relation_type="recorded_in",
        semantic_kind="issue_discovered_in_run",
        provenance_ref="issues.discovered_in_run_id",
    ),
    FkProjection(
        name="issue_discovered_in_receipt",
        select_sql=(
            """
            SELECT
              'issue::' || issue_id                      AS source_id,
              'issue'                                    AS source_kind,
              issue_id                                   AS source_name,
              'receipt::' || discovered_in_receipt_id      AS target_id,
              'receipt'                                  AS target_kind,
              discovered_in_receipt_id                    AS target_name,
              TRUE                                       AS active
            FROM issues
            WHERE discovered_in_receipt_id IS NOT NULL
            """
        ),
        relation_type="recorded_in",
        semantic_kind="issue_discovered_in_receipt",
        provenance_ref="issues.discovered_in_receipt_id",
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


# --- Authoritative coverage now implemented in code ---
# This projection remains scan-based until outbox events are available for
# incremental refreshes of every upstream authority table.
