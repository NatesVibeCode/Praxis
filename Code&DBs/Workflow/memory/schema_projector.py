"""HeartbeatModule that projects live Postgres schema into knowledge graph entities.

Creates one entity per public table with columns, indexes, triggers, FK
relationships, and which Python subsystems read/write it (from module_embeddings
AST extraction). Edges connect tables to each other (FKs, triggers) and to
catalog documents (subsystem ownership).

Designed for zero-context LLM observability: an agent can ask "what is the
workflow_runs table" and get back everything it needs.
"""
from __future__ import annotations

import json
import time
import traceback
from datetime import datetime
from typing import TYPE_CHECKING

from memory.types import Edge, Entity, EntityType, RelationType
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _ok, _fail

if TYPE_CHECKING:
    from memory.engine import MemoryEngine
    from storage.postgres import SyncPostgresConnection


def _table_entity_id(table_name: str) -> str:
    return f"table:{table_name}"


class SchemaProjector(HeartbeatModule):
    """Projects live Postgres schema into dedicated table entities."""

    def __init__(self, conn: "SyncPostgresConnection", engine: "MemoryEngine"):
        self._conn = conn
        self._engine = engine

    @property
    def name(self) -> str:
        return "schema_projector"

    def _ensure_table_entity_type(self, entity_id: str) -> EntityType:
        rows = self._conn.execute(
            "SELECT entity_type FROM memory_entities WHERE id = $1 LIMIT 1",
            entity_id,
        )
        existing_type = str(rows[0].get("entity_type") or "").strip() if rows else ""
        if existing_type and existing_type != EntityType.table.value:
            self._conn.execute(
                """
                UPDATE memory_entities
                   SET entity_type = $2,
                       metadata = COALESCE(metadata, '{}'::jsonb) || $3::jsonb,
                       updated_at = NOW()
                 WHERE id = $1
                """,
                entity_id,
                EntityType.table.value,
                json.dumps({"kind": "table", "entity_subtype": "schema_table"}),
            )
        return EntityType.table

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        errors: list[str] = []
        for label, fn in [
            ("migrate_legacy_table_entities", self._migrate_legacy_table_entities),
            ("project_tables", self._project_tables),
            ("project_fk_edges", self._project_fk_edges),
            ("project_trigger_edges", self._project_trigger_edges),
            ("project_subsystem_edges", self._project_subsystem_edges),
        ]:
            try:
                fn()
            except Exception:
                errors.append(f"{label}: {traceback.format_exc()}")
        if errors:
            return _fail(self.name, t0, "; ".join(errors))
        return _ok(self.name, t0)

    def _migrate_legacy_table_entities(self) -> tuple[int, list[str]]:
        rows = self._conn.execute(
            """
            UPDATE memory_entities
               SET entity_type = $1,
                   metadata = COALESCE(metadata, '{}'::jsonb) || $2::jsonb,
                   updated_at = NOW()
             WHERE entity_type IN ($3, $4)
               AND COALESCE(metadata->>'kind', '') = 'table'
               AND NOT archived
            RETURNING id
            """,
            EntityType.table.value,
            json.dumps({"kind": "table", "entity_subtype": "schema_table"}),
            EntityType.module.value,
            EntityType.document.value,
        )
        count = len(rows or [])
        if count:
            return count, [f"migrated {count} legacy table entities to document"]
        return 0, []

    # ------------------------------------------------------------------
    # CHECK constraint introspection
    # ------------------------------------------------------------------

    def _collect_check_constraints(self) -> dict[str, dict[str, list[str]]]:
        """Extract valid-value sets from CHECK constraints.

        Returns ``{table_name: {column_name: [allowed_values]}}``.
        """
        import re

        rows = self._conn.execute("""
            SELECT conrelid::regclass::text AS table_name,
                   pg_get_constraintdef(oid)  AS check_def
              FROM pg_constraint
             WHERE contype = 'c'
               AND connamespace = 'public'::regnamespace
        """)
        result: dict[str, dict[str, list[str]]] = {}
        for r in rows or []:
            defn = r["check_def"] or ""
            array_match = re.search(r"ARRAY\[(.+?)\]", defn)
            if not array_match:
                continue
            col_match = re.search(r"\(+\s*\(?(\w+)\)?", defn)
            if not col_match:
                continue
            values = re.findall(r"'([^']+)'", array_match.group(1))
            if values:
                result.setdefault(r["table_name"], {})[col_match.group(1)] = values
        return result

    # ------------------------------------------------------------------
    # Step 1: Create/update one entity per table
    # ------------------------------------------------------------------

    def _project_tables(self) -> tuple[int, list[str]]:
        """Create an entity per public table with columns, indexes, triggers."""
        now = datetime.utcnow()
        actions = 0
        findings: list[str] = []

        # Columns per table
        col_rows = self._conn.execute("""
            SELECT table_name, column_name, data_type, is_nullable,
                   column_default, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
        """)
        table_columns: dict[str, list[dict]] = {}
        for r in col_rows:
            col: dict = {
                'name': r['column_name'],
                'type': r['data_type'],
            }
            # Only include non-default attributes to reduce noise
            if r['is_nullable'] == 'YES':
                col['nullable'] = True
            if r['column_default'] and r['column_default'] != 'now()':
                col['default'] = r['column_default']
            table_columns.setdefault(r['table_name'], []).append(col)

        # Indexes per table
        idx_rows = self._conn.execute("""
            SELECT tablename, indexname, indexdef
            FROM pg_indexes
            WHERE schemaname = 'public'
            ORDER BY tablename, indexname
        """)
        table_indexes: dict[str, list[str]] = {}
        for r in idx_rows:
            table_indexes.setdefault(r['tablename'], []).append(r['indexname'])

        # Triggers per table
        trg_rows = self._conn.execute("""
            SELECT c.relname AS table_name, t.tgname AS trigger_name,
                   p.proname AS function_name
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_proc p ON t.tgfoid = p.oid
            WHERE NOT t.tgisinternal
            ORDER BY c.relname, t.tgname
        """)
        table_triggers: dict[str, list[dict]] = {}
        for r in trg_rows:
            table_triggers.setdefault(r['table_name'], []).append({
                'trigger': r['trigger_name'],
                'function': r['function_name'],
            })

        # Row counts (approximate, from pg_stat)
        count_rows = self._conn.execute("""
            SELECT relname, n_live_tup
            FROM pg_stat_user_tables
            WHERE schemaname = 'public'
        """)
        table_row_counts: dict[str, int] = {}
        for r in count_rows:
            table_row_counts[r['relname']] = r['n_live_tup']

        # Python consumers (from module_embeddings AST)
        consumer_rows = self._conn.execute("""
            WITH table_refs AS (
                SELECT
                    SPLIT_PART(REPLACE(module_path, 'Code&DBs/Workflow/', ''), '/', 1) AS subsystem,
                    name AS module_name,
                    module_path,
                    jsonb_array_elements_text(behavior->'db_tables') AS table_ref
                FROM module_embeddings
                WHERE behavior->'db_tables' IS NOT NULL
                  AND jsonb_array_length(behavior->'db_tables') > 0
            )
            SELECT table_ref, subsystem, module_name, module_path
            FROM table_refs
            WHERE table_ref IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public')
            ORDER BY table_ref, subsystem, module_name
        """)
        table_consumers: dict[str, list[dict]] = {}
        for r in consumer_rows:
            table_consumers.setdefault(r['table_ref'], []).append({
                'subsystem': r['subsystem'],
                'module': r['module_name'],
                'path': r['module_path'],
            })

        # Valid values extracted from CHECK constraints
        check_values = self._collect_check_constraints()

        # Create/update entity per table
        for table_name, columns in table_columns.items():
            entity_id = _table_entity_id(table_name)
            col_names = [c['name'] for c in columns]
            indexes = table_indexes.get(table_name, [])
            triggers = table_triggers.get(table_name, [])
            consumers = table_consumers.get(table_name, [])
            row_count = table_row_counts.get(table_name, 0)
            table_checks = check_values.get(table_name, {})

            # Annotate column dicts with CHECK-constraint valid values
            if table_checks:
                for col in columns:
                    if col['name'] in table_checks:
                        col['valid_values'] = table_checks[col['name']]

            # Build human-readable summary for text search
            consumer_summary = ""
            if consumers:
                subsystems = sorted(set(c['subsystem'] for c in consumers))
                modules = sorted(set(c['module'] for c in consumers))
                consumer_summary = (
                    f" Used by {', '.join(subsystems)} subsystems"
                    f" ({', '.join(modules[:8])}{'...' if len(modules) > 8 else ''})."
                )

            trigger_summary = ""
            if triggers:
                trigger_summary = (
                    f" Triggers: {', '.join(t['function'] for t in triggers)}."
                )

            vv_summary = ""
            if table_checks:
                parts = [f"{c}=[{', '.join(v)}]" for c, v in table_checks.items()]
                vv_summary = f" Valid values: {'; '.join(parts)}."

            summary = (
                f"Postgres table '{table_name}' with {len(columns)} columns "
                f"({', '.join(col_names[:6])}{'...' if len(col_names) > 6 else ''}). "
                f"{len(indexes)} indexes. ~{row_count} rows."
                f"{trigger_summary}{consumer_summary}{vv_summary}"
            )

            # Build clean metadata — omit empty fields
            meta: dict = {
                'kind': 'table',
                'entity_subtype': 'schema_table',
                'columns': columns,
            }
            if indexes:
                meta['indexes'] = indexes
            if triggers:
                meta['triggers'] = [
                    f"{t['trigger']} -> {t['function']}" for t in triggers
                ]
            if consumers:
                # Flatten to subsystem -> [module_names]
                by_sub: dict[str, list[str]] = {}
                for c in consumers:
                    by_sub.setdefault(c['subsystem'], []).append(c['module'])
                meta['used_by'] = {
                    sub: sorted(set(mods)) for sub, mods in sorted(by_sub.items())
                }
            if row_count:
                meta['approx_rows'] = row_count
            if table_checks:
                meta['valid_values'] = table_checks

            entity = Entity(
                id=entity_id,
                entity_type=self._ensure_table_entity_type(entity_id),
                name=table_name,
                content=summary,
                metadata=meta,
                created_at=now,
                updated_at=now,
                source='schema',
                confidence=1.0,
            )
            self._engine.insert(entity)
            actions += 1

        findings.append(f"projected {len(table_columns)} tables")
        return actions, findings

    # ------------------------------------------------------------------
    # Step 2: FK edges between tables
    # ------------------------------------------------------------------

    def _project_fk_edges(self) -> tuple[int, list[str]]:
        """Create depends_on edges between tables connected by foreign keys."""
        now = datetime.utcnow()
        actions = 0
        findings: list[str] = []

        fk_rows = self._conn.execute("""
            SELECT DISTINCT
                tc.table_name AS src_table,
                ccu.table_name AS tgt_table,
                kcu.column_name AS fk_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
                ON tc.constraint_name = ccu.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public'
              AND tc.table_name != ccu.table_name
        """)

        # Group FK columns per (src, tgt) pair
        fk_pairs: dict[tuple[str, str], list[str]] = {}
        for r in fk_rows:
            pair = (r['src_table'], r['tgt_table'])
            fk_pairs.setdefault(pair, []).append(r['fk_column'])

        for (src, tgt), fk_columns in fk_pairs.items():
            edge = Edge(
                source_id=_table_entity_id(src),
                target_id=_table_entity_id(tgt),
                relation_type=RelationType.depends_on,
                weight=0.9,
                metadata={
                    'edge_kind': 'foreign_key',
                    'fk_columns': fk_columns,
                },
                created_at=now,
            )
            self._engine.add_edge(edge)
            actions += 1

        findings.append(f"projected {len(fk_pairs)} FK edges")
        return actions, findings

    # ------------------------------------------------------------------
    # Step 3: Trigger edges between tables
    # ------------------------------------------------------------------

    def _project_trigger_edges(self) -> tuple[int, list[str]]:
        """Create edges for triggers that connect tables.

        Reads trigger function source to find which other tables they
        UPDATE/INSERT into, creating caused_by edges.
        """
        now = datetime.utcnow()
        actions = 0
        findings: list[str] = []

        # Get trigger functions and their source code
        trg_rows = self._conn.execute("""
            SELECT c.relname AS table_name,
                   t.tgname AS trigger_name,
                   p.proname AS function_name,
                   p.prosrc AS function_source
            FROM pg_trigger t
            JOIN pg_class c ON t.tgrelid = c.oid
            JOIN pg_proc p ON t.tgfoid = p.oid
            WHERE NOT t.tgisinternal
        """)

        # Get all public table names for matching
        all_tables = set()
        for r in self._conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
        ):
            all_tables.add(r['tablename'])

        for r in trg_rows:
            src_table = r['table_name']
            source = r['function_source'] or ''

            # Find other tables referenced in UPDATE/INSERT statements
            for other_table in all_tables:
                if other_table == src_table:
                    continue
                # Look for UPDATE <table> or INSERT INTO <table>
                if (f'UPDATE {other_table}' in source
                        or f'INSERT INTO {other_table}' in source
                        or f'insert into {other_table}' in source.lower()):
                    edge = Edge(
                        source_id=_table_entity_id(other_table),
                        target_id=_table_entity_id(src_table),
                        relation_type=RelationType.derived_from,
                        weight=0.85,
                        metadata={
                            'edge_kind': 'trigger',
                            'trigger_name': r['trigger_name'],
                            'function_name': r['function_name'],
                        },
                        created_at=now,
                    )
                    self._engine.add_edge(edge)
                    actions += 1

            # pg_notify channels → store in the table entity metadata
            import re
            notify_matches = re.findall(
                r"pg_notify\('(\w+)'", source
            )
            if notify_matches:
                entity_id = _table_entity_id(src_table)
                try:
                    table_entity_type = self._ensure_table_entity_type(entity_id)
                    existing = self._engine.get(entity_id, table_entity_type)
                    if existing and existing.metadata:
                        meta = dict(existing.metadata)
                        meta['pg_notify_channels'] = list(set(notify_matches))
                        self._engine.update(entity_id, table_entity_type, metadata=meta)
                        actions += 1
                except Exception:
                    pass  # table entity may not exist yet on first run

        findings.append(f"projected trigger edges from {len(trg_rows)} triggers")
        return actions, findings

    # ------------------------------------------------------------------
    # Step 4: Edges from tables to catalog documents
    # ------------------------------------------------------------------

    def _project_subsystem_edges(self) -> tuple[int, list[str]]:
        """Connect table entities to their owning catalog documents.

        Uses module_embeddings behavior->db_tables to determine which
        subsystem(s) use each table, then creates edges to the matching
        catalog document entity.
        """
        now = datetime.utcnow()
        actions = 0
        findings: list[str] = []

        # Load catalog doc entities
        catalog_docs = self._conn.execute("""
            SELECT id, source FROM memory_entities
            WHERE entity_type = 'document'
              AND source LIKE 'catalog/%'
              AND archived = false
        """)
        # Map subsystem directory names to catalog entity IDs
        subsystem_to_catalog: dict[str, str] = {}
        for r in catalog_docs:
            sub = r['source'].replace('catalog/', '').replace('_and_', '_')
            subsystem_to_catalog[sub] = r['id']
            for part in sub.split('_'):
                if len(part) >= 3 and part not in subsystem_to_catalog:
                    subsystem_to_catalog[part] = r['id']

        # Get table → subsystem mapping from AST
        consumer_rows = self._conn.execute("""
            WITH table_refs AS (
                SELECT
                    SPLIT_PART(
                        REPLACE(module_path, 'Code&DBs/Workflow/', ''), '/', 1
                    ) AS subsystem,
                    jsonb_array_elements_text(behavior->'db_tables') AS table_ref
                FROM module_embeddings
                WHERE behavior->'db_tables' IS NOT NULL
                  AND jsonb_array_length(behavior->'db_tables') > 0
            )
            SELECT table_ref, subsystem, COUNT(*) as module_count
            FROM table_refs
            WHERE table_ref IN (
                SELECT tablename FROM pg_tables WHERE schemaname = 'public'
            )
            GROUP BY table_ref, subsystem
            ORDER BY table_ref, module_count DESC
        """)

        # Create edges: table → catalog doc
        for r in consumer_rows:
            table_name = r['table_ref']
            subsystem = r['subsystem']
            catalog_id = subsystem_to_catalog.get(subsystem)
            if not catalog_id:
                continue

            edge = Edge(
                source_id=_table_entity_id(table_name),
                target_id=catalog_id,
                relation_type=RelationType.implements,
                weight=min(0.5 + r['module_count'] * 0.1, 0.95),
                metadata={
                    'edge_kind': 'table_to_catalog',
                    'subsystem': subsystem,
                    'module_count': r['module_count'],
                },
                created_at=now,
            )
            self._engine.add_edge(edge)
            actions += 1

        findings.append(
            f"projected {actions} table-to-catalog edges "
            f"across {len(subsystem_to_catalog)} subsystems"
        )
        return actions, findings
