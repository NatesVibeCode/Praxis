"""HeartbeatModule that discovers cross-entity relationships in the knowledge graph.

Runs AFTER MemorySync so fresh entities are available. Implements five mining
heuristics, each idempotent and fault-isolated.
"""
from __future__ import annotations

import re
import time
import traceback
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from memory.types import (
    EdgeProvenanceKind,
    Entity,
    EntityType,
    RelationType,
    enrichment_edge,
)
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _ok, _fail
from storage.postgres.vector_store import cosine_similarity

if TYPE_CHECKING:
    from memory.engine import MemoryEngine
    from storage.postgres import SyncPostgresConnection


def _slugify(text: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', text.lower()).strip('_')


class RelationshipMiner(HeartbeatModule):
    def __init__(self, conn: "SyncPostgresConnection", engine: "MemoryEngine"):
        self._conn = conn
        self._engine = engine

    @property
    def name(self) -> str:
        return 'relationship_miner'

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        errors: list[str] = []
        for label, fn in [
            ('agent_failure_patterns', self._mine_agent_failure_patterns),
            ('model_task_affinity', self._mine_model_task_affinity),
            ('constraint_bug_correlation', self._mine_constraint_bug_correlation),
            ('failure_cascades', self._mine_failure_cascades),
            ('friction_fix_loops', self._mine_friction_fix_loops),
            ('catalog_cross_references', self._mine_catalog_cross_references),
        ]:
            try:
                fn()
            except Exception:
                errors.append(f"{label}: {traceback.format_exc()}")
        if errors:
            return _fail(self.name, t0, "; ".join(errors))
        return _ok(self.name, t0)

    # ------------------------------------------------------------------
    # 1. Agent-failure pattern mining
    # ------------------------------------------------------------------

    def _mine_agent_failure_patterns(self) -> tuple[int, int]:
        rows = self._conn.execute(
            """
            SELECT
                metadata->>'agent' AS agent,
                COALESCE(NULLIF(metadata->>'failure_reason', ''), metadata->>'failure_code', 'unknown') AS failure_code,
                COUNT(*) AS cnt,
                array_agg(id ORDER BY created_at DESC) AS receipt_ids
            FROM memory_entities
            WHERE source = 'dispatch'
              AND archived = false
              AND metadata->>'status' = 'failed'
              AND metadata->>'agent' IS NOT NULL
            GROUP BY metadata->>'agent', COALESCE(NULLIF(metadata->>'failure_reason', ''), metadata->>'failure_code', 'unknown')
            HAVING COUNT(*) >= 3
            """
        )
        findings = 0
        actions = 0
        now = datetime.now(timezone.utc)

        for row in rows:
            agent = row['agent']
            failure_code = row['failure_code']
            count = row['cnt']
            sample_ids = list(row['receipt_ids'][:5])

            pattern_id = f"pattern:agent_fail:{_slugify(agent)}:{_slugify(failure_code)}"
            confidence = min(0.5 + count * 0.05, 0.95)

            entity = Entity(
                id=pattern_id,
                entity_type=EntityType.pattern,
                name=f"{agent} fails with {failure_code}",
                content=f"{count} failures observed",
                metadata={
                    'agent': agent,
                    'failure_code': failure_code,
                    'count': count,
                    'sample_receipt_ids': sample_ids,
                },
                created_at=now,
                updated_at=now,
                source='mining',
                confidence=confidence,
            )
            self._engine.insert(entity)
            actions += 1

            for receipt_id in sample_ids:
                edge = enrichment_edge(
                    source_id=pattern_id,
                    target_id=receipt_id,
                    relation_type=RelationType.correlates_with,
                    weight=0.7,
                    metadata={},
                    created_at=now,
                    provenance_kind=EdgeProvenanceKind.relationship_mining,
                    provenance_ref="agent_failure_patterns",
                )
                self._engine.add_edge(edge)
                actions += 1

            findings += 1

        return findings, actions

    # ------------------------------------------------------------------
    # 2. Model-task affinity scoring
    # ------------------------------------------------------------------

    def _mine_model_task_affinity(self) -> tuple[int, int]:
        rows = self._conn.execute(
            """
            SELECT
                metadata->>'model' AS model,
                COALESCE(metadata->>'phase', 'unknown') AS phase,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE metadata->>'status' = 'succeeded') AS passed,
                AVG((metadata->>'token_efficiency')::float)
                    FILTER (WHERE metadata->>'token_efficiency' IS NOT NULL) AS avg_eff,
                AVG((metadata->>'cache_hit_rate')::float)
                    FILTER (WHERE metadata->>'cache_hit_rate' IS NOT NULL) AS avg_cache,
                AVG((metadata->>'cost_usd')::float)
                    FILTER (WHERE metadata->>'cost_usd' IS NOT NULL) AS avg_cost,
                AVG((metadata->>'num_turns')::float)
                    FILTER (WHERE metadata->>'num_turns' IS NOT NULL) AS avg_turns,
                array_agg(id ORDER BY created_at DESC) AS receipt_ids
            FROM memory_entities
            WHERE source = 'dispatch'
              AND archived = false
              AND metadata->>'model' IS NOT NULL
              AND metadata->>'model' != ''
            GROUP BY metadata->>'model', COALESCE(metadata->>'phase', 'unknown')
            HAVING COUNT(*) >= 3
            """
        )
        findings = 0
        actions = 0
        now = datetime.now(timezone.utc)

        for row in rows:
            model = row['model']
            phase = row['phase']
            total = row['total']
            passed = row['passed'] or 0
            pass_rate = passed / total if total else 0.0
            avg_eff = row['avg_eff'] or 0.0
            avg_cache = row['avg_cache'] or 0.0
            avg_cost = row['avg_cost'] or 0.0
            avg_turns = row['avg_turns'] or 0.0

            # Aggregate tool_use profiles
            tool_rows = self._conn.execute(
                """
                SELECT metadata->'tool_use' AS tool_use
                FROM memory_entities
                WHERE source = 'dispatch'
                  AND archived = false
                  AND metadata->>'model' = $1
                  AND COALESCE(metadata->>'phase', 'unknown') = $2
                  AND metadata->'tool_use' IS NOT NULL
                """,
                model, phase,
            )
            tool_totals: dict[str, int] = {}
            tool_count = 0
            for tr in tool_rows:
                tu = tr['tool_use']
                if isinstance(tu, dict):
                    for tool_name, cnt in tu.items():
                        tool_totals[tool_name] = tool_totals.get(tool_name, 0) + (
                            cnt if isinstance(cnt, int) else 0
                        )
                    tool_count += 1

            tool_use_profile = {}
            if tool_count > 0:
                grand_total = sum(tool_totals.values()) or 1
                tool_use_profile = {
                    k: round(v / grand_total, 3) for k, v in tool_totals.items()
                }

            sample_ids = list(row['receipt_ids'][:10])
            pattern_id = f"pattern:model_affinity:{_slugify(model)}:{_slugify(phase)}"
            confidence = min(0.6 + total * 0.01, 0.95)

            entity = Entity(
                id=pattern_id,
                entity_type=EntityType.pattern,
                name=f"{model} on {phase}",
                content=f"pass_rate={pass_rate:.0%}, efficiency={avg_eff:.2f}, ${avg_cost:.4f}/job",
                metadata={
                    'model': model,
                    'phase': phase,
                    'sample_size': total,
                    'pass_rate': round(pass_rate, 4),
                    'avg_token_efficiency': round(avg_eff, 4),
                    'avg_cache_hit_rate': round(avg_cache, 4),
                    'avg_cost_usd': round(avg_cost, 6),
                    'avg_turns': round(avg_turns, 2),
                    'tool_use_profile': tool_use_profile,
                },
                created_at=now,
                updated_at=now,
                source='mining',
                confidence=confidence,
            )
            self._engine.insert(entity)
            actions += 1

            for receipt_id in sample_ids:
                edge = enrichment_edge(
                    source_id=pattern_id,
                    target_id=receipt_id,
                    relation_type=RelationType.covers,
                    weight=0.7,
                    metadata={},
                    created_at=now,
                    provenance_kind=EdgeProvenanceKind.relationship_mining,
                    provenance_ref="model_task_affinity",
                )
                self._engine.add_edge(edge)
                actions += 1

            findings += 1

        return findings, actions

    # ------------------------------------------------------------------
    # 3. Constraint-bug correlation
    # ------------------------------------------------------------------

    def _mine_constraint_bug_correlation(self) -> tuple[int, int]:
        constraints = self._conn.execute(
            """
            SELECT id, name FROM memory_entities
            WHERE source = 'constraints' AND archived = false
            """
        )
        findings = 0
        actions = 0
        now = datetime.now(timezone.utc)

        for c_row in constraints:
            c_id = c_row['id']
            pattern_text = c_row['name'] or ''
            if not pattern_text.strip():
                continue

            # Escape LIKE wildcards in pattern text
            escaped = pattern_text.replace('%', '\\%').replace('_', '\\_')
            bugs = self._conn.execute(
                """
                SELECT id FROM memory_entities
                WHERE source = 'bugs' AND archived = false
                  AND content ILIKE '%' || $1 || '%'
                """,
                escaped,
            )

            for b_row in bugs:
                b_id = b_row['id']
                # Check if edge already exists
                existing = self._engine.get_edges(c_id, direction='outgoing')
                already = any(
                    e.target_id == b_id and e.relation_type == RelationType.correlates_with
                    for e in existing
                )
                if already:
                    continue

                edge = enrichment_edge(
                    source_id=c_id,
                    target_id=b_id,
                    relation_type=RelationType.correlates_with,
                    weight=0.7,
                    metadata={},
                    created_at=now,
                    provenance_kind=EdgeProvenanceKind.relationship_mining,
                    provenance_ref="constraint_bug_correlation",
                )
                self._engine.add_edge(edge)
                actions += 1
                findings += 1

        return findings, actions

    # ------------------------------------------------------------------
    # 4. Failure cascade detection
    # ------------------------------------------------------------------

    def _mine_failure_cascades(self) -> tuple[int, int]:
        rows = self._conn.execute(
            """
            SELECT a.id AS a_id, b.id AS b_id
            FROM memory_entities a
            JOIN memory_entities b
              ON a.source = 'dispatch'
             AND b.source = 'dispatch'
             AND a.archived = false
             AND b.archived = false
             AND a.metadata->>'status' = 'failed'
             AND b.metadata->>'status' = 'failed'
             AND a.id < b.id
             AND b.created_at > a.created_at
             AND b.created_at <= a.created_at + interval '10 minutes'
            WHERE (
                split_part(a.name, '_', 1) = split_part(b.name, '_', 1)
                AND split_part(a.name, '_', 2) = split_part(b.name, '_', 2)
            )
            """
        )
        findings = 0
        actions = 0
        now = datetime.now(timezone.utc)

        for row in rows:
            earlier_id = row['a_id']
            later_id = row['b_id']

            existing = self._engine.get_edges(later_id, direction='outgoing')
            already = any(
                e.target_id == earlier_id and e.relation_type == RelationType.caused_by
                for e in existing
            )
            if already:
                continue

            edge = enrichment_edge(
                source_id=later_id,
                target_id=earlier_id,
                relation_type=RelationType.caused_by,
                weight=0.6,
                metadata={},
                created_at=now,
                provenance_kind=EdgeProvenanceKind.relationship_mining,
                provenance_ref="failure_cascades",
            )
            self._engine.add_edge(edge)
            actions += 1
            findings += 1

        return findings, actions

    # ------------------------------------------------------------------
    # 5. Friction-to-fix loop tracking
    # ------------------------------------------------------------------

    def _mine_friction_fix_loops(self) -> tuple[int, int]:
        # Get friction entities that have no outgoing 'triggered' edges
        friction_entities = self._conn.execute(
            """
            SELECT f.id, f.content
            FROM memory_entities f
            WHERE f.source = 'friction'
              AND f.archived = false
              AND NOT EXISTS (
                  SELECT 1 FROM memory_edges e
                  WHERE e.source_id = f.id
                    AND e.relation_type = 'triggered'
              )
            """
        )
        findings = 0
        actions = 0
        now = datetime.now(timezone.utc)

        for f_row in friction_entities:
            f_id = f_row['id']
            content = f_row['content'] or ''
            if not content.strip():
                continue

            bugs = self._conn.execute(
                """
                SELECT id, name FROM memory_entities
                WHERE source = 'bugs' AND archived = false
                  AND search_vector @@ plainto_tsquery('english', $1)
                LIMIT 3
                """,
                content,
            )

            for b_row in bugs:
                b_id = b_row['id']
                edge = enrichment_edge(
                    source_id=f_id,
                    target_id=b_id,
                    relation_type=RelationType.triggered,
                    weight=0.6,
                    metadata={},
                    created_at=now,
                    provenance_kind=EdgeProvenanceKind.relationship_mining,
                    provenance_ref="friction_fix_loops",
                )
                self._engine.add_edge(edge)
                actions += 1
                findings += 1

        return findings, actions

    # ------------------------------------------------------------------
    # 6. Catalog cross-reference mining
    # ------------------------------------------------------------------

    # Subsystem names recognized in catalog content and module_embeddings imports
    _SUBSYSTEMS = (
        'runtime', 'registry', 'adapters', 'memory', 'observability',
        'policy', 'storage', 'surfaces', 'contracts', 'scripts',
        'migrations', 'db_logic', 'core',
    )

    # Patterns that indicate a structural dependency in catalog text
    _DEP_PATTERNS = (
        re.compile(r'DEPENDS_ON:\s*(\S+)\s*(?:→|->|-->)\s*(\S+)', re.IGNORECASE),
        re.compile(r'Connects to:.*?(\w+/\w+)', re.IGNORECASE),
        re.compile(r'Imports?:\s*[`"]?(\w+(?:\.\w+)+)', re.IGNORECASE),
    )

    def _mine_catalog_cross_references(self) -> tuple[int, int]:
        """Mine depends_on edges between catalog document entities.

        Strategy:
        1. Load all catalog/* document entities.
        2. For each pair, check if doc A's content references subsystem B's
           file paths or module names.
        3. Create depends_on edges for cross-references found.
        4. Parse explicit DEPENDS_ON: lines for directed edges.
        """
        catalog_docs = self._conn.execute(
            """
            SELECT id, source, content FROM memory_entities
            WHERE entity_type = 'document'
              AND source LIKE 'catalog/%'
              AND archived = false
            """
        )
        if not catalog_docs:
            return 0, 0

        # Build source -> entity_id and entity_id -> subsystem_name maps
        source_to_id: dict[str, str] = {}
        id_to_subsystem: dict[str, str] = {}
        id_to_content: dict[str, str] = {}

        for row in catalog_docs:
            eid = row['id']
            source = row['source']  # e.g. "catalog/runtime"
            content = row['content'] or ''
            subsystem = source.replace('catalog/', '').replace('_and_', '_')

            source_to_id[source] = eid
            id_to_subsystem[eid] = subsystem
            id_to_content[eid] = content

        # Map subsystem keywords to entity IDs for matching
        subsystem_to_id: dict[str, str] = {}
        for eid, sub in id_to_subsystem.items():
            subsystem_to_id[sub] = eid
            # Also map partial names: "migrations_and_db_logic" -> matches "migrations"
            for part in sub.split('_'):
                if part not in subsystem_to_id and part in self._SUBSYSTEMS:
                    subsystem_to_id[part] = eid

        findings = 0
        actions = 0
        now = datetime.now(timezone.utc)

        # Get existing catalog edges to avoid duplicates
        existing_edges: set[tuple[str, str, str]] = set()
        for eid in id_to_subsystem:
            for edge in self._engine.get_edges(eid, direction='both'):
                existing_edges.add((edge.source_id, edge.target_id, edge.relation_type.value))

        # Build a lookup of distinctive names -> owning subsystem entity.
        # Each catalog doc defines classes/functions; when another doc
        # mentions those names, that's a cross-reference.
        _NAME_RE = re.compile(r'`(\w{4,}(?:Adapter|Registry|Router|Engine|Store|Runner|Repository|Resolver|Catalog|Runtime|Authority|Bundle|Worker|Orchestrator|Assembler|Compiler|Executor|Tracker|Hub|Classifier|Profile|Record|Decision|Guard|Scanner|Packer|Retriever|Linker|Extractor|Generator|Scorer|Miner|Hygienist))`')
        name_to_owner: dict[str, str] = {}  # class_name -> entity_id
        for eid, content in id_to_content.items():
            for m in _NAME_RE.finditer(content):
                cname = m.group(1)
                if cname not in name_to_owner:
                    name_to_owner[cname] = eid

        for src_id, content in id_to_content.items():
            src_sub = id_to_subsystem[src_id]

            # Strategy A: content-based cross-reference detection.
            # Scan content for file paths like "registry/model_routing.py",
            # module refs like "storage.postgres", or subsystem names in context.
            for target_sub, target_id in subsystem_to_id.items():
                if target_id == src_id:
                    continue

                # Check for file path references: "subsystem/" or "subsystem."
                path_pattern = re.compile(
                    rf'\b{re.escape(target_sub)}[/.][\w.]+',
                    re.IGNORECASE,
                )
                matches = path_pattern.findall(content)
                if not matches:
                    continue

                key = (src_id, target_id, 'depends_on')
                if key in existing_edges:
                    continue

                edge = enrichment_edge(
                    source_id=src_id,
                    target_id=target_id,
                    relation_type=RelationType.depends_on,
                    weight=min(0.5 + len(matches) * 0.05, 0.9),
                    metadata={
                        'match_count': len(matches),
                        'sample_matches': matches[:5],
                        'source_subsystem': src_sub,
                        'target_subsystem': target_sub,
                    },
                    created_at=now,
                    provenance_kind=EdgeProvenanceKind.relationship_mining,
                    provenance_ref="catalog_cross_references",
                )
                self._engine.add_edge(edge)
                existing_edges.add(key)
                actions += 1
                findings += 1

            # Strategy B: parse explicit DEPENDS_ON: lines.
            for dep_re in self._DEP_PATTERNS:
                for m in dep_re.finditer(content):
                    groups = m.groups()
                    if len(groups) >= 2:
                        src_path, tgt_path = groups[0], groups[1]
                    else:
                        continue

                    # Resolve target path to a subsystem
                    tgt_id = None
                    for sub_name, sub_id in subsystem_to_id.items():
                        if sub_id != src_id and sub_name in tgt_path.lower():
                            tgt_id = sub_id
                            break

                    if not tgt_id:
                        continue

                    key = (src_id, tgt_id, 'depends_on')
                    if key in existing_edges:
                        continue

                    edge = enrichment_edge(
                        source_id=src_id,
                        target_id=tgt_id,
                        relation_type=RelationType.depends_on,
                        weight=0.85,
                        metadata={
                            'source_path': src_path,
                            'target_path': tgt_path,
                            'extraction': 'explicit_depends_on',
                        },
                        created_at=now,
                        provenance_kind=EdgeProvenanceKind.relationship_mining,
                        provenance_ref="catalog_cross_references",
                    )
                    self._engine.add_edge(edge)
                    existing_edges.add(key)
                    actions += 1
                    findings += 1

            # Strategy C: class/function name cross-references.
            # If this doc mentions a class name that was defined in a
            # different catalog doc, create a depends_on edge.
            refs_by_target: dict[str, list[str]] = {}
            for cname, owner_id in name_to_owner.items():
                if owner_id == src_id:
                    continue
                if cname in content:
                    refs_by_target.setdefault(owner_id, []).append(cname)

            for target_id, names in refs_by_target.items():
                key = (src_id, target_id, 'depends_on')
                if key in existing_edges:
                    continue

                edge = enrichment_edge(
                    source_id=src_id,
                    target_id=target_id,
                    relation_type=RelationType.depends_on,
                    weight=min(0.5 + len(names) * 0.1, 0.9),
                    metadata={
                        'match_count': len(names),
                        'matched_names': names[:10],
                        'source_subsystem': src_sub,
                        'target_subsystem': id_to_subsystem.get(target_id, '?'),
                        'extraction': 'class_name_cross_ref',
                    },
                    created_at=now,
                    provenance_kind=EdgeProvenanceKind.relationship_mining,
                    provenance_ref="catalog_cross_references",
                )
                self._engine.add_edge(edge)
                existing_edges.add(key)
                actions += 1
                findings += 1

        # Strategy D: functional-fingerprint cross-references via module_embeddings.
        # The discover index has AST-extracted imports and db_tables for every
        # module/class/function. Use these to project real dependency edges
        # between catalog docs, plus behavioral-vector similarity for related_to.
        #
        # Step 1: Build subsystem → catalog_entity_id map from module_path prefixes.
        # Maps both full subsystem names and individual parts to catalog entity IDs.
        # e.g. "runtime" -> runtime catalog, "contracts" -> contracts_core_scripts catalog
        path_prefix_to_id: dict[str, str] = {}
        for eid, sub in id_to_subsystem.items():
            # Map the full subsystem name
            path_prefix_to_id[sub] = eid
            # Map individual parts for compound names like "contracts_core_scripts"
            for part in sub.split('_'):
                if len(part) >= 3 and part not in path_prefix_to_id:
                    path_prefix_to_id[part] = eid

        # Step 2: Query import-graph edges from module_embeddings.
        # Each row's behavior->'imports' lists package-level imports.
        # A module in subsystem A importing subsystem B = depends_on edge.
        import_rows = self._conn.execute(
            """
            SELECT module_path, behavior->>'imports' AS imports
            FROM module_embeddings
            WHERE behavior->'imports' IS NOT NULL
              AND jsonb_array_length(behavior->'imports') > 0
            """
        )
        # Count cross-subsystem import references: (src_sub_id, tgt_sub_id) -> count
        import_counts: dict[tuple[str, str], int] = {}
        for row in import_rows:
            mod_path = row['module_path'] or ''
            raw_imports = row['imports']
            # asyncpg returns JSONB sub-expressions as strings; parse them
            if isinstance(raw_imports, str):
                try:
                    imports = __import__('json').loads(raw_imports)
                except (ValueError, TypeError):
                    continue
            elif isinstance(raw_imports, list):
                imports = raw_imports
            else:
                continue

            # Determine which subsystem this module belongs to
            src_catalog_id = None
            for prefix, cid in path_prefix_to_id.items():
                if f'/{prefix}/' in mod_path or mod_path.endswith(f'/{prefix}'):
                    src_catalog_id = cid
                    break
            if not src_catalog_id:
                continue

            # Check each import against known subsystem prefixes
            for imp in imports:
                if not isinstance(imp, str):
                    continue
                for prefix, cid in path_prefix_to_id.items():
                    if cid != src_catalog_id and imp == prefix:
                        pair = (src_catalog_id, cid)
                        import_counts[pair] = import_counts.get(pair, 0) + 1

        # Create depends_on edges from import counts
        for (src_cid, tgt_cid), count in import_counts.items():
            key = (src_cid, tgt_cid, 'depends_on')
            if key in existing_edges:
                continue
            edge = enrichment_edge(
                source_id=src_cid,
                target_id=tgt_cid,
                relation_type=RelationType.depends_on,
                weight=min(0.5 + count * 0.03, 0.95),
                metadata={
                    'import_count': count,
                    'source_subsystem': id_to_subsystem.get(src_cid, '?'),
                    'target_subsystem': id_to_subsystem.get(tgt_cid, '?'),
                    'extraction': 'ast_import_graph',
                },
                created_at=now,
                provenance_kind=EdgeProvenanceKind.relationship_mining,
                provenance_ref="catalog_cross_references",
            )
            self._engine.add_edge(edge)
            existing_edges.add(key)
            actions += 1
            findings += 1

        # Step 3: Behavioral-vector similarity between subsystems.
        # Average the module_embeddings vectors per subsystem directory,
        # then compute pairwise cosine similarity in Python.
        subsystem_rows = self._conn.execute(
            """
            SELECT
                SPLIT_PART(REPLACE(module_path, 'Code&DBs/Workflow/', ''), '/', 1) AS subsystem,
                AVG(embedding) AS centroid
            FROM module_embeddings
            GROUP BY SPLIT_PART(REPLACE(module_path, 'Code&DBs/Workflow/', ''), '/', 1)
            HAVING COUNT(*) >= 3
            """
        )
        centroids: dict[str, object] = {}
        for row in subsystem_rows:
            centroid = row["centroid"]
            if centroid is not None:
                centroids[row["subsystem"]] = centroid

        subsystem_pairs: list[tuple[str, str, float]] = []
        subsystems = sorted(centroids)
        for i, sub_a in enumerate(subsystems):
            for sub_b in subsystems[i + 1:]:
                cosine_sim = float(cosine_similarity(centroids[sub_a], centroids[sub_b]))
                if cosine_sim >= 0.65:
                    subsystem_pairs.append((sub_a, sub_b, cosine_sim))

        subsystem_pairs.sort(key=lambda item: item[2], reverse=True)
        for sub_a, sub_b, cosine_sim in subsystem_pairs:
            cid_a = path_prefix_to_id.get(sub_a)
            cid_b = path_prefix_to_id.get(sub_b)
            if not cid_a or not cid_b:
                continue

            # Create bidirectional related_to (only one direction to avoid duplication)
            key = (cid_a, cid_b, 'related_to')
            if key in existing_edges:
                continue
            edge = enrichment_edge(
                source_id=cid_a,
                target_id=cid_b,
                relation_type=RelationType.related_to,
                weight=round(cosine_sim, 3),
                metadata={
                    'cosine_similarity': round(cosine_sim, 4),
                    'source_subsystem': sub_a,
                    'target_subsystem': sub_b,
                    'extraction': 'behavioral_vector_centroid',
                },
                created_at=now,
                provenance_kind=EdgeProvenanceKind.relationship_mining,
                provenance_ref="catalog_cross_references",
            )
            self._engine.add_edge(edge)
            existing_edges.add(key)
            actions += 1
            findings += 1

        return findings, actions
