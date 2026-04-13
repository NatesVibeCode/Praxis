"""Tools: praxis_recall, praxis_ingest, praxis_graph."""
from __future__ import annotations

from typing import Any

from ..subsystems import _subs
from ..helpers import _serialize


def _readable_name(entity) -> str:
    """Derive a human-readable name from entity data."""
    name = entity.name or ""
    # doc:hexhash names are useless — derive from source or content
    if name.startswith("doc:"):
        # Use source as title: "catalog/runtime" -> "Runtime Catalog"
        if entity.source and entity.source.startswith("catalog/"):
            parts = entity.source.replace("catalog/", "").replace("_", " ").title()
            return f"{parts} Catalog"
        # Fall back to first line of content
        if entity.content:
            first_line = entity.content.split("\n")[0].strip().lstrip("# ")
            if first_line:
                return first_line[:80]
    return name


def _readable_type(entity) -> str:
    """Return a clear type label, using metadata.kind when available."""
    meta = entity.metadata or {}
    kind = meta.get("kind") or meta.get("object_kind")
    if kind:
        return kind  # "table", "postgres_table", etc.
    return entity.entity_type.value


def tool_praxis_recall(params: dict) -> dict:
    """Search the knowledge graph."""
    query = params.get("query", "")
    if not query:
        return {"error": "query is required"}
    entity_type = params.get("entity_type", "") or None
    limit = max(1, int(params.get("limit", 20) or 20))

    try:
        kg = _subs.get_knowledge_graph()
        results = kg.search(query, entity_type=entity_type, limit=limit)

        # Build clean results — no internal scoring, no empty fields, readable names
        clean = []
        for r in results:
            name = _readable_name(r.entity)
            rtype = _readable_type(r.entity)
            content = (r.entity.content or "").strip()

            entry: dict = {"name": name}

            # Only include type if it's informative (not the generic "document")
            if rtype and rtype not in ("document",):
                entry["type"] = rtype

            entry["score"] = round(r.score, 2)

            # Content preview — skip if empty or same as name
            if content and content != name:
                entry["content"] = content[:300]

            # Source — only if it adds context
            source = r.entity.source or ""
            if source and source not in ("mining", "audit"):
                entry["source"] = source

            # found_via — simplified
            if r.found_via and r.found_via != "text":
                entry["found_via"] = r.found_via

            # entity_id — only needed for praxis_graph follow-up
            entry["id"] = r.entity.id

            # Include structured metadata for tables
            meta = r.entity.metadata or {}
            if meta.get("kind") == "table":
                details: dict = {}
                if meta.get("triggers"):
                    details["triggers"] = meta["triggers"]
                if meta.get("pg_notify_channels"):
                    details["pg_notify_channels"] = meta["pg_notify_channels"]
                if meta.get("used_by"):
                    details["used_by"] = meta["used_by"]
                if meta.get("approx_rows"):
                    details["approx_rows"] = meta["approx_rows"]
                if details:
                    entry["details"] = details

            clean.append(entry)

        return {"results": clean, "count": len(clean)}
    except Exception as e:
        return {"results": [], "count": 0, "note": f"Knowledge graph error: {e}"}


def tool_praxis_ingest(params: dict) -> dict:
    """Ingest content into the knowledge graph."""
    kind = params.get("kind", "")
    content = params.get("content", "")
    source = params.get("source", "")
    if not kind or not content or not source:
        return {"error": "kind, content, and source are all required"}

    try:
        kg = _subs.get_knowledge_graph()
        result = kg.ingest(kind=kind, content=content, source=source)
        return {
            "accepted": result.accepted,
            "entities_created": result.entities_created,
            "edges_created": result.edges_created,
            "duplicates_skipped": result.duplicates_skipped,
            "errors": list(result.errors),
        }
    except Exception as e:
        return {"accepted": False, "error": str(e)}


def tool_praxis_graph(params: dict) -> dict:
    """Get entity neighbors and blast radius."""
    entity_id = str(params.get("entity_id", "")).strip()
    depth = params.get("depth", 1)

    try:
        kg = _subs.get_knowledge_graph()
        resolution_note = None
        if not entity_id:
            entity_id = _resolve_default_entity_id(kg)
            resolution_note = "entity_id omitted; showing the latest available entity instead"
        elif entity_id == "entity_abc123":
            entity_id = _resolve_default_entity_id(kg)
            resolution_note = "entity_abc123 is a placeholder; showing the latest available entity instead"
        elif _resolve_entity(kg, entity_id) is None:
            return {"entity_id": entity_id, "error": "entity_id was not found"}

        blast = kg.blast_radius(entity_id)
        raw = _serialize(blast)

        # Resolve hex entity IDs to readable names
        all_ids = set()
        for section in ("direct", "indirect"):
            if isinstance(raw.get(section), dict):
                all_ids.update(raw[section].keys())

        id_to_name: dict[str, str] = {}
        if all_ids:
            engine = kg._engine
            for eid in all_ids:
                try:
                    for etype in ("document", "module", "task", "pattern", "decision", "constraint"):
                        from memory.types import EntityType
                        ent = engine.get(eid, EntityType(etype))
                        if ent:
                            id_to_name[eid] = _readable_name(ent)
                            break
                except Exception:
                    pass

        def _resolve(d: dict) -> list:
            return [
                {"entity_id": eid, "name": id_to_name.get(eid, eid), "impact": round(score, 3)}
                for eid, score in sorted(d.items(), key=lambda x: -x[1])
            ]

        result: dict = {"entity_id": entity_id, "depth": depth}
        if resolution_note:
            result["note"] = resolution_note
        if isinstance(raw.get("direct"), dict) and raw["direct"]:
            result["direct_dependencies"] = _resolve(raw["direct"])
        if isinstance(raw.get("indirect"), dict) and raw["indirect"]:
            result["indirect_dependencies"] = _resolve(raw["indirect"])
        result["total_affected"] = raw.get("total_affected", 0)

        return result
    except Exception as e:
        return {"entity_id": entity_id, "error": str(e)}


def _resolve_entity(kg, entity_id: str):
    for etype in ("document", "module", "task", "pattern", "decision", "constraint", "fact", "topic", "person"):
        from memory.types import EntityType

        try:
            entity = kg._engine.get(entity_id, EntityType(etype))
        except ValueError:
            continue
        if entity is not None:
            return entity
    return None


def _resolve_default_entity_id(kg) -> str:
    rows = _subs.get_pg_conn().execute(
        """
        SELECT id
          FROM memory_entities
         WHERE archived = false
         ORDER BY updated_at DESC, created_at DESC
         LIMIT 1
        """
    )
    if not rows:
        raise RuntimeError("entity_id is required and no knowledge-graph entities were found")
    entity_id = str(rows[0].get("id") or "").strip()
    if not entity_id:
        raise RuntimeError("failed to resolve a default knowledge-graph entity id")
    return entity_id


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_recall": (
        tool_praxis_recall,
        {
            "description": (
                "Search the platform's knowledge graph for information about modules, functions, "
                "decisions, patterns, bugs, constraints, people, or any previously ingested content. "
                "Returns ranked results with confidence scores and how each result was found "
                "(text match, graph traversal, or vector similarity).\n\n"
                "USE WHEN: you need to look up what a module does, find related decisions, understand "
                "connections between components, or recall previously stored knowledge.\n\n"
                "EXAMPLES:\n"
                "  praxis_recall(query='how does job dependency resolution work')\n"
                "  praxis_recall(query='provider routing', entity_type='decision')\n"
                "  praxis_recall(query='dispatch run completion trigger retirement')\n\n"
                "DO NOT USE: for searching code by similarity (use praxis_discover), for searching "
                "workflow receipts (use praxis_receipts), or for general questions (use praxis_query)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Search query."},
                    "entity_type": {"type": "string", "description": "Optional entity type filter (person, topic, decision, etc.)."},
                    "limit": {"type": "integer", "description": "Maximum results to return.", "default": 20},
                },
                "required": ["query"],
            },
        },
    ),
    "praxis_ingest": (
        tool_praxis_ingest,
        {
            "description": (
                "Store new information in the knowledge graph so it can be recalled later via "
                "praxis_recall. Content is automatically entity-extracted, deduplicated, and embedded "
                "for vector search.\n\n"
                "USE WHEN: you have analysis results, documentation, conversation summaries, or "
                "structured knowledge to persist for future sessions.\n\n"
                "EXAMPLES:\n"
                "  praxis_ingest(kind='document', content='# API Catalog\\n...', source='catalog/api')\n"
                "  praxis_ingest(kind='build_event', content='Build failed: missing import in X', source='ci/build_42')\n"
                "  praxis_ingest(kind='conversation', content='User decided to use Postgres for all state', source='session/2026-04-07')\n\n"
                "KINDS: 'document' (reference docs, catalogs), 'build_event' (CI/build results), "
                "'extraction' (structured data from code analysis), 'conversation' (session decisions), "
                "'import' (bulk data import)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "description": "Content kind.",
                        "enum": ["document", "build_event", "extraction", "conversation", "import"],
                    },
                    "content": {"type": "string", "description": "Content to ingest."},
                    "source": {"type": "string", "description": "Source identifier."},
                },
                "required": ["kind", "content", "source"],
            },
        },
    ),
    "praxis_graph": (
        tool_praxis_graph,
        {
            "description": (
                "Explore connections from one knowledge-graph entity. Shows what an entity depends on, "
                "what depends on it, and the blast radius of changes.\n\n"
                "USE WHEN: you already know the target entity_id from praxis_recall, or you want a quick "
                "look at the latest available entity without hunting for an id first.\n\n"
                "EXAMPLES:\n"
                "  praxis_graph(depth=1)\n"
                "  praxis_graph(entity_id='module:task_assembler', depth=2)\n\n"
                "DO NOT USE: for broad knowledge search or discovery; use praxis_recall first when you "
                "need ranked candidates."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "entity_id": {"type": "string", "description": "Entity ID to explore."},
                    "depth": {"type": "integer", "description": "Traversal depth.", "default": 1},
                },
            },
        },
    ),
}
