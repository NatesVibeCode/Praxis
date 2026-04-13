"""Populate the knowledge graph from the Dag Project codebase."""

from __future__ import annotations

import ast
import json
import os
import re
from pathlib import Path

from memory.knowledge_graph import KnowledgeGraph


def populate_from_codebase(
    kg: KnowledgeGraph,
    repo_root: str,
) -> dict:
    """Scan the Dag Project repo and ingest modules, documents, and edges.

    Returns stats: {entities_created, edges_created, errors}.
    """
    root = Path(repo_root)
    entities_created = 0
    edges_created = 0
    errors: list[str] = []

    # Track module name -> entity id for edge wiring
    module_ids: dict[str, str] = {}

    # -----------------------------------------------------------
    # (a) Scan runtime/*.py  -> 'module' entities
    # -----------------------------------------------------------
    runtime_dir = root / "Code&DBs" / "Workflow" / "runtime"
    entities_created, edges_created = _scan_python_dir(
        kg, runtime_dir, module_ids, entities_created, edges_created, errors,
    )

    # -----------------------------------------------------------
    # (b) Scan memory/*.py  -> 'module' entities
    # -----------------------------------------------------------
    memory_dir = root / "Code&DBs" / "Workflow" / "memory"
    entities_created, edges_created = _scan_python_dir(
        kg, memory_dir, module_ids, entities_created, edges_created, errors,
    )

    # -----------------------------------------------------------
    # (c) Scan Build Plan/*.md  -> 'document' entities
    # -----------------------------------------------------------
    build_plan_dir = root / "Build Plan"
    if build_plan_dir.is_dir():
        for md_file in sorted(build_plan_dir.glob("*.md")):
            try:
                text = md_file.read_text(errors="replace")[:500]
                result = kg.ingest(
                    kind="document",
                    content=text,
                    source=str(md_file),
                    metadata={"title": md_file.name},
                )
                entities_created += result.entities_created
                edges_created += result.edges_created
                if result.errors:
                    errors.extend(result.errors)
            except Exception as exc:
                errors.append(f"md ingest failed {md_file.name}: {exc}")

    # -----------------------------------------------------------
    # (d) Scan config/*.json  -> 'document' entities
    # -----------------------------------------------------------
    config_dir = root / "config"
    if config_dir.is_dir():
        for json_file in sorted(config_dir.glob("*.json")):
            try:
                text = json_file.read_text(errors="replace")[:500]
                result = kg.ingest(
                    kind="document",
                    content=text,
                    source=str(json_file),
                    metadata={"title": json_file.name},
                )
                entities_created += result.entities_created
                edges_created += result.edges_created
                if result.errors:
                    errors.extend(result.errors)
            except Exception as exc:
                errors.append(f"json ingest failed {json_file.name}: {exc}")

    # -----------------------------------------------------------
    # (e) Parse Python imports -> 'depends_on' edges between modules
    # -----------------------------------------------------------
    for py_dir in [runtime_dir, memory_dir]:
        if not py_dir.is_dir():
            continue
        for py_file in sorted(py_dir.glob("*.py")):
            src_name = py_file.stem
            if src_name not in module_ids:
                continue
            try:
                source_text = py_file.read_text(errors="replace")
                imports = _extract_imports(source_text)
                for imp_name in imports:
                    # Normalize: 'memory.engine' -> 'engine', 'runtime.workflow' -> 'dispatch'
                    base = imp_name.split(".")[-1]
                    if base in module_ids and base != src_name:
                        result = kg.ingest(
                            kind="extraction",
                            content=json.dumps({
                                "entities": [],
                                "edges": [{
                                    "source_id": module_ids[src_name],
                                    "target_id": module_ids[base],
                                    "relation_type": "depends_on",
                                    "weight": 0.8,
                                }],
                            }),
                            source=str(py_file),
                        )
                        edges_created += result.edges_created
                        if result.errors:
                            errors.extend(result.errors)
            except Exception as exc:
                errors.append(f"import parse failed {py_file.name}: {exc}")

    return {
        "entities_created": entities_created,
        "edges_created": edges_created,
        "errors": errors,
    }


def _scan_python_dir(
    kg: KnowledgeGraph,
    directory: Path,
    module_ids: dict[str, str],
    entities_created: int,
    edges_created: int,
    errors: list[str],
) -> tuple[int, int]:
    """Scan a directory of Python files and ingest each as a module entity."""
    if not directory.is_dir():
        return entities_created, edges_created

    for py_file in sorted(directory.glob("*.py")):
        if py_file.name == "__init__.py":
            continue
        try:
            source_text = py_file.read_text(errors="replace")
            content = _extract_module_summary(source_text)
            name = py_file.stem

            # Use extraction kind so we can set entity_type=module
            payload = json.dumps({
                "entities": [{
                    "id": f"mod:{name}",
                    "entity_type": "module",
                    "name": name,
                    "content": content,
                    "confidence": 0.9,
                    "metadata": {"file": str(py_file)},
                }],
                "edges": [],
            })
            result = kg.ingest(
                kind="extraction",
                content=payload,
                source=str(py_file),
            )
            if result.accepted:
                module_ids[name] = f"mod:{name}"
            entities_created += result.entities_created
            edges_created += result.edges_created
            if result.errors:
                errors.extend(result.errors)
        except Exception as exc:
            errors.append(f"py ingest failed {py_file.name}: {exc}")

    return entities_created, edges_created


def _extract_module_summary(source: str) -> str:
    """Extract the first docstring or first 5 lines."""
    try:
        tree = ast.parse(source)
        docstring = ast.get_docstring(tree)
        if docstring:
            return docstring[:500]
    except SyntaxError:
        pass
    lines = source.splitlines()[:5]
    return "\n".join(lines)


def _extract_imports(source: str) -> list[str]:
    """Extract imported module names from Python source."""
    imports: list[str] = []
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return imports

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                imports.append(node.module)
    return imports
