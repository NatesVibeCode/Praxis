"""Heartbeat module: keeps the discovery index and knowledge graph edges current.

Runs ModuleIndexer.index_codebase() for vector embeddings (incremental via
source_hash) and rebuilds the AST-extracted subsystem dependency map when
any source files have changed.  Covers both Python and TypeScript/TSX.
"""
from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import time
from pathlib import Path

from runtime.workspace_paths import code_tree_root
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _ok, _fail


# ---------------------------------------------------------------------------
# Dependency extraction (lightweight, no embeddings needed)
# ---------------------------------------------------------------------------

_STDLIB = frozenset(
    "os sys json asyncio typing datetime pathlib hashlib uuid time re "
    "collections dataclasses enum abc functools itertools logging math "
    "random copy io struct base64 urllib http textwrap inspect contextlib "
    "traceback subprocess shutil tempfile socket ssl csv signal ast "
    "warnings threading concurrent select queue importlib pkgutil argparse "
    "sqlite3 unittest".split()
)
_THIRDPARTY = frozenset(
    "aiohttp asyncpg psycopg fastapi uvicorn pydantic openai anthropic "
    "pytest yaml toml dotenv httpx starlette sse_starlette mcp google "
    "numpy sentence_transformers".split()
)

_TS_IMPORT_RE = re.compile(
    r"""(?:import|from)\s+['"]([^'"]+)['"]|import\s+.*?\s+from\s+['"]([^'"]+)['"]"""
)
_TS_COMPONENT_RE = re.compile(r"""(?:export\s+)?(?:default\s+)?function\s+([A-Z]\w+)\s*\(""")
_TS_HOOK_RE = re.compile(r"""(?:export\s+)?function\s+(use[A-Z]\w+)\s*\(""")


def _extract_ts_deps(source: str) -> tuple[list[str], list[str]]:
    """Extract components and local imports from TS/TSX source."""
    components = _TS_COMPONENT_RE.findall(source)
    hooks = _TS_HOOK_RE.findall(source)
    imports: list[str] = []
    for m in _TS_IMPORT_RE.finditer(source):
        imp = m.group(1) or m.group(2)
        if imp and imp.startswith("."):
            imports.append(imp)
    return components + hooks, imports


def _extract_dependency_map(workflow_root: str) -> tuple[dict, list[dict]]:
    """Walk Python and TS/TSX source, return (modules_by_subsystem, edges)."""
    modules: dict[str, dict] = {}
    edges: list[dict] = []

    for root, dirs, files in os.walk(workflow_root):
        dirs[:] = [d for d in dirs if d not in ("__pycache__", "node_modules", ".git", "dist")]
        for f in files:
            path = os.path.join(root, f)

            if f.endswith(".py") and not f.startswith("test_"):
                mod = os.path.relpath(path, workflow_root).replace("/", ".").removesuffix(".py")
                if mod.endswith(".__init__"):
                    mod = mod[:-9]
                try:
                    tree = ast.parse(open(path).read())
                except Exception:
                    continue

                imports: set[str] = set()
                classes: list[str] = []
                for node in ast.walk(tree):
                    if isinstance(node, ast.ImportFrom) and node.module:
                        imports.add(node.module)
                    elif isinstance(node, ast.Import):
                        for alias in node.names:
                            imports.add(alias.name)
                    elif isinstance(node, ast.ClassDef):
                        classes.append(node.name)

                internal = [
                    i for i in imports
                    if i.split(".")[0] not in _STDLIB
                    and i.split(".")[0] not in _THIRDPARTY
                ]
                modules[mod] = {"classes": classes[:5], "deps": internal}
                for dep in internal:
                    edges.append({"source": mod, "target": dep})

            elif f.endswith((".ts", ".tsx")) and not f.endswith(".d.ts"):
                # Map TS files to dotted module paths under surfaces.app.src
                rel = os.path.relpath(path, workflow_root)
                mod = rel.replace("/", ".").removesuffix(".tsx").removesuffix(".ts")
                try:
                    source = open(path).read()
                except Exception:
                    continue
                components, local_imports = _extract_ts_deps(source)
                modules[mod] = {"classes": components[:5], "deps": local_imports}
                for dep in local_imports:
                    edges.append({"source": mod, "target": dep})

    # Group by subsystem
    subsystems: dict[str, dict] = {}
    for mod, info in modules.items():
        top = mod.split(".")[0]
        subsystems.setdefault(top, {})[mod] = info

    return subsystems, edges


def _build_subsystem_doc(name: str, mods: dict, edges: list[dict]) -> str:
    """Build a compact document for a single subsystem."""
    sub_edges = [e for e in edges if e["source"].split(".")[0] == name]
    lines = [f"Subsystem: {name} ({len(mods)} modules, {len(sub_edges)} edges)"]
    for mod, info in sorted(mods.items()):
        cls = ", ".join(info["classes"][:3]) if info["classes"] else "-"
        deps = [d for d in info["deps"] if d != "__future__"][:4]
        dep_str = ", ".join(deps) if deps else "-"
        lines.append(f"  {mod}: [{cls}] -> [{dep_str}]")
    return "\n".join(lines)


def _content_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _summarize_index_errors(errors: object, *, limit: int = 3) -> str:
    if not isinstance(errors, (list, tuple)):
        return ""
    parts: list[str] = []
    for item in errors[:limit]:
        if not isinstance(item, dict):
            continue
        location = "/".join(
            part
            for part in (
                str(item.get("module_path") or "").strip(),
                str(item.get("kind") or "").strip(),
                str(item.get("name") or "").strip(),
            )
            if part
        )
        label = location or str(item.get("scope") or "index").strip() or "index"
        message = str(item.get("error_message") or "").strip()
        if message:
            parts.append(f"{label}: {message}")
    return "; ".join(parts)


# ---------------------------------------------------------------------------
# Heartbeat module
# ---------------------------------------------------------------------------

class CodebaseIndexModule(HeartbeatModule):
    """Keeps discovery index and knowledge graph dependency map current."""

    def __init__(
        self,
        conn,
        repo_root: str,
        *,
        knowledge_graph=None,
        index_codebase_enabled: bool = True,
    ) -> None:
        self._conn = conn
        self._repo_root = repo_root
        self._workflow_root = str(code_tree_root(Path(repo_root)) / "Workflow")
        self._kg = knowledge_graph
        self._index_codebase_enabled = index_codebase_enabled
        self._last_graph_hash: str | None = None

    def _ensure_conn(self):
        """Return an authoritative sync connection for codebase indexing."""
        if self._conn is None:
            from storage.postgres.connection import ensure_postgres_available

            self._conn = ensure_postgres_available()
            return self._conn
        if isinstance(self._conn, str):
            from storage.postgres.connection import ensure_postgres_available

            self._conn = ensure_postgres_available({"WORKFLOW_DATABASE_URL": self._conn})
            return self._conn
        if not hasattr(self._conn, "execute"):
            raise TypeError(
                "CodebaseIndexModule requires a SyncPostgresConnection-like object with execute()"
            )
        return self._conn

    @property
    def name(self) -> str:
        return "codebase_index"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        errors: list[str] = []

        # Phase 1: Update discovery index (vector embeddings)
        if self._index_codebase_enabled:
            try:
                from runtime.module_indexer import ModuleIndexer

                conn = self._ensure_conn()
                indexer = ModuleIndexer(conn=conn, repo_root=self._repo_root)
                index_result = indexer.index_codebase()
                if str(index_result.get("observability_state") or "complete") != "complete":
                    detail = _summarize_index_errors(index_result.get("errors"))
                    if not detail:
                        detail = "partial indexing failure"
                    errors.append(f"discovery index degraded: {detail}")
            except Exception as exc:
                errors.append(f"discovery index: {exc}")

        # Phase 2: Rebuild AST dependency graph if source changed
        try:
            if self._kg is None:
                from memory.knowledge_graph import KnowledgeGraph

                self._kg = KnowledgeGraph(conn=self._ensure_conn())
            subsystems, edges = _extract_dependency_map(self._workflow_root)
            skip = {"tests"}
            docs: dict[str, str] = {}
            for sub, mods in sorted(subsystems.items()):
                if sub in skip or sub.startswith("__"):
                    continue
                docs[sub] = _build_subsystem_doc(sub, mods, edges)

            composite = "\n---\n".join(docs.values())
            current_hash = _content_hash(composite)

            if current_hash != self._last_graph_hash:
                if self._kg is not None:
                    for sub, doc in docs.items():
                        try:
                            self._kg.ingest(
                                kind="document",
                                content=doc,
                                source=f"heartbeat/subsystem/{sub}",
                            )
                        except Exception as exc:
                            errors.append(f"ingest {sub}: {exc}")

                    cross: dict[str, set] = {}
                    for e in edges:
                        src = e["source"].split(".")[0]
                        tgt = e["target"].split(".")[0]
                        if src != tgt and src not in skip and tgt not in skip:
                            cross.setdefault(src, set()).add(tgt)
                    summary_lines = [f"Cross-Subsystem Dependencies ({len(edges)} edges)"]
                    for src in sorted(cross):
                        summary_lines.append(f"  {src} -> {' | '.join(sorted(cross[src]))}")
                    try:
                        self._kg.ingest(
                            kind="document",
                            content="\n".join(summary_lines),
                            source="heartbeat/cross-subsystem-deps",
                        )
                    except Exception as exc:
                        errors.append(f"ingest cross-deps: {exc}")

                self._last_graph_hash = current_hash
        except Exception as exc:
            errors.append(f"dependency graph: {exc}")

        if errors:
            return _fail(self.name, t0, "; ".join(errors))
        return _ok(self.name, t0)
