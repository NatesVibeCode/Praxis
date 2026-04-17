"""Module indexer: automatic functional synonym detection via vector embeddings.

Walks the codebase, extracts behavioral fingerprints from Python modules via
AST analysis, embeds them with sentence-transformers, and stores in Postgres
for semantic similarity search.

This lets agents discover functionally equivalent code before building new
infrastructure.  "Durable messaging" finds the outbox even though they share
zero keywords — because the embeddings capture *what the code does*, not what
it's named.
"""
from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import textwrap
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class BehaviorFingerprint:
    """What a code unit actually does — extracted from AST."""
    imports: list[str] = field(default_factory=list)
    db_tables: list[str] = field(default_factory=list)
    io_patterns: list[str] = field(default_factory=list)     # subprocess, file, network
    data_structures: list[str] = field(default_factory=list)  # classes instantiated/returned
    decorators: list[str] = field(default_factory=list)
    exceptions_handled: list[str] = field(default_factory=list)
    key_operations: list[str] = field(default_factory=list)   # verbs extracted from method names

    def to_dict(self) -> dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if v}

    def to_text(self) -> str:
        """Natural-language description of behaviors for embedding."""
        parts: list[str] = []
        if self.db_tables:
            parts.append(f"Interacts with database tables: {', '.join(self.db_tables)}.")
        if self.io_patterns:
            parts.append(f"I/O patterns: {', '.join(self.io_patterns)}.")
        if self.imports:
            # Only include significant imports
            sig = [i for i in self.imports if not i.startswith("__")]
            if sig:
                parts.append(f"Uses: {', '.join(sig[:15])}.")
        if self.key_operations:
            parts.append(f"Key operations: {', '.join(self.key_operations)}.")
        if self.data_structures:
            parts.append(f"Works with: {', '.join(self.data_structures)}.")
        return " ".join(parts)


@dataclass
class CodeUnit:
    """A single indexable code unit extracted from a Python file."""
    module_id: str
    module_path: str
    kind: str              # module, class, function
    name: str
    docstring: str
    signature: str
    behavior: BehaviorFingerprint
    summary: str           # combined natural-language description
    source_hash: str       # hash of source to detect changes


# ---------------------------------------------------------------------------
# AST extraction
# ---------------------------------------------------------------------------

# Patterns for extracting SQL table references
_SQL_TABLE_RE = re.compile(
    r"""(?:FROM|INTO|UPDATE|JOIN|INSERT\s+INTO|DELETE\s+FROM|TABLE)\s+
        (?:IF\s+(?:NOT\s+)?EXISTS\s+)?
        "?(\w+)"?""",
    re.IGNORECASE | re.VERBOSE,
)

# I/O pattern keywords
_IO_PATTERNS = {
    "subprocess": "spawns subprocesses",
    "os.popen": "spawns subprocesses",
    "Popen": "spawns subprocesses",
    "open(": "reads/writes files",
    "Path(": "filesystem path operations",
    "write_text": "writes files",
    "read_text": "reads files",
    "socket": "network socket operations",
    "requests.": "HTTP client requests",
    "urllib": "HTTP client requests",
    "asyncpg": "async Postgres operations",
    "psycopg": "Postgres operations",
    "aiohttp": "async HTTP operations",
    "tempfile": "temporary file operations",
    "shutil": "filesystem copy/move operations",
    "json.dump": "serializes JSON to disk",
    "json.load": "deserializes JSON from disk",
}

# Verbs to extract from method/function names
_VERB_RE = re.compile(r'^(get|set|create|delete|remove|update|insert|fetch|load|save|write|read|parse|validate|checkpoint|check|run|execute|dispatch|spawn|resolve|build|compute|calculate|mine|ingest|emit|subscribe|publish|notify|record|track|replay|recover|retry|heal|scan|search|query|route|match|classify|enrich|materialize|aggregate|merge|sync|flush|index|embed|encode|rank|score|detect|enforce|block|allow|gate|acquire|release|lease)')


def _extract_verbs(name: str) -> list[str]:
    """Extract action verbs from a function/method name."""
    # Convert camelCase to snake_case
    s = re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()
    parts = s.split('_')
    verbs = []
    for p in parts:
        m = _VERB_RE.match(p)
        if m:
            verbs.append(m.group(1))
    return verbs


def _extract_behavior(tree: ast.AST, source: str) -> BehaviorFingerprint:
    """Walk an AST and extract behavioral fingerprint."""
    fp = BehaviorFingerprint()

    for node in ast.walk(tree):
        # Imports
        if isinstance(node, ast.Import):
            for alias in node.names:
                fp.imports.append(alias.name.split('.')[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                fp.imports.append(node.module.split('.')[0])

        # Decorators
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Name):
                    fp.decorators.append(dec.id)
                elif isinstance(dec, ast.Attribute):
                    fp.decorators.append(dec.attr)
            # Extract verbs from function names
            fp.key_operations.extend(_extract_verbs(node.name))

        # Class definitions
        elif isinstance(node, ast.ClassDef):
            fp.data_structures.append(node.name)
            for dec in node.decorator_list:
                if isinstance(dec, ast.Name):
                    fp.decorators.append(dec.id)

        # Exception handlers
        elif isinstance(node, ast.ExceptHandler):
            if node.type:
                if isinstance(node.type, ast.Name):
                    fp.exceptions_handled.append(node.type.id)

        # String constants — check for SQL
        elif isinstance(node, ast.Constant) and isinstance(node.value, str):
            if len(node.value) > 20:
                tables = _SQL_TABLE_RE.findall(node.value)
                fp.db_tables.extend(t for t in tables if t.lower() not in (
                    'set', 'select', 'where', 'and', 'or', 'not', 'null', 'true', 'false',
                ))

    # I/O patterns from source text
    for pattern, description in _IO_PATTERNS.items():
        if pattern in source:
            if description not in fp.io_patterns:
                fp.io_patterns.append(description)

    # Deduplicate
    fp.imports = sorted(set(fp.imports))
    fp.db_tables = sorted(set(fp.db_tables))
    fp.io_patterns = sorted(set(fp.io_patterns))
    fp.data_structures = sorted(set(fp.data_structures))
    fp.decorators = sorted(set(fp.decorators))
    fp.exceptions_handled = sorted(set(fp.exceptions_handled))
    fp.key_operations = sorted(set(fp.key_operations))

    return fp


def _build_summary(name: str, kind: str, docstring: str, signature: str,
                   behavior: BehaviorFingerprint) -> str:
    """Build a natural-language summary for embedding.

    This is the critical piece — the embedding model sees this text, so it
    must describe *what the code does and what problem it solves*, not just
    its name.
    """
    parts: list[str] = []

    # Start with what it is
    parts.append(f"{kind.title()} '{name}'.")

    # Docstring is the richest signal
    if docstring:
        # Take first paragraph (usually the most useful)
        first_para = docstring.strip().split('\n\n')[0].strip()
        # Collapse whitespace
        first_para = ' '.join(first_para.split())
        if len(first_para) > 500:
            first_para = first_para[:500] + "..."
        parts.append(first_para)

    # Signature tells us the interface
    if signature and kind in ('function', 'class'):
        parts.append(f"Interface: {signature}")

    # Behavioral description
    behavior_text = behavior.to_text()
    if behavior_text:
        parts.append(behavior_text)

    return " ".join(parts)


def _function_signature(node: ast.FunctionDef | ast.AsyncFunctionDef) -> str:
    """Extract a human-readable function signature."""
    args = []
    for arg in node.args.args:
        ann = ""
        if arg.annotation:
            try:
                ann = f": {ast.unparse(arg.annotation)}"
            except Exception:
                pass
        args.append(f"{arg.arg}{ann}")

    ret = ""
    if node.returns:
        try:
            ret = f" -> {ast.unparse(node.returns)}"
        except Exception:
            pass

    prefix = "async " if isinstance(node, ast.AsyncFunctionDef) else ""
    return f"{prefix}def {node.name}({', '.join(args)}){ret}"


def _class_signature(node: ast.ClassDef) -> str:
    """Extract class name with bases."""
    bases = []
    for base in node.bases:
        try:
            bases.append(ast.unparse(base))
        except Exception:
            pass
    base_str = f"({', '.join(bases)})" if bases else ""
    return f"class {node.name}{base_str}"


def _source_hash(source: str) -> str:
    return hashlib.sha256(source.encode('utf-8')).hexdigest()[:16]


def _make_module_id(path: str, kind: str, name: str) -> str:
    """Deterministic ID for a code unit."""
    normalized = os.path.normpath(path)
    key = f"{normalized}::{kind}::{name}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _index_error_record(
    *,
    scope: str,
    code: str,
    error: BaseException,
    module_path: str | None = None,
    kind: str | None = None,
    name: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "scope": scope,
        "code": code,
        "error_type": type(error).__name__,
        "error_message": str(error),
    }
    if module_path:
        payload["module_path"] = module_path
    if kind:
        payload["kind"] = kind
    if name:
        payload["name"] = name
    return payload


# ---------------------------------------------------------------------------
# File walker and extractor
# ---------------------------------------------------------------------------

# Directories to skip
_SKIP_DIRS = {
    '__pycache__', '.git', '.pytest_cache', 'node_modules',
    '.mypy_cache', '.ruff_cache', 'venv', '.venv', 'eggs',
}

# Files to skip
_SKIP_FILES = {'__init__.py', 'conftest.py'}


def extract_code_units(filepath: str, repo_root: str) -> list[CodeUnit]:
    """Parse a Python file and extract indexable code units."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            source = f.read()
    except (OSError, UnicodeDecodeError):
        return []

    if not source.strip():
        return []

    try:
        tree = ast.parse(source, filename=filepath)
    except SyntaxError:
        return []

    rel_path = os.path.normpath(os.path.relpath(os.path.realpath(filepath), os.path.realpath(repo_root)))
    src_hash = _source_hash(source)
    units: list[CodeUnit] = []

    # Module-level unit
    module_doc = ast.get_docstring(tree) or ""
    module_behavior = _extract_behavior(tree, source)
    module_name = Path(filepath).stem

    units.append(CodeUnit(
        module_id=_make_module_id(rel_path, "module", module_name),
        module_path=rel_path,
        kind="module",
        name=module_name,
        docstring=module_doc,
        signature="",
        behavior=module_behavior,
        summary=_build_summary(module_name, "module", module_doc, "", module_behavior),
        source_hash=src_hash,
    ))

    # Top-level classes
    for node in ast.iter_child_nodes(tree):
        if isinstance(node, ast.ClassDef):
            # Skip private/test classes
            if node.name.startswith('_') and not node.name.startswith('__'):
                continue

            class_doc = ast.get_docstring(node) or ""
            class_sig = _class_signature(node)

            # Extract behavior from class body
            class_source = ast.get_source_segment(source, node) or ""
            try:
                class_tree = ast.parse(class_source)
                class_behavior = _extract_behavior(class_tree, class_source)
            except SyntaxError:
                class_behavior = BehaviorFingerprint()

            # Add method signatures to behavior
            for item in ast.iter_child_nodes(node):
                if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    if not item.name.startswith('_') or item.name in ('__init__', '__call__'):
                        class_behavior.key_operations.extend(_extract_verbs(item.name))

            class_behavior.key_operations = sorted(set(class_behavior.key_operations))

            units.append(CodeUnit(
                module_id=_make_module_id(rel_path, "class", node.name),
                module_path=rel_path,
                kind="class",
                name=node.name,
                docstring=class_doc,
                signature=class_sig,
                behavior=class_behavior,
                summary=_build_summary(node.name, "class", class_doc, class_sig, class_behavior),
                source_hash=src_hash,
            ))

        # Top-level functions (not methods)
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name.startswith('_'):
                continue

            func_doc = ast.get_docstring(node) or ""
            func_sig = _function_signature(node)

            func_source = ast.get_source_segment(source, node) or ""
            try:
                func_tree = ast.parse(func_source)
                func_behavior = _extract_behavior(func_tree, func_source)
            except SyntaxError:
                func_behavior = BehaviorFingerprint()

            units.append(CodeUnit(
                module_id=_make_module_id(rel_path, "function", node.name),
                module_path=rel_path,
                kind="function",
                name=node.name,
                docstring=func_doc,
                signature=func_sig,
                behavior=func_behavior,
                summary=_build_summary(node.name, "function", func_doc, func_sig, func_behavior),
                source_hash=src_hash,
            ))

    return units


def walk_codebase(repo_root: str, subdirs: list[str] | None = None) -> list[CodeUnit]:
    """Walk Python and TypeScript files in the codebase and extract all code units."""
    all_units: list[CodeUnit] = []

    if subdirs is None:
        subdirs = ["."]

    for subdir in subdirs:
        root_path = os.path.join(repo_root, subdir)
        if not os.path.isdir(root_path):
            continue

        for dirpath, dirnames, filenames in os.walk(root_path):
            # Skip excluded directories
            dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]

            for filename in filenames:
                filepath = os.path.join(dirpath, filename)

                if filename.endswith('.py'):
                    if filename in _SKIP_FILES:
                        continue
                    if filename.startswith('test_'):
                        continue
                    units = extract_code_units(filepath, repo_root)
                    all_units.extend(units)

                elif filename.endswith(('.ts', '.tsx')):
                    if filename.endswith('.d.ts') or filename.startswith('test'):
                        continue
                    units = extract_ts_code_units(filepath, repo_root)
                    all_units.extend(units)

    return all_units


# ---------------------------------------------------------------------------
# TypeScript / TSX extraction (regex-based, no AST parser needed)
# ---------------------------------------------------------------------------

# Patterns for TS extraction
_TS_IMPORT_RE = re.compile(r"""(?:import|from)\s+['"]([^'"]+)['"]|import\s+.*?\s+from\s+['"]([^'"]+)['"]""")
_TS_COMPONENT_RE = re.compile(r"""(?:export\s+)?(?:default\s+)?function\s+([A-Z]\w+)\s*\(""")
_TS_HOOK_RE = re.compile(r"""(?:export\s+)?function\s+(use[A-Z]\w+)\s*\(""")
_TS_INTERFACE_RE = re.compile(r"""(?:export\s+)?(?:interface|type)\s+(\w+)\s*[{=<]""")
_TS_EXPORT_FN_RE = re.compile(r"""export\s+(?:async\s+)?function\s+(\w+)\s*\(([^)]*)\)""")
_TS_CONST_FN_RE = re.compile(r"""export\s+const\s+(\w+)\s*=\s*(?:async\s+)?\(""")
_TS_FETCH_RE = re.compile(r"""fetch\s*\(\s*[`'"]([^`'"]*)[`'"]""")
_TS_USESTATE_RE = re.compile(r"""useState<([^>]+)>""")
_TS_USEEFFECT_RE = re.compile(r"""useEffect\s*\(""")
_TS_USEMODULEDATA_RE = re.compile(r"""useModuleData\w*\s*(?:<[^>]*>)?\s*\(\s*['"]([^'"]+)['"]""")
_TS_USEAPI_RE = re.compile(r"""useApi\w*\s*(?:<[^>]*>)?\s*\(\s*['"]([^'"]+)['"]""")
_TS_INTERFACE_FIELDS_RE = re.compile(
    r"""(?:interface|type)\s+(\w+)\s*(?:=\s*)?\{([^}]{1,500})\}""", re.DOTALL
)
_TS_JSX_TAG_RE = re.compile(r"""<([A-Z]\w+)""")
_TS_REGISTER_RE = re.compile(r"""registerModule\s*\(\s*['"]([^'"]+)['"]""")
_TS_COMMENT_RE = re.compile(r"""(?://|/\*\*?)\s*(.{10,80})""")

_TS_IO_PATTERNS = {
    "fetch(": "makes HTTP requests",
    "WebSocket": "WebSocket connections",
    "localStorage": "browser local storage",
    "sessionStorage": "browser session storage",
    "EventSource": "server-sent events",
    "window.location": "page navigation",
}


def _extract_ts_behavior(source: str) -> BehaviorFingerprint:
    """Extract behavioral fingerprint from TypeScript/TSX source."""
    fp = BehaviorFingerprint()

    # Imports
    for m in _TS_IMPORT_RE.finditer(source):
        imp = m.group(1) or m.group(2)
        if imp and not imp.startswith('.'):
            fp.imports.append(imp.split('/')[0])

    # I/O patterns
    for pattern, desc in _TS_IO_PATTERNS.items():
        if pattern in source:
            fp.io_patterns.append(desc)

    # API endpoints from fetch()
    for m in _TS_FETCH_RE.finditer(source):
        url = m.group(1)
        if url:
            fp.key_operations.append(f"fetches {url}")

    # API endpoints from useModuleData('bugs') / useApi('/api/...')
    for m in _TS_USEMODULEDATA_RE.finditer(source):
        fp.key_operations.append(f"displays data from {m.group(1)} endpoint")
        fp.db_tables.append(m.group(1))  # treat as "table" for cross-linking
    for m in _TS_USEAPI_RE.finditer(source):
        fp.key_operations.append(f"calls {m.group(1)}")

    # React hooks usage
    hooks = set()
    if _TS_USEEFFECT_RE.search(source):
        hooks.add("useEffect")
    for m in _TS_USESTATE_RE.finditer(source):
        hooks.add(f"useState<{m.group(1)}>")
    if "useCallback" in source:
        hooks.add("useCallback")
    if "useMemo" in source:
        hooks.add("useMemo")
    if "useRef" in source:
        hooks.add("useRef")
    if hooks:
        fp.data_structures.extend(sorted(hooks))

    # Deduplicate
    fp.imports = sorted(set(fp.imports))
    fp.io_patterns = sorted(set(fp.io_patterns))
    fp.key_operations = sorted(set(fp.key_operations))
    fp.data_structures = sorted(set(fp.data_structures))
    fp.db_tables = sorted(set(fp.db_tables))

    return fp


def _extract_ts_context(source: str) -> dict:
    """Extract rich context from TS/TSX for summary generation."""
    ctx: dict = {}

    # Interfaces with their fields → tells us what data the component handles
    ifaces: dict[str, list[str]] = {}
    for m in _TS_INTERFACE_FIELDS_RE.finditer(source):
        name = m.group(1)
        body = m.group(2)
        fields = [f.strip().split(':')[0].split('?')[0].strip()
                  for f in body.split(';') if f.strip() and ':' in f]
        if fields:
            ifaces[name] = fields[:8]
    if ifaces:
        ctx["interfaces"] = ifaces

    # JSX tags → tells us what primitives/children it renders
    jsx_tags = sorted(set(_TS_JSX_TAG_RE.findall(source)))
    # Filter out HTML-like (lowercase start already filtered by [A-Z]) and very common
    jsx_tags = [t for t in jsx_tags if t not in ("React", "Fragment")]
    if jsx_tags:
        ctx["renders"] = jsx_tags[:10]

    # Module registry name
    reg = _TS_REGISTER_RE.findall(source)
    if reg:
        ctx["registered_as"] = reg[0]

    # Leading comment/docstring
    first_lines = source[:500]
    comments = _TS_COMMENT_RE.findall(first_lines)
    if comments:
        ctx["description"] = comments[0].strip().rstrip("*/").strip()

    return ctx


def _build_ts_summary(name: str, kind: str, details: list[str],
                      behavior: BehaviorFingerprint, ctx: dict | None = None) -> str:
    """Build a rich summary for a TypeScript code unit."""
    parts = [f"React {kind} '{name}'."]
    parts.extend(details)

    if ctx:
        if "registered_as" in ctx:
            parts.append(f"Registered as module type '{ctx['registered_as']}'.")
        if "description" in ctx:
            parts.append(ctx["description"])
        if "interfaces" in ctx:
            for iname, fields in ctx["interfaces"].items():
                parts.append(f"Data shape {iname}: {', '.join(fields)}.")
        if "renders" in ctx:
            parts.append(f"Renders: {', '.join(ctx['renders'][:8])}.")

    bt = behavior.to_text()
    if bt:
        parts.append(bt)
    return " ".join(parts)


def extract_ts_code_units(filepath: str, repo_root: str) -> list[CodeUnit]:
    """Extract indexable code units from a TypeScript/TSX file."""
    try:
        with open(filepath, 'r', encoding='utf-8', errors='replace') as f:
            source = f.read()
    except (OSError, UnicodeDecodeError):
        return []

    if not source.strip():
        return []

    rel_path = os.path.normpath(os.path.relpath(os.path.realpath(filepath), os.path.realpath(repo_root)))
    src_hash = _source_hash(source)
    units: list[CodeUnit] = []
    behavior = _extract_ts_behavior(source)
    ctx = _extract_ts_context(source)
    basename = Path(filepath).stem

    # Module-level unit
    details: list[str] = []
    components = _TS_COMPONENT_RE.findall(source)
    hooks = _TS_HOOK_RE.findall(source)
    interfaces = _TS_INTERFACE_RE.findall(source)

    if components:
        details.append(f"Components: {', '.join(components[:5])}.")
    if hooks:
        details.append(f"Hooks: {', '.join(hooks[:5])}.")
    if interfaces:
        details.append(f"Types: {', '.join(interfaces[:5])}.")

    # Map to existing DB kinds: module for files, class for components
    units.append(CodeUnit(
        module_id=_make_module_id(rel_path, "module", basename),
        module_path=rel_path,
        kind="module",
        name=basename,
        docstring="",
        signature="",
        behavior=behavior,
        summary=_build_ts_summary(basename, "component" if components else "module", details, behavior, ctx),
        source_hash=src_hash,
    ))

    # Individual components
    for comp_name in components:
        comp_details = [f"React component defined in {basename}."]
        units.append(CodeUnit(
            module_id=_make_module_id(rel_path, "class", comp_name),
            module_path=rel_path,
            kind="class",
            name=comp_name,
            docstring="",
            signature=f"function {comp_name}(props)",
            behavior=behavior,
            summary=_build_ts_summary(comp_name, "component", comp_details, behavior, ctx),
            source_hash=src_hash,
        ))

    # Custom hooks as functions
    for hook_name in hooks:
        units.append(CodeUnit(
            module_id=_make_module_id(rel_path, "function", hook_name),
            module_path=rel_path,
            kind="function",
            name=hook_name,
            docstring="",
            signature=f"function {hook_name}()",
            behavior=behavior,
            summary=_build_ts_summary(hook_name, "hook", [f"Custom hook defined in {basename}."], behavior, ctx),
            source_hash=src_hash,
        ))

    return units


# ---------------------------------------------------------------------------
# Embedding + Postgres storage
# ---------------------------------------------------------------------------

from runtime.embedding_service import EmbeddingService
from storage.postgres.vector_store import (
    PostgresVectorStore,
    VectorFilter,
    format_vector_literal,
)


class ModuleIndexer:
    """Indexes codebase modules into Postgres with vector embeddings.

    Provides functional similarity search so agents can discover existing
    implementations before building new ones.
    """

    def __init__(self, conn, repo_root: str, model_name: str = "all-MiniLM-L6-v2") -> None:
        self._conn = conn
        self._repo_root = repo_root
        self._embedder = EmbeddingService(model_name)
        self._vector_store = PostgresVectorStore(conn, self._embedder)

    def index_codebase(
        self,
        subdirs: list[str] | None = None,
        force: bool = False,
    ) -> dict[str, int]:
        """Walk codebase, extract units, embed, and upsert into Postgres.

        Args:
            subdirs: Directories to scan (relative to repo_root).
                     Defaults to Workflow source dirs.
            force: If True, re-index even if source_hash unchanged.

        Returns dict with counts: indexed, skipped, total.
        """
        if subdirs is None:
            subdirs = [
                "Code&DBs/Workflow/runtime",
                "Code&DBs/Workflow/memory",
                "Code&DBs/Workflow/storage",
                "Code&DBs/Workflow/surfaces",
                "Code&DBs/Workflow/adapters",
                "Code&DBs/Workflow/registry",
                "Code&DBs/Workflow/observability",
            ]

        units = walk_codebase(self._repo_root, subdirs)

        if not units:
            return {"indexed": 0, "skipped": 0, "total": 0}

        # Check which units need re-indexing
        existing_hashes = {}
        if not force:
            try:
                rows = self._conn.execute(
                    "SELECT module_id, source_hash FROM module_embeddings"
                )
                existing_hashes = {r["module_id"]: r["source_hash"] for r in rows}
            except Exception:
                pass

        to_index = []
        skipped = 0
        for unit in units:
            if not force and unit.module_id in existing_hashes:
                if existing_hashes[unit.module_id] == unit.source_hash:
                    skipped += 1
                    continue
            to_index.append(unit)

        if not to_index:
            return {
                "indexed": 0,
                "skipped": skipped,
                "total": len(units),
                "observability_state": "complete",
                "errors": (),
            }

        # Batch embed all summaries
        summaries = [u.summary for u in to_index]
        embeddings = self._embedder.embed(summaries)
        if len(embeddings) != len(to_index):
            raise RuntimeError(
                f"embedding_batch_count_mismatch:{len(embeddings)}:{len(to_index)}"
            )

        # Upsert into Postgres
        indexed = 0
        errors: list[dict[str, Any]] = []
        for unit, embedding in zip(to_index, embeddings):
            try:
                if embedding is None:
                    raise RuntimeError("embedding_missing")
                if isinstance(embedding, (str, bytes)) or not isinstance(embedding, Sequence):
                    raise RuntimeError(f"embedding_invalid_type:{type(embedding).__name__}")
                normalized_embedding = tuple(float(value) for value in embedding)
                expected_dimensions = getattr(self._embedder, "dimensions", None)
                if isinstance(expected_dimensions, int) and len(normalized_embedding) != expected_dimensions:
                    raise RuntimeError(
                        f"embedding_dimensions_mismatch:{len(normalized_embedding)}:{expected_dimensions}"
                    )
                if not normalized_embedding:
                    raise RuntimeError("embedding_empty")
                vector_literal = format_vector_literal(normalized_embedding)
                self._conn.execute(
                    """INSERT INTO module_embeddings
                       (module_id, module_path, kind, name, docstring, signature,
                        behavior, summary, source_hash, embedding, indexed_at)
                       VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8, $9, $10::vector, NOW())
                       ON CONFLICT (module_id) DO UPDATE SET
                           module_path = EXCLUDED.module_path,
                           kind = EXCLUDED.kind,
                           name = EXCLUDED.name,
                           docstring = EXCLUDED.docstring,
                           signature = EXCLUDED.signature,
                           behavior = EXCLUDED.behavior,
                           summary = EXCLUDED.summary,
                           source_hash = EXCLUDED.source_hash,
                           embedding = EXCLUDED.embedding,
                           indexed_at = NOW()
                    """,
                    unit.module_id, unit.module_path, unit.kind, unit.name,
                    unit.docstring[:2000] if unit.docstring else "",
                    unit.signature[:500] if unit.signature else "",
                    json.dumps(unit.behavior.to_dict()),
                    unit.summary[:3000],
                    unit.source_hash,
                    vector_literal,
                )
                indexed += 1
            except Exception as exc:
                errors.append(
                    _index_error_record(
                        scope="index_codebase",
                        code="module_indexer.index_failed",
                        error=exc,
                        module_path=unit.module_path,
                        kind=unit.kind,
                        name=unit.name,
                    )
                )
                # Keep stderr noise for operators, but do not pretend the batch was healthy.
                import sys
                print(f"[module_indexer] Failed to index {unit.name}: {exc}", file=sys.stderr)

        return {
            "indexed": indexed,
            "skipped": skipped,
            "total": len(units),
            "observability_state": "degraded" if errors else "complete",
            "errors": tuple(errors),
        }

    def search(
        self,
        query: str,
        limit: int = 10,
        kind: str | None = None,
        threshold: float = 0.3,
    ) -> list[dict[str, Any]]:
        """Find modules functionally similar to a natural-language query.

        Uses hybrid retrieval: vector cosine similarity + full-text search,
        combined via reciprocal rank fusion.

        Args:
            query: Natural language description of what you're looking for.
            limit: Max results to return.
            kind: Filter by kind (module, class, function, subsystem).
            threshold: Minimum similarity score (0-1) to include.

        Returns list of dicts with module_path, name, kind, summary, score.
        """
        vector_query = self._vector_store.prepare(query)

        # Vector search
        vector_filters = [VectorFilter("kind", kind)] if kind else None
        vector_rows = vector_query.search(
            "module_embeddings",
            select_columns=(
                "module_id",
                "module_path",
                "kind",
                "name",
                "summary",
                "docstring",
                "behavior",
                "signature",
            ),
            filters=vector_filters,
            limit=limit * 2,
            min_similarity=threshold,
            score_alias="cosine_similarity",
        )

        # Full-text search
        fts_kind_filter = "AND kind = $2" if kind else ""
        fts_args = [query, kind] if kind else [query]
        fts_rows = self._conn.execute(
            f"""SELECT module_id,
                       ts_rank(search_vector, plainto_tsquery('english', $1)) AS text_rank
                FROM module_embeddings
                WHERE search_vector @@ plainto_tsquery('english', $1)
                {fts_kind_filter}
                ORDER BY text_rank DESC
                LIMIT {limit * 2}
            """,
            *fts_args,
        )

        # Reciprocal Rank Fusion
        vec_scores: dict[str, float] = {}
        vec_data: dict[str, dict] = {}
        for i, row in enumerate(vector_rows):
            mid = row["module_id"]
            vec_scores[mid] = 1.0 / (60 + i)  # RRF with k=60
            vec_data[mid] = dict(row)

        fts_scores: dict[str, float] = {}
        for i, row in enumerate(fts_rows):
            mid = row["module_id"]
            fts_scores[mid] = 1.0 / (60 + i)

        # Combine scores
        all_ids = set(vec_scores) | set(fts_scores)
        fused: list[tuple[str, float]] = []
        for mid in all_ids:
            score = vec_scores.get(mid, 0.0) + fts_scores.get(mid, 0.0)
            fused.append((mid, score))

        fused.sort(key=lambda x: -x[1])

        # Build results, enriching from vector data
        results: list[dict[str, Any]] = []
        for mid, score in fused[:limit]:
            if mid in vec_data:
                row = vec_data[mid]
                cosine = float(row.get("cosine_similarity", 0))
            else:
                # Fetch from DB if only from FTS
                fetched = vector_query.search(
                    "module_embeddings",
                    select_columns=(
                        "module_path",
                        "kind",
                        "name",
                        "summary",
                        "docstring",
                        "behavior",
                        "signature",
                    ),
                    filters=(
                        [VectorFilter("module_id", mid)]
                        + ([VectorFilter("kind", kind)] if kind else [])
                    ),
                    limit=1,
                    min_similarity=None,
                    score_alias="cosine_similarity",
                )
                if not fetched:
                    continue
                row = fetched[0]
                cosine = float(row.get("cosine_similarity", 0))

            results.append({
                "module_path": row["module_path"],
                "kind": row["kind"],
                "name": row["name"],
                "summary": row["summary"][:300],
                "docstring_preview": (row.get("docstring") or "")[:200],
                "signature": row.get("signature", ""),
                "cosine_similarity": round(cosine, 4),
                "fused_score": round(score, 6),
            })

        return results

    def stats(self) -> dict[str, Any]:
        """Return index statistics."""
        try:
            total = self._conn.fetchval("SELECT COUNT(*) FROM module_embeddings") or 0
            by_kind = self._conn.execute(
                "SELECT kind, COUNT(*) as cnt FROM module_embeddings GROUP BY kind ORDER BY cnt DESC"
            )
            return {
                "total_indexed": total,
                "by_kind": {r["kind"]: int(r["cnt"]) for r in by_kind},
                "observability_state": "complete",
                "errors": (),
            }
        except Exception as exc:
            return {
                "total_indexed": 0,
                "by_kind": {},
                "observability_state": "degraded",
                "errors": (
                    _index_error_record(
                        scope="stats",
                        code="module_indexer.stats_failed",
                        error=exc,
                    ),
                ),
            }
