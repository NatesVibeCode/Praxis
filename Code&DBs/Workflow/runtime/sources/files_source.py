"""Files source plugin.

Path-glob enumeration with metadata. Replaces ``find -name``, ``ls``, and
``find ... | xargs grep -l`` style fallbacks. When a query is provided
in exact/regex mode, the file is also content-scanned and only files
that contain at least one match are returned.
"""
from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from runtime.sources._relevance import query_tokens, token_overlap_score
from runtime.sources.code_source import _glob_to_regex, _within_time_window
from surfaces.mcp.tools._search_envelope import (
    MODE_EXACT,
    MODE_REGEX,
    MODE_SEMANTIC,
    SOURCE_FILES,
    SearchEnvelope,
    resolve_mode,
)


_PRUNE_DIR_NAMES = (
    "__pycache__",
    ".git",
    ".venv",
    "venv",
    "node_modules",
    ".pytest_cache",
    ".mypy_cache",
    ".tox",
    "dist",
    "build",
)


def _matches_any(path: str, patterns) -> bool:
    if not patterns:
        return False
    return any(_glob_to_regex(p).match(path) for p in patterns)


def _file_contains_query(path: Path, *, query: str, mode: str) -> bool:
    if mode == MODE_SEMANTIC:
        return True
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return False
    if mode == MODE_REGEX:
        try:
            pattern = re.compile(query.strip("/"))
        except re.error:
            return False
        return bool(pattern.search(text))
    return query in text


def search_files(
    *,
    envelope: SearchEnvelope,
    repo_root: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Enumerate files matching the envelope's scope."""

    scope = envelope.scope
    if not scope.paths:
        return [], {
            "status": "skipped",
            "reason": "scope.paths required for files source",
        }

    mode = resolve_mode(envelope)
    require_match = mode in (MODE_EXACT, MODE_REGEX)
    tokens = query_tokens(envelope.query)

    rows: list[dict[str, Any]] = []
    for dirpath, dirnames, filenames in os.walk(repo_root):
        dirnames[:] = [d for d in dirnames if d not in _PRUNE_DIR_NAMES]
        for filename in filenames:
            full = Path(dirpath) / filename
            try:
                rel = full.relative_to(repo_root).as_posix()
            except ValueError:
                continue
            if scope.exclude_paths and _matches_any(rel, scope.exclude_paths):
                continue
            if not _matches_any(rel, scope.paths):
                continue
            try:
                if not _within_time_window(full, scope):
                    continue
            except Exception:
                continue
            if require_match and envelope.query:
                if not _file_contains_query(full, query=envelope.query, mode=mode):
                    continue
            try:
                stat = full.stat()
                size = int(stat.st_size)
                mtime = datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).isoformat()
            except OSError:
                size = 0
                mtime = None
            # Score by token overlap on path+filename. When tokens is
            # empty (no matchable query words), token_overlap_score
            # returns 0.5 — a structured-query call (e.g. ``query="."``
            # with a path glob) just relies on the path filter and gets
            # a neutral score.
            score = token_overlap_score(tokens, f"{rel} {filename}")
            rows.append(
                {
                    "source": SOURCE_FILES,
                    "path": rel,
                    "name": filename,
                    "size_bytes": size,
                    "mtime_iso": mtime,
                    "match_text": rel,
                    "score": score,
                    "found_via": "files.token_overlap",
                }
            )
            if len(rows) >= envelope.limit:
                break
        if len(rows) >= envelope.limit:
            break

    return rows, {"status": "complete", "rows_considered": len(rows)}


__all__ = ["search_files"]
