"""Code search source plugin.

Translates a SearchEnvelope into code hits. Supports three modes:

- ``semantic`` — delegates to ``module_indexer.search`` (vector + FTS).
- ``exact`` — literal substring match across on-disk Python source.
- ``regex`` — Python regex match across on-disk Python source.

For ``exact``/``regex``, results carry path + line_no + line context so
agents stop reaching for ``grep -rn -A N``. For ``semantic``, results
carry the indexer's cosine similarity plus optional context extracted
from disk when the envelope asks for it.
"""
from __future__ import annotations

import os
import re
from collections.abc import Iterator, Sequence
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

from runtime.workspace_paths import strip_workflow_prefix
from runtime.module_indexer import default_index_subdirs

from surfaces.mcp.tools._search_envelope import (
    MODE_EXACT,
    MODE_REGEX,
    MODE_SEMANTIC,
    SHAPE_CONTEXT,
    SHAPE_FULL,
    SHAPE_MATCH,
    SOURCE_CODE,
    SearchEnvelope,
    SearchScope,
    resolve_mode,
)


_TEXT_EXTENSIONS = (".py", ".sql", ".md", ".json", ".yaml", ".yml", ".toml", ".sh")
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


class CodeSourceError(RuntimeError):
    """Raised when the code source cannot complete a query."""


@lru_cache(maxsize=512)
def _glob_to_regex(pattern: str) -> re.Pattern[str]:
    """Translate a path glob to a regex.

    Supports ``**`` (any depth), ``*`` (within one segment), ``?`` (one
    char, not ``/``), and literal text. Anchored at both ends.
    """

    parts: list[str] = []
    i = 0
    n = len(pattern)
    while i < n:
        c = pattern[i]
        if c == "*" and i + 1 < n and pattern[i + 1] == "*":
            parts.append(".*")
            i += 2
            if i < n and pattern[i] == "/":
                i += 1
        elif c == "*":
            parts.append("[^/]*")
            i += 1
        elif c == "?":
            parts.append("[^/]")
            i += 1
        else:
            parts.append(re.escape(c))
            i += 1
    return re.compile("^" + "".join(parts) + "$")


def _matches_any_glob(path: str, patterns: Sequence[str]) -> bool:
    if not patterns:
        return False
    for pattern in patterns:
        if _glob_to_regex(pattern).match(path):
            return True
    return False


def _path_in_scope(rel_path: str, scope: SearchScope) -> bool:
    if scope.exclude_paths and _matches_any_glob(rel_path, scope.exclude_paths):
        return False
    if scope.paths and not _matches_any_glob(rel_path, scope.paths):
        return False
    return True


def _iter_source_files(repo_root: Path, scope: SearchScope) -> Iterator[Path]:
    """Walk repo_root yielding text-source files inside the scope."""

    if scope.paths:
        roots = [repo_root]
    else:
        roots = [
            repo_root / subdir
            for subdir in default_index_subdirs(str(repo_root))
        ]

    visited: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        if root in visited:
            continue
        visited.add(root)
        for dirpath, dirnames, filenames in os.walk(root):
            dirnames[:] = [d for d in dirnames if d not in _PRUNE_DIR_NAMES]
            for filename in filenames:
                if not filename.endswith(_TEXT_EXTENSIONS):
                    continue
                path = Path(dirpath) / filename
                try:
                    rel = path.relative_to(repo_root).as_posix()
                except ValueError:
                    continue
                if not _path_in_scope(rel, scope):
                    continue
                yield path


def _within_time_window(path: Path, scope: SearchScope) -> bool:
    if not (scope.since_iso or scope.until_iso):
        return True
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return False
    if scope.since_iso:
        try:
            since = datetime.fromisoformat(scope.since_iso)
        except ValueError as exc:
            raise CodeSourceError(f"scope.since_iso invalid: {exc}") from exc
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        if mtime < since:
            return False
    if scope.until_iso:
        try:
            until = datetime.fromisoformat(scope.until_iso)
        except ValueError as exc:
            raise CodeSourceError(f"scope.until_iso invalid: {exc}") from exc
        if until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        if mtime > until:
            return False
    return True


def _extract_context(lines: list[str], line_no: int, context_lines: int) -> str:
    if context_lines <= 0:
        return lines[line_no - 1].rstrip("\n") if 0 < line_no <= len(lines) else ""
    start = max(0, line_no - 1 - context_lines)
    end = min(len(lines), line_no + context_lines)
    return "".join(lines[start:end]).rstrip("\n")


def _shape_payload(
    *,
    path: Path,
    rel_path: str,
    line_no: int,
    line_text: str,
    lines: list[str],
    shape: str,
    context_lines: int,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "path": rel_path,
        "line_no": line_no,
        "match_text": line_text.rstrip("\n"),
    }
    if shape == SHAPE_CONTEXT:
        payload["context"] = _extract_context(lines, line_no, context_lines)
    elif shape == SHAPE_FULL:
        try:
            payload["full"] = path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            payload["full_error"] = str(exc)
    return payload


def _exclude_term_hit(text: str, exclude_terms: Sequence[str]) -> bool:
    if not exclude_terms:
        return False
    haystack = text.lower()
    return any(term.lower() in haystack for term in exclude_terms)


def _scan_literal(
    *,
    repo_root: Path,
    envelope: SearchEnvelope,
    pattern: re.Pattern[str] | None,
    literal: str | None,
) -> Iterator[dict[str, Any]]:
    """Walk filesystem, return per-line matches."""

    scope = envelope.scope
    for path in _iter_source_files(repo_root, scope):
        if not _within_time_window(path, scope):
            continue
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        lines = text.splitlines(keepends=True)
        rel_path = path.relative_to(repo_root).as_posix()
        for index, line in enumerate(lines):
            if pattern is not None:
                if not pattern.search(line):
                    continue
            elif literal is not None:
                if literal not in line:
                    continue
            else:
                continue
            if _exclude_term_hit(line, scope.exclude_terms):
                continue
            yield _shape_payload(
                path=path,
                rel_path=rel_path,
                line_no=index + 1,
                line_text=line,
                lines=lines,
                shape=envelope.shape,
                context_lines=envelope.context_lines,
            )


def _strip_regex_delimiters(query: str) -> str:
    if len(query) >= 2 and query.startswith("/") and query.endswith("/"):
        return query[1:-1]
    return query


def _strip_quote_delimiters(query: str) -> str:
    if len(query) >= 2 and query[0] == query[-1] and query[0] in ("'", '"'):
        return query[1:-1]
    return query


def _index_freshness(indexer: Any) -> dict[str, Any]:
    snapshot: dict[str, Any] = {}
    try:
        stats = indexer.stats()
    except Exception as exc:
        return {
            "status": "degraded",
            "error": f"{type(exc).__name__}: {exc}",
        }
    snapshot["total_indexed"] = stats.get("total_indexed")
    snapshot["by_kind"] = stats.get("by_kind")
    state = stats.get("observability_state") or "complete"
    snapshot["status"] = state
    last_indexed = getattr(indexer, "last_indexed_iso", None)
    if callable(last_indexed):
        try:
            snapshot["last_indexed_iso"] = last_indexed()
        except Exception:
            pass
    elif isinstance(last_indexed, str):
        snapshot["last_indexed_iso"] = last_indexed
    return snapshot


def maybe_refresh_index(
    indexer: Any,
    *,
    stale_threshold: int = 5,
    sample_limit: int = 50,
    stall_budget_seconds: float = 30.0,
) -> dict[str, Any]:
    """Lazy reindex when on-disk drift exceeds a threshold.

    Returns a small report dict so callers can surface what happened in
    the response (``triggered``, ``stale_count``, ``elapsed_seconds``,
    ``status``). Best-effort: on any failure, returns ``status='skipped'``
    with the failure recorded.
    """

    report: dict[str, Any] = {"triggered": False, "status": "fresh"}
    stale_check = getattr(indexer, "stale_check", None)
    index_paths = getattr(indexer, "index_paths", None)
    if not callable(stale_check) or not callable(index_paths):
        report["status"] = "unsupported"
        return report
    try:
        check = stale_check(sample_limit=sample_limit)
    except Exception as exc:
        report["status"] = "skipped"
        report["error"] = f"{type(exc).__name__}: {exc}"
        return report
    stale_count = int(check.get("stale_count") or 0)
    missing_count = int(check.get("missing_count") or 0)
    report["stale_count"] = stale_count
    report["missing_count"] = missing_count
    if stale_count + missing_count < stale_threshold:
        return report
    stale_paths = list(check.get("stale_paths") or ())
    missing_paths = list(check.get("missing_paths") or ())
    affected = stale_paths + missing_paths
    subdirs = sorted({path.split("/", 1)[0] for path in affected if path}) or None
    try:
        result = index_paths(subdirs, stall_budget_seconds=stall_budget_seconds)
    except Exception as exc:
        report["status"] = "error"
        report["error"] = f"{type(exc).__name__}: {exc}"
        return report
    report["triggered"] = True
    report["status"] = "refreshed"
    if isinstance(result, dict):
        report["indexed"] = result.get("indexed")
        report["elapsed_seconds"] = result.get("elapsed_seconds")
    return report


def _semantic_results(
    *,
    indexer: Any,
    envelope: SearchEnvelope,
    repo_root: Path,
) -> list[dict[str, Any]]:
    scope = envelope.scope
    raw = indexer.search(
        query=envelope.query,
        limit=max(envelope.limit * 2, envelope.limit),
        kind=scope.entity_kind,
        threshold=0.3,
    )
    rows: list[dict[str, Any]] = []
    for r in raw:
        module_path = strip_workflow_prefix(r.get("module_path", ""))
        if not _path_in_scope(module_path, scope):
            continue
        full_path = repo_root / module_path
        if not _within_time_window(full_path, scope):
            continue
        line_text = (r.get("signature") or r.get("name") or "").strip()
        if _exclude_term_hit(
            f"{r.get('summary', '')} {r.get('docstring_preview', '')} {line_text}",
            scope.exclude_terms,
        ):
            continue
        # Use cosine similarity as the cross-source comparable score
        # (0-1 range, same as knowledge/decisions). fused_score is RRF
        # internal — useful for explain but not for cross-source ranking.
        cosine = float(r.get("cosine_similarity") or 0.0)
        fused = float(r.get("fused_score") or 0.0)
        row: dict[str, Any] = {
            "source": SOURCE_CODE,
            "path": module_path,
            "name": r.get("name"),
            "kind": r.get("kind"),
            "match_text": line_text,
            "score": cosine,
            "found_via": "semantic",
            "_explain": {
                "cosine_similarity": cosine,
                "fused_score": fused,
            },
        }
        signature = r.get("signature")
        if signature:
            row["signature"] = signature
        doc = (r.get("docstring_preview") or "").strip()
        if doc:
            row["description"] = doc[:200]
        elif r.get("summary"):
            row["description"] = str(r["summary"])[:200]
        if envelope.shape == SHAPE_FULL and full_path.exists():
            try:
                row["full"] = full_path.read_text(encoding="utf-8", errors="replace")
            except OSError as exc:
                row["full_error"] = str(exc)
        rows.append(row)
        if len(rows) >= envelope.limit:
            break
    return rows


def _literal_results(
    *,
    repo_root: Path,
    envelope: SearchEnvelope,
) -> list[dict[str, Any]]:
    mode = resolve_mode(envelope)
    pattern: re.Pattern[str] | None = None
    literal: str | None = None
    if mode == MODE_REGEX:
        try:
            pattern = re.compile(_strip_regex_delimiters(envelope.query))
        except re.error as exc:
            raise CodeSourceError(f"invalid regex: {exc}") from exc
    else:
        literal = _strip_quote_delimiters(envelope.query)

    rows: list[dict[str, Any]] = []
    for hit in _scan_literal(
        repo_root=repo_root,
        envelope=envelope,
        pattern=pattern,
        literal=literal,
    ):
        rows.append(
            {
                "source": SOURCE_CODE,
                "score": 1.0,
                "found_via": mode,
                "_explain": {
                    "mode": mode,
                    "matched_text": hit.get("match_text", "")[:200],
                    "literal": literal,
                    "regex": pattern.pattern if pattern is not None else None,
                },
                **hit,
            }
        )
        if len(rows) >= envelope.limit:
            break
    return rows


def search_code(
    *,
    envelope: SearchEnvelope,
    indexer: Any,
    repo_root: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Main entry point: returns (results, freshness_snapshot)."""

    mode = resolve_mode(envelope)
    if mode == MODE_SEMANTIC:
        results = _semantic_results(
            indexer=indexer, envelope=envelope, repo_root=repo_root
        )
    elif mode in (MODE_EXACT, MODE_REGEX):
        results = _literal_results(repo_root=repo_root, envelope=envelope)
    else:  # pragma: no cover - resolve_mode guarantees a concrete mode
        raise CodeSourceError(f"unsupported mode: {mode}")
    freshness = _index_freshness(indexer)
    return results, freshness


__all__ = ["CodeSourceError", "maybe_refresh_index", "search_code"]
