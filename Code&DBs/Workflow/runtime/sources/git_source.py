"""Git history source plugin.

Shells out to ``git log`` and ``git diff`` to retire the bash fallbacks
agents rely on for commit history, blame, and diff inspection. Filters:

- ``query`` matched against commit message + (optionally) diff body.
- ``scope.since_iso`` / ``scope.until_iso`` map to ``--since`` / ``--until``.
- ``scope.paths`` map to git pathspecs.
- ``scope.extras.author`` maps to ``--author``.
- ``scope.extras.diff`` (bool) toggles whether to include diff bodies.
- ``scope.extras.action`` switches between ``log`` (default), ``blame``,
  and ``diff`` modes.
"""
from __future__ import annotations

import re
import subprocess
from pathlib import Path
from typing import Any, Sequence

from surfaces.mcp.tools._search_envelope import (
    SHAPE_FULL,
    SOURCE_GIT,
    SearchEnvelope,
    resolve_mode,
)


_GIT_BINARY = "git"
_LOG_FIELD_SEP = "\x1f"
_LOG_RECORD_SEP = "\x1e"


class GitSourceError(RuntimeError):
    """Raised when the git source cannot complete a query."""


def _run_git(args: Sequence[str], *, cwd: Path) -> tuple[str, str, int]:
    try:
        proc = subprocess.run(
            [_GIT_BINARY, *args],
            cwd=str(cwd),
            capture_output=True,
            text=True,
            check=False,
            timeout=30,
        )
    except FileNotFoundError as exc:
        raise GitSourceError("git binary not available") from exc
    except subprocess.TimeoutExpired as exc:
        raise GitSourceError("git command timed out") from exc
    return proc.stdout, proc.stderr, proc.returncode


def _build_log_args(
    *,
    envelope: SearchEnvelope,
    include_diff: bool,
) -> list[str]:
    fmt = _LOG_FIELD_SEP.join(["%H", "%an", "%aI", "%s", "%b"]) + _LOG_RECORD_SEP
    args = ["log", f"--pretty=format:{fmt}", f"--max-count={envelope.limit * 4}"]
    if envelope.scope.since_iso:
        args.append(f"--since={envelope.scope.since_iso}")
    if envelope.scope.until_iso:
        args.append(f"--until={envelope.scope.until_iso}")
    extras = envelope.scope.extras or {}
    author = extras.get("author")
    if author:
        args.append(f"--author={author}")
    if extras.get("all"):
        args.append("--all")
    if include_diff:
        args.append("-p")
    if envelope.scope.paths:
        args.append("--")
        args.extend(envelope.scope.paths)
    return args


def _query_matches(query: str, mode: str, message: str) -> bool:
    if not query:
        return True
    if mode == "regex":
        try:
            pattern = re.compile(query.strip("/"))
        except re.error as exc:
            raise GitSourceError(f"invalid regex: {exc}") from exc
        return bool(pattern.search(message))
    return query.lower() in message.lower()


def _exclude_term_hit(text: str, exclude_terms) -> bool:
    if not exclude_terms:
        return False
    haystack = text.lower()
    return any(term.lower() in haystack for term in exclude_terms)


def _parse_log_records(stdout: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for raw in stdout.split(_LOG_RECORD_SEP):
        record = raw.strip()
        if not record:
            continue
        parts = record.split(_LOG_FIELD_SEP, 4)
        if len(parts) < 4:
            continue
        commit_hash, author, date_iso, subject = parts[0], parts[1], parts[2], parts[3]
        body = parts[4] if len(parts) > 4 else ""
        rows.append(
            {
                "commit_hash": commit_hash.strip(),
                "author": author.strip(),
                "committed_iso": date_iso.strip(),
                "subject": subject.strip(),
                "body": body.strip(),
            }
        )
    return rows


def _log_results(
    *,
    repo_root: Path,
    envelope: SearchEnvelope,
) -> list[dict[str, Any]]:
    extras = envelope.scope.extras or {}
    include_diff = bool(extras.get("diff", False))
    args = _build_log_args(envelope=envelope, include_diff=include_diff)

    stdout, stderr, rc = _run_git(args, cwd=repo_root)
    if rc != 0:
        raise GitSourceError(stderr.strip() or f"git exit {rc}")

    if include_diff:
        # When diff bodies are included, git emits records that include
        # the diff content in the body field. Splitting on the record
        # separator is still valid; the diff text is captured as part
        # of body.
        pass

    records = _parse_log_records(stdout)
    mode = resolve_mode(envelope)
    rows: list[dict[str, Any]] = []
    for rec in records:
        haystack = f"{rec['subject']} {rec['body']}"
        try:
            if not _query_matches(envelope.query, mode, haystack):
                continue
        except GitSourceError:
            raise
        if _exclude_term_hit(haystack, envelope.scope.exclude_terms):
            continue
        rows.append(
            {
                "source": SOURCE_GIT,
                "name": rec["subject"],
                "commit_hash": rec["commit_hash"],
                "author": rec["author"],
                "committed_iso": rec["committed_iso"],
                "match_text": rec["subject"],
                "context": rec["body"][:800] if envelope.shape != SHAPE_FULL else rec["body"],
                "score": 1.0,
                "found_via": "git_log",
            }
        )
        if len(rows) >= envelope.limit:
            break
    return rows


def _diff_results(
    *,
    repo_root: Path,
    envelope: SearchEnvelope,
) -> list[dict[str, Any]]:
    extras = envelope.scope.extras or {}
    rev_range = extras.get("rev") or "HEAD~1..HEAD"
    args = ["diff", str(rev_range)]
    if envelope.scope.paths:
        args.append("--")
        args.extend(envelope.scope.paths)
    stdout, stderr, rc = _run_git(args, cwd=repo_root)
    if rc != 0:
        raise GitSourceError(stderr.strip() or f"git diff exit {rc}")
    return [
        {
            "source": SOURCE_GIT,
            "name": f"diff {rev_range}",
            "rev_range": rev_range,
            "match_text": stdout[:400],
            "full": stdout,
            "score": 1.0,
            "found_via": "git_diff",
        }
    ]


def _blame_results(
    *,
    repo_root: Path,
    envelope: SearchEnvelope,
) -> list[dict[str, Any]]:
    extras = envelope.scope.extras or {}
    file_path = extras.get("file") or (envelope.scope.paths[0] if envelope.scope.paths else None)
    if not file_path:
        raise GitSourceError("blame requires scope.extras.file or scope.paths")
    args = ["blame", "--porcelain", str(file_path)]
    stdout, stderr, rc = _run_git(args, cwd=repo_root)
    if rc != 0:
        raise GitSourceError(stderr.strip() or f"git blame exit {rc}")
    rows = []
    for line in stdout.splitlines():
        if not line or line.startswith("\t"):
            continue
        if line.startswith("author "):
            rows.append(line[len("author ") :].strip())
    counter: dict[str, int] = {}
    for author in rows:
        counter[author] = counter.get(author, 0) + 1
    summary_rows = [
        {
            "source": SOURCE_GIT,
            "name": author,
            "match_text": f"{author}: {count} lines",
            "line_count": count,
            "file": str(file_path),
            "score": float(count),
            "found_via": "git_blame",
        }
        for author, count in sorted(counter.items(), key=lambda x: -x[1])
    ]
    return summary_rows[: envelope.limit]


def search_git(
    *,
    envelope: SearchEnvelope,
    repo_root: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Run a git history query."""

    extras = envelope.scope.extras or {}
    action = str(extras.get("action") or "log").strip().lower()
    try:
        if action == "log":
            results = _log_results(repo_root=repo_root, envelope=envelope)
        elif action == "diff":
            results = _diff_results(repo_root=repo_root, envelope=envelope)
        elif action == "blame":
            results = _blame_results(repo_root=repo_root, envelope=envelope)
        else:
            raise GitSourceError(f"unsupported git action: {action}")
    except GitSourceError as exc:
        return [], {"status": "error", "error": str(exc)}

    return results, {"status": "complete", "action": action}


__all__ = ["GitSourceError", "search_git"]
