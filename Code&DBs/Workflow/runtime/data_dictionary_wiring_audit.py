"""Wiring + hard-path audit over the Praxis codebase + decision graph.

Two categories of finding, each shipped as a separate report:

**Hard-path findings** — things in source, docs, skills, queue specs, and
surface metadata that will break when Praxis moves off the developer's laptop:

  * `absolute_user_path`   — `/Users/nate/...` or `/Users/...` outside
    of configured env vars
  * `hardcoded_localhost`  — `localhost` / `127.0.0.1` literal in code
  * `hardcoded_port`       — numeric ports like `:5432`, `:8420`, `:6379`

Each hard-path finding is classified so cleanup can separate live authority
from historical evidence:

  * `live_authority_bug`
  * `generated_derived_artifact`
  * `historical_receipt_evidence`
  * `test_fixture`

**Unwired findings** — things that exist but nothing references:

  * `unreferenced_decision` — operator_decisions with no
    semantic_assertion claiming to implement them, no recent bug
    citing them, and no receipt referencing them
  * `code_orphan_table`    — tables in data_dictionary_objects whose
    name doesn't appear anywhere in production source (excluding
    tests and migrations)

No automatic bug filing: findings are a report the operator reviews.
Avoids noise while still being actionable.
"""
from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable


# ---------------------------------------------------------------------------
# Finding shape
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class WiringFinding:
    category: str            # hard_path | unwired
    kind: str                # absolute_user_path | hardcoded_localhost | ...
    subject: str             # file path or decision key or object_kind
    evidence: str            # a short line of context / matched snippet
    details: dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> dict[str, Any]:
        return {
            "category": self.category,
            "kind": self.kind,
            "subject": self.subject,
            "evidence": self.evidence,
            "details": dict(self.details),
        }


# ---------------------------------------------------------------------------
# Source-tree scan helpers
# ---------------------------------------------------------------------------

def _resolve_default_root() -> Path:
    """Find the Code&DBs/Workflow root in either local or container contexts.

    The audit runs both on developer hosts and inside API containers.
    Walking up from `__file__` finds the right root portably without
    preserving host- or container-specific path literals as authority.
    """
    here = Path(__file__).resolve()
    # __file__ is at {root}/runtime/data_dictionary_wiring_audit.py — walk
    # up until we land at the ancestor directory named "Workflow".
    for ancestor in here.parents:
        if ancestor.name == "Workflow":
            return ancestor
    # Fallback: two levels up from the runtime module.
    return here.parent.parent


_DEFAULT_ROOT = _resolve_default_root()
_DEFAULT_REPO_ROOT = _DEFAULT_ROOT.parents[1]

_EXCLUDED_DIRS = frozenset({
    "__pycache__", ".git", ".pytest_cache", ".mypy_cache", "node_modules",
    "dist", "build", "venv", ".venv",
})

_AUDIT_FILE_SUFFIXES = frozenset({
    ".css",
    ".js",
    ".json",
    ".jsx",
    ".md",
    ".py",
    ".sh",
    ".toml",
    ".ts",
    ".tsx",
    ".yaml",
    ".yml",
})
_MAX_AUDIT_FILE_BYTES = 1_000_000
_DEFAULT_AUDIT_ROOTS = (
    ".claude",
    "AGENTS.md",
    "Code&DBs/Workflow/adapters",
    "Code&DBs/Workflow/bin",
    "Code&DBs/Workflow/memory",
    "Code&DBs/Workflow/registry",
    "Code&DBs/Workflow/runtime",
    "Code&DBs/Workflow/storage",
    "Code&DBs/Workflow/surfaces/cli",
    "Code&DBs/Workflow/surfaces/mcp",
    "GEMINI.md",
    "Skills",
    "config/cascade/specs",
    "config/workspace_layout.json",
    "docs",
    "scripts",
)

_EXCLUDED_PATH_SUBSTRINGS = (
    "/tests/",
    "/migrations/",
    "/system_authority/",
    "/_generated_",
    "/generated/",
    # Skip self: the audit module necessarily mentions the patterns it hunts.
    "/data_dictionary_wiring_audit.py",
)
_AUDIT_EXCLUDED_PATH_SUBSTRINGS = (
    # Skip audit definitions: they necessarily mention the patterns they hunt.
    "/audit_primitive_wiring.py",
    "/data_dictionary_wiring_audit.py",
)


def _iter_python_files(root: Path) -> Iterable[Path]:
    """Walk the tree yielding .py files, skipping test/migration/generated."""
    for dirpath, dirnames, filenames in os.walk(root):
        # prune in place
        dirnames[:] = [d for d in dirnames if d not in _EXCLUDED_DIRS]
        for f in filenames:
            if not f.endswith(".py"):
                continue
            p = Path(dirpath) / f
            s = str(p)
            if any(ex in s for ex in _EXCLUDED_PATH_SUBSTRINGS):
                continue
            yield p


def _iter_audit_files(root: Path) -> Iterable[Path]:
    """Walk repo-facing text files that may carry operator path authority."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _EXCLUDED_DIRS]
        for filename in filenames:
            path = Path(dirpath) / filename
            path_text = path.as_posix()
            if any(ex in path_text for ex in _AUDIT_EXCLUDED_PATH_SUBSTRINGS):
                continue
            if path.suffix.lower() not in _AUDIT_FILE_SUFFIXES:
                continue
            try:
                if path.stat().st_size > _MAX_AUDIT_FILE_BYTES:
                    continue
            except OSError:
                continue
            yield path


def _iter_default_audit_files(repo_root: Path) -> Iterable[Path]:
    """Yield live repo surfaces without walking bulk historical trees."""
    yielded: set[Path] = set()
    for rel in _DEFAULT_AUDIT_ROOTS:
        path = repo_root / rel
        if not path.exists():
            continue
        if path.is_file():
            if path.suffix.lower() in _AUDIT_FILE_SUFFIXES:
                yielded.add(path)
                yield path
            continue
        for candidate in _iter_audit_files(path):
            if candidate in yielded:
                continue
            yielded.add(candidate)
            yield candidate


def _repo_relative(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


def _classify_hard_path_subject(rel: str) -> tuple[str, str, str]:
    """Return (classification, surface, recommended_action)."""
    normalized = rel.replace("\\", "/")
    if normalized.startswith("./"):
        normalized = normalized[2:]
    parts = normalized.split("/")
    name = parts[-1] if parts else normalized

    if (
        "/tests/" in f"/{normalized}/"
        or normalized.startswith("tests/")
        or name.startswith("test_")
        or name.endswith("_test.py")
        or name.endswith(".test.ts")
        or name.endswith(".test.tsx")
    ):
        return (
            "test_fixture",
            "test",
            "Keep only if the literal is asserting path-drift behavior; otherwise use a tmp_path fixture.",
        )

    if normalized.startswith("Code&DBs/Workflow/artifacts/workflow/") and normalized.endswith((".json", ".queue.json")):
        return (
            "historical_workflow_packet",
            "historical_artifact",
            "Do not execute directly; inspect as evidence or regenerate from DB-backed workflow authority.",
        )

    if (
        normalized.startswith("artifacts/")
        or normalized.startswith("Code&DBs/Workflow/artifacts/")
        or normalized.startswith("planning/")
    ):
        return (
            "historical_receipt_evidence",
            "historical_artifact",
            "Do not rewrite blindly; preserve as evidence or regenerate from live authority.",
        )

    if (
        "/_generated_" in normalized
        or "/generated/" in normalized
        or normalized == "docs/MCP.md"
        or normalized.startswith("Code&DBs/Workflow/storage/_generated_")
        or normalized.startswith("Code&DBs/Workflow/system_authority/")
    ):
        return (
            "generated_derived_artifact",
            "generated_artifact",
            "Fix the source authority, then regenerate this derived file.",
        )

    if normalized.startswith("Skills/") and normalized.endswith("/SKILL.md"):
        return (
            "live_authority_bug",
            "skill",
            "Replace operator-local paths with registry, env, PATH, or repo-relative authority.",
        )

    if normalized.startswith("config/cascade/specs/") and normalized.endswith((".json", ".queue.json")):
        return (
            "live_authority_bug",
            "queue_spec",
            "Use repo-relative workdir/spec paths or runtime workspace authority.",
        )

    if normalized.startswith("docs/") or name in {"AGENTS.md", "GEMINI.md", "CLAUDE.md"}:
        return (
            "live_authority_bug",
            "doc",
            "Point docs at runtime authority and repo-relative commands, not one checkout.",
        )

    if normalized.startswith("Code&DBs/Workflow/surfaces/mcp/"):
        return (
            "live_authority_bug",
            "mcp_surface",
            "Use examples and schemas that resolve through repo/runtime authority.",
        )

    if normalized.startswith("Code&DBs/Workflow/surfaces/cli/") or normalized.startswith("scripts/"):
        return (
            "live_authority_bug",
            "cli_surface",
            "Resolve through launcher/workspace authority or environment.",
        )

    if normalized.startswith(".claude/") or normalized.startswith(".cursor") or normalized == ".cursorrules":
        return (
            "live_authority_bug",
            "harness_adapter",
            "Keep harness adapters derived from Praxis authority; avoid local checkout paths.",
        )

    return (
        "live_authority_bug",
        "source",
        "Route path authority through runtime.workspace_paths, runtime profile, env, or repo-relative refs.",
    )


def _excerpt(line: str, match: re.Match[str], width: int = 80) -> str:
    """Return a trimmed context snippet around the match."""
    line = line.rstrip("\n")
    start = max(0, match.start() - 20)
    end = min(len(line), match.end() + 40)
    snippet = line[start:end].strip()
    if len(snippet) > width:
        snippet = snippet[:width - 3] + "..."
    return snippet


# ---------------------------------------------------------------------------
# Hard-path audits
# ---------------------------------------------------------------------------

_RE_ABS_USER_PATH = re.compile(
    r"""(?:/Volumes)?/Users/[a-zA-Z0-9_.-]+(?:/[^"'\s)]*)?"""
)
_RE_LOCALHOST    = re.compile(r"""\b(?:localhost|127\.0\.0\.1)\b""")
_RE_PORT_LITERAL = re.compile(r""":(?P<port>5432|6379|8420|8000|9000|3000|5000)\b""")
_PORT_SENTINELS = (":5432", ":6379", ":8420", ":8000", ":9000", ":3000", ":5000")
_PORT_CONTEXT_TOKENS = (
    "localhost",
    "127.0.0.1",
    "http://",
    "https://",
    "postgres" + "://",
    "postgres" + "ql://",
    "redis://",
    "host",
    "listen",
    "port",
    "server",
    "url",
    "dsn",
)


def _line_has_port_context(line: str) -> bool:
    lowered = line.lower()
    return any(token in lowered for token in _PORT_CONTEXT_TOKENS)


def _is_slice_port_false_positive(line: str, match: re.Match[str]) -> bool:
    return match.start() > 0 and line[match.start() - 1] == "["


def audit_hard_paths(root: Path | str | None = None) -> list[WiringFinding]:
    """Scan repo-facing text for non-portable paths / hostnames / ports."""
    rp = Path(root) if root else _DEFAULT_REPO_ROOT
    files = _iter_audit_files(rp) if root else _iter_default_audit_files(rp)
    findings: list[WiringFinding] = []
    for path in files:
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        if (
            "/Users/" not in text
            and "localhost" not in text
            and "127.0.0.1" not in text
            and not any(port in text for port in _PORT_SENTINELS)
        ):
            continue
        lines = text.splitlines()
        rel = _repo_relative(path, rp)
        classification, surface, recommended_action = _classify_hard_path_subject(rel)
        for lineno, line in enumerate(lines, start=1):
            # Skip docstring / comment-heavy lines? Too aggressive. Keep raw.
            if "/Users/" in line:
                for m in _RE_ABS_USER_PATH.finditer(line):
                    findings.append(WiringFinding(
                        category="hard_path",
                        kind="absolute_user_path",
                        subject=f"{rel}:{lineno}",
                        evidence=_excerpt(line, m),
                        details={
                            "match": m.group(0),
                            "classification": classification,
                            "surface": surface,
                            "recommended_action": recommended_action,
                        },
                    ))
            if "localhost" in line or "127.0.0.1" in line:
                for m in _RE_LOCALHOST.finditer(line):
                    findings.append(WiringFinding(
                        category="hard_path",
                        kind="hardcoded_localhost",
                        subject=f"{rel}:{lineno}",
                        evidence=_excerpt(line, m),
                        details={
                            "match": m.group(0),
                            "classification": classification,
                            "surface": surface,
                            "recommended_action": recommended_action,
                        },
                    ))
            if any(port in line for port in _PORT_SENTINELS) and _line_has_port_context(line):
                for m in _RE_PORT_LITERAL.finditer(line):
                    if _is_slice_port_false_positive(line, m):
                        continue
                    findings.append(WiringFinding(
                        category="hard_path",
                        kind="hardcoded_port",
                        subject=f"{rel}:{lineno}",
                        evidence=_excerpt(line, m),
                        details={
                            "port": m.group("port"),
                            "classification": classification,
                            "surface": surface,
                            "recommended_action": recommended_action,
                        },
                    ))
    return findings


# ---------------------------------------------------------------------------
# Unwired-decisions audit
# ---------------------------------------------------------------------------

def audit_unreferenced_decisions(conn: Any) -> list[WiringFinding]:
    """Find operator_decisions that nothing cites.

    An operator_decision is considered 'referenced' if any of:
      * a bug has decision_ref exactly matching decision_key
      * a semantic_assertion is bound to it (bound_decision_id =
        operator_decision_id)
      * a receipt's decision_refs jsonb array contains the decision_key

    Decisions with no citation in any of those three stores are surfaced
    as unwired — they sit in authority but nothing enforces or uses them.
    """
    rows = conn.execute(
        """
        WITH bug_refs AS (
            SELECT DISTINCT decision_ref AS k FROM bugs
            WHERE decision_ref <> ''
        ),
        receipt_refs AS (
            SELECT DISTINCT jsonb_array_elements_text(
                CASE WHEN jsonb_typeof(decision_refs) = 'array'
                     THEN decision_refs
                     ELSE '[]'::jsonb END
            ) AS k FROM receipts
            WHERE started_at > now() - interval '90 days'
        ),
        bound_decision_ids AS (
            SELECT DISTINCT bound_decision_id AS id FROM semantic_assertions
            WHERE bound_decision_id IS NOT NULL
              AND assertion_status = 'active'
        )
        SELECT d.decision_key,
               d.decision_kind,
               d.decision_status,
               d.title,
               d.decided_at
          FROM operator_decisions d
         WHERE d.decision_status IN ('recorded', 'admitted', 'decided',
                                      'binding', 'active', 'standing')
           -- Exclude decision kinds whose semantics don't require binding
           -- to a semantic_assertion. legacy_fallback, dataset_rejection,
           -- dataset_promotion, and native_primary_cutover are all
           -- transactional/runtime records, not policy statements an
           -- assertion should enforce. Leaving them in would flag
           -- hundreds of normal operational records as "unwired".
           AND d.decision_kind NOT IN (
               'legacy_fallback', 'dataset_rejection', 'dataset_promotion',
               'native_primary_cutover', 'query'
           )
           AND NOT EXISTS (SELECT 1 FROM bug_refs       WHERE k = d.decision_key)
           AND NOT EXISTS (SELECT 1 FROM receipt_refs   WHERE k = d.decision_key)
           AND NOT EXISTS (SELECT 1 FROM bound_decision_ids
                           WHERE id = d.operator_decision_id)
         ORDER BY d.decided_at DESC
         LIMIT 200
        """
    ) or []
    out: list[WiringFinding] = []
    for r in rows:
        decided_at = r.get("decided_at")
        out.append(WiringFinding(
            category="unwired",
            kind="unreferenced_decision",
            subject=str(r.get("decision_key") or ""),
            evidence=str(r.get("title") or "")[:120],
            details={
                "decision_kind":   r.get("decision_kind"),
                "decision_status": r.get("decision_status"),
                "decided_at": (
                    decided_at.isoformat() if hasattr(decided_at, "isoformat")
                    else str(decided_at)
                ),
            },
        ))
    return out


# ---------------------------------------------------------------------------
# Code-orphan tables
# ---------------------------------------------------------------------------

def _excluded_orphan_subjects(conn: Any) -> set[str]:
    """Read the audit_exclusions table for code-orphan false positives."""
    try:
        rows = conn.execute(
            """
            SELECT subject FROM audit_exclusions
            WHERE audit_kind = 'wiring'
              AND finding_kind IN ('code_orphan_table', 'code_orphan_view',
                                   'code_orphan_legacy_named', 'code_orphan_other')
            """
        ) or []
        return {str(r["subject"]) for r in rows}
    except Exception:
        return set()


def _object_ref_table_name(value: Any) -> str | None:
    text = str(value or "").strip()
    if not text:
        return None
    for prefix in ("table.public.", "table:", "public."):
        if text.startswith(prefix):
            text = text[len(prefix):]
            break
    if "." in text:
        text = text.rsplit(".", 1)[-1]
    return text.strip() or None


def _db_native_table_references(conn: Any, table_names: list[str]) -> set[str]:
    """Return tables with DB-native proof that Python string search can miss."""
    if not table_names:
        return set()

    referenced: set[str] = set()

    def _add_rows(sql: str, *params: Any) -> None:
        try:
            rows = conn.execute(sql, *params) or []
        except Exception:
            return
        for row in rows:
            if isinstance(row, dict):
                value = row.get("table_name")
            else:
                value = row[0] if row else None
            table_name = _object_ref_table_name(value)
            if table_name:
                referenced.add(table_name)

    _add_rows(
        """
        SELECT DISTINCT rel.relname AS table_name
          FROM pg_class rel
          JOIN pg_namespace ns ON ns.oid = rel.relnamespace
         WHERE ns.nspname = 'public'
           AND rel.relname = ANY($1::text[])
           AND rel.relkind IN ('v', 'm')
        """,
        table_names,
    )
    _add_rows(
        """
        SELECT DISTINCT source.relname AS table_name
          FROM pg_class source
          JOIN pg_namespace ns ON ns.oid = source.relnamespace
         WHERE ns.nspname = 'public'
           AND source.relname = ANY($1::text[])
           AND EXISTS (
                SELECT 1
                  FROM pg_depend dep
                  JOIN pg_rewrite rewrite ON rewrite.oid = dep.objid
                  JOIN pg_class view_rel ON view_rel.oid = rewrite.ev_class
                 WHERE dep.refobjid = source.oid
                   AND view_rel.oid <> source.oid
           )
        """,
        table_names,
    )
    _add_rows(
        """
        SELECT DISTINCT target.relname AS table_name
          FROM pg_class target
          JOIN pg_namespace ns ON ns.oid = target.relnamespace
          JOIN pg_constraint con ON con.confrelid = target.oid
         WHERE ns.nspname = 'public'
           AND target.relname = ANY($1::text[])
           AND con.contype = 'f'
           AND con.conrelid <> target.oid
        """,
        table_names,
    )
    _add_rows(
        """
        SELECT DISTINCT regexp_replace(ref, '^(table\\.public\\.|table:|public\\.)', '') AS table_name
          FROM (
                SELECT source_ref AS ref
                  FROM authority_projection_contracts
                 WHERE enabled IS TRUE AND source_ref_kind = 'table'
                UNION ALL
                SELECT read_model_object_ref AS ref
                  FROM authority_projection_contracts
                 WHERE enabled IS TRUE
               ) refs
         WHERE regexp_replace(ref, '^(table\\.public\\.|table:|public\\.)', '') = ANY($1::text[])
        """,
        table_names,
    )
    return referenced


def audit_code_orphan_tables(
    conn: Any,
    *,
    root: Path | str | None = None,
) -> list[WiringFinding]:
    """Tables whose name doesn't appear in any production .py file.

    Uses a single pass over the source tree building a set of substring
    matches for every known table name, then subtracts from the
    data_dictionary_objects list.

    Honors the `audit_exclusions` table for known-false-positives
    (e.g. SQL views accessed outside Python).
    """
    rp = Path(root) if root else _DEFAULT_ROOT
    rows = conn.execute(
        "SELECT object_kind FROM data_dictionary_objects WHERE object_kind LIKE 'table:%'"
    ) or []
    excluded = _excluded_orphan_subjects(conn)
    table_names = [
        str(r["object_kind"])[len("table:"):] for r in rows
        if str(r["object_kind"]) not in excluded
    ]
    table_names = [t for t in table_names if t]

    # One read per file, substring-test every table name. Cheaper than
    # regex-compiling per table.
    seen: set[str] = set()
    for path in _iter_python_files(rp):
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for t in table_names:
            if t in seen:
                continue
            # Token boundary: avoid false positives on substrings inside
            # longer identifiers (e.g. `bugs` inside `debugs`).
            if re.search(rf"\b{re.escape(t)}\b", text):
                seen.add(t)

    seen.update(_db_native_table_references(conn, table_names))

    orphans = sorted(set(table_names) - seen)
    return [
        WiringFinding(
            category="unwired",
            kind="code_orphan_table",
            subject=f"table:{t}",
            evidence=(
                f"No production .py file mentions '{t}' — "
                "candidate for deletion or re-wiring"
            ),
        )
        for t in orphans
    ]


# ---------------------------------------------------------------------------
# Aggregate entry
# ---------------------------------------------------------------------------

def run_full_audit(
    conn: Any,
    *,
    root: Path | str | None = None,
    include_hard_paths: bool = True,
    include_unwired: bool = True,
) -> dict[str, Any]:
    findings: list[WiringFinding] = []
    if include_hard_paths:
        findings.extend(audit_hard_paths(root))
    if include_unwired:
        findings.extend(audit_unreferenced_decisions(conn))
        findings.extend(audit_code_orphan_tables(conn, root=root))

    by_kind: dict[str, int] = {}
    by_category: dict[str, int] = {}
    by_classification: dict[str, int] = {}
    by_surface: dict[str, int] = {}
    for f in findings:
        by_kind[f.kind] = by_kind.get(f.kind, 0) + 1
        by_category[f.category] = by_category.get(f.category, 0) + 1
        classification = str(f.details.get("classification") or "unclassified")
        surface = str(f.details.get("surface") or "unknown")
        by_classification[classification] = by_classification.get(classification, 0) + 1
        by_surface[surface] = by_surface.get(surface, 0) + 1

    return {
        "total": len(findings),
        "by_category": by_category,
        "by_kind": by_kind,
        "by_classification": by_classification,
        "by_surface": by_surface,
        "actionable_total": by_classification.get("live_authority_bug", 0),
        "findings": [f.to_payload() for f in findings],
    }


def audit_trend(conn: Any, *, limit: int = 50) -> dict[str, Any]:
    """Read the snapshot history so operators can see trend over time.

    Each row is one heartbeat cycle's audit result. Shows whether the
    three counts are trending up (regressing) or down (cleanup in
    progress).
    """
    limit = max(1, min(500, int(limit or 50)))
    rows = conn.execute(
        """
        SELECT snapshot_id::text,
               taken_at,
               triggered_by,
               hard_path_total,
               absolute_user_paths,
               hardcoded_localhost,
               hardcoded_ports,
               unreferenced_decisions,
               code_orphan_tables,
               duration_ms
        FROM data_dictionary_wiring_audit_snapshots
        ORDER BY taken_at DESC
        LIMIT $1
        """,
        limit,
    ) or []
    out = []
    for r in rows:
        taken = r.get("taken_at")
        out.append({
            "snapshot_id": r.get("snapshot_id"),
            "taken_at": (
                taken.isoformat() if hasattr(taken, "isoformat") else str(taken)
            ),
            "triggered_by": r.get("triggered_by"),
            "hard_path_total": int(r.get("hard_path_total") or 0),
            "absolute_user_paths": int(r.get("absolute_user_paths") or 0),
            "hardcoded_localhost": int(r.get("hardcoded_localhost") or 0),
            "hardcoded_ports": int(r.get("hardcoded_ports") or 0),
            "unreferenced_decisions": int(r.get("unreferenced_decisions") or 0),
            "code_orphan_tables": int(r.get("code_orphan_tables") or 0),
            "duration_ms": int(r.get("duration_ms") or 0),
        })

    if len(out) >= 2:
        latest, prior = out[0], out[-1]
        delta = {
            "hard_path_total": latest["hard_path_total"] - prior["hard_path_total"],
            "unreferenced_decisions": latest["unreferenced_decisions"] - prior["unreferenced_decisions"],
            "code_orphan_tables": latest["code_orphan_tables"] - prior["code_orphan_tables"],
        }
    else:
        delta = {}

    return {
        "count": len(out),
        "trend_delta_first_to_last": delta,
        "snapshots": out,
    }


__all__ = [
    "WiringFinding",
    "audit_code_orphan_tables",
    "audit_hard_paths",
    "audit_trend",
    "audit_unreferenced_decisions",
    "run_full_audit",
]
