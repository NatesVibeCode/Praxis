#!/usr/bin/env python3
"""check-migration-safety — reject migrations that try to disable enforcement.

Per /praxis-debate fork round 3: the JIT-surfacing layer (Cursor rules,
PreToolUse hooks, gateway-side `_standing_orders_surfaced`) is *advisory*.
Hard enforcement lives at the data layer. This validator is one of those
data-layer teeth — it inspects every migration in
`Code&DBs/Databases/migrations/workflow/` for patterns that would let a
bad actor (or a confused agent) silently disable policy enforcement.

Banned patterns (case-insensitive; comments stripped before matching):
  - SET session_replication_role = replica          (disables triggers
                                                     globally for the
                                                     session — used by
                                                     replication tooling
                                                     but a giant footgun
                                                     in app migrations)
  - ALTER TABLE ... DISABLE TRIGGER ...             (per-table trigger
                                                     bypass)
  - ALTER TABLE ... DISABLE ROW LEVEL SECURITY
  - DROP TRIGGER <policy_*>                          (drops policy-
                                                     enforcement triggers
                                                     by naming convention)
  - DROP POLICY <policy_*>
  - DELETE FROM operator_decisions WHERE …          (decisions are
                                                     supersedable, not
                                                     deletable; flip
                                                     `effective_to` instead)
  - TRUNCATE operator_decisions
  - TRUNCATE authority_operation_receipts            (receipts are the
                                                     audit trail; never
                                                     truncated)
  - TRUNCATE authority_events

Allowlist: a migration can document an exception by including the
sentinel comment `-- safety-bypass: <reason>` on the same line. The
validator records the bypass and lets it through. CI surfaces the bypass
count so reviewers see them.

Usage:
  scripts/check-migration-safety.py                                  # all migrations
  scripts/check-migration-safety.py path/to/0123_one.sql ...         # specific files
  scripts/check-migration-safety.py --json                            # machine-readable

Exit codes:
  0  — all migrations clean
  1  — one or more migrations had unbypassed bans
  2  — usage / IO error
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_MIGRATIONS_DIR = REPO_ROOT / "Code&DBs" / "Databases" / "migrations" / "workflow"

# Each entry: (rule_id, regex, human_message). All matches are
# case-insensitive. The regex sees a *normalized* line: comments
# stripped past the bypass sentinel, leading/trailing whitespace
# collapsed.
_BANNED_PATTERNS: tuple[tuple[str, re.Pattern[str], str], ...] = (
    (
        "session_replication_role_replica",
        re.compile(r"\bset\b\s+(?:local\s+|session\s+)?session_replication_role\s*(?:=|to)\s*'?replica'?", re.IGNORECASE),
        "SET session_replication_role = replica disables ALL triggers for the session. Replication tooling territory; never appropriate inside an application migration.",
    ),
    (
        "alter_table_disable_trigger",
        re.compile(r"\balter\s+table\b.*\bdisable\s+trigger\b", re.IGNORECASE | re.DOTALL),
        "ALTER TABLE ... DISABLE TRIGGER bypasses per-table enforcement. If a trigger is wrong, drop and recreate it through the catalog, don't silence it.",
    ),
    (
        "alter_table_disable_rls",
        re.compile(r"\balter\s+table\b.*\bdisable\s+row\s+level\s+security\b", re.IGNORECASE | re.DOTALL),
        "ALTER TABLE ... DISABLE ROW LEVEL SECURITY removes RLS. Never appropriate for authority tables.",
    ),
    (
        "drop_trigger_policy",
        re.compile(r"\bdrop\s+trigger\b.*\bpolicy[_a-zA-Z0-9]*\b", re.IGNORECASE | re.DOTALL),
        "DROP TRIGGER on a policy_* trigger removes enforcement. File a superseding operator_decisions row first; the projection will retire the trigger through the catalog.",
    ),
    (
        "drop_policy",
        re.compile(r"\bdrop\s+policy\b\s+(?:if\s+exists\s+)?policy[_a-zA-Z0-9]*", re.IGNORECASE),
        "DROP POLICY on a policy_* row removes enforcement. Same: supersede via operator_decisions, project through the catalog.",
    ),
    (
        "delete_from_operator_decisions",
        re.compile(r"\bdelete\s+from\s+operator_decisions\b", re.IGNORECASE),
        "operator_decisions rows are not deletable — set effective_to instead. The supersession history is the audit trail.",
    ),
    (
        "truncate_operator_decisions",
        re.compile(r"\btruncate\s+(?:table\s+)?operator_decisions\b", re.IGNORECASE),
        "operator_decisions rows are not deletable — TRUNCATE wipes the standing-order ledger.",
    ),
    (
        "truncate_authority_receipts",
        re.compile(r"\btruncate\s+(?:table\s+)?authority_operation_receipts\b", re.IGNORECASE),
        "authority_operation_receipts is the gateway audit trail. Never truncate.",
    ),
    (
        "truncate_authority_events",
        re.compile(r"\btruncate\s+(?:table\s+)?authority_events\b", re.IGNORECASE),
        "authority_events is the command-event ledger. Never truncate.",
    ),
)

_BYPASS_SENTINEL = re.compile(r"--\s*safety-bypass\s*:\s*(.+?)$", re.IGNORECASE)
_INLINE_COMMENT = re.compile(r"--.*$")


def _try_relative(path: Path) -> Path:
    """Render `path` relative to REPO_ROOT when possible, otherwise as-is.

    Synthetic test paths under /tmp/ and absolute paths from outside the
    repo show as-is so test output and CLI ergonomics don't break.
    """
    try:
        return path.relative_to(REPO_ROOT)
    except ValueError:
        return path


@dataclass
class Finding:
    path: Path
    line_number: int
    rule_id: str
    message: str
    line: str
    bypass_reason: str | None = None

    def to_json(self) -> dict[str, object]:
        return {
            "path": str(_try_relative(self.path)),
            "line": self.line_number,
            "rule_id": self.rule_id,
            "message": self.message,
            "matched_line": self.line.strip()[:240],
            "bypass_reason": self.bypass_reason,
        }


@dataclass
class Report:
    findings: list[Finding] = field(default_factory=list)
    bypassed: list[Finding] = field(default_factory=list)
    files_scanned: int = 0

    @property
    def has_blocking(self) -> bool:
        return bool(self.findings)


def _scan_text(path: Path, text: str) -> tuple[list[Finding], list[Finding]]:
    blocking: list[Finding] = []
    bypassed: list[Finding] = []
    lines = text.splitlines()
    for idx, raw in enumerate(lines, start=1):
        bypass_match = _BYPASS_SENTINEL.search(raw)
        bypass_reason = bypass_match.group(1).strip() if bypass_match else None
        # Strip the inline comment past the sentinel to avoid the
        # bypass note tripping the regex itself, but keep the SQL
        # statement intact for matching.
        normalized = _INLINE_COMMENT.sub("", raw).strip()
        if not normalized:
            continue
        for rule_id, pattern, message in _BANNED_PATTERNS:
            if pattern.search(normalized):
                finding = Finding(
                    path=path,
                    line_number=idx,
                    rule_id=rule_id,
                    message=message,
                    line=raw,
                    bypass_reason=bypass_reason,
                )
                if bypass_reason:
                    bypassed.append(finding)
                else:
                    blocking.append(finding)
                # One finding per line is enough; a line with two
                # banned patterns is exotic.
                break
    return blocking, bypassed


def scan_migrations(paths: Iterable[Path]) -> Report:
    report = Report()
    for path in paths:
        try:
            text = path.read_text(encoding="utf-8")
        except OSError as exc:
            print(f"check-migration-safety: cannot read {path}: {exc}", file=sys.stderr)
            continue
        report.files_scanned += 1
        blocking, bypassed = _scan_text(path, text)
        report.findings.extend(blocking)
        report.bypassed.extend(bypassed)
    return report


def _resolve_targets(arg_paths: list[str]) -> list[Path]:
    if not arg_paths:
        if not DEFAULT_MIGRATIONS_DIR.exists():
            return []
        return sorted(DEFAULT_MIGRATIONS_DIR.glob("*.sql"))
    resolved: list[Path] = []
    for arg in arg_paths:
        path = Path(arg)
        if not path.is_absolute():
            path = (REPO_ROOT / path).resolve()
        if path.is_dir():
            resolved.extend(sorted(path.rglob("*.sql")))
        elif path.exists():
            resolved.append(path)
        else:
            print(f"check-migration-safety: not found: {arg}", file=sys.stderr)
            sys.exit(2)
    return resolved


def _render_text(report: Report) -> str:
    lines: list[str] = []
    lines.append(f"Migration safety scan: {report.files_scanned} file(s).")
    if report.bypassed:
        lines.append(f"Bypassed (with -- safety-bypass: <reason>): {len(report.bypassed)}")
        for f in report.bypassed:
            rel = _try_relative(f.path)
            lines.append(f"  • {rel}:{f.line_number} [{f.rule_id}] (reason: {f.bypass_reason})")
    if report.findings:
        lines.append("")
        lines.append(f"BLOCKING ({len(report.findings)}):")
        for f in report.findings:
            rel = _try_relative(f.path)
            lines.append(f"  • {rel}:{f.line_number} [{f.rule_id}]")
            lines.append(f"      {f.message}")
            lines.append(f"      → {f.line.strip()[:240]}")
        lines.append("")
        lines.append(
            "If a banned pattern is genuinely required, add a same-line "
            "comment: -- safety-bypass: <durable reason>. CI surfaces the "
            "bypass count for review."
        )
    else:
        lines.append("All migrations clean.")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("paths", nargs="*")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args(argv)

    targets = _resolve_targets(args.paths)
    if not targets:
        print("check-migration-safety: no migrations to scan", file=sys.stderr)
        return 0

    report = scan_migrations(targets)

    if args.json:
        payload = {
            "files_scanned": report.files_scanned,
            "blocking": [f.to_json() for f in report.findings],
            "bypassed": [f.to_json() for f in report.bypassed],
        }
        print(json.dumps(payload, indent=2))
    else:
        print(_render_text(report))

    return 1 if report.has_blocking else 0


if __name__ == "__main__":
    sys.exit(main())
