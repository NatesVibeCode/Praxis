#!/usr/bin/env python3
"""Sweep hardcoded loopback praxis DSNs in queue JSON toward repo env authority.

Run from repo: python3 Code&DBs/Workflow/devtools/sweep_loopback_queue_dsns.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

PRAXIS = Path(__file__).resolve().parents[3]  # .../Code&DBs/Workflow/devtools/this file -> Praxis root

OLD = "postgresql://localhost:5432/praxis"
PSQL_VIA_ENV = r'psql \"$WORKFLOW_DATABASE_URL\"'
PREAMBLE = ". ./scripts/_workflow_env.sh && workflow_load_repo_env && "

# As stored in cascade JSON strings: backslash-quote DSN, backslash-n, cd, backslash-n
OLD_EXPORT_CD = (
    "export WORKFLOW_DATABASE_URL="
    + chr(92)
    + chr(34)
    + "postgresql://localhost:5432/praxis"
    + chr(92)
    + chr(34)
    + chr(92)
    + "n"
    + "cd /Users/nate/Praxis"
    + chr(92)
    + "n"
)
EXPORT_CD_REPLACE = (
    ". ./scripts/_workflow_env.sh && workflow_load_repo_env"
    + chr(92)
    + "n"
)

CD_THEN_INLINE = re.compile(
    r"(cd /[^ ]+ &&) WORKFLOW_DATABASE_URL=postgres(?:ql)?://localhost:5432/praxis "
)

# Remaining inline env prefix before a command (e.g. prompt backticks on own line)
INLINE_ENV = re.compile(r"WORKFLOW_DATABASE_URL=postgres(?:ql)?://localhost:5432/praxis ")

NATIVE_SMOKE_URL = "postgresql://nate@127.0.0.1:5432/praxis"


def _add_bash_lc_preamble(t: str) -> str:
    if "_workflow_env.sh" in t:
        return t
    if "bash -lc \\\"" not in t:
        return t
    if PSQL_VIA_ENV not in t and f'psql \\"$WORKFLOW_DATABASE_URL\\"' not in t:
        return t
    return t.replace("bash -lc \\\"", "bash -lc \\\"" + PREAMBLE, 1)


def transform(t: str, *, path: Path) -> str:
    if path.name == "PRAXIS_NATIVE_SELF_HOSTED_SMOKE.queue.json":
        return t.replace("postgresql://nate@localhost:5432/praxis", NATIVE_SMOKE_URL)

    t = t.replace(f"psql {OLD}", PSQL_VIA_ENV)
    t = t.replace(OLD_EXPORT_CD, EXPORT_CD_REPLACE)
    t = CD_THEN_INLINE.sub(r"\1 " + PREAMBLE, t)
    t = INLINE_ENV.sub(PREAMBLE, t)
    t = _add_bash_lc_preamble(t)
    return t


def main() -> int:
    roots = [
        PRAXIS / "config" / "cascade" / "specs",
        PRAXIS / "artifacts" / "workflow",
    ]
    spec_br = (
        PRAXIS
        / "Code&DBs"
        / "Workflow"
        / "artifacts"
        / "workflow"
        / "bug_resolution_program"
        / "bug_resolution_program_kickoff_20260423.json"
    )
    for base in roots:
        if not base.is_dir():
            print("skip", base, file=sys.stderr)
            continue
        for path in sorted(base.rglob("*.json")):
            if "bug_resolution_program_kickoff" in str(path):
                continue
            raw = path.read_text(encoding="utf-8")
            if "localhost:5432" not in raw and "postgresql://localhost" not in raw and "nate@localhost" not in raw:
                continue
            new = transform(raw, path=path)
            if new != raw:
                path.write_text(new, encoding="utf-8")
                print("updated", path.relative_to(PRAXIS))

    if spec_br.is_file():
        raw = spec_br.read_text(encoding="utf-8")
        new = raw.replace(
            "AGENTS.md fallback psql postgresql://localhost:5432/praxis returned",
            "AGENTS.md fallback psql (WORKFLOW_DATABASE_URL after _workflow_env) returned",
        )
        if new != raw:
            spec_br.write_text(new, encoding="utf-8")
            print("updated", spec_br.relative_to(PRAXIS))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
