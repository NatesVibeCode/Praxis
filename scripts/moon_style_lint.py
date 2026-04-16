#!/usr/bin/env python3
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
MOON_ROOT = ROOT / "Code&DBs" / "Workflow" / "surfaces" / "app" / "src" / "moon"
TOKENS_FILE = MOON_ROOT / "moon-tokens.css"
STYLE_FILES = [
    MOON_ROOT / "moon-build.css",
    *sorted((MOON_ROOT / "style").glob("*.css")),
]

HEX_COLOR = re.compile(r"#[0-9a-fA-F]{3,8}\b")
RGB_COLOR = re.compile(r"\b(?:rgba?|hsla?)\s*\([^)]*\)")
IMPORTANT = re.compile(r"!important\b")
VAR_FALLBACK = re.compile(r"var\([^)]*,[^)]*\)")


RULES = {
    "hex-color": {
        "pattern": HEX_COLOR,
        "message": "direct hex color literals are forbidden outside moon-tokens.css",
    },
    "rgb-color": {
        "pattern": RGB_COLOR,
        "message": "rgb/rgba/hsla color functions are forbidden outside moon-tokens.css",
    },
    "important": {
        "pattern": IMPORTANT,
        "message": "!important is forbidden outside moon-tokens.css",
    },
    "css-var-fallback": {
        "pattern": VAR_FALLBACK,
        "message": "legacy css var fallbacks are forbidden outside moon-tokens.css",
    },
}


def _strip_comment(line: str) -> str:
    comment_start = line.find("/*")
    if comment_start < 0:
        return line
    return line[:comment_start]


def _scan_line(line: str) -> list[str]:
    cleaned = _strip_comment(line)
    hits = []
    for rule_name, rule in RULES.items():
        if rule["pattern"].search(cleaned):
            hits.append(rule_name)
    return hits


def _scan_file(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    violations: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), 1):
        for rule_name in _scan_line(line):
            pattern = RULES[rule_name]["pattern"]
            match = pattern.search(_strip_comment(line))
            if not match:
                continue
            if path == TOKENS_FILE:
                continue
            violations.append(
                {
                    "file": str(path),
                    "line": line_number,
                    "rule": rule_name,
                    "message": RULES[rule_name]["message"],
                    "snippet": match.group(0),
                    "context": line.strip(),
                }
            )
    return violations


def main() -> int:
    files = [path for path in STYLE_FILES if path.exists()]
    violations: list[dict[str, Any]] = []
    for path in files:
        violations.extend(_scan_file(path))

    by_rule = {}
    for violation in violations:
        by_rule.setdefault(violation["rule"], 0)
        by_rule[violation["rule"]] += 1

    summary = {
        "moon_root": str(MOON_ROOT),
        "scanned_files": [str(path) for path in files],
        "violation_count": len(violations),
        "violations_by_rule": by_rule,
    }

    if violations:
        print(
            json.dumps(
                {
                    "ok": False,
                    "results": {"summary": summary, "violations": violations},
                    "errors": [
                        f"{len(violations)} token contract violation(s)",
                        "moon-build style files must be token-first",
                    ],
                    "warnings": [],
                },
                indent=2,
            )
        )
        return 1

    print(
        json.dumps(
            {
                "ok": True,
                "results": {"summary": summary, "violations": violations},
                "errors": [],
                "warnings": [],
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
