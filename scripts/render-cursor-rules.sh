#!/usr/bin/env bash
# render-cursor-rules.sh — generate .cursor/rules/*.mdc from
# policy/operator-decision-triggers.json.
#
# Cursor's rule system is its closest equivalent to a PreToolUse hook:
# .mdc files in .cursor/rules/ with frontmatter that auto-attach to the
# agent's context based on `globs:` (file pattern) or `alwaysApply: true`.
# We render one file per trigger so Cursor surfaces the standing order at
# the moment the agent edits a matching file.
#
# Output:
#   .cursor/rules/_standing-orders-jit.mdc      — alwaysApply master rule
#   .cursor/rules/<decision_key>.mdc            — one per trigger with file_glob
#
# Re-run after editing operator-decision-triggers.json.
# CI gate (Packet 1) compares committed output against fresh render.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TRIGGERS="${REPO_ROOT}/policy/operator-decision-triggers.json"
OUT="${REPO_ROOT}/.cursor/rules"

[[ -f "$TRIGGERS" ]] || { echo "missing: $TRIGGERS" >&2; exit 1; }
mkdir -p "$OUT"

# Wipe prior render so deletions in the registry propagate. Keep _standing-
# orders-jit.mdc reset so its content always matches this script's view.
find "$OUT" -maxdepth 1 -type f -name '*.mdc' -delete

PRAXIS_OUT_DIR="$OUT" python3 - "$TRIGGERS" <<'PY'
import json, os, re, sys
from pathlib import Path

triggers_path = sys.argv[1]
out_dir = Path(os.environ["PRAXIS_OUT_DIR"])

with open(triggers_path) as fp:
    registry = json.load(fp)

triggers = registry.get("triggers") or []

def slug(key: str) -> str:
    s = re.sub(r"[^a-zA-Z0-9._-]+", "-", key.strip())
    return s.strip("-")[:120] or "trigger"

# Master alwaysApply rule. Tells Cursor to call the universal shim before
# raw bash. Cursor cannot enforce this, but the rule gives the agent a
# concrete action to take instead of "go look at the registry."
master = """---
description: Praxis standing-order JIT surfacing — always-on
alwaysApply: true
---

# Praxis standing orders — JIT surface

Operator standing orders live in `operator_decisions` (Praxis.db,
`decision_kind=architecture_policy`). The trigger registry at
`policy/operator-decision-triggers.json` projects them into match
conditions.

**For raw shell commands** (anything you'd run via your built-in shell
tool that doesn't go through Praxis MCP): pipe the proposed command
through the universal shim BEFORE executing:

```bash
bin/praxis-policy-check Bash '{"command":"<your command>"}'
```

If the shim prints a `⚠ STANDING ORDER MATCH` block, read it and pivot
if the proposed action would violate it. Empty output = no match,
proceed.

**For Edit/Write to source files**: rules in `.cursor/rules/*.mdc` with
`globs:` frontmatter auto-attach to your context when you edit matching
files. Read them when they appear.

**For Praxis MCP/CLI calls** (`praxis_*` tools): the gateway already
appends `_standing_orders_surfaced` to every tool result. No extra step.

Full rationale for any `decision_key`:
```bash
bin/praxis-agent praxis_operator_decisions --input-json '{"action":"get","decision_key":"<key>"}'
```
"""

(out_dir / "_standing-orders-jit.mdc").write_text(master)

# Per-trigger rules with file_glob → Cursor auto-attach.
rendered = 0
for t in triggers:
    key = t.get("decision_key") or ""
    title = (t.get("title") or "").strip()
    # Registry uses `why` (matches operator_decisions field). Older entries
    # used `rationale`; fall back so neither shape silently produces the
    # "(no rationale captured)" placeholder.
    rationale = (t.get("why") or t.get("rationale") or "").strip()
    matches = t.get("match") or []

    file_globs = []
    for c in matches:
        g = c.get("file_glob")
        if g and g not in file_globs:
            file_globs.append(g)

    if not file_globs:
        continue  # semantic-only or bash-regex-only triggers — covered by master

    # Frontmatter: comma-separated globs. Cursor auto-attaches when the agent
    # opens or edits a matching path.
    fm_globs = ", ".join(file_globs)
    body = f"""---
description: {title or key}
globs: {fm_globs}
alwaysApply: false
---

# Standing order — {title or key}

`decision_key`: `{key}`

{rationale or "(no rationale captured)"}

Source: `operator_decisions` row, `decision_kind=architecture_policy`.
Full record: `bin/praxis-agent praxis_operator_decisions --input-json '{{"action":"get","decision_key":"{key}"}}'`
"""

    (out_dir / f"{slug(key)}.mdc").write_text(body)
    rendered += 1

print(f"rendered {rendered} per-trigger rules + 1 master into {out_dir}")
PY
