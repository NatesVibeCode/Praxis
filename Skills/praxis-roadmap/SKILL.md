---
name: praxis-roadmap
description: "Praxis DB-backed roadmap skill. Use to inspect, preview, validate, or commit roadmap items through the operator surfaces instead of markdown drift."
---

# Praxis Roadmap

## Current Surface Docs

- MCP/catalog reference: `docs/MCP.md`
- CLI reference: `docs/CLI.md`
- API route reference: `docs/API.md`
- Regenerate all three with `PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m scripts.generate_mcp_docs`
- If generated docs disagree with runtime output, trust `praxis workflow tools describe ...` and `praxis workflow routes --json`

Use this skill when roadmap state needs to be read or changed.

## Authority

Roadmap truth lives in the DB-backed operator surfaces, not in freehand markdown.

Read authority:

```text
praxis workflow roadmap view
praxis workflow roadmap view --root <roadmap_item_id>
```

Write authority:

```text
praxis workflow roadmap write preview --title <title> --intent-brief <brief>
praxis workflow roadmap write validate --title <title> --intent-brief <brief>
praxis workflow roadmap write commit --title <title> --intent-brief <brief>
```

Minimum verified write fields:

- `title`
- `intent_brief`

For complex write shapes, inspect the tool schema before committing:

```text
praxis workflow tools describe praxis_operator_write
```

## Rules

- preview before commit
- validate when the item shape or dependencies are non-trivial
- do not invent `parent_roadmap_item_id`, `depends_on`, or `source_bug_id`
- link real decisions, bugs, or registry paths when they exist
- use `praxis workflow query "<roadmap question>"` when you need read-only orientation first

## Output Contract

Return:

1. `Roadmap Read`
2. `Proposed Write`
3. `Validation Gate`
4. `Commit Decision`
5. `Risks`
