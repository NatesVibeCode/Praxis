---
name: praxis-roadmap
description: "Praxis DB-backed roadmap skill. Use to inspect, preview, validate, or commit roadmap items through the operator surfaces instead of markdown drift."
---

# Praxis Roadmap

Use this skill when roadmap state needs to be read or changed.

## Authority

Roadmap truth lives in the DB-backed operator surfaces, not in freehand markdown.

Read authority:

```text
praxis workflow tools describe praxis_operator_roadmap_view
praxis workflow tools call praxis_operator_roadmap_view --input-json '{}'
```

Write authority:

```text
praxis workflow tools describe praxis_operator_write
```

Verified `praxis_operator_write` actions:

- `preview`
- `validate`
- `commit`

Minimum verified write fields:

- `title`
- `intent_brief`

Example preview:

```text
praxis workflow tools call praxis_operator_write --input-json '{"action":"preview","title":"Consolidate CLI frontdoors","intent_brief":"one authority for operator CLI"}'
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
