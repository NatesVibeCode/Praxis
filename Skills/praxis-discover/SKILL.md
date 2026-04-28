---
name: praxis-discover
description: "Search before you build. Use this skill BEFORE implementing any new function, module, class, utility, or pattern in the Praxis codebase. Also use it when the user asks 'how does X work', 'where is X', 'do we already have X', or is about to write code that might duplicate existing functionality. Triggers on: build, implement, create, add, write, refactor, 'how do I', 'where is', 'do we have', 'is there a', or any request that involves writing new code."
---

# Search Before You Build

## Current Surface Docs

- MCP/catalog reference: `docs/MCP.md`
- CLI reference: `docs/CLI.md`
- API route reference: `docs/API.md`
- Regenerate all three with `PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m scripts.generate_mcp_docs`
- If generated docs disagree with runtime output, trust `praxis workflow tools describe ...` and `praxis workflow routes --json`

Every time you're about to write new code in this repo, stop and search first. The codebase is large and has extensive existing infrastructure. Duplicating what already exists wastes time and creates maintenance burden.

## When to Search

Search before ANY of these:
- Writing a new function, class, or module
- Adding a utility or helper
- Implementing a pattern (retry logic, validation, routing, etc.)
- Creating infrastructure (DB queries, MCP tools, surfaces)
- Refactoring — understand what exists before reshaping it

Also search when the user asks:
- "How does X work?"
- "Where is X?"
- "Do we already have something that does X?"

## How to Search

### 1. Code Discovery (semantic search over AST fingerprints)

```
praxis workflow discover "what you need in plain English"
```

This uses vector embeddings over AST-extracted behavioral fingerprints — it finds functionally similar code even when naming is completely different. Be descriptive about the *behavior* you need, not the name you'd give it.

Good queries:
- `"retry logic with exponential backoff"`
- `"validate workflow spec before execution"`
- `"route jobs to providers based on task type"`

You can filter by kind: `module`, `class`, `function`, `subsystem`.

### 2. Knowledge Graph (decisions, patterns, architecture)

```
praxis workflow recall "topic"
```

Search for prior decisions, documented patterns, and architectural context. Use this when you need to understand *why* something was built a certain way, not just *where* it is.

### 3. Database (receipts, bugs, constraints)

```
praxis workflow query "your question in plain English"
```

Routes automatically to the right subsystem. Use when you need to know about past workflow results, known bugs, or learned constraints.

### 3.5. Schema and safety help

When you are unsure which tool shape to use, ask the catalog before guessing:

```
praxis workflow tools describe praxis_discover
praxis workflow tools describe praxis_recall
praxis workflow tools describe praxis_query
```

For CQRS operations, ask the forge before editing wrappers or migrations:

```
praxis workflow tools call praxis_operation_forge --input-json '{"operation_name":"operator.example","operation_kind":"query"}'
```

The forge output is the action packet: use its register payload, fast-feedback commands, and reject paths as the checklist.

### 4. Direct code search (when you know what you're looking for)

Use `Grep` for exact names, `Glob` for file patterns. These are faster when you already know the identifier.

## Decision Framework

After searching, decide:

| Search Result | Action |
|---|---|
| Exact match exists | Reuse it directly — don't rebuild |
| Similar code exists | Adapt or extend it — don't create a parallel version |
| Pattern exists elsewhere | Follow the same pattern for consistency |
| Nothing found | Build it, but check with `praxis workflow recall` for architectural decisions that might affect your approach |

## After Code Changes

Use fast, local feedback before broad validation:

```text
PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m py_compile <touched-python-file>
PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m pytest <focused-test-file> -q
```

For workflow execution, provider routing, retry, or queue changes, add:

```text
praxis workflow firecheck --json
```

For MCP/CLI/API catalog or docs changes, regenerate docs with the canonical module command and run the docs metadata test:

```text
PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m scripts.generate_mcp_docs
PYTHONPATH="Code&DBs/Workflow" .venv/bin/python -m pytest Code&DBs/Workflow/tests/unit/test_mcp_docs_and_metadata.py -q
```

Then update the search index:

```
praxis workflow discover reindex --yes
```

This ensures future searches find your new code.

## Common Traps

- **Naming blindness**: The function you need might exist under a completely different name. `praxis workflow discover` handles this — use behavioral descriptions, not guessed names.
- **Layer duplication**: Before adding a new abstraction layer, search for existing ones. The codebase already has routing, validation, and execution infrastructure.
- **Reinventing DB queries**: Many common queries are already in surfaces or the bug tracker. Search before writing raw SQL.
- **Catalog blindness**: if you cannot remember the exact tool shape, use `praxis workflow tools describe <tool>` instead of inventing fields from memory.
