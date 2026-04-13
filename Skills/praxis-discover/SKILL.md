---
name: praxis-discover
description: "Search before you build. Use this skill BEFORE implementing any new function, module, class, utility, or pattern in the Praxis codebase. Also use it when the user asks 'how does X work', 'where is X', 'do we already have X', or is about to write code that might duplicate existing functionality. Triggers on: build, implement, create, add, write, refactor, 'how do I', 'where is', 'do we have', 'is there a', or any request that involves writing new code."
---

# Search Before You Build

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
praxis_discover(query="what you need in plain English")
```

This uses vector embeddings over AST-extracted behavioral fingerprints — it finds functionally similar code even when naming is completely different. Be descriptive about the *behavior* you need, not the name you'd give it.

Good queries:
- `"retry logic with exponential backoff"`
- `"validate workflow spec before execution"`
- `"route jobs to providers based on task type"`

You can filter by kind: `module`, `class`, `function`, `subsystem`.

### 2. Knowledge Graph (decisions, patterns, architecture)

```
praxis_recall(query="topic")
```

Search for prior decisions, documented patterns, and architectural context. Use this when you need to understand *why* something was built a certain way, not just *where* it is.

### 3. Database (receipts, bugs, constraints)

```
praxis_query("your question in plain English")
```

Routes automatically to the right subsystem. Use when you need to know about past workflow results, known bugs, or learned constraints.

### 4. Direct code search (when you know what you're looking for)

Use `Grep` for exact names, `Glob` for file patterns. These are faster when you already know the identifier.

## Decision Framework

After searching, decide:

| Search Result | Action |
|---|---|
| Exact match exists | Reuse it directly — don't rebuild |
| Similar code exists | Adapt or extend it — don't create a parallel version |
| Pattern exists elsewhere | Follow the same pattern for consistency |
| Nothing found | Build it, but check with `praxis_recall` for architectural decisions that might affect your approach |

## After Code Changes

When you've written or modified code, update the search index:

```
praxis_discover(action="reindex")
```

This ensures future searches find your new code.

## Common Traps

- **Naming blindness**: The function you need might exist under a completely different name. `praxis_discover` handles this — use behavioral descriptions, not guessed names.
- **Layer duplication**: Before adding a new abstraction layer, search for existing ones. The codebase already has routing, validation, and execution infrastructure.
- **Reinventing DB queries**: Many common queries are already in surfaces or the bug tracker. Search before writing raw SQL.
