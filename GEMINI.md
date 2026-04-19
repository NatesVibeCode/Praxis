# Praxis — Orient on Session Start

You are operating inside the Praxis repository. Before doing any work, fetch standing orders from Praxis.db.

## First action every session

Run one of these to retrieve current standing orders:

```
POST /orient                                           # HTTP when the Praxis API is reachable
praxis workflow tools call praxis_operator_decisions \ # MCP / CLI path
    --input-json '{"action":"list","decision_kind":"architecture_policy","active_only":true}'
```

If neither is reachable, fall back to direct Postgres:

```
psql postgresql://localhost:5432/praxis -c "
SELECT decision_scope_ref, decision_key, title, rationale
FROM operator_decisions
WHERE decision_kind='architecture_policy'
  AND effective_from <= now()
  AND (effective_to IS NULL OR effective_to > now())
ORDER BY decided_at DESC;
"
```

## Why this matters

Praxis.db is the cross-harness source of truth. Codex, Claude, Gemini, and Cursor all read the same `operator_decisions` table. Anything that lives only in `AGENTS.md`, `CLAUDE.md`, `GEMINI.md`, or `.cursorrules` is proprietary sidecar drift. Operator authority lives in Postgres.

## Binding behavior

Each row returned is a **standing order**. Treat each `title` + `rationale` pair as a durable directive that governs this session until explicitly retired (`effective_to` set).

Do not argue with them. File new decisions via `praxis_operator_architecture_policy` if the ruleset needs to change.

## Filing decisions inline

When a durable decision is made in conversation, persist it immediately — do not wait for closeout. Use:

```
praxis workflow tools call praxis_operator_architecture_policy --input-json '{
  "authority_domain": "<domain>",
  "policy_slug": "<slug>",
  "title": "<one-line title>",
  "rationale": "<durable reason>",
  "decided_by": "<principal>",
  "decision_source": "conversation"
}' --yes
```

## Orientation packet

Beyond standing orders, `POST /orient` returns:

- `instruction_authority` — canonical directive for this lane
- `endpoints` — live HTTP surface directory
- `capabilities` — what this runtime provides
- `health`, `recent_activity`, `engineering_observability` — current platform state
- `search_surfaces` — hybrid retrieval entrypoints (discover, recall, bugs, receipts)
- `cli_surface` — curated CLI aliases

Read `standing_orders` first, then `instruction_authority.packet_read_order` for the rest.
