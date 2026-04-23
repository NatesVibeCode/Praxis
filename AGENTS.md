# Praxis — Orient on Session Start

You are operating inside the Praxis repository. Before doing any work, fetch standing orders from Praxis.db.

## First action every session

Run one of these to retrieve current standing orders:

```
POST /orient                                           # HTTP when the Praxis API is reachable
praxis workflow tools call praxis_operator_decisions \ # MCP / CLI path
    --input-json '{"action":"list","decision_kind":"architecture_policy","active_only":true}'
```

If neither is reachable, fall back to the registry/runtime-provided network
Postgres DSN. Do not start, query, or infer authority from a localhost
Postgres instance; localhost Postgres is retired for operator state.

```
psql "$WORKFLOW_DATABASE_URL" -c "
SELECT decision_scope_ref, decision_key, title, rationale
FROM operator_decisions
WHERE decision_kind='architecture_policy'
  AND effective_from <= now()
  AND (effective_to IS NULL OR effective_to > now())
ORDER BY decided_at DESC;
"
```

## Fresh clone / empty DB fallback

If the database is not created yet, has no `operator_decisions` rows, or cannot
answer the standing-order query, do not invent operator-local policy from
sidecar docs. Run:

```
./scripts/bootstrap
```

Then repeat the orientation query. Until the DB answers, stay inside the public
fresh-clone scope from `README.md`, `SETUP.md`, and `config/runtime_profiles.json`.
Treat private operator decisions, local provider assumptions, and workspace
exceptions as unavailable unless they are present in Praxis.db.

## Why this matters

Praxis.db is the cross-harness source of truth. Codex, Claude, Gemini, and Cursor all read the same `operator_decisions` table. Anything that lives only in `AGENTS.md`, `CLAUDE.md`, `GEMINI.md`, or `.cursorrules` is proprietary sidecar drift. Operator authority lives in Postgres.

The active Praxis.db is the network/registry-resolved Postgres authority, not
the old localhost development database. If `WORKFLOW_DATABASE_URL` is absent,
discover it through the workflow registry/runtime authority or ask for the
current network DSN; do not resurrect local Postgres to make a command pass.

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

## Commit coordination

Before staging or committing, check repo-level commit ownership:

```
./scripts/git-commit-guard --json
```

For longer commit preparation, claim and release the gate around index writes:

```
./scripts/git-commit-guard claim --owner "<agent-or-session>" --json
./scripts/git-commit-guard release --owner "<agent-or-session>" --json
```

If the gate reports `claimed` or `git_index_locked`, wait instead of forcing Git.
