# Action fingerprint capture — enable guide

Tool-opportunity detection corpus. All surfaces (gateway receipts,
PostToolUse hooks, sandbox CLI agents) write into the same
`action_fingerprints` table; cross-surface frequency drives the
`tool_opportunities_pending` view.

## What's already on (after migration 383 lands)

- `action_fingerprints` table.
- AFTER INSERT trigger on `authority_operation_receipts` →
  every CQRS gateway op writes a `gateway_op` fingerprint with
  `source_surface='gateway:<caller_ref>'`. Zero config.
- `tool_opportunities_pending` view — `praxis workflow query
  "tool opportunities"` style queries can hit it directly.

## What's coded but default-off

Two PostToolUse capture lanes — both reuse the same Python writer
(`postact_fingerprint.py`) and the same shape-normalization rules.
Neither is wired in `.claude/settings.json` yet.

### Lane 1 — Claude Code host (this harness)

Captures raw `Bash`, `Edit`, `Write`, `MultiEdit`, `Read` tool calls
in this repo's Claude Code sessions.

**Enable:**

1. Add a `PostToolUse` matcher to `.claude/settings.json`:

   ```jsonc
   {
     "matcher": "Bash|Edit|Write|MultiEdit|Read",
     "hooks": [
       { "type": "command",
         "command": "$CLAUDE_PROJECT_DIR/.claude/hooks/postact-fingerprint.sh" }
     ]
   }
   ```

2. Export the enable flag in your shell / launchctl env:

   ```bash
   export PRAXIS_FINGERPRINT_ENABLED=1
   ```

The bash wrapper short-circuits unless step 2 is set, so step 1 is
reversible without removing the entry.

**Disable:** unset `PRAXIS_FINGERPRINT_ENABLED` (or remove the
settings.json entry). No DB cleanup needed — old rows are still
useful corpus.

### Lane 2 — Sandbox CLI agents

Sandboxed Codex / Gemini / Claude Code workers running inside the
sandbox image. They don't trigger this harness's hooks; they need
their own. Same script handles both lanes — it tags the row by
`PRAXIS_FINGERPRINT_SOURCE_SURFACE`.

**Install in a sandbox image:**

1. Copy or bind-mount `.claude/hooks/postact-fingerprint.sh` and
   `.claude/hooks/postact_fingerprint.py` into the sandbox.
2. In the sandbox harness's settings (Claude Code `settings.json`,
   Codex `~/.codex/config.toml` PostToolUse equivalent, Gemini's
   tool_use hook config), point a `PostToolUse` entry at the script.
3. Set sandbox environment:

   ```bash
   export PRAXIS_FINGERPRINT_ENABLED=1
   export PRAXIS_FINGERPRINT_SOURCE_SURFACE="sandbox-worker:<agent>"
   export WORKFLOW_DATABASE_URL="<praxis.db DSN reachable from sandbox>"
   ```

   Sandbox creds policy applies — DSN must be the per-sandbox-issued
   credential, not the operator's session DSN.

The fingerprint writer never blocks the tool call (fire-and-forget
nohup) and fails open on any DSN / connection error, so a misconfig
leaves the agent fully functional minus the fingerprint row.

## Querying the corpus

The table + both views are on the `praxis_search` db read allowlist
(see `Code&DBs/Workflow/runtime/sources/db_read_source.py`), so you
can hit them via the standard search surface — no docker exec or
direct DB access required:

```bash
# pending tool opportunities (raw shapes seen ≥3×, no decision yet)
praxis workflow tools call praxis_search --input-json '{
  "query": "tool opportunities pending",
  "sources": ["db"],
  "limit": 20,
  "scope": {"extras": {"table": "tool_opportunities_pending"}}
}'

# gateway-op recurrence (already-tools, for composite mining)
praxis workflow tools call praxis_search --input-json '{
  "query": "gateway op recurrence",
  "sources": ["db"],
  "limit": 20,
  "scope": {"extras": {"table": "gateway_op_recurrence"}}
}'

# raw fingerprint rows for a specific shape_hash
praxis workflow tools call praxis_search --input-json '{
  "query": "fingerprint rows",
  "sources": ["db"],
  "limit": 50,
  "scope": {"extras": {
    "table": "action_fingerprints",
    "where": "shape_hash = '\''<hash>'\''"
  }}
}'
```

Or directly in SQL (via docker exec / psql):

```sql
SELECT * FROM tool_opportunities_pending LIMIT 20;
SELECT operation_name, occurrence_count FROM gateway_op_recurrence
  ORDER BY occurrence_count DESC LIMIT 20;
SELECT source_surface, COUNT(*)
FROM action_fingerprints
WHERE ts > now() - interval '7 days'
GROUP BY source_surface ORDER BY 2 DESC;
```

## Claiming or declining an opportunity

The view filters out shapes that already have a `tool_opportunity`
decision row. To claim or decline, insert into `operator_decisions`
with `decision_key = 'tool-opportunity::' || substring(shape_hash,1,16)`,
`decision_kind='tool_opportunity'`, and `decision_status` ∈ {`active`,
`declined`, `retired`} per the lifecycle the operator wants.

`tool_opportunities_pending.proposed_decision_key` provides this
key already-built per row.

## Shape rules (current)

- **Gateway ops**: `operation_name + operation_kind` is the shape.
  Payload literals never enter the hash. (Trigger-side, plpgsql.)
- **Bash**: command verb chain + flag *names*; quoted strings dropped,
  numbers → `#N`, hex blobs → `#H`, `--flag=value` → `--flag=*`.
- **Edit / Write / MultiEdit / Read**: action_kind + dir-shape +
  extension. First 3 dirs kept, deeper levels collapsed to `...`.

The shape-normalization rules live in `postact_fingerprint.py` and
are intentionally loose. Detection bias is currently toward
"different shapes that should be the same" rather than
"same shape that should be different" — the view's ≥3 threshold
absorbs the noise.
