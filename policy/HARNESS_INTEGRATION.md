# Harness Integration — Operator-Decision JIT Surfacing

This directory holds the **harness-neutral** policy registry and integration
recipes that surface operator standing orders at the moment of action,
regardless of which agent harness (Claude Code, Codex, Gemini, plain shell
operator, future) is taking the action.

## Why this is harness-neutral

Per `architecture-policy::surfaces::cli-mcp-parallel`, Praxis exposes three
sibling surfaces over `operation_catalog_gateway`: CLI, MCP, HTTP. Agents
also act *outside* Praxis surfaces — they shell out to docker, edit source
files directly, write to disk. The enforcement story has to cover both
paths or it has gaps.

So we have **two layers** that share the same registry and the same
matcher:

1. **Gateway-side, universal** — `surfaces/policy/trigger_check.py` is
   imported and called from `surfaces/mcp/invocation.py::invoke_tool`. Every
   MCP, CLI, and HTTP call into Praxis runs through this. Every agent
   harness, every external script, every operator-shell `praxis workflow`
   invocation passes the same check. The result of the check is appended
   to the tool's response payload as `_standing_orders_surfaced`.

2. **Per-harness, raw-tool path** — the agent harness's PreToolUse hook
   (or equivalent) calls `surfaces.policy.check(...)` for tool calls that
   don't go through Praxis (Bash, Edit, Write, etc.). The same matcher,
   the same registry, just invoked from a different entry point.

## The registry (single source)

`policy/operator-decision-triggers.json` is hand-authored alongside
`operator_decisions` writes. Each entry maps a `decision_key` (from
`operator_decisions.decision_key`) to one or more match conditions.

We can't put match conditions on `operator_decisions` itself: migration
264 enforces `scope_clamp_preserved_verbatim` so the rationale stays
human-authored prose. The trigger registry is the structured-projection
sidecar, same pattern as `data_dictionary_objects` projecting from
`operator_decisions` for type contracts.

Match condition shape:

```json
{
  "tool": "Bash",                   // optional; matches harness tool name
  "regex": "...",                   // optional; matches tool input or command
  "file_glob": "**/*.Dockerfile",   // optional; matches file_path arg
  "string_match": "verbal_seal",    // optional; matches new content
  "advisory_only": false            // optional; flagged as advisory in surface
}
```

## Per-harness recipes

### Claude Code

Already wired:
- `.claude/settings.json` registers a `PreToolUse` hook on
  `Bash|Edit|MultiEdit|Write|Read`.
- `.claude/hooks/preact-orient-friction.sh` invokes the Python entry.
- `.claude/hooks/preact_orient_friction.py` imports
  `surfaces.policy.check` from `Code&DBs/Workflow/surfaces/policy/` and
  emits a friction event via `bin/praxis-agent praxis_friction`.

Verify with:
```bash
echo '{"tool_name":"Bash","tool_input":{"command":"docker restart praxis-workflow-worker-1"}}' \
  | CLAUDE_PROJECT_DIR=/Users/nate/Praxis \
    .claude/hooks/preact-orient-friction.sh
```

### Codex

Already wired (codex-cli ≥ 0.121):
- `.codex/hooks.json` registers `PreToolUse` on
  `local_shell|shell|apply_patch|write_file|read_file`.
- `.codex/hooks/preact-orient-friction.sh` invokes the Python entry.
- `.codex/hooks/preact_orient_friction.py` imports
  `surfaces.policy.check` and emits a friction event tagged
  `harness=codex_cli`.

If your codex-cli is older than 0.121 (or the project-local discovery
isn't picking up `.codex/hooks.json`), copy the contents of
`.codex/hooks.json` into `~/.codex/hooks.json` user-globally — the
hook scripts work either way because they're harness-neutral.

Tool-name normalization: Codex's native shell tool is `local_shell`
(argv list) and its edit tool is `apply_patch`. The matcher at
`surfaces.policy.trigger_check._normalize_tool_name` aliases these to
`Bash`/`Edit` so the trigger registry stays harness-neutral.

Verify with:
```bash
echo '{"tool_name":"local_shell","tool_input":{"command":["docker","restart","praxis-workflow-worker-1"]}}' \
  | CODEX_PROJECT_DIR=/Users/nate/Praxis \
    .codex/hooks/preact-orient-friction.sh
```

### Gemini

Already wired (gemini-cli ≥ 0.39):
- `.gemini/settings.json` registers `BeforeTool` on
  `run_shell_command|replace|MultiEdit|write_file|read_file`.
- `.gemini/hooks/preact-orient-friction.sh` invokes the Python entry.
- `.gemini/hooks/preact_orient_friction.py` imports
  `surfaces.policy.check` and emits a friction event tagged
  `harness=gemini_cli`.

Tool-name normalization: Gemini's native shell tool is `run_shell_command`
and its edit/write/read tools are `replace`/`write_file`/`read_file`.
Aliased in `surfaces.policy.trigger_check._normalize_tool_name`.

Verify with:
```bash
echo '{"tool_name":"run_shell_command","tool_input":{"command":"docker restart praxis-workflow-worker-1"}}' \
  | GEMINI_PROJECT_DIR=/Users/nate/Praxis \
    .gemini/hooks/preact-orient-friction.sh
```

### Cursor

Cursor has no event-driven PreToolUse hook. Its closest equivalent is
the rules system: `.cursor/rules/*.mdc` files with `globs:` frontmatter
auto-attach to the agent's context when it edits matching files. We
render one per trigger that has a `file_glob` plus a master
`_standing-orders-jit.mdc` (alwaysApply) that points at
`bin/praxis-policy-check` for raw shell commands.

Re-render after editing the trigger registry:
```bash
scripts/render-cursor-rules.sh
```

This is generated output — check it in, but treat
`policy/operator-decision-triggers.json` as the source of truth. The CI
gate (Packet 1) compares the committed render against a fresh render
and fails if they diverge.

### Universal CLI shim (`bin/praxis-policy-check`)

For any harness without a true PreToolUse hook (Cursor today, future
harnesses), or for plain shell scripts that want to consult the registry,
call the shim directly:
```bash
bin/praxis-policy-check Bash '{"command":"<your command>"}'
bin/praxis-policy-check write_file '{"file_path":"GEMINI.md","content":"..."}'
```
Output: rendered standing-order surface on stdout (multi-line text) if
any matched, empty otherwise. Always exits 0 — fail open.

### Plain operator (shell-only, no agent)

Operator running `praxis workflow tools call ...` from a terminal hits
the gateway and gets `_standing_orders_surfaced` in the result. No
additional integration needed.

For raw shell commands the operator runs *outside* praxis — that's the
operator's own decision to make; the friction surface is only worth
firing for agents who don't already know the policy.

## Adding a new standing order

1. File the decision via `praxis_operator_decisions(action="record",
   decision_kind="architecture_policy", decision_key="...", title="...",
   rationale="...")`. The decision is the human-authored authority.

2. Add a corresponding entry to `policy/operator-decision-triggers.json`
   with one or more `match` conditions. The registry is the structured
   projection.

3. (Optional) Test with:
   ```bash
   PYTHONPATH=Code\&DBs/Workflow python3 -c "
   from surfaces.policy import check, render_additional_context
   m = check('Bash', {'command': '<command that should match>'})
   print(render_additional_context(m, 'Bash'))
   "
   ```

4. (Optional) If the standing order is purely semantic (no shell pattern,
   no file glob, no string match), leave `match` empty and add
   `"$semantic_only": true`. The decision still surfaces via
   `praxis_orient` and gets recorded; it just doesn't fire as a trigger.

## Future: Packet 2 — Policy Authority data layer

The agent-side enforcement here is *advisory*. Per /praxis-debate fork,
hard rejection lives at the data layer: BEFORE INSERT/UPDATE triggers on
authority tables, backed by a `policy_definitions` table that's FK-bound
to `operator_decisions`. Migration validators that reject SQL that would
violate or attempt to disable enforcement (catches `SET
session_replication_role = replica`, `ALTER TRIGGER DISABLE`, etc.). That
work is scoped as Packet 2 — separate build, separate phase. The
trigger registry here continues to live alongside it as the surfacing
layer.
