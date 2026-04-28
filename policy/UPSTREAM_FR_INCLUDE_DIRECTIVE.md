# Upstream Feature Request — `@include` directive in agent instruction files

Filed by: Praxis (nate@praxis)
Status: ready to submit
Targets: Anthropic (Claude Code), OpenAI (Codex CLI), Google (Gemini CLI)

## Ask

Support a markdown include directive (`@include path/to/file.md` or
similar) in your agent's repo-level instruction file (`CLAUDE.md`,
`AGENTS.md`, `GEMINI.md`). When the harness loads the instruction file
on session start, replace the directive with the contents of the
referenced file at the same nesting level.

## Why

Multi-harness teams maintain near-identical content across CLAUDE.md,
AGENTS.md, GEMINI.md, and `.cursorrules`. The current best-practice is
"keep them in sync by hand" — which fails because:

1. Each harness file's audience is the same agent population reading
   the same standing orders. Forking the content into N files creates
   N drift surfaces.
2. CI gates can render a canonical source into N harness-specific
   files, but rendered files are committed-output that pollute diffs
   and PR review attention.
3. Operator authority that rationally lives in one place (a database
   table, a single markdown file) gets fanned out to N harness files,
   and any file that drifts becomes a load-bearing wrong answer.

A native `@include` lets us keep the canonical orient text in one file
referenced N times, no rendering layer.

## Proposed shape

```markdown
<!-- CLAUDE.md / AGENTS.md / GEMINI.md -->
# Praxis — orient

@include policy/orient-shared.md

## Harness-specific notes
<small notes that genuinely differ per harness>
```

Where `policy/orient-shared.md` carries the harness-neutral content.

Resolution rules we'd expect:
- Path is relative to the file containing the directive.
- Recursion depth limited (e.g. 5) to prevent cycles.
- Cycle detection: error or no-op (either is fine, surfaced in logs).
- Not a Turing-complete templating system. No conditionals, no
  variables. Just content substitution. (Cursor's `@import` in
  `.cursorrules` is roughly the right shape — adopt similar.)

## Current workaround (so you can see the smell)

Praxis maintains:

- `policy/operator-decisions-snapshot.json` — DB-rendered authority
- `policy/operator-decision-triggers.json` — hand-authored projection
- `.cursor/rules/*.mdc` — generated from triggers via
  `scripts/render-cursor-rules.sh`
- `CLAUDE.md`, `AGENTS.md`, `GEMINI.md` — hand-synced manually

Plus a CI gate (`scripts/check-policy-artifacts.sh --check`) that fails
PRs when the generated artifacts drift from sources of truth. The
hand-synced harness files have no such gate because they live in
prose and a strict-equality diff is wrong (each file genuinely differs
in 5%—the harness-specific notes).

A native `@include` makes the harness files trivially:

```
# Praxis — orient
@include policy/orient.md
## Harness notes
- ...
```

…and the drift problem evaporates.

## Submission channels

| Provider | Channel | Notes |
|---|---|---|
| Anthropic (Claude Code) | https://github.com/anthropics/claude-code/issues | Tag `feature-request` |
| OpenAI (Codex CLI) | https://github.com/openai/codex/issues | (Per package.json's `bugs.url`) |
| Google (Gemini CLI) | https://github.com/google-gemini/gemini-cli/issues | Tag `enhancement` |

For each, paste the **Ask**, **Why**, and **Proposed shape** sections.
The **Current workaround** section is optional — include it if the
issue tracker culture favors evidence over abstraction.

## Status tracking

When filed, append to this file:

```markdown
### Filed status
- Anthropic Claude Code: <issue URL> filed YYYY-MM-DD
- OpenAI Codex CLI:      <issue URL> filed YYYY-MM-DD
- Google Gemini CLI:     <issue URL> filed YYYY-MM-DD
```
