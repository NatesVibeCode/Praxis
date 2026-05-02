# Workflow Authoring Guide

A workflow in Praxis is a directed acyclic graph of jobs. This guide covers how to think about designing one, the two authoring paths, and the patterns that cover most real use cases. For the complete field reference see [WORKFLOW_SPEC.md](WORKFLOW_SPEC.md).

## Two authoring paths

**NL via Canvas (compose pipeline)** — describe the outcome in plain English in Canvas's "New workflow" panel. The compose pipeline runs synthesis (a frontier model decomposes your intent into ~20 packet seeds), then 20 parallel author calls expand the seeds, then pill triage and validation gate before anything renders. The Release tray shows pre-flight checks; it blocks dispatch until all pass. This path produces a complete `.queue.json` plan you can inspect and edit before committing.

**Direct `.queue.json`** — write the spec by hand or copy from `examples/`. Better when you know exactly what jobs you need, want to version the spec in git, or are scripting workflow generation. Run with `praxis workflow run path/to/spec.queue.json`.

Both paths produce the same output: a workflow plan in Praxis.db, executed by the same runtime, producing the same receipts.

## The contract triad

Every well-formed workflow declares three things:

```json
{
  "outcome_goal": "What success looks like in one sentence",
  "anti_requirements": ["What the workflow must not do"],
  "verify_refs": ["scripts/my_check.sh"]
}
```

**`outcome_goal`** tells the trust compiler what the workflow is for. It's not documentation — it's used during compile to scope the write envelope and verify that the released plan actually addresses the stated intent.

**`anti_requirements`** are hard constraints. "Do not modify public API signatures." "Do not touch the database schema." These are compiled into the write-scope gates: if a job tries to write outside its allowed envelope, it fails closed. Anti-requirements give you enforcement, not just intention.

**`verify_refs`** are scripts run after all jobs succeed. Each must exit 0 for the workflow to be marked complete. Pointing a workflow's verify block at your test suite means a workflow can't claim success unless the tests pass.

## `auto/` routing

The `agent` field in a job routes it to the best model for that task type. The router consults the provider registry and picks the highest-scoring eligible model at dispatch time — not hardcoded.

| Route | Selects | Reach for it when... |
|-------|---------|---------------------|
| `auto/build` | Best coder | Generating code, implementing features |
| `auto/review` | Best reviewer | Code review, quality analysis |
| `auto/architecture` | Best reasoner | System design, planning, decisions |
| `auto/test` | Best terminal operator | Writing and running tests |
| `auto/refactor` | Best refactorer | Cleanup, restructuring, renaming |
| `auto/wiring` | Cheapest fast model | Config, glue code, simple edits |
| `auto/debate` | Best reasoner | Adversarial analysis, stress-testing a plan |
| `auto/research` | Research model | Deep research, web-backed inquiry |

Override with a direct route when you need a specific provider:

```json
{"agent": "anthropic/claude-sonnet-4-6"}
```

Use `auto/` when you don't care which specific model wins, only what kind of reasoning the job needs. The router handles the rest, including fallback to next-best on failure.

## DAG composition

Jobs run concurrently when their dependencies are satisfied. Declare explicit ordering only where it matters:

```json
{
  "jobs": [
    {"label": "analyze", "agent": "auto/architecture", "prompt": "..."},
    {"label": "implement", "agent": "auto/build", "prompt": "...", "depends_on": ["analyze"]},
    {"label": "test", "agent": "auto/test", "prompt": "...", "depends_on": ["implement"]},
    {"label": "review", "agent": "auto/review", "prompt": "...", "depends_on": ["implement", "test"]}
  ]
}
```

`review` waits for both `implement` and `test`. `analyze` runs immediately. `implement` and `test` are sequential here, but if `test` didn't depend on `implement` they'd run in parallel.

**Sprint ordering** is a shorthand for sequential phases. If any job has a `sprint` field, jobs are sorted by sprint number and sprint N+1 jobs implicitly depend on all sprint N jobs:

```json
{"label": "plan", "agent": "auto/architecture", "sprint": 1},
{"label": "build", "agent": "auto/build", "sprint": 2},
{"label": "test", "agent": "auto/test", "sprint": 3}
```

Use `sprint` for linear pipelines; use `depends_on` when the graph is non-trivial.

## Write scope

Limit what files a job can touch with `scope.write`:

```json
{
  "label": "fix-auth",
  "agent": "auto/build",
  "prompt": "Fix the session token storage bug",
  "scope": {"write": ["src/auth/", "tests/auth/"]}
}
```

Jobs without a `scope` field inherit the workflow's default envelope. Jobs with `scope.write` are constrained to those paths — attempts to write outside them fail closed and surface as a scope violation in the friction ledger.

## Fan-out

`replicate: N` creates N parallel copies of a job with suffixed labels:

```json
{
  "label": "review",
  "agent": "auto/review",
  "prompt": "Review this module from your assigned perspective",
  "replicate": 3
}
```

This creates `review_r1`, `review_r2`, `review_r3` running in parallel. Useful for parallel review passes, multi-perspective debate, or running the same verification across multiple inputs.

## Common patterns

### Single-agent task

```json
{
  "name": "fix-bug",
  "outcome_goal": "Fix the failing test in auth/test_session.py",
  "anti_requirements": ["Do not change the public API"],
  "jobs": [
    {
      "label": "fix",
      "agent": "auto/build",
      "prompt": "Fix the failing test at auth/test_session.py:42. The test expects get_session() to raise SessionExpired when the token is past its TTL.",
      "scope": {"write": ["src/auth/", "tests/auth/"]}
    }
  ],
  "verify_refs": ["scripts/run_auth_tests.sh"]
}
```

### Test-gated build

```json
{
  "name": "add-feature",
  "outcome_goal": "Add rate limiting to the public API",
  "anti_requirements": ["Do not change existing endpoint contracts"],
  "jobs": [
    {"label": "implement", "agent": "auto/build", "prompt": "Add rate limiting middleware to all /v1 routes", "scope": {"write": ["src/middleware/", "src/api/"]}},
    {"label": "test", "agent": "auto/test", "prompt": "Write tests covering rate limit enforcement and bypass attempts", "depends_on": ["implement"], "scope": {"write": ["tests/middleware/"]}},
    {"label": "review", "agent": "auto/review", "prompt": "Review the implementation and tests for correctness and edge cases", "depends_on": ["implement", "test"]}
  ],
  "verify_refs": ["scripts/run_tests.sh"]
}
```

### Parallel fan-out review

```json
{
  "name": "adversarial-review",
  "outcome_goal": "Multi-perspective review of the auth rewrite",
  "jobs": [
    {"label": "review", "agent": "auto/review", "prompt": "Review the auth module rewrite. Focus on: r1=security, r2=performance, r3=API surface consistency.", "replicate": 3},
    {"label": "synthesize", "agent": "auto/architecture", "prompt": "Synthesize the three review perspectives into a prioritized list of issues", "depends_on": ["review_r1", "review_r2", "review_r3"]}
  ]
}
```

### Debate before build

```json
{
  "name": "design-with-debate",
  "outcome_goal": "Design and implement a caching layer for the provider registry",
  "jobs": [
    {"label": "debate", "agent": "auto/debate", "prompt": "Stress-test this caching design: [design spec]. What breaks? What are the cache-invalidation failure modes?"},
    {"label": "revise", "agent": "auto/architecture", "prompt": "Revise the design based on the debate findings", "depends_on": ["debate"]},
    {"label": "implement", "agent": "auto/build", "prompt": "Implement the revised caching design", "depends_on": ["revise"], "scope": {"write": ["src/registry/cache/"]}}
  ]
}
```

## When is a workflow "done"?

The Release tray in Canvas shows four pre-flight checks. All must pass before dispatch:

1. **Jobs exist** — at least one job compiled into the plan.
2. **Trigger configured** — a manual, webhook, or schedule trigger is attached.
3. **Pre-flight checks pass** — no unresolved source refs, write-scope conflicts, or verifier admission failures.
4. **Outcome gate met** — the compiled plan addresses the stated `outcome_goal`.

From the CLI, `praxis workflow run` runs the same compile-and-validate pass before submitting. A workflow that fails validation doesn't dispatch — it surfaces the blocking finding instead.

---

**See also:** [WORKFLOW_SPEC.md](WORKFLOW_SPEC.md) — full field reference. [CONCEPTS.md](CONCEPTS.md) — trust compiler and core terms. [CANVAS.md](CANVAS.md) — using Canvas to compose workflows.
