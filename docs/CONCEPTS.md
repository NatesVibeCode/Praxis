# Praxis Concepts

This document explains the core ideas behind Praxis Engine before you write your first workflow or open Moon. If something in the README, ARCHITECTURE, or tool docs uses an unfamiliar term, start here.

## The problem Praxis solves

Most AI tools work the same way: you ask, it produces output, you move on. The tool forgets what it did. Patterns that worked once must be re-discovered. Nothing compounds.

Praxis is built on a different premise: **every action should make the next one easier**. That requires the system to remember what it decided, prove what it did, and let successful patterns graduate into reusable authority. The mechanism that makes this work is the trust compiler.

## The trust compiler

The trust compiler is not a single module — it is what Praxis does at the moment a job is dispatched. Before any agent touches a file or calls an API, the runtime compiles:

- **Context** — the right background, no more. What the agent should know about the task, the repo, and the operator's standing orders.
- **Legal actions** — not the full tool catalog, but the 3–5 tools whose output type matches what this job is allowed to produce. The agent cannot reach for a tool that isn't in scope.
- **Write scope** — an explicit envelope of paths the agent may touch. Anything outside it is locked.
- **Gates** — checkpoints the job must pass before its output is accepted (verifier commands, approval requirements).
- **Recovery paths** — what happens on failure: retry policy, escalation, fallback routes.

The effect: **the right choice is the easiest choice**. The agent isn't picking from 177 tools on instinct; it's choosing from 4 tools that are already appropriate. Mistakes happen less because the environment forecloses them, not because the agent is smarter.

The trust compiler is visible in Moon's Overview panel as the `WORKFLOW_CONTRACT` display: `TASK / READ / WRITE / LOCKED / TOOLS / APPROVAL / VERIFIER` — each field is what the compiler emitted before the job ran.

## Core terms

**Workflow** — A directed acyclic graph of jobs, declared as a `.queue.json` file. The top-level unit of work in Praxis. Has an `outcome_goal`, optional `anti_requirements`, and a list of `jobs`.

**Job** — One node in a workflow DAG. Has an `agent` route (e.g. `auto/build`), a `prompt`, and optionally `depends_on` links to other jobs, a `scope` envelope, and a `verify_command`. Jobs run concurrently when their dependencies are satisfied.

**Receipt** — An immutable ledger row written by the CQRS gateway every time an operation runs. Contains the input hash, output hash, idempotency key, execution status, and a replay path. Every gateway dispatch writes one. Receipts answer "what did the system actually do?" and make replay deterministic when the idempotency policy allows.

**CQRS gateway** — The engine bus. Every Praxis operation runs through `execute_operation_from_subsystems`, which validates the payload, writes a receipt, and emits authority events for command operations. No MCP tool bypasses the gateway. No receipt = the operation didn't happen as far as Praxis is concerned.

**Primitive** — A declared platform capability registered in `primitive_catalog`. Primitives describe engines, gateway wrappers, repositories, and authorities, and they are consistency-checked against code so the catalog stays grounded. The `primitive_catalog` is itself a primitive — the system is self-describing.

**Decision authority** — Standing orders that persist across runs, in `operator_decisions`. When you file a decision ("always use CLI routing, not API"), that row is consulted at compile time and by agents before they pick a path. Decisions can be superseded, scoped, and inherited. They outlast any individual run.

**Data pill** — A typed data object passed between workflow jobs. Pills are declared in the data dictionary and carry lineage, quality rules, and governance tags. When a job produces a pill, the next job's context is enriched with what that pill contains — not raw text, typed state.

**Gate** — A checkpoint on a workflow edge. The four gate kinds that affect execution today: `Approval` (pauses for human sign-off), `Validation` (runs a verification command on the upstream step), `Branch` (conditional routing), `On Failure` (failure path). Gates are what make workflows governable, not just executable.

**Pattern** — A recurring shape extracted from completed runs, failures, and friction events. Patterns sit between raw evidence and bug tickets in the authority hierarchy. A pattern that appears often enough can be promoted into the `primitive_catalog`, graduating from observed behavior into declared platform capability.

## One graph, many lenses

All Praxis surfaces — Moon, the CLI, the MCP tools, the REST API — are lenses on the same underlying graph in Postgres. When you describe a workflow in Moon's "New workflow" entry and it materializes into a plan, that plan is rows in Praxis.db. When you run `praxis workflow run`, that's the CLI dispatching against the same rows. When an MCP tool returns a receipt, the receipt is in the same `authority_operation_receipts` table that Moon reads.

Nothing is translated between layers. No sync job, no mirror, no eventual consistency. **An edit in Moon and the next run's behavior are the same DB write.**

This is why Moon is not a dashboard on top of Praxis — it is Praxis. The CLI and MCP surfaces are equally first-class; they access the same authority, produce the same receipts, and respect the same standing orders.

## Why receipts matter

A receipt is not a log line. Log lines are written once and forgotten. A receipt is a queryable row with an input hash that makes replay deterministic: if you ask the system to re-run the same operation with the same inputs, the gateway finds the existing receipt and returns the cached result — no second dispatch, no second cost, provably identical output.

For agents this means: "what did I ask last time?" is always answerable. For operators it means: "what did the system do during this incident?" is reconstructible. For the platform it means: patterns extracted from receipts are grounded in real behavior, not memory.

---

**Next:** [WORKFLOWS.md](WORKFLOWS.md) — how to design and run a workflow. [MOON.md](MOON.md) — how to use the Moon canvas. [OPERATOR_GUIDE.md](OPERATOR_GUIDE.md) — day-2 operations. For full term reference see [ARCHITECTURE.md](ARCHITECTURE.md).
