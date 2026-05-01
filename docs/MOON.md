# Moon UI Guide

Moon is the canonical operator UI for Praxis — one canvas, one chat panel, five surfaces. Every interaction persists: there is no separate "save" step, no sync lag, no state that lives only in the browser. What you see in Moon is what Praxis.db contains.

For the trust-compiler and term concepts behind what Moon shows you, see [CONCEPTS.md](CONCEPTS.md).

## The five surfaces

**Overview** — the live workspace. Shows the current state of your workflows, the trust compiler output for the selected run, and a sandbox terminal. Start here.

**New workflow (Build)** — the compose entry point. Describe an outcome in plain English and the compose pipeline materializes a full workflow plan. The graph editor lets you inspect and adjust the result before dispatch.

**Atlas** — the accumulation view. Shows how your platform's confidence has evolved over time across 20 tracked architecture areas. Not a dashboard of today — a view of what's compounding.

**Manifests** — the catalog. Search durable manifests by family, type, status, and free text.

**Strategy Console** — the chat panel. Slides over any surface. Use it for context, planning, and triage — not for executing operations. Operations run through the workflow engine; the Console helps you decide what to run.

---

## Overview

The Overview panel has three columns: the workflow list/sidebar on the left, the `WORKFLOW_CONTRACT` panel in the center, and the receipts panel on the right.

**WORKFLOW_CONTRACT panel** is the trust compiler made visible. When a run is selected, it shows:

| Field | What it means |
|-------|--------------|
| `TASK` | The compiled job description the agent received |
| `READ SCOPE` | What files and context the agent was permitted to read |
| `WRITE SCOPE` | What files the agent was permitted to modify |
| `LOCKED` | What was explicitly locked out of scope |
| `TOOLS` | The 3–5 tools the agent could reach for |
| `APPROVAL` | Whether a human gate was required |
| `VERIFIER` | The verify command or script that must exit 0 |
| `RETRY` | The retry policy if the job fails |
| `MATERIALIZED` | Whether the plan has been materialized into a dispatched run |

Reading this panel answers "what did the trust compiler allow?" — not what the agent said it would do, but what the runtime enforced.

**Receipts panel** shows the operation ledger for the selected run. Each row is a receipt from `authority_operation_receipts`: input hash, output hash, execution status, duration. Click a row to inspect the full receipt including the replay path.

**Sandbox terminal** is a read-only view into the job's execution environment. Shows stdout/stderr from running jobs.

---

## New Workflow (Build)

The Build surface is where workflows are composed. Two entry paths:

**Describe it** — type your intent in plain English. Example: "Add rate limiting to all /v1 API routes without changing endpoint contracts." The compose pipeline runs:

1. **Synthesis** (~30s) — a frontier model decomposes your intent into ~20 packet seeds.
2. **Fork-out** (~2–3 min) — 20 parallel author calls expand the seeds, prefix-cached for cost efficiency.
3. **Pill triage + validation** — typed gaps, source-ref resolution, write-scope contracts, verifier admission.

Nothing renders until all gates pass. This is by design — a partially valid plan isn't a plan.

**Start from scratch** — opens the visual graph editor with an empty canvas. Add nodes manually, assign routes using the node popout, connect edges, attach gates.

### The graph editor

Nodes are workflow jobs. Each node shows its label and assigned route. Click a node to open the Inspector dock, which shows the job's prompt, route, scope, and authority tabs.

**Node popout** — click the action icons on a node to assign a route (`auto/build`, `auto/review`, a direct provider, an integration). The route assignment persists to `build_graph` immediately.

**Edge gates** — click the midpoint gate pod on any edge to attach a gate. The four gates that affect execution today:
- **Branch** — conditional routing based on the upstream job's output.
- **On Failure** — execution path taken when the upstream job fails.
- **Approval** — pauses the downstream job until a human approves.
- **Validation** — runs a verification command on the upstream job's output before the downstream job starts.

### The Release tray

The Release tray in the bottom-right shows four pre-flight checks. All must be green before you can dispatch:

1. **Jobs exist** — at least one job is in the plan.
2. **Trigger configured** — manual, webhook, or schedule trigger is attached.
3. **Pre-flight checks pass** — no unresolved refs, scope conflicts, or verifier failures.
4. **Outcome gate met** — the compiled plan addresses the stated `outcome_goal`.

If any check is blocked, a **Fix** button navigates to the specific node or dock causing the issue.

**Dispatch** → **Confirm Release** is the two-click commit sequence. After confirmation: the workflow is created if needed, the definition and plan are committed, and the run is triggered. The Release tray transitions to a **View Run** button.

---

## Atlas

Atlas shows confidence infrastructure over time — not what ran today, but what has accumulated.

**20 tracked architecture areas** are displayed as weighted tiles. Each area has:
- **Weight** — how much activity has landed in this area over the platform's history.
- **Write-rate sparkline** — the recent velocity of changes in this area.
- **Hot/dormant signal** — areas with recent, consistent activity are "hot"; areas with no recent writes are "dormant."

Reading Atlas: a "hot" area with rising weight means work is compounding there — patterns are being added, primitives are being promoted, standing orders are building up. A "dormant" area with low weight may be stable or may be neglected. Atlas doesn't tell you which — it shows you what to investigate.

The "confidence infrastructure, materialized" tagline on the Praxis landing page refers to this view. The platform is not just executing work — it is demonstrating, through accumulated receipts and patterns, that it reliably handles certain classes of problems.

---

## Manifests

Manifests are durable catalog entries — workflows, primitives, integrations, authority objects. Search by family, type, status, or free text.

Use Manifests to find existing workflows before building new ones, inspect the current state of platform primitives, and verify that a component you depend on has shipped and is consistent.

---

## Strategy Console

The Console is the chat partner panel that slides over any surface. It has three starter prompt categories:

- **Backward** — "What changed since my last session?" — orients you to recent activity.
- **Forward** — "Help me plan the next build step." — uses current context to suggest what to work on.
- **Sideways** — "Find the relevant context for this screen." — retrieves knowledge, decisions, and receipts relevant to what you're looking at.

The Console does not execute operations — it helps you reason about them. Run actual workflows from the Build surface or the CLI; use the Console to understand what's there and what to do next.

---

## Common workflows in Moon

**Check platform health:** Overview → `praxis workflow query "status"` in the Console, or check the Atlas for recent activity signals.

**Compose a new workflow from intent:** New workflow → "Describe it" → type your outcome → wait for synthesis + fork-out → inspect the plan in the graph editor → fix any Release tray blocks → Dispatch.

**Inspect a failed run:** Overview → select the run → read the WORKFLOW_CONTRACT for the failing job's scope → check the receipts panel for the error receipt → Console → "What went wrong with this run?"

**Find an existing workflow before building a new one:** Manifests → search → if it exists, run it; if it doesn't, build it.

---

**See also:** [CONCEPTS.md](CONCEPTS.md) — trust compiler and core terms. [WORKFLOWS.md](WORKFLOWS.md) — workflow authoring. [OPERATOR_GUIDE.md](OPERATOR_GUIDE.md) — day-2 operations including patterns and decisions.
