---
name: praxis-conversation-closeout
description: "Praxis repo-local closeout skill. Use when work completed in a conversation must be turned into durable DB-backed operator records instead of being left in chat history, especially when Nate made architecture decisions that belong in the decisions table."
---

# Praxis Conversation Closeout

## Application Metadata

| Application | Name | Description |
| --- | --- | --- |
| Claude | `Praxis Conversation Closeout` | Turn conversation outcomes into durable DB-backed records with explicit authority. |
| Gemini | `Praxis Conversation Closeout` | Turn conversation outcomes into durable DB-backed records with explicit authority. |
| Codex/OpenAI | `Praxis Conversation Closeout` | Turn conversation outcomes into durable DB-backed records with explicit authority. |

Legacy internal skill id: `praxis-conversation-closeout`

Primary references:

- `docs/ARCHITECTURE.md`
- `docs/MCP.md`
- `Skills/praxis-roadmap/SKILL.md`
- `Skills/praxis-bug-logging/SKILL.md`

Use this skill when a conversation produced real work, decisions, backlog changes, or evidence that should survive the thread and become queryable operator state.

## Mission

Do not preserve important work as chat residue.

Classify what happened, then persist it through the narrowest authoritative DB-backed surface:

- architecture policy -> `praxis_operator_architecture_policy` when the conversation established durable policy
- other durable operator decisions -> `praxis_operator_decisions`
- roadmap or backlog additions -> `praxis_operator_write`
- existing bugs or roadmap items that are now complete -> `praxis_operator_closeout`
- newly discovered defects -> `praxis_bugs`
- searchable conversation knowledge with no stronger operator table -> `praxis_ingest`

If you do not know which authority owns a piece of work, ask the router first:

```text
praxis workflow query "What DB-backed record should capture this conversation outcome?"
```

Then switch to the specific surface. Do not stay at the router once the authority is clear.

## Core Laws

1. Conversation history is not authority.
2. The decisions table is for durable decisions, not generic summaries.
3. Architecture policy belongs in `operator_decisions` under typed policy authority, not in markdown drift.
4. Bugs, roadmap rows, and closeout receipts keep their own write seams; do not stuff them into decisions just because they were discussed.
5. Do not invent ids, decision keys, roadmap ids, bug ids, or provenance.
6. Read first when duplicate risk is non-trivial.
7. Finish with an explicit list of what was persisted, what authority owns it, and what still lacks a safe home.

## Workflow

### 1. Extract durable outcomes from the conversation

Separate the thread into explicit candidate records:

- architecture decisions
- other operator decisions
- new roadmap work
- closeout-ready roadmap or bug work
- bugs discovered
- residual knowledge worth recall later

Reject fluff:

- status chatter
- speculative ideas that were not chosen
- implementation narration with no durable consequence

### 2. Match each outcome to one authority

Use the strongest write seam available.

For architectural decisions, prefer the typed surface:

```text
praxis workflow tools describe praxis_operator_architecture_policy
praxis workflow tools call praxis_operator_architecture_policy --input-json '{...}' --yes
```

Use `praxis_operator_decisions` when the outcome is a durable operator decision but not a typed architecture-policy row:

```text
praxis workflow tools describe praxis_operator_decisions
praxis workflow tools call praxis_operator_decisions --input-json '{"action":"record",...}' --yes
```

Use roadmap write authority for new backlog shape:

```text
praxis workflow tools describe praxis_operator_write
```

Use closeout when the conversation finished already-tracked work:

```text
praxis workflow tools describe praxis_operator_closeout
```

Use bug authority for defects:

```text
praxis workflow tools describe praxis_bugs
```

Use ingest only when no stronger operator table exists and the content still deserves durable recall:

```text
praxis workflow tools describe praxis_ingest
```

### 3. Persist architecture decisions explicitly

When Nate made an architecture decision that should govern future work, record it.

Prefer `praxis_operator_architecture_policy` when you can state:

- `authority_domain`
- `policy_slug`
- `title`
- `rationale`
- `decided_by`
- `decision_source`

Example:

```text
praxis workflow tools call praxis_operator_architecture_policy --input-json '{"authority_domain":"decision_tables","policy_slug":"db-native-authority","title":"Decision tables are DB-native authority","rationale":"Durable control must stay queryable in Postgres rather than chat or scripts.","decided_by":"nate","decision_source":"conversation"}' --yes
```

If the decision is durable but does not fit the stricter typed path, use `praxis_operator_decisions` with `action:"record"`.

Before writing, list similar rows when collision risk is real:

```text
praxis workflow tools call praxis_operator_decisions --input-json '{"action":"list","decision_kind":"architecture_policy","limit":20}'
```

### 4. Persist the rest of the work without collapsing authority

Use the correct table for the actual object:

- new roadmap item -> `praxis_operator_write` with `preview`, then `validate` when needed, then `commit`
- completed roadmap item or bug set -> `praxis_operator_closeout`
- new bug -> `praxis_bugs`
- recallable conversation artifact with no better home -> `praxis_ingest` using `kind:"conversation"` or another schema-valid kind

Do not create a decision row just to say that work happened.

### 5. Verify by reading back through the authority surface

After mutation, verify through the same operator lane or a read lane that sees the same truth:

- `praxis workflow tools call praxis_operator_decisions --input-json '{"action":"list",...}'`
- `praxis workflow query "<what changed?>"`
- `praxis workflow bugs ...`
- `praxis workflow tools call praxis_operator_roadmap_view --input-json '{...}'`

If you cannot verify the write, call that out plainly.

## Output Contract

When using this skill, return:

1. `Durable Outcomes`
2. `Authority Mapping`
3. `Writes Performed`
4. `Verification Readback`
5. `Unpersisted Residue`

## Reject These

- "I saved the conversation" as if that were operator state
- writing architecture policy into freehand docs without updating DB authority
- using `praxis_ingest` when a stronger operator table exists
- using the decisions table as a junk drawer for bugs, roadmap items, or generic summaries
- inventing write payload fields without `praxis workflow tools describe ...`
