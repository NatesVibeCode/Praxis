---
name: praxis-conversation-closeout
description: "Praxis repo-local closeout skill. Use when conversation outcomes must become durable DB-backed operator records instead of chat residue, including architecture decisions, unfinished roadmap work, bugs discovered or fixed in-thread, and closeout receipts."
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

Use this skill when a conversation produced real work, decisions, backlog changes, fixes, enhancements, or evidence that should survive the thread and become queryable operator state.

## Mission

Do not preserve important work as chat residue.

Classify what happened, then persist it through the narrowest authoritative DB-backed surface:

- architecture policy -> `praxis_operator_architecture_policy` when the conversation established durable policy
- other durable operator decisions -> `praxis_operator_decisions`
- roadmap or backlog additions, including enhancements and planned follow-up work -> `praxis_operator_write`
- existing bugs or roadmap items that are now complete -> `praxis_operator_closeout`
- newly discovered defects, including defects fixed during the conversation but never formally tracked -> `praxis_bugs`
- searchable conversation knowledge with no stronger operator table -> `praxis_ingest`

If a defect was fixed in the thread but no bug row exists yet, file the bug anyway, attach evidence, and resolve it through bug authority. If the remaining work is an enhancement rather than a defect, create a roadmap item instead of burying it in prose.

If you do not know which authority owns a piece of work, ask the router first:

```text
praxis workflow query "What DB-backed record should capture this conversation outcome?"
```

Then switch to the specific surface. Do not stay at the router once the authority is clear.

Prefer first-class `praxis workflow` entrypoints when they exist:

- `praxis workflow bugs ...`
- `praxis workflow roadmap write ...`
- `praxis workflow roadmap closeout ...`
- `praxis workflow query ...`
- `praxis workflow recall ...`

Drop to `praxis workflow tools describe <tool>` or `praxis workflow tools call <tool> --input-json '{...}' --yes` only for decision and policy surfaces that do not yet have a stable dedicated command.

## Core Laws

1. Conversation history is not authority.
2. The decisions table is for durable decisions, not generic summaries.
3. Architecture policy belongs in `praxis_operator_architecture_policy`, not in `operator_decisions` or markdown drift.
4. Bugs, roadmap rows, and closeout receipts keep their own write seams; do not stuff them into decisions just because they were discussed.
5. If a stable `praxis workflow <noun>` command exists, prefer it over a raw tool name.
6. Do not invent ids, decision keys, roadmap ids, bug ids, or provenance.
7. Read first when duplicate risk is non-trivial.
8. Finish with an explicit list of what was persisted, what authority owns it, and what still lacks a safe home.
9. A fix that shipped without a bug row still lacks durable defect authority; file and resolve it when the conversation clearly fixed a real bug.
10. Enhancements, hardening, and planned follow-up work belong in roadmap authority unless they are genuinely describing a defect.

## Workflow

### 1. Extract durable outcomes from the conversation

Separate the thread into explicit candidate records:

- architecture decisions
- other operator decisions
- new roadmap work or enhancements
- defects still needing work
- defects fixed in-thread but never tracked
- closeout-ready roadmap or bug work
- bugs discovered
- residual knowledge worth recall later

Reject fluff:

- status chatter
- speculative ideas that were not chosen
- implementation narration with no durable consequence

### 2. Match each outcome to one authority

Use the strongest write seam available.

When the conversation includes implementation work or follow-up obligations, use this split:

- defect still broken, deferred, or only partially mitigated -> `praxis workflow bugs file --title "<title>" --severity <P0|P1|P2|P3> --category <category> --description "<description>" --filed-by "<actor>" --source-kind <source_kind>`
- defect fixed in this thread but never tracked -> `praxis workflow bugs file --title "<title>" --severity <P0|P1|P2|P3> --category <category> --description "<description>" --filed-by "<actor>" --source-kind <source_kind>`, then `attach_evidence`, then `resolve` only after real verification evidence exists
- enhancement, hardening, refactor, or new capability request -> `praxis workflow roadmap write <preview|validate|commit> --title <title> --intent-brief <brief>`
- already tracked bug or roadmap item now complete -> `praxis workflow roadmap closeout <preview|commit> [--bug-id <id>]... [--roadmap-item-id <id>]...`

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

Only use this path when you can provide the minimum durable packet:

- `decision_key`
- `decision_kind`
- `title`
- `rationale`
- `decided_by`
- `decision_source`
- `decision_scope_kind` and `decision_scope_ref` when the decision kind requires typed scope

If you cannot satisfy that packet cleanly, do not write a generic decision row.

Use roadmap write authority through the stable CLI surface:

```text
praxis workflow roadmap write preview --title "<title>" --intent-brief "<brief>"
praxis workflow roadmap write validate --title "<title>" --intent-brief "<brief>"
praxis workflow roadmap write commit --title "<title>" --intent-brief "<brief>"
```

Use closeout when the conversation finished already-tracked work:

```text
praxis workflow roadmap closeout preview [--bug-id <id>]... [--roadmap-item-id <id>]...
praxis workflow roadmap closeout commit [--bug-id <id>]... [--roadmap-item-id <id>]...
```

Use bug authority through the stable CLI surface:

```text
praxis workflow bugs search "<symptom or title>"
praxis workflow bugs file --title "<title>" --severity <P0|P1|P2|P3> --category <category> --description "<description>" --filed-by "<actor>" --source-kind <source_kind>
praxis workflow bugs attach_evidence ...
praxis workflow bugs resolve ...
```

Use ingest only when no stronger operator table exists and the content still deserves durable recall:

```text
praxis workflow tools describe praxis_ingest
praxis workflow recall "<distinctive phrase from the ingested content>"
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

Before recording, confirm the decision kind can carry the scope you plan to write. Some kinds require typed scope and will reject underspecified rows.

Before writing, list similar rows when collision risk is real:

```text
praxis workflow tools call praxis_operator_decisions --input-json '{"action":"list","decision_kind":"architecture_policy","limit":20}'
```

### 4. Persist the rest of the work without collapsing authority

Use the correct table for the actual object:

- new roadmap item or enhancement -> `praxis workflow roadmap write <preview|validate|commit> --title <title> --intent-brief <brief>`
- new bug for a defect still needing work -> `praxis workflow bugs file --title "<title>" --severity <P0|P1|P2|P3> --category <category> --description "<description>" --filed-by "<actor>" --source-kind <source_kind>`, plus immediate evidence attachment when authoritative provenance exists
- defect fixed during the conversation but not previously tracked -> duplicate check first with `praxis workflow bugs search`, then `file`, attach `observed_in`, `attempted_fix`, and `validates_fix` evidence as available, then `resolve` to `FIXED` only after validation evidence is attached
- completed roadmap item or bug set -> discover the exact existing ids first, run `praxis workflow roadmap closeout preview`, inspect the reconciliation result, then `commit` only those exact `bug_ids` and `roadmap_item_ids`
- recallable conversation artifact with no better home -> `praxis_ingest` using `kind:"conversation"` or another schema-valid kind

Do not create a decision row just to say that work happened. Do not skip bug creation just because the patch already landed, and do not file an enhancement as a bug just because it was discovered while fixing something else.

### 5. Verify by reading back through the authority surface

After mutation, verify through the same operator lane or a read lane that sees the same truth:

- `praxis workflow tools call praxis_operator_decisions --input-json '{"action":"list",...}'`
- `praxis workflow query "<what changed?>"`
- `praxis workflow bugs ...`
- `praxis workflow roadmap view [--root <roadmap_item_id>]`
- `praxis workflow recall "<distinctive phrase from the ingested conversation record>"`

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
- using raw tool calls for roadmap writes or closeout when the stable `praxis workflow roadmap` surface already exists
- inventing write payload fields without `praxis workflow tools describe ...`
- calling a shipped fix "done" without either a resolved bug row or a closeout receipt under the correct authority
- filing enhancements as bugs, or leaving real follow-up enhancements as chat residue instead of roadmap rows
