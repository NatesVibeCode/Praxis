# Praxis MCP Tools

Praxis exposes 48 catalog-backed tools via the [Model Context Protocol](https://modelcontextprotocol.io/).

CLI discovery is generated from the same catalog metadata:

- `workflow tools list`
- `workflow tools search <text> [--exact]`
- `workflow tools describe <tool|alias>`
- `workflow tools call <tool|alias> --input-json '{...}'`
- single-result searches print the direct describe and entrypoint commands

## Catalog Summary

| Tool | Surface | Tier | Alias | Risks | Description |
| --- | --- | --- | --- | --- | --- |
| `praxis_discover` | `code` | `stable` | `workflow discover` | `read`, `write` | Find existing code that already does what you need — BEFORE writing new code. Uses hybrid retrieval: vector embeddings over AST-extracted behavioral fingerprints plus Postgres full-text search, fused with reciprocal rank fusion so you get both semantic and exact-ish matches even when naming differs. |
| `praxis_data` | `data` | `stable` | `workflow data` | `dispatch`, `read`, `write` | Run deterministic data cleanup and reconciliation jobs: parse datasets, profile fields, filter records, sort rows, normalize values, repair rows, run repair loops, backfill missing values, redact sensitive fields, checkpoint state, replay cursor windows, approve plans, apply approved plans, validate contracts, transform records, join or merge sources, aggregate groups, split partitions, export shaped datasets, dedupe keys, route dead-letter rows, reconcile source vs target state, sync target state deterministically, generate workflow specs, and launch those jobs through Praxis. |
| `praxis_artifacts` | `evidence` | `stable` | `workflow artifacts` | `read` | Browse and compare files produced by workflow sandbox runs. Each workflow job can write artifacts (code, logs, reports) — this tool lets you find, search, and diff them. |
| `praxis_bugs` | `evidence` | `stable` | `workflow bugs` | `dispatch`, `read`, `write` | Track bugs in the platform's Postgres-backed bug tracker. List open bugs, file new ones, search by keyword, inspect similar historical fixes, replay a bug from canonical evidence, bulk backfill replay provenance, or resolve existing bugs. |
| `praxis_constraints` | `evidence` | `advanced` | - | `read` | View automatically-mined constraints from past workflow failures. The system learns rules like 'files in runtime/ must include imports' from repeated failures. |
| `praxis_friction` | `evidence` | `advanced` | - | `read` | View the friction ledger — a record of every time a guardrail blocked or warned about an action (scope violations, secret leaks, policy bounces). |
| `praxis_receipts` | `evidence` | `advanced` | - | `read` | Search through past workflow results and analyze costs. Every workflow run produces receipts — this tool lets you search them by keyword and analyze token/cost spending. |
| `praxis_governance` | `governance` | `advanced` | - | `read` | Safety checks before dispatching work. Scan prompts for leaked secrets (API keys, tokens, passwords) or verify that a set of file paths falls within allowed scope. |
| `praxis_heal` | `governance` | `advanced` | - | `read` | Diagnose why a workflow job failed and get a recommended recovery action: retry (transient error), escalate (needs human attention), skip (non-critical), or halt (stop the pipeline). |
| `praxis_integration` | `integration` | `advanced` | - | `dispatch`, `read` | Call, list, or describe registered integrations (API connectors, webhooks, and other external services). |
| `praxis_provider_onboard` | `integration` | `advanced` | - | `read`, `write` | Onboard a CLI or API provider into Praxis Engine. Probes transport, discovers models, tests capacity, writes to all routing tables, and updates native runtime authority. |
| `praxis_graph` | `knowledge` | `advanced` | - | `read` | Explore connections from one knowledge-graph entity. Shows what an entity depends on, what depends on it, and the blast radius of changes. |
| `praxis_ingest` | `knowledge` | `advanced` | - | `write` | Store new information in the knowledge graph so it can be recalled later via praxis_recall. Content is automatically entity-extracted, deduplicated, and embedded for vector search. |
| `praxis_recall` | `knowledge` | `stable` | `workflow recall` | `read` | Search the platform's knowledge graph for information about modules, functions, decisions, patterns, bugs, constraints, people, or any previously ingested content. Returns ranked results with confidence scores and how each result was found (text match, graph traversal, or vector similarity). |
| `praxis_research` | `knowledge` | `stable` | - | `read` | Search the knowledge graph specifically for research findings and analysis results. Lighter-weight than praxis_recall — focused on retrieving prior research. |
| `praxis_circuits` | `operations` | `stable` | `workflow circuits` | `read`, `write` | Inspect effective circuit-breaker state or apply a durable manual override for one provider. |
| `praxis_diagnose` | `operations` | `stable` | `workflow diagnose` | `read` | Diagnose one workflow run by id. Combines the receipt, failure classification, and provider health into a single operator-facing report. |
| `praxis_health` | `operations` | `stable` | `workflow health` | `read` | Full system health check — Postgres connectivity, disk space, operator panel state, workflow lane recommendations, context cache stats, and memory graph health. |
| `praxis_heartbeat` | `operations` | `advanced` | - | `read`, `write` | Run or check the knowledge graph maintenance cycle. The heartbeat syncs receipts, bugs, constraints, and friction events into the knowledge graph, mines relationships between entities, generates daily/weekly rollups, and archives stale nodes. |
| `praxis_maintenance` | `operations` | `advanced` | - | `write` | Run explicit operator maintenance actions that mutate observability aggregates. |
| `praxis_reload` | `operations` | `advanced` | - | `write` | Clear all in-process caches so DB and config changes take effect without restarting Claude Desktop. |
| `praxis_status` | `operations` | `advanced` | - | `read` | Quick snapshot of how workflows are performing — total runs, pass/fail rate, categorized failure breakdown by zone (external/config/internal), and adjusted pass rate that excludes external provider failures. |
| `praxis_operator_architecture_policy` | `operator` | `advanced` | - | `write` | Record a durable architecture-policy decision in operator authority. |
| `praxis_operator_closeout` | `operator` | `advanced` | - | `read`, `write` | Preview or commit proof-backed bug and roadmap closeout through the shared reconciliation gate. |
| `praxis_operator_decisions` | `operator` | `advanced` | - | `read`, `write` | List or record canonical operator decisions through the shared operator_decisions table. |
| `praxis_operator_native_primary_cutover_gate` | `operator` | `advanced` | - | `write` | Admit a native primary cutover gate into operator-control decision and gate authority tables. |
| `praxis_operator_roadmap_view` | `operator` | `advanced` | - | `read` | Read one roadmap subtree and its dependency edges from DB-backed authority. |
| `praxis_operator_view` | `operator` | `advanced` | - | `read` | Render detailed operator observability views — deeper than praxis_status. |
| `praxis_operator_write` | `operator` | `advanced` | - | `read`, `write` | Preview, validate, or commit roadmap rows through the shared operator-write validation gate. |
| `praxis_decompose` | `planning` | `stable` | - | `read` | Break down a large objective into small, workflow-ready micro-sprints. Returns each sprint with estimated complexity, dependencies between sprints, and the critical path. |
| `praxis_intent_match` | `planning` | `stable` | - | `read` | Find existing UI components, workflows, and integrations that match what you want to build. Searches the registry and proposes how to compose them into an app. |
| `praxis_manifest_generate` | `planning` | `advanced` | - | `write` | Generate a complete app manifest (UI layout, data flow, integrations) from a natural language description. Combines intent matching with LLM generation to produce a ready-to-render manifest. |
| `praxis_manifest_refine` | `planning` | `advanced` | - | `write` | Iterate on a previously generated app manifest. Apply user feedback to adjust layout, add/remove modules, change data sources, or modify behavior. |
| `praxis_session` | `planning` | `advanced` | - | `read` | View or validate session carry-forward packs — compressed context snapshots that help new sessions pick up where previous ones left off. |
| `praxis_query` | `query` | `stable` | `workflow query` | `read` | Ask any question about the system in plain English. This is the best starting point when you're unsure which tool to use — it automatically routes your question to the right subsystem. Think of it as a router, not as the deep authority for every domain. |
| `praxis_research_workflow` | `research` | `advanced` | - | `dispatch`, `read` | Run a parallel multi-angle research workflow on any topic. One call generates a workflow spec (seed decomposition, N parallel research workers via replicate, synthesis) and launches it through the service bus. |
| `praxis_context_shard` | `session` | `session` | - | `session` | Return the bounded execution shard for the current workflow MCP session. This is only valid inside workflow Docker jobs using the signed MCP bridge. |
| `praxis_session_context` | `session` | `session` | - | `session` | Read or write persistent context on your agent session. Context survives across tool calls and is available on retry. |
| `praxis_subscribe_events` | `session` | `session` | - | `session` | Pull build state events since the agent's last cursor position. Returns new events and advances the cursor. Call repeatedly to stay in sync with platform state changes. |
| `praxis_get_submission` | `submissions` | `session` | - | `session` | Read a sealed workflow submission within the current workflow MCP session. The session token owns run_id/workflow_id and the tool only accepts submission_id or job_label for the target submission. |
| `praxis_review_submission` | `submissions` | `session` | - | `session` | Review a sealed workflow submission within the current workflow MCP session. The session token owns run_id/workflow_id/job_label for the reviewer. The tool only accepts submission_id or job_label for the target submission. |
| `praxis_submit_artifact_bundle` | `submissions` | `session` | - | `session` | Submit a sealed artifact bundle result for the current workflow MCP session. The session token owns run_id, workflow_id, and job_label. This tool never accepts those ids as input and returns structured errors instead of stack traces. |
| `praxis_submit_code_change` | `submissions` | `session` | - | `session` | Submit a sealed code-change result for the current workflow MCP session. The session token owns run_id, workflow_id, and job_label. This tool never accepts those ids as input and returns structured errors instead of stack traces. |
| `praxis_submit_research_result` | `submissions` | `session` | - | `session` | Submit a sealed research result for the current workflow MCP session. The session token owns run_id, workflow_id, and job_label. This tool never accepts those ids as input and returns structured errors instead of stack traces. |
| `praxis_connector` | `workflow` | `advanced` | - | `dispatch`, `read`, `write` | Build API connectors for third-party applications. One call stamps a workflow spec and launches a 4-job pipeline (discover API → map objects → build client → review). |
| `praxis_wave` | `workflow` | `advanced` | - | `dispatch`, `read`, `write` | Manage execution waves — groups of jobs with dependency ordering. Waves track which jobs are runnable (all dependencies met) and which are blocked. |
| `praxis_workflow` | `workflow` | `advanced` | - | `dispatch`, `read`, `write` | Execute work by launching a workflow for LLM agents. This is the primary way to run tasks — building code, running tests, writing reviews, refactoring, and debates. |
| `praxis_workflow_validate` | `workflow` | `advanced` | - | `read` | Dry-run a workflow spec to check for errors before executing it. Returns whether the spec is valid, how many jobs it contains, and which agents each job resolves to. |

## Tool Reference

### Code

#### `praxis_discover`

- Surface: `code`
- Tier: `stable`
- Badges: `stable`, `code`, `alias:discover`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow discover`
- CLI schema help: `workflow tools describe praxis_discover`
- When to use: Search for existing code by behavior with hybrid retrieval before building something new.
- When not to use: Do not use it for architectural decisions or receipt analytics.
- Recommended alias: `workflow discover`
- Selector: `action`; default `search`; values `search`, `reindex`, `stats`
- Required args: (none)

Example input:

```json
{
  "action": "search",
  "query": "retry logic with exponential backoff"
}
```

### Data

#### `praxis_data`

- Surface: `data`
- Tier: `stable`
- Badges: `stable`, `data`, `alias:data`, `mutates-state`, `dispatches-work`
- Risks: `dispatch`, `read`, `write`
- CLI entrypoint: `workflow data`
- CLI schema help: `workflow tools describe praxis_data`
- When to use: Run deterministic parsing, normalization, validation, mapping, dedupe, or reconcile jobs and optionally launch them through the workflow engine.
- When not to use: Do not use it for fuzzy inference, free-form classification, or cases where an LLM must invent the transform logic.
- Recommended alias: `workflow data`
- Selector: `action`; default `profile`; values `parse`, `profile`, `filter`, `sort`, `normalize`, `repair`, `repair_loop`, `backfill`, `redact`, `checkpoint`, `replay`, `approve`, `apply`, `validate`, `transform`, `join`, `merge`, `aggregate`, `split`, `export`, `dead_letter`, `dedupe`, `reconcile`, `sync`, `run`, `workflow_spec`, `launch`
- Required args: (none)

Example input:

```json
{
  "action": "profile",
  "input_path": "artifacts/data/users.csv"
}
```

### Evidence

#### `praxis_artifacts`

- Surface: `evidence`
- Tier: `stable`
- Badges: `stable`, `evidence`, `alias:artifacts`
- Risks: `read`
- CLI entrypoint: `workflow artifacts`
- CLI schema help: `workflow tools describe praxis_artifacts`
- When to use: Browse sandbox outputs, search artifact paths, or compare generated files.
- When not to use: Do not use it for workflow receipt history or knowledge-graph recall.
- Recommended alias: `workflow artifacts`
- Selector: `action`; default `stats`; values `stats`, `list`, `search`, `diff`
- Required args: (none)

Example input:

```json
{
  "action": "list"
}
```

#### `praxis_bugs`

- Surface: `evidence`
- Tier: `stable`
- Badges: `stable`, `evidence`, `alias:bugs`, `mutates-state`, `dispatches-work`
- Risks: `dispatch`, `read`, `write`
- CLI entrypoint: `workflow bugs`
- CLI schema help: `workflow tools describe praxis_bugs`
- When to use: Inspect the bug tracker, run keyword or hybrid search, file a new bug, or drive replay-ready bug workflows.
- When not to use: Do not use it for general system status or semantic knowledge search.
- Recommended alias: `workflow bugs`
- Selector: `action`; default `list`; values `list`, `file`, `search`, `stats`, `packet`, `history`, `replay`, `backfill_replay`, `attach_evidence`, `patch_resume`, `resolve`
- Required args: (none)

Example input:

```json
{
  "action": "list",
  "status": "OPEN",
  "severity": "P1"
}
```

#### `praxis_constraints`

- Surface: `evidence`
- Tier: `advanced`
- Badges: `advanced`, `evidence`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_constraints`
- CLI schema help: `workflow tools describe praxis_constraints`
- When to use: Inspect mined constraints and scope-specific guardrails.
- When not to use: Do not use it for code similarity or bug enumeration.
- Selector: `action`; default `list`; values `list`, `for_scope`
- Required args: (none)

Example input:

```json
{
  "action": "list"
}
```

#### `praxis_friction`

- Surface: `evidence`
- Tier: `advanced`
- Badges: `advanced`, `evidence`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_friction`
- CLI schema help: `workflow tools describe praxis_friction`
- When to use: Inspect friction and guardrail events that are slowing workflows down.
- When not to use: Do not use it for health probes or general bug search.
- Selector: `action`; default `stats`; values `stats`, `list`
- Required args: (none)

Example input:

```json
{
  "action": "stats"
}
```

#### `praxis_receipts`

- Surface: `evidence`
- Tier: `advanced`
- Badges: `advanced`, `evidence`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_receipts`
- CLI schema help: `workflow tools describe praxis_receipts`
- When to use: Search workflow receipts or inspect token burn and execution evidence.
- When not to use: Do not use it for current health or knowledge-graph recall.
- Selector: `action`; default `search`; values `search`, `token_burn`
- Required args: (none)

Example input:

```json
{
  "action": "search",
  "query": "sandbox timeout"
}
```

### Governance

#### `praxis_governance`

- Surface: `governance`
- Tier: `advanced`
- Badges: `advanced`, `governance`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_governance`
- CLI schema help: `workflow tools describe praxis_governance`
- When to use: Scan prompts and scope for policy, secret, or governance violations.
- When not to use: Do not use it as a general quality dashboard or health probe.
- Selector: `action`; default `scan_prompt`; values `scan_prompt`, `scan_scope`
- Required args: (none)

Example input:

```json
{
  "action": "scan_prompt",
  "text": "Ship the API key in the test fixture"
}
```

#### `praxis_heal`

- Surface: `governance`
- Tier: `advanced`
- Badges: `advanced`, `governance`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_heal`
- CLI schema help: `workflow tools describe praxis_heal`
- When to use: Diagnose failures and propose healing actions with platform-specific guidance.
- When not to use: Do not use it as a generic health command or workflow launcher.
- Selector: none
- Required args: `job_label`

Example input:

```json
{
  "job_label": "build",
  "failure_code": "sandbox.timeout",
  "stderr": "command timed out"
}
```

### Integration

#### `praxis_integration`

- Surface: `integration`
- Tier: `advanced`
- Badges: `advanced`, `integration`, `dispatches-work`
- Risks: `dispatch`, `read`
- CLI entrypoint: `workflow tools call praxis_integration`
- CLI schema help: `workflow tools describe praxis_integration`
- When to use: List integrations, inspect one, validate credentials, or invoke an integration action.
- When not to use: Do not use it to build connectors or launch workflows.
- Selector: `action`; default `list`; values `call`, `list`, `describe`, `test_credentials`, `health`
- Required args: (none)

Example input:

```json
{
  "action": "list"
}
```

#### `praxis_provider_onboard`

- Surface: `integration`
- Tier: `advanced`
- Badges: `advanced`, `integration`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_provider_onboard`
- CLI schema help: `workflow tools describe praxis_provider_onboard`
- When to use: Probe or onboard a new provider/model route into the platform.
- When not to use: Do not use it for ordinary model selection or workflow dispatch.
- Selector: `action`; default `probe`; values `probe`, `onboard`
- Required args: `provider_slug`

Example input:

```json
{
  "action": "probe",
  "provider_slug": "openrouter",
  "transport": "api"
}
```

### Knowledge

#### `praxis_graph`

- Surface: `knowledge`
- Tier: `advanced`
- Badges: `advanced`, `knowledge`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_graph`
- CLI schema help: `workflow tools describe praxis_graph`
- When to use: Inspect blast radius and graph neighbors for a known or latest knowledge-graph entity.
- When not to use: Do not use it for broad knowledge search; use recall first when you need ranked candidates.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "depth": 1
}
```

#### `praxis_ingest`

- Surface: `knowledge`
- Tier: `advanced`
- Badges: `advanced`, `knowledge`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_ingest`
- CLI schema help: `workflow tools describe praxis_ingest`
- When to use: Persist new documents, build events, or research into the knowledge graph.
- When not to use: Do not use it for ad hoc questions where nothing should be persisted.
- Selector: none
- Required args: `kind`, `content`, `source`

Example input:

```json
{
  "kind": "document",
  "source": "catalog/runtime",
  "content": "# Runtime catalog"
}
```

#### `praxis_recall`

- Surface: `knowledge`
- Tier: `stable`
- Badges: `stable`, `knowledge`, `alias:recall`
- Risks: `read`
- CLI entrypoint: `workflow recall`
- CLI schema help: `workflow tools describe praxis_recall`
- When to use: Search the knowledge graph for decisions, patterns, entities, and prior analysis using ranked text, graph, and vector retrieval.
- When not to use: Do not use it for code similarity or workflow receipt queries.
- Recommended alias: `workflow recall`
- Selector: none
- Required args: `query`

Example input:

```json
{
  "query": "provider routing",
  "entity_type": "decision"
}
```

#### `praxis_research`

- Surface: `knowledge`
- Tier: `stable`
- Badges: `stable`, `knowledge`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_research`
- CLI schema help: `workflow tools describe praxis_research`
- When to use: Search prior research findings and analysis results with a lighter-weight surface than recall.
- When not to use: Do not use it for general knowledge or code search.
- Selector: `action`; default `search`; values `search`
- Required args: `query`

Example input:

```json
{
  "action": "search",
  "query": "provider routing performance"
}
```

### Operations

#### `praxis_circuits`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `alias:circuits`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow circuits`
- CLI schema help: `workflow tools describe praxis_circuits`
- When to use: Inspect effective circuit-breaker state or apply a durable manual override for one provider.
- When not to use: Do not use it for task-route eligibility windows or generic health checks.
- Recommended alias: `workflow circuits`
- Selector: `action`; default `list`; values `list`, `history`, `open`, `close`, `reset`
- Required args: (none)

Example input:

```json
{
  "action": "list"
}
```

#### `praxis_diagnose`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `alias:diagnose`
- Risks: `read`
- CLI entrypoint: `workflow diagnose`
- CLI schema help: `workflow tools describe praxis_diagnose`
- When to use: Diagnose one workflow run by id and combine receipt, failure, and provider health context.
- When not to use: Do not use it for broad health checks or generic receipt search.
- Recommended alias: `workflow diagnose`
- Selector: none
- Required args: `run_id`

Example input:

```json
{
  "run_id": "run_abc123"
}
```

#### `praxis_health`

- Surface: `operations`
- Tier: `stable`
- Badges: `stable`, `operations`, `alias:health`
- Risks: `read`
- CLI entrypoint: `workflow health`
- CLI schema help: `workflow tools describe praxis_health`
- When to use: Run a full preflight before dispatch or when the platform feels degraded.
- When not to use: Do not use it to inspect one specific workflow run.
- Recommended alias: `workflow health`
- Selector: none
- Required args: (none)

Example input:

```json
{}
```

#### `praxis_heartbeat`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_heartbeat`
- CLI schema help: `workflow tools describe praxis_heartbeat`
- When to use: Check or run the knowledge-graph maintenance cycle.
- When not to use: Do not use it as a replacement for workflow dispatch or session recall.
- Selector: `action`; default `status`; values `run`, `status`
- Required args: (none)

Example input:

```json
{
  "action": "status"
}
```

#### `praxis_maintenance`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_maintenance`
- CLI schema help: `workflow tools describe praxis_maintenance`
- When to use: Run explicit operator maintenance actions that mutate runtime state.
- When not to use: Do not use it for ordinary observability or health reading.
- Selector: `action`; default `reset_metrics`; values `reset_metrics`, `backfill_bug_replay_provenance`
- Required args: (none)

Example input:

```json
{
  "action": "reset_metrics",
  "confirm": true
}
```

#### `praxis_reload`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_reload`
- CLI schema help: `workflow tools describe praxis_reload`
- When to use: Clear in-process caches after changing runtime config or MCP catalog state.
- When not to use: Do not use it as a routine health command.
- Selector: none
- Required args: (none)

Example input:

```json
{}
```

#### `praxis_status`

- Surface: `operations`
- Tier: `advanced`
- Badges: `advanced`, `operations`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_status`
- CLI schema help: `workflow tools describe praxis_status`
- When to use: Inspect workflow pass rate, failure mix, and in-flight run summaries from receipts.
- When not to use: Do not use it for deep health probes or workflow dispatch.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "since_hours": 24
}
```

### Operator

#### `praxis_operator_architecture_policy`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_operator_architecture_policy`
- CLI schema help: `workflow tools describe praxis_operator_architecture_policy`
- When to use: Record one typed architecture policy decision in operator_decisions when explicit guidance should become durable control authority.
- When not to use: Do not use it for generic decision history reads; use praxis_operator_decisions for that.
- Selector: none
- Required args: `authority_domain`, `policy_slug`, `title`, `rationale`, `decided_by`, `decision_source`

Example input:

```json
{
  "authority_domain": "decision_tables",
  "policy_slug": "db-native-authority",
  "title": "Decision tables are DB-native authority",
  "rationale": "Keep authority in Postgres.",
  "decided_by": "nate",
  "decision_source": "cto.guidance"
}
```

#### `praxis_operator_closeout`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_operator_closeout`
- CLI schema help: `workflow tools describe praxis_operator_closeout`
- When to use: Preview or commit operator work-item closeout through the shared gate.
- When not to use: Do not use it for roadmap item creation or read-only status views.
- Selector: `action`; default `preview`; values `preview`, `commit`
- Required args: (none)

Example input:

```json
{
  "action": "preview",
  "work_item_id": "WI-123"
}
```

#### `praxis_operator_decisions`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_operator_decisions`
- CLI schema help: `workflow tools describe praxis_operator_decisions`
- When to use: List or record durable operator decisions such as architecture policy rows in the canonical operator_decisions table.
- When not to use: Do not use it for roadmap item authoring or cutover-gate admission.
- Selector: `action`; default `list`; values `list`, `record`
- Required args: (none)

Example input:

```json
{
  "action": "list",
  "decision_kind": "architecture_policy"
}
```

#### `praxis_operator_native_primary_cutover_gate`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_operator_native_primary_cutover_gate`
- CLI schema help: `workflow tools describe praxis_operator_native_primary_cutover_gate`
- When to use: Admit a native-primary cutover gate with required decision metadata into operator-control.
- When not to use: Do not use it for read-only operator status views.
- Selector: none
- Required args: `decided_by`, `decision_source`, `rationale`

Example input:

```json
{
  "decided_by": "operator-auto",
  "decision_source": "runbook",
  "rationale": "manual rollout hold ended",
  "roadmap_item_id": "roadmap_item.platform.deploy"
}
```

#### `praxis_operator_roadmap_view`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_operator_roadmap_view`
- CLI schema help: `workflow tools describe praxis_operator_roadmap_view`
- When to use: Read roadmap and operator backlog views without mutating them.
- When not to use: Do not use it to commit roadmap changes.
- Selector: none
- Required args: (none)

Example input:

```json
{}
```

#### `praxis_operator_view`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_operator_view`
- CLI schema help: `workflow tools describe praxis_operator_view`
- When to use: Discover native operator status, scoreboard, topology, or replay-ready bug views.
- When not to use: Do not use it to mutate operator state.
- Selector: `view`; default `status`; values `status`, `scoreboard`, `graph`, `lineage`, `replay_ready_bugs`
- Required args: `view`

Example input:

```json
{
  "view": "graph",
  "run_id": "run_123"
}
```

#### `praxis_operator_write`

- Surface: `operator`
- Tier: `advanced`
- Badges: `advanced`, `operator`, `mutates-state`
- Risks: `read`, `write`
- CLI entrypoint: `workflow tools call praxis_operator_write`
- CLI schema help: `workflow tools describe praxis_operator_write`
- When to use: Preview, validate, or commit roadmap writes through the operator gate.
- When not to use: Do not use it for read-only backlog inspection.
- Selector: `action`; default `preview`; values `preview`, `validate`, `commit`
- Required args: `title`, `intent_brief`

Example input:

```json
{
  "action": "preview",
  "title": "Consolidate CLI frontdoors",
  "intent_brief": "one authority for operator CLI"
}
```

### Planning

#### `praxis_decompose`

- Surface: `planning`
- Tier: `stable`
- Badges: `stable`, `planning`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_decompose`
- CLI schema help: `workflow tools describe praxis_decompose`
- When to use: Break a large objective into workflow-sized micro-sprints before dispatch.
- When not to use: Do not use it to execute work or inspect historical run state.
- Selector: none
- Required args: `objective`

Example input:

```json
{
  "objective": "Consolidate operator read and write surfaces"
}
```

#### `praxis_intent_match`

- Surface: `planning`
- Tier: `stable`
- Badges: `stable`, `planning`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_intent_match`
- CLI schema help: `workflow tools describe praxis_intent_match`
- When to use: Match a product intent against existing platform components before generating a manifest.
- When not to use: Do not use it for code search or historical run analysis.
- Selector: none
- Required args: `intent`

Example input:

```json
{
  "intent": "invoice approval workflow with status tracking"
}
```

#### `praxis_manifest_generate`

- Surface: `planning`
- Tier: `advanced`
- Badges: `advanced`, `planning`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_manifest_generate`
- CLI schema help: `workflow tools describe praxis_manifest_generate`
- When to use: Generate a new manifest from an intent after you've confirmed the building blocks.
- When not to use: Do not use it for code execution or connector onboarding.
- Selector: none
- Required args: `intent`

Example input:

```json
{
  "intent": "customer onboarding pipeline with approval steps"
}
```

#### `praxis_manifest_refine`

- Surface: `planning`
- Tier: `advanced`
- Badges: `advanced`, `planning`, `mutates-state`
- Risks: `write`
- CLI entrypoint: `workflow tools call praxis_manifest_refine`
- CLI schema help: `workflow tools describe praxis_manifest_refine`
- When to use: Iterate on an existing generated manifest based on feedback.
- When not to use: Do not use it without a manifest id from a prior generation step.
- Selector: none
- Required args: `manifest_id`, `feedback`

Example input:

```json
{
  "manifest_id": "manifest_abc123",
  "feedback": "Add weekly trends and remove the status grid"
}
```

#### `praxis_session`

- Surface: `planning`
- Tier: `advanced`
- Badges: `advanced`, `planning`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_session`
- CLI schema help: `workflow tools describe praxis_session`
- When to use: Inspect or validate session carry-forward packs between work sessions.
- When not to use: Do not use it as a live workflow-session context surface.
- Selector: `action`; default `latest`; values `latest`, `validate`
- Required args: (none)

Example input:

```json
{
  "action": "latest"
}
```

### Query

#### `praxis_query`

- Surface: `query`
- Tier: `stable`
- Badges: `stable`, `query`, `alias:query`
- Risks: `read`
- CLI entrypoint: `workflow query`
- CLI schema help: `workflow tools describe praxis_query`
- When to use: Route a natural-language question to the right platform subsystem from the terminal when you are not sure which exact tool to use.
- When not to use: Do not use it when you already know the exact specialist tool you need.
- Recommended alias: `workflow query`
- Selector: none
- Required args: `question`

Example input:

```json
{
  "question": "what is failing right now?"
}
```

### Research

#### `praxis_research_workflow`

- Surface: `research`
- Tier: `advanced`
- Badges: `advanced`, `research`, `dispatches-work`
- Risks: `dispatch`, `read`
- CLI entrypoint: `workflow tools call praxis_research_workflow`
- CLI schema help: `workflow tools describe praxis_research_workflow`
- When to use: Launch or inspect fan-out research workflows for deeper multi-angle investigations.
- When not to use: Do not use it for single-shot questions where recall or query is enough.
- Selector: `action`; default `run`; values `run`, `list`
- Required args: (none)

Example input:

```json
{
  "action": "run",
  "topic": "best practices for durable MCP transports",
  "workers": 8
}
```

### Session

#### `praxis_context_shard`

- Surface: `session`
- Tier: `session`
- Badges: `session`, `session`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_context_shard`
- CLI schema help: `workflow tools describe praxis_context_shard`
- When to use: Read the bounded execution shard for the active workflow MCP session.
- When not to use: Do not use it outside workflow-session execution or as a general repository browser.
- Selector: `view`; default `full`; values `full`, `summary`, `sections`
- Required args: (none)

Example input:

```json
{
  "view": "summary"
}
```

#### `praxis_session_context`

- Surface: `session`
- Tier: `session`
- Badges: `session`, `session`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_session_context`
- CLI schema help: `workflow tools describe praxis_session_context`
- When to use: Read or write persistent context owned by the active workflow MCP session.
- When not to use: Do not use it outside token-scoped workflow execution.
- Selector: `action`; default `read`; values `read`, `write`
- Required args: (none)

Example input:

```json
{
  "action": "read"
}
```

#### `praxis_subscribe_events`

- Surface: `session`
- Tier: `session`
- Badges: `session`, `session`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_subscribe_events`
- CLI schema help: `workflow tools describe praxis_subscribe_events`
- When to use: Poll workflow-scoped event updates since the last cursor position for the active session.
- When not to use: Do not use it outside token-scoped workflow execution.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "channel": "build_state",
  "limit": 50
}
```

### Submissions

#### `praxis_get_submission`

- Surface: `submissions`
- Tier: `session`
- Badges: `session`, `submissions`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_get_submission`
- CLI schema help: `workflow tools describe praxis_get_submission`
- When to use: Read a sealed submission in the active workflow MCP session.
- When not to use: Do not use it outside token-scoped workflow review flows.
- Selector: none
- Required args: (none)

Example input:

```json
{
  "submission_id": "submission_abc123"
}
```

#### `praxis_review_submission`

- Surface: `submissions`
- Tier: `session`
- Badges: `session`, `submissions`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_review_submission`
- CLI schema help: `workflow tools describe praxis_review_submission`
- When to use: Approve, reject, or request changes on a sealed submission inside a workflow session.
- When not to use: Do not use it outside token-scoped workflow review flows.
- Selector: none
- Required args: `decision`, `summary`

Example input:

```json
{
  "submission_id": "submission_abc123",
  "decision": "approve",
  "summary": "Looks good"
}
```

#### `praxis_submit_artifact_bundle`

- Surface: `submissions`
- Tier: `session`
- Badges: `session`, `submissions`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_submit_artifact_bundle`
- CLI schema help: `workflow tools describe praxis_submit_artifact_bundle`
- When to use: Submit an artifact-bundle result owned by the active workflow session.
- When not to use: Do not use it outside token-scoped workflow execution.
- Selector: none
- Required args: `summary`, `primary_paths`, `result_kind`

Example input:

```json
{
  "summary": "Generated migration bundle",
  "primary_paths": [
    "artifacts/migrations"
  ],
  "result_kind": "artifact_bundle"
}
```

#### `praxis_submit_code_change`

- Surface: `submissions`
- Tier: `session`
- Badges: `session`, `submissions`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_submit_code_change`
- CLI schema help: `workflow tools describe praxis_submit_code_change`
- When to use: Submit a sealed code-change result owned by the active workflow session.
- When not to use: Do not use it outside token-scoped workflow execution.
- Selector: none
- Required args: `summary`, `primary_paths`, `result_kind`

Example input:

```json
{
  "summary": "Fixed MCP transport framing",
  "primary_paths": [
    "surfaces/mcp/protocol.py"
  ],
  "result_kind": "code_change"
}
```

#### `praxis_submit_research_result`

- Surface: `submissions`
- Tier: `session`
- Badges: `session`, `submissions`, `session-only`
- Risks: `session`
- CLI entrypoint: `workflow tools call praxis_submit_research_result`
- CLI schema help: `workflow tools describe praxis_submit_research_result`
- When to use: Submit a sealed research result owned by the active workflow session.
- When not to use: Do not use it outside token-scoped workflow execution.
- Selector: none
- Required args: `summary`, `primary_paths`, `result_kind`

Example input:

```json
{
  "summary": "Surveyed MCP CLI exposure patterns",
  "primary_paths": [
    "notes/research.md"
  ],
  "result_kind": "research_result"
}
```

### Workflow

#### `praxis_connector`

- Surface: `workflow`
- Tier: `advanced`
- Badges: `advanced`, `workflow`, `mutates-state`, `dispatches-work`
- Risks: `dispatch`, `read`, `write`
- CLI entrypoint: `workflow tools call praxis_connector`
- CLI schema help: `workflow tools describe praxis_connector`
- When to use: Build, inspect, register, or verify third-party API connectors.
- When not to use: Do not use it for invoking an existing integration at runtime.
- Selector: `action`; default `build`; values `build`, `list`, `get`, `register`, `verify`
- Required args: (none)

Example input:

```json
{
  "action": "build",
  "app_name": "Slack"
}
```

#### `praxis_wave`

- Surface: `workflow`
- Tier: `advanced`
- Badges: `advanced`, `workflow`, `mutates-state`, `dispatches-work`
- Risks: `dispatch`, `read`, `write`
- CLI entrypoint: `workflow tools call praxis_wave`
- CLI schema help: `workflow tools describe praxis_wave`
- When to use: Observe or coordinate wave-based execution programs.
- When not to use: Do not use it for single workflow runs with no wave orchestration.
- Selector: `action`; default `observe`; values `observe`, `start`, `next`, `record`
- Required args: (none)

Example input:

```json
{
  "action": "next"
}
```

#### `praxis_workflow`

- Surface: `workflow`
- Tier: `advanced`
- Badges: `advanced`, `workflow`, `mutates-state`, `dispatches-work`
- Risks: `dispatch`, `read`, `write`
- CLI entrypoint: `workflow tools call praxis_workflow`
- CLI schema help: `workflow tools describe praxis_workflow`
- When to use: Run, inspect, claim, acknowledge, retry, cancel, or list workflows through the MCP workflow surface.
- When not to use: Do not use it for natural-language questions or health checks.
- Selector: `action`; default `run`; values `run`, `spawn`, `status`, `inspect`, `claim`, `acknowledge`, `cancel`, `list`, `notifications`, `retry`, `repair`, `chain`
- Required args: (none)

Example input:

```json
{
  "action": "list"
}
```

#### `praxis_workflow_validate`

- Surface: `workflow`
- Tier: `advanced`
- Badges: `advanced`, `workflow`
- Risks: `read`
- CLI entrypoint: `workflow tools call praxis_workflow_validate`
- CLI schema help: `workflow tools describe praxis_workflow_validate`
- When to use: Validate a workflow spec before launching it.
- When not to use: Do not use it when you need to actually run the workflow.
- Selector: none
- Required args: `spec_path`

Example input:

```json
{
  "spec_path": "Code&DBs/Workflow/artifacts/workflow/operating_model_paradigm.queue.json"
}
```
