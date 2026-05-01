# Operator Guide

Day-2 operations for Praxis Engine. This guide covers what to do after `./scripts/bootstrap` succeeds: health monitoring, reading receipts, filing and closing decisions, working with patterns and bugs, managing providers, and using the Forge tools to extend the platform.

For concepts behind this guide see [CONCEPTS.md](CONCEPTS.md). For setup and first-run see `README.md` and `SETUP.md`.

## Health monitoring

**Quick status:**
```bash
praxis workflow query "status"
```
Returns standing orders, open bugs, recent runs, and active circuit breakers. Every line is a queryable row — this is Praxis showing you its current beliefs.

**Detailed health check:**
```bash
praxis workflow tools call praxis_health --input-json '{}'
```
Returns structured health for each subsystem: gateway, workflow runtime, provider adapters, knowledge graph, and storage. Look for `"status": "degraded"` or `"circuit_open": true` entries.

**What circuit breakers mean:** When a provider fails consistently, the router opens a circuit for that provider. The affected provider stops receiving jobs until the circuit closes (usually 5–15 minutes, depending on the policy). Circuit-open providers are visible in `praxis_health` output and are excluded from `auto/` routing — the router falls back to the next-best eligible model automatically.

**Watching a live run:**
```bash
praxis workflow run-status <run_id>
```
Shows job-level status, which jobs are running, which have failed, and which are waiting on dependencies.

---

## Understanding receipts

Every gateway dispatch writes a receipt to `authority_operation_receipts`. Receipts are not logs — they are queryable authority rows.

**Find receipts for a run:**
```bash
praxis workflow tools call praxis_receipts --input-json '{"query": "run_id:<run_id>", "limit": 20}'
```

**What a receipt tells you:**
- `input_hash` — deterministic hash of what was sent in. Two dispatches with the same input hash replay from cache when the idempotency policy allows.
- `output_hash` — hash of what came back. Compare across runs to confirm identical outputs.
- `execution_status` — `completed`, `replayed`, or `failed`.
- `cause_receipt_id` — if this operation was triggered by another, this links back. Lets you reconstruct causal chains.
- `duration_ms` — actual wall time for the dispatch.

**Replay:** Run any read operation twice with identical inputs and the gateway returns the cached receipt — no second dispatch, no second cost, provably identical output. Command operations with `idempotency_policy=idempotent` replay when safe; `non_idempotent` commands always run fresh.

---

## Operator decisions

Decisions are standing orders that persist across runs and steer agent behavior at compile time. They live in `operator_decisions` as queryable rows.

**View current decisions:**
```bash
praxis workflow query "architecture policies"
praxis workflow tools call praxis_recall --input-json '{"query": "standing orders", "limit": 10}'
```

**File a new decision:**
```bash
praxis workflow tools call praxis_bugs --input-json '{
  "action": "file",
  "title": "Always use scoped write envelopes for auth module jobs",
  "description": "...",
  "decision_kind": "architecture_policy"
}'
```

More precisely, decisions are filed through the operator surface. The key fields are `decision_kind` (e.g. `architecture_policy`, `delivery_plan`), a `decision_key` (slug format, used in standing-order hooks), and a `decision` body.

**Supersede a decision:** Filing a new decision that references an existing `decision_key` with `supersedes_ref` creates an explicit supersession history — the old decision becomes `superseded`, the new one is `active`. Agents see the current `active` rows at compile time; the full history is queryable for audit.

**Why decisions beat config files:** Config files are read once and forgotten. Decision rows are queryable, scoped by kind, inherited by sub-scopes, and referenced by receipts. When an agent picks a route, the decision it consulted is in the receipt. When a decision is superseded, the change is in the audit trail. Config files don't offer any of this.

---

## The pattern → primitive promotion loop

Praxis extracts recurring shapes from friction events, failures, and receipts and surfaces them as `patterns`. A pattern is not a bug — it's a structural signal that something keeps happening.

**See current patterns:**
```bash
praxis workflow tools call praxis_patterns --input-json '{"action": "list", "limit": 10}'
```

Pattern kinds: `architecture_smell`, `runtime_failure_pattern`, `operator_friction`, `missing_authority`, `weak_observability`.

**Promote a pattern to a primitive:**

When a pattern appears often enough and is well-understood, it can be promoted into `primitive_catalog` — graduating from observed behavior into declared platform capability. Before promotion, check consistency:

```bash
praxis workflow tools call praxis_patterns --input-json '{"action": "materialize_candidates"}'
```

This surfaces patterns eligible for promotion with their consistency check results. A pattern passes consistency when the declared shape matches what the code actually does.

Promotion is not automatic — it requires an operator decision and a verifier run. That's intentional: `primitive_catalog` is the platform's source of truth, and truth requires proof.

---

## Bug lifecycle

Bugs in Praxis are first-class authority objects. Filing a bug creates a queryable row with structured evidence; resolving it requires proof.

**File a bug:**
```bash
praxis workflow tools call praxis_bugs --input-json '{
  "action": "file",
  "title": "Short, searchable title",
  "description": "What failed, when, evidence",
  "priority": "P2"
}'
```

**Check for duplicates before filing:** The tracker deduplicates on vector similarity. Query first:
```bash
praxis workflow tools call praxis_bugs --input-json '{"action": "search", "query": "your bug description"}'
```

**Resolve a bug — requires a verifier run:**

Bugs cannot be claimed `FIXED` without pointing at a registered verifier authority that the tracker actually executes. Use `FIX_PENDING_VERIFICATION` until you can run the verifier.

```bash
praxis workflow tools call praxis_bugs --input-json '{
  "action": "resolve",
  "bug_id": "BUG-XXXXXXXX",
  "status": "FIXED",
  "verifier_ref": "verifier.job.python.pytest_file",
  "inputs": {"path": "/Users/nate/Praxis/Code&DBs/Workflow/tests/unit/test_X.py"},
  "target_kind": "path",
  "target_ref": "/Users/nate/Praxis/Code&DBs/Workflow/tests/unit/test_X.py"
}'
```

The tracker writes a `verification_runs` row with stdout/stderr/exit_code/latency, links it as `evidence_role=validates_fix`, and only then flips status to `FIXED`. "I think it's fixed" is not enough — only "the verifier ran and passed" is.

---

## Provider management

**Check current provider state:**
```bash
praxis workflow tools call praxis_health --input-json '{"include_providers": true}'
```

**List available routes:**
```bash
praxis workflow tools call praxis_recall --input-json '{"query": "task type routing available models"}'
```

**How routing works:** For `auto/` routes, the router filters providers to those with valid credentials and healthy status, filters models to those allowed by the runtime profile, scores each eligible model against the task type, and selects the highest-scoring model. If the selected model fails, the router falls back to the next-best eligible one — no silent route changes, only authorized alternatives.

**Tune task-type routing:** Routing scores are rows in `task_type_routing`. To change which model wins for `auto/build`, update the score for that provider/task-type pair. This is a DB write through the CQRS gateway, not a config file edit.

**Onboard a new provider:**
```bash
praxis workflow tools call praxis_provider_onboard --input-json '{
  "provider_slug": "new-provider",
  "transport": "cli"
}'
```

The onboarding tool probes transport, discovers models, writes onboarding authority, and runs the canonical post-onboarding sync. See [docs/PROVIDERS.md](PROVIDERS.md) for credential setup.

---

## Auth refresh

When a provider reports `auth_state: timeout` — after Docker Desktop / OrbStack / macOS restarts, or when the worker shows "Up" but failing auth probes:

```bash
make refresh
```

This exports macOS Keychain provider credentials, recreates the containers with fresh tokens, and verifies auth through `praxis_cli_auth_doctor` inside the worker. Do not run `docker compose up -d --force-recreate workflow-worker` directly — it skips the keychain re-export step.

---

## Extending the platform with Forge tools

The Forge tools preview the correct path for adding new capabilities. They are read-only — they never create anything; they return the payload you'd use to create it. See [docs/tools/forge.md](tools/forge.md) for the full Forge guide.

**Before adding a new authority boundary:**
```bash
praxis workflow tools call praxis_authority_domain_forge --input-json '{
  "authority_domain_ref": "authority.my_new_capability"
}'
```
Returns existing domain state, nearby domains, and whether `ok_to_register=true`. If an existing domain covers your use case, reuse it. If the forge shows a clear gap, proceed to `praxis_register_authority_domain`.

**Before adding a new CQRS operation:**
```bash
praxis workflow tools call praxis_operation_forge --input-json '{
  "operation_name": "my_new_operation",
  "operation_kind": "query"
}'
```
Returns the registration payload, the three CQRS table rows to create, and the migration template. Using this output directly means the migration populates `operation_catalog_registry`, `authority_object_registry`, and `data_dictionary_objects` consistently — no catalog drift.

---

## Quick reference

| Task | Command |
|------|---------|
| Platform status | `praxis workflow query "status"` |
| Health check | `praxis workflow tools call praxis_health --input-json '{}'` |
| Find receipts | `praxis workflow tools call praxis_receipts --input-json '{"query": "..."}'` |
| List open bugs | `praxis workflow tools call praxis_bugs --input-json '{"action": "list", "open_only": true}'` |
| Search decisions | `praxis workflow tools call praxis_recall --input-json '{"query": "..."}'` |
| List patterns | `praxis workflow tools call praxis_patterns --input-json '{"action": "list"}'` |
| List tools | `praxis workflow tools list` |
| Describe a tool | `praxis workflow tools describe praxis_<name>` |
| Auth refresh | `make refresh` |

---

**See also:** [CONCEPTS.md](CONCEPTS.md) — core terms. [WORKFLOWS.md](WORKFLOWS.md) — authoring workflows. [MOON.md](MOON.md) — the Moon canvas. [docs/tools/forge.md](tools/forge.md) — Forge tools for platform builders.
