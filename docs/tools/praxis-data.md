# praxis_data — Deterministic Data Operations

`praxis_data` runs exact data cleanup, transformation, and reconciliation jobs without handing the work to an LLM. When the platform should own parsing, mapping, validation, or diff logic precisely — not heuristically — this is the right tool.

**Surface:** `data` · **Tier:** `stable` · **Alias:** `workflow data`

## The problem it solves

LLMs are good at reasoning about data. They are unreliable at *operating on* data: normalizing 50,000 rows, deduplicating a key space, joining two sources on a compound key, or running a repair loop until a validation schema passes. These operations need determinism, receipts, and the ability to replay — none of which an LLM `auto/build` job provides.

`praxis_data` handles the operations side. The LLM describes what to do; `praxis_data` does it provably.

## When to use `praxis_data` vs. `auto/build`

| Situation | Use |
|-----------|-----|
| Transform rows according to a known schema | `praxis_data` |
| Validate all records against a contract | `praxis_data` |
| Join, dedupe, aggregate structured data | `praxis_data` |
| Repair-loop until a quality threshold is met | `praxis_data` |
| Figure out what transformation is needed | `auto/architecture` → then `praxis_data` |
| Write new application code that processes data | `auto/build` |
| Analyze a sample and produce a schema | `auto/architecture` |

The rule of thumb: if the operation would be correct to implement as a deterministic SQL query or a schema-driven pipeline, `praxis_data` handles it. If it requires judgment or novel reasoning, an LLM job comes first and produces the spec, then `praxis_data` executes.

## Action reference

| Action | What it does |
|--------|-------------|
| `profile` | Describe field types, null rates, cardinality, sample values for a dataset |
| `filter` | Keep only rows matching a predicate set |
| `sort` | Order rows by one or more fields |
| `normalize` | Apply normalization rules: `trim`, `lower`, `upper`, `strip_nulls`, regex replacement |
| `repair` | Set field values on rows matching predicates |
| `repair_loop` | Iteratively repair until a validation schema passes or max iterations hit |
| `backfill` | Fill missing values with a default or derived value |
| `redact` | Mask or remove sensitive fields (`mask_email`, `remove`, `hash`) |
| `checkpoint` | Snapshot cursor position for resumable processing |
| `replay` | Resume from a prior checkpoint, processing only new records |
| `approve` | Mark a generated plan as operator-approved before applying |
| `apply` | Apply an approved plan to a target dataset |
| `validate` | Check all rows against a schema contract; return pass/fail per row |
| `transform` | Apply field-level transformations to every row |
| `join` | Join two datasets on key fields |
| `merge` | Merge two datasets with conflict resolution |
| `aggregate` | Group by fields and compute aggregations (`count`, `sum`, `avg`, `min`, `max`) |
| `split` | Partition a dataset into multiple outputs by a field value |
| `export` | Write the dataset to a shaped output (CSV, JSON, JSONL) |
| `dedupe` | Remove duplicate rows by key fields |
| `route_dead_letter` | Move rows that fail validation to a separate dead-letter output |
| `reconcile` | Compare source and target, produce a diff of mismatches |
| `sync` | Apply reconcile diff to bring target in sync with source |
| `generate_spec` | Generate a workflow spec for a multi-step pipeline |
| `launch` | Launch a generated workflow spec through Praxis Engine |

## CLI usage

```bash
# Profile a file to understand its shape
praxis workflow data action=profile input_path=artifacts/data/users.csv

# Or via tools call (for scripting):
praxis workflow tools call praxis_data --input-json '{
  "action": "profile",
  "input_path": "artifacts/data/users.csv"
}'
```

## Workflow job examples

### Validate and repair a dataset

```json
{
  "name": "data-quality-pass",
  "outcome_goal": "Validate the users export and repair invalid rows",
  "jobs": [
    {
      "label": "validate",
      "agent": "mcp_task",
      "prompt": "praxis_data validate users.json schema={'email':{'required':true,'regex':'.+@.+'},'status':{'enum':['active','inactive']}}"
    },
    {
      "label": "repair",
      "agent": "mcp_task",
      "prompt": "praxis_data repair_loop users.json repairs={'status':{'value':'active'}} schema={'status':{'enum':['active','inactive']}}",
      "depends_on": ["validate"]
    }
  ]
}
```

### Join and reconcile two sources

```json
{
  "name": "source-reconciliation",
  "outcome_goal": "Reconcile users export against the canonical DB export",
  "jobs": [
    {
      "label": "join",
      "agent": "mcp_task",
      "prompt": "praxis_data join input_path=exports/users_api.json secondary_input_path=exports/users_db.json keys=['user_id'] right_prefix='db_'"
    },
    {
      "label": "reconcile",
      "agent": "mcp_task",
      "prompt": "praxis_data reconcile input_path=exports/source.json secondary_input_path=exports/target.json keys=['user_id']",
      "depends_on": ["join"]
    }
  ]
}
```

### Profile → LLM schema design → validate

```json
{
  "name": "schema-first-pipeline",
  "outcome_goal": "Derive a quality schema from the data and validate against it",
  "jobs": [
    {
      "label": "profile",
      "agent": "mcp_task",
      "prompt": "praxis_data profile input_path=artifacts/data/orders.csv"
    },
    {
      "label": "design-schema",
      "agent": "auto/architecture",
      "prompt": "Based on the profile output, design a validation schema for this orders dataset. Output a JSON schema dict.",
      "depends_on": ["profile"]
    },
    {
      "label": "validate",
      "agent": "mcp_task",
      "prompt": "praxis_data validate input_path=artifacts/data/orders.csv schema=<output from design-schema>",
      "depends_on": ["design-schema"]
    }
  ]
}
```

## Common gotchas

**Paths must be artifact-relative.** `input_path` values are resolved relative to the workflow's artifact store. Use `praxis workflow artifacts` to browse available files.

**`repair_loop` has a max iteration cap.** It will not run forever. If the loop exits before the schema passes, check the output's `iterations_run` and `final_validation_result` fields.

**`apply` requires an approved plan.** The `approve` → `apply` sequence is intentional. You cannot skip `approve` and call `apply` directly — the gateway rejects `apply` without a linked `approval_manifest_id`.

**Checkpoint/replay requires a stable `cursor_field`.** The `cursor_field` must be a field that increases monotonically (e.g. `updated_at`, `sequence_id`). Rows without this field are excluded from checkpoint-based windows.

---

**See also:** [MCP.md](../MCP.md) — full `praxis_data` tool reference with all parameters. [CONCEPTS.md](../CONCEPTS.md) — CQRS gateway and receipts. [OPERATOR_GUIDE.md](../OPERATOR_GUIDE.md) — day-2 operations.
