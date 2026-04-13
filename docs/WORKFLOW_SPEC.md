# Workflow Spec Reference

Workflow specs are JSON files (conventionally `*.queue.json`) that define a directed acyclic graph of jobs for Praxis Engine to execute.

## Top-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Human-readable workflow name |
| `workflow_id` | string | No | Unique identifier. Auto-generated from `name` if omitted |
| `phase` | string | No | Execution phase. Defaults to `"execute"` |
| `jobs` | array | Yes | Non-empty list of job objects |
| `outcome_goal` | string | No | What success looks like for this workflow |
| `anti_requirements` | array | No | Constraints on what the workflow must not do |
| `verify_refs` | array | No | Paths to verification scripts run after all jobs complete |
| `workspace_ref` | string | No | Override workspace binding |
| `runtime_profile_ref` | string | No | Override runtime profile from `runtime_profiles.json` |

## Job Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `label` | string | No | Job identifier. Auto-generated from sprint/index if omitted |
| `id` | string | No | Alternative identifier (translated to label internally) |
| `agent` | string | No | Agent route. Defaults to `"auto/build"` |
| `prompt` | string | Yes* | Task prompt. Required for LLM-routed jobs |
| `depends_on` | array | No | List of job labels this job waits for |
| `scope` | object | No | Permission scoping. `{"write": ["path/"]}` |
| `sprint` | integer | No | Sprint number for implicit ordering |
| `replicate` | integer | No | Fan-out: create N copies of this job |

*Prompt is required for all jobs that route to an LLM adapter.

## Agent Routing

The `agent` field determines which provider and model execute the job.

### auto/ Routes (Semantic Routing)

| Route | Selects | Use Case |
|-------|---------|----------|
| `auto/build` | Best coder | Code generation, implementation |
| `auto/review` | Best reviewer | Code review, quality analysis |
| `auto/architecture` | Best reasoner | System design, planning |
| `auto/test` | Best terminal operator | Writing and running tests |
| `auto/refactor` | Best refactorer | Code cleanup, restructuring |
| `auto/wiring` | Cheapest fast model | Config, glue code, simple tasks |
| `auto/debate` | Best reasoner | Adversarial analysis |
| `auto/research` | Research model | Deep research tasks |

### Direct Routes

Specify provider and model directly:

```json
{"agent": "anthropic/claude-sonnet-4-6"}
{"agent": "openai/gpt-5.4"}
{"agent": "google/gemini-3.1-pro-preview"}
```

## DAG Rules

1. **Acyclic** -- No circular dependencies. The compiler rejects cycles.
2. **Label uniqueness** -- Every job label must be unique within the spec.
3. **Dependency resolution** -- `depends_on` references must match existing job labels or IDs.
4. **Sprint ordering** -- If any job has a `sprint` field, jobs are sorted by sprint number. Sprint N+1 jobs implicitly depend on all sprint N jobs.
5. **Fan-out** -- `replicate: N` creates N copies of the job with suffixed labels (`job_r1`, `job_r2`, ...).

## Verify Blocks

`verify_refs` lists paths to scripts that run after all jobs succeed:

```json
{
  "verify_refs": [
    "scripts/run_tests.sh",
    "scripts/lint_check.sh"
  ]
}
```

Each script must exit 0 for the workflow to be marked successful.

## Scope Constraints

Limit which files a job can write to:

```json
{
  "label": "fix-auth",
  "agent": "auto/build",
  "prompt": "Fix the auth bug",
  "scope": {
    "write": ["src/auth/", "tests/auth/"]
  }
}
```

## Complete Example

```json
{
  "name": "refactor-pipeline",
  "workflow_id": "refactor-pipeline-v1",
  "phase": "execute",
  "outcome_goal": "Refactor the data pipeline for clarity and add tests",
  "anti_requirements": [
    "Do not change public API signatures",
    "Do not modify database schema"
  ],
  "jobs": [
    {
      "label": "analyze",
      "agent": "auto/architecture",
      "prompt": "Analyze the data pipeline and propose a refactoring plan"
    },
    {
      "label": "refactor",
      "agent": "auto/refactor",
      "prompt": "Execute the refactoring plan from the analysis",
      "depends_on": ["analyze"],
      "scope": {"write": ["src/pipeline/"]}
    },
    {
      "label": "test",
      "agent": "auto/test",
      "prompt": "Write unit and integration tests for the refactored pipeline",
      "depends_on": ["refactor"],
      "scope": {"write": ["tests/pipeline/"]}
    },
    {
      "label": "review",
      "agent": "auto/review",
      "prompt": "Review the refactoring and tests for correctness",
      "depends_on": ["refactor", "test"]
    }
  ],
  "verify_refs": ["scripts/run_tests.sh"]
}
```
