# Workflow Pipeline Root Cause Evaluation - 2026-04-28

Scope: read-only forensic evaluation of the current workflow pipeline after `workflow_d7ab00d09ac7`.

Operator constraint: no new workflow runs, no retries, no queue mutation. Checks used canonical workflow read surfaces, dry preview, static spec validation, read-only schema/evidence reads, and code inspection.

## Verdict

The latest canary did fire on OpenAI `gpt-5.4-mini`. It did not die because nothing was running. It died because the pipeline handed the model a contradictory contract:

- prompt said to write `Code&DBs/.../PLAN.md`
- persisted shard said writable truth was only `scratch/workflow_d7ab00d09ac7`
- completion contract said this was a `code_change` requiring a sealed `workflow_job_submissions` row
- submission tool returned a non-durable `pending_auto_seal`
- submission gate later found zero sealed rows and failed with `workflow_submission.required_missing`

This is not retryable provider capacity anymore. It is a pipeline contract failure.

## Current State

- `praxis workflow active --json`: queue depth `0`, pending `0`, ready `0`, claimed `0`, running `0`.
- `workflow_d7ab00d09ac7`: failed.
- Plan job reached attempt `5` on `openai/gpt-5.4-mini`.
- Execute and verify jobs are blocked by upstream `workflow_submission.required_missing`.
- `workflow_job_submissions` count for the run: `0`.
- Canonical receipts exist for attempts `1`, `2`, `3`, and `5`; attempts `1-3` were `provider.capacity`, attempt `5` was `workflow_submission.required_missing`.
- `workflow inspect workflow_d7ab00d09ac7`: incomplete evidence; missing workflow events, bundle size evidence, runtime state, and operator frames.

## What The Dry Checks Proved

### 1. Static validation is too shallow

`praxis workflow validate ...wave-6-contract-deps-cleanup-workflow-runtime-1.queue.json` passed.

It only proved the spec parses and the three explicit OpenAI routes resolve. It did not prove:

- prompt output paths match write scope
- verify commands target in-scope artifacts
- submission result kind matches artifact intent
- allowed tools are actually callable under shard enforcement
- stale persisted runtime context will be regenerated before retry
- Markdown artifact paths are acceptable to scope resolution

Relevant code:

- `runtime/workflow_validation.py`: validator focuses on agent resolution and route/provider preflight.
- `runtime/workflow_spec.py`: raw spec validation checks field shapes, not execution-manifest consistency.

### 2. Current preview is better than the failed persisted run

Dry preview of the same spec now infers artifact write scopes:

- Plan: `.../PLAN.md`
- Execute: `.../PLAN.md`, `.../EXECUTION.md`
- Verify: `.../PLAN.md`, `.../EXECUTION.md`, `.../CLOSEOUT.md`

That means the current compiler can infer the file outputs from prompt/verify text.

But preview still reports `scope_resolution_error` for Execute and Verify:

`scope file reference '.../EXECUTION.md' does not match any Python file under /Users/nate/Praxis`

That is a design bug: workflow artifacts are Markdown outputs, not Python modules. Artifact scope and code blast-radius scope are being mixed.

Relevant code:

- `runtime/workflow/_context_building.py`: `_normalized_job_write_scope()` now falls back to artifact inference.
- `runtime/workflow/_context_building.py`: `_job_execution_context_shard()` runs code-oriented `resolve_scope()` against artifact output paths and stores the error.

### 3. The persisted runtime context was stale and poisoned

The failed run did not use the current preview's artifact write scope. Durable `workflow_job_runtime_context` says:

- Plan access policy write scope: `scratch/workflow_d7ab00d09ac7`
- Plan shard write scope: `scratch/workflow_d7ab00d09ac7`
- Execute access policy write scope: `scratch/workflow_d7ab00d09ac7`
- Verify access policy write scope: `scratch/workflow_d7ab00d09ac7`
- `execution_manifest_ref`: `null`

That explains the model transcript:

`the workspace is effectively empty, and the only explicit write scope in the shard is scratch/workflow_d7ab00d09ac7`

Root issue: retry reused a persisted runtime manifest created under older/weaker compile rules. The system does not clearly distinguish "retry old manifest" from "compile fresh manifest with current policy."

Relevant code:

- `runtime/workflow/_context_building.py`: execution loads persisted context from `workflow_job_runtime_context` when present.
- `runtime/workflow/execution_bundle.py`: empty write scope falls back to `scratch/<run_id>`.

### 4. Prompt, shard, and verify path disagreed

The packet spec has no explicit `write_scope`, `write`, `scope`, `submission_required`, or `completion_contract`.

The prompt told the model to write:

`Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-6-contract-deps-cleanup-workflow-runtime-1/PLAN.md`

The stored shard told the model only:

`scratch/workflow_d7ab00d09ac7`

The verify command expected:

`test -s Code&DBs/.../PLAN.md`

The model wrote:

`/workspace/scratch/workflow_d7ab00d09ac7/Code&DBs/.../PLAN.md`

This is duplicated authority. The model made a reasonable choice inside the shard; the runtime later judged against a different truth.

### 5. The tool contract was incoherent inside the shard

Plan prompt says to use `praxis workflow discover/recall`.

Persisted Plan token allowed:

- `praxis_context_shard`
- `praxis_query`
- `praxis_status_snapshot`
- `praxis_receipts`
- `praxis_bugs`
- `praxis_health`
- submission/review tools

It did not allow `praxis_discover` or `praxis_recall`. Also, scoped sessions reject broad tools that cannot prove shard enforcement.

The clamp itself is directionally correct, but the prompt should never tell the model to use tools the token will reject. The model was forced into local-only behavior inside an empty workspace.

Relevant code:

- `surfaces/mcp/invocation.py`: token allowed-tool enforcement.
- `surfaces/mcp/invocation.py`: broad readers fail closed when shard scope is present.
- `surfaces/mcp/tools/bugs.py`: session blocks broad bug enumeration actions.

### 6. Submission returned a false comfort state

The model concluded:

`The submission was sealed through Praxis and is pending auto-seal after sandbox dehydration.`

The database proves there was no sealed row:

- `workflow_job_submissions`: `0` rows for the run.

The code path explains it. In docker packet mode, when no host diff is visible yet, `submit_code_change` returns:

- `submission_id: None`
- `status: pending_auto_seal`
- `reason_code: workflow_submission.pending_auto_seal`

Then the post-execution gate re-checks, tries auto-seal, finds no sealed row / no in-scope measured change, and fails:

`submission_required=true but no sealed submission exists for the current attempt`

This is the biggest contract bug. A submit tool should not let the model report "sealed" unless there is a durable submission id or a durable pending-submission record with an explicit later transition.

Relevant code:

- `runtime/workflow/submission_capture.py`: `pending_auto_seal` return path.
- `runtime/workflow/submission_gate.py`: post-execution auto-seal and fail-closed enforcement.

### 7. Submission authority contradicts sandbox/dehydration authority

The runtime says the authoritative deliverable for submission-required jobs is `workflow_job_submissions`, not on-disk artifacts.

But the auto-seal path depends on dehydrating sandbox files back to the host so `_measured_operations()` can diff the host workspace and create the submission row.

That is a hybrid authority model:

- declared authority: durable submission row
- actual dependency: host filesystem diff after sandbox dehydration

It can work, but it is fragile and difficult for future agents to reason about.

Relevant code:

- `runtime/sandbox_runtime.py`: comment says sealed submission is authority and host dehydration is legacy only.
- `runtime/sandbox_runtime.py`: later comment says dehydration is required because seal flow reads on-disk host diff.

### 8. The result kind is wrong for planning artifacts

Plan job says "Do not edit code in this job" and produces `PLAN.md`.

The completion contract says:

- `submission_required: true`
- `result_kind: code_change`
- submit tool: `praxis_submit_code_change`

A planning document is an artifact bundle, not a code change. Current submission surfaces already have `praxis_submit_artifact_bundle`, but this packet path selected `code_change`.

Relevant code:

- `surfaces/api/workflow_submission.py`: `result_kind` must match the submit tool.
- `runtime/workflow/execution_bundle.py`: completion contract defaults to `code_change` for mutating tasks when not overridden.

### 9. Provider authority is split between "runnable" and "healthy"

Provider control plane says OpenAI `gpt-5.4-mini` is runnable for 12 Praxis job types:

- control on
- breaker closed
- capability runnable
- credential state unknown

Run diagnosis says OpenAI provider health is unhealthy with recent `sandbox_error` failures.

Both can be true, but they are not reconciled at route selection time in a way an operator can trust. "Runnable" means catalog-admitted, not "healthy enough to send more work." The platform needs to make that distinction explicit.

Also, Google/Gemini currently has zero runnable rows in the Praxis profile. The failed spec explicitly targeted OpenAI, not Gemini. Nothing in this packet would have gone to Gemini.

## End-To-End Failure Map

| Step | Current result | Root cause |
| --- | --- | --- |
| Packet spec | Valid JSON, but missing explicit write/submission contract | Spec relies on inference for critical authority |
| Static validation | Passed | Validator checks routes, not execution contract integrity |
| Current preview | Infers artifact paths, but reports Markdown scope errors | Artifact scope is being passed through code-scope resolver |
| Persisted manifest | Stale scratch-only write scope | Retry reused old runtime context |
| Provider selection | OpenAI route admitted and ran | Provider not the final blocker, though health is degraded |
| Sandbox mount | Empty `/workspace`, scratch-only shard | Shard did not include artifact path prompt expected |
| Tool access | Prompt requested blocked tools | Prompt/tool/token/shard contract mismatch |
| Model behavior | Wrote PLAN under scratch path | Correct behavior under wrong shard |
| Submission | Returned `pending_auto_seal`, no row | Non-durable "submit" state looked successful |
| Dehydration/auto-seal | No sealed row produced | Host diff authority did not see required in-scope artifact |
| Gate | Failed `workflow_submission.required_missing` | Correct fail-closed result after earlier contract ambiguity |
| Observability | Incomplete inspection | Missing workflow events, runtime state, bundle size evidence |

## Priority Fixes

1. Add a read-only pipeline evaluator before any run/retry.
   It should compile the spec and fail if prompt paths, verify paths, write scope, allowed tools, result kind, submission contract, and scope resolution disagree.

2. Refuse stale persisted manifests on retry unless the retry explicitly says it is reusing the old manifest.
   Better default: regenerate manifest on retry and record manifest hash/revision in the retry guard.

3. Remove the scratch fallback for packet jobs with artifact paths.
   If a job has prompt/verify artifact paths and no explicit write scope, infer the exact artifact paths or fail compile. Scratch should be only an explicit scratch task, not silent authority.

4. Make `pending_auto_seal` non-success or durable.
   Either record a durable pending submission row, or return a hard "not sealed yet" state that the model cannot summarize as sealed.

5. Use `artifact_bundle` for plan/closeout/report artifacts.
   Do not force `code_change` for jobs that explicitly say "do not edit code."

6. Split artifact scope from code blast-radius scope.
   Markdown output artifacts should not go through a Python-file resolver. Code blast radius can be empty while artifact write scope remains valid.

7. Make prompts generated from allowed/enforceable tool contracts.
   If a scoped token cannot use `praxis_query`, `praxis_discover`, or `praxis_recall`, the prompt must not instruct the model to use them. Use `praxis_context_shard` and shard-clamped `praxis_search`, or inject the needed context up front.

8. Reconcile provider "runnable" with provider "healthy."
   Catalog admission, credential state, breaker state, and recent execution health should project into one operator-facing route verdict.

9. Fill the evidence gaps.
   Each transition should have a workflow event, runtime state evidence, bundle hash/size, and operator frame linkage. "Failed" is not enough; the platform should prove how it reached failed.

## Stop Boundary

Do not retry this fleet until one dry pipeline evaluator can prove, before execution:

- current manifest is fresh
- write scope equals intended artifact outputs
- verify commands target in-scope paths
- submission tool/result kind matches the job output
- allowed tools match prompt instructions
- broad tools are either shard-clamped or omitted
- provider route is admitted and healthy enough
- expected submission row can be produced deterministically

No phasers until the targeting computer stops arguing with itself.
