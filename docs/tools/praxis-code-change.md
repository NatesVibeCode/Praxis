# praxis_code_change_candidate — Trust-Compiled Code Edits

The code-change candidate pipeline is how agents propose file edits as structured data rather than writing directly to source. The runtime verifies the proposal, derives the patch, runs the verifier, and applies only after the evidence chain is complete.

**Tools in this pipeline:**
- `praxis_submit_code_change_candidate` — propose edits as structured data
- `praxis_code_change_candidate_preflight` — verify patch against real head, run verifier
- `praxis_code_change_candidate_review` — record reviewer decision
- `praxis_code_change_candidate_materialize` — apply the approved candidate

## The problem it solves

`auto/build` jobs today do what most AI coding tools do: the agent writes to files directly, the result is either right or wrong, and the only feedback is whether tests pass afterward. There's no receipts at the edit boundaries, no pre-application verification, and no reviewable artifact between "proposed" and "applied."

The code-change candidate pipeline breaks this into explicit stages:

1. **Submit** — the agent proposes edits as data (file + action + content). It does not touch live source.
2. **Preflight** — the runtime recomputes the patch from the real base head, runs the declared verifier, and validates any authority-impact claims. Reviewers see this result, not the raw agent submission.
3. **Review** — an operator or a review agent reads the preflight record and records a decision (`approve`, `reject`, `request_changes`).
4. **Materialize** — the runtime rechecks verifier/gate evidence and applies source. Only after all checks pass.

The result: code edits with receipts at every boundary, verifier evidence linked as proof, and a reviewable artifact before anything touches your files.

## When to use this pipeline vs. `auto/build`

| Situation | Use |
|-----------|-----|
| High-stakes file edit that needs a verifier-backed receipt | Code change candidate pipeline |
| Edit that must be reviewed before applying | Code change candidate pipeline |
| Bug fix with a specific verifier that must pass | Code change candidate pipeline |
| Quick exploratory iteration where speed matters more than receipts | `auto/build` |
| Complex reasoning required to figure out what to change | `auto/build` or `auto/architecture` → then candidate pipeline |

The pipeline is especially well-suited for: security patches, auth changes, public API modifications, migration files, and any change where "the agent thinks it worked" is not sufficient.

## Step-by-step usage

### 1. Submit a candidate

```bash
praxis workflow tools call praxis_submit_code_change_candidate --input-json '{
  "bug_id": "BUG-XXXXXXXX",
  "summary": "Fix session token TTL check in get_session()",
  "proposal_payload": {
    "intended_files": ["src/auth/session.py"],
    "rationale": "The TTL comparison uses < instead of <=, allowing tokens at exact expiry to pass",
    "edits": [
      {
        "file": "src/auth/session.py",
        "action": "exact_block_replace",
        "old_block": "if now < token.expires_at:",
        "new_block": "if now <= token.expires_at:"
      }
    ],
    "verifier_ref": "verifier.job.python.pytest_file",
    "verifier_inputs": {
      "path": "/absolute/path/to/tests/auth/test_session.py"
    }
  },
  "source_context_refs": [
    {"path": "src/auth/session.py", "content": "<full file content the agent read>"}
  ],
  "review_routing": "human_review"
}'
```

The response includes a `candidate_id`. Keep it — you need it for all subsequent steps.

### 2. Run preflight

```bash
praxis workflow tools call praxis_code_change_candidate_preflight --input-json '{
  "candidate_id": "<candidate_id from step 1>",
  "triggered_by": "operator"
}'
```

Preflight:
- Recomputes the patch from the **real current head** (not the agent's snapshot). If the file has changed since the agent read it, the diff is recomputed.
- Runs the declared `verifier_ref` with `verifier_inputs`.
- Scans for authority-impact contract violations.
- Returns a preflight record with pass/fail status and evidence.

If preflight fails, investigate the `rejection_reasons` in the response before proceeding.

### 3. Review the candidate

```bash
praxis workflow tools call praxis_code_change_candidate_review --input-json '{
  "candidate_id": "<candidate_id>",
  "reviewer_ref": "operator",
  "decision": "approve",
  "reasons": ["Preflight passed. TTL fix is correct."]
}'
```

`review_routing: "auto_apply"` in the submit call skips this step and auto-approves when preflight passes. Use `"human_review"` (the default) when the change requires a second set of eyes.

### 4. Materialize

```bash
praxis workflow tools call praxis_code_change_candidate_materialize --input-json '{
  "candidate_id": "<candidate_id>",
  "materialized_by": "operator"
}'
```

The runtime rechecks verifier/gate evidence, then applies the patch to source. The materialization result includes the applied diff and the verification receipt. If evidence is missing or stale, materialize refuses.

## Edit action types

| Action | When to use |
|--------|------------|
| `exact_block_replace` | Replace a specific block of text. Requires `old_block` (must match exactly) and `new_block`. Most precise. |
| `full_file_replace` | Replace the entire file contents. Use when changes are too pervasive for block-level targeting. Requires `new_content`. |

## Common gotchas

**`source_context_refs` must reflect what the agent actually read.** The preflight compares the submitted snapshots against the real current head to detect drift. If the agent read stale files, preflight will flag the mismatch.

**`old_block` must be an exact substring match.** Whitespace, indentation, and line endings must match exactly. If the block was normalized by your editor or reformatted after the agent read it, the match will fail.

**Verifier paths must be absolute.** `verifier_inputs.path` uses the `verifier.job.python.pytest_file` verifier, which runs from `/`. Relative paths fail with "file or directory not found."

**`review_routing: "auto_apply"` still requires a passing preflight.** Auto-apply does not skip verification — it skips the human review step only. Materialize still rechecks evidence.

**Candidate IDs are session-scoped.** Candidates created in one MCP session are accessible in subsequent sessions via `candidate_id`, but the base head reference (`base_head_ref`) is snapshotted at submit time. If the repo HEAD moves significantly between submit and materialize, preflight will surface the drift.

---

**See also:** [MCP.md](../MCP.md) — full parameter reference for all four tools. [CONCEPTS.md](../CONCEPTS.md) — receipts and the CQRS gateway. [OPERATOR_GUIDE.md](../OPERATOR_GUIDE.md) — bug resolution and verifier-backed FIXED status.
