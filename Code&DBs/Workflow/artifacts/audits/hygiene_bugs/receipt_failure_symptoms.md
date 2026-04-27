# Receipt-Failure Hygiene Audit — Root-Cause Re-Anchoring vs Symptom Closure

**Audit scope:** auto-filed `receipt_store` bugs whose title matches `Repeated receipt failure: <failure_code>`, plus the `mcp_workflow_server`-filed receipt-failure cousins that share evidence runs and signatures.

**Audit date:** 2026-04-27

**Authority routes used:**
- `praxis_query` → `bug_tracker` (search title `Repeated receipt failure`).
- `praxis_bugs` action `packet` and `history` for each candidate `bug_id`.
- `praxis_receipts` action `search` for failed receipts in last 48h.

**Spec note (six rows):** the operator brief described "six repeated-receipt-failure rows". The strict title `Repeated receipt failure:` matches exactly four rows in the live bug tracker (`BUG-665AA992`, `BUG-28A9CC35`, `BUG-F28BF7B6`, `BUG-B5C4B240`). I extended the audit to two `mcp_workflow_server`-filed receipt-failure cousins (`BUG-D2363EB8`, `BUG-17CC1088`) that recurred under the same workflow_unified executor and were filed for receipt-failure pathologies — together those make six. The discrepancy is documented per row below.

---

## Row 1 — BUG-665AA992 · `Repeated receipt failure: host_resource_capacity` · step_1

| Field | Value |
|---|---|
| `failure_code` | `host_resource_capacity` |
| `failure_category` | `timeout` |
| `signature` | `a04f157b93a73e636e0e` |
| `recurrence_count` (7d) | 6 |
| `impacted_run_count` | 3 |
| `impacted_receipt_count` | 3 |
| `latest_receipt` | `receipt:workflow_8913ede67521:595:1` (latency_ms=30162) |
| Provider mix | anthropic/claude-opus-4-7 (single agent observed) |
| Replay state | `replay_ready=true`, replay receipt anchored to current run |

**Verdict: SYMPTOM — re-anchor under host-budget root cause; do not close yet.**

The `latency_ms ≈ 30162 ms` is a fingerprint, not an accident: the executor is hitting the per-job host wall-clock budget and the host is signalling `host_resource_capacity` rather than letting the model finish. The discovery receipt sat under workflow_unified with verification skipped, so the capture is a true symptom row, not a fix candidate.

Closing this on its own would suppress the signal that the per-job host budget is too tight for current Claude-side latency on `step_1` reviews. The auto-filer is doing the right thing; the **next anchor must be a manual P1/P2 root-cause bug describing the budget mismatch (host wall < observed model thinking time)** with this bug attached as `observed_in` evidence and then resolved as duplicate.

**Action:** keep OPEN. File a parent root-cause bug ("workflow_unified host wall-clock budget ≈ 30s clips review-class jobs before completion") and link `BUG-665AA992` as a duplicate-of via `attach_evidence`.

---

## Row 2 — BUG-28A9CC35 · `Repeated receipt failure: host_resource_capacity` · step_2

| Field | Value |
|---|---|
| `failure_code` | `host_resource_capacity` |
| `failure_category` | `timeout` |
| `signature` | `1b6f5f1ef731776855a7` |
| `recurrence_count` (7d) | 6 |
| `impacted_run_count` | 3 |
| `impacted_receipt_count` | 3 |
| `latest_receipt` | `receipt:workflow_9af0c9b09c5b:593:1` (latency_ms=30117, agent=openai/gpt-5.4) |
| Provider mix | **anthropic/claude-opus-4-7 AND openai/gpt-5.4** (two distinct agents) |
| Replay state | `replay_ready=true` |

**Verdict: SYMPTOM — same root cause as Row 1; **NOT** a Claude-auth artefact.**

This row clears the otherwise-tempting hypothesis that host_resource_capacity timeouts are downstream of the just-fixed Claude auth bug `BUG-B41802F3` (FIXED 2026-04-27 17:17). The packet shows the recurrence cluster contains receipts from `openai/gpt-5.4` as well as `anthropic/claude-opus-4-7`, all at ~30s latency. Cross-provider symptom + identical wall-clock = host-side budget, not provider-side auth.

**Action:** keep OPEN. Same as Row 1 — re-anchor under the proposed parent host-budget root-cause bug; do not close as duplicate of `BUG-B41802F3`.

---

## Row 3 — BUG-F28BF7B6 · `Repeated receipt failure: sandbox_error` · step_3

| Field | Value |
|---|---|
| `failure_code` | `sandbox_error` |
| `failure_category` | `sandbox_error` |
| `signature` | `60b998cda77d8851180a` |
| `recurrence_count` (7d) | 2 |
| `impacted_run_count` | 1 |
| `impacted_receipt_count` | 1 |
| `latest_receipt` | `receipt:workflow_b614c602d5d2:565:1` (anthropic/claude-opus-4-7) |
| Replay state | `replay_ready=true`, replay receipt = discovery receipt |

**Cross-evidence (`praxis_receipts search 'sandbox_error'`):** receipt `receipt:workflow_478149599bdb:572:1` for `step_1` carries `stdout_preview: "Not logged in · Please run /login"` — Claude CLI rejected the session because OAuth state was missing. That is exactly the failure pattern fixed in `BUG-B41802F3` (Keychain/OAuth/`.claude.json` projection drift, FIXED 2026-04-27 17:17:41).

**Verdict: PURE SYMPTOM — close as duplicate-of `BUG-B41802F3`.**

The bug captures a symptom that the fix in `BUG-B41802F3` directly removes. The receipt evidence and the fix's `validation` line ("worker `claude -p` returned success", "praxis_cli_auth_doctor flipped anthropic from timeout to authenticated") align. Re-anchoring is *not* needed here — the root cause is already filed AND fixed.

**Action:** resolve `BUG-F28BF7B6` with `status=DEFERRED` (until verifier-backed proof closes the parent) or `status=FIXED` with `verifier_ref` pointing at the `BUG-B41802F3` fix verifier once registered. Attach `evidence_role=validates_fix` link to `BUG-B41802F3`.

---

## Row 4 — BUG-B5C4B240 · `Repeated receipt failure: workflow_submission.required_missing` · step_1

| Field | Value |
|---|---|
| `failure_code` | `workflow_submission.required_missing` |
| `failure_category` | `infrastructure` |
| `signature` | `f74c4160592eb0cdc778` |
| `recurrence_count` (7d) | 6 |
| `impacted_run_count` | 3 |
| `impacted_receipt_count` | 3 |
| `latest_receipt` | `receipt:workflow_40a9582fc641:531:1` (anthropic/claude-opus-4-7) |
| Replay state | `replay_ready=true` |

**Verdict: SYMPTOM — re-anchor under `BUG-D2363EB8` (`phantom_ship`) cluster.**

`workflow_submission.required_missing` fires from `submission_gate` Stage 4 when `verification_required=True` and `verification_artifact_refs` is empty — i.e., the agent claimed completion but no on-disk write was sealed. That is the same symptom captured manually as `BUG-D2363EB8` ("praxis_submit_code_change rejects sandbox-written files as phantom_ship in empty workflow workspace") plus the secondary path described in `BUG-31E77A5E` ("submission_gate Stage 4 fails build jobs without explicit verify_refs even when in-scope file landed and auto-seal sealed the submission", FIX_PENDING_VERIFICATION).

The recurrence-6 / impacted_run=3 evidence is high-value, but the *bug* is a duplicate of two manually-filed root-causes that already have richer descriptions and (in the Stage-4 case) a working-tree fix.

**Action:** keep OPEN until either (a) `BUG-31E77A5E` exits FIX_PENDING_VERIFICATION with a passing verifier, or (b) `BUG-D2363EB8`'s workspace-hydration root-cause is filed and fixed. Attach this bug as `observed_in` evidence on both `BUG-D2363EB8` and `BUG-31E77A5E`. Resolve as duplicate-of when the parent fix verifies.

---

## Row 5 (cousin) — BUG-D2363EB8 · `praxis_submit_code_change rejects sandbox-written files as phantom_ship`

| Field | Value |
|---|---|
| Filed by | `mcp_workflow_server` |
| Severity | P1 |
| Receipt-failure pathology | `workflow_submission.phantom_ship` reason_code on submit |
| Replay state | `replay_ready=false` (`bug.replay_missing_run_context`) |

**Verdict: ROOT-CAUSE BUG (already anchored as such); needs fix, not closure.**

This bug *is* the root cause referenced by Row 4. It describes the exact mechanism: `/workspace` starts empty, agent writes files inside the sandbox, but the submission bridge / workspace-hydration layer does not see them, so `praxis_submit_code_change` returns `workflow_submission.phantom_ship`. That cascade then trips Stage 4 which the receipt store autoclassifies as `workflow_submission.required_missing` (Row 4).

**Action:** elevate priority — this is the genuine root cause for at least Row 4 and possibly the larger workspace-hydration cluster with `BUG-17CC1088` and `BUG-632E6F45`. Add `observed_in` link from `BUG-B5C4B240`. No closure.

---

## Row 6 (cousin) — BUG-17CC1088 · `Workflow review step_3 sandbox missing target repo and broken praxis CLI runtime`

| Field | Value |
|---|---|
| Filed by | `mcp_workflow_server` |
| Severity | P1 |
| Receipt-failure pathology | review job cannot start — empty `/workspace` + ModuleNotFoundError on `json` stdlib |
| Replay state | `replay_ready=false` |

**Verdict: ROOT-CAUSE BUG; partial overlap with Row 5 and `BUG-632E6F45`.**

This bug is the per-image manifestation of the `/workspace`-empty pathology that surfaces as `sandbox_error` (Row 3), `host_resource_capacity` (Rows 1-2 via the agent burning the budget on workspace-discovery retries), and `phantom_ship` (Row 5). It also independently describes the broken bundled `praxis` CLI (json stdlib import failure) which is distinct from the workspace-hydration issue but converges on the same review-job impact.

**Action:** keep OPEN. This is the closest existing root-cause anchor for the receipt-failure family. Suggest splitting into two children: (a) `/workspace` hydration drift between baseline.workspace_manifest and the running container, and (b) packaged `praxis` CLI runtime fragility. The current title bundles both.

---

## Cluster summary

| Symptom row | Disposition | Re-anchor target |
|---|---|---|
| BUG-665AA992 (host_resource_capacity step_1) | re-anchor | NEW: host wall-clock budget bug (file P1) |
| BUG-28A9CC35 (host_resource_capacity step_2) | re-anchor | same as Row 1 |
| BUG-F28BF7B6 (sandbox_error step_3) | symptom closure | duplicate-of BUG-B41802F3 (FIXED) |
| BUG-B5C4B240 (workflow_submission.required_missing step_1) | re-anchor (defer closure until parent verifies) | BUG-D2363EB8 + BUG-31E77A5E |
| BUG-D2363EB8 (phantom_ship) | root anchor — needs fix | self |
| BUG-17CC1088 (sandbox missing target repo + broken praxis CLI) | root anchor — needs split | self |

## Evidence trail

- All four `receipt_store`-auto-filed rows reach `replay_ready=true`, meaning each has an authoritative replay receipt and the bug's `bug.replay` action is a one-call replay.
- `historical_fixes.reason_code = bug.historical_fixes.none` for all four — i.e., the platform has not seen this signature resolved before; that is consistent with all four being live regressions, not stale captures.
- The two `mcp_workflow_server`-filed cousins are `replay_ready=false` because they were filed without a discovery_receipt (`bug.replay_missing_run_context`). They cannot be auto-replayed; they need manual repro.
- Rows 1 and 2 each have `recurrence_count=6` over 7 days against `impacted_run_count=3` — i.e., each of three different runs hit the budget twice (once on first try, once on retry). That's the fingerprint of a deterministic budget cap, not a flaky failure.

## Recommendation precedence

1. File the parent host-budget bug. Without it, Rows 1 and 2 stay open indefinitely while the receipt store keeps re-firing the same signature.
2. Resolve `BUG-F28BF7B6` against the FIXED `BUG-B41802F3` once the auth-fix verifier is registered.
3. Promote `BUG-D2363EB8` from "filed observation" to "root-cause investigation" — its workspace-hydration mechanism is the upstream of Row 4 and likely interacts with Rows 1 and 2 (agents losing wall-clock budget probing an empty `/workspace`).
4. Split `BUG-17CC1088` so the packaged-`praxis`-CLI fragility doesn't get hidden behind the workspace-hydration headline.

## Authoritative paths consulted

- bug_tracker rows: `BUG-665AA992`, `BUG-28A9CC35`, `BUG-F28BF7B6`, `BUG-B5C4B240`, `BUG-D2363EB8`, `BUG-17CC1088`, `BUG-632E6F45`, `BUG-B41802F3`, `BUG-31E77A5E`.
- receipt evidence: `receipt:workflow_8913ede67521:595:1`, `receipt:workflow_9af0c9b09c5b:593:1`, `receipt:workflow_b614c602d5d2:565:1`, `receipt:workflow_478149599bdb:572:1`, `receipt:workflow_40a9582fc641:531:1`.
- run evidence: `workflow_8913ede67521`, `workflow_9af0c9b09c5b`, `workflow_884b7b454f96`, `workflow_6f2c02113db8`, `workflow_b614c602d5d2`, `workflow_478149599bdb`, `workflow_40a9582fc641`.
- decision_refs: `decision.bug_tracker.filing.receipt_store.implicit`, `decision.bug_tracker.filing.mcp_workflow_server.implicit`.
