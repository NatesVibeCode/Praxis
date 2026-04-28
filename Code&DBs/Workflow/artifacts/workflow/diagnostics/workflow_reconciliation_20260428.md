# Workflow Reconciliation - 2026-04-28

## Verdict

Do not launch the old 2026-04-24 packet fleet. It is stale under the current
manifest, shard, route, and verifier contracts.

No workflows were launched during this reconciliation.

## Current Authority Snapshot

- Live bugs: 549 total, 189 OPEN, 31 FIX_PENDING_VERIFICATION, 275 FIXED, 50 DEFERRED, 4 WONT_FIX.
- Old packet set: 48 queue specs under `bug_resolution_program/current_20260424/packets`.
- Old packet bug refs: 110 unique IDs.
- Old packet status split: 83 OPEN, 24 FIXED, 2 DEFERRED, 1 FIX_PENDING_VERIFICATION.

## Old Packet Eval

Read-only pipeline eval over all 48 old queue specs:

- 48 failed, 0 passed.
- 393 errors, 144 warnings.
- Major errors:
  - `prompt_tool_scope_not_enforced`: 147
  - `scope_resolution_error`: 123
  - `prompt_tool_not_allowed`: 84
  - `artifact_job_uses_code_change_submission`: 36
  - `artifact_path_outside_write_scope`: 3
- Major warning:
  - `execution_manifest_ref_missing`: 144

Root cause: the old template assumes broad workflow tools inside scoped worker
sessions, lacks execution manifests, and uses artifact-oriented prompts with
code-change submission contracts. It is not compatible with scoped manifests.

## Repairs Applied

- `praxis_launch_plan` no longer selects non-physical bug columns
  (`replay_ready`, `replay_reason_code`) from `bugs`; replay state is derived
  through bug tracker authority.
- Bug-derived packets now extract concrete repo paths from bug authority text
  before falling back to workspace root.
- The proof packet for `BUG-724759AE` now derives this shard:
  - `Code&DBs/Workflow/runtime/workflow/_shared.py`
  - `Code&DBs/Workflow/runtime/workflow_validation.py`
- Bug-derived packets route through the registered build lane instead of
  emitting dead `auto/fix` routes.
- The `fix` task type now aliases to the code-edit/build execution profile and
  requires verifier refs.

## Proof Preview

Generated preview only for `BUG-724759AE`:

- Agent route: `auto/build`
- Resolved routes: none unresolved
- Verify refs: Python compile refs generated for both shard files
- Inline execution manifest: present
- Pipeline eval: `ok=true`, `error_count=0`, `warning_count=0`

## Adjusted Priority

1. Use one proof packet first: `BUG-724759AE`.
2. After one proof works, handle the fail-closed provider authority cluster:
   `BUG-724759AE`, `BUG-2D9A6DED`, `BUG-5DFF1C68`.
3. Then provider credential/catalog cleanup:
   `BUG-70706DC9`, `BUG-023252F7`.
4. Then FIX_PENDING_VERIFICATION provider route items:
   `BUG-EBE27625`, `BUG-5444AA3C`, `BUG-D4CC68A9`, `BUG-1B959922`.
5. Quarantine or regenerate the old 48 packet specs rather than retrying them.

## Remaining Guardrail

The generated proof packet is launch-shape clean, but fleet execution is still
blocked by operating policy: one proof run must complete with observable work
before any broader packet wave is launched.
