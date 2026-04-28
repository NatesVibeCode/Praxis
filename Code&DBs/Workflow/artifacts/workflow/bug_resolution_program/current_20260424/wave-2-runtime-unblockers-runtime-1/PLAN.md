# Runtime Unblockers Packet Plan

## Authority Model
- Top authority: Praxis.db standing orders for this workflow packet.
- Secondary authority: the execution context shard and completion contract for `workflow_abfc0871c97d`.
- Bug authority: canonical bug tracker records for `BUG-90E70AA6` and `BUG-A9A42870`.
- Evidence authority: receipts, existing workflow artifacts, and repo files only.
- This job is planning-only. Do not change code in this packet.
- Downstream work must stay inside the declared write scope and must not broaden the fix surface by inference.

## Files To Read
- `.env`
- The OrbStack migration path that initializes or repairs Docker authority
- The Docker authority startup or health-check path
- Any shell bootstrap or entrypoint that sources `.env`
- Any existing tests that cover env parsing, shell sourcing, migration recovery, or authority availability
- The surrounding workflow artifact packet for this wave, if present, to align with prior decisions

## Files Allowed To Change
- For this planning job: none
- For downstream execution, keep changes narrowly scoped to:
  - `.env` normalization for BOM/CRLF removal
  - The OrbStack migration / Docker authority implementation path
  - Targeted tests that prove the two bug outcomes
- Do not touch unrelated workflow, packaging, or product files unless a reviewed dependency forces it

## Verification Path
- BUG-A9A42870:
  - Verify `.env` is UTF-8 without BOM and uses LF line endings
  - Verify shell sourcing succeeds in a clean POSIX shell
  - Verify no variable names or values changed beyond line-ending / encoding normalization
- BUG-90E70AA6:
  - Reproduce or simulate the migration state that leaves VM data root-owned or corrupted
  - Verify Docker authority becomes available after the repair path runs
  - Verify failure is explicit and actionable if the corrupted state cannot be recovered safely
- General:
  - Run the smallest test set that proves the intended outcome for each bug
  - Prefer fixture or scratch data over live VM mutation

## Stop Boundary
- Stop if the bug packets or repo paths needed to implement the fix are still ambiguous after file inspection
- Stop if the only path to validation requires destructive changes to live OrbStack VM data
- Stop if the fix would require unrelated refactors, broad formatting churn, or changes outside the allowed scope
- Stop if downstream verification cannot prove the intended outcome without weakening the safety boundary

## Intended Outcome Per Bug
- BUG-90E70AA6 [P1/RUNTIME]:
  - OrbStack migration should not leave Docker authority unavailable when VM data is root-owned or corrupted
  - The repair path should either restore authority availability or fail closed with a clear remediation signal
  - Recovery must be repeatable and covered by a targeted test
- BUG-A9A42870 [P2/RUNTIME]:
  - Repo `.env` must be normalized to a shell-safe encoding and line ending format
  - Shell sourcing must work without BOM/CRLF breakage
  - The change must preserve environment semantics while removing the source of sourcing failure

