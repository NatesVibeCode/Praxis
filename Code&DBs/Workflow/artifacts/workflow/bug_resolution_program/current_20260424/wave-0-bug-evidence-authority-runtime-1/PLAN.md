# BUG-A5FE235C Packet Plan

## Authority Model
- Praxis.db standing orders are the top-level contract for this packet.
- Use the execution shard, direct database state, repo snapshots, and canonical receipts as the authoritative sources.
- Treat cached or projection-backed views as advisory only; the current health snapshot reports stale projections and an open read-side circuit breaker, so do not base planning or verification on those views alone.
- Prefer direct runtime and database evidence over indirect summaries when they disagree.

## Files To Read
- The native cutover entrypoint that decides whether the workspace stays on the old mount or moves to local runtime authority.
- The workspace mount lifecycle and mount-resolution code that can preserve or drop old-mount authority.
- The database authority selection and persistence code that records which side owns the workspace/database relationship.
- Any migration, reconciliation, or bootstrap code that runs during cutover.
- Regression tests and fixtures covering native cutover, authority handoff, stale-mount cleanup, and split-authority cases.
- Any bug receipts, prior evidence, or historical notes attached to BUG-A5FE235C.

## Files Allowed To Change
- None in this planning job.
- This packet does not authorize code edits.
- In the follow-up execution packet, limit edits to the cutover orchestration, authority selection/reconciliation path, and tests/fixtures needed to prove the split cannot recur.

## Verification Path
1. Reproduce the split-authority state in a controlled native cutover scenario.
2. Capture pre- and post-cutover authority for the workspace mount and database owner.
3. Confirm whether the old mount still governs one side while local runtime governs the other.
4. Apply the smallest fix that makes authority converge to a single source of truth.
5. Run a focused regression that proves cutover leaves no split state behind and that stale old-mount authority is ignored or removed deterministically.
6. Add a direct DB assertion test if the fix changes persistence semantics.

## Stop Boundary
- Stop once the exact split point is identified and the smallest credible fix scope is defined.
- Do not expand into unrelated mount-management refactors, broad runtime cleanup, or projection-refresh work unless the evidence shows they are required.
- If evidence still depends on stale projections, stop and collect direct DB and filesystem evidence instead of inferring behavior.

## Per-Bug Intended Outcome
- BUG-A5FE235C: native cutover must end with one authority source only, with workspace and database authority aligned after cutover; the old mount must not continue to control one side while local runtime controls the other.
