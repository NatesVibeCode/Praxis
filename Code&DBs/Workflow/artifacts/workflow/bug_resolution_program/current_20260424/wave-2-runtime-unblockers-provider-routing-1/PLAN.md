# Wave 2 Runtime Unblockers: Provider Routing Plan

## Authority Model
- Primary authority is the current workflow shard and execution bundle for `workflow.wave.2.runtime.unblockers.provider.routing.1`.
- This packet is review/planning only. Do not change runtime code, database state, or verification artifacts in this job.
- The only writable target for this job is this plan artifact under the packet path.
- If implementation details conflict with the bug packet, the bug tracker and receipt evidence govern the intended runtime behavior, and the implementation packet should resolve the conflict before code changes.
- Stop at the planning boundary unless a later execution packet explicitly authorizes code edits and verification.

## Files To Read
- This plan artifact and any sibling workflow packet notes under `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-2-runtime-unblockers-provider-routing-1/`.
- The runtime code that owns query readback routing and the `quality_views` rollup path.
- The runtime code that defines `ProviderAdapterContract` and the provider adapter implementations that participate in cross-cutting concerns.
- The runtime code that emits or classifies `route.unhealthy` receipt failures, including retry and unhealthy-route handling.
- The bug packets and receipt evidence for:
  - `BUG-91D41A89`
  - `BUG-C094B483`
  - `BUG-EAD36E8A`

## Files Allowed To Change
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-2-runtime-unblockers-provider-routing-1/PLAN.md` only.
- No source files, tests, migrations, or database content are in scope for this planning job.

## Verification Path
- For the later implementation packet, reproduce each bug from its canonical evidence before changing code.
- Confirm the query-routing fix by exercising readback questions and checking that they no longer route to an empty `quality_views` rollup.
- Confirm the provider contract fix by checking that cross-cutting concerns are represented in the shared adapter contract and enforced by implementations.
- Confirm the receipt-routing fix by re-running the failing path and verifying that `route.unhealthy` no longer repeats under the same conditions.
- Use targeted tests or replay checks only after the implementation packet lands, then validate against receipts and bug resolution status.

## Stop Boundary
- Stop after this plan is written and sealed.
- Do not edit runtime code, tests, or DB data in this job.
- Do not start implementation, refactors, or verification runs beyond what is needed to author the plan.
- If additional filenames are needed for implementation, defer that discovery to the next packet rather than expanding scope here.

## Per-Bug Intended Outcome
- `BUG-91D41A89 [P2/RUNTIME]`: Readback questions should route to a non-empty, appropriate `quality_views` rollup path, or a clearly defined fallback, instead of landing on an empty rollup.
- `BUG-C094B483 [P2/RUNTIME]`: `ProviderAdapterContract` should explicitly cover the cross-cutting concerns required by provider routing, and adapter implementations should conform to that strengthened contract.
- `BUG-EAD36E8A [P2/RUNTIME]`: Repeated `route.unhealthy` receipt failures should be eliminated by tightening unhealthy-route handling so the same receipt failure mode does not recur.
