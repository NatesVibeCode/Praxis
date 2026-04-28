# Plan: provider_routing packet

Bug in scope: `BUG-D39EBC3F` `[P1/VERIFY]` workflow health reports healthy while projections are critical and routes are unhealthy.

## Authority Model

1. `/orient` and the current workflow shard are the top-level planning authority.
   - Observed envelope: `workflow.wave.0.bug.evidence.authority.provider.routing.1`
   - Job label: `Plan provider_routing packet`
   - Run id: `workflow_8bd42ebf5138`
   - Repo/workdir authority: `/workspace`
   - Write scope: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-0-bug-evidence-authority-provider-routing-1/PLAN.md`
2. Standing orders from Praxis.db apply first.
   - Stay within the current execution shard.
   - Prefer read-only orientation and evidence gathering before any implementation packet.
   - Do not widen scope beyond the provider routing / workflow health contradiction.
3. Live system health is the current observed state authority.
   - `praxis_health` is the live signal for preflight health, projection freshness, read-side circuit breaker state, and route outcomes.
   - Current contradiction from `praxis_health`: overall health is `healthy` while projection freshness is `critical`, the read-side circuit breaker is `open`, and route outcomes report `error`.
4. Bug tracker evidence is the bug-specific authority.
   - `BUG-D39EBC3F` packet/history and linked receipts are the source of bug lineage and prior observations.
5. Source code and tests are implementation evidence only.
   - They may localize the fault in the next packet, but they do not override live health or bug-tracker evidence.
6. This file is a planning artifact only.
   - It is not a fix, not verification, and not a DB mutation.

Observed current evidence from `praxis_health`:
- `preflight.overall = healthy`
- projection freshness SLA = `critical`
- read-side circuit breaker = `open`
- route outcomes = `error`

## Files To Read

Read only the minimum files needed to trace the bad health verdict and its inputs:

1. The workflow health entrypoint and aggregator.
   - The code that produces the overall workflow health verdict.
   - The code that merges preflight, projection freshness, and route outcome signals.
2. The projection freshness evaluator.
   - The code that marks projections critical and opens the read-side circuit breaker.
3. The route outcome collector.
   - The code that surfaces provider/transport route failures into workflow health.
4. Bug packet/history for `BUG-D39EBC3F`.
   - Packet, history, and any linked receipts or replay evidence.
5. Existing tests around health precedence.
   - Tests for workflow health, projection freshness, route outcomes, and precedence between `healthy`, `warning`, and `critical`.

If the exact paths are not obvious, discover them in the next execution packet before reading or changing anything.

## Files Allowed To Change

For this planning job:
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-0-bug-evidence-authority-provider-routing-1/PLAN.md` only.

Not allowed in this job:
- Any code file.
- Any test file.
- Any DB row, bug record, or receipt.
- Any artifact outside this plan file.

## Verification Path

The next packet should prove the bug with read-only evidence before any fix is attempted:

1. Re-read `praxis_health` and confirm the contradiction remains reproducible:
   - overall health reports `healthy`
   - projections are `critical`
   - routes are unhealthy or errored
2. Pull the bug packet/history for `BUG-D39EBC3F` and any linked receipts or replay evidence.
3. Identify the exact aggregation boundary where the overall workflow health is downgraded or incorrectly left `healthy`.
4. Classify the failure mode:
   - precedence bug, where critical projection/route states are ignored
   - field-mapping bug, where route or projection errors never reach the verdict
   - stale-cache bug, where health reads old data while the underlying projections are already critical
5. Read only the minimum implementation files needed to localize the fault, then stop.
6. Only after the fault is localized should the execution packet change code and run targeted tests.

## Stop Boundary

Stop after this plan is sealed and before any implementation work begins.

Do not:
- edit code
- edit tests
- mutate the bug tracker
- perform speculative refactors
- broaden scope beyond the provider routing / workflow health contradiction
- turn this planning packet into a fix packet
- use any write authority outside this PLAN.md file

If the next packet needs implementation, hand it off with the exact read path and failing authority boundary identified.

## Per-Bug Intended Outcome

### `BUG-D39EBC3F`

Intended outcome:
- workflow health must not report `healthy` when projections are `critical` or route outcomes are unhealthy
- the health verdict must reflect the worse of the relevant subsystems, not the preflight result alone
- route and projection failures must be visible in the authoritative health surface
- the next execution packet should end with a reproducible failure or a validated fix path, not a silent pass
