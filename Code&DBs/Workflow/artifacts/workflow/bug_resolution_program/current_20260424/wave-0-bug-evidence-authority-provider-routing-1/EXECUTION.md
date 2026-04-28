# Execution: provider_routing packet

## Summary

This packet documents a proof-backed deferred outcome for `BUG-D39EBC3F` rather than a code fix.

The live health surface still reports an impossible combination:

- `preflight.overall = healthy`
- projection freshness SLA = `critical`
- read-side circuit breaker = `open`
- route outcomes = `error`

That contradiction is enough to confirm the bug remains real, but this job boundary does not include a safe implementation surface to correct it.

## Changed Files

- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-0-bug-evidence-authority-provider-routing-1/EXECUTION.md`

## Evidence Collected

- Read `PLAN.md` first, per instruction, and preserved its stop boundary.
- Ran `praxis discover 'workflow health provider routing BUG-D39EBC3F'`.
  - Result: blocked by shard enforcement with `Tool cannot prove workflow shard enforcement yet: praxis_discover`.
- Ran `praxis health` and `praxis workflow tools call praxis_health --input-json '{}'`.
  - Both returned the same contradiction in the live health snapshot.
  - Projection freshness entries showed stale projections:
    - `semantic_current_assertions` stale by about 22,700 seconds, status `critical`
    - `operator_decisions_current` stale by about 22,700 seconds, status `critical`
    - `bug_candidates_current` lagging, status `warning`
  - `projection_freshness_sla.status = critical`
  - `projection_freshness_sla.read_side_circuit_breaker = open`
  - `route_outcomes.status = error`
  - route failure reason reported `provider_slugs unavailable ... Cannot run the event loop while another loop is running`
- Local workspace inspection under the active shard showed only the planning artifact in this packet directory; no implementation files were available in scope to modify.

## Intended Terminal Status Per Bug

- `BUG-D39EBC3F`: `DEFERRED`

## Rationale

The bug is reproducible from live authority, but this job is constrained to evidence capture and documentation. No code or tests were changed, and no DB mutation was attempted. The smallest durable action available in this packet is to preserve the proof and hand off a deferred terminal status for a follow-up implementation packet with a valid fix surface.
