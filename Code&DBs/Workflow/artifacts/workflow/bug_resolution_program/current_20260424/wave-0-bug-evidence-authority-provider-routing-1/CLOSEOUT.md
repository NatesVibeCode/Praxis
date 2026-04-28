# Closeout: provider_routing packet

Bug in scope: `BUG-D39EBC3F` `[P1/VERIFY]` workflow health reports healthy while projections are critical and routes are unhealthy.

## Status Outcome

- Verification result: `FAILED`
- Bug state: left open
- Packet outcome: blocker recorded, no resolution committed

## Narrowest Verifier Run

Command:

```bash
praxis health
```

Observed proof from the live health snapshot at `2026-04-28T13:29:31Z`:

- `preflight.overall = healthy`
- `projection_freshness_sla.status = critical`
- `projection_freshness_sla.read_side_circuit_breaker = open`
- `route_outcomes.status = error`

This is the exact contradiction described by the bug. The health surface is still reporting `healthy` while the authoritative projection freshness and route layers are failing.

## Evidence

- The `praxis health` output included the live contradiction in one response.
- Projection freshness entries showed stale critical projections:
  - `semantic_current_assertions`
  - `operator_decisions_current`
- The route failure reason reported an unavailable provider slug path and an event-loop runtime error.
- The result was returned through the CLI with a truncated preview, but the authoritative fields above were present in the output.

## Bug-State Mutation Attempt

- No bug-state mutation was performed.
- The requested bug-state verbs for this packet were `praxis workflow bugs attach_evidence` and `praxis workflow bugs resolve`.
- In this shard, the direct `praxis workflow bugs` surface was not available for use; `praxis workflow bugs --help` returned `Tool not allowed: praxis_bugs`.
- Because verification failed, the bug remains open and no resolve action was attempted.

## Unresolved Risks

- The health verdict may be cached or stale relative to the projection layer, so the contradiction could persist until the underlying aggregation boundary is fixed.
- The route failure still points at a runtime event-loop/provider-routing issue, which may be masking a broader route-outcome collection problem.
- No repair or follow-up verification was executed in this packet.

