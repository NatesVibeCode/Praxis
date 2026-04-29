# Closeout

Packet: `bug_resolution_program_20260428_full.wave-0-authority-repair-authority-bug-system-historical-spec-guard-regression-2026-04-28`
Job: `verify_and_resolve_packet`
Verify ref: `verify.bug_resolution_packet.wave-0-authority-repair-authority-bug-system-historical-spec-guard-regression-2026-04-28.verify_and_resolve_packet`

## Terminal Status

Bug remains `OPEN`.

## Rationale

Verification did not pass cleanly on the required workflow surfaces, so the bug cannot be truthfully resolved from this shard.

## Proof

1. Packet authority remains `docker_packet_only`; `praxis context_shard --view summary` returned only packet write scope plus the verify ref, not a hydrated product authority tree.
2. `praxis workflow tools call praxis_orient --input-json '{}'` returned cleanly and confirmed the session is operating against `/workspace`.
3. The bug tracker surface required by orient is not callable from this token:
   - `praxis workflow tools describe praxis_bugs` returned `unknown tool: praxis_bugs`
   - `PRAXIS_ALLOWED_MCP_TOOLS` does not include `praxis_bugs`
4. Query-based fallback did not return cleanly:
   - direct alias calls such as `praxis query "..."` failed with `Tool cannot prove workflow shard enforcement yet: praxis_query`
   - canonical call `praxis workflow tools call praxis_query --input-json '{...}'` failed with `TimeoutError: timed out`
5. Replay-readiness is not clean at the platform read side:
   - `praxis workflow tools call praxis_health --input-json '{}'` returned a truncated but successful payload whose `preflight.overall` was `degraded`
   - the same payload reported `projection_freshness_sla.read_side_circuit_open`

## Conclusion

The packet's earlier documentation supports an authority-bounded `DEFERRED` disposition for the historical-spec guard regression itself, but this verification job has a stricter closeout contract: workflow orient plus bug stats/list/search plus replay-ready view must all return cleanly for the affected path. That contract is not met here because the bug surface is unavailable to this workflow token and the read-side health is degraded with the circuit breaker open.

## Required Follow-up

1. Re-run this packet in a shard that exposes the bug tracker surface (`praxis_bugs` or its allowed equivalent) to the workflow token.
2. Re-run after the read-side circuit breaker is closed and replay-readiness is clean.
3. Only then attach proof and resolve through the bug tracker surface if the authority-bounded disposition remains supported.
