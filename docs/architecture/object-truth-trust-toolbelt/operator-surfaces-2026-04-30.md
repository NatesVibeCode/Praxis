# Client Operating Model Phase 11 Operator Surfaces

## Verdict

Phase 11 operator surfaces are a read-model substrate over existing evidence, not a new authority layer.

The authority model is:

- Object Truth owns observed and resolved client-system truth.
- Virtual Lab owns simulated consequences and verifier output.
- Sandbox drift owns predicted-vs-observed divergence evidence.
- Cartridge contracts own package compatibility and readiness evidence.
- Managed runtime accounting owns run receipts, cost, and health evidence.
- Operator surfaces normalize these inputs for inspection only.

## Read-Model Contract

Every Phase 11 view returns the same envelope:

- `stable_id` / `view_id`
- `generated_at`
- `freshness`
- `permission_scope`
- `correlation_ids`
- `evidence_refs`
- `state`
- `payload`

The envelope deliberately distinguishes:

- `unknown`
- `missing`
- `not_authorized`
- `stale`
- `blocked`
- `conflict`
- `healthy`
- `empty`
- `partial`

This gives API, MCP, and Canvas surfaces one consistent inspection shape without forcing each caller to compose raw records differently.

## Implemented Views

- `system_census`: scoped counts across systems, connectors, integrations, lifecycle state, authority state, verification state, simulations, verifier queues, drift alerts, and cartridge status.
- `object_truth_view`: canonical object summary, field values, provenance, authority tags, conflicts, gaps, and scoped redaction.
- `identity_authority_view`: identity graph, source authority winners, missing authority, and conflict state.
- `simulation_timeline_view`: bounded event timeline with filtering and reverse-chronological ordering.
- `verifier_results_view`: latest verifier posture, blocking findings, advisory findings, and check matrix.
- `sandbox_drift_view`: expected/observed refs, category severity, blockers, and derived review actions.
- `cartridge_status_view`: manifest/version/readiness status, compatibility findings, warnings, and blockers.
- `managed_runtime_accounting_summary`: receipt, usage, cost, execution-mode, and pool-health summary.
- `next_safe_actions_view`: generated safe actions with snapshot refs, preconditions, expected effects, reversibility class, and blockers.
- `workflow_builder_validation`: side-effect-free composition validation with machine-readable rejection reasons.

## Safety Boundary

These builders do not:

- persist state
- mutate state
- call live systems
- register CQRS operations
- register API routes
- register MCP tools
- create UI-only authority

Unsafe or ambiguous next actions are blocked with machine-readable reasons. Stale snapshots block safe actions when freshness is required.

## Follow-Up

API and MCP registration remains a follow-up. When implemented, those surfaces should call these builders after retrieving evidence through the existing authoritative query/CQRS paths.
