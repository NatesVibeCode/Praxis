# Phase 11 Build Packet: Operator Inspection, Canvas Surfaces, and Workflow Builder UX

## Intent

Phase 11 delivers the operator-facing surfaces required to inspect system truth, understand source authority, observe simulation and verifier outcomes, detect sandbox drift, assess cartridge status, and take only bounded safe actions from the workflow builder. This phase is UI and contract focused. It does not introduce new execution engines, new persistence models, or open-ended authoring.

## Outcome

At the end of Phase 11, an operator can:

1. View a system census across relevant objects and execution domains.
2. Inspect an object’s current truth, provenance, and authority chain.
3. Compare claimed identity with source-backed identity and conflict state.
4. Review simulation timeline events and verifier outputs in one place.
5. Detect sandbox drift and cartridge health/status before taking action.
6. See the next safe actions the system is willing to permit.
7. Access the same information over bounded API and MCP surfaces.

## Non-Goals

- No freeform workflow programming language.
- No autonomous repair loops.
- No silent mutation of system state from inspection surfaces.
- No expansion of cartridge runtime semantics.
- No redesign of core data model beyond read-model shaping needed for these surfaces.

## Build Boundaries

### In Scope

- Read-oriented operator views.
- Workflow builder UX for constrained, suggested, safe-next actions.
- API endpoints and MCP tools for census, inspection, timeline, verifier, drift, and cartridge state.
- Test coverage for contracts, permissions, empty states, conflict states, and stale data handling.
- Validation scripts/checklists proving the surfaces reflect current truth consistently.

### Out of Scope

- Deep editing of objects from these surfaces.
- Bulk destructive actions.
- Cross-tenant aggregation if tenancy exists.
- Historical replay beyond bounded timeline windows already supported by the platform.
- New auth model; phase consumes existing authn/authz primitives.

## Primary Operator Surfaces

### 1. System Census Views

Purpose: provide a fast top-level inventory of what exists, what is healthy, and what needs attention.

Required capabilities:

- Show object counts by type, lifecycle state, authority state, and verification state.
- Show active simulations, failed simulations, pending verifications, and drift alerts.
- Show cartridge counts by status: healthy, degraded, blocked, missing, stale.
- Support filtering by environment, workspace, owner, source, cartridge, and last-updated window.
- Support drill-down into object truth inspection from any row or aggregate.

Required states:

- Empty census.
- Partial data unavailable.
- Stale snapshot with timestamp and freshness indicator.
- Permission-limited census where totals are scoped.

### 2. Object Truth Inspection

Purpose: let an operator answer “what is true right now about this object, why do we believe it, and what is unresolved?”

Required sections:

- Canonical object summary.
- Current resolved truth fields.
- Field-level provenance and confidence/authority tags.
- Last mutation, last verification, last simulation touch.
- Conflicts, mismatches, suppressed issues, and unresolved questions.

Interaction rules:

- Read-first default.
- Expandable field-level provenance.
- Clear distinction between observed value, inferred value, operator-provided value, and source-authoritative value.
- Links to upstream source records and downstream dependents where available.

### 3. Identity and Source Authority Views

Purpose: expose which identity/source wins, where conflicts exist, and how authority was resolved.

Required sections:

- Identity graph: canonical ID, aliases, external IDs, cartridge-local IDs.
- Source authority ranking and current winner per field group.
- Source freshness timestamps.
- Identity collisions and split/merge suspicion indicators.
- Authority explanation panel stating why a source won.

Must handle:

- Source disagreement.
- Missing authoritative source.
- Circular references or duplicate mappings.
- Recently changed authority configuration.

### 4. Simulation Timeline

Purpose: reconstruct the operator-relevant sequence of events for a workflow, object, or cartridge.

Required timeline events:

- Workflow created/queued/started/completed/failed/cancelled.
- Simulation checkpoints.
- Verification runs and verdict changes.
- Sandbox materializations and drift detections.
- Cartridge installed/updated/degraded/blocked transitions.
- Safe action recommendations produced or revoked.

Timeline requirements:

- Reverse chronological by default with exact timestamps.
- Filter by event type, severity, actor, cartridge, and correlation ID.
- Collapsible event payloads.
- Stable references to related object inspection and verifier records.

### 5. Verifier Results

Purpose: make verifier outcomes legible and actionable without requiring raw log inspection.

Required sections:

- Latest verdict summary.
- Rule/check matrix with pass/fail/warn/skip.
- Evidence excerpts or references.
- Regression deltas from prior run.
- Blocking vs advisory findings.

UX requirements:

- One-click navigation from a failed object or workflow to the relevant verifier details.
- Explicit statement of what prevents the next action.
- Display of verifier version and input snapshot timestamp.

### 6. Sandbox Drift

Purpose: surface divergence between expected and observed sandbox state.

Required sections:

- Expected baseline/snapshot reference.
- Observed current state reference.
- Drift summary by category: files, config, env, dependencies, permissions, outputs.
- Severity classification: informational, caution, blocking.
- First seen, last seen, and whether drift is growing/shrinking.

Operator needs:

- Distinguish intended mutation from unexpected drift.
- Show drift provenance where known.
- Show why drift blocks or does not block safe actions.

### 7. Cartridge Status

Purpose: expose cartridge availability, compatibility, health, and readiness for use.

Required sections:

- Installed version and expected version.
- Capability summary.
- Health state and reason.
- Dependency and compatibility checks.
- Last successful run and last failure.
- Blockers preventing use in current workflow context.

### 8. Next Safe Actions

Purpose: convert inspection into bounded action recommendations.

Action model:

- Suggested actions are generated, not freeform.
- Every action shows preconditions, expected effects, reversibility class, and blockers.
- Unsafe or ambiguous actions are omitted rather than merely disabled when confidence is too low.
- Actions must reference the exact state snapshot they were derived from.

Examples:

- Re-run verifier against latest snapshot.
- Refresh source import for stale authority.
- Open cartridge diagnostics.
- Reconcile identity conflict.
- Re-run simulation from last stable checkpoint.

### 9. Workflow Builder UX

Purpose: let operators assemble bounded workflows from safe primitives while preserving system guardrails.

Required builder constraints:

- Palette contains only approved workflow blocks.
- Builder only permits valid compositions.
- Builder explains why an edge or step is not allowed.
- Builder surfaces upstream prerequisites and downstream effects before save/run.
- Builder shows preflight validation and final safe-action summary.

Required builder views:

- Workflow canvas or structured list builder.
- Step inspector.
- Data/source dependency panel.
- Validation panel.
- Simulation preview entry point.

## Data and Read-Model Requirements

Phase 11 should rely on read models optimized for inspection rather than forcing the UI to compose raw records ad hoc.

Required read models:

- `system_census`
- `object_truth_view`
- `identity_authority_view`
- `simulation_timeline_view`
- `verifier_results_view`
- `sandbox_drift_view`
- `cartridge_status_view`
- `next_safe_actions_view`

Each read model must include:

- Stable ID.
- Snapshot/generated timestamp.
- Freshness or staleness indicator.
- Correlation IDs for cross-surface linking.
- Permission-scoped payload shape.

## API Surface

Expose bounded read endpoints only. Naming may adapt to platform conventions, but the contract categories should remain intact.

### Required Endpoints

- `GET /operator/census`
- `GET /operator/objects/{id}/truth`
- `GET /operator/objects/{id}/identity-authority`
- `GET /operator/workflows/{id}/timeline`
- `GET /operator/workflows/{id}/verifier-results`
- `GET /operator/sandboxes/{id}/drift`
- `GET /operator/cartridges/{id}/status`
- `GET /operator/workflows/{id}/next-safe-actions`
- `POST /operator/workflows/validate-builder`

### Contract Expectations

- All read endpoints return snapshot timestamp and freshness metadata.
- Responses distinguish `unknown`, `missing`, and `not_authorized`.
- Filtering and pagination are explicit and stable.
- Timeline and verifier payloads support bounded expansion of details.
- Validation endpoint is side-effect free.

### Error Handling

- `404` when the resource is absent in scope.
- `403` when visible existence must be suppressed by policy, use current platform convention consistently.
- `409` for stale snapshot or authority conflict on validation requests where applicable.
- `422` for invalid builder compositions.

## MCP Surface

Mirror the operator inspection capability set through MCP tools so agentic/operator tooling can access the same truth safely.

### Required MCP Tools

- `operator.get_census`
- `operator.inspect_object_truth`
- `operator.inspect_identity_authority`
- `operator.get_simulation_timeline`
- `operator.get_verifier_results`
- `operator.get_sandbox_drift`
- `operator.get_cartridge_status`
- `operator.get_next_safe_actions`
- `operator.validate_workflow_builder`

### MCP Constraints

- Tools are read-only except validation, which remains side-effect free.
- Tool schemas must use bounded enums and documented optional filters.
- Tool outputs must match API semantics closely enough for cross-channel consistency tests.
- Tool results must carry provenance/freshness metadata, not just content payloads.

## Permissions and Safety

Minimum expectations:

- Every surface is permission scoped.
- Source details can be redacted while still exposing existence of a conflict if policy allows.
- Safe actions are computed from current scoped truth, not global hidden state.
- Builder validation must not leak inaccessible objects or cartridges through detailed errors.

## UX Acceptance Criteria

The phase is complete only if the following are demonstrably true:

1. An operator can move from census to a specific failing object in two hops or fewer.
2. An operator can identify the authoritative source for a field without reading raw records.
3. An operator can see why a verifier blocked progress and what safe action remains available.
4. An operator can distinguish sandbox drift from intended execution changes.
5. An operator can assess whether a cartridge is safe and compatible to use.
6. The workflow builder refuses invalid compositions before execution.
7. API and MCP surfaces return materially consistent answers for the same object snapshot.

## Test Plan

### Contract Tests

- Census endpoint/tool returns expected aggregates, filters, and empty states.
- Object truth contract preserves provenance, authority, and conflict fields.
- Identity authority contract handles winner, tie, missing authority, and stale source cases.
- Timeline contract preserves order, pagination, filtering, and event correlation.
- Verifier results contract distinguishes blocking and advisory outcomes.
- Drift contract includes expected vs observed references and severity.
- Cartridge status contract includes compatibility and health fields.
- Next safe actions contract includes preconditions, blockers, and snapshot reference.
- Builder validation contract rejects invalid graphs and returns machine-readable reasons.

### Integration Tests

- Census drill-down to object truth.
- Object truth links to identity authority and verifier result views.
- Timeline event links resolve to referenced objects/verifications.
- Safe actions derived from a verifier-blocked workflow are consistent with cartridge and drift state.
- API and MCP produce equivalent payload classes for the same fixtures.

### UI Tests

- Loading, empty, error, stale, and permission-limited states for each surface.
- Filtering and pagination behavior on census and timeline.
- Expand/collapse provenance and verifier evidence panels.
- Builder validation feedback for invalid joins, missing prerequisites, and blocked cartridges.

### Negative Tests

- Hidden resources do not leak via counts, errors, or action suggestions.
- Stale snapshots do not produce misleading safe actions.
- Missing authority data is shown as missing, not as an implicit failure.
- Partial subsystem outages degrade the surface gracefully with explicit warnings.

## Validation Plan

Validation must be executable by product, design, and engineering without code archaeology.

### Fixture Set

Prepare at least these fixture scenarios:

- Healthy object with single authoritative source.
- Object with source conflict.
- Workflow with failed verifier and available safe next action.
- Sandbox with non-blocking drift.
- Sandbox with blocking drift.
- Cartridge degraded due to compatibility issue.
- Builder graph rejected due to invalid step composition.

### Manual Validation Script

1. Open census and confirm counts match fixture inventory.
2. Drill into a conflicted object and confirm field-level provenance is visible.
3. Confirm authority view identifies the winning source and explains why.
4. Open the workflow timeline and confirm verifier and drift events are present in order.
5. Open verifier results and confirm blocking rules are clearly separated from advisory rules.
6. Open sandbox drift and confirm expected vs observed state is interpretable.
7. Open cartridge status and confirm compatibility blocker is explicit.
8. Open next safe actions and confirm only permitted actions are shown.
9. Use builder validation and confirm the invalid graph is rejected before execution.
10. Cross-check one scenario via API and MCP and confirm semantic parity.

## Delivery Checklist

- Packet-reviewed scope agreed.
- Read-model shapes defined.
- API contracts defined.
- MCP tool schemas defined.
- UX states and acceptance criteria defined.
- Test fixtures identified.
- Validation script agreed.
- No code changes bundled into this packet.

## Exit Criteria

Phase 11 is ready to hand to implementation when:

- The scope above is accepted without ambiguous surface ownership.
- Each operator question has a single canonical surface answer.
- Each surface has a bounded contract and explicit non-goals.
- Test and validation coverage are sufficient to detect truth mismatches across UI, API, and MCP.
