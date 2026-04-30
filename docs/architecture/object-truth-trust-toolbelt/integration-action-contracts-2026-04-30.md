# Integration Action Contracts - 2026-04-30

## Verdict

Phase 5 should not make integrations more powerful. It should make their
behavior harder to misunderstand.

The durable shape is a capture-only contract layer over the current integration
registry and executor patterns. The registry remains the authority for which
actions exist. The contract layer records what an action means operationally:
inputs, outputs, errors, side effects, replay, permissions, events, rollback,
observability, and explicit gaps.

## Authority Model

Current runtime authority stays where it is:

- `integration_registry` owns integration identity and advertised capabilities.
- `runtime.integrations.__init__.execute_integration` owns executor selection.
- `runtime.integration_manifest` owns manifest parsing and simple HTTP handler construction.
- `runtime.integrations.platform` owns static platform executors.
- `runtime.integrations.webhook` owns outbound HTTP webhook execution.

New file:

- `runtime.integrations.action_contracts` owns typed contract capture only.

It does not execute actions, mutate registry rows, register CQRS operations,
write migrations, store credentials, or infer guarantees not present in
evidence.

## Contract Shape

Each `IntegrationActionContract` captures:

- stable `action_id`
- source and target systems
- trigger types
- typed input envelope
- typed success, partial-success, and error envelopes
- idempotency state, key origin, dedupe scope, and replay behavior
- side effects and downstream automation risk
- retry and replay expectations
- permission and executing identity bindings
- webhook/event delivery semantics
- rollback class and playbook/compensation references
- observability and audit requirements
- typed open gaps

Every contract has deterministic JSON serialization and a stable hash so future
Object Truth and Virtual Lab workers can compare contract drift.

## Gap Rule

Unknowns are first-class data. A mutating action with unknown idempotency is not
"probably fine"; it emits `unknown_idempotency_behavior`.

Validation emits typed gaps for:

- missing input schema typing
- missing output schema typing
- unknown side effects
- unknown idempotency behavior
- unclear permissions
- undocumented webhook/event versioning
- missing rollback path
- missing observability or audit coverage
- unverified automation snapshots

This is the point of the layer: it creates a machine-readable stop sign before
Virtual Lab or managed runtime treats an integration action as safe.

## Registry Drafting

`draft_contract_from_registry_definition()` turns one registry definition plus
one capability row into a conservative draft contract.

It can infer:

- action identity from `integration_id/action`
- target system from integration provider/name
- manifest body placeholders from `body_template`
- current `IntegrationResult` output/error envelope
- OAuth/API-key/no-auth identity hints from `auth_shape`
- read-only idempotency for obvious read actions
- mutating risk for write-like action names or HTTP methods

It refuses to infer:

- provider idempotency guarantees
- downstream automation behavior
- rollback safety
- tenant isolation
- least-privilege proof
- webhook delivery semantics

Those remain gaps until an authoritative export, implementation receipt, admin
capture, or owner review proves them.

## Known Platform Overrides

The module includes narrow overrides for current platform actions where runtime
code already proves stronger behavior:

| Action | Contract posture |
| --- | --- |
| `workflow/cancel` | conditionally idempotent by `run_id`; forward-fix rollback |
| `workflow/invoke` | non-idempotent dispatch; compensatable via cancel/forward-fix |
| `praxis-dispatch/dispatch_job` | non-idempotent dispatch; compensatable via cancel/forward-fix |
| `praxis-dispatch/check_status` | read-only |
| `praxis-dispatch/search_receipts` | read-only |
| `notifications/send` | non-idempotent notification delivery |

Generic webhook and connector actions remain conservative unless evidence is
captured.

## Automation Snapshots

`AutomationRuleSnapshot` captures active, disabled, or intended automation
rules with:

- source-of-truth reference
- snapshot timestamp
- trigger and filter conditions
- action chain
- suppression rules
- rate limits
- environment dependencies
- linked action contract IDs
- pause/disable method
- owner and capture confidence

Snapshots without authoritative source evidence, linked actions, pause method,
or live status emit typed gaps.

## Phase 7 Dependency

Virtual Lab should consume only contract dictionaries and validation gaps. It
should not call integration executors directly.

Simulation can proceed when:

- action contracts exist for every action in the simulated automation chain
- mutating actions have idempotency declared or a high/blocker gap is carried
- side effects are explicit enough to model state consequences
- rollback class is declared
- event delivery semantics are known or represented as simulation uncertainty
- permissions are known or the simulation blocks promotion

## Migration Need

No migration was added in Phase 5.

The next durable step is a DB-backed contract registry if operators want these
contracts promoted from code/domain fixtures into queryable runtime authority.
That should be a separate CQRS packet with operation catalog rows, receipt
requirements, and event contracts. This phase intentionally stops short of that
so it does not create a shadow authority beside `integration_registry`.
