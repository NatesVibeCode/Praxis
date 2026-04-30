# Simulation Runtime - 2026-04-30

## Verdict

Phase 7 adds deterministic Virtual Lab simulation primitives, not a live
automation executor.

Object Truth remains the authority for observed client-system facts. Phase 6
Virtual Lab state remains the authority for modeled object mutation. The Phase
7 runtime coordinates scenarios, simulated actions, automation eligibility,
trace collection, assertions, verifiers, typed gaps, and blockers around those
state commands.

## Authority Model

Current boundary:

- `runtime.object_truth` owns observed client facts, lineage, freshness, and
  evidence.
- `runtime.virtual_lab.state` owns environment revisions, copy-on-write object
  overlays, state events, receipts, and replay.
- `runtime.virtual_lab.simulation` owns predicted execution consequences for a
  single deterministic scenario run.

The simulation layer does not persist state, register CQRS operations, call
external systems, use ambient time, or use ambient randomness.

## Scenario Contract

A scenario contains:

- `SimulationInitialState`
- ordered `SimulationAction` values
- deterministic `SimulationConfig`
- optional `AutomationRule` values
- optional `SimulationAssertion` values
- optional `SimulationVerifier` values

`SimulationConfig` requires an explicit seed and controlled clock start. Runtime
event times are produced by a deterministic tick clock. The seed is recorded in
the scenario/config digest; no ambient random source is used.

## Supported Actions

The action dispatcher supports the Phase 6 object mutation vocabulary:

- `patch_object`
- `replace_object_overlay`
- `tombstone_object`
- `restore_object`

Each supported action is dispatched through the Phase 6 state command helpers.
Unsupported action kinds return an `ActionExecutionResult` with
`status=unsupported`, a `SimulationTypedGap`, and a `PromotionBlocker`.
Unsupported behavior is never hidden as a successful no-op.

## Automation Ordering

Automation rules are canonicalized by:

1. `priority`
2. `rule_id`

After a state event, each active rule is evaluated in that order. Eligible rules
emit `AutomationEvaluationResult` and `AutomationFiringResult` records. Effects
are normal `SimulationAction` values and are queued ahead of remaining scenario
actions so automation consequences drain predictably after the triggering
change.

Automation command ids include the deterministic firing ordinal. That lets the
runtime model repeated automation firings without accidentally collapsing a loop
into a duplicate-command no-op.

## Loop Guard

The runtime stops with typed blockers when any configured guardrail is exceeded:

- `max_actions`
- `max_automation_firings`
- `max_recursion_depth`
- per-rule `max_firings`

Loop guard failures emit `runtime.guardrail_exceeded`, a
`SimulationTypedGap`, and a `PromotionBlocker`. The terminal stop reason is
`guardrail_exceeded`.

## Trace Contract

`SimulationTrace` includes:

- runtime `SimulationEvent` records
- Phase 6 state `EventEnvelope` records
- `StateTransition` records derived from every emitted state event
- automation evaluations
- automation firings

Every runtime event has a monotonic sequence number, deterministic id,
controlled timestamp, source area, causation id, correlation id, and typed
payload. State transitions carry pre/post state digests and action provenance.

## Assertions And Verifiers

Assertions are scenario-local expectation checks. Implemented assertion kinds:

- `final_object_field_equals`
- `event_count_at_least`
- `no_blockers`

Verifiers are post-run promotion checks over terminal state and trace.
Implemented verifier kinds:

- `trace_contains_event_type`
- `no_blockers`
- `all_assertions_passed`

Both produce machine-readable result records. Assertion and verifier failures
are not string-only; they include expected/actual values, findings, severity,
and location where applicable.

## Terminal Results

`SimulationRunResult` distinguishes:

- `passed` with `stop_reason=success`
- `failed` for assertion or verifier failures
- `blocked` for unsupported capability, runtime fault, or guardrail failure

Typed gaps and blockers are first-class result fields, not buried in log text.

## Migration Need

No migration was added.

If simulation runs need to become durable runtime authority, add a separate
DB-backed repository and CQRS command/query operations. That follow-up must
register operation catalog rows, receipt behavior, storage schema, and event
contracts explicitly rather than turning this pure module into hidden
persistence.
