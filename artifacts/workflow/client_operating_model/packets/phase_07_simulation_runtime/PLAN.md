# Phase 7 Build Packet: Simulation Runtime, Automation Firing, and Verifiers

## Status

- Phase: `07`
- Name: `Simulation Runtime, Automation Firing, and Verifiers`
- Intent: define the bounded implementation packet for executing simulated actions, evaluating automation rules, emitting state transitions, and validating outcomes through assertions and verifiers.
- Code changes in this packet: `not included`

## Objective

Build the simulation runtime layer that can:

1. Execute a deterministic sequence of simulated actions against modeled client state.
2. Evaluate automation rules after relevant action and state events.
3. Emit typed state transitions and runtime events for downstream inspection.
4. Run assertions and verifiers against simulation traces and terminal state.
5. Expose typed gaps and promotion blockers before any production-facing rollout.

This phase is complete when a caller can submit a simulation scenario, receive a reproducible execution trace, observe automation firings and state transitions, and run structured verifiers that return actionable pass/fail output.

## Bounded Scope

### In Scope

- Simulation runtime orchestration for scenario execution.
- Simulated action dispatcher and action result contracts.
- Automation rule evaluation and firing order.
- State transition emission and trace collection.
- Assertion and verifier interfaces.
- Typed error, warning, and gap reporting.
- Determinism controls, fixture-driven tests, and validation criteria.

### Out of Scope

- Production automation execution against live systems.
- UI authoring tools for scenarios or rules.
- Background scheduling outside a single simulation run.
- Non-deterministic external integrations without mocks/fakes.
- Rule authoring DSL redesign unless required for typing/runtime execution.

## Primary Outcomes

- A simulation run is represented as a single bounded transaction with explicit inputs, execution context, emitted events, and terminal result.
- Every simulated action produces a typed result, even on failure or no-op.
- Automation evaluation is predictable, traceable, and cycle-guarded.
- Verifiers can assert both intermediate and terminal conditions.
- Promotion to the next phase is blocked unless runtime traces, automation firings, and verifier outputs are typed and test-covered.

## Required Deliverables

1. Runtime entrypoint for executing a simulation scenario.
2. Action execution pipeline for simulated actions.
3. Automation evaluation engine with stable ordering and recursion protection.
4. State transition/event emitter with typed payloads.
5. Assertion/verifier registry or equivalent contract surface.
6. Structured simulation report format for pass/fail, gaps, and diagnostics.
7. Test suite covering happy path, failure path, ordering, determinism, and blocker cases.

## Execution Model

### Runtime Flow

1. Accept a typed scenario definition and initial modeled state.
2. Normalize and validate inputs before execution begins.
3. Open a simulation run context with run id, clock, seed, and trace collector.
4. Execute each scenario action in sequence.
5. After each action, emit action result events and resulting state transitions.
6. Evaluate all relevant automation rules against the updated state and recent events.
7. Execute eligible automation firings in deterministic order.
8. Continue until the action queue and automation queue are drained or a stop condition is reached.
9. Run scenario assertions and global verifiers.
10. Return a terminal simulation report with trace, verdicts, blockers, and typed gaps.

### Determinism Requirements

- Stable action ordering.
- Stable automation ordering for equally eligible rules.
- Seeded randomness only; no ambient randomness.
- Controlled clock/time source.
- No network or live side effects.
- Serialized event ids or monotonic sequence numbers for replay/debugging.

## Workstreams

### 1. Simulation Runtime Core

Implement the top-level runtime coordinator.

Responsibilities:

- Validate scenario shape and runtime prerequisites.
- Hold run-scoped context such as seed, clock, trace, and config.
- Coordinate action execution, event emission, automation evaluation, and verifier execution.
- Enforce stop conditions, max step limits, and failure escalation rules.

Acceptance criteria:

- A single API can run a full scenario end-to-end.
- Runtime output always includes terminal status and a complete trace summary.
- Hard failures and soft failures are distinguished.

### 2. Simulated Action Execution

Implement the action dispatcher and per-action result normalization.

Responsibilities:

- Resolve action handlers by typed action kind.
- Execute against the in-memory modeled state.
- Return typed results for success, no-op, retryable error, non-retryable error, and unsupported action.
- Capture before/after state references or equivalent state deltas.

Acceptance criteria:

- All supported actions return a uniform result envelope.
- Unsupported or partially typed actions surface as typed gaps, not silent failures.
- Action execution is traceable and replayable from recorded inputs.

### 3. Automation Rule Evaluation and Firing

Implement deterministic automation handling after state changes.

Responsibilities:

- Determine which rules are eligible after an action or transition.
- Evaluate rule predicates against current state and recent event context.
- Emit rule evaluation and firing events.
- Guard against loops, duplicate firings, and unbounded recursion.

Acceptance criteria:

- Rule evaluation order is documented and stable.
- Firings are attributable to the triggering event/transition.
- Loop guards produce explicit blocker or warning output when limits are hit.

### 4. State Transition Emission

Implement typed event/transition generation.

Responsibilities:

- Emit state transitions whenever modeled state changes.
- Distinguish action events, automation events, transition events, warnings, and runtime faults.
- Persist enough metadata for assertion and debugging use.

Acceptance criteria:

- Every material state mutation has a corresponding emitted transition.
- Events carry sequence numbers and causal links where available.
- Downstream verifiers can consume the same typed event stream without re-deriving state changes.

### 5. Assertions and Verifiers

Implement runtime assertion contracts and post-run verifier contracts.

Responsibilities:

- Support scenario-level assertions on intermediate and final conditions.
- Support reusable verifiers for invariants, safety conditions, and coverage expectations.
- Return structured findings with severity, location, and recommended remediation path.

Acceptance criteria:

- Assertion failures identify the failed predicate and trace location.
- Verifiers can inspect both final state and full event trace.
- Results are machine-readable and suitable for promotion gates.

## Contract Surface

The following contract families must exist, even if exact names differ:

### Scenario Input Contracts

- `SimulationScenario`
- `SimulationInitialState`
- `SimulationAction`
- `SimulationConfig`

Minimum properties:

- scenario/run identity
- ordered actions
- initial state snapshot or builder input
- deterministic config: seed, clock, limits
- optional assertions/verifiers

### Runtime Output Contracts

- `SimulationRunResult`
- `SimulationTrace`
- `SimulationEvent`
- `StateTransition`
- `AutomationEvaluationResult`
- `AutomationFiringResult`
- `AssertionResult`
- `VerifierResult`

Minimum properties:

- terminal status
- emitted event list or stream handle
- final state snapshot or reference
- failures, warnings, typed gaps, blockers
- timing/step counts

### Error and Gap Contracts

- `SimulationRuntimeError`
- `SimulationTypedGap`
- `PromotionBlocker`

Minimum properties:

- stable code
- human-readable message
- severity
- source area: action, automation, verifier, transition, runtime
- trace location or event reference when available

## Typed Gaps To Close

The packet is not promotable if any of the following remain untyped or implicit:

- Action result variants and failure modes.
- Automation predicate evaluation result shape.
- Automation firing provenance: what triggered the rule and why it fired.
- Transition payload shape for each modeled entity/state kind.
- Assertion/verifier output schema.
- Runtime stop reasons: success, assertion failure, runtime fault, guardrail exceeded, unsupported capability.
- Gap/blocker representation surfaced to callers.

Expected typed-gap classes:

- unsupported simulated action
- unsupported rule predicate/effect
- untyped state transition payload
- unverifiable scenario condition
- ambiguous automation ordering
- non-deterministic dependency

## Promotion Blockers

Do not promote beyond this phase if any blocker is present:

1. Simulation runs are not deterministic under identical inputs.
2. Automation rules can fire without traceable provenance.
3. State mutations can occur without emitted transitions.
4. Assertion or verifier failures are string-only and not machine-readable.
5. Unsupported actions/rules fail silently or degrade into generic errors.
6. Loop/cycle protection is absent or untested.
7. Terminal reports do not distinguish runtime faults from expectation failures.
8. Core runtime paths lack fixture or integration test coverage.

## Testing Requirements

### Unit Tests

- Action handler success/failure/no-op envelopes.
- Automation predicate evaluation truth table behavior.
- Transition emission for each supported state mutation type.
- Assertion/verifier result formatting and severity mapping.
- Stop-reason mapping and blocker generation.

### Integration Tests

- End-to-end scenario with manual actions only.
- End-to-end scenario where actions trigger multiple automation firings.
- Scenario with chained transitions and terminal verifier checks.
- Scenario that hits loop guard/max-step protection.
- Scenario with unsupported capability producing typed gaps and blocked promotion.

### Determinism Tests

- Same scenario, same seed, same output trace.
- Same scenario, different seed only changes seed-dependent branches.
- Stable ordering when multiple automations are eligible simultaneously.

### Negative Tests

- Invalid scenario definition rejected before execution.
- Malformed rule/effect surfaces structured runtime error.
- Assertion failure preserves trace and final partial state.
- Verifier crash is isolated and surfaced as verifier/runtime failure per design.

## Validation Plan

Validation for this packet must include:

1. A canonical fixture set of scenarios with expected traces and outcomes.
2. Replay validation proving trace reproducibility.
3. Manual inspection of at least one automation-heavy scenario to confirm causal ordering.
4. Contract review ensuring all public runtime payloads are typed and documented.
5. Gate review against promotion blockers listed in this packet.

## Implementation Notes

- Favor pure functions at the action, transition, and rule-evaluation boundaries.
- Keep runtime side effects isolated to trace/report emission abstractions.
- Prefer append-only trace construction over mutable hidden logs.
- Make guardrails explicit: max actions, max firings, max recursion depth, max warnings.

## Open Questions

- What existing state model is the source of truth for transition typing?
- Are automation effects limited to the same action vocabulary or do they need a distinct effect contract?
- Should verifier execution short-circuit on fatal failures or always produce a full findings set?
- Is partial-state persistence required after hard runtime failure?

These questions should be resolved before implementation begins, but they do not justify widening the phase scope.

## Exit Criteria

This packet is complete when:

- The runtime can execute bounded simulation scenarios end-to-end.
- Simulated actions, automation evaluations, firings, and transitions are all emitted as typed artifacts.
- Assertions and verifiers return structured, machine-readable results.
- Typed gaps and promotion blockers are surfaced in terminal reports.
- Required unit, integration, determinism, and negative tests exist and pass.
- No out-of-scope production execution capability is introduced.
