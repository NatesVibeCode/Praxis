# Sandbox Drift - 2026-04-30

## Verdict

Phase 8 adds bounded sandbox promotion/readback/drift primitives, not a live
sandbox executor.

Object Truth still owns observed client-system facts. Virtual Lab simulation
still owns predicted behavior. The new sandbox drift layer owns the comparison
and classification record that proves whether sandbox readback confirms,
partially confirms, falsifies, or blocks those predictions.

## Authority Model

Current boundary:

- `runtime.object_truth` owns observed source facts, lineage, freshness, and
  evidence.
- `runtime.virtual_lab.state` owns modeled environment revisions and predicted
  object state transitions.
- `runtime.virtual_lab.simulation` owns deterministic predicted scenario
  execution.
- `runtime.virtual_lab.sandbox_drift` owns Phase 8 promotion manifest,
  execution/readback evidence records, predicted-vs-actual comparison rows,
  drift classifications, handoff references, and stop/continue summaries.

The sandbox drift module does not deploy builds, mutate live or sandbox
environments, call integrations, persist records, file bugs, or open gaps.
Those are separate operator actions through the authoritative surfaces.

## Promotion Manifest

`PromotionManifest` records the bounded set of candidates allowed into a Phase
8 validation window. Each `PromotionCandidate` carries:

- candidate id
- owner
- build or version reference
- sandbox target
- prior scope reference
- scenario references
- prediction references
- contract and assumption references

Duplicate candidate ids fail closed. The manifest has a deterministic digest
for receipts or later DB-backed projection work.

## Execution And Readback Evidence

`SandboxExecutionRecord` captures the controlled sandbox run metadata:

- execution id
- candidate id
- scenario ref
- sandbox target
- environment/config/seed refs
- terminal execution status
- start/end timestamps
- explicit deviations and operator intervention flag

`SandboxReadbackEvidence` records actual readback observations without hiding
trust state. Evidence has separate `available` and `trusted` booleans plus an
immutable reference when available. Missing or untrusted required evidence
blocks comparison instead of implying success.

## Comparison Contract

`PredictedActualCheck` is the explicit comparison input. It binds:

- comparison dimension
- predicted value
- actual value
- evidence refs
- whether evidence is required
- optional accepted status for bounded manual judgments such as
  `partial_match`
- impact and disposition notes

`compare_predicted_actual()` returns a `SandboxComparisonReport` whose rows use
the stable Phase 8 states:

- `match`
- `partial_match`
- `drift`
- `blocked`

Rollup is conservative: any blocked row blocks the report; any drift row makes
the report drift; otherwise partial match outranks match.

## Drift Classification

Every non-match row must be represented by a `DriftClassification` before a
`DriftLedger` can be built. Stable reason codes are:

- `ENV_MISCONFIG`
- `SEED_DATA_VARIANCE`
- `CONTRACT_UNDERSPECIFIED`
- `CONTRACT_INCORRECT`
- `IMPLEMENTATION_DEFECT`
- `PREDICTION_ERROR`
- `DEPENDENCY_CHANGE`
- `OBSERVABILITY_GAP`
- `TEST_HARNESS_FAULT`
- `NONDETERMINISM`
- `UNKNOWN`

Classifications also carry severity, layer, disposition, owner, cause
assessment, and optional handoff refs.

## Implementation Defect Guardrail

The module refuses `IMPLEMENTATION_DEFECT` unless the classification cause
assessment explicitly excludes environment, contract, and harness causes.

This is intentional. Phase 8 is supposed to separate product defects from
sandbox setup, seed data, contract ambiguity, prediction errors, and harness
faults. A bug handoff is only supportable after those alternative explanations
are excluded.

## Handoff References

`HandoffReference` records proposed or linked downstream authority work without
performing it. Supported kinds:

- bug
- gap
- contract note
- evidence
- receipt

This keeps the pure-domain layer honest: it can say what should be handed off,
but it does not silently become the bug tracker, gap log, or contract writer.

## Stop Or Continue

`build_stop_continue_summary()` turns manifest candidates, comparison reports,
and drift ledgers into one Phase 8 summary.

Recommendations:

- `continue`
- `continue_with_constraints`
- `rerun_phase`
- `stop`

Candidate decisions:

- `validated`
- `drifted`
- `blocked`
- `stopped`

Critical drift or `stop_phase` dispositions stop. Blocked evidence or
`rerun_required` dispositions require rerun. Non-critical drift continues only
with explicit constraints.

## Migration Need

No migration was added.

If Phase 8 records need to become durable runtime authority, add a separate
DB-backed repository and CQRS operations. That follow-up should register the
operation catalog rows, receipt behavior, event contracts, storage schema, and
read models explicitly rather than turning this pure module into hidden
persistence.
