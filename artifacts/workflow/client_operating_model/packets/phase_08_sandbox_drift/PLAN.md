# Phase 8: Live Sandbox Promotion and Drift Feedback

## Intent

This packet defines a bounded execution plan for promoting selected candidates into a live sandbox, reading back actual sandbox behavior, comparing predicted outcomes to observed outcomes, and feeding drift findings back into contracts, gaps, and bug records.

Phase 8 is an operational validation phase. It does not expand feature scope, redesign upstream planning, or authorize production rollout.

## Objectives

1. Promote a controlled set of phase-ready candidates into the sandbox.
2. Execute the promoted candidates under bounded, repeatable sandbox conditions.
3. Read back actual outputs, state transitions, and failure signals from the sandbox.
4. Compare predicted behavior against actual behavior at the contract, workflow, and system levels.
5. Classify drift with stable reason codes.
6. Update project records for contracts, gaps, and bugs based on observed drift.
7. Produce a validation package that clearly supports either continuation or stop.

## In Scope

- Promotion candidate selection and readiness screening.
- Sandbox deployment or activation of selected candidates.
- Controlled scenario execution in sandbox.
- Capture of actual responses, artifacts, logs, and state deltas.
- Predicted-vs-actual comparison against explicit expectations.
- Drift classification and triage.
- Documentation updates to contracts, gaps, and bug inventories.
- Targeted test execution needed to validate Phase 8 conclusions.
- A bounded exit decision for each candidate and for the phase overall.

## Out of Scope

- Production release or broad user rollout.
- Net-new feature work unrelated to sandbox drift findings.
- Refactoring for code quality alone.
- Performance tuning beyond what is required to explain observed drift.
- Open-ended exploratory testing without a mapped hypothesis.
- Rewriting upstream requirements unless the observed drift proves they are incorrect.

## Inputs

- Approved promotion candidates from prior phases.
- Candidate predictions, expected outputs, and acceptance criteria.
- Current contract definitions and known assumptions.
- Existing gap log and bug inventory.
- Sandbox environment definition, seed data, and execution constraints.
- Test suite segments relevant to promoted flows.

## Deliverables

- A promotion manifest listing each candidate, owner, version, and sandbox target.
- A sandbox execution record for each run.
- A predicted-vs-actual comparison table for each candidate.
- A drift ledger with reason codes, severity, and disposition.
- Updated contract notes where observed behavior changes understanding.
- Updated gaps log for unresolved mismatches or coverage holes.
- Updated bug records for confirmed implementation defects.
- A phase validation summary with explicit stop/continue decisions.

## Entry Criteria

A candidate may enter Phase 8 only if all of the following are true:

- The candidate has a stable build artifact or release identifier.
- Expected behavior is written in a form that can be compared to observed behavior.
- Required contracts and assumptions are versioned or otherwise pinned.
- Sandbox access, credentials, and seed conditions are available.
- The run plan specifies scenarios, observables, and failure thresholds.
- There is an assigned owner for triage and follow-up.

## Work Sequence

### 1. Promotion Candidate Selection

For each candidate:

- Confirm the candidate maps to a prior approved scope item.
- Confirm the candidate is bounded enough to isolate drift causes.
- Confirm dependencies are known and either fixed or explicitly acknowledged.
- Record the prediction set:
  - expected contract behavior
  - expected state transitions
  - expected user-visible outputs
  - expected non-success behaviors, if applicable

Output:

- Promotion manifest with candidate IDs, build/version references, scenarios, and owners.

### 2. Sandbox Promotion

Promote only the approved manifest items into the sandbox.

For each promotion:

- Record artifact/version identifiers.
- Record environment identifiers and configuration references.
- Record seed data version or preparation method.
- Record start time, operator, and any pre-run deviations.

Constraints:

- No untracked configuration changes during execution.
- No parallel changes to the same candidate within the same validation window.
- If sandbox conditions drift before execution starts, abort and restage.

Output:

- Promotion record attached to the candidate execution package.

### 3. Sandbox Execution

Execute only the predefined scenarios.

For each run:

- Execute the exact scenario steps in sequence.
- Capture actual outputs, status codes, messages, state changes, and logs.
- Capture timing and retry behavior if these affect contract outcomes.
- Mark any operator intervention explicitly.

Execution rules:

- If a blocking failure prevents scenario completion, stop that scenario and classify the failure path.
- If the sandbox itself is unstable, stop the run and classify it as environment-origin drift until disproven.
- Do not broaden into ad hoc scenarios during the main run.

Output:

- Per-scenario execution record with raw evidence links or references.

### 4. Readback and Evidence Capture

Read back the sandbox state after execution using the same observables used in prediction.

Readback should include, where applicable:

- API responses
- persisted records
- event emissions
- derived artifacts
- audit/log traces
- user-visible UI or workflow state

Evidence rules:

- Prefer immutable or timestamped evidence.
- Preserve enough context to reproduce the comparison.
- Mark missing evidence explicitly rather than inferring success.

Output:

- Candidate evidence package with readback snapshots and evidence index.

### 5. Predicted-vs-Actual Comparison

Compare each candidate against its explicit prediction set.

Minimum comparison dimensions:

- contract conformance
- output correctness
- state transition correctness
- error-path correctness
- sequencing/order correctness
- data shape/content correctness
- operational behavior relevant to acceptance

Comparison result states:

- `match`: actual behavior aligns with prediction within accepted tolerance
- `partial_match`: core flow succeeds but one or more bounded deviations exist
- `drift`: actual behavior materially differs from prediction
- `blocked`: comparison cannot be completed due to environment or evidence failure

Output:

- Comparison table with prediction, actual, delta, impact, and disposition.

### 6. Drift Classification

Every non-`match` result must receive at least one drift reason code.

Primary drift reason codes:

- `ENV_MISCONFIG`: sandbox configuration differs from required setup
- `SEED_DATA_VARIANCE`: seed data shape/content invalidates prediction
- `CONTRACT_UNDERSPECIFIED`: contract existed but did not constrain the observed behavior enough
- `CONTRACT_INCORRECT`: documented contract is wrong relative to intended behavior
- `IMPLEMENTATION_DEFECT`: implementation violates intended and documented behavior
- `PREDICTION_ERROR`: expected outcome was modeled incorrectly
- `DEPENDENCY_CHANGE`: upstream or downstream dependency behavior changed
- `OBSERVABILITY_GAP`: required evidence could not be captured or trusted
- `TEST_HARNESS_FAULT`: execution harness or fixture caused invalid results
- `NONDETERMINISM`: repeated runs produce materially different outcomes without controlled explanation
- `UNKNOWN`: drift observed but root cause not yet supportable

Optional secondary tags:

- severity: `critical`, `high`, `medium`, `low`
- layer: `contract`, `workflow`, `integration`, `data`, `environment`, `observability`
- disposition: `fix_now`, `document`, `defer`, `rerun_required`, `stop_phase`

Classification rule:

- Do not assign `IMPLEMENTATION_DEFECT` unless evidence excludes contract, environment, and harness explanations sufficiently.

Output:

- Drift ledger with reason codes, evidence references, owner, and next action.

## Record Updates

### Contracts

Update contracts when observed behavior shows the current contract is missing, ambiguous, or wrong.

Allowed contract updates in this phase:

- clarify expected inputs/outputs
- clarify state transition rules
- specify tolerated variances
- document dependency assumptions
- add explicit failure-mode expectations

Contract update rule:

- Separate “intended behavior” from “current observed behavior” when they differ. Do not rewrite intent to hide a defect.

### Gaps

Open or update a gap when:

- expected evidence cannot be gathered
- acceptance criteria cannot be evaluated cleanly
- dependencies or environments cannot be controlled enough for validation
- comparison logic is incomplete

Gap entries should include:

- gap statement
- impact on validation confidence
- temporary workaround, if any
- owner and closure condition

### Bugs

Open or update a bug when:

- observed behavior violates intended behavior with sufficient evidence
- the issue is reproducible or supportably attributable
- the deviation is not explained by contract ambiguity alone

Bug entries should include:

- reproduction scenario
- expected vs actual
- candidate/build identifier
- evidence reference
- severity and owner

## Tests

Phase 8 testing is validation-oriented and should stay bounded to promoted candidates.

Required test layers:

- targeted automated tests covering the promoted flows, where such tests exist
- any regression tests directly related to observed drift
- repeat runs for nondeterministic or environment-sensitive failures

Test expectations:

- Run only the minimum relevant subset needed to confirm findings.
- Record exact test commands or suites executed.
- Distinguish pre-existing failures from phase-introduced findings.
- If a required test cannot run, record that as a validation limitation.

## Validation

Phase 8 is considered validated for a candidate only if all of the following are true:

- The candidate was promoted through a recorded, reproducible sandbox path.
- Required scenarios executed or were explicitly blocked with evidence.
- Predicted-vs-actual comparison is complete enough to support a disposition.
- All non-match outcomes have reason codes and owners.
- Required updates to contracts, gaps, and bugs have been recorded.
- Follow-up actions are explicit: promote, rerun, fix, defer, or stop.

Phase-level validation requires:

- Every candidate in the manifest has a final disposition.
- No unresolved critical drift remains without an explicit stop decision.
- Validation artifacts are sufficient for downstream review without rerunning discovery.

## Stop Boundaries

Stop the candidate immediately if any of the following occurs:

- The sandbox configuration is untrusted or changes mid-run.
- Required evidence cannot be captured, and trust cannot be restored quickly.
- A critical contract violation risks contaminating subsequent scenarios.
- Seed data is proven incompatible with the planned run and cannot be corrected within bounds.
- The result cannot be attributed cleanly enough to distinguish product behavior from environment behavior.

Stop the phase overall if any of the following occurs:

- Multiple candidates fail due to shared sandbox instability.
- Drift volume exceeds the team’s bounded triage capacity for this phase window.
- Contract ambiguity is broad enough that comparisons are no longer meaningful.
- A systemic observability failure prevents trustworthy readback.

## Exit Outputs

At phase close, publish:

- final promotion manifest with dispositions
- comparison summary by candidate
- drift ledger
- contract updates
- gap updates
- bug updates
- test and validation record
- explicit recommendation: `continue`, `continue_with_constraints`, `rerun_phase`, or `stop`

## Non-Goals and Discipline Rules

- Do not fix code as part of this packet.
- Do not absorb unexplained drift into documentation just to pass validation.
- Do not widen candidate scope to “make use” of the sandbox window.
- Do not treat missing evidence as implied success.

## Completion Standard

This packet is complete when the Phase 8 team can execute it without inventing new process, and when each promoted candidate can be cleanly classified as validated, drifted, blocked, or stopped with supporting evidence.
