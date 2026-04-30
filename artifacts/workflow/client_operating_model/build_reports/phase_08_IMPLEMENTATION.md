# Phase 08 Implementation Report

Date: 2026-04-30

## Summary

Implemented pure-domain sandbox promotion/readback/drift primitives for Client
Operating Model Phase 8.

The implementation does not deploy builds, mutate live or sandbox
environments, call integrations, persist records, file bugs, or open gaps. It
defines the bounded evidence and decision contracts needed to compare predicted
Virtual Lab behavior with actual sandbox readback and hand off drift findings
to the proper authorities.

## Existing Authority Discovery

Used the live standing-order query first, then the Praxis discovery skill, the
phase packet, and the existing Virtual Lab state/simulation contracts.

Relevant authority:

- Object Truth owns observed facts.
- Virtual Lab owns predicted consequences.
- Phase 7 simulation output is the prediction side of Phase 8.
- Sandbox drift proves or falsifies predictions with readback evidence.
- Implementation defects must not be classified unless environment, contract,
  and harness explanations are excluded.

Required local reads completed:

- `AGENTS.md`
- `artifacts/workflow/client_operating_model/packets/phase_07_simulation_runtime/PLAN.md`
- `artifacts/workflow/client_operating_model/packets/phase_08_sandbox_drift/PLAN.md`
- `Code&DBs/Workflow/runtime/virtual_lab/state.py`
- `Code&DBs/Workflow/runtime/virtual_lab/simulation.py`

## Changed Files

- `Code&DBs/Workflow/runtime/virtual_lab/sandbox_drift.py`
- `Code&DBs/Workflow/runtime/virtual_lab/__init__.py`
- `Code&DBs/Workflow/tests/unit/test_sandbox_drift.py`
- `docs/architecture/object-truth-trust-toolbelt/sandbox-drift-2026-04-30.md`
- `artifacts/workflow/client_operating_model/build_reports/phase_08_IMPLEMENTATION.md`

## Implemented Contracts

Added typed domain records for:

- promotion candidates and promotion manifests
- sandbox execution records
- sandbox readback evidence and evidence packages
- predicted-vs-actual checks and comparison rows
- comparison reports with `match`, `partial_match`, `drift`, and `blocked`
  states
- stable drift reason codes
- severity, layer, and disposition
- cause assessments
- bug/gap/contract/evidence/receipt handoff references
- drift ledgers
- candidate exit decisions
- phase stop/continue summaries

## Guardrails

The implementation defect lane is protected. `DriftClassification` rejects
`IMPLEMENTATION_DEFECT` unless the cause assessment explicitly excludes:

- environment causes
- contract causes
- harness causes

Missing or untrusted required evidence produces a blocked comparison row. The
caller must classify that as a non-match, usually with `OBSERVABILITY_GAP`,
instead of treating the absence of evidence as success.

## Validation Behavior

The focused test suite covers:

- exact match
- partial match with contract-note handoff
- material drift with gap handoff
- blocked evidence with observability-gap classification
- environment-origin drift that stays out of the bug-defect lane
- implementation-defect classification guardrail

## Validation Commands

```bash
PYTHONPATH='Code&DBs/Workflow' .venv/bin/python -m py_compile 'Code&DBs/Workflow/runtime/virtual_lab/sandbox_drift.py' 'Code&DBs/Workflow/runtime/virtual_lab/__init__.py'
PYTHONPATH='Code&DBs/Workflow' .venv/bin/python -m pytest 'Code&DBs/Workflow/tests/unit/test_sandbox_drift.py' -q
```

Result:

```text
6 passed
```

## Blockers

No code blocker in the requested scope.

## Migration Needs

No migration was added.

If Phase 8 records need durable authority, add a separate DB-backed repository
plus CQRS command/query operations. That packet should register operation
catalog rows, receipt behavior, event contracts, storage schema, and replay/read
models explicitly.
