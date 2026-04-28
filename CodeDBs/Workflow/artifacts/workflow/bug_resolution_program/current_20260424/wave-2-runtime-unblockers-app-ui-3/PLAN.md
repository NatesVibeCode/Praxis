# Wave 2 Runtime Unblockers App UI 3 Plan

## Authority Model
- Primary authority: Praxis.db bug records for `BUG-A63D9317`, `BUG-AA6AC4E0`, `BUG-AAE46E22`, and `BUG-F758C3D4`.
- Secondary authority: the current workspace source tree and its tests. If code and bug wording diverge, the bug packet and standing workflow scope win for this packet.
- Scope authority: this job is planning only. No code, lockfile, generated artifact, schema, or workflow data edits are allowed in this job.
- Change authority for the follow-on implementation: only the minimum files needed to remove the runtime/UI unblockers, plus any directly coupled tests or package metadata required to verify the fix.

## Files To Read
- `CodeDBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-2-runtime-unblockers-app-ui-3/PLAN.md`
- The source files named by each bug once located in the workspace.
- Any directly imported module above those files when a fix depends on the import graph.
- The smallest targeted tests that exercise the affected runtime path, import path, or dependency check.

## Files Allowed To Change
- This job: only `CodeDBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-2-runtime-unblockers-app-ui-3/PLAN.md`.
- Follow-on implementation, if approved:
  - Connector registrar and registry projection code for `BUG-A63D9317`.
  - `runtime/build_planning_contract.py` and its immediate contract tests for `BUG-AA6AC4E0`.
  - Dependency manifest or lockfile entries that pin `anthropic` to `0.87.0` for `BUG-AAE46E22`.
  - Dev UI API import/export wiring and any package init or generated-artifact exposure code for `BUG-F758C3D4`.
- Do not widen the write set to unrelated subsystems, generated outputs, or migration files unless a failing test proves the dependency is unavoidable.

## Verification Path
- For architecture and wiring fixes, run the smallest targeted import or unit tests that prove the changed path now resolves once.
- For `BUG-A63D9317`, verify the registrar no longer auto-imports filesystem clients and that only the intended registry projection is emitted.
- For `BUG-AA6AC4E0`, verify the planning contract module matches the runtime contract shape and that the contract-drift check passes.
- For `BUG-AAE46E22`, verify the dependency audit accepts `anthropic==0.87.0` and that the relevant test or import path still passes.
- For `BUG-F758C3D4`, verify the Dev UI API can import generated connector artifacts from `artifacts.connectors` without fallback or manual path hacks.
- Prefer focused checks over full-suite runs unless a targeted check exposes a broader regression.

## Stop Boundary
- Stop after the plan is written and the implementation scope is explicit.
- Do not edit code in this job.
- In a later implementation job, stop before any broad refactor, cross-domain cleanup, or non-essential registry reshaping.
- If the fix requires files outside the listed areas, halt and re-evaluate instead of expanding the scope silently.

## Per-Bug Intended Outcome
- `BUG-A63D9317`: remove duplicate filesystem-client auto-import behavior from the connector registrar and collapse registry writes to a single authoritative projection.
- `BUG-AA6AC4E0`: eliminate the contract drift in `runtime/build_planning_contract.py` by aligning the runtime planning contract with the canonical contract definition.
- `BUG-AAE46E22`: raise `anthropic` from `0.86.0` to `0.87.0` to satisfy the CVE-driven dependency audit requirement without introducing unrelated dependency churn.
- `BUG-F758C3D4`: make the Dev UI API able to import generated connector artifacts from `artifacts.connectors` through a stable, discoverable module path.
