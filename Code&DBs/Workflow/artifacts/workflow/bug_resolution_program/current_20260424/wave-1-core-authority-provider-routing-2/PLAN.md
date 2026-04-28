# Plan: provider_routing packet

## Authority Model
- Primary authority is the live Praxis orient envelope plus the signed execution shard, not ambient shell state or host filesystem guesses.
- Runtime binding comes from `/orient`: repo root `/workspace`, workdir `/workspace`, and the active workflow session shard. Any routing decision that ignores that binding is an authority drift candidate.
- Standing orders relevant here are: use the catalog-backed workflow surface first, treat authority drift as a real bug signal, keep writes inside declared scope, and prefer repo-local resolution over external checkout routing unless an explicit authority decision says otherwise.
- For this packet, launcher/provider routing may be inspected and documented only. No code, DB state, launcher state, or routing behavior is to be changed here.

## Files To Read
- `Code&DBs/Workflow/...` global launcher entrypoint that decides the active target workspace or checkout.
- `Code&DBs/Workflow/...` workspace binding / root-resolution logic consumed by the launcher.
- `Code&DBs/Workflow/...` provider routing and dispatch code that selects the concrete execution path.
- `Code&DBs/Workflow/...` tests or fixtures covering launcher target selection, repo-local override precedence, and workspace path selection.
- `Code&DBs/Workflow/...` any regression fixtures or bug evidence tied to hidden repo-local fixes under global routing.

## Files Allowed To Change
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-1-core-authority-provider-routing-2/PLAN.md`

## Verification Path
- Reconfirm the orient envelope and execution shard before implementation work in the downstream packet.
- Read the launcher and routing surfaces to isolate the precedence bug: where active-workspace authority is lost, and where the wrong path escapes to an external checkout.
- In the execute packet, verify with focused launcher/routing tests that:
  - the active workspace is chosen first,
  - repo-local fixes remain visible to the launcher,
  - external routing does not happen unless a future explicit authority decision authorizes it.

## Stop Boundary
- Stop after updating this plan.
- Do not edit code, migrations, DB state, or tests in this job.
- Do not attempt to repair the launcher or routing behavior from this packet.
- Do not broaden scope beyond provider routing for the global launcher / active workspace authority issue.

## Per-Bug Intended Outcome
- `BUG-96F12329 [P2/WIRING]`: the global Praxis launcher resolves against the active workspace first, preserves visibility of repo-local fixes, and stops routing outside the current execution authority unless an explicit future authority decision permits that path.
