# Bug Triage 2026-04-09

Canonical triage for the current open bug backlog after the runtime-boundary sweep.

## Summary

- Open bugs reviewed: 50
- Active fix packet:
  `workflow_dc15fc0243cb` / `W1_runtime_maintenance_and_orchestration_boundary_cleanup`
- Borderline/runtime-authority adjudication packet:
  `W2_runtime_repository_vs_boundary_adjudication`
- Junk or test-generated backlog items:
  8

## Bucket A: Active Fix Packet W1

These are treated as real boundary leaks because maintenance or orchestration loops are mutating canonical state inline instead of delegating to an explicit owned seam.

- `BUG-B49A7ED9A6C2` `Graph hygiene runtime archives memory entities directly`
- `BUG-B9C8733A12F3` `Heartbeat orphan cleanup deletes memory edges directly`
- `BUG-50ACCE7BAE38` `Workflow worker claims and updates run nodes directly`
- `BUG-5344DCF772E1` `Load balancer bootstraps provider concurrency authority directly`
- `BUG-3EF0D1862F23` `Load balancer runtime writes provider concurrency rows directly`

## Bucket B: Duplicate Of Active Packet W1

These describe the same underlying seam as Bucket A and should be reconciled when W1 closes out.

- `BUG-5344DCF772E1` and `BUG-3EF0D1862F23` are the same load-balancer authority seam described from two angles.

## Bucket C: Review/Adjudication Packet W2

These are not ignored, but they are not auto-promoted to “must refactor” just because runtime code writes to Postgres. Most look like repository/store modules or authority seeders by design and need a harder product-direction review before any rewrite.

- `BUG-EB861DA883EF` `Memory CRUD owns memory entities and edges directly`
- `BUG-6FF35361BF49` `Verification authority writes verify_refs directly`
- `BUG-41F70113DFC3` `Compile artifacts runtime materializes artifacts and packets directly`
- `BUG-A3538875D2D9` `Workflow receipt writer persists receipts and notifications directly`
- `BUG-B981792C01F4` `Retrieval telemetry bootstraps and records metrics directly`
- `BUG-49DE406B89CA` `Module indexer writes module embeddings directly`
- `BUG-3EA4D30C037E` `Capability catalog sync writes capability catalog rows directly`
- `BUG-8EE7514C0983` `Friction ledger runtime records friction events directly`
- `BUG-BF1D05739C13` `Compile index materialization writes snapshot rows directly`
- `BUG-011038E343EB` `Debate metrics runtime writes round and consensus tables directly`
- `BUG-07C1BFD40B03` `Compile artifacts runtime writes compile artifacts and execution packets directly`
- `BUG-0E7511AA44D6` `Sandbox artifacts runtime persists sandbox artifact rows directly`
- `BUG-D77FFA613FBA` `Subscription repository runtime owns event subscriptions and checkpoints`
- `BUG-B73E47948CC1` `Observability runtime writes workflow metrics directly`
- `BUG-0AE3B74D83CD` `File storage runtime writes and deletes uploaded files directly`
- `BUG-7F4B3804EC0E` `Config registry writes platform config rows directly`
- `BUG-849EFC7B53C8` `Database maintenance runtime rebuilds memory graph rows directly`
- `BUG-CEC41B042F4F` `Receipt store runtime persists receipts directly`
- `BUG-DF02BB8DF621` `Integration registry sync writes registry rows from runtime code`
- `BUG-6760FE9BE1E0` `Control commands runtime writes command rows and system events directly`
- `BUG-0C9449F404EB` `Event log bootstrap writes platform events from runtime code`
- `BUG-010099E3219F` `Result cache runtime writes workflow result cache directly`
- `BUG-2EB83612CCB2` `Execution lease manager writes and reaps leases directly`
- `BUG-65BB03A64946` `Operating model executor owns workflow graph persistence and execution rows`
- `BUG-58FEF4AC0B5D` `Task type router writes task_type_routing rows directly`
- `BUG-6D489F9683CF` `Job runtime context persists execution context snapshots directly`
- `BUG-8E762C12E824` `Repo snapshot store writes repo snapshots directly`
- `BUG-AD0DB73979AC` `Chat orchestrator persists conversations and messages in the runtime layer`
- `BUG-DC213265758A` `Verifier authority records verification and healing runs directly`
- `BUG-612F9A07E104` `Trigger runtime owns durable subscription and checkpoint state`
- `BUG-9BE740CA0821` `Reference catalog sync writes catalog rows from runtime code`
- `BUG-5EFC81856C2C` `Cost tracker runtime writes the cost ledger directly`
- `BUG-D4A871354DFE` `Claims runtime persists lease proposal and sandbox state directly`
- `BUG-56E479A6E104` `Capability feedback runtime records outcomes as durable state`
- `BUG-4502C997A314` `Workflow runtime persists canonical runs and definitions directly`
- `BUG-38F7141BBC17` `Provider onboarding writes CLI profile rows directly`
- `BUG-D45A73DABE47` `Post-workflow sync runtime persists sync status rows directly`

## Bucket D: Test/Placeholder/Junk Candidates

These look like synthetic MCP test bugs or placeholder rows rather than real product defects. They should be marked `WONT_FIX` or otherwise cleaned from canonical backlog after explicit operator confirmation.

- `BUG-4CABB0C0` `P3 minor`
- `BUG-95755161` `P0 critical`
- `BUG-11E3D831` `Unique dispatch failure XYZ`
- `BUG-8D2ABB6A` `Test bug from MCP`
- `BUG-1D3C77D1` `P3 minor`
- `BUG-88DD2E34` `P0 critical`
- `BUG-EE5F7BB3` `Unique dispatch failure XYZ`
- `BUG-1F7B758D` `Test bug from MCP`

## Decision Notes

- Direct writes inside explicit repository/store modules are not automatically a bug.
- Direct writes inside maintenance loops, read surfaces, or orchestration helpers are treated as real product-direction violations.
- W1 is the concrete cleanup packet.
- W2 is the adjudication packet for scanner-heavy runtime findings that need stronger proof before rewriting core authority modules.
