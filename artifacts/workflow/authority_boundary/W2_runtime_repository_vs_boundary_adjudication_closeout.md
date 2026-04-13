# W2 Runtime Repository Vs Boundary Adjudication Closeout

Date: 2026-04-09

Scope:
- Adjudicate the scanner-heavy runtime backlog from Bucket C after W1
- Separate explicit repository/store modules from real authority leaks
- Record which rows are already packetized elsewhere versus which need follow-through

## Validation Set

- `./scripts/test.sh validate config/cascade/specs/authority_boundary/W2_runtime_repository_vs_boundary_adjudication.json`
- Source audit in `artifacts/workflow_outputs/workflow_output_workflow_e198c8f297b1_job_366_runtime.authority.disposition.review.md`

## Disposition Summary

### Follow-Through Packets

- `BUG-4502C997A314` and `BUG-65BB03A64946` remain real workflow-runtime persistence leaks and are packaged into `config/cascade/specs/authority_boundary/W8_runtime_workflow_persistence_cutover.json`.
- `BUG-A3538875D2D9` is a real workflow receipt/notification leak and is packaged into `config/cascade/specs/authority_boundary/W8_runtime_workflow_persistence_cutover.json`.
- `BUG-AD0DB73979AC` is a real chat orchestration persistence leak and is packaged into `config/cascade/specs/authority_boundary/W9_chat_orchestrator_persistence_cutover.json`.
- `BUG-849EFC7B53C8` is a real memory-maintenance leak and is packaged into `config/cascade/specs/authority_boundary/W10_database_maintenance_memory_persistence_cutover.json`.

### Already Packetized Elsewhere

- `BUG-612F9A07E104` is already owned by `config/cascade/specs/bus_brain/W2_trigger_checkpoint_cutover.json`.

### Duplicate

- `BUG-41F70113DFC3` is a duplicate of `BUG-07C1BFD40B03`.

### By Design

- `BUG-EB861DA883EF` `Memory CRUD owns memory entities and edges directly`
- `BUG-6FF35361BF49` `Verification authority writes verify_refs directly`
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
- `BUG-CEC41B042F4F` `Receipt store runtime persists receipts directly`
- `BUG-DF02BB8DF621` `Integration registry sync writes registry rows from runtime code`
- `BUG-6760FE9BE1E0` `Control commands runtime writes command rows and system events directly`
- `BUG-0C9449F404EB` `Event log bootstrap writes platform events from runtime code`
- `BUG-010099E3219F` `Result cache runtime writes workflow result cache directly`
- `BUG-2EB83612CCB2` `Execution lease manager writes and reaps leases directly`
- `BUG-58FEF4AC0B5D` `Task type router writes task_type_routing rows directly`
- `BUG-6D489F9683CF` `Job runtime context persists execution context snapshots directly`
- `BUG-8E762C12E824` `Repo snapshot store writes repo snapshots directly`
- `BUG-DC213265758A` `Verifier authority records verification and healing runs directly`
- `BUG-9BE740CA0821` `Reference catalog sync writes catalog rows from runtime code`
- `BUG-5EFC81856C2C` `Cost tracker runtime writes the cost ledger directly`
- `BUG-D4A871354DFE` `Claims runtime persists lease proposal and sandbox state directly`
- `BUG-56E479A6E104` `Capability feedback runtime records outcomes as durable state`
- `BUG-38F7141BBC17` `Provider onboarding writes CLI profile rows directly`
- `BUG-D45A73DABE47` `Post-workflow sync runtime persists sync status rows directly`

## Notes

- The W2 review was source-first; the follow-through packet refs above are the only paths future workers should use for the real leaks.
- The by-design rows are explicit authority/store modules and should not be reopened without new code evidence.
