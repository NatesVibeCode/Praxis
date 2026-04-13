# Collision Guard

Wave packaging must not treat the current dirty core files as parallel-safe.

## No-Parallel Core Files

The following files must remain non-parallel across all next-wave packets:
- `Code&DBs/Workflow/registry/agent_config.py`
- `Code&DBs/Workflow/runtime/task_type_router.py`
- `Code&DBs/Workflow/runtime/workflow/unified.py`
- `scripts/workflow.sh`
- `scripts/test.sh`

## Packet Ownership: explicit touch map

Any packet with a touch in one of these files has explicit ownership below. Treat these as hard dependencies when scheduling.

### registry/agent_config.py
- `artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json`:
  - `collision_guard` (read-only)

### runtime/task_type_router.py
- `artifacts/workflow/cleanup_phase1_wire_orphans.queue.json`:
  - `p1a_wire_composite_scorer` (write)
  - `p1e_wire_support_ticket_drafts` (write)
- `artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json`:
  - `collision_guard` (read-only)

### runtime/workflow/unified.py
- `artifacts/workflow/cleanup_phase6_naming_packaging.queue.json`:
  - `p6b_package_workflow_runtime` (write)
- `artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json`:
  - `dispatch_truth_repair` (read, write)
  - `collision_guard` (read)
  - `stale_bug_audit_and_close` (read)
- `artifacts/workflow/fix_bugs_wave2.queue.json`:
  - `fix_retry_from_classifier` (write)
- `artifacts/workflow/db_only_phase/wave1_trigger_and_event_truth.queue.json`:
  - `notify_consumption_cutover` (read, write)
- `artifacts/workflow/db_only_phase/wave2_command_bus_cutover.queue.json`:
  - `control_command_fallback_delete` (write)
- `artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json`:
  - `failure_contract_unification` (write)
  - `failure_runtime_cutover` (write)

### scripts/workflow.sh
- `artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json`:
  - `bootstrap_surface_parity` (write)
  - `collision_guard` (read-only)

### scripts/test.sh
- `artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json`:
  - `bootstrap_surface_parity` (write)
  - `collision_guard` (read-only)

## Guard Rule

- A packet that writes one of the files above owns that seam for its wave unless
  it has an explicit `depends_on` link to the owning packet.
- Packets that only read a file must coordinate if sequencing is needed, but may
  run in parallel with other read-only packets.
- `collision_guard` must be refreshed for this file whenever ownership changes.
