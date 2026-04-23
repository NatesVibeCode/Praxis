# Phase 11 — Roadmap Authority Convergence (low blast radius)

Status: implementation_in_progress

Goal: remove write-path ambiguity for roadmap closeout, prove native operator authority admission for closeout, and make roadmap-tree post-closeout projection explicit and deterministic.

## Sprint A — Closeout write delegation hardening

- Component doc:
  - Delegate closeout state transitions in
    [Code&DBs/Workflow/surfaces/api/operator_write.py](Code&DBs/Workflow/surfaces/api/operator_write.py)
    to the closeout repository seam.
  - Verify through unit test that no direct SQL mutation occurs in closeout path:
    [Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py](Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py)
- Rationale:
  - keeps one write authority in repository methods (`work_item_closeout_repository`) and prevents route-specific bypasses.
- Validation evidence pointer:
  - `test_native_operator_work_item_closeout_uses_shared_gate`
  - `test_work_item_closeout_commit_delegates_to_closeout_repository`

## Sprint B — Native operator closeout admission proof

- Component doc:
  - Verify both preview and commit command execution goes through shared
    `operator_write.reconcile_work_item_closeout` and not
    `operator_write.areconcile_work_item_closeout`.
- Evidence:
  - [Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py](Code&DBs/Workflow/tests/unit/test_native_operator_work_item_closeout_cli.py)
    (`test_native_operator_work_item_closeout_preview_commits_also_use_shared_gate`)
- Rationale:
  - proves the canonical entrypoint is the shared frontdoor for both dry-run and commit phases.
- Decision:
  - # DECISION: the CLI surface remains a command adapter; all closeout route authority is enforced in frontdoor policy and repository seam.

## Sprint C — Deterministic roadmap-tree projection after closeout

- Component doc:
  - Make roadmap-tree query contract explicit with `include_completed_nodes` in
    [Code&DBs/Workflow/runtime/operations/queries/roadmap_tree.py](Code&DBs/Workflow/runtime/operations/queries/roadmap_tree.py)
    and repository fetch path in
    [Code&DBs/Workflow/surfaces/api/_operator_repository.py](Code&DBs/Workflow/surfaces/api/_operator_repository.py).
  - Add closeout read-projection regression coverage in
    [Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py](Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py)
    (`test_work_item_closeout_gate_read_projection_reflects_commit_status`).
- Evidence:
  - CQRS query pass-through assertion:
    [Code&DBs/Workflow/tests/unit/test_cqrs.py](Code&DBs/Workflow/tests/unit/test_cqrs.py)
  - Closeout projection state transition assertions:
    [Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py](Code&DBs/Workflow/tests/integration/test_work_item_closeout_gate.py)
- Decision register:
  - # DECISION in repository fetch layer: completed-node visibility is now explicit (`include_completed_nodes`), removing implicit filtering assumptions.
  - # SEE: public CQRS contract and repository seam above.

## Gate intent

- No UI scope.
- No direct closeout SQL writes remain in frontdoor command handlers.
- Preview and commit parity are preserved: preview returns plan only; commit persists and reads back completed status deterministically in `query_roadmap_tree`.
