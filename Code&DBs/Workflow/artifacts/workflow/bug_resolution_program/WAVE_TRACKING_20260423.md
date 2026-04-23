# Wave Tracking — 2026-04-23

## Wave 0 status

- Durable chain fired: `yes`
- Active chain id: `workflow_chain_655a52dd62a4`
- Result ref: `workflow_chain:workflow_chain_655a52dd62a4`
- Current durable wave: `wave_0_authority_repair_batch_001`
- Chain status at handoff: `running`
- Durable chain waves: `14`
- Specs registered in chain: `67`
- Max packet specs per chain wave: `5`

The earlier memory-backed `praxis_wave` start call returned `running` but did not persist across fresh processes. That failure is tracked as `BUG-AF7C1773`. The active program is now running through the DB-backed `workflow_chain` authority.

The durable chain was materialized and submitted with:

```bash
python3 scripts/bug_resolution_program.py materialize-chain \
  --coordination Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260423.json \
  --output Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_chain_20260423.json \
  --max-parallel 5
```

Then the chain was submitted through `praxis_workflow(action="chain")`.

## Packet inventory by wave

- `wave_0_authority_repair`: `50` packets / `63` bugs
- `wave_1_evidence_normalization`: `14` packets / `18` bugs
- `wave_2_execute`: `3` packets / `4` bugs
- `wave_3_verify_closeout`: `0` packets / `0` bugs

## Dependency line

- `wave_0_authority_repair_batch_001` -> ... -> `wave_0_authority_repair_batch_010`
- `wave_0_authority_repair_batch_010` -> `wave_1_evidence_normalization_batch_001`
- `wave_1_evidence_normalization_batch_001` -> ... -> `wave_1_evidence_normalization_batch_003`
- `wave_1_evidence_normalization_batch_003` -> `wave_2_execute_batch_001`

## Immediate next operating move

- Monitor the active durable chain:
- `workflow_chain_655a52dd62a4`
- The first durable wave contains these five packet specs:
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-cluster-authority-bug-system.queue.json`
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-22-bug-evidence-authority.queue.json`
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-22-db-authority.queue.json`
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-hygiene-2026-04-22-secret-authority.queue.json`
- `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets/wave-0-authority-repair-authority-bug-system-title-atlas-missing-graph-table.queue.json`

The first submit attempt created failed chain `workflow_chain_2ef0a929f5a6` after a transient spec-file lookup failure. That is tracked as `BUG-64149FA7`; the clean retry is `workflow_chain_655a52dd62a4`.

## Notes

- The frozen kickoff snapshot is the scope anchor. New bugs after `2026-04-23T21:35:20.333028+00:00` are not part of this program unless they are regressions caused by program work.
- The durable chain coordination file is ready at `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_chain_20260423.json`.
- The supervisor spec is ready at `config/cascade/specs/W_bug_resolution_program_supervisor_20260423.queue.json`.
- The packet template is ready at `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program_packet_template_20260423.queue.json`.
