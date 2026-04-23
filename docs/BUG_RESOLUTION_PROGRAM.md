# Bug Resolution Program

This workflow program turns the open bug backlog into one frozen, evidence-aware coordination artifact and then materializes bounded packet specs from it.

## Artifacts

- Coordination output:
  `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260423.json`
- Packet template:
  `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program_packet_template_20260423.queue.json`
- Durable chain coordination:
  `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_chain_20260423.json`
- Supervisor spec:
  `config/cascade/specs/W_bug_resolution_program_supervisor_20260423.queue.json`

## Operator flow

1. Freeze the kickoff backlog:

   ```bash
   python3 scripts/bug_resolution_program.py freeze \
     --program-id bug_resolution_program_20260423 \
     --output Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260423.json
   ```

2. If the coordination state is `frozen`, materialize one packet spec per cluster or singleton bug:

   ```bash
   python3 scripts/bug_resolution_program.py materialize-packets \
     --coordination Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260423.json \
     --template Code&DBs/Workflow/artifacts/workflow/bug_resolution_program_packet_template_20260423.queue.json \
     --output-dir Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/packets
   ```

3. Materialize the durable workflow-chain coordination file:

   ```bash
   python3 scripts/bug_resolution_program.py materialize-chain \
     --coordination Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_kickoff_20260423.json \
     --output Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/bug_resolution_program_chain_20260423.json \
     --max-parallel 5
   ```

4. Submit the durable chain through `praxis_workflow(action="chain")`.

The active chain for this kickoff is `workflow_chain_655a52dd62a4`. The old memory-backed `open-wave` helper is not durable across fresh processes and is tracked as `BUG-AF7C1773`.

## Program rules

- The coordination file is the frozen scope anchor for the current backlog cycle.
- `wave_0_authority_repair` always comes first.
- A packet may only resolve `FIXED` after validation evidence is attached.
- New bugs after kickoff are out of scope unless caused by the program itself.
