# DB-Only Phase Pack

This folder is a runnable wave pack for the DB-only backlog phase. Postgres is
the planning authority; these artifacts are the packet surface and proof pack.

## Waves

| Wave | Queue | Primary roadmap rows | Absorbed bugs |
| --- | --- | --- | --- |
| 0 | `artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json` | `roadmap_item.maintenance.alpha.foundation`, `roadmap_item.phase.method.bootstrap`, `roadmap_item.dispatch.truth.contract` | `BUG-6D64923C`, stale-open audit and backlog hygiene set |
| 1 | `artifacts/workflow/db_only_phase/wave1_trigger_and_event_truth.queue.json` | `roadmap_item.workflow.trigger.checkpoint_cutover` | `BUG-14B00013`, `BUG-661DC83D`, `BUG-0388B701` |
| 2 | `artifacts/workflow/db_only_phase/wave2_command_bus_cutover.queue.json` | `roadmap_item.workflow.command_bus.hard_cutover` | `BUG-26DAFEBF`, `BUG-756CD965`, `BUG-5A3AD7C1` |
| 3 | `artifacts/workflow/db_only_phase/wave3_orient_and_registry_truth.queue.json` | `roadmap_item.orient.canonical.authority` | `BUG-8B0E04AD`, `BUG-DDB3AA43`, `BUG-CDB5894B` (regression closeout only) |
| 4 | `artifacts/workflow/db_only_phase/wave4_authority_cleanup_and_failure_semantics.queue.json` | `roadmap_item.authority.cleanup.failure_semantics*` plus reused `artifacts/workflow/authority_cleanup/*.queue.json` | `BUG-BDAD34DC`, `BUG-D0DC2D32`, `BUG-C487AEB4` (proof-only) |
| 5 | `artifacts/workflow/db_only_phase/wave5_activity_truth_and_telemetry.queue.json` | `roadmap_item.activity.truth.loop`, `roadmap_item.object.state.substrate` | `BUG-718C3494`, `BUG-965E983B` |

## Model Lanes

- `openai/gpt-5.3-codex-spark`: narrow one-seam edits, SQL hygiene, tiny runtime wiring.
- `openai/gpt-5.4-mini`: small multi-file build and wiring packets.
- `openai/gpt-5.4`: ambiguous audits, proof packets, architecture, and review.

## Run

- Validate: `./scripts/workflow.sh validate <queue>` or `./scripts/test.sh validate <queue>`
- Test front door: `./scripts/test.sh selftest|suite list|suite focus|plan|check-affected|validate`
- Dry-run a representative wave: `./scripts/workflow.sh dry-run artifacts/workflow/db_only_phase/wave0_backlog_truth_and_dispatch_foundation.queue.json`
- Launch one wave at a time: `./scripts/workflow.sh run <queue>`

## Writeback

- `artifacts/workflow/db_only_phase/db_only_phase_writeback.sql` updates roadmap rows
  with row refs and absorbed bug metadata.
- The same script closes stale/noise bugs that are no longer first-class backlog
  inputs.
