# Root Bug Table Remediation 2026-04-23

This is the current automated workflow set for the live Praxis bugs table.

Authority snapshot:

- Source: `praxis workflow tools call praxis_bugs`
- Total bugs: 83
- Open bugs: 49
- Fixed bugs: 34
- Replay-ready bugs: 4
- Replay-blocked bugs: 45

Primary spec:

- `artifacts/workflow/root_bug_table_remediation_20260423.queue.json`

Execution shape:

| Wave | Authority slice | Depends on | Bug IDs |
| --- | --- | --- | --- |
| `wave0_backlog_collision_guard` | Refresh bug truth, dirty-worktree collision map, and launch readiness. This writes a guard artifact only; it does not patch code. | None | None |
| `wave1_bug_authority_and_replay_provenance` | Bug tracker, evidence links, replay provenance, bug API/MCP surfaces, and clustering. | `wave0` | `BUG-1DBACCD8`, `BUG-1D9FAF57`, `BUG-A75BC81E`, `BUG-69870BA5`, `BUG-07771B2A`, `BUG-8D8C5256` |
| `wave2_secret_env_runtime_policy` | Runtime env authority, secret resolution, worker concurrency, workspace boundaries, sandbox policy, and CORS/static defaults. | `wave0` | `BUG-D74C9598`, `BUG-2CF335E3`, `BUG-25224975`, `BUG-A8B8F200`, `BUG-EBE6B0E1`, `BUG-76A0519D`, `BUG-A9A42870`, `BUG-EE9CD6E9` |
| `wave3_input_validation_boundary` | MCP/API input contracts, path traversal defenses, queue cancel/list bounds, domain/data-contract validation, and JSON shape hardening. | `wave0` | `BUG-34C2F2DA`, `BUG-5FBAF694`, `BUG-FF008A47`, `BUG-EC542D26`, `BUG-3E0DDC73`, `BUG-419EDA6D`, `BUG-C4497068`, `BUG-24F87D0F`, `BUG-616A0D2E`, `BUG-94DB9A53`, `BUG-AD855F0C`, `BUG-DB815755`, `BUG-B3315290` |
| `wave4_deployment_launcher_pwa` | Launcher behavior, PWA/static serving, service worker cache safety, and deployment smoke coverage. | `wave0`, `wave2` | `BUG-839B787D`, `BUG-8B3AED45`, `BUG-22B49805` |
| `wave5_operator_registry_governance` | Operator decisions, semantic assertions, registry overrides, data dictionary governance, and migration hygiene. | `wave0`, `wave1` | `BUG-317E3347`, `BUG-DF4CAB94`, `BUG-121B5049`, `BUG-1FB4A132` |
| `wave6_contract_drift_and_audit_quality` | Data dictionary audit quality, primitive wiring audit, build/submission contracts, domain contracts, and provider type drift. | `wave5` | `BUG-0AC033CF`, `BUG-08E4B65C`, `BUG-AA6AC4E0`, `BUG-BFEA3DEE`, `BUG-0C0174D7`, `BUG-9ACEE016`, `BUG-E162C5F1`, `BUG-7C80AB3F` |
| `wave7_dependency_security_updates` | Runtime and app dependency vulnerability remediation with audit proof. | `wave0` | `BUG-EE65C154`, `BUG-ABBEFC6F`, `BUG-AAE46E22` |
| `wave8_replay_ready_receipt_failures` | The four replay-ready failures, using recorded replay runs and receipts as the fix authority. | `wave1` | `BUG-A3B51E8D`, `BUG-1980557E`, `BUG-7C5D8AE4`, `BUG-39D02693` |
| `wave9_closeout_resolution_and_validation` | Cross-wave verification, bug state closure, evidence receipts, and final operator report. | Waves 1-4 and 6-8 | All 49 open bug IDs |

Validation before launch:

```bash
praxis workflow tools call praxis_workflow_validate --input-json '{"spec_path":"artifacts/workflow/root_bug_table_remediation_20260423.queue.json"}'
./scripts/test.sh validate artifacts/workflow/root_bug_table_remediation_20260423.queue.json
```

Launch, when ready:

```bash
praxis workflow tools call praxis_workflow --input-json '{"action":"run","spec_path":"artifacts/workflow/root_bug_table_remediation_20260423.queue.json","wait":false}'
```

Track:

```bash
praxis workflow tools call praxis_workflow --input-json '{"action":"status","run_id":"<run_id>"}'
praxis workflow tools call praxis_workflow --input-json '{"action":"inspect","run_id":"<run_id>"}'
```

Design notes:

- One master DAG is intentional. Standing order says collapse before orchestrating.
- Jobs use the concrete `openai/gpt-5.4` route so launch does not depend on `auto/*` task-type routing being healthy or on the currently exhausted Anthropic subscription lane.
- The first job is a collision guard because the repo already has a large dirty worktree.
- Jobs must not resurrect localhost Postgres authority.
- Bugs should only be closed with verifier-backed evidence or a clear non-fixed resolution.
- Old queue specs in `artifacts/workflow` are evidence only; some contain retired assumptions.
