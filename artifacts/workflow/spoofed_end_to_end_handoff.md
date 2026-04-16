# Spoofed End-to-End Workflow Handoff

- Status: blocked at runtime execution; the canonical run submitted successfully but remains queued with no worker claim.
- Submitted run: `workflow_8b04978942c2`
- Workflow: `examples/research_pipeline.queue.json`
- Filed bug: `BUG-BB46C9F9` - workflow frontdoor aborts before durable submission when Postgres authority is unavailable

## Real signals observed

- `praxis workflow run examples/research_pipeline.queue.json` initially failed under the default authority path with `WORKFLOW_DATABASE_URL authority unavailable: InvalidAuthorizationSpecificationError: role "postgres" does not exist`.
- `pg_isready -d postgresql://nate@127.0.0.1:5432/praxis` reports the database is accepting connections.
- Re-running with `WORKFLOW_DATABASE_URL=postgresql://nate@127.0.0.1:5432/praxis` submitted the workflow successfully.
- `praxis workflow tools call praxis_workflow --input-json '{"action":"status","run_id":"workflow_8b04978942c2"}' --yes` shows the run is still `queued` and waiting for a claim.
- `praxis workflow tools call praxis_workflow --input-json '{"action":"list"}' --yes` confirms the run exists but has not advanced.
- CQRS is the read/write seam under the repo operator surfaces; query discovery passed through the CQRS-backed surfaces rather than ad hoc scripts.
- Relevant CQRS evidence includes `Code&DBs/Workflow/tests/unit/test_cqrs.py`, `Code&DBs/Workflow/surfaces/cli/commands/roadmap.py`, and `Code&DBs/Workflow/surfaces/mcp/tools/query.py`.
- Attempting to claim the live run with `praxis workflow tools call praxis_workflow --input-json '{"action":"claim","run_id":"workflow_8b04978942c2","subscription_id":"trigger_evaluator"}' --yes` failed with `runtime route 'workflow_8b04978942c2' is missing` and `workflow.claim.failed`.

## Previewed execution shape

- `examples/research_pipeline.queue.json`
  - `research` -> `auto/research`
  - `analyze` -> `auto/architecture`, depends on `research`
  - `synthesize` -> `auto/build`, depends on `analyze`
  - `review` -> `auto/review`, depends on `synthesize`

- `artifacts/workflow/e2e_all_step_types.queue.json`
  - `step1_agent_analyze` -> agent job
  - `step2_check_review` -> review job depending on `step1_agent_analyze`
  - `step3_api_integration` -> file-writing integration step depending on `step2_check_review`
  - `step4_human_notification` -> simulated notification step depending on `step3_api_integration`

- `artifacts/workflow/deterministic_smoke.queue.json`
  - `prepare` -> deterministic task
  - `admit` -> deterministic task depending on `prepare`

## Spoofed downstream outputs

- `research`: collect async HTTP client best practices for `aiohttp`, `httpx`, and `urllib3 v2`.
- `analyze`: reduce the research to the top 3 approaches with trade-offs.
- `synthesize`: produce a comparison document with code examples.
- `review`: flag any missing considerations or accuracy gaps.
- `step3_api_integration`: write `artifacts/dispatch_outputs/e2e_failure_report.md`.
- `step4_human_notification`: write `artifacts/dispatch_outputs/e2e_notification_log.md` with a simulated delivered email.
- `prepare`: `result=prepared`
- `admit`: `result=admitted`

## Deterministic DB action

- `praxis_data approve` succeeded and returned a DB-backed approval manifest with `ok: true`.

## Notes

- This artifact is synthetic where noted. It exists to preserve the handoff after the worker pool stalled at `queued`.
- The underlying workflow shape is valid. The current blocker is claim/execution availability, not spec validation.
- CQRS was intentionally preserved as part of the handoff so the next step does not accidentally collapse query, command, and workflow dispatch into one blob of bad architecture.
- Bugs filed so far: `BUG-BB46C9F9` for the Postgres authority failure and `BUG-267CC804` for the missing runtime route on claim.
