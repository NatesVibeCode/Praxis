# Execute Packet: `wave-0-bug-evidence-authority-bug-evidence-1`

## Summary

Smallest durable fix implemented for `BUG-1D9FAF57`: external bug filing now fails closed unless the caller supplies authoritative discovery provenance (`discovered_in_run_id` or `discovered_in_receipt_id`).

I did not resolve any bug rows in this job.

## Changed files

- `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py`
- `Code&DBs/Workflow/surfaces/mcp/cli_metadata.py`
- `Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py`

## Commands run

1. Read packet plan and authority files:
   - `sed -n '1,220p' Code\&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-0-bug-evidence-authority-bug-evidence-1/PLAN.md`
   - `sed -n '1,220p' Code\&DBs/Workflow/runtime/bug_tracker.py`
   - `sed -n '1,320p' Code\&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
   - `sed -n '1,260p' Code\&DBs/Workflow/surfaces/mcp/tools/bugs.py`
   - `sed -n '1,220p' Code\&DBs/Workflow/runtime/bug_resolution_program.py`
   - `sed -n '200,230p' Code\&DBs/Workflow/runtime/operation_catalog_gateway.py`
   - `sed -n '429,566p' Code\&DBs/Workflow/runtime/operation_catalog_gateway.py`
   - `sed -n '300,399p' Code\&DBs/Workflow/surfaces/mcp/tools/health.py`

2. Discover before code changes:
   - Raw MCP JSON-RPC call to `praxis_discover` with query: `bug filing discovery provenance authority bug surface contract underlinked remediation`

3. Static verification:
   - `PYTHONPATH='Code&DBs/Workflow' python3 -m py_compile Code\&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py Code\&DBs/Workflow/surfaces/mcp/tools/bugs.py Code\&DBs/Workflow/surfaces/mcp/cli_metadata.py Code\&DBs/Workflow/tests/unit/test_workflow_query_handlers.py`

4. Submission attempts:
   - Raw MCP JSON-RPC call to `praxis_submit_code_change`
   - Raw MCP JSON-RPC call to `praxis_get_submission`

## Evidence collected

### Implemented fix: `BUG-1D9FAF57`

- The shared bug filing contract now rejects provenance-free submissions before calling `bt.file_bug(...)`.
  - Evidence: `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py:208-245`
  - Exact behavior:
    - reads `discovered_in_run_id`
    - reads `discovered_in_receipt_id`
    - raises `ValueError("file bug requires discovered_in_run_id or discovered_in_receipt_id so the bug is not underlinked")` when both are absent
- The MCP tool contract and examples now reflect the requirement instead of advertising underlinked filing.
  - Evidence: `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py:199-203`
  - Evidence: `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py:279-286`
  - Evidence: `Code&DBs/Workflow/surfaces/mcp/cli_metadata.py:72-83`
- Unit coverage added for both fail-closed behavior and provenance pass-through.
  - Evidence: `Code&DBs/Workflow/tests/unit/test_workflow_query_handlers.py:5126-5195`
- Syntax compilation passed for all changed files.

### Deferred architecture proof: `BUG-175EB9F3`

- The bug-resolution lifecycle is still intentionally spread across packeting/program logic and runtime authority logic.
  - Evidence: `Code&DBs/Workflow/runtime/bug_resolution_program.py:17-57`
  - Evidence: packet plan authority model and file lists in `PLAN.md`
- This packet-sized fix did not attempt to collapse runtime, packet, evidence, receipt, and surface responsibilities into a single lifecycle owner.

### Deferred DB-authority alignment: `BUG-1DBACCD8`

- Read surfaces still directly query `bugs` from multiple locations and will need coordinated verification against the documented fallback story.
  - Evidence: `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py:1114-1130`
  - Evidence: `Code&DBs/Workflow/runtime/bug_tracker.py:2179-2188` (underlinked count query over `bugs` plus `bug_evidence_links`)
- I did not change any bug read-path SQL in this job.

### Deferred operation-receipt authority: `BUG-9B812B32`

- The runtime persists operation receipts durably, but also strips or re-attaches them as response decoration in separate helpers.
  - Evidence: cached result body removes `operation_receipt` in `Code&DBs/Workflow/runtime/operation_catalog_gateway.py:210-213`
  - Evidence: durable proof write path in `Code&DBs/Workflow/runtime/operation_catalog_gateway.py:429-509`
  - Evidence: response decoration path in `Code&DBs/Workflow/runtime/operation_catalog_gateway.py:512-566`
- Proving or changing atomicity across cached responses, authority events, and HTTP/MCP envelopes is broader than the filing-contract repair landed here.

### Deferred reload operational-proof authority: `BUG-A84383D1`

- `praxis_reload` currently mutates caches/runtime modules first, then records a `system_event` audit flag in the returned payload.
  - Evidence: `Code&DBs/Workflow/surfaces/mcp/tools/health.py:300-332`
  - Evidence: `Code&DBs/Workflow/surfaces/mcp/tools/health.py:335-397`
- This is still a reload-specific audit path, not an operation-catalog receipt path, so I did not claim it fixed.

## Environment limits

- `pytest` is not installed in this container: `/usr/bin/python3: No module named pytest`
- `git` is not installed in this container: `/bin/bash: git: command not found`
- The bundled `praxis` shim is present but not executable and the container Python cannot import stdlib `json`, so I used raw MCP JSON-RPC over `curl` for discovery/submission duties.
- Because of the missing stdlib `json` import in the runtime Python image, I could not execute live Python imports of the workflow modules; verification is limited to static source inspection plus `py_compile`.
- The required submission seal is currently blocked by the platform returning `workflow_submission.baseline_missing` for run `workflow_b88858ea2e3f`, job `Execute bug_evidence packet`, attempt `1`. Follow-up `praxis_get_submission` returned `workflow_submission.not_found`.

## Intended terminal status per bug

- `BUG-1D9FAF57`: intended `FIXED` after independent verifier confirms the new fail-closed filing contract on the canonical bug surface.
- `BUG-175EB9F3`: intended `DEFERRED` for a dedicated architecture packet; proof gathered that the lifecycle is still multi-surface and broader than this fix.
- `BUG-1DBACCD8`: intended `DEFERRED` pending coordinated read-path authority alignment across bug surfaces and documented fallback.
- `BUG-9B812B32`: intended `DEFERRED` pending end-to-end proof that operation receipts are authoritative across persistence, cache, and response envelopes.
- `BUG-A84383D1`: intended `DEFERRED` pending migration of reload proof from ad hoc audit event reporting to canonical durable operational receipts.
