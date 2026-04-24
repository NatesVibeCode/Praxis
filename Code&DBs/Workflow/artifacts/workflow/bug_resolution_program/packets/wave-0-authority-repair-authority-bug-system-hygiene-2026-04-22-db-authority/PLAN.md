# Bugs in scope

- `BUG-1DBACCD8`
- Packet owner: `lane:authority_bug_system`
- Lane: `Authority / bug system (authority_bug_system)`
- Wave: `wave_0_authority_repair`
- Packet kind: `authority_repair`
- Cluster: `hygiene-2026-04-22-db-authority`
- Depends on: none

# Titles in scope

- `[hygiene-2026-04-22/db-authority] Bug surface and documented Postgres fallback read different bug tables`

# Files to read first

- `artifacts/workflow/root_bug_table_remediation_20260423.md`
- `artifacts/workflow/root_bug_table_remediation_20260423.queue.json`
- `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py`
- `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
- `Code&DBs/Workflow/surfaces/api/handlers/_query_bugs.py`
- `Code&DBs/Workflow/surfaces/api/handlers/workflow_query_core.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py`
- `Code&DBs/Workflow/runtime/engineering_observability.py`
- `Code&DBs/Workflow/runtime/bug_tracker.py`
- `Code&DBs/Workflow/runtime/bug_evidence.py`
- `Code&DBs/Workflow/tests/unit/test_bug_tracker.py`
- `Code&DBs/Workflow/tests/unit/test_bug_tracker_read_only.py`
- `Code&DBs/Workflow/tests/unit/test_bug_tracker_bug_evidence_wiring.py`
- `Code&DBs/Workflow/tests/unit/test_bug_surface_db_isolation_guardrail.py`
- `Code&DBs/Workflow/tests/unit/test_engineering_observability.py`
- `Code&DBs/Workflow/tests/unit/test_rest_engineering_observability.py`

# Files allowed to change

- `Code&DBs/Workflow/runtime/bug_tracker.py`
- `Code&DBs/Workflow/runtime/bug_evidence.py`
- `Code&DBs/Workflow/runtime/receipt_store.py`
- `Code&DBs/Workflow/runtime/work_item_clustering.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/discover.py`
- `Code&DBs/Workflow/surfaces/api/handlers/_bug_surface_contract.py`
- `Code&DBs/Workflow/surfaces/api/handlers/_query_bugs.py`
- `Code&DBs/Workflow/storage/postgres/bug_evidence_repository.py`
- `Code&DBs/Workflow/tests/unit/test_bug_tracker.py`
- `Code&DBs/Workflow/tests/unit/test_bug_tracker_read_only.py`
- `Code&DBs/Workflow/tests/unit/test_bug_tracker_bug_evidence_wiring.py`
- `Code&DBs/Workflow/tests/unit/test_bug_surface_db_isolation_guardrail.py`
- Current repo truth note: `/orient` authority lives in `Code&DBs/Workflow/surfaces/api/handlers/workflow_admin.py` and bug scoreboard shaping lives in `Code&DBs/Workflow/runtime/engineering_observability.py`, but those files are not in the declared wave-1 write scope from `artifacts/workflow/root_bug_table_remediation_20260423.queue.json`.

# Verification or closure proof required

- `workflow orient` must return cleanly for the affected path, with no split-brain bug-table read path.
- Bug `stats`, `list`, and `search` surfaces must all return cleanly and agree on the same bug authority/DB identity.
- The replay-ready operator view must return cleanly for the same authority path.
- The execute packet must prove that fallback paths cannot read a different bug table, or are hard-gated closed.
- The execute packet must prove one bug authority, one DB authority identity, and observable evidence linkage.
- If duplicate or search behavior changes, cheap title-like duplicate checks must stay distinct from semantic enrichment and must not hang silently.
- If tests write bug-surface rows, they must use rollback-isolated helpers; the DB-isolation guardrail must remain green.
- Focused proof should include the bug-surface test slice named in the wave-1 write scope, plus the observability tests needed to prove `/orient` and replay-ready views if those surfaces are touched.
- Closure requires verifier-backed evidence for `FIXED`, or an exact `resume_context` blocker patch if the scope cannot be completed inside this packet.

# Stop boundary

- Do not edit code in this planning packet.
- Do not widen beyond `BUG-1DBACCD8` and the `hygiene-2026-04-22-db-authority` title above.
- Do not add ad hoc SQL scripts or treat localhost Postgres as authority.
- Do not mutate bug state, resolve bugs, or patch `resume_context` from this packet.
- Do not expand file changes beyond the declared wave-1 write scope without an explicit scope change.
