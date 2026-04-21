# Bug Authority

Use this reference when you need the exact bug tracker contract.

## Canonical Tables

Read-only inspection is fine. Writes should go through the tracker surface.

- `public.bugs`
- `public.bug_evidence_links`
- `public.roadmap_items`

The source migration is:

- `Code&DBs/Databases/migrations/workflow/009_bug_and_roadmap_authority.sql`

## Canonical Enums

### Status

- `OPEN`
- `IN_PROGRESS`
- `FIXED`
- `WONT_FIX`
- `DEFERRED`

### Severity

- `P0`
- `P1`
- `P2`
- `P3`

### Category

- `SCOPE`
- `VERIFY`
- `IMPORT`
- `WIRING`
- `ARCHITECTURE`
- `RUNTIME`
- `TEST`
- `OTHER`

## Canonical Write Surface

Prefer the bug tracker authority surface:

```text
praxis workflow bugs list
praxis workflow bugs search "<title>"
praxis workflow tools describe praxis_bugs
praxis workflow tools call praxis_bugs --input-json '{...}' --yes
```

Relevant implementation files:

- `Code&DBs/Workflow/runtime/bug_tracker.py`
- `Code&DBs/Workflow/surfaces/mcp/tools/bugs.py`

## File Action Mapping

`praxis workflow tools call praxis_bugs --input-json '{"action":"file",...}'` maps to `BugTracker.file_bug(...)`.
MCP tool `praxis_bugs` with input `{"action":"file",...}` maps to the same filing path when you are using the MCP surface directly.

Important behavior:

- `bug_id` is generated as `BUG-XXXXXXXX`
- `bug_key` is generated from `bug_id`
- `status` is initialized to `OPEN`
- `priority` currently mirrors `severity`
- `summary` is derived from `description[:200]`

Useful inputs:

- `title`
- `severity`
- `category`
- `description`
- `filed_by`
- `source_kind`
- `decision_ref`
- `discovered_in_run_id`
- `discovered_in_receipt_id`
- `owner_ref`
- `tags`

## Evidence Contract

`praxis workflow tools call praxis_bugs --input-json '{"action":"attach_evidence",...}'` maps to `BugTracker.link_evidence(...)`.

Allowed evidence kinds:

- `receipt`
- `run`
- `verification_run`
- `healing_run`

Allowed evidence roles:

- `observed_in`
- `attempted_fix`
- `validates_fix`

Validation is strict:

- `receipt` refs must exist in `receipts`
- `run` refs must exist in `workflow_runs`
- `verification_run` refs must exist in `verification_runs`
- `healing_run` refs must exist in `healing_runs`

## Resolution Contract

`praxis workflow tools call praxis_bugs --input-json '{"action":"resolve",...}'` only allows terminal statuses:

- `FIXED`
- `WONT_FIX`
- `DEFERRED`

Best practice:

- attach `validates_fix` evidence before marking `FIXED`

## Read-Only SQL Checks

Inspect the live bug schema:

```sql
select column_name, data_type, is_nullable
from information_schema.columns
where table_schema = 'public' and table_name = 'bugs'
order by ordinal_position;
```

Inspect recent bugs:

```sql
select bug_id, bug_key, title, status, severity, category, filed_by, opened_at
from public.bugs
order by opened_at desc
limit 20;
```

Inspect evidence links:

```sql
select bug_id, evidence_kind, evidence_ref, evidence_role, created_at
from public.bug_evidence_links
order by created_at desc
limit 20;
```
