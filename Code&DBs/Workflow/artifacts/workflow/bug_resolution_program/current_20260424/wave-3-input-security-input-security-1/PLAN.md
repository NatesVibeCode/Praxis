# Wave 3 Input Security Packet Plan

## Authority Model

1. Praxis.db bug records are the primary authority for the four in-scope bugs and their intended security fixes.
2. The workflow shard for `workflow_776a61b69a72` is the execution authority for this job.
3. This plan is a planning artifact only. It does not authorize code edits, test edits, or database writes.
4. If repository structure or implementation details differ from the bug-title inference below, the bug record and the live codebase take precedence over guesses.
5. The packet should stay inside the declared write scope for this job and should not expand into implementation work.

## Files To Read

Read these before any future implementation packet work:

1. This plan file: `Code&DBs/Workflow/artifacts/workflow/bug_resolution_program/current_20260424/wave-3-input-security-input-security-1/PLAN.md`
2. The REST handler module that owns the `rest_of_path:path` catchall route for `BUG-24F87D0F`.
3. The REST observability module that serves `/api/observability/code-hotspots` for `BUG-419EDA6F`.
4. The MCP operator dispatch module that normalizes multi-action operator tool actions for `BUG-34C2F2DA`.
5. The MCP workflow dispatch module that branches on `action` for `BUG-5FBAF694`.
6. Any shared validation helper, enum schema, path-normalization helper, or route-parameter sanitizer used by the above call sites.

## Files Allowed To Change

1. Only this plan file may be changed in this job.
2. No application source files, tests, fixtures, migrations, or DB records may be changed in this planning job.
3. Any later implementation packet must reopen file-write scope explicitly before touching code.

## Verification Path

1. Verify the packet scope against the four bug titles and the workflow shard before implementation.
2. Confirm the plan only asks for discovery and scoping, not code edits.
3. Confirm the packet does not rely on unverified repository paths; resolve actual file paths during the execution packet.
4. Confirm the stop boundary keeps this job at planning-only and leaves all fixes for the next packet.

## Stop Boundary

Stop after documenting the packet plan and do not proceed to code changes, test execution, DB writes, or bug-state mutations.

## Per-Bug Intended Outcome

1. `BUG-24F87D0F` - Add traversal-safe validation for the `rest_of_path` catchall route so arbitrary path segments cannot escape the intended route root.
2. `BUG-34C2F2DA` - Add upfront enum validation for multi-action operator dispatch so normalization never accepts an invalid action token.
3. `BUG-419EDA6F` - Sanitize and validate observability `roots` and `path_prefix` before they are used to build `/api/observability/code-hotspots` paths.
4. `BUG-5FBAF694` - Add upfront enum validation for the workflow `action` dispatch so the `if`/`elif` chain only handles declared actions.
