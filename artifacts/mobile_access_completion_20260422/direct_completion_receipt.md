# Mobile Access Completion Direct Receipt

Date: 2026-04-22
Mode: direct implementation; workflow launch/inspect/retry intentionally not used at operator request.
Workspace: `/Volumes/Users/natha/Documents/Builds/Praxis`

## Implemented Authority

- Launcher API resolution now exposes `GET /api/launcher/resolve`.
- Launcher config resolves `workspace_ref=praxis` and `host_ref=default` through live API authority.
- Mobile session/bootstrap tables are present in the live DB.
- Mobile capability grants are plan-envelope bound.
- Mobile control commands fail closed on forged plan hashes and revoked/mismatched grants.
- Mobile approval/device mutation APIs apply no-store headers.
- WebAuthn RP-ID and sign-count metadata validation are covered.
- PWA manifest and service worker assets exist in the app surface.
- Migration `200_cqrs_authority_kernel.sql` now collapses authority-domain seed rows before `ON CONFLICT`.

## Validation

- `praxis launcher doctor --json`: passed while API was running from this workspace; resolved executable path `/Volumes/Users/natha/Documents/Builds/Praxis/scripts/praxis`.
- `PYTHONPATH='Code&DBs/Workflow' python3 -m pytest --noconftest -q 'Code&DBs/Workflow/tests/unit/test_mobile_authority.py' 'Code&DBs/Workflow/tests/unit/test_workflow_migration_idempotence.py' 'Code&DBs/Workflow/tests/unit/test_workflow_submission_policy.py' 'Code&DBs/Workflow/tests/unit/test_control_commands.py' 'Code&DBs/Workflow/tests/unit/test_api_rest_startup.py' 'Code&DBs/Workflow/tests/unit/test_launcher_authority.py'`: 86 passed in 27.65s.
- `npm run typecheck` from `Code&DBs/Workflow/surfaces/app`: passed.
- Live DB `schema_migrations`: `188_mobile_sessions.sql`, `199_workspace_base_path_authority.sql`, and `200_cqrs_authority_kernel.sql` are recorded as canonical.
- Live DB tables present: `mobile_bootstrap_tokens`, `mobile_sessions`, `mobile_session_budget_events`.
- Live API `GET /api/launcher/resolve?workspace_ref=praxis&host_ref=default`: returned `ok: true`.
- Live API `POST /api/approvals/nonexistent/ratify`: returned `Cache-Control: no-store, no-cache, must-revalidate, private`.
- Live API `POST /api/devices/nonexistent/revoke`: returned `Cache-Control: no-store, no-cache, must-revalidate, private`.

## Bug Closeout Evidence

- Verification run: `verification_run:96c1c71340ec4e21b8e19fcbe8a3ace5`.
- `BUG-A1DC262B`: covered by `test_mobile_session_budget_spend_is_atomic_and_receipted`.
- `BUG-D90C8F9D`: covered by `test_chain_attack_mismatched_plan_hash_fails_closed`, `test_chain_attack_revoked_grant_does_not_authorize`, and `test_resolver_rejects_wrong_plan_hash`.
- `BUG-018E3541`: migration 200 boot degradation from duplicate authority-domain seed rows was discovered during direct API launch and fixed in this pass.

## Notes

- No workflow `run_id` exists because workflow execution was explicitly excluded from this completion path.
- Codex command-runner background cleanup prevented leaving a detached API process running from this turn, but foreground launch verified clean startup and launcher doctor success.
