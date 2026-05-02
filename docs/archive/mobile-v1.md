# Mobile v1 - Archived 2026-04-24

Mobile was designed as a multi-user external-access path: WebAuthn, device enrollment, bootstrap-token exchange, per-command approval requests, capability-grant budget envelopes. It conflated two different use cases that do not share a reasonable design:

1. External-user mobile: users managing their own codebases from a phone. Requires multi-user auth, per-workspace isolation, public docs, and the full ceremony.
2. Operator god-mode: Nate accessing the Praxis platform itself from a phone. Single-user trust, tunnel-gated networking, no new code needed beyond Tailscale and existing Canvas at narrow viewport.

Neither use case was working, and neither is needed today. Archived rather than deleted so the multi-user ceremony can be picked up whenever external users become real.

## Revival Reference

- Archive branch: `archive/mobile-v1-2026-04-24`
- HEAD SHA at archive time: `ec8aa0bf7e09adf3751bfd0bdafd9b3a0de3867a`
- Revival is not a straight merge. The platform has moved since archive. Expect to rethink auth, approval UX, PWA shell, and Canvas integration before bringing any module back.

## Archived Surface

Python runtime:

- `Code&DBs/Workflow/runtime/capability/approval_lifecycle.py`
- `Code&DBs/Workflow/runtime/capability/sessions.py`
- `Code&DBs/Workflow/runtime/webauthn/`
- `Code&DBs/Workflow/runtime/mobile_security.py`

API surface:

- `POST /api/mobile/bootstrap-token`
- `GET /mobile/manifest.webmanifest`
- `GET /mobile/sw.js`
- `POST /api/auth/bootstrap/exchange`
- `POST /approvals/{request_id}/ratify`
- `POST /devices/{device_id}/revoke`
- Mobile workflow launch/approval shims in the agent-sessions app

Tests:

- `Code&DBs/Workflow/tests/unit/test_mobile_authority.py`

Database migrations retained in history:

- `185_mobile_capability_ledger.sql`
- `186_gate_evaluations_grant_coverage.sql`
- `187_webauthn_challenges.sql`
- `188_mobile_sessions.sql`
- `189_oversight_instrumentation.sql`

The forward migration `220_archive_mobile_v1.sql` drops the mobile-only tables. It does not remove historical migrations.

## Revival Rules

1. Decide the real use case first: external users or operator god-mode.
2. Create a new feature branch from current main.
3. Cherry-pick only modules that still serve the chosen use case.
4. Write a new forward migration for any recreated tables.
5. Do not restore both use cases in one package.

## Standing Decision

`decision.2026-04-24.mobile-v1-archived`: mobile v1 conflated public-user access with operator god-mode; neither shipped; operator god-mode is solved through Tailscale plus the existing Canvas/operator console surface; external-user mobile is deferred until real external users exist.
