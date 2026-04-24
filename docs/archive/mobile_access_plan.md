# Mobile Access Current State

## Verdict

Mobile access is archived.

The repository keeps historical database migrations and archive notes as evidence, but mobile runtime handlers, WebAuthn helpers, bootstrap-token exchange, mobile PWA routes, mobile session budgets, and mobile workflow-launch shims are not active mainline code.

## What Remains

- `Code&DBs/Databases/migrations/workflow/185_mobile_capability_ledger.sql`
- `Code&DBs/Databases/migrations/workflow/186_gate_evaluations_grant_coverage.sql`
- `Code&DBs/Databases/migrations/workflow/187_webauthn_challenges.sql`
- `Code&DBs/Databases/migrations/workflow/188_mobile_sessions.sql`
- `Code&DBs/Databases/migrations/workflow/189_oversight_instrumentation.sql`
- `Code&DBs/Databases/migrations/workflow/220_archive_mobile_v1.sql`
- `docs/archive/mobile-v1.md`

## What Is Not Active

- No `runtime/webauthn/` package.
- No mobile session or bootstrap-token runtime.
- No mobile approval lifecycle runtime.
- No `/mobile` PWA surface.
- No mobile workflow-launch or command-approval shim.
- No phone-specific authority store.

## Authority Model

Mobile approval may return later as a capability-ledger problem, not merely an authentication problem. The future runtime must answer:

`Is this exact plan envelope covered by an active, unrevoked grant in Praxis.db, within risk, scope, blast-radius, and expiry limits?`

Passkeys identify the operator or device. Grant resolution authorizes the action. A phone UI can present and ratify decisions; it must not become an authority store.

## Revival Path

1. Decide whether the use case is external-user mobile or operator god-mode.
2. Start from current main, not from the old cascade plan.
3. Reuse archive code only when it still matches the chosen use case.
4. Add new forward migrations for recreated tables.
5. Prove grant resolution, revocation, stale-step-up rejection, and blast-radius ceilings before exposing a UI.
