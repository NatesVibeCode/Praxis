# Mobile Access Current State

## Verdict

Mobile access is not set up yet.

The repository has DB authority pieces for the capability-ledger direction, but it does not have the runtime handlers, WebAuthn ceremonies, mobile PWA surface, tunnel setup, or end-to-end approval flow. Treat the previous mobile cascade specs as retired planning residue, not an active build path.

## What Exists

- `Code&DBs/Databases/migrations/workflow/185_mobile_capability_ledger.sql`
  - Defines the initial device, grant, and approval ledger tables.
- `Code&DBs/Databases/migrations/workflow/186_gate_evaluations_grant_coverage.sql`
  - Extends gate evaluation records with grant and plan-envelope coverage.
- `Code&DBs/Databases/migrations/workflow/187_webauthn_challenges.sql`
  - Defines the challenge ledger for registration and assertion ceremonies.
- `Code&DBs/Databases/migrations/workflow/189_oversight_instrumentation.sql`
  - Adds oversight tables for policy drift, autonomy depth, session blast radius, and blast-radius policy.

## What Does Not Exist

- No `runtime/webauthn/` package.
- No `runtime/capability/plan_envelope.py`, `resolver.py`, `approval_lifecycle.py`, `blast_radius.py`, `policy_drift.py`, or `autonomy_chain.py`.
- No API handlers for WebAuthn, approvals, mobile metrics, or mobile status.
- No `surfaces/app/src/mobile/` PWA surface.
- No service worker or app manifest for installable mobile use.
- No Cloudflare Tunnel deployment command.
- No end-to-end tests proving approval, ratification, revocation, or stolen-phone behavior.

## Authority Model

The durable direction is still correct: mobile approval is a capability-ledger problem, not merely an authentication problem.

The runtime must eventually answer one question before allowing autonomous action:

`Is this exact plan envelope covered by an active, unrevoked grant in Praxis.db, within risk, scope, blast-radius, and expiry limits?`

Passkeys identify the operator/device. The grant resolver authorizes the action. The phone UI only presents and ratifies decisions; it must not become an authority store.

## Easy Next Steps

1. Verify the current DB state:
   - Run `./scripts/bootstrap`.
   - Run `praxis workflow query "status"`.
   - Confirm migrations 185, 186, 187, and 189 are applied.

2. Build the backend authority spine:
   - Add `runtime/capability/plan_envelope.py`.
   - Add `runtime/capability/resolver.py`.
   - Add `runtime/capability/approval_lifecycle.py`.
   - Add tests that fail closed when no grant covers the plan envelope.

3. Add WebAuthn only after the grant resolver exists:
   - Add `runtime/webauthn/ceremonies.py`.
   - Add API handlers for register/assert begin and complete.
   - Store only challenge/session/token hashes where applicable.
   - Enforce RP ID, sign-count monotonicity, expiry, and a 30-second step-up window.

4. Wire approvals into the command bus:
   - Stamp plan envelopes before command classification.
   - Replace broad auto-execute authority with grant resolution.
   - Keep risk labels as UI hints only.
   - Write `grant_ref` and `plan_envelope_hash` into gate evidence.

5. Build the smallest mobile surface:
   - Create `surfaces/app/src/mobile/`.
   - Start with pending approvals and burn-rate header only.
   - Add passkey setup and step-up prompt after backend ceremonies pass tests.

6. Expose it to the phone:
   - Add a production-safe `/mobile/status`.
   - Add a deployable tunnel path only after local status and approval flows pass.
   - Avoid a separate launch script; expose deployment through `praxis workflow deploy tunnel` or another catalog-backed operator surface.

7. Validate end to end:
   - Pending approval appears.
   - Fresh step-up ratifies it.
   - Stale step-up is rejected.
   - Revoking a device revokes sessions, grants, and pending approvals.
   - Blast-radius ceilings force a new gate.

## Retired Cleanup

The old mobile cascade program and per-phase specs were removed because they implied an autonomous eight-phase build path that did not match the actual repository state. Future mobile work should be split into small verified specs only after the backend authority spine is real.
