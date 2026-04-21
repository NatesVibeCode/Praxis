## 1. THE HYBRID

Mobile access is a hybrid: WebAuthn passkeys bind operator authority to enrolled devices, the PWA provides the mobile approval surface, every control action receives a canonical plan-hash envelope before execution policy is evaluated, `gate_evaluations` is extended to persist both `grant_ref` and `plan_envelope_hash` beside the existing `PolicyEngine.evaluate_gate` evidence, and `_SAFE_AUTO_EXECUTE_TYPES` is killed as authority; it can survive only as a UI hint until Phase 2 renames it, while durable execution authority moves to capability grants, approval lifecycle state, exact plan-envelope hash coverage, blast-radius ceilings, policy-drift checks, and gate rows.

## 2. TABLES

`185_mobile_capability_ledger.sql`

```sql
BEGIN;

CREATE TABLE IF NOT EXISTS capability_grants (
    grant_ref text PRIMARY KEY,
    principal_ref text NOT NULL,
    device_id uuid,
    grant_kind text NOT NULL,
    capability_scope jsonb NOT NULL,
    max_risk_level text NOT NULL CHECK (max_risk_level IN ('low', 'medium', 'high')),
    plan_envelope_hash text,
    approval_request_id uuid,
    issued_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    revoked_by text,
    revoke_reason text,
    decision_ref text,
    CONSTRAINT capability_grants_ref_nonblank CHECK (btrim(grant_ref) <> ''),
    CONSTRAINT capability_grants_principal_nonblank CHECK (btrim(principal_ref) <> ''),
    CONSTRAINT capability_grants_kind_valid CHECK (grant_kind IN ('device_session', 'plan', 'command', 'blast_radius')),
    CONSTRAINT capability_grants_scope_object CHECK (jsonb_typeof(capability_scope) = 'object'),
    CONSTRAINT capability_grants_expiry_after_issue CHECK (expires_at > issued_at)
);

CREATE INDEX IF NOT EXISTS idx_capability_grants_principal_active
    ON capability_grants (principal_ref, expires_at DESC)
    WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_capability_grants_plan_envelope
    ON capability_grants (plan_envelope_hash)
    WHERE plan_envelope_hash IS NOT NULL AND revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS device_enrollments (
    device_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    principal_ref text NOT NULL,
    credential_id text NOT NULL UNIQUE,
    credential_public_key bytea NOT NULL,
    credential_sign_count bigint NOT NULL DEFAULT 0,
    device_label text NOT NULL,
    aaguid text,
    transports jsonb NOT NULL DEFAULT '[]'::jsonb,
    enrolled_at timestamptz NOT NULL DEFAULT now(),
    last_asserted_at timestamptz,
    revoked_at timestamptz,
    revoked_by text,
    revoke_reason text,
    CONSTRAINT device_enrollments_principal_nonblank CHECK (btrim(principal_ref) <> ''),
    CONSTRAINT device_enrollments_label_nonblank CHECK (btrim(device_label) <> '')
);

CREATE INDEX IF NOT EXISTS idx_device_enrollments_principal_active
    ON device_enrollments (principal_ref, enrolled_at DESC)
    WHERE revoked_at IS NULL;

CREATE TABLE IF NOT EXISTS approval_requests (
    request_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    request_status text NOT NULL DEFAULT 'pending'
        CHECK (request_status IN ('pending', 'ratified', 'revoked', 'expired', 'superseded')),
    principal_ref text NOT NULL,
    device_id uuid,
    requested_by_kind text NOT NULL,
    requested_by_ref text NOT NULL,
    command_type text NOT NULL,
    control_command_id text,
    plan_envelope_hash text NOT NULL,
    plan_summary text NOT NULL,
    risk_level text NOT NULL CHECK (risk_level IN ('low', 'medium', 'high')),
    blast_radius jsonb NOT NULL DEFAULT '{}'::jsonb,
    grant_ref text,
    requested_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    ratified_at timestamptz,
    ratified_by text,
    revoked_at timestamptz,
    revoked_by text,
    revoke_reason text,
    CONSTRAINT approval_requests_principal_nonblank CHECK (btrim(principal_ref) <> ''),
    CONSTRAINT approval_requests_plan_hash_nonblank CHECK (btrim(plan_envelope_hash) <> ''),
    CONSTRAINT approval_requests_expiry_after_request CHECK (expires_at > requested_at),
    CONSTRAINT approval_requests_status_timestamps CHECK (
        (request_status = 'ratified' AND ratified_at IS NOT NULL)
        OR (request_status = 'revoked' AND revoked_at IS NOT NULL)
        OR (request_status NOT IN ('ratified', 'revoked'))
    )
);

CREATE INDEX IF NOT EXISTS idx_approval_requests_pending
    ON approval_requests (requested_at DESC)
    WHERE request_status = 'pending';

CREATE INDEX IF NOT EXISTS idx_approval_requests_plan_hash
    ON approval_requests (plan_envelope_hash, requested_at DESC);

ALTER TABLE capability_grants
    ADD CONSTRAINT capability_grants_device_id_fkey
    FOREIGN KEY (device_id)
    REFERENCES device_enrollments (device_id)
    ON DELETE RESTRICT;

ALTER TABLE approval_requests
    ADD CONSTRAINT approval_requests_device_id_fkey
    FOREIGN KEY (device_id)
    REFERENCES device_enrollments (device_id)
    ON DELETE RESTRICT;

ALTER TABLE approval_requests
    ADD CONSTRAINT approval_requests_grant_ref_fkey
    FOREIGN KEY (grant_ref)
    REFERENCES capability_grants (grant_ref)
    ON DELETE RESTRICT;

COMMIT;
```

`186_gate_evaluations_grant_coverage.sql`

```sql
BEGIN;

ALTER TABLE gate_evaluations
    ADD COLUMN IF NOT EXISTS grant_ref text,
    ADD COLUMN IF NOT EXISTS plan_envelope_hash text;

ALTER TABLE gate_evaluations
    DROP CONSTRAINT IF EXISTS gate_evaluations_plan_envelope_hash_nonblank;

ALTER TABLE gate_evaluations
    ADD CONSTRAINT gate_evaluations_plan_envelope_hash_nonblank
    CHECK (plan_envelope_hash IS NULL OR btrim(plan_envelope_hash) <> '') NOT VALID;

CREATE INDEX IF NOT EXISTS idx_gate_evaluations_grant_ref
    ON gate_evaluations (grant_ref)
    WHERE grant_ref IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_gate_evaluations_plan_envelope_hash
    ON gate_evaluations (plan_envelope_hash, decided_at DESC)
    WHERE plan_envelope_hash IS NOT NULL;

COMMENT ON COLUMN gate_evaluations.grant_ref IS
    'Capability grant that covered this gate evaluation, when one was active.';

COMMENT ON COLUMN gate_evaluations.plan_envelope_hash IS
    'Canonical hash of the stamped control plan envelope evaluated by policy.';

COMMIT;
```

`187_webauthn_challenges.sql`

```sql
BEGIN;

CREATE TABLE IF NOT EXISTS webauthn_challenges (
    challenge_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    challenge_kind text NOT NULL CHECK (challenge_kind IN ('register', 'assert')),
    challenge_token text NOT NULL UNIQUE,
    principal_ref text,
    device_id uuid,
    rp_id text NOT NULL,
    user_handle text,
    public_key_options jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    consumed_at timestamptz,
    consumed_by_session_id uuid,
    CONSTRAINT webauthn_challenges_token_nonblank CHECK (btrim(challenge_token) <> ''),
    CONSTRAINT webauthn_challenges_rp_nonblank CHECK (btrim(rp_id) <> ''),
    CONSTRAINT webauthn_challenges_expiry_after_create CHECK (expires_at > created_at)
);

CREATE INDEX IF NOT EXISTS idx_webauthn_challenges_active
    ON webauthn_challenges (challenge_kind, expires_at)
    WHERE consumed_at IS NULL;

COMMIT;
```

`188_mobile_sessions.sql`

```sql
BEGIN;

CREATE TABLE IF NOT EXISTS mobile_sessions (
    mobile_session_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    principal_ref text NOT NULL,
    device_id uuid NOT NULL REFERENCES device_enrollments (device_id) ON DELETE RESTRICT,
    session_token_hash text NOT NULL UNIQUE,
    bootstrap_token_hash text UNIQUE,
    assertion_at timestamptz NOT NULL,
    step_up_valid_until timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    last_seen_at timestamptz NOT NULL DEFAULT now(),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    revoked_by text,
    revoke_reason text,
    CONSTRAINT mobile_sessions_principal_nonblank CHECK (btrim(principal_ref) <> ''),
    CONSTRAINT mobile_sessions_token_hash_nonblank CHECK (btrim(session_token_hash) <> ''),
    CONSTRAINT mobile_sessions_step_up_window CHECK (step_up_valid_until <= assertion_at + interval '30 seconds'),
    CONSTRAINT mobile_sessions_expiry_after_create CHECK (expires_at > created_at)
);

CREATE INDEX IF NOT EXISTS idx_mobile_sessions_principal_active
    ON mobile_sessions (principal_ref, last_seen_at DESC)
    WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_mobile_sessions_device_active
    ON mobile_sessions (device_id, last_seen_at DESC)
    WHERE revoked_at IS NULL;

COMMIT;
```

## 3. HTTP ENDPOINTS

`POST /auth/webauthn/register/begin` creates a registration challenge for an authenticated principal, stores it in `webauthn_challenges`, and returns public-key creation options.

`POST /auth/webauthn/register/complete` verifies attestation, creates or refreshes `device_enrollments`, consumes the challenge, and returns the enrolled device summary.

`POST /auth/webauthn/assert/begin` creates an assertion challenge for a principal or device, stores it in `webauthn_challenges`, and returns public-key request options.

`POST /auth/webauthn/assert/complete` verifies assertion, updates `credential_sign_count` and `last_asserted_at`, creates or refreshes `mobile_sessions`, and opens the 30-second step-up window.

`POST /auth/bootstrap/exchange` exchanges a bootstrap token for a mobile session after a successful assertion; it never extends step-up authority without a fresh assertion.

`POST /approvals/request` accepts a stamped plan envelope from the control path, records `approval_requests`, and emits a pending-approval event.

`GET /approvals/pending` returns pending approval requests visible to the current principal, filtered by active device enrollment and expiry.

`POST /approvals/{id}/ratify` requires a session whose `step_up_valid_until` is still in the future, ratifies the request, issues `capability_grants`, and links `approval_requests.grant_ref`.

`POST /approvals/{id}/revoke` revokes a pending or ratified approval request and revokes the linked grant when present.

`GET /approvals/subscribe` opens an SSE stream for pending, ratified, revoked, expired, burn-rate, policy-drift, and autonomy-depth events visible to the principal.

`POST /capability/grants/{id}/revoke` revokes one grant by `grant_ref`, records who revoked it, and makes future resolver checks for the matching envelope or command miss.

`GET /mobile/metrics/snapshot` returns the oversight snapshot: pending approvals, active grants, active session burn-rate counters from `session_blast_radius`, active ceilings from `session_blast_radius_policy`, unresolved `policy_drift_events`, and current `autonomy_chain_ledger.depth_counter`.

## 4. PYTHON MODULES

`runtime/capability/types.py` owns dataclasses and enums for principals, devices, grants, request statuses, plan envelopes, blast-radius dimensions, resolver outcomes, and mobile metrics payloads.

`runtime/capability/plan_envelope.py` canonicalizes and stamps `ControlIntent` into a stable envelope containing command type, requested-by fields, target refs, payload digest, risk level, blast-radius estimate, created-at bucket, and `plan_envelope_hash`.

`runtime/capability/resolver.py` resolves whether an active `capability_grants` row covers a stamped envelope by checking principal, device, expiry, revocation, risk ceiling, command type, target scope, and exact `plan_envelope_hash` when the grant is plan-bound.

`runtime/capability/approval_lifecycle.py` creates approval requests, ratifies requests into grants, revokes requests, expires stale requests, and emits SSE-visible state changes.

`runtime/capability/blast_radius.py` updates `session_blast_radius`, compares active counters to `session_blast_radius_policy`, and forces approval when any ceiling is crossed even if a grant otherwise covers the command.

`runtime/capability/policy_drift.py` records inverse checks into `policy_drift_events` when runtime behavior violates active operator policy and exposes unresolved counts to mobile metrics.

`runtime/capability/autonomy_chain.py` increments depth for auto-covered actions, resets or terminates the active chain on human ratification, and feeds `autonomy_depth` for the mobile snapshot.

`runtime/webauthn/ceremonies.py` owns challenge generation, attestation verification, assertion verification, sign-count checks, and conversion from verified assertions into `device_enrollments` and `mobile_sessions` updates.

`surfaces/api/handlers/webauthn_auth.py` exposes registration, assertion, and bootstrap exchange endpoints.

`surfaces/api/handlers/approvals.py` exposes approval request, pending, ratify, revoke, grant revoke, and SSE subscribe endpoints.

`surfaces/api/handlers/mobile_metrics.py` exposes the metrics snapshot endpoint and keeps the read model aligned with already-applied migration `189_oversight_instrumentation.sql`.

## 5. TYPESCRIPT MODULES

`surfaces/app/src/mobile/` is a Phase 5 stub until the backend contract is settled. It should contain typed API clients and placeholder views only: `api.ts` for endpoint calls, `types.ts` mirroring Python capability payloads, `ApprovalInbox.tsx` for pending approvals, `PasskeySetup.tsx` for registration, `StepUpPrompt.tsx` for assertion, `BurnRateHeader.tsx` for visible blast-radius ceilings, and `MobileMetricsSnapshot.tsx` for oversight counters.

## 6. THE CALL GRAPH

1. `chat_tools.py` receives `run_workflow`, `retry_job`, or `cancel_workflow` and builds the `ControlIntent` shape currently sent to `request_control_command`.

2. Before the command bus call, `chat_tools.py` calls `plan_envelope.stamp(intent, context)` to canonicalize payload, estimate blast radius, assign risk metadata, and compute `plan_envelope_hash`.

3. `resolver.resolve_capability_grant(envelope, principal_ref, device_id)` checks active grants against the stamped envelope and returns either a covering `grant_ref` or a miss reason.

4. A miss creates or reuses an `approval_requests` row through `approval_lifecycle`; a ratified request issues the grant that future resolver calls can see.

5. `blast_radius.record_or_gate(envelope)` updates the session working-set counters and can force approval even when the resolver found a grant, because burn-rate ceilings are session-level limits.

6. `classify_control_intent` remains the control-command choke point, but it consumes resolver and blast-radius output; `_SAFE_AUTO_EXECUTE_TYPES` is not consulted for authority.

7. `request_control_command` persists the command; auto-execution is allowed only when explicit grant or policy coverage exists, and the plan envelope remains attached to command metadata.

8. `submission_policy.evaluate_publish_policy` continues using `PolicyEngine.evaluate_gate`; `_insert_gate_evaluation` inserts existing gate evidence plus `grant_ref` and `plan_envelope_hash`.

9. `approval_lifecycle` emits request and ratification events, `autonomy_chain` increments or resets depth, and mobile SSE subscribers receive pending state plus metrics updates.

## 7. WHERE _SAFE_AUTO_EXECUTE_TYPES GOES

Phase 2 renames `_SAFE_AUTO_EXECUTE_TYPES` to `_UI_RISK_HINT_TYPES`. It remains near the control-command enum only because UI consumers need a cheap first-pass hint for button copy, default badges, and whether a command usually appears low risk. No runtime execution, approval, or gate decision may read it after the rename. The only authoritative checks are grant resolution, approval lifecycle state, plan-envelope hash equality, blast-radius ceilings, policy drift, and the gate evaluation row produced by the policy engine.

## 8. STOLEN-PHONE DEFENSE

Ratification requires a fresh WebAuthn assertion with `mobile_sessions.step_up_valid_until` less than or equal to 30 seconds after `assertion_at`; holding an unlocked phone or copied session token is insufficient once that window closes. A CLI `revoke-device` command must revoke the `device_enrollments` row, active `mobile_sessions`, active `capability_grants` bound to that device, and any pending approval requests for that device, then emit an SSE event that clears mobile UI state. The PWA must always show the visible burn-rate ceiling from `session_blast_radius_policy` beside current `session_blast_radius` counters so the operator can see how much damage remains possible before the next forced gate.
