## 1. THE HYBRID

Mobile access is a hybrid: WebAuthn passkeys bind an operator to a specific device, the PWA supplies the mobile control surface, every control action is stamped into a plan-hash envelope before policy evaluation, `gate_evaluations` is extended so the existing `PolicyEngine.evaluate_gate` path records both grant coverage and the envelope hash, and `_SAFE_AUTO_EXECUTE_TYPES` stops being execution authority; it remains only as a UI risk hint until renamed, while durable authority moves to capability grants, approval lifecycle state, gate rows, blast-radius ceilings, policy-drift checks, and the existing control-command lifecycle.

## 2. TABLES

`185_mobile_capability_ledger.sql`

```sql
BEGIN;

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

CREATE TABLE IF NOT EXISTS capability_grants (
    grant_ref text PRIMARY KEY,
    principal_ref text NOT NULL,
    device_id uuid REFERENCES device_enrollments (device_id) ON DELETE RESTRICT,
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

CREATE TABLE IF NOT EXISTS approval_requests (
    request_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    request_status text NOT NULL DEFAULT 'pending'
        CHECK (request_status IN ('pending', 'ratified', 'revoked', 'expired', 'superseded')),
    principal_ref text NOT NULL,
    device_id uuid REFERENCES device_enrollments (device_id) ON DELETE RESTRICT,
    requested_by_kind text NOT NULL,
    requested_by_ref text NOT NULL,
    command_type text NOT NULL,
    control_command_id text,
    plan_envelope_hash text NOT NULL,
    plan_summary text NOT NULL,
    risk_level text NOT NULL CHECK (risk_level IN ('low', 'medium', 'high')),
    blast_radius jsonb NOT NULL DEFAULT '{}'::jsonb,
    grant_ref text REFERENCES capability_grants (grant_ref) ON DELETE RESTRICT,
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

`POST /auth/webauthn/register/complete` verifies the attestation response, creates or refreshes a `device_enrollments` row, consumes the challenge, and returns the enrolled device summary.

`POST /auth/webauthn/assert/begin` creates an assertion challenge for a known principal or device, stores it in `webauthn_challenges`, and returns public-key request options.

`POST /auth/webauthn/assert/complete` verifies the assertion, updates `credential_sign_count` and `last_asserted_at`, creates a short-lived `mobile_sessions` row, and opens the 30-second step-up window.

`POST /auth/bootstrap/exchange` exchanges a bootstrap token for a mobile session only after a successful assertion; it never extends the step-up window without a fresh assertion.

`POST /approvals/request` accepts a stamped plan envelope from the control path, records an `approval_requests` row, and emits a pending-approval event for mobile subscribers.

`GET /approvals/pending` returns pending approval requests visible to the current principal, filtered by active device enrollment and request expiry.

`POST /approvals/{id}/ratify` requires a mobile session whose `step_up_valid_until` is still in the future, marks the request ratified, issues the matching `capability_grants` row, and links `approval_requests.grant_ref`.

`POST /approvals/{id}/revoke` revokes a pending or previously ratified approval request and revokes the linked grant when present.

`GET /approvals/subscribe` opens an SSE stream for pending, ratified, revoked, expired, burn-rate, and policy-drift events visible to the principal.

`POST /capability/grants/{id}/revoke` revokes one grant by `grant_ref`, records who revoked it, and causes future resolver checks for the matching envelope or command to miss.

`GET /mobile/metrics/snapshot` returns the mobile oversight snapshot: pending approvals, active grants, active session burn-rate counters from `session_blast_radius`, active blast-radius policy ceilings, unresolved `policy_drift_events`, and current `autonomy_chain_ledger.depth_counter`.

## 4. PYTHON MODULES

`runtime/capability/types.py` owns dataclasses and enums for principals, devices, grants, request statuses, plan envelopes, blast-radius dimensions, and resolver outcomes.

`runtime/capability/plan_envelope.py` canonicalizes and stamps control intent into a stable envelope containing command type, requested-by fields, target refs, payload digest, risk level, blast-radius estimate, created-at bucket, and `plan_envelope_hash`.

`runtime/capability/resolver.py` resolves whether an active `capability_grants` row covers a stamped envelope; it checks principal, device, expiry, revocation, risk ceiling, command type, target scope, and exact `plan_envelope_hash` when the grant is plan-bound.

`runtime/capability/approval_lifecycle.py` creates approval requests, ratifies requests into grants, revokes requests, expires stale requests, and emits SSE-visible state changes.

`runtime/capability/blast_radius.py` updates `session_blast_radius`, compares active counters to `session_blast_radius_policy`, and forces a new approval request when any ceiling is crossed even if a grant otherwise covers the command.

`runtime/capability/policy_drift.py` records inverse checks into `policy_drift_events` when runtime behavior violates active operator policy and exposes unresolved counts to mobile metrics.

`runtime/capability/autonomy_chain.py` increments depth for auto-covered actions, resets or terminates the active chain on human ratification, and feeds `autonomy_depth` for the mobile snapshot.

`runtime/webauthn/ceremonies.py` owns challenge generation, attestation verification, assertion verification, sign-count checks, and conversion from verified assertions into `device_enrollments` and `mobile_sessions` updates.

`surfaces/api/handlers/webauthn_auth.py` exposes the four WebAuthn endpoints and the bootstrap exchange endpoint.

`surfaces/api/handlers/approvals.py` exposes approval request, pending, ratify, revoke, grant revoke, and SSE subscribe endpoints.

`surfaces/api/handlers/mobile_metrics.py` exposes the metrics snapshot endpoint and keeps the read model aligned with migration 189 oversight instrumentation.

## 5. TYPESCRIPT MODULES

`surfaces/app/src/mobile/` is a Phase 5 stub until the backend contract is settled. It should contain typed API clients and placeholder views only: `api.ts` for endpoint calls, `types.ts` mirroring the Python capability payloads, `ApprovalInbox.tsx` for pending approvals, `PasskeySetup.tsx` for registration, `StepUpPrompt.tsx` for assertion, `BurnRateHeader.tsx` for visible blast-radius ceilings, and `MobileMetricsSnapshot.tsx` for oversight counters.

## 6. THE CALL GRAPH

1. `chat_tools.py` receives a `run_workflow`, `retry_job`, or `cancel_workflow` tool call and builds the same `ControlIntent` shape currently sent to `request_control_command`.

2. Before calling the command bus, `chat_tools.py` calls `plan_envelope.stamp(intent, context)` to canonicalize the payload, estimate blast radius, assign risk metadata, and compute `plan_envelope_hash`.

3. `resolver.resolve_capability_grant(envelope, principal_ref, device_id)` checks active grants against the stamped envelope and returns either a covered `grant_ref` or a miss reason that can become an `approval_requests` row.

4. `blast_radius.record_or_gate(envelope)` updates the session working-set counters and can force approval even when the resolver found a grant, because burn-rate ceilings are session-level limits.

5. `classify_control_intent` still computes low/medium/high disposition for compatibility, but it consumes resolver and blast-radius output; `_SAFE_AUTO_EXECUTE_TYPES` is no longer consulted as authority.

6. `request_control_command` persists the command; auto-execution is allowed only when the resolver returned a live grant or the command is otherwise covered by explicit policy, and the plan envelope remains attached to the command metadata.

7. `submission_policy.evaluate_publish_policy` continues using `PolicyEngine.evaluate_gate`, then `_insert_gate_evaluation` inserts the existing gate evidence plus `grant_ref` and `plan_envelope_hash`.

8. `approval_lifecycle` emits request and ratification events, `autonomy_chain` increments or resets depth, and mobile SSE subscribers receive the current pending state and metrics snapshot.

## 7. WHERE _SAFE_AUTO_EXECUTE_TYPES GOES

Phase 2 renames `_SAFE_AUTO_EXECUTE_TYPES` to `_UI_RISK_HINT_TYPES`. It remains a frozenset near the control command enum because the UI still needs a cheap first-pass hint for button copy, default badges, and whether a command usually feels low risk. No runtime execution, approval, or gate decision may read it after the rename. The only authoritative checks are grant resolution, approval lifecycle state, plan-envelope hash equality, blast-radius ceilings, policy drift, and the gate evaluation row produced by the policy engine.

## 8. STOLEN-PHONE DEFENSE

Ratification requires a fresh WebAuthn assertion with `mobile_sessions.step_up_valid_until` less than 30 seconds after `assertion_at`; holding an unlocked phone or copied session token is insufficient once that window closes. A CLI `revoke-device` operator command must revoke the `device_enrollments` row, revoke active `mobile_sessions`, revoke active `capability_grants` bound to that device, and emit an SSE event that clears pending mobile UI state. The PWA must always show the active burn-rate ceiling from `session_blast_radius_policy` beside current session counters so an operator can see how much damage remains possible before the next forced gate.
