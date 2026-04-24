from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from fastapi.responses import Response
from fastapi.testclient import TestClient

import runtime.audit_primitive as audit_primitive
from runtime.capability.plan_envelope import build_plan_envelope, canonical_payload_digest
from runtime.capability.resolver import resolve_capability_grant
from runtime.control_commands import (
    ControlCommandPolicyError,
    ControlCommandType,
    ControlExecutionMode,
    ControlIntent,
    evaluate_control_intent_policy,
    stamp_control_intent,
)
from runtime.capability.approval_lifecycle import revoke_device_authority
from runtime.capability.sessions import (
    MobileSessionError,
    exchange_bootstrap_token,
    hash_secret,
    spend_session_budget,
)
from runtime.mobile_security import AUTH_COOKIE_NAME, set_mobile_session_cookie
from runtime.webauthn.assertions import verify_assertion_metadata
from runtime.webauthn.rp_id import validate_rp_id


class _GrantConn:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, *_args):
        return list(self.rows)


class _RevocationConn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_exc_info) -> bool:
        return False

    def execute(self, sql: str, *_args):
        self.statements.append(sql)
        if "UPDATE device_enrollments" in sql:
            return [{"device_id": "00000000-0000-0000-0000-000000000001"}]
        if "UPDATE mobile_sessions" in sql:
            return [{"session_id": "00000000-0000-0000-0000-000000000002"}]
        if "UPDATE capability_grants" in sql:
            return [{"grant_id": "mobile.grant.1"}]
        if "UPDATE approval_requests" in sql:
            return [{"request_id": "00000000-0000-0000-0000-000000000003"}]
        return []


class _BootstrapExchangeConn:
    def __init__(self, *, consumed_at=None, revoked_at=None, expires_at=None) -> None:
        self.statements: list[str] = []
        self.token_id = "00000000-0000-0000-0000-000000000010"
        self.expires_at = expires_at or datetime.now(timezone.utc) + timedelta(minutes=10)
        self.consumed_at = consumed_at
        self.revoked_at = revoked_at
        self.consumed_session_id = None

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *_exc_info) -> bool:
        return False

    def execute(self, sql: str, *args):
        self.statements.append(sql)
        if "FROM mobile_bootstrap_tokens" in sql:
            token_hash = args[0]
            if token_hash != hash_secret("bootstrap-secret"):
                return []
            return [
                {
                    "token_id": self.token_id,
                    "principal_ref": "operator:nate",
                    "token_hash": token_hash,
                    "expires_at": self.expires_at,
                    "consumed_at": self.consumed_at,
                    "revoked_at": self.revoked_at,
                }
            ]
        if "INSERT INTO device_enrollments" in sql:
            return [
                {
                    "device_id": args[0],
                    "principal_ref": args[1],
                    "device_label": args[4],
                    "enrolled_at": args[5],
                    "last_asserted_at": args[5],
                }
            ]
        if "INSERT INTO mobile_sessions" in sql:
            return [
                {
                    "session_id": args[0],
                    "principal_ref": args[1],
                    "device_id": args[2],
                    "created_at": args[4],
                    "expires_at": args[5],
                    "last_step_up_at": args[4],
                    "budget_limit": args[6],
                    "budget_used": 0,
                }
            ]
        if "UPDATE mobile_bootstrap_tokens" in sql:
            self.consumed_session_id = args[2]
            return []
        return []


class _BudgetConn:
    def __init__(self, *, budget_limit: int = 3, budget_used: int = 0) -> None:
        self.budget_limit = budget_limit
        self.budget_used = budget_used
        self.events: list[dict[str, object]] = []
        self.statements: list[str] = []

    def execute(self, sql: str, *args):
        self.statements.append(sql)
        session_id, units, reason_code, recorded_at = args
        if self.budget_used + units > self.budget_limit:
            return []
        self.budget_used += units
        event_id = f"budget-event-{len(self.events) + 1}"
        self.events.append(
            {
                "budget_event_id": event_id,
                "session_id": session_id,
                "units": units,
                "budget_used_after": self.budget_used,
                "reason_code": reason_code,
                "recorded_at": recorded_at,
            }
        )
        return [
            {
                "session_id": session_id,
                "principal_ref": "operator:nate",
                "budget_used": self.budget_used,
                "budget_event_id": event_id,
            }
        ]


def test_plan_envelope_hash_ignores_authority_stamps() -> None:
    payload = {"run_id": "run-1", "grant_ref": "grant-spoof"}
    assert canonical_payload_digest(payload) == canonical_payload_digest({"run_id": "run-1"})

    envelope = build_plan_envelope(
        command_type="workflow.submit",
        requested_by_kind="mobile",
        requested_by_ref="phone",
        risk_level="low",
        payload=payload,
    )
    stamped = dict(payload)
    stamped["plan_envelope_hash"] = envelope.plan_hash
    assert envelope.plan_hash == build_plan_envelope(
        command_type="workflow.submit",
        requested_by_kind="mobile",
        requested_by_ref="phone",
        risk_level="low",
        payload=stamped,
    ).plan_hash


def test_mobile_control_intent_requires_covered_grant() -> None:
    intent = ControlIntent(
        command_type=ControlCommandType.WORKFLOW_SUBMIT,
        requested_by_kind="mobile",
        requested_by_ref="phone",
        idempotency_key="idem.mobile.1",
        payload={"repo_root": "/repo", "spec_path": "spec.queue.json"},
    )

    decision = evaluate_control_intent_policy(_GrantConn([]), intent)
    assert decision.mode == ControlExecutionMode.CONFIRM_REQUIRED.value
    assert decision.reason_code == "control.policy.capability_grant_required"

    stamped = stamp_control_intent(
        ControlIntent(
            command_type=intent.command_type,
            requested_by_kind=intent.requested_by_kind,
            requested_by_ref=intent.requested_by_ref,
            idempotency_key=intent.idempotency_key,
            payload={**dict(intent.payload), "grant_ref": "mobile.grant.1"},
        )
    )
    grant_rows = [
        {
            "grant_id": "mobile.grant.1",
            "principal_ref": "operator:nate",
            "device_id": None,
            "grant_kind": "plan",
            "capability_scope": {"command_types": ["workflow.submit"]},
            "max_risk_level": "low",
            "plan_envelope_hash": stamped.plan_envelope_hash,
            "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
            "revoked_at": None,
        }
    ]
    decision = evaluate_control_intent_policy(_GrantConn(grant_rows), stamped)
    assert decision.mode == ControlExecutionMode.AUTO_EXECUTE.value
    assert decision.reason_code == "capability.grant.covered"


def test_chain_attack_mismatched_plan_hash_fails_closed() -> None:
    intent = ControlIntent(
        command_type=ControlCommandType.WORKFLOW_SUBMIT,
        requested_by_kind="mobile",
        requested_by_ref="phone",
        idempotency_key="idem.mobile.chain",
        payload={
            "repo_root": "/repo",
            "spec_path": "spec.queue.json",
            "grant_ref": "mobile.grant.forged",
        },
        plan_envelope_hash="plan:v1:forged",
    )

    try:
        evaluate_control_intent_policy(_GrantConn([]), intent)
    except ControlCommandPolicyError as exc:
        assert exc.reason_code == "control.command.plan_hash_mismatch"
    else:
        raise AssertionError("forged plan-envelope hashes must fail closed")


def test_chain_attack_revoked_grant_does_not_authorize() -> None:
    stamped = stamp_control_intent(
        ControlIntent(
            command_type=ControlCommandType.WORKFLOW_SUBMIT,
            requested_by_kind="mobile",
            requested_by_ref="phone",
            idempotency_key="idem.mobile.revoked",
            payload={
                "repo_root": "/repo",
                "spec_path": "spec.queue.json",
                "grant_ref": "mobile.grant.revoked",
            },
        )
    )
    decision = evaluate_control_intent_policy(
        _GrantConn(
            [
                {
                    "grant_id": "mobile.grant.revoked",
                    "principal_ref": "operator:nate",
                    "device_id": None,
                    "grant_kind": "plan",
                    "capability_scope": {"command_types": ["workflow.submit"]},
                    "max_risk_level": "low",
                    "plan_envelope_hash": stamped.plan_envelope_hash,
                    "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
                    "revoked_at": datetime.now(timezone.utc),
                }
            ]
        ),
        stamped,
    )

    assert decision.mode == ControlExecutionMode.CONFIRM_REQUIRED.value
    assert decision.reason_code == "capability.grant.not_covered"


def test_resolver_rejects_wrong_plan_hash() -> None:
    envelope = build_plan_envelope(
        command_type="workflow.submit",
        requested_by_kind="mobile",
        requested_by_ref="phone",
        risk_level="low",
        payload={"spec_path": "spec.queue.json"},
    )
    result = resolve_capability_grant(
        _GrantConn(
            [
                {
                    "grant_id": "mobile.grant.bad",
                    "principal_ref": "operator:nate",
                    "device_id": None,
                    "grant_kind": "plan",
                    "capability_scope": {"command_types": ["workflow.submit"]},
                    "max_risk_level": "low",
                    "plan_envelope_hash": "plan:v1:wrong",
                    "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
                    "revoked_at": None,
                }
            ]
        ),
        envelope=envelope,
        grant_ref="mobile.grant.bad",
    )
    assert result.covered is False
    assert result.reason_code == "capability.grant.not_covered"


def test_device_revoke_revokes_pending_approval_requests_atomically() -> None:
    conn = _RevocationConn()

    result = revoke_device_authority(
        conn,
        device_id="00000000-0000-0000-0000-000000000001",
        revoked_by="operator:nate",
        revoke_reason="stolen phone",
    )

    assert result == {
        "device_id": "00000000-0000-0000-0000-000000000001",
        "device_rows": 1,
        "session_rows": 1,
        "grant_rows": 1,
        "approval_rows": 1,
    }
    assert any(
        "UPDATE approval_requests" in statement and "request_status = 'pending'" in statement
        for statement in conn.statements
    )


def test_bootstrap_exchange_hashes_consumes_token_and_creates_session() -> None:
    conn = _BootstrapExchangeConn()

    exchange = exchange_bootstrap_token(
        conn,
        bootstrap_token_secret="bootstrap-secret",
        device_id="00000000-0000-0000-0000-000000000020",
        session_token_secret="session-secret",
    )

    assert exchange["session"]["principal_ref"] == "operator:nate"
    assert exchange["session"]["device_id"] == "00000000-0000-0000-0000-000000000020"
    assert exchange["session_token_secret"] == "session-secret"
    assert conn.consumed_session_id == exchange["session"]["session_id"]
    assert any("WHERE token_hash = $1" in statement for statement in conn.statements)
    assert any("INSERT INTO device_enrollments" in statement for statement in conn.statements)
    assert any("UPDATE mobile_bootstrap_tokens" in statement for statement in conn.statements)


def test_mobile_session_budget_spend_is_atomic_and_receipted() -> None:
    conn = _BudgetConn(budget_limit=3, budget_used=1)

    result = spend_session_budget(
        conn,
        session_id="00000000-0000-0000-0000-000000000030",
        units=2,
        reason_code="mobile.approval.ratify",
    )

    assert result["budget_used"] == 3
    assert result["budget_event_id"] == "budget-event-1"
    assert conn.events == [
        {
            "budget_event_id": "budget-event-1",
            "session_id": "00000000-0000-0000-0000-000000000030",
            "units": 2,
            "budget_used_after": 3,
            "reason_code": "mobile.approval.ratify",
            "recorded_at": conn.events[0]["recorded_at"],
        }
    ]
    statement = " ".join(conn.statements[0].split())
    assert "UPDATE mobile_sessions SET budget_used = budget_used + $2" in statement
    assert "budget_used + $2 <= budget_limit" in statement
    assert "INSERT INTO mobile_session_budget_events" in statement

    try:
        spend_session_budget(
            conn,
            session_id="00000000-0000-0000-0000-000000000030",
            units=1,
            reason_code="mobile.approval.ratify",
        )
    except MobileSessionError as exc:
        assert exc.reason_code == "mobile.session_budget_denied"
    else:
        raise AssertionError("session budget overrun should fail closed")
    assert len(conn.events) == 1


def test_webauthn_rejects_rp_mismatch_and_replayed_sign_count() -> None:
    try:
        validate_rp_id(rp_id="evil.example", expected_rp_id="praxis.local", origin_host="evil.example")
    except Exception as exc:
        assert getattr(exc, "reason_code") == "webauthn.rp_id_mismatch"
    else:
        raise AssertionError("RP mismatch should fail closed")

    challenge = {
        "device_id": "device-1",
        "expires_at": datetime.now(timezone.utc) + timedelta(minutes=5),
        "consumed_at": None,
    }
    device = {
        "device_id": "device-1",
        "principal_ref": "operator:nate",
        "credential_sign_count": 7,
        "revoked_at": None,
    }
    try:
        verify_assertion_metadata(
            challenge=challenge,
            device=device,
            rp_id="praxis.local",
            expected_rp_id="praxis.local",
            origin_host="praxis.local",
            sign_count=7,
        )
    except ValueError as exc:
        assert str(exc) == "webauthn.sign_count_not_increasing"
    else:
        raise AssertionError("non-increasing sign count should fail closed")


def test_audit_autorun_needs_capability_grant(monkeypatch) -> None:
    monkeypatch.setattr(
        audit_primitive,
        "plan_all",
        lambda *_args, **_kwargs: {
            "plans": [
                {
                    "finding": {"id": "finding-1"},
                    "action": {
                        "pattern": "known",
                        "action": "noop",
                        "subject": "subject",
                        "autorun_ok": True,
                    },
                }
            ]
        },
    )
    monkeypatch.setitem(
        audit_primitive._PATTERN_REGISTRY,
        "known",
        SimpleNamespace(cost_tier="deterministic", deterministic=True),
    )
    result = audit_primitive.apply_autorunnable(_GrantConn([]))
    assert result["applied_count"] == 0
    assert result["authority_grant_covered"] is False
    assert result["skipped"][0]["reason"] == "capability grant required"


def test_mobile_cookie_and_pwa_contracts() -> None:
    response = Response()
    set_mobile_session_cookie(response, session_token="secret")
    cookie_header = response.headers["set-cookie"]
    assert AUTH_COOKIE_NAME in cookie_header
    assert "HttpOnly" in cookie_header
    assert "Secure" in cookie_header
    assert "SameSite=strict" in cookie_header

    app_root = Path(__file__).resolve().parents[2] / "surfaces" / "app"
    assert (app_root / "public" / "sw.js").read_text(encoding="utf-8")
    assert (app_root / "public" / "manifest.webmanifest").read_text(encoding="utf-8")


def test_stale_mobile_approval_ratify_response_is_no_store(monkeypatch) -> None:
    import surfaces.api.rest as rest

    monkeypatch.setattr(rest, "_shared_pg_conn", lambda: object())
    stale = datetime.now(timezone.utc) - timedelta(minutes=10)
    with TestClient(rest.app) as client:
        response = client.post(
            "/approvals/00000000-0000-0000-0000-000000000001/ratify",
            json={
                "ratified_by": "operator:nate",
                "assertion_verified_at": stale.isoformat(),
                "assertion_expires_at": (stale + timedelta(minutes=1)).isoformat(),
            },
        )

    assert response.status_code == 401
    assert response.json()["error_code"] == "approval.assertion_stale"
    assert "no-store" in response.headers["Cache-Control"]


def test_bootstrap_exchange_endpoint_sets_secure_cookie_without_returning_secret(monkeypatch) -> None:
    import surfaces.api.rest as rest

    monkeypatch.setattr(rest, "_shared_pg_conn", lambda: _BootstrapExchangeConn())
    with TestClient(rest.app) as client:
        response = client.post(
            "/auth/bootstrap/exchange",
            json={
                "bootstrap_token": "bootstrap-secret",
                "device_id": "00000000-0000-0000-0000-000000000020",
            },
        )

    assert response.status_code == 200
    assert "session_token_secret" not in response.text
    assert "bootstrap-secret" not in response.text
    assert "praxis_mobile_session" in response.headers["set-cookie"]
    assert "HttpOnly" in response.headers["set-cookie"]
    assert "Secure" in response.headers["set-cookie"]
    assert "no-store" in response.headers["Cache-Control"]
