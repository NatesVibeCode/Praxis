"""WebAuthn assertion metadata verification."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from .rp_id import validate_rp_id


@dataclass(frozen=True, slots=True)
class AssertionVerification:
    principal_ref: str | None
    device_id: str | None
    rp_id: str
    sign_count: int


def _value(row: Mapping[str, Any], key: str) -> Any:
    return row.get(key)


def _ensure_live_challenge(challenge: Mapping[str, Any], *, now: datetime) -> None:
    if _value(challenge, "consumed_at") is not None:
        raise ValueError("webauthn.challenge_replayed")
    expires_at = _value(challenge, "expires_at")
    if isinstance(expires_at, datetime) and expires_at <= now:
        raise ValueError("webauthn.challenge_expired")


def verify_assertion_metadata(
    *,
    challenge: Mapping[str, Any],
    device: Mapping[str, Any],
    rp_id: str,
    expected_rp_id: str,
    origin_host: str | None,
    sign_count: int,
    now: datetime | None = None,
) -> AssertionVerification:
    """Verify server-side assertion metadata before cryptographic acceptance."""

    effective_now = now or datetime.now(timezone.utc)
    _ensure_live_challenge(challenge, now=effective_now)
    normalized_rp_id = validate_rp_id(
        rp_id=rp_id,
        expected_rp_id=expected_rp_id,
        origin_host=origin_host,
    )
    if _value(device, "revoked_at") is not None:
        raise ValueError("webauthn.device_revoked")
    stored_count = int(_value(device, "credential_sign_count") or 0)
    if int(sign_count) <= stored_count:
        raise ValueError("webauthn.sign_count_not_increasing")
    challenge_device = _value(challenge, "device_id")
    if challenge_device is not None and str(challenge_device) != str(_value(device, "device_id")):
        raise ValueError("webauthn.challenge_device_mismatch")
    return AssertionVerification(
        principal_ref=None if _value(device, "principal_ref") is None else str(_value(device, "principal_ref")),
        device_id=None if _value(device, "device_id") is None else str(_value(device, "device_id")),
        rp_id=normalized_rp_id,
        sign_count=int(sign_count),
    )


__all__ = ["AssertionVerification", "verify_assertion_metadata"]
