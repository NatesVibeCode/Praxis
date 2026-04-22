"""WebAuthn RP-ID validation."""

from __future__ import annotations


class WebAuthnValidationError(RuntimeError):
    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


def _normalize_host(value: str) -> str:
    normalized = value.strip().lower().rstrip(".")
    if not normalized:
        raise WebAuthnValidationError("webauthn.rp_id_required", "RP ID/host must be non-empty")
    if "/" in normalized or ":" in normalized:
        raise WebAuthnValidationError("webauthn.rp_id_invalid", "RP ID must be a host name, not a URL")
    return normalized


def validate_rp_id(
    *,
    rp_id: str,
    expected_rp_id: str,
    origin_host: str | None = None,
) -> str:
    """Validate that the asserted RP ID is the configured server RP authority."""

    rp = _normalize_host(rp_id)
    expected = _normalize_host(expected_rp_id)
    if rp != expected:
        raise WebAuthnValidationError(
            "webauthn.rp_id_mismatch",
            f"RP ID mismatch: expected {expected}",
        )
    if origin_host is not None:
        origin = _normalize_host(origin_host)
        if origin != rp and not origin.endswith(f".{rp}"):
            raise WebAuthnValidationError(
                "webauthn.origin_mismatch",
                "origin host is not covered by RP ID",
            )
    return rp


__all__ = ["WebAuthnValidationError", "validate_rp_id"]
