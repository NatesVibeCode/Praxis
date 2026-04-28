"""Canonical cryptographic helpers for Praxis authority proofs.

This module owns deterministic serialization, digest generation, and HMAC
signing helpers. It deliberately does not persist key material or create a
parallel policy store; callers bring keys from the existing credential path.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from runtime._helpers import _json_compatible


DEFAULT_DIGEST_ALGORITHM = "sha256"
DEFAULT_CANONICALIZATION_VERSION = 1


class CryptoAuthorityError(RuntimeError):
    """Raised when a cryptographic authority contract is invalid."""

    def __init__(self, reason_code: str, message: str) -> None:
        super().__init__(message)
        self.reason_code = reason_code


@dataclass(frozen=True, slots=True)
class CanonicalDigest:
    purpose: str
    value: str
    algorithm: str = DEFAULT_DIGEST_ALGORITHM
    canonicalization_version: int = DEFAULT_CANONICALIZATION_VERSION

    @property
    def prefixed(self) -> str:
        return f"{self.algorithm}:{self.value}"

    def to_metadata(self) -> dict[str, Any]:
        return {
            "purpose": self.purpose,
            "algorithm": self.algorithm,
            "canonicalization_version": self.canonicalization_version,
            "value": self.value,
        }


@dataclass(frozen=True, slots=True)
class HmacKey:
    kid: str
    secret_seed: str
    status: str = "active"

    def material(self) -> bytes:
        return _hmac_key_material(self.secret_seed)


@dataclass(frozen=True, slots=True)
class HmacKeyring:
    active_kid: str
    keys: Mapping[str, HmacKey]

    @property
    def active_key(self) -> HmacKey:
        key = self.keys.get(self.active_kid)
        if key is None:
            raise CryptoAuthorityError(
                "crypto.keyring_active_key_missing",
                f"active HMAC key is missing from keyring: {self.active_kid}",
            )
        if key.status != "active":
            raise CryptoAuthorityError(
                "crypto.keyring_active_key_inactive",
                f"active HMAC key is not marked active: {self.active_kid}",
            )
        return key

    def key_for(self, kid: str | None) -> HmacKey | None:
        if not kid:
            return self.active_key
        key = self.keys.get(kid)
        if key is None:
            return None
        if key.status not in {"active", "verify_only"}:
            return None
        return key


def canonical_json(value: object) -> str:
    """Serialize a value exactly once for authority hashing."""

    return json.dumps(
        _json_compatible(value),
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    )


def canonical_digest(
    value: object,
    *,
    purpose: str,
    algorithm: str = DEFAULT_DIGEST_ALGORITHM,
) -> CanonicalDigest:
    if algorithm != DEFAULT_DIGEST_ALGORITHM:
        raise CryptoAuthorityError(
            "crypto.digest_algorithm_unsupported",
            f"unsupported digest algorithm: {algorithm}",
        )
    purpose_text = str(purpose or "").strip()
    if not purpose_text:
        raise CryptoAuthorityError(
            "crypto.digest_purpose_missing",
            "canonical digest purpose is required",
        )
    payload = canonical_json(value).encode("utf-8")
    return CanonicalDigest(
        purpose=purpose_text,
        algorithm=algorithm,
        canonicalization_version=DEFAULT_CANONICALIZATION_VERSION,
        value=hashlib.sha256(payload).hexdigest(),
    )


def canonical_digest_hex(
    value: object,
    *,
    purpose: str,
    algorithm: str = DEFAULT_DIGEST_ALGORITHM,
) -> str:
    return canonical_digest(value, purpose=purpose, algorithm=algorithm).value


def digest_bytes_hex(
    payload: bytes,
    *,
    purpose: str,
    algorithm: str = DEFAULT_DIGEST_ALGORITHM,
) -> str:
    if algorithm != DEFAULT_DIGEST_ALGORITHM:
        raise CryptoAuthorityError(
            "crypto.digest_algorithm_unsupported",
            f"unsupported digest algorithm: {algorithm}",
        )
    purpose_text = str(purpose or "").strip()
    if not purpose_text:
        raise CryptoAuthorityError(
            "crypto.digest_purpose_missing",
            "byte digest purpose is required",
        )
    return hashlib.sha256(payload).hexdigest()


def urlsafe_b64encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).decode("ascii").rstrip("=")


def urlsafe_b64decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode((data + padding).encode("ascii"))


def hmac_sha256_b64url(payload: bytes, *, secret_seed: str) -> str:
    return urlsafe_b64encode(
        hmac.new(_hmac_key_material(secret_seed), payload, hashlib.sha256).digest()
    )


def hmac_sha256_b64url_verify(payload: bytes, signature: str, *, secret_seed: str) -> bool:
    expected = hmac_sha256_b64url(payload, secret_seed=secret_seed)
    return hmac.compare_digest(expected, str(signature or ""))


def load_hmac_keyring_from_env(
    env: Mapping[str, str],
    *,
    secret_env: str,
    key_id_env: str | None = None,
    keyring_json_env: str | None = None,
    default_kid: str,
) -> HmacKeyring:
    """Load a signing keyring without inventing a new secret store.

    Preferred shape for rotation:
      {"active_kid":"kid-2026-04","keys":{"kid-2026-04":"secret","kid-old":"old"}}
    List form is also accepted:
      {"active":"kid-2026-04","keys":[{"kid":"kid-2026-04","secret":"secret"}]}
    """

    if keyring_json_env:
        raw_keyring = str(env.get(keyring_json_env, "") or "").strip()
        if raw_keyring:
            return _parse_hmac_keyring_json(
                raw_keyring,
                source_env=keyring_json_env,
                fallback_active_kid=str(env.get(key_id_env or "", "") or "").strip(),
                default_kid=default_kid,
            )

    secret_seed = str(env.get(secret_env, "") or "").strip()
    if not secret_seed:
        raise CryptoAuthorityError(
            "crypto.hmac_secret_missing",
            f"{secret_env} is required for HMAC signing",
        )
    kid = str(env.get(key_id_env or "", "") or "").strip() or default_kid
    return HmacKeyring(
        active_kid=kid,
        keys={kid: HmacKey(kid=kid, secret_seed=secret_seed, status="active")},
    )


def _hmac_key_material(secret_seed: str) -> bytes:
    seed = str(secret_seed or "").strip()
    if not seed:
        raise CryptoAuthorityError("crypto.hmac_secret_missing", "HMAC secret is required")
    return hashlib.sha256(seed.encode("utf-8")).digest()


def _parse_hmac_keyring_json(
    raw_keyring: str,
    *,
    source_env: str,
    fallback_active_kid: str,
    default_kid: str,
) -> HmacKeyring:
    try:
        payload = json.loads(raw_keyring)
    except json.JSONDecodeError as exc:
        raise CryptoAuthorityError(
            "crypto.hmac_keyring_invalid_json",
            f"{source_env} must contain valid JSON",
        ) from exc
    if not isinstance(payload, Mapping):
        raise CryptoAuthorityError(
            "crypto.hmac_keyring_invalid",
            f"{source_env} must contain a keyring object",
        )
    active_kid = (
        str(payload.get("active_kid") or payload.get("active") or "").strip()
        or fallback_active_kid
        or default_kid
    )
    raw_keys = payload.get("keys")
    keys: dict[str, HmacKey] = {}
    if isinstance(raw_keys, Mapping):
        for raw_kid, raw_secret in raw_keys.items():
            kid = str(raw_kid or "").strip()
            secret = str(raw_secret or "").strip()
            if kid and secret:
                status = "active" if kid == active_kid else "verify_only"
                keys[kid] = HmacKey(kid=kid, secret_seed=secret, status=status)
    elif isinstance(raw_keys, list):
        for item in raw_keys:
            if not isinstance(item, Mapping):
                continue
            kid = str(item.get("kid") or "").strip()
            secret = str(item.get("secret") or item.get("secret_seed") or "").strip()
            status = str(item.get("status") or ("active" if kid == active_kid else "verify_only")).strip()
            if kid and secret:
                keys[kid] = HmacKey(kid=kid, secret_seed=secret, status=status)
    else:
        raise CryptoAuthorityError(
            "crypto.hmac_keyring_keys_missing",
            f"{source_env} must contain keys as an object or list",
        )
    if active_kid not in keys:
        raise CryptoAuthorityError(
            "crypto.hmac_keyring_active_key_missing",
            f"{source_env} active key is missing: {active_kid}",
        )
    return HmacKeyring(active_kid=active_kid, keys=keys)


__all__ = [
    "CanonicalDigest",
    "CryptoAuthorityError",
    "HmacKey",
    "HmacKeyring",
    "canonical_digest",
    "canonical_digest_hex",
    "canonical_json",
    "digest_bytes_hex",
    "hmac_sha256_b64url",
    "hmac_sha256_b64url_verify",
    "load_hmac_keyring_from_env",
    "urlsafe_b64decode",
    "urlsafe_b64encode",
]
