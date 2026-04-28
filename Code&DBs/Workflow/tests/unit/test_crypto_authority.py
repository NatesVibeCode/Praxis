from __future__ import annotations

import hashlib
import json

import pytest

from runtime.crypto_authority import (
    CryptoAuthorityError,
    canonical_digest,
    canonical_digest_hex,
    hmac_sha256_b64url,
    hmac_sha256_b64url_verify,
    load_hmac_keyring_from_env,
)


def test_canonical_digest_carries_crypto_ready_identity_metadata() -> None:
    payload = {"b": 2, "a": {"z": 1}}

    digest = canonical_digest(payload, purpose="pattern.identity")

    expected = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    assert digest.value == expected
    assert digest.to_metadata() == {
        "purpose": "pattern.identity",
        "algorithm": "sha256",
        "canonicalization_version": 1,
        "value": expected,
    }
    assert digest.prefixed == f"sha256:{expected}"


def test_canonical_digest_requires_explicit_purpose() -> None:
    with pytest.raises(CryptoAuthorityError) as exc_info:
        canonical_digest_hex({"a": 1}, purpose="")

    assert exc_info.value.reason_code == "crypto.digest_purpose_missing"


def test_hmac_keyring_supports_active_and_verify_only_keys() -> None:
    env = {
        "KEYRING": json.dumps(
            {
                "active_kid": "kid-new",
                "keys": {
                    "kid-new": "new-secret",
                    "kid-old": "old-secret",
                },
            }
        )
    }

    keyring = load_hmac_keyring_from_env(
        env,
        secret_env="UNUSED",
        keyring_json_env="KEYRING",
        default_kid="fallback",
    )

    assert keyring.active_key.kid == "kid-new"
    assert keyring.key_for("kid-old").status == "verify_only"
    payload = b'{"proof":"stable"}'
    signature = hmac_sha256_b64url(payload, secret_seed="old-secret")
    assert hmac_sha256_b64url_verify(payload, signature, secret_seed="old-secret")
    assert not hmac_sha256_b64url_verify(payload, signature, secret_seed="new-secret")
