"""Anti-bypass tests for the runtime failure-identity consumer boundary.

Consumers of failure-identity payloads in the ``runtime/`` and
``surfaces/`` trees must route through the failure-identity authority:

  * ``runtime.bug_evidence.build_failure_signature`` — authoritative
    seven-field failure identity + fingerprint helper.
  * ``runtime.bug_tracker.build_failure_signature`` — public re-export
    that delegates to the ``bug_evidence`` authority.
  * ``runtime.primitive_contracts.build_failure_identity_contract`` /
    ``failure_identity_fields`` — projects the identity field list
    from the authority for /orient consumers.

Concretely:

  * Only the allowlisted authority modules may import
    ``runtime.bug_tagging.stable_fingerprint``. That is the hash
    primitive used inside ``build_failure_signature``; routing a raw
    identity payload through it outside the authority would fork the
    fingerprint from the contract.
  * The contract's ``identity_fields`` and the public
    ``failure_identity_fields()`` helper must match the parameter list
    of ``build_failure_signature`` so cold-start consumers reading the
    contract see the same shape the authority produces.

This test walks ``runtime/**/*.py`` and ``surfaces/**/*.py`` and fails
when a non-authority module imports the fingerprint primitive. It also
pins the contract-to-authority alignment.

It enforces:
``decision.2026-04-19.runtime-failure-identity-consumer-boundary``.
"""

from __future__ import annotations

import inspect
import re
from pathlib import Path

import pytest


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]


# Modules that ARE the failure-identity authority. ``bug_evidence``
# owns the identity payload and the only sanctioned call into
# ``stable_fingerprint`` for identity shapes. ``bug_tracker`` is the
# public re-export surface and pulls ``build_failure_signature`` from
# bug_evidence. ``primitive_contracts`` projects the contract shape.
_FAILURE_IDENTITY_AUTHORITY_ALLOWLIST: frozenset[str] = frozenset(
    {
        "runtime/bug_evidence.py",
        "runtime/bug_tracker.py",
        "runtime/primitive_contracts.py",
    }
)


# Import of ``stable_fingerprint`` from ``runtime.bug_tagging``. The
# hash primitive is authority-only for identity use; the allowlist
# above enumerates the modules permitted to invoke it. Docstring
# references to the name are unaffected because this pattern requires
# an ``import`` keyword.
_STABLE_FINGERPRINT_IMPORT_PATTERN: re.Pattern[str] = re.compile(
    r"""from\s+runtime\.bug_tagging\s+import\s+[^\n]*\bstable_fingerprint\b"""
)


def _relative_python_files(root: Path) -> list[Path]:
    return sorted(p for p in root.rglob("*.py") if p.is_file())


def _as_relative_key(path: Path) -> str:
    return path.relative_to(_WORKFLOW_ROOT).as_posix()


@pytest.mark.parametrize(
    "scanned_root",
    [
        _WORKFLOW_ROOT / "runtime",
        _WORKFLOW_ROOT / "surfaces",
    ],
    ids=["runtime", "surfaces"],
)
def test_no_stable_fingerprint_imports_outside_authority(scanned_root: Path) -> None:
    """No module outside the failure-identity authority may import the fingerprint primitive."""

    offenders: list[str] = []
    for path in _relative_python_files(scanned_root):
        key = _as_relative_key(path)
        if key in _FAILURE_IDENTITY_AUTHORITY_ALLOWLIST:
            continue
        source = path.read_text(encoding="utf-8")
        for match in _STABLE_FINGERPRINT_IMPORT_PATTERN.finditer(source):
            offenders.append(f"{key}: -> {match.group(0)!r}")
    assert not offenders, (
        "Raw runtime.bug_tagging.stable_fingerprint imports detected — "
        "route identity payloads through "
        "runtime.bug_tracker.build_failure_signature (which delegates "
        "to runtime.bug_evidence.build_failure_signature):\n  - "
        + "\n  - ".join(offenders)
    )


def test_failure_identity_authority_allowlist_paths_exist() -> None:
    """Fail loudly if an allowlist entry drifts out of sync with the filesystem."""
    for relative in _FAILURE_IDENTITY_AUTHORITY_ALLOWLIST:
        path = _WORKFLOW_ROOT / relative
        assert path.is_file(), (
            f"allowlisted failure-identity authority path does not exist: {relative}"
        )


def test_failure_identity_contract_fields_match_signature() -> None:
    """Contract identity_fields must match build_failure_signature parameters."""
    from runtime.bug_evidence import build_failure_signature
    from runtime.primitive_contracts import (
        build_failure_identity_contract,
        failure_identity_fields,
    )

    contract = build_failure_identity_contract()
    contract_fields = tuple(contract["identity_fields"])

    helper_fields = failure_identity_fields()
    assert contract_fields == helper_fields

    signature = inspect.signature(build_failure_signature)
    # Drop ``source_kind`` because it is an optional provenance tag the
    # contract does not expose to consumers as an identity field.
    signature_params = tuple(
        name for name in signature.parameters if name != "source_kind"
    )
    assert contract_fields == signature_params


def test_bug_tracker_re_export_delegates_to_bug_evidence() -> None:
    """The public re-export must delegate to the authority helper."""
    from runtime import bug_evidence
    from runtime.bug_tracker import build_failure_signature as tracker_signature

    source = inspect.getsource(tracker_signature)
    assert "_bug_evidence.build_failure_signature" in source, (
        "runtime.bug_tracker.build_failure_signature must delegate to "
        "runtime.bug_evidence.build_failure_signature, not recompute "
        "the identity locally."
    )

    identity = tracker_signature(
        failure_code="example.test.failure",
        job_label="unit-test",
    )
    assert "fingerprint" in identity
    assert identity["failure_code"] == "example.test.failure"
    # Round-trip: same inputs produce the same fingerprint.
    identity_again = bug_evidence.build_failure_signature(
        failure_code="example.test.failure",
        job_label="unit-test",
    )
    assert identity == identity_again


def test_failure_identity_contract_declares_authority() -> None:
    """The contract must point consumers at the authority helper by name."""
    from runtime.primitive_contracts import build_failure_identity_contract

    contract = build_failure_identity_contract()
    assert contract["authority"] == "runtime.bug_evidence.build_failure_signature"
    assert contract["fingerprint_field"] == "fingerprint"
