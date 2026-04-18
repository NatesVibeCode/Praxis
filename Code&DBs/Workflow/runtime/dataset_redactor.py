"""Pure-function redaction classifier for dataset candidates.

The candidate subscriber calls :func:`classify_redaction` once per
candidate at ingest time. The result lands in
``dataset_raw_candidates.redaction_status`` and gates promotion:

- ``clean`` — no sensitive markers detected.
- ``unverified`` — payload empty or unscannable; default starting state.
- ``redaction_required`` — markers present that an operator could
  accept after manual redaction (e.g. PII patterns).
- ``sensitive_blocked`` — hard blockers (env-secret references,
  Authorization headers, raw API keys). The DB write path refuses to
  promote any row in this state.

This is a deliberately small, allow-listed scan. It is not a substitute
for proper data-loss prevention; it is a forcing function so that
sensitive content cannot quietly enter a curated dataset.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any


REDACTION_CLEAN = "clean"
REDACTION_UNVERIFIED = "unverified"
REDACTION_REQUIRED = "redaction_required"
REDACTION_SENSITIVE_BLOCKED = "sensitive_blocked"


# Hard blockers — match anywhere in stringified payload.
_BLOCKER_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("api_key_anthropic", re.compile(r"sk-ant-[A-Za-z0-9_\-]{20,}")),
    ("api_key_openai", re.compile(r"sk-(?:proj-)?[A-Za-z0-9]{20,}")),
    ("api_key_google", re.compile(r"AIza[0-9A-Za-z_\-]{20,}")),
    ("authorization_header", re.compile(r"(?i)\bauthorization\s*[:=]\s*['\"]?bearer\s+[A-Za-z0-9._\-]+")),
    ("aws_access_key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("private_key_pem", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("env_secret_value", re.compile(r"(?i)\b(?:secret|password|token|api_key)\s*=\s*['\"]?[^\s'\"]{8,}")),
)

# Soft markers — operator-redactable.
_SOFT_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("email_address", re.compile(r"\b[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b")),
    ("phone_number", re.compile(r"\b(?:\+?\d{1,3}[\s\-]?)?\(?\d{3}\)?[\s\-]?\d{3}[\s\-]?\d{4}\b")),
    ("ssn_us", re.compile(r"\b\d{3}-\d{2}-\d{4}\b")),
    ("absolute_home_path", re.compile(r"/Users/[A-Za-z0-9._\-]+/")),
)


@dataclass(frozen=True, slots=True)
class RedactionVerdict:
    """Outcome of a redaction scan."""

    status: str
    blockers: tuple[str, ...] = ()
    soft_markers: tuple[str, ...] = ()

    def to_summary(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "blockers": list(self.blockers),
            "soft_markers": list(self.soft_markers),
        }


def _coerce_to_text(payload: Any) -> str:
    if payload is None:
        return ""
    if isinstance(payload, str):
        return payload
    try:
        return json.dumps(payload, default=str, ensure_ascii=False)
    except (TypeError, ValueError):
        return str(payload)


def classify_redaction(*payloads: Any) -> RedactionVerdict:
    """Scan one or more payloads (input, output, parsed_output) jointly.

    Returns the combined verdict. ``unverified`` is returned when every
    payload is empty/unscannable; this allows callers to distinguish
    "nothing to scan" from "scanned and clean".
    """

    parts = [_coerce_to_text(p) for p in payloads]
    haystack = "\n".join(p for p in parts if p)
    if not haystack.strip():
        return RedactionVerdict(status=REDACTION_UNVERIFIED)

    blockers = tuple(
        name for name, pattern in _BLOCKER_PATTERNS if pattern.search(haystack)
    )
    if blockers:
        return RedactionVerdict(status=REDACTION_SENSITIVE_BLOCKED, blockers=blockers)

    soft = tuple(
        name for name, pattern in _SOFT_PATTERNS if pattern.search(haystack)
    )
    if soft:
        return RedactionVerdict(status=REDACTION_REQUIRED, soft_markers=soft)

    return RedactionVerdict(status=REDACTION_CLEAN)


def is_promotable_redaction(status: str) -> bool:
    """Promotion-path predicate. Only ``clean`` is auto-promotable; an
    operator can promote ``redaction_required`` after manual review,
    but ``sensitive_blocked`` and ``unverified`` are never promoted.
    """

    return status == REDACTION_CLEAN


__all__ = [
    "REDACTION_CLEAN",
    "REDACTION_REQUIRED",
    "REDACTION_SENSITIVE_BLOCKED",
    "REDACTION_UNVERIFIED",
    "RedactionVerdict",
    "classify_redaction",
    "is_promotable_redaction",
]
