"""Projects classification tags into data_dictionary_classifications.

Walks `data_dictionary_entries` (the merged field inventory across tables,
object_types, integrations, tools, datasets, ingests) and emits auto-layer
tags based on name heuristics and structural type hints:

* **PII name heuristics** (`classification_pii_name_heuristics`) —
  flag fields whose names strongly imply personally-identifiable info:
  email / phone / ssn / credit_card / ip_address.
* **Credential name heuristics** (`classification_credential_name_heuristics`) —
  flag fields that look like secrets (password, api_key, token, credential,
  *_secret) with tag_key=`sensitive`, tag_value=`high`.
* **Owner / identity heuristics** (`classification_owner_name_heuristics`) —
  flag fields like `user_id`, `owner_id`, `tenant_id` with
  tag_key=`owner_domain`, tag_value=`identity`.
* **Structured-shape hint** (`classification_structured_shape`) — for
  JSON-typed fields emit tag_key=`structured_shape`, tag_value=`json` so
  consumers know the payload is nested data rather than a scalar.

Operator tags (source=`operator`) are never touched. Each step writes with
its own projector_tag in origin_ref so `replace_projected_classifications`
prunes its own stale rows idempotently.
"""

from __future__ import annotations

import logging
import re
import time
import traceback
from typing import Any, Iterable

from runtime.data_dictionary_classifications import apply_projected_classifications
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok

logger = logging.getLogger(__name__)


# --- PII / sensitive name regexes ----------------------------------------
#
# Anchored on word boundaries so `email_id` matches but `mail` alone does
# not (we want to avoid flagging columns that merely *contain* an ambiguous
# substring — only columns whose name SEGMENT looks like the tag.)

_PII_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("email",       re.compile(r"(?:^|[_/.])(?:e[-_]?mail|email_address)(?:$|[_/.])", re.I)),
    ("phone",       re.compile(r"(?:^|[_/.])(?:phone|mobile|cell)(?:_number)?(?:$|[_/.])", re.I)),
    ("ssn",         re.compile(r"(?:^|[_/.])(?:ssn|social_security(?:_number)?)(?:$|[_/.])", re.I)),
    ("credit_card", re.compile(r"(?:^|[_/.])(?:credit_card|cc_number|card_number)(?:$|[_/.])", re.I)),
    ("ip_address",  re.compile(r"(?:^|[_/.])(?:ip_address|ipaddr|client_ip|remote_ip)(?:$|[_/.])", re.I)),
    ("postal",      re.compile(r"(?:^|[_/.])(?:zip(?:_code)?|postal_code|postcode)(?:$|[_/.])", re.I)),
    ("dob",         re.compile(r"(?:^|[_/.])(?:dob|date_of_birth|birth_date|birthdate)(?:$|[_/.])", re.I)),
]

_CREDENTIAL_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"(?:^|[_/.])(?:password|passwd|pwd)(?:$|[_/.])", re.I),
    re.compile(r"(?:^|[_/.])(?:api_key|apikey)(?:$|[_/.])", re.I),
    re.compile(r"(?:^|[_/.])(?:secret|token|access_token|refresh_token)(?:$|[_/.])", re.I),
    re.compile(r"(?:^|[_/.])(?:credential|private_key|client_secret)(?:$|[_/.])", re.I),
]

_OWNER_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"(?:^|[_/.])(?:user_id|owner_id|tenant_id|account_id|actor_id)(?:$|[_/.])", re.I),
    re.compile(r"(?:^|[_/.])(?:created_by|updated_by|deleted_by|submitted_by)(?:$|[_/.])", re.I),
]


def _match_pii(field_path: str) -> str | None:
    for tag_value, pattern in _PII_PATTERNS:
        if pattern.search(field_path):
            return tag_value
    return None


def _match_credential(field_path: str) -> bool:
    return any(p.search(field_path) for p in _CREDENTIAL_PATTERNS)


def _match_owner(field_path: str) -> bool:
    return any(p.search(field_path) for p in _OWNER_PATTERNS)


class DataDictionaryClassificationsProjector(HeartbeatModule):
    """Project classification tags from entry name/type heuristics."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "data_dictionary_classifications_projector"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        errors: list[str] = []
        entries = self._load_entries()

        # PII + credential heuristics are intentionally disabled. Praxis
        # is internal infrastructure with no real customer PII, so auto-
        # detection produced high-noise governance escalations. Operator-
        # layer tags (source='operator') still drive governance when
        # something genuinely needs the flag. Re-enable by adding the
        # step back to the list.
        enabled_steps: list[tuple[str, Any]] = [
            ("owner_name_heuristics", lambda: self._project_owners(entries)),
            ("structured_shape",      lambda: self._project_structured_shape(entries)),
        ]

        # Run disabled steps with an empty entry list so their projector-tag
        # rows get pruned via replace_projected_classifications idempotence.
        for retired_tag in (
            "classification_pii_name_heuristics",
            "classification_credential_name_heuristics",
        ):
            try:
                apply_projected_classifications(
                    self._conn,
                    projector_tag=retired_tag,
                    entries=[],
                    source="auto",
                )
            except Exception:
                pass

        for label, fn in enabled_steps:
            try:
                fn()
            except Exception:
                errors.append(f"{label}: {traceback.format_exc(limit=3)}")
                logger.exception(
                    "data dictionary classifications projector step %s failed", label
                )
        if errors:
            return _fail(self.name, t0, "; ".join(errors))
        return _ok(self.name, t0)

    # -- load inventory ----------------------------------------------------

    def _load_entries(self) -> list[dict[str, Any]]:
        """Read every known (object_kind, field_path, field_kind) triple.

        Uses the effective view so operator-layer field definitions win over
        auto — we only tag fields that the dictionary knows about.
        """
        rows = self._conn.execute(
            """
            SELECT object_kind, field_path, field_kind
              FROM data_dictionary_effective
             WHERE field_path <> ''
            """
        )
        return [dict(r) for r in rows or []]

    # -- PII name heuristics -----------------------------------------------

    def _project_pii(self, entries: Iterable[dict[str, Any]]) -> None:
        out: list[dict[str, Any]] = []
        for e in entries:
            field_path = str(e.get("field_path") or "")
            tag_value = _match_pii(field_path)
            if not tag_value:
                continue
            out.append({
                "object_kind": str(e.get("object_kind") or ""),
                "field_path": field_path,
                "tag_key": "pii",
                "tag_value": tag_value,
                "confidence": 0.9,
                "origin_ref": {
                    "projector": "classification_pii_name_heuristics",
                    "rule": f"name_match:{tag_value}",
                },
            })
        apply_projected_classifications(
            self._conn,
            projector_tag="classification_pii_name_heuristics",
            entries=out,
            source="auto",
        )

    # -- credential name heuristics ----------------------------------------

    def _project_credentials(self, entries: Iterable[dict[str, Any]]) -> None:
        out: list[dict[str, Any]] = []
        for e in entries:
            field_path = str(e.get("field_path") or "")
            if not _match_credential(field_path):
                continue
            out.append({
                "object_kind": str(e.get("object_kind") or ""),
                "field_path": field_path,
                "tag_key": "sensitive",
                "tag_value": "high",
                "confidence": 0.95,
                "origin_ref": {
                    "projector": "classification_credential_name_heuristics",
                    "rule": "credential_token_name",
                },
            })
        apply_projected_classifications(
            self._conn,
            projector_tag="classification_credential_name_heuristics",
            entries=out,
            source="auto",
        )

    # -- owner / identity heuristics ---------------------------------------

    def _project_owners(self, entries: Iterable[dict[str, Any]]) -> None:
        out: list[dict[str, Any]] = []
        for e in entries:
            field_path = str(e.get("field_path") or "")
            if not _match_owner(field_path):
                continue
            out.append({
                "object_kind": str(e.get("object_kind") or ""),
                "field_path": field_path,
                "tag_key": "owner_domain",
                "tag_value": "identity",
                "confidence": 0.8,
                "origin_ref": {
                    "projector": "classification_owner_name_heuristics",
                    "rule": "identity_fk_name",
                },
            })
        apply_projected_classifications(
            self._conn,
            projector_tag="classification_owner_name_heuristics",
            entries=out,
            source="auto",
        )

    # -- structured-shape hint ---------------------------------------------

    def _project_structured_shape(self, entries: Iterable[dict[str, Any]]) -> None:
        out: list[dict[str, Any]] = []
        for e in entries:
            field_kind = str(e.get("field_kind") or "").lower()
            if field_kind not in ("json", "object", "array"):
                continue
            out.append({
                "object_kind": str(e.get("object_kind") or ""),
                "field_path": str(e.get("field_path") or ""),
                "tag_key": "structured_shape",
                "tag_value": field_kind,
                "confidence": 1.0,
                "origin_ref": {
                    "projector": "classification_structured_shape",
                    "rule": f"field_kind:{field_kind}",
                },
            })
        apply_projected_classifications(
            self._conn,
            projector_tag="classification_structured_shape",
            entries=out,
            source="auto",
        )


__all__ = [
    "DataDictionaryClassificationsProjector",
    "_match_pii",
    "_match_credential",
    "_match_owner",
]
