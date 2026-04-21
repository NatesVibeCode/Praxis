"""Unit tests for the data dictionary classifications projector.

The projector walks known field entries and emits tags based on name
heuristics (PII / credential / owner) and type hints (structured_shape).
These tests stub the entry inventory and assert the projector emits the
right tags per step.
"""
from __future__ import annotations

from typing import Any

from memory import data_dictionary_classifications_projector as projector
from memory.data_dictionary_classifications_projector import (
    DataDictionaryClassificationsProjector,
    _match_credential,
    _match_owner,
    _match_pii,
)


class _FakeConn:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self._rows = rows

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        if "FROM data_dictionary_effective" in sql:
            return self._rows
        return []


def _install_catcher(monkeypatch) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []

    def _apply(conn, **kw):
        calls.append(kw)
        return {
            "projector": kw.get("projector_tag"),
            "classifications_written": len(kw.get("entries", [])),
        }

    monkeypatch.setattr(projector, "apply_projected_classifications", _apply)
    return calls


# --- regex helpers --------------------------------------------------------


def test_match_pii_detects_email_variants() -> None:
    assert _match_pii("email") == "email"
    assert _match_pii("user_email") == "email"
    assert _match_pii("customer.email_address") == "email"
    # `mail` alone does not match — we want the segment to look like email.
    assert _match_pii("mailbox") is None
    # Bare text shouldn't be falsely flagged.
    assert _match_pii("status") is None


def test_match_pii_detects_ssn_and_credit_card() -> None:
    assert _match_pii("ssn") == "ssn"
    assert _match_pii("social_security_number") == "ssn"
    assert _match_pii("cc_number") == "credit_card"
    assert _match_pii("credit_card") == "credit_card"


def test_match_pii_detects_phone_and_ip_and_dob() -> None:
    assert _match_pii("phone") == "phone"
    assert _match_pii("mobile_number") == "phone"
    assert _match_pii("client_ip") == "ip_address"
    assert _match_pii("dob") == "dob"
    assert _match_pii("date_of_birth") == "dob"


def test_match_credential_detects_secret_names() -> None:
    assert _match_credential("password") is True
    assert _match_credential("api_key") is True
    assert _match_credential("access_token") is True
    assert _match_credential("client_secret") is True
    assert _match_credential("private_key") is True
    # A completely unrelated name must not match.
    assert _match_credential("status") is False


def test_match_owner_detects_identity_fks() -> None:
    assert _match_owner("user_id") is True
    assert _match_owner("owner_id") is True
    assert _match_owner("tenant_id") is True
    assert _match_owner("created_by") is True
    assert _match_owner("status") is False


# --- projector steps ------------------------------------------------------


def test_project_pii_emits_one_tag_per_matched_field(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    entries = [
        {"object_kind": "table:users", "field_path": "email", "field_kind": "text"},
        {"object_kind": "table:users", "field_path": "phone_number", "field_kind": "text"},
        # Non-matching field must not produce a tag.
        {"object_kind": "table:users", "field_path": "name", "field_kind": "text"},
    ]
    DataDictionaryClassificationsProjector(_FakeConn([]))._project_pii(entries)
    assert len(calls) == 1
    assert calls[0]["projector_tag"] == "classification_pii_name_heuristics"
    emitted = calls[0]["entries"]
    assert len(emitted) == 2
    by_value = {e["tag_value"] for e in emitted}
    assert by_value == {"email", "phone"}
    assert all(e["tag_key"] == "pii" for e in emitted)


def test_project_credentials_tags_sensitive_high(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    entries = [
        {"object_kind": "table:integrations", "field_path": "api_key", "field_kind": "text"},
        {"object_kind": "table:integrations", "field_path": "access_token", "field_kind": "text"},
        {"object_kind": "table:integrations", "field_path": "status", "field_kind": "text"},
    ]
    DataDictionaryClassificationsProjector(_FakeConn([]))._project_credentials(entries)
    assert len(calls) == 1
    assert calls[0]["projector_tag"] == "classification_credential_name_heuristics"
    emitted = calls[0]["entries"]
    assert len(emitted) == 2
    assert all(e["tag_key"] == "sensitive" and e["tag_value"] == "high" for e in emitted)


def test_project_owners_tags_identity(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    entries = [
        {"object_kind": "table:orders", "field_path": "user_id", "field_kind": "reference"},
        {"object_kind": "table:orders", "field_path": "created_by", "field_kind": "reference"},
        {"object_kind": "table:orders", "field_path": "total", "field_kind": "number"},
    ]
    DataDictionaryClassificationsProjector(_FakeConn([]))._project_owners(entries)
    assert len(calls) == 1
    emitted = calls[0]["entries"]
    assert len(emitted) == 2
    assert all(
        e["tag_key"] == "owner_domain" and e["tag_value"] == "identity"
        for e in emitted
    )


def test_project_structured_shape_tags_json_cols(monkeypatch) -> None:
    calls = _install_catcher(monkeypatch)
    entries = [
        {"object_kind": "table:bugs", "field_path": "metadata", "field_kind": "json"},
        {"object_kind": "table:bugs", "field_path": "tags", "field_kind": "array"},
        # Non-structured field kinds should not tag.
        {"object_kind": "table:bugs", "field_path": "title", "field_kind": "text"},
    ]
    DataDictionaryClassificationsProjector(_FakeConn([]))._project_structured_shape(entries)
    assert len(calls) == 1
    emitted = calls[0]["entries"]
    assert len(emitted) == 2
    by_value = {e["tag_value"] for e in emitted}
    assert by_value == {"json", "array"}


# --- run() aggregates all steps ------------------------------------------


def test_run_reports_errors_when_step_raises(monkeypatch) -> None:
    monkeypatch.setattr(
        DataDictionaryClassificationsProjector,
        "_project_owners",
        lambda self, entries: (_ for _ in ()).throw(RuntimeError("boom owners")),
    )
    monkeypatch.setattr(
        DataDictionaryClassificationsProjector,
        "_project_credentials",
        lambda self, entries: None,
    )
    monkeypatch.setattr(
        DataDictionaryClassificationsProjector,
        "_project_pii",
        lambda self, entries: None,
    )
    monkeypatch.setattr(
        DataDictionaryClassificationsProjector,
        "_project_structured_shape",
        lambda self, entries: None,
    )
    result = DataDictionaryClassificationsProjector(_FakeConn([])).run()
    assert result.ok is False
    assert "owner_name_heuristics" in (result.error or "")
