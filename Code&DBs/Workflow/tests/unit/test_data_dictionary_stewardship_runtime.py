"""Unit tests for `runtime.data_dictionary_stewardship`.

Exercises the orchestration layer: input validation, entry normalization,
and read paths. Storage is stubbed so these tests run without a live
Postgres.
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime import data_dictionary_stewardship as runtime
from runtime.data_dictionary_stewardship import (
    DataDictionaryStewardshipError,
    apply_projected_stewards,
    clear_operator_steward,
    describe_stewards,
    find_by_steward,
    set_operator_steward,
    stewardship_summary,
)


class _FakeStore:
    def __init__(self, known_objects: set[str] | None = None) -> None:
        self.known = known_objects or set()
        self.upserted: list[dict[str, Any]] = []
        self.replaced: list[dict[str, Any]] = []
        self.deleted: list[dict[str, Any]] = []
        self.list_for: list[dict[str, Any]] = []
        self.owned: list[dict[str, Any]] = []
        self.layers: list[dict[str, Any]] = []
        self.counts_source: dict[str, int] = {}
        self.counts_kind: dict[str, int] = {}

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            runtime, "get_object",
            lambda conn, *, object_kind: (
                {"object_kind": object_kind} if object_kind in self.known else None
            ),
        )
        monkeypatch.setattr(
            runtime, "upsert_steward",
            lambda conn, **kw: (self.upserted.append(kw) or {**kw}),
        )
        monkeypatch.setattr(
            runtime, "replace_projected_stewards",
            lambda conn, **kw: (self.replaced.append(kw) or len(kw["entries"])),
        )
        monkeypatch.setattr(
            runtime, "delete_steward",
            lambda conn, **kw: (self.deleted.append(kw) or True),
        )
        monkeypatch.setattr(
            runtime, "list_stewards_for",
            lambda conn, *, object_kind, field_path=None: self.list_for,
        )
        monkeypatch.setattr(
            runtime, "list_assets_owned_by",
            lambda conn, *, steward_id, steward_kind=None: self.owned,
        )
        monkeypatch.setattr(
            runtime, "list_steward_layers",
            lambda conn, *, object_kind, field_path=None: self.layers,
        )
        monkeypatch.setattr(
            runtime, "count_stewards_by_source", lambda conn: self.counts_source,
        )
        monkeypatch.setattr(
            runtime, "count_stewards_by_kind", lambda conn: self.counts_kind,
        )


# --- apply_projected_stewards --------------------------------------------


def test_apply_projected_stewards_requires_tag(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryStewardshipError):
        apply_projected_stewards(conn=None, projector_tag="  ", entries=[])


def test_apply_projected_stewards_refuses_operator_source(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryStewardshipError):
        apply_projected_stewards(
            conn=None, projector_tag="t", entries=[], source="operator",
        )


def test_apply_projected_stewards_requires_kind_and_id(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryStewardshipError):
        apply_projected_stewards(
            conn=None, projector_tag="t",
            entries=[{
                "object_kind": "table:x", "steward_kind": "owner", "steward_id": "",
            }],
        )


def test_apply_projected_stewards_defaults_origin_projector(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    apply_projected_stewards(
        conn=None,
        projector_tag="namespace_owners",
        entries=[{
            "object_kind": "table:bugs",
            "steward_kind": "owner",
            "steward_id": "bug_authority",
            "steward_type": "service",
        }],
    )
    stored = store.replaced[0]["entries"][0]
    assert stored["origin_ref"]["projector"] == "namespace_owners"
    assert stored["steward_kind"] == "owner"
    assert stored["steward_id"] == "bug_authority"
    assert stored["steward_type"] == "service"


def test_apply_projected_stewards_defaults_steward_type_to_person(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    apply_projected_stewards(
        conn=None,
        projector_tag="t",
        entries=[{
            "object_kind": "table:a",
            "steward_kind": "owner",
            "steward_id": "alice@company.com",
        }],
    )
    stored = store.replaced[0]["entries"][0]
    assert stored["steward_type"] == "person"


# --- set_operator_steward / clear_operator_steward ----------------------


def test_set_operator_steward_requires_known_object(monkeypatch) -> None:
    store = _FakeStore()  # no objects known
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryStewardshipError) as exc_info:
        set_operator_steward(
            conn=None,
            object_kind="table:missing",
            steward_kind="owner",
            steward_id="alice",
        )
    assert exc_info.value.status_code == 404


def test_set_operator_steward_requires_kind_and_id(monkeypatch) -> None:
    store = _FakeStore(known_objects={"table:a"})
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryStewardshipError):
        set_operator_steward(
            conn=None, object_kind="table:a", steward_kind="", steward_id="",
        )


def test_set_operator_steward_writes_operator_source(monkeypatch) -> None:
    store = _FakeStore(known_objects={"table:a"})
    store.install(monkeypatch)
    set_operator_steward(
        conn=None,
        object_kind="table:a",
        steward_kind="owner",
        steward_id="alice@company.com",
        steward_type="person",
    )
    assert store.upserted[0]["source"] == "operator"
    assert store.upserted[0]["origin_ref"] == {"source": "operator"}
    assert store.upserted[0]["steward_id"] == "alice@company.com"


def test_clear_operator_steward_forwards_keys(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    clear_operator_steward(
        conn=None,
        object_kind="table:a",
        steward_kind="owner",
        steward_id="alice",
        field_path="",
    )
    deleted = store.deleted[0]
    assert deleted["object_kind"] == "table:a"
    assert deleted["steward_kind"] == "owner"
    assert deleted["steward_id"] == "alice"
    assert deleted["source"] == "operator"


# --- describe_stewards --------------------------------------------------


def test_describe_stewards_includes_layers_when_requested(monkeypatch) -> None:
    store = _FakeStore()
    store.list_for = [{"steward_kind": "owner", "steward_id": "alice"}]
    store.layers = [{"steward_kind": "owner", "source": "auto"}]
    store.install(monkeypatch)
    payload = describe_stewards(
        conn=None, object_kind="table:a", include_layers=True,
    )
    assert payload["effective"] == store.list_for
    assert payload["layers"] == store.layers


def test_describe_stewards_omits_layers_by_default(monkeypatch) -> None:
    store = _FakeStore()
    store.list_for = [{"steward_kind": "owner"}]
    store.install(monkeypatch)
    payload = describe_stewards(conn=None, object_kind="table:a")
    assert "layers" not in payload


def test_describe_stewards_requires_object_kind(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryStewardshipError):
        describe_stewards(conn=None, object_kind="   ")


# --- find_by_steward ----------------------------------------------------


def test_find_by_steward_returns_matches(monkeypatch) -> None:
    store = _FakeStore()
    store.owned = [
        {"object_kind": "table:a", "steward_kind": "owner"},
    ]
    store.install(monkeypatch)
    payload = find_by_steward(
        conn=None, steward_id="alice", steward_kind="owner",
    )
    assert payload["steward_id"] == "alice"
    assert payload["steward_kind"] == "owner"
    assert payload["matches"] == store.owned


def test_find_by_steward_requires_steward_id(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryStewardshipError):
        find_by_steward(conn=None, steward_id="")


# --- stewardship_summary ------------------------------------------------


def test_stewardship_summary_combines_counts(monkeypatch) -> None:
    store = _FakeStore()
    store.counts_source = {"auto": 30, "operator": 2}
    store.counts_kind = {"owner": 12, "publisher": 14, "contact": 6}
    store.install(monkeypatch)
    payload = stewardship_summary(conn=None)
    assert payload == {
        "stewards_by_source": {"auto": 30, "operator": 2},
        "stewards_by_kind": {"owner": 12, "publisher": 14, "contact": 6},
    }
