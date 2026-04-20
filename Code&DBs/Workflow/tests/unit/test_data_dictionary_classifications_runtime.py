"""Unit tests for `runtime.data_dictionary_classifications`.

Exercises the thin orchestration layer: input validation, entry
normalization, and read paths. Storage is stubbed so these tests run
without a live Postgres.
"""
from __future__ import annotations

from typing import Any

import pytest

from runtime import data_dictionary_classifications as runtime
from runtime.data_dictionary_classifications import (
    DataDictionaryClassificationError,
    apply_projected_classifications,
    classification_summary,
    clear_operator_classification,
    describe_classifications,
    find_by_tag,
    set_operator_classification,
)


# --- fakes ----------------------------------------------------------------


class _FakeStore:
    def __init__(self, known_objects: set[str] | None = None) -> None:
        self.known = known_objects or set()
        self.upserted: list[dict[str, Any]] = []
        self.replaced: list[dict[str, Any]] = []
        self.deleted: list[dict[str, Any]] = []
        self.list_for: list[dict[str, Any]] = []
        self.list_by: list[dict[str, Any]] = []
        self.layers: list[dict[str, Any]] = []
        self.counts: dict[str, int] = {}

    def install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            runtime, "get_object",
            lambda conn, *, object_kind: (
                {"object_kind": object_kind} if object_kind in self.known else None
            ),
        )
        monkeypatch.setattr(
            runtime, "upsert_classification",
            lambda conn, **kw: (self.upserted.append(kw) or {**kw}),
        )
        monkeypatch.setattr(
            runtime, "replace_projected_classifications",
            lambda conn, **kw: (self.replaced.append(kw) or len(kw["entries"])),
        )
        monkeypatch.setattr(
            runtime, "delete_classification",
            lambda conn, **kw: (self.deleted.append(kw) or True),
        )
        monkeypatch.setattr(
            runtime, "list_classifications_for",
            lambda conn, *, object_kind, field_path=None: self.list_for,
        )
        monkeypatch.setattr(
            runtime, "list_by_tag",
            lambda conn, *, tag_key, tag_value=None: self.list_by,
        )
        monkeypatch.setattr(
            runtime, "list_classification_layers",
            lambda conn, *, object_kind, field_path=None: self.layers,
        )
        monkeypatch.setattr(
            runtime, "count_classifications_by_source",
            lambda conn: self.counts,
        )


# --- apply_projected_classifications -------------------------------------


def test_apply_projected_classifications_requires_tag(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryClassificationError):
        apply_projected_classifications(conn=None, projector_tag="  ", entries=[])


def test_apply_projected_classifications_refuses_operator_source(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryClassificationError):
        apply_projected_classifications(
            conn=None, projector_tag="t", entries=[], source="operator",
        )


def test_apply_projected_classifications_requires_object_and_key(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryClassificationError):
        apply_projected_classifications(
            conn=None, projector_tag="t",
            entries=[{"object_kind": "", "tag_key": "pii"}],
        )


def test_apply_projected_classifications_defaults_origin_projector(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    apply_projected_classifications(
        conn=None,
        projector_tag="pii_heuristics",
        entries=[{
            "object_kind": "table:users",
            "field_path": "email",
            "tag_key": "pii",
            "tag_value": "email",
        }],
    )
    stored = store.replaced[0]["entries"][0]
    assert stored["origin_ref"]["projector"] == "pii_heuristics"
    assert stored["tag_key"] == "pii"
    assert stored["tag_value"] == "email"


# --- set_operator_classification / clear_operator_classification ---------


def test_set_operator_classification_requires_known_object(monkeypatch) -> None:
    store = _FakeStore()  # no objects known
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryClassificationError) as exc_info:
        set_operator_classification(
            conn=None, object_kind="table:missing", tag_key="pii",
        )
    assert exc_info.value.status_code == 404


def test_set_operator_classification_writes_operator_source(monkeypatch) -> None:
    store = _FakeStore(known_objects={"table:users"})
    store.install(monkeypatch)
    set_operator_classification(
        conn=None,
        object_kind="table:users",
        field_path="email",
        tag_key="pii",
        tag_value="email",
    )
    assert store.upserted[0]["source"] == "operator"
    assert store.upserted[0]["origin_ref"] == {"source": "operator"}
    assert store.upserted[0]["tag_key"] == "pii"


def test_set_operator_classification_requires_tag_key(monkeypatch) -> None:
    store = _FakeStore(known_objects={"table:a"})
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryClassificationError):
        set_operator_classification(
            conn=None, object_kind="table:a", tag_key="",
        )


def test_clear_operator_classification_forwards_keys(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    clear_operator_classification(
        conn=None, object_kind="table:a", tag_key="pii", field_path="email",
    )
    deleted = store.deleted[0]
    assert deleted["object_kind"] == "table:a"
    assert deleted["tag_key"] == "pii"
    assert deleted["field_path"] == "email"
    assert deleted["source"] == "operator"


# --- describe_classifications --------------------------------------------


def test_describe_classifications_includes_layers_when_requested(monkeypatch) -> None:
    store = _FakeStore()
    store.list_for = [{"tag_key": "pii", "tag_value": "email"}]
    store.layers = [{"tag_key": "pii", "source": "auto"}]
    store.install(monkeypatch)
    payload = describe_classifications(
        conn=None, object_kind="table:users", include_layers=True,
    )
    assert payload["effective"] == store.list_for
    assert payload["layers"] == store.layers


def test_describe_classifications_omits_layers_by_default(monkeypatch) -> None:
    store = _FakeStore()
    store.list_for = [{"tag_key": "pii"}]
    store.install(monkeypatch)
    payload = describe_classifications(conn=None, object_kind="table:users")
    assert "layers" not in payload


def test_describe_classifications_requires_object_kind(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryClassificationError):
        describe_classifications(conn=None, object_kind="   ")


# --- find_by_tag ---------------------------------------------------------


def test_find_by_tag_returns_matches(monkeypatch) -> None:
    store = _FakeStore()
    store.list_by = [
        {"object_kind": "table:users", "field_path": "email", "tag_value": "email"},
    ]
    store.install(monkeypatch)
    payload = find_by_tag(conn=None, tag_key="pii", tag_value="email")
    assert payload["tag_key"] == "pii"
    assert payload["tag_value"] == "email"
    assert payload["matches"] == store.list_by


def test_find_by_tag_requires_tag_key(monkeypatch) -> None:
    store = _FakeStore()
    store.install(monkeypatch)
    with pytest.raises(DataDictionaryClassificationError):
        find_by_tag(conn=None, tag_key="")


# --- classification_summary ----------------------------------------------


def test_classification_summary_returns_counts(monkeypatch) -> None:
    store = _FakeStore()
    store.counts = {"auto": 42, "operator": 3}
    store.install(monkeypatch)
    payload = classification_summary(conn=None)
    assert payload == {"classifications_by_source": {"auto": 42, "operator": 3}}
