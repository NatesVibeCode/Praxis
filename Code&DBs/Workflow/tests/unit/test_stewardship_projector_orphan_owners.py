"""Regression pins for the orphan-owner governance cluster.

Closes BUG-EEA0502A / BUG-80C6B62F / BUG-13492A13 / BUG-48241FEA /
BUG-61D7951E / BUG-A75077EE / BUG-D577C0FA / BUG-F333D3A1 / BUG-E5E236F5 /
BUG-5535F587 / BUG-A745DE65.

Before the fix, the governance scanner flagged a cluster of
``sensitive-without-owner`` rows because several table-name prefixes and two
non-``table:`` object kinds had no rule in
``memory.data_dictionary_stewardship_projector``:

* Nine table-name prefixes (``provider_*``, ``webhook*``, ``market_*``,
  ``registry_*``, ``credential*``) fell through every regex in
  ``_NAMESPACE_OWNERS``, so the namespace projector never emitted an
  ``owner`` steward for them. The scanner saw a sensitive classification
  with no owner and raised each as its own bug.

* ``tool:praxis_provider_onboard`` and ``object_type:contact`` are outside
  the ``table:*`` namespace entirely, so even if a prefix rule existed, the
  projector's ``_known_tables`` query (``WHERE object_kind LIKE 'table:%'``)
  would have skipped them.

The fix adds:

1. Five new prefix rules in ``_NAMESPACE_OWNERS``.
2. A new ``_EXPLICIT_OWNERS`` dict with the two non-table object kinds.
3. A fourth projection step ``_project_explicit_owners`` that consults
   ``_EXPLICIT_OWNERS`` directly and writes owner rows via
   ``apply_projected_stewards`` with its own ``projector_tag``.

Pins:

* Each of the nine orphan prefixes resolves to the expected owner via
  ``_namespace_owner``.
* ``_EXPLICIT_OWNERS`` contains the two expected mappings and nothing else
  (adding more later requires a deliberate, visible change to this pin).
* ``_project_explicit_owners`` emits entries whose shape matches the
  authoritative stewardship row schema and passes its own
  ``projector_tag`` to ``apply_projected_stewards`` so
  ``replace_projected_stewards`` can prune its rows idempotently.
* ``_project_explicit_owners`` is wired into ``run()`` so the step actually
  fires under the heartbeat.
"""
from __future__ import annotations

from typing import Any

import pytest

import memory.data_dictionary_stewardship_projector as projector_mod
from memory.data_dictionary_stewardship_projector import (
    DataDictionaryStewardshipProjector,
    _EXPLICIT_OWNERS,
    _namespace_owner,
)


# -- namespace-prefix rules (closes 9 of the 11 orphan-owner bugs) --------


@pytest.mark.parametrize(
    ("table_name", "expected_owner"),
    [
        # Added in the governance cluster fix. Each example name mirrors an
        # actual orphaned data_dictionary object from the scanner report.
        ("provider_cli_profiles",     "provider_authority"),
        ("provider_execution_keys",   "provider_authority"),
        ("webhook_subscriptions",     "webhook_authority"),
        ("webhooks",                  "webhook_authority"),
        ("market_listings",           "market_authority"),
        ("market_offers",             "market_authority"),
        ("registry_snapshots",        "registry_authority"),
        ("credential_bindings",       "credential_authority"),
        ("credentials",               "credential_authority"),
    ],
)
def test_namespace_owner_resolves_new_orphan_prefixes(
    table_name: str, expected_owner: str
) -> None:
    """Each prefix that used to orphan-to-no-owner now resolves."""
    assert _namespace_owner(table_name) == expected_owner


def test_namespace_owner_unmatched_prefix_still_returns_none() -> None:
    """A wholly-unrelated name still returns ``None`` — the projector must
    not silently assign an owner to names it doesn't recognize."""
    assert _namespace_owner("completely_unrelated_table_name") is None


# -- explicit owners for non-table object kinds ---------------------------


def test_explicit_owners_maps_known_non_table_kinds() -> None:
    """Closes BUG-13492A13 and BUG-A745DE65.

    These two object kinds sit outside ``table:*`` so no namespace-prefix
    rule can reach them; ``_EXPLICIT_OWNERS`` carries the mapping directly.
    """
    assert _EXPLICIT_OWNERS["tool:praxis_provider_onboard"] == "provider_authority"
    assert _EXPLICIT_OWNERS["object_type:contact"] == "data_dictionary_authority"


def test_explicit_owners_is_exactly_the_two_known_mappings() -> None:
    """Tight pin so any future addition here is a deliberate, visible
    change — a silent new mapping would bypass the normal namespace-rule
    review path and land unreviewed."""
    assert set(_EXPLICIT_OWNERS.keys()) == {
        "tool:praxis_provider_onboard",
        "object_type:contact",
    }


# -- _project_explicit_owners behavior ------------------------------------


class _FakeConn:
    """Minimal stand-in for the sqlite/postgres connection surface the
    projector uses. Only ``execute`` is ever called and only by the two
    inventory loads, which this test doesn't exercise."""

    def execute(self, *_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        return []


def test_project_explicit_owners_emits_owner_rows(monkeypatch) -> None:
    """The method must emit one ``owner`` steward per ``_EXPLICIT_OWNERS``
    entry, with the schema the downstream apply helper expects."""
    captured: dict[str, Any] = {}

    def _fake_apply(
        _conn: Any,
        *,
        projector_tag: str,
        entries: list[dict[str, Any]],
        source: str,
    ) -> None:
        captured["projector_tag"] = projector_tag
        captured["entries"] = entries
        captured["source"] = source

    monkeypatch.setattr(projector_mod, "apply_projected_stewards", _fake_apply)

    proj = DataDictionaryStewardshipProjector(_FakeConn())
    proj._project_explicit_owners()

    # Dedicated projector_tag so replace_projected_stewards can prune this
    # step's rows without touching the other three steps.
    assert captured["projector_tag"] == "stewardship_explicit_owners"
    assert captured["source"] == "auto"

    entries = captured["entries"]
    assert len(entries) == len(_EXPLICIT_OWNERS)

    by_kind = {e["object_kind"]: e for e in entries}
    assert set(by_kind.keys()) == set(_EXPLICIT_OWNERS.keys())

    for object_kind, owner in _EXPLICIT_OWNERS.items():
        row = by_kind[object_kind]
        assert row["field_path"] == ""
        assert row["steward_kind"] == "owner"
        assert row["steward_id"] == owner
        assert row["steward_type"] == "service"
        assert 0 < row["confidence"] <= 1
        assert row["origin_ref"]["projector"] == "stewardship_explicit_owners"
        assert row["origin_ref"]["rule"] == f"explicit:{object_kind}"


def test_project_explicit_owners_is_wired_into_run(monkeypatch) -> None:
    """The explicit-owners step must actually fire under ``run()`` — a
    method that exists but is never called would leave the cluster unfixed
    at runtime."""
    called: list[str] = []

    def _fake_apply(
        _conn: Any,
        *,
        projector_tag: str,
        entries: list[dict[str, Any]],
        source: str,
    ) -> None:
        called.append(projector_tag)

    monkeypatch.setattr(projector_mod, "apply_projected_stewards", _fake_apply)

    proj = DataDictionaryStewardshipProjector(_FakeConn())
    result = proj.run()

    assert result.ok is True, result.error
    # All four projector tags fire — the new tag joins the three originals.
    assert "stewardship_explicit_owners" in called
    assert "stewardship_audit_column_publishers" in called
    assert "stewardship_namespace_owners" in called
    assert "stewardship_projector_publishers" in called
