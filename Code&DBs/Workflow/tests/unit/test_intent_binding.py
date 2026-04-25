from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import pytest

from runtime import intent_binding
from runtime.intent_binding import (
    AmbiguousCandidate,
    BoundIntent,
    BoundPill,
    ProposedPillScaffold,
    SuggestedPillCandidate,
    UnboundCandidate,
    bind_data_pills,
    commit_proposed_pill,
)


class _StubConn:
    """Minimal conn stand-in that answers describe_object via monkeypatch."""


def _install_dictionary(monkeypatch, catalog: dict[str, list[dict[str, object]] | None]) -> None:
    """Wire a fake data-dictionary authority keyed by object_kind."""

    def _fake_list_object_kinds(conn, **_kwargs):
        return [
            {
                "object_kind": object_kind,
                "label": object_kind.replace("_", " ").title(),
                "category": "workflow",
                "summary": f"{object_kind.replace('_', ' ')} authority object",
            }
            for object_kind in catalog
        ]

    def _fake_describe_object(conn, *, object_kind, **_kwargs):
        if object_kind not in catalog:
            raise RuntimeError(f"unknown object_kind {object_kind!r}")
        fields = catalog[object_kind]
        return {
            "object": {"object_kind": object_kind},
            "fields": list(fields or []),
            "entries_by_source": {},
        }

    monkeypatch.setattr(intent_binding, "list_object_kinds", _fake_list_object_kinds)
    monkeypatch.setattr(intent_binding, "describe_object", _fake_describe_object)


def test_bind_data_pills_returns_bound_pill_for_exact_match(monkeypatch) -> None:
    _install_dictionary(
        monkeypatch,
        {
            "users": [
                {
                    "field_path": "first_name",
                    "field_kind": "text",
                    "source": "auto",
                    "display_order": 3,
                }
            ],
        },
    )

    result = bind_data_pills(
        "Update users.first_name to match the caller's preferred display name.",
        conn=_StubConn(),
    )

    assert isinstance(result, BoundIntent)
    assert len(result.bound) == 1
    pill = result.bound[0]
    assert pill.matched_span == "users.first_name"
    assert pill.object_kind == "users"
    assert pill.field_path == "first_name"
    assert pill.field_kind == "text"
    assert pill.display_order == 3
    assert pill.source == "auto"
    assert result.ambiguous == []
    assert result.unbound == []


def test_bind_data_pills_binds_namespaced_object_kind(monkeypatch) -> None:
    _install_dictionary(
        monkeypatch,
        {
            "tool:praxis_connector": [
                {
                    "field_path": "app_name",
                    "field_kind": "text",
                    "source": "auto",
                    "display_order": 20,
                }
            ],
        },
    )

    result = bind_data_pills(
        "Confirm tool:praxis_connector.app_name before building the integration.",
        conn=_StubConn(),
    )

    assert len(result.bound) == 1
    assert result.bound[0].object_kind == "tool:praxis_connector"
    assert result.bound[0].field_path == "app_name"


def test_bind_data_pills_marks_unknown_object_kind_as_unbound(monkeypatch) -> None:
    _install_dictionary(monkeypatch, {})  # no known objects

    result = bind_data_pills(
        "Compare workflow_runs.status against the queue.", conn=_StubConn()
    )

    assert result.bound == []
    assert len(result.unbound) == 1
    unbound = result.unbound[0]
    assert isinstance(unbound, UnboundCandidate)
    assert unbound.object_kind == "workflow_runs"
    assert unbound.field_path == "status"
    assert unbound.reason == "object_kind_not_found"


def test_bind_data_pills_marks_missing_field_as_unbound(monkeypatch) -> None:
    _install_dictionary(
        monkeypatch,
        {
            "users": [
                {
                    "field_path": "email",
                    "field_kind": "text",
                    "source": "auto",
                }
            ],
        },
    )

    result = bind_data_pills(
        "Read users.favorite_color from the dictionary.", conn=_StubConn()
    )

    assert result.bound == []
    assert len(result.unbound) == 1
    assert result.unbound[0].reason == "field_path_not_in_object"
    assert result.unbound[0].object_kind == "users"
    assert result.unbound[0].field_path == "favorite_color"
    # Field-missing-on-known-object gets a scaffold — caller can fill + commit
    # to create the field instead of treating it as a typo.
    scaffold = result.unbound[0].proposed_pill
    assert isinstance(scaffold, ProposedPillScaffold)
    assert scaffold.object_kind == "users"
    assert scaffold.field_path == "favorite_color"
    assert "description" in scaffold.required_to_fill
    assert "field_kind" in scaffold.required_to_fill  # no hint from the name


def test_unknown_object_kind_does_not_produce_scaffold(monkeypatch) -> None:
    _install_dictionary(monkeypatch, {})

    result = bind_data_pills("Inspect aliens.planet for science.", conn=_StubConn())

    assert len(result.unbound) == 1
    assert result.unbound[0].reason == "object_kind_not_found"
    # Can't propose a new field on an unknown object — scaffold stays None.
    assert result.unbound[0].proposed_pill is None


def test_allowlist_rejection_does_not_produce_scaffold(monkeypatch) -> None:
    _install_dictionary(
        monkeypatch,
        {"orders": [{"field_path": "total_cents", "field_kind": "number", "source": "auto"}]},
    )

    result = bind_data_pills(
        "Inspect orders.total_cents for audit.",
        conn=_StubConn(),
        object_kinds=["users"],
    )

    assert len(result.unbound) == 1
    assert result.unbound[0].reason == "object_kind_not_allowlisted"
    # Allowlist rejection means caller explicitly scoped binding — don't
    # offer to mutate authority outside that scope.
    assert result.unbound[0].proposed_pill is None


def test_scaffold_infers_field_kind_hint_from_name(monkeypatch) -> None:
    _install_dictionary(
        monkeypatch,
        {"users": [{"field_path": "email", "field_kind": "text", "source": "auto"}]},
    )

    cases = {
        "users.created_at": "datetime",
        "users.is_admin": "boolean",
        "users.login_count": "number",
        "users.preferences_json": "json",
        "users.friend_ids": "array",
        "users.primary_org_id": "reference",
        "users.signup_on": "date",
    }
    for intent, expected_hint in cases.items():
        result = bind_data_pills(f"Read {intent} from the dictionary.", conn=_StubConn())
        assert len(result.unbound) == 1, f"expected one unbound for {intent}"
        scaffold = result.unbound[0].proposed_pill
        assert scaffold is not None, f"expected scaffold for {intent}"
        assert scaffold.field_kind_hint == expected_hint, (
            f"expected {expected_hint} hint for {intent}, got {scaffold.field_kind_hint}"
        )
        # If we inferred a hint, field_kind is not required from the caller.
        assert "field_kind" not in scaffold.required_to_fill


def test_commit_proposed_pill_writes_through_set_operator_override(monkeypatch) -> None:
    scaffold = ProposedPillScaffold(
        object_kind="users",
        field_path="preferred_timezone",
        field_kind_hint=None,
        required_to_fill=["description", "field_kind"],
        rationale="test",
    )

    captured: dict[str, object] = {}

    def _fake_set_override(conn, **kwargs):
        captured["conn"] = conn
        captured["kwargs"] = kwargs
        return {
            "object_kind": kwargs["object_kind"],
            "field_path": kwargs["field_path"],
            "entry": {"field_kind": kwargs["field_kind"]},
        }

    monkeypatch.setattr(intent_binding, "set_operator_override", _fake_set_override)

    receipt = commit_proposed_pill(
        scaffold,
        conn=_StubConn(),
        description="User's preferred IANA timezone, e.g. America/Los_Angeles",
        field_kind="text",
    )

    assert receipt["object_kind"] == "users"
    assert receipt["field_path"] == "preferred_timezone"
    kwargs = captured["kwargs"]
    assert kwargs["field_kind"] == "text"
    assert kwargs["description"].startswith("User's preferred IANA")


def test_commit_proposed_pill_requires_description(monkeypatch) -> None:
    scaffold = ProposedPillScaffold(
        object_kind="users",
        field_path="created_at",
        field_kind_hint="datetime",
        required_to_fill=["description"],
        rationale="test",
    )

    with pytest.raises(ValueError, match="description is required"):
        commit_proposed_pill(scaffold, conn=_StubConn(), description="   ")


def test_commit_proposed_pill_requires_field_kind_when_no_hint(monkeypatch) -> None:
    scaffold = ProposedPillScaffold(
        object_kind="users",
        field_path="mystery_field",
        field_kind_hint=None,
        required_to_fill=["description", "field_kind"],
        rationale="test",
    )

    with pytest.raises(ValueError, match="field_kind is required"):
        commit_proposed_pill(scaffold, conn=_StubConn(), description="some desc")


def test_commit_proposed_pill_uses_hint_when_field_kind_missing(monkeypatch) -> None:
    scaffold = ProposedPillScaffold(
        object_kind="users",
        field_path="last_login_at",
        field_kind_hint="datetime",
        required_to_fill=["description"],
        rationale="test",
    )

    captured: dict[str, object] = {}

    def _fake_set_override(conn, **kwargs):
        captured["kwargs"] = kwargs
        return {"object_kind": kwargs["object_kind"], "field_path": kwargs["field_path"], "entry": {}}

    monkeypatch.setattr(intent_binding, "set_operator_override", _fake_set_override)

    commit_proposed_pill(scaffold, conn=_StubConn(), description="Last login timestamp")
    assert captured["kwargs"]["field_kind"] == "datetime"


def test_bind_data_pills_flags_ambiguous_duplicates(monkeypatch) -> None:
    # Two dictionary rows for the same field_path — ambiguous.
    _install_dictionary(
        monkeypatch,
        {
            "users": [
                {"field_path": "status", "field_kind": "text", "source": "auto"},
                {"field_path": "status", "field_kind": "enum", "source": "operator_override"},
            ],
        },
    )

    result = bind_data_pills("Inspect users.status before action.", conn=_StubConn())

    assert result.bound == []
    assert result.unbound == []
    assert len(result.ambiguous) == 1
    ambiguous = result.ambiguous[0]
    assert isinstance(ambiguous, AmbiguousCandidate)
    assert ambiguous.matched_span == "users.status"
    assert len(ambiguous.candidates) == 2
    sources = {c["source"] for c in ambiguous.candidates}
    assert sources == {"auto", "operator_override"}


def test_bind_data_pills_deduplicates_repeated_refs(monkeypatch) -> None:
    _install_dictionary(
        monkeypatch,
        {"users": [{"field_path": "email", "field_kind": "text", "source": "auto"}]},
    )

    result = bind_data_pills(
        "Copy users.email into profile. Never log users.email in plaintext.",
        conn=_StubConn(),
    )

    assert len(result.bound) == 1
    assert result.bound[0].field_path == "email"


def test_bind_data_pills_respects_allowlist(monkeypatch) -> None:
    _install_dictionary(
        monkeypatch,
        {
            "users": [{"field_path": "email", "field_kind": "text", "source": "auto"}],
            "orders": [{"field_path": "total_cents", "field_kind": "integer", "source": "auto"}],
        },
    )

    result = bind_data_pills(
        "Look at users.email and orders.total_cents for the audit.",
        conn=_StubConn(),
        object_kinds=["users"],
    )

    assert len(result.bound) == 1
    assert result.bound[0].object_kind == "users"
    assert len(result.unbound) == 1
    assert result.unbound[0].object_kind == "orders"
    assert result.unbound[0].reason == "object_kind_not_allowlisted"


def test_bind_data_pills_warns_when_no_refs_present(monkeypatch) -> None:
    _install_dictionary(monkeypatch, {"users": []})

    result = bind_data_pills(
        "Please update the user's name to something nice.", conn=_StubConn()
    )

    assert result.bound == []
    assert result.unbound == []
    assert result.ambiguous == []
    assert any("no object.field references" in w for w in result.warnings)


def test_bind_data_pills_suggests_candidates_from_loose_prose(monkeypatch) -> None:
    _install_dictionary(
        monkeypatch,
        {
            "integrations": [
                {
                    "field_path": "app_name",
                    "field_kind": "text",
                    "label": "App name",
                    "description": "Name of the application or product being integrated.",
                    "source": "auto",
                    "display_order": 1,
                },
                {
                    "field_path": "app_domain",
                    "field_kind": "text",
                    "label": "App domain",
                    "description": "Primary web domain for the app or service.",
                    "source": "auto",
                    "display_order": 2,
                },
                {
                    "field_path": "auth_type",
                    "field_kind": "text",
                    "label": "Authentication type",
                    "description": "OAuth, API key, bearer token, or other credential style.",
                    "source": "auto",
                },
            ],
            "workflow_steps": [
                {
                    "field_path": "step_name",
                    "field_kind": "text",
                    "label": "Workflow step name",
                    "description": "Plan, search, retrieve, evaluate, or build stage.",
                    "source": "auto",
                }
            ],
        },
    )

    result = bind_data_pills(
        (
            "A repeatable workflow where we feed in an app name or app domain, "
            "search and retrieve docs, evaluate auth, then build a custom integration."
        ),
        conn=_StubConn(),
    )

    assert result.bound == []
    assert result.unbound == []
    assert result.ambiguous == []
    assert any("no object.field references" in w for w in result.warnings)
    assert all(isinstance(candidate, SuggestedPillCandidate) for candidate in result.suggested)
    refs = {candidate.ref for candidate in result.suggested}
    assert "integrations.app_name" in refs
    assert "integrations.app_domain" in refs
    assert "integrations.auth_type" in refs
    assert any(candidate.confidence in {"medium", "high"} for candidate in result.suggested)


def test_bind_data_pills_can_disable_suggestions(monkeypatch) -> None:
    _install_dictionary(
        monkeypatch,
        {
            "integrations": [
                {
                    "field_path": "app_domain",
                    "field_kind": "text",
                    "label": "App domain",
                    "description": "Primary domain for an application integration.",
                    "source": "auto",
                }
            ]
        },
    )

    result = bind_data_pills(
        "Use the app domain to build an integration.",
        conn=_StubConn(),
        suggest=False,
    )

    assert result.suggested == []
    assert any("no object.field references" in w for w in result.warnings)


def test_bind_data_pills_rejects_empty_intent(monkeypatch) -> None:
    _install_dictionary(monkeypatch, {})

    result = bind_data_pills("   ", conn=_StubConn())
    assert result.intent == ""
    assert "intent is empty" in result.warnings


def test_bind_data_pills_ignores_version_strings(monkeypatch) -> None:
    # 1.0.2 looks like ``a.b.c`` but shouldn't bind as a reference.
    _install_dictionary(monkeypatch, {})

    result = bind_data_pills(
        "Bump tooling to 1.0.2 and verify behavior.", conn=_StubConn()
    )

    assert result.bound == []
    assert result.ambiguous == []
    assert result.unbound == []


def test_bound_intent_to_dict_round_trips() -> None:
    bound_intent = BoundIntent(
        intent="users.email",
        bound=[
            BoundPill(
                matched_span="users.email",
                object_kind="users",
                field_path="email",
                field_kind="text",
                source="auto",
                display_order=1,
            )
        ],
    )
    payload = bound_intent.to_dict()
    assert payload["suggested"] == []
    assert payload["bound"][0]["object_kind"] == "users"
    assert payload["bound"][0]["field_path"] == "email"
