"""Tests for data-dictionary-backed type_contract validation (Phase 1.3.b).

Honors architecture-policy::platform-architecture::data-dictionary-universal-
compile-time-clamp. Every type_contract slug declared on a tool must
resolve to a data_dictionary_objects row; unresolved slugs surface as
structured findings for tests / CI gates / typed-gap emission.
"""
from __future__ import annotations

from runtime.catalog_type_contract_validation import (
    _extract_slugs_from_type_contract,
    collect_catalog_type_contract_slugs,
    emit_typed_gaps_for_findings,
    validate_type_contract_slugs_against_data_dictionary,
)


class _StubConn:
    """Records the last SELECT + returns a configurable set of rows."""

    def __init__(self, existing: list[str] | None = None) -> None:
        self._existing = list(existing or [])
        self.last_sql: str | None = None
        self.last_args: tuple = ()

    def execute(self, sql: str, *args):
        self.last_sql = sql
        self.last_args = args
        # Emulate: return only rows whose object_kind is both requested AND
        # in the configured existing set.
        requested = set(args)
        return [
            {"object_kind": slug}
            for slug in self._existing
            if slug in requested
        ]


# ---------------------------------------------------------------------------
# _extract_slugs_from_type_contract
# ---------------------------------------------------------------------------


def test_extract_slugs_flattens_consumes_and_produces_across_actions():
    contract = {
        "list": {"consumes": [], "produces": ["praxis.bug.record_list"]},
        "file": {
            "consumes": ["praxis.bug.observation"],
            "produces": ["praxis.bug.record"],
        },
    }
    slugs = _extract_slugs_from_type_contract(contract)
    assert slugs == {
        "praxis.bug.record_list",
        "praxis.bug.observation",
        "praxis.bug.record",
    }


def test_extract_slugs_ignores_blank_and_non_dict_entries():
    contract = {
        "default": {"consumes": ["", "  ", "praxis.x"], "produces": []},
        "bad_entry": "not a dict",
    }
    slugs = _extract_slugs_from_type_contract(contract)  # type: ignore[arg-type]
    assert slugs == {"praxis.x"}


def test_extract_slugs_handles_empty_contract():
    assert _extract_slugs_from_type_contract({}) == set()
    assert _extract_slugs_from_type_contract(None) == set()  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# collect_catalog_type_contract_slugs (integration with real catalog)
# ---------------------------------------------------------------------------


def test_collect_includes_praxis_bugs_slugs():
    """The real catalog has praxis_bugs with type_contract; confirm slugs surface."""
    by_tool = collect_catalog_type_contract_slugs()
    assert "praxis_bugs" in by_tool
    assert "praxis.bug.record" in by_tool["praxis_bugs"]
    assert "praxis.bug.record_list" in by_tool["praxis_bugs"]
    assert "praxis.bug.observation" in by_tool["praxis_bugs"]


def test_collect_skips_tools_without_type_contract():
    """Tools that don't declare a type_contract don't appear in the output."""
    by_tool = collect_catalog_type_contract_slugs()
    # All entries must have non-empty slug sets.
    for tool_name, slugs in by_tool.items():
        assert slugs, f"tool {tool_name!r} has empty slug set (should be skipped)"


# ---------------------------------------------------------------------------
# validate_type_contract_slugs_against_data_dictionary
# ---------------------------------------------------------------------------


def test_validator_returns_empty_when_all_slugs_exist():
    """When every declared slug has a DD row, findings are empty."""
    # Build a stub conn where EVERY catalog slug is "in" data_dictionary_objects.
    by_tool = collect_catalog_type_contract_slugs()
    all_slugs: list[str] = []
    for slugs in by_tool.values():
        all_slugs.extend(slugs)
    conn = _StubConn(existing=all_slugs)
    findings = validate_type_contract_slugs_against_data_dictionary(conn)
    assert findings == []


def test_validator_surfaces_findings_for_missing_slugs():
    """When NO slugs exist in DD (empty stub), every declared slug surfaces."""
    conn = _StubConn(existing=[])
    findings = validate_type_contract_slugs_against_data_dictionary(conn)
    by_tool = collect_catalog_type_contract_slugs()
    total_slugs = sum(len(slugs) for slugs in by_tool.values())
    assert len(findings) == total_slugs
    # Every finding has the expected shape.
    for f in findings:
        assert set(f.keys()) == {
            "tool",
            "slug",
            "missing_type",
            "reason_code",
            "legal_repair_actions",
        }
        assert f["missing_type"] == "data_dictionary_object"
        assert f["reason_code"] == "data_dictionary.object_kind.missing"


def test_validator_partial_coverage_surfaces_only_missing():
    """Mix: some slugs exist, some don't — only missing surface."""
    # Mark praxis.bug.record as existing, everything else missing.
    conn = _StubConn(existing=["praxis.bug.record"])
    findings = validate_type_contract_slugs_against_data_dictionary(conn)
    finding_slugs = {f["slug"] for f in findings}
    assert "praxis.bug.record" not in finding_slugs
    # There should be other bug slugs still missing (record_list, observation, etc.)
    assert any("praxis.bug." in s for s in finding_slugs)


def test_validator_findings_sorted_deterministic():
    """Findings are deterministic — sorted by (tool, slug) for CI stability."""
    conn = _StubConn(existing=[])
    findings_1 = validate_type_contract_slugs_against_data_dictionary(conn)
    findings_2 = validate_type_contract_slugs_against_data_dictionary(conn)
    # Same order on both runs (no randomness from dict/set iteration).
    pairs_1 = [(f["tool"], f["slug"]) for f in findings_1]
    pairs_2 = [(f["tool"], f["slug"]) for f in findings_2]
    assert pairs_1 == pairs_2


def test_validator_handles_empty_catalog_type_contracts(monkeypatch):
    """When no tool declares a type_contract, validator returns [] without
    ever querying the DB."""
    from runtime import catalog_type_contract_validation as mod

    monkeypatch.setattr(mod, "collect_catalog_type_contract_slugs", lambda: {})

    conn = _StubConn(existing=[])
    findings = mod.validate_type_contract_slugs_against_data_dictionary(conn)
    assert findings == []
    # Confirm no SQL was issued — short-circuit when nothing to check.
    assert conn.last_sql is None


def test_validator_uses_single_select_for_all_slugs():
    """One SELECT covers every slug across every tool — no per-tool queries."""
    conn = _StubConn(existing=[])
    validate_type_contract_slugs_against_data_dictionary(conn)
    # Only one execute() call should have happened; _StubConn records the last.
    assert conn.last_sql is not None
    assert "SELECT object_kind FROM data_dictionary_objects" in conn.last_sql


def test_validator_findings_have_legal_repair_actions_as_list():
    """legal_repair_actions is a list (consistent with Unresolved* errors),
    not a single string. Consumers iterate unconditionally."""
    conn = _StubConn(existing=[])
    findings = validate_type_contract_slugs_against_data_dictionary(conn)
    for f in findings:
        assert isinstance(f["legal_repair_actions"], list)
        assert "add_data_dictionary_objects_row" in f["legal_repair_actions"]


# ---------------------------------------------------------------------------
# emit_typed_gaps_for_findings (Phase 1.6 emission wiring)
# ---------------------------------------------------------------------------


class _EventCaptureConn:
    """Captures all INSERTs for emit_typed_gap verification.

    ``events`` filters to system_events writes only — kept stable for
    legacy count-based assertions after typed_gap_events landed dual-write
    to authority_events as part of the Phase 2 CQRS migration.
    """

    def __init__(self) -> None:
        self.all_writes: list[tuple[str, tuple]] = []

    def execute(self, sql: str, *args):
        self.all_writes.append((sql, args))
        return []

    @property
    def events(self) -> list[tuple[str, tuple]]:
        return [
            (sql, args)
            for sql, args in self.all_writes
            if "INSERT INTO system_events" in sql
        ]


def test_emit_typed_gaps_for_findings_returns_emitted_count():
    conn = _EventCaptureConn()
    findings = [
        {
            "tool": "praxis_bugs",
            "slug": "praxis.bug.record",
            "missing_type": "data_dictionary_object",
            "reason_code": "data_dictionary.object_kind.missing",
            "legal_repair_actions": ["add_data_dictionary_objects_row"],
        },
        {
            "tool": "praxis_bugs",
            "slug": "praxis.bug.other",
            "missing_type": "data_dictionary_object",
            "reason_code": "data_dictionary.object_kind.missing",
            "legal_repair_actions": ["add_data_dictionary_objects_row"],
        },
    ]
    emitted = emit_typed_gaps_for_findings(conn, findings)
    assert emitted == 2
    assert len(conn.events) == 2


def test_emit_typed_gaps_for_findings_empty_list_emits_zero():
    conn = _EventCaptureConn()
    emitted = emit_typed_gaps_for_findings(conn, [])
    assert emitted == 0
    assert conn.events == []


def test_emit_typed_gaps_payload_carries_slug_and_tool():
    import json

    conn = _EventCaptureConn()
    finding = {
        "tool": "praxis_bugs",
        "slug": "praxis.bug.observation",
        "missing_type": "data_dictionary_object",
        "reason_code": "data_dictionary.object_kind.missing",
        "legal_repair_actions": ["add_data_dictionary_objects_row"],
    }
    emit_typed_gaps_for_findings(conn, [finding])
    assert len(conn.events) == 1
    sql, args = conn.events[0]
    assert args[0] == "typed_gap.created"
    payload = json.loads(args[3])
    assert payload["gap_kind"] == "type_contract_slug"
    assert payload["missing_type"] == "data_dictionary_object"
    assert payload["source_ref"] == "tool:praxis_bugs"
    assert payload["context"] == {"slug": "praxis.bug.observation"}
    assert payload["legal_repair_actions"] == ["add_data_dictionary_objects_row"]


def test_emit_typed_gaps_accepts_string_legal_repair_actions_for_backcompat():
    """If a finding slipped through with legal_repair_actions as a single
    string (legacy shape), the emitter still wraps it correctly."""
    import json

    conn = _EventCaptureConn()
    finding = {
        "tool": "praxis_x",
        "slug": "praxis.x.y",
        "missing_type": "data_dictionary_object",
        "reason_code": "r",
        "legal_repair_actions": "a_single_action",  # legacy string shape
    }
    emit_typed_gaps_for_findings(conn, [finding])
    payload = json.loads(conn.events[0][1][3])
    assert payload["legal_repair_actions"] == ["a_single_action"]


def test_emit_typed_gaps_skips_non_dict_findings():
    """Malformed findings (not dicts) are skipped silently, no crash."""
    conn = _EventCaptureConn()
    emitted = emit_typed_gaps_for_findings(
        conn,
        [
            None,  # type: ignore[list-item]
            "not a dict",  # type: ignore[list-item]
            {
                "tool": "ok",
                "slug": "s",
                "missing_type": "m",
                "reason_code": "r",
                "legal_repair_actions": ["a"],
            },
        ],
    )
    assert emitted == 1
