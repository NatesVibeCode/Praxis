"""Unit tests for runtime.primitive_consistency_scanner."""

from __future__ import annotations

from typing import Any

import pytest

from runtime.primitive_consistency_scanner import (
    scan_all_primitives,
    scan_primitive_consistency,
)


class _SyncConn:
    def __init__(
        self,
        *,
        operation_refs: set[str] | None = None,
        event_contract_refs: set[str] | None = None,
        authority_domains: set[str] | None = None,
        tables: set[str] | None = None,
    ) -> None:
        self._operation_refs = operation_refs or set()
        self._event_contract_refs = event_contract_refs or set()
        self._authority_domains = authority_domains or set()
        self._tables = tables or set()

    def execute(self, sql: str, *args: Any) -> list[dict[str, Any]]:
        normalized = " ".join(sql.split())
        if "FROM operation_catalog_registry" in normalized:
            return [{"?column?": 1}] if args[0] in self._operation_refs else []
        if "FROM authority_event_contracts" in normalized:
            return [{"?column?": 1}] if args[0] in self._event_contract_refs else []
        if "FROM authority_domains" in normalized:
            return [{"?column?": 1}] if args[0] in self._authority_domains else []
        if "to_regclass" in normalized:
            name = args[0]
            return [{"regclass_name": name if name in self._tables else None}]
        return []


def test_scan_emits_no_findings_when_blueprint_matches_reality() -> None:
    primitive = {
        "primitive_slug": "semantic_predicate_catalog",
        "spec": {
            "authority_module": "runtime.semantic_predicate_authority",
            "owns_table": "semantic_predicate_catalog",
            "authority_domain_ref": "authority.semantic_predicate_catalog",
            "operation_refs": ["semantic-predicate-record"],
            "event_contract_refs": ["event_contract.semantic_predicate.recorded"],
            "test_modules": ["tests.unit.test_semantic_predicate_authority"],
        },
    }
    conn = _SyncConn(
        operation_refs={"semantic-predicate-record"},
        event_contract_refs={"event_contract.semantic_predicate.recorded"},
        authority_domains={"authority.semantic_predicate_catalog"},
        tables={"semantic_predicate_catalog"},
    )
    findings = scan_primitive_consistency(conn=conn, primitive=primitive)
    assert findings == []


def test_scan_flags_missing_module() -> None:
    primitive = {
        "primitive_slug": "ghost",
        "spec": {"authority_module": "runtime.this_does_not_exist"},
    }
    findings = scan_primitive_consistency(conn=_SyncConn(), primitive=primitive)
    assert any(f["rule"] == "declared_module_does_not_resolve" for f in findings)


def test_scan_flags_missing_operation_ref() -> None:
    primitive = {
        "primitive_slug": "x",
        "spec": {"operation_refs": ["non-registered-operation"]},
    }
    findings = scan_primitive_consistency(conn=_SyncConn(), primitive=primitive)
    assert any(
        f["rule"] == "operation_ref_missing_in_catalog"
        and f["operation_ref"] == "non-registered-operation"
        for f in findings
    )


def test_scan_flags_missing_event_contract() -> None:
    primitive = {
        "primitive_slug": "x",
        "spec": {"event_contract_refs": ["event_contract.never.declared"]},
    }
    findings = scan_primitive_consistency(conn=_SyncConn(), primitive=primitive)
    assert any(
        f["rule"] == "event_contract_ref_missing"
        and f["event_contract_ref"] == "event_contract.never.declared"
        for f in findings
    )


def test_scan_flags_missing_table() -> None:
    primitive = {
        "primitive_slug": "x",
        "spec": {"owns_table": "table_that_does_not_exist"},
    }
    findings = scan_primitive_consistency(conn=_SyncConn(), primitive=primitive)
    assert any(
        f["rule"] == "owns_table_missing_in_postgres"
        and f["table"] == "table_that_does_not_exist"
        for f in findings
    )


def test_scan_flags_missing_authority_domain() -> None:
    primitive = {
        "primitive_slug": "x",
        "spec": {"authority_domain_ref": "authority.not_registered"},
    }
    findings = scan_primitive_consistency(conn=_SyncConn(), primitive=primitive)
    assert any(
        f["rule"] == "authority_domain_missing_or_disabled"
        and f["authority_domain_ref"] == "authority.not_registered"
        for f in findings
    )


def test_scan_handles_non_object_spec() -> None:
    primitive = {"primitive_slug": "weird", "spec": "string-spec"}
    findings = scan_primitive_consistency(conn=_SyncConn(), primitive=primitive)
    assert findings == [
        {
            "primitive_slug": "weird",
            "rule": "spec_must_be_object",
            "detail": "str is str",
        }
    ] or any(f["rule"] == "spec_must_be_object" for f in findings)


def test_scan_all_aggregates_count() -> None:
    primitives = [
        {"primitive_slug": "ok", "spec": {}},
        {"primitive_slug": "broken", "spec": {"authority_module": "runtime.ghost"}},
    ]
    out = scan_all_primitives(conn=_SyncConn(), primitives=primitives)
    assert out["primitive_count"] == 2
    assert out["findings_count"] >= 1
    assert any(f["primitive_slug"] == "broken" for f in out["findings"])
