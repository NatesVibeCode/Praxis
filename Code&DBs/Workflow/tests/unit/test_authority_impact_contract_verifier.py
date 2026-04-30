"""Pass 8 tests: authority impact contract verifier (defense-in-depth)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from runtime.verifier_builtins import builtin_verify_authority_impact_contract


_AUTHORITY_BEARING = ["Code&DBs/Databases/migrations/workflow/342_foo.sql"]
_NON_AUTHORITY = ["docs/notes.md", "README.md"]


@dataclass
class _FakeConn:
    candidate_row: dict[str, Any] | None = None
    declared_count: int = 0
    preflight_row: dict[str, Any] | None = None
    queries: list[tuple[str, tuple[Any, ...]]] = field(default_factory=list)

    def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        normalized = " ".join(query.split())
        self.queries.append((normalized, args))
        if "FROM code_change_candidate_payloads" in normalized:
            return self.candidate_row
        if "FROM candidate_authority_impacts" in normalized and "COUNT" in normalized:
            return {"declared_count": self.declared_count}
        if "FROM candidate_latest_preflight" in normalized:
            return self.preflight_row
        raise AssertionError(f"unexpected query: {normalized}")


def _connection_fn(_conn: Any) -> _FakeConn:
    assert _conn is not None
    return _conn


def _candidate(intended_files: list[str], base_head_ref: str = "abc123") -> dict[str, Any]:
    return {
        "candidate_id": "11111111-1111-1111-1111-111111111111",
        "base_head_ref": base_head_ref,
        "intended_files": intended_files,
    }


def _preflight(
    *,
    status: str = "passed",
    base_head_ref: str = "abc123",
    contract_complete: bool = True,
    contested: int = 0,
    additions: int = 0,
    temp_passed: bool = True,
) -> dict[str, Any]:
    return {
        "preflight_id": "22222222-2222-2222-2222-222222222222",
        "preflight_status": status,
        "base_head_ref_at_preflight": base_head_ref,
        "impact_contract_complete": contract_complete,
        "contested_impact_count": contested,
        "temp_verifier_passed": temp_passed,
        "runtime_addition_impact_count": additions,
    }


def test_returns_error_when_candidate_id_missing() -> None:
    status, outputs = builtin_verify_authority_impact_contract(
        inputs={}, conn=_FakeConn(), connection_fn=_connection_fn
    )
    assert status == "error"
    assert outputs["reason_code"] == "verifier.authority_impact_contract.candidate_id_required"


def test_failed_when_candidate_not_found() -> None:
    conn = _FakeConn(candidate_row=None)
    status, outputs = builtin_verify_authority_impact_contract(
        inputs={"candidate_id": "11111111-1111-1111-1111-111111111111"},
        conn=conn,
        connection_fn=_connection_fn,
    )
    assert status == "failed"
    assert outputs["reason_code"] == "verifier.authority_impact_contract.candidate_not_found"


def test_passes_when_candidate_not_authority_bearing() -> None:
    conn = _FakeConn(candidate_row=_candidate(_NON_AUTHORITY))
    status, outputs = builtin_verify_authority_impact_contract(
        inputs={"candidate_id": "11111111-1111-1111-1111-111111111111"},
        conn=conn,
        connection_fn=_connection_fn,
    )
    assert status == "passed"
    assert outputs["reason_code"] == "verifier.authority_impact_contract.not_authority_bearing"
    assert outputs["intended_files"] == _NON_AUTHORITY


def test_failed_when_authority_bearing_with_no_declared_impacts() -> None:
    conn = _FakeConn(
        candidate_row=_candidate(_AUTHORITY_BEARING),
        declared_count=0,
    )
    status, outputs = builtin_verify_authority_impact_contract(
        inputs={"candidate_id": "11111111-1111-1111-1111-111111111111"},
        conn=conn,
        connection_fn=_connection_fn,
    )
    assert status == "failed"
    assert outputs["reason_code"] == "verifier.authority_impact_contract.declared_impacts_missing"
    assert outputs["declared_impact_count"] == 0


def test_failed_when_no_preflight_record_present() -> None:
    conn = _FakeConn(
        candidate_row=_candidate(_AUTHORITY_BEARING),
        declared_count=2,
        preflight_row=None,
    )
    status, outputs = builtin_verify_authority_impact_contract(
        inputs={"candidate_id": "11111111-1111-1111-1111-111111111111"},
        conn=conn,
        connection_fn=_connection_fn,
    )
    assert status == "failed"
    assert outputs["reason_code"] == "verifier.authority_impact_contract.preflight_required"
    assert outputs["declared_impact_count"] == 2


def test_failed_when_preflight_base_does_not_match_candidate() -> None:
    conn = _FakeConn(
        candidate_row=_candidate(_AUTHORITY_BEARING, base_head_ref="abc123"),
        declared_count=1,
        preflight_row=_preflight(base_head_ref="def456"),
    )
    status, outputs = builtin_verify_authority_impact_contract(
        inputs={"candidate_id": "11111111-1111-1111-1111-111111111111"},
        conn=conn,
        connection_fn=_connection_fn,
    )
    assert status == "failed"
    assert outputs["reason_code"] == "verifier.authority_impact_contract.preflight_stale"
    assert outputs["candidate_base_head_ref"] == "abc123"
    assert outputs["preflight_base_head_ref"] == "def456"


def test_failed_when_preflight_status_not_passed() -> None:
    conn = _FakeConn(
        candidate_row=_candidate(_AUTHORITY_BEARING),
        declared_count=1,
        preflight_row=_preflight(status="failed_temp_verifier"),
    )
    status, outputs = builtin_verify_authority_impact_contract(
        inputs={"candidate_id": "11111111-1111-1111-1111-111111111111"},
        conn=conn,
        connection_fn=_connection_fn,
    )
    assert status == "failed"
    assert outputs["reason_code"] == "verifier.authority_impact_contract.preflight_not_passed"
    assert outputs["preflight_status"] == "failed_temp_verifier"


def test_failed_when_contract_marked_incomplete() -> None:
    conn = _FakeConn(
        candidate_row=_candidate(_AUTHORITY_BEARING),
        declared_count=1,
        preflight_row=_preflight(contract_complete=False),
    )
    status, outputs = builtin_verify_authority_impact_contract(
        inputs={"candidate_id": "11111111-1111-1111-1111-111111111111"},
        conn=conn,
        connection_fn=_connection_fn,
    )
    assert status == "failed"
    assert outputs["reason_code"] == "verifier.authority_impact_contract.contract_incomplete"


def test_failed_when_contested_impacts_present() -> None:
    conn = _FakeConn(
        candidate_row=_candidate(_AUTHORITY_BEARING),
        declared_count=2,
        preflight_row=_preflight(contested=3),
    )
    status, outputs = builtin_verify_authority_impact_contract(
        inputs={"candidate_id": "11111111-1111-1111-1111-111111111111"},
        conn=conn,
        connection_fn=_connection_fn,
    )
    assert status == "failed"
    assert outputs["reason_code"] == "verifier.authority_impact_contract.contested_impacts_present"
    assert outputs["contested_impact_count"] == 3


def test_passed_when_all_gates_green() -> None:
    conn = _FakeConn(
        candidate_row=_candidate(_AUTHORITY_BEARING),
        declared_count=2,
        preflight_row=_preflight(additions=1),
    )
    status, outputs = builtin_verify_authority_impact_contract(
        inputs={"candidate_id": "11111111-1111-1111-1111-111111111111"},
        conn=conn,
        connection_fn=_connection_fn,
    )
    assert status == "passed"
    assert outputs["reason_code"] == "verifier.authority_impact_contract.green"
    assert outputs["declared_impact_count"] == 2
    assert outputs["runtime_addition_impact_count"] == 1
    assert outputs["temp_verifier_passed"] is True
