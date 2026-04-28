from __future__ import annotations

from runtime.friction_ledger import FrictionLedger, FrictionType
from runtime.platform_patterns import (
    PATTERN_HYDRATION_ALGORITHM,
    PATTERN_HYDRATION_CANONICALIZATION,
    PATTERN_HYDRATION_CONTRACT,
    PATTERN_HYDRATION_PURPOSE,
    PATTERN_IDENTITY_ALGORITHM,
    PATTERN_IDENTITY_CANONICALIZATION,
    PATTERN_IDENTITY_PURPOSE,
    PlatformPatternAuthority,
    pattern_hydration_digest,
    pattern_identity_digest,
    pattern_ref_for_key,
)


def test_pattern_identity_is_purpose_bound() -> None:
    digest = pattern_identity_digest("failure_code:provider.capacity")

    assert len(digest) == 64
    assert pattern_ref_for_key("failure_code:provider.capacity").startswith("PATTERN-")
    assert PATTERN_IDENTITY_PURPOSE == "platform_pattern.identity"
    assert PATTERN_IDENTITY_ALGORITHM == "sha256"
    assert PATTERN_IDENTITY_CANONICALIZATION == "platform_pattern_identity_v1"


def test_materialize_friction_pattern_records_authority_and_evidence() -> None:
    from _pg_test_conn import get_isolated_conn

    conn = get_isolated_conn()
    try:
        ledger = FrictionLedger(conn)
        message = (
            '{"event":"cli_command_failure","fingerprint":"pattern-test-abc",'
            '"reason_code":"cli.unsupported_arguments",'
            '"command":"workflow status --since-hours 24000",'
            '"output":"workflow status does not support arguments"}'
        )
        ledger.record(FrictionType.HARD_FAILURE, "cli.workflow", "workflow status", message)
        ledger.record(FrictionType.HARD_FAILURE, "cli.workflow", "workflow status", message)

        authority = PlatformPatternAuthority(conn)
        candidates = authority.candidate_bundle(
            sources=["friction"],
            threshold=2,
        )

        assert candidates["count"] == 1
        candidate = candidates["candidates"][0]
        assert candidate["pattern_key"] == "friction:pattern-test-abc"
        assert candidate["promotion_candidate"] is True
        assert candidate["identity_digest_purpose"] == PATTERN_IDENTITY_PURPOSE

        materialized = authority.materialize_candidates(
            sources=["friction"],
            threshold=2,
            created_by="test_platform_patterns",
        )

        assert materialized["materialized_count"] == 1
        pattern = materialized["patterns"][0]
        assert pattern["pattern_ref"] == candidate["pattern_ref"]
        assert pattern["status"] == "confirmed"
        assert pattern["identity_digest"] == candidate["identity_digest"]
        assert pattern["identity_digest_algorithm"] == "sha256"
        assert pattern["evidence_count"] == 2

        evidence = authority.list_evidence(pattern["pattern_ref"])
        assert len(evidence) == 2
        assert {item["evidence_kind"] for item in evidence} == {"friction_event"}
        assert {item["evidence_role"] for item in evidence} == {"observed_in"}
    finally:
        conn.close()


def test_candidate_hydration_connects_retrieval_semantics_and_primitives() -> None:
    from _pg_test_conn import get_isolated_conn

    conn = get_isolated_conn()
    try:
        ledger = FrictionLedger(conn)
        message = (
            '{"event":"cli_command_failure","fingerprint":"pattern-hydrate-abc",'
            '"reason_code":"cli.unsupported_arguments",'
            '"command":"workflow status --since-hours 24000",'
            '"output":"workflow status does not support arguments"}'
        )
        ledger.record(FrictionType.HARD_FAILURE, "cli.workflow", "workflow status", message)
        ledger.record(FrictionType.HARD_FAILURE, "cli.workflow", "workflow status", message)

        authority = PlatformPatternAuthority(conn)
        bundle = authority.candidate_bundle(
            sources=["friction"],
            threshold=2,
            include_hydration=True,
        )

        assert bundle["view"] == "pattern_candidates_hydrated"
        candidate = bundle["candidates"][0]
        hydration = candidate["hydration"]
        assert hydration["hydration_digest"] == pattern_hydration_digest(candidate)
        assert hydration["hydration_digest_purpose"] == PATTERN_HYDRATION_PURPOSE
        assert hydration["hydration_digest_algorithm"] == PATTERN_HYDRATION_ALGORITHM
        assert (
            hydration["hydration_digest_canonicalization"]
            == PATTERN_HYDRATION_CANONICALIZATION
        )
        assert hydration["manifest_contract"] == PATTERN_HYDRATION_CONTRACT
        assert hydration["retrieval_plane"]["authority"] == "candidate_evidence_only"
        assert "fts" in hydration["retrieval_plane"]["compatible_indexes"]
        assert {
            item["predicate_slug"] for item in hydration["semantic_binding_suggestions"]
        } >= {
            "pattern_has_kind",
            "pattern_owner_surface",
            "pattern_evidenced_by",
        }
        assert {
            item["id"] for item in hydration["primitive_hydration"]["legal_actions"]
        } == {"inspect_evidence", "inspect_semantic_bindings"}
        mutating = {
            item["id"]: item
            for item in hydration["primitive_hydration"]["blocked_or_mutating_actions"]
        }
        assert mutating["materialize_pattern"]["legal_status"] == "requires_mutating_scope"
        assert mutating["record_semantic_assertions"]["legal_status"] == (
            "blocked_until_operator_confirms_predicates"
        )
        assert "pattern.semantic_bindings_unmaterialized" in {
            gap["gap_type"] for gap in hydration["typed_gaps"]
        }
    finally:
        conn.close()


def test_candidate_hydration_blocks_materialization_below_threshold() -> None:
    from _pg_test_conn import get_isolated_conn

    conn = get_isolated_conn()
    try:
        ledger = FrictionLedger(conn)
        message = (
            '{"event":"cli_command_failure","fingerprint":"pattern-hydrate-threshold",'
            '"reason_code":"cli.missing_required_scope",'
            '"command":"workflow run",'
            '"output":"workflow run requires explicit scope"}'
        )
        ledger.record(FrictionType.HARD_FAILURE, "cli.workflow", "workflow run", message)
        ledger.record(FrictionType.HARD_FAILURE, "cli.workflow", "workflow run", message)

        authority = PlatformPatternAuthority(conn)
        bundle = authority.candidate_bundle(
            sources=["friction"],
            threshold=3,
            include_hydration=True,
        )

        candidate = bundle["candidates"][0]
        assert candidate["promotion_candidate"] is False
        hydration = candidate["hydration"]
        assert "pattern.evidence_threshold_unmet" in {
            gap["gap_type"] for gap in hydration["typed_gaps"]
        }
        mutating = {
            item["id"]: item
            for item in hydration["primitive_hydration"]["blocked_or_mutating_actions"]
        }
        assert mutating["materialize_pattern"]["legal_status"] == "blocked"
        assert mutating["materialize_pattern"]["blocked_by"] == [
            "pattern.evidence_threshold_unmet"
        ]
    finally:
        conn.close()
