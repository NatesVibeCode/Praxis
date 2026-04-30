from __future__ import annotations

from runtime.operator_surfaces.client_operating_model import (
    build_cartridge_status_view,
    build_identity_authority_view,
    build_managed_runtime_accounting_summary,
    build_next_safe_actions_view,
    build_object_truth_view,
    build_sandbox_drift_view,
    build_simulation_timeline_view,
    build_system_census_view,
    build_verifier_results_view,
    validate_workflow_builder_graph,
)


GENERATED_AT = "2026-04-30T12:00:00Z"
STALE_SOURCE = "2026-04-30T11:00:00Z"
FRESH_SOURCE = "2026-04-30T11:59:00Z"


def test_empty_census_preserves_explicit_empty_state() -> None:
    view = build_system_census_view(
        system_records=[],
        generated_at=GENERATED_AT,
        permission_scope={"scope_ref": "tenant.acme", "visibility": "full"},
        evidence_refs=["fixture.empty_census"],
    )

    assert view["state"] == "empty"
    assert view["payload"]["counts"]["systems"] == 0
    assert view["payload"]["systems"] == []
    assert view["generated_at"] == GENERATED_AT
    assert view["permission_scope"]["scope_ref"] == "tenant.acme"
    assert view["evidence_refs"] == ["fixture.empty_census"]


def test_permission_limited_object_truth_redacts_field_without_losing_conflict_shape() -> None:
    view = build_object_truth_view(
        object_ref="object.account.1",
        canonical_summary={"object_ref": "object.account.1", "display_name": "Acme"},
        fields=[
            {
                "field_name": "tax_id",
                "value": "12-3456789",
                "authority": "irs_registry",
                "provenance": [{"source_ref": "crm.account.1"}],
            }
        ],
        generated_at=GENERATED_AT,
        permission_scope={
            "scope_ref": "tenant.acme.support",
            "visibility": "limited",
            "redacted_fields": ["tax_id"],
        },
        evidence_refs=["object_truth.snapshot.1"],
    )

    field = view["payload"]["fields"][0]
    assert view["state"] == "partial"
    assert field["state"] == "not_authorized"
    assert field["canonical_value"] is None
    assert field["provenance"] == []
    assert view["permission_scope"]["visibility"] == "limited"


def test_identity_conflict_and_missing_authority_are_machine_readable() -> None:
    view = build_identity_authority_view(
        object_ref="object.person.1",
        identity={"canonical_id": "person.canonical.1", "cluster_state": "review-required"},
        source_authority=[
            {
                "field_group": "legal_name",
                "winner": None,
                "ranking": ["hris", "crm"],
                "evidence_refs": ["authority.legal_name.1"],
            }
        ],
        conflicts=[{"reason_code": "identity.source_disagreement", "sources": ["hris", "crm"]}],
        generated_at=GENERATED_AT,
    )

    assert view["state"] == "conflict"
    assert view["payload"]["missing_authority"] is True
    assert view["payload"]["source_authority"][0]["state"] == "missing"
    assert {item["reason_code"] for item in view["payload"]["conflicts"]} >= {
        "identity.source_disagreement",
        "identity.review-required",
    }


def test_timeline_orders_reverse_chronological_and_filters_event_type() -> None:
    view = build_simulation_timeline_view(
        subject_ref="workflow.1",
        events=[
            {"event_id": "event.old", "event_type": "simulation.checkpoint", "occurred_at": "2026-04-30T10:00:00Z"},
            {"event_id": "event.ignore", "event_type": "sandbox.drift", "occurred_at": "2026-04-30T11:00:00Z"},
            {"event_id": "event.new", "event_type": "simulation.checkpoint", "occurred_at": "2026-04-30T12:00:00Z"},
        ],
        filters={"event_type": "simulation.checkpoint"},
        generated_at=GENERATED_AT,
    )

    assert [event["event_id"] for event in view["payload"]["events"]] == ["event.new", "event.old"]
    assert view["payload"]["filters"] == {"event_type": "simulation.checkpoint"}


def test_verifier_results_separate_blocking_from_advisory() -> None:
    view = build_verifier_results_view(
        subject_ref="workflow.1",
        snapshot_ref="snapshot.1",
        verifier_results=[
            {
                "verifier_id": "required.contract",
                "verifier_kind": "contract",
                "status": "failed",
                "severity": "error",
                "findings": [{"reason_code": "CONTRACT_MISMATCH"}],
            },
            {
                "verifier_id": "advisory.naming",
                "verifier_kind": "lint",
                "status": "failed",
                "severity": "warning",
            },
        ],
        generated_at=GENERATED_AT,
    )

    assert view["state"] == "blocked"
    assert [item["verifier_id"] for item in view["payload"]["blocking_findings"]] == ["required.contract"]
    assert [item["verifier_id"] for item in view["payload"]["advisory_findings"]] == ["advisory.naming"]


def test_drift_view_derives_blocking_severity_and_action() -> None:
    view = build_sandbox_drift_view(
        sandbox_ref="sandbox.alpha",
        comparison_report={
            "report_id": "comparison.1",
            "status": "drift",
            "evidence_package_id": "sandbox.readback.1",
            "rows": [
                {
                    "row_id": "row.config",
                    "dimension": "config",
                    "status": "drift",
                    "prediction": {"flag": True},
                    "actual": {"flag": False},
                }
            ],
        },
        drift_ledger={
            "ledger_id": "drift.ledger.1",
            "classifications": [
                {
                    "classification_id": "classification.1",
                    "row_id": "row.config",
                    "severity": "high",
                    "disposition": "fix_now",
                    "reason_codes": ["ENV_MISCONFIG"],
                }
            ],
        },
        expected_snapshot_ref="snapshot.expected",
        generated_at=GENERATED_AT,
    )

    assert view["state"] == "blocked"
    assert view["payload"]["categories"][0]["severity"] == "blocking"
    assert view["payload"]["suggested_actions"][0]["action_ref"] == "sandbox_drift.resolve_blocking_drift"
    assert view["payload"]["suggested_actions"][0]["blockers"][0]["reason_code"] == "sandbox_drift.blocks_safe_actions"


def test_cartridge_status_distinguishes_blocked_and_degraded() -> None:
    blocked = build_cartridge_status_view(
        cartridge_ref="cartridge.billing",
        validation_report={
            "canonical_digest": "sha256:abc",
            "manifest": {"cartridge_version": "1.0.0", "build_id": "build.1"},
            "findings": [
                {
                    "severity": "error",
                    "category": "compatibility",
                    "reason_code": "COMPAT_RUNTIME_UNSUPPORTED",
                }
            ],
        },
        generated_at=GENERATED_AT,
    )
    degraded = build_cartridge_status_view(
        cartridge_ref="cartridge.billing",
        validation_report={
            "canonical_digest": "sha256:def",
            "manifest": {"cartridge_version": "1.0.1", "build_id": "build.2"},
            "findings": [{"severity": "warning", "category": "drift", "reason_code": "DRIFT_HOOK_OPTIONAL"}],
        },
        generated_at=GENERATED_AT,
    )

    assert blocked["state"] == "blocked"
    assert blocked["payload"]["status"] == "blocked"
    assert degraded["state"] == "partial"
    assert degraded["payload"]["status"] == "degraded"


def test_stale_snapshot_blocks_safe_actions() -> None:
    view = build_next_safe_actions_view(
        subject_ref="workflow.1",
        snapshot_ref="snapshot.old",
        action_candidates=[
            {
                "action_ref": "verifier.rerun",
                "preconditions": ["snapshot.fresh"],
                "expected_effects": ["new verifier result"],
                "reversibility_class": "read_only",
            }
        ],
        generated_at=GENERATED_AT,
        source_updated_at=STALE_SOURCE,
        max_age_seconds=60,
    )

    assert view["state"] == "blocked"
    assert view["payload"]["actions"] == []
    assert view["payload"]["blocked_actions"][0]["blockers"][0]["reason_code"] == "safe_action.snapshot_stale"


def test_builder_invalid_graph_returns_machine_readable_reasons() -> None:
    view = validate_workflow_builder_graph(
        graph={
            "nodes": [
                {"node_id": "start", "block_ref": "source.refresh"},
                {"node_id": "unsafe", "block_ref": "unapproved.block"},
                {"node_id": "verify", "block_ref": "verifier.run"},
            ],
            "edges": [
                {"from": "verify", "to": "start"},
                {"from": "missing", "to": "verify"},
            ],
        },
        approved_blocks={
            "source.refresh": {"provides": ["fresh_snapshot"]},
            "verifier.run": {"requires": ["fresh_snapshot"], "provides": ["verifier_result"]},
        },
        allowed_edges=[{"from_block": "source.refresh", "to_block": "verifier.run"}],
        generated_at=GENERATED_AT,
    )

    reasons = {item["reason_code"] for item in view["payload"]["validation"]["errors"]}
    assert view["state"] == "blocked"
    assert view["payload"]["validation"]["ok"] is False
    assert {"builder.block_not_approved", "builder.edge_unknown_node", "builder.edge_not_allowed"} <= reasons


def test_managed_runtime_summary_blocks_unavailable_pool() -> None:
    view = build_managed_runtime_accounting_summary(
        subject_ref="tenant.acme",
        receipts=[
            {
                "receipt_id": "receipt.1",
                "execution_mode": "managed",
                "terminal_status": "succeeded",
                "cost_summary": {"amount": "1.250000"},
            }
        ],
        pool_health={"pool_ref": "pool.1", "state": "unavailable", "dispatch_allowed": False},
        generated_at=GENERATED_AT,
        source_updated_at=FRESH_SOURCE,
        max_age_seconds=300,
    )

    assert view["state"] == "blocked"
    assert view["payload"]["cost"]["amount"] == "1.250000"
    assert view["payload"]["pool_health"]["state"] == "unavailable"
