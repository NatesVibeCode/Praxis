from __future__ import annotations

from dataclasses import replace

from runtime.integrations.action_contracts import (
    AutomationRuleSnapshot,
    AutomationRuleStatus,
    EventDeliveryContract,
    EventDeliverySemantics,
    EventDirection,
    GapKind,
    GapSeverity,
    IdempotencyState,
    IdentityType,
    PermissionBinding,
    RetryPolicyKind,
    RollbackClass,
    SnapshotConfidence,
    draft_contract_from_registry_definition,
    stable_digest,
    validate_automation_snapshot,
)


def test_draft_contract_from_manifest_capability_captures_schema_and_gaps() -> None:
    contract = draft_contract_from_registry_definition(
        {
            "id": "hubspot",
            "name": "HubSpot",
            "provider": "hubspot",
            "manifest_source": "manifest",
            "auth_shape": {
                "kind": "oauth2",
                "credential_ref": "credential.hubspot.oauth",
                "scopes": ["crm.objects.contacts.write"],
            },
        },
        {
            "action": "create_contact",
            "description": "Create contact",
            "method": "POST",
            "path": "https://api.example.com/crm/v3/objects/contacts",
            "body_template": {
                "email": "{{email}}",
                "firstname": "{{first_name}}",
                "lastname": "{{last_name}}",
            },
        },
        captured_at="2026-04-30T00:00:00Z",
        owner="owner.crm",
    )

    assert contract.action_id == "integration_action.hubspot.create_contact"
    assert contract.systems.target.provider == "hubspot"
    assert contract.inputs.field_names() == ("email", "first_name", "last_name")
    assert contract.outputs.success.field_names() == ("status", "data", "summary", "error")
    assert contract.permissions[0].identity_type == IdentityType.OAUTH_CLIENT
    assert contract.permissions[0].credential_ref == "credential.hubspot.oauth"
    assert contract.idempotency.state == IdempotencyState.UNKNOWN

    gaps = contract.validation_gaps()
    gap_kinds = {gap.gap_kind for gap in gaps}
    assert GapKind.UNKNOWN_IDEMPOTENCY_BEHAVIOR in gap_kinds
    assert GapKind.UNKNOWN_SIDE_EFFECTS in gap_kinds
    assert GapKind.MISSING_OBSERVABILITY_OR_AUDIT_COVERAGE in gap_kinds
    assert {gap.severity for gap in gaps if gap.gap_kind == GapKind.UNKNOWN_IDEMPOTENCY_BEHAVIOR} == {
        GapSeverity.HIGH
    }


def test_workflow_cancel_override_records_conditionally_idempotent_runtime_contract() -> None:
    contract = draft_contract_from_registry_definition(
        {
            "id": "workflow",
            "name": "Workflow",
            "provider": "praxis",
            "auth_shape": {"kind": "none"},
        },
        {
            "action": "cancel",
            "description": "Cancel workflow run",
        },
        owner="owner.workflow_runtime",
    )

    assert contract.idempotency.state == IdempotencyState.CONDITIONALLY_IDEMPOTENT
    assert contract.idempotency.key_fields == ("run_id",)
    assert contract.rollback.rollback_class == RollbackClass.FORWARD_FIX_ONLY
    assert contract.rollback.operator_playbook_ref == "workflow_control_runbook.restore_or_resubmit"
    assert contract.observability.missing_dimensions() == ()
    assert GapKind.UNKNOWN_IDEMPOTENCY_BEHAVIOR not in {
        gap.gap_kind for gap in contract.validation_gaps()
    }


def test_unknown_identity_is_blocker_even_when_other_contract_parts_exist() -> None:
    contract = draft_contract_from_registry_definition(
        {
            "id": "custom",
            "name": "Custom",
            "provider": "custom",
            "auth_shape": {"kind": "mystery"},
        },
        {
            "action": "update_record",
            "method": "PATCH",
            "body_template": {"record_id": "{{record_id}}"},
        },
        owner="owner.custom",
    )
    assert contract.permissions[0].identity_type == IdentityType.UNKNOWN

    identity_gaps = [
        gap
        for gap in contract.validation_gaps()
        if gap.gap_kind == GapKind.UNCLEAR_PERMISSIONS
    ]

    assert identity_gaps
    assert identity_gaps[0].severity == GapSeverity.BLOCKER


def test_webhook_event_contract_without_version_or_semantics_emits_typed_gap() -> None:
    contract = draft_contract_from_registry_definition(
        {
            "id": "webhook",
            "name": "Webhook",
            "provider": "http",
            "auth_shape": {"kind": "api_key", "credential_ref": "credential.webhook"},
        },
        {
            "action": "post",
            "method": "POST",
            "path": "https://hooks.example.com/events",
            "body_template": {"event": "{{event_type}}"},
        },
        owner="owner.webhook",
    )
    contract = replace(
        contract,
        webhook_events=(
            EventDeliveryContract(
                event_name="external.event",
                direction=EventDirection.OUTBOUND,
                producer="praxis.workflow",
                consumer="external.webhook",
                delivery_semantics=EventDeliverySemantics.UNKNOWN,
                retry_policy=RetryPolicyKind.UNKNOWN,
            ),
        ),
    )

    assert GapKind.UNDOCUMENTED_WEBHOOK_EVENT_VERSIONING in {
        gap.gap_kind for gap in contract.validation_gaps()
    }


def test_automation_snapshot_requires_authoritative_source_and_linked_actions() -> None:
    snapshot = AutomationRuleSnapshot(
        rule_id="automation.contact_sync",
        name="Contact sync",
        source_of_truth_ref="",
        snapshot_timestamp="",
        trigger_condition="contact.updated",
        owner="owner.crm",
        status=AutomationRuleStatus.UNKNOWN,
        capture_method=SnapshotConfidence.UNKNOWN,
    )

    gaps = validate_automation_snapshot(snapshot)
    assert {gap.gap_kind for gap in gaps} == {GapKind.UNVERIFIED_AUTOMATION_SNAPSHOT}
    assert len(gaps) == 4
    assert {gap.severity for gap in gaps} == {GapSeverity.HIGH, GapSeverity.MEDIUM}


def test_contract_hash_and_gap_ids_are_deterministic() -> None:
    contract = draft_contract_from_registry_definition(
        {
            "id": "notifications",
            "name": "Notifications",
            "provider": "praxis",
            "auth_shape": {"kind": "none"},
        },
        {
            "action": "send",
            "description": "Send notification",
        },
        owner="owner.notifications",
    )

    assert contract.contract_hash() == contract.contract_hash()
    assert stable_digest(contract.as_dict(include_hash=False)) == contract.contract_hash()

    first = [gap.resolved_gap_id() for gap in contract.validation_gaps()]
    second = [gap.resolved_gap_id() for gap in contract.validation_gaps()]
    assert first == second


def test_api_key_permission_without_owner_is_high_gap() -> None:
    contract = draft_contract_from_registry_definition(
        {
            "id": "internal",
            "name": "Internal",
            "provider": "http",
            "auth_shape": {"kind": "api_key", "credential_ref": "credential.internal"},
        },
        {
            "action": "update_ticket",
            "method": "POST",
            "body_template": {"ticket_id": "{{ticket_id}}"},
        },
        owner="",
    )
    assert isinstance(contract.permissions[0], PermissionBinding)

    permission_gaps = [
        gap
        for gap in contract.validation_gaps()
        if gap.gap_kind == GapKind.UNCLEAR_PERMISSIONS
    ]

    assert permission_gaps
    assert permission_gaps[0].severity == GapSeverity.HIGH
