from __future__ import annotations

from runtime.build_authority import build_authority_bundle


def test_build_authority_keeps_resolved_references_as_suggestions_until_accepted() -> None:
    definition = {
        "type": "operating_model",
        "references": [
            {
                "id": "ref-001",
                "type": "integration",
                "slug": "@gmail/search",
                "raw": "@gmail/search",
                "resolved": True,
                "resolved_to": "integration_registry:gmail/search",
            }
        ],
        "draft_flow": [
            {
                "id": "step-001",
                "title": "Review support inbox",
                "summary": "Review the support inbox.",
                "reference_slugs": ["@gmail/search"],
                "depends_on": [],
                "order": 1,
            }
        ],
        "definition_revision": "def_explicit_approval_ref",
    }

    bundle = build_authority_bundle(definition)

    binding = bundle["binding_ledger"][0]
    assert binding["state"] == "suggested"
    assert binding["accepted_target"] is None
    assert binding["candidate_targets"][0]["target_ref"] == "integration_registry:gmail/search"
    assert bundle["projection_status"]["state"] == "blocked"
    assert bundle["build_issues"][0]["gate_rule"]["binding_state"] == "suggested"


def test_build_authority_keeps_admitted_imports_as_suggestions_until_binding_accept() -> None:
    definition = {
        "type": "operating_model",
        "draft_flow": [
            {
                "id": "step-001",
                "title": "Use escalation policy",
                "summary": "Use the latest escalation policy.",
                "depends_on": [],
                "order": 1,
            }
        ],
        "import_snapshots": [
            {
                "snapshot_id": "import_alpha",
                "source_kind": "net",
                "source_locator": "find current escalation policy",
                "requested_shape": {
                    "label": "Escalation Policy",
                    "target_ref": "#escalation-policy",
                    "kind": "type",
                },
                "payload": {"result": "ok"},
                "freshness_ttl": 3600,
                "captured_at": "2026-04-15T10:00:00+00:00",
                "stale_after_at": "2099-04-15T11:00:00+00:00",
                "approval_state": "admitted",
                "admitted_targets": [
                    {
                        "target_ref": "#escalation-policy",
                        "label": "Escalation Policy",
                        "kind": "type",
                    }
                ],
                "binding_id": "binding:import:import_alpha",
                "node_id": "step-001",
            }
        ],
        "definition_revision": "def_explicit_approval_import",
    }

    bundle = build_authority_bundle(definition)

    binding = bundle["binding_ledger"][0]
    assert binding["state"] == "suggested"
    assert binding["accepted_target"] is None
    assert "explicit binding approval is still required" in binding["rationale"]
    assert bundle["projection_status"]["state"] == "blocked"


def test_build_authority_preserves_explicitly_accepted_binding_across_snapshot_recompute() -> None:
    definition = {
        "type": "operating_model",
        "draft_flow": [
            {
                "id": "step-001",
                "title": "Use escalation policy",
                "summary": "Use the latest escalation policy.",
                "depends_on": [],
                "order": 1,
            }
        ],
        "binding_ledger": [
            {
                "binding_id": "binding:import:import_alpha",
                "source_kind": "import_request",
                "source_label": "Escalation Policy",
                "source_span": None,
                "source_node_ids": ["step-001"],
                "state": "accepted",
                "candidate_targets": [
                    {
                        "target_ref": "#escalation-policy",
                        "label": "Escalation Policy",
                        "kind": "type",
                    }
                ],
                "accepted_target": {
                    "target_ref": "#escalation-policy",
                    "label": "Escalation Policy",
                    "kind": "type",
                },
                "rationale": "Accepted in build workspace.",
                "created_at": "2026-04-15T10:00:00+00:00",
                "updated_at": "2026-04-15T10:01:00+00:00",
                "freshness": None,
            }
        ],
        "import_snapshots": [
            {
                "snapshot_id": "import_alpha",
                "source_kind": "net",
                "source_locator": "find current escalation policy",
                "requested_shape": {
                    "label": "Escalation Policy",
                    "target_ref": "#escalation-policy",
                    "kind": "type",
                },
                "payload": {"result": "ok"},
                "freshness_ttl": 3600,
                "captured_at": "2026-04-15T10:00:00+00:00",
                "stale_after_at": "2099-04-15T11:00:00+00:00",
                "approval_state": "admitted",
                "admitted_targets": [
                    {
                        "target_ref": "#escalation-policy",
                        "label": "Escalation Policy",
                        "kind": "type",
                    }
                ],
                "binding_id": "binding:import:import_alpha",
                "node_id": "step-001",
            }
        ],
        "definition_revision": "def_explicit_approval_preserve",
    }

    bundle = build_authority_bundle(definition)

    binding = bundle["binding_ledger"][0]
    assert binding["state"] == "accepted"
    assert binding["accepted_target"]["target_ref"] == "#escalation-policy"
    assert bundle["projection_status"]["state"] == "ready"
    assert bundle["build_issues"] == []


def test_build_authority_does_not_require_execution_route_before_hardening() -> None:
    definition = {
        "type": "operating_model",
        "draft_flow": [
            {
                "id": "step-001",
                "title": "Review support inbox",
                "summary": "Review the support inbox.",
                "depends_on": [],
                "order": 1,
            }
        ],
        "binding_ledger": [
            {
                "binding_id": "binding:ref-001",
                "source_kind": "reference",
                "source_label": "triage-agent",
                "source_span": None,
                "source_node_ids": ["step-001"],
                "state": "accepted",
                "candidate_targets": [
                    {
                        "target_ref": "task_type_routing:auto/review",
                        "label": "Auto Review",
                        "kind": "agent",
                    }
                ],
                "accepted_target": {
                    "target_ref": "task_type_routing:auto/review",
                    "label": "Auto Review",
                    "kind": "agent",
                },
                "rationale": "Accepted in build workspace.",
                "created_at": "2026-04-15T10:00:00+00:00",
                "updated_at": "2026-04-15T10:01:00+00:00",
                "freshness": None,
            }
        ],
        "definition_revision": "def_explicit_approval_no_execution_setup",
    }

    bundle = build_authority_bundle(definition)

    assert bundle["projection_status"]["state"] == "ready"
    assert bundle["build_issues"] == []


def test_build_authority_blocks_incomplete_hardened_execution_routes() -> None:
    definition = {
        "type": "operating_model",
        "draft_flow": [
            {
                "id": "step-001",
                "title": "Review support inbox",
                "summary": "Review the support inbox.",
                "depends_on": [],
                "order": 1,
            }
        ],
        "execution_setup": {
            "phases": [
                {
                    "step_id": "step-001",
                    "title": "Review support inbox",
                }
            ]
        },
        "definition_revision": "def_incomplete_hardened_route",
    }

    bundle = build_authority_bundle(definition)

    assert bundle["projection_status"]["state"] == "blocked"
    assert bundle["build_issues"] == [
        {
            "issue_id": "issue:missing-route:step-001",
            "kind": "missing_route",
            "node_id": "step-001",
            "binding_id": None,
            "label": "Choose how this step runs",
            "summary": "This step has no executable route yet, so the workflow cannot be hardened or run.",
            "severity": "blocking",
            "gate_rule": {"required_field": "execution_setup.phases.agent_route"},
        }
    ]
