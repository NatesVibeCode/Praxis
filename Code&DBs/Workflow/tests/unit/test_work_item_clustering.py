from __future__ import annotations

from datetime import datetime, timezone

from runtime.work_item_clustering import cluster_bug_items, cluster_roadmap_items
from surfaces.api._operator_repository import (
    OperatorRoadmapDependencyRecord,
    OperatorRoadmapItemRecord,
    OperatorRoadmapTreeSnapshot,
)
from surfaces.api.handlers import _bug_surface_contract as bug_contract


def test_bug_list_payload_clusters_by_explicit_context_before_single_rows() -> None:
    bugs = [
        {
            "bug_id": "BUG-ROUTE-1",
            "title": "Provider route probe misses failed transport",
            "status": "OPEN",
            "severity": "P1",
            "category": "RUNTIME",
            "tags": ["cluster:provider-route-health", "provider_slug:anthropic"],
            "resume_context": {"next_steps": ["fix provider registry preflight"]},
        },
        {
            "bug_id": "BUG-ROUTE-2",
            "title": "Route health dashboard stays green on transport failure",
            "status": "OPEN",
            "severity": "P1",
            "category": "RUNTIME",
            "tags": ["cluster:provider-route-health"],
        },
        {
            "bug_id": "BUG-MOON-1",
            "title": "Moon release tray hides stale trigger",
            "status": "OPEN",
            "severity": "P2",
            "category": "VERIFY",
            "source_issue_id": "issue.moon-release",
        },
    ]

    class _Tracker:
        def count_bugs(self, **_kwargs):
            return len(bugs)

        def list_bugs(self, **_kwargs):
            return bugs

    payload = bug_contract.list_bugs_payload(
        bt=_Tracker(),
        bt_mod=object(),
        body={},
        serialize_bug=lambda bug: dict(bug),
        default_limit=10,
        include_replay_details=False,
        parse_status=lambda _mod, _raw: None,
        parse_severity=lambda _mod, _raw: None,
        parse_category=lambda _mod, _raw: None,
    )

    assert payload["clustering"]["authority"] == "runtime.work_item_clustering.cluster_bug_items"
    assert payload["clusters"][0]["cluster_key"] == "bug.tag.cluster:provider-route-health"
    assert payload["clusters"][0]["bug_ids"] == ["BUG-ROUTE-1", "BUG-ROUTE-2"]
    assert payload["clusters"][0]["next_steps"] == ["fix provider registry preflight"]
    assert payload["clustering"]["singleton_count"] == 1


def test_bug_clustering_can_be_suppressed_for_legacy_callers() -> None:
    payload = cluster_bug_items(
        [
            {
                "bug_id": "BUG-1",
                "title": "Standalone issue",
                "status": "OPEN",
                "severity": "P3",
                "category": "OTHER",
            }
        ],
        include_singletons=False,
    )

    assert payload["clusters"] == []
    assert payload["singleton_count"] == 1


def test_roadmap_clustering_groups_parent_waves_and_lineage() -> None:
    items = [
        {
            "roadmap_item_id": "roadmap_item.program",
            "title": "Authority cleanup program",
            "status": "active",
            "lifecycle": "claimed",
            "priority": "p1",
            "parent_roadmap_item_id": None,
            "source_bug_id": None,
            "registry_paths": ["surfaces/operator"],
        },
        {
            "roadmap_item_id": "roadmap_item.program.phase_1",
            "title": "Roadmap read model clustering",
            "status": "active",
            "lifecycle": "claimed",
            "priority": "p1",
            "parent_roadmap_item_id": "roadmap_item.program",
            "source_bug_id": "BUG-CLUSTER",
            "registry_paths": ["surfaces/operator"],
        },
        {
            "roadmap_item_id": "roadmap_item.program.phase_2",
            "title": "Bug table clustering",
            "status": "active",
            "lifecycle": "planned",
            "priority": "p1",
            "parent_roadmap_item_id": "roadmap_item.program",
            "source_bug_id": "BUG-CLUSTER",
            "registry_paths": ["surfaces/operator"],
        },
    ]
    dependencies = [
        {
            "roadmap_item_id": "roadmap_item.program.phase_2",
            "depends_on_roadmap_item_id": "roadmap_item.program.phase_1",
        }
    ]

    payload = cluster_roadmap_items(items, dependencies=dependencies)

    parent_cluster = payload["clusters"][0]
    assert parent_cluster["reason_code"] == "roadmap_cluster.parent_child_wave"
    assert parent_cluster["roadmap_item_ids"] == [
        "roadmap_item.program",
        "roadmap_item.program.phase_1",
        "roadmap_item.program.phase_2",
    ]
    assert parent_cluster["dependency_roadmap_item_ids"] == [
        "roadmap_item.program.phase_1"
    ]
    assert payload["membership_policy"] == "overlapping_clusters_allowed"
    assert any(
        cluster["reason_code"] == "roadmap_cluster.source_bug"
        and cluster["roadmap_item_ids"]
        == ["roadmap_item.program.phase_1", "roadmap_item.program.phase_2"]
        for cluster in payload["clusters"]
    )


def test_roadmap_tree_snapshot_exposes_clusters_before_item_rows() -> None:
    now = datetime(2026, 4, 20, 12, tzinfo=timezone.utc)
    root = OperatorRoadmapItemRecord(
        roadmap_item_id="roadmap_item.cluster.root",
        roadmap_key="roadmap.cluster.root",
        title="Clustered roadmap root",
        item_kind="capability",
        status="active",
        lifecycle="claimed",
        priority="p1",
        parent_roadmap_item_id=None,
        source_bug_id=None,
        source_idea_id=None,
        registry_paths=("surfaces/operator",),
        summary="Root summary",
        acceptance_criteria={},
        decision_ref="decision.cluster.root",
        target_start_at=None,
        target_end_at=None,
        completed_at=None,
        created_at=now,
        updated_at=now,
    )
    child = OperatorRoadmapItemRecord(
        roadmap_item_id="roadmap_item.cluster.root.child",
        roadmap_key="roadmap.cluster.root.child",
        title="Clustered roadmap child",
        item_kind="capability",
        status="active",
        lifecycle="planned",
        priority="p1",
        parent_roadmap_item_id=root.roadmap_item_id,
        source_bug_id="BUG-CLUSTER",
        source_idea_id=None,
        registry_paths=("surfaces/operator",),
        summary="Child summary",
        acceptance_criteria={},
        decision_ref="decision.cluster.root",
        target_start_at=None,
        target_end_at=None,
        completed_at=None,
        created_at=now,
        updated_at=now,
    )
    dependency = OperatorRoadmapDependencyRecord(
        roadmap_item_dependency_id="dep.cluster",
        roadmap_item_id=child.roadmap_item_id,
        depends_on_roadmap_item_id=root.roadmap_item_id,
        dependency_kind="blocks",
        decision_ref=None,
        created_at=now,
    )
    snapshot = OperatorRoadmapTreeSnapshot(
        root_roadmap_item_id=root.roadmap_item_id,
        root_item=root,
        roadmap_items=(root, child),
        roadmap_item_dependencies=(dependency,),
        as_of=now,
    )

    payload = snapshot.to_json()

    assert payload["instruction_authority"]["packet_read_order"][1] == "roadmap_item_clusters"
    assert payload["counts"]["roadmap_item_clusters"] >= 1
    assert any(
        cluster["reason_code"] == "roadmap_cluster.parent_child_wave"
        and cluster["roadmap_item_ids"] == [root.roadmap_item_id, child.roadmap_item_id]
        for cluster in payload["roadmap_item_clusters"]
    )
