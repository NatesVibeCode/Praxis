from __future__ import annotations

from datetime import datetime, timedelta, timezone

from runtime import atlas_graph


def test_build_atlas_payload_wraps_graph_with_metadata(monkeypatch) -> None:
    graph = {
        "nodes": [
            {
                "data": {
                    "id": "area::authority",
                    "label": "Operator Authority",
                    "type": "functional_area",
                    "area": "authority",
                    "preview": "Decision authority",
                    "color": "#a9b1d6",
                    "is_area": True,
                    "degree": 2,
                }
            },
            {
                "data": {
                    "id": "table:operator_decisions",
                    "label": "operator_decisions",
                    "type": "table",
                    "area": "authority",
                    "preview": "Decision table",
                    "color": "#a9b1d6",
                }
            },
        ],
        "edges": [
            {
                "data": {
                    "id": "table:a|depends_on|table:b",
                    "source": "table:a",
                    "target": "table:b",
                    "label": "depends_on",
                    "weight": 1.0,
                }
            },
            {
                "data": {
                    "id": "area::authority|agg_depends_on|area::memory",
                    "source": "area::authority",
                    "target": "area::memory",
                    "label": "depends_on",
                    "weight": 3.0,
                    "is_aggregate": True,
                },
                "classes": "aggregate",
            },
        ],
    }
    monkeypatch.setattr(atlas_graph, "build_graph", lambda *, database_url=None: graph)
    monkeypatch.setattr(
        atlas_graph,
        "fetch_graph_freshness",
        lambda conn: {
            "graph_freshness_state": "fresh",
            "memory_entities_max_updated_at": "2026-04-20T22:48:24+00:00",
            "memory_edges_max_updated_at": "2026-04-20T22:45:41+00:00",
            "authority_projection_last_run_at": "2026-04-20T22:45:41+00:00",
            "authority_projection_source_max_updated_at": "2026-04-20T22:45:40+00:00",
            "authority_projection_lag_seconds": 0.0,
            "authority_projection_edge_count": 166,
            "authority_projection_last_run_source": "memory_edges.last_validated_at",
        },
    )
    monkeypatch.setattr(atlas_graph, "_connect", lambda database_url=None: object())

    payload = atlas_graph.build_atlas_payload()

    assert payload["ok"] is True
    assert payload["nodes"] == graph["nodes"]
    assert payload["edges"] == graph["edges"]
    assert payload["areas"] == [
        {
            "slug": "authority",
            "title": "Operator Authority",
            "summary": "Decision authority",
            "color": "#a9b1d6",
            "member_count": 2,
        }
    ]
    assert payload["metadata"]["source_authority"] == "Praxis.db"
    assert payload["metadata"]["node_count"] == 2
    assert payload["metadata"]["edge_count"] == 1
    assert payload["metadata"]["aggregate_edge_count"] == 1
    assert payload["metadata"]["graph_freshness_state"] == "fresh"
    assert payload["metadata"]["authority_projection_edge_count"] == 166
    assert payload["metadata"]["freshness"]["authority_projection_last_run_at"] == "2026-04-20T22:45:41+00:00"
    assert payload["warnings"] == []


def test_build_atlas_payload_keeps_graph_available_when_freshness_fails(monkeypatch) -> None:
    monkeypatch.setattr(atlas_graph, "build_graph", lambda *, database_url=None: {"nodes": [], "edges": []})
    monkeypatch.setattr(atlas_graph, "_connect", lambda database_url=None: object())

    def _raise(_conn):
        raise RuntimeError("clock table missing")

    monkeypatch.setattr(atlas_graph, "fetch_graph_freshness", _raise)

    payload = atlas_graph.build_atlas_payload()

    assert payload["ok"] is True
    assert payload["metadata"]["graph_freshness_state"] == "unknown"
    assert payload["metadata"]["freshness"]["freshness_error"] == "RuntimeError: clock table missing"
    assert payload["warnings"] == ["atlas_freshness_unavailable:RuntimeError"]


def test_classify_graph_freshness_reports_projection_lag() -> None:
    projected_at = datetime(2026, 4, 20, 22, 45, tzinfo=timezone.utc)
    source_at = projected_at + timedelta(seconds=atlas_graph.FRESHNESS_LAG_TOLERANCE_SECONDS + 1)

    state, lag_seconds = atlas_graph.classify_graph_freshness(
        authority_source_updated_at=source_at,
        authority_projection_last_run_at=projected_at,
        authority_projection_edge_count=10,
    )

    assert state == "projection_lagging"
    assert lag_seconds == atlas_graph.FRESHNESS_LAG_TOLERANCE_SECONDS + 1


def test_classify_graph_freshness_reports_unknown_without_projection_edges() -> None:
    state, lag_seconds = atlas_graph.classify_graph_freshness(
        authority_source_updated_at=datetime.now(timezone.utc),
        authority_projection_last_run_at=datetime.now(timezone.utc),
        authority_projection_edge_count=0,
    )

    assert state == "unknown"
    assert lag_seconds is None


def test_compute_activity_score_decays_from_recent_timestamps() -> None:
    now = datetime(2026, 4, 24, 12, 0, tzinfo=timezone.utc)

    recent = atlas_graph.compute_activity_score(now - timedelta(minutes=5), reference_at=now)
    older = atlas_graph.compute_activity_score(now - timedelta(days=21), reference_at=now)
    missing = atlas_graph.compute_activity_score(None, reference_at=now)

    assert recent > older
    assert recent > 0.99
    assert older > atlas_graph.MIN_ACTIVITY_SCORE
    assert missing == atlas_graph.MIN_ACTIVITY_SCORE


def test_infer_schema_area_maps_known_table_markers() -> None:
    assert atlas_graph.infer_schema_area(
        "table:operator_decisions",
        "operator_decisions",
        "table",
        "",
        "schema",
    ) == "authority"


def test_build_graph_emits_activity_signal_on_memory_entity_nodes(monkeypatch) -> None:
    now = datetime.now(timezone.utc)
    recent_at = now - timedelta(minutes=10)
    older_at = now - timedelta(days=30)

    monkeypatch.setattr(atlas_graph, "_connect", lambda database_url=None: object())
    monkeypatch.setattr(
        atlas_graph,
        "fetch_entities",
        lambda conn: [
            {
                "id": "bug::BUG-123",
                "entity_type": "bug",
                "name": "Live bug",
                "content_preview": "Recently touched",
                "source": "memory_entities",
                "updated_at": recent_at,
            },
            {
                "id": "decision::old",
                "entity_type": "operator_decision",
                "name": "Old decision",
                "content_preview": "Dormant",
                "source": "memory_entities",
                "updated_at": older_at,
            },
        ],
    )
    monkeypatch.setattr(atlas_graph, "fetch_capabilities", lambda conn: [])
    monkeypatch.setattr(
        atlas_graph,
        "fetch_functional_areas",
        lambda conn: [{"area_slug": "bugs", "title": "Bugs", "summary": "Bug authority"}],
    )
    monkeypatch.setattr(
        atlas_graph,
        "fetch_area_relations",
        lambda conn: [
            {
                "source_kind": "bug",
                "source_ref": "BUG-123",
                "target_ref": "functional_area.bugs",
                "relation_kind": "owns",
            }
        ],
    )
    monkeypatch.setattr(
        atlas_graph,
        "fetch_edges",
        lambda conn, known_ids: [
            {
                "source_id": "bug::BUG-123",
                "target_id": "decision::old",
                "relation_type": "related_to",
                "weight": 1.0,
                "updated_at": recent_at,
            }
        ],
    )
    monkeypatch.setattr(atlas_graph, "fetch_tools", lambda: [])
    monkeypatch.setattr(atlas_graph, "fetch_data_dictionary_objects", lambda conn: [])
    monkeypatch.setattr(atlas_graph, "fetch_data_dictionary_lineage", lambda conn: [])
    monkeypatch.setattr(atlas_graph, "fetch_surface_catalog_items", lambda conn: [])

    graph = atlas_graph.build_graph()
    nodes = {node["data"]["id"]: node["data"] for node in graph["nodes"]}
    edges = {edge["data"]["id"]: edge["data"] for edge in graph["edges"]}

    assert nodes["bug::BUG-123"]["updated_at"] == recent_at.isoformat()
    assert nodes["bug::BUG-123"]["activity_score"] > nodes["decision::old"]["activity_score"]
    assert nodes["area::bugs"]["activity_score"] == nodes["bug::BUG-123"]["activity_score"]
    assert edges["bug::BUG-123|related_to|decision::old"]["activity_score"] > 0.99


def test_build_graph_projects_surface_catalog_and_dictionary_lineage(monkeypatch) -> None:
    monkeypatch.setattr(atlas_graph, "_connect", lambda database_url=None: object())
    monkeypatch.setattr(atlas_graph, "fetch_entities", lambda conn: [])
    monkeypatch.setattr(atlas_graph, "fetch_capabilities", lambda conn: [])
    monkeypatch.setattr(
        atlas_graph,
        "fetch_functional_areas",
        lambda conn: [
            {"area_slug": "canvas", "title": "Canvas", "summary": "Builder surface"},
            {"area_slug": "authority", "title": "Authority", "summary": "Control tables"},
        ],
    )
    monkeypatch.setattr(atlas_graph, "fetch_area_relations", lambda conn: [])
    monkeypatch.setattr(atlas_graph, "fetch_edges", lambda conn, known_ids: [])
    monkeypatch.setattr(atlas_graph, "fetch_tools", lambda: [])
    monkeypatch.setattr(
        atlas_graph,
        "fetch_data_dictionary_objects",
        lambda conn: [
            {
                "object_kind": "table:surface_catalog_registry",
                "label": "surface_catalog_registry",
                "category": "table",
                "summary": "Canonical Canvas primitive registry",
                "origin_ref": {"source": "schema_projector"},
            },
            {
                "object_kind": "table:operator_decisions",
                "label": "operator_decisions",
                "category": "table",
                "summary": "Decision authority",
                "origin_ref": {"source": "schema_projector"},
            },
        ],
    )
    monkeypatch.setattr(
        atlas_graph,
        "fetch_data_dictionary_lineage",
        lambda conn: [
            {
                "src_object_kind": "table:surface_catalog_registry",
                "src_field_path": "",
                "dst_object_kind": "table:operator_decisions",
                "dst_field_path": "",
                "edge_kind": "references",
                "effective_source": "auto",
                "confidence": 1.0,
            }
        ],
    )
    monkeypatch.setattr(
        atlas_graph,
        "fetch_surface_catalog_items",
        lambda conn: [
            {
                "catalog_item_id": "trigger-manual",
                "surface_name": "canvas",
                "label": "Manual",
                "family": "trigger",
                "status": "ready",
                "drop_kind": "node",
                "action_value": "trigger",
                "gate_family": None,
                "description": "User-initiated run",
                "truth_category": "runtime",
                "surface_tier": "primary",
                "binding_revision": "binding.surface_catalog_registry.canvas.bootstrap.20260415",
                "decision_ref": "decision.surface_catalog_registry.canvas.bootstrap.20260415",
            }
        ],
    )

    graph = atlas_graph.build_graph()
    nodes = {node["data"]["id"]: node["data"] for node in graph["nodes"]}
    edges = {edge["data"]["id"]: edge["data"] for edge in graph["edges"]}

    assert nodes["surface_catalog::trigger-manual"]["area"] == "canvas"
    assert nodes["surface_catalog::trigger-manual"]["authority_source"] == "surface_catalog_registry"
    assert nodes["table:surface_catalog_registry"]["authority_source"] == "data_dictionary_objects"
    assert nodes["table:operator_decisions"]["area"] == "authority"
    assert (
        edges["table:surface_catalog_registry|references|table:operator_decisions"]["authority_source"]
        == "data_dictionary_lineage_effective"
    )


def test_build_graph_canonicalizes_derived_from_labels(monkeypatch) -> None:
    monkeypatch.setattr(atlas_graph, "_connect", lambda database_url=None: object())
    monkeypatch.setattr(
        atlas_graph,
        "fetch_entities",
        lambda conn: [
            {
                "id": "bug::a",
                "entity_type": "bug",
                "name": "Bug A",
                "content_preview": "",
                "source": "memory_entities",
                "updated_at": None,
            },
            {
                "id": "bug::b",
                "entity_type": "bug",
                "name": "Bug B",
                "content_preview": "",
                "source": "memory_entities",
                "updated_at": None,
            },
            {
                "id": "decision::a",
                "entity_type": "operator_decision",
                "name": "Decision A",
                "content_preview": "",
                "source": "memory_entities",
                "updated_at": None,
            },
            {
                "id": "decision::b",
                "entity_type": "operator_decision",
                "name": "Decision B",
                "content_preview": "",
                "source": "memory_entities",
                "updated_at": None,
            },
        ],
    )
    monkeypatch.setattr(atlas_graph, "fetch_capabilities", lambda conn: [])
    monkeypatch.setattr(
        atlas_graph,
        "fetch_functional_areas",
        lambda conn: [{"area_slug": "bugs", "title": "Bugs", "summary": "Bug authority"}],
    )
    monkeypatch.setattr(atlas_graph, "fetch_area_relations", lambda conn: [])
    monkeypatch.setattr(
        atlas_graph,
        "fetch_edges",
        lambda conn, known_ids: [
            {
                "source_id": "bug::a",
                "target_id": "bug::b",
                "relation_type": "derived_from",
                "weight": 1.0,
                "updated_at": None,
            },
            {
                "source_id": "decision::a",
                "target_id": "decision::b",
                "relation_type": "derives_from",
                "weight": 1.0,
                "updated_at": None,
            },
        ],
    )
    monkeypatch.setattr(atlas_graph, "fetch_tools", lambda: [])
    monkeypatch.setattr(atlas_graph, "fetch_data_dictionary_objects", lambda conn: [])
    monkeypatch.setattr(atlas_graph, "fetch_data_dictionary_lineage", lambda conn: [])
    monkeypatch.setattr(atlas_graph, "fetch_surface_catalog_items", lambda conn: [])

    graph = atlas_graph.build_graph()
    labels = {
        edge["data"]["label"]
        for edge in graph["edges"]
        if not edge["data"].get("is_aggregate")
    }

    assert labels == {"derived_from"}
