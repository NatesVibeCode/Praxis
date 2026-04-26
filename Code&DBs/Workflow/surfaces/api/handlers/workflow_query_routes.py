"""Route table composition for workflow query endpoints."""

from __future__ import annotations

from ._shared import RouteEntry, RouteMatcher, _exact, _prefix, _prefix_suffix
from . import _query_bugs as _bugs
from . import _query_handoff as _handoff
from . import projections as _projections
from . import workflow_query as _handler


def _prefix_single_segment(
    path_prefix: str,
    *,
    excluded: set[str] | None = None,
) -> RouteMatcher:
    excluded_values = frozenset(excluded or ())

    def _matches(candidate: str, *, prefix=path_prefix, excluded=excluded_values) -> bool:
        if not candidate.startswith(prefix):
            return False
        suffix = candidate[len(prefix) :]
        return bool(suffix) and "/" not in suffix and suffix not in excluded

    return _matches


def _workflow_build_path(candidate: str) -> bool:
    return candidate.startswith("/api/workflows/") and "/build" in candidate[len("/api/workflows/") :]


def _object_type_fields_path(candidate: str) -> bool:
    path = candidate.split("?", 1)[0]
    prefix = "/api/object-types/"
    if not path.startswith(prefix):
        return False
    tail = path[len(prefix):]
    return tail.endswith("/fields") and tail.count("/") == 1


def _object_type_field_path(candidate: str) -> bool:
    path = candidate.split("?", 1)[0]
    prefix = "/api/object-types/"
    if not path.startswith(prefix):
        return False
    tail = path[len(prefix):]
    return "/fields/" in tail and tail.count("/") == 2


QUERY_POST_ROUTES: list[RouteEntry] = [
    (_exact("/query"), _handler._handle_query_post),
    (_exact("/api/compile/preview"), _handler._handle_compile_preview_post),
    (_exact("/api/catalog/review-decisions"), _handler._handle_catalog_review_decisions_post),
    (_exact("/api/object-types"), _handler._handle_object_types_post),
    (_object_type_fields_path, _handler._handle_object_fields_post),
    (
        lambda candidate: candidate == "/api/documents"
        or (
            candidate.startswith("/api/documents/")
            and candidate.endswith("/attach")
        ),
        _handler._handle_documents_post,
    ),
    (_exact("/api/files"), _handler._handle_files_post),
    (_workflow_build_path, _handler._handle_workflow_build_post),
    (
        lambda candidate: candidate in {
            "/api/objects",
        },
        _handler._handle_objects_post,
    ),
    (_exact("/api/workflows"), _handler._handle_workflows_post),
    (_exact("/api/workflow-triggers"), _handler._handle_workflow_triggers_post),
    (_prefix("/api/trigger/"), _handler._handle_trigger_post),
]

QUERY_PUT_ROUTES: list[RouteEntry] = [
    (_prefix_single_segment("/api/object-types/"), _handler._handle_object_types_put),
    (_exact("/api/objects/update"), _handler._handle_objects_post),
    (_prefix_single_segment("/api/objects/"), _handler._handle_objects_put),
    (
        _prefix_single_segment("/api/workflows/", excluded={"run", "delete"}),
        _handler._handle_workflows_post,
    ),
    (_prefix_single_segment("/api/workflow-triggers/"), _handler._handle_workflow_triggers_post),
]

QUERY_GET_ROUTES: list[RouteEntry] = [
    (_prefix_suffix("/api/workflows/", "/build/stream"), _handler._handle_build_stream),
    (_exact("/api/dashboard"), _handler._handle_dashboard_get),
    *_projections.PROJECTION_GET_ROUTES,
    (_exact("/api/leaderboard"), _handler._handle_leaderboard_get),
    (_exact("/api/runs/recent"), _handler._handle_runs_recent_get),
    (_exact("/api/references"), _handler._handle_references_get),
    (_exact("/api/source-options"), _handler._handle_source_options_get),
    (_exact("/api/manifest-heads"), _handler._handle_manifest_heads_get),
    (_exact("/api/manifests"), _handler._handle_manifests_get),
    (_exact("/api/manifests/history"), _handler._handle_manifest_history_get),
    (_exact("/api/handoff/latest"), _handoff._handle_handoff_latest_get),
    (_exact("/api/handoff/lineage"), _handoff._handle_handoff_lineage_get),
    (_exact("/api/handoff/status"), _handoff._handle_handoff_status_get),
    (_exact("/api/handoff/history"), _handoff._handle_handoff_history_get),
    (_exact("/api/templates"), _handler._handle_templates_get),
    (_exact("/api/models"), _handler._handle_models_get),
    (_exact("/api/models/market"), _handler._handle_market_models_get),
    # /api/integrations is owned by INTEGRATIONS_GET_ROUTES — the old inline
    # handler returned a different shape (no source/catalog_dispatch/actions)
    # and conflicted with the admin handler. Removed to collapse to one authority.
    (_exact("/api/catalog"), _handler._handle_catalog_get),
    (_exact("/api/catalog/operations"), _handler._handle_operation_catalog_get),
    (_exact("/api/catalog/review-decisions"), _handler._handle_catalog_review_decisions_get),
    (_exact("/api/intent/analyze"), _handler._handle_intent_analyze_get),
    (_exact("/api/search"), _handler._handle_search_get),
    (_exact("/api/bugs/replay-ready"), _bugs._handle_bugs_replay_ready_get),
    (_exact("/api/bugs"), _bugs._handle_bugs_get),
    (_exact("/api/registries/search"), _handler._handle_registries_search_get),
    (_exact("/api/object-types"), _handler._handle_object_types_get),
    (_object_type_fields_path, _handler._handle_object_fields_get),
    (_prefix_single_segment("/api/object-types/"), _handler._handle_object_types_get),
    (_prefix_suffix("/api/files/", "/content"), _handler._handle_files_get),
    (_exact("/api/files"), _handler._handle_files_get),
    (_exact("/api/documents"), _handler._handle_documents_get),
    (_exact("/api/objects"), _handler._handle_objects_get),
    (_prefix_single_segment("/api/objects/"), _handler._handle_objects_get),
    (_exact("/api/workflows"), _handler._handle_workflows_get),
    (_workflow_build_path, _handler._handle_workflow_build_get),
    (_prefix_suffix("/api/workflows/", "/runs"), _handler._handle_workflows_runs_get),
    (_prefix("/api/workflows/"), _handler._handle_workflows_get),
    (_exact("/api/workflow-triggers"), _handler._handle_workflow_triggers_get),
]

QUERY_DELETE_ROUTES: list[RouteEntry] = [
    (_object_type_field_path, _handler._handle_object_fields_delete),
    (_prefix_single_segment("/api/object-types/"), _handler._handle_object_types_delete),
    (_exact("/api/objects/delete"), _handler._handle_objects_post),
    (_prefix_single_segment("/api/objects/"), _handler._handle_objects_delete),
    (_prefix_single_segment("/api/workflows/delete/"), _handler._handle_workflow_delete),
    (
        lambda candidate: candidate.startswith("/api/files/")
        and not candidate.endswith("/content"),
        _handler._handle_files_delete,
    ),
]

QUERY_ROUTES: dict[str, object] = {
    "/bugs": _bugs._handle_bugs,
    "/recall": _handler._handle_recall,
    "/ingest": _handler._handle_ingest,
    "/graph": _handler._handle_graph,
    "/receipts": _handler._handle_receipts,
    "/constraints": _handler._handle_constraints,
    "/friction": _handler._handle_friction,
    "/heal": _handler._handle_heal,
    "/artifacts": _handler._handle_artifacts,
    "/decompose": _handler._handle_decompose,
    "/research": _handler._handle_research,
}


__all__ = [
    "QUERY_DELETE_ROUTES",
    "QUERY_GET_ROUTES",
    "QUERY_PUT_ROUTES",
    "QUERY_POST_ROUTES",
    "QUERY_ROUTES",
]
