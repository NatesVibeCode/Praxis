"""Tools: praxis_intent_match, praxis_manifest_generate, praxis_manifest_refine."""
from __future__ import annotations

from typing import Any

from runtime.canonical_manifests import (
    ManifestRuntimeBoundaryError,
    generate_manifest,
    refine_manifest,
)

from ..subsystems import _subs


def tool_praxis_intent_match(params: dict) -> dict:
    """Search registries for primitives matching an intent and propose composition."""
    intent = params.get("intent", "")
    if not intent:
        return {"error": "intent is required"}

    try:
        matcher = _subs.get_intent_matcher()
        matches = matcher.match(intent)
        plan = matcher.compose(intent, matches)

        def _match_list(items):
            return [
                {"id": m.id, "name": m.name, "description": m.description,
                 "category": m.category, "rank": round(m.rank, 4)}
                for m in items
            ]

        return {
            "match_result": {
                "intent": matches.intent,
                "ui_components": _match_list(matches.ui_components),
                "calculations": _match_list(matches.calculations),
                "workflows": _match_list(matches.workflows),
                "coverage_score": matches.coverage_score,
                "gaps": list(matches.gaps),
            },
            "composition_plan": {
                "components": list(plan.components),
                "calculations": list(plan.calculations),
                "workflows": list(plan.workflows),
                "bindings": [
                    {"source_id": b.source_id, "source_type": b.source_type,
                     "target_id": b.target_id, "target_type": b.target_type,
                     "rationale": b.rationale}
                    for b in plan.bindings
                ],
                "layout_suggestion": plan.layout_suggestion,
                "confidence": plan.confidence,
            },
        }
    except Exception as e:
        return {"error": f"IntentMatcher error: {e}"}


def tool_praxis_manifest_generate(params: dict) -> dict:
    """Generate an app manifest from a natural language intent."""
    intent = params.get("intent", "")
    if not intent:
        return {"error": "intent is required"}

    try:
        result = generate_manifest(
            _subs.get_pg_conn(),
            matcher=_subs.get_intent_matcher(),
            generator=_subs.get_manifest_generator(),
            intent=intent,
        )

        return {
            "manifest_id": result.manifest_id,
            "manifest": result.manifest,
            "version": result.version,
            "confidence": result.confidence,
            "explanation": result.explanation,
        }
    except ManifestRuntimeBoundaryError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": f"ManifestGenerator error: {e}"}


def tool_praxis_manifest_refine(params: dict) -> dict:
    """Refine an existing app manifest based on user feedback."""
    manifest_id = params.get("manifest_id", "")
    feedback = params.get("feedback", "")
    if not manifest_id or not feedback:
        return {"error": "manifest_id and feedback are required"}

    try:
        result = refine_manifest(
            _subs.get_pg_conn(),
            generator=_subs.get_manifest_generator(),
            manifest_id=manifest_id,
            instruction=feedback,
        )

        return {
            "manifest_id": result.manifest_id,
            "manifest": result.manifest,
            "version": result.version,
            "confidence": result.confidence,
            "explanation": result.explanation,
        }
    except ManifestRuntimeBoundaryError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": f"ManifestGenerator refine error: {e}"}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_intent_match": (
        tool_praxis_intent_match,
        {
            "description": (
                "Find existing UI components, workflows, and integrations that match what you want "
                "to build. Searches the registry and proposes how to compose them into an app.\n\n"
                "USE WHEN: the user describes what they want to build and you need to check what "
                "building blocks already exist. This is the first step before praxis_manifest_generate.\n\n"
                "EXAMPLE: praxis_intent_match(intent='invoice processing workflow with approval steps')\n\n"
                "DO NOT USE: for code-level search (use praxis_discover), or for knowledge lookup (use praxis_recall)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {"type": "string", "description": "Natural language intent describing what the user wants to build."},
                },
                "required": ["intent"],
            },
        },
    ),
    "praxis_manifest_generate": (
        tool_praxis_manifest_generate,
        {
            "description": (
                "Generate a complete app manifest (UI layout, data flow, integrations) from a "
                "natural language description. Combines intent matching with LLM generation to "
                "produce a ready-to-render manifest.\n\n"
                "USE WHEN: the user wants to create a new app, dashboard, or workflow from a description.\n\n"
                "EXAMPLE: praxis_manifest_generate(intent='customer onboarding pipeline with status tracking')\n\n"
                "FOLLOW-UP: use praxis_manifest_refine to iterate on the generated manifest."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "intent": {"type": "string", "description": "Natural language intent describing the app to build."},
                },
                "required": ["intent"],
            },
        },
    ),
    "praxis_manifest_refine": (
        tool_praxis_manifest_refine,
        {
            "description": (
                "Iterate on a previously generated app manifest. Apply user feedback to adjust "
                "layout, add/remove modules, change data sources, or modify behavior.\n\n"
                "USE WHEN: the user wants to change something about a manifest generated by "
                "praxis_manifest_generate.\n\n"
                "EXAMPLE: praxis_manifest_refine(manifest_id='manifest_abc123', "
                "feedback='Add a chart showing weekly trends and remove the status grid')"
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "manifest_id": {"type": "string", "description": "ID of the manifest to refine."},
                    "feedback": {"type": "string", "description": "User feedback describing desired changes."},
                },
                "required": ["manifest_id", "feedback"],
            },
        },
    ),
}
