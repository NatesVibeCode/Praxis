"""MCP wrapper for the match_rules.backfill CQRS operation.

Thin pass-through to the registered ``match_rules.backfill`` operation.
The handler runs the same `_plan_benchmark_rules` planner the onboarding
wizard uses and writes via `_apply_benchmark_plan`, so each invocation
records an authority_operation_receipts row and emits a
`match_rules.backfilled` event on success.
"""

from __future__ import annotations

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_match_rules_backfill(params: dict, _progress_emitter=None) -> dict:
    """Backfill provider_model_market_match_rules + benchmark_profile."""

    payload: dict = {
        "source_slug": str(params.get("source_slug") or "artificial_analysis").strip(),
        "dry_run": bool(params.get("dry_run", False)),
    }
    provider_slugs = params.get("provider_slugs") or ()
    if provider_slugs:
        payload["provider_slugs"] = tuple(
            str(p).strip() for p in provider_slugs if str(p).strip()
        )
    if _progress_emitter:
        mode = "preview" if payload["dry_run"] else "apply"
        scope = (
            ", ".join(payload.get("provider_slugs") or ())
            or "all providers with missing rules"
        )
        _progress_emitter.emit(
            progress=0,
            total=1,
            message=f"Backfilling match rules ({mode}) for {scope}",
        )
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="match_rules.backfill",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(
            progress=1,
            total=1,
            message=f"Done — backfill {status}",
        )
    return result


TOOLS: dict[str, tuple[callable, dict[str, object]]] = {
    "praxis_match_rules_backfill": (
        tool_praxis_match_rules_backfill,
        {
            "description": (
                "Backfill provider_model_market_match_rules + provider_model_candidates."
                "benchmark_profile for active candidates that lack an enabled rule for "
                "the configured benchmark source.\n\n"
                "USE WHEN: new candidates were added (e.g. fresh provider onboarding) and "
                "the picker is falling back to capability_tags + priority instead of "
                "benchmark-weighted voter selection. Each candidate either gets a "
                "confidence-rated bound rule (exact / normalized / family / dated) or an "
                "explicit source_unavailable gap row when the benchmark source publishes "
                "no comparable model.\n\n"
                "AGGREGATOR-AWARE: OpenRouter and Together <creator>/<model> slugs route "
                "through the underlying creator (e.g. openrouter + qwen/qwen3-max → "
                "alibaba/qwen3-max in Artificial Analysis).\n\n"
                "DISPATCH: thin wrapper over the registered match_rules.backfill CQRS "
                "operation. Each call records an authority_operation_receipts row and "
                "emits a match_rules.backfilled event on success."
            ),
            "kind": "write",
            "cli": {
                "surface": "integration",
                "tier": "advanced",
                "when_to_use": (
                    "Backfill benchmark rules for newly added providers or candidates "
                    "when selection is falling back to capability tags and priority."
                ),
                "when_not_to_use": (
                    "Do not use it for ordinary model selection, provider onboarding "
                    "smoke tests, or read-only route inspection."
                ),
                "risks": {"default": "write"},
                "examples": [
                    {
                        "title": "Preview a match-rule backfill",
                        "input": {"source_slug": "artificial_analysis", "dry_run": True},
                    },
                ],
            },
            "inputSchema": {
                "type": "object",
                "properties": {
                    "source_slug": {
                        "type": "string",
                        "default": "artificial_analysis",
                        "description": "market_benchmark_source_registry.source_slug to backfill against.",
                    },
                    "provider_slugs": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Optional restriction to specific provider_slugs. "
                            "Empty/omitted = all providers with active candidates "
                            "missing enabled rules."
                        ),
                    },
                    "dry_run": {
                        "type": "boolean",
                        "default": False,
                        "description": (
                            "When true, plans rules and reports the would-be writes "
                            "without touching provider_model_market_match_rules or "
                            "benchmark_profile."
                        ),
                    },
                },
            },
        },
    ),
}
