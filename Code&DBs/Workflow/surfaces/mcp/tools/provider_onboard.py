"""Tools: praxis_provider_onboard."""
from __future__ import annotations

from runtime.operation_catalog_gateway import execute_operation_from_env

from ..subsystems import workflow_database_env


def tool_praxis_provider_onboard(params: dict, _progress_emitter=None) -> dict:
    """Onboard a CLI or API provider through the shared operation catalog."""

    action = str(params.get("action", "probe")).strip().lower()
    provider_slug = str(params.get("provider_slug", "")).strip()
    if not provider_slug:
        return {"error": "provider_slug is required"}

    transport = str(params.get("transport", "")).strip().lower()
    models = params.get("models") or []
    api_key_env_var = params.get("api_key_env_var")
    dry_run = action == "probe"

    payload = {
        "provider_slug": provider_slug,
        "dry_run": dry_run,
    }
    if transport:
        payload["transport"] = transport
    if models:
        payload["models"] = list(models)
    if api_key_env_var:
        payload["api_key_env_var"] = api_key_env_var

    if _progress_emitter:
        _progress_emitter.emit(progress=0, total=2, message=f"Preparing provider onboarding for {provider_slug}")
    result = execute_operation_from_env(
        env=workflow_database_env(),
        operation_name="operator.provider_onboarding",
        payload=payload,
    )
    if _progress_emitter:
        status = "ok" if result.get("ok") else "failed"
        _progress_emitter.emit(progress=2, total=2, message=f"Done — {provider_slug} {status}")
    return result


TOOLS: dict[str, tuple[callable, dict[str, object]]] = {
    "praxis_provider_onboard": (
        tool_praxis_provider_onboard,
        {
            "description": (
                "Onboard a CLI or API provider into Praxis Engine through one catalog-backed "
                "operation. Probes transport, discovers models, writes onboarding authority, "
                "and performs the canonical post-onboarding sync.\n\n"
                "USE WHEN: connecting a new provider or adding models to an existing provider.\n\n"
                "EXAMPLES:\n"
                "  Probe first:  praxis_provider_onboard(action='probe', provider_slug='anthropic', transport='cli')\n"
                "  Then onboard: praxis_provider_onboard(action='onboard', provider_slug='anthropic', transport='cli')\n"
                "  API provider: praxis_provider_onboard(action='onboard', provider_slug='openrouter', transport='api', "
                "api_key_env_var='OPENROUTER_API_KEY')\n\n"
                "The 'probe' action is a dry run. The 'onboard' action writes onboarding authority "
                "and applies the canonical post-onboarding sync.\n\n"
                "DO NOT USE: for checking provider health (use praxis_health)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["probe", "onboard"],
                        "description": "'probe' (dry run) or 'onboard' (write to DB authority)",
                    },
                    "provider_slug": {
                        "type": "string",
                        "description": "Provider identifier (e.g., 'anthropic', 'openai', 'google', 'openrouter')",
                    },
                    "transport": {
                        "type": "string",
                        "enum": ["cli", "api"],
                        "description": "Transport type: 'cli' for CLI tools, 'api' for direct API",
                    },
                    "models": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional specific model slugs to onboard (discovers all if omitted)",
                    },
                    "api_key_env_var": {
                        "type": "string",
                        "description": "Env var name for API key (e.g., 'OPENROUTER_API_KEY')",
                    },
                },
                "required": ["action", "provider_slug"],
            },
        },
    ),
}
