from __future__ import annotations

import registry.provider_execution_registry as provider_registry


def test_provider_registry_degrades_to_builtins_when_db_authority_is_unavailable(monkeypatch):
    original_registry = dict(provider_registry._REGISTRY)
    original_aliases = dict(provider_registry._ALIAS_MAP)
    original_config = dict(provider_registry._ADAPTER_CONFIG)
    original_failures = dict(provider_registry._ADAPTER_FAILURE_MAPPINGS)
    original_loaded = provider_registry._DB_LOADED
    original_status = provider_registry._load_status
    original_error = provider_registry._load_error
    original_timestamp = provider_registry._load_timestamp

    monkeypatch.setattr(
        provider_registry,
        "_require_database_url",
        lambda: (_ for _ in ()).throw(RuntimeError("db missing")),
    )

    try:
        provider_registry.reload_from_db()

        health = provider_registry.registry_health()
        contract = provider_registry.resolve_adapter_contract("openai", "llm_task")

        assert health["status"] == "degraded_builtin"
        assert health["error"] == "db missing"
        assert "openai" in health["providers"]
        assert contract is not None
        assert contract.adapter_type == "llm_task"
        assert contract.transport_kind == "http"
    finally:
        provider_registry._REGISTRY.clear()
        provider_registry._REGISTRY.update(original_registry)
        provider_registry._ALIAS_MAP.clear()
        provider_registry._ALIAS_MAP.update(original_aliases)
        provider_registry._ADAPTER_CONFIG.clear()
        provider_registry._ADAPTER_CONFIG.update(original_config)
        provider_registry._ADAPTER_FAILURE_MAPPINGS.clear()
        provider_registry._ADAPTER_FAILURE_MAPPINGS.update(original_failures)
        provider_registry._DB_LOADED = original_loaded
        provider_registry._load_status = original_status
        provider_registry._load_error = original_error
        provider_registry._load_timestamp = original_timestamp
