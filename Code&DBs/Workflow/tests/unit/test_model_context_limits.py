from __future__ import annotations

import sys
from pathlib import Path

import pytest

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

import registry.model_context_limits as model_context_limits


@pytest.fixture(autouse=True)
def _reset_model_profile_cache() -> None:
    model_context_limits._model_profiles_cache = None
    model_context_limits._model_profiles_loaded = False
    yield
    model_context_limits._model_profiles_cache = None
    model_context_limits._model_profiles_loaded = False


def test_context_window_requires_explicit_workflow_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)

    with pytest.raises(RuntimeError, match="requires explicit WORKFLOW_DATABASE_URL"):
        model_context_limits.context_window_for_model("openai", "gpt-5.4")


def test_context_window_requires_provider_and_model_slug() -> None:
    with pytest.raises(RuntimeError, match="requires provider_slug and model_slug"):
        model_context_limits.context_window_for_model("openai", None)


def test_context_window_does_not_fallback_to_env_or_default_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://test@localhost:5432/praxis_test")
    monkeypatch.setenv("PRAXIS_CONTEXT_WINDOW_OPENAI_GPT_5_4", "999999")
    monkeypatch.setenv("PRAXIS_DEFAULT_CONTEXT_WINDOW", "128000")
    monkeypatch.setattr(
        model_context_limits,
        "_load_model_profiles_context_windows",
        lambda: {},
    )

    with pytest.raises(RuntimeError, match="missing authoritative context window for openai/gpt-5.4"):
        model_context_limits.context_window_for_model("openai", "gpt-5.4")


def test_context_window_returns_authoritative_model_profile(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        model_context_limits,
        "_load_model_profiles_context_windows",
        lambda: {("openai", "gpt-5.4"): 200_000},
    )

    assert model_context_limits.context_window_for_model("openai", "gpt-5.4") == 200_000


def test_safe_context_budget_requires_authoritative_budget_ratio(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        model_context_limits,
        "_load_model_profiles_context_windows",
        lambda: {("openai", "gpt-5.4"): 200_000},
    )

    import registry.config_registry as config_registry
    import runtime.adaptive_params as adaptive_params

    class _FakeConfig:
        def get(self, key: str):
            assert key == "context.budget_ratio"
            return 0.5

    monkeypatch.setattr(config_registry, "get_config", lambda: _FakeConfig())
    monkeypatch.setattr(
        adaptive_params,
        "get_adaptive_params",
        lambda: (_ for _ in ()).throw(AssertionError("adaptive params fallback is not allowed")),
    )

    assert model_context_limits.safe_context_budget("openai", "gpt-5.4") == 100_000


def test_context_window_normalizes_missing_user_in_workflow_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, str] = {}

    class _FakeConn:
        async def fetch(self, _sql: str):
            return [
                {
                    "provider_name": "openai",
                    "model_name": "gpt-5.4",
                    "default_parameters": {"context_window": 200_000},
                }
            ]

        async def close(self) -> None:
            return None

    class _FakeAsyncPGModule:
        async def connect(self, dsn: str):
            observed["dsn"] = dsn
            return _FakeConn()

    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://localhost:5432/praxis_test")
    monkeypatch.setitem(sys.modules, "asyncpg", _FakeAsyncPGModule())

    assert model_context_limits.context_window_for_model("openai", "gpt-5.4") == 200_000
    assert observed["dsn"] == "postgresql://postgres@localhost:5432/praxis_test"
