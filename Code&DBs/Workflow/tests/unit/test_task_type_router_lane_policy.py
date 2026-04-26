from __future__ import annotations

import pytest

from runtime.lane_policy import ProviderLanePolicy
from runtime.task_type_router import TaskRouteAuthorityError, TaskTypeRouter


class _FakeRouter:
    """Minimal stand-in exposing just what _apply_lane_policy reads."""

    def __init__(self, default_adapter_type: str = "cli_llm") -> None:
        self._default_adapter_type = default_adapter_type
        self._conn = object()  # not actually used; loader is monkey-patched


def _policies_dict() -> dict[str, ProviderLanePolicy]:
    return {
        "anthropic": ProviderLanePolicy(
            provider_slug="anthropic",
            allowed_adapter_types=frozenset({"cli_llm"}),
            overridable=False,
        ),
        "openai": ProviderLanePolicy(
            provider_slug="openai",
            allowed_adapter_types=frozenset({"cli_llm", "llm_task"}),
            overridable=True,
        ),
    }


@pytest.fixture
def patch_loader(monkeypatch):
    def _install(policies: dict[str, ProviderLanePolicy]):
        monkeypatch.setattr(
            "runtime.lane_policy.load_provider_lane_policies",
            lambda _conn: policies,
        )
    return _install


def test_lane_policy_drops_anthropic_llm_task(patch_loader) -> None:
    patch_loader(_policies_dict())
    rows = [
        {"provider_slug": "anthropic", "model_slug": "claude-opus", "adapter_type": "llm_task"},
        {"provider_slug": "anthropic", "model_slug": "claude-opus", "adapter_type": "cli_llm"},
    ]
    kept = TaskTypeRouter._apply_lane_policy(_FakeRouter(), "chat", rows)
    assert len(kept) == 1
    assert kept[0]["adapter_type"] == "cli_llm"


def test_lane_policy_keeps_openai_llm_task(patch_loader) -> None:
    patch_loader(_policies_dict())
    rows = [
        {"provider_slug": "openai", "model_slug": "gpt-5", "adapter_type": "llm_task"},
    ]
    kept = TaskTypeRouter._apply_lane_policy(_FakeRouter(), "chat", rows)
    assert len(kept) == 1


def test_lane_policy_raises_when_chain_fully_rejected(patch_loader) -> None:
    patch_loader(_policies_dict())
    rows = [
        {"provider_slug": "anthropic", "model_slug": "claude-opus", "adapter_type": "llm_task"},
    ]
    with pytest.raises(TaskRouteAuthorityError, match="rejected by provider lane policy"):
        TaskTypeRouter._apply_lane_policy(_FakeRouter(), "chat", rows)


def test_lane_policy_fails_closed_when_no_policy_rows(patch_loader) -> None:
    """Empty policies dict means there is no provider lane authority."""
    patch_loader({})
    rows = [
        {"provider_slug": "anthropic", "model_slug": "claude", "adapter_type": "llm_task"},
        {"provider_slug": "unknown", "model_slug": "x", "adapter_type": "cli_llm"},
    ]
    with pytest.raises(TaskRouteAuthorityError, match="provider lane policy authority"):
        TaskTypeRouter._apply_lane_policy(_FakeRouter(), "chat", rows)


def test_lane_policy_fails_closed_for_unlisted_provider(patch_loader) -> None:
    """Provider not in the seeded policies is not runnable."""
    patch_loader(_policies_dict())
    rows = [
        {"provider_slug": "cursor", "model_slug": "auto", "adapter_type": "cli_llm"},
    ]
    with pytest.raises(TaskRouteAuthorityError, match="rejected by provider lane policy"):
        TaskTypeRouter._apply_lane_policy(_FakeRouter(), "chat", rows)


def test_lane_policy_passes_empty_rows_through(patch_loader) -> None:
    patch_loader(_policies_dict())
    assert TaskTypeRouter._apply_lane_policy(_FakeRouter(), "chat", []) == []


def test_lane_policy_orders_prepaid_before_metered(patch_loader) -> None:
    """Zero-marginal-cost (prepaid) routes precede metered routes after
    admission, regardless of adapter_type. A metered CLI correctly lands
    in failover; a prepaid API correctly lands as primary."""
    patch_loader(_policies_dict())
    rows = [
        # metered API
        {"provider_slug": "openai", "model_slug": "gpt-5", "adapter_type": "llm_task",
         "billing_mode": "metered_api"},
        # prepaid CLI (the common case)
        {"provider_slug": "openai", "model_slug": "gpt-5-cli", "adapter_type": "cli_llm",
         "billing_mode": "subscription_included"},
        # metered CLI (hypothetical — must NOT beat prepaid)
        {"provider_slug": "openai", "model_slug": "metered-cli", "adapter_type": "cli_llm",
         "billing_mode": "metered_api"},
        # prepaid API (fixed-credit) — MUST beat metered routes
        {"provider_slug": "openai", "model_slug": "prepaid-api", "adapter_type": "llm_task",
         "billing_mode": "prepaid_credit"},
    ]
    kept = TaskTypeRouter._apply_lane_policy(_FakeRouter(), "chat", rows)
    modes = [r["billing_mode"] for r in kept]
    # Both prepaid rows first (stable order preserved within class),
    # then both metered rows.
    assert modes == ["subscription_included", "prepaid_credit", "metered_api", "metered_api"]
    # Input order preserved within each class
    assert [r["model_slug"] for r in kept] == [
        "gpt-5-cli", "prepaid-api", "gpt-5", "metered-cli",
    ]


def test_lane_policy_uses_default_adapter_when_row_missing_type(patch_loader) -> None:
    patch_loader(_policies_dict())
    # Row without adapter_type should fall back to router default (cli_llm)
    rows = [
        {"provider_slug": "anthropic", "model_slug": "claude-opus"},
    ]
    kept = TaskTypeRouter._apply_lane_policy(_FakeRouter("cli_llm"), "chat", rows)
    assert len(kept) == 1

    # But if default is llm_task and anthropic policy blocks it, row is dropped
    with pytest.raises(TaskRouteAuthorityError):
        TaskTypeRouter._apply_lane_policy(_FakeRouter("llm_task"), "chat", rows)
