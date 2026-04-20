from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest

_runtime_pkg = types.ModuleType("runtime")
_runtime_pkg.__path__ = [str(Path(__file__).resolve().parents[2] / "runtime")]
sys.modules.setdefault("runtime", _runtime_pkg)

_route_outcomes_spec = importlib.util.spec_from_file_location(
    "runtime.route_outcomes",
    Path(__file__).resolve().parents[2] / "runtime" / "route_outcomes.py",
)
_route_outcomes_mod = importlib.util.module_from_spec(_route_outcomes_spec)  # type: ignore[arg-type]
sys.modules["runtime.route_outcomes"] = _route_outcomes_mod
_route_outcomes_spec.loader.exec_module(_route_outcomes_mod)  # type: ignore[union-attr]

_auto_router_spec = importlib.util.spec_from_file_location(
    "runtime.auto_router",
    Path(__file__).resolve().parents[2] / "runtime" / "auto_router.py",
)
_auto_router_mod = importlib.util.module_from_spec(_auto_router_spec)  # type: ignore[arg-type]
sys.modules["runtime.auto_router"] = _auto_router_mod
_auto_router_spec.loader.exec_module(_auto_router_mod)  # type: ignore[union-attr]

from runtime.auto_router import RouteCandidate, refresh_candidates, resolve_route
from runtime.route_outcomes import (
    RouteOutcome,
    RouteOutcomeAuthorityError,
    RouteOutcomeStore,
)


def _now() -> datetime:
    return datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)


class _FakeMetricsView:
    def recent_route_outcomes(
        self,
        *,
        provider_slug: str,
        model_slug: str | None = None,
        adapter_type: str | None = None,
        limit: int = 20,  # noqa: ARG002
    ) -> list[dict[str, object]]:
        if provider_slug != "openai":
            return []
        if model_slug not in (None, "gpt-5.4"):
            return []
        if adapter_type not in (None, "cli_llm"):
            return []
        return [
            {
                "run_id": "run-db",
                "provider_slug": "openai",
                "model_slug": "gpt-5.4",
                "adapter_type": "cli_llm",
                "status": "succeeded",
                "failure_code": None,
                "failure_category": "",
                "latency_ms": 5,
                "created_at": datetime(2026, 4, 8, 11, 59, tzinfo=timezone.utc),
            }
        ]

    def provider_slugs(self) -> tuple[str, ...]:
        return ("anthropic", "openai")


class _EmptyMetricsView:
    def recent_route_outcomes(
        self,
        *,
        provider_slug: str,  # noqa: ARG002
        model_slug: str | None = None,  # noqa: ARG002
        adapter_type: str | None = None,  # noqa: ARG002
        limit: int = 20,  # noqa: ARG002
    ) -> list[dict[str, object]]:
        return []

    def provider_slugs(self) -> tuple[str, ...]:
        return ()


class _ExplodingMetricsView:
    def recent_route_outcomes(
        self,
        *,
        provider_slug: str,  # noqa: ARG002
        model_slug: str | None = None,  # noqa: ARG002
        adapter_type: str | None = None,  # noqa: ARG002
        limit: int = 20,  # noqa: ARG002
    ) -> list[dict[str, object]]:
        raise RuntimeError("recent_route_outcomes failed")

    def provider_slugs(self) -> tuple[str, ...]:
        raise RuntimeError("provider_slugs failed")


def test_route_outcomes_track_explicit_route_identity_and_ignore_config_noise() -> None:
    store = RouteOutcomeStore(
        buffer_size=8,
        metrics_view_factory=lambda: _EmptyMetricsView(),
    )

    store.record_outcome(
        RouteOutcome(
            provider_slug="openai",
            model_slug="gpt-5.4",
            adapter_type="cli_llm",
            status="failed",
            failure_code="credential_error",
            failure_category="credential_error",
            latency_ms=10,
            recorded_at=_now(),
        )
    )
    store.record_outcome(
        RouteOutcome(
            provider_slug="openai",
            model_slug="gpt-5.4",
            adapter_type="cli_llm",
            status="failed",
            failure_code="verification_failed",
            failure_category="verification_failed",
            latency_ms=12,
            recorded_at=_now(),
        )
    )

    recent = store.recent_outcomes("openai", model_slug="gpt-5.4")
    assert [outcome.failure_category for outcome in recent] == [
        "verification_failed",
        "credential_error",
    ]
    assert recent[0].route_key == "openai/gpt-5.4@cli_llm"
    assert store.consecutive_failures("openai", model_slug="gpt-5.4") == 1
    assert store.is_route_healthy(
        "openai",
        model_slug="gpt-5.4",
        max_consecutive_failures=2,
    )


def test_route_outcomes_merge_durable_metrics_with_local_overlay() -> None:
    store = RouteOutcomeStore(
        buffer_size=8,
        metrics_view_factory=lambda: _FakeMetricsView(),
    )

    store.record_outcome(
        RouteOutcome(
            provider_slug="openai",
            model_slug="gpt-5.4",
            adapter_type="cli_llm",
            status="failed",
            failure_code="verification_failed",
            failure_category="verification_failed",
            latency_ms=10,
            recorded_at=_now(),
            run_id="run-live",
        )
    )

    recent = store.recent_outcomes("openai", model_slug="gpt-5.4", adapter_type="cli_llm")
    assert [outcome.run_id for outcome in recent] == ["run-live", "run-db"]
    assert store.consecutive_failures("openai", model_slug="gpt-5.4") == 1
    assert store.provider_slugs() == ("anthropic", "openai")


def test_route_outcomes_summary_includes_recent_health() -> None:
    store = RouteOutcomeStore(
        buffer_size=8,
        metrics_view_factory=lambda: _EmptyMetricsView(),
    )

    store.record_outcome(
        RouteOutcome(
            provider_slug="openai",
            model_slug="gpt-5.4",
            adapter_type="cli_llm",
            status="failed",
            failure_code="verification_failed",
            failure_category="verification_failed",
            latency_ms=10,
            recorded_at=_now(),
        )
    )
    store.record_outcome(
        RouteOutcome(
            provider_slug="openai",
            model_slug="gpt-5.4",
            adapter_type="cli_llm",
            status="succeeded",
            failure_code=None,
            failure_category="",
            latency_ms=8,
            recorded_at=_now(),
        )
    )

    summary = store.summary(recent_limit=2, max_consecutive_failures=2)

    assert summary["provider_count"] == 1
    assert summary["healthy_provider_count"] == 1
    assert summary["unhealthy_provider_count"] == 0
    assert summary["provider_slugs"] == ["openai"]
    assert summary["providers"][0]["provider_slug"] == "openai"
    assert summary["providers"][0]["consecutive_failures"] == 1
    assert summary["providers"][0]["healthy"] is True
    assert [item["status"] for item in summary["providers"][0]["recent_outcomes"]] == [
        "succeeded",
        "failed",
    ]
    assert summary["providers"][0]["recent_outcomes"][0]["recorded_at"] == _now().isoformat()
    assert summary["max_consecutive_failures"] == 2


def test_route_outcomes_surface_observability_import_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(
        sys.modules,
        "runtime.observability",
        types.ModuleType("runtime.observability"),
    )
    store = RouteOutcomeStore(buffer_size=8)

    with pytest.raises(RouteOutcomeAuthorityError) as exc_info:
        store.recent_outcomes("openai")

    assert exc_info.value.reason_code == "route_outcomes.metrics_view_unavailable"
    assert exc_info.value.operation == "metrics_view"
    assert exc_info.value.error_type == "ImportError"


def test_route_outcomes_surface_metrics_view_factory_failures() -> None:
    def _raise_metrics_view_error() -> _ExplodingMetricsView:
        raise RuntimeError("metrics view failed")

    store = RouteOutcomeStore(
        buffer_size=8,
        metrics_view_factory=_raise_metrics_view_error,
    )

    with pytest.raises(RouteOutcomeAuthorityError) as exc_info:
        store.recent_outcomes("openai")

    assert exc_info.value.reason_code == "route_outcomes.metrics_view_unavailable"
    assert exc_info.value.operation == "metrics_view"
    assert exc_info.value.error_type == "RuntimeError"
    assert exc_info.value.error_message == "metrics view failed"


def test_route_outcomes_surface_metrics_query_failures() -> None:
    store = RouteOutcomeStore(
        buffer_size=8,
        metrics_view_factory=lambda: _ExplodingMetricsView(),
    )

    with pytest.raises(RouteOutcomeAuthorityError) as recent_exc:
        store.recent_outcomes("openai")
    with pytest.raises(RouteOutcomeAuthorityError) as provider_exc:
        store.provider_slugs()

    assert recent_exc.value.reason_code == "route_outcomes.recent_outcomes_unavailable"
    assert recent_exc.value.operation == "recent_route_outcomes"
    assert recent_exc.value.error_message == "recent_route_outcomes failed"
    assert provider_exc.value.reason_code == "route_outcomes.provider_slugs_unavailable"
    assert provider_exc.value.operation == "provider_slugs"
    assert provider_exc.value.error_message == "provider_slugs failed"


def test_auto_router_skips_only_the_unhealthy_model_route() -> None:
    previous = _auto_router_mod._CANDIDATES
    try:
        refresh_candidates(
            (
                RouteCandidate("openai", "gpt-5.4", "mid", 1),
                RouteCandidate("openai", "gpt-5.4-mini", "mid", 2),
            )
        )
        store = RouteOutcomeStore(
            buffer_size=8,
            metrics_view_factory=lambda: _EmptyMetricsView(),
        )
        store.record_outcome(
            RouteOutcome(
                provider_slug="openai",
                model_slug="gpt-5.4",
                adapter_type="cli_llm",
                status="failed",
                failure_code="verification_failed",
                failure_category="verification_failed",
                latency_ms=10,
                recorded_at=_now(),
            )
        )

        decision = resolve_route("mid", route_outcomes=store, max_consecutive_failures=1)

        assert decision.provider_slug == "openai"
        assert decision.model_slug == "gpt-5.4-mini"
    finally:
        refresh_candidates(previous)


def test_auto_router_raises_when_all_routes_are_unhealthy() -> None:
    previous = _auto_router_mod._CANDIDATES
    try:
        refresh_candidates(
            (
                RouteCandidate("openai", "gpt-5.4", "mid", 1),
                RouteCandidate("anthropic", "claude-sonnet-4", "frontier", 1),
                RouteCandidate("openai", "gpt-5.4-mini", "economy", 1),
            )
        )
        store = RouteOutcomeStore(
            buffer_size=8,
            metrics_view_factory=lambda: _EmptyMetricsView(),
        )
        for provider_slug, model_slug in (
            ("openai", "gpt-5.4"),
            ("anthropic", "claude-sonnet-4"),
            ("openai", "gpt-5.4-mini"),
        ):
            store.record_outcome(
                RouteOutcome(
                    provider_slug=provider_slug,
                    model_slug=model_slug,
                    adapter_type="cli_llm",
                    status="failed",
                    failure_code="verification_failed",
                    failure_category="verification_failed",
                    latency_ms=10,
                    recorded_at=_now(),
                )
            )

        with pytest.raises(RuntimeError, match="no healthy candidates available"):
            resolve_route("auto", route_outcomes=store, max_consecutive_failures=1)
    finally:
        refresh_candidates(previous)
