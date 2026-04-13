from __future__ import annotations

import importlib.util
import sys
import types
from datetime import datetime, timezone
from pathlib import Path

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
from runtime.route_outcomes import RouteOutcome, RouteOutcomeStore


def _now() -> datetime:
    return datetime(2026, 4, 8, 12, 0, tzinfo=timezone.utc)


def test_route_outcomes_track_explicit_route_identity_and_ignore_config_noise() -> None:
    store = RouteOutcomeStore(buffer_size=8)

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


def test_auto_router_skips_only_the_unhealthy_model_route() -> None:
    previous = _auto_router_mod._CANDIDATES
    try:
        refresh_candidates(
            (
                RouteCandidate("openai", "gpt-5.4", "mid", 1),
                RouteCandidate("openai", "gpt-5.4-mini", "mid", 2),
            )
        )
        store = RouteOutcomeStore(buffer_size=8)
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
