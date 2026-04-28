"""Regression tests for BUG-3C9ECE97 — provider slot acquisition failures
must fail closed with a structured runtime error instead of silently
entering ``nullcontext(True)``.

Before this fix, ``runtime/workflow/execution_backends.py::_provider_slot``
swallowed any exception from ``get_load_balancer().slot(provider_slug)``
into ``nullcontext(True)`` — i.e. infrastructure outages (load balancer
offline, admission DB unreachable, etc.) were silently translated into
"admission granted". That hid broken control state, let ``execute_cli``
and ``execute_api`` run as if provider quota had been acquired, and
could overcommit providers.

The fix introduces :class:`ProviderSlotAcquisitionError` and a
structured :func:`_provider_slot_acquisition_failure` dict with
``error_code=provider_slot_acquisition_error``. Both call sites
(``execute_cli`` and ``execute_api``) now catch the error and return
the structured failure without entering the body.

The bypass contextvar (``provider_slot_bypass``) remains — that is the
explicit nested-call escape hatch. The debate on this bug settled on:
bypass = explicit caller opt-in; infra failure = never a bypass.
"""

from __future__ import annotations

from contextlib import nullcontext
from types import SimpleNamespace
from typing import Any

import pytest

from runtime.workflow import execution_backends as eb
from runtime.workflow.execution_backends import (
    ProviderSlotAcquisitionError,
    _provider_slot,
    _provider_slot_acquisition_failure,
    provider_slot_bypass,
)
from runtime.host_resource_admission import (
    HostResourceAdmissionUnavailable,
    HostResourceCapacityError,
)


# ------------------------------------------------------------------- helpers


class _HealthySlotCM:
    def __init__(self, granted: bool):
        self._granted = granted

    def __enter__(self):
        return self._granted

    def __exit__(self, *args):
        return False


class _HealthyLoadBalancer:
    def __init__(self, granted: bool = True):
        self._granted = granted
        self.calls: list[str] = []

    def slot(self, provider_slug: str):
        self.calls.append(provider_slug)
        return _HealthySlotCM(self._granted)


class _BrokenLoadBalancer:
    def __init__(self, exc: BaseException | None = None):
        self.exc = exc or RuntimeError("admission DB unreachable")
        self.calls: list[str] = []

    def slot(self, provider_slug: str):
        self.calls.append(provider_slug)
        raise self.exc


# --------------------------------------------------- unit: _provider_slot


def test_provider_slot_bypass_returns_nullcontext_true(monkeypatch):
    """The explicit bypass escape hatch stays working — nested callers
    that already hold the parent's slot must be able to opt out."""
    bad = _BrokenLoadBalancer()
    monkeypatch.setattr(eb, "get_load_balancer", lambda: bad)

    with provider_slot_bypass():
        cm = _provider_slot("anthropic")

    with cm as acquired:
        assert acquired is True
    # Bypass short-circuits before any load-balancer call
    assert bad.calls == []


def test_provider_slot_empty_provider_returns_nullcontext_true(monkeypatch):
    """Empty provider_slug also short-circuits (no provider to meter)."""
    bad = _BrokenLoadBalancer()
    monkeypatch.setattr(eb, "get_load_balancer", lambda: bad)

    cm = _provider_slot("")
    with cm as acquired:
        assert acquired is True
    assert bad.calls == []


def test_provider_slot_healthy_returns_balancer_cm(monkeypatch):
    """Positive control: a healthy load balancer's CM is returned unchanged."""
    healthy = _HealthyLoadBalancer(granted=True)
    monkeypatch.setattr(eb, "get_load_balancer", lambda: healthy)

    cm = _provider_slot("anthropic")
    with cm as acquired:
        assert acquired is True
    assert healthy.calls == ["anthropic"]


def test_provider_slot_raises_on_load_balancer_exception(monkeypatch):
    """The BUG-3C9ECE97 fix: infrastructure failure is NOT a silent
    admission-granted path. The exception must surface as a typed
    :class:`ProviderSlotAcquisitionError` so call sites can fail closed."""
    underlying = RuntimeError("admission DB unreachable")
    broken = _BrokenLoadBalancer(exc=underlying)
    monkeypatch.setattr(eb, "get_load_balancer", lambda: broken)

    with pytest.raises(ProviderSlotAcquisitionError) as excinfo:
        _provider_slot("anthropic")

    err = excinfo.value
    assert err.provider_slug == "anthropic"
    # Cause chain must preserve the underlying exception for diagnosis.
    assert err.__cause__ is underlying


def test_provider_slot_does_not_return_nullcontext_true_on_failure(monkeypatch):
    """Belt-and-suspenders against regression: if someone in the future
    re-introduces the 'swallow as nullcontext(True)' pattern, this test
    fails. We check by routing the exception into a sentinel and asserting
    we never see a True-valued context manager on the failure path."""
    monkeypatch.setattr(eb, "get_load_balancer", lambda: _BrokenLoadBalancer())

    try:
        cm = _provider_slot("anthropic")
    except ProviderSlotAcquisitionError:
        return  # expected path
    # If we got here, _provider_slot silently swallowed — the original bug.
    with cm as acquired:  # pragma: no cover
        pytest.fail(
            f"expected ProviderSlotAcquisitionError on broken load balancer; "
            f"got context manager yielding acquired={acquired!r}"
        )


# ------------------------------ unit: _provider_slot_acquisition_failure


def test_provider_slot_acquisition_failure_has_structured_shape():
    """The structured failure dict must carry a distinct error_code so
    dashboards can tell infra outage apart from ordinary capacity pressure."""
    exc = RuntimeError("admission DB unreachable")
    out = _provider_slot_acquisition_failure("anthropic", exc)

    assert out["status"] == "failed"
    assert out["exit_code"] == 1
    assert out["error_code"] == "provider_slot_acquisition_error"
    # The distinction: route health, provider capacity, and slot acquisition
    # have separate failure codes.
    assert out["error_code"] != "route.unhealthy"
    assert out["error_code"] != "provider.capacity"
    assert "anthropic" in out["stderr"]
    assert "admission DB unreachable" in out["stderr"]


# ------------------------------- integration: execute_cli / execute_api


def _minimal_agent_config(provider: str):
    # Only the attributes _provider_slot / early guards read. We don't run
    # the full body — we intercept before sandbox dispatch.
    return SimpleNamespace(
        provider=provider,
        model="test-model",
        wrapper_command=None,
        max_output_tokens=256,
        timeout_seconds=10,
        sandbox_policy=None,
    )


def test_execute_cli_returns_structured_failure_on_load_balancer_down(monkeypatch):
    """Integration: if ``get_load_balancer().slot()`` raises, ``execute_cli``
    must return the structured ``provider_slot_acquisition_error`` dict
    and must NOT invoke any downstream sandbox machinery."""
    monkeypatch.setattr(eb, "get_load_balancer", lambda: _BrokenLoadBalancer())

    # Poison build_command — if execute_cli ever progresses past the slot
    # acquisition, we'd see this raise. That proves the body is gated.
    def _never_called(*args, **kwargs):
        raise AssertionError(
            "execute_cli must return before sandbox dispatch when slot acquisition fails"
        )

    monkeypatch.setattr(eb, "build_command", _never_called)

    result = eb.execute_cli(
        _minimal_agent_config("anthropic"),
        prompt="irrelevant",
        workdir="/tmp",
    )
    assert result["status"] == "failed"
    assert result["error_code"] == "provider_slot_acquisition_error"
    assert "anthropic" in result["stderr"]


def test_execute_api_returns_structured_failure_on_load_balancer_down(monkeypatch):
    """Integration: same contract for ``execute_api``. The auto/review
    and auto/build hot paths both use ``execute_api`` (HTTP transport),
    so this is the path that overcommits if the bug returns."""
    monkeypatch.setattr(eb, "get_load_balancer", lambda: _BrokenLoadBalancer())

    # Poison the profile resolution in the same way — if we get past the
    # slot acquisition, we hit this sentinel.
    def _never_called(*args, **kwargs):
        raise AssertionError(
            "execute_api must return before transport dispatch when slot acquisition fails"
        )

    # Patch provider_execution_registry.get_profile on the module where it
    # is imported at call time — execute_api does a local import.
    from registry import provider_execution_registry as reg

    monkeypatch.setattr(reg, "get_profile", _never_called)

    result = eb.execute_api(
        _minimal_agent_config("anthropic"),
        prompt="irrelevant",
        workdir="/tmp",
    )
    assert result["status"] == "failed"
    assert result["error_code"] == "provider_slot_acquisition_error"
    assert "anthropic" in result["stderr"]


def test_capacity_path_uses_provider_capacity(monkeypatch):
    """Regression guard: ordinary capacity pressure (the load balancer
    IS healthy but says 'no slot right now') must continue returning
    ``provider.capacity`` — not route-health or slot-acquisition failures.
    Conflating those would lose the distinction the fix introduced."""
    healthy_but_full = _HealthyLoadBalancer(granted=False)
    monkeypatch.setattr(eb, "get_load_balancer", lambda: healthy_but_full)

    result = eb.execute_cli(
        _minimal_agent_config("anthropic"),
        prompt="irrelevant",
        workdir="/tmp",
    )
    assert result["status"] == "failed"
    assert result["error_code"] == "provider.capacity"
    assert result["error_code"] != "route.unhealthy"
    assert result["error_code"] != "provider_slot_acquisition_error"


def test_execute_cli_returns_structured_failure_on_host_resource_capacity(monkeypatch):
    """Host-resource admission is a second gate after provider admission.
    Capacity pressure there must surface as its own retryable code instead of
    collapsing into sandbox_error."""
    monkeypatch.setattr(eb, "get_load_balancer", lambda: _HealthyLoadBalancer(granted=True))
    monkeypatch.setattr(eb, "build_command", lambda **_kwargs: ["provider-cli"])
    monkeypatch.setattr(eb, "normalize_command_parts_for_docker", lambda parts: list(parts))
    monkeypatch.setattr(eb, "_sandbox_provider_for_execution", lambda *_args, **_kwargs: "docker_local")
    monkeypatch.setattr(eb, "_sandbox_image", lambda *_args, **_kwargs: "praxis-worker:test")

    def _capacity_block(*_args, **_kwargs):
        raise HostResourceCapacityError(
            holder_id="sandbox_session:run.alpha:job.one",
            host_id="unit-host",
            resource_name="sandbox_local_docker",
            capacity=1,
            wait_s=0,
        )

    monkeypatch.setattr(eb, "hold_host_resources_for_sandbox", _capacity_block)

    result = eb.execute_cli(
        _minimal_agent_config("anthropic"),
        prompt="irrelevant",
        workdir="/tmp",
    )

    assert result["status"] == "failed"
    assert result["error_code"] == "host_resource_capacity"
    assert result["host_resource_admission"]["resource_name"] == "sandbox_local_docker"


def test_execute_cli_returns_structured_failure_on_host_resource_unavailable(monkeypatch):
    monkeypatch.setattr(eb, "get_load_balancer", lambda: _HealthyLoadBalancer(granted=True))
    monkeypatch.setattr(eb, "build_command", lambda **_kwargs: ["provider-cli"])
    monkeypatch.setattr(eb, "normalize_command_parts_for_docker", lambda parts: list(parts))
    monkeypatch.setattr(eb, "_sandbox_provider_for_execution", lambda *_args, **_kwargs: "docker_local")
    monkeypatch.setattr(eb, "_sandbox_image", lambda *_args, **_kwargs: "praxis-worker:test")

    def _admission_unavailable(*_args, **_kwargs):
        raise HostResourceAdmissionUnavailable("lease DB unavailable")

    monkeypatch.setattr(eb, "hold_host_resources_for_sandbox", _admission_unavailable)

    result = eb.execute_cli(
        _minimal_agent_config("anthropic"),
        prompt="irrelevant",
        workdir="/tmp",
    )

    assert result["status"] == "failed"
    assert result["error_code"] == "host_resource_admission_unavailable"
    assert result["host_resource_admission"]["message"] == "lease DB unavailable"
