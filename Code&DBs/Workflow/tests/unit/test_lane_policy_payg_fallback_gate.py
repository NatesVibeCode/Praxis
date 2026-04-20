"""Regression pin for BUG-2A950857.

Before the fix, ``runtime.lane_policy.admit_adapter_type`` never consulted
the authoritative ``allow_payg_fallback`` flag that
``adapters.provider_transport.AdapterEconomicsContract`` stamps on every
adapter economics row. The flag was threaded through
``runtime.routing_economics.resolve_route_economics`` (output dict key),
through ``runtime.task_type_router.RouteEconomics`` (dataclass field),
through operator health (``runtime.health`` surface), and rendered to
operators as part of route decision metadata — but no selection branch
read it. That made it advisory-only: operators saw a "PAYG fallback
disabled" policy reflected in their dashboards while the runtime happily
admitted those same PAYG lanes, a classic authority-split trap where
the surfaced signal and the enforced signal drift apart.

The fix adds a single admission-time gate in ``admit_adapter_type``:

* Paid lane (``llm_task``) + economics authority explicitly sets
  ``allow_payg_fallback=False`` → refuse with reason code
  ``lane.rejected.payg_fallback_disabled``.

CLI (``cli_llm``) is NOT gated by this flag — subscription-included lanes
declare ``allow_payg_fallback`` to describe the fallback semantics of the
*paid* lane they sit alongside, not of themselves.

``allow_payg_fallback=None`` (default / missing key) keeps admission
permissive — legacy paths that haven't threaded the flag yet do not
regress.

Pins:

1. ``llm_task`` + ``allow_payg_fallback=False`` → rejected with the
   dedicated reason code (the core pin).
2. ``llm_task`` + ``allow_payg_fallback=True`` → admitted (explicit
   permission passes through).
3. ``llm_task`` + ``allow_payg_fallback=None`` → admitted (missing flag
   stays permissive — legacy-path safety).
4. ``cli_llm`` + ``allow_payg_fallback=False`` → admitted (the flag is
   scoped to metered lanes only).
5. The PAYG gate runs AFTER the other paid-lane gates — so a lane that
   is both budget-exhausted AND fallback-disabled reports the pressure
   gate's reason first (preserves gate-ordering contract).
"""
from __future__ import annotations

import pytest

from runtime.lane_policy import ProviderLanePolicy, admit_adapter_type


_POLICIES: dict[str, ProviderLanePolicy] = {
    "openai": ProviderLanePolicy(
        provider_slug="openai",
        allowed_adapter_types=frozenset({"cli_llm", "llm_task"}),
        overridable=True,
    ),
}


# -- 1. core pin: False → rejected ---------------------------------------


def test_llm_task_rejected_when_allow_payg_fallback_false() -> None:
    """The BUG-2A950857 pin.

    Before the fix, admission returned ``(True, "lane.admitted")`` here
    because the flag was never read. After the fix, the dedicated reason
    code surfaces the authority-declared refusal to operators.
    """
    admitted, reason = admit_adapter_type(
        _POLICIES,
        "openai",
        "llm_task",
        allow_payg_fallback=False,
    )
    assert admitted is False
    assert reason == "lane.rejected.payg_fallback_disabled", (
        "paid lane must be refused when economics authority declares "
        "allow_payg_fallback=False"
    )


# -- 2. explicit True passes through -------------------------------------


def test_llm_task_admitted_when_allow_payg_fallback_true() -> None:
    """Explicit opt-in from the economics authority: admission proceeds."""
    admitted, reason = admit_adapter_type(
        _POLICIES,
        "openai",
        "llm_task",
        allow_payg_fallback=True,
    )
    assert admitted is True
    assert reason == "lane.admitted"


# -- 3. None (default / missing) stays permissive ------------------------


def test_llm_task_admitted_when_allow_payg_fallback_is_none() -> None:
    """Callers that don't thread the flag yet (legacy paths, tests with
    synthetic rows that omit the key) must not regress. ``None`` means
    unspecified — admission stays permissive, matching pre-fix behavior."""
    admitted, reason = admit_adapter_type(
        _POLICIES,
        "openai",
        "llm_task",
        allow_payg_fallback=None,
    )
    assert admitted is True
    assert reason == "lane.admitted"

    # And the default (parameter omitted entirely) matches None.
    admitted2, reason2 = admit_adapter_type(_POLICIES, "openai", "llm_task")
    assert admitted2 is True
    assert reason2 == "lane.admitted"


# -- 4. cli_llm is NOT gated by this flag --------------------------------


def test_cli_llm_admitted_even_when_allow_payg_fallback_false() -> None:
    """``allow_payg_fallback`` describes the *paid* lane's fallback status.

    CLI is prepaid-included and not a PAYG bucket, so the gate does not
    apply. A subscription-included row with ``allow_payg_fallback=False``
    still admits ``cli_llm`` — the flag's scope is ``llm_task`` only.
    """
    admitted, reason = admit_adapter_type(
        _POLICIES,
        "openai",
        "cli_llm",
        allow_payg_fallback=False,
    )
    assert admitted is True
    assert reason == "lane.admitted"


# -- 5. gate ordering: earlier paid-lane gates fire first ----------------


def test_payg_fallback_gate_respects_earlier_paid_lane_gates() -> None:
    """When a paid lane would fail multiple gates, the function must
    report the EARLIER gate's reason so operators aren't misled into
    thinking the only blocker is fallback policy when budget authority
    is also unreachable.

    Ordering contract (unchanged by this fix):
        budget_authority_unreachable
        > budget_window_data_quality_error
        > budget_exhausted (spend_pressure=high)
        > payg_fallback_disabled (new)
    """
    # authority-unreachable beats fallback-disabled
    admitted, reason = admit_adapter_type(
        _POLICIES,
        "openai",
        "llm_task",
        budget_authority_unreachable=True,
        allow_payg_fallback=False,
    )
    assert admitted is False
    assert reason == "lane.rejected.budget_authority_unreachable"

    # data-quality error beats fallback-disabled
    admitted, reason = admit_adapter_type(
        _POLICIES,
        "openai",
        "llm_task",
        budget_window_data_quality_error=True,
        allow_payg_fallback=False,
    )
    assert admitted is False
    assert reason == "lane.rejected.budget_window_data_quality_error"

    # spend_pressure=high beats fallback-disabled
    admitted, reason = admit_adapter_type(
        _POLICIES,
        "openai",
        "llm_task",
        spend_pressure="high",
        allow_payg_fallback=False,
    )
    assert admitted is False
    assert reason == "lane.rejected.budget_exhausted"


# -- 6. unknown adapter stays the earliest gate --------------------------


def test_unknown_adapter_still_rejected_before_payg_gate() -> None:
    """``unknown_adapter`` is the very first check. An unknown adapter
    with ``allow_payg_fallback=False`` must still report
    ``lane.rejected.unknown_adapter`` rather than the new PAYG reason."""
    admitted, reason = admit_adapter_type(
        _POLICIES,
        "openai",
        "mystery_lane",
        allow_payg_fallback=False,
    )
    assert admitted is False
    assert reason == "lane.rejected.unknown_adapter"
