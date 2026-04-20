"""AdapterEconomicsContract authority contract.

Pins the BUG-8DAA5468 fix: ``prefer_prepaid`` and ``allow_payg_fallback``
normalization lives in exactly one place —
:meth:`adapters.provider_transport.AdapterEconomicsContract.from_raw`.
Sparse rows fail closed there so consumers (routing, health, observability)
never need to default, and no future layer can silently drift its own
defaults from a sibling.
"""
from __future__ import annotations

import pytest

from adapters.provider_transport import (
    AdapterEconomicsAuthorityError,
    AdapterEconomicsContract,
)


def _complete_raw(**overrides) -> dict[str, object]:
    base: dict[str, object] = {
        "billing_mode": "metered_api",
        "budget_bucket": "openai_api_payg",
        "effective_marginal_cost": 1.0,
        "prefer_prepaid": False,
        "allow_payg_fallback": True,
    }
    base.update(overrides)
    return base


def test_from_raw_builds_typed_contract_from_complete_row() -> None:
    contract = AdapterEconomicsContract.from_raw(
        provider_slug="openai",
        adapter_type="llm_task",
        raw=_complete_raw(),
    )
    assert contract.provider_slug == "openai"
    assert contract.adapter_type == "llm_task"
    assert contract.billing_mode == "metered_api"
    assert contract.budget_bucket == "openai_api_payg"
    assert contract.effective_marginal_cost == pytest.approx(1.0)
    assert contract.prefer_prepaid is False
    assert contract.allow_payg_fallback is True


def test_as_dict_round_trips_all_fields() -> None:
    raw = _complete_raw(prefer_prepaid=True, allow_payg_fallback=False)
    contract = AdapterEconomicsContract.from_raw(
        provider_slug="openai",
        adapter_type="cli_llm",
        raw=raw,
    )
    result = contract.as_dict()
    assert result == {
        "billing_mode": "metered_api",
        "budget_bucket": "openai_api_payg",
        "effective_marginal_cost": 1.0,
        "prefer_prepaid": True,
        "allow_payg_fallback": False,
    }


@pytest.mark.parametrize(
    "missing_field",
    ["billing_mode", "budget_bucket", "effective_marginal_cost"],
)
def test_from_raw_rejects_missing_core_fields(missing_field: str) -> None:
    raw = _complete_raw()
    raw.pop(missing_field)
    with pytest.raises(AdapterEconomicsAuthorityError) as excinfo:
        AdapterEconomicsContract.from_raw(
            provider_slug="openai",
            adapter_type="llm_task",
            raw=raw,
        )
    assert missing_field in str(excinfo.value)


@pytest.mark.parametrize(
    "missing_field",
    ["prefer_prepaid", "allow_payg_fallback"],
)
def test_from_raw_rejects_missing_bool_authority_fields(missing_field: str) -> None:
    """The core BUG-8DAA5468 fix: sparse bool fields are hard errors.

    Either authority field being absent or None is a structural gap: we
    cannot prove the intended fallback policy, so we must refuse the row
    rather than silently pick a default that might disagree with a sibling
    layer's default.
    """
    raw = _complete_raw()
    raw.pop(missing_field)
    with pytest.raises(AdapterEconomicsAuthorityError) as excinfo:
        AdapterEconomicsContract.from_raw(
            provider_slug="openai",
            adapter_type="llm_task",
            raw=raw,
        )
    message = str(excinfo.value)
    assert missing_field in message
    assert "BUG-8DAA5468" in message


@pytest.mark.parametrize(
    "missing_field",
    ["prefer_prepaid", "allow_payg_fallback"],
)
def test_from_raw_rejects_none_bool_authority_fields(missing_field: str) -> None:
    """None is treated as missing — not as an implicit False.

    This is the case a sparse DB row surfaces (SELECT returns NULL for
    columns that weren't set). Same fail-closed path as omission.
    """
    raw = _complete_raw(**{missing_field: None})
    with pytest.raises(AdapterEconomicsAuthorityError) as excinfo:
        AdapterEconomicsContract.from_raw(
            provider_slug="openai",
            adapter_type="llm_task",
            raw=raw,
        )
    assert missing_field in str(excinfo.value)


@pytest.mark.parametrize(
    ("field", "bad_value"),
    [
        ("prefer_prepaid", "true"),
        ("prefer_prepaid", 1),
        ("allow_payg_fallback", "false"),
        ("allow_payg_fallback", 0),
    ],
)
def test_from_raw_rejects_non_bool_authority_fields(field: str, bad_value: object) -> None:
    """Strings/ints that look truthy are refused — authority must be typed bool.

    Truthy values can mask wiring bugs: a string "false" is truthy, and a 0
    int passes ``bool()`` truthy checks differently from the DB's typed
    boolean column. Forcing bool typing here keeps the authority unambiguous.
    """
    raw = _complete_raw(**{field: bad_value})
    with pytest.raises(AdapterEconomicsAuthorityError) as excinfo:
        AdapterEconomicsContract.from_raw(
            provider_slug="openai",
            adapter_type="llm_task",
            raw=raw,
        )
    assert field in str(excinfo.value)
    assert "bool" in str(excinfo.value)


def test_contract_is_frozen() -> None:
    """Contract is immutable — consumers can share without defensive copies."""
    contract = AdapterEconomicsContract.from_raw(
        provider_slug="openai",
        adapter_type="llm_task",
        raw=_complete_raw(),
    )
    with pytest.raises((AttributeError, TypeError)):
        contract.allow_payg_fallback = True  # type: ignore[misc]


def test_error_type_is_runtime_error_subclass() -> None:
    """Keeps compatibility for callers that previously caught RuntimeError."""
    assert issubclass(AdapterEconomicsAuthorityError, RuntimeError)
