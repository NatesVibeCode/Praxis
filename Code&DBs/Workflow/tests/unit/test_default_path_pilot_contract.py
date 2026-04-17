from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from adapters.provider_types import ProviderAdapterContract
from runtime.default_path_pilot import DefaultPathPilotError, DefaultPathPilotResolution


def _resolution_stub(*, endpoint_transport_kind: str = "https") -> DefaultPathPilotResolution:
    resolution = object.__new__(DefaultPathPilotResolution)
    object.__setattr__(
        resolution,
        "route_runtime",
        SimpleNamespace(
            route_decision=SimpleNamespace(
                provider_slug="openai",
                model_slug="gpt-5.4",
            ),
            selected_candidate_ref="candidate.openai.default-path.alpha.gpt54",
        ),
    )
    object.__setattr__(
        resolution,
        "endpoint",
        SimpleNamespace(
            provider_endpoint_binding=SimpleNamespace(
                transport_kind=endpoint_transport_kind,
            ),
            provider_endpoint_binding_id="provider_endpoint_binding.default-path.alpha",
        ),
    )
    return resolution


def _resolution_payload_stub() -> DefaultPathPilotResolution:
    resolution = _resolution_stub()
    object.__setattr__(
        resolution,
        "route",
        SimpleNamespace(
            route_eligibility_state_id="eligibility.default_path.alpha",
        ),
    )
    object.__setattr__(
        resolution,
        "failover",
        SimpleNamespace(
            selected_provider_failover_binding_id="provider_failover_binding.default-path.alpha",
            provider_failover_bindings=(
                SimpleNamespace(candidate_ref="candidate.openai.default-path.alpha.gpt54"),
                SimpleNamespace(candidate_ref="candidate.openai.default-path.alpha.gpt54mini"),
            ),
            selected_provider_failover_binding=SimpleNamespace(
                failover_role="primary",
                trigger_rule="health_degraded",
                position_index=0,
            ),
        ),
    )
    object.__setattr__(
        resolution,
        "endpoint",
        SimpleNamespace(
            endpoint_uri="https://api.example.test/v1/chat/completions",
            endpoint_kind="chat_completions",
            provider_endpoint_binding_id="provider_endpoint_binding.default-path.alpha",
            provider_endpoint_binding=SimpleNamespace(
                transport_kind="https",
                request_policy={"timeout_ms": 30_000},
                auth_ref="secret.default-path.openai",
            ),
        ),
    )
    object.__setattr__(resolution, "route_authority", "registry.provider_routing")
    object.__setattr__(resolution, "failover_endpoint_authority", "registry.endpoint_failover")
    object.__setattr__(
        resolution,
        "as_of",
        SimpleNamespace(isoformat=lambda: "2026-04-09T12:00:00+00:00"),
    )
    object.__setattr__(
        resolution,
        "route_runtime",
        SimpleNamespace(
            route_decision_id="route_decision.default_path.alpha",
            selected_candidate_ref="candidate.openai.default-path.alpha.gpt54",
            route_decision=SimpleNamespace(
                provider_ref="provider.openai",
                provider_slug="openai",
                model_slug="gpt-5.4",
                balance_slot=0,
                decision_reason_code="routing.preferred_candidate",
                allowed_candidate_refs=(
                    "candidate.openai.default-path.alpha.gpt54",
                    "candidate.openai.default-path.alpha.gpt54mini",
                ),
            ),
        ),
    )
    return resolution


def _adapter_contract_stub() -> ProviderAdapterContract:
    return ProviderAdapterContract(
        provider_slug="openai",
        adapter_type="llm_task",
        transport_kind="http",
        execution_kind="request",
        failure_namespace="provider.openai",
        prompt_envelope={"protocol_family": "openai_chat_completions"},
        tool_policy={"mode": "catalog"},
        structured_output={"format": "json"},
        timeout_seconds=300,
        telemetry={"authority": "test"},
        retry_policy={"max_attempts": 3},
        failure_mapping={"timeout": "provider.openai.timeout"},
        readiness={"status": "ready"},
        retryable_failure_codes=(
            "provider.openai.timeout",
            "provider.openai.rate_limited",
            "provider.openai.service_unavailable",
        ),
        failover_failure_codes=(
            "provider.openai.timeout",
            "provider.openai.service_unavailable",
        ),
    )


def test_default_path_pilot_accepts_first_party_adapter_contract_with_explicit_failover_codes(
    monkeypatch,
) -> None:
    import runtime.default_path_pilot as default_path_pilot_mod

    resolution = _resolution_stub()
    monkeypatch.setattr(
        default_path_pilot_mod,
        "resolve_adapter_contract",
        lambda *_args, **_kwargs: _adapter_contract_stub(),
    )

    contract = resolution.first_party_provider_adapter_contract()

    assert contract.adapter_type == "llm_task"
    assert contract.transport_kind == "http"
    assert contract.prompt_envelope["protocol_family"] == "openai_chat_completions"
    assert set(contract.failover_failure_codes).issubset(set(contract.retryable_failure_codes))


def test_default_path_pilot_rejects_first_party_contract_without_explicit_failover_codes(
    monkeypatch,
) -> None:
    import runtime.default_path_pilot as default_path_pilot_mod

    resolution = _resolution_stub()
    base_contract = _adapter_contract_stub()
    invalid_contract = replace(base_contract, failover_failure_codes=())
    monkeypatch.setattr(
        default_path_pilot_mod,
        "resolve_adapter_contract",
        lambda *_args, **_kwargs: invalid_contract,
    )

    with pytest.raises(DefaultPathPilotError) as exc_info:
        resolution.first_party_provider_adapter_contract()

    assert exc_info.value.reason_code == "default_path_pilot.adapter_contract_incompatible"
    assert exc_info.value.details == {
        "field": "provider_adapter_contract.failover_failure_codes",
    }


def test_default_path_pilot_payload_exposes_route_and_failover_seams_for_llm_task(
    monkeypatch,
) -> None:
    import runtime.default_path_pilot as default_path_pilot_mod

    resolution = _resolution_payload_stub()
    monkeypatch.setattr(
        default_path_pilot_mod,
        "resolve_adapter_contract",
        lambda *_args, **_kwargs: _adapter_contract_stub(),
    )

    payload = resolution.to_llm_task_input_payload()

    assert payload["route_contract_required"] is True
    assert payload["provider_adapter_contract"]["adapter_type"] == "llm_task"
    assert payload["runtime_route"] == {
        "route_decision_id": "route_decision.default_path.alpha",
        "selected_candidate_ref": "candidate.openai.default-path.alpha.gpt54",
        "provider_ref": "provider.openai",
        "provider_slug": "openai",
        "model_slug": "gpt-5.4",
        "balance_slot": 0,
        "decision_reason_code": "routing.preferred_candidate",
        "allowed_candidate_refs": [
            "candidate.openai.default-path.alpha.gpt54",
            "candidate.openai.default-path.alpha.gpt54mini",
        ],
        "failover_role": "primary",
        "failover_trigger_rule": "health_degraded",
        "failover_position_index": 0,
        "failover_slice_candidate_refs": [
            "candidate.openai.default-path.alpha.gpt54",
            "candidate.openai.default-path.alpha.gpt54mini",
        ],
        "endpoint_kind": "chat_completions",
        "endpoint_transport_kind": "https",
        "route_eligibility_state_id": "eligibility.default_path.alpha",
        "selected_provider_failover_binding_id": "provider_failover_binding.default-path.alpha",
        "provider_endpoint_binding_id": "provider_endpoint_binding.default-path.alpha",
        "route_authority": "registry.provider_routing",
        "failover_endpoint_authority": "registry.endpoint_failover",
        "as_of": "2026-04-09T12:00:00+00:00",
    }
