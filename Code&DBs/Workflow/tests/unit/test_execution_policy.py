from __future__ import annotations

from types import SimpleNamespace

from runtime.workflow.execution_policy import resolve_cli_execution_policy


def test_cli_execution_policy_uses_explicit_sandbox_contract() -> None:
    policy = resolve_cli_execution_policy(
        {
            "sandbox_profile": {
                "network_policy": "disabled",
                "auth_mount_policy": "none",
            }
        },
        profile=SimpleNamespace(
            api_endpoint="https://api.openai.com/v1",
            api_protocol_family="openai_responses",
            api_key_env_vars=("OPENAI_API_KEY",),
        ),
    )

    assert policy.network_policy == "disabled"
    assert policy.network_enabled is False
    assert policy.auth_mount_policy == "none"


def test_cli_execution_policy_uses_lane_metadata_for_cli_only_provider() -> None:
    policy = resolve_cli_execution_policy(
        {},
        profile=SimpleNamespace(
            api_endpoint="",
            api_protocol_family="",
            api_key_env_vars=(),
            lane_policies={
                "cli_llm": {
                    "admitted_by_policy": True,
                    "execution_topology": "local_cli",
                    "transport_kind": "cli",
                }
            },
        ),
    )

    assert policy.network_policy == "provider_only"
    assert policy.network_enabled is True
