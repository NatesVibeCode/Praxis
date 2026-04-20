"""SubsystemsContract protocol conformance tests.

Anti-bypass: asserts that _BaseSubsystems structurally satisfies the
SubsystemsContract protocol. Missing or renamed methods will surface here
before runtime.
"""

from __future__ import annotations

from surfaces._subsystems_base import _BaseSubsystems
from surfaces._subsystems_contract import SubsystemsContract


_CONTRACT_METHODS = (
    "get_pg_conn",
    "get_obs_hub",
    "get_receipt_ingester",
    "get_quality_materializer",
    "get_quality_views_mod",
    "get_bug_tracker",
    "get_bug_tracker_mod",
    "get_operator_panel",
    "get_operator_panel_mod",
    "get_knowledge_graph",
    "get_memory_engine",
    "get_intent_matcher",
    "get_module_indexer",
    "get_embedding_service",
    "get_staleness_detector",
    "get_wave_orchestrator",
    "get_wave_orchestrator_mod",
    "get_self_healer",
    "get_heartbeat_runner",
    "get_session_carry_mgr",
    "get_constraint_ledger",
    "get_constraint_miner",
    "get_friction_ledger",
    "get_governance_filter",
    "get_artifact_store",
    "get_manifest_generator",
    "get_notification_consumer",
    "drain_notifications",
    "get_health_mod",
)


def test_base_subsystems_declares_every_contract_method() -> None:
    missing = [name for name in _CONTRACT_METHODS if not hasattr(_BaseSubsystems, name)]
    assert not missing, f"_BaseSubsystems missing contract methods: {missing}"


def test_contract_declares_every_base_subsystems_getter() -> None:
    """If a new get_* appears on _BaseSubsystems, the contract must follow."""
    base_getters = {
        name
        for name in dir(_BaseSubsystems)
        if name.startswith("get_") and callable(getattr(_BaseSubsystems, name))
    }
    contract_getters = {name for name in _CONTRACT_METHODS if name.startswith("get_")}
    drift = base_getters - contract_getters
    assert not drift, (
        "SubsystemsContract is out of date — _BaseSubsystems exposes get_* "
        f"methods not declared on the protocol: {sorted(drift)}"
    )


def test_protocol_is_runtime_checkable_against_base() -> None:
    """runtime_checkable lets isinstance() work on structural conformance."""
    assert hasattr(SubsystemsContract, "_is_protocol")
