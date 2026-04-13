from __future__ import annotations

from pathlib import Path


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]

_ONBOARDING_WRITE_SQL = (
    "INSERT INTO provider_cli_profiles",
    "INSERT INTO provider_transport_admissions",
    "INSERT INTO provider_transport_probe_receipts",
    "INSERT INTO provider_model_candidates",
    "INSERT INTO model_profile_candidate_bindings",
    "INSERT INTO provider_model_market_match_rules",
)


def test_provider_onboarding_authority_writes_live_in_registry_repository() -> None:
    repository_source = (_WORKFLOW_ROOT / "registry" / "provider_onboarding_repository.py").read_text(
        encoding="utf-8"
    )
    for statement in _ONBOARDING_WRITE_SQL:
        assert statement in repository_source


def test_runtime_tree_does_not_own_provider_onboarding_authority_writes() -> None:
    runtime_root = _WORKFLOW_ROOT / "runtime"
    for path in runtime_root.rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        assert not any(statement in source for statement in _ONBOARDING_WRITE_SQL), path


def test_runtime_provider_onboarding_is_only_a_registry_shim() -> None:
    source = (_WORKFLOW_ROOT / "runtime" / "provider_onboarding.py").read_text(encoding="utf-8")
    assert "registry.provider_onboarding" in source
