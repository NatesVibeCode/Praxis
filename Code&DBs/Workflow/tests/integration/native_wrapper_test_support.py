from __future__ import annotations

from pathlib import Path

from runtime.instance import PRAXIS_RUNTIME_PROFILE_ENV, PRAXIS_RUNTIME_PROFILES_CONFIG_ENV


REPO_ROOT = Path(__file__).resolve().parents[4]
WRAPPER = REPO_ROOT / "scripts" / "native-primary.sh"
EXPECTED_CONTRACT_KEYS = (
    "WORKFLOW_DATABASE_URL",
    "PRAXIS_LOCAL_POSTGRES_DATA_DIR",
    "PRAXIS_RUNTIME_PROFILE",
    "PRAXIS_INSTANCE_NAME",
    "PRAXIS_RECEIPTS_DIR",
    "PRAXIS_TOPOLOGY_DIR",
)


def contract_keys() -> tuple[str, ...]:
    return EXPECTED_CONTRACT_KEYS


def contract_map() -> dict[str, str]:
    """Return a static contract map matching the expected keys.

    The contract file was removed; the canonical values now live in
    the wrapper script and the runtime_profiles config.
    """
    repo = str(REPO_ROOT)
    return {
        "WORKFLOW_DATABASE_URL": f"postgresql://postgres@localhost:5432/praxis",
        "PRAXIS_LOCAL_POSTGRES_DATA_DIR": f"{repo}/Code&DBs/Databases/postgres-dev/data",
        "PRAXIS_RUNTIME_PROFILE": "praxis",
        "PRAXIS_INSTANCE_NAME": "praxis",
        "PRAXIS_RECEIPTS_DIR": f"{repo}/artifacts/runtime_receipts",
        "PRAXIS_TOPOLOGY_DIR": f"{repo}/artifacts/runtime_topology",
    }


def repo_local_env() -> dict[str, str]:
    return {
        PRAXIS_RUNTIME_PROFILES_CONFIG_ENV: str(REPO_ROOT / "config" / "runtime_profiles.json"),
        PRAXIS_RUNTIME_PROFILE_ENV: "praxis",
    }


def clear_native_contract_env(env: dict[str, str]) -> dict[str, str]:
    for name in [PRAXIS_RUNTIME_PROFILES_CONFIG_ENV, *contract_keys()]:
        env.pop(name, None)
    return env
