from __future__ import annotations

import json
import os
import subprocess
from io import StringIO
from pathlib import Path

from runtime.instance import native_instance_contract
from surfaces.cli.main import main as workflow_cli_main
from tests.integration.native_wrapper_test_support import (
    REPO_ROOT,
    WRAPPER,
    clear_native_contract_env,
    contract_keys,
    contract_map,
    repo_local_env as _repo_local_env,
)

def _probe_fields() -> tuple[str, ...]:
    return ("command", "pythonpath", "profiles_config", *contract_keys())


def _probe_python_script() -> str:
    env_refs = [
        '"${PYTHONPATH-}"',
        '"${PRAXIS_RUNTIME_PROFILES_CONFIG-}"',
        *[f'"${{{name}-}}"' for name in contract_keys()],
    ]
    format_string = "|".join(["%s"] * (1 + len(env_refs))) + r"\n"
    joined_refs = " \\\n    ".join(env_refs)
    return f"""#!/usr/bin/env bash
set -euo pipefail

if [[ "${{1-}}" == "-c" ]]; then
  code="${{2-}}"
  if [[ "$code" == *"sys.version_info[:2] == (3, 14)"* ]]; then
    exit 0
  fi
  shift 2
  printf '{format_string}' \\
    "${{1-}}" \\
    {joined_refs}
  exit 0
fi

if [[ "${{1-}}" == "-" ]]; then
  if [[ -n "${{WORKFLOW_AUTHORITY_JSON-}}" ]]; then
    printf '%s\n%s\n' 'postgresql://postgres@localhost:5432/praxis' 'test'
  else
    printf '{{"database_url":"postgresql://postgres@localhost:5432/praxis","authority_source":"test"}}\n'
  fi
  exit 0
fi

printf 'unexpected invocation: %s\\n' "$*" >&2
exit 1
"""


def _version_probe_only_script() -> str:
    return """#!/usr/bin/env bash
set -euo pipefail

if [[ "${1-}" == "-c" ]]; then
  code="${2-}"
  if [[ "$code" == *"sys.version_info[:2] == (3, 14)"* ]]; then
    exit 0
  fi
fi

printf 'unexpected invocation: %s\\n' "$*" >&2
exit 1
"""


def test_bounded_native_primary_proof(tmp_path: Path) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python3.14"
    fake_python.write_text(_probe_python_script(), encoding="utf-8")
    fake_python.chmod(0o755)

    wrapper_env = os.environ.copy()
    wrapper_env["PATH"] = f"{fake_bin}:{wrapper_env['PATH']}"
    clear_native_contract_env(wrapper_env)

    wrapper_result = subprocess.run(
        [str(WRAPPER)],
        check=True,
        capture_output=True,
        text=True,
        env=wrapper_env,
    )

    probed = dict(zip(_probe_fields(), wrapper_result.stdout.rstrip("\n").split("|"), strict=True))
    contract = contract_map()
    assert probed["command"] == "instance"
    assert probed["pythonpath"].split(":")[0] == str(REPO_ROOT / "Code&DBs" / "Workflow")
    assert probed["profiles_config"] == str(REPO_ROOT / "config" / "runtime_profiles.json")
    for name in contract_keys():
        assert probed[name] == contract[name]

    repo_local = _repo_local_env()
    legacy_env = dict(repo_local)

    blocked_stdout = StringIO()
    assert workflow_cli_main(["native-operator", "start"], env=legacy_env, stdout=blocked_stdout) == 2
    blocked_message = blocked_stdout.getvalue()
    assert "start has been removed" in blocked_message
    assert "workflow native-operator instance" in blocked_message

    instance_stdout = StringIO()
    assert workflow_cli_main(["native-operator", "instance"], env=legacy_env, stdout=instance_stdout) == 0
    instance_payload = json.loads(instance_stdout.getvalue())

    expected_contract = native_instance_contract(env=repo_local)
    assert instance_payload == expected_contract


def test_bounded_native_primary_proof_wrapper_matches_real_instance_surface() -> None:
    wrapper_env = os.environ.copy()
    clear_native_contract_env(wrapper_env)

    wrapper_result = subprocess.run(
        [str(WRAPPER)],
        check=True,
        capture_output=True,
        text=True,
        env=wrapper_env,
    )

    instance_stdout = StringIO()
    assert workflow_cli_main(["native-operator", "instance"], env=_repo_local_env(), stdout=instance_stdout) == 0

    wrapper_payload = json.loads(wrapper_result.stdout)
    instance_payload = json.loads(instance_stdout.getvalue())

    assert wrapper_payload == instance_payload


def test_bounded_native_primary_proof_wrapper_rejects_ambient_contract_drift(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python3.14"
    fake_python.write_text(_version_probe_only_script(), encoding="utf-8")
    fake_python.chmod(0o755)

    wrapper_env = os.environ.copy()
    wrapper_env["PATH"] = f"{fake_bin}:{wrapper_env['PATH']}"
    clear_native_contract_env(wrapper_env)
    wrapper_env["PRAXIS_LOCAL_POSTGRES_DATA_DIR"] = "/tmp/not-the-praxis-contract"

    wrapper_result = subprocess.run(
        [str(WRAPPER)],
        check=False,
        capture_output=True,
        text=True,
        env=wrapper_env,
    )

    assert wrapper_result.returncode == 1
    assert "reject ambient PRAXIS_LOCAL_POSTGRES_DATA_DIR override" in wrapper_result.stderr


def test_bounded_native_primary_proof_wrapper_rejects_remaining_ambient_contract_drift(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python3.14"
    fake_python.write_text(_version_probe_only_script(), encoding="utf-8")
    fake_python.chmod(0o755)

    for env_name in contract_keys():
        wrapper_env = os.environ.copy()
        wrapper_env["PATH"] = f"{fake_bin}:{wrapper_env['PATH']}"
        clear_native_contract_env(wrapper_env)
        wrapper_env[env_name] = f"drifted-{env_name.lower()}"

        wrapper_result = subprocess.run(
            [str(WRAPPER)],
            check=False,
            capture_output=True,
            text=True,
            env=wrapper_env,
        )

        assert wrapper_result.returncode == 1
        assert f"reject ambient {env_name} override" in wrapper_result.stderr


def test_bounded_native_primary_proof_wrapper_rejects_empty_contract_overrides(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python3.14"
    fake_python.write_text(_version_probe_only_script(), encoding="utf-8")
    fake_python.chmod(0o755)

    for env_name in contract_keys():
        wrapper_env = os.environ.copy()
        wrapper_env["PATH"] = f"{fake_bin}:{wrapper_env['PATH']}"
        clear_native_contract_env(wrapper_env)
        wrapper_env[env_name] = ""

        wrapper_result = subprocess.run(
            [str(WRAPPER)],
            check=False,
            capture_output=True,
            text=True,
            env=wrapper_env,
        )

        assert wrapper_result.returncode == 1
        assert f"reject ambient {env_name} override" in wrapper_result.stderr
