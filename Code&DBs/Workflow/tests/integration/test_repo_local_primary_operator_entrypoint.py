from __future__ import annotations

import os
import subprocess
from pathlib import Path

from tests.integration.native_wrapper_test_support import (
    REPO_ROOT,
    WRAPPER,
    clear_native_contract_env,
    contract_keys,
    contract_map,
)


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


def _version_probe_only_script(*, succeed: bool = True) -> str:
    status = "0" if succeed else "1"
    return f"""#!/usr/bin/env bash
set -euo pipefail

if [[ "${{1-}}" == "-c" ]]; then
  code="${{2-}}"
  if [[ "$code" == *"sys.version_info[:2] == (3, 14)"* ]]; then
    exit {status}
  fi
fi

printf 'unexpected invocation: %s\\n' "$*" >&2
exit 1
"""


def test_repo_local_primary_operator_entrypoint_resolves_instance_first(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python3.14"
    fake_python.write_text(_probe_python_script(), encoding="utf-8")
    fake_python.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    clear_native_contract_env(env)

    result = subprocess.run(
        [str(WRAPPER)],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )

    command, pythonpath, profiles_config, *contract_values = (
        result.stdout.rstrip("\n").split("|")
    )
    contract = contract_map()
    probed = dict(zip(contract_keys(), contract_values, strict=True))

    assert command == "instance"
    assert pythonpath.split(":")[0] == str(REPO_ROOT / "Code&DBs" / "Workflow")
    assert profiles_config == str(REPO_ROOT / "config" / "runtime_profiles.json")
    for name in contract_keys():
        assert probed[name] == contract[name]


def test_repo_local_primary_operator_entrypoint_requires_python314_and_does_not_fallback(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python3.13"
    fake_python.write_text(
        """#!/usr/bin/env bash
set -euo pipefail

printf 'unexpected fallback to python3.13: %s\\n' "$*" >&2
exit 0
""",
        encoding="utf-8",
    )
    fake_python.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:/usr/bin:/bin"

    result = subprocess.run(
        [str(WRAPPER)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 127
    assert "Python 3.14 on PATH" in result.stderr


def test_repo_local_primary_operator_entrypoint_rejects_mislabeled_python314(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python3.14"
    fake_python.write_text(_version_probe_only_script(succeed=False), encoding="utf-8")
    fake_python.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    clear_native_contract_env(env)

    result = subprocess.run(
        [str(WRAPPER)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 127
    assert "Python 3.14 on PATH" in result.stderr


def test_repo_local_primary_operator_entrypoint_fails_closed_on_ambient_contract_drift(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python3.14"
    fake_python.write_text(_version_probe_only_script(), encoding="utf-8")
    fake_python.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    clear_native_contract_env(env)
    env["WORKFLOW_DATABASE_URL"] = "postgresql://localhost:59999/praxis_test"

    result = subprocess.run(
        [str(WRAPPER)],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 1
    assert "reject ambient WORKFLOW_DATABASE_URL override" in result.stderr


def test_repo_local_primary_operator_entrypoint_fails_closed_on_remaining_contract_drift(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python3.14"
    fake_python.write_text(_version_probe_only_script(), encoding="utf-8")
    fake_python.chmod(0o755)

    for env_name in contract_keys():
        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        clear_native_contract_env(env)
        env[env_name] = f"drifted-{env_name.lower()}"

        result = subprocess.run(
            [str(WRAPPER)],
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert f"reject ambient {env_name} override" in result.stderr


def test_repo_local_primary_operator_entrypoint_fails_closed_on_empty_contract_overrides(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_python = fake_bin / "python3.14"
    fake_python.write_text(_version_probe_only_script(), encoding="utf-8")
    fake_python.chmod(0o755)

    for env_name in contract_keys():
        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        clear_native_contract_env(env)
        env[env_name] = ""

        result = subprocess.run(
            [str(WRAPPER)],
            check=False,
            capture_output=True,
            text=True,
            env=env,
        )

        assert result.returncode == 1
        assert f"reject ambient {env_name} override" in result.stderr
