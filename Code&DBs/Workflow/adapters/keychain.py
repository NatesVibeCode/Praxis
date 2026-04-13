"""Secret resolution with cross-platform support.

Resolution order:
  1. .env file (repo root)
  2. macOS Keychain (service=praxis, account=<env_var_name>) — Darwin only
  3. os.environ[<env_var_name>]

To store a secret on macOS:
    security add-generic-password -U -a praxis -s <ENV_VAR_NAME> -w <secret_value>

On Linux / other platforms, use .env or environment variables.
"""

from __future__ import annotations

import logging
import os
import pathlib
import subprocess
from functools import lru_cache

logger = logging.getLogger(__name__)

_dotenv_cache: dict[str, str] | None = None


def _load_dotenv() -> dict[str, str]:
    """Load .env from the repo root (walks up looking for .git)."""
    global _dotenv_cache
    if _dotenv_cache is not None:
        return _dotenv_cache
    d = pathlib.Path(__file__).resolve().parent
    while d != d.parent:
        if (d / ".git").exists():
            env_path = d / ".env"
            if env_path.exists():
                result: dict[str, str] = {}
                for line in env_path.read_text().splitlines():
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    result[k.strip()] = v.strip().strip('"').strip("'")
                _dotenv_cache = result
                return result
            break
        d = d.parent
    _dotenv_cache = {}
    return _dotenv_cache

_KEYCHAIN_SERVICE = "praxis"


def keychain_get(env_var_name: str) -> str | None:
    """Retrieve a secret from macOS Keychain by env var name.

    Returns the secret value or None if not found / not on macOS.
    """
    if os.uname().sysname != "Darwin":
        return None
    try:
        result = subprocess.run(
            [
                "security",
                "find-generic-password",
                "-a", _KEYCHAIN_SERVICE,
                "-s", env_var_name,
                "-w",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
        return None
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return None


def keychain_set(env_var_name: str, value: str) -> bool:
    """Store or update a secret in macOS Keychain.

    Returns True on success.
    """
    if os.uname().sysname != "Darwin":
        return False
    try:
        result = subprocess.run(
            [
                "security",
                "add-generic-password",
                "-U",
                "-a", _KEYCHAIN_SERVICE,
                "-s", env_var_name,
                "-w", value,
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


def keychain_delete(env_var_name: str) -> bool:
    """Remove a secret from macOS Keychain. Returns True on success."""
    if os.uname().sysname != "Darwin":
        return False
    try:
        result = subprocess.run(
            [
                "security",
                "delete-generic-password",
                "-a", _KEYCHAIN_SERVICE,
                "-s", env_var_name,
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


@lru_cache(maxsize=32)
def _keychain_available() -> bool:
    """Check if macOS Keychain is usable (on macOS + security binary exists)."""
    if os.uname().sysname != "Darwin":
        return False
    try:
        result = subprocess.run(
            ["security", "help"],
            capture_output=True,
            timeout=3,
        )
        return result.returncode in (0, 1)  # security help exits 1 but works
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False


def resolve_secret(env_var_name: str, *, env: dict[str, str] | None = None) -> str | None:
    """Resolve a secret: .env → Keychain → environment variable.

    This is the standard resolution function — use this instead of
    raw os.environ.get() for any secret/API key lookup.
    """
    # 1. .env file
    dotenv = _load_dotenv()
    value = dotenv.get(env_var_name, "").strip()
    if value:
        return value

    # 2. macOS Keychain
    if _keychain_available():
        value = keychain_get(env_var_name)
        if value:
            return value

    # 3. Environment variable
    source = env if env is not None else os.environ
    value = source.get(env_var_name, "").strip()
    return value or None


def require_secret(env_var_name: str) -> str:
    """Resolve a secret or raise with a helpful error message."""
    value = resolve_secret(env_var_name)
    if not value:
        raise RuntimeError(
            f"{env_var_name} not found. Set it in .env, environment, "
            f"or macOS Keychain (service=praxis)"
        )
    return value
