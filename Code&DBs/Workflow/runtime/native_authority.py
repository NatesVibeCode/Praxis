"""Shared native authority defaults for setup and factory entrypoints.

These helpers intentionally fail closed when the native runtime-profile
document is missing or malformed. Setup/factory code may rely on these
defaults, but it must not fabricate fallback refs.
"""

from __future__ import annotations

from registry.native_runtime_profile_sync import (
    default_native_runtime_profile_ref,
    default_native_workspace_ref,
)


def default_native_authority_refs() -> tuple[str, str]:
    """Return the canonical default workspace/runtime-profile refs."""

    workspace_ref = str(default_native_workspace_ref()).strip()
    runtime_profile_ref = str(default_native_runtime_profile_ref()).strip()
    if not workspace_ref:
        raise RuntimeError("default native workspace_ref is empty")
    if not runtime_profile_ref:
        raise RuntimeError("default native runtime_profile_ref is empty")
    return workspace_ref, runtime_profile_ref


def default_native_runtime_profile_ref_required() -> str:
    """Return the canonical default runtime_profile_ref."""

    return default_native_authority_refs()[1]


__all__ = [
    "default_native_authority_refs",
    "default_native_runtime_profile_ref_required",
]
