"""Shared fail-closed provider authority errors."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

from runtime._helpers import _fail


@dataclass(slots=True)
class ProviderAuthorityError(RuntimeError):
    """Typed fail-closed error for provider authority gaps or denials."""

    reason_code: str
    message: str
    details: Mapping[str, Any] | None = field(default=None)

    def __post_init__(self) -> None:
        RuntimeError.__init__(self, self.message)


def provider_authority_fail(
    code: str,
    message: str,
    /,
    **details: Any,
) -> ProviderAuthorityError:
    return _fail(
        code,
        message,
        error_type=ProviderAuthorityError,
        **details,
    )


__all__ = [
    "ProviderAuthorityError",
    "provider_authority_fail",
]
