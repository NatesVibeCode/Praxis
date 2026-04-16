"""Health helpers for the native workflow frontdoor."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from surfaces._boot import workflow_database_status


def database_status_service(env: Mapping[str, str] | None = None) -> Any:
    return workflow_database_status(env=env)


def database_bootstrap_service(env: Mapping[str, str] | None = None) -> Any:
    return workflow_database_status(env=env, bootstrap=True)


def build_health_payload(
    *,
    resolve_instance: Callable[[Mapping[str, str] | None], tuple[Mapping[str, str], Any]],
    postgres_health_service: Callable[[Mapping[str, str] | None], Any],
    postgres_bootstrap_service: Callable[[Mapping[str, str] | None], Any],
    env: Mapping[str, str] | None = None,
    bootstrap: bool = False,
) -> dict[str, Any]:
    source, instance = resolve_instance(env)
    postgres_status = (
        postgres_bootstrap_service(source)
        if bootstrap
        else postgres_health_service(source)
    )
    return {
        "native_instance": instance.to_contract(),
        "database": postgres_status.to_json(),
    }


__all__ = [
    "build_health_payload",
    "database_bootstrap_service",
    "database_status_service",
]
