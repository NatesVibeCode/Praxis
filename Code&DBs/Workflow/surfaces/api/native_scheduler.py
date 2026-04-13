"""Native frontdoor for deterministic recurring scheduler inspection.

This surface is intentionally narrow:

- native instance authority is resolved first
- schedule meaning comes from stored runtime rows, not wrapper memory
- ambiguity fails closed instead of inventing a scheduler tie-break
"""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from runtime.instance import NativeWorkflowInstance, resolve_native_instance
from runtime.native_scheduler import (
    NativeSchedulerError,
    NativeSchedulerRepository,
    NativeSchedulerRuntime,
    NativeScheduledWorkflow,
    PostgresNativeSchedulerRepository,
)
from storage.postgres import connect_workflow_database

from ._operator_helpers import _json_compatible, _now, _run_async as _shared_run_async


class _Connection(Protocol):
    async def close(self) -> None:
        """Close the connection."""


def _run_async(awaitable: Awaitable[Any]) -> Any:
    return _shared_run_async(
        awaitable,
        error_type=NativeSchedulerError,
        reason_code="native_scheduler.async_boundary_required",
        message="native scheduler sync entrypoints require a non-async call boundary",
    )


@dataclass(slots=True)
class NativeSchedulerFrontdoor:
    """Thin repo-local frontdoor for recurring scheduler inspection."""

    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    )
    scheduler_repository_factory: Callable[[_Connection], NativeSchedulerRepository] | None = None

    def __post_init__(self) -> None:
        if self.scheduler_repository_factory is None:
            self.scheduler_repository_factory = self._default_repository_factory

    @staticmethod
    def _default_repository_factory(conn: _Connection) -> NativeSchedulerRepository:
        return PostgresNativeSchedulerRepository(conn)  # type: ignore[arg-type]

    def _resolve_instance(self, *, env: Mapping[str, str] | None) -> tuple[Mapping[str, str], NativeWorkflowInstance]:
        source = env if env is not None else os.environ
        return source, resolve_native_instance(env=source)

    async def _inspect_schedule(
        self,
        *,
        env: Mapping[str, str] | None,
        target_ref: str,
        schedule_kind: str,
        as_of: datetime,
    ) -> NativeScheduledWorkflow:
        conn = await self.connect_database(env)
        try:
            assert self.scheduler_repository_factory is not None
            runtime = NativeSchedulerRuntime(
                repository=self.scheduler_repository_factory(conn),
            )
            return await runtime.inspect_schedule(
                target_ref=target_ref,
                schedule_kind=schedule_kind,
                as_of=as_of,
            )
        finally:
            await conn.close()

    def inspect_schedule(
        self,
        *,
        target_ref: str,
        schedule_kind: str,
        env: Mapping[str, str] | None = None,
        as_of: datetime | None = None,
    ) -> dict[str, Any]:
        source, instance = self._resolve_instance(env=env)
        plan = _run_async(
            self._inspect_schedule(
                env=source,
                target_ref=target_ref,
                schedule_kind=schedule_kind,
                as_of=_now() if as_of is None else as_of,
            )
        )
        return {
            "native_instance": instance.to_contract(),
            "schedule": plan.to_json(),
        }


def inspect_schedule(
    *,
    target_ref: str,
    schedule_kind: str,
    env: Mapping[str, str] | None = None,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    """Inspect one native recurring schedule path through repo-local authority."""

    return NativeSchedulerFrontdoor().inspect_schedule(
        target_ref=target_ref,
        schedule_kind=schedule_kind,
        env=env,
        as_of=as_of,
    )


def _parse_as_of(raw_as_of: str | None) -> datetime | None:
    if raw_as_of is None:
        return None
    value = datetime.fromisoformat(raw_as_of)
    if value.tzinfo is None:
        raise NativeSchedulerError(
            "native_scheduler.invalid_request",
            "--as-of must include timezone information",
            details={"value": raw_as_of},
        )
    return value


def _emit(payload: Mapping[str, Any]) -> int:
    json.dump(_json_compatible(payload), os.fdopen(os.dup(1), "w"), indent=2, sort_keys=True)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Repo-local native scheduler frontdoor")
    subparsers = parser.add_subparsers(dest="command", required=True)

    inspect_parser = subparsers.add_parser("inspect")
    inspect_parser.add_argument("--target-ref", required=True)
    inspect_parser.add_argument("--schedule-kind", required=True)
    inspect_parser.add_argument("--as-of")

    args = parser.parse_args(argv)
    if args.command == "inspect":
        payload = inspect_schedule(
            target_ref=args.target_ref,
            schedule_kind=args.schedule_kind,
            as_of=_parse_as_of(args.as_of),
        )
        return _emit(payload)
    raise AssertionError(f"unsupported command: {args.command}")


__all__ = [
    "NativeSchedulerError",
    "NativeSchedulerFrontdoor",
    "inspect_schedule",
    "main",
]


if __name__ == "__main__":  # pragma: no cover - manual operator entrypoint
    raise SystemExit(main())
