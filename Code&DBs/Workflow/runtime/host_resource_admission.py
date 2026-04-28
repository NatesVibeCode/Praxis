"""DB-backed host resource admission for local sandbox execution.

Provider slots answer "may we spend this model route?"  This module answers
"may this host safely start another local sandbox?"  It reuses the canonical
``execution_leases`` table by modeling capacity as bounded slot leases:

    host_resource:<host_id>:<resource_name>:slot:<n>

The lease rows are short-lived, inspectable, and automatically reaped by the
existing lease backend before each acquire attempt.
"""

from __future__ import annotations

import os
import re
import socket
import time
from collections.abc import Mapping, Sequence
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Generator

from runtime._workflow_database import resolve_runtime_database_url
from runtime.execution_leases import LeaseHandle, LeaseManager, PostgresLeaseBackend
from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool


_DISABLE_ENV = "PRAXIS_HOST_RESOURCE_ADMISSION_DISABLED"
_HOST_ID_ENV = "PRAXIS_HOST_RESOURCE_ID"
_WAIT_ENV = "PRAXIS_HOST_RESOURCE_WAIT_S"
_TTL_GRACE_ENV = "PRAXIS_HOST_RESOURCE_TTL_GRACE_S"
_DOCKER_SLOTS_ENV = "PRAXIS_HOST_DOCKER_SANDBOX_SLOTS"
_SUBPROCESS_SLOTS_ENV = "PRAXIS_HOST_SUBPROCESS_SLOTS"
_FD_HEAVY_SLOTS_ENV = "PRAXIS_HOST_FD_HEAVY_SLOTS"

_DEFAULT_WAIT_S = 30.0
_DEFAULT_TTL_GRACE_S = 120
_DEFAULT_DOCKER_SLOTS = 2
_DEFAULT_SUBPROCESS_SLOTS = 8
_DEFAULT_FD_HEAVY_SLOTS = 2

RESOURCE_DOCKER_SANDBOX = "sandbox_local_docker"
RESOURCE_SUBPROCESS = "sandbox_subprocess"
RESOURCE_FD_HEAVY = "host_fd_heavy"


def _truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _positive_int_env(name: str, default: int) -> int:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _positive_float_env(name: str, default: float) -> float:
    raw = str(os.environ.get(name, "")).strip()
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _safe_fragment(value: object, *, fallback: str) -> str:
    text = str(value or "").strip() or fallback
    safe = re.sub(r"[^A-Za-z0-9_.:-]+", "_", text)
    return safe.strip("_") or fallback


def default_host_resource_id() -> str:
    configured = str(os.environ.get(_HOST_ID_ENV, "")).strip()
    if configured:
        return _safe_fragment(configured, fallback="host")
    return _safe_fragment(socket.gethostname(), fallback="host")


@dataclass(frozen=True, slots=True)
class HostResourceRequirement:
    resource_name: str
    capacity: int
    slots_required: int = 1


@dataclass(frozen=True, slots=True)
class HostResourceClaim:
    resource_name: str
    resource_key: str
    lease_id: str
    expires_at: str


@dataclass(frozen=True, slots=True)
class HostResourceBundleClaim:
    holder_id: str
    host_id: str
    claims: tuple[HostResourceClaim, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "holder_id": self.holder_id,
            "host_id": self.host_id,
            "claims": [
                {
                    "resource_name": claim.resource_name,
                    "resource_key": claim.resource_key,
                    "lease_id": claim.lease_id,
                    "expires_at": claim.expires_at,
                }
                for claim in self.claims
            ],
        }


class HostResourceAdmissionError(RuntimeError):
    reason_code = "host_resource_admission_error"

    def to_dict(self) -> dict[str, object]:
        return {"reason_code": self.reason_code, "message": str(self)}


class HostResourceCapacityError(HostResourceAdmissionError):
    reason_code = "host_resource_capacity"

    def __init__(
        self,
        *,
        holder_id: str,
        host_id: str,
        resource_name: str,
        capacity: int,
        wait_s: float,
    ) -> None:
        self.holder_id = holder_id
        self.host_id = host_id
        self.resource_name = resource_name
        self.capacity = capacity
        self.wait_s = wait_s
        RuntimeError.__init__(
            self,
            (
                "Host resource at capacity: "
                f"{resource_name} on {host_id} after {wait_s:g}s "
                f"(capacity={capacity}, holder={holder_id})"
            ),
        )

    def to_dict(self) -> dict[str, object]:
        payload = super().to_dict()
        payload.update(
            {
                "holder_id": self.holder_id,
                "host_id": self.host_id,
                "resource_name": self.resource_name,
                "capacity": self.capacity,
                "wait_s": self.wait_s,
            }
        )
        return payload


class HostResourceAdmissionUnavailable(HostResourceAdmissionError):
    reason_code = "host_resource_admission_unavailable"


class HostResourceAdmission:
    """Acquire bounded host-resource slots through ``execution_leases``."""

    def __init__(
        self,
        *,
        host_id: str | None = None,
        manager: LeaseManager | None = None,
        database_configured: bool | None = None,
    ) -> None:
        self.host_id = _safe_fragment(host_id or default_host_resource_id(), fallback="host")
        self._manager = manager
        self._database_configured = database_configured

    @staticmethod
    def default_requirements() -> tuple[HostResourceRequirement, ...]:
        return (
            HostResourceRequirement(
                RESOURCE_DOCKER_SANDBOX,
                _positive_int_env(_DOCKER_SLOTS_ENV, _DEFAULT_DOCKER_SLOTS),
            ),
            HostResourceRequirement(
                RESOURCE_SUBPROCESS,
                _positive_int_env(_SUBPROCESS_SLOTS_ENV, _DEFAULT_SUBPROCESS_SLOTS),
            ),
            HostResourceRequirement(
                RESOURCE_FD_HEAVY,
                _positive_int_env(_FD_HEAVY_SLOTS_ENV, _DEFAULT_FD_HEAVY_SLOTS),
            ),
        )

    def _database_available(self) -> bool:
        if self._database_configured is not None:
            return self._database_configured
        try:
            return bool(resolve_runtime_database_url(required=False))
        except Exception:
            return False

    def _lease_manager(self) -> LeaseManager:
        if self._manager is not None:
            return self._manager
        try:
            conn = SyncPostgresConnection(get_workflow_pool())
            return LeaseManager(backend=PostgresLeaseBackend(conn))
        except Exception as exc:
            raise HostResourceAdmissionUnavailable(str(exc)) from exc

    def _resource_key(self, resource_name: str, slot_index: int) -> str:
        resource = _safe_fragment(resource_name, fallback="resource")
        return f"host_resource:{self.host_id}:{resource}:slot:{slot_index}"

    @staticmethod
    def _handle_from_claim(
        claim: HostResourceClaim,
        *,
        holder_id: str,
    ) -> LeaseHandle:
        return LeaseHandle(
            lease_id=claim.lease_id,
            resource_key=claim.resource_key,
            holder_id=holder_id,
            expires_at=datetime.now(timezone.utc),
        )

    def _try_acquire_requirement(
        self,
        manager: LeaseManager,
        *,
        requirement: HostResourceRequirement,
        holder_id: str,
        ttl_seconds: int,
    ) -> tuple[HostResourceClaim, ...] | None:
        acquired: list[HostResourceClaim] = []
        for _ in range(requirement.slots_required):
            handle: LeaseHandle | None = None
            for slot_index in range(requirement.capacity):
                candidate = manager.acquire(
                    self._resource_key(requirement.resource_name, slot_index),
                    holder_id,
                    ttl_seconds=ttl_seconds,
                )
                if candidate is not None:
                    handle = candidate
                    break
            if handle is None:
                for claim in acquired:
                    manager.release(self._handle_from_claim(claim, holder_id=holder_id))
                return None
            acquired.append(
                HostResourceClaim(
                    resource_name=requirement.resource_name,
                    resource_key=handle.resource_key,
                    lease_id=handle.lease_id,
                    expires_at=handle.expires_at.isoformat(),
                )
            )
        return tuple(acquired)

    def _release_with_manager(
        self,
        manager: LeaseManager,
        claim: HostResourceBundleClaim | None,
    ) -> None:
        if claim is None or not claim.claims:
            return
        for resource_claim in reversed(claim.claims):
            manager.release(self._handle_from_claim(resource_claim, holder_id=claim.holder_id))

    def acquire(
        self,
        *,
        holder_id: str,
        requirements: Sequence[HostResourceRequirement] | None = None,
        wait_s: float | None = None,
        ttl_seconds: int = 300,
    ) -> HostResourceBundleClaim | None:
        if _truthy(os.environ.get(_DISABLE_ENV)):
            return None
        if not self._database_available():
            return None

        manager = self._lease_manager()
        normalized_holder = _safe_fragment(holder_id, fallback="holder")
        normalized_requirements = tuple(requirements or self.default_requirements())
        normalized_wait_s = (
            _positive_float_env(_WAIT_ENV, _DEFAULT_WAIT_S)
            if wait_s is None
            else max(0.0, float(wait_s))
        )
        deadline = time.monotonic() + normalized_wait_s

        while True:
            acquired: list[HostResourceClaim] = []
            blocked_requirement: HostResourceRequirement | None = None
            for requirement in normalized_requirements:
                requirement_claims = self._try_acquire_requirement(
                    manager,
                    requirement=requirement,
                    holder_id=normalized_holder,
                    ttl_seconds=ttl_seconds,
                )
                if requirement_claims is None:
                    blocked_requirement = requirement
                    break
                acquired.extend(requirement_claims)

            if blocked_requirement is None:
                return HostResourceBundleClaim(
                    holder_id=normalized_holder,
                    host_id=self.host_id,
                    claims=tuple(acquired),
                )

            self._release_with_manager(
                manager,
                HostResourceBundleClaim(normalized_holder, self.host_id, tuple(acquired)),
            )
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise HostResourceCapacityError(
                    holder_id=normalized_holder,
                    host_id=self.host_id,
                    resource_name=blocked_requirement.resource_name,
                    capacity=blocked_requirement.capacity,
                    wait_s=normalized_wait_s,
                )
            time.sleep(min(0.25, remaining))

    def release(self, claim: HostResourceBundleClaim | None) -> None:
        if claim is None or not claim.claims:
            return
        self._release_with_manager(self._lease_manager(), claim)

    @contextmanager
    def hold(
        self,
        *,
        holder_id: str,
        requirements: Sequence[HostResourceRequirement] | None = None,
        wait_s: float | None = None,
        ttl_seconds: int = 300,
    ) -> Generator[HostResourceBundleClaim | None, None, None]:
        claim = self.acquire(
            holder_id=holder_id,
            requirements=requirements,
            wait_s=wait_s,
            ttl_seconds=ttl_seconds,
        )
        try:
            yield claim
        finally:
            self.release(claim)


_HOST_RESOURCE_ADMISSION: HostResourceAdmission | None = None


def get_host_resource_admission() -> HostResourceAdmission:
    global _HOST_RESOURCE_ADMISSION
    if _HOST_RESOURCE_ADMISSION is None:
        _HOST_RESOURCE_ADMISSION = HostResourceAdmission()
    return _HOST_RESOURCE_ADMISSION


def sandbox_resource_requirements(
    *,
    sandbox_provider: str,
    execution_transport: str,
    metadata: Mapping[str, object] | None = None,
) -> tuple[HostResourceRequirement, ...]:
    del execution_transport, metadata
    provider = str(sandbox_provider or "").strip()
    if provider != "docker_local":
        return ()

    # Docker-local sandboxes consume the Docker process itself, a subprocess
    # pipe pair, and enough host file descriptors during auth/MCP/workspace
    # setup that they should be admitted as fd-heavy work.
    return HostResourceAdmission.default_requirements()


@contextmanager
def hold_host_resources_for_sandbox(
    *,
    sandbox_provider: str,
    execution_transport: str,
    sandbox_session_id: str,
    timeout_seconds: int,
    metadata: Mapping[str, object] | None = None,
    admission: HostResourceAdmission | None = None,
) -> Generator[HostResourceBundleClaim | None, None, None]:
    requirements = sandbox_resource_requirements(
        sandbox_provider=sandbox_provider,
        execution_transport=execution_transport,
        metadata=metadata,
    )
    if not requirements:
        with nullcontext(None) as claim:
            yield claim
        return

    ttl_seconds = max(60, int(timeout_seconds) + _positive_int_env(_TTL_GRACE_ENV, _DEFAULT_TTL_GRACE_S))
    manager = admission or get_host_resource_admission()
    with manager.hold(
        holder_id=sandbox_session_id,
        requirements=requirements,
        wait_s=_positive_float_env(_WAIT_ENV, _DEFAULT_WAIT_S),
        ttl_seconds=ttl_seconds,
    ) as claim:
        yield claim


__all__ = [
    "HostResourceAdmission",
    "HostResourceAdmissionError",
    "HostResourceAdmissionUnavailable",
    "HostResourceBundleClaim",
    "HostResourceCapacityError",
    "HostResourceClaim",
    "HostResourceRequirement",
    "RESOURCE_DOCKER_SANDBOX",
    "RESOURCE_FD_HEAVY",
    "RESOURCE_SUBPROCESS",
    "default_host_resource_id",
    "get_host_resource_admission",
    "hold_host_resources_for_sandbox",
    "sandbox_resource_requirements",
]
