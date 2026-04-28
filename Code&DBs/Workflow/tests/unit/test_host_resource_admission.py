from __future__ import annotations

from datetime import datetime

import pytest

from runtime.execution_leases import LeaseInfo, LeaseManager
from runtime.host_resource_admission import (
    HostResourceAdmission,
    HostResourceCapacityError,
    HostResourceRequirement,
    RESOURCE_DOCKER_SANDBOX,
    hold_host_resources_for_sandbox,
    sandbox_resource_requirements,
)


class _MemoryLeaseBackend:
    def __init__(self) -> None:
        self.leases: dict[str, LeaseInfo] = {}

    def reap_expired(self, now: datetime) -> int:
        expired = [
            resource_key
            for resource_key, lease in self.leases.items()
            if lease.expires_at <= now
        ]
        for resource_key in expired:
            del self.leases[resource_key]
        return len(expired)

    def try_acquire(
        self,
        lease_id: str,
        resource_key: str,
        holder_id: str,
        now: datetime,
        expires_at: datetime,
    ) -> bool:
        if resource_key in self.leases:
            return False
        self.leases[resource_key] = LeaseInfo(
            lease_id=lease_id,
            resource_key=resource_key,
            holder_id=holder_id,
            acquired_at=now,
            expires_at=expires_at,
            renewed_at=None,
        )
        return True

    def release(self, lease_id: str) -> bool:
        for resource_key, lease in list(self.leases.items()):
            if lease.lease_id == lease_id:
                del self.leases[resource_key]
                return True
        return False

    def renew(self, lease_id: str, new_expires: datetime, now: datetime) -> bool:
        del now
        for resource_key, lease in list(self.leases.items()):
            if lease.lease_id == lease_id:
                self.leases[resource_key] = LeaseInfo(
                    lease_id=lease.lease_id,
                    resource_key=lease.resource_key,
                    holder_id=lease.holder_id,
                    acquired_at=lease.acquired_at,
                    expires_at=new_expires,
                    renewed_at=new_expires,
                )
                return True
        return False

    def is_held(self, resource_key: str, now: datetime) -> LeaseInfo | None:
        lease = self.leases.get(resource_key)
        if lease is None or lease.expires_at <= now:
            return None
        return lease


def _admission() -> tuple[HostResourceAdmission, _MemoryLeaseBackend]:
    backend = _MemoryLeaseBackend()
    manager = LeaseManager(backend=backend)
    return HostResourceAdmission(
        host_id="unit-host",
        manager=manager,
        database_configured=True,
    ), backend


def test_host_resource_admission_models_capacity_as_db_slot_leases() -> None:
    admission, backend = _admission()
    requirement = HostResourceRequirement(RESOURCE_DOCKER_SANDBOX, capacity=1)

    first = admission.acquire(
        holder_id="job-one",
        requirements=(requirement,),
        wait_s=0,
    )

    assert first is not None
    assert len(first.claims) == 1
    assert first.claims[0].resource_key == (
        "host_resource:unit-host:sandbox_local_docker:slot:0"
    )

    with pytest.raises(HostResourceCapacityError):
        admission.acquire(
            holder_id="job-two",
            requirements=(requirement,),
            wait_s=0,
        )

    admission.release(first)
    second = admission.acquire(
        holder_id="job-two",
        requirements=(requirement,),
        wait_s=0,
    )

    assert second is not None
    assert len(backend.leases) == 1
    admission.release(second)
    assert backend.leases == {}


def test_host_resource_hold_releases_all_claims_on_exit() -> None:
    admission, backend = _admission()
    requirements = (
        HostResourceRequirement(RESOURCE_DOCKER_SANDBOX, capacity=2),
        HostResourceRequirement("subprocess", capacity=2),
    )

    with admission.hold(
        holder_id="job-one",
        requirements=requirements,
        wait_s=0,
    ) as claim:
        assert claim is not None
        assert len(claim.claims) == 2
        assert len(backend.leases) == 2

    assert backend.leases == {}


def test_disabled_host_resource_hold_does_not_touch_lease_manager(monkeypatch) -> None:
    monkeypatch.setenv("PRAXIS_HOST_RESOURCE_ADMISSION_DISABLED", "1")

    class _NoLeaseManagerAdmission(HostResourceAdmission):
        def _lease_manager(self):
            raise AssertionError("lease manager should not be touched when disabled")

    admission = _NoLeaseManagerAdmission(
        host_id="unit-host",
        database_configured=True,
    )

    with admission.hold(
        holder_id="job-one",
        requirements=(HostResourceRequirement(RESOURCE_DOCKER_SANDBOX, capacity=1),),
        wait_s=0,
    ) as claim:
        assert claim is None


def test_sandbox_resource_requirements_only_gate_local_docker() -> None:
    assert sandbox_resource_requirements(
        sandbox_provider="cloudflare_remote",
        execution_transport="cli",
    ) == ()

    requirements = sandbox_resource_requirements(
        sandbox_provider="docker_local",
        execution_transport="cli",
    )

    assert {requirement.resource_name for requirement in requirements} >= {
        RESOURCE_DOCKER_SANDBOX,
        "sandbox_subprocess",
        "host_fd_heavy",
    }


def test_hold_host_resources_for_sandbox_uses_injected_admission() -> None:
    admission, backend = _admission()

    with hold_host_resources_for_sandbox(
        sandbox_provider="docker_local",
        execution_transport="cli",
        sandbox_session_id="sandbox_session:run.alpha:job.one",
        timeout_seconds=60,
        admission=admission,
    ) as claim:
        assert claim is not None
        assert len(backend.leases) == 3

    assert backend.leases == {}
