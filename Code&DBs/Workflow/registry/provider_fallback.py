"""Postgres-backed provider route control-tower repository.

This module is the explicit shadow seam over the canonical routing-control
tables:

- ``provider_route_health_windows``
- ``provider_budget_windows``
- ``route_eligibility_states``

It keeps the repository separate from the live routing path while preserving
the same durable record shape already used by the authority reader.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime

import asyncpg

from .provider_routing import (
    PostgresProviderRouteAuthorityRepository,
    ProviderBudgetWindowAuthorityRecord,
    ProviderRouteAuthority,
    ProviderRouteAuthorityRepositoryError,
    ProviderRouteHealthWindowAuthorityRecord,
    RouteEligibilityStateAuthorityRecord,
)

ProviderRouteControlTower = ProviderRouteAuthority
ProviderRouteControlTowerRepositoryError = ProviderRouteAuthorityRepositoryError


class PostgresProviderRouteControlTowerRepository:
    """Explicit Postgres repository for the provider route control tower."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._authority_repository = PostgresProviderRouteAuthorityRepository(conn)

    async def bootstrap_provider_route_control_tower_schema(self) -> None:
        """Apply the control-tower schema in an idempotent, fail-closed way."""

        await self._authority_repository.bootstrap_provider_route_authority_schema()

    async def fetch_provider_route_health_windows(
        self,
        *,
        candidate_refs: Sequence[str] | None = None,
    ) -> tuple[ProviderRouteHealthWindowAuthorityRecord, ...]:
        return await self._authority_repository.fetch_provider_route_health_windows(
            candidate_refs=candidate_refs,
        )

    async def fetch_provider_budget_windows(
        self,
        *,
        provider_policy_ids: Sequence[str] | None = None,
    ) -> tuple[ProviderBudgetWindowAuthorityRecord, ...]:
        return await self._authority_repository.fetch_provider_budget_windows(
            provider_policy_ids=provider_policy_ids,
        )

    async def fetch_route_eligibility_states(
        self,
        *,
        model_profile_ids: Sequence[str] | None = None,
        provider_policy_ids: Sequence[str] | None = None,
        candidate_refs: Sequence[str] | None = None,
    ) -> tuple[RouteEligibilityStateAuthorityRecord, ...]:
        return await self._authority_repository.fetch_route_eligibility_states(
            model_profile_ids=model_profile_ids,
            provider_policy_ids=provider_policy_ids,
            candidate_refs=candidate_refs,
        )

    async def load_provider_route_control_tower(
        self,
        *,
        model_profile_ids: Sequence[str] | None = None,
        provider_policy_ids: Sequence[str] | None = None,
        candidate_refs: Sequence[str] | None = None,
    ) -> ProviderRouteControlTower:
        """Load the canonical provider route control tower from Postgres."""

        return await self._authority_repository.load_provider_route_authority(
            model_profile_ids=model_profile_ids,
            provider_policy_ids=provider_policy_ids,
            candidate_refs=candidate_refs,
        )

    async def load_provider_route_control_tower_snapshot(
        self,
        *,
        as_of: datetime,
        model_profile_ids: Sequence[str] | None = None,
        provider_policy_ids: Sequence[str] | None = None,
        candidate_refs: Sequence[str] | None = None,
    ) -> ProviderRouteControlTower:
        """Load an explicit provider route control-tower snapshot at or before ``as_of``."""

        return await self._authority_repository.load_provider_route_authority_snapshot(
            as_of=as_of,
            model_profile_ids=model_profile_ids,
            provider_policy_ids=provider_policy_ids,
            candidate_refs=candidate_refs,
        )


async def load_provider_route_control_tower(
    conn: asyncpg.Connection,
    *,
    model_profile_ids: Sequence[str] | None = None,
    provider_policy_ids: Sequence[str] | None = None,
    candidate_refs: Sequence[str] | None = None,
) -> ProviderRouteControlTower:
    """Load the canonical provider route control tower using Postgres."""

    repository = PostgresProviderRouteControlTowerRepository(conn)
    return await repository.load_provider_route_control_tower(
        model_profile_ids=model_profile_ids,
        provider_policy_ids=provider_policy_ids,
        candidate_refs=candidate_refs,
    )


async def load_provider_route_control_tower_snapshot(
    conn: asyncpg.Connection,
    *,
    as_of: datetime,
    model_profile_ids: Sequence[str] | None = None,
    provider_policy_ids: Sequence[str] | None = None,
    candidate_refs: Sequence[str] | None = None,
) -> ProviderRouteControlTower:
    """Load the canonical provider route control tower snapshot at or before ``as_of``."""

    repository = PostgresProviderRouteControlTowerRepository(conn)
    return await repository.load_provider_route_control_tower_snapshot(
        as_of=as_of,
        model_profile_ids=model_profile_ids,
        provider_policy_ids=provider_policy_ids,
        candidate_refs=candidate_refs,
    )


__all__ = [
    "PostgresProviderRouteControlTowerRepository",
    "ProviderBudgetWindowAuthorityRecord",
    "ProviderRouteControlTower",
    "ProviderRouteControlTowerRepositoryError",
    "ProviderRouteHealthWindowAuthorityRecord",
    "RouteEligibilityStateAuthorityRecord",
    "load_provider_route_control_tower",
    "load_provider_route_control_tower_snapshot",
]
