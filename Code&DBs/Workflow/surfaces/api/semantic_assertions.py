"""Repo-local frontdoor for semantic predicate and assertion authority."""

from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from runtime.event_log import CHANNEL_SEMANTIC_ASSERTION, aemit
from runtime.semantic_assertions import (
    SemanticAssertionRecord,
    SemanticAssertionRepository,
    SemanticPredicateRecord,
    normalize_semantic_assertion_record,
)
from storage.postgres import (
    PostgresSemanticAssertionRepository,
    connect_workflow_database,
)

from ._operator_helpers import _normalize_as_of, _now, _run_async
from ._payload_contract import coerce_choice, coerce_text_sequence, optional_text, require_text


class _Connection(Protocol):
    async def execute(self, query: str, *args: object) -> str:
        """Execute one statement."""

    async def fetch(self, query: str, *args: object) -> list[Any]:
        """Fetch rows."""

    async def fetchrow(self, query: str, *args: object) -> Any:
        """Fetch one row."""

    def transaction(self) -> AsyncIterator[object]:
        """Open a transaction context."""

    async def close(self) -> None:
        """Close the connection."""


_PREDICATE_STATUSES = frozenset({"active", "inactive"})
_CARDINALITY_MODES = frozenset(
    {"many", "single_active_per_subject", "single_active_per_edge"}
)
_ASSERTION_STATUSES = frozenset({"active", "superseded", "retracted"})


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    return value


@dataclass(slots=True)
class SemanticAssertionFrontdoor:
    """Repo-local semantic assertion surface over explicit Postgres authority."""

    connect_database: Callable[[Mapping[str, str] | None], Awaitable[_Connection]] = (
        connect_workflow_database
    )
    repository_factory: Callable[
        [_Connection],
        SemanticAssertionRepository,
    ] | None = None

    def __post_init__(self) -> None:
        if self.repository_factory is None:
            self.repository_factory = self._default_repository_factory

    @staticmethod
    def _default_repository_factory(
        conn: _Connection,
    ) -> SemanticAssertionRepository:
        return PostgresSemanticAssertionRepository(conn)  # type: ignore[arg-type]

    async def register_predicate_async(
        self,
        *,
        predicate_slug: str,
        subject_kind_allowlist: tuple[str, ...] | list[str],
        object_kind_allowlist: tuple[str, ...] | list[str],
        cardinality_mode: str = "many",
        predicate_status: str = "active",
        description: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        now = _now()
        normalized_created_at = (
            now
            if created_at is None
            else _normalize_as_of(
                created_at,
                error_type=ValueError,
                reason_code="semantic_assertion.invalid_created_at",
            )
        )
        normalized_updated_at = (
            normalized_created_at
            if updated_at is None
            else _normalize_as_of(
                updated_at,
                error_type=ValueError,
                reason_code="semantic_assertion.invalid_updated_at",
            )
        )
        predicate = SemanticPredicateRecord(
            predicate_slug=require_text(predicate_slug, field_name="predicate_slug"),
            predicate_status=coerce_choice(
                predicate_status,
                field_name="predicate_status",
                choices=_PREDICATE_STATUSES,
            ),
            subject_kind_allowlist=coerce_text_sequence(
                subject_kind_allowlist,
                field_name="subject_kind_allowlist",
            ),
            object_kind_allowlist=coerce_text_sequence(
                object_kind_allowlist,
                field_name="object_kind_allowlist",
            ),
            cardinality_mode=coerce_choice(
                cardinality_mode,
                field_name="cardinality_mode",
                choices=_CARDINALITY_MODES,
            ),
            description=optional_text(description, field_name="description") or "",
            created_at=normalized_created_at,
            updated_at=normalized_updated_at,
        )
        conn = await self.connect_database(env)
        try:
            async with conn.transaction():
                assert self.repository_factory is not None
                repository = self.repository_factory(conn)
                persisted = await repository.upsert_predicate(predicate=predicate)
                event_id = await aemit(
                    conn,
                    channel=CHANNEL_SEMANTIC_ASSERTION,
                    event_type="semantic_predicate_registered",
                    entity_id=persisted.predicate_slug,
                    entity_kind="semantic_predicate",
                    payload={"semantic_predicate": persisted.to_json()},
                    emitted_by="semantic_assertions.register_predicate",
                )
        finally:
            await conn.close()
        return {
            "semantic_predicate": persisted.to_json(),
            "semantic_event_id": event_id,
        }

    async def record_assertion_async(
        self,
        *,
        predicate_slug: str,
        subject_kind: str,
        subject_ref: str,
        object_kind: str,
        object_ref: str,
        qualifiers_json: Mapping[str, Any] | None = None,
        source_kind: str,
        source_ref: str,
        evidence_ref: str | None = None,
        bound_decision_id: str | None = None,
        valid_from: datetime | None = None,
        valid_to: datetime | None = None,
        assertion_status: str = "active",
        semantic_assertion_id: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        now = _now()
        normalized_valid_from = (
            now
            if valid_from is None
            else _normalize_as_of(
                valid_from,
                error_type=ValueError,
                reason_code="semantic_assertion.invalid_valid_from",
            )
        )
        normalized_valid_to = (
            None
            if valid_to is None
            else _normalize_as_of(
                valid_to,
                error_type=ValueError,
                reason_code="semantic_assertion.invalid_valid_to",
            )
        )
        normalized_created_at = (
            normalized_valid_from
            if created_at is None
            else _normalize_as_of(
                created_at,
                error_type=ValueError,
                reason_code="semantic_assertion.invalid_created_at",
            )
        )
        normalized_updated_at = (
            now
            if updated_at is None
            else _normalize_as_of(
                updated_at,
                error_type=ValueError,
                reason_code="semantic_assertion.invalid_updated_at",
            )
        )
        assertion = normalize_semantic_assertion_record(
            SemanticAssertionRecord(
            semantic_assertion_id=optional_text(
                semantic_assertion_id,
                field_name="semantic_assertion_id",
            )
            or "",
            predicate_slug=require_text(predicate_slug, field_name="predicate_slug"),
            assertion_status=coerce_choice(
                assertion_status,
                field_name="assertion_status",
                choices=_ASSERTION_STATUSES,
            ),
            subject_kind=require_text(subject_kind, field_name="subject_kind"),
            subject_ref=require_text(subject_ref, field_name="subject_ref"),
            object_kind=require_text(object_kind, field_name="object_kind"),
            object_ref=require_text(object_ref, field_name="object_ref"),
            qualifiers_json=(
                {}
                if qualifiers_json is None
                else dict(_require_mapping(qualifiers_json, field_name="qualifiers_json"))
            ),
            source_kind=require_text(source_kind, field_name="source_kind"),
            source_ref=require_text(source_ref, field_name="source_ref"),
            evidence_ref=optional_text(evidence_ref, field_name="evidence_ref"),
            bound_decision_id=optional_text(
                bound_decision_id,
                field_name="bound_decision_id",
            ),
            valid_from=normalized_valid_from,
            valid_to=normalized_valid_to,
            created_at=normalized_created_at,
            updated_at=normalized_updated_at,
            )
        )
        conn = await self.connect_database(env)
        try:
            async with conn.transaction():
                assert self.repository_factory is not None
                repository = self.repository_factory(conn)
                predicate = await repository.load_predicate(
                    predicate_slug=assertion.predicate_slug,
                )
                if predicate is None:
                    raise ValueError(
                        "predicate_slug does not resolve to a registered semantic predicate: "
                        f"{assertion.predicate_slug}"
                    )
                if assertion.subject_kind not in predicate.subject_kind_allowlist:
                    raise ValueError(
                        f"subject_kind must be one of {', '.join(predicate.subject_kind_allowlist)}"
                    )
                if assertion.object_kind not in predicate.object_kind_allowlist:
                    raise ValueError(
                        f"object_kind must be one of {', '.join(predicate.object_kind_allowlist)}"
                    )
                persisted, superseded = await repository.record_assertion(
                    assertion=assertion,
                    cardinality_mode=predicate.cardinality_mode,
                    as_of=normalized_updated_at,
                )
                event_id = await aemit(
                    conn,
                    channel=CHANNEL_SEMANTIC_ASSERTION,
                    event_type="semantic_assertion_recorded",
                    entity_id=persisted.semantic_assertion_id,
                    entity_kind="semantic_assertion",
                    payload={
                        "semantic_assertion": persisted.to_json(),
                        "superseded_assertion_ids": [
                            item.semantic_assertion_id for item in superseded
                        ],
                    },
                    emitted_by="semantic_assertions.record_assertion",
                )
        finally:
            await conn.close()
        return {
            "semantic_assertion": persisted.to_json(),
            "superseded_assertions": [item.to_json() for item in superseded],
            "semantic_event_id": event_id,
        }

    async def retract_assertion_async(
        self,
        *,
        semantic_assertion_id: str,
        retracted_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        normalized_retracted_at = (
            _now()
            if retracted_at is None
            else _normalize_as_of(
                retracted_at,
                error_type=ValueError,
                reason_code="semantic_assertion.invalid_retracted_at",
            )
        )
        normalized_updated_at = (
            normalized_retracted_at
            if updated_at is None
            else _normalize_as_of(
                updated_at,
                error_type=ValueError,
                reason_code="semantic_assertion.invalid_updated_at",
            )
        )
        conn = await self.connect_database(env)
        try:
            async with conn.transaction():
                assert self.repository_factory is not None
                repository = self.repository_factory(conn)
                persisted = await repository.retract_assertion(
                    semantic_assertion_id=require_text(
                        semantic_assertion_id,
                        field_name="semantic_assertion_id",
                    ),
                    retracted_at=normalized_retracted_at,
                    updated_at=normalized_updated_at,
                )
                event_id = await aemit(
                    conn,
                    channel=CHANNEL_SEMANTIC_ASSERTION,
                    event_type="semantic_assertion_retracted",
                    entity_id=persisted.semantic_assertion_id,
                    entity_kind="semantic_assertion",
                    payload={"semantic_assertion": persisted.to_json()},
                    emitted_by="semantic_assertions.retract_assertion",
                )
        finally:
            await conn.close()
        return {
            "semantic_assertion": persisted.to_json(),
            "semantic_event_id": event_id,
        }

    async def list_assertions_async(
        self,
        *,
        predicate_slug: str | None = None,
        subject_kind: str | None = None,
        subject_ref: str | None = None,
        object_kind: str | None = None,
        object_ref: str | None = None,
        source_kind: str | None = None,
        source_ref: str | None = None,
        active_only: bool = True,
        as_of: datetime | None = None,
        limit: int = 100,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        normalized_as_of = (
            None
            if as_of is None
            else _normalize_as_of(
                as_of,
                error_type=ValueError,
                reason_code="semantic_assertion.invalid_as_of",
            )
        )
        normalized_limit = max(1, int(limit or 100))
        conn = await self.connect_database(env)
        try:
            assert self.repository_factory is not None
            repository = self.repository_factory(conn)
            if active_only and normalized_as_of is None:
                rows = await repository.list_current_assertions(
                    predicate_slug=optional_text(
                        predicate_slug,
                        field_name="predicate_slug",
                    ),
                    subject_kind=optional_text(
                        subject_kind,
                        field_name="subject_kind",
                    ),
                    subject_ref=optional_text(subject_ref, field_name="subject_ref"),
                    object_kind=optional_text(object_kind, field_name="object_kind"),
                    object_ref=optional_text(object_ref, field_name="object_ref"),
                    source_kind=optional_text(source_kind, field_name="source_kind"),
                    source_ref=optional_text(source_ref, field_name="source_ref"),
                    limit=normalized_limit,
                )
                projection_source = "semantic_current_assertions"
                response_as_of = _now()
            else:
                response_as_of = normalized_as_of or _now()
                rows = await repository.list_assertions(
                    predicate_slug=optional_text(
                        predicate_slug,
                        field_name="predicate_slug",
                    ),
                    subject_kind=optional_text(
                        subject_kind,
                        field_name="subject_kind",
                    ),
                    subject_ref=optional_text(subject_ref, field_name="subject_ref"),
                    object_kind=optional_text(object_kind, field_name="object_kind"),
                    object_ref=optional_text(object_ref, field_name="object_ref"),
                    source_kind=optional_text(source_kind, field_name="source_kind"),
                    source_ref=optional_text(source_ref, field_name="source_ref"),
                    active_at=response_as_of if active_only else None,
                    active_only=active_only,
                    limit=normalized_limit,
                )
                projection_source = "semantic_assertions"
        finally:
            await conn.close()
        return {
            "semantic_assertions": [row.to_json() for row in rows],
            "as_of": response_as_of.isoformat(),
            "active_only": active_only,
            "projection_source": projection_source,
            "filters": {
                "predicate_slug": optional_text(predicate_slug, field_name="predicate_slug"),
                "subject_kind": optional_text(subject_kind, field_name="subject_kind"),
                "subject_ref": optional_text(subject_ref, field_name="subject_ref"),
                "object_kind": optional_text(object_kind, field_name="object_kind"),
                "object_ref": optional_text(object_ref, field_name="object_ref"),
                "source_kind": optional_text(source_kind, field_name="source_kind"),
                "source_ref": optional_text(source_ref, field_name="source_ref"),
                "limit": normalized_limit,
            },
        }

    async def rebuild_current_projection_async(
        self,
        *,
        as_of: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        normalized_as_of = (
            _now()
            if as_of is None
            else _normalize_as_of(
                as_of,
                error_type=ValueError,
                reason_code="semantic_assertion.invalid_as_of",
            )
        )
        conn = await self.connect_database(env)
        try:
            async with conn.transaction():
                assert self.repository_factory is not None
                repository = self.repository_factory(conn)
                refreshed_count = await repository.rebuild_current_assertions(
                    as_of=normalized_as_of,
                )
                event_id = await aemit(
                    conn,
                    channel=CHANNEL_SEMANTIC_ASSERTION,
                    event_type="semantic_projection_rebuilt",
                    entity_id="semantic_current_assertions",
                    entity_kind="semantic_projection",
                    payload={
                        "projection_name": "semantic_current_assertions",
                        "as_of": normalized_as_of.isoformat(),
                        "row_count": refreshed_count,
                    },
                    emitted_by="semantic_assertions.rebuild_projection",
                )
        finally:
            await conn.close()
        return {
            "projection_name": "semantic_current_assertions",
            "as_of": normalized_as_of.isoformat(),
            "row_count": refreshed_count,
            "semantic_event_id": event_id,
        }

    def register_predicate(
        self,
        *,
        predicate_slug: str,
        subject_kind_allowlist: tuple[str, ...] | list[str],
        object_kind_allowlist: tuple[str, ...] | list[str],
        cardinality_mode: str = "many",
        predicate_status: str = "active",
        description: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.register_predicate_async(
                predicate_slug=predicate_slug,
                subject_kind_allowlist=subject_kind_allowlist,
                object_kind_allowlist=object_kind_allowlist,
                cardinality_mode=cardinality_mode,
                predicate_status=predicate_status,
                description=description,
                created_at=created_at,
                updated_at=updated_at,
                env=env,
            ),
            message=(
                "semantic_assertion.async_boundary_required: "
                "semantic assertion sync entrypoints require a non-async call boundary"
            ),
        )

    def record_assertion(
        self,
        *,
        predicate_slug: str,
        subject_kind: str,
        subject_ref: str,
        object_kind: str,
        object_ref: str,
        qualifiers_json: Mapping[str, Any] | None = None,
        source_kind: str,
        source_ref: str,
        evidence_ref: str | None = None,
        bound_decision_id: str | None = None,
        valid_from: datetime | None = None,
        valid_to: datetime | None = None,
        assertion_status: str = "active",
        semantic_assertion_id: str | None = None,
        created_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.record_assertion_async(
                predicate_slug=predicate_slug,
                subject_kind=subject_kind,
                subject_ref=subject_ref,
                object_kind=object_kind,
                object_ref=object_ref,
                qualifiers_json=qualifiers_json,
                source_kind=source_kind,
                source_ref=source_ref,
                evidence_ref=evidence_ref,
                bound_decision_id=bound_decision_id,
                valid_from=valid_from,
                valid_to=valid_to,
                assertion_status=assertion_status,
                semantic_assertion_id=semantic_assertion_id,
                created_at=created_at,
                updated_at=updated_at,
                env=env,
            ),
            message=(
                "semantic_assertion.async_boundary_required: "
                "semantic assertion sync entrypoints require a non-async call boundary"
            ),
        )

    def retract_assertion(
        self,
        *,
        semantic_assertion_id: str,
        retracted_at: datetime | None = None,
        updated_at: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.retract_assertion_async(
                semantic_assertion_id=semantic_assertion_id,
                retracted_at=retracted_at,
                updated_at=updated_at,
                env=env,
            ),
            message=(
                "semantic_assertion.async_boundary_required: "
                "semantic assertion sync entrypoints require a non-async call boundary"
            ),
        )

    def list_assertions(
        self,
        *,
        predicate_slug: str | None = None,
        subject_kind: str | None = None,
        subject_ref: str | None = None,
        object_kind: str | None = None,
        object_ref: str | None = None,
        source_kind: str | None = None,
        source_ref: str | None = None,
        active_only: bool = True,
        as_of: datetime | None = None,
        limit: int = 100,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.list_assertions_async(
                predicate_slug=predicate_slug,
                subject_kind=subject_kind,
                subject_ref=subject_ref,
                object_kind=object_kind,
                object_ref=object_ref,
                source_kind=source_kind,
                source_ref=source_ref,
                active_only=active_only,
                as_of=as_of,
                limit=limit,
                env=env,
            ),
            message=(
                "semantic_assertion.async_boundary_required: "
                "semantic assertion sync entrypoints require a non-async call boundary"
            ),
        )

    def rebuild_current_projection(
        self,
        *,
        as_of: datetime | None = None,
        env: Mapping[str, str] | None = None,
    ) -> dict[str, Any]:
        return _run_async(
            self.rebuild_current_projection_async(
                as_of=as_of,
                env=env,
            ),
            message=(
                "semantic_assertion.async_boundary_required: "
                "semantic assertion sync entrypoints require a non-async call boundary"
            ),
        )


def register_predicate(
    *,
    predicate_slug: str,
    subject_kind_allowlist: tuple[str, ...] | list[str],
    object_kind_allowlist: tuple[str, ...] | list[str],
    cardinality_mode: str = "many",
    predicate_status: str = "active",
    description: str | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return SemanticAssertionFrontdoor().register_predicate(
        predicate_slug=predicate_slug,
        subject_kind_allowlist=subject_kind_allowlist,
        object_kind_allowlist=object_kind_allowlist,
        cardinality_mode=cardinality_mode,
        predicate_status=predicate_status,
        description=description,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


async def aregister_predicate(
    *,
    predicate_slug: str,
    subject_kind_allowlist: tuple[str, ...] | list[str],
    object_kind_allowlist: tuple[str, ...] | list[str],
    cardinality_mode: str = "many",
    predicate_status: str = "active",
    description: str | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return await SemanticAssertionFrontdoor().register_predicate_async(
        predicate_slug=predicate_slug,
        subject_kind_allowlist=subject_kind_allowlist,
        object_kind_allowlist=object_kind_allowlist,
        cardinality_mode=cardinality_mode,
        predicate_status=predicate_status,
        description=description,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


def record_assertion(
    *,
    predicate_slug: str,
    subject_kind: str,
    subject_ref: str,
    object_kind: str,
    object_ref: str,
    qualifiers_json: Mapping[str, Any] | None = None,
    source_kind: str,
    source_ref: str,
    evidence_ref: str | None = None,
    bound_decision_id: str | None = None,
    valid_from: datetime | None = None,
    valid_to: datetime | None = None,
    assertion_status: str = "active",
    semantic_assertion_id: str | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return SemanticAssertionFrontdoor().record_assertion(
        predicate_slug=predicate_slug,
        subject_kind=subject_kind,
        subject_ref=subject_ref,
        object_kind=object_kind,
        object_ref=object_ref,
        qualifiers_json=qualifiers_json,
        source_kind=source_kind,
        source_ref=source_ref,
        evidence_ref=evidence_ref,
        bound_decision_id=bound_decision_id,
        valid_from=valid_from,
        valid_to=valid_to,
        assertion_status=assertion_status,
        semantic_assertion_id=semantic_assertion_id,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


async def arecord_assertion(
    *,
    predicate_slug: str,
    subject_kind: str,
    subject_ref: str,
    object_kind: str,
    object_ref: str,
    qualifiers_json: Mapping[str, Any] | None = None,
    source_kind: str,
    source_ref: str,
    evidence_ref: str | None = None,
    bound_decision_id: str | None = None,
    valid_from: datetime | None = None,
    valid_to: datetime | None = None,
    assertion_status: str = "active",
    semantic_assertion_id: str | None = None,
    created_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return await SemanticAssertionFrontdoor().record_assertion_async(
        predicate_slug=predicate_slug,
        subject_kind=subject_kind,
        subject_ref=subject_ref,
        object_kind=object_kind,
        object_ref=object_ref,
        qualifiers_json=qualifiers_json,
        source_kind=source_kind,
        source_ref=source_ref,
        evidence_ref=evidence_ref,
        bound_decision_id=bound_decision_id,
        valid_from=valid_from,
        valid_to=valid_to,
        assertion_status=assertion_status,
        semantic_assertion_id=semantic_assertion_id,
        created_at=created_at,
        updated_at=updated_at,
        env=env,
    )


def retract_assertion(
    *,
    semantic_assertion_id: str,
    retracted_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return SemanticAssertionFrontdoor().retract_assertion(
        semantic_assertion_id=semantic_assertion_id,
        retracted_at=retracted_at,
        updated_at=updated_at,
        env=env,
    )


async def aretract_assertion(
    *,
    semantic_assertion_id: str,
    retracted_at: datetime | None = None,
    updated_at: datetime | None = None,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return await SemanticAssertionFrontdoor().retract_assertion_async(
        semantic_assertion_id=semantic_assertion_id,
        retracted_at=retracted_at,
        updated_at=updated_at,
        env=env,
    )


def list_assertions(
    *,
    predicate_slug: str | None = None,
    subject_kind: str | None = None,
    subject_ref: str | None = None,
    object_kind: str | None = None,
    object_ref: str | None = None,
    source_kind: str | None = None,
    source_ref: str | None = None,
    active_only: bool = True,
    as_of: datetime | None = None,
    limit: int = 100,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return SemanticAssertionFrontdoor().list_assertions(
        predicate_slug=predicate_slug,
        subject_kind=subject_kind,
        subject_ref=subject_ref,
        object_kind=object_kind,
        object_ref=object_ref,
        source_kind=source_kind,
        source_ref=source_ref,
        active_only=active_only,
        as_of=as_of,
        limit=limit,
        env=env,
    )


async def alist_assertions(
    *,
    predicate_slug: str | None = None,
    subject_kind: str | None = None,
    subject_ref: str | None = None,
    object_kind: str | None = None,
    object_ref: str | None = None,
    source_kind: str | None = None,
    source_ref: str | None = None,
    active_only: bool = True,
    as_of: datetime | None = None,
    limit: int = 100,
    env: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    return await SemanticAssertionFrontdoor().list_assertions_async(
        predicate_slug=predicate_slug,
        subject_kind=subject_kind,
        subject_ref=subject_ref,
        object_kind=object_kind,
        object_ref=object_ref,
        source_kind=source_kind,
        source_ref=source_ref,
        active_only=active_only,
        as_of=as_of,
        limit=limit,
        env=env,
    )


__all__ = [
    "SemanticAssertionFrontdoor",
    "alist_assertions",
    "arecord_assertion",
    "aregister_predicate",
    "aretract_assertion",
    "list_assertions",
    "record_assertion",
    "register_predicate",
    "retract_assertion",
]
