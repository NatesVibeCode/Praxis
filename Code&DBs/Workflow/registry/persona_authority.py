"""Postgres-backed persona and fork/worktree authority repository.

This module reads canonical persona profiles, persona-context bindings, and
fork/worktree bindings from the W29 runtime-breadth authority tables in
Postgres. It resolves explicit effective-dated authority slices and fails
closed when a selector is missing, under-specified, or ambiguous.

Canonical tables (011_runtime_breadth_authority.sql):
    persona_profiles           — persona_profile_id PK
    persona_context_bindings   — persona_context_binding_id PK
    fork_profiles              — fork_profile_id PK
    fork_worktree_bindings     — fork_worktree_binding_id PK
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import json
from typing import Any

import asyncpg


class PersonaAndForkAuthorityRepositoryError(RuntimeError):
    """Raised when persona or fork authority cannot be resolved safely."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


# ---------------------------------------------------------------------------
# Validation helpers
# ---------------------------------------------------------------------------

def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PersonaAndForkAuthorityRepositoryError(
            "persona_authority.invalid_row",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _require_nullable_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _require_int(value: object, *, field_name: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise PersonaAndForkAuthorityRepositoryError(
            "persona_authority.invalid_row",
            f"{field_name} must be an integer",
            details={"field": field_name},
        )
    return value


def _require_datetime(value: object, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise PersonaAndForkAuthorityRepositoryError(
            "persona_authority.invalid_row",
            f"{field_name} must be a datetime",
            details={"field": field_name},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise PersonaAndForkAuthorityRepositoryError(
            "persona_authority.invalid_row",
            f"{field_name} must be timezone-aware",
            details={"field": field_name},
        )
    return value.astimezone(timezone.utc)


def _optional_datetime(value: object, *, field_name: str) -> datetime | None:
    if value is None:
        return None
    return _require_datetime(value, field_name=field_name)


def _require_selector_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise PersonaAndForkAuthorityRepositoryError(
            "persona_authority.invalid_selector",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    if not value.strip():
        raise PersonaAndForkAuthorityRepositoryError(
            "persona_authority.invalid_selector",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    if value != value.strip():
        raise PersonaAndForkAuthorityRepositoryError(
            "persona_authority.invalid_selector",
            f"{field_name} must not contain leading or trailing whitespace",
            details={"field": field_name},
        )
    return value


def _optional_selector_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_selector_text(value, field_name=field_name)


def _normalize_as_of(value: datetime) -> datetime:
    if not isinstance(value, datetime):
        raise PersonaAndForkAuthorityRepositoryError(
            "persona_authority.invalid_as_of",
            "as_of must be a datetime",
            details={"value_type": type(value).__name__},
        )
    if value.tzinfo is None or value.utcoffset() is None:
        raise PersonaAndForkAuthorityRepositoryError(
            "persona_authority.invalid_as_of",
            "as_of must be timezone-aware",
            details={"value_type": type(value).__name__},
        )
    return value.astimezone(timezone.utc)


# ---------------------------------------------------------------------------
# Canonical record types — thin projections over the W29 migration tables
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class PersonaProfileAuthorityRecord:
    """Canonical persona profile row from persona_profiles."""

    persona_profile_id: str
    persona_name: str
    persona_kind: str
    status: str
    instruction_contract: str
    response_contract: Any
    tool_policy: Any
    runtime_hints: Any
    effective_from: datetime
    effective_to: datetime | None
    decision_ref: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class PersonaContextBindingAuthorityRecord:
    """Canonical binding row from persona_context_bindings."""

    persona_context_binding_id: str
    persona_profile_id: str
    binding_scope: str
    workspace_ref: str | None
    runtime_profile_ref: str | None
    model_profile_id: str | None
    provider_policy_id: str | None
    context_selector: Any
    binding_status: str
    position_index: int
    effective_from: datetime
    effective_to: datetime | None
    decision_ref: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ForkWorktreeBindingAuthorityRecord:
    """Canonical binding row from fork_worktree_bindings."""

    fork_worktree_binding_id: str
    fork_profile_id: str
    sandbox_session_id: str
    workflow_run_id: str
    binding_scope: str
    binding_status: str
    workspace_ref: str
    runtime_profile_ref: str
    base_ref: str
    fork_ref: str
    worktree_ref: str
    created_at: datetime
    retired_at: datetime | None
    decision_ref: str | None


# ---------------------------------------------------------------------------
# Selectors — keyed on canonical stored axes
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class PersonaActivationSelector:
    """Explicit selector for persona activation.

    This selector supports two bounded modes:

    - persona-profile mode: persona_profile_id + binding_scope + as_of
    - operator-path mode: binding_scope + workspace_ref + runtime_profile_ref
      + operator_path + as_of
    """

    binding_scope: str
    as_of: datetime
    persona_profile_id: str | None = None
    workspace_ref: str | None = None
    runtime_profile_ref: str | None = None
    operator_path: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "binding_scope",
            _require_selector_text(self.binding_scope, field_name="binding_scope"),
        )
        object.__setattr__(
            self,
            "persona_profile_id",
            _optional_selector_text(self.persona_profile_id, field_name="persona_profile_id"),
        )
        object.__setattr__(
            self,
            "workspace_ref",
            _optional_selector_text(self.workspace_ref, field_name="workspace_ref"),
        )
        object.__setattr__(
            self,
            "runtime_profile_ref",
            _optional_selector_text(self.runtime_profile_ref, field_name="runtime_profile_ref"),
        )
        object.__setattr__(
            self,
            "operator_path",
            _optional_selector_text(self.operator_path, field_name="operator_path"),
        )
        object.__setattr__(self, "as_of", _normalize_as_of(self.as_of))

        operator_path_fields = (
            self.workspace_ref,
            self.runtime_profile_ref,
            self.operator_path,
        )
        has_persona_profile = self.persona_profile_id is not None
        has_any_operator_path_field = any(value is not None for value in operator_path_fields)
        has_all_operator_path_fields = all(value is not None for value in operator_path_fields)

        if has_persona_profile and has_any_operator_path_field:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.invalid_selector",
                "persona selector must use either persona_profile_id or operator-path axes, not both",
                details={
                    "binding_scope": self.binding_scope,
                    "has_persona_profile_id": True,
                    "has_workspace_ref": self.workspace_ref is not None,
                    "has_runtime_profile_ref": self.runtime_profile_ref is not None,
                    "has_operator_path": self.operator_path is not None,
                },
            )
        if not has_persona_profile and not has_any_operator_path_field:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.invalid_selector",
                "persona selector must include persona_profile_id or the full operator-path discriminator",
                details={"binding_scope": self.binding_scope},
            )
        if has_any_operator_path_field and not has_all_operator_path_fields:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.invalid_selector",
                "operator-path persona selectors require workspace_ref, runtime_profile_ref, and operator_path",
                details={
                    "binding_scope": self.binding_scope,
                    "has_workspace_ref": self.workspace_ref is not None,
                    "has_runtime_profile_ref": self.runtime_profile_ref is not None,
                    "has_operator_path": self.operator_path is not None,
                },
            )

    @property
    def selector_mode(self) -> str:
        return "persona_profile" if self.persona_profile_id is not None else "operator_path"


@dataclass(frozen=True, slots=True)
class ForkOwnershipSelector:
    """Explicit selector for fork/worktree ownership via canonical axes.

    Keys on (workspace_ref, runtime_profile_ref, fork_ref, worktree_ref),
    which is a UNIQUE constraint on fork_worktree_bindings.
    """

    workspace_ref: str
    runtime_profile_ref: str
    fork_ref: str
    worktree_ref: str

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "workspace_ref",
            _require_selector_text(self.workspace_ref, field_name="workspace_ref"),
        )
        object.__setattr__(
            self,
            "runtime_profile_ref",
            _require_selector_text(self.runtime_profile_ref, field_name="runtime_profile_ref"),
        )
        object.__setattr__(
            self,
            "fork_ref",
            _require_selector_text(self.fork_ref, field_name="fork_ref"),
        )
        object.__setattr__(
            self,
            "worktree_ref",
            _require_selector_text(self.worktree_ref, field_name="worktree_ref"),
        )


# ---------------------------------------------------------------------------
# Authority snapshot
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class PersonaAndForkAuthority:
    """Inspectable snapshot of persona and fork/worktree authority rows."""

    as_of: datetime
    persona_selector: PersonaActivationSelector
    fork_selector: ForkOwnershipSelector
    persona_profile: PersonaProfileAuthorityRecord
    persona_context_bindings: tuple[PersonaContextBindingAuthorityRecord, ...]
    fork_worktree_binding: ForkWorktreeBindingAuthorityRecord

    @property
    def persona_profile_id(self) -> str:
        return self.persona_profile.persona_profile_id

    @property
    def fork_worktree_binding_id(self) -> str:
        return self.fork_worktree_binding.fork_worktree_binding_id

    @property
    def binding_scope(self) -> str:
        return self.persona_selector.binding_scope


# ---------------------------------------------------------------------------
# Row parsers
# ---------------------------------------------------------------------------

def _persona_profile_from_row(row: asyncpg.Record) -> PersonaProfileAuthorityRecord:
    return PersonaProfileAuthorityRecord(
        persona_profile_id=_require_text(row["persona_profile_id"], field_name="persona_profile_id"),
        persona_name=_require_text(row["persona_name"], field_name="persona_name"),
        persona_kind=_require_text(row["persona_kind"], field_name="persona_kind"),
        status=_require_text(row["status"], field_name="status"),
        instruction_contract=_require_text(row["instruction_contract"], field_name="instruction_contract"),
        response_contract=row["response_contract"],
        tool_policy=row["tool_policy"],
        runtime_hints=row["runtime_hints"],
        effective_from=_require_datetime(row["effective_from"], field_name="effective_from"),
        effective_to=_optional_datetime(row["effective_to"], field_name="effective_to"),
        decision_ref=_require_text(row["decision_ref"], field_name="decision_ref"),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
    )


def _persona_context_binding_from_row(
    row: asyncpg.Record,
) -> PersonaContextBindingAuthorityRecord:
    return PersonaContextBindingAuthorityRecord(
        persona_context_binding_id=_require_text(row["persona_context_binding_id"], field_name="persona_context_binding_id"),
        persona_profile_id=_require_text(row["persona_profile_id"], field_name="persona_profile_id"),
        binding_scope=_require_text(row["binding_scope"], field_name="binding_scope"),
        workspace_ref=_require_nullable_text(row["workspace_ref"], field_name="workspace_ref"),
        runtime_profile_ref=_require_nullable_text(row["runtime_profile_ref"], field_name="runtime_profile_ref"),
        model_profile_id=_require_nullable_text(row["model_profile_id"], field_name="model_profile_id"),
        provider_policy_id=_require_nullable_text(row["provider_policy_id"], field_name="provider_policy_id"),
        context_selector=row["context_selector"],
        binding_status=_require_text(row["binding_status"], field_name="binding_status"),
        position_index=_require_int(row["position_index"], field_name="position_index"),
        effective_from=_require_datetime(row["effective_from"], field_name="effective_from"),
        effective_to=_optional_datetime(row["effective_to"], field_name="effective_to"),
        decision_ref=_require_text(row["decision_ref"], field_name="decision_ref"),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
    )


def _fork_worktree_binding_from_row(
    row: asyncpg.Record,
) -> ForkWorktreeBindingAuthorityRecord:
    return ForkWorktreeBindingAuthorityRecord(
        fork_worktree_binding_id=_require_text(row["fork_worktree_binding_id"], field_name="fork_worktree_binding_id"),
        fork_profile_id=_require_text(row["fork_profile_id"], field_name="fork_profile_id"),
        sandbox_session_id=_require_text(row["sandbox_session_id"], field_name="sandbox_session_id"),
        workflow_run_id=_require_text(row["workflow_run_id"], field_name="workflow_run_id"),
        binding_scope=_require_text(row["binding_scope"], field_name="binding_scope"),
        binding_status=_require_text(row["binding_status"], field_name="binding_status"),
        workspace_ref=_require_text(row["workspace_ref"], field_name="workspace_ref"),
        runtime_profile_ref=_require_text(row["runtime_profile_ref"], field_name="runtime_profile_ref"),
        base_ref=_require_text(row["base_ref"], field_name="base_ref"),
        fork_ref=_require_text(row["fork_ref"], field_name="fork_ref"),
        worktree_ref=_require_text(row["worktree_ref"], field_name="worktree_ref"),
        created_at=_require_datetime(row["created_at"], field_name="created_at"),
        retired_at=_optional_datetime(row["retired_at"], field_name="retired_at"),
        decision_ref=_require_nullable_text(row["decision_ref"], field_name="decision_ref"),
    )


def _require_json_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    normalized_value = value
    if isinstance(normalized_value, str):
        try:
            normalized_value = json.loads(normalized_value)
        except json.JSONDecodeError as exc:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.invalid_row",
                f"{field_name} must be a JSON object",
                details={"field": field_name},
            ) from exc
    if not isinstance(normalized_value, Mapping):
        raise PersonaAndForkAuthorityRepositoryError(
            "persona_authority.invalid_row",
            f"{field_name} must be a JSON object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return normalized_value


def _optional_operator_path(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    if not value.strip() or value != value.strip():
        return None
    return value


# ---------------------------------------------------------------------------
# Repository
# ---------------------------------------------------------------------------

class PostgresPersonaAndForkAuthorityRepository:
    """Explicit Postgres repository for persona and fork/worktree authority."""

    def __init__(self, conn: asyncpg.Connection) -> None:
        self._conn = conn

    async def _load_persona_profile(
        self,
        *,
        persona_profile_id: str,
        as_of: datetime,
    ) -> PersonaProfileAuthorityRecord:
        try:
            rows = await self._conn.fetch(
                """
                SELECT
                    persona_profile_id,
                    persona_name,
                    persona_kind,
                    status,
                    instruction_contract,
                    response_contract,
                    tool_policy,
                    runtime_hints,
                    effective_from,
                    effective_to,
                    decision_ref,
                    created_at
                FROM persona_profiles
                WHERE persona_profile_id = $1
                  AND status = 'active'
                  AND effective_from <= $2
                  AND (effective_to IS NULL OR effective_to > $2)
                ORDER BY effective_from DESC
                """,
                persona_profile_id,
                as_of,
            )
        except asyncpg.PostgresError as exc:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.read_failed",
                "failed to read persona profile",
                details={
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "table": "persona_profiles",
                },
            ) from exc

        if not rows:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.persona_profile_missing",
                "no active persona profile matched the requested selector",
                details={"persona_profile_id": persona_profile_id, "as_of": as_of.isoformat()},
            )
        if len(rows) > 1:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.persona_profile_ambiguous",
                "more than one active persona profile matched the requested selector",
                details={"persona_profile_id": persona_profile_id, "row_count": len(rows)},
            )
        return _persona_profile_from_row(rows[0])

    async def _load_persona_context_bindings_for_profile(
        self,
        *,
        persona_profile_id: str,
        binding_scope: str,
        as_of: datetime,
    ) -> tuple[PersonaContextBindingAuthorityRecord, ...]:
        try:
            rows = await self._conn.fetch(
                """
                SELECT
                    persona_context_binding_id,
                    persona_profile_id,
                    binding_scope,
                    workspace_ref,
                    runtime_profile_ref,
                    model_profile_id,
                    provider_policy_id,
                    context_selector,
                    binding_status,
                    position_index,
                    effective_from,
                    effective_to,
                    decision_ref,
                    created_at
                FROM persona_context_bindings
                WHERE persona_profile_id = $1
                  AND binding_scope = $2
                  AND binding_status = 'active'
                  AND effective_from <= $3
                  AND (effective_to IS NULL OR effective_to > $3)
                ORDER BY position_index, persona_context_binding_id
                """,
                persona_profile_id,
                binding_scope,
                as_of,
            )
        except asyncpg.PostgresError as exc:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.read_failed",
                "failed to read persona-context bindings",
                details={
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "table": "persona_context_bindings",
                },
            ) from exc

        if not rows:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.persona_binding_missing",
                "no active persona-context binding matched the requested selector",
                details={
                    "persona_profile_id": persona_profile_id,
                    "binding_scope": binding_scope,
                    "as_of": as_of.isoformat(),
                },
            )

        return tuple(_persona_context_binding_from_row(row) for row in rows)

    async def _load_persona_activation_for_operator_path(
        self,
        *,
        selector: PersonaActivationSelector,
    ) -> tuple[PersonaProfileAuthorityRecord, tuple[PersonaContextBindingAuthorityRecord, ...]]:
        try:
            rows = await self._conn.fetch(
                """
                SELECT
                    persona_context_binding_id,
                    persona_profile_id,
                    binding_scope,
                    workspace_ref,
                    runtime_profile_ref,
                    model_profile_id,
                    provider_policy_id,
                    context_selector,
                    binding_status,
                    position_index,
                    effective_from,
                    effective_to,
                    decision_ref,
                    created_at
                FROM persona_context_bindings
                WHERE binding_scope = $1
                  AND workspace_ref = $2
                  AND runtime_profile_ref = $3
                  AND binding_status = 'active'
                  AND effective_from <= $4
                  AND (effective_to IS NULL OR effective_to > $4)
                ORDER BY position_index, persona_context_binding_id
                """,
                selector.binding_scope,
                selector.workspace_ref,
                selector.runtime_profile_ref,
                selector.as_of,
            )
        except asyncpg.PostgresError as exc:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.read_failed",
                "failed to read operator-path persona bindings",
                details={
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "table": "persona_context_bindings",
                },
            ) from exc

        if not rows:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.persona_binding_missing",
                "no active persona-context binding matched the requested operator path",
                details={
                    "binding_scope": selector.binding_scope,
                    "workspace_ref": selector.workspace_ref,
                    "runtime_profile_ref": selector.runtime_profile_ref,
                    "operator_path": selector.operator_path,
                    "as_of": selector.as_of.isoformat(),
                },
            )

        bindings = tuple(_persona_context_binding_from_row(row) for row in rows)
        undiscriminated_binding_ids: list[str] = []
        matched_bindings: list[PersonaContextBindingAuthorityRecord] = []
        for index, binding in enumerate(bindings):
            try:
                context_selector = _require_json_mapping(
                    binding.context_selector,
                    field_name=f"persona_context_bindings[{index}].context_selector",
                )
            except PersonaAndForkAuthorityRepositoryError:
                undiscriminated_binding_ids.append(binding.persona_context_binding_id)
                continue
            operator_path = _optional_operator_path(context_selector.get("operator_path"))
            if operator_path is None:
                undiscriminated_binding_ids.append(binding.persona_context_binding_id)
                continue
            if operator_path == selector.operator_path:
                matched_bindings.append(binding)

        if undiscriminated_binding_ids:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.persona_operator_path_missing",
                "operator-path persona bindings must carry an explicit context_selector.operator_path discriminator",
                details={
                    "binding_scope": selector.binding_scope,
                    "workspace_ref": selector.workspace_ref,
                    "runtime_profile_ref": selector.runtime_profile_ref,
                    "operator_path": selector.operator_path,
                    "persona_context_binding_ids": tuple(undiscriminated_binding_ids),
                },
            )
        if not matched_bindings:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.persona_binding_missing",
                "no active persona-context binding matched the requested operator path",
                details={
                    "binding_scope": selector.binding_scope,
                    "workspace_ref": selector.workspace_ref,
                    "runtime_profile_ref": selector.runtime_profile_ref,
                    "operator_path": selector.operator_path,
                    "as_of": selector.as_of.isoformat(),
                },
            )

        persona_profile_ids = tuple(
            dict.fromkeys(binding.persona_profile_id for binding in matched_bindings)
        )
        if len(persona_profile_ids) > 1:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.persona_operator_path_ambiguous",
                "more than one active persona profile matched the requested operator path",
                details={
                    "binding_scope": selector.binding_scope,
                    "workspace_ref": selector.workspace_ref,
                    "runtime_profile_ref": selector.runtime_profile_ref,
                    "operator_path": selector.operator_path,
                    "persona_profile_ids": persona_profile_ids,
                    "persona_context_binding_ids": tuple(
                        binding.persona_context_binding_id for binding in matched_bindings
                    ),
                },
            )

        persona_profile = await self._load_persona_profile(
            persona_profile_id=persona_profile_ids[0],
            as_of=selector.as_of,
        )
        return persona_profile, tuple(matched_bindings)

    async def load_persona_activation(
        self,
        *,
        selector: PersonaActivationSelector,
    ) -> tuple[PersonaProfileAuthorityRecord, tuple[PersonaContextBindingAuthorityRecord, ...]]:
        if not isinstance(selector, PersonaActivationSelector):
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.invalid_selector",
                "persona_selector must be a PersonaActivationSelector",
                details={"value_type": type(selector).__name__},
            )

        if selector.selector_mode == "operator_path":
            return await self._load_persona_activation_for_operator_path(
                selector=selector,
            )

        if selector.persona_profile_id is None:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.invalid_selector",
                "persona-profile selectors must include persona_profile_id",
                details={"binding_scope": selector.binding_scope},
            )
        bindings = await self._load_persona_context_bindings_for_profile(
            persona_profile_id=selector.persona_profile_id,
            binding_scope=selector.binding_scope,
            as_of=selector.as_of,
        )
        persona_profile = await self._load_persona_profile(
            persona_profile_id=selector.persona_profile_id,
            as_of=selector.as_of,
        )
        return persona_profile, bindings

    async def load_fork_worktree_binding(
        self,
        *,
        selector: ForkOwnershipSelector,
    ) -> ForkWorktreeBindingAuthorityRecord:
        if not isinstance(selector, ForkOwnershipSelector):
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.invalid_selector",
                "fork_selector must be a ForkOwnershipSelector",
                details={"value_type": type(selector).__name__},
            )

        try:
            rows = await self._conn.fetch(
                """
                SELECT
                    fork_worktree_binding_id,
                    fork_profile_id,
                    sandbox_session_id,
                    workflow_run_id,
                    binding_scope,
                    binding_status,
                    workspace_ref,
                    runtime_profile_ref,
                    base_ref,
                    fork_ref,
                    worktree_ref,
                    created_at,
                    retired_at,
                    decision_ref
                FROM fork_worktree_bindings
                WHERE workspace_ref = $1
                  AND runtime_profile_ref = $2
                  AND fork_ref = $3
                  AND worktree_ref = $4
                  AND binding_status = 'active'
                  AND (retired_at IS NULL)
                ORDER BY created_at DESC, fork_worktree_binding_id
                """,
                selector.workspace_ref,
                selector.runtime_profile_ref,
                selector.fork_ref,
                selector.worktree_ref,
            )
        except asyncpg.PostgresError as exc:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.read_failed",
                "failed to read fork/worktree binding",
                details={
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "table": "fork_worktree_bindings",
                },
            ) from exc

        if not rows:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.fork_ownership_missing",
                "no active fork/worktree binding matched the requested ownership selector",
                details={
                    "workspace_ref": selector.workspace_ref,
                    "runtime_profile_ref": selector.runtime_profile_ref,
                    "fork_ref": selector.fork_ref,
                    "worktree_ref": selector.worktree_ref,
                },
            )
        if len(rows) > 1:
            raise PersonaAndForkAuthorityRepositoryError(
                "persona_authority.fork_ownership_ambiguous",
                "more than one active fork/worktree binding matched the requested ownership selector",
                details={
                    "workspace_ref": selector.workspace_ref,
                    "runtime_profile_ref": selector.runtime_profile_ref,
                    "fork_ref": selector.fork_ref,
                    "worktree_ref": selector.worktree_ref,
                    "row_count": len(rows),
                },
            )
        return _fork_worktree_binding_from_row(rows[0])

    async def load_persona_and_fork_authority(
        self,
        *,
        persona_selector: PersonaActivationSelector,
        fork_selector: ForkOwnershipSelector,
    ) -> PersonaAndForkAuthority:
        persona_profile, persona_context_bindings = await self.load_persona_activation(
            selector=persona_selector,
        )
        fork_worktree_binding = await self.load_fork_worktree_binding(
            selector=fork_selector,
        )
        return PersonaAndForkAuthority(
            as_of=persona_selector.as_of,
            persona_selector=persona_selector,
            fork_selector=fork_selector,
            persona_profile=persona_profile,
            persona_context_bindings=persona_context_bindings,
            fork_worktree_binding=fork_worktree_binding,
        )


async def load_persona_and_fork_authority(
    conn: asyncpg.Connection,
    *,
    persona_selector: PersonaActivationSelector,
    fork_selector: ForkOwnershipSelector,
) -> PersonaAndForkAuthority:
    """Load one explicit persona and fork/worktree authority snapshot."""

    repository = PostgresPersonaAndForkAuthorityRepository(conn)
    return await repository.load_persona_and_fork_authority(
        persona_selector=persona_selector,
        fork_selector=fork_selector,
    )


__all__ = [
    "ForkOwnershipSelector",
    "ForkWorktreeBindingAuthorityRecord",
    "PersonaActivationSelector",
    "PersonaAndForkAuthority",
    "PersonaAndForkAuthorityRepositoryError",
    "PersonaContextBindingAuthorityRecord",
    "PersonaProfileAuthorityRecord",
    "PostgresPersonaAndForkAuthorityRepository",
    "load_persona_and_fork_authority",
]
