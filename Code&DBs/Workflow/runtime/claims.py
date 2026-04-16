"""Claim, lease, and proposal runtime mechanics.

This module owns:

- explicit claim/lease/proposal state transitions
- compare-and-swap route progression inside Postgres
- explicit sandbox session allocation and reuse rules
- sandbox bindings that prove shared-session usage without merging lifecycle truth

It does not append receipts or workflow events directly. Receipts remain owned by
`receipts/`; this module only keeps the runtime state mutation boundary explicit.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import lru_cache
from typing import Final, cast
import hashlib
import json

import asyncpg

from storage.migrations import WorkflowMigrationError, workflow_migration_statements
from runtime.execution.state_machine import validate_transition

from .domain import RouteIdentity, RunState, RuntimeBoundaryError, RuntimeLifecycleError
from registry.persona_authority import (
    ForkOwnershipSelector,
    ForkWorktreeBindingAuthorityRecord,
    PersonaAndForkAuthorityRepositoryError,
    PostgresPersonaAndForkAuthorityRepository,
)

_RUNTIME_SCHEMA_FILENAME = "004_claim_lease_proposal_runtime.sql"
_DUPLICATE_SQLSTATES = {"42P07", "42710"}
_VALID_SHARE_MODES = frozenset({"exclusive", "shared"})
_BOUNDED_FORK_OWNERSHIP_REUSE_REASON_CODE = "packet.authoritative_fork"
_TERMINAL_STATES = frozenset(
    {
        RunState.CLAIM_REJECTED,
        RunState.LEASE_EXPIRED,
        RunState.PROPOSAL_INVALID,
    }
)
_LEASE_REQUIRED_STATES = frozenset(
    {
        RunState.LEASE_REQUESTED,
        RunState.LEASE_BLOCKED,
        RunState.LEASE_ACTIVE,
        RunState.LEASE_EXPIRED,
        RunState.PROPOSAL_SUBMITTED,
        RunState.PROPOSAL_INVALID,
    }
)
_PROPOSAL_REQUIRED_STATES = frozenset(
    {
        RunState.PROPOSAL_SUBMITTED,
        RunState.PROPOSAL_INVALID,
    }
)

ALLOWED_TRANSITIONS: Final[Mapping[RunState, frozenset[RunState]]] = {
    RunState.CLAIM_RECEIVED: frozenset(
        {
            RunState.CLAIM_VALIDATING,
        }
    ),
    RunState.CLAIM_VALIDATING: frozenset(
        {
            RunState.CLAIM_ACCEPTED,
            RunState.CLAIM_BLOCKED,
            RunState.CLAIM_REJECTED,
        }
    ),
    RunState.CLAIM_BLOCKED: frozenset(
        {
            RunState.CLAIM_VALIDATING,
            RunState.CLAIM_REJECTED,
        }
    ),
    RunState.CLAIM_ACCEPTED: frozenset({RunState.LEASE_REQUESTED}),
    RunState.LEASE_REQUESTED: frozenset({RunState.LEASE_ACTIVE, RunState.LEASE_BLOCKED}),
    RunState.LEASE_BLOCKED: frozenset({RunState.LEASE_REQUESTED}),
    RunState.LEASE_ACTIVE: frozenset(
        {
            RunState.LEASE_EXPIRED,
            RunState.PROPOSAL_SUBMITTED,
            RunState.PROPOSAL_INVALID,
        }
    ),
}


@dataclass(frozen=True, slots=True)
class SandboxSessionRequest:
    """Explicit sandbox policy for one runtime transition."""

    sandbox_group_id: str | None = None
    share_mode: str = "exclusive"
    reuse_reason_code: str | None = None
    base_ref: str = "refs/heads/main"
    base_digest: str = "sha256:unknown"
    sandbox_root: str = "/tmp/workflow-sandbox"
    expires_at: datetime | None = None
    fork_ref: str | None = None
    worktree_ref: str | None = None


@dataclass(frozen=True, slots=True)
class ClaimLeaseProposalTransitionRequest:
    """One explicit runtime transition request."""

    run_id: str
    from_state: RunState
    to_state: RunState
    reason_code: str
    occurred_at: datetime
    expected_transition_seq: int
    claim_id: str
    lease_id: str | None = None
    proposal_id: str | None = None
    event_id: str | None = None
    sandbox: SandboxSessionRequest | None = None


@dataclass(frozen=True, slots=True)
class ClaimLeaseProposalSnapshot:
    """Canonical read view for the route mechanics owned by this module."""

    run_id: str
    workflow_id: str
    request_id: str
    current_state: RunState
    claim_id: str
    lease_id: str | None
    proposal_id: str | None
    attempt_no: int
    transition_seq: int
    sandbox_group_id: str | None
    sandbox_session_id: str | None
    share_mode: str
    reuse_reason_code: str | None
    last_event_id: str | None


def _json_value(value: object) -> object:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _require_text(value: str | None, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise RuntimeBoundaryError(f"{field_name} must be a non-empty string")
    return value.strip()


def _require_utc(value: datetime, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise RuntimeBoundaryError(f"{field_name} must be a datetime")
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise RuntimeBoundaryError(f"{field_name} must be UTC-backed")
    return value


def _optional_text(value: str | None, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _share_mode(value: str) -> str:
    normalized = _require_text(value, field_name="share_mode")
    if normalized not in _VALID_SHARE_MODES:
        raise RuntimeBoundaryError("share_mode must be 'exclusive' or 'shared'")
    return normalized


def _uses_bounded_fork_ownership_path(*, share_mode: str, reuse_reason_code: str | None) -> bool:
    return (
        share_mode == "shared"
        and reuse_reason_code == _BOUNDED_FORK_OWNERSHIP_REUSE_REASON_CODE
    )


def _validate_route_path_selection(*, share_mode: str, reuse_reason_code: str | None) -> None:
    if (
        reuse_reason_code == _BOUNDED_FORK_OWNERSHIP_REUSE_REASON_CODE
        and share_mode != "shared"
    ):
        raise RuntimeBoundaryError(
            "bounded fork ownership path requires share_mode 'shared' at route registration"
        )


def _is_duplicate_object_error(error: BaseException) -> bool:
    return getattr(error, "sqlstate", None) in _DUPLICATE_SQLSTATES


@lru_cache(maxsize=1)
def _schema_statements() -> tuple[str, ...]:
    try:
        return workflow_migration_statements(_RUNTIME_SCHEMA_FILENAME)
    except WorkflowMigrationError as exc:
        if exc.reason_code == "workflow.migration_empty":
            raise RuntimeBoundaryError(
                "runtime schema file did not contain executable statements"
            ) from exc
        raise RuntimeBoundaryError(
            "runtime schema file could not be read from the canonical workflow migration root"
        ) from exc


def _snapshot_from_row(row: asyncpg.Record) -> ClaimLeaseProposalSnapshot:
    return ClaimLeaseProposalSnapshot(
        run_id=cast(str, row["run_id"]),
        workflow_id=cast(str, row["workflow_id"]),
        request_id=cast(str, row["request_id"]),
        current_state=RunState(cast(str, row["current_state"])),
        claim_id=cast(str, row["claim_id"]),
        lease_id=cast(str | None, row["lease_id"]),
        proposal_id=cast(str | None, row["proposal_id"]),
        attempt_no=cast(int, row["attempt_no"]),
        transition_seq=cast(int, row["transition_seq"]),
        sandbox_group_id=cast(str | None, row["sandbox_group_id"]),
        sandbox_session_id=cast(str | None, row["sandbox_session_id"]),
        share_mode=cast(str, row["share_mode"]),
        reuse_reason_code=cast(str | None, row["reuse_reason_code"]),
        last_event_id=cast(str | None, row["last_event_id"]),
    )


def _transition_allowed(*, from_state: RunState, to_state: RunState) -> None:
    allowed = ALLOWED_TRANSITIONS.get(from_state, frozenset())
    if to_state not in allowed:
        raise RuntimeLifecycleError(
            f"invalid claim/lease/proposal transition: {from_state.value} -> {to_state.value}"
        )


def _lease_id_for_transition(
    *,
    current_lease_id: str | None,
    requested_lease_id: str | None,
    to_state: RunState,
) -> str | None:
    if current_lease_id is not None:
        if requested_lease_id is not None and requested_lease_id != current_lease_id:
            raise RuntimeLifecycleError("lease_id cannot be rewritten once assigned")
        return current_lease_id
    if to_state not in _LEASE_REQUIRED_STATES:
        return None
    return _require_text(requested_lease_id, field_name="lease_id")


def _proposal_id_for_transition(
    *,
    current_proposal_id: str | None,
    requested_proposal_id: str | None,
    to_state: RunState,
) -> str | None:
    if current_proposal_id is not None:
        if requested_proposal_id is not None and requested_proposal_id != current_proposal_id:
            raise RuntimeLifecycleError("proposal_id cannot be rewritten once assigned")
        return current_proposal_id
    if to_state not in _PROPOSAL_REQUIRED_STATES:
        return None
    return _require_text(requested_proposal_id, field_name="proposal_id")


def _sandbox_binding_id(*, run_id: str, transition_seq: int, binding_role: str) -> str:
    return f"sandbox_binding:{run_id}:{transition_seq}:{binding_role}"


def _sandbox_session_id(*, run_id: str, transition_seq: int) -> str:
    return f"sandbox_session:{run_id}:{transition_seq}"


def _shared_sandbox_compatibility_key(
    *,
    sandbox_group_id: str,
    workspace_ref: str,
    runtime_profile_ref: str,
    authority_context_digest: str,
    base_ref: str,
    base_digest: str,
) -> str:
    return "|".join(
        (
            sandbox_group_id,
            workspace_ref,
            runtime_profile_ref,
            authority_context_digest,
            base_ref,
            base_digest,
        )
    )


def _advisory_lock_key(value: str) -> int:
    digest = hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, byteorder="big", signed=True)


@dataclass(slots=True)
class ClaimLeaseProposalRuntime:
    """Explicit Postgres-backed runtime authority for the claim/lease/proposal path."""

    default_sandbox_ttl: timedelta = timedelta(hours=2)

    async def bootstrap_schema(self, conn: asyncpg.Connection) -> None:
        async with conn.transaction():
            for statement in _schema_statements():
                try:
                    async with conn.transaction():
                        await conn.execute(statement)
                except asyncpg.PostgresError as exc:
                    if _is_duplicate_object_error(exc):
                        continue
                    raise RuntimeBoundaryError(
                        f"failed to bootstrap claim/lease/proposal runtime schema: {statement[:120]}"
                    ) from exc

    async def register_route(
        self,
        conn: asyncpg.Connection,
        *,
        route_identity: RouteIdentity,
        current_state: RunState,
        share_mode: str = "exclusive",
        reuse_reason_code: str | None = None,
    ) -> ClaimLeaseProposalSnapshot:
        _require_text(route_identity.run_id, field_name="route_identity.run_id")
        _require_text(route_identity.workflow_id, field_name="route_identity.workflow_id")
        _require_text(route_identity.request_id, field_name="route_identity.request_id")
        claim_id = _require_text(route_identity.claim_id, field_name="route_identity.claim_id")
        normalized_share_mode = _share_mode(share_mode)
        normalized_reuse_reason = _optional_text(
            reuse_reason_code,
            field_name="reuse_reason_code",
        )
        _validate_route_path_selection(
            share_mode=normalized_share_mode,
            reuse_reason_code=normalized_reuse_reason,
        )

        async with conn.transaction():
            run_row = await conn.fetchrow(
                """
                SELECT run_id, workflow_id, request_id, current_state, last_event_id
                FROM workflow_runs
                WHERE run_id = $1
                FOR UPDATE
                """,
                route_identity.run_id,
            )
            if run_row is None:
                raise RuntimeBoundaryError(
                    f"workflow run {route_identity.run_id!r} must exist before route registration"
                )
            if run_row["current_state"] != current_state.value:
                raise RuntimeLifecycleError(
                    "workflow_runs current_state must match the route registration state"
                )

            await conn.execute(
                """
                INSERT INTO workflow_claim_lease_proposal_runtime (
                    run_id,
                    workflow_id,
                    request_id,
                    authority_context_ref,
                    authority_context_digest,
                    claim_id,
                    lease_id,
                    proposal_id,
                    promotion_decision_id,
                    attempt_no,
                    transition_seq,
                    sandbox_group_id,
                    sandbox_session_id,
                    share_mode,
                    reuse_reason_code,
                    created_at,
                    updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, NULL, NULL, $12, $13, $14, $14
                )
                ON CONFLICT (run_id) DO NOTHING
                """,
                route_identity.run_id,
                route_identity.workflow_id,
                route_identity.request_id,
                route_identity.authority_context_ref,
                route_identity.authority_context_digest,
                claim_id,
                route_identity.lease_id,
                route_identity.proposal_id,
                route_identity.promotion_decision_id,
                route_identity.attempt_no,
                route_identity.transition_seq,
                normalized_share_mode,
                normalized_reuse_reason,
                _now(),
            )

            row = await self._fetch_route_row(conn, run_id=route_identity.run_id, for_update=False)
            assert row is not None
            snapshot = _snapshot_from_row(row)
            if snapshot.claim_id != claim_id:
                raise RuntimeBoundaryError("existing route row carries a different claim_id")
            return snapshot

    async def inspect_route(
        self,
        conn: asyncpg.Connection,
        *,
        run_id: str,
    ) -> ClaimLeaseProposalSnapshot:
        row = await self._fetch_route_row(conn, run_id=run_id, for_update=False)
        if row is None:
            raise RuntimeBoundaryError(f"runtime route {run_id!r} is missing")
        return _snapshot_from_row(row)

    async def advance_transition(
        self,
        conn: asyncpg.Connection,
        *,
        transition: ClaimLeaseProposalTransitionRequest,
    ) -> ClaimLeaseProposalSnapshot:
        _transition_allowed(from_state=transition.from_state, to_state=transition.to_state)
        validate_transition(transition)
        _require_utc(transition.occurred_at, field_name="occurred_at")
        _require_text(transition.reason_code, field_name="reason_code")

        async with conn.transaction():
            row = await self._fetch_route_row(conn, run_id=transition.run_id, for_update=True)
            if row is None:
                raise RuntimeBoundaryError(f"runtime route {transition.run_id!r} is missing")

            if row["current_state"] != transition.from_state.value:
                raise RuntimeLifecycleError(
                    f"expected {transition.from_state.value} but route is {row['current_state']}"
                )
            if row["transition_seq"] != transition.expected_transition_seq:
                raise RuntimeLifecycleError(
                    "transition_seq mismatch: concurrent runtime update won the compare-and-swap"
                )

            current_claim_id = cast(str, row["claim_id"])
            if transition.claim_id != current_claim_id:
                raise RuntimeLifecycleError("claim_id must match the current route lineage")

            next_lease_id = _lease_id_for_transition(
                current_lease_id=cast(str | None, row["lease_id"]),
                requested_lease_id=transition.lease_id,
                to_state=transition.to_state,
            )
            next_proposal_id = _proposal_id_for_transition(
                current_proposal_id=cast(str | None, row["proposal_id"]),
                requested_proposal_id=transition.proposal_id,
                to_state=transition.to_state,
            )
            next_transition_seq = transition.expected_transition_seq + 1

            sandbox_group_id = cast(str | None, row["sandbox_group_id"])
            sandbox_session_id = cast(str | None, row["sandbox_session_id"])
            share_mode = cast(str, row["share_mode"])
            reuse_reason_code = cast(str | None, row["reuse_reason_code"])
            if transition.to_state is RunState.LEASE_ACTIVE:
                sandbox_group_id, sandbox_session_id, share_mode, reuse_reason_code = (
                    await self._activate_lease_sandbox(
                        conn,
                        row=row,
                        transition=transition,
                        lease_id=next_lease_id,
                        next_transition_seq=next_transition_seq,
                    )
                )
                await self._insert_sandbox_binding(
                    conn,
                    sandbox_session_id=sandbox_session_id,
                    workflow_id=cast(str, row["workflow_id"]),
                    run_id=transition.run_id,
                    claim_id=current_claim_id,
                    lease_id=next_lease_id,
                    proposal_id=None,
                    binding_role="lease",
                    reuse_reason_code=reuse_reason_code,
                    bound_at=transition.occurred_at,
                    transition_seq=next_transition_seq,
                )
            elif transition.to_state is RunState.PROPOSAL_SUBMITTED:
                sandbox_session_id = cast(str | None, row["sandbox_session_id"])
                if sandbox_session_id is None:
                    raise RuntimeBoundaryError(
                        "proposal submission requires an active sandbox session"
                    )
                await self._insert_sandbox_binding(
                    conn,
                    sandbox_session_id=sandbox_session_id,
                    workflow_id=cast(str, row["workflow_id"]),
                    run_id=transition.run_id,
                    claim_id=current_claim_id,
                    lease_id=next_lease_id,
                    proposal_id=next_proposal_id,
                    binding_role="proposal",
                    reuse_reason_code=cast(str | None, row["reuse_reason_code"]),
                    bound_at=transition.occurred_at,
                    transition_seq=next_transition_seq,
                )

            terminal_reason_code = (
                transition.reason_code if transition.to_state in _TERMINAL_STATES else None
            )
            run_updated = await conn.execute(
                """
                UPDATE workflow_runs
                SET current_state = $2,
                    terminal_reason_code = $3,
                    last_event_id = COALESCE($4, last_event_id)
                WHERE run_id = $1
                  AND current_state = $5
                RETURNING run_id
                """,
                transition.run_id,
                transition.to_state.value,
                terminal_reason_code,
                transition.event_id,
                transition.from_state.value,
            )
            if not run_updated:
                raise RuntimeLifecycleError(
                    "workflow_run state drifted during transition"
                )
            await conn.execute(
                """
                UPDATE workflow_claim_lease_proposal_runtime
                SET lease_id = $2,
                    proposal_id = $3,
                    transition_seq = $4,
                    sandbox_group_id = $5,
                    sandbox_session_id = $6,
                    share_mode = $7,
                    reuse_reason_code = $8,
                    updated_at = $9
                WHERE run_id = $1
                """,
                transition.run_id,
                next_lease_id,
                next_proposal_id,
                next_transition_seq,
                sandbox_group_id,
                sandbox_session_id,
                share_mode,
                reuse_reason_code,
                transition.occurred_at,
            )

            updated_row = await self._fetch_route_row(
                conn,
                run_id=transition.run_id,
                for_update=False,
            )
            assert updated_row is not None
            return _snapshot_from_row(updated_row)

    async def _activate_lease_sandbox(
        self,
        conn: asyncpg.Connection,
        *,
        row: asyncpg.Record,
        transition: ClaimLeaseProposalTransitionRequest,
        lease_id: str | None,
        next_transition_seq: int,
    ) -> tuple[str | None, str, str, str | None]:
        if transition.sandbox is None:
            raise RuntimeBoundaryError("lease activation requires an explicit sandbox request")
        assert lease_id is not None

        sandbox = transition.sandbox
        requested_share_mode = _share_mode(sandbox.share_mode)
        sandbox_group_id = _optional_text(
            sandbox.sandbox_group_id,
            field_name="sandbox_group_id",
        )
        requested_reuse_reason_code = _optional_text(
            sandbox.reuse_reason_code,
            field_name="reuse_reason_code",
        )
        base_ref = _require_text(sandbox.base_ref, field_name="base_ref")
        base_digest = _require_text(sandbox.base_digest, field_name="base_digest")
        sandbox_root = _require_text(sandbox.sandbox_root, field_name="sandbox_root")
        expires_at = sandbox.expires_at or (transition.occurred_at + self.default_sandbox_ttl)
        _require_utc(expires_at, field_name="expires_at")

        workspace_ref = cast(str, row["workspace_ref"])
        runtime_profile_ref = cast(str, row["runtime_profile_ref"])
        authority_context_digest = cast(str, row["authority_context_digest"])
        route_share_mode = _share_mode(cast(str, row["share_mode"]))
        route_reuse_reason_code = _optional_text(
            cast(str | None, row["reuse_reason_code"]),
            field_name="route.reuse_reason_code",
        )
        uses_bounded_fork_ownership_path = _uses_bounded_fork_ownership_path(
            share_mode=route_share_mode,
            reuse_reason_code=route_reuse_reason_code,
        )
        request_attempts_bounded_fork_ownership = (
            requested_reuse_reason_code == _BOUNDED_FORK_OWNERSHIP_REUSE_REASON_CODE
            or sandbox.fork_ref is not None
            or sandbox.worktree_ref is not None
        )
        if uses_bounded_fork_ownership_path:
            if requested_share_mode != route_share_mode:
                raise RuntimeBoundaryError(
                    "bounded fork ownership path requires share_mode 'shared' on lease activation"
                )
            if requested_reuse_reason_code != route_reuse_reason_code:
                raise RuntimeBoundaryError(
                    "bounded fork ownership path requires reuse_reason_code "
                    f"{_BOUNDED_FORK_OWNERSHIP_REUSE_REASON_CODE!r} on lease activation"
                )
            share_mode = route_share_mode
            reuse_reason_code = route_reuse_reason_code
        else:
            if request_attempts_bounded_fork_ownership:
                raise RuntimeBoundaryError(
                    "bounded fork ownership adoption must be selected at route registration"
                )
            share_mode = requested_share_mode
            reuse_reason_code = requested_reuse_reason_code

        authoritative_binding: ForkWorktreeBindingAuthorityRecord | None = None
        if uses_bounded_fork_ownership_path:
            authoritative_binding = await self._load_bounded_fork_ownership_binding(
                conn,
                workspace_ref=workspace_ref,
                runtime_profile_ref=runtime_profile_ref,
                share_mode=share_mode,
                base_ref=base_ref,
                sandbox=sandbox,
            )
        shared_compatibility_key: str | None = None
        if share_mode == "shared":
            if sandbox_group_id is None:
                raise RuntimeBoundaryError("shared sandbox reuse requires sandbox_group_id")
            if reuse_reason_code is None:
                raise RuntimeBoundaryError("shared sandbox reuse requires reuse_reason_code")
            shared_compatibility_key = _shared_sandbox_compatibility_key(
                sandbox_group_id=sandbox_group_id,
                workspace_ref=workspace_ref,
                runtime_profile_ref=runtime_profile_ref,
                authority_context_digest=authority_context_digest,
                base_ref=base_ref,
                base_digest=base_digest,
            )
            await conn.execute(
                "SELECT pg_advisory_xact_lock($1::bigint)",
                _advisory_lock_key(f"shared_sandbox_group:{sandbox_group_id}"),
            )
            await conn.execute(
                """
                UPDATE sandbox_sessions
                SET closed_at = expires_at,
                    closed_reason_code = COALESCE(closed_reason_code, 'sandbox.expired')
                WHERE sandbox_group_id = $1
                  AND share_mode = 'shared'
                  AND closed_at IS NULL
                  AND expires_at <= $2
                """,
                sandbox_group_id,
                transition.occurred_at,
            )
            authoritative_sandbox_session_id: str | None = None
            if authoritative_binding is not None:
                authoritative_sandbox_session_id = (
                    await self._require_live_authoritative_shared_session(
                        conn,
                        binding=authoritative_binding,
                        sandbox_group_id=sandbox_group_id,
                        shared_compatibility_key=shared_compatibility_key,
                        as_of=transition.occurred_at,
                    )
                )
            live_sessions = await conn.fetch(
                """
                SELECT sandbox_session_id, shared_compatibility_key
                FROM sandbox_sessions
                WHERE sandbox_group_id = $1
                  AND share_mode = 'shared'
                  AND closed_at IS NULL
                  AND expires_at > $2
                ORDER BY opened_at ASC, sandbox_session_id ASC
                FOR UPDATE
                """,
                sandbox_group_id,
                transition.occurred_at,
            )
            compatible_sessions = [
                cast(str, live_session["sandbox_session_id"])
                for live_session in live_sessions
                if live_session["shared_compatibility_key"] == shared_compatibility_key
            ]
            incompatible_sessions = [
                cast(str, live_session["sandbox_session_id"])
                for live_session in live_sessions
                if live_session["shared_compatibility_key"] != shared_compatibility_key
            ]
            if incompatible_sessions:
                raise RuntimeBoundaryError(
                    "shared sandbox reuse rejected: sandbox_group_id already carries a live incompatible session"
                )
            if len(compatible_sessions) > 1:
                raise RuntimeBoundaryError(
                    "shared sandbox reuse rejected: duplicate live shared sessions exist for the compatibility tuple"
                )
            if authoritative_sandbox_session_id is not None:
                if authoritative_sandbox_session_id not in compatible_sessions:
                    raise RuntimeBoundaryError(
                        "bounded fork ownership adoption rejected lease activation: "
                        "the active fork/worktree binding does not point to a live compatible shared sandbox session"
                    )
                sandbox_session_id = authoritative_sandbox_session_id
                await conn.execute(
                    """
                    UPDATE sandbox_sessions
                    SET expires_at = GREATEST(expires_at, $2)
                    WHERE sandbox_session_id = $1
                    """,
                    sandbox_session_id,
                    expires_at,
                )
                return sandbox_group_id, sandbox_session_id, share_mode, reuse_reason_code
            if compatible_sessions:
                sandbox_session_id = compatible_sessions[0]
                await conn.execute(
                    """
                    UPDATE sandbox_sessions
                    SET expires_at = GREATEST(expires_at, $2)
                    WHERE sandbox_session_id = $1
                    """,
                    sandbox_session_id,
                    expires_at,
                )
                return sandbox_group_id, sandbox_session_id, share_mode, reuse_reason_code
        else:
            sandbox_group_id = sandbox_group_id or f"group:{transition.run_id}"

        sandbox_session_id = _sandbox_session_id(
            run_id=transition.run_id,
            transition_seq=next_transition_seq,
        )
        await conn.execute(
            """
            INSERT INTO sandbox_sessions (
                sandbox_session_id,
                sandbox_group_id,
                workspace_ref,
                runtime_profile_ref,
                base_ref,
                base_digest,
                authority_context_digest,
                shared_compatibility_key,
                sandbox_root,
                share_mode,
                opened_at,
                expires_at,
                closed_at,
                closed_reason_code,
                owner_route_ref
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NULL, NULL, $13
            )
            """,
            sandbox_session_id,
            sandbox_group_id,
            workspace_ref,
            runtime_profile_ref,
            base_ref,
            base_digest,
            authority_context_digest,
            shared_compatibility_key,
            sandbox_root,
            share_mode,
            transition.occurred_at,
            expires_at,
            transition.run_id,
        )
        return sandbox_group_id, sandbox_session_id, share_mode, reuse_reason_code

    async def _load_bounded_fork_ownership_binding(
        self,
        conn: asyncpg.Connection,
        *,
        workspace_ref: str,
        runtime_profile_ref: str,
        share_mode: str,
        base_ref: str,
        sandbox: SandboxSessionRequest,
    ) -> ForkWorktreeBindingAuthorityRecord:
        fork_ref = _optional_text(sandbox.fork_ref, field_name="fork_ref")
        worktree_ref = _optional_text(sandbox.worktree_ref, field_name="worktree_ref")
        if fork_ref is None or worktree_ref is None:
            raise RuntimeBoundaryError(
                "bounded fork ownership path requires both fork_ref and worktree_ref"
            )
        if share_mode != "shared":
            raise RuntimeBoundaryError(
                "bounded fork ownership adoption is only supported for shared sandbox reuse"
            )

        repository = PostgresPersonaAndForkAuthorityRepository(conn)
        try:
            binding = await repository.load_fork_worktree_binding(
                selector=ForkOwnershipSelector(
                    workspace_ref=_require_text(
                        workspace_ref,
                        field_name="request_envelope.workspace_ref",
                    ),
                    runtime_profile_ref=_require_text(
                        runtime_profile_ref,
                        field_name="request_envelope.runtime_profile_ref",
                    ),
                    fork_ref=fork_ref,
                    worktree_ref=worktree_ref,
                )
            )
        except PersonaAndForkAuthorityRepositoryError as exc:
            if exc.reason_code in {
                "persona_authority.fork_ownership_missing",
                "persona_authority.fork_ownership_ambiguous",
            }:
                raise RuntimeBoundaryError(
                    f"bounded fork ownership adoption rejected lease activation: {exc}"
                ) from exc
            raise RuntimeBoundaryError(
                "bounded fork ownership adoption could not read fork/worktree authority safely"
            ) from exc

        if binding.base_ref != base_ref:
            raise RuntimeBoundaryError(
                "bounded fork ownership adoption requires base_ref to match the active fork/worktree binding"
            )
        return binding

    async def _require_live_authoritative_shared_session(
        self,
        conn: asyncpg.Connection,
        *,
        binding: ForkWorktreeBindingAuthorityRecord,
        sandbox_group_id: str,
        shared_compatibility_key: str,
        as_of: datetime,
    ) -> str:
        row = await conn.fetchrow(
            """
            SELECT
                sandbox_session_id,
                sandbox_group_id,
                shared_compatibility_key,
                share_mode,
                expires_at,
                closed_at
            FROM sandbox_sessions
            WHERE sandbox_session_id = $1
            FOR UPDATE
            """,
            binding.sandbox_session_id,
        )
        if row is None:
            raise RuntimeBoundaryError(
                "bounded fork ownership adoption rejected lease activation: "
                "the active fork/worktree binding does not point to a live compatible shared sandbox session"
            )

        session_share_mode = _share_mode(cast(str, row["share_mode"]))
        session_expires_at = cast(datetime | None, row["expires_at"])
        session_closed_at = cast(datetime | None, row["closed_at"])
        session_sandbox_group_id = _require_text(
            cast(str | None, row["sandbox_group_id"]),
            field_name="sandbox_sessions.sandbox_group_id",
        )
        session_shared_compatibility_key = _require_text(
            cast(str | None, row["shared_compatibility_key"]),
            field_name="sandbox_sessions.shared_compatibility_key",
        )
        if (
            session_share_mode != "shared"
            or session_closed_at is not None
            or session_expires_at is None
            or session_expires_at <= as_of
            or session_sandbox_group_id != sandbox_group_id
            or session_shared_compatibility_key != shared_compatibility_key
        ):
            raise RuntimeBoundaryError(
                "bounded fork ownership adoption rejected lease activation: "
                "the active fork/worktree binding does not point to a live compatible shared sandbox session"
            )
        return binding.sandbox_session_id

    async def _insert_sandbox_binding(
        self,
        conn: asyncpg.Connection,
        *,
        sandbox_session_id: str,
        workflow_id: str,
        run_id: str,
        claim_id: str,
        lease_id: str | None,
        proposal_id: str | None,
        binding_role: str,
        reuse_reason_code: str | None,
        bound_at: datetime,
        transition_seq: int,
    ) -> None:
        await conn.execute(
            """
            INSERT INTO sandbox_bindings (
                sandbox_binding_id,
                sandbox_session_id,
                workflow_id,
                run_id,
                claim_id,
                lease_id,
                proposal_id,
                work_packet_id,
                binding_role,
                reuse_reason_code,
                bound_at,
                released_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, NULL, $8, $9, $10, NULL
            )
            """,
            _sandbox_binding_id(
                run_id=run_id,
                transition_seq=transition_seq,
                binding_role=binding_role,
            ),
            sandbox_session_id,
            workflow_id,
            run_id,
            claim_id,
            lease_id,
            proposal_id,
            binding_role,
            reuse_reason_code,
            bound_at,
        )

    async def _fetch_route_row(
        self,
        conn: asyncpg.Connection,
        *,
        run_id: str,
        for_update: bool,
    ) -> asyncpg.Record | None:
        suffix = " FOR UPDATE OF route, run" if for_update else ""
        return await conn.fetchrow(
            f"""
            SELECT
                route.run_id,
                route.workflow_id,
                route.request_id,
                route.authority_context_digest,
                route.claim_id,
                route.lease_id,
                route.proposal_id,
                route.attempt_no,
                route.transition_seq,
                route.sandbox_group_id,
                route.sandbox_session_id,
                route.share_mode,
                route.reuse_reason_code,
                run.current_state,
                run.last_event_id,
                run.request_envelope->>'workspace_ref' AS workspace_ref,
                run.request_envelope->>'runtime_profile_ref' AS runtime_profile_ref
            FROM workflow_claim_lease_proposal_runtime AS route
            JOIN workflow_runs AS run
              ON run.run_id = route.run_id
            WHERE route.run_id = $1
            {suffix}
            """,
            run_id,
        )


__all__ = [
    "ALLOWED_TRANSITIONS",
    "ClaimLeaseProposalRuntime",
    "ClaimLeaseProposalSnapshot",
    "ClaimLeaseProposalTransitionRequest",
    "SandboxSessionRequest",
]
