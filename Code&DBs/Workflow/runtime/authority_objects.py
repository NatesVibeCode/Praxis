"""CQRS authority object inspection runtime.

This is the read side for the authority object registry. It answers the two
questions future agents need before touching durable state:

- Who owns this object?
- What is currently drifting outside the CQRS contract?
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel


class AuthorityObjectError(RuntimeError):
    """Raised when authority object inspection rejects a request."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        status_code: int = 400,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.status_code = status_code
        self.details = dict(details or {})


class ListAuthorityObjectsCommand(BaseModel):
    object_kind: str | None = None
    authority_domain_ref: str | None = None
    lifecycle_status: str | None = None
    include_inactive: bool = False
    limit: int = 100


class ListAuthorityDriftCommand(BaseModel):
    drift_kind: str | None = None
    object_kind: str | None = None
    limit: int = 100


class ListAuthorityAdoptionCommand(BaseModel):
    adoption_status: str | None = None
    authority_domain_ref: str | None = None
    table_name: str | None = None
    limit: int = 100


class ListAuthorityDomainSummaryCommand(BaseModel):
    adoption_status: str | None = None
    authority_domain_ref: str | None = None
    limit: int = 100


def _text(value: object, *, field_name: str, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise AuthorityObjectError(
                "authority_objects.invalid_submission",
                f"{field_name} is required",
                details={"field": field_name},
            )
        return None
    if not isinstance(value, str) or not value.strip():
        raise AuthorityObjectError(
            "authority_objects.invalid_submission",
            f"{field_name} must be a non-empty string",
            details={"field": field_name},
        )
    return value.strip()


def _limit(value: object) -> int:
    try:
        limit = int(value or 100)
    except (TypeError, ValueError) as exc:
        raise AuthorityObjectError(
            "authority_objects.invalid_submission",
            "limit must be an integer",
            details={"field": "limit", "value": value},
        ) from exc
    if limit < 1 or limit > 1000:
        raise AuthorityObjectError(
            "authority_objects.invalid_submission",
            "limit must be between 1 and 1000",
            details={"field": "limit", "value": limit},
        )
    return limit


def _fetch(conn: Any, query: str, *args: Any) -> list[dict[str, Any]]:
    if hasattr(conn, "fetch") and callable(conn.fetch):
        rows = conn.fetch(query, *args)
    else:
        rows = conn.execute(query, *args)
    return [dict(row) for row in rows or []]


def list_authority_objects(
    conn: Any,
    *,
    object_kind: str | None = None,
    authority_domain_ref: str | None = None,
    lifecycle_status: str | None = None,
    include_inactive: bool = False,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return registered authority objects with optional filters."""

    clauses = ["SELECT * FROM authority_object_ownership WHERE TRUE"]
    args: list[Any] = []
    normalized_kind = _text(object_kind, field_name="object_kind")
    normalized_domain = _text(authority_domain_ref, field_name="authority_domain_ref")
    normalized_status = _text(lifecycle_status, field_name="lifecycle_status")

    if normalized_kind is not None:
        args.append(normalized_kind)
        clauses.append(f"AND object_kind = ${len(args)}")
    if normalized_domain is not None:
        args.append(normalized_domain)
        clauses.append(f"AND authority_domain_ref = ${len(args)}")
    if normalized_status is not None:
        args.append(normalized_status)
        clauses.append(f"AND lifecycle_status = ${len(args)}")
    elif not include_inactive:
        clauses.append("AND lifecycle_status IN ('draft', 'active', 'legacy')")

    args.append(_limit(limit))
    clauses.append(f"ORDER BY authority_domain_ref, object_kind, object_name LIMIT ${len(args)}")
    return _fetch(conn, "\n".join(clauses), *args)


def list_authority_drift(
    conn: Any,
    *,
    drift_kind: str | None = None,
    object_kind: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return machine-readable CQRS authority drift."""

    clauses = ["SELECT * FROM authority_object_drift_report WHERE TRUE"]
    args: list[Any] = []
    normalized_drift = _text(drift_kind, field_name="drift_kind")
    normalized_kind = _text(object_kind, field_name="object_kind")
    if normalized_drift is not None:
        args.append(normalized_drift)
        clauses.append(f"AND drift_kind = ${len(args)}")
    if normalized_kind is not None:
        args.append(normalized_kind)
        clauses.append(f"AND object_kind = ${len(args)}")
    args.append(_limit(limit))
    clauses.append(f"ORDER BY drift_kind, object_kind, object_name LIMIT ${len(args)}")
    return _fetch(conn, "\n".join(clauses), *args)


def list_authority_adoption(
    conn: Any,
    *,
    adoption_status: str | None = None,
    authority_domain_ref: str | None = None,
    table_name: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return legacy-vs-CQRS table adoption state."""

    clauses = ["SELECT * FROM authority_schema_adoption_report WHERE TRUE"]
    args: list[Any] = []
    normalized_status = _text(adoption_status, field_name="adoption_status")
    normalized_domain = _text(authority_domain_ref, field_name="authority_domain_ref")
    normalized_table = _text(table_name, field_name="table_name")
    if normalized_status is not None:
        args.append(normalized_status)
        clauses.append(f"AND adoption_status = ${len(args)}")
    if normalized_domain is not None:
        args.append(normalized_domain)
        clauses.append(f"AND authority_domain_ref = ${len(args)}")
    if normalized_table is not None:
        args.append(normalized_table)
        clauses.append(f"AND table_name = ${len(args)}")
    args.append(_limit(limit))
    clauses.append(f"ORDER BY adoption_status, authority_domain_ref, table_name LIMIT ${len(args)}")
    return _fetch(conn, "\n".join(clauses), *args)


def list_authority_domain_summary(
    conn: Any,
    *,
    adoption_status: str | None = None,
    authority_domain_ref: str | None = None,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return domain-ranked CQRS adoption state for legacy inventory."""

    clauses = ["SELECT * FROM authority_legacy_domain_assignment_summary WHERE TRUE"]
    args: list[Any] = []
    normalized_status = _text(adoption_status, field_name="adoption_status")
    normalized_domain = _text(authority_domain_ref, field_name="authority_domain_ref")
    if normalized_status is not None:
        args.append(normalized_status)
        clauses.append(f"AND adoption_status = ${len(args)}")
    if normalized_domain is not None:
        args.append(normalized_domain)
        clauses.append(f"AND authority_domain_ref = ${len(args)}")
    args.append(_limit(limit))
    clauses.append(
        f"ORDER BY adoption_status, table_count DESC, authority_domain_ref LIMIT ${len(args)}"
    )
    return _fetch(conn, "\n".join(clauses), *args)


def handle_list_authority_objects(
    command: ListAuthorityObjectsCommand,
    subsystems: Any,
) -> dict[str, Any]:
    rows = list_authority_objects(
        subsystems.get_pg_conn(),
        object_kind=command.object_kind,
        authority_domain_ref=command.authority_domain_ref,
        lifecycle_status=command.lifecycle_status,
        include_inactive=command.include_inactive,
        limit=command.limit,
    )
    return {"status": "listed", "objects": rows, "count": len(rows)}


def handle_list_authority_drift(
    command: ListAuthorityDriftCommand,
    subsystems: Any,
) -> dict[str, Any]:
    rows = list_authority_drift(
        subsystems.get_pg_conn(),
        drift_kind=command.drift_kind,
        object_kind=command.object_kind,
        limit=command.limit,
    )
    return {"status": "listed", "drift": rows, "count": len(rows)}


def handle_list_authority_adoption(
    command: ListAuthorityAdoptionCommand,
    subsystems: Any,
) -> dict[str, Any]:
    rows = list_authority_adoption(
        subsystems.get_pg_conn(),
        adoption_status=command.adoption_status,
        authority_domain_ref=command.authority_domain_ref,
        table_name=command.table_name,
        limit=command.limit,
    )
    return {"status": "listed", "adoption": rows, "count": len(rows)}


def handle_list_authority_domain_summary(
    command: ListAuthorityDomainSummaryCommand,
    subsystems: Any,
) -> dict[str, Any]:
    rows = list_authority_domain_summary(
        subsystems.get_pg_conn(),
        adoption_status=command.adoption_status,
        authority_domain_ref=command.authority_domain_ref,
        limit=command.limit,
    )
    return {"status": "listed", "summary": rows, "count": len(rows)}


__all__ = [
    "AuthorityObjectError",
    "ListAuthorityAdoptionCommand",
    "ListAuthorityDomainSummaryCommand",
    "ListAuthorityDriftCommand",
    "ListAuthorityObjectsCommand",
    "handle_list_authority_adoption",
    "handle_list_authority_domain_summary",
    "handle_list_authority_drift",
    "handle_list_authority_objects",
    "list_authority_adoption",
    "list_authority_domain_summary",
    "list_authority_drift",
    "list_authority_objects",
]
