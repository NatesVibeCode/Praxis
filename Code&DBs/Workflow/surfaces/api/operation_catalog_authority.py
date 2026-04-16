"""Shared operation-catalog authority for API surfaces."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timezone
from typing import Any

from contracts.operation_catalog import build_operation_catalog_response
from runtime.operation_catalog import (
    list_operation_source_policies,
    list_resolved_operation_definitions,
)


def build_operation_catalog_payload(pg: Any) -> dict[str, Any]:
    operations = [
        asdict(record)
        for record in list_resolved_operation_definitions(
            pg,
            include_disabled=True,
            limit=500,
        )
    ]
    source_policies = [
        asdict(record)
        for record in list_operation_source_policies(
            pg,
            include_disabled=True,
            limit=50,
        )
    ]
    return build_operation_catalog_response(
        operations=operations,
        source_policies=source_policies,
        generated_at=datetime.now(timezone.utc),
    )


__all__ = ["build_operation_catalog_payload"]
