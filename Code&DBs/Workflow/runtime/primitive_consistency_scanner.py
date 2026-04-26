"""Consistency scanner for the primitive_catalog.

Each primitive_catalog row declares the modules, catalog rows, and tests
its primitive must have to exist consistently.  This scanner reads each
declared spec and emits findings for:

  * declared module path that does not import (orphaned blueprint or
    dead module reference)
  * declared operation_ref that has no row in operation_catalog_registry
    (catalog under-registered)
  * declared event_contract_ref that has no row in
    authority_event_contracts (event contract under-registered)
  * declared test module that does not import (test was deleted or
    renamed without updating the blueprint)

Findings are structured.  Callers (CI test, gateway handler, operator)
decide how loud to be.

The scanner is read-only and side-effect-free, matching the pattern of
the invariant scanner.
"""

from __future__ import annotations

import importlib
import importlib.util
from collections.abc import Iterable, Mapping
from typing import Any


def _import_path_resolves(module_path: str) -> bool:
    """Return True when ``module_path`` is importable on the current sys.path."""
    if not module_path:
        return False
    try:
        spec = importlib.util.find_spec(module_path)
    except (ModuleNotFoundError, ValueError, ImportError):
        return False
    return spec is not None


def _operation_ref_exists(conn: Any, operation_ref: str) -> bool:
    rows = conn.execute(
        "SELECT 1 FROM operation_catalog_registry WHERE operation_ref = $1 LIMIT 1",
        operation_ref,
    )
    return bool(rows)


def _event_contract_ref_exists(conn: Any, event_contract_ref: str) -> bool:
    rows = conn.execute(
        "SELECT 1 FROM authority_event_contracts WHERE event_contract_ref = $1 LIMIT 1",
        event_contract_ref,
    )
    return bool(rows)


def _authority_domain_exists(conn: Any, authority_domain_ref: str) -> bool:
    rows = conn.execute(
        "SELECT 1 FROM authority_domains WHERE authority_domain_ref = $1 AND enabled = TRUE LIMIT 1",
        authority_domain_ref,
    )
    return bool(rows)


def _table_exists(conn: Any, table_name: str) -> bool:
    rows = conn.execute(
        "SELECT to_regclass($1) AS regclass_name",
        table_name,
    )
    if not rows:
        return False
    return bool(rows[0].get("regclass_name"))


_MODULE_SPEC_KEYS = (
    "authority_module",
    "engine_module",
    "handler_module",
    "consistency_scanner_module",
)
_MODULE_LIST_SPEC_KEYS = (
    "gateway_command_modules",
    "gateway_query_modules",
    "depends_on_modules",
    "test_modules",
)
_OPERATION_REF_KEYS = ("operation_refs",)
_EVENT_CONTRACT_KEYS = ("event_contract_refs",)


def scan_primitive_consistency(
    *,
    conn: Any,
    primitive: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Run consistency checks for one primitive blueprint."""

    slug = str(primitive.get("primitive_slug") or "").strip()
    spec = primitive.get("spec") or {}
    if not isinstance(spec, Mapping):
        return [
            {
                "primitive_slug": slug,
                "rule": "spec_must_be_object",
                "detail": f"spec is {type(spec).__name__}",
            }
        ]

    findings: list[dict[str, Any]] = []

    # Single-string module references
    for key in _MODULE_SPEC_KEYS:
        value = spec.get(key)
        if isinstance(value, str) and value.strip():
            if not _import_path_resolves(value):
                findings.append(
                    {
                        "primitive_slug": slug,
                        "rule": "declared_module_does_not_resolve",
                        "spec_key": key,
                        "module_path": value,
                    }
                )

    # List-of-string module references
    for key in _MODULE_LIST_SPEC_KEYS:
        value = spec.get(key) or []
        if not isinstance(value, list):
            continue
        for module_path in value:
            if not isinstance(module_path, str) or not module_path.strip():
                continue
            if not _import_path_resolves(module_path):
                findings.append(
                    {
                        "primitive_slug": slug,
                        "rule": "declared_module_does_not_resolve",
                        "spec_key": key,
                        "module_path": module_path,
                    }
                )

    # owns_table -> the table must exist in Postgres
    owns_table = spec.get("owns_table")
    if isinstance(owns_table, str) and owns_table.strip():
        if not _table_exists(conn, owns_table.strip()):
            findings.append(
                {
                    "primitive_slug": slug,
                    "rule": "owns_table_missing_in_postgres",
                    "table": owns_table.strip(),
                }
            )

    # authority_domain_ref -> must be enabled in authority_domains
    authority_domain_ref = spec.get("authority_domain_ref")
    if isinstance(authority_domain_ref, str) and authority_domain_ref.strip():
        if not _authority_domain_exists(conn, authority_domain_ref.strip()):
            findings.append(
                {
                    "primitive_slug": slug,
                    "rule": "authority_domain_missing_or_disabled",
                    "authority_domain_ref": authority_domain_ref.strip(),
                }
            )

    # operation_refs -> each must exist in operation_catalog_registry
    for key in _OPERATION_REF_KEYS:
        value = spec.get(key) or []
        if not isinstance(value, list):
            continue
        for operation_ref in value:
            if not isinstance(operation_ref, str) or not operation_ref.strip():
                continue
            if not _operation_ref_exists(conn, operation_ref.strip()):
                findings.append(
                    {
                        "primitive_slug": slug,
                        "rule": "operation_ref_missing_in_catalog",
                        "spec_key": key,
                        "operation_ref": operation_ref.strip(),
                    }
                )

    # event_contract_refs -> each must exist in authority_event_contracts
    for key in _EVENT_CONTRACT_KEYS:
        value = spec.get(key) or []
        if not isinstance(value, list):
            continue
        for ref in value:
            if not isinstance(ref, str) or not ref.strip():
                continue
            if not _event_contract_ref_exists(conn, ref.strip()):
                findings.append(
                    {
                        "primitive_slug": slug,
                        "rule": "event_contract_ref_missing",
                        "spec_key": key,
                        "event_contract_ref": ref.strip(),
                    }
                )

    return findings


def scan_all_primitives(
    *,
    conn: Any,
    primitives: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Run consistency checks across every primitive in ``primitives``.

    Returns ``{primitive_count, findings_count, findings}``.
    """

    primitives_list = list(primitives)
    findings: list[dict[str, Any]] = []
    for primitive in primitives_list:
        findings.extend(
            scan_primitive_consistency(conn=conn, primitive=primitive)
        )
    return {
        "primitive_count": len(primitives_list),
        "findings_count": len(findings),
        "findings": findings,
    }


__all__ = [
    "scan_all_primitives",
    "scan_primitive_consistency",
]
