"""Runtime-derived authority overlap discovery for candidate patches.

The candidate's writing agent does not get to decide what authority units the
patch touches. The runtime derives that view from the patch contents, the
intended file paths, and (for migrations) the SQL text. The result is a set
of `runtime_derived` impact rows that get compared against the agent's
declared impact rows during preflight.

Discovery vectors covered in V1:

* Path-pattern classification (which authority domain a file belongs to).
* SQL migration parsing for operation catalog / authority object / data
  dictionary / event contract registration.
* Python handler file inspection for command/query class names.

Future vectors (deferred, not in V1):

* AST parsing of FastAPI route decorators.
* MCP TOOLS dict diffing.
* Provider route table mutations.
* Verifier authority registration.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import PurePosixPath
import re
from typing import Any


AUTHORITY_BEARING_PATH_PATTERNS: tuple[tuple[str, str], ...] = (
    ("Code&DBs/Databases/migrations/workflow/", "migration_ref"),
    ("Code&DBs/Workflow/runtime/operations/commands/", "handler_ref"),
    ("Code&DBs/Workflow/runtime/operations/queries/", "handler_ref"),
    ("Code&DBs/Workflow/surfaces/mcp/catalog.py", "mcp_tool"),
    ("Code&DBs/Workflow/surfaces/mcp/tools/", "mcp_tool"),
    ("Code&DBs/Workflow/surfaces/mcp/cli_metadata.py", "cli_alias"),
    ("Code&DBs/Workflow/surfaces/api/", "http_route"),
    ("Code&DBs/Workflow/runtime/operation_catalog.py", "operation_ref"),
    ("Code&DBs/Workflow/runtime/operation_catalog_bindings.py", "operation_ref"),
    ("Code&DBs/Workflow/runtime/operation_catalog_gateway.py", "operation_ref"),
    ("Code&DBs/Workflow/runtime/task_type_router.py", "provider_route_ref"),
    ("Code&DBs/Workflow/runtime/workflow/", "operation_ref"),
    ("Code&DBs/Workflow/runtime/feedback_authority.py", "event_type"),
    ("Code&DBs/Workflow/runtime/verifier_authority.py", "verifier_ref"),
    ("Code&DBs/Workflow/runtime/sandbox_runtime.py", "operation_ref"),
    ("Code&DBs/Workflow/runtime/authority_objects.py", "authority_object_ref"),
    ("Code&DBs/Workflow/runtime/data_dictionary", "data_dictionary_object_kind"),
    ("Code&DBs/Workflow/runtime/projector", "data_dictionary_object_kind"),
)


@dataclass(frozen=True, slots=True)
class DiscoveredImpact:
    """One runtime-derived authority impact for a candidate patch."""

    unit_kind: str
    unit_ref: str
    dispatch_effect: str
    intent_hint: str
    discovery_evidence: dict[str, Any]
    predecessor_unit_kind: str | None = None
    predecessor_unit_ref: str | None = None


def _normalize_path(path: str) -> str:
    text = str(path or "").strip()
    if not text:
        return ""
    return str(PurePosixPath(text))


def classify_path(path: str) -> str | None:
    """Return the authority unit kind associated with a source path, or None."""

    normalized = _normalize_path(path)
    if not normalized:
        return None
    for prefix, unit_kind in AUTHORITY_BEARING_PATH_PATTERNS:
        if normalized.startswith(prefix) or normalized == prefix.rstrip("/"):
            return unit_kind
    return None


def is_authority_bearing(intended_files: Iterable[str]) -> bool:
    """True if any intended file lands in an authority-bearing path."""

    for path in intended_files or ():
        if classify_path(path):
            return True
    return False


_RE_REGISTER_ATOMIC = re.compile(
    r"register_operation_atomic\s*\(([^)]*)\)",
    re.IGNORECASE | re.DOTALL,
)
_RE_REGISTER_ATOMIC_NAME = re.compile(
    r"p_operation_name\s*:?=\s*'([^']+)'",
    re.IGNORECASE,
)
_RE_REGISTER_ATOMIC_REF = re.compile(
    r"p_operation_ref\s*:?=\s*'([^']+)'",
    re.IGNORECASE,
)
_RE_OP_CATALOG_INSERT = re.compile(
    r"INSERT\s+INTO\s+operation_catalog_registry\b",
    re.IGNORECASE,
)
_RE_OP_CATALOG_VALUES_REF = re.compile(
    r"VALUES\s*\(\s*'([^']+)'\s*,\s*'([^']+)'",
    re.IGNORECASE,
)
_RE_DD_INSERT = re.compile(
    r"INSERT\s+INTO\s+data_dictionary_objects\b",
    re.IGNORECASE,
)
_RE_AUTH_OBJ_INSERT = re.compile(
    r"INSERT\s+INTO\s+authority_object_registry\b",
    re.IGNORECASE,
)
_RE_EVENT_CONTRACT_INSERT = re.compile(
    r"INSERT\s+INTO\s+authority_event_contracts\b",
    re.IGNORECASE,
)
_RE_EVENT_CONTRACT_VALUES = re.compile(
    r"VALUES\s*\(\s*'([^']+)'\s*,\s*'([^']+)'",
    re.IGNORECASE,
)
_RE_CREATE_TABLE = re.compile(
    r"CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-z_][a-z0-9_]*)",
    re.IGNORECASE,
)
_RE_DROP_TABLE = re.compile(
    r"DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?([a-z_][a-z0-9_]*)",
    re.IGNORECASE,
)
_RE_CREATE_VIEW = re.compile(
    r"CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+([a-z_][a-z0-9_]*)",
    re.IGNORECASE,
)
_RE_CREATE_TYPE = re.compile(
    r"CREATE\s+TYPE\s+([a-z_][a-z0-9_]*)",
    re.IGNORECASE,
)
_RE_DROP_OPERATION = re.compile(
    r"DELETE\s+FROM\s+operation_catalog_registry\b[^;]*operation_(?:ref|name)\s*=\s*'([^']+)'",
    re.IGNORECASE | re.DOTALL,
)


def _migration_filename(path: str) -> str:
    return _normalize_path(path).rsplit("/", 1)[-1]


def _discover_from_migration(path: str, sql_text: str) -> list[DiscoveredImpact]:
    impacts: list[DiscoveredImpact] = []
    filename = _migration_filename(path)
    if not filename:
        return impacts

    impacts.append(
        DiscoveredImpact(
            unit_kind="migration_ref",
            unit_ref=filename,
            dispatch_effect="register",
            intent_hint="extend",
            discovery_evidence={"reason": "migration_file_added", "path": path},
        )
    )

    for match in _RE_REGISTER_ATOMIC.finditer(sql_text or ""):
        body = match.group(1)
        name = _RE_REGISTER_ATOMIC_NAME.search(body)
        ref = _RE_REGISTER_ATOMIC_REF.search(body)
        if name:
            impacts.append(
                DiscoveredImpact(
                    unit_kind="operation_ref",
                    unit_ref=name.group(1),
                    dispatch_effect="register",
                    intent_hint="extend",
                    discovery_evidence={
                        "reason": "register_operation_atomic",
                        "migration": filename,
                        "operation_ref": ref.group(1) if ref else None,
                    },
                )
            )

    if _RE_OP_CATALOG_INSERT.search(sql_text or ""):
        body_after = sql_text.split("operation_catalog_registry", 1)[-1] if sql_text else ""
        for match in _RE_OP_CATALOG_VALUES_REF.finditer(body_after):
            impacts.append(
                DiscoveredImpact(
                    unit_kind="operation_ref",
                    unit_ref=match.group(2),
                    dispatch_effect="register",
                    intent_hint="extend",
                    discovery_evidence={
                        "reason": "operation_catalog_registry_insert",
                        "migration": filename,
                        "operation_ref_in_values": match.group(1),
                    },
                )
            )

    for match in _RE_EVENT_CONTRACT_INSERT.finditer(sql_text or ""):
        body_after = sql_text.split("authority_event_contracts", 1)[-1] if sql_text else ""
        for value_match in _RE_EVENT_CONTRACT_VALUES.finditer(body_after):
            impacts.append(
                DiscoveredImpact(
                    unit_kind="event_type",
                    unit_ref=value_match.group(2),
                    dispatch_effect="register",
                    intent_hint="extend",
                    discovery_evidence={
                        "reason": "authority_event_contracts_insert",
                        "migration": filename,
                        "event_contract_ref": value_match.group(1),
                    },
                )
            )

    for match in _RE_CREATE_TABLE.finditer(sql_text or ""):
        impacts.append(
            DiscoveredImpact(
                unit_kind="database_object",
                unit_ref=match.group(1),
                dispatch_effect="register",
                intent_hint="extend",
                discovery_evidence={"reason": "create_table", "migration": filename},
            )
        )

    for match in _RE_CREATE_VIEW.finditer(sql_text or ""):
        impacts.append(
            DiscoveredImpact(
                unit_kind="database_object",
                unit_ref=match.group(1),
                dispatch_effect="register",
                intent_hint="extend",
                discovery_evidence={"reason": "create_view", "migration": filename},
            )
        )

    for match in _RE_CREATE_TYPE.finditer(sql_text or ""):
        impacts.append(
            DiscoveredImpact(
                unit_kind="database_object",
                unit_ref=match.group(1),
                dispatch_effect="register",
                intent_hint="extend",
                discovery_evidence={"reason": "create_type", "migration": filename},
            )
        )

    for match in _RE_DROP_TABLE.finditer(sql_text or ""):
        impacts.append(
            DiscoveredImpact(
                unit_kind="database_object",
                unit_ref=match.group(1),
                dispatch_effect="retire",
                intent_hint="retire",
                discovery_evidence={"reason": "drop_table", "migration": filename},
                predecessor_unit_kind="database_object",
                predecessor_unit_ref=match.group(1),
            )
        )

    for match in _RE_DROP_OPERATION.finditer(sql_text or ""):
        impacts.append(
            DiscoveredImpact(
                unit_kind="operation_ref",
                unit_ref=match.group(1),
                dispatch_effect="retire",
                intent_hint="retire",
                discovery_evidence={"reason": "delete_operation_catalog_row", "migration": filename},
                predecessor_unit_kind="operation_ref",
                predecessor_unit_ref=match.group(1),
            )
        )

    return impacts


_RE_PY_COMMAND_CLASS = re.compile(
    r"^class\s+([A-Z][A-Za-z0-9_]*)\s*\(\s*BaseModel\s*\)",
    re.MULTILINE,
)
_RE_PY_HANDLER_FN = re.compile(
    r"^def\s+(handle_[a-z_][a-z0-9_]*)\s*\(",
    re.MULTILINE,
)


def _discover_from_python_handler(path: str, py_text: str) -> list[DiscoveredImpact]:
    impacts: list[DiscoveredImpact] = []
    for match in _RE_PY_COMMAND_CLASS.finditer(py_text or ""):
        impacts.append(
            DiscoveredImpact(
                unit_kind="handler_ref",
                unit_ref=f"{path}::{match.group(1)}",
                dispatch_effect="register",
                intent_hint="extend",
                discovery_evidence={"reason": "command_or_query_class", "path": path},
            )
        )
    for match in _RE_PY_HANDLER_FN.finditer(py_text or ""):
        impacts.append(
            DiscoveredImpact(
                unit_kind="handler_ref",
                unit_ref=f"{path}::{match.group(1)}",
                dispatch_effect="register",
                intent_hint="extend",
                discovery_evidence={"reason": "handler_function", "path": path},
            )
        )
    return impacts


def discover_authority_overlap(
    *,
    intended_files: Sequence[str],
    file_contents: Mapping[str, str],
) -> list[DiscoveredImpact]:
    """Walk intended files and produce runtime-derived impact rows.

    `file_contents` maps repo-relative path -> post-patch content. The caller
    (preflight) passes the file content as it appears in the temp worktree
    AFTER the patch has been applied.
    """

    impacts: list[DiscoveredImpact] = []
    seen: set[tuple[str, str, str]] = set()

    def _add(impact: DiscoveredImpact) -> None:
        key = (impact.unit_kind, impact.unit_ref, impact.dispatch_effect)
        if key in seen:
            return
        seen.add(key)
        impacts.append(impact)

    for path in intended_files or ():
        normalized = _normalize_path(path)
        if not normalized:
            continue

        unit_kind = classify_path(normalized)
        if unit_kind is None:
            continue

        body = file_contents.get(normalized) or file_contents.get(path) or ""

        if unit_kind == "migration_ref":
            for impact in _discover_from_migration(normalized, body):
                _add(impact)
            continue

        if unit_kind == "handler_ref" and body:
            for impact in _discover_from_python_handler(normalized, body):
                _add(impact)
            continue

        _add(
            DiscoveredImpact(
                unit_kind=unit_kind,
                unit_ref=normalized,
                dispatch_effect="register",
                intent_hint="extend",
                discovery_evidence={
                    "reason": "authority_bearing_path_match",
                    "path": normalized,
                },
            )
        )

    return impacts


__all__ = [
    "AUTHORITY_BEARING_PATH_PATTERNS",
    "DiscoveredImpact",
    "classify_path",
    "is_authority_bearing",
    "discover_authority_overlap",
]
