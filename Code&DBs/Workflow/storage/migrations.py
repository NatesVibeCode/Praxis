"""Canonical workflow migration path helpers.

This module is intentionally tiny so other packages can resolve migration files
without importing the heavier Postgres storage surface and creating circular
dependencies.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

from ._generated_workflow_migration_authority import (
    WORKFLOW_FULL_BOOTSTRAP_SEQUENCE as _GENERATED_WORKFLOW_FULL_BOOTSTRAP_SEQUENCE,
    WORKFLOW_MIGRATION_EXPECTED_OBJECTS as _GENERATED_WORKFLOW_MIGRATION_EXPECTED_OBJECTS,
    WORKFLOW_MIGRATION_POLICIES as _GENERATED_WORKFLOW_MIGRATION_POLICIES,
    WORKFLOW_MIGRATION_SEQUENCE as _GENERATED_WORKFLOW_MIGRATION_SEQUENCE,
    WORKFLOW_POLICY_BUCKETS as _GENERATED_WORKFLOW_POLICY_BUCKETS,
)

_POSTGRES_IDENTIFIER_MAX_CHARS = 63


class WorkflowMigrationError(RuntimeError):
    """Raised when the canonical workflow migration tree is missing or invalid."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        path: Path | None = None,
        filename: str | None = None,
        details: Mapping[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.path = path
        self.filename = filename
        self._details = dict(details or {})

    @property
    def details(self) -> dict[str, str]:
        details = dict(self._details)
        if self.path is not None:
            details["path"] = str(self.path)
        if self.filename is not None:
            details["filename"] = self.filename
        return details


WorkflowMigrationPathError = WorkflowMigrationError

_WORKFLOW_MIGRATION_SEQUENCE = _GENERATED_WORKFLOW_MIGRATION_SEQUENCE


@dataclass(frozen=True, slots=True)
class WorkflowMigrationExpectedObject:
    """One canonical object that a workflow migration must materialize."""

    object_type: str
    object_name: str


@dataclass(frozen=True, slots=True)
class WorkflowMigrationManifestEntry:
    """One canonical workflow migration entry."""

    sequence_no: int
    filename: str
    path: Path


def _expected_objects(
    *,
    tables: tuple[str, ...] = (),
    indexes: tuple[str, ...] = (),
    columns: tuple[str, ...] = (),
    constraints: tuple[str, ...] = (),
    functions: tuple[str, ...] = (),
) -> tuple[WorkflowMigrationExpectedObject, ...]:
    return tuple(
        WorkflowMigrationExpectedObject(object_type="table", object_name=name)
        for name in tables
    ) + tuple(
        WorkflowMigrationExpectedObject(
            object_type="index",
            object_name=name[:_POSTGRES_IDENTIFIER_MAX_CHARS],
        )
        for name in indexes
    ) + tuple(
        WorkflowMigrationExpectedObject(object_type="column", object_name=name)
        for name in columns
    ) + tuple(
        WorkflowMigrationExpectedObject(object_type="constraint", object_name=name)
        for name in constraints
    ) + tuple(
        WorkflowMigrationExpectedObject(object_type="function", object_name=name)
        for name in functions
    )


_WORKFLOW_MIGRATION_EXPECTED_OBJECTS = {
    filename: tuple(
        WorkflowMigrationExpectedObject(object_type=object_type, object_name=object_name)
        for object_type, object_name in objects
    )
    for filename, objects in _GENERATED_WORKFLOW_MIGRATION_EXPECTED_OBJECTS.items()
}



def _workflow_migrations_root_path() -> Path:
    return Path(__file__).resolve().parents[2] / "Databases" / "migrations" / "workflow"


def _numbered_workflow_migration_filenames(root: Path) -> tuple[str, ...]:
    return tuple(sorted(path.name for path in root.glob("[0-9][0-9][0-9]_*.sql")))


def _workflow_migration_policy(filename: str) -> str | None:
    return _GENERATED_WORKFLOW_MIGRATION_POLICIES.get(filename)


def _resolve_workflow_migration_path(
    filename: str,
    *,
    allowed_policies: tuple[str, ...],
    unknown_reason_code: str,
    forbidden_reason_code: str,
    unknown_message: str,
    forbidden_message: str,
) -> Path:
    root = workflow_migrations_root()
    policy = _workflow_migration_policy(filename)
    if policy is None:
        candidate = root / filename
        raise WorkflowMigrationPathError(
            unknown_reason_code,
            unknown_message,
            path=candidate,
            filename=filename,
            details={
                "allowed_policies": ",".join(allowed_policies),
                "known_policies": ",".join(
                    f"{name}:{migration_policy}"
                    for name, migration_policy in sorted(
                        _GENERATED_WORKFLOW_MIGRATION_POLICIES.items()
                    )
                ),
            },
        )
    if policy not in allowed_policies:
        candidate = root / filename
        raise WorkflowMigrationPathError(
            forbidden_reason_code,
            forbidden_message,
            path=candidate,
            filename=filename,
            details={
                "migration_policy": policy,
                "allowed_policies": ",".join(allowed_policies),
            },
        )
    return root / filename


def _validate_generated_workflow_migration_policy(root: Path) -> None:
    actual_numbered_filenames = _numbered_workflow_migration_filenames(root)
    classified_filenames = tuple(sorted(_GENERATED_WORKFLOW_MIGRATION_POLICIES))
    if actual_numbered_filenames != classified_filenames:
        actual_set = set(actual_numbered_filenames)
        classified_set = set(classified_filenames)
        raise WorkflowMigrationPathError(
            "workflow.migration_policy_drift",
            "generated workflow migration policy drifted from on-disk numbered SQL files",
            path=root,
            details={
                "unclassified_filenames": ",".join(sorted(actual_set - classified_set)),
                "missing_on_disk_filenames": ",".join(sorted(classified_set - actual_set)),
                "actual_numbered_filenames": ",".join(actual_numbered_filenames),
                "classified_filenames": ",".join(classified_filenames),
            },
        )

    bucketed_filenames = tuple(
        filename
        for filenames in _GENERATED_WORKFLOW_POLICY_BUCKETS.values()
        for filename in filenames
    )
    if len(bucketed_filenames) != len(set(bucketed_filenames)):
        duplicates: list[str] = []
        seen: set[str] = set()
        for filename in bucketed_filenames:
            if filename in seen and filename not in duplicates:
                duplicates.append(filename)
            seen.add(filename)
        raise WorkflowMigrationPathError(
            "workflow.migration_policy_invalid",
            "generated workflow migration policy buckets overlap",
            path=root,
            details={
                "duplicate_filenames": ",".join(sorted(duplicates)),
            },
        )

    expected_full_bootstrap = tuple(
        sorted(
            set(_GENERATED_WORKFLOW_POLICY_BUCKETS.get("canonical", ()))
            | set(_GENERATED_WORKFLOW_POLICY_BUCKETS.get("bootstrap_only", ()))
        )
    )
    if expected_full_bootstrap != _GENERATED_WORKFLOW_FULL_BOOTSTRAP_SEQUENCE:
        raise WorkflowMigrationPathError(
            "workflow.migration_policy_invalid",
            "generated workflow full bootstrap sequence does not match policy-derived bootstrap order",
            path=root,
            details={
                "expected_full_bootstrap": ",".join(expected_full_bootstrap),
                "generated_full_bootstrap": ",".join(
                    _GENERATED_WORKFLOW_FULL_BOOTSTRAP_SEQUENCE
                ),
            },
        )


@lru_cache(maxsize=1)
def workflow_migrations_root() -> Path:
    """Return the one canonical workflow migration root."""

    root = _workflow_migrations_root_path()
    if not root.is_dir():
        raise WorkflowMigrationPathError(
            "workflow.migration_root_missing",
            "canonical workflow migration root is missing",
            path=root,
        )
    _validate_generated_workflow_migration_policy(root)
    return root


@lru_cache(maxsize=1)
def workflow_migration_manifest() -> tuple[WorkflowMigrationManifestEntry, ...]:
    """Return the exact canonical migration sequence and fail closed on drift."""

    root = workflow_migrations_root()
    actual_filenames = {path.name for path in root.glob("*.sql")}
    expected_filenames = set(_WORKFLOW_MIGRATION_SEQUENCE)

    missing_filenames = tuple(
        filename for filename in _WORKFLOW_MIGRATION_SEQUENCE if filename not in actual_filenames
    )
    if missing_filenames:
        raise WorkflowMigrationPathError(
            "workflow.migration_manifest_incomplete",
            "canonical workflow migration manifest is incomplete",
            path=root,
            filename=missing_filenames[0],
            details={
                "missing_filenames": ",".join(missing_filenames),
                "expected_filenames": ",".join(_WORKFLOW_MIGRATION_SEQUENCE),
            },
        )

    return tuple(
        WorkflowMigrationManifestEntry(
            sequence_no=index,
            filename=filename,
            path=root / filename,
        )
        for index, filename in enumerate(_WORKFLOW_MIGRATION_SEQUENCE, start=1)
    )


def workflow_migration_path(filename: str) -> Path:
    """Resolve one canonical workflow migration file and fail closed if missing."""

    return _resolve_workflow_migration_path(
        filename,
        allowed_policies=("canonical",),
        unknown_reason_code="workflow.migration_unknown",
        forbidden_reason_code="workflow.migration_policy_forbidden",
        unknown_message="workflow migration filename is not in the canonical manifest",
        forbidden_message="workflow migration filename is outside the canonical policy boundary",
    )


def workflow_bootstrap_migration_path(filename: str) -> Path:
    """Resolve one workflow migration file allowed in full bootstrap order."""

    return _resolve_workflow_migration_path(
        filename,
        allowed_policies=("canonical", "bootstrap_only"),
        unknown_reason_code="workflow.bootstrap_migration_unknown",
        forbidden_reason_code="workflow.bootstrap_migration_policy_forbidden",
        unknown_message="workflow bootstrap migration filename is not classified by policy",
        forbidden_message="workflow bootstrap migration filename is outside bootstrap policy",
    )


def _read_workflow_migration_text(path: Path, *, filename: str, reason_prefix: str) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        workflow_migration_manifest.cache_clear()
        try:
            workflow_migrations_root.cache_clear()
            workflow_migrations_root()
        except WorkflowMigrationPathError as manifest_exc:
            raise manifest_exc from exc
        raise WorkflowMigrationPathError(
            f"{reason_prefix}.read_failed",
            "workflow migration file could not be read",
            path=path,
            filename=filename,
        ) from exc
    except OSError as exc:  # pragma: no cover - defensive failure path
        raise WorkflowMigrationPathError(
            f"{reason_prefix}.read_failed",
            "workflow migration file could not be read",
            path=path,
            filename=filename,
        ) from exc


@lru_cache(maxsize=64)
def workflow_migration_sql_text(filename: str) -> str:
    """Load one canonical workflow migration file."""

    path = workflow_migration_path(filename)
    return _read_workflow_migration_text(
        path,
        filename=filename,
        reason_prefix="workflow.migration",
    )


@lru_cache(maxsize=64)
def workflow_bootstrap_migration_sql_text(filename: str) -> str:
    """Load one workflow migration file allowed in full bootstrap order."""

    path = workflow_bootstrap_migration_path(filename)
    return _read_workflow_migration_text(
        path,
        filename=filename,
        reason_prefix="workflow.bootstrap_migration",
    )


def _workflow_migration_statements_from_text(
    sql_text: str,
    *,
    path: Path,
    filename: str,
    reason_code: str,
    message: str,
) -> tuple[str, ...]:
    statements = _split_sql_statements(sql_text)
    if not statements:
        raise WorkflowMigrationPathError(
            reason_code,
            message,
            path=path,
            filename=filename,
        )
    return statements


@lru_cache(maxsize=64)
def workflow_migration_statements(filename: str) -> tuple[str, ...]:
    """Load canonical workflow migration statements using one shared parser."""

    path = workflow_migration_path(filename)
    return _workflow_migration_statements_from_text(
        workflow_migration_sql_text(filename),
        path=path,
        filename=filename,
        reason_code="workflow.migration_empty",
        message="canonical workflow migration file did not contain executable statements",
    )


@lru_cache(maxsize=64)
def workflow_bootstrap_migration_statements(filename: str) -> tuple[str, ...]:
    """Load bootstrap-eligible workflow migration statements using one shared parser."""

    path = workflow_bootstrap_migration_path(filename)
    return _workflow_migration_statements_from_text(
        workflow_bootstrap_migration_sql_text(filename),
        path=path,
        filename=filename,
        reason_code="workflow.bootstrap_migration_empty",
        message="workflow bootstrap migration file did not contain executable statements",
    )


def _split_sql_statements(sql_text: str) -> tuple[str, ...]:
    """Split SQL text on statement terminators outside quoted bodies.

    The canonical workflow migrations include PL/pgSQL functions, so a plain
    ``split(';')`` corrupts function bodies and makes schema bootstrap lie
    about migration failures. Keep this parser local and boring: it only needs
    to respect line comments, block comments, quoted identifiers/strings, and
    dollar-quoted bodies.
    """

    statements: list[str] = []
    current: list[str] = []
    index = 0
    in_single_quote = False
    in_double_quote = False
    line_comment = False
    block_comment_depth = 0
    dollar_tag: str | None = None

    while index < len(sql_text):
        char = sql_text[index]
        next_char = sql_text[index + 1] if index + 1 < len(sql_text) else ""

        if line_comment:
            current.append(char)
            index += 1
            if char == "\n":
                line_comment = False
            continue

        if block_comment_depth:
            current.append(char)
            if char == "/" and next_char == "*":
                current.append(next_char)
                block_comment_depth += 1
                index += 2
                continue
            if char == "*" and next_char == "/":
                current.append(next_char)
                block_comment_depth -= 1
                index += 2
                continue
            index += 1
            continue

        if dollar_tag is not None:
            if sql_text.startswith(dollar_tag, index):
                current.append(dollar_tag)
                index += len(dollar_tag)
                dollar_tag = None
                continue
            current.append(char)
            index += 1
            continue

        if in_single_quote:
            current.append(char)
            index += 1
            if char == "'" and next_char == "'":
                current.append(next_char)
                index += 1
                continue
            if char == "'":
                in_single_quote = False
            continue

        if in_double_quote:
            current.append(char)
            index += 1
            if char == '"' and next_char == '"':
                current.append(next_char)
                index += 1
                continue
            if char == '"':
                in_double_quote = False
            continue

        if char == "-" and next_char == "-":
            current.extend((char, next_char))
            line_comment = True
            index += 2
            continue

        if char == "/" and next_char == "*":
            current.extend((char, next_char))
            block_comment_depth = 1
            index += 2
            continue

        if char == "'":
            current.append(char)
            in_single_quote = True
            index += 1
            continue

        if char == '"':
            current.append(char)
            in_double_quote = True
            index += 1
            continue

        if char == "$":
            tag_end = index + 1
            while tag_end < len(sql_text) and (
                sql_text[tag_end].isalnum() or sql_text[tag_end] == "_"
            ):
                tag_end += 1
            if tag_end < len(sql_text) and sql_text[tag_end] == "$":
                dollar_tag = sql_text[index : tag_end + 1]
                current.append(dollar_tag)
                index = tag_end + 1
                continue

        if char == ";":
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            index += 1
            continue

        current.append(char)
        index += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return tuple(statements)


@lru_cache(maxsize=64)
def workflow_migration_expected_objects(
    filename: str,
) -> tuple[WorkflowMigrationExpectedObject, ...]:
    """Return the explicit expected-object contract for one canonical migration."""

    path = workflow_migration_path(filename)
    objects = _WORKFLOW_MIGRATION_EXPECTED_OBJECTS.get(filename)
    if objects is None:
        raise WorkflowMigrationPathError(
            "workflow.migration_expected_objects_missing",
            "canonical workflow migration is missing an expected-object contract",
            path=path,
            filename=filename,
        )
    if not objects:
        raise WorkflowMigrationPathError(
            "workflow.migration_expected_objects_empty",
            "canonical workflow migration expected-object contract is empty",
            path=path,
            filename=filename,
        )
    return objects


def clear_workflow_migration_caches() -> None:
    """Reset cached canonical migration lookups for tests and patched call sites."""

    workflow_migrations_root.cache_clear()
    workflow_migration_manifest.cache_clear()
    workflow_migration_sql_text.cache_clear()
    workflow_migration_statements.cache_clear()
    workflow_migration_expected_objects.cache_clear()


__all__ = [
    "clear_workflow_migration_caches",
    "WorkflowMigrationError",
    "WorkflowMigrationExpectedObject",
    "WorkflowMigrationManifestEntry",
    "WorkflowMigrationPathError",
    "workflow_migration_expected_objects",
    "workflow_migration_manifest",
    "workflow_migration_path",
    "workflow_migration_sql_text",
    "workflow_migration_statements",
    "workflow_migrations_root",
]
