from __future__ import annotations

import json
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any


_NUMBERED_SQL_PATTERN = re.compile(r"^(?P<prefix>\d{3,})_(?P<slug>[a-z0-9_]+)\.sql$")
_SLUG_PATTERN = re.compile(r"[^a-z0-9]+")


@dataclass(frozen=True)
class WorkflowMigrationSequenceState:
    workflow_root: Path
    migration_root: Path
    filenames: tuple[str, ...]
    tie_break_order: dict[str, tuple[str, ...]]
    duplicate_prefixes: dict[str, tuple[str, ...]]
    managed_duplicate_prefixes: dict[str, tuple[str, ...]]
    unmanaged_duplicate_prefixes: dict[str, tuple[str, ...]]
    highest_prefix: int
    next_prefix: int


def workflow_root_from_path(path: Path | None = None) -> Path:
    if path is not None:
        return Path(path).resolve()
    return Path(__file__).resolve().parents[1]


def workflow_migration_root(workflow_root: Path | None = None) -> Path:
    root = workflow_root_from_path(workflow_root)
    return root.parent / "Databases" / "migrations" / "workflow"


def workflow_migration_authority_spec_path(workflow_root: Path | None = None) -> Path:
    root = workflow_root_from_path(workflow_root)
    return root / "system_authority" / "workflow_migration_authority.json"


def load_workflow_migration_authority_spec(workflow_root: Path | None = None) -> dict[str, Any]:
    return json.loads(
        workflow_migration_authority_spec_path(workflow_root).read_text(encoding="utf-8")
    )


def numbered_workflow_migration_filenames(workflow_root: Path | None = None) -> tuple[str, ...]:
    root = workflow_migration_root(workflow_root)
    return tuple(sorted(path.name for path in root.glob("[0-9][0-9][0-9]_*.sql")))


def normalize_workflow_migration_slug(slug: str) -> str:
    normalized = _SLUG_PATTERN.sub("_", str(slug or "").strip().lower()).strip("_")
    if not normalized:
        raise ValueError("migration slug must contain at least one alphanumeric character")
    return normalized


def workflow_migration_sequence_state(
    workflow_root: Path | None = None,
) -> WorkflowMigrationSequenceState:
    root = workflow_root_from_path(workflow_root)
    filenames = numbered_workflow_migration_filenames(root)
    spec = load_workflow_migration_authority_spec(root)
    tie_break_order = {
        str(prefix): tuple(str(name) for name in names)
        for prefix, names in (spec.get("tie_break_order") or {}).items()
    }

    groups: dict[str, list[str]] = defaultdict(list)
    highest_prefix = 0
    for filename in filenames:
        match = _NUMBERED_SQL_PATTERN.match(filename)
        if match is None:
            continue
        prefix = match.group("prefix")
        highest_prefix = max(highest_prefix, int(prefix))
        groups[prefix].append(filename)

    duplicate_prefixes = {
        prefix: tuple(sorted(members))
        for prefix, members in sorted(groups.items())
        if len(members) >= 2
    }
    managed_duplicate_prefixes = {
        prefix: filenames
        for prefix, filenames in duplicate_prefixes.items()
        if prefix in tie_break_order
    }
    unmanaged_duplicate_prefixes = {
        prefix: filenames
        for prefix, filenames in duplicate_prefixes.items()
        if prefix not in tie_break_order
    }

    return WorkflowMigrationSequenceState(
        workflow_root=root,
        migration_root=workflow_migration_root(root),
        filenames=filenames,
        tie_break_order=tie_break_order,
        duplicate_prefixes=duplicate_prefixes,
        managed_duplicate_prefixes=managed_duplicate_prefixes,
        unmanaged_duplicate_prefixes=unmanaged_duplicate_prefixes,
        highest_prefix=highest_prefix,
        next_prefix=highest_prefix + 1,
    )


def propose_workflow_migration_filename(
    *,
    slug: str,
    workflow_root: Path | None = None,
) -> str:
    state = workflow_migration_sequence_state(workflow_root)
    normalized_slug = normalize_workflow_migration_slug(slug)
    return f"{state.next_prefix:03d}_{normalized_slug}.sql"


def raise_for_unmanaged_duplicate_prefixes(workflow_root: Path | None = None) -> None:
    state = workflow_migration_sequence_state(workflow_root)
    if not state.unmanaged_duplicate_prefixes:
        return
    lines = [
        "workflow migrations have unmanaged duplicate numeric prefixes:",
    ]
    for prefix, filenames in sorted(state.unmanaged_duplicate_prefixes.items()):
        rendered = ", ".join(filenames)
        lines.append(f"  - {prefix}: {rendered}")
    lines.append(
        "Allocate a new file with `workflow schema next-migration <slug>` instead of "
        "reusing an existing prefix."
    )
    raise ValueError("\n".join(lines))


__all__ = [
    "WorkflowMigrationSequenceState",
    "load_workflow_migration_authority_spec",
    "normalize_workflow_migration_slug",
    "numbered_workflow_migration_filenames",
    "propose_workflow_migration_filename",
    "raise_for_unmanaged_duplicate_prefixes",
    "workflow_migration_authority_spec_path",
    "workflow_migration_root",
    "workflow_migration_sequence_state",
    "workflow_root_from_path",
]
