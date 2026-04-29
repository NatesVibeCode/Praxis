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


@dataclass(frozen=True)
class WorkflowMigrationRenumberAction:
    old_filename: str
    new_filename: str
    reason: str

    def to_dict(self) -> dict[str, str]:
        return {
            "old_filename": self.old_filename,
            "new_filename": self.new_filename,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class WorkflowMigrationAllocation:
    requested_slug: str
    normalized_slug: str
    next_prefix: int
    proposed_filename: str
    renumber_actions: tuple[WorkflowMigrationRenumberAction, ...]
    operator_messages: tuple[str, ...]

    @property
    def renumber_applied(self) -> bool:
        return bool(self.renumber_actions)

    def to_dict(self) -> dict[str, Any]:
        return {
            "requested_slug": self.requested_slug,
            "normalized_slug": self.normalized_slug,
            "next_prefix": self.next_prefix,
            "proposed_filename": self.proposed_filename,
            "renumber_applied": self.renumber_applied,
            "renumber_actions": [action.to_dict() for action in self.renumber_actions],
            "operator_messages": list(self.operator_messages),
        }


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
    return allocate_workflow_migration_filename(
        slug=slug,
        workflow_root=workflow_root,
    ).proposed_filename


def allocate_workflow_migration_filename(
    *,
    slug: str,
    workflow_root: Path | None = None,
) -> WorkflowMigrationAllocation:
    """Allocate the next migration filename after mandatory duplicate repair."""

    normalized_slug = normalize_workflow_migration_slug(slug)
    renumber_actions = renumber_unmanaged_duplicate_prefixes(workflow_root, apply=True)
    state = workflow_migration_sequence_state(workflow_root)
    proposed_filename = f"{state.next_prefix:03d}_{normalized_slug}.sql"
    operator_messages: tuple[str, ...] = ()
    if renumber_actions:
        moved = ", ".join(
            f"{action.old_filename} -> {action.new_filename}"
            for action in renumber_actions
        )
        operator_messages = (
            "Automatically renumbered unmanaged duplicate migration prefixes before "
            f"allocating the next migration: {moved}.",
        )
    return WorkflowMigrationAllocation(
        requested_slug=slug,
        normalized_slug=normalized_slug,
        next_prefix=state.next_prefix,
        proposed_filename=proposed_filename,
        renumber_actions=tuple(renumber_actions),
        operator_messages=operator_messages,
    )


def raise_for_unmanaged_duplicate_prefixes(workflow_root: Path | None = None) -> None:
    """Legacy guard kept for callers that still use the old name.

    The policy is repair-first, not fail-closed: unmanaged duplicate migration
    prefixes are automatically moved to fresh prefixes. A hard failure only
    means the repair itself could not be applied safely.
    """

    state = workflow_migration_sequence_state(workflow_root)
    if not state.unmanaged_duplicate_prefixes:
        return
    renumber_unmanaged_duplicate_prefixes(workflow_root, apply=True)


def _filename_policy_rank(spec: dict[str, Any], filename: str) -> tuple[int, int, str]:
    manifest = [str(item) for item in (spec.get("canonical_manifest") or ())]
    if filename in manifest:
        return (0, manifest.index(filename), filename)
    policy_buckets = spec.get("policy_buckets") if isinstance(spec.get("policy_buckets"), dict) else {}
    order = ("canonical", "bootstrap_only", "deprecated", "dead")
    for bucket_index, bucket in enumerate(order, start=1):
        filenames = [str(item) for item in (policy_buckets.get(bucket) or ())]
        if filename in filenames:
            return (bucket_index, filenames.index(filename), filename)
    return (len(order) + 1, 10**9, filename)


def _replace_filename(value: Any, old_filename: str, new_filename: str) -> tuple[Any, bool]:
    if isinstance(value, str):
        return (new_filename, True) if value == old_filename else (value, False)
    if isinstance(value, list):
        changed = False
        replaced: list[Any] = []
        for item in value:
            next_item, item_changed = _replace_filename(item, old_filename, new_filename)
            changed = changed or item_changed
            replaced.append(next_item)
        return replaced, changed
    if isinstance(value, dict):
        changed = False
        replaced: dict[str, Any] = {}
        for key, item in value.items():
            next_key = new_filename if key == old_filename else key
            next_item, item_changed = _replace_filename(item, old_filename, new_filename)
            changed = changed or item_changed or next_key != key
            replaced[next_key] = next_item
        return replaced, changed
    return value, False


def renumber_unmanaged_duplicate_prefixes(
    workflow_root: Path | None = None,
    *,
    apply: bool = False,
) -> tuple[WorkflowMigrationRenumberAction, ...]:
    """Move accidental duplicate-prefix migrations onto fresh numeric prefixes.

    Managed duplicate prefixes are legacy apply-order exceptions declared in
    ``tie_break_order``. Unmanaged duplicates are almost always an authoring
    mistake, so the repair keeps the already-authoritative filename at its
    current prefix and moves the rest to the next free prefix.
    """

    root = workflow_root_from_path(workflow_root)
    state = workflow_migration_sequence_state(root)
    if not state.unmanaged_duplicate_prefixes:
        return ()

    spec = load_workflow_migration_authority_spec(root)
    used_prefixes = {
        match.group("prefix")
        for filename in state.filenames
        if (match := _NUMBERED_SQL_PATTERN.match(filename)) is not None
    }
    next_prefix = state.highest_prefix + 1
    actions: list[WorkflowMigrationRenumberAction] = []

    for prefix, filenames in sorted(state.unmanaged_duplicate_prefixes.items()):
        keep = min(filenames, key=lambda filename: _filename_policy_rank(spec, filename))
        for filename in sorted(item for item in filenames if item != keep):
            match = _NUMBERED_SQL_PATTERN.match(filename)
            if match is None:
                continue
            while f"{next_prefix:03d}" in used_prefixes:
                next_prefix += 1
            new_filename = f"{next_prefix:03d}_{match.group('slug')}.sql"
            used_prefixes.add(f"{next_prefix:03d}")
            next_prefix += 1
            actions.append(
                WorkflowMigrationRenumberAction(
                    old_filename=filename,
                    new_filename=new_filename,
                    reason=f"unmanaged duplicate prefix {prefix}; kept {keep}",
                )
            )

    if not apply:
        return tuple(actions)

    migration_root = workflow_migration_root(root)
    updated_spec: Any = spec
    spec_changed = False
    for action in actions:
        old_path = migration_root / action.old_filename
        new_path = migration_root / action.new_filename
        if not old_path.exists():
            raise FileNotFoundError(f"migration file not found: {old_path}")
        if new_path.exists():
            raise FileExistsError(f"target migration file already exists: {new_path}")
        old_path.rename(new_path)
        updated_spec, changed = _replace_filename(
            updated_spec,
            action.old_filename,
            action.new_filename,
        )
        spec_changed = spec_changed or changed

    if spec_changed:
        workflow_migration_authority_spec_path(root).write_text(
            json.dumps(updated_spec, indent=2, sort_keys=False) + "\n",
            encoding="utf-8",
        )

    post_state = workflow_migration_sequence_state(root)
    if post_state.unmanaged_duplicate_prefixes:
        raise RuntimeError(
            "migration renumbering left unmanaged duplicate prefixes: "
            f"{post_state.unmanaged_duplicate_prefixes}"
        )
    return tuple(actions)


__all__ = [
    "WorkflowMigrationAllocation",
    "WorkflowMigrationRenumberAction",
    "WorkflowMigrationSequenceState",
    "allocate_workflow_migration_filename",
    "load_workflow_migration_authority_spec",
    "normalize_workflow_migration_slug",
    "numbered_workflow_migration_filenames",
    "propose_workflow_migration_filename",
    "raise_for_unmanaged_duplicate_prefixes",
    "renumber_unmanaged_duplicate_prefixes",
    "workflow_migration_authority_spec_path",
    "workflow_migration_root",
    "workflow_migration_sequence_state",
    "workflow_root_from_path",
]
