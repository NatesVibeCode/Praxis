from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from pprint import pformat
import sys

_WORKFLOW_ROOT = Path(__file__).resolve().parents[1]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from system_authority.workflow_migration_sequence_manager import (
    load_workflow_migration_authority_spec,
    renumber_unmanaged_duplicate_prefixes,
)

_POLICY_ORDER = ("canonical", "bootstrap_only", "deprecated", "dead")


def _validate_tie_break_coverage(
    *,
    full_bootstrap_filenames: set[str],
    tie_break_order: dict[str, tuple[str, ...]],
) -> None:
    """Fail if duplicate-prefix groups are missing from tie_break_order.

    Workflow migrations share a 3-digit prefix (legacy artifact of parallel
    branches). Apply order for a duplicate group must be explicit — an authored
    ``tie_break_order[prefix]`` list in the authority JSON — because
    ``sorted(canonical | bootstrap_only)`` alphabetises filenames with no
    documented reason. This validator is what forces a new duplicate prefix to
    declare its order.
    """

    groups: dict[str, list[str]] = defaultdict(list)
    for filename in full_bootstrap_filenames:
        prefix = filename[:3]
        if prefix.isdigit():
            groups[prefix].append(filename)

    missing_groups: list[str] = []
    mismatched_groups: list[str] = []
    for prefix, filenames in sorted(groups.items()):
        if len(filenames) < 2:
            continue
        declared = tie_break_order.get(prefix)
        if declared is None:
            missing_groups.append(
                f"prefix {prefix!r} has {len(filenames)} duplicate-prefix files but no "
                "tie_break_order entry: "
                + ", ".join(sorted(filenames))
            )
            continue
        declared_set = set(declared)
        on_disk_set = set(filenames)
        if declared_set != on_disk_set:
            mismatched_groups.append(
                f"prefix {prefix!r} tie_break_order mismatch: "
                f"declared={sorted(declared_set)} on_disk={sorted(on_disk_set)}"
            )
        if len(declared_set) != len(declared):
            mismatched_groups.append(
                f"prefix {prefix!r} tie_break_order has duplicate entries: {list(declared)}"
            )

    orphan_prefixes = [
        prefix
        for prefix in tie_break_order
        if len(groups.get(prefix, ())) < 2
    ]

    problems = missing_groups + mismatched_groups
    if orphan_prefixes:
        problems.append(
            "orphan tie_break_order entries for non-duplicate prefixes: "
            + ", ".join(sorted(orphan_prefixes))
        )
    if problems:
        raise SystemExit(
            "workflow migration authority tie_break_order drift:\n  - "
            + "\n  - ".join(problems)
        )


def _tie_break_aware_full_bootstrap(
    *,
    full_bootstrap: frozenset[str],
    tie_break_order: dict[str, tuple[str, ...]],
) -> tuple[str, ...]:
    """Return full-bootstrap filenames in explicit apply order.

    Primary sort is the 3-digit prefix. Within a prefix with ≥2 members, use
    the declared ``tie_break_order`` index. Singletons keep alphabetical order.
    """

    groups: dict[str, list[str]] = defaultdict(list)
    singletons: list[str] = []
    for filename in full_bootstrap:
        prefix = filename[:3]
        if prefix.isdigit():
            groups[prefix].append(filename)
        else:
            singletons.append(filename)

    ordered: list[str] = []
    for prefix in sorted(groups.keys()):
        members = groups[prefix]
        if len(members) == 1:
            ordered.extend(members)
            continue
        declared = tie_break_order[prefix]
        order_index = {name: i for i, name in enumerate(declared)}
        ordered.extend(sorted(members, key=lambda f: order_index[f]))
    # any non-numeric-prefix entries sort last in their own alphabetical block
    ordered.extend(sorted(singletons))
    return tuple(ordered)


def _validate_bootstrap_prerequisites(
    *,
    bootstrap_prerequisites: dict[str, tuple[str, ...]],
    full_bootstrap: tuple[str, ...],
) -> None:
    order_index = {filename: index for index, filename in enumerate(full_bootstrap)}
    problems: list[str] = []
    for filename, prerequisites in sorted(bootstrap_prerequisites.items()):
        if filename not in order_index:
            problems.append(
                f"{filename!r} declares bootstrap prerequisites but is not bootstrap-eligible"
            )
            continue
        if len(set(prerequisites)) != len(prerequisites):
            problems.append(f"{filename!r} has duplicate bootstrap prerequisites")
        for prerequisite in prerequisites:
            if prerequisite not in order_index:
                problems.append(
                    f"{filename!r} prerequisite {prerequisite!r} is not bootstrap-eligible"
                )
                continue
            if order_index[prerequisite] >= order_index[filename]:
                problems.append(
                    f"{filename!r} prerequisite {prerequisite!r} must appear earlier "
                    "in full bootstrap order"
                )
    if problems:
        raise SystemExit(
            "workflow migration authority bootstrap_prerequisites drift:\n  - "
            + "\n  - ".join(problems)
        )


def main() -> None:
    workflow_root = _WORKFLOW_ROOT
    output_path = workflow_root / "storage" / "_generated_workflow_migration_authority.py"

    renumber_actions = renumber_unmanaged_duplicate_prefixes(workflow_root, apply=True)
    if renumber_actions:
        print(
            "[migration-authority] heads up — submitted migration(s) collided with an "
            "existing prefix and were bumped to the next free number. The file was "
            "renamed AND its 'Migration <num>' header references were rewritten in "
            "place. Nothing for you to do; this is the routine handler. Summary:",
            file=sys.stderr,
        )
        for action in renumber_actions:
            print(
                f"  • {action.old_filename} -> {action.new_filename}",
                file=sys.stderr,
            )
    spec = load_workflow_migration_authority_spec(workflow_root)
    policy_buckets = {
        policy: tuple(spec["policy_buckets"].get(policy, ())) for policy in _POLICY_ORDER
    }
    manifest = tuple(spec["canonical_manifest"])
    tie_break_order = {
        str(prefix): tuple(str(name) for name in filenames)
        for prefix, filenames in (spec.get("tie_break_order") or {}).items()
    }
    full_bootstrap_set = frozenset(
        set(policy_buckets["canonical"]) | set(policy_buckets["bootstrap_only"])
    )
    _validate_tie_break_coverage(
        full_bootstrap_filenames=full_bootstrap_set,
        tie_break_order=tie_break_order,
    )
    full_bootstrap = _tie_break_aware_full_bootstrap(
        full_bootstrap=full_bootstrap_set,
        tie_break_order=tie_break_order,
    )
    bootstrap_prerequisites = {
        str(filename): tuple(str(prerequisite) for prerequisite in prerequisites)
        for filename, prerequisites in (spec.get("bootstrap_prerequisites") or {}).items()
    }
    _validate_bootstrap_prerequisites(
        bootstrap_prerequisites=bootstrap_prerequisites,
        full_bootstrap=full_bootstrap,
    )
    expected_objects = {
        filename: tuple(
            (str(item["object_type"]), str(item["object_name"]))
            for item in objects
        )
        for filename, objects in spec["expected_objects"].items()
    }
    readiness_sequence = tuple(
        (filename, expected_objects[filename]) for filename in manifest
    )
    migration_policies = {
        filename: policy
        for policy, filenames in policy_buckets.items()
        for filename in filenames
    }
    # Migrations whose only schema-effect is `CREATE OR REPLACE FUNCTION`
    # (or trigger) on objects already created by an earlier migration.
    # These re-define behavior without adding new objects, so readiness
    # inspection cannot tell when the function body drifted. Startup bootstrap
    # intentionally ignores this list; explicit repair/migration operations may
    # use it when asked to refresh function or trigger bodies.
    migrations_always_reapply = tuple(spec.get("migrations_always_reapply") or ())

    generated = "\n".join(
        [
            '"""Generated workflow migration authority artifacts.\n\nDo not edit this file directly. Update\n`system_authority/workflow_migration_authority.json` and regenerate instead.\n"""',
            "",
            "from __future__ import annotations",
            "",
            "WORKFLOW_MIGRATION_SEQUENCE = " + pformat(manifest, width=88, sort_dicts=False),
            "",
            "WORKFLOW_FULL_BOOTSTRAP_SEQUENCE = "
            + pformat(full_bootstrap, width=88, sort_dicts=False),
            "",
            "WORKFLOW_POLICY_BUCKETS = "
            + pformat(policy_buckets, width=88, sort_dicts=False),
            "",
            "WORKFLOW_MIGRATION_POLICIES = "
            + pformat(migration_policies, width=88, sort_dicts=False),
            "",
            "WORKFLOW_BOOTSTRAP_PREREQUISITES = "
            + pformat(bootstrap_prerequisites, width=88, sort_dicts=False),
            "",
            "WORKFLOW_MIGRATION_EXPECTED_OBJECTS = "
            + pformat(expected_objects, width=88, sort_dicts=False),
            "",
            "WORKFLOW_SCHEMA_READINESS_SEQUENCE = "
            + pformat(readiness_sequence, width=88, sort_dicts=False),
            "",
            "WORKFLOW_MIGRATION_TIE_BREAK_ORDER = "
            + pformat(tie_break_order, width=88, sort_dicts=False),
            "",
            "WORKFLOW_MIGRATIONS_ALWAYS_REAPPLY = "
            + pformat(migrations_always_reapply, width=88, sort_dicts=False),
            "",
        ]
    )
    output_path.write_text(generated + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
