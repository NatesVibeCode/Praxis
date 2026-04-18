"""Backfill: rewrite stored repo refs to canonical form.

Runs ``to_repo_ref()`` over every persisted path so that downstream search,
diff, and migration tooling can rely on one canonical spelling.

Tables covered:
- ``module_embeddings.module_path`` (discover index)

Run with:
    WORKFLOW_DATABASE_URL="$WORKFLOW_DATABASE_URL" \\
    PYTHONPATH='Code&DBs/Workflow' python3 Code&DBs/Workflow/scripts/backfill_repo_refs.py
"""
from __future__ import annotations

from runtime.workspace_paths import code_tree_dirname, to_repo_ref
from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool


def _backfill_module_embeddings(conn: SyncPostgresConnection) -> int:
    canonical = code_tree_dirname()
    rows = conn.execute(
        """
        SELECT id, module_path
        FROM module_embeddings
        WHERE module_path IS NOT NULL
          AND module_path NOT LIKE $1
        """,
        f"{canonical}/%",
    )
    updated = 0
    for row in rows or ():
        canonical_ref = to_repo_ref(row["module_path"])
        if canonical_ref == row["module_path"]:
            continue
        conn.execute(
            "UPDATE module_embeddings SET module_path = $1 WHERE id = $2",
            canonical_ref,
            row["id"],
        )
        updated += 1
    return updated


def main() -> None:
    conn = SyncPostgresConnection(get_workflow_pool())
    updates = _backfill_module_embeddings(conn)
    print(f"module_embeddings: {updates} rows rewritten to canonical refs")


if __name__ == "__main__":
    main()
