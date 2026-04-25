# Workflow Migrations

This directory is the canonical home for workflow schema migrations.

Authority rules:

- migration files live here
- storage code applies them
- runtime code does not own them
- the executable canonical sequence is owned by
  `Code&DBs/Workflow/storage/migrations.py`
- this README explains the rules, but it is not the machine-checked manifest

Current status:

- these files are the active authority
- storage and runtime code must resolve workflow migrations from this directory
- `Code&DBs/Workflow/storage/` may apply or verify them, but it does not own
  duplicate copies

Do not add new workflow migration files anywhere else.

## Operation catalog registration (dependency chain)

When you add or enable a row in `operation_catalog_registry`, Postgres CHECK
triggers enforce a **three-step chain**. If you insert only the catalog row,
you will see sequential `CheckViolation` errors with little context unless you
read the trigger message carefully.

**Apply rows in this order:**

1. **`data_dictionary_objects`** — `object_kind` uses the
   `operation.<operation_ref>` convention (underscores in the ref, e.g.
   `operation.launch_plan`). The row must exist before the authority object.
2. **`authority_object_registry`** — `object_ref` matches the operation
   (`operation.launch_plan`), and `data_dictionary_object_kind` points at the
   dictionary row from step 1. Required before the catalog row when the
   operation is enabled.
3. **`operation_catalog_registry`** — the HTTP/MCP-visible operation definition.

**Worked example:** `234_register_plan_operations.sql` (comments + INSERT order).

**Machine authority:** `Code&DBs/Workflow/storage/migrations.py` owns the
ordered manifest; this README is human guidance only.

## Manual apply receipts (`schema_migrations`)

If you apply migration SQL **out of band** (psql, one-off script) and skip the
normal bootstrap runner, you must still record a ledger row. A naive
`INSERT INTO schema_migrations (filename, …)` fails with **NOT NULL** on
`content_sha256` and `bootstrap_role` (see BUG-431B3436).

**Canonical API (Python):** `storage.postgres.schema.record_migration_apply`
(also exported as `storage.postgres.record_migration_apply`). It:

- reads the migration file from this directory via the storage resolver,
- computes `content_sha256`,
- sets `bootstrap_role` to `canonical` or `bootstrap_only` per generated policy,
- upserts `applied_at` / `applied_by`.

**Shell / SQL:** only if you cannot call Python—compute SHA-256 of the exact
on-disk file bytes, set `bootstrap_role` to `canonical` unless the file is
listed as `bootstrap_only` in generated migration authority, and include a
non-empty `applied_by` (e.g. `manual_operator`).
