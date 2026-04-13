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
