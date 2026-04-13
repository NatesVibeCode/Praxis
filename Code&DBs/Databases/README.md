# Databases

Use this area for:

- schema artifacts
- migrations
- fixtures
- local development data
- local test data

Keep durable product behavior in contracts and runtime code, not in ad hoc database shape guesses.

## Authority Rules

- Canonical migration assets belong under `Code&DBs/Databases/migrations/`.
- Canonical fixture assets belong under `Code&DBs/Databases/fixtures/`.
- Local runtime data such as the repo-local Postgres cluster belongs under explicit dev/test subtrees here.
- Runtime code may apply or verify migrations, but it does not own migration files.
- If migration assets are duplicated between this tree and `Workflow/storage/`, that is transitional drift and must be cleaned up instead of normalized.

## Expected Structure

- `migrations/workflow/`
- `fixtures/workflow/`
- `postgres-dev/`
- `dispatch-state/`

## Current Note

Workflow runtime bootstrap resolves canonical schema files from
`Code&DBs/Databases/migrations/workflow/`.
If SQL assets reappear under `Workflow/storage/`, treat that as authority drift
and remove the duplicate copy instead of teaching runtime code two roots.
