# api

Owns:

- request and response translation
- API-specific transport concerns
- tiny operator-facing wrappers over existing repo-local services

Does not own:

- lifecycle truth
- policy logic
- persistence behavior

The API should expose the system, not re-implement it.

Discoverability front doors:

- `workflow api routes` prints the live HTTP route catalog from the CLI.
- `workflow api routes --search ... --method ... --tag ... --path-prefix ...`
  narrows the live route catalog before printing it.
- `GET /api/routes` returns the same catalog as JSON for programmatic clients.
- `GET /api/routes?search=...&method=...&tag=...&path_prefix=...` returns the
  filtered catalog for machines.
