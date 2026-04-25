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

- `docs/API.md` is the generated HTTP route reference.
- `docs/CLI.md` is the generated terminal reference for route discovery and catalog-backed aliases.
- `docs/MCP.md` is the generated MCP/catalog tool reference.
- `workflow api routes` prints the live HTTP route catalog from the CLI.
- `workflow api help` opens the same route-discovery help from the API namespace.
- `workflow routes` is the flat alias when you want the same discovery path without the `api` namespace.
- `workflow help routes` opens the same route-discovery help text as `workflow help api`.
- `workflow api routes --search ... --method ... --tag ... --path-prefix ...`
  narrows the live route catalog before printing it.
- The route payload includes facet summaries for method and tag counts, plus a
  suggested follow-up filter when the catalog is unfiltered.
- `GET /api/routes` returns the same catalog as JSON for programmatic clients.
- `GET /api/routes?search=...&method=...&tag=...&path_prefix=...` returns the
  filtered catalog for machines.
