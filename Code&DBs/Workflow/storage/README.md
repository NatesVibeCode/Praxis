# storage

Owns:

- repositories
- persistence adapters
- migration application and verification logic
- transaction boundaries

Does not own:

- migration files
- business rules
- surface logic
- admission or routing policy

Storage should be boring and deterministic.

Workflow storage reads the canonical workflow migration tree at
`Code&DBs/Databases/migrations/workflow/`.

The v1 control-plane spine is authority-only:

- canonical tables
- explicit indexes
- table and column comments that encode the binding rules

Derived projections, summaries, and convenience tables stay out of this spine.
