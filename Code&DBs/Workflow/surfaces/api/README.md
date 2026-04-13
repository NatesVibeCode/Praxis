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
