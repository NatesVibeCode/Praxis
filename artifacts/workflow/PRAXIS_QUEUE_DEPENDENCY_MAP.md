# Queue Dependency Map

- Checked-in smoke queue documents the intended contract for review and tests.
- DB-native smoke definition is the runtime source of truth.
- Native smoke depends on:
  - runtime instance resolution
  - registry authority loading
  - frontdoor submit and status
  - admitted execution
  - evidence inspection
  - outbox replay
- No dependency in this path should bypass repo-local DB authority.
