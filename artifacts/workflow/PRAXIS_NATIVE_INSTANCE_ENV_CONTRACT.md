# Native Instance Env Contract

- Required operator inputs:
  - `WORKFLOW_DATABASE_URL`
  - `PRAXIS_RUNTIME_PROFILES_CONFIG`
  - `PRAXIS_RUNTIME_PROFILE`
  - `PRAXIS_LOCAL_POSTGRES_DATA_DIR`
- Derived instance values:
  - `PRAXIS_INSTANCE_NAME`
  - `PRAXIS_RECEIPTS_DIR`
  - `PRAXIS_TOPOLOGY_DIR`
  - `repo_root`
  - `workdir`
- The native smoke contract treats runtime profile and workspace as `praxis`.
- The checked-in contract must not embed machine-local absolute paths outside the explicit verify commands.
