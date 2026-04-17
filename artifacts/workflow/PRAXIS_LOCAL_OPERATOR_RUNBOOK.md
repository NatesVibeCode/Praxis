# Local Operator Runbook

- Run `praxis workflow schema apply --scope workflow --yes --json` before smoke on a fresh local DB.
- Run `praxis workflow native-operator smoke` to prove repo-local instance resolution, DB reachability, admitted execution, and receipt-backed completion.
- Inspect the run with:
  - `praxis workflow native-operator status <run_id>`
  - `praxis workflow native-operator inspect <run_id>`
- If smoke fails, treat receipts and outbox as truth. Logs are only supporting evidence.
