# Native Operator Handoff

- The checked-in smoke queue is a review artifact, not runtime authority.
- Runtime authority lives in `workflow_definitions`, `workflow_runs`, `workflow_events`, `receipts`, and `workflow_outbox`.
- `praxis workflow native-operator smoke` loads the canonical smoke template from DB authority, isolates ids, submits one run, executes that admitted run, then proves completion from receipts and outbox.
- Handoff is complete only when the final outbox row carries `workflow_completion_receipt` with status `succeeded`.
