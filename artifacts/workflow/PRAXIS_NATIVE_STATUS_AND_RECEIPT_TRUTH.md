# Native Status And Receipt Truth

- Native status is a derived operator view over canonical DB state.
- Canonical durable truth surfaces are:
  - `workflow_runs`
  - `workflow_events`
  - `receipts`
  - `workflow_outbox`
- A healthy terminal run shows:
  - run state `succeeded`
  - complete inspection watermark
  - final outbox envelope kind `receipt`
  - final outbox receipt type `workflow_completion_receipt`
- Operator reads may summarize that truth, but they do not replace it.
