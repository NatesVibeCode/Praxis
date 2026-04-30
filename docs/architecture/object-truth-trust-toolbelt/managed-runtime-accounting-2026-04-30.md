# Managed Runtime Accounting - 2026-04-30

## Verdict

Phase 10 adds a deterministic domain authority for optional managed runtime
accounting and observability. It does not make managed runtime mandatory and
does not create billing, invoicing, orchestration, or persistence tables.

The authority model is simple:

- execution policy resolves `managed`, `exported`, or `hybrid` configuration
  into one explicit placement decision
- meter events are append-only logical inputs and deduplicated by idempotency
  key
- usage summaries are derived from meter events
- cost summaries reference a pricing schedule version
- final run receipts freeze placement, timing, usage, status, and cost basis
- heartbeat summaries decide whether managed capacity is dispatchable
- internal audit projections keep infrastructure detail
- customer observability projections remove internal worker and pool identity

## Runtime Boundary

Implemented code lives in `runtime.managed_runtime.accounting`. It is pure
Python domain code with no database, HTTP, MCP, worker, or migration dependency.
That is intentional. Phase 10 defines the contract future surfaces can persist
through the CQRS gateway without letting a tool wrapper or script become the
source of truth.

## Execution Modes

`ExecutionModePolicy` is scoped to one tenant and environment. It carries:

- configured mode: `managed`, `exported`, or `hybrid`
- managed workload allow-list
- optional exported workload allow-list
- workload-level mode overrides
- managed enablement and tenant permission flags
- decision refs that explain the policy basis

`select_execution_mode` returns a `ModeSelection` instead of silently falling
through. A requested managed run that is disabled, denied, or unsupported is
not routed elsewhere. It fails fast with a reason code. Hybrid configuration
may route eligible work to managed runtime and preserve exported execution for
unsupported customer-hosted work.

## Accounting Contract

`RunMeterEvent` captures the run, tenant, environment, workflow, execution
mode, runtime version, event kind, timestamp, resource dimensions, and
idempotency key. Duplicate events with the same idempotency key are ignored
when building a summary.

`RunUsageSummary` derives:

- start and end timestamps
- duration
- billable wall seconds
- CPU core seconds
- memory GiB seconds
- accelerator seconds
- diagnostic event count
- duplicate event count
- cost summary

Exported runs receive a `not_applicable` cost summary. Managed runs can carry
`estimated`, `provisional`, or `finalized` cost status. If a managed run lacks a
pricing schedule, the summary stays provisional with `pricing_schedule_missing`
as the basis.

## Pricing Versioning

`PricingScheduleVersion` carries a stable schedule ref, version ref, effective
timestamp, currency, resource rates, and minimum charge. Cost computation stores
the schedule version ref on the `CostSummary`, so later pricing changes do not
rewrite historical receipts by accident.

## Receipt Contract

`finalize_run_receipt` creates a deterministic `RunReceipt` from:

- run identity
- mode selection
- usage summary
- terminal status
- runtime version and optional internal pool ref
- policy reason and decision refs
- receipt version and generation timestamp

The receipt is frozen data. Versioned correction can be represented by creating
a new receipt with `correction_of_receipt_id`; Phase 10 does not implement a
mutation path.

## Heartbeat And Pool Health

`RuntimeHeartbeat` records worker, pool, tenant, environment, runtime version,
observed time, capacity, active runs, accepting-work state, stuck runs, and last
error code.

`derive_pool_health` produces a `PoolHealthSummary`:

- `healthy`: fresh capacity, no blocking reason
- `degraded`: fresh capacity exists but stale workers, errors, stuck runs, or
  capacity pressure are present
- `stale`: heartbeats exist but are outside the fresh window
- `unavailable`: no usable heartbeat evidence

Dispatch is blocked for stale or unavailable pools, and for fresh pools with no
available capacity or stuck-run evidence.

## Internal Audit

`AuditEvent` and `build_internal_audit_contract` preserve internal details
needed by support, operations, and compliance:

- actor and action
- target identity
- before/after version refs
- reason code
- full receipt payload
- meter event ids
- pool health with internal pool ref

This is intentionally not customer-safe.

## Customer Observability

`customer_observability_summary` answers the customer-facing questions:

- what ran
- where it ran at the execution-mode level
- how long it ran
- whether it succeeded or failed
- what managed-runtime cost basis applies
- whether managed runtime capacity is healthy enough to trust

It excludes internal worker refs, pool refs, raw meter event ids, audit actors,
and line-item infrastructure detail.

## Logical Records

| Record | Primary identifier | Mutability | Producer | Consumer |
| --- | --- | --- | --- | --- |
| `runtime_pool` | `pool_ref` | append/versioned config | Phase 11 control plane | dispatcher, audit, health |
| `runtime_heartbeat` | worker + observed timestamp | append-only | managed worker | health projection |
| `run_meter_event` | `idempotency_key` | append-only, idempotent | run lifecycle hook | usage summary, audit |
| `run_usage_summary` | `run_id` + attempt | derived/recomputable | accounting projector | receipt, customer cost |
| `run_receipt` | `receipt_id` | immutable, corrected by new version | receipt finalizer | run detail, audit |
| `pricing_schedule_version` | `version_ref` | immutable after use | operator/control plane | cost summary |
| `audit_event` | `audit_event_id` | append-only | operator/runtime surfaces | internal audit |
| `customer_observability_view` | `run_id` + attempt | projected | customer API/MCP/UI | customer support view |

## Phase 11 Surface Needs

- Persist these contracts through CQRS operations, not direct tool code.
- Add registry rows for managed runtime write/query operations.
- Add DB constraints for idempotency keys, receipt immutability, and pricing
  schedule version references.
- Connect run lifecycle hooks to meter event emission.
- Expose internal audit and customer observability as separate projections.
