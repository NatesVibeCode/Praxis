# Phase 10 Build Packet: Optional Managed Runtime, Compute Accounting, and Observability

## Objective

Define a bounded implementation packet for an optional managed runtime layer that can execute approved workloads on behalf of customers while preserving exported and hybrid deployment paths. This phase adds compute accounting, cost visibility, run receipts, heartbeat-based runtime health, audit views, and customer-facing observability without requiring a full control-plane rewrite.

## Scope

### In Scope

- Optional managed execution mode for supported workflows and jobs.
- Exported and hybrid alternatives that preserve customer control over runtime placement.
- Compute metering for runtime consumption at the run, tenant, and environment levels.
- Cost visibility derived from metering data with transparent attribution rules.
- Run receipts that summarize workload identity, timing, resource use, status, and billable dimensions.
- Heartbeat health signals for managed workers and runtime pools.
- Internal audit views for support, operations, and compliance review.
- Customer-facing observability views for runs, costs, health, and recent failures.
- Automated tests and validation for the above behaviors.

### Out of Scope

- General-purpose multi-cloud orchestration beyond the minimum required managed runtime target.
- Full billing system implementation, invoicing, payments, or tax handling.
- Arbitrary workload scheduling for unsupported job classes.
- Deep tracing across every dependency in the stack.
- Long-term data warehouse design beyond the event and summary records required here.

## Target Outcomes

- Customers can choose `managed`, `exported`, or `hybrid` execution per environment or workload class.
- Every managed run produces a durable run receipt with consistent resource and cost fields.
- Operators can determine worker health and detect stuck capacity through heartbeats and freshness windows.
- Support and compliance users can inspect immutable run and account activity through audit views.
- Customers can inspect status, cost, and health information without internal-only operational detail.

## Operating Model

### Execution Modes

| Mode | Description | Primary Customer Need |
|---|---|---|
| Managed | Platform-hosted execution in approved runtime pools. | Lowest operational overhead. |
| Exported | Customer-hosted execution with platform-issued artifacts and local runtime control. | Maximum control and isolation. |
| Hybrid | Mix of platform-managed and customer-hosted execution by environment, workload type, or policy. | Gradual adoption and regulated placement. |

### Managed Runtime Principles

- Managed runtime is opt-in, not a mandatory replacement for exported deployments.
- Runtime capabilities are explicit and versioned.
- Workload eligibility is policy-driven.
- Resource accounting is attached to the actual run lifecycle, not inferred later from logs alone.
- Observability surfaces are role-aware: internal audit depth differs from customer-facing visibility.

## Deliverables

### 1. Managed Execution Control Path

Implement the planning design for:

- Managed runtime registration and capability advertisement.
- Job dispatch into managed pools.
- Policy checks for whether a run may execute in managed mode.
- Isolation boundaries for tenant workloads.
- Retry and failure classification sufficient for receipts and customer status.

Acceptance criteria:

- Supported runs can be routed to managed execution when the environment is configured for it.
- Unsupported or disallowed runs fail fast with a clear reason code.
- Mode selection is visible in run metadata and receipts.

### 2. Exported and Hybrid Alternatives

Define the compatibility path for:

- Exported artifacts or runtime bundles for customer-hosted execution.
- Shared run identity and status semantics across managed and exported paths.
- Hybrid routing rules by environment, workload class, policy, or customer tier.
- Consistent observability and receipt generation regardless of execution placement where data is available.

Acceptance criteria:

- A workload can be configured to run in managed or exported mode without changing business-level identifiers.
- Hybrid environments expose which runs were platform-managed versus customer-hosted.
- Missing customer-hosted telemetry is handled explicitly rather than silently backfilled.

### 3. Compute Metering

Define metering records for at least:

- Run start and end timestamps.
- Wall-clock runtime.
- CPU and memory reservation or usage basis, depending on implementation feasibility.
- Accelerator usage if applicable.
- Network or storage dimensions only if they are reliable and bounded in this phase.
- Tenant, environment, workflow, runtime version, and execution mode dimensions.

Metering requirements:

- Records are append-only or otherwise auditable.
- Raw events can be reconciled into summary usage views.
- Billable units and non-billable diagnostics are clearly separated.

Acceptance criteria:

- Every managed run emits enough metering data to compute runtime duration and core resource consumption.
- Aggregates can be produced by tenant and date range.
- Metering records link directly to run IDs and receipt IDs.

### 4. Cost Visibility

Expose derived cost estimates or billable totals using clear rules:

- Unit rates are versioned and attributable to a pricing schedule.
- Customer-facing cost fields indicate whether values are estimated, provisional, or finalized.
- Internal views can include richer breakdowns than external views.
- Hybrid and exported runs must not present managed runtime cost fields unless the basis is real and documented.

Acceptance criteria:

- Customers can view per-run cost and aggregate cost for managed runs.
- Operators can explain how a cost figure was computed for a sampled run.
- Pricing schedule changes do not mutate historical receipts retroactively without explicit version linkage.

### 5. Run Receipts

Create a receipt model generated at run completion, and optionally updated during execution, containing:

- Run ID, tenant, environment, workflow, attempt, and execution mode.
- Runtime identity and version.
- Start time, end time, duration, terminal status, and error classification.
- Metered resource dimensions and cost summary.
- Relevant policy decisions or execution labels.
- Provenance fields such as receipt version and generation timestamp.

Acceptance criteria:

- Receipts are durable, queryable, and immutable after finalization except for explicitly versioned corrections.
- A receipt can be retrieved from a run detail view.
- Receipt content is consistent with meter data and status history.

### 6. Heartbeat Health

Add heartbeat-based health monitoring for managed workers or runtime pools:

- Worker heartbeats with freshness windows and last-seen timestamps.
- Pool-level derived health such as healthy, degraded, stale, or unavailable.
- Detection of stuck workers, orphaned runs, or capacity starvation signals where feasible.
- Alertable conditions with explicit thresholds.

Acceptance criteria:

- Operators can identify unhealthy runtime capacity within one heartbeat timeout window.
- Run dispatch avoids clearly stale or unavailable capacity.
- Health state transitions are visible in internal operational views.

### 7. Audit Views

Provide internal audit surfaces for:

- Run lifecycle history.
- Mode selection and policy decisions.
- Receipt creation and correction events.
- Metering and pricing schedule references.
- Operator actions that affect runtime routing, retries, or overrides.

Acceptance criteria:

- Audit views are timestamped, filterable, and tied to stable identifiers.
- Internal users can reconstruct why a run executed in managed versus exported mode.
- Receipt changes are visible with before/after version references.

### 8. Customer-Facing Observability

Expose customer-visible observability for:

- Current and recent run status.
- Execution mode per run.
- Run-level timing and failure reason summaries.
- Managed runtime cost visibility.
- Environment or pool health summaries when relevant to the customer.

Constraints:

- Do not leak internal-only infrastructure identifiers.
- Separate estimated versus final values.
- Prefer concise status models over internal operational jargon.

Acceptance criteria:

- Customers can answer what ran, where it ran, how long it took, what it cost, and whether the runtime is healthy enough to trust.
- Internal-only details remain excluded from customer surfaces.

## Data Artifacts

Define or update planning artifacts for these logical records:

- `runtime_instance` or `runtime_pool`
- `runtime_heartbeat`
- `run_meter_event`
- `run_usage_summary`
- `run_receipt`
- `pricing_schedule_version`
- `audit_event`
- `customer_observability_view`

Each artifact definition should specify:

- Primary identifier
- Required fields
- Mutability rules
- Retention expectation
- Producer and consumer surfaces

## Non-Functional Requirements

- Receipts and metering must be idempotent under retry.
- Health freshness logic must tolerate clock skew within a defined bound.
- Observability queries must remain usable at tenant scale for recent-run windows.
- Audit records must be tamper-evident or operationally immutable.
- Managed-mode failures must degrade safely back to visible error states, not silent loss.

## Dependencies

- Stable run identity and lifecycle events from earlier phases.
- Environment and tenant model capable of storing execution mode policy.
- Authentication and authorization controls for internal versus customer views.
- Existing telemetry/event pipeline or a bounded equivalent for this phase.

## Risks and Controls

| Risk | Control |
|---|---|
| Metering disagrees with runtime reality. | Source usage from execution lifecycle and reconcile against receipts. |
| Hybrid mode creates inconsistent customer experience. | Standardize status, receipt, and mode fields across execution paths. |
| Heartbeat noise creates false alarms. | Use explicit freshness windows, grace periods, and derived pool health. |
| Cost numbers are mistaken for final invoices. | Label values as estimated, provisional, or finalized. |
| Audit and customer views leak internal details. | Enforce separate schemas or projection layers for internal and external consumers. |

## Test Plan

### Unit Tests

- Execution mode selection logic for managed, exported, and hybrid policies.
- Receipt generation from successful, failed, retried, and cancelled runs.
- Meter aggregation and pricing application with versioned schedules.
- Heartbeat freshness evaluation and pool health derivation.
- Audit projection logic and customer-visible field filtering.

### Integration Tests

- Managed run end-to-end from dispatch through receipt creation.
- Hybrid environment routing across both managed and customer-hosted runs.
- Meter data reconciliation into usage summaries and customer cost views.
- Heartbeat interruption causing degraded capacity visibility and dispatch avoidance.
- Receipt correction flow preserving historical versions.

### Negative Tests

- Missing heartbeat data.
- Duplicate meter events.
- Partial run failure before receipt finalization.
- Customer-hosted run with incomplete telemetry.
- Pricing schedule change during an in-flight run.
- Unauthorized access to internal audit fields.

### Regression Tests

- Exported-only customers remain unaffected when managed runtime is disabled.
- Existing run detail views continue to function without managed-mode adoption.
- Billing-adjacent outputs do not appear for non-managed runs unless explicitly supported.

## Validation Plan

Validation for this phase is complete when:

1. A representative managed run can be executed and produces a consistent status trail, meter data, and final receipt.
2. A representative exported run and a hybrid-configured run demonstrate consistent identifiers and differentiated execution mode visibility.
3. Sample cost calculations can be independently recomputed from stored meter data and schedule versions.
4. Simulated worker heartbeat loss changes pool health within the configured timeout and is visible in internal operations views.
5. Customer-facing observability surfaces show bounded, non-sensitive status and cost information.
6. Audit views can explain who changed routing or pricing references and why a given run was placed where it was.

## Definition of Done

- Planning artifacts for managed runtime, metering, receipts, health, audit, and customer observability are specified.
- Execution mode behavior and hybrid fallback rules are documented.
- Test coverage expectations are defined across unit, integration, negative, and regression layers.
- Validation scenarios exist for managed, exported, and hybrid paths.
- No code changes are included in this packet.

## Suggested Implementation Sequence

1. Finalize execution mode policy and canonical run/receipt schema.
2. Implement managed dispatch and lifecycle hooks needed for meter emission.
3. Add receipt generation and usage summary derivation.
4. Add heartbeat tracking and internal health views.
5. Add customer-facing observability projections.
6. Complete tests, validation runs, and operational review.
