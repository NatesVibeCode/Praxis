# V1 Envelopes

## Purpose

This file defines the first machine-oriented contract spine for workflow evidence, route lineage, and promotion decisions.

It is narrow by design:

- shared evidence ordering
- route identity lineage
- event envelopes
- receipt envelopes
- promotion decision rows
- pre-decision cancellation boundary

Authority comes from:

- `Build Plan/11_RECEIPT_AND_EVENT_MODEL.md`
- `Build Plan/11_LIFECYCLE_STATE_MACHINE.md`
- `Build Plan/18_AUTHORITY_MAP.md`
- `Build Plan/21_BUILD_PATTERN_GUARDRAILS.md`
- `Code&DBs/Workflow/docs/02_EVENT_AND_RECEIPT_CONTRACTS.md`
- `Code&DBs/Workflow/docs/10_V1_POSTGRES_TABLES.md`
- `Code&DBs/Workflow/docs/11_CLAIM_LEASE_PROPOSAL_LIFECYCLE.md`
- `Code&DBs/Workflow/docs/11B_PROPOSAL_AND_PROMOTION_STATES.md`

## Contract Rules

- `schema_version` is required on events and receipts and must resolve to v1.
- Every envelope carries explicit `workflow_id`, `run_id`, and `request_id`.
- `causation_id` is explicit whenever a row exists because of a prior event, decision, or receipt.
- `evidence_seq` is the run-scoped ordering field shared by `workflow_events` and `receipts`.
- `transition_seq` is route-scoped lineage, not evidence ordering.
- `decision_refs` and `artifacts` are typed reference objects, not free text.
- Replay consumes persisted evidence only.
- If the evidence set cannot prove the transition, the transition did not happen.

## Shared Evidence Ordering

- `evidence_seq` is strictly increasing within one `run_id`.
- `workflow_events` and `receipts` share the same `evidence_seq` sequence space.
- No two evidence rows in one run may reuse the same `evidence_seq`.
- Replay sorts by `evidence_seq`, not by timestamp guesswork.
- If a row ties or collides on `evidence_seq`, the evidence set is invalid.

## Route Identity V1

`route_identity` is the stable lineage object across claim, lease, proposal, gate, and promotion.

Required fields:

- `workflow_id`
- `run_id`
- `claim_id`
- `lease_id` when assigned
- `proposal_id` when assigned
- `promotion_decision_id` when recorded
- `attempt_no`
- `transition_seq`
- `authority_context_ref`

Rules:

- `request_id` stays constant across the envelope set for the run and must not change across the route.
- fresh attempts get fresh ids.
- `attempt_no` increments only when a new attempt replaces a dead or terminal attempt.
- `transition_seq` is monotonic within the route lineage.
- `transition_seq` identifies the authoritative transition being proven.
- The event and receipt that prove the same authoritative transition must agree on `route_identity` and `transition_seq`.
- `transition_seq` does not replace `evidence_seq`.
- Stale ids are conflicts, not transfers.
- A row from a later attempt may not be used to explain an earlier attempt.

## Event Envelope V1

Required fields:

| Field | Required | Notes |
| --- | --- | --- |
| `event_id` | yes | Stable primary identity for the event row. |
| `event_type` | yes | Canonical event name such as `workflow_received` or `node_failed`. |
| `schema_version` | yes | Envelope version gate. |
| `workflow_id` | yes | Workflow identity for joins and replay. |
| `run_id` | yes | Run identity for joins and replay. |
| `request_id` | yes | Stable logical request identity across the full run. |
| `causation_id` | when applicable | Prior event, decision, or receipt that caused this row. |
| `node_id` | when applicable | Present when the event is node-scoped. |
| `evidence_seq` | yes | Shared run-scoped ordering field across events and receipts. |
| `occurred_at` | yes | Authoritative UTC time the event became true. |
| `actor_type` | yes | The emitting authority class. |
| `reason_code` | yes | Machine-readable cause for the event. |
| `payload` | yes | Structured detail that does not fit in the top-level fields. |

Rules:

- `schema_version` must be `1` for the v1 envelope.
- `event_type` must be a canonical vocabulary value, not ad hoc prose.
- `occurred_at` is authoritative time, not a display timestamp.
- if two events tie on wall clock time, `evidence_seq` still orders them.
- `reason_code` must stay machine-readable and compatible with the failure taxonomy or decision vocabulary.
- `payload` must not smuggle canonical fields that belong at the top level.

## Receipt Envelope V1

Required fields:

| Field | Required | Notes |
| --- | --- | --- |
| `receipt_id` | yes | Stable primary identity for the receipt row. |
| `receipt_type` | yes | Canonical action type for the attempt. |
| `schema_version` | yes | Envelope version gate. |
| `workflow_id` | yes | Workflow identity for joins and replay. |
| `run_id` | yes | Run identity for joins and replay. |
| `request_id` | yes | Stable logical request identity across the full run. |
| `causation_id` | when applicable | Prior event, decision, or receipt that caused this action. |
| `node_id` | when applicable | Present when the receipt is node-scoped. |
| `attempt_no` | when applicable | Monotonic attempt number for retries. |
| `supersedes_receipt_id` | when applicable | Prior failed or blocked receipt this one replaces. |
| `evidence_seq` | yes | Shared run-scoped ordering field across events and receipts. |
| `executor_type` | yes | The concrete executor class that attempted the action. |
| `started_at` | yes | UTC time the action began. |
| `finished_at` | yes | UTC time the action ended. |
| `status` | yes | Outcome state for the attempted action. |
| `inputs` | yes | Structured input payload used for the action. |
| `outputs` | yes | Structured output payload produced by the action. |
| `artifacts` | yes | References to durable outputs or attachments. |
| `failure_code` | when applicable | Machine-readable failure result when the action did not succeed. |
| `decision_refs` | yes | References to the policy or promotion decisions that shaped the outcome. |

Rules:

- `schema_version` must be `1` for the v1 envelope.
- `started_at` and `finished_at` must be UTC-backed.
- `started_at` must not be after `finished_at`.
- instantaneous actions may use the same value for both timestamps.
- `status` must classify the action outcome, not narrate it.
- `failure_code` is required whenever the action fails or blocks in a way that needs classification.
- `inputs`, `outputs`, `artifacts`, and `decision_refs` must stay structured, not rendered prose.

## Typed Reference Objects

### `decision_refs[]`

Each element must be an object with, at minimum:

- `decision_type`
- `decision_id`
- `reason_code`
- `source_table`

### `artifacts[]`

Each element must be an object with, at minimum:

- `artifact_id`
- `artifact_type`
- `content_hash`
- `storage_ref`

Rules:

- free text strings are invalid in these fields.
- rendered CLI output is not a valid reference object.
- if a later consumer cannot resolve the typed reference, the reference is incomplete.

## Promotion Decision Row V1

The promotion decision row is the only authoritative promotion decision path.

Required fields:

- `promotion_decision_id`
- `proposal_id`
- `workflow_id`
- `run_id`
- `decision`
- `reason_code`
- `decided_at`
- `decided_by`
- `policy_snapshot_ref`
- `validation_receipt_ref`
- `proposal_manifest_hash`
- `validated_head_ref`
- `promotion_intent_at`
- `finalized_at`
- `canonical_commit_ref`
- `target_kind`
- `target_ref`

Rules:

- exactly one authoritative row is allowed per `proposal_id` in v1.
- `decision` is `accept` or `reject` only.
- `block` is not a promotion decision row; block lives in `gate_blocked`.
- `accept` authorizes promotion only when the accepted manifest, validation receipt, and validated head evidence all match.
- `reject` forbids promotion.
- a second row for the same `proposal_id` is a contract failure.
- the row is append-only; no rewrite, no delete, no hidden second authority.

## Cancellation Boundary

- `workflow_cancelled` is only valid before `promotion_decision_id` exists for the current proposal lineage.
- `promotion_decision_recorded -> cancelled` is invalid.
- any pre-decision non-terminal state may emit `workflow_cancelled` only on explicit stop from the owning authority.
- pre-decision cancellation must name the owning authority, source state, and `transition_seq`.
- silence is not cancellation.
- once a decision exists, the only valid terminal outcomes are `promoted`, `promotion_rejected`, or `promotion_failed`.

## Non-Negotiable Invariants

- shared evidence ordering beats timestamp order.
- route lineage beats ambient state.
- one `proposal_id` maps to one authoritative promotion decision row.
- cancellation is pre-decision only.
- post-decision cancellation is forbidden.
- replay uses stored evidence only.
