# First Slice Test Matrix

## Purpose

This matrix turns the envelope spine into fail-closed contract tests.

Each row is a real check:

- one pass fixture
- one fail fixture
- one explicit assertion
- one explicit failure mode

## Fixture Rules

- Pass fixtures must satisfy all required fields and lineage rules for the specific row.
- Fail fixtures must violate one contract rule at a time.
- Multi-row checks must provide the full evidence set to the validator.
- Contract tests must fail if a surface, helper, or replay path accepts a row that the contract rejects.

## Check Matrix

| ID | Suggested test name | Contract area | Pass fixture | Fail fixture | Assertion | Expected failure |
| --- | --- | --- | --- | --- | --- | --- |
| ENV-01 | `test_event_envelope_requires_required_fields` | event envelope shape | One `workflow_event_v1` row with `event_id`, `event_type`, `schema_version=1`, `workflow_id`, `run_id`, `request_id`, `evidence_seq`, `occurred_at`, `actor_type`, `reason_code`, and `payload`. | The same row with `evidence_seq` missing. | The validator accepts the full row. | The validator rejects the malformed row. |
| ENV-02 | `test_receipt_envelope_requires_required_fields` | receipt envelope shape | One `receipt_v1` row with `receipt_id`, `receipt_type`, `schema_version=1`, `workflow_id`, `run_id`, `request_id`, `evidence_seq`, `executor_type`, `started_at`, `finished_at`, `status`, `inputs`, `outputs`, `artifacts`, and `decision_refs`. | The same row with `decision_refs` missing or `finished_at` earlier than `started_at`. | The validator accepts the full row. | The validator rejects the malformed row. |
| REF-01 | `test_reference_fields_are_typed_objects` | typed references | `decision_refs` and `artifacts` arrays containing objects with the required keys. | `decision_refs` or `artifacts` as strings, blobs, or free text. | The validator accepts typed references. | The validator rejects untyped references. |
| ORD-01 | `test_evidence_seq_orders_mixed_event_and_receipt_rows` | shared evidence ordering | A mixed row set for one run with event and receipt rows assigned `evidence_seq` values `1`, `2`, `3`. | The same rows with timestamps that suggest a different order than `evidence_seq`. | Replay order follows `evidence_seq` only. | Timestamp-based ordering is rejected. |
| ORD-02 | `test_duplicate_evidence_seq_is_invalid` | shared evidence ordering | One evidence row per `evidence_seq` value in a run. | Two evidence rows in the same run with the same `evidence_seq`. | The sequence space validates as a strict order. | Duplicate sequence reuse fails closed. |
| RID-01 | `test_route_identity_preserves_lineage_across_stages` | route identity lineage | A claim-to-promotion fixture where `workflow_id`, `run_id`, and `request_id` stay fixed while `claim_id`, `lease_id`, `proposal_id`, and `promotion_decision_id` appear only when assigned. | The same lineage with a stale `claim_id` or `proposal_id` reused from a prior attempt. | One route identity explains the whole path. | Stale id reuse is rejected as a conflict. |
| RID-02 | `test_transition_seq_matches_the_proof_pair` | route identity lineage | An event and receipt pair that prove the same authoritative transition and carry the same `route_identity` and `transition_seq`. | The same pair with different `transition_seq` values. | The pair is accepted as one transition proof. | Mismatched transition lineage fails closed. |
| CAN-01 | `test_cancel_before_promotion_decision_is_allowed` | pre-decision cancellation | An explicit `workflow_cancelled` event/receipt bundle before any `promotion_decision_id` exists. | The same stop bundle after a decision row exists. | Cancellation is accepted only before the decision boundary. | Post-decision cancel is rejected. |
| CAN-02 | `test_cancel_after_promotion_decision_is_rejected` | pre-decision cancellation | A route with `promotion_decision_recorded` already persisted. | A `workflow_cancelled` event/receipt on the same proposal lineage. | The route remains on the decision path. | Cancellation after decision is invalid. |
| CAN-03 | `test_cancel_requires_authority_and_source_state` | cancellation evidence | A cancellation bundle that names the owning authority, source state, and `transition_seq`. | A cancellation bundle missing authority or source state. | The stop is explicit and attributable. | Incomplete stop evidence is rejected. |
| GATE-01 | `test_gate_blocked_stays_pre_decision` | gate blocking boundary | A `gate_blocked` fixture with blocker, deadline, and no promotion decision row. | The same fixture with a hidden or prefilled `promotion_decision_id`. | The block remains pre-decision only. | Post-decision leakage is rejected. |
| PROM-01 | `test_one_promotion_decision_row_per_proposal` | promotion decision row | Exactly one `promotion_decision` row for one `proposal_id`. | Two rows with the same `proposal_id`. | One authoritative decision exists per proposal. | A second row fails closed. |
| PROM-02 | `test_promotion_decision_accept_or_reject_only` | promotion decision row | A decision row whose `decision` is `accept` or `reject`. | A decision row whose `decision` is `block` or any free text. | The decision value is recognized. | Invalid decision values are rejected. |
| PROM-03 | `test_accept_decision_requires_matching_validation_and_head_refs` | promotion decision row | An `accept` row with matching `proposal_manifest_hash`, `validation_receipt_ref`, `policy_snapshot_ref`, `validated_head_ref`, and promotion target refs. | An `accept` row with a mismatched manifest hash or missing validation receipt ref. | The accept decision can authorize promotion. | Accept without matching evidence is rejected. |
| PROM-04 | `test_reject_decision_forbids_promotion` | promotion decision row | A `reject` row and no canonical promotion receipt. | The same `reject` row followed by an attempted promotion receipt. | Rejection is terminal for promotion. | Promotion after reject is forbidden. |
| PROM-05 | `test_second_promotion_decision_for_same_proposal_fails_closed` | promotion decision row | One existing decision row for a `proposal_id`. | A second decision row for the same `proposal_id`. | The first row remains authoritative. | The second decision is rejected. |

## Coverage Rules

- Every contract area above must have at least one positive and one negative check.
- Every negative check must isolate one violated rule.
- At least one mixed-row ordering check must span both `workflow_events` and `receipts`.
- At least one lineage check must prove that stale ids are conflicts, not transfers.
- At least one cancellation check must prove the pre-decision boundary.
- At least one promotion check must prove that a second decision row for the same proposal is not allowed.

## Exit Rule

If a surface, validator, or replay path can pass one of the fail fixtures above, the contract spine is incomplete.
