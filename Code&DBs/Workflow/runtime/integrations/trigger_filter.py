"""Trigger composition — evaluate filter expressions on webhook payloads.

Filter expression format (JSON condition tree):
{
    "op": "and",
    "conditions": [
        {"field": "data.object.amount", "op": "gt", "value": 5000},
        {"field": "type", "op": "eq", "value": "payment_intent.succeeded"}
    ]
}

Delegates to runtime.condition_evaluator for evaluation.
"""

from __future__ import annotations

from dataclasses import dataclass

from runtime.condition_evaluator import evaluate_filter, validate_filter  # noqa: F401


@dataclass
class TriggerDecision:
    should_trigger: bool
    transformed_payload: dict
    filter_matched: bool
    reason: str


def evaluate_trigger(payload: dict, endpoint: dict) -> TriggerDecision:
    """Evaluate filter + optional transform for a webhook endpoint."""
    filter_expr = endpoint.get("filter_expression")
    transform_spec_dict = endpoint.get("transform_spec")

    if not filter_expr:
        matched = True
        reason = "no filter configured"
    else:
        matched = evaluate_filter(payload, filter_expr)
        reason = "filter matched" if matched else "filter did not match"

    transformed = payload
    if matched and transform_spec_dict:
        from runtime.integrations.data_mapper import transform_from_dict, apply_transform
        spec = transform_from_dict(transform_spec_dict)
        transformed = apply_transform(payload, spec)

    return TriggerDecision(
        should_trigger=matched,
        transformed_payload=transformed,
        filter_matched=matched,
        reason=reason,
    )
