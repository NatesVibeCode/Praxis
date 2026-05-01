"""Synthetic Environment authority domain primitives.

Synthetic Environment owns mutable seeded worlds: clear/reset lifecycle,
effect ledgers, state diffs, deterministic clocks, and outside event injection.
Synthetic Data remains the generated dataset authority; Virtual Lab remains the
consequence-proof execution layer.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
from typing import Any, Mapping, Sequence


SYNTHETIC_ENVIRONMENT_SCHEMA_VERSION = 1
MAX_ENVIRONMENT_RECORDS = 100_000
LIFECYCLE_STATES = {"active", "cleared", "retired", "blocked"}


class SyntheticEnvironmentError(ValueError):
    """Domain-level synthetic environment failure with machine-readable detail."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: Any, *, length: int = 16) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()[:length]


def _state_digest(state: Mapping[str, Any]) -> str:
    return f"sha256:v1:{_digest(state, length=40)}"


def _slug(value: object) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")
    return text or "synthetic_environment"


def _clean_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SyntheticEnvironmentError(
            "synthetic_environment.invalid_input",
            f"{field_name} must be a non-empty string",
            details={"field_name": field_name},
        )
    return value.strip()


def _clean_optional_text(value: object) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str) or not value.strip():
        raise SyntheticEnvironmentError(
            "synthetic_environment.invalid_input",
            "optional text fields must be non-empty strings when supplied",
        )
    return value.strip()


def _clean_mapping(value: object, *, field_name: str) -> dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise SyntheticEnvironmentError(
            "synthetic_environment.invalid_input",
            f"{field_name} must be a JSON object",
            details={"field_name": field_name},
        )
    return dict(value)


def _clean_string_list(value: object, *, field_name: str) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, Sequence) or isinstance(value, bytes):
        raise SyntheticEnvironmentError(
            "synthetic_environment.invalid_input",
            f"{field_name} must be a list of strings",
            details={"field_name": field_name},
        )
    return [str(item).strip() for item in value if str(item).strip()]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_clock_time(value: object | None) -> str:
    if value is None or value == "":
        return _utc_now()
    if isinstance(value, datetime):
        clock = value
    elif isinstance(value, str):
        try:
            clock = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise SyntheticEnvironmentError(
                "synthetic_environment.invalid_clock_time",
                "clock_time must be an ISO timestamp",
                details={"clock_time": value},
            ) from exc
    else:
        raise SyntheticEnvironmentError(
            "synthetic_environment.invalid_clock_time",
            "clock_time must be an ISO timestamp",
        )
    if clock.tzinfo is None:
        clock = clock.replace(tzinfo=timezone.utc)
    return clock.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _advance_clock_time(clock_time: object, *, seconds: int | None, set_time: object | None) -> str:
    if set_time is not None and set_time != "":
        return _normalize_clock_time(set_time)
    if seconds is None:
        raise SyntheticEnvironmentError(
            "synthetic_environment.invalid_clock_advance",
            "seconds or set_time is required",
        )
    base = datetime.fromisoformat(_normalize_clock_time(clock_time).replace("Z", "+00:00"))
    return (base + timedelta(seconds=int(seconds))).astimezone(timezone.utc).replace(
        microsecond=0
    ).isoformat().replace("+00:00", "Z")


def _record_state_from_dataset(dataset: Mapping[str, Any]) -> dict[str, Any]:
    dataset_ref = _clean_text(dataset.get("dataset_ref"), field_name="dataset.dataset_ref")
    records = list(dataset.get("records") or [])
    if not records:
        raise SyntheticEnvironmentError(
            "synthetic_environment.dataset_has_no_records",
            "Synthetic Environment requires a dataset with records",
            details={"dataset_ref": dataset_ref},
        )
    if len(records) > MAX_ENVIRONMENT_RECORDS:
        raise SyntheticEnvironmentError(
            "synthetic_environment.record_limit_exceeded",
            "Synthetic Environment cannot seed more than MAX_ENVIRONMENT_RECORDS",
            details={"dataset_ref": dataset_ref, "record_count": len(records), "max": MAX_ENVIRONMENT_RECORDS},
        )
    record_map: dict[str, dict[str, Any]] = {}
    for raw_record in records:
        if not isinstance(raw_record, Mapping):
            raise SyntheticEnvironmentError(
                "synthetic_environment.invalid_dataset_record",
                "dataset records must be JSON objects",
                details={"dataset_ref": dataset_ref},
            )
        record_ref = _clean_text(raw_record.get("record_ref"), field_name="record.record_ref")
        if record_ref in record_map:
            raise SyntheticEnvironmentError(
                "synthetic_environment.duplicate_record_ref",
                "dataset records cannot contain duplicate refs",
                details={"dataset_ref": dataset_ref, "record_ref": record_ref},
            )
        fields = _clean_mapping(raw_record.get("fields"), field_name="record.fields")
        record_map[record_ref] = {
            "record_ref": record_ref,
            "dataset_ref": dataset_ref,
            "object_kind": _clean_text(raw_record.get("object_kind"), field_name="record.object_kind"),
            "object_slug": _clean_text(raw_record.get("object_slug"), field_name="record.object_slug"),
            "ordinal": int(raw_record.get("ordinal") or 0),
            "display_name": _clean_text(raw_record.get("display_name"), field_name="record.display_name"),
            "name_ref": _clean_text(raw_record.get("name_ref"), field_name="record.name_ref"),
            "fields": fields,
            "lineage": {
                **dict(raw_record.get("lineage") or {}),
                "authority_domain_ref": "authority.synthetic_environment",
                "source_dataset_ref": dataset_ref,
                "truth_state": "synthetic_environment",
                "object_truth_promotion_allowed": False,
            },
            "effect_refs": [],
        }
    ordered_refs = sorted(record_map, key=lambda ref: (record_map[ref]["object_kind"], record_map[ref]["ordinal"], ref))
    return {
        "schema_version": SYNTHETIC_ENVIRONMENT_SCHEMA_VERSION,
        "source_dataset_ref": dataset_ref,
        "records": {record_ref: record_map[record_ref] for record_ref in ordered_refs},
        "record_order": ordered_refs,
    }


def _current_records(environment: Mapping[str, Any]) -> dict[str, Any]:
    current_state = environment.get("current_state") or {}
    if not isinstance(current_state, Mapping):
        return {}
    records = current_state.get("records") or {}
    return dict(records) if isinstance(records, Mapping) else {}


def _seed_records(environment: Mapping[str, Any]) -> dict[str, Any]:
    seed_state = environment.get("seed_state") or {}
    if not isinstance(seed_state, Mapping):
        return {}
    records = seed_state.get("records") or {}
    return dict(records) if isinstance(records, Mapping) else {}


def _with_state_metrics(environment: dict[str, Any]) -> dict[str, Any]:
    seed_records = _seed_records(environment)
    current_records = _current_records(environment)
    environment["seed_state_digest"] = _state_digest(environment["seed_state"])
    environment["current_state_digest"] = _state_digest(environment["current_state"])
    environment["record_count"] = len(seed_records)
    environment["current_record_count"] = len(current_records)
    environment["dirty_record_count"] = _diff_counts(seed_records, current_records)["dirty_record_count"]
    return environment


def _diff_counts(before_records: Mapping[str, Any], after_records: Mapping[str, Any]) -> dict[str, int]:
    before_refs = set(before_records)
    after_refs = set(after_records)
    changed = sum(
        1
        for ref in sorted(before_refs & after_refs)
        if _canonical_json(before_records[ref]) != _canonical_json(after_records[ref])
    )
    added = len(after_refs - before_refs)
    removed = len(before_refs - after_refs)
    return {
        "records_added": added,
        "records_removed": removed,
        "records_changed": changed,
        "records_unchanged": len(before_refs & after_refs) - changed,
        "dirty_record_count": added + removed + changed,
    }


def _changed_fields(before_record: Mapping[str, Any], after_record: Mapping[str, Any]) -> list[str]:
    before_fields = before_record.get("fields") or {}
    after_fields = after_record.get("fields") or {}
    keys = sorted(set(before_fields) | set(after_fields))
    changed = [key for key in keys if before_fields.get(key) != after_fields.get(key)]
    if before_record.get("display_name") != after_record.get("display_name"):
        changed.append("display_name")
    return changed


def diff_synthetic_environment(
    environment: Mapping[str, Any],
    *,
    compare_to: str = "seed",
    limit: int = 50,
) -> dict[str, Any]:
    """Return a compact diff between current state and the requested base."""

    if compare_to != "seed":
        raise SyntheticEnvironmentError(
            "synthetic_environment.unsupported_diff_base",
            "only compare_to='seed' is supported",
            details={"compare_to": compare_to},
        )
    seed_records = _seed_records(environment)
    current_records = _current_records(environment)
    counts = _diff_counts(seed_records, current_records)
    removed = [
        {"record_ref": ref, "object_kind": seed_records[ref].get("object_kind")}
        for ref in sorted(set(seed_records) - set(current_records))[:limit]
    ]
    added = [
        {"record_ref": ref, "object_kind": current_records[ref].get("object_kind")}
        for ref in sorted(set(current_records) - set(seed_records))[:limit]
    ]
    changed: list[dict[str, Any]] = []
    for ref in sorted(set(seed_records) & set(current_records)):
        before_record = seed_records[ref]
        after_record = current_records[ref]
        if _canonical_json(before_record) == _canonical_json(after_record):
            continue
        changed.append(
            {
                "record_ref": ref,
                "object_kind": after_record.get("object_kind"),
                "display_name": after_record.get("display_name"),
                "changed_fields": _changed_fields(before_record, after_record),
            }
        )
        if len(changed) >= limit:
            break
    return {
        "environment_ref": environment.get("environment_ref"),
        "compare_to": compare_to,
        "before_state_digest": environment.get("seed_state_digest"),
        "after_state_digest": environment.get("current_state_digest"),
        **counts,
        "records_added_preview": added,
        "records_removed_preview": removed,
        "records_changed_preview": changed,
        "limit": limit,
    }


def create_synthetic_environment_from_dataset(
    *,
    dataset: Mapping[str, Any],
    namespace: str | None = None,
    environment_ref: str | None = None,
    seed: str | None = None,
    clock_time: object | None = None,
    metadata: Mapping[str, Any] | None = None,
    observed_by_ref: str | None = None,
    source_ref: str | None = None,
    sequence_number: int = 1,
    actor_ref: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Create a mutable environment seeded from one Synthetic Data dataset."""

    dataset_ref = _clean_text(dataset.get("dataset_ref"), field_name="dataset.dataset_ref")
    clean_namespace = _clean_text(namespace or dataset.get("namespace") or "default", field_name="namespace")
    clean_seed = _clean_text(seed or f"{dataset_ref}:environment", field_name="seed")
    seed_state = _record_state_from_dataset(dataset)
    ref = _clean_optional_text(environment_ref) or (
        f"synthetic_environment:{_slug(clean_namespace)}:{_digest([dataset_ref, clean_seed, seed_state], length=20)}"
    )
    now = _utc_now()
    environment = {
        "environment_ref": ref,
        "namespace": clean_namespace,
        "source_dataset_ref": dataset_ref,
        "seed": clean_seed,
        "lifecycle_state": "active",
        "clock_time": _normalize_clock_time(clock_time),
        "seed_state": seed_state,
        "current_state": deepcopy(seed_state),
        "metadata": {
            **dict(metadata or {}),
            "source_quality_state": dataset.get("quality_state"),
            "source_quality_score": float(dataset.get("quality_score") or 0.0),
        },
        "permissions": {
            "live_writes_allowed": False,
            "object_truth_promotion_allowed": False,
            "clear_allowed": True,
            "reset_allowed": True,
            "outside_event_injection_allowed": True,
        },
        "observed_by_ref": _clean_optional_text(observed_by_ref),
        "source_ref": _clean_optional_text(source_ref),
        "created_at": now,
        "updated_at": now,
    }
    _with_state_metrics(environment)
    effect = _build_effect(
        environment=environment,
        sequence_number=sequence_number,
        action="create",
        effect_type="environment.created",
        actor_ref=actor_ref,
        target_refs=[],
        before_digest=None,
        after_digest=environment["current_state_digest"],
        changed_fields={},
        effect_payload={
            "source_dataset_ref": dataset_ref,
            "record_count": environment["record_count"],
            "clock_time": environment["clock_time"],
        },
        reversible=False,
    )
    return environment, effect


def clear_synthetic_environment(
    environment: Mapping[str, Any],
    *,
    reason: str | None = None,
    sequence_number: int,
    actor_ref: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Clear current records while preserving seed state and history."""

    updated = deepcopy(dict(environment))
    before_digest = str(updated["current_state_digest"])
    before_records = _current_records(updated)
    current_state = deepcopy(updated["current_state"])
    current_state["records"] = {}
    current_state["record_order"] = []
    updated["current_state"] = current_state
    updated["lifecycle_state"] = "cleared"
    updated["updated_at"] = _utc_now()
    _with_state_metrics(updated)
    effect = _build_effect(
        environment=updated,
        sequence_number=sequence_number,
        action="clear",
        effect_type="environment.cleared",
        actor_ref=actor_ref,
        target_refs=sorted(before_records),
        before_digest=before_digest,
        after_digest=updated["current_state_digest"],
        changed_fields={ref: ["record_removed"] for ref in sorted(before_records)},
        effect_payload={"reason": reason or "operator_requested_clear"},
        reversible=True,
    )
    return updated, effect


def reset_synthetic_environment(
    environment: Mapping[str, Any],
    *,
    reason: str | None = None,
    sequence_number: int,
    actor_ref: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Reset current records back to the seed state."""

    updated = deepcopy(dict(environment))
    before_digest = str(updated["current_state_digest"])
    before_records = _current_records(updated)
    updated["current_state"] = deepcopy(updated["seed_state"])
    updated["lifecycle_state"] = "active"
    updated["updated_at"] = _utc_now()
    _with_state_metrics(updated)
    after_records = _current_records(updated)
    counts = _diff_counts(before_records, after_records)
    effect = _build_effect(
        environment=updated,
        sequence_number=sequence_number,
        action="reset",
        effect_type="environment.reset",
        actor_ref=actor_ref,
        target_refs=sorted(set(before_records) | set(after_records)),
        before_digest=before_digest,
        after_digest=updated["current_state_digest"],
        changed_fields={"summary": sorted(key for key, value in counts.items() if value)},
        effect_payload={"reason": reason or "operator_requested_reset", **counts},
        reversible=False,
    )
    return updated, effect


def inject_synthetic_environment_event(
    environment: Mapping[str, Any],
    *,
    event_type: str,
    event_payload: Mapping[str, Any] | None = None,
    target_refs: Sequence[str] | None = None,
    occurred_at: object | None = None,
    event_ref: str | None = None,
    sequence_number: int,
    actor_ref: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Inject an outside event and persist the resulting deterministic mutation."""

    updated = deepcopy(dict(environment))
    if updated.get("lifecycle_state") != "active":
        raise SyntheticEnvironmentError(
            "synthetic_environment.not_active",
            "outside events can only mutate an active Synthetic Environment",
            details={"environment_ref": updated.get("environment_ref"), "lifecycle_state": updated.get("lifecycle_state")},
        )
    clean_event_type = _clean_text(event_type, field_name="event_type")
    payload = _clean_mapping(event_payload, field_name="event_payload")
    current_records = _current_records(updated)
    if not current_records:
        raise SyntheticEnvironmentError(
            "synthetic_environment.no_current_records",
            "cannot inject outside event into an empty environment",
            details={"environment_ref": updated.get("environment_ref")},
        )
    targets = _select_targets(
        current_records=current_records,
        requested_refs=target_refs,
        event_type=clean_event_type,
        event_payload=payload,
        state_digest=str(updated.get("current_state_digest")),
    )
    timestamp = _normalize_clock_time(occurred_at or updated.get("clock_time"))
    ref = _clean_optional_text(event_ref) or (
        f"synthetic_environment_event:{_slug(updated['environment_ref'])}:{_digest([clean_event_type, payload, targets, timestamp], length=16)}"
    )
    before_digest = str(updated["current_state_digest"])
    before_records = deepcopy(current_records)
    changed_fields: dict[str, list[str]] = {}
    for record_ref in targets:
        record = deepcopy(current_records[record_ref])
        record["fields"] = _mutated_fields_for_event(
            fields=dict(record.get("fields") or {}),
            event_type=clean_event_type,
            event_ref=ref,
            event_payload=payload,
            occurred_at=timestamp,
        )
        record["lineage"] = {
            **dict(record.get("lineage") or {}),
            "last_synthetic_environment_event_ref": ref,
            "last_synthetic_environment_effect_sequence": sequence_number,
        }
        current_records[record_ref] = record
        changed_fields[record_ref] = _changed_fields(before_records[record_ref], record)
    updated["current_state"] = {
        **dict(updated["current_state"]),
        "records": current_records,
        "record_order": [ref for ref in updated["current_state"].get("record_order", []) if ref in current_records],
    }
    updated["clock_time"] = max(timestamp, _normalize_clock_time(updated.get("clock_time")))
    updated["updated_at"] = _utc_now()
    _with_state_metrics(updated)
    effect = _build_effect(
        environment=updated,
        sequence_number=sequence_number,
        action="event_inject",
        effect_type="environment.event_injected",
        actor_ref=actor_ref,
        target_refs=targets,
        before_digest=before_digest,
        after_digest=updated["current_state_digest"],
        changed_fields=changed_fields,
        effect_payload={
            "event_ref": ref,
            "event_type": clean_event_type,
            "event_payload": payload,
            "occurred_at": timestamp,
            "payload_digest": _state_digest(payload),
        },
        event_ref=ref,
        reversible=True,
    )
    return updated, effect


def advance_synthetic_environment_clock(
    environment: Mapping[str, Any],
    *,
    seconds: int | None = None,
    set_time: object | None = None,
    reason: str | None = None,
    sequence_number: int,
    actor_ref: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Advance or set the environment clock through a recorded effect."""

    updated = deepcopy(dict(environment))
    before_digest = str(updated["current_state_digest"])
    before_clock = _normalize_clock_time(updated.get("clock_time"))
    after_clock = _advance_clock_time(before_clock, seconds=seconds, set_time=set_time)
    updated["clock_time"] = after_clock
    updated["updated_at"] = _utc_now()
    _with_state_metrics(updated)
    effect = _build_effect(
        environment=updated,
        sequence_number=sequence_number,
        action="clock_advance",
        effect_type="environment.clock_advanced",
        actor_ref=actor_ref,
        target_refs=[],
        before_digest=before_digest,
        after_digest=updated["current_state_digest"],
        changed_fields={},
        effect_payload={
            "reason": reason or "operator_requested_clock_advance",
            "before_clock_time": before_clock,
            "after_clock_time": after_clock,
            "seconds": seconds,
            "set_time": _clean_optional_text(set_time) if isinstance(set_time, str) else None,
        },
        reversible=True,
    )
    return updated, effect


def _select_targets(
    *,
    current_records: Mapping[str, Any],
    requested_refs: Sequence[str] | None,
    event_type: str,
    event_payload: Mapping[str, Any],
    state_digest: str,
) -> list[str]:
    requested = _clean_string_list(requested_refs, field_name="target_refs")
    if requested:
        missing = [ref for ref in requested if ref not in current_records]
        if missing:
            raise SyntheticEnvironmentError(
                "synthetic_environment.target_not_found",
                "outside event targets must exist in current environment state",
                details={"missing_target_refs": missing[:20], "missing_count": len(missing)},
            )
        return sorted(dict.fromkeys(requested))
    refs = sorted(current_records)
    index = int(_digest([event_type, event_payload, state_digest], length=10), 16) % len(refs)
    return [refs[index]]


def _mutated_fields_for_event(
    *,
    fields: dict[str, Any],
    event_type: str,
    event_ref: str,
    event_payload: Mapping[str, Any],
    occurred_at: str,
) -> dict[str, Any]:
    payload_digest = _state_digest(event_payload)
    event_count = int(fields.get("synthetic_environment_event_count") or 0) + 1
    fields.update(
        {
            "synthetic_environment_event_count": event_count,
            "last_external_event_ref": event_ref,
            "last_external_event_type": event_type,
            "last_external_event_at": occurred_at,
            "last_external_event_payload_digest": payload_digest,
        }
    )
    if event_type == "crm.owner_changed":
        fields["owner_ref"] = event_payload.get("owner_ref") or event_payload.get("external_owner_ref") or "synthetic_owner:unassigned"
        fields["owner_change_reason"] = event_payload.get("reason") or "outside_event"
    elif event_type == "payment.failed":
        fields["payment_status"] = "failed"
        fields["risk_state"] = "at_risk"
        fields["failure_reason"] = event_payload.get("failure_reason") or event_payload.get("reason") or "outside_event"
    elif event_type == "ticket.escalated":
        fields["priority"] = event_payload.get("priority") or "critical"
        fields["escalated"] = True
        fields["escalated_at"] = occurred_at
    elif event_type == "webhook.received":
        fields["last_webhook_topic"] = event_payload.get("topic") or event_payload.get("event") or "unknown"
        fields["webhook_status"] = event_payload.get("status") or "received"
    elif event_type == "identity.merged":
        fields["identity_state"] = "merged"
        fields["merged_into_ref"] = event_payload.get("merged_into_ref") or event_payload.get("winner_ref")
    else:
        fields["generic_outside_event_seen"] = True
    return fields


def _build_effect(
    *,
    environment: Mapping[str, Any],
    sequence_number: int,
    action: str,
    effect_type: str,
    actor_ref: str | None,
    target_refs: Sequence[str],
    before_digest: str | None,
    after_digest: str,
    changed_fields: Mapping[str, Any],
    effect_payload: Mapping[str, Any],
    event_ref: str | None = None,
    reversible: bool,
) -> dict[str, Any]:
    env_ref = _clean_text(environment.get("environment_ref"), field_name="environment.environment_ref")
    sequence = int(sequence_number)
    if sequence < 1:
        raise SyntheticEnvironmentError(
            "synthetic_environment.invalid_sequence",
            "effect sequence_number must be positive",
            details={"sequence_number": sequence_number},
        )
    targets = list(target_refs)
    changed_record_count = len(targets)
    basis = {
        "environment_ref": env_ref,
        "sequence_number": sequence,
        "action": action,
        "effect_type": effect_type,
        "event_ref": event_ref,
        "target_refs": targets,
        "before_state_digest": before_digest,
        "after_state_digest": after_digest,
        "effect_payload": dict(effect_payload),
    }
    return {
        "effect_ref": f"synthetic_environment_effect:{_slug(env_ref)}:{sequence}:{_digest(basis, length=18)}",
        "environment_ref": env_ref,
        "sequence_number": sequence,
        "effect_type": effect_type,
        "action": action,
        "event_ref": event_ref,
        "actor_ref": _clean_optional_text(actor_ref) or "operator",
        "target_refs": targets,
        "before_state_digest": before_digest,
        "after_state_digest": after_digest,
        "changed_record_count": changed_record_count,
        "changed_fields": dict(changed_fields),
        "reversible": bool(reversible),
        "effect": dict(effect_payload),
        "created_at": _utc_now(),
    }


__all__ = [
    "MAX_ENVIRONMENT_RECORDS",
    "SyntheticEnvironmentError",
    "advance_synthetic_environment_clock",
    "clear_synthetic_environment",
    "create_synthetic_environment_from_dataset",
    "diff_synthetic_environment",
    "inject_synthetic_environment_event",
    "reset_synthetic_environment",
]
