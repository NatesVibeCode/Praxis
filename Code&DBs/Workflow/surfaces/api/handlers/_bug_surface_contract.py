"""Shared request normalization and response shaping for bug surfaces."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from typing import Any

from runtime import bug_evidence as _bug_evidence


BugSerializer = Callable[[Any], dict[str, Any]]
Serializer = Callable[[Any], Any]
BugParser = Callable[[Any, object], Any]


def parse_bug_status(bt_mod: Any, raw_status: object) -> Any:
    if raw_status is None:
        return None
    tracker_cls = getattr(bt_mod, "BugTracker", None)
    normalizer = getattr(tracker_cls, "_normalize_status", None)
    if callable(normalizer):
        status = normalizer(raw_status, default=None)
    else:
        status = getattr(getattr(bt_mod, "BugStatus", None), str(raw_status).strip().upper(), None)
    if status is None:
        raise ValueError("status must be one of OPEN, IN_PROGRESS, FIXED, WONT_FIX, DEFERRED")
    return status


def parse_bug_severity(bt_mod: Any, raw_severity: object) -> Any:
    if raw_severity is None:
        return None
    tracker_cls = getattr(bt_mod, "BugTracker", None)
    normalizer = getattr(tracker_cls, "_normalize_severity", None)
    if callable(normalizer):
        severity = normalizer(raw_severity, default=None)
    else:
        severity = getattr(getattr(bt_mod, "BugSeverity", None), str(raw_severity).strip().upper(), None)
    if severity is None:
        raise ValueError("severity must be one of P0, P1, P2, P3")
    return severity


def parse_bug_category(bt_mod: Any, raw_category: object) -> Any:
    if raw_category is None:
        return None
    tracker_cls = getattr(bt_mod, "BugTracker", None)
    normalizer = getattr(tracker_cls, "_normalize_category", None)
    if callable(normalizer):
        category = normalizer(raw_category, default=None)
    else:
        category = getattr(getattr(bt_mod, "BugCategory", None), str(raw_category).strip().upper(), None)
    if category is None:
        raise ValueError(
            "category must be one of SCOPE, VERIFY, IMPORT, WIRING, ARCHITECTURE, RUNTIME, TEST, OTHER"
        )
    return category


def _normalize_tags(raw_tags: object) -> tuple[str, ...] | None:
    if raw_tags is None:
        return None
    if isinstance(raw_tags, str):
        tags = tuple(tag.strip() for tag in raw_tags.split(",") if tag.strip())
        return tags or None
    if isinstance(raw_tags, Sequence) and not isinstance(raw_tags, (str, bytes, bytearray)):
        tags = tuple(str(tag).strip() for tag in raw_tags if str(tag).strip())
        return tags or None
    return None


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _source_issue_filter_kwargs(body: Mapping[str, Any]) -> dict[str, Any]:
    source_issue_id = _optional_text(body.get("source_issue_id"))
    return {"source_issue_id": source_issue_id} if source_issue_id is not None else {}


def _optional_object(value: object, *, field_name: str) -> dict[str, Any]:
    if value in (None, ""):
        return {}
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a JSON object")
    return dict(value)


def _path_like_target_ref(inputs: Mapping[str, Any]) -> str | None:
    for key in ("path", "file", "module", "target"):
        value = _optional_text(inputs.get(key))
        if value:
            return value
    return None


def annotate_bug_dicts_with_replay_state(
    bt: Any,
    bugs: Sequence[Any],
    *,
    serialize_bug: BugSerializer,
    replay_ready_only: bool = False,
    include_replay_details: bool = True,
    receipt_limit: int = 1,
    limit: int = 50,
) -> list[dict[str, Any]]:
    annotated: list[dict[str, Any]] = []
    for bug in bugs:
        bug_dict = dict(serialize_bug(bug))
        replay_state = _bug_evidence.replay_state_from_hint(
            bt.replay_hint(
                bug.bug_id,
                receipt_limit=receipt_limit,
                allow_backfill=False,
            )
        )
        bug_dict["replay_ready"] = replay_state["replay_ready"]
        if include_replay_details:
            bug_dict.update(replay_state)
        if replay_ready_only and not bug_dict["replay_ready"]:
            continue
        annotated.append(bug_dict)
        if len(annotated) >= limit:
            break
    return annotated


def list_bugs_payload(
    *,
    bt: Any,
    bt_mod: Any,
    body: Mapping[str, Any],
    serialize_bug: BugSerializer,
    default_limit: int,
    include_replay_details: bool,
    parse_status: BugParser = parse_bug_status,
    parse_severity: BugParser = parse_bug_severity,
    parse_category: BugParser = parse_bug_category,
) -> dict[str, Any]:
    parsed_status = parse_status(bt_mod, body.get("status"))
    parsed_severity = parse_severity(bt_mod, body.get("severity"))
    category = parse_category(bt_mod, body.get("category"))
    limit = max(1, int(body.get("limit", default_limit) or default_limit))
    title_like = body.get("title_like")
    include_replay_state = bool(body.get("include_replay_state", False))
    replay_ready_only = bool(body.get("replay_ready_only", False))
    open_only = bool(body.get("open_only", False))
    tags = _normalize_tags(body.get("tags"))
    exclude_tags = _normalize_tags(body.get("exclude_tags"))

    filter_kwargs = _source_issue_filter_kwargs(body)
    total_count = bt.count_bugs(
        status=parsed_status,
        severity=parsed_severity,
        category=category,
        title_like=title_like if isinstance(title_like, str) else None,
        tags=tags,
        exclude_tags=exclude_tags,
        open_only=open_only,
        **filter_kwargs,
    )
    bugs = bt.list_bugs(
        status=parsed_status,
        severity=parsed_severity,
        category=category,
        title_like=title_like if isinstance(title_like, str) else None,
        tags=tags,
        exclude_tags=exclude_tags,
        open_only=open_only,
        limit=max(total_count, limit) if replay_ready_only else limit,
        **filter_kwargs,
    )
    if include_replay_state or replay_ready_only:
        bug_dicts = annotate_bug_dicts_with_replay_state(
            bt,
            bugs,
            serialize_bug=serialize_bug,
            replay_ready_only=replay_ready_only,
            include_replay_details=include_replay_details,
            limit=limit,
        )
    else:
        bug_dicts = [serialize_bug(bug) for bug in bugs[:limit]]
    return {
        "bugs": bug_dicts[:limit],
        "count": len(bug_dicts) if replay_ready_only else total_count,
        "returned_count": len(bug_dicts[:limit]),
    }


def file_bug_payload(
    *,
    bt: Any,
    bt_mod: Any,
    body: Mapping[str, Any],
    serialize_bug: BugSerializer,
    filed_by_default: str,
    source_kind_default: str,
    include_similar_bugs: bool = False,
    parse_severity: BugParser = parse_bug_severity,
    parse_category: BugParser = parse_bug_category,
) -> dict[str, Any]:
    title = str(body.get("title") or "").strip()
    if not title:
        raise ValueError("title is required to file a bug")
    category = parse_category(bt_mod, body.get("category")) or bt_mod.BugCategory.OTHER
    resume_ctx = body.get("resume_context")
    if resume_ctx is not None and not isinstance(resume_ctx, dict):
        raise ValueError("resume_context must be a JSON object when provided")
    filed = bt.file_bug(
        title=title,
        severity=parse_severity(bt_mod, body.get("severity")) or bt_mod.BugSeverity.P2,
        category=category,
        description=str(body.get("description") or ""),
        filed_by=str(body.get("filed_by") or filed_by_default).strip() or filed_by_default,
        source_kind=str(body.get("source_kind") or source_kind_default).strip() or source_kind_default,
        decision_ref=str(body.get("decision_ref") or "").strip(),
        discovered_in_run_id=_optional_text(body.get("discovered_in_run_id")),
        discovered_in_receipt_id=_optional_text(body.get("discovered_in_receipt_id")),
        owner_ref=_optional_text(body.get("owner_ref")),
        source_issue_id=_optional_text(body.get("source_issue_id")),
        tags=_normalize_tags(body.get("tags")) or (),
        resume_context=resume_ctx if isinstance(resume_ctx, dict) else None,
    )
    bug = filed[0] if isinstance(filed, tuple) else filed
    payload: dict[str, Any] = {"filed": True, "bug": serialize_bug(bug)}
    if include_similar_bugs and isinstance(filed, tuple) and len(filed) > 1 and filed[1]:
        payload["similar_bugs"] = filed[1]
    return payload


def search_bugs_payload(
    *,
    bt: Any,
    bt_mod: Any,
    body: Mapping[str, Any],
    serialize_bug: BugSerializer,
    default_limit: int,
    parse_status: BugParser = parse_bug_status,
    parse_severity: BugParser = parse_bug_severity,
    parse_category: BugParser = parse_bug_category,
) -> dict[str, Any]:
    title = str(body.get("title") or "").strip()
    if not title:
        raise ValueError("title is required for search")
    limit = max(1, int(body.get("limit", default_limit) or default_limit))
    include_replay_state = bool(body.get("include_replay_state", False))
    filter_kwargs = _source_issue_filter_kwargs(body)
    bugs = bt.search(
        title,
        limit=limit,
        status=parse_status(bt_mod, body.get("status")),
        severity=parse_severity(bt_mod, body.get("severity")),
        category=parse_category(bt_mod, body.get("category")),
        tags=_normalize_tags(body.get("tags")),
        exclude_tags=_normalize_tags(body.get("exclude_tags")),
        open_only=bool(body.get("open_only", False)),
        **filter_kwargs,
    )
    if include_replay_state:
        bug_dicts = annotate_bug_dicts_with_replay_state(
            bt,
            bugs,
            serialize_bug=serialize_bug,
            include_replay_details=True,
            limit=limit,
        )
    else:
        bug_dicts = [serialize_bug(bug) for bug in bugs]
    return {"bugs": bug_dicts, "count": len(bug_dicts)}


def stats_payload(*, bt: Any, serialize: Serializer) -> dict[str, Any]:
    return {"stats": serialize(bt.stats())}


def packet_payload(
    *,
    bt: Any,
    body: Mapping[str, Any],
    serialize: Serializer,
) -> dict[str, Any]:
    bug_id = str(body.get("bug_id") or "").strip()
    if not bug_id:
        raise ValueError("bug_id is required to build a failure packet")
    packet = bt.failure_packet(
        bug_id,
        receipt_limit=max(1, int(body.get("receipt_limit", 5) or 5)),
    )
    if packet is None:
        raise ValueError(f"bug not found: {bug_id}")
    return {"packet": serialize(packet, strip_empty=True)}


def history_payload(
    *,
    bt: Any,
    body: Mapping[str, Any],
    serialize: Serializer,
) -> dict[str, Any]:
    bug_id = str(body.get("bug_id") or "").strip()
    if not bug_id:
        raise ValueError("bug_id is required to read bug history")
    packet = bt.failure_packet(
        bug_id,
        receipt_limit=max(1, int(body.get("receipt_limit", 5) or 5)),
    )
    if packet is None:
        raise ValueError(f"bug not found: {bug_id}")
    return {"history": serialize(_bug_evidence.history_summary(bug_id=bug_id, packet=packet), strip_empty=True)}


def replay_payload(
    *,
    bt: Any,
    body: Mapping[str, Any],
    serialize: Serializer,
) -> dict[str, Any]:
    bug_id = str(body.get("bug_id") or "").strip()
    if not bug_id:
        raise ValueError("bug_id is required to replay a bug")
    replay = bt.replay_bug(
        bug_id,
        receipt_limit=max(1, int(body.get("receipt_limit", 5) or 5)),
    )
    if replay is None:
        raise ValueError(f"bug not found: {bug_id}")
    return {"replay": serialize(replay)}


def backfill_replay_payload(
    *,
    bt: Any,
    body: Mapping[str, Any],
    serialize: Serializer,
) -> dict[str, Any]:
    limit_raw = body.get("limit")
    limit = None if limit_raw in (None, "") else max(0, int(limit_raw))
    return {
        "backfill": serialize(
            bt.bulk_backfill_replay_provenance(
                limit=limit,
                open_only=bool(body.get("open_only", True)),
                receipt_limit=max(1, int(body.get("receipt_limit", 1) or 1)),
            )
        )
    }


def attach_evidence_payload(
    *,
    bt: Any,
    body: Mapping[str, Any],
    serialize: Serializer,
    created_by_default: str,
) -> dict[str, Any]:
    bug_id = str(body.get("bug_id") or "").strip()
    evidence_kind = str(body.get("evidence_kind") or "").strip()
    evidence_ref = str(body.get("evidence_ref") or "").strip()
    evidence_role = str(body.get("evidence_role") or "observed_in").strip() or "observed_in"
    if not bug_id:
        raise ValueError("bug_id is required to attach bug evidence")
    if not evidence_kind:
        raise ValueError("evidence_kind is required to attach bug evidence")
    if not evidence_ref:
        raise ValueError("evidence_ref is required to attach bug evidence")
    link = bt.link_evidence(
        bug_id,
        evidence_kind=evidence_kind,
        evidence_ref=evidence_ref,
        evidence_role=evidence_role,
        created_by=str(body.get("created_by") or created_by_default).strip() or created_by_default,
        notes=_optional_text(body.get("notes")),
    )
    if link is None:
        raise ValueError("failed to attach bug evidence")
    return {"attached": True, "evidence_link": serialize(link)}


def resolve_bug_payload(
    *,
    bt: Any,
    bt_mod: Any,
    body: Mapping[str, Any],
    serialize_bug: BugSerializer,
    serialize: Serializer = lambda value, **_kwargs: value,
    resolved_statuses: set[Any],
    parse_status: BugParser = parse_bug_status,
    created_by_default: str = "bug_surface.resolve",
    run_registered_verifier: Callable[..., dict[str, Any]] | None = None,
) -> dict[str, Any]:
    bug_id = str(body.get("bug_id") or "").strip()
    if not bug_id:
        raise ValueError("bug_id is required to resolve a bug")
    status = parse_status(bt_mod, body.get("status"))
    if status is None:
        raise ValueError("status is required to resolve a bug")
    if status not in resolved_statuses:
        allowed = ", ".join(sorted(item.value for item in resolved_statuses))
        raise ValueError(f"resolve status must be one of {allowed}")
    verifier_ref = _optional_text(body.get("verifier_ref"))
    bug_getter = getattr(bt, "get", None)
    if callable(bug_getter) and bug_getter(bug_id) is None:
        raise ValueError(f"bug not found: {bug_id}")
    if verifier_ref and status != getattr(bt_mod.BugStatus, "FIXED", None):
        raise ValueError("verifier_ref may only be used when resolving status FIXED")
    verification_payload: dict[str, Any] | None = None
    evidence_link: dict[str, Any] | None = None
    if verifier_ref:
        verify_inputs = _optional_object(
            body.get("inputs"),
            field_name="inputs",
        )
        verifier_conn = getattr(bt, "_conn", None)
        inferred_target_ref = _path_like_target_ref(verify_inputs)
        target_kind = _optional_text(body.get("target_kind")) or (
            "path" if inferred_target_ref else "platform"
        )
        target_ref = _optional_text(body.get("target_ref")) or inferred_target_ref or bug_id
        if run_registered_verifier is None:
            from runtime.verifier_authority import run_registered_verifier as _run_registered_verifier

            run_registered_verifier = _run_registered_verifier
        verification_payload = run_registered_verifier(
            verifier_ref,
            inputs=verify_inputs,
            target_kind=target_kind,
            target_ref=target_ref,
            conn=verifier_conn,
            promote_bug=False,
        )
        verification_status = str(verification_payload.get("status") or "").strip()
        verification_run_id = str(verification_payload.get("verification_run_id") or "").strip()
        if verification_status != "passed":
            parts = [
                f"verifier {verifier_ref} did not pass for {bug_id}",
                f"status={verification_status or 'unknown'}",
            ]
            if verification_run_id:
                parts.append(f"verification_run_id={verification_run_id}")
            raise ValueError("; ".join(parts))
        if not verification_run_id:
            raise ValueError(
                f"verifier {verifier_ref} passed for {bug_id} but did not record a verification_run_id"
            )
        evidence_link = bt.link_evidence(
            bug_id,
            evidence_kind="verification_run",
            evidence_ref=verification_run_id,
            evidence_role="validates_fix",
            created_by=str(body.get("created_by") or created_by_default).strip() or created_by_default,
            notes=_optional_text(body.get("notes"))
            or f"Passed verifier {verifier_ref} during FIXED resolution.",
        )
        if evidence_link is None:
            raise ValueError(
                f"failed to attach validates_fix verification evidence for {bug_id}"
            )
    bug = bt.resolve(bug_id, status)
    if bug is None:
        raise ValueError(f"bug not found: {bug_id}")
    payload = {"resolved": True, "bug": serialize_bug(bug)}
    if verification_payload is not None:
        payload["verification"] = serialize(verification_payload, strip_empty=True)
    if evidence_link is not None:
        payload["evidence_link"] = serialize(evidence_link, strip_empty=True)
    return payload


def patch_resume_payload(
    *,
    bt: Any,
    body: Mapping[str, Any],
    serialize_bug: BugSerializer,
) -> dict[str, Any]:
    bug_id = str(body.get("bug_id") or "").strip()
    if not bug_id:
        raise ValueError("bug_id is required to patch resume_context")
    raw_patch = body.get("resume_patch")
    if raw_patch is None:
        raw_patch = body.get("patch")
    if not isinstance(raw_patch, dict):
        raise ValueError("resume_patch must be a JSON object")
    bug = bt.merge_resume_context(bug_id, raw_patch)
    if bug is None:
        raise ValueError(f"bug not found: {bug_id}")
    return {"updated": True, "bug": serialize_bug(bug)}


__all__ = [
    "annotate_bug_dicts_with_replay_state",
    "attach_evidence_payload",
    "backfill_replay_payload",
    "file_bug_payload",
    "history_payload",
    "list_bugs_payload",
    "packet_payload",
    "parse_bug_category",
    "parse_bug_severity",
    "parse_bug_status",
    "patch_resume_payload",
    "replay_payload",
    "resolve_bug_payload",
    "search_bugs_payload",
    "stats_payload",
]
