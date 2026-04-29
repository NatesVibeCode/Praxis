"""Tools: praxis_dataset — refinery candidates, scores, promotions, exports."""

from __future__ import annotations

import asyncio
from runtime.async_bridge import run_sync_safe
from typing import Any

from runtime.dataset_candidate_subscriber import (
    aconsume_dataset_candidate_events,
    aingest_receipts_backfill,
)
from runtime.dataset_curation_projection_subscriber import (
    aconsume_dataset_curation_events,
)
from runtime.dataset_exporter import (
    DatasetExportError,
    aexport_dataset,
)
from runtime.dataset_staleness import areconcile_dataset_staleness
from surfaces.api.dataset_read import (
    CandidateFilter,
    afetch_lineage,
    ainspect_candidate,
    ainspect_policy,
    alist_candidates,
    alist_export_manifests,
    alist_policies,
    alist_promotions,
    asummarize_refinery,
    asuggest_preference_pairs,
    alist_manual_review_inbox,
)
from surfaces.api.operator_write import (
    arecord_dataset_policy,
    arecord_dataset_promotion,
    arecord_dataset_rejection,
    asupersede_dataset_promotion,
)


def _run(coro: Any) -> Any:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return run_sync_safe(coro)
    raise RuntimeError("praxis_dataset must be called from a non-async boundary")


def _str(value: Any, default: str = "") -> str:
    return str(value).strip() if isinstance(value, (str, int, float)) and str(value).strip() else default


def _opt_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _list_of_str(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [str(v) for v in value if str(v).strip()]
    return []


def tool_praxis_dataset(params: dict[str, Any]) -> dict[str, Any]:
    """Single-tool refinery surface: candidates, policies, promotions, export, staleness."""

    action = _str(params.get("action"), default="summary").lower()
    try:
        if action == "summary":
            return _run(asummarize_refinery())

        if action == "candidates_list":
            filters = CandidateFilter(
                candidate_kind=_opt_str(params.get("candidate_kind")),
                route_slug=_opt_str(params.get("route_slug")),
                eligibility=_opt_str(params.get("eligibility")),
                policy_id=_opt_str(params.get("policy_id")),
                redaction_status=_opt_str(params.get("redaction_status")),
                staleness_status=_opt_str(params.get("staleness_status")),
                limit=int(params.get("limit") or 50),
                offset=int(params.get("offset") or 0),
            )
            return _run(alist_candidates(filters=filters))

        if action == "candidate_inspect":
            cid = _str(params.get("candidate_id"))
            if not cid:
                return {"error": "candidate_id is required"}
            return _run(ainspect_candidate(candidate_id=cid))

        if action == "candidates_scan":
            receipt_ids = params.get("receipt_ids")
            since_days = params.get("since_days")
            if params.get("backfill") or receipt_ids or since_days is not None:
                rids = None
                if isinstance(receipt_ids, list):
                    rids = [str(r) for r in receipt_ids if r]
                elif isinstance(receipt_ids, str) and receipt_ids.strip():
                    rids = [r.strip() for r in receipt_ids.split(",") if r.strip()]
                return _run(
                    aingest_receipts_backfill(
                        since_days=int(since_days) if since_days is not None else None,
                        receipt_ids=rids,
                        limit=int(params.get("limit") or 500),
                    )
                )
            return _run(
                aconsume_dataset_candidate_events(limit=int(params.get("limit") or 100))
            )

        if action == "projection_refresh":
            return _run(
                aconsume_dataset_curation_events(limit=int(params.get("limit") or 100))
            )

        if action == "stale_reconcile":
            return _run(
                areconcile_dataset_staleness(
                    reconciled_by=_str(params.get("reconciled_by"), default="praxis_dataset")
                )
            )

        if action == "policy_list":
            return _run(
                alist_policies(
                    specialist_target=_opt_str(params.get("specialist_target")),
                    active_only=bool(params.get("active_only", True)),
                )
            )

        if action == "policy_show":
            ref = _str(params.get("policy_id") or params.get("policy_slug"))
            if not ref:
                return {"error": "policy_id or policy_slug is required"}
            return _run(ainspect_policy(policy_id_or_slug=ref))

        if action == "policy_record":
            return _run(
                arecord_dataset_policy(
                    policy_slug=_str(params["policy_slug"]),
                    specialist_target=_str(params["specialist_target"]),
                    rubric=params["rubric"],
                    decided_by=_str(params["decided_by"]),
                    rationale=_str(params["rationale"]),
                    auto_promote=bool(params.get("auto_promote", False)),
                    supersedes_policy_id=_opt_str(params.get("supersedes_policy_id")),
                )
            )

        if action == "promotions_list":
            return _run(
                alist_promotions(
                    specialist_target=_opt_str(params.get("specialist_target")),
                    dataset_family=_opt_str(params.get("dataset_family")),
                    split_tag=_opt_str(params.get("split_tag")),
                    active_only=bool(params.get("active_only", True)),
                    limit=int(params.get("limit") or 50),
                    offset=int(params.get("offset") or 0),
                )
            )

        if action == "candidate_promote":
            return _run(
                arecord_dataset_promotion(
                    candidate_ids=_list_of_str(params.get("candidate_ids") or params.get("candidate_id")),
                    dataset_family=_str(params.get("dataset_family"), default="sft"),
                    specialist_target=_str(params["specialist_target"]),
                    policy_id=_str(params["policy_id"]),
                    payload=params["payload"],
                    promoted_by=_str(params["promoted_by"]),
                    rationale=_str(params["rationale"]),
                    promotion_kind=_str(params.get("promotion_kind"), default="manual"),
                    split_tag=_opt_str(params.get("split_tag")),
                    decision_ref=_opt_str(params.get("decision_ref")),
                )
            )

        if action == "inbox":
            return _run(
                alist_manual_review_inbox(
                    candidate_kind=_opt_str(params.get("candidate_kind")),
                    specialist_target=_opt_str(params.get("specialist_target")),
                    limit=int(params.get("limit") or 25),
                    offset=int(params.get("offset") or 0),
                )
            )

        if action == "preference_suggest":
            return _run(
                asuggest_preference_pairs(
                    candidate_kind=_opt_str(params.get("candidate_kind")),
                    specialist_target=_opt_str(params.get("specialist_target")),
                    limit=int(params.get("limit") or 20),
                )
            )

        if action == "preference_create":
            chosen = _str(params.get("chosen_candidate_id"))
            rejected = _str(params.get("rejected_candidate_id"))
            if not chosen or not rejected:
                return {"error": "chosen_candidate_id and rejected_candidate_id are required"}
            return _run(
                arecord_dataset_promotion(
                    candidate_ids=[chosen, rejected],
                    dataset_family="preference",
                    specialist_target=_str(params["specialist_target"]),
                    policy_id=_str(params["policy_id"]),
                    payload=params["payload"],
                    promoted_by=_str(params["promoted_by"]),
                    rationale=_str(params["rationale"]),
                    promotion_kind=_str(params.get("promotion_kind"), default="manual"),
                    split_tag=_opt_str(params.get("split_tag")),
                    decision_ref=_opt_str(params.get("decision_ref")),
                )
            )

        if action == "eval_add":
            return _run(
                arecord_dataset_promotion(
                    candidate_ids=_list_of_str(params.get("candidate_ids") or params.get("candidate_id")),
                    dataset_family="eval",
                    specialist_target=_str(params["specialist_target"]),
                    policy_id=_str(params["policy_id"]),
                    payload=params["payload"],
                    promoted_by=_str(params["promoted_by"]),
                    rationale=_str(params["rationale"]),
                    promotion_kind=_str(params.get("promotion_kind"), default="manual"),
                    split_tag=_str(params.get("split_tag"), default="eval"),
                    decision_ref=_opt_str(params.get("decision_ref")),
                )
            )

        if action == "candidate_reject":
            return _run(
                arecord_dataset_rejection(
                    candidate_id=_str(params["candidate_id"]),
                    rejected_by=_str(params["rejected_by"]),
                    reason=_str(params["reason"]),
                )
            )

        if action == "promotion_supersede":
            return _run(
                asupersede_dataset_promotion(
                    promotion_id=_str(params["promotion_id"]),
                    superseded_reason=_str(params["superseded_reason"]),
                    superseded_by_operator=_str(params["superseded_by_operator"]),
                    superseded_by=_opt_str(params.get("superseded_by")),
                )
            )

        if action == "lineage":
            return _run(
                afetch_lineage(
                    promotion_id=_opt_str(params.get("promotion_id")),
                    candidate_id=_opt_str(params.get("candidate_id")),
                    specialist_target=_opt_str(params.get("specialist_target")),
                    limit=int(params.get("limit") or 200),
                )
            )

        if action == "manifests_list":
            return _run(
                alist_export_manifests(
                    specialist_target=_opt_str(params.get("specialist_target")),
                    dataset_family=_opt_str(params.get("dataset_family")),
                    limit=int(params.get("limit") or 50),
                )
            )

        if action == "export":
            return _run(
                aexport_dataset(
                    dataset_family=_str(params["dataset_family"]),
                    specialist_target=_str(params["specialist_target"]),
                    split_tag=_str(params["split_tag"]),
                    output_path=_str(params["output_path"]),
                    exported_by=_str(params["exported_by"]),
                )
            )

        return {"error": f"unknown action {action!r}"}
    except DatasetExportError as exc:
        return {"error": f"dataset_export_error: {exc}", "action": action}
    except KeyError as exc:
        return {"error": f"missing required parameter: {exc.args[0]}", "action": action}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}", "action": action}


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_dataset": (
        tool_praxis_dataset,
        {
            "description": (
                "Praxis dataset refinery: turn evidence-linked execution receipts into curated, "
                "lineage-preserving training and eval data for specialist SLMs (slm/review first).\n\n"
                "USE WHEN: you want to see what high-quality evidence Praxis is producing per route, "
                "promote a receipt into an SFT example, build an eval case for a held-out revision, "
                "or export a JSONL training file with a content-hashed manifest.\n\n"
                "EXAMPLES:\n"
                "  Summary:           praxis_dataset(action='summary')\n"
                "  List candidates:   praxis_dataset(action='candidates_list', candidate_kind='review', eligibility='sft_eligible')\n"
                "  Inspect lineage:   praxis_dataset(action='candidate_inspect', candidate_id='...')\n"
                "  Scan new receipts: praxis_dataset(action='candidates_scan', limit=200)\n"
                "  Record policy:     praxis_dataset(action='policy_record', policy_slug='review.v1', "
                "specialist_target='slm/review', rubric={...}, decided_by='nathan', rationale='initial')\n"
                "  Promote SFT:       praxis_dataset(action='candidate_promote', candidate_ids=['c_...'], "
                "specialist_target='slm/review', policy_id='pol_...', payload={'prompt':{...}, 'target_output':{...}}, "
                "promoted_by='nathan', rationale='verifier+reviewer agree', decision_ref='od_...', split_tag='train')\n"
                "  Add eval case:     praxis_dataset(action='eval_add', candidate_ids=['c_...'], "
                "specialist_target='slm/review', policy_id='pol_...', payload={'case_input':{...}, 'revision_scope':{...}}, "
                "promoted_by='nathan', rationale='held-out gold', decision_ref='od_...')\n"
                "  Export JSONL:      praxis_dataset(action='export', dataset_family='sft', "
                "specialist_target='slm/review', split_tag='train', output_path='artifacts/dataset/review_sft_train.jsonl', "
                "exported_by='nathan')\n"
                "  Reconcile stale:   praxis_dataset(action='stale_reconcile')\n\n"
                "DO NOT USE: as a general-purpose SQL tool — go through the structured actions so writes "
                "stay through operator_write.py and emit the right events."
            ),
            "cli": {
                "surface": "operations",
                "tier": "stable",
                "recommended_alias": "dataset",
                "when_to_use": (
                    "Curate, score, and promote evidence-linked training/eval data per specialist; "
                    "export reproducible JSONL with manifest hashes."
                ),
                "when_not_to_use": (
                    "Do not use for raw SQL or for writing receipts/decisions directly — those have "
                    "their own surfaces."
                ),
                "risks": {
                    "default": "read",
                    "actions": {
                        "summary": "read",
                        "candidates_list": "read",
                        "candidate_inspect": "read",
                        "candidates_scan": "read",
                        "projection_refresh": "read",
                        "policy_list": "read",
                        "policy_show": "read",
                        "lineage": "read",
                        "manifests_list": "read",
                        "promotions_list": "read",
                        "export": "read",
                        "policy_record": "write",
                        "candidate_promote": "write",
                        "candidate_reject": "write",
                        "inbox": "read",
                        "preference_suggest": "read",
                        "preference_create": "write",
                        "eval_add": "write",
                        "promotion_supersede": "write",
                        "stale_reconcile": "write",
                    },
                },
                "examples": [
                    {
                        "title": "List eligible review candidates",
                        "input": {
                            "action": "candidates_list",
                            "candidate_kind": "review",
                            "eligibility": "sft_eligible",
                            "limit": 10,
                        },
                    },
                    {
                        "title": "Inspect one candidate's lineage",
                        "input": {"action": "candidate_inspect", "candidate_id": "c_..."},
                    },
                    {
                        "title": "Export a training split",
                        "input": {
                            "action": "export",
                            "dataset_family": "sft",
                            "specialist_target": "slm/review",
                            "split_tag": "train",
                            "output_path": "artifacts/dataset/review_sft_train.jsonl",
                            "exported_by": "nathan",
                        },
                    },
                ],
            },
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": [
                            "summary",
                            "candidates_scan",
                            "candidates_list",
                            "candidate_inspect",
                            "candidate_promote",
                            "candidate_reject",
                            "inbox",
                            "preference_suggest",
                            "preference_create",
                            "eval_add",
                            "promotion_supersede",
                            "promotions_list",
                            "policy_list",
                            "policy_show",
                            "policy_record",
                            "lineage",
                            "manifests_list",
                            "export",
                            "stale_reconcile",
                            "projection_refresh",
                        ],
                        "default": "summary",
                    },
                    "candidate_id": {"type": "string"},
                    "candidate_ids": {"type": "array", "items": {"type": "string"}},
                    "candidate_kind": {
                        "type": "string",
                        "enum": [
                            "review",
                            "triage",
                            "operator_explain",
                            "route_choice",
                            "repair",
                        ],
                    },
                    "route_slug": {"type": "string"},
                    "specialist_target": {"type": "string"},
                    "policy_id": {"type": "string"},
                    "policy_slug": {"type": "string"},
                    "supersedes_policy_id": {"type": "string"},
                    "decided_by": {"type": "string"},
                    "rationale": {"type": "string"},
                    "rubric": {"type": "object"},
                    "auto_promote": {"type": "boolean"},
                    "eligibility": {
                        "type": "string",
                        "enum": [
                            "rejected",
                            "manual_review",
                            "sft_eligible",
                            "preference_eligible",
                            "eval_eligible",
                            "routing_eligible",
                        ],
                    },
                    "redaction_status": {
                        "type": "string",
                        "enum": [
                            "clean",
                            "unverified",
                            "redaction_required",
                            "sensitive_blocked",
                        ],
                    },
                    "staleness_status": {
                        "type": "string",
                        "enum": ["fresh", "definition_stale", "evidence_stale"],
                    },
                    "dataset_family": {
                        "type": "string",
                        "enum": ["sft", "preference", "eval", "routing"],
                    },
                    "split_tag": {
                        "type": "string",
                        "enum": ["train", "eval", "holdout"],
                    },
                    "promotion_kind": {
                        "type": "string",
                        "enum": ["manual", "auto"],
                    },
                    "promotion_id": {"type": "string"},
                    "promoted_by": {"type": "string"},
                    "rejected_by": {"type": "string"},
                    "reason": {"type": "string"},
                    "decision_ref": {"type": "string"},
                    "superseded_reason": {"type": "string"},
                    "superseded_by_operator": {"type": "string"},
                    "superseded_by": {"type": "string"},
                    "chosen_candidate_id": {"type": "string"},
                    "rejected_candidate_id": {"type": "string"},
                    "payload": {"type": "object"},
                    "output_path": {"type": "string"},
                    "exported_by": {"type": "string"},
                    "active_only": {"type": "boolean"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 1000},
                    "offset": {"type": "integer", "minimum": 0},
                    "reconciled_by": {"type": "string"},
                    "backfill": {"type": "boolean"},
                    "since_days": {"type": "integer", "minimum": 1},
                    "receipt_ids": {
                        "oneOf": [
                            {"type": "string"},
                            {"type": "array", "items": {"type": "string"}},
                        ]
                    },
                },
                "required": ["action"],
            },
        },
    ),
}
