"""Repo-policy onboarding authority and operator education helpers."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from hashlib import sha1
from pathlib import Path
import re
from typing import Any

from core.object_truth_ops import build_task_environment_contract, canonical_digest
from storage.postgres.repo_policy_contract_repository import (
    PostgresRepoPolicyContractRepository,
    RepoPolicyContractRecord,
)


REPO_POLICY_CONTRACT_KIND = "operator_onboarding.repo_policy_contract.v1"
REPO_POLICY_CONTRACT_PURPOSE = "operator_onboarding.repo_policy_contract.v1"
REPO_POLICY_TASK_TYPE = "operator_repo_policy_onboarding"
DEFAULT_DISCLOSURE_REPEAT_LIMIT = 5
REPO_POLICY_DECISION_REF = (
    "architecture-policy::operator-onboarding::"
    "first-run-repo-policy-contract-and-pattern-disclosure"
)
REPO_POLICY_TOOL_REFS = ("praxis_setup", "praxis_patterns", "praxis_bugs")
_SLUG_PATTERN = re.compile(r"[^a-z0-9]+")

STARTER_ANTI_PATTERNS: tuple[dict[str, str], ...] = (
    {
        "title": "No raw secrets in chat or logs",
        "why": "Credentials, tokens, and exported secrets should go through secure capture or redacted references only.",
    },
    {
        "title": "No production-side mutations without named proof",
        "why": "Sensitive systems should require an explicit operator rule, scope, or verifier before writes.",
    },
    {
        "title": "Do not normalize around known-bad data silently",
        "why": "If a source system is malformed, persist the exception or pattern instead of teaching the runtime to pretend it is valid.",
    },
)

_FORBIDDEN_ACTIONS = frozenset({"create", "update", "delete", "rename"})
_FORBIDDEN_RULE_ENFORCEMENT_LEVELS = frozenset({"hard", "advisory"})


def _slug(value: str) -> str:
    normalized = _SLUG_PATTERN.sub("-", str(value or "").strip().lower()).strip("-")
    return normalized or "item"


def _stable_id(prefix: str, *parts: str) -> str:
    digest = sha1("::".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}.{digest}"


def _raw_rule_text(value: object) -> str:
    if isinstance(value, Mapping):
        raw = str(value.get("raw_text") or value.get("rule") or value.get("title") or "").strip()
        if raw:
            return raw
        action = str(value.get("action") or "").strip()
        path = str(value.get("path_glob") or value.get("path_substring") or value.get("path") or "").strip()
        return " ".join(part for part in (action, path) if part).strip()
    return str(value or "").strip()


def _string_list(values: Sequence[object] | None) -> list[str]:
    if values is None:
        return []
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _forbidden_action_texts(values: Sequence[object] | None) -> list[str]:
    if values is None:
        return []
    seen: set[str] = set()
    normalized: list[str] = []
    for value in values:
        text = _raw_rule_text(value)
        if not text or text in seen:
            continue
        seen.add(text)
        normalized.append(text)
    return normalized


def _optional_string_list(values: Sequence[object] | None) -> list[str] | None:
    if values is None:
        return None
    return _string_list(values)


def _normalize_sensitive_systems(values: Sequence[object] | None) -> list[dict[str, Any]]:
    if values is None:
        return []
    normalized: list[dict[str, Any]] = []
    for value in values:
        if isinstance(value, Mapping):
            label = str(value.get("label") or value.get("system_ref") or "").strip()
            if not label:
                continue
            normalized.append(
                {
                    "label": label,
                    "system_ref": str(value.get("system_ref") or f"system:{_slug(label)}").strip(),
                    "sensitivity": str(value.get("sensitivity") or "sensitive").strip(),
                    "handling_notes": str(value.get("handling_notes") or value.get("notes") or "").strip() or None,
                }
            )
            continue
        text = str(value or "").strip()
        if not text:
            continue
        normalized.append(
            {
                "label": text,
                "system_ref": f"system:{_slug(text)}",
                "sensitivity": "sensitive",
                "handling_notes": None,
            }
        )
    return normalized


def _normalize_forbidden_rule_mapping(value: Mapping[str, Any]) -> dict[str, Any] | None:
    raw_text = _raw_rule_text(value)
    action = str(value.get("action") or "").strip().lower()
    if action and action not in _FORBIDDEN_ACTIONS:
        action = ""
    path_glob = str(value.get("path_glob") or value.get("glob") or "").strip()
    path_substring = str(value.get("path_substring") or value.get("path") or "").strip()
    if path_glob:
        path_substring = ""
    enforcement_level = str(value.get("enforcement_level") or value.get("enforcement") or "").strip().lower()
    if enforcement_level not in _FORBIDDEN_RULE_ENFORCEMENT_LEVELS:
        enforcement_level = "hard" if action or path_glob or path_substring else "advisory"
    machine_enforceable = bool(action or path_glob or path_substring)
    if not raw_text and not machine_enforceable:
        return None
    return {
        "rule_id": str(value.get("rule_id") or "").strip()
        or _stable_id("forbidden_action_rule", raw_text, action, path_glob, path_substring),
        "raw_text": raw_text or " ".join(part for part in (action, path_glob, path_substring) if part),
        "action": action or None,
        "path_glob": path_glob or None,
        "path_substring": path_substring or None,
        "enforcement_level": enforcement_level,
        "machine_enforceable": machine_enforceable,
    }


def _normalize_forbidden_rule_text(value: object) -> dict[str, Any] | None:
    raw_text = _raw_rule_text(value)
    if not raw_text:
        return None
    normalized = raw_text.strip()
    lower = normalized.lower()
    tokens = lower.split()
    action = tokens[0] if tokens and tokens[0] in _FORBIDDEN_ACTIONS else ""
    remainder = " ".join(tokens[1:]).strip() if action else lower
    path_glob = ""
    path_substring = ""
    if remainder:
        if any(char in remainder for char in "*?["):
            path_glob = remainder
        elif "/" in remainder or "." in remainder:
            path_substring = remainder
    machine_enforceable = bool(action or path_glob or path_substring)
    return {
        "rule_id": _stable_id("forbidden_action_rule", raw_text, action, path_glob, path_substring),
        "raw_text": raw_text,
        "action": action or None,
        "path_glob": path_glob or None,
        "path_substring": path_substring or None,
        "enforcement_level": "hard" if machine_enforceable else "advisory",
        "machine_enforceable": machine_enforceable,
    }


def normalize_forbidden_action_rules(values: Sequence[object] | None) -> list[dict[str, Any]]:
    if values is None:
        return []
    normalized: list[dict[str, Any]] = []
    seen: set[str] = set()
    for value in values:
        rule = (
            _normalize_forbidden_rule_mapping(value)
            if isinstance(value, Mapping)
            else _normalize_forbidden_rule_text(value)
        )
        if rule is None:
            continue
        signature = "::".join(
            str(rule.get(part) or "")
            for part in ("raw_text", "action", "path_glob", "path_substring", "enforcement_level")
        )
        if signature in seen:
            continue
        seen.add(signature)
        normalized.append(rule)
    return normalized


def starter_repo_policy_bundle() -> dict[str, Any]:
    return {
        "questions": [
            "What rules of this repo should Praxis always respect?",
            "Which systems are sensitive enough that Praxis should treat them as high-risk or proof-gated?",
            "What operating SOPs should future runs follow without being retaught?",
            "What failure or anti-pattern examples should become reusable guardrails?",
        ],
        "starter_anti_patterns": list(STARTER_ANTI_PATTERNS),
        "suggested_fields": [
            "repo_rules",
            "sops",
            "anti_patterns",
            "forbidden_actions",
            "sensitive_systems",
        ],
    }


def _section_refs(prefix: str, values: Sequence[str]) -> list[str]:
    return [f"{prefix}:{_slug(value)}" for value in values]


def _repo_policy_sections(
    *,
    repo_rules: Sequence[str],
    sops: Sequence[str],
    anti_patterns: Sequence[str],
    forbidden_actions: Sequence[str],
    sensitive_systems: Sequence[Mapping[str, Any]],
    forbidden_action_rules: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    normalized_forbidden_actions = _forbidden_action_texts(forbidden_actions)
    normalized_forbidden_action_rules = (
        [dict(item) for item in forbidden_action_rules]
        if forbidden_action_rules is not None
        else normalize_forbidden_action_rules(normalized_forbidden_actions)
    )
    return {
        "repo_rules": list(repo_rules),
        "sops": list(sops),
        "anti_patterns": list(anti_patterns),
        "forbidden_actions": normalized_forbidden_actions,
        "forbidden_action_rules": normalized_forbidden_action_rules,
        "sensitive_systems": [dict(item) for item in sensitive_systems],
    }


def build_repo_policy_contract(
    *,
    repo_root: str,
    repo_rules: Sequence[str],
    sops: Sequence[str],
    anti_patterns: Sequence[str],
    forbidden_actions: Sequence[str],
    sensitive_systems: Sequence[Mapping[str, Any]],
    disclosure_repeat_limit: int,
    previous_contract_digest: str | None,
) -> dict[str, Any]:
    sections = _repo_policy_sections(
        repo_rules=repo_rules,
        sops=sops,
        anti_patterns=anti_patterns,
        forbidden_actions=forbidden_actions,
        sensitive_systems=sensitive_systems,
    )
    task_contract = build_task_environment_contract(
        task_type=REPO_POLICY_TASK_TYPE,
        authority_inputs={
            "repo_root": repo_root,
            **sections,
        },
        allowed_model_routes=[],
        tool_refs=list(REPO_POLICY_TOOL_REFS),
        sop_refs=_section_refs("sop", sops),
        policy_refs=[REPO_POLICY_DECISION_REF],
        previous_contract_digest=previous_contract_digest,
        failure_pattern_refs=_section_refs("anti-pattern", anti_patterns),
    )
    body = {
        "kind": REPO_POLICY_CONTRACT_KIND,
        "repo_root": repo_root,
        "schema_version": 1,
        "repo_policy_sections": sections,
        "task_environment_contract": task_contract,
        "disclosure_policy": {
            "repeat_limit": max(0, int(disclosure_repeat_limit)),
            "events": ["bug", "pattern"],
            "message_window": "first_many_times",
        },
        "decision_ref": REPO_POLICY_DECISION_REF,
    }
    body["contract_digest"] = canonical_digest(body, purpose=REPO_POLICY_CONTRACT_PURPOSE)
    return body


def _current_sections(record: RepoPolicyContractRecord | None) -> dict[str, Any]:
    if record is None:
        return {
            "repo_rules": [],
            "sops": [],
            "anti_patterns": [],
            "forbidden_actions": [],
            "forbidden_action_rules": [],
            "sensitive_systems": [],
        }
    raw = record.contract_body.get("repo_policy_sections")
    if isinstance(raw, Mapping):
        forbidden_actions = _forbidden_action_texts(raw.get("forbidden_actions") or [])
        raw_forbidden_action_rules = raw.get("forbidden_action_rules")
        forbidden_action_rules = normalize_forbidden_action_rules(
            raw_forbidden_action_rules
            if isinstance(raw_forbidden_action_rules, Sequence)
            and not isinstance(raw_forbidden_action_rules, (str, bytes, bytearray))
            else forbidden_actions
        )
        return {
            "repo_rules": _string_list(raw.get("repo_rules") or []),
            "sops": _string_list(raw.get("sops") or []),
            "anti_patterns": _string_list(raw.get("anti_patterns") or []),
            "forbidden_actions": forbidden_actions,
            "forbidden_action_rules": forbidden_action_rules,
            "sensitive_systems": _normalize_sensitive_systems(raw.get("sensitive_systems") or []),
        }
    return {
        "repo_rules": [],
        "sops": [],
        "anti_patterns": [],
        "forbidden_actions": [],
        "forbidden_action_rules": [],
        "sensitive_systems": [],
    }


def upsert_repo_policy_contract(
    conn: Any,
    *,
    repo_root: str | Path,
    repo_rules: Sequence[object] | None = None,
    sops: Sequence[object] | None = None,
    anti_patterns: Sequence[object] | None = None,
    forbidden_actions: Sequence[object] | None = None,
    sensitive_systems: Sequence[object] | None = None,
    submitted_by: str,
    change_reason: str,
    disclosure_repeat_limit: int = DEFAULT_DISCLOSURE_REPEAT_LIMIT,
) -> RepoPolicyContractRecord:
    normalized_root = str(repo_root)
    repo = PostgresRepoPolicyContractRepository(conn)
    current = repo.get_current(repo_root=normalized_root)
    current_sections = _current_sections(current)

    merged_repo_rules = (
        _string_list(repo_rules)
        if repo_rules is not None
        else list(current_sections["repo_rules"])
    )
    merged_sops = (
        _string_list(sops)
        if sops is not None
        else list(current_sections["sops"])
    )
    merged_anti_patterns = (
        _string_list(anti_patterns)
        if anti_patterns is not None
        else list(current_sections["anti_patterns"])
    )
    merged_forbidden_actions = (
        _forbidden_action_texts(forbidden_actions)
        if forbidden_actions is not None
        else list(current_sections["forbidden_actions"])
    )
    merged_sensitive_systems = (
        _normalize_sensitive_systems(sensitive_systems)
        if sensitive_systems is not None
        else list(current_sections["sensitive_systems"])
    )

    if not any(
        (
            merged_repo_rules,
            merged_sops,
            merged_anti_patterns,
            merged_forbidden_actions,
            merged_sensitive_systems,
        )
    ):
        raise ValueError(
            "repo policy onboarding requires at least one repo rule, SOP, anti-pattern, forbidden action, or sensitive system"
        )

    repeat_limit = max(
        0,
        int(
            disclosure_repeat_limit
            if disclosure_repeat_limit is not None
            else (
                current.disclosure_repeat_limit
                if current is not None
                else DEFAULT_DISCLOSURE_REPEAT_LIMIT
            )
        ),
    )
    body = build_repo_policy_contract(
        repo_root=normalized_root,
        repo_rules=merged_repo_rules,
        sops=merged_sops,
        anti_patterns=merged_anti_patterns,
        forbidden_actions=merged_forbidden_actions,
        sensitive_systems=merged_sensitive_systems,
        disclosure_repeat_limit=repeat_limit,
        previous_contract_digest=current.current_contract_hash if current is not None else None,
    )
    contract_id = (
        current.repo_policy_contract_id
        if current is not None
        else _stable_id("repo_policy_contract", normalized_root)
    )
    revision_id = _stable_id(
        "repo_policy_contract_revision",
        normalized_root,
        body["contract_digest"],
        str((current.current_revision_no + 1) if current is not None else 1),
    )
    return repo.upsert_contract(
        repo_policy_contract_id=contract_id,
        repo_root=normalized_root,
        status="active",
        revision_id=revision_id,
        contract_hash=str(body["contract_digest"]),
        contract_body=body,
        created_by=str(submitted_by or "operator"),
        change_reason=str(change_reason or "operator_repo_policy_onboarding"),
        disclosure_repeat_limit=repeat_limit,
    )


def get_repo_policy_contract(
    conn: Any,
    *,
    repo_root: str | Path,
) -> RepoPolicyContractRecord | None:
    repo = PostgresRepoPolicyContractRepository(conn)
    return repo.get_current(repo_root=str(repo_root))


def repo_policy_probe_observed_state(record: RepoPolicyContractRecord | None) -> dict[str, Any]:
    if record is None:
        return {
            "current_contract_present": False,
            "starter_bundle": starter_repo_policy_bundle(),
        }
    sections = _current_sections(record)
    return {
        "current_contract_present": True,
        "repo_policy_contract_id": record.repo_policy_contract_id,
        "current_revision_id": record.current_revision_id,
        "current_revision_no": record.current_revision_no,
        "current_contract_hash": record.current_contract_hash,
        "disclosure_repeat_limit": record.disclosure_repeat_limit,
        "bug_disclosure_count": record.bug_disclosure_count,
        "pattern_disclosure_count": record.pattern_disclosure_count,
        "section_counts": {
            "repo_rules": len(sections["repo_rules"]),
            "sops": len(sections["sops"]),
            "anti_patterns": len(sections["anti_patterns"]),
            "forbidden_actions": len(sections["forbidden_actions"]),
            "forbidden_action_rules": len(sections["forbidden_action_rules"]),
            "sensitive_systems": len(sections["sensitive_systems"]),
        },
        "repo_policy_sections": sections,
    }


def repo_policy_runtime_payload(record: RepoPolicyContractRecord | None) -> dict[str, Any] | None:
    if record is None:
        return None
    body = record.contract_body if isinstance(record.contract_body, Mapping) else {}
    return {
        "repo_policy_contract_id": record.repo_policy_contract_id,
        "repo_root": record.repo_root,
        "current_revision_id": record.current_revision_id,
        "current_revision_no": record.current_revision_no,
        "current_contract_hash": record.current_contract_hash,
        "decision_ref": str(body.get("decision_ref") or REPO_POLICY_DECISION_REF),
        "disclosure_policy": (
            dict(body.get("disclosure_policy"))
            if isinstance(body.get("disclosure_policy"), Mapping)
            else {}
        ),
        "repo_policy_sections": _current_sections(record),
        "task_environment_contract": (
            dict(body.get("task_environment_contract"))
            if isinstance(body.get("task_environment_contract"), Mapping)
            else {}
        ),
    }


def consume_operator_disclosure(
    conn: Any,
    *,
    repo_root: str | Path,
    disclosure_kind: str,
) -> dict[str, Any] | None:
    normalized_kind = str(disclosure_kind or "").strip().lower()
    if normalized_kind not in {"bug", "pattern"}:
        return None
    repo = PostgresRepoPolicyContractRepository(conn)
    current = repo.get_current(repo_root=str(repo_root))
    if current is None or current.disclosure_repeat_limit <= 0:
        return None
    current_count = (
        current.bug_disclosure_count if normalized_kind == "bug" else current.pattern_disclosure_count
    )
    if current_count >= current.disclosure_repeat_limit:
        return None
    updated = repo.increment_disclosure(repo_root=str(repo_root), disclosure_kind=normalized_kind)
    if updated is None:
        return None
    shown = updated.bug_disclosure_count if normalized_kind == "bug" else updated.pattern_disclosure_count
    remaining = max(updated.disclosure_repeat_limit - shown, 0)
    if normalized_kind == "pattern":
        message = (
            "Praxis stored this recurring behavior as reusable pattern/anti-pattern memory for this repo. "
            "Future runs can bind it into contracts and guardrails instead of relearning it from scratch."
        )
        affects = ["task contracts", "pattern recall", "future guardrail suggestions"]
    else:
        message = (
            "This bug is a concrete fix record. If the same failure shape keeps recurring, Praxis can promote that evidence into reusable pattern/anti-pattern memory for this repo."
        )
        affects = ["bug history", "future pattern promotion", "dedupe and triage context"]
    return {
        "kind": "operator_onboarding_disclosure",
        "disclosure_kind": normalized_kind,
        "message": message,
        "times_shown": shown,
        "times_remaining": remaining,
        "repeat_limit": updated.disclosure_repeat_limit,
        "repo_policy_contract_id": updated.repo_policy_contract_id,
        "current_revision_no": updated.current_revision_no,
        "affects_future_behavior": affects,
        "decision_ref": REPO_POLICY_DECISION_REF,
    }


__all__ = [
    "DEFAULT_DISCLOSURE_REPEAT_LIMIT",
    "REPO_POLICY_DECISION_REF",
    "STARTER_ANTI_PATTERNS",
    "build_repo_policy_contract",
    "consume_operator_disclosure",
    "get_repo_policy_contract",
    "normalize_forbidden_action_rules",
    "repo_policy_runtime_payload",
    "repo_policy_probe_observed_state",
    "starter_repo_policy_bundle",
    "upsert_repo_policy_contract",
]
