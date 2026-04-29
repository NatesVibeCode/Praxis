"""Authoring and acceptance contract helpers for workflow artifacts.

These contracts separate:
- how a model should produce an artifact (`authoring_contract`)
- how the platform evaluates whether the artifact is good enough (`acceptance_contract`)
"""

from __future__ import annotations

import json
import fnmatch
import re
from collections.abc import Mapping, Sequence
from typing import Any

from runtime.repo_policy_onboarding import normalize_forbidden_action_rules

_VALID_REVIEW_DECISIONS = frozenset({"approve", "request_changes", "reject"})
_MISSING = object()
_ARTIFACT_WRITE_SCOPE_RE = re.compile(
    r"(?<![\w./&+-])"
    r"((?:[A-Za-z0-9._@:+&-]+/)*"
    r"artifacts/[A-Za-z0-9._@:+&-]+(?:/[A-Za-z0-9._@:+&-]+)*/?)"
)


def _json_safe(value: object) -> Any:
    return json.loads(json.dumps(value, default=str))


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    items: list[str] = []
    for item in value:
        text = str(item or "").strip()
        if text:
            items.append(text)
    return items


def _dict_value(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _dedupe_strings(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def _artifact_scope_texts(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Mapping):
        texts: list[str] = []
        for nested in value.values():
            texts.extend(_artifact_scope_texts(nested))
        return texts
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        texts: list[str] = []
        for nested in value:
            texts.extend(_artifact_scope_texts(nested))
        return texts
    return []


def infer_artifact_write_scope(job: Mapping[str, Any]) -> list[str]:
    """Infer artifact write scope from explicit job output contracts only."""

    candidates: list[str] = []
    for field_name in (
        "outcome_goal",
        "output_goal",
        "output_path",
        "description",
        "prompt",
        "expected_outputs",
        "authoring_contract",
        "acceptance_contract",
        "verify_command",
    ):
        for text in _artifact_scope_texts(job.get(field_name)):
            for match in _ARTIFACT_WRITE_SCOPE_RE.finditer(text):
                path = match.group(1).rstrip("`'\"),;:.")
                path = path.rstrip("/")
                if path and path not in candidates:
                    candidates.append(path)
    return candidates


def _normalize_output_schema(value: object | None) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return _json_safe(dict(value))


def _normalize_assertion(value: object) -> dict[str, Any] | None:
    if not isinstance(value, Mapping):
        return None
    assertion = _json_safe(dict(value))
    kind = str(assertion.get("kind") or "").strip().lower()
    if not kind:
        return None
    assertion["kind"] = kind
    if "severity" in assertion:
        assertion["severity"] = str(assertion.get("severity") or "").strip().lower() or "hard"
    return assertion


def _repo_policy_sections(repo_policy_contract: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(repo_policy_contract, Mapping):
        return {}
    sections = repo_policy_contract.get("repo_policy_sections")
    return dict(sections) if isinstance(sections, Mapping) else {}


def _operation_candidates(operation: Mapping[str, Any]) -> list[str]:
    action = str(operation.get("action") or "").strip().lower()
    path = str(operation.get("path") or "").strip().lower()
    from_path = str(operation.get("from_path") or "").strip().lower()
    candidates = [action, path, from_path]
    if action and path:
        candidates.append(f"{action} {path}")
    if action and from_path and path:
        candidates.append(f"{action} {from_path} {path}")
    return [candidate for candidate in candidates if candidate]


def _forbidden_action_match(
    rule: Mapping[str, Any],
    *,
    operation: Mapping[str, Any],
) -> dict[str, Any] | None:
    if not isinstance(rule, Mapping):
        return None
    enforcement_level = str(rule.get("enforcement_level") or "").strip().lower()
    if enforcement_level not in {"", "hard"}:
        return None
    if rule.get("machine_enforceable") is False:
        return None
    rule_action = str(rule.get("action") or "").strip().lower()
    path_glob = str(rule.get("path_glob") or "").strip().lower()
    path_substring = str(rule.get("path_substring") or "").strip().lower()
    action = str(operation.get("action") or "").strip().lower()
    path = str(operation.get("path") or "").strip().lower()
    from_path = str(operation.get("from_path") or "").strip().lower()
    candidates = _operation_candidates(operation)

    if rule_action and action != rule_action:
        return None

    if path_glob:
        if any(fnmatch.fnmatch(candidate, path_glob) for candidate in candidates):
            return {"match_kind": "glob", "operation": dict(operation)}
        return None

    if path_substring:
        if path_substring in path or path_substring in from_path:
            return {"match_kind": "action_path_substring", "operation": dict(operation)}
        return None

    if rule_action and action == rule_action:
        return {"match_kind": "action_exact", "operation": dict(operation)}
    return None


def _repo_policy_report(
    submission: Mapping[str, Any],
    *,
    repo_policy_contract: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if not isinstance(repo_policy_contract, Mapping) or not repo_policy_contract:
        return {
            "contract_present": False,
            "enforced": False,
            "forbidden_actions": [],
            "forbidden_action_rules": [],
            "violations": [],
        }
    sections = _repo_policy_sections(repo_policy_contract)
    raw_rules = sections.get("forbidden_action_rules")
    forbidden_action_rules = normalize_forbidden_action_rules(
        raw_rules
        if isinstance(raw_rules, Sequence)
        and not isinstance(raw_rules, (str, bytes, bytearray))
        else sections.get("forbidden_actions")
    )
    forbidden_actions = _dedupe_strings(
        [
            str(rule.get("raw_text") or "").strip()
            for rule in forbidden_action_rules
            if str(rule.get("raw_text") or "").strip()
        ]
    )
    raw_declared_operations = submission.get("declared_operations")
    raw_operation_set = submission.get("operation_set")
    operations = [
        dict(item)
        for item in [
            *(list(raw_declared_operations) if isinstance(raw_declared_operations, Sequence) and not isinstance(raw_declared_operations, (str, bytes, bytearray)) else []),
            *(list(raw_operation_set) if isinstance(raw_operation_set, Sequence) and not isinstance(raw_operation_set, (str, bytes, bytearray)) else []),
        ]
        if isinstance(item, Mapping)
    ]
    violations: list[dict[str, Any]] = []
    seen_violations: set[str] = set()
    for rule in forbidden_action_rules:
        for operation in operations:
            matched = _forbidden_action_match(rule, operation=operation)
            if matched is None:
                continue
            violation = {
                "rule_id": rule.get("rule_id"),
                "rule": rule.get("raw_text"),
                "enforcement_level": rule.get("enforcement_level"),
                "match_kind": matched["match_kind"],
                "operation": matched["operation"],
            }
            signature = json.dumps(violation, sort_keys=True, default=str)
            if signature in seen_violations:
                continue
            seen_violations.add(signature)
            violations.append(violation)
    return {
        "contract_present": True,
        "enforced": bool(forbidden_actions),
        "repo_policy_contract_id": str(repo_policy_contract.get("repo_policy_contract_id") or "").strip() or None,
        "current_contract_hash": str(repo_policy_contract.get("current_contract_hash") or "").strip() or None,
        "forbidden_actions": forbidden_actions,
        "forbidden_action_rules": forbidden_action_rules,
        "sensitive_systems": [
            str(item.get("label") or item.get("system_ref") or "").strip()
            for item in list(sections.get("sensitive_systems") or [])
            if isinstance(item, Mapping)
            and str(item.get("label") or item.get("system_ref") or "").strip()
        ],
        "violations": violations,
    }


def normalize_authoring_contract(
    *,
    output_schema: object | None = None,
    authoring_contract: object | None = None,
    acceptance_contract: object | None = None,
) -> dict[str, Any]:
    raw_authoring = _dict_value(authoring_contract)
    raw_acceptance = _dict_value(acceptance_contract)
    structural = _dict_value(raw_acceptance.get("structural"))

    normalized_output_schema = _normalize_output_schema(
        raw_authoring.get("output_schema")
        or structural.get("output_schema")
        or output_schema
    )
    required_sections = _dedupe_strings(
        [
            *_string_list(raw_authoring.get("required_sections")),
            *_string_list(structural.get("required_sections")),
        ]
    )
    required_fields = _dedupe_strings(
        [
            *_string_list(raw_authoring.get("required_fields")),
            *_string_list(structural.get("required_fields")),
        ]
    )
    notes = _dedupe_strings(_string_list(raw_authoring.get("notes")))

    normalized: dict[str, Any] = {}
    artifact_kind = str(raw_authoring.get("artifact_kind") or "").strip()
    if artifact_kind:
        normalized["artifact_kind"] = artifact_kind
    if required_sections:
        normalized["required_sections"] = required_sections
    if required_fields:
        normalized["required_fields"] = required_fields
    if normalized_output_schema:
        normalized["output_schema"] = normalized_output_schema
    stop_boundary = str(raw_authoring.get("stop_boundary") or "").strip()
    if stop_boundary:
        normalized["stop_boundary"] = stop_boundary
    submission_format = str(raw_authoring.get("submission_format") or "").strip()
    if submission_format:
        normalized["submission_format"] = submission_format
    if notes:
        normalized["notes"] = notes
    return normalized


def normalize_acceptance_contract(
    *,
    output_schema: object | None = None,
    authoring_contract: object | None = None,
    acceptance_contract: object | None = None,
    verify_refs: Sequence[str] | None = None,
) -> dict[str, Any]:
    raw_authoring = _dict_value(authoring_contract)
    raw_acceptance = _dict_value(acceptance_contract)
    raw_structural = _dict_value(raw_acceptance.get("structural"))
    raw_review = _dict_value(raw_acceptance.get("review"))

    normalized_output_schema = _normalize_output_schema(
        raw_structural.get("output_schema")
        or raw_authoring.get("output_schema")
        or output_schema
    )
    required_sections = _dedupe_strings(
        [
            *_string_list(raw_structural.get("required_sections")),
            *_string_list(raw_authoring.get("required_sections")),
        ]
    )
    required_fields = _dedupe_strings(
        [
            *_string_list(raw_structural.get("required_fields")),
            *_string_list(raw_authoring.get("required_fields")),
        ]
    )
    structural: dict[str, Any] = {}
    if required_sections:
        structural["required_sections"] = required_sections
    if required_fields:
        structural["required_fields"] = required_fields
    if normalized_output_schema:
        structural["output_schema"] = normalized_output_schema

    assertions = [
        normalized
        for normalized in (
            _normalize_assertion(item)
            for item in list(raw_acceptance.get("assertions") or [])
        )
        if normalized is not None
    ]

    review: dict[str, Any] = {}
    criteria = _dedupe_strings(_string_list(raw_review.get("criteria")))
    if criteria:
        review["criteria"] = criteria
    required_decision = str(raw_review.get("required_decision") or "").strip().lower()
    if required_decision in _VALID_REVIEW_DECISIONS:
        review["required_decision"] = required_decision

    normalized: dict[str, Any] = {}
    if structural:
        normalized["structural"] = structural
    if assertions:
        normalized["assertions"] = assertions
    normalized_verify_refs = _dedupe_strings(
        [*_string_list(raw_acceptance.get("verify_refs")), *_string_list(list(verify_refs or []))]
    )
    if normalized_verify_refs:
        normalized["verify_refs"] = normalized_verify_refs
    if review:
        normalized["review"] = review
    return normalized


def _schema_to_skeleton(schema: object) -> Any:
    if isinstance(schema, Mapping):
        schema_dict = dict(schema)
        schema_type = str(schema_dict.get("type") or "").strip().lower()
        if schema_type == "object":
            properties = _dict_value(schema_dict.get("properties"))
            required = _string_list(schema_dict.get("required")) or list(properties.keys())
            skeleton: dict[str, Any] = {}
            for key in required:
                if key in properties:
                    skeleton[key] = _schema_to_skeleton(properties[key])
            for key, value in properties.items():
                if key not in skeleton:
                    skeleton[key] = _schema_to_skeleton(value)
            return skeleton
        if schema_type == "array":
            items = schema_dict.get("items")
            return [_schema_to_skeleton(items)] if items is not None else []
        if schema_type == "string":
            return "<string>"
        if schema_type == "number":
            return 0.0
        if schema_type == "integer":
            return 0
        if schema_type == "boolean":
            return False
        return {str(key): _schema_to_skeleton(value) for key, value in schema_dict.items()}
    if isinstance(schema, list):
        return [_schema_to_skeleton(schema[0])] if schema else []
    return "<value>"


def _render_heading_list(name: str, values: Sequence[str]) -> str:
    if not values:
        return ""
    rendered = "\n".join(f"- {value}" for value in values)
    return f"{name}:\n{rendered}"


def render_authoring_contract(authoring_contract: Mapping[str, Any] | None) -> str:
    contract = dict(authoring_contract) if isinstance(authoring_contract, Mapping) else {}
    if not contract:
        return ""

    parts = ["** AUTHORING CONTRACT **"]
    artifact_kind = str(contract.get("artifact_kind") or "").strip()
    if artifact_kind:
        parts.append(f"artifact_kind: {artifact_kind}")

    required_sections = _string_list(contract.get("required_sections"))
    if required_sections:
        parts.append(_render_heading_list("required_sections", required_sections))
        scaffold = "\n".join(f"## {section}\n- " for section in required_sections)
        parts.append("section_scaffold:\n" + scaffold)

    required_fields = _string_list(contract.get("required_fields"))
    if required_fields:
        parts.append(_render_heading_list("required_fields", required_fields))

    output_schema = _normalize_output_schema(contract.get("output_schema"))
    if output_schema:
        parts.append(
            "output_schema_scaffold:\n```json\n"
            + json.dumps(_schema_to_skeleton(output_schema), indent=2, sort_keys=True)
            + "\n```"
        )
        parts.append(
            "When you submit the artifact, include a JSON object that matches the scaffold above. "
            "If you include prose too, place the JSON inside a fenced ```json``` block."
        )

    stop_boundary = str(contract.get("stop_boundary") or "").strip()
    if stop_boundary:
        parts.append(f"stop_boundary: {stop_boundary}")

    submission_format = str(contract.get("submission_format") or "").strip()
    if submission_format:
        parts.append(f"submission_format: {submission_format}")

    notes = _string_list(contract.get("notes"))
    if notes:
        parts.append(_render_heading_list("notes", notes))

    return "\n".join(part for part in parts if part)


def _assertion_to_text(assertion: Mapping[str, Any]) -> str:
    kind = str(assertion.get("kind") or "").strip()
    if kind == "section_present":
        return f"section_present section={assertion.get('section')}"
    if kind == "count_at_least":
        if assertion.get("path"):
            return f"count_at_least path={assertion.get('path')} min={assertion.get('min')}"
        if assertion.get("pattern"):
            return f"count_at_least pattern={assertion.get('pattern')} min={assertion.get('min')}"
    if kind == "field_present":
        return f"field_present path={assertion.get('path')}"
    if kind == "field_numeric":
        return f"field_numeric path={assertion.get('path')}"
    if kind == "field_at_least":
        return f"field_at_least path={assertion.get('path')} min={assertion.get('min')}"
    if kind == "citations_at_least":
        return f"citations_at_least min={assertion.get('min')}"
    return json.dumps(dict(assertion), sort_keys=True, default=str)


def render_acceptance_contract(acceptance_contract: Mapping[str, Any] | None) -> str:
    contract = dict(acceptance_contract) if isinstance(acceptance_contract, Mapping) else {}
    if not contract:
        return ""

    parts = ["** ACCEPTANCE CONTRACT **"]
    structural = _dict_value(contract.get("structural"))
    if structural:
        required_sections = _string_list(structural.get("required_sections"))
        if required_sections:
            parts.append(_render_heading_list("structural.required_sections", required_sections))
        required_fields = _string_list(structural.get("required_fields"))
        if required_fields:
            parts.append(_render_heading_list("structural.required_fields", required_fields))
        output_schema = _normalize_output_schema(structural.get("output_schema"))
        if output_schema:
            parts.append("structural.output_schema: required")

    assertions = [
        _assertion_to_text(assertion)
        for assertion in list(contract.get("assertions") or [])
        if isinstance(assertion, Mapping)
    ]
    if assertions:
        parts.append(_render_heading_list("assertions", assertions))

    verify_refs = _string_list(contract.get("verify_refs"))
    if verify_refs:
        parts.append(_render_heading_list("verify_refs", verify_refs))

    review = _dict_value(contract.get("review"))
    if review:
        criteria = _string_list(review.get("criteria"))
        if criteria:
            parts.append(_render_heading_list("review.criteria", criteria))
        required_decision = str(review.get("required_decision") or "").strip()
        if required_decision:
            parts.append(f"review.required_decision: {required_decision}")

    return "\n".join(part for part in parts if part)


def _extract_json_payload(summary: str) -> tuple[Any | None, str | None]:
    stripped = summary.strip()
    if not stripped:
        return None, None

    candidates: list[str] = []
    # Fenced blocks first — when prose wraps JSON, the fenced block is
    # the intended payload, not whatever the prose starts with.
    candidates.extend(
        match.group(1).strip()
        for match in re.finditer(r"```json\s*(.*?)```", summary, flags=re.IGNORECASE | re.DOTALL)
    )
    candidates.extend(
        match.group(1).strip()
        for match in re.finditer(r"```\s*(\{.*?\}|\[.*?\])\s*```", summary, flags=re.DOTALL)
    )
    if stripped.startswith("{") or stripped.startswith("["):
        candidates.append(stripped)

    for candidate in candidates:
        try:
            return json.loads(candidate), None
        except json.JSONDecodeError as exc:
            last_error = str(exc)
    return None, locals().get("last_error")


def _section_present(summary: str, section: str) -> bool:
    escaped = re.escape(section.strip())
    pattern = rf"(^|\n)\s{{0,3}}(?:#+\s*)?{escaped}\s*:?(?:\n|$)"
    return re.search(pattern, summary, flags=re.IGNORECASE) is not None


def _lookup_path(payload: Any, path: str) -> Any:
    if not path:
        return payload
    current: Any = payload
    for raw_part in path.split("."):
        part = raw_part.strip()
        if not part:
            return _MISSING
        if isinstance(current, Mapping):
            if part not in current:
                return _MISSING
            current = current[part]
            continue
        if isinstance(current, list):
            try:
                index = int(part)
            except ValueError:
                return _MISSING
            if index < 0 or index >= len(current):
                return _MISSING
            current = current[index]
            continue
        return _MISSING
    return current


def _validate_against_schema(payload: Any, schema: object, *, path: str = "$") -> list[str]:
    errors: list[str] = []
    if not isinstance(schema, Mapping):
        return errors
    schema_dict = dict(schema)
    schema_type = str(schema_dict.get("type") or "").strip().lower()
    if schema_type == "object":
        if not isinstance(payload, Mapping):
            return [f"{path} must be an object"]
        properties = _dict_value(schema_dict.get("properties"))
        required = _string_list(schema_dict.get("required")) or list(properties.keys())
        for key in required:
            if key not in payload:
                errors.append(f"{path}.{key} is required")
        for key, child_schema in properties.items():
            if key in payload:
                errors.extend(_validate_against_schema(payload[key], child_schema, path=f"{path}.{key}"))
        return errors
    if schema_type == "array":
        if not isinstance(payload, list):
            return [f"{path} must be an array"]
        item_schema = schema_dict.get("items")
        if item_schema is not None:
            for index, item in enumerate(payload):
                errors.extend(_validate_against_schema(item, item_schema, path=f"{path}[{index}]"))
        return errors
    if schema_type == "string" and not isinstance(payload, str):
        return [f"{path} must be a string"]
    if schema_type == "number" and not isinstance(payload, (int, float)):
        return [f"{path} must be numeric"]
    if schema_type == "integer" and not isinstance(payload, int):
        return [f"{path} must be an integer"]
    if schema_type == "boolean" and not isinstance(payload, bool):
        return [f"{path} must be a boolean"]
    if schema_type:
        return errors
    if isinstance(payload, Mapping):
        for key, child_schema in schema_dict.items():
            if key not in payload:
                errors.append(f"{path}.{key} is required")
                continue
            errors.extend(_validate_against_schema(payload[key], child_schema, path=f"{path}.{key}"))
        return errors
    return errors


def _count_citations(summary: str) -> int:
    markdown_links = len(re.findall(r"\[[^\]]+\]\([^)]+\)", summary))
    urls = len(re.findall(r"https?://\S+", summary))
    bracket_citations = len(re.findall(r"\[[0-9]+\]", summary))
    return max(markdown_links + urls, bracket_citations)


def _coerce_number(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip().replace(",", "")
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            return None
    return None


def _evaluate_assertions(summary: str, payload: Any, assertions: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for assertion in assertions:
        kind = str(assertion.get("kind") or "").strip().lower()
        passed = False
        details: dict[str, Any] = {}
        if kind == "section_present":
            section = str(assertion.get("section") or "").strip()
            passed = bool(section) and _section_present(summary, section)
            details["section"] = section
        elif kind in {"field_present", "present"}:
            path = str(assertion.get("path") or "").strip()
            value = _lookup_path(payload, path) if payload is not None else _MISSING
            passed = value is not _MISSING and value not in (None, "", [], {})
            details["path"] = path
        elif kind == "field_numeric":
            path = str(assertion.get("path") or "").strip()
            value = _lookup_path(payload, path) if payload is not None else _MISSING
            passed = _coerce_number(value) is not None
            details["path"] = path
        elif kind == "field_at_least":
            path = str(assertion.get("path") or "").strip()
            minimum = _coerce_number(assertion.get("min"))
            value = _lookup_path(payload, path) if payload is not None else _MISSING
            numeric = _coerce_number(value)
            passed = minimum is not None and numeric is not None and numeric >= minimum
            details["path"] = path
            details["min"] = minimum
            details["actual"] = numeric
        elif kind == "count_at_least":
            minimum = int(assertion.get("min") or 0)
            if assertion.get("path"):
                path = str(assertion.get("path") or "").strip()
                value = _lookup_path(payload, path) if payload is not None else _MISSING
                if isinstance(value, (list, tuple, dict, str)):
                    actual = len(value)
                elif value is _MISSING or value is None:
                    actual = 0
                else:
                    actual = 1
                details["path"] = path
            else:
                pattern = str(assertion.get("pattern") or "").strip()
                actual = len(re.findall(pattern, summary, flags=re.IGNORECASE)) if pattern else 0
                details["pattern"] = pattern
            passed = actual >= minimum
            details["min"] = minimum
            details["actual"] = actual
        elif kind == "citations_at_least":
            minimum = int(assertion.get("min") or 0)
            actual = _count_citations(summary)
            passed = actual >= minimum
            details["min"] = minimum
            details["actual"] = actual

        results.append(
            {
                "kind": kind,
                "passed": passed,
                "severity": str(assertion.get("severity") or "hard"),
                "details": details,
            }
        )
    return results


def evaluate_submission_acceptance(
    *,
    submission: Mapping[str, Any],
    acceptance_contract: Mapping[str, Any] | None,
    repo_policy_contract: Mapping[str, Any] | None = None,
) -> tuple[str, dict[str, Any]]:
    contract = dict(acceptance_contract) if isinstance(acceptance_contract, Mapping) else {}
    structural = _dict_value(contract.get("structural"))
    assertions = [
        dict(item) for item in list(contract.get("assertions") or []) if isinstance(item, Mapping)
    ]
    verify_refs = _string_list(contract.get("verify_refs"))
    review = _dict_value(contract.get("review"))
    summary = str(submission.get("summary") or "")
    latest_review = _dict_value(submission.get("latest_review"))
    verification_artifact_refs = _string_list(submission.get("verification_artifact_refs"))
    repo_policy = _repo_policy_report(
        submission,
        repo_policy_contract=repo_policy_contract,
    )

    contract_requested = bool(structural or assertions or verify_refs or review or repo_policy.get("enforced"))
    payload, payload_error = _extract_json_payload(summary)

    structural_report: dict[str, Any] = {
        "required_sections": [],
        "required_fields": [],
        "schema": None,
    }
    hard_failures: list[str] = []

    for section in _string_list(structural.get("required_sections")):
        present = _section_present(summary, section)
        structural_report["required_sections"].append({"section": section, "passed": present})
        if not present:
            hard_failures.append(f"missing required section: {section}")

    for path in _string_list(structural.get("required_fields")):
        value = _lookup_path(payload, path) if payload is not None else _MISSING
        passed = value is not _MISSING and value not in (None, "", [], {})
        structural_report["required_fields"].append({"path": path, "passed": passed})
        if not passed:
            hard_failures.append(f"missing required field: {path}")

    schema = _normalize_output_schema(structural.get("output_schema"))
    if schema:
        if payload is None:
            structural_report["schema"] = {
                "passed": False,
                "errors": ["structured payload missing"] if payload_error is None else [payload_error],
            }
            hard_failures.append("structured payload missing")
        else:
            schema_errors = _validate_against_schema(payload, schema)
            structural_report["schema"] = {
                "passed": not schema_errors,
                "errors": schema_errors,
            }
            if schema_errors:
                hard_failures.extend(schema_errors)

    assertion_results = _evaluate_assertions(summary, payload, assertions)
    for result in assertion_results:
        if not result["passed"] and str(result.get("severity") or "hard").strip().lower() != "soft":
            hard_failures.append(f"assertion failed: {result['kind']}")
    seen_policy_failures: set[str] = set()
    for violation in repo_policy.get("violations") or []:
        if isinstance(violation, Mapping):
            failure = f"repo policy forbids: {violation.get('rule')}"
            if failure not in seen_policy_failures:
                seen_policy_failures.add(failure)
                hard_failures.append(failure)

    review_criteria = _string_list(review.get("criteria"))
    required_decision = str(review.get("required_decision") or "").strip().lower()
    review_required = bool(review_criteria or required_decision)
    latest_decision = str(latest_review.get("decision") or "").strip().lower()

    review_report = {
        "required": review_required,
        "criteria": review_criteria,
        "required_decision": required_decision or None,
        "latest_decision": latest_decision or None,
        "authoritative": review_required,
    }

    verification_report = {
        "required_refs": verify_refs,
        "attached_refs": verification_artifact_refs,
        "passed": (not verify_refs) or bool(verification_artifact_refs),
    }

    status = "not_requested"
    if contract_requested:
        if hard_failures:
            status = "failed"
        elif verify_refs and not verification_artifact_refs:
            status = "pending_verification"
        elif review_required and not latest_decision:
            status = "pending_review"
        elif required_decision and latest_decision and latest_decision != required_decision:
            status = "failed"
        elif review_required and latest_decision in {"reject", "request_changes"} and not required_decision:
            status = "failed"
        elif review_required and latest_decision:
            status = "passed"
        else:
            status = "passed"

    report = {
        "contract_requested": contract_requested,
        "structural": structural_report,
        "assertions": assertion_results,
        "verification": verification_report,
        "review": review_report,
        "repo_policy": repo_policy,
        "payload_present": payload is not None,
        "payload_error": payload_error,
        "hard_failures": hard_failures,
    }
    if payload is not None:
        report["parsed_payload"] = payload
    return status, report


__all__ = [
    "evaluate_submission_acceptance",
    "infer_artifact_write_scope",
    "normalize_acceptance_contract",
    "normalize_authoring_contract",
    "render_acceptance_contract",
    "render_authoring_contract",
]
