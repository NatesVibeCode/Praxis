"""Code-change candidate authoring contract.

The model states intent in a small structured DSL. Runtime code validates
that intent against source context and derives the patch projection. Unified
diff text is an artifact of this module, never the model's source of truth.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import difflib
import hashlib
import json
from pathlib import Path
from typing import Any

from runtime.workspace_paths import to_repo_ref


VALID_EDIT_ACTIONS = frozenset({"full_file_replace", "exact_block_replace"})
VALID_REVIEW_ROUTING = frozenset({"auto_apply", "human_review"})


class CandidateAuthoringError(RuntimeError):
    """Raised when a candidate proposal is not safe to seal."""

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


@dataclass(frozen=True, slots=True)
class CandidatePatchProjection:
    """Runtime-derived patch projection for a structured proposal."""

    intended_files: tuple[str, ...]
    changed_paths: tuple[str, ...]
    operation_set: tuple[dict[str, str], ...]
    unified_diff: str
    patch_sha256: str
    per_file_summary: tuple[dict[str, Any], ...]
    anti_pattern_hits: tuple[dict[str, Any], ...]
    normalized_proposal: dict[str, Any]
    new_contents: dict[str, str]

    def to_json(self) -> dict[str, Any]:
        return {
            "intended_files": list(self.intended_files),
            "changed_paths": list(self.changed_paths),
            "operation_set": [dict(item) for item in self.operation_set],
            "unified_diff": self.unified_diff,
            "patch_sha256": self.patch_sha256,
            "per_file_summary": [dict(item) for item in self.per_file_summary],
            "anti_pattern_hits": [dict(item) for item in self.anti_pattern_hits],
            "proposal_payload": dict(self.normalized_proposal),
        }


def _json_safe(value: object) -> Any:
    return json.loads(json.dumps(value, sort_keys=True, default=str))


def _require_mapping(value: object, *, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise CandidateAuthoringError(
            "code_change_candidate.invalid_payload",
            f"{field_name} must be an object",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _require_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CandidateAuthoringError(
            "code_change_candidate.invalid_payload",
            f"{field_name} must be a non-empty string",
            details={"field": field_name, "value_type": type(value).__name__},
        )
    return value


def _optional_text(value: object, *, field_name: str) -> str | None:
    if value is None:
        return None
    return _require_text(value, field_name=field_name)


def _normalize_path(value: object, *, field_name: str) -> str:
    raw = _require_text(value, field_name=field_name).strip()
    normalized = to_repo_ref(raw)
    if Path(normalized).is_absolute() or normalized.startswith("../") or "/../" in normalized:
        raise CandidateAuthoringError(
            "code_change_candidate.path_outside_repo",
            f"{field_name} must be a repo-relative path",
            details={"field": field_name, "path": raw},
        )
    return normalized


def _ordered_unique(values: Sequence[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return tuple(ordered)


def _is_tracking_doc_path(path: str) -> bool:
    normalized = path.strip("/").lower()
    return (
        normalized == "artifacts"
        or normalized.startswith("artifacts/")
        or "/artifacts/" in f"/{normalized}/"
        or "/packets/" in f"/{normalized}/"
    )


def _anti_pattern_hits(intended_files: Sequence[str]) -> tuple[dict[str, Any], ...]:
    if intended_files and all(_is_tracking_doc_path(path) for path in intended_files):
        return (
            {
                "rule_slug": "tracking_doc_only",
                "severity": "fail",
                "reason_code": "code_change_candidate.tracking_doc_only",
                "message": "candidate only touches tracking/artifact paths",
                "paths": list(intended_files),
            },
        )
    return ()


def _normalize_edit(raw_edit: object, *, index: int) -> dict[str, Any]:
    edit = _require_mapping(raw_edit, field_name=f"edits[{index}]")
    path = _normalize_path(edit.get("file") or edit.get("path"), field_name=f"edits[{index}].file")
    action = _require_text(edit.get("action"), field_name=f"edits[{index}].action").strip().lower()
    if action not in VALID_EDIT_ACTIONS:
        raise CandidateAuthoringError(
            "code_change_candidate.unsupported_edit_action",
            f"edits[{index}].action must be one of {sorted(VALID_EDIT_ACTIONS)}",
            details={"field": f"edits[{index}].action", "action": action},
        )
    normalized: dict[str, Any] = {"file": path, "action": action}
    if action == "full_file_replace":
        new_content = edit.get("new_content")
        if not isinstance(new_content, str):
            raise CandidateAuthoringError(
                "code_change_candidate.invalid_payload",
                f"edits[{index}].new_content must be a string",
                details={"field": f"edits[{index}].new_content"},
            )
        normalized["new_content"] = new_content
    elif action == "exact_block_replace":
        old_block = edit.get("old_block")
        new_block = edit.get("new_block")
        if not isinstance(old_block, str) or old_block == "":
            raise CandidateAuthoringError(
                "code_change_candidate.invalid_payload",
                f"edits[{index}].old_block must be a non-empty string",
                details={"field": f"edits[{index}].old_block"},
            )
        if not isinstance(new_block, str):
            raise CandidateAuthoringError(
                "code_change_candidate.invalid_payload",
                f"edits[{index}].new_block must be a string",
                details={"field": f"edits[{index}].new_block"},
            )
        normalized["old_block"] = old_block
        normalized["new_block"] = new_block
    return normalized


def normalize_candidate_proposal(proposal_payload: object) -> dict[str, Any]:
    """Validate and normalize the V0 candidate proposal payload."""

    proposal = _require_mapping(proposal_payload, field_name="proposal_payload")
    edits_value = proposal.get("edits")
    if not isinstance(edits_value, Sequence) or isinstance(edits_value, (str, bytes, bytearray)):
        raise CandidateAuthoringError(
            "code_change_candidate.empty_proposal",
            "proposal_payload.edits must be a non-empty list",
            details={"field": "proposal_payload.edits"},
        )
    edits = [_normalize_edit(raw_edit, index=index) for index, raw_edit in enumerate(edits_value)]
    if not edits:
        raise CandidateAuthoringError(
            "code_change_candidate.empty_proposal",
            "candidate proposal must contain at least one edit",
        )

    raw_intended = proposal.get("intended_files")
    intended_files: list[str] = []
    if isinstance(raw_intended, Sequence) and not isinstance(raw_intended, (str, bytes, bytearray)):
        intended_files.extend(
            _normalize_path(path, field_name=f"intended_files[{index}]")
            for index, path in enumerate(raw_intended)
        )
    intended_files.extend(str(edit["file"]) for edit in edits)
    normalized_intended = _ordered_unique(intended_files)
    hits = _anti_pattern_hits(normalized_intended)
    if any(hit.get("severity") == "fail" for hit in hits):
        raise CandidateAuthoringError(
            "code_change_candidate.tracking_doc_only",
            "candidate proposals must change source, not only tracking artifacts",
            details={"anti_pattern_hits": list(hits)},
        )

    verifier_ref = _optional_text(proposal.get("verifier_ref"), field_name="proposal_payload.verifier_ref")
    verifier_inputs = proposal.get("verifier_inputs") if isinstance(proposal.get("verifier_inputs"), Mapping) else {}
    return {
        "intended_files": list(normalized_intended),
        "rationale": str(proposal.get("rationale") or "").strip(),
        "edits": edits,
        "verifier_ref": verifier_ref,
        "verifier_inputs": _json_safe(dict(verifier_inputs)),
    }


def normalize_source_context_refs(source_context_refs: object) -> dict[str, str]:
    """Return path -> file content from supported source snapshot shapes."""

    if source_context_refs is None:
        return {}
    context: dict[str, str] = {}

    def add(path_value: object, content_value: object, *, field_name: str) -> None:
        if not isinstance(content_value, str):
            return
        path = _normalize_path(path_value, field_name=field_name)
        context[path] = content_value

    if isinstance(source_context_refs, Mapping):
        for key in ("files", "snapshots", "source_context_refs"):
            nested = source_context_refs.get(key)
            if isinstance(nested, Sequence) and not isinstance(nested, (str, bytes, bytearray)):
                for index, item in enumerate(nested):
                    if not isinstance(item, Mapping):
                        continue
                    path_value = item.get("path") or item.get("file")
                    content_value = item.get("content")
                    if content_value is None:
                        content_value = item.get("text")
                    add(path_value, content_value, field_name=f"{key}[{index}].path")
        for path_value, raw_value in source_context_refs.items():
            if path_value in {"files", "snapshots", "source_context_refs"}:
                continue
            if isinstance(raw_value, str):
                add(path_value, raw_value, field_name="source_context_refs.path")
            elif isinstance(raw_value, Mapping):
                content_value = raw_value.get("content")
                if content_value is None:
                    content_value = raw_value.get("text")
                add(path_value, content_value, field_name="source_context_refs.path")
    elif isinstance(source_context_refs, Sequence) and not isinstance(source_context_refs, (str, bytes, bytearray)):
        for index, item in enumerate(source_context_refs):
            if not isinstance(item, Mapping):
                continue
            content_value = item.get("content")
            if content_value is None:
                content_value = item.get("text")
            add(item.get("path") or item.get("file"), content_value, field_name=f"source_context_refs[{index}].path")

    return context


def _sha256_text(value: str) -> str:
    return f"sha256:{hashlib.sha256(value.encode('utf-8')).hexdigest()}"


def _diff_for_file(path: str, old_content: str, new_content: str) -> str:
    return "".join(
        difflib.unified_diff(
            old_content.splitlines(keepends=True),
            new_content.splitlines(keepends=True),
            fromfile=f"a/{path}",
            tofile=f"b/{path}",
        )
    )


def derive_candidate_patch_from_sources(
    *,
    proposal_payload: object,
    source_context_refs: object,
) -> CandidatePatchProjection:
    """Derive a unified diff from a structured proposal and source snapshots."""

    proposal = normalize_candidate_proposal(proposal_payload)
    source_map = normalize_source_context_refs(source_context_refs)
    current_contents = dict(source_map)
    touched_paths: list[str] = []

    for index, edit in enumerate(proposal["edits"]):
        path = str(edit["file"])
        if path not in current_contents:
            raise CandidateAuthoringError(
                "code_change_candidate.source_context_missing",
                "source_context_refs is missing a full snapshot for an edited file",
                details={"field": f"edits[{index}].file", "path": path},
            )
        old_content = current_contents[path]
        if edit["action"] == "full_file_replace":
            current_contents[path] = str(edit["new_content"])
        elif edit["action"] == "exact_block_replace":
            old_block = str(edit["old_block"])
            match_count = old_content.count(old_block)
            if match_count != 1:
                raise CandidateAuthoringError(
                    "code_change_candidate.old_block_match_failed",
                    "exact_block_replace old_block must match exactly once",
                    details={"path": path, "matches": match_count, "edit_index": index},
                )
            current_contents[path] = old_content.replace(old_block, str(edit["new_block"]), 1)
        touched_paths.append(path)

    intended_files = tuple(proposal["intended_files"])
    changed_paths = tuple(
        path
        for path in _ordered_unique(touched_paths)
        if source_map.get(path, "") != current_contents.get(path, "")
    )
    if not changed_paths:
        raise CandidateAuthoringError(
            "code_change_candidate.no_effective_change",
            "candidate proposal produced no source changes",
            details={"intended_files": list(intended_files)},
        )

    anti_pattern_hits = _anti_pattern_hits(intended_files)
    unified_diff = "".join(
        _diff_for_file(path, source_map[path], current_contents[path])
        for path in changed_paths
    )
    patch_sha256 = _sha256_text(unified_diff)
    per_file_summary = tuple(
        {
            "path": path,
            "action": "update",
            "old_sha256": _sha256_text(source_map[path]),
            "new_sha256": _sha256_text(current_contents[path]),
        }
        for path in changed_paths
    )
    operation_set = tuple({"path": path, "action": "update"} for path in changed_paths)
    return CandidatePatchProjection(
        intended_files=intended_files,
        changed_paths=changed_paths,
        operation_set=operation_set,
        unified_diff=unified_diff,
        patch_sha256=patch_sha256,
        per_file_summary=per_file_summary,
        anti_pattern_hits=anti_pattern_hits,
        normalized_proposal=proposal,
        new_contents={path: current_contents[path] for path in changed_paths},
    )


def source_context_from_worktree(
    *,
    worktree_root: Path,
    intended_files: Sequence[str],
) -> dict[str, str]:
    """Read full file snapshots for intended files from a git worktree."""

    root = worktree_root.resolve()
    context: dict[str, str] = {}
    for path in intended_files:
        repo_ref = _normalize_path(path, field_name="intended_files[]")
        target = (root / repo_ref).resolve()
        try:
            target.relative_to(root)
        except ValueError as exc:
            raise CandidateAuthoringError(
                "code_change_candidate.path_outside_repo",
                "candidate path escaped the worktree root",
                details={"path": repo_ref},
            ) from exc
        try:
            context[repo_ref] = target.read_text(encoding="utf-8")
        except OSError as exc:
            raise CandidateAuthoringError(
                "code_change_candidate.source_context_missing",
                "worktree is missing an edited file",
                details={"path": repo_ref, "error": str(exc)},
            ) from exc
    return context


__all__ = [
    "CandidateAuthoringError",
    "CandidatePatchProjection",
    "VALID_EDIT_ACTIONS",
    "VALID_REVIEW_ROUTING",
    "derive_candidate_patch_from_sources",
    "normalize_candidate_proposal",
    "normalize_source_context_refs",
    "source_context_from_worktree",
]
