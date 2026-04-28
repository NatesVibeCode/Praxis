"""Read-only workflow pipeline contract evaluator.

This module is deliberately not a provider prober and not a workflow runner.
It consumes the same validation + execution-preview surfaces used before
submission and checks whether the compiled worker contract is internally
consistent enough to launch.
"""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any, Mapping, Sequence

from runtime.workflow.artifact_contracts import infer_artifact_write_scope


_BROAD_TOOLS_REQUIRING_NATIVE_CLAMP = frozenset(
    {
        "praxis_query",
        "praxis_discover",
        "praxis_recall",
        "praxis_graph",
        "praxis_research",
        "praxis_bugs",
        "praxis_receipts",
        "praxis_status_snapshot",
    }
)

_TOOL_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("praxis_query", re.compile(r"\bpraxis(?:\s+workflow)?\s+query\b|\bpraxis_query\b", re.I)),
    ("praxis_discover", re.compile(r"\bpraxis(?:\s+workflow)?\s+discover\b|\bpraxis_discover\b", re.I)),
    ("praxis_recall", re.compile(r"\bpraxis(?:\s+workflow)?\s+recall\b|\bpraxis_recall\b", re.I)),
    ("praxis_bugs", re.compile(r"\bpraxis(?:\s+workflow)?\s+bugs\b|\bpraxis_bugs\b", re.I)),
    ("praxis_receipts", re.compile(r"\bpraxis(?:\s+workflow)?\s+receipts\b|\bpraxis_receipts\b", re.I)),
    ("praxis_search", re.compile(r"\bpraxis(?:\s+workflow)?\s+search\b|\bpraxis_search\b", re.I)),
    ("praxis_context_shard", re.compile(r"\bpraxis(?:\s+workflow)?\s+context_shard\b|\bpraxis_context_shard\b", re.I)),
    ("praxis_orient", re.compile(r"\bpraxis(?:\s+workflow)?\s+orient\b|\bpraxis_orient\b", re.I)),
)

_ARTIFACT_ONLY_EXTENSIONS = frozenset(
    {".md", ".txt", ".json", ".jsonl", ".csv", ".tsv", ".yaml", ".yml"}
)


@dataclass(frozen=True)
class PipelineEvalFinding:
    severity: str
    kind: str
    message: str
    label: str | None = None
    evidence: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "severity": self.severity,
            "kind": self.kind,
            "message": self.message,
        }
        if self.label:
            payload["label"] = self.label
        if self.evidence:
            payload["evidence"] = dict(self.evidence)
        return payload


@dataclass(frozen=True)
class PipelineEvalResult:
    ok: bool
    spec_name: str
    workflow_id: str | None
    total_jobs: int
    error_count: int
    warning_count: int
    findings: tuple[PipelineEvalFinding, ...]
    provider_probe: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "spec_name": self.spec_name,
            "workflow_id": self.workflow_id,
            "total_jobs": self.total_jobs,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "findings": [finding.to_dict() for finding in self.findings],
            "provider_probe": dict(self.provider_probe),
        }


def _text_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        text = value.strip()
        return [text] if text else []
    if not isinstance(value, Sequence) or isinstance(value, (bytes, bytearray)):
        return []
    return [text for item in value if (text := str(item or "").strip())]


def _dict(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _scope_allows_path(path: str, scopes: Sequence[str]) -> bool:
    normalized_path = path.strip().lstrip("./")
    for scope in scopes:
        normalized_scope = str(scope or "").strip().lstrip("./").rstrip("/")
        if not normalized_scope:
            continue
        if normalized_path == normalized_scope:
            return True
        if normalized_path.startswith(normalized_scope + "/"):
            return True
    return False


def _has_shard_scope(shard: Mapping[str, Any], bundle: Mapping[str, Any]) -> bool:
    access_policy = _dict(bundle.get("access_policy"))
    for source in (shard, access_policy):
        for key in (
            "resolved_read_scope",
            "declared_read_scope",
            "write_scope",
            "test_scope",
            "blast_radius",
            "allowed_record_refs",
            "allowed_entity_refs",
        ):
            if _text_list(source.get(key)):
                return True
    return False


def _referenced_tools(prompt: str) -> list[str]:
    found: list[str] = []
    for tool_name, pattern in _TOOL_PATTERNS:
        if pattern.search(prompt or ""):
            found.append(tool_name)
    return found


def _looks_artifact_only(paths: Sequence[str]) -> bool:
    if not paths:
        return False
    for path in paths:
        dot = ""
        basename = path.rsplit("/", 1)[-1]
        if "." in basename:
            dot = "." + basename.rsplit(".", 1)[-1].lower()
        if dot not in _ARTIFACT_ONLY_EXTENSIONS:
            return False
    return True


def _job_by_label(spec: Any) -> dict[str, dict[str, Any]]:
    jobs = getattr(spec, "jobs", ()) or ()
    return {str(job.get("label") or f"job_{index}"): dict(job) for index, job in enumerate(jobs)}


def evaluate_pipeline_preview(
    spec: Any,
    *,
    validation_result: Mapping[str, Any] | None,
    preview_payload: Mapping[str, Any],
) -> PipelineEvalResult:
    """Evaluate a workflow execution preview without launching anything."""

    findings: list[PipelineEvalFinding] = []
    validation = _dict(validation_result)
    preview = _dict(preview_payload)

    if validation and not bool(validation.get("valid")):
        findings.append(
            PipelineEvalFinding(
                severity="error",
                kind=str(validation.get("error_kind") or "validation_failed"),
                message=str(validation.get("error") or "workflow validation failed"),
            )
        )
    for warning in validation.get("preflight_warnings") or ():
        item = _dict(warning)
        severity = str(item.get("severity") or "warning").lower()
        findings.append(
            PipelineEvalFinding(
                severity="error" if severity == "error" else "warning",
                kind=str(item.get("kind") or "validation_preflight"),
                label=str(item.get("label") or "").strip() or None,
                message=str(item.get("message") or item.get("kind") or "validation preflight finding"),
            )
        )

    for warning in preview.get("warnings") or ():
        findings.append(
            PipelineEvalFinding(
                severity="warning",
                kind="preview_warning",
                message=str(warning),
            )
        )

    spec_jobs = _job_by_label(spec)
    preview_jobs = preview.get("jobs") or []
    for preview_job in preview_jobs:
        job_view = _dict(preview_job)
        label = str(job_view.get("label") or "").strip() or None
        source_job = spec_jobs.get(label or "", {})
        prompt = str(source_job.get("prompt") or job_view.get("prompt") or "")
        shard = _dict(job_view.get("execution_context_shard"))
        bundle = _dict(job_view.get("execution_bundle"))
        access_policy = _dict(bundle.get("access_policy"))
        completion = _dict(job_view.get("completion_contract") or bundle.get("completion_contract"))
        write_scope = _text_list(shard.get("write_scope")) or _text_list(access_policy.get("write_scope"))
        allowed_tools = set(_text_list(job_view.get("mcp_tool_names")) or _text_list(bundle.get("mcp_tool_names")))
        inferred_artifact_paths = infer_artifact_write_scope(source_job or job_view)

        scope_error = str(shard.get("scope_resolution_error") or "").strip()
        if scope_error:
            findings.append(
                PipelineEvalFinding(
                    severity="error",
                    kind="scope_resolution_error",
                    label=label,
                    message=scope_error,
                    evidence={"write_scope": write_scope},
                )
            )

        if inferred_artifact_paths and not write_scope:
            findings.append(
                PipelineEvalFinding(
                    severity="error",
                    kind="artifact_write_scope_missing",
                    label=label,
                    message="job names artifact outputs but compiled write_scope is empty",
                    evidence={"artifact_paths": inferred_artifact_paths},
                )
            )
        for path in inferred_artifact_paths:
            if write_scope and not _scope_allows_path(path, write_scope):
                findings.append(
                    PipelineEvalFinding(
                        severity="error",
                        kind="artifact_path_outside_write_scope",
                        label=label,
                        message="prompt/verify artifact path is outside compiled write_scope",
                        evidence={"path": path, "write_scope": write_scope},
                    )
                )

        if inferred_artifact_paths and any(path.startswith("scratch/") for path in write_scope):
            if any(not path.startswith("scratch/") for path in inferred_artifact_paths):
                findings.append(
                    PipelineEvalFinding(
                        severity="error",
                        kind="scratch_fallback_with_artifact_paths",
                        label=label,
                        message="compiled write_scope fell back to scratch while prompt/verify names durable artifact paths",
                        evidence={"artifact_paths": inferred_artifact_paths, "write_scope": write_scope},
                    )
                )

        if completion.get("submission_required"):
            submit_tools = set(_text_list(completion.get("submit_tool_names")))
            if not submit_tools:
                findings.append(
                    PipelineEvalFinding(
                        severity="error",
                        kind="submission_tool_missing",
                        label=label,
                        message="submission_required=true but completion contract has no submit tool",
                    )
                )
            elif allowed_tools and not submit_tools.intersection(allowed_tools):
                findings.append(
                    PipelineEvalFinding(
                        severity="error",
                        kind="submission_tool_not_allowed",
                        label=label,
                        message="completion contract submit tool is not present in compiled MCP tool allowlist",
                        evidence={"submit_tool_names": sorted(submit_tools), "mcp_tool_names": sorted(allowed_tools)},
                    )
                )

        result_kind = str(completion.get("result_kind") or "").strip()
        prompt_lowers = prompt.lower()
        task_type = str(job_view.get("task_type") or source_job.get("task_type") or "").strip().lower()
        if (
            result_kind == "code_change"
            and inferred_artifact_paths
            and _looks_artifact_only(inferred_artifact_paths)
            and ("do not edit code" in prompt_lowers or task_type in {"review", "analysis", "research"})
        ):
            findings.append(
                PipelineEvalFinding(
                    severity="error",
                    kind="artifact_job_uses_code_change_submission",
                    label=label,
                    message="artifact-only job is compiled as code_change instead of artifact_bundle",
                    evidence={"result_kind": result_kind, "artifact_paths": inferred_artifact_paths},
                )
            )

        referenced_tools = _referenced_tools(prompt)
        for tool_name in referenced_tools:
            if allowed_tools and tool_name not in allowed_tools:
                findings.append(
                    PipelineEvalFinding(
                        severity="error",
                        kind="prompt_tool_not_allowed",
                        label=label,
                        message=f"prompt instructs model to use {tool_name}, but the compiled token does not allow it",
                        evidence={"tool": tool_name, "mcp_tool_names": sorted(allowed_tools)},
                    )
                )
            if tool_name in _BROAD_TOOLS_REQUIRING_NATIVE_CLAMP and _has_shard_scope(shard, bundle):
                findings.append(
                    PipelineEvalFinding(
                        severity="error",
                        kind="prompt_tool_scope_not_enforced",
                        label=label,
                        message=f"prompt instructs model to use {tool_name}, but scoped workflow sessions currently fail it closed",
                        evidence={"tool": tool_name},
                    )
                )

        if not str(bundle.get("execution_manifest_ref") or "").strip():
            findings.append(
                PipelineEvalFinding(
                    severity="warning",
                    kind="execution_manifest_ref_missing",
                    label=label,
                    message="compiled bundle has no execution_manifest_ref; retry/freshness checks cannot bind to a manifest revision",
                )
            )

    error_count = sum(1 for finding in findings if finding.severity == "error")
    warning_count = sum(1 for finding in findings if finding.severity == "warning")
    provider_probe = {
        "mode": "not_run",
        "reason": (
            "pipeline eval is read-only. Refresh provider availability through "
            "the canonical heartbeat/provider-probe surface as an explicit operation."
        ),
    }
    return PipelineEvalResult(
        ok=error_count == 0,
        spec_name=str(preview.get("spec_name") or getattr(spec, "name", "") or ""),
        workflow_id=str(preview.get("workflow_id") or getattr(spec, "workflow_id", "") or "").strip() or None,
        total_jobs=int(preview.get("total_jobs") or len(getattr(spec, "jobs", []) or [])),
        error_count=error_count,
        warning_count=warning_count,
        findings=tuple(findings),
        provider_probe=provider_probe,
    )


__all__ = [
    "PipelineEvalFinding",
    "PipelineEvalResult",
    "evaluate_pipeline_preview",
]
