"""Canonical workflow spec authority.

This module owns both workflow-spec contracts used in the repo today:

1. Queue specs submitted through workflow command surfaces.
2. Single-run workflow specs consumed by the deterministic runtime.

Queue specs come in two formats:

- **New authoring format**: requires ``name``, ``outcome_goal``,
  ``task_type``, ``authoring_contract``, ``acceptance_contract`` at spec
  level.  Prompts, labels, scope are auto-derived.
- **Legacy format**: requires ``name`` and ``jobs`` with per-job
  ``prompt``.  Still supported but deprecated.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

from runtime.workflow_graph_compiler import (
    GraphWorkflowCompileError,
    compile_graph_workflow_request,
    spec_uses_graph_runtime,
)

if TYPE_CHECKING:
    from .workflow import WorkflowSpec as RuntimeWorkflowSpec

_log = logging.getLogger(__name__)
_PRE_RELOAD_WORKFLOW_SPEC_ERROR = globals().get("WorkflowSpecError")
_PRE_RELOAD_WORKFLOW_SPEC = globals().get("WorkflowSpec")

_HISTORICAL_SPEC_OVERRIDE_ENV = "PRAXIS_ALLOW_HISTORICAL_QUEUE_SPEC"
_POSTGRESQL_DSN_PREFIX = "postgres" + "ql://"
_RETIRED_AUTHORITY_MARKERS: tuple[tuple[str, str], ...] = (
    (
        _POSTGRESQL_DSN_PREFIX + "nate@127.0.0.1:5432/dag_workflow",
        "retired localhost dag_workflow database authority",
    ),
    (
        _POSTGRESQL_DSN_PREFIX + "localhost:5432/praxis",
        "retired localhost Praxis.db authority",
    ),
    (
        "/Users/nate/Praxis",
        "operator-local /Users/nate/Praxis workspace authority",
    ),
    (
        "/Volumes/Users/natha/Documents/Builds/Praxis",
        "operator-local absolute workspace authority",
    ),
)

# ---------------------------------------------------------------------------
# New authoring format field sets
# ---------------------------------------------------------------------------
_AUTHORING_REQUIRED = frozenset({"name", "outcome_goal", "task_type", "authoring_contract", "acceptance_contract"})
_AUTHORING_OPTIONAL_SPEC = frozenset({
    "anti_requirements", "verify_refs",
    # Single-job shorthand fields (promote to spec level)
    "agent", "tier", "capabilities", "allowed_tools", "context_sections", "submission_required",
    "prefer_cost",
})
_AUTHORING_JOB_FIELDS = frozenset({
    "task_type", "authoring_contract", "acceptance_contract", "sprint",
    "agent", "tier", "capabilities", "replicate", "replicate_with",
    "allowed_tools", "verify_refs", "context_sections", "submission_required",
    "prefer_cost",
})
# Fields removed from authoring — rejected with clear messages
_REMOVED_AUTHORING_FIELDS = frozenset({
    "prompt", "label", "output_schema", "scope_read", "scope_write", "read", "write",
    "provider_slug", "model_slug", "adapter_type", "persist", "use_cache",
    "timeout", "max_tokens", "temperature", "max_retries",
    "definition_revision", "plan_revision", "packet_provenance",
    "workspace_ref", "runtime_profile_ref",
    "skip_auto_review", "reviews_workflow_id", "review_target_modules",
    "system_prompt", "phase", "depends_on",
})


class WorkflowSpecError(ValueError):
    """Raised when a workflow spec file is missing or invalid."""


class WorkflowSpec:
    """Parsed queue-spec representation used by workflow submit/validate surfaces."""

    def __init__(
        self,
        *,
        name: str,
        workflow_id: str,
        phase: str,
        jobs: list[dict[str, Any]],
        task_type: str = "",
        verify_refs: list[str] | None = None,
        outcome_goal: str,
        anti_requirements: list[str],
        workspace_ref: str | None = None,
        runtime_profile_ref: str | None = None,
        raw: dict[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.workflow_id = workflow_id
        self.phase = phase
        self.jobs = jobs
        self.task_type = task_type
        self.verify_refs = verify_refs or []
        self.outcome_goal = outcome_goal
        self.anti_requirements = anti_requirements
        self.workspace_ref = workspace_ref
        self.runtime_profile_ref = runtime_profile_ref
        self._raw = raw or {}

    @classmethod
    def load(cls, path: str) -> "WorkflowSpec":
        """Read and validate a queue workflow spec JSON file."""

        _validate_spec_path(path)
        raw = load_raw(path)
        if not isinstance(raw, dict):
            raise WorkflowSpecError(f"Spec file must contain a JSON object: {path}")

        return cls.from_dict(raw)

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "WorkflowSpec":
        """Build a WorkflowSpec from an already-parsed raw dict.

        Mirrors ``load()`` but skips the path validation and JSON read so callers
        holding a dict (standalone scripts, MCP surfaces, ad hoc validation) can
        coerce without writing to disk.
        """
        if not isinstance(raw, dict):
            raise WorkflowSpecError(
                f"WorkflowSpec.from_dict expects a dict; got {type(raw).__name__}"
            )

        if _is_new_authoring_format(raw):
            return cls._load_new_format(raw)

        _log.debug("Loading spec dict in legacy format")
        return cls._load_legacy_format(raw)

    # ------------------------------------------------------------------
    # New authoring format
    # ------------------------------------------------------------------

    @classmethod
    def _load_new_format(cls, raw: dict[str, Any]) -> "WorkflowSpec":
        from adapters.task_profiles import TaskProfileAuthorityError, resolve_profile
        from runtime.prompt_generation import generate_job_prompt

        normalized = dict(raw)

        # --- required spec fields ---
        name = _require_string(normalized, "name")
        outcome_goal = _require_string(normalized, "outcome_goal")
        task_type = _require_string(normalized, "task_type")
        authoring_contract = _require_dict(normalized, "authoring_contract")
        acceptance_contract = _require_dict(normalized, "acceptance_contract")

        anti_requirements = _as_string_list(normalized.get("anti_requirements"))
        verify_refs = _as_string_list(normalized.get("verify_refs"))

        try:
            profile = resolve_profile(task_type)
        except TaskProfileAuthorityError as exc:
            raise WorkflowSpecError(
                "new-format workflow specs require DB-backed task profile authority; "
                f"{exc}"
            ) from exc

        # --- build job list ---
        if "jobs" in normalized:
            raw_jobs = normalized["jobs"]
            if not isinstance(raw_jobs, list) or len(raw_jobs) == 0:
                raise WorkflowSpecError("'jobs' must be a non-empty list")
            jobs: list[dict[str, Any]] = []
            for index, item in enumerate(raw_jobs):
                if not isinstance(item, dict):
                    raise WorkflowSpecError(f"Job '{index}' must be an object")
                jobs.append(dict(item))
        else:
            # Single-job shorthand: spec-level fields are the job
            jobs = [{}]
            # Promote single-job shorthand fields from spec level
            for field in ("agent", "tier", "capabilities", "allowed_tools",
                          "context_sections", "submission_required", "prefer_cost"):
                if field in normalized:
                    jobs[0][field] = normalized[field]

        # --- enrich each job ---
        for index, job in enumerate(jobs):
            # Inherit task_type from spec unless overridden
            job_task_type = str(job.get("task_type") or task_type).strip()
            if job_task_type != task_type:
                try:
                    job_profile = resolve_profile(job_task_type)
                except TaskProfileAuthorityError as exc:
                    raise WorkflowSpecError(
                        "new-format workflow job requires DB-backed task profile authority; "
                        f"{exc}"
                    ) from exc
            else:
                job_profile = profile

            # Contract inheritance: full replace, not merge.
            # If task_type overridden without contract override, try profile defaults first.
            if "authoring_contract" not in job:
                if job.get("task_type") and job_profile is not None and job_profile.default_authoring_contract:
                    job["authoring_contract"] = job_profile.default_authoring_contract
                else:
                    job["authoring_contract"] = authoring_contract
            if "acceptance_contract" not in job:
                if job.get("task_type") and job_profile is not None and job_profile.default_acceptance_contract:
                    job["acceptance_contract"] = job_profile.default_acceptance_contract
                else:
                    job["acceptance_contract"] = acceptance_contract

            # Scope inference from task_type profile
            scope = job.get("scope") or {}
            if not isinstance(scope, dict):
                scope = {}
            if "read" not in scope and job_profile is not None and job_profile.default_scope_read:
                scope["read"] = list(job_profile.default_scope_read)
            if "write" not in scope and job_profile is not None and job_profile.default_scope_write:
                scope["write"] = list(job_profile.default_scope_write)
            if scope:
                job["scope"] = scope

            # Generate prompt
            job["prompt"] = generate_job_prompt(
                outcome_goal=outcome_goal,
                task_type=job_task_type,
                authoring_contract=job["authoring_contract"],
                acceptance_contract=job["acceptance_contract"],
                anti_requirements=anti_requirements,
                scope_read=scope.get("read"),
                scope_write=scope.get("write"),
                verify_refs=_as_string_list(job.get("verify_refs")) or verify_refs,
                system_prompt_hint=job_profile.system_prompt_hint if job_profile is not None else "",
            )

            # Auto-generate label
            job["label"] = f"job_{index + 1}"

            # Defaults
            job["task_type"] = job_task_type
            job.setdefault("agent", normalized.get("agent", "auto/build"))
            if "prefer_cost" not in job and "prefer_cost" in normalized:
                job["prefer_cost"] = normalized["prefer_cost"]
            if "system_prompt" not in job and job_profile is not None and job_profile.system_prompt_hint:
                job["system_prompt"] = job_profile.system_prompt_hint

        # --- ordering ---
        jobs = _expand_replicate_jobs(jobs)
        if any("sprint" in job for job in jobs):
            jobs.sort(key=lambda j: j.get("sprint", 0))
            _generate_sprint_dependencies(jobs)
        elif len(jobs) > 1:
            _generate_sequential_dependencies(jobs)

        return cls(
            name=name,
            workflow_id=_auto_workflow_id(name),
            phase="execute",
            jobs=jobs,
            task_type=task_type,
            verify_refs=verify_refs,
            outcome_goal=outcome_goal,
            anti_requirements=anti_requirements,
            raw=normalized,
        )

    # ------------------------------------------------------------------
    # Legacy format (deprecated — still supported for existing specs)
    # ------------------------------------------------------------------

    @classmethod
    def _load_legacy_format(cls, raw: dict[str, Any]) -> "WorkflowSpec":
        normalized = dict(raw)
        if "workflow_id" not in normalized and "name" in normalized:
            normalized["workflow_id"] = _auto_workflow_id(normalized["name"])

        if "phase" not in normalized:
            normalized["phase"] = "execute"
        missing = [field for field in ("name", "workflow_id", "jobs") if field not in normalized]
        if missing:
            raise WorkflowSpecError(f"Missing required fields: {', '.join(missing)}")

        jobs = normalized["jobs"]
        if not isinstance(jobs, list) or len(jobs) == 0:
            raise WorkflowSpecError("'jobs' must be a non-empty list")

        normalized_jobs: list[dict[str, Any]] = []
        id_to_label: dict[str, str] = {}
        for index, item in enumerate(jobs):
            if not isinstance(item, dict):
                raise WorkflowSpecError(f"Job '{index}' must be an object")

            job = dict(item)
            if "route_id" in job:
                raise WorkflowSpecError(
                    f"Job '{job.get('label', index)}' uses legacy 'route_id'; use 'agent' instead",
                )
            if "verify" in job:
                raise WorkflowSpecError(
                    f"Job '{job.get('label', index)}' uses legacy 'verify'; use 'verify_refs' instead",
                )
            if "label" not in job and "slug" in job:
                job["label"] = job["slug"]
            if "label" not in job:
                sprint = job.get("sprint", index + 1)
                job["label"] = f"sprint_{sprint}_job_{index}"
            # Track id → label mapping for dependency translation
            if "id" in job:
                id_to_label[job["id"]] = job["label"]
            agent = job.get("agent")
            if not isinstance(agent, str) or not agent.strip():
                job["agent"] = "auto/build"
            else:
                job["agent"] = agent.strip()
            if _job_requires_prompt(job) and "prompt" not in job:
                raise WorkflowSpecError(f"Job '{job.get('label', index)}' missing 'prompt'")
            # Normalize depends → depends_on
            if "depends" in job and "depends_on" not in job:
                job["depends_on"] = job.pop("depends")
            # Normalize write_scope / write → scope.write
            if "scope" not in job:
                ws = job.get("write_scope") or job.get("write")
                if ws:
                    job["scope"] = {"write": ws if isinstance(ws, list) else [ws]}

            normalized_jobs.append(job)

        # Translate dependency references from id → label
        if id_to_label:
            for job in normalized_jobs:
                deps = job.get("depends_on")
                if isinstance(deps, list):
                    job["depends_on"] = [id_to_label.get(d, d) for d in deps]

        # Expand replicate jobs (fan-out for count-based, loop for item-based)
        normalized_jobs = _expand_replicate_jobs(normalized_jobs)

        if any("sprint" in job for job in normalized_jobs):
            normalized_jobs.sort(key=lambda job: job.get("sprint", 0))
            _generate_sprint_dependencies(normalized_jobs)

        if "verify" in normalized:
            raise WorkflowSpecError("Spec uses legacy 'verify'; use 'verify_refs' instead")

        return cls(
            name=str(normalized["name"]),
            workflow_id=str(normalized["workflow_id"]),
            phase=str(normalized["phase"]),
            jobs=normalized_jobs,
            verify_refs=_as_string_list(normalized.get("verify_refs")),
            outcome_goal=_as_string(normalized.get("outcome_goal")),
            anti_requirements=_as_string_list(normalized.get("anti_requirements")),
            workspace_ref=_as_optional_string(normalized.get("workspace_ref")),
            runtime_profile_ref=_as_optional_string(normalized.get("runtime_profile_ref")),
            raw=normalized,
        )

    def summary(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "workflow_id": self.workflow_id,
            "phase": self.phase,
            "task_type": self.task_type,
            "job_count": len(self.jobs),
            "verify_count": len(self.verify_refs),
            "verify_ref_count": len(self.verify_refs),
            "outcome_goal": self.outcome_goal,
            "anti_requirements": self.anti_requirements,
            "workspace_ref": self.workspace_ref,
            "runtime_profile_ref": self.runtime_profile_ref,
            "job_labels": [job["label"] for job in self.jobs],
        }


def _preserve_reloaded_class_identity(previous: object, current: type) -> type:
    """Update a pre-reload class object in place when importlib.reload runs.

    The MCP hot-reload path reloads this module while CLI/test callers may
    already hold ``WorkflowSpec`` or ``WorkflowSpecError`` references imported
    from facade modules. Rebinding the module name alone leaves those callers
    pointed at stale classes, so update the old class object with the new
    implementation and keep it as the exported authority.
    """

    if not isinstance(previous, type):
        return current
    for name, value in current.__dict__.items():
        if name in {"__dict__", "__weakref__"}:
            continue
        setattr(previous, name, value)
    return previous


WorkflowSpecError = _preserve_reloaded_class_identity(
    _PRE_RELOAD_WORKFLOW_SPEC_ERROR,
    WorkflowSpecError,
)
WorkflowSpec = _preserve_reloaded_class_identity(
    _PRE_RELOAD_WORKFLOW_SPEC,
    WorkflowSpec,
)


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

def _is_new_authoring_format(raw: dict[str, Any]) -> bool:
    """New format has all required authoring fields at spec level."""
    return all(field in raw for field in _AUTHORING_REQUIRED)


# ---------------------------------------------------------------------------
# Helpers for new format
# ---------------------------------------------------------------------------

def _require_string(raw: dict[str, Any], field: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value.strip():
        raise WorkflowSpecError(f"'{field}' must be a non-empty string")
    return value.strip()


def _require_dict(raw: dict[str, Any], field: str) -> dict[str, Any]:
    value = raw.get(field)
    if not isinstance(value, dict):
        raise WorkflowSpecError(f"'{field}' must be an object")
    return value


def _generate_sequential_dependencies(jobs: list[dict[str, Any]]) -> None:
    """Array order = sequential: job[i+1] depends on job[i]."""
    for i in range(1, len(jobs)):
        if not jobs[i].get("depends_on"):
            prev_label = jobs[i - 1].get("label")
            if prev_label:
                jobs[i]["depends_on"] = [prev_label]


# ---------------------------------------------------------------------------
# Authoring-format validation
# ---------------------------------------------------------------------------

def validate_authoring_spec(raw: dict[str, Any]) -> tuple[bool, list[str]]:
    """Validate a raw dict as a new-format authoring spec."""
    errors: list[str] = []

    for field in sorted(_AUTHORING_REQUIRED):
        if field not in raw:
            errors.append(f"missing required field: {field}")

    if "name" in raw and (not isinstance(raw["name"], str) or not raw["name"].strip()):
        errors.append("name must be a non-empty string")
    if "outcome_goal" in raw and (not isinstance(raw["outcome_goal"], str) or not raw["outcome_goal"].strip()):
        errors.append("outcome_goal must be a non-empty string")
    if "task_type" in raw and (not isinstance(raw["task_type"], str) or not raw["task_type"].strip()):
        errors.append("task_type must be a non-empty string")

    if "authoring_contract" in raw:
        errors.extend(_validate_authoring_contract(raw["authoring_contract"]))
    if "acceptance_contract" in raw:
        errors.extend(_validate_acceptance_contract(raw["acceptance_contract"]))

    if "jobs" in raw:
        if not isinstance(raw["jobs"], list) or len(raw["jobs"]) == 0:
            errors.append("'jobs' must be a non-empty list")
        else:
            for i, job in enumerate(raw["jobs"]):
                if not isinstance(job, dict):
                    errors.append(f"jobs[{i}] must be an object")
                    continue
                unknown_job = set(job.keys()) - _AUTHORING_JOB_FIELDS
                removed_job = unknown_job & _REMOVED_AUTHORING_FIELDS
                for key in sorted(removed_job):
                    errors.append(f"jobs[{i}].{key}: removed from authoring schema (auto-derived at runtime)")
                for key in sorted(unknown_job - removed_job):
                    errors.append(f"jobs[{i}]: unknown field '{key}'")
                if "prefer_cost" in job and job["prefer_cost"] is not None and not isinstance(job["prefer_cost"], bool):
                    errors.append(f"jobs[{i}].prefer_cost must be a boolean or null")

    # Reject removed fields at spec level
    removed_at_spec = set(raw.keys()) & _REMOVED_AUTHORING_FIELDS
    for key in sorted(removed_at_spec):
        errors.append(f"'{key}' is removed from the authoring schema (auto-derived at runtime)")

    if "prefer_cost" in raw and raw["prefer_cost"] is not None and not isinstance(raw["prefer_cost"], bool):
        errors.append("prefer_cost must be a boolean or null")

    return len(errors) == 0, errors


def _generate_sprint_dependencies(jobs: list[dict[str, Any]]) -> None:
    """Add depends_on edges from sprint ordering.

    Sprint N+1 jobs auto-depend on all sprint N jobs, unless the job
    already has explicit depends_on.  Mutates jobs in place.
    """
    by_sprint: dict[int, list[str]] = {}
    for job in jobs:
        sprint = job.get("sprint")
        if sprint is None:
            continue
        label = str(job.get("label") or "")
        if label:
            by_sprint.setdefault(int(sprint), []).append(label)
    if not by_sprint:
        return
    sprints_sorted = sorted(by_sprint)
    for job in jobs:
        sprint = job.get("sprint")
        if sprint is None:
            continue
        if job.get("depends_on"):
            continue  # explicit deps take precedence
        idx = sprints_sorted.index(int(sprint))
        if idx > 0:
            prev_sprint = sprints_sorted[idx - 1]
            job["depends_on"] = list(by_sprint[prev_sprint])


def _expand_replicate_jobs(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Expand jobs with ``replicate`` or ``replicate_with`` into concrete copies.

    **Count-based** (``replicate: N``)::

        {"label": "workers", "replicate": 40, "prompt": "Worker {{WORKER_INDEX}} of {{WORKER_COUNT}}"}

    becomes 40 jobs with ``{{WORKER_INDEX}}`` and ``{{WORKER_COUNT}}`` replaced.

    **Item-based** (``replicate_with: [...]``)::

        {"label": "searches", "replicate_with": ["AI agents", "LLM routing", "tool use"],
         "prompt": "Research: {{ITEM}}"}

    becomes 3 jobs with ``{{ITEM}}``, ``{{WORKER_INDEX}}``, and ``{{WORKER_COUNT}}`` replaced.
    Items can be strings, dicts, or lists — dicts/lists are JSON-serialized.

    Downstream ``depends_on`` references to the original label are rewritten
    to depend on all expanded labels.
    """
    has_replicate = any(
        isinstance(j.get("replicate"), int) or isinstance(j.get("replicate_with"), list)
        for j in jobs
    )
    if not has_replicate:
        return jobs

    # First pass: expand replicate jobs, build label mapping
    expanded: list[dict[str, Any]] = []
    label_expansion: dict[str, list[str]] = {}  # original_label → [expanded_labels]

    for job in jobs:
        items = job.get("replicate_with")
        count = job.get("replicate")

        # replicate_with takes precedence — item-driven loop (for-each)
        if isinstance(items, list) and len(items) > 0:
            count = len(items)
            primitive_kind = "loop"
        elif not isinstance(count, int) or count <= 1:
            expanded.append(job)
            continue
        else:
            items = None  # pure count-based
            primitive_kind = "fanout"

        if count > 200:
            raise WorkflowSpecError(
                f"Job '{job.get('label')}' replicate/replicate_with count={count} exceeds maximum (200)"
            )

        original_label = job["label"]
        width = len(str(count))
        child_labels: list[str] = []

        for i in range(1, count + 1):
            child = dict(job)
            child_label = f"{original_label}_{i:0{width}d}"
            child["label"] = child_label
            child.pop("replicate", None)
            child.pop("replicate_with", None)
            # Count-based bursts require API providers — CLI adapters break
            # under concurrency. Pin adapter_type unless the spec already
            # set one explicitly.
            if primitive_kind == "fanout" and "adapter_type" not in child:
                child["adapter_type"] = "llm_task"

            # Serialize item if present
            item_str = ""
            if items is not None:
                item = items[i - 1]
                if isinstance(item, (dict, list)):
                    item_str = json.dumps(item, ensure_ascii=False)
                else:
                    item_str = str(item)

            # Template substitution in prompt
            prompt = child.get("prompt", "")
            prompt = prompt.replace("{{WORKER_INDEX}}", str(i))
            prompt = prompt.replace("{{WORKER_COUNT}}", str(count))
            if item_str:
                prompt = prompt.replace("{{ITEM}}", item_str)
            child["prompt"] = prompt

            # Same substitution in system_prompt if present
            sys_prompt = child.get("system_prompt")
            if isinstance(sys_prompt, str):
                sys_prompt = sys_prompt.replace("{{WORKER_INDEX}}", str(i))
                sys_prompt = sys_prompt.replace("{{WORKER_COUNT}}", str(count))
                if item_str:
                    sys_prompt = sys_prompt.replace("{{ITEM}}", item_str)
                child["system_prompt"] = sys_prompt

            child_labels.append(child_label)
            expanded.append(child)

        label_expansion[original_label] = child_labels

    # Second pass: rewrite depends_on references to expanded labels
    if label_expansion:
        for job in expanded:
            deps = job.get("depends_on")
            if not isinstance(deps, list):
                continue
            new_deps: list[str] = []
            for dep in deps:
                if dep in label_expansion:
                    new_deps.extend(label_expansion[dep])
                else:
                    new_deps.append(dep)
            job["depends_on"] = new_deps

    return expanded


_PROMPTLESS_GRAPH_ADAPTER_TYPES = {
    "api_task",
    "deterministic_task",
    "control_operator",
    "mcp_task",
    "context_compiler",
    "output_parser",
    "file_writer",
    "verifier",
}


def _job_requires_prompt(job: dict[str, Any]) -> bool:
    adapter_type = job.get("adapter_type")
    if isinstance(adapter_type, str) and adapter_type.strip() in _PROMPTLESS_GRAPH_ADAPTER_TYPES:
        return False
    if isinstance(job.get("operator"), dict):
        return False
    if isinstance(job.get("template_jobs"), list) or isinstance(job.get("branches"), dict):
        return False
    if any(key in job for key in ("url", "endpoint", "method", "headers", "body", "body_template")):
        return False
    return True


def _auto_workflow_id(name: object) -> str:
    if not isinstance(name, str):
        return ""
    return name.lower().replace(" ", "_").replace(":", "")[:40]


def _as_optional_string(value: object) -> str | None:
    return value if isinstance(value, str) else None


def _as_string(value: object) -> str:
    return value if isinstance(value, str) else ""


def _as_string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, str)]


def _as_dict_list(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(item) for item in value if isinstance(item, dict)]


def _resolve_spec_path(path: str) -> Path:
    """Resolve a spec path against cwd; fall back to repo root if relative."""
    file_path = Path(path)
    if not file_path.is_absolute() and not file_path.exists():
        search = Path(__file__).resolve().parent
        while search != search.parent:
            if (search / ".git").exists():
                candidate = search / path
                if candidate.exists():
                    return candidate
                break
            search = search.parent
    return file_path


def _validate_spec_path(path: str) -> Path:
    file_path = _resolve_spec_path(path)
    if not file_path.exists():
        raise WorkflowSpecError(f"Spec file not found: {path}")
    if not file_path.name.endswith(".json"):
        raise WorkflowSpecError(f"Spec file must be .json: {path}")
    return file_path


def _unsafe_psql_instruction(text: str) -> bool:
    lowered = text.lower()
    if "psql" not in lowered:
        return False
    return any(
        marker in lowered
        for marker in (
            _POSTGRESQL_DSN_PREFIX,
            "$workflow_database_url",
            " update ",
            "\nupdate ",
            " insert ",
            "\ninsert ",
            " delete ",
            "\ndelete ",
        )
    )


def _historical_authority_issues(text: str) -> list[str]:
    issues = [
        reason
        for marker, reason in _RETIRED_AUTHORITY_MARKERS
        if marker in text
    ]
    if _unsafe_psql_instruction(text):
        issues.append("direct psql/SQL repair instruction in queue spec")
    return issues


def _validate_spec_file_authority(path: Path, display_path: str, text: str) -> None:
    if os.environ.get(_HISTORICAL_SPEC_OVERRIDE_ENV) == "1":
        return

    issues = _historical_authority_issues(text)
    if not issues:
        return

    formatted = "\n  - ".join(issues)
    raise WorkflowSpecError(
        "Spec file carries historical or retired operator authority and "
        f"cannot be executed as a live workflow: {display_path}\n"
        f"  - {formatted}\n"
        "Use DB-backed workflow authority or regenerate a clean repo-relative "
        f"spec. Set {_HISTORICAL_SPEC_OVERRIDE_ENV}=1 only for explicit "
        "historical evidence inspection."
    )


_KNOWN_ADAPTER_TYPES = {"cli_llm", "llm_task", "deterministic_task"}
_SPEC_FIELD_NAMES = {
    "prompt",
    "provider_slug",
    "model_slug",
    "tier",
    "adapter_type",
    "timeout",
    "workdir",
    "max_tokens",
    "temperature",
    "label",
    "workspace_ref",
    "runtime_profile_ref",
    "system_prompt",
    "context_sections",
    "max_retries",
    "scope_read",
    "scope_write",
    "allowed_tools",
    "verify_refs",
    "definition_revision",
    "plan_revision",
    "packet_provenance",
    "output_schema",
    "authoring_contract",
    "acceptance_contract",
    "max_context_tokens",
    "persist",
    "capabilities",
    "use_cache",
    "task_type",
    "prefer_cost",
    "submission_required",
    "skip_auto_review",
    "reviews_workflow_id",
    "review_target_modules",
}
_ALLOWED_SPEC_KEYS = _SPEC_FIELD_NAMES | {"system_prompt", "context_sections"}


def _validate_authoring_contract(value: object) -> list[str]:
    errors: list[str] = []
    if value is None:
        return errors
    if not isinstance(value, dict):
        return ["authoring_contract must be a dict or null"]
    if "artifact_kind" in value and value["artifact_kind"] is not None and not isinstance(value["artifact_kind"], str):
        errors.append("authoring_contract.artifact_kind must be a string or null")
    if "required_sections" in value:
        sections = value["required_sections"]
        if sections is not None:
            if not isinstance(sections, list):
                errors.append("authoring_contract.required_sections must be a list of strings or null")
            elif not all(isinstance(section, str) and section.strip() for section in sections):
                errors.append("authoring_contract.required_sections entries must all be non-empty strings")
    if "required_fields" in value:
        fields = value["required_fields"]
        if fields is not None:
            if not isinstance(fields, list):
                errors.append("authoring_contract.required_fields must be a list of strings or null")
            elif not all(isinstance(field, str) and field.strip() for field in fields):
                errors.append("authoring_contract.required_fields entries must all be non-empty strings")
    if "output_schema" in value and value["output_schema"] is not None and not isinstance(value["output_schema"], dict):
        errors.append("authoring_contract.output_schema must be a dict or null")
    if "stop_boundary" in value and value["stop_boundary"] is not None and not isinstance(value["stop_boundary"], str):
        errors.append("authoring_contract.stop_boundary must be a string or null")
    if "submission_format" in value and value["submission_format"] is not None and not isinstance(value["submission_format"], str):
        errors.append("authoring_contract.submission_format must be a string or null")
    if "notes" in value:
        notes = value["notes"]
        if notes is not None:
            if not isinstance(notes, list):
                errors.append("authoring_contract.notes must be a list of strings or null")
            elif not all(isinstance(note, str) and note.strip() for note in notes):
                errors.append("authoring_contract.notes entries must all be non-empty strings")
    return errors


def _validate_acceptance_contract(value: object) -> list[str]:
    errors: list[str] = []
    if value is None:
        return errors
    if not isinstance(value, dict):
        return ["acceptance_contract must be a dict or null"]
    structural = value.get("structural")
    if structural is not None:
        if not isinstance(structural, dict):
            errors.append("acceptance_contract.structural must be a dict or null")
        else:
            if "required_sections" in structural:
                sections = structural["required_sections"]
                if sections is not None:
                    if not isinstance(sections, list):
                        errors.append("acceptance_contract.structural.required_sections must be a list of strings or null")
                    elif not all(isinstance(section, str) and section.strip() for section in sections):
                        errors.append("acceptance_contract.structural.required_sections entries must all be non-empty strings")
            if "required_fields" in structural:
                fields = structural["required_fields"]
                if fields is not None:
                    if not isinstance(fields, list):
                        errors.append("acceptance_contract.structural.required_fields must be a list of strings or null")
                    elif not all(isinstance(field, str) and field.strip() for field in fields):
                        errors.append("acceptance_contract.structural.required_fields entries must all be non-empty strings")
            if "output_schema" in structural and structural["output_schema"] is not None and not isinstance(structural["output_schema"], dict):
                errors.append("acceptance_contract.structural.output_schema must be a dict or null")
    if "assertions" in value:
        assertions = value["assertions"]
        if assertions is not None:
            if not isinstance(assertions, list):
                errors.append("acceptance_contract.assertions must be a list of objects or null")
            elif not all(isinstance(assertion, dict) for assertion in assertions):
                errors.append("acceptance_contract.assertions entries must all be objects")
    if "verify_refs" in value:
        verify_refs = value["verify_refs"]
        if verify_refs is not None:
            if not isinstance(verify_refs, list):
                errors.append("acceptance_contract.verify_refs must be a list of strings or null")
            elif not all(isinstance(verify_ref, str) and verify_ref.strip() for verify_ref in verify_refs):
                errors.append("acceptance_contract.verify_refs entries must all be non-empty strings")
    review = value.get("review")
    if review is not None:
        if not isinstance(review, dict):
            errors.append("acceptance_contract.review must be a dict or null")
        else:
            if "criteria" in review:
                criteria = review["criteria"]
                if criteria is not None:
                    if not isinstance(criteria, list):
                        errors.append("acceptance_contract.review.criteria must be a list of strings or null")
                    elif not all(isinstance(item, str) and item.strip() for item in criteria):
                        errors.append("acceptance_contract.review.criteria entries must all be non-empty strings")
            if "required_decision" in review and review["required_decision"] is not None:
                if not isinstance(review["required_decision"], str) or not review["required_decision"].strip():
                    errors.append("acceptance_contract.review.required_decision must be a non-empty string or null")
    return errors

def validate_workflow_spec(raw: dict[str, Any]) -> tuple[bool, list[str]]:
    """Validate a raw dict as a single runtime workflow spec."""

    errors: list[str] = []

    if not isinstance(raw, dict):
        return False, ["spec must be a JSON object"]

    if spec_uses_graph_runtime(raw):
        try:
            compile_graph_workflow_request(raw)
        except GraphWorkflowCompileError as exc:
            return False, [str(exc)]
        return True, []

    if "prompt" not in raw:
        errors.append("missing required field: prompt")
    elif not isinstance(raw["prompt"], str) or not raw["prompt"].strip():
        errors.append("prompt must be a non-empty string")

    if "provider_slug" in raw:
        if not isinstance(raw["provider_slug"], str):
            errors.append("provider_slug must be a string")

    if "model_slug" in raw and raw["model_slug"] is not None and not isinstance(raw["model_slug"], str):
        errors.append("model_slug must be a string or null")

    if "adapter_type" in raw:
        if not isinstance(raw["adapter_type"], str):
            errors.append("adapter_type must be a string")
        elif raw["adapter_type"] not in _KNOWN_ADAPTER_TYPES:
            errors.append(f"adapter_type must be one of {sorted(_KNOWN_ADAPTER_TYPES)}")

    if "timeout" in raw:
        if not isinstance(raw["timeout"], (int, float)):
            errors.append("timeout must be a number")
        elif raw["timeout"] <= 0:
            errors.append("timeout must be positive")

    if "max_tokens" in raw:
        if not isinstance(raw["max_tokens"], int):
            errors.append("max_tokens must be an integer")
        elif raw["max_tokens"] <= 0:
            errors.append("max_tokens must be positive")

    if "temperature" in raw:
        if not isinstance(raw["temperature"], (int, float)):
            errors.append("temperature must be a number")
        elif not (0.0 <= raw["temperature"] <= 2.0):
            errors.append("temperature must be between 0.0 and 2.0")

    if "label" in raw and raw["label"] is not None and not isinstance(raw["label"], str):
        errors.append("label must be a string or null")

    if "capabilities" in raw:
        capabilities = raw["capabilities"]
        if capabilities is not None:
            if not isinstance(capabilities, list):
                errors.append("capabilities must be a list of strings or null")
            elif not all(isinstance(capability, str) for capability in capabilities):
                errors.append("capabilities entries must all be strings")

    if "workdir" in raw and raw["workdir"] is not None and not isinstance(raw["workdir"], str):
        errors.append("workdir must be a string or null")

    if "system_prompt" in raw and raw["system_prompt"] is not None and not isinstance(raw["system_prompt"], str):
        errors.append("system_prompt must be a string or null")

    if "context_sections" in raw:
        context_sections = raw["context_sections"]
        if not isinstance(context_sections, list):
            errors.append("context_sections must be a list")
        else:
            for index, item in enumerate(context_sections):
                if not isinstance(item, dict):
                    errors.append(f"context_sections[{index}] must be an object")
                elif "name" not in item or "content" not in item:
                    errors.append(f"context_sections[{index}] must have 'name' and 'content' keys")

    if "max_retries" in raw:
        if not isinstance(raw["max_retries"], int):
            errors.append("max_retries must be an integer")
        elif raw["max_retries"] < 0:
            errors.append("max_retries must be non-negative")

    for scope_key in ("scope_read", "scope_write"):
        if scope_key not in raw:
            continue
        scope_value = raw[scope_key]
        if scope_value is None:
            continue
        if not isinstance(scope_value, list):
            errors.append(f"{scope_key} must be a list of strings or null")
        elif not all(isinstance(path, str) for path in scope_value):
            errors.append(f"{scope_key} entries must all be strings")

    if "allowed_tools" in raw:
        allowed_tools = raw["allowed_tools"]
        if allowed_tools is not None:
            if not isinstance(allowed_tools, list):
                errors.append("allowed_tools must be a list of strings or null")
            elif not all(isinstance(tool, str) for tool in allowed_tools):
                errors.append("allowed_tools entries must all be strings")

    if "verify_refs" in raw:
        verify_refs = raw["verify_refs"]
        if verify_refs is not None:
            if not isinstance(verify_refs, list):
                errors.append("verify_refs must be a list of strings or null")
            elif not all(isinstance(verify_ref, str) and verify_ref.strip() for verify_ref in verify_refs):
                errors.append("verify_refs entries must all be non-empty strings")

    if "definition_revision" in raw and raw["definition_revision"] is not None:
        if not isinstance(raw["definition_revision"], str) or not raw["definition_revision"].strip():
            errors.append("definition_revision must be a non-empty string or null")

    if "plan_revision" in raw and raw["plan_revision"] is not None:
        if not isinstance(raw["plan_revision"], str) or not raw["plan_revision"].strip():
            errors.append("plan_revision must be a non-empty string or null")

    if "packet_provenance" in raw and raw["packet_provenance"] is not None:
        if not isinstance(raw["packet_provenance"], dict):
            errors.append("packet_provenance must be an object or null")

    if "output_schema" in raw:
        output_schema = raw["output_schema"]
        if output_schema is not None and not isinstance(output_schema, dict):
            errors.append("output_schema must be a dict or null")

    if "authoring_contract" in raw:
        errors.extend(_validate_authoring_contract(raw["authoring_contract"]))

    if "acceptance_contract" in raw:
        errors.extend(_validate_acceptance_contract(raw["acceptance_contract"]))

    if "use_cache" in raw and not isinstance(raw["use_cache"], bool):
        errors.append("use_cache must be a boolean")

    if "task_type" in raw and raw["task_type"] is not None and not isinstance(raw["task_type"], str):
        errors.append("task_type must be a string or null")

    if "prefer_cost" in raw and raw["prefer_cost"] is not None and not isinstance(raw["prefer_cost"], bool):
        errors.append("prefer_cost must be a boolean or null")

    if "submission_required" in raw and raw["submission_required"] is not None and not isinstance(raw["submission_required"], bool):
        errors.append("submission_required must be a boolean or null")

    if "skip_auto_review" in raw and not isinstance(raw["skip_auto_review"], bool):
        errors.append("skip_auto_review must be a boolean")

    if "reviews_workflow_id" in raw and raw["reviews_workflow_id"] is not None:
        if not isinstance(raw["reviews_workflow_id"], str):
            errors.append("reviews_workflow_id must be a string or null")

    if "review_target_modules" in raw:
        review_target_modules = raw["review_target_modules"]
        if review_target_modules is not None:
            if not isinstance(review_target_modules, list):
                errors.append("review_target_modules must be a list of strings or null")
            elif not all(isinstance(module, str) for module in review_target_modules):
                errors.append("review_target_modules entries must all be strings")

    unknown = set(raw.keys()) - _ALLOWED_SPEC_KEYS
    for key in sorted(unknown):
        errors.append(f"unknown field: {key}")

    return len(errors) == 0, errors


def _validate_batch(raw: dict[str, Any]) -> tuple[bool, list[str]]:
    """Validate a raw dict as a batch workflow spec."""

    errors: list[str] = []
    if not isinstance(raw, dict):
        return False, ["batch spec must be a JSON object"]

    if raw.get("kind") != "workflow_batch":
        errors.append('batch spec must have "kind": "workflow_batch"')

    if "jobs" not in raw:
        errors.append("batch spec must have a 'jobs' array")
    elif not isinstance(raw["jobs"], list):
        errors.append("'jobs' must be an array")
    elif len(raw["jobs"]) == 0:
        errors.append("'jobs' array must not be empty")
    else:
        for index, job_raw in enumerate(raw["jobs"]):
            _, job_errors = validate_workflow_spec(job_raw)
            for err in job_errors:
                errors.append(f"jobs[{index}]: {err}")

    if "max_parallel" in raw:
        max_parallel = raw["max_parallel"]
        if not isinstance(max_parallel, int) or max_parallel < 1:
            errors.append("max_parallel must be a positive integer")

    return len(errors) == 0, errors


def _raw_to_runtime_workflow_spec(raw: dict[str, Any]) -> "RuntimeWorkflowSpec":
    from .workflow import WorkflowSpec as RuntimeWorkflowSpec

    kwargs: dict[str, Any] = {
        "prompt": raw["prompt"],
        "model_slug": raw.get("model_slug"),
        "tier": raw.get("tier"),
        "timeout": int(raw.get("timeout", 300)),
        "workdir": raw.get("workdir"),
        "max_tokens": int(raw.get("max_tokens", 4096)),
        "temperature": float(raw.get("temperature", 0.0)),
        "label": raw.get("label"),
        "workspace_ref": raw.get("workspace_ref"),
        "runtime_profile_ref": raw.get("runtime_profile_ref"),
        "system_prompt": raw.get("system_prompt"),
        "context_sections": raw.get("context_sections"),
        "max_retries": int(raw.get("max_retries", 0)),
        "scope_read": raw.get("scope_read"),
        "scope_write": raw.get("scope_write"),
        "allowed_tools": raw.get("allowed_tools"),
        "verify_refs": raw.get("verify_refs"),
        "definition_revision": raw.get("definition_revision"),
        "plan_revision": raw.get("plan_revision"),
        "packet_provenance": raw.get("packet_provenance"),
        "output_schema": raw.get("output_schema"),
        "authoring_contract": raw.get("authoring_contract"),
        "acceptance_contract": raw.get("acceptance_contract"),
        "persist": bool(raw.get("persist", False)),
        "use_cache": bool(raw.get("use_cache", False)),
        "capabilities": raw.get("capabilities"),
        "task_type": raw.get("task_type"),
        "prefer_cost": bool(raw.get("prefer_cost", False)),
        "submission_required": raw.get("submission_required"),
        "skip_auto_review": bool(raw.get("skip_auto_review", False)),
        "reviews_workflow_id": raw.get("reviews_workflow_id"),
        "review_target_modules": raw.get("review_target_modules"),
    }
    if "provider_slug" in raw:
        kwargs["provider_slug"] = raw.get("provider_slug")
    if "adapter_type" in raw:
        kwargs["adapter_type"] = raw.get("adapter_type")
    return RuntimeWorkflowSpec(**kwargs)


def is_batch_spec(raw: dict[str, Any]) -> bool:
    """Return True when the raw dict is a workflow batch spec."""

    return isinstance(raw, dict) and raw.get("kind") == "workflow_batch"


def load_workflow_spec(
    path: str,
    *,
    variables: dict[str, Any] | None = None,
) -> RuntimeWorkflowSpec:
    """Load a single runtime workflow spec from disk."""

    _validate_spec_path(path)
    raw = load_raw(path)
    if is_batch_spec(raw):
        raise WorkflowSpecError(
            f"{path} is a batch spec (kind=workflow_batch). Use load_workflow_batch() instead."
        )

    if variables:
        from .template_engine import render_spec

        raw = render_spec(raw, variables)

    ok, errors = validate_workflow_spec(raw)
    if not ok:
        raise WorkflowSpecError(f"Invalid workflow spec in {path}:\n  " + "\n  ".join(errors))

    return _raw_to_runtime_workflow_spec(raw)


def load_workflow_batch(
    path: str,
    *,
    variables: dict[str, Any] | None = None,
) -> tuple[list[RuntimeWorkflowSpec], int | None]:
    """Load a runtime workflow batch spec from disk."""

    _validate_spec_path(path)
    raw = load_raw(path)
    if variables:
        from .template_engine import render_spec

        raw = render_spec(raw, variables)

    ok, errors = _validate_batch(raw)
    if not ok:
        raise WorkflowSpecError(f"Invalid batch spec in {path}:\n  " + "\n  ".join(errors))

    max_parallel = raw.get("max_parallel")
    specs = [_raw_to_runtime_workflow_spec(job) for job in raw["jobs"]]
    return specs, max_parallel


def load_raw(path: str) -> dict[str, Any]:
    """Load raw JSON from a spec file without additional parsing.

    If the given path is relative and not found under the current working
    directory, walk up from this module to find the repo root (marked by
    `.git`) and try the path there — so `praxis workflow run <spec>` works
    from any cwd.
    """

    file_path = _validate_spec_path(path)
    text = file_path.read_text(encoding="utf-8")
    _validate_spec_file_authority(file_path, path, text)
    loaded = json.loads(text)
    if not isinstance(loaded, dict):
        raise ValueError(f"Spec file must contain a JSON object: {path}")
    return loaded


__all__ = [
    "WorkflowSpec",
    "WorkflowSpecError",
    "is_batch_spec",
    "load_raw",
    "load_workflow_batch",
    "load_workflow_spec",
    "validate_authoring_spec",
    "validate_workflow_spec",
]
