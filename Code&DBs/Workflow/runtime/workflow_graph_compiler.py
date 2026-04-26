"""Compile graph-capable queue specs into WorkflowRequest authority.

This module is the single queue-spec-to-graph compiler for graph-admitted
workflow specs, including the single-prompt dispatch lane. It fails closed on
unsupported lanes instead of teaching each caller its own partial control-flow
dialect.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any
from typing import TYPE_CHECKING

from contracts.domain import (
    MINIMAL_WORKFLOW_NODE_TYPE,
    SUPPORTED_ADAPTER_TYPES,
    SUPPORTED_SCHEMA_VERSION,
    WorkflowEdgeContract,
    WorkflowNodeContract,
    WorkflowRequest,
)
from registry.native_runtime_profile_sync import NativeRuntimeProfileSyncError
from runtime.native_authority import default_native_authority_refs

if TYPE_CHECKING:
    from storage.postgres.connection import SyncPostgresConnection

_GRAPH_RUNTIME_TRIGGER_ADAPTER_TYPES = SUPPORTED_ADAPTER_TYPES - frozenset({
    "cli_llm",
    "llm_task",
})
_STATIC_BRANCHING_KINDS = frozenset({"if", "switch"})
_COMPILE_LOCAL_WORKSPACE_REF = "workspace.compile.unbound"
_COMPILE_LOCAL_RUNTIME_PROFILE_REF = "runtime_profile.compile.unbound"
_ARTIFACT_WRITE_SCOPE_RE = re.compile(
    r"(?<![\w./-])(artifacts/[A-Za-z0-9._@:+-]+(?:/[A-Za-z0-9._@:+-]+)*/?)"
)


def _default_workspace_ref(
    conn: "SyncPostgresConnection | None" = None,
) -> str:
    if conn is not None:
        return default_native_authority_refs(conn)[0]
    try:
        return default_native_authority_refs()[0]
    except (NativeRuntimeProfileSyncError, RuntimeError):
        return _COMPILE_LOCAL_WORKSPACE_REF


def _default_runtime_profile_ref(
    conn: "SyncPostgresConnection | None" = None,
) -> str:
    if conn is not None:
        return default_native_authority_refs(conn)[1]
    try:
        return default_native_authority_refs()[1]
    except (NativeRuntimeProfileSyncError, RuntimeError):
        return _COMPILE_LOCAL_RUNTIME_PROFILE_REF


class GraphWorkflowCompileError(ValueError):
    """Raised when a graph-capable queue spec cannot be lowered safely."""

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


@dataclass(slots=True)
class _CompileState:
    workspace_ref: str
    runtime_profile_ref: str
    nodes: list[WorkflowNodeContract]
    edges: list[WorkflowEdgeContract]
    next_node_index: int = 0
    next_edge_index: int = 0

    def add_node(
        self,
        *,
        node_id: str,
        adapter_type: str,
        display_name: str,
        inputs: Mapping[str, Any],
        expected_outputs: Mapping[str, Any] | None = None,
        template_owner_node_id: str | None = None,
    ) -> WorkflowNodeContract:
        node = WorkflowNodeContract(
            node_id=node_id,
            node_type=MINIMAL_WORKFLOW_NODE_TYPE,
            adapter_type=adapter_type,
            display_name=display_name,
            inputs=dict(inputs),
            expected_outputs=dict(expected_outputs or {}),
            success_condition={"kind": "always"},
            failure_behavior={"kind": "stop"},
            authority_requirements={
                "workspace_ref": self.workspace_ref,
                "runtime_profile_ref": self.runtime_profile_ref,
            },
            execution_boundary={"workspace_ref": self.workspace_ref},
            position_index=self.next_node_index,
            template_owner_node_id=template_owner_node_id,
        )
        self.next_node_index += 1
        self.nodes.append(node)
        return node

    def add_edge(
        self,
        *,
        from_node_id: str,
        to_node_id: str,
        edge_type: str = "after_success",
        release_condition: Mapping[str, Any] | None = None,
        payload_mapping: Mapping[str, Any] | None = None,
        template_owner_node_id: str | None = None,
    ) -> WorkflowEdgeContract:
        edge = WorkflowEdgeContract(
            edge_id=f"edge_{self.next_edge_index}",
            edge_type=edge_type,
            from_node_id=from_node_id,
            to_node_id=to_node_id,
            release_condition=dict(release_condition or {"kind": "always"}),
            payload_mapping=dict(payload_mapping or {}),
            position_index=self.next_edge_index,
            template_owner_node_id=template_owner_node_id,
        )
        self.next_edge_index += 1
        self.edges.append(edge)
        return edge


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _clone_json(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def _is_mapping(value: object) -> bool:
    return isinstance(value, Mapping)


def _as_text(value: object) -> str | None:
    return value if isinstance(value, str) and value.strip() else None


def _string_list(value: object) -> list[str]:
    if not isinstance(value, list):
        return []
    return [
        item.strip()
        for item in value
        if isinstance(item, str) and item.strip()
    ]


def _dependency_specs(job: Mapping[str, Any]) -> list[dict[str, Any]]:
    dependency_edges = job.get("dependency_edges")
    if isinstance(dependency_edges, list):
        specs: list[dict[str, Any]] = []
        for item in dependency_edges:
            if not isinstance(item, Mapping):
                continue
            label = _as_text(item.get("label"))
            if not label:
                continue
            edge_type = _as_text(item.get("edge_type")) or "after_success"
            release_condition = (
                dict(item.get("release_condition"))
                if _is_mapping(item.get("release_condition"))
                else {"kind": "always"}
            )
            specs.append(
                {
                    "label": label,
                    "edge_type": edge_type,
                    "release_condition": release_condition,
                }
            )
        if specs:
            return specs
    return [
        {
            "label": label,
            "edge_type": "after_success",
            "release_condition": {"kind": "always"},
        }
        for label in _string_list(job.get("depends_on"))
    ]


def _job_write_scope(job: Mapping[str, Any]) -> list[str]:
    write_scope = _string_list(job.get("write_scope"))
    if write_scope:
        return write_scope
    scope = job.get("scope")
    if _is_mapping(scope):
        write_scope = _string_list(scope.get("write"))
        if write_scope:
            return write_scope
    write_scope = _string_list(job.get("write"))
    if write_scope:
        return write_scope
    return _infer_artifact_write_scope(job)


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


def _infer_artifact_write_scope(job: Mapping[str, Any]) -> list[str]:
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
    ):
        for text in _artifact_scope_texts(job.get(field_name)):
            for match in _ARTIFACT_WRITE_SCOPE_RE.finditer(text):
                path = match.group(1).rstrip("`'\"),;:.")
                path = path.rstrip("/")
                if path and path not in candidates:
                    candidates.append(path)
    return candidates


def _job_read_scope(job: Mapping[str, Any]) -> list[str]:
    read_scope = _string_list(job.get("read_scope"))
    if read_scope:
        return read_scope
    scope = job.get("scope")
    if not _is_mapping(scope):
        return []
    return _string_list(scope.get("read"))


def _job_verify_refs(job: Mapping[str, Any]) -> list[str]:
    return _string_list(job.get("verify_refs"))


def _concrete_provider_and_model(value: object) -> tuple[str | None, str | None]:
    provider_slug, model_slug = _provider_and_model(value)
    if not provider_slug or not model_slug:
        return None, None
    if provider_slug.strip().lower() in {"auto", "human", "integration"}:
        return None, None
    return provider_slug, model_slug


def _single_job_prompt_graph_candidate(job: Mapping[str, Any]) -> bool:
    if not _as_text(job.get("prompt")):
        return False
    if _is_mapping(job.get("operator")):
        return False
    if isinstance(job.get("template_jobs"), list) or _is_mapping(job.get("branches")):
        return False
    if any(
        key in job
        for key in ("url", "endpoint", "method", "headers", "body", "body_template")
    ):
        return False
    explicit_adapter_type = _as_text(job.get("adapter_type"))
    if explicit_adapter_type:
        return explicit_adapter_type in {"cli_llm", "llm_task"}
    provider_slug, model_slug = _concrete_provider_and_model(job.get("agent"))
    if provider_slug and model_slug:
        return True
    model_value = _as_text(job.get("model"))
    return model_value is not None


def _graph_control_marker(job: Mapping[str, Any]) -> bool:
    explicit_adapter_type = _as_text(job.get("adapter_type"))
    if explicit_adapter_type in _GRAPH_RUNTIME_TRIGGER_ADAPTER_TYPES:
        return True
    if _is_mapping(job.get("operator")):
        return True
    if isinstance(job.get("template_jobs"), list) or _is_mapping(job.get("branches")):
        return True
    if isinstance(job.get("dependency_edges"), list):
        return True
    if any(key in job for key in ("url", "endpoint", "method", "headers", "body", "body_template")):
        return True
    if _is_mapping(job.get("expected_outputs")) and not _as_text(job.get("prompt")):
        return True
    return False


def _graph_nested_control_marker(job: Mapping[str, Any]) -> bool:
    explicit_adapter_type = _as_text(job.get("adapter_type"))
    if explicit_adapter_type == "control_operator":
        return True
    if _is_mapping(job.get("operator")):
        return True
    if isinstance(job.get("template_jobs"), list) or _is_mapping(job.get("branches")):
        return True
    return False


def _job_iter(job: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    nested: list[Mapping[str, Any]] = []
    for branch_jobs in (job.get("branches") or {}).values() if _is_mapping(job.get("branches")) else ():
        if isinstance(branch_jobs, list):
            nested.extend(item for item in branch_jobs if isinstance(item, Mapping))
    template_jobs = job.get("template_jobs")
    if isinstance(template_jobs, list):
        nested.extend(item for item in template_jobs if isinstance(item, Mapping))
    return tuple(nested)


def spec_uses_graph_runtime(spec_dict: Mapping[str, Any]) -> bool:
    jobs = spec_dict.get("jobs")
    if not isinstance(jobs, list):
        return False
    pending = [job for job in jobs if isinstance(job, Mapping)]
    if (
        len(pending) == 1
        and (
            bool(spec_dict.get("graph_runtime_submit"))
            or _single_job_prompt_graph_candidate(pending[0])
        )
    ):
        return True
    while pending:
        job = pending.pop()
        if _graph_control_marker(job):
            return True
        pending.extend(_job_iter(job))
    return False


def _task_type_for_job(job: Mapping[str, Any]) -> str:
    explicit = _as_text(job.get("task_type"))
    if explicit:
        return explicit
    agent = str(job.get("agent") or "").lower()
    if any(token in agent for token in ("classify", "triage", "categor")):
        return "analysis"
    if any(token in agent for token in ("draft", "compose")):
        return "creative"
    if any(token in agent for token in ("review", "judge", "check")):
        return "review"
    if any(token in agent for token in ("architect", "design")):
        return "architecture"
    if any(token in agent for token in ("build", "code", "implement")):
        return "code_generation"
    if "research" in agent:
        return "research"
    return "general"


def _graph_adapter_type(job: Mapping[str, Any]) -> str:
    operator = job.get("operator")
    explicit = _as_text(job.get("adapter_type"))
    if explicit == "control_operator" or _is_mapping(operator):
        return "control_operator"
    if explicit:
        if explicit not in SUPPORTED_ADAPTER_TYPES:
            raise GraphWorkflowCompileError(
                "workflow.graph_job_unsupported",
                f"graph runtime does not support adapter_type={explicit!r}",
                details={"label": job.get("label"), "adapter_type": explicit},
            )
        return explicit
    if any(key in job for key in ("url", "endpoint", "method", "headers", "body", "body_template")):
        return "api_task"
    if _is_mapping(job.get("expected_outputs")) and not _as_text(job.get("prompt")):
        return "deterministic_task"
    if _as_text(job.get("prompt")) or _as_text(job.get("agent")) or _as_text(job.get("model")):
        return "llm_task"
    raise GraphWorkflowCompileError(
        "workflow.graph_job_unsupported",
        "graph runtime could not infer an adapter_type for the job",
        details={"label": job.get("label")},
    )


def _provider_uses_cli_transport(
    provider: str | None,
    model: str | None,
    conn: "SyncPostgresConnection | None",
) -> bool:
    """True iff provider_model_candidates carries a non-empty cli_config.cmd_template.

    Mirrors the rule registry/agent_config.py uses to set
    ExecutionTransport.cli on the AgentConfig. Centralising the check at
    compile time lets the graph compiler emit cli_llm nodes for
    subscription-auth CLI providers (codex, claude, gemini) without
    threading the full AgentRegistry load through every caller.
    """
    if conn is None or not provider or not model:
        return False
    try:
        rows = conn.execute(
            """SELECT cli_config FROM provider_model_candidates
               WHERE provider_slug = $1 AND model_slug = $2 AND status = 'active'
               ORDER BY priority ASC, created_at DESC LIMIT 1""",
            provider,
            model,
        )
    except Exception:
        return False
    if not rows:
        return False
    raw = rows[0].get("cli_config") or {}
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return False
    if not isinstance(raw, Mapping):
        return False
    template = raw.get("cmd_template")
    return bool(template)


def _select_graph_adapter_type(
    job: Mapping[str, Any],
    *,
    conn: "SyncPostgresConnection | None",
) -> str:
    """Return adapter_type, upgrading inferred llm_task to cli_llm when the
    requested provider/model is registered as a CLI transport.

    Spec authors who write `agent: openai/gpt-5-codex` (or any provider
    whose provider_model_candidates row carries a cli_config.cmd_template)
    get cli_llm dispatch automatically. Without the upgrade the inferred
    llm_task default would route through the HTTP API path, which fails
    fast for subscription-auth CLIs that have no env-var credentials.

    Explicit adapter_type from the spec is honoured as-is.
    """
    inferred = _graph_adapter_type(job)
    if inferred != "llm_task":
        return inferred
    if _as_text(job.get("adapter_type")):
        return inferred
    provider, model = _provider_and_model(job.get("agent"))
    if _provider_uses_cli_transport(provider, model, conn):
        return "cli_llm"
    return inferred


def _graph_regular_job_supported(job: Mapping[str, Any]) -> None:
    integration_id = str(job.get("integration_id") or "").strip().lower()
    integration_action = str(job.get("integration_action") or "").strip().lower()
    agent = str(job.get("agent") or "").strip().lower()
    if integration_id == "workflow" or integration_action == "invoke" or agent == "human":
        raise GraphWorkflowCompileError(
            "workflow.graph_job_unsupported",
            "graph runtime does not support workflow invocation or human-only queue jobs yet",
            details={
                "label": job.get("label"),
                "integration_id": integration_id or None,
                "integration_action": integration_action or None,
                "agent": agent or None,
            },
        )
    if integration_id == "notifications":
        raise GraphWorkflowCompileError(
            "workflow.graph_job_unsupported",
            "graph runtime does not support notification fanout queue jobs yet",
            details={"label": job.get("label"), "integration_id": integration_id},
        )


def _provider_and_model(value: object) -> tuple[str | None, str | None]:
    text = _as_text(value)
    if not text:
        return None, None
    if "/" in text:
        provider_slug, model_slug = text.split("/", 1)
        provider_slug = provider_slug.strip() or None
        model_slug = model_slug.strip() or None
        return provider_slug, model_slug
    return None, text


def _compile_llm_inputs(job: Mapping[str, Any], *, display_name: str) -> dict[str, Any]:
    inputs = dict(job.get("inputs") or {}) if _is_mapping(job.get("inputs")) else {}
    prompt = _as_text(job.get("prompt"))
    system_prompt = _as_text(job.get("system_prompt"))
    workdir = _as_text(job.get("workdir"))
    if prompt:
        inputs["prompt"] = prompt
    if system_prompt:
        inputs["system_prompt"] = system_prompt
    if workdir:
        inputs.setdefault("workdir", workdir)
    provider_slug, model_slug = _provider_and_model(job.get("model"))
    agent_provider_slug, agent_model_slug = _concrete_provider_and_model(job.get("agent"))
    chosen_provider_slug = provider_slug or agent_provider_slug
    if chosen_provider_slug:
        inputs.setdefault("provider_slug", chosen_provider_slug)
    chosen_model_slug = model_slug or agent_model_slug
    if chosen_model_slug:
        inputs.setdefault("model_slug", chosen_model_slug)
    elif _as_text(job.get("model")):
        inputs.setdefault("model_slug", _as_text(job.get("model")))
    agent_slug = _as_text(job.get("agent"))
    if agent_slug:
        inputs.setdefault("agent_slug", agent_slug)
    for field_name in ("timeout", "max_tokens", "temperature"):
        value = job.get(field_name)
        if value is not None:
            inputs.setdefault(field_name, value)
    write_scope = _job_write_scope(job)
    if write_scope:
        inputs.setdefault("scope_write", write_scope)
    inputs.setdefault("task_type", _task_type_for_job(job))
    inputs.setdefault("task_name", display_name)
    return inputs


def _compile_api_inputs(job: Mapping[str, Any], *, display_name: str) -> dict[str, Any]:
    inputs = dict(job.get("inputs") or {}) if _is_mapping(job.get("inputs")) else {}
    integration_args = job.get("integration_args")
    integration_args_map = dict(integration_args) if _is_mapping(integration_args) else {}
    url = (
        _as_text(job.get("url"))
        or _as_text(job.get("endpoint"))
        or _as_text(integration_args_map.get("url"))
        or _as_text(integration_args_map.get("endpoint"))
    )
    if not url:
        raise GraphWorkflowCompileError(
            "workflow.graph_job_invalid",
            "graph api_task requires url or endpoint",
            details={"label": job.get("label")},
        )
    inputs.setdefault("url", url)
    method = _as_text(job.get("method")) or _as_text(integration_args_map.get("method")) or "GET"
    inputs.setdefault("method", method)
    headers = job.get("headers") if _is_mapping(job.get("headers")) else integration_args_map.get("headers")
    if _is_mapping(headers):
        inputs.setdefault("headers", dict(headers))
    if "body" in job:
        inputs.setdefault("body", job.get("body"))
    elif "body_template" in job:
        inputs.setdefault("body", job.get("body_template"))
    elif "body" in integration_args_map:
        inputs.setdefault("body", integration_args_map.get("body"))
    elif "body_template" in integration_args_map:
        inputs.setdefault("body", integration_args_map.get("body_template"))
    timeout = job.get("timeout")
    if timeout is None:
        timeout = integration_args_map.get("timeout")
    if timeout is not None:
        inputs.setdefault("timeout", timeout)
    inputs.setdefault("task_name", display_name)
    return inputs


def _compile_regular_node_inputs(
    job: Mapping[str, Any],
    *,
    adapter_type: str,
    display_name: str,
) -> dict[str, Any]:
    if adapter_type in {"llm_task", "cli_llm"}:
        return _compile_llm_inputs(job, display_name=display_name)
    if adapter_type == "api_task":
        return _compile_api_inputs(job, display_name=display_name)
    inputs = dict(job.get("inputs") or {}) if _is_mapping(job.get("inputs")) else {}
    inputs.setdefault("task_name", display_name)
    return inputs


def _compile_single_prompt_dispatch_graph(
    state: _CompileState,
    *,
    job: Mapping[str, Any],
    conn: "SyncPostgresConnection | None" = None,
) -> None:
    label = _job_label(job, fallback="run")
    display_name = _display_name(job, fallback=label)
    adapter_type = _select_graph_adapter_type(job, conn=conn)
    if adapter_type not in {"cli_llm", "llm_task"}:
        raise GraphWorkflowCompileError(
            "workflow.graph_job_unsupported",
            "single prompt graph expansion only supports cli_llm and llm_task jobs",
            details={"label": label, "adapter_type": adapter_type},
        )
    _graph_regular_job_supported(job)

    write_scope = _job_write_scope(job)
    read_scope = _job_read_scope(job)
    verify_refs = _job_verify_refs(job)
    workdir = _as_text(job.get("workdir"))
    system_prompt = _as_text(job.get("system_prompt"))
    prompt = _as_text(job.get("prompt")) or display_name

    llm_inputs = _compile_regular_node_inputs(
        job,
        adapter_type=adapter_type,
        display_name=display_name,
    )
    context_inputs: dict[str, Any] = {
        "task_name": f"{display_name} context",
        "prompt": prompt,
    }
    if read_scope:
        context_inputs["scope_read"] = read_scope
    if write_scope:
        context_inputs["scope_write"] = write_scope
    if workdir:
        context_inputs["workdir"] = workdir
    context_sections = job.get("context_sections")
    if isinstance(context_sections, list) and context_sections:
        context_inputs["context_sections"] = _clone_json(context_sections)
    if system_prompt:
        context_inputs["system_prompt"] = system_prompt
    for field_name in ("provider_slug", "model_slug"):
        value = llm_inputs.get(field_name)
        if isinstance(value, str) and value.strip():
            context_inputs[field_name] = value

    context_node_id = f"{label}__context"
    parser_node_id = f"{label}__parser"
    state.add_node(
        node_id=context_node_id,
        adapter_type="context_compiler",
        display_name=f"{display_name} context",
        inputs=context_inputs,
    )
    state.add_node(
        node_id=label,
        adapter_type=adapter_type,
        display_name=display_name,
        inputs=llm_inputs,
        expected_outputs=(
            dict(job.get("expected_outputs"))
            if _is_mapping(job.get("expected_outputs"))
            else {}
        ),
    )
    state.add_edge(
        from_node_id=context_node_id,
        to_node_id=label,
        payload_mapping={"prompt": "user_message", "system_prompt": "system_message"},
    )
    state.add_node(
        node_id=parser_node_id,
        adapter_type="output_parser",
        display_name="parse output",
        inputs={
            "task_name": "parse output",
            "scope_write": write_scope,
        },
    )
    state.add_edge(
        from_node_id=label,
        to_node_id=parser_node_id,
        payload_mapping={"completion": "completion"},
    )

    previous_node_id = parser_node_id
    if write_scope and workdir:
        writer_node_id = f"{label}__writer"
        state.add_node(
            node_id=writer_node_id,
            adapter_type="file_writer",
            display_name="write files",
            inputs={
                "task_name": "write files",
                "workspace_root": workdir,
            },
        )
        state.add_edge(
            from_node_id=parser_node_id,
            to_node_id=writer_node_id,
            payload_mapping={"code_blocks": "code_blocks"},
        )
        previous_node_id = writer_node_id

    if verify_refs:
        verifier_node_id = f"{label}__verifier"
        verifier_inputs: dict[str, Any] = {
            "task_name": "verify",
            "bindings": verify_refs,
        }
        if workdir:
            verifier_inputs["workdir"] = workdir
        state.add_node(
            node_id=verifier_node_id,
            adapter_type="verifier",
            display_name="verify",
            inputs=verifier_inputs,
        )
        state.add_edge(from_node_id=previous_node_id, to_node_id=verifier_node_id)


def _job_label(job: Mapping[str, Any], *, fallback: str) -> str:
    label = _as_text(job.get("label")) or _as_text(job.get("id")) or _as_text(job.get("name"))
    if not label:
        return fallback
    return label


def _display_name(job: Mapping[str, Any], *, fallback: str) -> str:
    return (
        _as_text(job.get("display_name"))
        or _as_text(job.get("title"))
        or _as_text(job.get("name"))
        or _as_text(job.get("label"))
        or fallback
    )


def _compile_nested_sequence(
    state: _CompileState,
    *,
    jobs: Sequence[Mapping[str, Any]],
    owner_node_id: str,
    scope_label: str,
    template_owner_node_id: str | None,
    conn: "SyncPostgresConnection | None" = None,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    if not jobs:
        raise GraphWorkflowCompileError(
            "workflow.graph_job_invalid",
            "control operator nested job lists must not be empty",
            details={"owner_node_id": owner_node_id, "scope_label": scope_label},
        )
    label_to_node_id: dict[str, str] = {}
    ordered: list[tuple[Mapping[str, Any], str, str]] = []
    for index, nested_job in enumerate(jobs, start=1):
        if _graph_nested_control_marker(nested_job):
            raise GraphWorkflowCompileError(
                "workflow.graph_job_unsupported",
                "nested control operators inside branch/template bodies are not supported yet",
                details={"owner_node_id": owner_node_id, "scope_label": scope_label},
            )
        _graph_regular_job_supported(nested_job)
        nested_label = _job_label(nested_job, fallback=f"{scope_label}_{index}")
        actual_node_id = f"{owner_node_id}__{scope_label}__{nested_label}"
        if nested_label in label_to_node_id:
            raise GraphWorkflowCompileError(
                "workflow.graph_invalid",
                f"duplicate nested job label {nested_label!r} inside {owner_node_id}:{scope_label}",
            )
        label_to_node_id[nested_label] = actual_node_id
        ordered.append((nested_job, nested_label, actual_node_id))
        adapter_type = _select_graph_adapter_type(nested_job, conn=conn)
        state.add_node(
            node_id=actual_node_id,
            adapter_type=adapter_type,
            display_name=_display_name(nested_job, fallback=nested_label),
            inputs=_compile_regular_node_inputs(
                nested_job,
                adapter_type=adapter_type,
                display_name=_display_name(nested_job, fallback=nested_label),
            ),
            expected_outputs=(
                dict(nested_job.get("expected_outputs"))
                if _is_mapping(nested_job.get("expected_outputs"))
                else {}
            ),
            template_owner_node_id=template_owner_node_id,
        )

    root_ids: list[str] = []
    terminal_ids = {actual_node_id for _, _, actual_node_id in ordered}
    for index, (nested_job, nested_label, actual_node_id) in enumerate(ordered):
        dependency_specs = _dependency_specs(nested_job)
        if not dependency_specs and index > 0:
            dependency_specs = [
                {
                    "label": ordered[index - 1][1],
                    "edge_type": "after_success",
                    "release_condition": {"kind": "always"},
                }
            ]
        if not dependency_specs:
            root_ids.append(actual_node_id)
            continue
        for dependency in dependency_specs:
            dependency_label = str(dependency["label"])
            dependency_node_id = label_to_node_id.get(dependency_label)
            if dependency_node_id is None:
                raise GraphWorkflowCompileError(
                    "workflow.graph_invalid",
                    f"unknown nested dependency {dependency_label!r} inside {owner_node_id}:{scope_label}",
                )
            terminal_ids.discard(dependency_node_id)
            state.add_edge(
                from_node_id=dependency_node_id,
                to_node_id=actual_node_id,
                edge_type=str(dependency.get("edge_type") or "after_success"),
                release_condition=(
                    dict(dependency.get("release_condition"))
                    if _is_mapping(dependency.get("release_condition"))
                    else {"kind": "always"}
                ),
                template_owner_node_id=template_owner_node_id,
            )
    if not root_ids:
        raise GraphWorkflowCompileError(
            "workflow.graph_invalid",
            "nested branch/template graph has no root nodes",
            details={"owner_node_id": owner_node_id, "scope_label": scope_label},
        )
    return tuple(root_ids), tuple(sorted(terminal_ids))


def compile_graph_workflow_request(
    spec_dict: Mapping[str, Any],
    *,
    run_id: str | None = None,
    conn: "SyncPostgresConnection | None" = None,
) -> WorkflowRequest:
    if not isinstance(spec_dict, Mapping):
        raise GraphWorkflowCompileError(
            "workflow.graph_invalid",
            "graph-capable workflow spec must be a mapping",
        )
    jobs = spec_dict.get("jobs")
    if not isinstance(jobs, list) or not jobs:
        raise GraphWorkflowCompileError(
            "workflow.graph_invalid",
            "graph-capable workflow spec requires a non-empty jobs array",
        )
    workspace_ref = _as_text(spec_dict.get("workspace_ref")) or _default_workspace_ref(conn)
    runtime_profile_ref = (
        _as_text(spec_dict.get("runtime_profile_ref")) or _default_runtime_profile_ref(conn)
    )
    state = _CompileState(
        workspace_ref=workspace_ref,
        runtime_profile_ref=runtime_profile_ref,
        nodes=[],
        edges=[],
    )

    top_level_jobs = [
        _clone_json(job)
        for job in jobs
        if isinstance(job, Mapping)
    ]
    if len(top_level_jobs) != len(jobs):
        raise GraphWorkflowCompileError(
            "workflow.graph_invalid",
            "jobs entries must all be objects for graph-capable specs",
        )

    if (
        len(top_level_jobs) == 1
        and (
            bool(spec_dict.get("graph_runtime_submit"))
            or _single_job_prompt_graph_candidate(top_level_jobs[0])
        )
    ):
        if not _single_job_prompt_graph_candidate(top_level_jobs[0]):
            raise GraphWorkflowCompileError(
                "workflow.graph_job_unsupported",
                "single-job graph submission requires a prompt-backed cli_llm/llm_task job or a concrete provider/model route",
                details={"label": top_level_jobs[0].get("label")},
            )
        _compile_single_prompt_dispatch_graph(state, job=top_level_jobs[0], conn=conn)
    else:
        top_label_to_node_id: dict[str, str] = {}
        top_level_by_label: dict[str, Mapping[str, Any]] = {}
        branch_terminals_by_operator: dict[str, tuple[str, ...]] = {}
        for index, job in enumerate(top_level_jobs, start=1):
            label = _job_label(job, fallback=f"job_{index}")
            if label in top_label_to_node_id:
                raise GraphWorkflowCompileError(
                    "workflow.graph_invalid",
                    f"duplicate top-level job label {label!r}",
                )
            top_label_to_node_id[label] = label
            top_level_by_label[label] = job

        for index, job in enumerate(top_level_jobs, start=1):
            label = _job_label(job, fallback=f"job_{index}")
            display_name = _display_name(job, fallback=label)
            adapter_type = _select_graph_adapter_type(job, conn=conn)
            if adapter_type != "control_operator":
                _graph_regular_job_supported(job)
            inputs: dict[str, Any]
            if adapter_type == "control_operator":
                operator = job.get("operator")
                if not _is_mapping(operator):
                    raise GraphWorkflowCompileError(
                        "workflow.graph_invalid",
                        "control_operator jobs require an operator mapping",
                        details={"label": label},
                    )
                inputs = {
                    "task_name": display_name,
                    "operator": dict(operator),
                }
                dependency_mode = _as_text(job.get("dependency_mode"))
                if dependency_mode:
                    inputs["dependency_mode"] = dependency_mode
            else:
                inputs = _compile_regular_node_inputs(
                    job,
                    adapter_type=adapter_type,
                    display_name=display_name,
                )
                dependency_mode = _as_text(job.get("dependency_mode"))
                if dependency_mode:
                    inputs["dependency_mode"] = dependency_mode
            state.add_node(
                node_id=label,
                adapter_type=adapter_type,
                display_name=display_name,
                inputs=inputs,
                expected_outputs=(
                    dict(job.get("expected_outputs"))
                    if _is_mapping(job.get("expected_outputs"))
                    else {}
                ),
            )

            if adapter_type != "control_operator":
                continue

            operator = dict(job.get("operator") or {})
            operator_kind = str(operator.get("kind") or "").strip()
            if operator_kind in _STATIC_BRANCHING_KINDS:
                branches = job.get("branches")
                if not _is_mapping(branches):
                    raise GraphWorkflowCompileError(
                        "workflow.graph_invalid",
                        "if/switch control jobs require a branches mapping",
                        details={"label": label, "operator_kind": operator_kind},
                    )
                defined_branches = {
                    branch_name: branch_jobs
                    for branch_name, branch_jobs in dict(branches).items()
                    if isinstance(branch_jobs, list)
                }
                if operator_kind == "switch":
                    case_branches = {
                        str(case.get("branch") or "").strip()
                        for case in operator.get("cases") or ()
                        if isinstance(case, Mapping) and str(case.get("branch") or "").strip()
                    }
                    missing = sorted(branch for branch in defined_branches if branch not in case_branches)
                    if missing:
                        raise GraphWorkflowCompileError(
                            "workflow.graph_invalid",
                            "switch branches must match operator.cases",
                            details={"label": label, "unexpected_branches": missing},
                        )
                branch_terminal_ids: list[str] = []
                for branch_name, branch_jobs in defined_branches.items():
                    roots, terminals = _compile_nested_sequence(
                        state,
                        jobs=tuple(
                            branch_job
                            for branch_job in branch_jobs
                            if isinstance(branch_job, Mapping)
                        ),
                        owner_node_id=label,
                        scope_label=branch_name,
                        template_owner_node_id=None,
                        conn=conn,
                    )
                    for root_id in roots:
                        state.add_edge(
                            from_node_id=label,
                            to_node_id=root_id,
                            release_condition={"branch": branch_name},
                        )
                    branch_terminal_ids.extend(terminals)
                branch_terminals_by_operator[label] = tuple(dict.fromkeys(branch_terminal_ids))
                continue

            template_jobs = job.get("template_jobs")
            if operator_kind in {"foreach", "batch", "repeat_until", "while"}:
                if not isinstance(template_jobs, list):
                    raise GraphWorkflowCompileError(
                        "workflow.graph_invalid",
                        "dynamic control operators require template_jobs",
                        details={"label": label, "operator_kind": operator_kind},
                    )
                _compile_nested_sequence(
                    state,
                    jobs=tuple(
                        template_job
                        for template_job in template_jobs
                        if isinstance(template_job, Mapping)
                    ),
                    owner_node_id=label,
                    scope_label="template",
                    template_owner_node_id=label,
                    conn=conn,
                )
                continue

        for top_job in top_level_jobs:
            label = _job_label(top_job, fallback="job")
            dependency_specs = _dependency_specs(top_job)
            if not dependency_specs:
                continue
            depends_on = [str(dependency["label"]) for dependency in dependency_specs]
            for dependency in dependency_specs:
                dependency_label = str(dependency["label"])
                dependency_node_id = top_label_to_node_id.get(dependency_label)
                if dependency_node_id is None:
                    raise GraphWorkflowCompileError(
                        "workflow.graph_invalid",
                        f"unknown top-level dependency {dependency_label!r}",
                        details={"label": label},
                    )
                dependency_job = top_level_by_label[dependency_label]
                dependency_kind = ""
                if _graph_adapter_type(dependency_job) == "control_operator":
                    dependency_kind = str(
                        (dependency_job.get("operator") or {}).get("kind") or ""
                    ).strip()
                if dependency_kind in _STATIC_BRANCHING_KINDS:
                    branch_terminals = branch_terminals_by_operator.get(dependency_label, ())
                    if not branch_terminals:
                        raise GraphWorkflowCompileError(
                            "workflow.graph_invalid",
                            "branching control operators must expose at least one branch terminal",
                            details={"label": label, "depends_on": dependency_label},
                        )
                    if len(depends_on) > 1:
                        raise GraphWorkflowCompileError(
                            "workflow.graph_job_unsupported",
                            "jobs that continue after if/switch cannot mix that dependency with other parents yet",
                            details={"label": label, "depends_on": depends_on},
                        )
                    target_node = next(node for node in state.nodes if node.node_id == label)
                    if str(target_node.inputs.get("dependency_mode") or "all").strip() != "any":
                        target_index = state.nodes.index(target_node)
                        state.nodes[target_index] = WorkflowNodeContract(
                            node_id=target_node.node_id,
                            node_type=target_node.node_type,
                            adapter_type=target_node.adapter_type,
                            display_name=target_node.display_name,
                            inputs={**dict(target_node.inputs), "dependency_mode": "any"},
                            expected_outputs=dict(target_node.expected_outputs),
                            success_condition=dict(target_node.success_condition),
                            failure_behavior=dict(target_node.failure_behavior),
                            authority_requirements=dict(target_node.authority_requirements),
                            execution_boundary=dict(target_node.execution_boundary),
                            position_index=target_node.position_index,
                            template_owner_node_id=target_node.template_owner_node_id,
                        )
                    for branch_terminal_id in branch_terminals:
                        state.add_edge(from_node_id=branch_terminal_id, to_node_id=label)
                    continue
                state.add_edge(
                    from_node_id=dependency_node_id,
                    to_node_id=label,
                    edge_type=str(dependency.get("edge_type") or "after_success"),
                    release_condition=(
                        dict(dependency.get("release_condition"))
                        if _is_mapping(dependency.get("release_condition"))
                        else {"kind": "always"}
                    ),
                )

    spec_fingerprint = hashlib.sha256(
        json.dumps(_clone_json(spec_dict), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()
    workflow_id = (
        _as_text(spec_dict.get("workflow_id"))
        or _as_text(spec_dict.get("name"))
        or f"workflow.graph.{spec_fingerprint[:12]}"
    )
    request_id = run_id or f"request.graph.{spec_fingerprint[:12]}"
    definition_hash = f"sha256:{spec_fingerprint[:16]}"
    workflow_definition_id = (
        _as_text(spec_dict.get("definition_revision"))
        or f"workflow_definition.graph.{spec_fingerprint[:16]}"
    )
    return WorkflowRequest(
        schema_version=SUPPORTED_SCHEMA_VERSION,
        workflow_id=workflow_id,
        request_id=request_id,
        workflow_definition_id=workflow_definition_id,
        definition_hash=definition_hash,
        workspace_ref=workspace_ref,
        runtime_profile_ref=runtime_profile_ref,
        nodes=tuple(state.nodes),
        edges=tuple(state.edges),
        requested_at=_utc_now(),
    )


__all__ = [
    "GraphWorkflowCompileError",
    "compile_graph_workflow_request",
    "spec_uses_graph_runtime",
]
