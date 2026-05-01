"""Model Eval suite catalog and workflow-spec import.

The lab intentionally treats normal Workflow specs as fixtures: import the
same spec, bind the same verifier expectations, then vary only the model,
prompt, effort, provider policy, and swarm topology.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from runtime.model_eval.pins import (
    PinnedModelEvalRouteError,
    validate_model_eval_model_config,
)
from runtime.workflow_spec import WorkflowSpec, WorkflowSpecError


DEFAULT_PROMPT_VARIANTS: tuple[dict[str, Any], ...] = (
    {
        "prompt_variant_id": "contract_first",
        "label": "Contract-first",
        "system_suffix": (
            "Optimize for exact contract fidelity. Prefer boring, explicit, "
            "machine-checkable output over clever prose."
        ),
    },
    {
        "prompt_variant_id": "evidence_first",
        "label": "Evidence-first",
        "system_suffix": (
            "Name assumptions and evidence boundaries. Do not promote guesses "
            "into facts."
        ),
    },
    {
        "prompt_variant_id": "terse_instruct",
        "label": "Terse instruct",
        "system_suffix": (
            "Return the smallest complete answer that satisfies the schema. "
            "Avoid commentary."
        ),
    },
)


OPENROUTER_ZDR_ENDPOINT_SOURCE = "openrouter.endpoints.zdr.2026-05-01"


DEFAULT_MODEL_CONFIGS: tuple[dict[str, Any], ...] = (
    {
        "config_id": "gpt-5.4-nano-azure-low",
        "model_slug": "openai/gpt-5.4-nano",
        "agent": "openrouter/openai/gpt-5.4-nano",
        "provider_order": ["azure"],
        "reasoning_effort": "low",
        "temperature": 0.1,
        "supports_seed": True,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["cheap", "docs", "csv", "swarm"],
    },
    {
        "config_id": "gpt-5.4-mini-azure-low",
        "model_slug": "openai/gpt-5.4-mini",
        "agent": "openrouter/openai/gpt-5.4-mini",
        "provider_order": ["azure"],
        "reasoning_effort": "low",
        "temperature": 0.1,
        "supports_seed": True,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["docs", "code", "tool", "workflow"],
    },
    {
        "config_id": "kimi-k2.6-parasail-int4",
        "model_slug": "moonshotai/kimi-k2.6",
        "agent": "openrouter/moonshotai/kimi-k2.6",
        "provider_order": ["parasail/int4"],
        "temperature": 0.1,
        "supports_seed": True,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["docs", "csv", "tool", "swarm", "workflow"],
        "notes": ["Kimi model admitted only through a non-Moonshot ZDR endpoint."],
    },
    {
        "config_id": "qwen3-coder-deepinfra-turbo",
        "model_slug": "qwen/qwen3-coder",
        "agent": "openrouter/qwen/qwen3-coder",
        "provider_order": ["deepinfra/turbo"],
        "temperature": 0.1,
        "supports_seed": True,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["code", "workflow", "tool"],
    },
    {
        "config_id": "qwen3-32b-groq",
        "model_slug": "qwen/qwen3-32b",
        "agent": "openrouter/qwen/qwen3-32b",
        "provider_order": ["groq"],
        "temperature": 0.1,
        "supports_seed": True,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["cheap", "docs", "csv", "swarm"],
    },
    {
        "config_id": "gpt-oss-20b-parasail-fp4",
        "model_slug": "openai/gpt-oss-20b",
        "agent": "openrouter/openai/gpt-oss-20b",
        "provider_order": ["parasail/fp4"],
        "temperature": 0.1,
        "supports_seed": True,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["cheap", "docs", "csv", "workflow"],
    },
    {
        "config_id": "gemma-3-27b-deepinfra-fp8",
        "model_slug": "google/gemma-3-27b-it",
        "agent": "openrouter/google/gemma-3-27b-it",
        "provider_order": ["deepinfra/fp8"],
        "temperature": 0.1,
        "supports_seed": True,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["docs", "csv", "tool", "swarm"],
    },
    {
        "config_id": "llama-4-scout-groq",
        "model_slug": "meta-llama/llama-4-scout",
        "agent": "openrouter/meta-llama/llama-4-scout",
        "provider_order": ["groq"],
        "temperature": 0.1,
        "supports_seed": True,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["cheap", "docs", "csv", "tool", "swarm"],
    },
    {
        "config_id": "nemotron-3-nano-deepinfra-fp4",
        "model_slug": "nvidia/nemotron-3-nano-30b-a3b",
        "agent": "openrouter/nvidia/nemotron-3-nano-30b-a3b",
        "provider_order": ["deepinfra/fp4"],
        "temperature": 0.1,
        "supports_seed": True,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["cheap", "docs", "csv", "tool", "swarm"],
    },
    {
        "config_id": "nemotron-nano-9b-v2-deepinfra-bf16",
        "model_slug": "nvidia/nemotron-nano-9b-v2",
        "agent": "openrouter/nvidia/nemotron-nano-9b-v2",
        "provider_order": ["deepinfra/bf16"],
        "temperature": 0.1,
        "supports_seed": True,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["cheap", "csv", "tool", "swarm"],
    },
    {
        "config_id": "glm-5.1-deepinfra-fp4",
        "model_slug": "z-ai/glm-5.1",
        "agent": "openrouter/z-ai/glm-5.1",
        "provider_order": ["deepinfra/fp4"],
        "temperature": 0.1,
        "supports_seed": True,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["code", "tool", "workflow"],
        "notes": ["GLM model admitted only through a non-Z.AI ZDR endpoint."],
    },
    {
        "config_id": "minimax-m2.7-fireworks",
        "model_slug": "minimax/minimax-m2.7",
        "agent": "openrouter/minimax/minimax-m2.7",
        "provider_order": ["fireworks"],
        "temperature": 0.1,
        "supports_seed": False,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["docs", "csv", "swarm"],
        "notes": ["Endpoint does not support seed; consistency relies on fixed fixture and prompt."],
    },
    {
        "config_id": "deepseek-v4-flash-deepinfra-fp4",
        "model_slug": "deepseek/deepseek-v4-flash",
        "agent": "openrouter/deepseek/deepseek-v4-flash",
        "provider_order": ["deepinfra/fp4"],
        "temperature": 0.1,
        "supports_seed": True,
        "endpoint_source": OPENROUTER_ZDR_ENDPOINT_SOURCE,
        "families": ["cheap", "csv", "workflow"],
        "notes": ["Model allowed; DeepSeek provider remains blocked."],
    },
)


def _artifact_schema_description() -> str:
    return (
        "Return JSON with task_id, answer, and artifacts. artifacts is an "
        "array of {path, media_type, content}. Do not use Markdown fences."
    )


def _praxis_search_tool() -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": "praxis_search",
            "description": "Search Praxis code, decisions, bugs, and knowledge.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string"},
                    "sources": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                },
                "required": ["query"],
            },
        },
    }


def _praxis_workflow_validate_tool() -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": "praxis_workflow_validate",
            "description": "Validate one workflow spec path.",
            "parameters": {
                "type": "object",
                "properties": {"spec_path": {"type": "string"}},
                "required": ["spec_path"],
            },
        },
    }


def _praxis_model_eval_tool() -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": "praxis_model_eval",
            "description": "Plan, run, inspect, compare, promote, or export Model Eval matrices.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["plan", "run", "inspect", "compare", "promote", "export"],
                    },
                    "suite_slugs": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["action"],
            },
        },
    }


def _praxis_bugs_tool() -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": "praxis_bugs",
            "description": "File, inspect, deduplicate, or resolve Praxis bugs.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {"type": "string"},
                    "query": {"type": "string"},
                },
                "required": ["action"],
            },
        },
    }


def _praxis_operator_decisions_tool() -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": "praxis_operator_decisions",
            "description": "List or record canonical operator decisions.",
            "parameters": {
                "type": "object",
                "properties": {
                    "action": {"type": "string", "enum": ["list", "record"]},
                    "decision_kind": {"type": "string"},
                    "active_only": {"type": "boolean"},
                    "limit": {"type": "integer"},
                },
                "required": ["action"],
            },
        },
    }


def _builtin_tasks() -> tuple[dict[str, Any], ...]:
    return (
        {
            "task_id": "doc.user_guide",
            "suite_slug": "docs",
            "family": "structured_doc",
            "validator": "doc_user_guide",
            "max_tokens": 3500,
            "prompt": (
                "Create a user-facing guide for Praxis Model Eval. Audience: "
                "tasteful non-syntax operator. Include exact headings: "
                "# Model Eval User Guide, ## What It Tests, ## Running A Matrix, "
                "## Reading Results, ## Promotion Rules, ## Troubleshooting. "
                "The guide must say production routing is unchanged until an "
                "operator promotes a verified winner. "
                + _artifact_schema_description()
            ),
        },
        {
            "task_id": "doc.closeout",
            "suite_slug": "docs",
            "family": "structured_doc",
            "validator": "structured_doc_headings",
            "expected_artifact": "closeout.md",
            "expected_headings": [
                "# Model Eval Closeout",
                "## Decision",
                "## Evidence",
                "## Unknowns",
                "## Follow-ups",
            ],
            "max_tokens": 2800,
            "prompt": (
                "Create closeout.md for a Model Eval matrix that tested docs, CSV, "
                "tool choice, and swarm work. Include exact headings: # Model Eval "
                "Closeout, ## Decision, ## Evidence, ## Unknowns, ## Follow-ups. "
                "Name at least one unknown/assumption and state that production "
                "routing is unchanged. "
                + _artifact_schema_description()
            ),
        },
        {
            "task_id": "doc.launch_spec",
            "suite_slug": "docs",
            "family": "structured_doc",
            "validator": "structured_doc_headings",
            "expected_artifact": "launch_spec.md",
            "expected_headings": [
                "# Model Eval Launch Spec",
                "## Goal",
                "## Scope",
                "## Inputs",
                "## Acceptance",
                "## Risks",
            ],
            "max_tokens": 3200,
            "prompt": (
                "Create launch_spec.md for launching a repeatable Model Eval suite. "
                "Include exact headings: # Model Eval Launch Spec, ## Goal, "
                "## Scope, ## Inputs, ## Acceptance, ## Risks. Name assumptions "
                "and explicitly say routing unchanged until a separate approval. "
                + _artifact_schema_description()
            ),
        },
        {
            "task_id": "doc.customer_guide",
            "suite_slug": "docs",
            "family": "structured_doc",
            "validator": "structured_doc_headings",
            "expected_artifact": "customer_guide.md",
            "expected_headings": [
                "# Customer Model Selection Guide",
                "## What We Test",
                "## What We Do Not Send",
                "## How Results Are Used",
                "## Open Questions",
            ],
            "max_tokens": 3200,
            "prompt": (
                "Create customer_guide.md explaining how Praxis chooses cheaper "
                "or faster models safely. Include exact headings: # Customer Model "
                "Selection Guide, ## What We Test, ## What We Do Not Send, "
                "## How Results Are Used, ## Open Questions. Mention no production "
                "routing changes from eval alone and name unknowns. "
                + _artifact_schema_description()
            ),
        },
        {
            "task_id": "deck.model_routing_review",
            "suite_slug": "pptx",
            "family": "deck_generation",
            "validator": "pptx_deck_manifest",
            "max_tokens": 3800,
            "prompt": (
                "Create a six-slide PPTX deck manifest for an operator review "
                "of model routing. Return deck.json as an artifact. deck.json "
                "must contain slides[], where each slide has title, bullets[], "
                "speaker_notes, and visual_hint. Cover privacy gate, task lanes, "
                "cost, consistency, promotion, and next experiments. "
                + _artifact_schema_description()
            ),
        },
        {
            "task_id": "deck.real_pptx_smoke",
            "suite_slug": "pptx",
            "family": "deck_generation",
            "validator": "pptx_render",
            "max_tokens": 4200,
            "prompt": (
                "Create a real PPTX artifact named model_eval_review.pptx. Encode "
                "the .pptx bytes as base64 in artifact.content and use media_type "
                "application/vnd.openxmlformats-officedocument.presentationml.presentation. "
                "It must have at least four titled slides covering privacy, cost, "
                "consistency, and promotion. "
                + _artifact_schema_description()
            ),
        },
        {
            "task_id": "csv.extract_accounts",
            "suite_slug": "csv",
            "family": "csv_extraction",
            "validator": "csv_extract_accounts",
            "max_tokens": 2500,
            "prompt": (
                "Extract the account records below into extracted_accounts.csv. "
                "Columns exactly: account_id,owner,status,next_action,risk_score. "
                "Records: A-17 owner Dana status blocked next action \"verify "
                "credential, then retry\" risk 91; B-04 owner Eli status ready "
                "next action launch dry run risk 22; C-88 owner Mo status needs "
                "review next action inspect CSV import risk 64; D-31 owner Rae "
                "status ready next action publish docs risk 18. "
                + _artifact_schema_description()
            ),
        },
        {
            "task_id": "csv.create_rollout",
            "suite_slug": "csv",
            "family": "csv_creation",
            "validator": "csv_create_rollout",
            "max_tokens": 2500,
            "prompt": (
                "Create rollout_plan.csv for five implementation weeks. Columns "
                "exactly: week,workstream,owner,deliverable,done_definition. "
                "Rows must cover docs, PPTX/deck generation, CSV extraction, "
                "tool-use, and swarm testing. "
                + _artifact_schema_description()
            ),
        },
        {
            "task_id": "csv.reconcile_accounts",
            "suite_slug": "csv",
            "family": "csv_reconciliation",
            "validator": "csv_reconcile_accounts",
            "max_tokens": 2800,
            "prompt": (
                "Reconcile account statuses into account_reconciliation.csv. "
                "Columns exactly: account_id,source_a_status,source_b_status,disposition,notes. "
                "Source A: A-17 blocked; B-04 ready; C-88 needs_review; D-31 ready. "
                "Source B: A-17 ready; B-04 ready; C-88 blocked; D-31 ready. "
                "Use disposition conflict where statuses differ and match where equal. "
                "Notes for C-88 must say review. "
                + _artifact_schema_description()
            ),
        },
        {
            "task_id": "xlsx.workbook_transform_manifest",
            "suite_slug": "csv",
            "family": "workbook_transform",
            "validator": "workbook_manifest",
            "max_tokens": 3200,
            "prompt": (
                "Create workbook_manifest.json describing an XLSX workbook transform "
                "with at least two sheets, formulas, a chart, and a recalculation "
                "policy. It should transform rollout rows into a weekly dashboard. "
                + _artifact_schema_description()
            ),
        },
        {
            "task_id": "tool.search_single",
            "suite_slug": "tools",
            "family": "tool_use_easy",
            "validator": "tool_single_search",
            "max_tokens": 400,
            "prompt": (
                "Call the search tool exactly once to look up: model eval authority. "
                "Do not answer in prose. Do not call any other tool."
            ),
            "tools": [_praxis_search_tool()],
        },
        {
            "task_id": "tool.validate_single",
            "suite_slug": "tools",
            "family": "tool_use_easy",
            "validator": "tool_single_validate",
            "max_tokens": 400,
            "prompt": (
                "Call the workflow validation tool exactly once for this spec path: "
                "scratch/model-eval/example.workflow.json. Do not answer in prose."
            ),
            "tools": [_praxis_workflow_validate_tool()],
        },
        {
            "task_id": "tool.model_eval_plan_single",
            "suite_slug": "tools",
            "family": "tool_use_easy",
            "validator": "tool_single_model_eval",
            "max_tokens": 400,
            "prompt": (
                "Call the model eval tool exactly once with action plan for the tools suite. "
                "Do not run the matrix. Do not answer in prose."
            ),
            "tools": [_praxis_model_eval_tool()],
        },
        {
            "task_id": "tool.bugs_search_single",
            "suite_slug": "tools",
            "family": "tool_use_easy",
            "validator": "tool_single_bugs",
            "max_tokens": 400,
            "prompt": (
                "Call the bug tracker tool exactly once to search for model eval bugs. "
                "Use a read/search action. Do not answer in prose."
            ),
            "tools": [_praxis_bugs_tool()],
        },
        {
            "task_id": "tool.operator_decisions_list_single",
            "suite_slug": "tools",
            "family": "tool_use_easy",
            "validator": "tool_single_operator_decisions",
            "max_tokens": 400,
            "prompt": (
                "Call the operator decisions tool exactly once to list active architecture policies. "
                "Do not record a decision. Do not answer in prose."
            ),
            "tools": [_praxis_operator_decisions_tool()],
        },
        {
            "task_id": "tool.choose_read_authority",
            "suite_slug": "tools",
            "family": "tool_choice",
            "validator": "tool_choice_search",
            "max_tokens": 500,
            "prompt": (
                "You need read-only discovery: find the authority decision for Model Eval. "
                "Choose exactly one best tool from the catalog and call it. Do not answer in prose."
            ),
            "tools": [_praxis_search_tool(), _praxis_workflow_validate_tool(), _praxis_bugs_tool()],
        },
        {
            "task_id": "tool.choose_model_eval_authority",
            "suite_slug": "tools",
            "family": "tool_choice",
            "validator": "tool_choice_model_eval",
            "max_tokens": 500,
            "prompt": (
                "You need to plan, not run, a model evaluation matrix for the tools suite. "
                "Choose exactly one best authority tool and call it. Do not answer in prose."
            ),
            "tools": [_praxis_search_tool(), _praxis_workflow_validate_tool(), _praxis_model_eval_tool()],
        },
        {
            "task_id": "tool.choose_specific_types",
            "suite_slug": "tools",
            "family": "tool_sequence",
            "validator": "tool_call_sequence",
            "max_tokens": 1200,
            "prompt": (
                "You are given two tool types: search and validation. Call "
                "exactly one search tool to find workflow model-eval authority, "
                "then exactly one validation tool for a workflow spec path. Do "
                "not answer in prose until after tool calls."
            ),
            "tools": [_praxis_search_tool(), _praxis_workflow_validate_tool()],
        },
        {
            "task_id": "tool.loop_search_then_answer",
            "suite_slug": "tools",
            "family": "tool_execution_loop",
            "run_mode": "tool_execution_loop",
            "validator": "tool_execution_transcript",
            "max_tokens": 1200,
            "prompt": (
                "Call praxis_search for model eval authority, read the tool result, "
                "then return final JSON with a tool_transcript.json artifact. "
                "The transcript must list model turns, tool calls, tool results, "
                "and receipt ids when available. Do not invent tools."
            ),
            "tools": [_praxis_search_tool()],
        },
        {
            "task_id": "swarm.instruct_packet",
            "suite_slug": "swarm",
            "family": "swarm_coordination",
            "validator": "swarm_packet",
            "max_tokens": 3500,
            "prompt": (
                "Design a swarm packet for testing cheap instruct models. "
                "Return swarm_plan.json. It must define four workers with "
                "non-overlapping responsibilities, inputs, outputs, verifier "
                "contracts, and a deterministic reducer. It must include a "
                "budget cap and say that workers cannot change production "
                "routing. "
                + _artifact_schema_description()
            ),
        },
        {
            "task_id": "swarm.reducer_consistency",
            "suite_slug": "swarm",
            "family": "swarm_coordination",
            "validator": "swarm_reducer_packet",
            "max_tokens": 3000,
            "prompt": (
                "Create swarm_reducer.json for four worker outputs: docs, CSV, "
                "tool execution, and PPTX. The reducer must detect overlap, choose "
                "a winner or merge decision, enforce a budget cap, and state that "
                "production routing is unchanged. "
                + _artifact_schema_description()
            ),
        },
    )


def builtin_suite_catalog() -> dict[str, Any]:
    tasks = list(_builtin_tasks())
    suites: dict[str, dict[str, Any]] = {}
    for task in tasks:
        suite = suites.setdefault(
            task["suite_slug"],
            {"suite_slug": task["suite_slug"], "task_count": 0, "task_ids": []},
        )
        suite["task_count"] += 1
        suite["task_ids"].append(task["task_id"])
    return {
        "suites": list(suites.values()),
        "tasks": tasks,
        "default_prompt_variants": list(DEFAULT_PROMPT_VARIANTS),
        "default_model_configs": list(DEFAULT_MODEL_CONFIGS),
    }


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:12]


def catalog_version_hash() -> str:
    payload = {
        "tasks": _builtin_tasks(),
        "prompt_variants": DEFAULT_PROMPT_VARIANTS,
        "model_configs": DEFAULT_MODEL_CONFIGS,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _model_config_errors(configs: list[dict[str, Any]]) -> list[dict[str, str]]:
    errors: list[dict[str, str]] = []
    for index, config in enumerate(configs):
        try:
            validate_model_eval_model_config(config)
        except PinnedModelEvalRouteError as exc:
            errors.append(
                {
                    "index": str(index),
                    "config_id": str(config.get("config_id") or ""),
                    "model_slug": str(config.get("model_slug") or ""),
                    "agent": str(config.get("agent") or config.get("agent_slug") or ""),
                    "error": str(exc),
                }
            )
    return errors


def _default_run_mode(task: dict[str, Any]) -> str:
    if task.get("tools"):
        return "tool_choice_static"
    family = str(task.get("family") or "")
    if family == "swarm_coordination":
        return "swarm"
    if family == "imported_workflow":
        return "workflow_import"
    return "structured_output"


def import_workflow_spec_tasks(path: str, *, limit: int = 20) -> list[dict[str, Any]]:
    spec_path = Path(path).expanduser()
    spec = WorkflowSpec.load(str(spec_path))
    tasks: list[dict[str, Any]] = []
    for index, job in enumerate(spec.jobs[: max(1, limit)]):
        label = str(job.get("label") or f"job_{index + 1}").strip()
        prompt = str(job.get("prompt") or "").strip()
        acceptance = job.get("acceptance_contract") or {}
        if not prompt:
            prompt = json.dumps(job, sort_keys=True)
        task_key = _hash_text(f"{spec_path}:{label}:{prompt}")
        tasks.append(
            {
                "task_id": f"workflow.{spec.workflow_id}.{label}.{task_key}",
                "suite_slug": "workflow-import",
                "family": "imported_workflow",
                "run_mode": "workflow_import",
                "validator": "workflow_job_packet",
                "max_tokens": 4000,
                "source_workflow": {
                    "path": str(spec_path),
                    "name": spec.name,
                    "workflow_id": spec.workflow_id,
                    "task_type": spec.task_type,
                    "job_label": label,
                    "verify_refs": list(spec.verify_refs),
                },
                "prompt": (
                    "You are running a Model Eval consistency pass over an "
                    "imported Praxis workflow job. Use the prompt and acceptance "
                    "contract below, but do not claim the workflow executed. "
                    "Return decision_packet.md as an artifact with headings "
                    "# Workflow Job Packet, ## Imported Spec, ## Proposed Work, "
                    "## Acceptance Evidence, ## Risks, ## Verifier Notes.\n\n"
                    f"WORKFLOW_NAME: {spec.name}\nJOB_LABEL: {label}\n"
                    f"PROMPT:\n{prompt}\n\nACCEPTANCE_CONTRACT:\n"
                    f"{json.dumps(acceptance, sort_keys=True)}\n\n"
                    + _artifact_schema_description()
                ),
            }
        )
    return tasks


def build_suite_plan(
    *,
    suite_slugs: list[str] | None = None,
    workflow_spec_paths: list[str] | None = None,
    prompt_variants: list[dict[str, Any]] | None = None,
    model_configs: list[dict[str, Any]] | None = None,
    max_workflow_jobs: int = 20,
    run_mode: str | None = None,
) -> dict[str, Any]:
    catalog = builtin_suite_catalog()
    selected_slugs = set(suite_slugs or [])
    if not selected_slugs:
        selected_slugs = {"docs", "pptx", "csv", "tools", "swarm"}
    tasks = [
        dict(task)
        for task in catalog["tasks"]
        if str(task.get("suite_slug")) in selected_slugs
    ]
    for task in tasks:
        task.setdefault("run_mode", _default_run_mode(task))
    import_errors: list[dict[str, str]] = []
    for spec_path in workflow_spec_paths or []:
        try:
            tasks.extend(import_workflow_spec_tasks(spec_path, limit=max_workflow_jobs))
        except (OSError, WorkflowSpecError, ValueError) as exc:
            import_errors.append({"path": spec_path, "error": str(exc)})
    selected_run_mode = str(run_mode or "").strip()
    if selected_run_mode:
        tasks = [task for task in tasks if str(task.get("run_mode") or "") == selected_run_mode]

    variants = prompt_variants or list(DEFAULT_PROMPT_VARIANTS)
    configs = model_configs or list(DEFAULT_MODEL_CONFIGS)
    model_config_errors = _model_config_errors(configs)
    matrix_count = len(tasks) * max(1, len(variants)) * max(1, len(configs))
    return {
        "ok": not import_errors and not model_config_errors,
        "authority": "authority.model_eval",
        "task_count": len(tasks),
        "model_config_count": len(configs),
        "prompt_variant_count": len(variants),
        "matrix_count": matrix_count,
        "tasks": tasks,
        "model_configs": configs,
        "prompt_variants": variants,
        "import_errors": import_errors,
        "model_config_errors": model_config_errors,
        "catalog_version_hash": catalog_version_hash(),
        "consistency_contract": {
            "fixed": ["workflow_spec", "task_prompt", "fixture", "validator"],
            "varied": [
                "agent",
                "model_slug",
                "provider_order",
                "prompt_variant",
                "reasoning_effort",
                "temperature",
                "tool_policy",
                "swarm_topology",
            ],
            "promotion_rule": (
                "Model Eval receipts never mutate task_type_routing; promotion "
                "requires an explicit operator action."
            ),
        },
    }


__all__ = [
    "DEFAULT_MODEL_CONFIGS",
    "DEFAULT_PROMPT_VARIANTS",
    "builtin_suite_catalog",
    "catalog_version_hash",
    "build_suite_plan",
    "import_workflow_spec_tasks",
]
