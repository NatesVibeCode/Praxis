from __future__ import annotations

import json
from pathlib import Path

from runtime.model_eval.catalog import build_suite_plan, catalog_version_hash
import pytest

from runtime.model_eval import runner as model_eval_runner
from runtime.model_eval.openrouter import BLOCKED_PROVIDER_SLUGS, OpenRouterError, build_lab_request
from runtime.model_eval.pins import (
    PinnedModelEvalRouteError,
    validate_model_eval_model_config,
    validate_pinned_agent_slug,
)
from runtime.model_eval.validators import validate_task_output
from runtime.operations.commands.model_eval import (
    ModelEvalBenchmarkIngestCommand,
    handle_model_eval_benchmark_ingest,
)
from runtime.workflow._admission import _model_eval_worker_contract_error


def _pinned_config(model_slug: str = "x") -> dict[str, object]:
    return {
        "config_id": "m",
        "model_slug": model_slug,
        "agent": f"provider/{model_slug}",
        "provider_order": ["provider"],
    }


def test_model_eval_plan_defaults_have_consistency_contract() -> None:
    plan = build_suite_plan(suite_slugs=["docs", "csv"], model_configs=[_pinned_config()])

    assert plan["ok"] is True
    assert plan["task_count"] == 8
    assert "workflow_spec" in plan["consistency_contract"]["fixed"]
    assert "agent" in plan["consistency_contract"]["varied"]
    assert "model_slug" in plan["consistency_contract"]["varied"]
    assert "task_type_routing" in plan["consistency_contract"]["promotion_rule"]
    assert len(plan["catalog_version_hash"]) == 64


def test_model_eval_catalog_version_hash_is_deterministic() -> None:
    assert catalog_version_hash() == catalog_version_hash()


def test_model_eval_tool_suite_has_ladder_tasks() -> None:
    plan = build_suite_plan(suite_slugs=["tools"], model_configs=[_pinned_config()])
    task_ids = {task["task_id"] for task in plan["tasks"]}

    assert plan["task_count"] == 9
    assert "tool.search_single" in task_ids
    assert "tool.validate_single" in task_ids
    assert "tool.model_eval_plan_single" in task_ids
    assert "tool.bugs_search_single" in task_ids
    assert "tool.operator_decisions_list_single" in task_ids
    assert "tool.loop_search_then_answer" in task_ids
    assert "tool.choose_specific_types" in task_ids


def test_model_eval_default_configs_are_pinned() -> None:
    plan = build_suite_plan(suite_slugs=["docs"])

    assert plan["ok"] is True
    assert plan["model_config_errors"] == []
    for config in plan["model_configs"]:
        agent = validate_model_eval_model_config(config)
        assert agent.startswith("openrouter/")


def test_model_eval_rejects_auto_chat_or_missing_worker_agents() -> None:
    for agent in ["", "auto/model_eval_worker", "auto/chat", "chat", "testing/chat"]:
        with pytest.raises(PinnedModelEvalRouteError):
            validate_pinned_agent_slug(agent)


def test_model_eval_rejects_blocked_direct_provider_but_allows_model_family_pin() -> None:
    with pytest.raises(PinnedModelEvalRouteError):
        validate_pinned_agent_slug("deepseek/deepseek-v4-flash")

    assert (
        validate_model_eval_model_config(
            {
                "config_id": "deepseek-model-via-openrouter",
                "model_slug": "deepseek/deepseek-v4-flash",
                "agent": "openrouter/deepseek/deepseek-v4-flash",
                "provider_order": ["deepinfra/fp4"],
            }
        )
        == "openrouter/deepseek/deepseek-v4-flash"
    )


def test_model_eval_worker_workflow_contract_rejects_unpinned_routes() -> None:
    assert (
        _model_eval_worker_contract_error(
            {
                "task_type": "model_eval_worker",
                "agent": "openrouter/google/gemini-2.5-flash",
                "model_eval_candidate_ref": "candidate.openrouter.google-gemini-2.5-flash",
            }
        )
        is None
    )
    assert "forbids auto" in str(
        _model_eval_worker_contract_error(
            {
                "task_type": "model_eval_worker",
                "agent": "auto/model_eval_worker",
                "model_eval_candidate_ref": "candidate.auto",
            }
        )
    )
    assert "requires a concrete agent" in str(
        _model_eval_worker_contract_error(
            {
                "task_type": "model_eval_worker",
                "model_eval_candidate_ref": "candidate.missing",
            }
        )
    )
    assert "requires model_eval_candidate_ref" in str(
        _model_eval_worker_contract_error(
            {
                "task_type": "model_eval_worker",
                "agent": "openrouter/google/gemini-2.5-flash",
            }
        )
    )


def test_model_eval_worker_has_no_task_type_routing_seed() -> None:
    migration_root = Path(__file__).resolve().parents[3] / "Databases" / "migrations" / "workflow"
    offenders = []
    for path in migration_root.glob("*.sql"):
        text = path.read_text(encoding="utf-8")
        if "model_eval_worker" in text and (
            "task_type_routing" in text or "task_type_route" in text
        ):
            offenders.append(path.name)

    assert offenders == []


def test_model_eval_openrouter_request_is_privacy_locked() -> None:
    body = build_lab_request(
        model_slug="openai/gpt-5.4-nano",
        provider_order=["azure"],
        system_prompt="system",
        user_prompt="user",
        max_tokens=200,
        reasoning_effort="low",
    )

    provider = body["provider"]
    assert provider["data_collection"] == "deny"
    assert provider["zdr"] is True
    assert provider["allow_fallbacks"] is False
    assert provider["require_parameters"] is True
    assert "canvasshot" in BLOCKED_PROVIDER_SLUGS
    assert "deepseek" in BLOCKED_PROVIDER_SLUGS
    assert body["max_completion_tokens"] == 200
    assert body["reasoning"] == {"effort": "low"}


def test_model_eval_rejects_free_or_disallowed_provider_routes() -> None:
    with pytest.raises(OpenRouterError):
        build_lab_request(
            model_slug="qwen/qwen3-coder:free",
            provider_order=[],
            system_prompt="system",
            user_prompt="user",
            max_tokens=200,
        )
    with pytest.raises(OpenRouterError):
        build_lab_request(
            model_slug="qwen/qwen3-coder",
            provider_order=[],
            system_prompt="system",
            user_prompt="user",
            max_tokens=200,
        )
    with pytest.raises(OpenRouterError):
        build_lab_request(
            model_slug="deepseek/deepseek-v4-flash",
            provider_order=["deepseek"],
            system_prompt="system",
            user_prompt="user",
            max_tokens=200,
        )


def test_csv_extract_validator_checks_exact_fixture() -> None:
    task = {"validator": "csv_extract_accounts"}
    payload = {
        "task_id": "csv.extract_accounts",
        "answer": "done",
        "artifacts": [
            {
                "path": "extracted_accounts.csv",
                "media_type": "text/csv",
                "content": (
                    "account_id,owner,status,next_action,risk_score\n"
                    "A-17,Dana,blocked,\"verify credential, then retry\",91\n"
                    "B-04,Eli,ready,launch dry run,22\n"
                    "C-88,Mo,needs review,inspect CSV import,64\n"
                    "D-31,Rae,ready,publish docs,18\n"
                ),
            }
        ],
    }

    result = validate_task_output(task, payload)

    assert result["ok"] is True
    assert result["score"] == 1.0


def test_csv_validator_rejects_overflow_without_crashing() -> None:
    task = {"validator": "csv_create_rollout"}
    payload = {
        "task_id": "csv.create_rollout",
        "answer": "done",
        "artifacts": [
            {
                "path": "rollout_plan.csv",
                "media_type": "text/csv",
                "content": (
                    "week,workstream,owner,deliverable,done_definition\n"
                    "1,docs,Ari,guide,published,extra\n"
                    "2,pptx,Bea,deck,approved\n"
                    "3,csv,Cam,extractor,verified\n"
                    "4,tool,Dev,catalog,validated\n"
                    "5,swarm,Eli,reducer,scored\n"
                ),
            }
        ],
    }

    result = validate_task_output(task, payload)

    assert result["ok"] is False
    assert any(check["check"] == "no CSV overflow columns" and not check["ok"] for check in result["checks"])


def test_new_doc_csv_workbook_and_swarm_validators_score_expected_artifacts() -> None:
    doc_result = validate_task_output(
        {
            "validator": "structured_doc_headings",
            "expected_artifact": "closeout.md",
            "expected_headings": ["# Model Eval Closeout", "## Evidence", "## Unknowns"],
        },
        {
            "artifacts": [
                {
                    "path": "closeout.md",
                    "media_type": "text/markdown",
                    "content": (
                        "# Model Eval Closeout\n\n## Evidence\n"
                        + ("Evidence text. " * 60)
                        + "\n## Unknowns\nAssumption: route prices may change.\n"
                        "Production routing is unchanged.\n"
                    ),
                }
            ]
        },
    )
    reconcile_result = validate_task_output(
        {"validator": "csv_reconcile_accounts"},
        {
            "artifacts": [
                {
                    "path": "account_reconciliation.csv",
                    "media_type": "text/csv",
                    "content": (
                        "account_id,source_a_status,source_b_status,disposition,notes\n"
                        "A-17,blocked,ready,conflict,status mismatch\n"
                        "B-04,ready,ready,match,no change\n"
                        "C-88,needs_review,blocked,conflict,review required\n"
                        "D-31,ready,ready,match,no change\n"
                    ),
                }
            ]
        },
    )
    workbook_result = validate_task_output(
        {"validator": "workbook_manifest"},
        {
            "artifacts": [
                {
                    "path": "workbook_manifest.json",
                    "media_type": "application/json",
                    "content": json.dumps(
                        {
                            "sheets": [
                                {"name": "Data", "formulas": ["=SUM(A1:A4)"]},
                                {"name": "Dashboard", "charts": [{"type": "bar"}]},
                            ],
                            "calculation": "recalc on open",
                        }
                    ),
                }
            ]
        },
    )
    swarm_result = validate_task_output(
        {"validator": "swarm_reducer_packet"},
        {
            "artifacts": [
                {
                    "path": "swarm_reducer.json",
                    "media_type": "application/json",
                    "content": json.dumps(
                        {
                            "worker_outputs": ["docs", "csv", "tool", "pptx"],
                            "overlap_detection": "compare touched sections",
                            "budget": {"cap": 1.25},
                            "decision": "merge non-overlapping outputs",
                            "boundary": "production routing unchanged",
                        }
                    ),
                }
            ]
        },
    )

    assert doc_result["ok"] is True
    assert reconcile_result["ok"] is True
    assert workbook_result["ok"] is True
    assert swarm_result["ok"] is True


def test_runner_records_validator_exception_as_row_failure(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    def fake_chat_completion(_request, *, timeout_seconds: int) -> dict:
        return {
            "provider": "azure",
            "model": "openai/gpt-5.4-nano",
            "choices": [
                {
                    "message": {
                        "content": json.dumps(
                            {
                                "artifacts": [
                                    {
                                        "path": "rollout_plan.csv",
                                        "media_type": "text/csv",
                                        "content": "week,workstream,owner,deliverable,done_definition\n",
                                    }
                                ]
                            }
                        )
                    }
                }
            ],
            "usage": {"cost": 0},
        }

    def exploding_validator(_task, _payload) -> dict:
        raise RuntimeError("boom")

    monkeypatch.setattr(model_eval_runner, "chat_completion", fake_chat_completion)
    monkeypatch.setattr(model_eval_runner, "validate_task_output", exploding_validator)

    result = model_eval_runner._run_one(
        task={
            "task_id": "csv.create_rollout",
            "family": "csv",
            "suite_slug": "csv",
            "prompt": "create csv",
            "validator": "csv_create_rollout",
        },
        model_config={
            "config_id": "nano",
            "model_slug": "openai/gpt-5.4-nano",
            "agent": "openrouter/openai/gpt-5.4-nano",
            "provider_order": ["azure"],
        },
        prompt_variant={"prompt_variant_id": "contract_first"},
        output_root=tmp_path,
        timeout_seconds=1,
        dry_run=False,
    )

    assert result["ok"] is False
    assert result["status"] == "verification_failed"
    assert result["verification"]["checks"][0]["check"] == "validator exception"


def test_runner_uses_deterministic_seed() -> None:
    assert model_eval_runner._seed("config", "task", "variant") == model_eval_runner._seed(
        "config",
        "task",
        "variant",
    )
    assert model_eval_runner._seed("config", "task", "variant") != model_eval_runner._seed(
        "config",
        "task",
        "other",
    )


def test_runner_fails_closed_on_served_provider_mismatch(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    def fake_chat_completion(_request, *, timeout_seconds: int) -> dict:
        return {
            "provider": "canvasshot",
            "model": "canvasshotai/kimi-k2.6",
            "choices": [{"message": {"content": json.dumps({"task_id": "x", "answer": "ok", "artifacts": []})}}],
            "usage": {"cost": 0},
        }

    monkeypatch.setattr(model_eval_runner, "chat_completion", fake_chat_completion)

    result = model_eval_runner._run_one(
        task={
            "task_id": "doc.user_guide",
            "family": "structured_doc",
            "suite_slug": "docs",
            "prompt": "create guide",
            "validator": "doc_user_guide",
        },
        model_config={
            "config_id": "kimi",
            "model_slug": "canvasshotai/kimi-k2.6",
            "agent": "openrouter/canvasshotai/kimi-k2.6",
            "provider_order": ["parasail"],
        },
        prompt_variant={"prompt_variant_id": "contract_first"},
        output_root=tmp_path,
        timeout_seconds=1,
        dry_run=False,
    )

    assert result["ok"] is False
    assert result["status"] == "route_mismatch"


def test_runner_rejects_unpinned_case_before_api_call(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    def fake_chat_completion(_request, *, timeout_seconds: int) -> dict:
        raise AssertionError("unpinned model_eval_worker must not call the API")

    monkeypatch.setattr(model_eval_runner, "chat_completion", fake_chat_completion)

    result = model_eval_runner._run_one(
        task={
            "task_id": "doc.user_guide",
            "family": "structured_doc",
            "suite_slug": "docs",
            "prompt": "create guide",
            "validator": "doc_user_guide",
        },
        model_config={"config_id": "missing-agent", "model_slug": "openai/gpt-5.4-nano"},
        prompt_variant={"prompt_variant_id": "contract_first"},
        output_root=tmp_path,
        timeout_seconds=1,
        dry_run=False,
    )

    assert result["ok"] is False
    assert result["status"] == "permission_refused"
    assert "concrete agent" in result["error"]


def test_runner_rejects_blocked_pinned_provider_before_api_call(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    def fake_chat_completion(_request, *, timeout_seconds: int) -> dict:
        raise AssertionError("blocked provider must not call the API")

    monkeypatch.setattr(model_eval_runner, "chat_completion", fake_chat_completion)

    result = model_eval_runner._run_one(
        task={
            "task_id": "csv.create_rollout",
            "family": "csv_creation",
            "suite_slug": "csv",
            "prompt": "create csv",
            "validator": "csv_create_rollout",
        },
        model_config={
            "config_id": "blocked-provider",
            "model_slug": "deepseek-v4-flash",
            "agent": "deepseek/deepseek-v4-flash",
            "provider_order": ["deepinfra/fp4"],
        },
        prompt_variant={"prompt_variant_id": "contract_first"},
        output_root=tmp_path,
        timeout_seconds=1,
        dry_run=False,
    )

    assert result["ok"] is False
    assert result["status"] == "privacy_rejected"
    assert "blocked" in result["error"]


def test_tool_execution_loop_records_transcript_and_gateway_receipt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path,
) -> None:
    calls: list[dict] = []

    def fake_chat_completion(request, *, timeout_seconds: int) -> dict:
        if any(message.get("role") == "tool" for message in request.get("messages", [])):
            return {
                "provider": "azure",
                "model": "openai/gpt-5.4-nano",
                "choices": [
                    {
                        "message": {
                            "content": json.dumps(
                                {
                                    "task_id": "tool.loop_search_then_answer",
                                    "answer": "searched",
                                    "artifacts": [],
                                }
                            )
                        }
                    }
                ],
                "usage": {"cost": 0},
            }
        return {
            "provider": "azure",
            "model": "openai/gpt-5.4-nano",
            "choices": [
                {
                    "message": {
                        "tool_calls": [
                            {
                                "id": "call_1",
                                "type": "function",
                                "function": {
                                    "name": "praxis_search",
                                    "arguments": json.dumps({"query": "model eval authority"}),
                                },
                            }
                        ]
                    }
                }
            ],
            "usage": {"cost": 0},
        }

    def fake_execute_operation_from_subsystems(_subsystems, *, operation_name, payload, requested_mode):
        calls.append(
            {
                "operation_name": operation_name,
                "payload": payload,
                "requested_mode": requested_mode,
            }
        )
        return {
            "ok": True,
            "operation_name": operation_name,
            "results": [],
            "operation_receipt": {"receipt_id": "33333333-3333-3333-3333-333333333333"},
        }

    import runtime.operation_catalog_gateway as gateway

    monkeypatch.setattr(model_eval_runner, "chat_completion", fake_chat_completion)
    monkeypatch.setattr(gateway, "execute_operation_from_subsystems", fake_execute_operation_from_subsystems)

    result = model_eval_runner._run_one(
        task={
            "task_id": "tool.loop_search_then_answer",
            "family": "tool_execution_loop",
            "suite_slug": "tools",
            "run_mode": "tool_execution_loop",
            "prompt": "call search then answer",
            "validator": "tool_execution_transcript",
            "tools": [
                {
                    "type": "function",
                    "function": {
                        "name": "praxis_search",
                        "parameters": {"type": "object", "properties": {"query": {"type": "string"}}},
                    },
                }
            ],
        },
        model_config={
            "config_id": "nano",
            "model_slug": "openai/gpt-5.4-nano",
            "agent": "openrouter/openai/gpt-5.4-nano",
            "provider_order": ["azure"],
        },
        prompt_variant={"prompt_variant_id": "contract_first"},
        output_root=tmp_path,
        timeout_seconds=2,
        dry_run=False,
        subsystems=object(),
    )

    assert result["ok"] is True
    assert result["status"] == "verified"
    assert calls[0]["operation_name"] == "search.federated"
    transcript = result["tool_transcript"]
    assert any(step.get("receipt_id") == "33333333-3333-3333-3333-333333333333" for step in transcript["steps"])


def test_matrix_dispatches_case_runs_through_gateway(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:
    calls: list[dict] = []

    class Context:
        cause_receipt_id = "11111111-1111-1111-1111-111111111111"

    class Subsystems:
        pass

    def fake_current_caller_context():
        return Context()

    def fake_execute_operation_from_subsystems(_subsystems, *, operation_name, payload, requested_mode):
        calls.append(
            {
                "operation_name": operation_name,
                "payload": payload,
                "requested_mode": requested_mode,
            }
        )
        return {
            "ok": True,
            "status": "verified",
            "score": 1.0,
            "cost": 0,
            "task_id": payload["task"]["task_id"],
            "task_family": payload["task"]["family"],
            "suite_slug": payload["task"]["suite_slug"],
            "config_id": payload["model_config"]["config_id"],
            "model_slug": payload["model_config"]["model_slug"],
            "prompt_variant_id": payload["prompt_variant"]["prompt_variant_id"],
            "operation_receipt": {"receipt_id": "22222222-2222-2222-2222-222222222222"},
        }

    import runtime.operation_catalog_gateway as gateway

    monkeypatch.setattr(gateway, "current_caller_context", fake_current_caller_context)
    monkeypatch.setattr(gateway, "execute_operation_from_subsystems", fake_execute_operation_from_subsystems)
    monkeypatch.setattr(model_eval_runner, "_repo_root", lambda: tmp_path)

    result = model_eval_runner.run_model_eval_matrix(
        suite_slugs=["docs"],
        model_configs=[_pinned_config()],
        prompt_variants=[{"prompt_variant_id": "p"}],
        max_runs=1,
        dry_run=True,
        run_label="unit-child-dispatch",
        subsystems=Subsystems(),
    )

    assert result["executed_count"] == 1
    assert calls[0]["operation_name"] == "model_eval_run_case"
    assert calls[0]["requested_mode"] == "command"
    assert calls[0]["payload"]["matrix_receipt_id"] == Context.cause_receipt_id


def test_model_eval_run_kickoff_returns_inspectable_lab_run_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from surfaces.mcp.tools import model_eval as model_eval_tool

    executed: list[dict[str, object]] = []

    def fake_spawn_threaded(target, *, name: str, daemon: bool = True):
        executed.append({"thread_name": name, "daemon": daemon})
        target()

    def fake_execute_operation_from_env(*, env, operation_name, payload, caller_context):
        executed.append(
            {
                "operation_name": operation_name,
                "payload": payload,
                "cause_receipt_id": caller_context.cause_receipt_id,
                "correlation_id": caller_context.correlation_id,
                "transport_kind": caller_context.transport_kind,
            }
        )
        return {"ok": True}

    class FakeUuid:
        hex = "abcdef1234567890"

        def __str__(self) -> str:
            return "00000000-0000-0000-0000-abcdef123456"

    monkeypatch.setattr(model_eval_tool.uuid, "uuid4", lambda: FakeUuid())
    monkeypatch.setattr(model_eval_tool.time, "time", lambda: 123.0)
    monkeypatch.setattr(model_eval_tool, "spawn_threaded", fake_spawn_threaded)
    monkeypatch.setattr(
        model_eval_tool,
        "execute_operation_from_env",
        fake_execute_operation_from_env,
    )
    monkeypatch.setattr(model_eval_tool, "workflow_database_env", lambda: {"x": "y"})

    result = model_eval_tool._kickoff_model_eval_run({"suite_slugs": ["docs"], "dry_run": True})

    assert result["ok"] is True
    assert result["lab_run_id"] == "model-eval-abcdef123456"
    assert result["inspect_input"] == {
        "action": "inspect",
        "lab_run_id": "model-eval-abcdef123456",
    }
    assert executed[1]["operation_name"] == "model_eval_run_matrix"
    assert executed[1]["payload"]["run_label"] == "model-eval-abcdef123456"
    assert executed[1]["transport_kind"] == "workflow"


def test_model_eval_run_tool_executes_dry_runs_inline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from surfaces.mcp.tools import model_eval as model_eval_tool

    calls: list[dict[str, object]] = []

    class FakeUuid:
        hex = "fedcba6543210000"

        def __str__(self) -> str:
            return "00000000-0000-0000-0000-fedcba654321"

    def fake_execute_operation_from_env(*, env, operation_name, payload, caller_context):
        calls.append(
            {
                "operation_name": operation_name,
                "payload": payload,
                "transport_kind": caller_context.transport_kind,
            }
        )
        return {"ok": True, "executed_count": 1}

    def fake_kickoff(_payload):
        raise AssertionError("dry_run must not use short-lived daemon kickoff")

    monkeypatch.setattr(model_eval_tool.uuid, "uuid4", lambda: FakeUuid())
    monkeypatch.setattr(
        model_eval_tool,
        "execute_operation_from_env",
        fake_execute_operation_from_env,
    )
    monkeypatch.setattr(model_eval_tool, "workflow_database_env", lambda: {"x": "y"})
    monkeypatch.setattr(model_eval_tool, "_kickoff_model_eval_run", fake_kickoff)

    result = model_eval_tool.tool_praxis_model_eval(
        {"action": "run", "suite_slugs": ["docs"], "dry_run": True}
    )

    assert result["ok"] is True
    assert result["execution_mode"] == "inline"
    assert result["lab_run_id"] == "model-eval-fedcba654321"
    assert calls[0]["operation_name"] == "model_eval_run_matrix"
    assert calls[0]["payload"]["run_label"] == "model-eval-fedcba654321"
    assert calls[0]["transport_kind"] == "workflow"


def test_tool_ladder_validators_score_expected_names() -> None:
    payload = {"tool_calls": [{"function": {"name": "praxis_search", "arguments": "{}"}}]}

    result = validate_task_output({"validator": "tool_single_search"}, payload)
    wrong = validate_task_output({"validator": "tool_single_validate"}, payload)

    assert result["ok"] is True
    assert wrong["ok"] is False
    assert any(check["check"] == "calls praxis_workflow_validate" and not check["ok"] for check in wrong["checks"])


def test_tool_ladder_validators_cover_bug_and_decision_tools() -> None:
    bug_payload = {"tool_calls": [{"function": {"name": "praxis_bugs", "arguments": "{}"}}]}
    decision_payload = {
        "tool_calls": [{"function": {"name": "praxis_operator_decisions", "arguments": "{}"}}]
    }

    bug_result = validate_task_output({"validator": "tool_single_bugs"}, bug_payload)
    decision_result = validate_task_output(
        {"validator": "tool_single_operator_decisions"},
        decision_payload,
    )

    assert bug_result["ok"] is True
    assert decision_result["ok"] is True


def test_benchmark_ingest_updates_candidate_profiles_as_priors_only() -> None:
    class Conn:
        def __init__(self) -> None:
            self.calls: list[tuple] = []

        def fetch(self, _sql, provider_slug, model_slug, prior_json):
            self.calls.append((provider_slug, model_slug, json.loads(prior_json)))
            if provider_slug == "openrouter" and model_slug == "openai/gpt-5.4-nano":
                return [
                    {
                        "provider_slug": provider_slug,
                        "model_slug": model_slug,
                        "benchmark_profile": {"model_eval_public_benchmark_priors": [json.loads(prior_json)]},
                    }
                ]
            return []

    class Subsystems:
        def __init__(self) -> None:
            self.conn = Conn()

        def get_pg_conn(self):
            return self.conn

    command = ModelEvalBenchmarkIngestCommand(
        benchmark_slug="terminal-bench",
        source_url="https://arxiv.org/abs/2601.11868",
        version="2026-01",
        rows=[
            {
                "provider_slug": "openrouter",
                "model_slug": "openai/gpt-5.4-nano",
                "metric_slug": "agentic_terminal_score",
                "score": 0.42,
                "task_family": "tool_execution_loop",
            },
            {
                "provider_slug": "missing",
                "model_slug": "model",
                "metric_slug": "agentic_terminal_score",
                "score": 0.1,
            },
        ],
    )

    result = handle_model_eval_benchmark_ingest(command, Subsystems())

    assert result["ok"] is True
    assert result["updated_count"] == 1
    assert result["unmatched_count"] == 1
    assert result["event_payload"]["routing_effect"] == "prior_only_not_score_truth"


def test_swarm_validator_rejects_overlapping_packet_shape() -> None:
    task = {"validator": "swarm_packet"}
    payload = {
        "task_id": "swarm.instruct_packet",
        "answer": "done",
        "artifacts": [
            {
                "path": "swarm_plan.json",
                "media_type": "application/json",
                "content": json.dumps(
                    {
                        "workers": [
                            {"name": "same"},
                            {"name": "same"},
                            {"name": "same"},
                            {"name": "same"},
                        ],
                        "reducer": {},
                        "budget": 2,
                        "boundary": "no production routing changes",
                    }
                ),
            }
        ],
    }

    result = validate_task_output(task, payload)

    assert result["ok"] is False
    assert any(check["check"] == "worker names distinct" and not check["ok"] for check in result["checks"])
