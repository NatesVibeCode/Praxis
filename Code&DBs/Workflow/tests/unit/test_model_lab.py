from __future__ import annotations

import json

from runtime.model_eval.catalog import build_suite_plan, catalog_version_hash
import pytest

from runtime.model_eval import runner as model_eval_runner
from runtime.model_eval.openrouter import BLOCKED_PROVIDER_SLUGS, OpenRouterError, build_lab_request
from runtime.model_eval.validators import validate_task_output


def test_model_eval_plan_defaults_have_consistency_contract() -> None:
    plan = build_suite_plan(suite_slugs=["docs", "csv"], model_configs=[{"config_id": "m", "model_slug": "x"}])

    assert plan["ok"] is True
    assert plan["task_count"] == 3
    assert "workflow_spec" in plan["consistency_contract"]["fixed"]
    assert "model_slug" in plan["consistency_contract"]["varied"]
    assert "task_type_routing" in plan["consistency_contract"]["promotion_rule"]
    assert len(plan["catalog_version_hash"]) == 64


def test_model_eval_catalog_version_hash_is_deterministic() -> None:
    assert catalog_version_hash() == catalog_version_hash()


def test_model_eval_tool_suite_has_ladder_tasks() -> None:
    plan = build_suite_plan(suite_slugs=["tools"], model_configs=[{"config_id": "m", "model_slug": "x"}])
    task_ids = {task["task_id"] for task in plan["tasks"]}

    assert plan["task_count"] == 8
    assert "tool.search_single" in task_ids
    assert "tool.validate_single" in task_ids
    assert "tool.model_eval_plan_single" in task_ids
    assert "tool.bugs_search_single" in task_ids
    assert "tool.operator_decisions_list_single" in task_ids
    assert "tool.choose_specific_types" in task_ids


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
    assert "moonshot" in BLOCKED_PROVIDER_SLUGS
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
        model_config={"config_id": "nano", "model_slug": "openai/gpt-5.4-nano", "provider_order": ["azure"]},
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
            "provider": "moonshot",
            "model": "moonshotai/kimi-k2.6",
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
        model_config={"config_id": "kimi", "model_slug": "moonshotai/kimi-k2.6", "provider_order": ["parasail"]},
        prompt_variant={"prompt_variant_id": "contract_first"},
        output_root=tmp_path,
        timeout_seconds=1,
        dry_run=False,
    )

    assert result["ok"] is False
    assert result["status"] == "route_mismatch"


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
        model_configs=[{"config_id": "m", "model_slug": "x"}],
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
