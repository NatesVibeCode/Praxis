from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from runtime import model_executor, native_authority, task_assembler, task_type_router


class _NoopConn:
    def execute(self, query: str, *args):
        if "FROM run_nodes" in query:
            return []
        return []


class _FakeRouter:
    instances: list["_FakeRouter"] = []
    decisions: list[SimpleNamespace] = []

    def __init__(self, conn) -> None:
        self.conn = conn
        self.resolutions: list[tuple[str, str | None]] = []
        self.outcomes: list[dict[str, object]] = []
        self.instances.append(self)

    def resolve_failover_chain(self, agent_slug: str, **kwargs):
        self.resolutions.append((agent_slug, kwargs.get("runtime_profile_ref")))
        return list(self.decisions)

    def record_outcome(
        self,
        task_type: str,
        provider_slug: str,
        model_slug: str,
        *,
        succeeded: bool,
        failure_code: str | None = None,
        failure_category: str = "",
        failure_zone: str = "",
    ) -> None:
        self.outcomes.append(
            {
                "task_type": task_type,
                "provider_slug": provider_slug,
                "model_slug": model_slug,
                "succeeded": succeeded,
                "failure_code": failure_code,
                "failure_category": failure_category,
                "failure_zone": failure_zone,
            }
        )


def _decision(
    provider_slug: str = "google",
    model_slug: str = "gemini-3.1-pro-preview",
    *,
    transport_type: str = "CLI",
    adapter_type: str = "cli_llm",
) -> SimpleNamespace:
    return SimpleNamespace(
        task_type="planner",
        provider_slug=provider_slug,
        model_slug=model_slug,
        transport_type=transport_type,
        adapter_type=adapter_type,
        candidate_ref=f"candidate.{provider_slug}.{model_slug}",
        max_tokens=None,
        reasoning_effort_slug="",
    )


def _patch_router(monkeypatch, decisions: list[SimpleNamespace]) -> None:
    _FakeRouter.instances = []
    _FakeRouter.decisions = decisions
    monkeypatch.setattr(task_type_router, "TaskTypeRouter", _FakeRouter)
    monkeypatch.setattr(
        native_authority,
        "default_native_runtime_profile_ref_required",
        lambda conn=None: "runtime_profile.unit",
    )


def test_routed_json_uses_native_runtime_profile_and_records_success(monkeypatch) -> None:
    decision = _decision()
    _patch_router(monkeypatch, [decision])
    captured: dict[str, object] = {}

    def _execute(decision_arg, prompt, **kwargs):
        captured["decision"] = decision_arg
        captured["prompt"] = prompt
        captured.update(kwargs)
        return '{"endpoint":"bugs"}'

    monkeypatch.setattr(
        task_assembler.TaskAssembler,
        "_execute_routed_decision",
        staticmethod(_execute),
    )

    assembler = task_assembler.TaskAssembler(_NoopConn())
    parsed, route = assembler._call_routed_json(
        "configure it",
        task_type="planner",
        purpose="quadrant_hydration:A1:metric",
        parser=assembler._parse_json,
    )

    router = _FakeRouter.instances[0]
    assert parsed == {"endpoint": "bugs"}
    assert route.provider_slug == "google"
    assert route.model_slug == "gemini-3.1-pro-preview"
    assert route.runtime_profile_ref == "runtime_profile.unit"
    assert router.resolutions == [("auto/planner", "runtime_profile.unit")]
    assert router.outcomes == [
        {
            "task_type": "planner",
            "provider_slug": "google",
            "model_slug": "gemini-3.1-pro-preview",
            "succeeded": True,
            "failure_code": None,
            "failure_category": "",
            "failure_zone": "",
        }
    ]
    assert captured["runtime_profile_ref"] == "runtime_profile.unit"
    assert captured["purpose"] == "quadrant_hydration:A1:metric"


def test_routed_json_falls_over_after_parse_failure(monkeypatch) -> None:
    first = _decision("google", "gemini-3-flash-preview")
    second = _decision("fireworks", "accounts/fireworks/models/kimi-k2p6")
    _patch_router(monkeypatch, [first, second])
    calls: list[str] = []

    def _execute(decision_arg, prompt, **kwargs):
        calls.append(f"{decision_arg.provider_slug}/{decision_arg.model_slug}")
        if len(calls) == 1:
            return "not json"
        return '{"endpoint":"objects?type=task"}'

    monkeypatch.setattr(
        task_assembler.TaskAssembler,
        "_execute_routed_decision",
        staticmethod(_execute),
    )

    assembler = task_assembler.TaskAssembler(_NoopConn())
    parsed, route = assembler._call_routed_json(
        "configure it",
        task_type="auto/planner",
        purpose="quadrant_hydration:B1:data-table",
        parser=assembler._parse_json,
    )

    router = _FakeRouter.instances[0]
    assert parsed == {"endpoint": "objects?type=task"}
    assert route.provider_slug == "fireworks"
    assert calls == [
        "google/gemini-3-flash-preview",
        "fireworks/accounts/fireworks/models/kimi-k2p6",
    ]
    assert router.outcomes[0]["succeeded"] is False
    assert router.outcomes[0]["failure_code"] == "unparseable_output"
    assert router.outcomes[1]["succeeded"] is True


def test_model_executor_app_cards_use_routed_prompt(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _call(cls, conn, prompt, **kwargs):
        captured["prompt"] = prompt
        captured.update(kwargs)
        return task_assembler.RoutedPromptResult(
            text="tool result",
            task_type="chat",
            provider_slug="google",
            model_slug="gemini-3.1-pro-preview",
            transport_type="CLI",
            adapter_type="cli_llm",
            runtime_profile_ref="runtime_profile.unit",
            candidate_ref="candidate.google.gemini-3.1-pro-preview",
        )

    monkeypatch.setattr(
        task_assembler.TaskAssembler,
        "call_routed_prompt",
        classmethod(_call),
    )

    result = model_executor._execute_action(
        _NoopConn(),
        "run-1",
        {
            "id": "card-1",
            "task": "Summarize tool result",
            "executor": {"kind": "app", "name": "Tool Summarizer"},
            "toolPermissions": ["praxis_bugs"],
        },
        "/tmp/repo",
    )

    assert captured["task_type"] == "chat"
    assert captured["purpose"] == "app_card:card-1"
    assert result["status"] == "succeeded"
    assert result["outputs"]["stdout"] == "tool result"
    assert result["outputs"]["resolved_agent"] == "google/gemini-3.1-pro-preview"
    assert result["outputs"]["runtime_profile_ref"] == "runtime_profile.unit"


def test_hardcoded_openrouter_sonnet_fanout_path_is_removed() -> None:
    task_source = Path(task_assembler.__file__).read_text(encoding="utf-8")
    model_executor_source = Path(model_executor.__file__).read_text(encoding="utf-8")

    combined = task_source + model_executor_source
    assert "_call_haiku" not in combined
    assert "anthropic/claude-sonnet-4.6" not in combined
