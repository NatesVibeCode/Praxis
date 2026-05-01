from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime import spec_materializer


def test_compile_prompt_launch_spec_uses_provider_default_model_when_model_missing(monkeypatch) -> None:
    monkeypatch.setattr(
        spec_materializer,
        "resolve_default_adapter_type",
        lambda provider_slug=None: "cli_llm" if provider_slug == "openai" else None,
    )
    monkeypatch.setattr(
        spec_materializer,
        "default_model_for_provider",
        lambda provider_slug: "gpt-4.1" if provider_slug == "openai" else None,
    )
    monkeypatch.setattr(
        spec_materializer,
        "resolve_lane_policy",
        lambda provider_slug, adapter_type: {"admitted_by_policy": True},
    )

    spec = spec_materializer.compile_prompt_launch_spec(
        prompt="Reply with exactly: OPENAI_DEFAULT_WORKFLOW_OK",
        provider_slug="openai",
        model_slug=None,
        scope_write=["greeting.py"],
        workdir="/repo",
    )

    assert spec.jobs[0]["agent"] == "openai/gpt-4.1"
    assert spec.jobs[0]["write_scope"] == ["greeting.py"]
    assert spec.jobs[0]["workdir"] == "/repo"
    assert spec.workflow_id.startswith("workflow_cli_prompt.")
    assert spec.definition_revision.startswith("def_")
    assert spec.plan_revision.startswith("plan_")
    inline_spec = spec.to_inline_spec_dict()
    assert inline_spec["graph_runtime_submit"] is True
    assert inline_spec["definition_revision"] == spec.definition_revision
    assert inline_spec["plan_revision"] == spec.plan_revision
    assert inline_spec["packet_provenance"] == {
        "source_kind": "prompt_launch",
        "definition_row": {"definition_revision": spec.definition_revision},
        "materialized_spec_row": {
            "definition_revision": spec.definition_revision,
            "plan_revision": spec.plan_revision,
        },
    }


def test_compile_prompt_launch_spec_prefers_provider_specific_adapter_when_unspecified(monkeypatch) -> None:
    monkeypatch.setattr(
        spec_materializer,
        "resolve_default_adapter_type",
        lambda provider_slug=None: "llm_task" if provider_slug == "cursor" else None,
    )
    monkeypatch.setattr(
        spec_materializer,
        "resolve_lane_policy",
        lambda provider_slug, adapter_type: {"admitted_by_policy": True},
    )

    spec = spec_materializer.compile_prompt_launch_spec(
        prompt="Reply with exactly: CURSOR_BACKGROUND_AGENT_OK",
        provider_slug="cursor",
        model_slug="auto",
    )

    assert spec.jobs[0]["adapter_type"] == "llm_task"
    assert spec.jobs[0]["agent"] == "cursor/auto"


def test_compile_prompt_launch_spec_carries_runtime_profile_authority(monkeypatch) -> None:
    monkeypatch.setattr(
        spec_materializer,
        "resolve_default_adapter_type",
        lambda provider_slug=None: "cli_llm" if provider_slug == "openai" else None,
    )
    monkeypatch.setattr(
        spec_materializer,
        "resolve_lane_policy",
        lambda provider_slug, adapter_type: {"admitted_by_policy": True},
    )

    spec = spec_materializer.compile_prompt_launch_spec(
        prompt="Summarize the attached notes",
        provider_slug="openai",
        model_slug="gpt-5.4-mini",
        workspace_ref="scratch_agent",
        runtime_profile_ref="scratch_agent",
    )

    assert spec.workspace_ref == "scratch_agent"
    assert spec.runtime_profile_ref == "scratch_agent"
    inline_spec = spec.to_inline_spec_dict()
    assert inline_spec["workspace_ref"] == "scratch_agent"
    assert inline_spec["runtime_profile_ref"] == "scratch_agent"


def test_compile_prompt_launch_spec_rejects_unadmitted_prompt_provider(monkeypatch) -> None:
    monkeypatch.setattr(
        spec_materializer,
        "resolve_default_adapter_type",
        lambda provider_slug=None: "llm_task" if provider_slug == "cursor" else None,
    )
    monkeypatch.setattr(
        spec_materializer,
        "resolve_lane_policy",
        lambda provider_slug, adapter_type: {
            "admitted_by_policy": False,
            "policy_reason": "Prompt probe did not complete successfully for cursor/composer-2",
            "decision_ref": "decision.provider-onboarding.cursor.20260415T165657Z",
        },
    )
    monkeypatch.setattr(spec_materializer, "registered_providers", lambda: ["google", "openai"])

    try:
        spec_materializer.compile_prompt_launch_spec(
            prompt="Reply with exactly: NOPE",
            provider_slug="cursor",
            model_slug="composer-2",
        )
    except ValueError as exc:
        message = str(exc)
    else:
        raise AssertionError("compile_prompt_launch_spec should reject unadmitted providers")

    assert "provider 'cursor' is not admitted for llm_task" in message
    assert "Prompt probe did not complete successfully for cursor/composer-2" in message
    assert "decision.provider-onboarding.cursor.20260415T165657Z" in message
    assert "google, openai" in message
