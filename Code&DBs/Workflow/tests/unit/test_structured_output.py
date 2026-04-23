"""Tests for adapters.structured_output — model output parsing.

Verifies that model stdout is correctly parsed into StructuredOutput
regardless of format (JSON, fenced code blocks, NDJSON streams).
"""

from __future__ import annotations

import json

import pytest

from adapters.structured_output import (
    CodeBlock,
    StructuredOutput,
    parse_model_output,
)


def _profile_row(
    *,
    provider_slug: str,
    binary_name: str,
    base_flags: list[str],
    forbidden_flags: list[str],
    default_model: str,
    lane_policies: dict[str, dict[str, object]],
    adapter_economics: dict[str, dict[str, object]],
    api_endpoint: str | None = None,
    api_protocol_family: str | None = None,
    api_key_env_vars: list[str] | None = None,
    model_flag: str | None = "--model",
    system_prompt_flag: str | None = None,
    json_schema_flag: str | None = None,
    output_format: str = "json",
    output_envelope_key: str = "result",
    default_timeout: int = 300,
) -> dict[str, object]:
    return {
        "provider_slug": provider_slug,
        "binary_name": binary_name,
        "default_model": default_model,
        "api_endpoint": api_endpoint,
        "api_protocol_family": api_protocol_family,
        "api_key_env_vars": api_key_env_vars or [],
        "prompt_mode": "stdin",
        "base_flags": base_flags,
        "model_flag": model_flag,
        "system_prompt_flag": system_prompt_flag,
        "json_schema_flag": json_schema_flag,
        "output_format": output_format,
        "output_envelope_key": output_envelope_key,
        "forbidden_flags": forbidden_flags,
        "default_timeout": default_timeout,
        "aliases": [],
        "mcp_config_style": None,
        "mcp_args_template": None,
        "sandbox_env_overrides": {},
        "exclude_from_rotation": False,
        "lane_policies": lane_policies,
        "adapter_economics": adapter_economics,
    }


def _cli_lane_policy() -> dict[str, object]:
    return {
        "admitted_by_policy": True,
        "execution_topology": "local_cli",
        "transport_kind": "cli",
        "policy_reason": "Admitted local CLI lane.",
    }


def _http_lane_policy() -> dict[str, object]:
    return {
        "admitted_by_policy": True,
        "execution_topology": "direct_http",
        "transport_kind": "http",
        "policy_reason": "Admitted direct HTTP lane.",
    }


def _prepaid_economics(provider_slug: str, *, allow_payg_fallback: bool) -> dict[str, object]:
    return {
        "billing_mode": "subscription_included",
        "budget_bucket": f"{provider_slug}_monthly",
        "effective_marginal_cost": 0.0,
        "prefer_prepaid": True,
        "allow_payg_fallback": allow_payg_fallback,
    }


def _metered_economics(provider_slug: str) -> dict[str, object]:
    return {
        "billing_mode": "metered_api",
        "budget_bucket": f"{provider_slug}_api_payg",
        "effective_marginal_cost": 1.0,
        "prefer_prepaid": False,
        "allow_payg_fallback": True,
    }


def _provider_profiles():
    from registry.provider_execution_registry import _parse_profile_row

    rows = (
        _profile_row(
            provider_slug="anthropic",
            binary_name="claude",
            default_model="claude-sonnet-4-6",
            base_flags=["-p", "--output-format", "json"],
            model_flag="--model",
            system_prompt_flag="--system-prompt",
            json_schema_flag="--json-schema",
            output_format="json",
            output_envelope_key="result",
            forbidden_flags=[
                "--dangerously-skip-permissions",
                "--allow-dangerously-skip-permissions",
                "--add-dir",
            ],
            lane_policies={"cli_llm": _cli_lane_policy()},
            adapter_economics={
                "cli_llm": _prepaid_economics("anthropic", allow_payg_fallback=False)
            },
        ),
        _profile_row(
            provider_slug="openai",
            binary_name="codex",
            default_model="gpt-4.1",
            api_endpoint="https://api.openai.com/v1/chat/completions",
            api_protocol_family="openai_chat_completions",
            api_key_env_vars=["OPENAI_API_KEY"],
            base_flags=["exec", "-", "--json"],
            output_format="ndjson",
            output_envelope_key="text",
            forbidden_flags=["--full-auto"],
            lane_policies={"cli_llm": _cli_lane_policy(), "llm_task": _http_lane_policy()},
            adapter_economics={
                "cli_llm": _prepaid_economics("openai", allow_payg_fallback=True),
                "llm_task": _metered_economics("openai"),
            },
        ),
        _profile_row(
            provider_slug="google",
            binary_name="gemini",
            default_model="gemini-2.5-flash",
            api_endpoint="https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent",
            api_protocol_family="google_generate_content",
            api_key_env_vars=["GEMINI_API_KEY", "GOOGLE_API_KEY"],
            base_flags=["-p", ".", "-o", "json"],
            output_format="json",
            output_envelope_key="response",
            forbidden_flags=["--approval-mode", "--yolo", "-y"],
            default_timeout=600,
            lane_policies={"cli_llm": _cli_lane_policy(), "llm_task": _http_lane_policy()},
            adapter_economics={
                "cli_llm": _prepaid_economics("google", allow_payg_fallback=True),
                "llm_task": _metered_economics("google"),
            },
        ),
    )
    return {
        profile.provider_slug: profile
        for profile in (_parse_profile_row(row) for row in rows)
    }


class TestParseJSON:
    """JSON structured output parsing."""

    def test_valid_json_with_code_blocks(self):
        text = json.dumps({
            "code_blocks": [
                {
                    "file_path": "runtime/domain.py",
                    "content": "class RunState:\n    \"\"\"Workflow run state.\"\"\"\n    pass\n",
                    "language": "python",
                    "action": "replace",
                }
            ],
            "explanation": "Added docstring to RunState",
        })
        result = parse_model_output(text)
        assert result.has_code
        assert result.parse_strategy == "json"
        assert len(result.code_blocks) == 1
        assert result.code_blocks[0].file_path == "runtime/domain.py"
        assert "RunState" in result.code_blocks[0].content
        assert result.explanation == "Added docstring to RunState"

    def test_json_with_multiple_code_blocks(self):
        text = json.dumps({
            "code_blocks": [
                {"file_path": "a.py", "content": "# a", "language": "python", "action": "create"},
                {"file_path": "b.py", "content": "# b", "language": "python", "action": "replace"},
            ],
            "explanation": "Created two files",
        })
        result = parse_model_output(text)
        assert len(result.code_blocks) == 2
        assert result.file_paths == ("a.py", "b.py")

    def test_json_empty_code_blocks(self):
        text = json.dumps({"code_blocks": [], "explanation": "Nothing to write"})
        result = parse_model_output(text)
        # Empty code_blocks in JSON → falls through to other strategies
        assert not result.has_code

    def test_json_with_extra_metadata(self):
        text = json.dumps({
            "code_blocks": [
                {"file_path": "x.py", "content": "pass", "language": "python", "action": "replace"},
            ],
            "explanation": "test",
            "confidence": 0.95,
            "model": "claude-sonnet",
        })
        result = parse_model_output(text)
        assert result.has_code
        assert result.metadata.get("confidence") == 0.95


class TestParseFenced:
    """Fenced code block parsing."""

    def test_fence_with_path(self):
        text = "Here's the fix:\n\n```python:runtime/domain.py\nclass RunState:\n    pass\n```\n\nDone."
        result = parse_model_output(text)
        assert result.has_code
        assert result.parse_strategy == "fenced"
        assert result.code_blocks[0].file_path == "runtime/domain.py"
        assert "RunState" in result.code_blocks[0].content

    def test_file_header_pattern(self):
        text = "FILE: runtime/domain.py\n```python\nclass RunState:\n    pass\n```"
        result = parse_model_output(text)
        assert result.has_code
        assert result.code_blocks[0].file_path == "runtime/domain.py"

    def test_generic_fence_with_default_path(self):
        text = "```python\ndef hello():\n    return 'hi'\n```"
        result = parse_model_output(text, default_file_path="utils.py")
        assert result.has_code
        assert result.code_blocks[0].file_path == "utils.py"

    def test_generic_fence_without_default_path_no_match(self):
        text = "```python\ndef hello():\n    return 'hi'\n```"
        result = parse_model_output(text)
        # No default_path and no path in fence → no code blocks from fenced parser
        # Falls through to raw_text
        assert result.parse_strategy == "raw_text"


class TestParseNDJSON:
    """NDJSON stream parsing (Claude stream-json format)."""

    def test_ndjson_with_result(self):
        lines = [
            json.dumps({"type": "content_block_delta", "delta": {"type": "text_delta", "text": "Hello "}}),
            json.dumps({"type": "content_block_delta", "delta": {"type": "text_delta", "text": "world"}}),
            json.dumps({"type": "result", "result": "Hello world"}),
        ]
        text = "\n".join(lines)
        result = parse_model_output(text)
        assert result.explanation == "Hello world"
        assert "ndjson" in result.parse_strategy

    def test_codex_ndjson_item_completed(self):
        """Codex outputs NDJSON with item.completed events."""
        code_json = json.dumps({
            "code_blocks": [
                {"file_path": "test.py", "content": "# test", "language": "python", "action": "create"},
            ],
            "explanation": "Created test file",
        })
        lines = [
            json.dumps({"type": "thread.started", "thread_id": "abc"}),
            json.dumps({"type": "turn.started"}),
            json.dumps({"type": "item.completed", "item": {"id": "item_0", "type": "agent_message", "text": code_json}}),
            json.dumps({"type": "turn.completed", "usage": {"input_tokens": 100, "output_tokens": 50}}),
        ]
        text = "\n".join(lines)
        result = parse_model_output(text)
        assert result.has_code
        assert "ndjson" in result.parse_strategy
        assert result.code_blocks[0].file_path == "test.py"

    def test_gemini_json_envelope(self):
        """Gemini wraps response in {"response": "..."}."""
        inner = json.dumps({
            "code_blocks": [
                {"file_path": "hello.py", "content": "print('hi')", "language": "python", "action": "replace"},
            ],
            "explanation": "Simple print",
        })
        text = json.dumps({"session_id": "abc", "response": inner, "stats": {}})
        result = parse_model_output(text)
        assert result.has_code
        assert result.parse_strategy == "json"
        assert result.code_blocks[0].file_path == "hello.py"

    def test_ndjson_with_json_code_in_result(self):
        code_output = json.dumps({
            "code_blocks": [
                {"file_path": "test.py", "content": "# test", "language": "python", "action": "create"},
            ],
            "explanation": "Created test file",
        })
        # Need multiple lines starting with { to trigger NDJSON detection
        lines = [
            json.dumps({"type": "content_block_delta", "delta": {"type": "text_delta", "text": "working..."}}),
            json.dumps({"type": "result", "result": code_output}),
        ]
        text = "\n".join(lines)
        result = parse_model_output(text)
        assert result.has_code
        assert result.parse_strategy == "ndjson+json"


class TestEdgeCases:
    """Edge cases and fallbacks."""

    def test_empty_string(self):
        result = parse_model_output("")
        assert not result.has_code
        assert result.parse_strategy == "empty"

    def test_whitespace_only(self):
        result = parse_model_output("   \n\n  ")
        assert not result.has_code
        assert result.parse_strategy == "empty"

    def test_plain_text(self):
        result = parse_model_output("The answer is 42.")
        assert not result.has_code
        assert result.parse_strategy == "raw_text"
        assert result.explanation == "The answer is 42."

    def test_invalid_json(self):
        result = parse_model_output("{invalid json")
        assert result.parse_strategy == "raw_text"

    def test_code_block_immutability(self):
        cb = CodeBlock(file_path="a.py", content="pass", language="python", action="replace")
        assert cb.file_path == "a.py"
        with pytest.raises(AttributeError):
            cb.file_path = "b.py"  # frozen dataclass

    def test_structured_output_immutability(self):
        so = StructuredOutput(
            code_blocks=(),
            explanation="test",
            raw_text="test",
        )
        with pytest.raises(AttributeError):
            so.explanation = "changed"


class TestNoFilesystemFlags:
    """Verify the adapter never passes filesystem-granting flags."""

    def test_anthropic_profile_no_dangerous_flags(self):
        anthropic = _provider_profiles()["anthropic"]
        flags = " ".join(anthropic.base_flags)
        assert "--dangerously-skip-permissions" not in flags
        assert "--add-dir" not in flags

    def test_openai_profile_no_full_auto(self):
        openai = _provider_profiles()["openai"]
        flags = " ".join(openai.base_flags)
        assert "--full-auto" not in flags
        # "exec" subcommand is fine — it's "--full-auto" that grants filesystem access

    def test_google_profile_no_yolo(self):
        google = _provider_profiles()["google"]
        flags = " ".join(google.base_flags)
        assert "yolo" not in flags

    def test_build_cmd_no_add_dir(self):
        from adapters import provider_transport

        cmd = provider_transport.build_command(
            "anthropic",
            profiles=_provider_profiles(),
            model="claude-sonnet-4-6",
            binary_override="/usr/bin/claude",
        )
        cmd_str = " ".join(cmd)
        assert "--add-dir" not in cmd_str
        assert "--dangerously-skip-permissions" not in cmd_str
        assert "--allowedTools" not in cmd_str

    def test_registry_all_providers_flags_safe(self):
        from adapters import provider_transport

        for slug, report in provider_transport.validate_profiles(
            _provider_profiles(),
            adapter_config={},
            failure_mappings={},
        ).items():
            assert report["flags_safe"], f"{slug} has forbidden flags in base_flags"

    def test_registry_forbidden_flags_enforced(self):
        profiles = _provider_profiles()
        for slug in ("anthropic", "openai", "google"):
            profile = profiles.get(slug)
            assert profile is not None, f"missing profile for {slug}"
            assert len(profile.forbidden_flags) > 0, f"{slug} has no forbidden flags"
            flags_str = " ".join(profile.base_flags)
            for forbidden in profile.forbidden_flags:
                assert forbidden not in flags_str, (
                    f"{slug}: forbidden flag {forbidden!r} found in base_flags"
                )
