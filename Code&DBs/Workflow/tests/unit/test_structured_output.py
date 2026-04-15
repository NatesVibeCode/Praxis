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
        from adapters import provider_transport

        anthropic = next(
            profile
            for profile in provider_transport.BUILTIN_PROVIDER_PROFILES
            if profile.provider_slug == "anthropic"
        )
        flags = " ".join(anthropic.base_flags)
        assert "--dangerously-skip-permissions" not in flags
        assert "--add-dir" not in flags

    def test_openai_profile_no_full_auto(self):
        from adapters import provider_transport

        openai = next(
            profile
            for profile in provider_transport.BUILTIN_PROVIDER_PROFILES
            if profile.provider_slug == "openai"
        )
        flags = " ".join(openai.base_flags)
        assert "--full-auto" not in flags
        # "exec" subcommand is fine — it's "--full-auto" that grants filesystem access

    def test_google_profile_no_yolo(self):
        from adapters import provider_transport

        google = next(
            profile
            for profile in provider_transport.BUILTIN_PROVIDER_PROFILES
            if profile.provider_slug == "google"
        )
        flags = " ".join(google.base_flags)
        assert "yolo" not in flags

    def test_build_cmd_no_add_dir(self):
        from adapters import provider_transport

        cmd = provider_transport.build_command(
            "anthropic",
            profiles={profile.provider_slug: profile for profile in provider_transport.BUILTIN_PROVIDER_PROFILES},
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
            {profile.provider_slug: profile for profile in provider_transport.BUILTIN_PROVIDER_PROFILES},
            adapter_config={},
            failure_mappings={},
        ).items():
            assert report["flags_safe"], f"{slug} has forbidden flags in base_flags"

    def test_registry_forbidden_flags_enforced(self):
        from adapters import provider_transport

        for slug in ("anthropic", "openai", "google"):
            profile = next(
                candidate
                for candidate in provider_transport.BUILTIN_PROVIDER_PROFILES
                if candidate.provider_slug == slug
            )
            assert profile is not None, f"missing profile for {slug}"
            assert len(profile.forbidden_flags) > 0, f"{slug} has no forbidden flags"
            flags_str = " ".join(profile.base_flags)
            for forbidden in profile.forbidden_flags:
                assert forbidden not in flags_str, (
                    f"{slug}: forbidden flag {forbidden!r} found in base_flags"
                )
