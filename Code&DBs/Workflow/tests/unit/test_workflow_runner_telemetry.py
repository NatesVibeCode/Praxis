from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace


_mod_path = Path(__file__).resolve().parents[2] / "surfaces" / "cli" / "workflow_runner.py"
_spec = importlib.util.spec_from_file_location("workflow_runner_telemetry", _mod_path)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["workflow_runner_telemetry"] = _mod
_spec.loader.exec_module(_mod)


def test_anthropic_cli_telemetry_parses_when_api_protocol_family_is_none(monkeypatch) -> None:
    import registry.provider_execution_registry as provider_registry

    monkeypatch.setattr(
        provider_registry,
        "get_profile",
        lambda provider: SimpleNamespace(api_protocol_family=None)
        if provider == "anthropic"
        else None,
    )

    raw = json.dumps(
        {
            "type": "result",
            "subtype": "success",
            "duration_api_ms": 3913,
            "num_turns": 2,
            "result": "Hello, world!",
            "total_cost_usd": 0.050855,
            "usage": {
                "input_tokens": 3,
                "cache_creation_input_tokens": 6496,
                "cache_read_input_tokens": 19930,
                "output_tokens": 11,
                "server_tool_use": {
                    "web_search_requests": 1,
                    "web_fetch_requests": 2,
                },
            },
            "modelUsage": {
                "claude-sonnet-4-6": {
                    "inputTokens": 3,
                    "outputTokens": 11,
                    "cacheReadInputTokens": 19930,
                    "cacheCreationInputTokens": 6496,
                    "costUSD": 0.050855,
                },
            },
        }
    )

    telemetry, result_text = _mod.WorkflowRunner._parse_cli_telemetry(raw, "anthropic")

    assert result_text == "Hello, world!"
    assert telemetry is not None
    assert telemetry.input_tokens == 3
    assert telemetry.output_tokens == 11
    assert telemetry.cache_read_tokens == 19930
    assert telemetry.cache_creation_tokens == 6496
    assert telemetry.cost_usd == 0.050855
    assert telemetry.model == "claude-sonnet-4-6"
    assert telemetry.duration_api_ms == 3913
    assert telemetry.num_turns == 2
    assert telemetry.tool_use["web_search_requests"] == 1
    assert telemetry.tool_use["web_fetch_requests"] == 2
