"""Unit tests for runtime/daily_heartbeat.py.

Covers the pure parsing helpers, each probe function (with a FakeConn + mocked
subprocess), and the orchestrator end-to-end with probes stubbed out.
"""
from __future__ import annotations

import asyncio
import json
import subprocess
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from runtime import daily_heartbeat as dh


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_pick_usage_tokens_anthropic_top_level() -> None:
    payload = {"usage": {"input_tokens": 12, "output_tokens": 5}}
    i, o, raw = dh._pick_usage_tokens(payload)
    assert i == 12
    assert o == 5
    assert raw["input_tokens"] == 12


def test_pick_usage_tokens_anthropic_nested_message() -> None:
    payload = {"message": {"usage": {"input_tokens": 3, "output_tokens": 7}}}
    i, o, _ = dh._pick_usage_tokens(payload)
    assert (i, o) == (3, 7)


def test_pick_usage_tokens_openai_prompt_completion_keys() -> None:
    payload = {"response": {"usage": {"prompt_tokens": 40, "completion_tokens": 2}}}
    i, o, _ = dh._pick_usage_tokens(payload)
    assert (i, o) == (40, 2)


def test_pick_usage_tokens_gemini_usage_metadata() -> None:
    payload = {"usageMetadata": {"promptTokenCount": 8, "candidatesTokenCount": 4}}
    i, o, raw = dh._pick_usage_tokens(payload)
    assert (i, o) == (8, 4)
    assert raw["raw"]["promptTokenCount"] == 8


def test_pick_usage_tokens_missing_returns_none() -> None:
    assert dh._pick_usage_tokens({}) == (None, None, {})
    assert dh._pick_usage_tokens("not a dict") == (None, None, {})  # type: ignore[arg-type]


def test_pick_usage_tokens_non_numeric_coerces_to_none() -> None:
    payload = {"usage": {"input_tokens": "abc", "output_tokens": None}}
    i, o, _ = dh._pick_usage_tokens(payload)
    assert i is None and o is None


def test_pick_model_slug_anthropic_top_level() -> None:
    assert dh._pick_model_slug({"model": "claude-sonnet-4-6"}) == "claude-sonnet-4-6"


def test_pick_model_slug_anthropic_model_usage_shape() -> None:
    # Recent Claude CLI shape: top-level "modelUsage" keyed by model slug.
    payload = {"modelUsage": {"claude-sonnet-4-6": {"inputTokens": 2}}}
    assert dh._pick_model_slug(payload) == "claude-sonnet-4-6"


def test_pick_model_slug_gemini_modelversion() -> None:
    assert dh._pick_model_slug({"modelVersion": "gemini-2.5-flash"}) == "gemini-2.5-flash"


def test_pick_model_slug_missing_returns_none() -> None:
    assert dh._pick_model_slug({}) is None
    assert dh._pick_model_slug("nope") is None  # type: ignore[arg-type]


def test_coerce_json_list_handles_already_parsed_list() -> None:
    assert dh._coerce_json_list(["-p", "--json"]) == ["-p", "--json"]


def test_coerce_json_list_handles_jsonb_string() -> None:
    # Regression: asyncpg hands jsonb columns back as encoded strings unless
    # a codec is registered. Without coercion, ``list("[...]")`` iterates the
    # string character-by-character and destroys argv.
    encoded = json.dumps(["-p", "--output-format", "json"])
    assert dh._coerce_json_list(encoded) == ["-p", "--output-format", "json"]


def test_coerce_json_list_handles_none_and_garbage() -> None:
    assert dh._coerce_json_list(None) == []
    assert dh._coerce_json_list("not valid json") == []
    assert dh._coerce_json_list('{"not": "a list"}') == []


def test_coerce_json_dict_handles_already_parsed_dict() -> None:
    assert dh._coerce_json_dict({"a": 1}) == {"a": 1}


def test_coerce_json_dict_handles_jsonb_string() -> None:
    encoded = json.dumps({"cli_llm": {"billing_mode": "subscription_included"}})
    out = dh._coerce_json_dict(encoded)
    assert out == {"cli_llm": {"billing_mode": "subscription_included"}}


def test_coerce_json_dict_handles_none_and_garbage() -> None:
    assert dh._coerce_json_dict(None) == {}
    assert dh._coerce_json_dict("not json") == {}
    assert dh._coerce_json_dict("[1, 2, 3]") == {}


def test_parse_cli_output_json() -> None:
    text = json.dumps({"result": "HEARTBEAT_OK", "usage": {"input_tokens": 1}})
    parsed = dh._parse_cli_output(text, "json")
    assert parsed["result"] == "HEARTBEAT_OK"


def test_parse_cli_output_ndjson_merges_codex_event_stream() -> None:
    # Mirror codex's real event shape: thread.started → turn.started
    # → item.completed(agent_message) → turn.completed(usage). No single line
    # carries both the agent text and the usage block, so the parser has to
    # MERGE across lines: collect text from item.completed and keep the last
    # usage/model/stats block seen on top-level keys.
    ndjson = (
        json.dumps({"type": "thread.started", "thread_id": "t1"}) + "\n"
        + json.dumps({"type": "turn.started"}) + "\n"
        + json.dumps({
            "type": "item.completed",
            "item": {"id": "item_0", "type": "agent_message", "text": "HEARTBEAT_OK"},
        }) + "\n"
        + json.dumps({
            "type": "turn.completed",
            "usage": {"input_tokens": 120, "output_tokens": 7},
        }) + "\n"
    )
    parsed = dh._parse_cli_output(ndjson, "ndjson")
    # text gathered from the agent_message event
    assert parsed.get("text") == "HEARTBEAT_OK"
    # usage merged in from the final event
    assert parsed.get("usage") == {"input_tokens": 120, "output_tokens": 7}


def test_parse_cli_output_empty_and_malformed() -> None:
    assert dh._parse_cli_output("", "json") == {}
    assert dh._parse_cli_output("not json", "json") == {}


def test_probe_env_strips_session_markers(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CLAUDECODE", "1")
    monkeypatch.setenv("ANTHROPIC_SESSION_ID", "abc")
    monkeypatch.setenv("PATH", "/tmp/bin")
    env = dh._probe_env()
    assert "CLAUDECODE" not in env
    assert "ANTHROPIC_SESSION_ID" not in env
    assert env["PATH"] == "/tmp/bin"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


def test_heartbeat_run_result_to_json_roundtrip() -> None:
    snap = dh.ProbeSnapshot(
        probe_kind="credential_expiry",
        subject_id="OPENAI_API_KEY",
        status="ok",
        summary="keychain",
        subject_sub="api_key",
        latency_ms=42,
        days_until_expiry=30,
        details={"source_kind": "keychain"},
    )
    started = datetime(2026, 4, 20, 9, 30, tzinfo=timezone.utc)
    result = dh.HeartbeatRunResult(
        heartbeat_run_id="heartbeat_run.all.20260420T093000Z.abcd1234",
        scope="all",
        triggered_by="cli",
        started_at=started,
        completed_at=started + timedelta(seconds=5),
        status="succeeded",
        probes_total=1,
        probes_ok=1,
        probes_failed=0,
        summary="scope=all total=1 ok=1 failed=0",
        snapshots=[snap],
    )
    payload = result.to_json()
    assert payload["heartbeat_run_id"] == result.heartbeat_run_id
    assert payload["status"] == "succeeded"
    assert payload["snapshots"][0]["probe_kind"] == "credential_expiry"
    assert payload["snapshots"][0]["days_until_expiry"] == 30
    assert payload["snapshots"][0]["details"]["source_kind"] == "keychain"


# ---------------------------------------------------------------------------
# FakeConn — honors the bits of asyncpg.Connection that the probes use.
# ---------------------------------------------------------------------------


class _FakeConn:
    """Minimal async stand-in for asyncpg.Connection used by probes + orchestrator."""

    def __init__(self, fetch_rows: dict[str, list[dict[str, Any]]] | None = None):
        self._fetch_rows = fetch_rows or {}
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        for marker, rows in self._fetch_rows.items():
            if marker in query:
                return list(rows)
        return []

    async def execute(self, query: str, *args: Any) -> str:
        self.executed.append((query, args))
        return "OK"

    async def close(self) -> None:
        return None


# ---------------------------------------------------------------------------
# probe_connectors
# ---------------------------------------------------------------------------


def _connector_row(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "slug": "example-connector",
        "display_name": "Example",
        "health_status": "healthy",
        "verification_status": "verified",
        "error_rate": 0.0,
        "total_calls": 100,
        "total_errors": 0,
        "last_call_at": None,
        "last_success_at": None,
        "last_error_at": None,
        "last_verified_at": None,
    }
    base.update(overrides)
    return base


def test_probe_connectors_healthy_row_maps_to_ok() -> None:
    conn = _FakeConn({"FROM connector_registry": [_connector_row()]})
    snaps = asyncio.run(dh.probe_connectors(conn))
    assert len(snaps) == 1
    assert snaps[0].status == "ok"
    assert snaps[0].probe_kind == "connector_liveness"
    assert snaps[0].subject_id == "example-connector"


def test_probe_connectors_high_error_rate_maps_to_failed() -> None:
    conn = _FakeConn({"FROM connector_registry": [_connector_row(error_rate=0.9)]})
    snaps = asyncio.run(dh.probe_connectors(conn))
    assert snaps[0].status == "failed"


def test_probe_connectors_mid_error_rate_maps_to_degraded() -> None:
    conn = _FakeConn(
        {"FROM connector_registry": [_connector_row(error_rate=0.25, health_status="degraded")]}
    )
    snaps = asyncio.run(dh.probe_connectors(conn))
    assert snaps[0].status == "degraded"


def test_probe_connectors_unverified_maps_to_warning() -> None:
    conn = _FakeConn(
        {
            "FROM connector_registry": [
                _connector_row(health_status="unknown", verification_status="unverified")
            ]
        }
    )
    snaps = asyncio.run(dh.probe_connectors(conn))
    assert snaps[0].status == "warning"


def test_probe_connectors_empty_registry_returns_no_snapshots() -> None:
    conn = _FakeConn({"FROM connector_registry": []})
    snaps = asyncio.run(dh.probe_connectors(conn))
    assert snaps == []


# ---------------------------------------------------------------------------
# probe_credentials
# ---------------------------------------------------------------------------


def test_probe_credentials_api_key_present_in_keychain(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dh, "_keychain_present", lambda env_var: env_var == "OPENAI_API_KEY")
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)

    conn = _FakeConn(
        {
            "FROM provider_cli_profiles": [
                {"provider_slug": "openai", "api_key_env_vars": ["OPENAI_API_KEY"]}
            ]
        }
    )
    snaps = asyncio.run(dh.probe_credentials(conn))
    assert len(snaps) == 1
    snap = snaps[0]
    assert snap.status == "ok"
    assert snap.probe_kind == "credential_expiry"
    assert snap.subject_id == "OPENAI_API_KEY"
    assert snap.subject_sub == "api_key"
    assert snap.details["source_kind"] == "keychain"


def test_probe_credentials_api_key_env_fallback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dh, "_keychain_present", lambda env_var: False)
    monkeypatch.setenv("ONLY_IN_ENV_KEY", "value")
    conn = _FakeConn(
        {
            "FROM provider_cli_profiles": [
                {"provider_slug": "custom", "api_key_env_vars": ["ONLY_IN_ENV_KEY"]}
            ]
        }
    )
    snaps = asyncio.run(dh.probe_credentials(conn))
    assert snaps[0].status == "ok"
    assert snaps[0].details["source_kind"] == "env"


def test_probe_credentials_api_key_missing_is_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dh, "_keychain_present", lambda env_var: False)
    monkeypatch.delenv("ABSENT_KEY", raising=False)
    conn = _FakeConn(
        {
            "FROM provider_cli_profiles": [
                {"provider_slug": "nowhere", "api_key_env_vars": ["ABSENT_KEY"]}
            ]
        }
    )
    snaps = asyncio.run(dh.probe_credentials(conn))
    assert snaps[0].status == "failed"
    assert snaps[0].details["source_kind"] == "missing"


def test_probe_credentials_api_key_env_vars_can_be_jsonb_string(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dh, "_keychain_present", lambda env_var: True)
    conn = _FakeConn(
        {
            "FROM provider_cli_profiles": [
                {"provider_slug": "p", "api_key_env_vars": json.dumps(["STRINGY_KEY"])}
            ]
        }
    )
    snaps = asyncio.run(dh.probe_credentials(conn))
    assert snaps[0].subject_id == "STRINGY_KEY"


def test_probe_credentials_oauth_expired_is_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dh, "_keychain_present", lambda env_var: False)
    past = datetime.now(timezone.utc) - timedelta(days=3)
    conn = _FakeConn(
        {
            "FROM credential_tokens": [
                {
                    "integration_id": "slack-abc",
                    "token_kind": "access",
                    "expires_at": past,
                    "scopes": ["read"],
                    "updated_at": past,
                }
            ]
        }
    )
    snaps = asyncio.run(dh.probe_credentials(conn))
    assert len(snaps) == 1
    assert snaps[0].status == "failed"
    assert snaps[0].subject_sub == "oauth_access"
    assert (snaps[0].days_until_expiry or 0) < 0


def test_probe_credentials_oauth_expiring_within_window_is_degraded(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(dh, "_keychain_present", lambda env_var: False)
    soon = datetime.now(timezone.utc) + timedelta(days=3)
    conn = _FakeConn(
        {
            "FROM credential_tokens": [
                {
                    "integration_id": "github-xyz",
                    "token_kind": "access",
                    "expires_at": soon,
                    "scopes": ["repo"],
                    "updated_at": soon,
                }
            ]
        }
    )
    snaps = asyncio.run(dh.probe_credentials(conn))
    assert snaps[0].status == "degraded"
    assert (snaps[0].days_until_expiry or -1) <= dh._CREDENTIAL_EXPIRY_WARNING_DAYS


def test_probe_credentials_oauth_no_expiry_is_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(dh, "_keychain_present", lambda env_var: False)
    conn = _FakeConn(
        {
            "FROM credential_tokens": [
                {
                    "integration_id": "always-valid",
                    "token_kind": "access",
                    "expires_at": None,
                    "scopes": None,
                    "updated_at": None,
                }
            ]
        }
    )
    snaps = asyncio.run(dh.probe_credentials(conn))
    assert snaps[0].status == "ok"
    assert snaps[0].days_until_expiry is None


# ---------------------------------------------------------------------------
# probe_providers
# ---------------------------------------------------------------------------


def _provider_admission() -> dict[str, Any]:
    return {
        "provider_slug": "anthropic",
        "adapter_type": "cli_llm",
        "transport_kind": "cli",
        "admitted_by_policy": True,
    }


def _provider_profile() -> dict[str, Any]:
    return {
        "provider_slug": "anthropic",
        "binary_name": "claude",
        "base_flags": ["--print", "--output-format", "json"],
        "model_flag": "--model",
        "default_model": "claude-sonnet-4-5",
        "output_format": "json",
        "output_envelope_key": "result",
        "api_key_env_vars": ["ANTHROPIC_API_KEY"],
        "adapter_economics": {
            "cli_llm": {
                "billing_mode": "subscription_included",
                "budget_bucket": "anthropic_max",
                "effective_marginal_cost": 0.0,
            }
        },
        "prompt_mode": "stdin",
    }


def test_probe_providers_skips_non_cli_transport() -> None:
    admission = _provider_admission()
    admission["transport_kind"] = "http"
    conn = _FakeConn(
        {
            "FROM provider_transport_admissions": [admission],
            "FROM provider_cli_profiles": [_provider_profile()],
        }
    )
    snaps = asyncio.run(dh.probe_providers(conn, timeout_s=5))
    assert len(snaps) == 1
    assert snaps[0].status == "skipped"
    assert snaps[0].probe_kind == "provider_usage"


def test_probe_providers_skips_when_profile_missing() -> None:
    conn = _FakeConn(
        {
            "FROM provider_transport_admissions": [_provider_admission()],
            "FROM provider_cli_profiles": [],
        }
    )
    snaps = asyncio.run(dh.probe_providers(conn, timeout_s=5))
    assert len(snaps) == 1
    assert snaps[0].status == "skipped"


def test_probe_providers_successful_cli_call(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_subprocess_run(cmd, **kwargs):  # noqa: ANN001, ANN003
        return types.SimpleNamespace(
            returncode=0,
            stdout=json.dumps(
                {
                    "result": "HEARTBEAT_OK reply",
                    "usage": {"input_tokens": 10, "output_tokens": 3},
                    "model": "claude-sonnet-4-6",
                }
            ),
            stderr="",
        )

    monkeypatch.setattr(dh.subprocess, "run", fake_subprocess_run)
    conn = _FakeConn(
        {
            "FROM provider_transport_admissions": [_provider_admission()],
            "FROM provider_cli_profiles": [_provider_profile()],
        }
    )
    snaps = asyncio.run(dh.probe_providers(conn, timeout_s=5))
    assert len(snaps) == 1
    snap = snaps[0]
    assert snap.status == "ok"
    assert snap.input_tokens == 10
    assert snap.output_tokens == 3
    assert snap.subject_id == "anthropic"
    assert snap.subject_sub == "cli_llm"
    # model_slug now comes from the CLI response, not from pinned default_model.
    assert snap.details["model_slug"] == "claude-sonnet-4-6"


def test_probe_providers_survives_jsonb_encoded_base_flags(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: asyncpg returns jsonb as a string unless a codec is set.

    If the probe iterates that string instead of JSON-parsing, argv explodes
    into single characters and every CLI call blows up. This test feeds the
    probe the exact shape asyncpg actually delivers.
    """
    captured_commands: list[list[str]] = []

    def fake_subprocess_run(cmd, **kwargs):  # noqa: ANN001, ANN003
        captured_commands.append(list(cmd))
        return types.SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"result": "HEARTBEAT_OK", "usage": {"input_tokens": 1, "output_tokens": 1}}),
            stderr="",
        )

    monkeypatch.setattr(dh.subprocess, "run", fake_subprocess_run)

    profile = _provider_profile()
    # Simulate the drift: asyncpg hands jsonb back as an encoded string.
    profile["base_flags"] = json.dumps(profile["base_flags"])
    profile["adapter_economics"] = json.dumps(profile["adapter_economics"])

    conn = _FakeConn(
        {
            "FROM provider_transport_admissions": [_provider_admission()],
            "FROM provider_cli_profiles": [profile],
        }
    )
    snaps = asyncio.run(dh.probe_providers(conn, timeout_s=5))
    assert len(snaps) == 1
    assert snaps[0].status == "ok"
    # Argv should be ["claude", "--print", "--output-format", "json"] — 4 items,
    # not 33 characters. The explicit shape guards against future drift.
    assert captured_commands == [["claude", "--print", "--output-format", "json"]]


def test_probe_providers_rate_limited_maps_to_degraded(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_subprocess_run(cmd, **kwargs):  # noqa: ANN001, ANN003
        return types.SimpleNamespace(
            returncode=1, stdout="{}", stderr="Error 429: rate limit exceeded"
        )

    monkeypatch.setattr(dh.subprocess, "run", fake_subprocess_run)
    conn = _FakeConn(
        {
            "FROM provider_transport_admissions": [_provider_admission()],
            "FROM provider_cli_profiles": [_provider_profile()],
        }
    )
    snaps = asyncio.run(dh.probe_providers(conn, timeout_s=5))
    assert snaps[0].status == "degraded"
    assert snaps[0].details["rate_limited"] is True


def test_probe_providers_binary_missing_is_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_subprocess_run(cmd, **kwargs):  # noqa: ANN001, ANN003
        raise FileNotFoundError(cmd[0])

    monkeypatch.setattr(dh.subprocess, "run", fake_subprocess_run)
    conn = _FakeConn(
        {
            "FROM provider_transport_admissions": [_provider_admission()],
            "FROM provider_cli_profiles": [_provider_profile()],
        }
    )
    snaps = asyncio.run(dh.probe_providers(conn, timeout_s=5))
    assert snaps[0].status == "failed"
    assert snaps[0].details["error"] == "binary_not_found"


def test_probe_providers_timeout_is_failed(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_subprocess_run(cmd, **kwargs):  # noqa: ANN001, ANN003
        raise subprocess.TimeoutExpired(cmd, timeout=5)

    monkeypatch.setattr(dh.subprocess, "run", fake_subprocess_run)
    conn = _FakeConn(
        {
            "FROM provider_transport_admissions": [_provider_admission()],
            "FROM provider_cli_profiles": [_provider_profile()],
        }
    )
    snaps = asyncio.run(dh.probe_providers(conn, timeout_s=5))
    assert snaps[0].status == "failed"
    assert "timed out" in snaps[0].summary


# ---------------------------------------------------------------------------
# probe_mcp_servers
# ---------------------------------------------------------------------------


def test_probe_mcp_servers_empty_config_returns_no_snaps(tmp_path: Path) -> None:
    cfg = tmp_path / ".mcp.json"
    cfg.write_text(json.dumps({"mcpServers": {}}))
    snaps = asyncio.run(dh.probe_mcp_servers(timeout_s=1, mcp_config_path=cfg))
    assert snaps == []


def test_probe_mcp_servers_missing_config_returns_no_snaps(tmp_path: Path) -> None:
    snaps = asyncio.run(dh.probe_mcp_servers(timeout_s=1, mcp_config_path=tmp_path / "nope.json"))
    assert snaps == []


def test_probe_mcp_servers_non_stdio_is_skipped(tmp_path: Path) -> None:
    cfg = tmp_path / ".mcp.json"
    cfg.write_text(
        json.dumps(
            {
                "mcpServers": {
                    "my-http-server": {"type": "http", "url": "https://example.test"},
                }
            }
        )
    )
    snaps = asyncio.run(dh.probe_mcp_servers(timeout_s=1, mcp_config_path=cfg))
    assert len(snaps) == 1
    assert snaps[0].status == "skipped"
    assert snaps[0].details["transport"] == "http"


def test_probe_mcp_servers_stdio_delegates_to_probe(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def fake_probe(name, spec, *, timeout_s):  # noqa: ANN001, ANN003
        return dh.ProbeSnapshot(
            probe_kind="mcp_liveness",
            subject_id=name,
            status="ok",
            summary=f"{name}: stubbed",
            latency_ms=7,
            details={"transport": "stdio", "stub": True},
        )

    monkeypatch.setattr(dh, "_probe_stdio_mcp_server", fake_probe)
    cfg = tmp_path / ".mcp.json"
    cfg.write_text(
        json.dumps(
            {"mcpServers": {"praxis-workflow-mcp": {"command": "python3", "args": ["-m", "x"]}}}
        )
    )
    snaps = asyncio.run(dh.probe_mcp_servers(timeout_s=1, mcp_config_path=cfg))
    assert len(snaps) == 1
    assert snaps[0].subject_id == "praxis-workflow-mcp"
    assert snaps[0].status == "ok"
    assert snaps[0].details["stub"] is True


# ---------------------------------------------------------------------------
# run_daily_heartbeat — orchestrator
# ---------------------------------------------------------------------------


def _stub_probes(monkeypatch: pytest.MonkeyPatch, **kwargs: list[dh.ProbeSnapshot]) -> None:
    """Stub the four probe functions, each returning the provided list."""

    async def _providers(_conn, *, timeout_s):  # noqa: ANN001
        return list(kwargs.get("providers", []))

    async def _connectors(_conn):  # noqa: ANN001
        return list(kwargs.get("connectors", []))

    async def _credentials(_conn):  # noqa: ANN001
        return list(kwargs.get("credentials", []))

    async def _mcp(*, timeout_s, mcp_config_path=None):  # noqa: ANN001
        return list(kwargs.get("mcp", []))

    monkeypatch.setattr(dh, "probe_providers", _providers)
    monkeypatch.setattr(dh, "probe_connectors", _connectors)
    monkeypatch.setattr(dh, "probe_credentials", _credentials)
    monkeypatch.setattr(dh, "probe_mcp_servers", _mcp)


def test_run_daily_heartbeat_succeeded_when_all_probes_ok(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()

    async def _connect(_env=None):  # noqa: ANN001
        return conn

    monkeypatch.setattr(dh, "connect_workflow_database", _connect)

    snap_ok = dh.ProbeSnapshot(
        probe_kind="connector_liveness",
        subject_id="c1",
        status="ok",
        summary="c1 ok",
    )
    _stub_probes(monkeypatch, connectors=[snap_ok])

    result = asyncio.run(dh.run_daily_heartbeat(scope="connectors", triggered_by="test"))
    assert result.status == "succeeded"
    assert result.probes_total == 1
    assert result.probes_ok == 1
    assert result.probes_failed == 0
    assert result.scope == "connectors"
    assert result.triggered_by == "test"

    queries = [q for (q, _args) in conn.executed]
    assert any("INSERT INTO heartbeat_runs" in q for q in queries)
    assert any("INSERT INTO heartbeat_probe_snapshots" in q for q in queries)
    assert any("UPDATE heartbeat_runs" in q for q in queries)


def test_run_daily_heartbeat_partial_when_some_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()

    async def _connect(_env=None):  # noqa: ANN001
        return conn

    monkeypatch.setattr(dh, "connect_workflow_database", _connect)

    _stub_probes(
        monkeypatch,
        connectors=[
            dh.ProbeSnapshot(
                probe_kind="connector_liveness", subject_id="c1", status="ok", summary=""
            ),
            dh.ProbeSnapshot(
                probe_kind="connector_liveness", subject_id="c2", status="failed", summary=""
            ),
        ],
    )
    result = asyncio.run(dh.run_daily_heartbeat(scope="connectors", triggered_by="test"))
    assert result.status == "partial"
    assert result.probes_ok == 1
    assert result.probes_failed == 1


def test_run_daily_heartbeat_failed_when_all_fail(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()

    async def _connect(_env=None):  # noqa: ANN001
        return conn

    monkeypatch.setattr(dh, "connect_workflow_database", _connect)

    _stub_probes(
        monkeypatch,
        credentials=[
            dh.ProbeSnapshot(
                probe_kind="credential_expiry",
                subject_id="missing",
                status="failed",
                summary="",
            )
        ],
    )
    result = asyncio.run(dh.run_daily_heartbeat(scope="credentials", triggered_by="test"))
    assert result.status == "failed"
    assert result.probes_failed == 1
    assert result.probes_ok == 0


def test_run_daily_heartbeat_all_scope_runs_every_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()

    async def _connect(_env=None):  # noqa: ANN001
        return conn

    monkeypatch.setattr(dh, "connect_workflow_database", _connect)

    _stub_probes(
        monkeypatch,
        providers=[
            dh.ProbeSnapshot(
                probe_kind="provider_usage", subject_id="p", status="ok", summary=""
            )
        ],
        connectors=[
            dh.ProbeSnapshot(
                probe_kind="connector_liveness", subject_id="c", status="ok", summary=""
            )
        ],
        credentials=[
            dh.ProbeSnapshot(
                probe_kind="credential_expiry", subject_id="k", status="ok", summary=""
            )
        ],
        mcp=[
            dh.ProbeSnapshot(
                probe_kind="mcp_liveness", subject_id="m", status="ok", summary=""
            )
        ],
    )

    result = asyncio.run(dh.run_daily_heartbeat(scope="all", triggered_by="test"))
    kinds = {snap.probe_kind for snap in result.snapshots}
    assert kinds == {
        "provider_usage",
        "connector_liveness",
        "credential_expiry",
        "mcp_liveness",
    }
    assert result.probes_total == 4
    assert result.status == "succeeded"


def test_run_daily_heartbeat_scope_error_is_surfaced(monkeypatch: pytest.MonkeyPatch) -> None:
    conn = _FakeConn()

    async def _connect(_env=None):  # noqa: ANN001
        return conn

    monkeypatch.setattr(dh, "connect_workflow_database", _connect)

    async def _boom(_conn):  # noqa: ANN001
        raise RuntimeError("kaboom")

    monkeypatch.setattr(dh, "probe_connectors", _boom)

    result = asyncio.run(dh.run_daily_heartbeat(scope="connectors", triggered_by="test"))
    # No snapshots succeeded and we logged an error — orchestrator marks it failed.
    assert result.status == "failed"
    assert result.probes_total == 0


def test_run_id_is_unique_and_scope_tagged() -> None:
    now = datetime(2026, 4, 20, 9, 30, tzinfo=timezone.utc)
    a = dh._run_id(now, "all")
    b = dh._run_id(now, "all")
    assert a != b
    assert a.startswith("heartbeat_run.all.20260420T093000Z.")
    assert len(a.split(".")[-1]) == 8  # 8-char uuid suffix


def test_resolved_scopes_expands_all() -> None:
    assert dh._resolved_scopes("all") == (
        "providers",
        "connectors",
        "credentials",
        "mcp",
        "model_retirement",
    )
    assert dh._resolved_scopes("providers") == ("providers",)
