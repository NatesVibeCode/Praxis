"""Tests for runtime.integration_manifest — declarative integration loading."""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from unittest.mock import patch

import pytest

from runtime.integration_manifest import (
    ActionSpec,
    AuthShape,
    IntegrationManifest,
    ManifestLoadReport,
    _interpolate_template,
    _parse_manifest,
    build_manifest_handler,
    load_manifest_report,
    load_manifests,
    manifest_to_registry_row,
    resolve_token,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def manifest_dir(tmp_path):
    toml_content = textwrap.dedent("""\
        [integration]
        id = "test-api"
        name = "Test API"
        description = "A test integration."
        provider = "http"
        icon = "zap"

        [auth]
        kind = "api_key"
        env_var = "TEST_API_KEY"

        [[capabilities]]
        action = "send"
        description = "Send a message."
        method = "POST"
        path = "https://api.example.com/send"

        [capabilities.body_template]
        message = "{{message}}"
        channel = "{{channel}}"

        [[capabilities]]
        action = "list"
        description = "List items."
        method = "GET"
        path = "https://api.example.com/items"
    """)
    (tmp_path / "test-api.toml").write_text(toml_content)
    return tmp_path


@pytest.fixture()
def manifest(manifest_dir):
    manifests = load_manifests(manifest_dir)
    assert len(manifests) == 1
    return manifests[0]


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

class TestManifestParsing:
    def test_load_from_directory(self, manifest):
        assert manifest.id == "test-api"
        assert manifest.name == "Test API"
        assert manifest.provider == "http"
        assert manifest.icon == "zap"

    def test_auth_shape(self, manifest):
        assert manifest.auth_shape.kind == "api_key"
        assert manifest.auth_shape.env_var == "TEST_API_KEY"

    def test_capabilities(self, manifest):
        assert len(manifest.capabilities) == 2
        send = manifest.capabilities[0]
        assert send.action == "send"
        assert send.method == "POST"
        assert send.path == "https://api.example.com/send"
        assert send.body_template == {"message": "{{message}}", "channel": "{{channel}}"}

    def test_empty_directory(self, tmp_path):
        assert load_manifests(tmp_path) == []

    def test_nonexistent_directory(self):
        assert load_manifests(Path("/nonexistent")) == []

    def test_load_manifest_report_keeps_good_rows_and_surfaces_errors(self, tmp_path):
        (tmp_path / "good.toml").write_text(
            '[integration]\nid = "good"\nname = "Good"\nprovider = "http"\n'
        )
        (tmp_path / "bad.toml").write_text("this is not [valid toml")

        report = load_manifest_report(tmp_path)

        assert isinstance(report, ManifestLoadReport)
        assert [manifest.id for manifest in report.manifests] == ["good"]
        assert len(report.errors) == 1
        assert "bad.toml" in report.errors[0]

    def test_malformed_toml_skipped(self, tmp_path):
        (tmp_path / "bad.toml").write_text("this is not [valid toml")
        manifests = load_manifests(tmp_path)
        assert manifests == []

    def test_invalid_id_rejected(self, tmp_path):
        toml_content = '[integration]\nid = "!!!bad!!!"\nname = "Bad"\n'
        (tmp_path / "bad-id.toml").write_text(toml_content)
        manifests = load_manifests(tmp_path)
        assert manifests == []

    def test_oversized_file_rejected(self, tmp_path):
        content = "[integration]\nname = 'x'\n" + ("# pad\n" * 20_000)
        (tmp_path / "big.toml").write_text(content)
        manifests = load_manifests(tmp_path)
        assert manifests == []

    def test_bad_url_scheme_rejected(self, tmp_path):
        toml_content = (
            '[integration]\nid = "bad-scheme"\nname = "Bad"\n\n'
            '[[capabilities]]\naction = "run"\npath = "ftp://evil.com/x"\n'
        )
        (tmp_path / "bad-scheme.toml").write_text(toml_content)
        manifests = load_manifests(tmp_path)
        assert manifests == []

    def test_bad_http_method_rejected(self, tmp_path):
        toml_content = (
            '[integration]\nid = "bad-method"\nname = "Bad"\n\n'
            '[[capabilities]]\naction = "run"\nmethod = "HACK"\n'
            'path = "https://example.com"\n'
        )
        (tmp_path / "bad-method.toml").write_text(toml_content)
        manifests = load_manifests(tmp_path)
        assert manifests == []

    def test_too_many_capabilities_rejected(self, tmp_path):
        caps = "\n".join(
            f'[[capabilities]]\naction = "a{i}"\npath = "https://x.com/{i}"\n'
            for i in range(51)
        )
        toml_content = f'[integration]\nid = "too-many"\nname = "Big"\n\n{caps}'
        (tmp_path / "too-many.toml").write_text(toml_content)
        manifests = load_manifests(tmp_path)
        assert manifests == []


# ---------------------------------------------------------------------------
# Registry row generation
# ---------------------------------------------------------------------------

class TestRegistryRow:
    def test_row_shape(self, manifest):
        row = manifest_to_registry_row(manifest)
        assert row["id"] == "test-api"
        assert row["name"] == "Test API"
        assert row["provider"] == "http"
        assert row["manifest_source"] == "manifest"
        assert row["auth_status"] == "connected"
        assert row["mcp_server_id"] is None

    def test_capabilities_carry_action_details(self, manifest):
        row = manifest_to_registry_row(manifest)
        caps = row["capabilities"]
        assert len(caps) == 2
        assert caps[0]["action"] == "send"
        assert caps[0]["method"] == "POST"
        assert caps[0]["path"] == "https://api.example.com/send"
        assert caps[0]["body_template"] == {"message": "{{message}}", "channel": "{{channel}}"}
        assert caps[1]["action"] == "list"
        assert caps[1]["method"] == "GET"

    def test_auth_shape_dict(self, manifest):
        row = manifest_to_registry_row(manifest)
        shape = row["auth_shape"]
        assert shape["kind"] == "api_key"
        assert shape["env_var"] == "TEST_API_KEY"

    def test_endpoint_templates_dict(self, manifest):
        row = manifest_to_registry_row(manifest)
        assert row["endpoint_templates"]["send"] == "https://api.example.com/send"


# ---------------------------------------------------------------------------
# Token resolution
# ---------------------------------------------------------------------------

class TestTokenResolution:
    def test_resolve_token_does_not_fall_back_to_env_when_credential_ref_fails(self, monkeypatch):
        monkeypatch.setattr(
            "adapters.credentials.resolve_credential",
            lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("credential_ref failed")),
        )
        monkeypatch.setattr(
            "adapters.keychain.resolve_secret",
            lambda *_args, **_kwargs: "env-token",
        )

        assert resolve_token(
            {"credential_ref": "secret.demo.connector", "env_var": "TEST_API_KEY"},
            pg=object(),
            integration_id="demo-int",
        ) is None

    def test_resolve_token_still_uses_env_var_auth_when_no_credential_ref(self, monkeypatch):
        monkeypatch.setattr(
            "adapters.keychain.resolve_secret",
            lambda *_args, **_kwargs: "env-token",
        )

        assert resolve_token(
            {"env_var": "TEST_API_KEY"},
            pg=object(),
            integration_id="demo-int",
        ) == "env-token"


# ---------------------------------------------------------------------------
# Template interpolation
# ---------------------------------------------------------------------------

class TestInterpolation:
    def test_simple_replacement(self):
        template = {"msg": "{{message}}", "ch": "{{channel}}"}
        result = _interpolate_template(template, {"message": "hello", "channel": "#general"})
        assert result == {"msg": "hello", "ch": "#general"}

    def test_nested_replacement(self):
        template = {"outer": {"inner": "{{value}}"}}
        result = _interpolate_template(template, {"value": "42"})
        assert result == {"outer": {"inner": "42"}}

    def test_no_placeholders_passthrough(self):
        template = {"static": "value", "number": 42}
        result = _interpolate_template(template, {"unused": "x"})
        assert result == {"static": "value", "number": 42}

    def test_missing_arg_leaves_placeholder(self):
        template = {"msg": "{{missing}}"}
        result = _interpolate_template(template, {})
        assert result == {"msg": "{{missing}}"}


# ---------------------------------------------------------------------------
# Handler construction
# ---------------------------------------------------------------------------

class TestManifestHandler:
    def test_build_returns_callable(self, manifest):
        definition = manifest_to_registry_row(manifest)
        definition["manifest_source"] = "manifest"
        handler = build_manifest_handler(definition, "send")
        assert callable(handler)

    def test_build_returns_none_for_unknown_action(self, manifest):
        definition = manifest_to_registry_row(manifest)
        handler = build_manifest_handler(definition, "nonexistent")
        assert handler is None

    def test_handler_makes_http_call(self, manifest):
        definition = manifest_to_registry_row(manifest)
        handler = build_manifest_handler(definition, "send")

        class FakeResponse:
            status = 200
            def read(self):
                return json.dumps({"ok": True}).encode()
            def __enter__(self):
                return self
            def __exit__(self, *a):
                pass

        # Handler delegates to execute_webhook, which uses urllib
        with patch("runtime.integrations.webhook.urllib.request.urlopen", return_value=FakeResponse()) as mock_urlopen:
            result = handler({"message": "hi", "channel": "#test"}, None)

        assert result["status"] == "succeeded"
        assert result["data"]["http_status"] == 200
        req = mock_urlopen.call_args[0][0]
        assert req.full_url == "https://api.example.com/send"
        assert req.method == "POST"
        body = json.loads(req.data)
        assert body["message"] == "hi"
        assert body["channel"] == "#test"
