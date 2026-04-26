from __future__ import annotations

import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from runtime.compile_artifacts import CompileArtifactError, CompileArtifactRecord
import runtime.compile_index as compile_index
import runtime.compiler as compiler
import runtime.compiler_semantic as compiler_semantic

_REPO_ROOT = str(Path(__file__).resolve().parents[4])


class _FakeConn:
    def execute(self, query: str, *args):
        if "FROM compiler_route_hints" in query:
            return [
                {"hint_text": "review", "route_slug": "auto/review"},
                {"hint_text": "build", "route_slug": "auto/build"},
                {"hint_text": "triage", "route_slug": "auto/build"},
            ]
        return []


class _RecordingCompileConn(_FakeConn):
    def __init__(self) -> None:
        self.events: list[tuple[str, tuple]] = []

    def execute(self, query: str, *args):
        self.events.append((query, args))
        return super().execute(query, *args)


class _WorkflowCompileConn(_FakeConn):
    def execute(self, query: str, *args):
        if "FROM workflow_jobs" in query:
            return [{
                "status": "succeeded",
                "stdout_preview": json.dumps(
                    {
                        "title": "Support Mail",
                        "prose": "Use @gmail/search before triage-agent reviews the queue.",
                        "authority": "",
                        "sla": {},
                        "capabilities": [],
                    }
                ),
            }]
        return super().execute(query, *args)


class _StubMatcher:
    def __init__(self, conn, embedder=None) -> None:
        self._conn = conn
        self._embedder = embedder

    def match(self, intent: str, limit: int = 10):
        return SimpleNamespace(
            ui_components=(),
            calculations=(),
            workflows=(),
        )

    def compose(self, intent: str, matches):
        return SimpleNamespace(
            components=(),
            calculations=(),
            workflows=(),
            bindings=(),
            layout_suggestion="",
            confidence=0.0,
        )


class _StubEmbedder:
    model_name = "test-embedder"
    dimensions = 3

    def embed_one(self, text: str):
        return (0.1, 0.2, 0.3)


@pytest.fixture(autouse=True)
def _stub_compiler_embedder(monkeypatch) -> None:
    monkeypatch.setattr(compiler_semantic, "_COMPILER_EMBEDDER", _StubEmbedder())
    monkeypatch.setattr(compiler_semantic, "_COMPILER_EMBEDDER_ERROR", None)


def _compile_index_snapshot(
    *,
    catalog: list[dict[str, object]],
    integrations: list[dict[str, object]],
    object_types: list[dict[str, object]],
    capabilities: list[dict[str, object]],
    route_hints: tuple[tuple[str, str], ...],
    compile_index_ref: str = "compile_index.compiler.test",
    compile_surface_revision: str = "compile_surface.compiler.test",
    repo_root: str = _REPO_ROOT,
    repo_fingerprint: str = "compile-index-fingerprint",
    freshness_state: str = "fresh",
    freshness_reason: str | None = None,
) -> compile_index.CompileIndexSnapshot:
    source_counts = {
        "reference_catalog": len(catalog),
        "integration_registry": len(integrations),
        "object_types": len(object_types),
        "compiler_route_hints": len(route_hints),
        "capability_catalog": len(capabilities),
    }
    source_fingerprints = {
        "reference_catalog": "catalog-fingerprint",
        "integration_registry": "integration-fingerprint",
        "object_types": "object-type-fingerprint",
        "compiler_route_hints": "route-hint-fingerprint",
        "capability_catalog": "capability-fingerprint",
    }
    repo_info = {
        "repo_root": repo_root,
        "git_head": "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef",
        "git_branch": "main",
        "git_dirty": False,
        "git_status_hash": "0123456789abcdef",
        "repo_fingerprint": repo_fingerprint,
    }
    payload = {
        "schema_version": 1,
        "repo_info": repo_info,
        "surface_manifest": {
            "repo_root": repo_root,
            "surface_name": "compiler",
            "surface_revision": "surface_compiler_manifest_test",
            "tracked_files": ["Code&DBs/Workflow/runtime/compiler.py"],
            "file_fingerprints": {
                "Code&DBs/Workflow/runtime/compiler.py": "surface-file-fingerprint"
            },
        },
        "source_fingerprints": source_fingerprints,
        "source_counts": source_counts,
        "reference_catalog": catalog,
        "integration_registry": integrations,
        "object_types": object_types,
        "compiler_route_hints": [
            {"hint_text": hint, "route_slug": route}
            for hint, route in route_hints
        ],
        "capability_catalog": capabilities,
    }
    return compile_index.CompileIndexSnapshot(
        schema_version=1,
        compile_index_ref=compile_index_ref,
        compile_surface_revision=compile_surface_revision,
        compile_surface_name="compiler",
        repo_root=repo_root,
        repo_fingerprint=repo_fingerprint,
        repo_info=repo_info,
        surface_manifest=payload["surface_manifest"],
        source_fingerprints=source_fingerprints,
        source_counts=source_counts,
        decision_ref="decision.compile.index.refresh.test",
        refresh_count=1,
        refreshed_at=datetime(2026, 4, 8, 19, 0, tzinfo=timezone.utc),
        stale_after_at=datetime(2026, 4, 8, 20, 0, tzinfo=timezone.utc),
        freshness_state=freshness_state,
        freshness_reason=freshness_reason,
        reference_catalog=tuple(catalog),
        integration_registry=tuple(integrations),
        object_types=tuple(object_types),
        compiler_route_hints=route_hints,
        capability_catalog=tuple(capabilities),
        payload=payload,
    )


def test_call_llm_compile_resolves_via_task_type_routing(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_call_llm(request):
        captured["request_provider"] = request.provider_slug
        captured["request_model"] = request.model_slug
        captured["request_endpoint"] = request.endpoint_uri
        captured["request_protocol"] = request.protocol_family
        captured["request_api_key"] = request.api_key
        return SimpleNamespace(
            content=json.dumps(
                {
                    "title": "Support Mail",
                    "prose": "Use @gmail/search before triage-agent reviews the queue.",
                    "authority": "",
                    "sla": {},
                    "capabilities": [],
                }
            ),
        )

    def _fake_endpoint(provider, model):
        captured["endpoint_provider"] = provider
        captured["endpoint_model"] = model
        return "https://broker.example/v1/chat/completions"

    def _fake_protocol(provider):
        captured["protocol_provider"] = provider
        return "openai_chat_completions"

    def _fake_env_vars(provider):
        captured["env_vars_provider"] = provider
        return ["SAMPLE_BROKER_API_KEY"]

    def _fake_resolve_secret(name, *, env=None):
        captured.setdefault("secret_names", []).append(name)
        return "sk-test"

    def _fake_get_pool():
        return object()

    class _FakeSyncConn:
        def __init__(self, pool):
            captured["conn_pool"] = pool

        def fetch(self, query, *args):
            captured["catalog_query"] = " ".join(str(query).split())
            captured["catalog_args"] = args
            return [
                {
                    "provider_slug": "sample-broker",
                    "model_slug": "vendor/some-model",
                }
            ]

    monkeypatch.setitem(
        sys.modules,
        "adapters.llm_client",
        SimpleNamespace(LLMRequest=SimpleNamespace, call_llm=_fake_call_llm),
    )
    monkeypatch.setitem(
        sys.modules,
        "adapters.keychain",
        SimpleNamespace(resolve_secret=_fake_resolve_secret),
    )
    monkeypatch.setitem(
        sys.modules,
        "registry.provider_execution_registry",
        SimpleNamespace(
            resolve_api_endpoint=_fake_endpoint,
            resolve_api_protocol_family=_fake_protocol,
            resolve_api_key_env_vars=_fake_env_vars,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "storage.postgres.connection",
        SimpleNamespace(
            SyncPostgresConnection=_FakeSyncConn,
            get_workflow_pool=_fake_get_pool,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "registry.native_runtime_profile_sync",
        SimpleNamespace(default_native_runtime_profile_ref=lambda _pg: "nate-private"),
    )

    result = compiler._call_llm_compile(
        "Route support mail",
        "Context: support queue",
        conn=_WorkflowCompileConn(),
    )

    assert "effective_private_provider_job_catalog" in captured["catalog_query"]
    assert captured["catalog_args"] == ("nate-private",)
    assert captured["endpoint_provider"] == "sample-broker"
    assert captured["endpoint_model"] == "vendor/some-model"
    assert captured["protocol_provider"] == "sample-broker"
    assert captured["env_vars_provider"] == "sample-broker"
    assert captured["secret_names"] == ["SAMPLE_BROKER_API_KEY"]
    assert captured["request_provider"] == "sample-broker"
    assert captured["request_model"] == "vendor/some-model"
    assert captured["request_protocol"] == "openai_chat_completions"
    assert captured["request_api_key"] == "sk-test"
    assert result["title"] == "Support Mail"
    assert result["prose"] == "Use @gmail/search before triage-agent reviews the queue."


def test_call_llm_compile_falls_back_to_next_llm_task_route(monkeypatch) -> None:
    captured: dict[str, object] = {"calls": []}

    def _fake_call_llm(request):
        captured["calls"].append((request.provider_slug, request.model_slug))
        if request.model_slug == "deepseek/deepseek-v4-flash":
            raise RuntimeError("HTTP 429: upstream rate limited")
        return SimpleNamespace(
            content=json.dumps(
                {
                    "title": "Fallback Compile",
                    "prose": "Use @webhook/post then compile-agent normalizes the request.",
                    "authority": "",
                    "sla": {},
                    "capabilities": [],
                }
            ),
        )

    def _fake_endpoint(provider, model):
        return "https://broker.example/v1/chat/completions"

    def _fake_protocol(provider):
        return "openai_chat_completions"

    def _fake_env_vars(provider):
        return ["OPENROUTER_API_KEY"]

    def _fake_resolve_secret(name, *, env=None):
        return "sk-test"

    class _FakeSyncConn:
        def __init__(self, pool):
            captured["conn_pool"] = pool

        def fetch(self, query, *args):
            captured["catalog_query"] = " ".join(str(query).split())
            captured["catalog_args"] = args
            return [
                {
                    "provider_slug": "openrouter",
                    "model_slug": "deepseek/deepseek-v4-flash",
                },
                {
                    "provider_slug": "openrouter",
                    "model_slug": "deepseek/deepseek-v4-pro",
                },
            ]

    monkeypatch.setitem(
        sys.modules,
        "adapters.llm_client",
        SimpleNamespace(LLMRequest=SimpleNamespace, call_llm=_fake_call_llm),
    )
    monkeypatch.setitem(
        sys.modules,
        "adapters.keychain",
        SimpleNamespace(resolve_secret=_fake_resolve_secret),
    )
    monkeypatch.setitem(
        sys.modules,
        "registry.provider_execution_registry",
        SimpleNamespace(
            resolve_api_endpoint=_fake_endpoint,
            resolve_api_protocol_family=_fake_protocol,
            resolve_api_key_env_vars=_fake_env_vars,
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "storage.postgres.connection",
        SimpleNamespace(
            SyncPostgresConnection=_FakeSyncConn,
            get_workflow_pool=lambda: object(),
        ),
    )
    monkeypatch.setitem(
        sys.modules,
        "registry.native_runtime_profile_sync",
        SimpleNamespace(default_native_runtime_profile_ref=lambda _pg: "nate-private"),
    )

    result = compiler._call_llm_compile(
        "Normalize a webhook request",
        "Context: fallback test",
        conn=_WorkflowCompileConn(),
    )

    assert "effective_private_provider_job_catalog" in captured["catalog_query"]
    assert captured["catalog_args"] == ("nate-private",)
    assert captured["calls"] == [
        ("openrouter", "deepseek/deepseek-v4-flash"),
        ("openrouter", "deepseek/deepseek-v4-pro"),
    ]
    assert result["title"] == "Fallback Compile"


def test_compile_prose_uses_preloaded_compile_index_snapshot_without_reloading(monkeypatch) -> None:
    snapshot = _compile_index_snapshot(
        catalog=[
            {
                "slug": "@gmail/search",
                "ref_type": "integration",
                "display_name": "Gmail Search",
                "resolved_id": "gmail",
                "resolved_table": "integration_registry",
                "description": "Search connected Gmail accounts",
            }
        ],
        integrations=[
            {
                "id": "gmail",
                "name": "Gmail",
                "provider": "google",
                "auth_status": "connected",
                "description": "Mail provider",
                "capabilities": [{"action": "search", "description": "Search inbox"}],
            }
        ],
        object_types=[],
        capabilities=compiler._build_capability_catalog(
            [
                {
                    "id": "gmail",
                    "name": "Gmail",
                    "provider": "google",
                    "auth_status": "connected",
                    "description": "Mail provider",
                    "capabilities": [{"action": "search", "description": "Search inbox"}],
                }
            ]
        ),
        route_hints=(("review", "auto/review"),),
    )

    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda *args, **kwargs: pytest.fail("compile index should be prehydrated by the caller"),
    )
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)

    result = compiler.compile_prose(
        "Triage @gmail/search with review-agent",
        compile_index_snapshot=snapshot,
        conn=_FakeConn(),
    )

    assert result["compile_index"]["compile_index_ref"] == snapshot.compile_index_ref
    assert result["compile_index"]["freshness_state"] == "fresh"


def test_compile_prose_reuses_definition_artifact_before_semantic_retrieval(monkeypatch) -> None:
    snapshot = _compile_index_snapshot(
        catalog=[],
        integrations=[],
        object_types=[],
        capabilities=[],
        route_hints=(),
    )
    first_result = compiler.compile_prose(
        "Triage support email",
        title="Support Mail",
        compile_index_snapshot=snapshot,
        conn=_FakeConn(),
    )
    definition = first_result["definition"]
    payload_json = json.dumps(definition, sort_keys=True, separators=(",", ":"), default=str)
    reusable = CompileArtifactRecord(
        compile_artifact_id="compile_artifact.definition.reused1234567890",
        artifact_kind="definition",
        artifact_ref=definition["definition_revision"],
        revision_ref=definition["definition_revision"],
        parent_artifact_ref=None,
        input_fingerprint=definition["compile_provenance"]["input_fingerprint"],
        content_hash=hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        authority_refs=(),
        payload=definition,
        decision_ref="decision.compile.definition.reused1234567890",
    )

    monkeypatch.setattr(
        compiler.CompileArtifactStore,
        "load_reusable_artifact",
        lambda self, *, artifact_kind, input_fingerprint: reusable,
    )
    monkeypatch.setattr(
        "runtime.intent_matcher.IntentMatcher",
        lambda *args, **kwargs: pytest.fail("intent matcher should not run when definition reuse hits"),
    )

    result = compiler.compile_prose(
        "Triage support email",
        title="Support Mail",
        compile_index_snapshot=snapshot,
        conn=_FakeConn(),
    )

    assert result["definition"]["definition_revision"] == definition["definition_revision"]
    assert result["semantic_retrieval"] == {
        "mode": "reused",
        "reason": "definition.compile.exact_input_match",
    }
    assert result["reuse_provenance"]["decision"] == "reused"
    assert result["reuse_provenance"]["input_fingerprint"] == definition["compile_provenance"]["input_fingerprint"]


def test_compile_prose_fails_on_malformed_reusable_definition_artifact(monkeypatch) -> None:
    snapshot = _compile_index_snapshot(
        catalog=[],
        integrations=[],
        object_types=[],
        capabilities=[],
        route_hints=(),
    )
    monkeypatch.setattr(
        compiler.CompileArtifactStore,
        "load_reusable_artifact",
        lambda self, *, artifact_kind, input_fingerprint: (_ for _ in ()).throw(
            CompileArtifactError("reusable compile artifact payload hash does not match the recorded content_hash")
        ),
    )

    with pytest.raises(RuntimeError, match="compile_artifact.reuse_failed"):
        compiler.compile_prose(
            "Triage support email",
            title="Support Mail",
            compile_index_snapshot=snapshot,
            conn=_FakeConn(),
        )


def test_compile_prose_fails_when_definition_artifact_persistence_fails(monkeypatch) -> None:
    snapshot = _compile_index_snapshot(
        catalog=[],
        integrations=[],
        object_types=[],
        capabilities=[],
        route_hints=(),
    )
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)
    monkeypatch.setattr(
        compiler.CompileArtifactStore,
        "record_definition",
        lambda self, **kwargs: (_ for _ in ()).throw(
            CompileArtifactError("compile artifact write rejected")
        ),
    )

    with pytest.raises(RuntimeError, match="compile_artifact.persist_failed"):
        compiler.compile_prose(
            "Triage support email",
            title="Support Mail",
            compile_index_snapshot=snapshot,
            conn=_FakeConn(),
        )


def test_compile_prose_refreshes_stale_compile_index_snapshot_when_unpinned(monkeypatch) -> None:
    monkeypatch.setattr(compiler, "_get_connection", lambda: _FakeConn())
    refreshed_snapshot = _compile_index_snapshot(
        catalog=[],
        integrations=[],
        object_types=[],
        capabilities=[],
        route_hints=(),
        compile_index_ref="compile_index.compiler.refreshed",
        compile_surface_revision="compile_surface.compiler.refreshed",
    )
    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            compile_index.CompileIndexAuthorityError(
                "compile_index.snapshot_stale",
                "compile index snapshot is stale",
                details={"freshness_state": "stale"},
            )
        ),
    )
    refresh_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        compiler,
        "refresh_compile_index",
        lambda conn, **kwargs: refresh_calls.append(dict(kwargs)) or refreshed_snapshot,
    )
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)

    result = compiler.compile_prose("Handle stale compile index")

    assert result["compile_index"]["compile_index_ref"] == "compile_index.compiler.refreshed"
    assert refresh_calls == [
        {
            "repo_root": Path(_REPO_ROOT),
            "surface_name": "compiler",
        }
    ]


def test_compile_prose_preserves_stale_pinned_compile_index_failure(monkeypatch) -> None:
    monkeypatch.setattr(compiler, "_get_connection", lambda: _FakeConn())
    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            compile_index.CompileIndexAuthorityError(
                "compile_index.snapshot_stale",
                "compile index snapshot is stale",
                details={"freshness_state": "stale"},
            )
        ),
    )
    monkeypatch.setattr(
        compiler,
        "refresh_compile_index",
        lambda *args, **kwargs: pytest.fail("pinned snapshot should not auto-refresh"),
    )

    with pytest.raises(RuntimeError, match="compile_index.snapshot_stale"):
        compiler.compile_prose(
            "Handle stale compile index",
            compile_surface_revision="compile_surface.compiler.pinned",
        )


def test_compile_prose_fails_closed_when_database_authority_is_missing(monkeypatch) -> None:
    monkeypatch.setattr(
        compiler,
        "_get_connection",
        lambda: (_ for _ in ()).throw(
            compiler.PostgresConfigurationError(
                "postgres.config_missing",
                "WORKFLOW_DATABASE_URL must be set to a Postgres DSN",
            )
        ),
    )

    with pytest.raises(RuntimeError, match="postgres.config_missing"):
        compiler.compile_prose("Compile without database authority")


def test_hydrate_env_from_dotenv_only_loads_explicit_repo_env_values(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.delenv("WORKFLOW_DATABASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.setattr(compiler, "_compiler_repo_root", lambda: tmp_path)
    (tmp_path / ".env").write_text(
        "OPENAI_API_KEY=test-openai-key\n",
        encoding="utf-8",
    )

    compiler._hydrate_env_from_dotenv()

    assert "WORKFLOW_DATABASE_URL" not in os.environ
    assert os.environ["OPENAI_API_KEY"] == "test-openai-key"


def test_compile_prose_returns_plain_dict_when_llm_falls_back(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_COMPILER_ENABLE_LLM", "1")
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)

    def _raise_missing_llm(prose: str, context: str, *, conn=None) -> dict[str, object]:
        raise RuntimeError("planner unavailable")

    monkeypatch.setattr(compiler, "_call_llm_compile", _raise_missing_llm)

    integrations = [
        {
            "id": "gmail",
            "name": "Gmail",
            "provider": "google",
            "auth_status": "connected",
            "description": "Mail provider",
            "capabilities": [{"action": "search", "description": "Search inbox"}],
        }
    ]
    object_types = [
        {
            "type_id": "ticket",
            "name": "Ticket",
            "description": "Support ticket",
            "fields": [{"name": "status", "type": "string", "description": "Workflow state"}],
        }
    ]
    snapshot = _compile_index_snapshot(
        catalog=[
            {
                "slug": "@gmail/search",
                "ref_type": "integration",
                "display_name": "Gmail Search",
                "resolved_id": "gmail",
                "resolved_table": "integration_registry",
                "description": "Search connected Gmail accounts",
            },
            {
                "slug": "#ticket/status",
                "ref_type": "object",
                "display_name": "Ticket Status",
                "resolved_id": "ticket",
                "resolved_table": "object_types",
                "description": "Ticket state field",
            },
        ],
        integrations=integrations,
        object_types=object_types,
        capabilities=compiler._build_capability_catalog(integrations),
        route_hints=(
            ("review", "auto/review"),
            ("build", "auto/build"),
            ("triage", "auto/build"),
        ),
    )
    snapshot_calls: list[dict[str, object]] = []
    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda conn, **kwargs: snapshot_calls.append(dict(kwargs)) or snapshot,
    )
    monkeypatch.setattr(compiler, "_get_connection", lambda: _FakeConn())

    result = compiler.compile_prose("Triage @gmail/search with review-agent for #ticket/status")
    definition = result["definition"]

    assert isinstance(result, dict)
    assert "jobs" not in result
    assert "execution_plan" not in result
    assert "execution_setup" not in result
    assert "surface_manifest" not in result
    assert "build_receipt" not in result
    assert definition["execution_setup"]["method"]["key"] == "grounded_research"
    assert any(
        command["id"] == "generate_plan"
        for command in definition["surface_manifest"]["surface_now"]["commands"]
    )
    assert definition["build_receipt"]["decisions"][0]["aspect"] == "method"
    assert definition["build_receipt"]["data_audit"]["transport_mode"] == "definition_embedded"
    assert definition["build_receipt"]["data_gaps"]
    assert definition["execution_setup"]["method"]["key"] == "grounded_research"
    assert definition["type"] == "operating_model"
    assert definition["source_prose"] == "Triage @gmail/search with review-agent for #ticket/status"
    assert definition["compiled_prose"] == "Triage @gmail/search with review-agent for #ticket/status"
    assert isinstance(definition["references"], list)
    assert isinstance(definition["capabilities"], list)
    assert definition["definition_graph"]["version"] == 1
    assert any(node["kind"] == "draft_step" for node in definition["definition_graph"]["nodes"])
    assert definition["definition_graph"]["metadata"]["compiled_prose"] == definition["compiled_prose"]
    assert all(isinstance(reference, dict) for reference in definition["references"])
    assert isinstance(definition["narrative_blocks"], list)
    assert isinstance(definition["draft_flow"], list)
    assert definition["draft_flow"][0]["reference_slugs"] == ["@gmail/search", "review-agent", "#ticket/status"]
    assert isinstance(definition["draft_flow"][0]["capability_slugs"], list)
    assert definition["trigger_intent"] == []
    assert definition["definition_revision"].startswith("def_")
    assert "planner unavailable" in (result["error"] or "")
    assert result["compile_index"]["compile_index_ref"] == snapshot.compile_index_ref
    assert result["compile_index"]["compile_surface_revision"] == snapshot.compile_surface_revision
    assert result["compile_index"]["freshness_state"] == "fresh"
    assert snapshot_calls == [
        {
            "snapshot_ref": None,
            "surface_revision": None,
            "surface_name": "compiler",
            "require_fresh": True,
            "repo_root": Path(_REPO_ROOT),
        }
    ]
    assert result["semantic_retrieval"]["mode"] == "semantic"
    assert result["semantic_retrieval"]["reason"] is None
    assert result["refinement"]["status"] == "fallback"
    assert result["refinement"]["applied"] is False


def test_compile_prose_remains_bootstrap_only_when_projection_is_ready(monkeypatch) -> None:
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)
    monkeypatch.setattr(compiler, "_get_connection", lambda: _FakeConn())
    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda conn, **kwargs: _compile_index_snapshot(
            catalog=[],
            integrations=[],
            object_types=[],
            capabilities=compiler._build_capability_catalog([]),
            route_hints=(("build", "auto/build"),),
        ),
    )
    monkeypatch.setattr(
        "runtime.operating_model_planner.plan_definition",
        lambda *_args, **_kwargs: pytest.fail("compile bootstrap must not auto-plan"),
    )

    result = compiler.compile_prose(
        "Research existing workflow findings before build",
        conn=_FakeConn(),
    )

    assert result["compiled_spec"] is None
    assert result["projection_status"]["state"] == "ready"
    assert [capability["slug"] for capability in result["definition"]["capabilities"]] == [
        "research/local-knowledge"
    ]
    assert any(
        "bootstrap planning state only" in note
        for note in result["planning_notes"]
    )


def test_compile_prose_uses_llm_output_and_resolves_catalog_entries(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_COMPILER_ENABLE_LLM", "1")
    catalog = [
        {
            "slug": "@gmail/search",
            "ref_type": "integration",
            "display_name": "Gmail Search",
            "resolved_id": "gmail",
            "resolved_table": "integration_registry",
            "description": "Search connected Gmail accounts",
        },
        {
            "slug": "#ticket/status",
            "ref_type": "object",
            "display_name": "Ticket Status",
            "resolved_id": "ticket",
            "resolved_table": "object_types",
            "description": "Ticket state field",
        },
    ]
    integrations = [
        {
            "id": "gmail",
            "name": "Gmail",
            "provider": "google",
            "auth_status": "connected",
            "description": "Mail provider",
            "capabilities": [{"action": "search", "description": "Search inbox"}],
        }
    ]
    object_types = [
        {
            "type_id": "ticket",
            "name": "Ticket",
            "description": "Support ticket",
            "fields": [{"name": "status", "type": "string", "description": "Workflow state"}],
        }
    ]

    monkeypatch.setattr(compiler, "_get_connection", lambda: _FakeConn())
    monkeypatch.setattr(compiler, "_load_reference_catalog", lambda conn: catalog)
    monkeypatch.setattr(compiler, "_load_integrations", lambda conn: integrations)
    monkeypatch.setattr(compiler, "_load_object_types", lambda conn: object_types)
    monkeypatch.setattr(compiler, "load_capability_catalog", lambda conn: compiler._build_capability_catalog(integrations))
    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda conn, **kwargs: _compile_index_snapshot(
            catalog=catalog,
            integrations=integrations,
            object_types=object_types,
            capabilities=compiler._build_capability_catalog(integrations),
            route_hints=(
                ("review", "auto/review"),
                ("build", "auto/build"),
                ("triage", "auto/build"),
            ),
        ),
    )
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)
    monkeypatch.setattr(
        compiler,
        "_call_llm_compile",
        lambda prose, context, *, conn=None: {
            "title": "Inbox Triage",
            "prose": "Use @gmail/search before triage-agent updates #ticket/status. review-agent validates the output.",
            "authority": "Support operations owns inbox triage.",
            "sla": {"response_minutes": 15},
            "capabilities": ["research/local-knowledge", "tool/gmail/search"],
        },
    )

    result = compiler.compile_prose("Handle support email")
    definition = result["definition"]

    assert definition["authority"] == "Support operations owns inbox triage."
    assert definition["sla"] == {"response_minutes": 15}
    assert definition["references"][0]["resolved_to"] == "integration_registry:gmail/search"
    assert definition["references"][1]["resolved_to"] == "task_type_routing:auto/build"
    assert definition["references"][2]["resolved_to"] == "object_types:ticket"
    assert definition["references"][3]["resolved_to"] == "task_type_routing:auto/review"
    assert [capability["slug"] for capability in definition["capabilities"]] == [
        "research/local-knowledge",
        "tool/gmail/search",
    ]
    assert any(node["kind"] == "reference" for node in definition["definition_graph"]["nodes"])
    assert any(edge["kind"] == "derived_from_block" for edge in definition["definition_graph"]["edges"])
    assert definition["narrative_blocks"][0]["reference_slugs"] == [
        "@gmail/search",
        "triage-agent",
        "#ticket/status",
    ]
    assert [step["title"] for step in definition["draft_flow"]] == [
        "Use @gmail/search before triage-agent updates #ticket/status",
        "review-agent validates the output",
    ]
    assert definition["draft_flow"][1]["depends_on"] == ["step-001"]
    assert definition["trigger_intent"] == []
    assert definition["definition_revision"].startswith("def_")
    assert definition["execution_setup"]["runtime_profile_ref"] == "compile/research.grounded"
    assert definition["surface_manifest"]["headline"].startswith("Built a Grounded Research setup")
    assert definition["build_receipt"]["tradeoffs"]
    assert definition["build_receipt"]["data_audit"]["resolved_reference_count"] == 4
    assert any(
        "Prompt and persona authority refs are not attached yet"
        in gap
        for gap in definition["build_receipt"]["data_gaps"]
    )
    assert result["error"] is None
    assert result["semantic_retrieval"]["mode"] == "semantic"
    assert result["refinement"]["status"] == "refined"
    assert result["refinement"]["applied"] is True


def test_compile_prose_sanitizes_duplicate_low_value_words_from_llm_output(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_COMPILER_ENABLE_LLM", "1")
    monkeypatch.setattr(compiler, "_get_connection", lambda: _FakeConn())
    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda conn, **kwargs: _compile_index_snapshot(
            catalog=[],
            integrations=[],
            object_types=[],
            capabilities=compiler._build_capability_catalog([]),
            route_hints=(),
        ),
    )
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)

    prose = (
        "I want to be able to 1) capture the application UI, "
        "2) research the API docs with Brave, "
        "3) record the docs and plan the connector, "
        "4) build a basic connector to the common objects."
    )
    monkeypatch.setattr(
        compiler,
        "_call_llm_compile",
        lambda prose, context, *, conn=None: {
            "title": "Connector flow",
            "prose": (
                "I want to be able to 1) capture the application UI, "
                "2) research the API docs with Brave, "
                "3) record the docs and and plan the connector, "
                "4) build a basic connector to the common objects."
            ),
            "authority": "",
            "sla": {},
        },
    )

    result = compiler.compile_prose(prose, conn=_FakeConn())
    definition = result["definition"]

    assert "and and" not in definition["compiled_prose"]
    assert definition["compiled_prose"] == prose
    assert result["refinement"]["applied"] is False
    assert result["refinement"]["status"] == "unchanged"


def test_compile_prose_rejects_llm_output_that_drops_critical_source_tokens(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_COMPILER_ENABLE_LLM", "1")
    monkeypatch.setattr(compiler, "_get_connection", lambda: _FakeConn())
    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda conn, **kwargs: _compile_index_snapshot(
            catalog=[],
            integrations=[],
            object_types=[],
            capabilities=compiler._build_capability_catalog([]),
            route_hints=(),
        ),
    )
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)

    prose = (
        "I want to be able to 1) capture the application UI, "
        "2) research the API docs with Brave, "
        "3) record the docs and plan the connector, "
        "4) build a basic connector to the common objects."
    )
    monkeypatch.setattr(
        compiler,
        "_call_llm_compile",
        lambda prose, context, *, conn=None: {
            "title": "Connector flow",
            "prose": (
                "I want to be able to 1) capture the application of an application, "
                "2) research the API docs with Brave, "
                "3) record the docs and plan the connector, "
                "4) build a basic connector to the common objects."
            ),
            "authority": "",
            "sla": {},
        },
    )

    result = compiler.compile_prose(prose, conn=_FakeConn())
    definition = result["definition"]

    assert definition["compiled_prose"] == prose
    assert "UI" in definition["compiled_prose"]
    assert result["refinement"]["applied"] is False
    assert result["refinement"]["status"] == "fallback"
    assert result["refinement"]["reason"] == "unsafe_source_token_loss:ui"
    assert "llm_compile_guarded: unsafe_source_token_loss:ui" in (result["error"] or "")


def test_compile_prose_uses_populated_reference_catalog_when_sync_is_noop(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_COMPILER_ENABLE_LLM", "1")
    catalog = [
        {
            "slug": "@gmail/search",
            "ref_type": "integration",
            "display_name": "Gmail Search",
            "resolved_id": "gmail",
            "resolved_table": "integration_registry",
            "description": "Search connected Gmail accounts",
        },
        {
            "slug": "#ticket/status",
            "ref_type": "object",
            "display_name": "Ticket Status",
            "resolved_id": "ticket",
            "resolved_table": "object_types",
            "description": "Ticket state field",
        },
    ]

    snapshot = _compile_index_snapshot(
        catalog=catalog,
        integrations=[],
        object_types=[],
        capabilities=compiler._build_capability_catalog([]),
        route_hints=(("build", "auto/build"),),
    )
    monkeypatch.setattr(compiler, "_get_connection", lambda: _FakeConn())
    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda conn, **kwargs: snapshot,
    )
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)
    monkeypatch.setattr(
        compiler,
        "_call_llm_compile",
        lambda prose, context, *, conn=None: {
            "title": "Inbox Triage",
            "prose": "Use @gmail/search before triage-agent updates #ticket/status.",
            "authority": "",
            "sla": {},
        },
    )

    result = compiler.compile_prose("Handle support email")
    definition = result["definition"]

    assert definition["references"][0]["resolved_to"] == "integration_registry:gmail/search"
    assert definition["references"][1]["resolved_to"] == "task_type_routing:auto/build"
    assert definition["references"][2]["resolved_to"] == "object_types:ticket"
    assert definition["execution_setup"]["method"]["key"] == "grounded_research"
    assert definition["build_receipt"]["data_audit"]["reference_count"] == 3
    assert result["compile_index"]["compile_index_ref"] == snapshot.compile_index_ref
    assert result["error"] is None


def test_compile_prose_emits_research_toolchains_from_runtime_catalog(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_COMPILER_ENABLE_LLM", "1")
    integrations = [
        {
            "id": "recruiter",
            "name": "Recruiter Pipeline",
            "provider": "recruiter",
            "auth_status": "connected",
            "description": "Lead intelligence",
            "capabilities": [
                {"action": "company_intel", "description": "Research company details"},
            ],
        }
    ]

    monkeypatch.setattr(compiler, "_get_connection", lambda: _FakeConn())
    monkeypatch.setattr(compiler, "_load_reference_catalog", lambda conn: [])
    monkeypatch.setattr(compiler, "_load_integrations", lambda conn: integrations)
    monkeypatch.setattr(compiler, "_load_object_types", lambda conn: [])
    monkeypatch.setattr(compiler, "load_capability_catalog", lambda conn: compiler._build_capability_catalog(integrations))
    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda conn, **kwargs: _compile_index_snapshot(
            catalog=[],
            integrations=integrations,
            object_types=[],
            capabilities=compiler._build_capability_catalog(integrations),
            route_hints=(),
        ),
    )
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)
    monkeypatch.setattr(
        compiler,
        "_call_llm_compile",
        lambda prose, context, *, conn=None: {
            "title": "Market Research",
            "prose": "Research competing companies, compare findings across sources, and use @recruiter/company_intel before research-agent writes the brief.",
            "authority": "",
            "sla": {},
        },
    )

    result = compiler.compile_prose("Research competitors")
    definition = result["definition"]

    slugs = [capability["slug"] for capability in definition["capabilities"]]
    assert "research/local-knowledge" in slugs
    assert "research/fan-out" in slugs
    assert "tool/recruiter/company_intel" in slugs
    assert definition["execution_setup"]["method"]["key"] == "seed_fanout_synthesize"
    # architecture-policy::compile::retrieval-is-the-filter-no-template-
    # fallbacks (2026-04-25). briefing_fields + blocking_inputs are empty
    # because the keyword-gated template was deleted. Retrieval is the
    # filter; no canned research-inputs list.
    assert definition["execution_setup"]["constraints"]["briefing_fields"] == []
    assert definition["execution_setup"]["constraints"]["blocking_inputs"] == []
    assert definition["execution_setup"]["budget_policy"]["fanout_workers"] == 4
    assert definition["surface_manifest"]["surface_now"]["approaches"][0]["label"] == "Seed research plan"
    assert any(
        decision["choice"] == "seed_fanout_synthesize"
        for decision in definition["build_receipt"]["decisions"]
    )
    assert "jobs" not in result
    assert "execution_plan" not in result
    assert result["semantic_retrieval"]["mode"] == "semantic"
    assert result["refinement"]["status"] == "refined"


def test_compile_prose_reports_degraded_semantic_mode_when_embedder_disabled(monkeypatch) -> None:
    monkeypatch.setattr(compiler, "_get_connection", lambda: _FakeConn())
    monkeypatch.setattr(compiler, "_load_reference_catalog", lambda conn: [])
    monkeypatch.setattr(compiler, "_load_integrations", lambda conn: [])
    monkeypatch.setattr(compiler, "_load_object_types", lambda conn: [])
    monkeypatch.setattr(compiler, "load_capability_catalog", lambda conn: compiler._build_capability_catalog([]))
    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda conn, **kwargs: _compile_index_snapshot(
            catalog=[],
            integrations=[],
            object_types=[],
            capabilities=compiler._build_capability_catalog([]),
            route_hints=(),
        ),
    )
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)
    monkeypatch.setenv("WORKFLOW_COMPILER_DISABLE_EMBEDDINGS", "1")
    monkeypatch.setattr(
        compiler,
        "_call_llm_compile",
        lambda prose, context, *, conn=None: {
            "title": "Disabled embeddings",
            "prose": prose,
            "authority": "",
            "sla": {},
        },
    )

    result = compiler.compile_prose("Handle degraded compile mode")
    definition = result["definition"]

    assert result["semantic_retrieval"]["mode"] == "degraded"
    assert result["semantic_retrieval"]["reason"] == "disabled_by_env"
    assert result["refinement"]["status"] == "deterministic"
    assert definition["execution_setup"]["method"]["key"] == "single_agent"


def test_compile_prose_promotes_workflow_cues_to_staged_execution(monkeypatch) -> None:
    monkeypatch.setenv("WORKFLOW_COMPILER_ENABLE_LLM", "1")
    monkeypatch.setattr(compiler, "_get_connection", lambda: _FakeConn())
    monkeypatch.setattr(compiler, "_load_reference_catalog", lambda conn: [])
    monkeypatch.setattr(compiler, "_load_integrations", lambda conn: [])
    monkeypatch.setattr(compiler, "_load_object_types", lambda conn: [])
    monkeypatch.setattr(compiler, "load_capability_catalog", lambda conn: compiler._build_capability_catalog([]))
    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda conn, **kwargs: _compile_index_snapshot(
            catalog=[],
            integrations=[],
            object_types=[],
            capabilities=compiler._build_capability_catalog([]),
            route_hints=(),
        ),
    )
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)
    monkeypatch.setattr(
        compiler,
        "_call_llm_compile",
        lambda prose, context, *, conn=None: {
            "title": "Bug Intake Flow",
            "prose": "Build a workflow that ingests bug reports, routes by severity, and requires review before closure.",
            "authority": "",
            "sla": {},
        },
    )

    result = compiler.compile_prose("Build a workflow")
    definition = result["definition"]

    assert definition["execution_setup"]["method"]["key"] == "staged_execution"
    assert definition["execution_setup"]["task_class"] == "workflow"
    assert len(definition["execution_setup"]["phases"]) == 2
    assert any(
        command["id"] == "attach_trigger"
        for command in definition["surface_manifest"]["surface_now"]["commands"]
    )
    assert any(
        "No trigger intent was captured"
        in gap
        for gap in definition["build_receipt"]["data_gaps"]
    )


def test_compile_prose_connector_keyword_prose_does_not_emit_template_fallback(monkeypatch) -> None:
    """architecture-policy::compile::retrieval-is-the-filter-no-template-
    fallbacks (2026-04-25). BUG-3330D2CD retired the keyword-gated constant
    functions infer_blocking_inputs / infer_briefing_fields / connector_
    flow_self_scaffolds_inputs that used to emit the same hardcoded 5-item
    list (Target application / Official API docs / Authentication /
    Persistence / Common object) whenever prose contained any of
    (connector, api docs, common objects, application, docs). That template
    preempted definition_graph's prose-grounded capability nodes in the
    Moon build_graph render.

    This test pins the new contract: connector-keyword prose produces empty
    blocking_inputs AND empty briefing_fields, no 'fill_blocking_inputs' or
    'fill_briefing_fields' surface commands, no typed_gap.created events
    carrying the template input_label strings, and no
    'blocking_inputs_required' build_receipt decision. Retrieval is the
    filter; if retrieval finds nothing a retrieval.no_match typed_gap fires
    (follow-up commit). We no longer fabricate inputs from keyword
    presence.

    Covers all three keyword-hit paths the four retired tests used to pin:
      (a) explicit connector / api docs / common objects flow
      (b) intake flow with self-scaffolding signals
      (c) synonym coverage (SaaS / developer portal / reference docs / bridge)
    """

    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda conn, **kwargs: _compile_index_snapshot(
            catalog=[],
            integrations=[],
            object_types=[],
            capabilities=compiler._build_capability_catalog([]),
            route_hints=(),
        ),
    )
    monkeypatch.setattr("runtime.intent_matcher.IntentMatcher", _StubMatcher)

    template_items = {
        "Target application or applications in scope",
        "Official API docs entrypoint or outbound internet research target",
        "Authentication setup and credential shape",
        "Persistence contract for captured docs and connector state",
        "Common object scope and target field mappings",
    }

    prose_cases = {
        "a_connector": (
            "I want to be able to 1) capture the application UI, "
            "2) research the API docs with Brave, "
            "3) record the docs and plan the connector, "
            "4) build a basic connector to the common objects."
        ),
        "b_intake": (
            "Im going to feed you an application name, "
            "We will need to search the web via fan out research and Brave to find the docs for that application's API, "
            "bring them back and store them in the db, "
            "then we need to make a plan for a first pass skinny integration for this connector to Praxis, "
            "then that needs to get created and tested until it works."
        ),
        "c_synonyms": (
            "Once I give you a SaaS name, inspect that product's developer portal or reference docs, "
            "record the findings in the database, sketch a lean Praxis bridge, implement the adapter, "
            "and verify it with QA coverage until it works."
        ),
    }

    for label, prose in prose_cases.items():
        conn = _RecordingCompileConn()
        monkeypatch.setattr(compiler, "_get_connection", lambda conn=conn: conn)
        result = compiler.compile_prose(prose, conn=conn)
        definition = result["definition"]
        setup = definition["execution_setup"]
        assert setup["constraints"]["blocking_inputs"] == [], (
            f"{label}: blocking_inputs leaked template content"
        )
        assert setup["constraints"]["briefing_fields"] == [], (
            f"{label}: briefing_fields leaked template content"
        )
        command_ids = {
            command["id"]
            for command in definition.get("surface_manifest", {}).get("surface_now", {}).get("commands", [])
            if isinstance(command, dict) and command.get("id")
        }
        assert "fill_blocking_inputs" not in command_ids, (
            f"{label}: fill_blocking_inputs command leaked"
        )
        assert "fill_briefing_fields" not in command_ids, (
            f"{label}: fill_briefing_fields command leaked"
        )
        decision_choices = {
            decision.get("choice")
            for decision in definition.get("build_receipt", {}).get("decisions", [])
            if isinstance(decision, dict)
        }
        assert "blocking_inputs_required" not in decision_choices, (
            f"{label}: blocking_inputs_required decision leaked"
        )
        typed_gap_labels = {
            json.loads(args[3]).get("context", {}).get("input_label")
            for sql, args in conn.events
            if "INSERT INTO system_events" in sql and args[0] == "typed_gap.created"
        }
        assert template_items.isdisjoint(typed_gap_labels), (
            f"{label}: typed_gap event carried template input_label"
        )
