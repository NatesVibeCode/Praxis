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


def test_call_llm_compile_uses_medium_refine_route_by_default(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _fake_submit(conn, spec):
        captured["conn"] = conn
        captured["spec"] = spec
        return {"run_id": "run.compile.medium"}

    monkeypatch.delenv("WORKFLOW_REFINE_AGENT_ROUTE", raising=False)
    monkeypatch.setitem(
        sys.modules,
        "runtime.workflow.unified",
        SimpleNamespace(submit_workflow_inline=_fake_submit),
    )

    result = compiler._call_llm_compile(
        "Route support mail",
        "Context: support queue",
        conn=_WorkflowCompileConn(),
    )

    spec = captured["spec"]
    assert isinstance(spec, dict)
    assert spec["jobs"][0]["agent"] == "auto/medium"
    assert result["title"] == "Support Mail"
    assert result["prose"] == "Use @gmail/search before triage-agent reviews the queue."


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


def test_compile_prose_skips_malformed_reusable_definition_artifact(monkeypatch) -> None:
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

    result = compiler.compile_prose(
        "Triage support email",
        title="Support Mail",
        compile_index_snapshot=snapshot,
        conn=_FakeConn(),
    )

    assert result["reuse_provenance"]["decision"] == "compiled"
    assert result["error"] is None


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

    result = compiler.compile_prose("Build a workflow from this prose", conn=_FakeConn())

    assert result["compiled_spec"] is None
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
    assert definition["execution_setup"]["constraints"]["briefing_fields"] == [
        "Research topic, company, or question to investigate",
        "Comparison set or entities in scope",
        "Scope boundaries and decision the research should support",
        "Freshness window or time horizon",
        "Required primary sources or source restrictions",
        "Output format or deliverable expectation",
    ]
    assert definition["execution_setup"]["constraints"]["blocking_inputs"] == []
    assert definition["execution_setup"]["budget_policy"]["fanout_workers"] == 4
    assert definition["surface_manifest"]["surface_now"]["approaches"][0]["label"] == "Seed research plan"
    assert any(
        command["id"] == "fill_briefing_fields"
        for command in definition["surface_manifest"]["surface_now"]["commands"]
    )
    assert any(
        decision["choice"] == "seed_fanout_synthesize"
        for decision in definition["build_receipt"]["decisions"]
    )
    assert any(
        decision["choice"] == "suggested_inputs_emitted"
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


def test_compile_prose_connector_flow_surfaces_external_research_and_blocking_inputs(monkeypatch) -> None:
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

    result = compiler.compile_prose(prose, conn=_FakeConn())
    definition = result["definition"]
    setup = definition["execution_setup"]
    phases = setup["phases"]

    assert setup["method"]["key"] == "staged_execution"
    assert setup["method"]["label"] == "Staged Execution"
    assert "research/gemini-cli" in [capability["slug"] for capability in definition["capabilities"]]
    assert "research/local-knowledge" not in [capability["slug"] for capability in definition["capabilities"]]
    assert setup["constraints"]["blocking_inputs"] == [
        "Target application or applications in scope",
        "Official API docs entrypoint or outbound internet research target",
        "Authentication setup and credential shape",
        "Persistence contract for captured docs and connector state",
        "Common object scope and target field mappings",
    ]
    assert setup["constraints"]["briefing_fields"] == setup["constraints"]["blocking_inputs"]
    assert [phase["title"] for phase in phases] == [
        "capture the application UI",
        "research the API docs with Brave",
        "record the docs and plan the connector",
        "build a basic connector to the common objects",
    ]
    assert [phase["role_label"] for phase in phases] == [
        "Intake",
        "Research",
        "Plan",
        "Build",
    ]
    assert phases[1]["required_inputs"] == [
        "Target application or applications in scope",
        "Official API docs entrypoint or outbound internet research target",
        "Authentication setup and credential shape",
    ]
    assert phases[2]["persistence_targets"] == [
        "source-backed docs notes",
        "connector configuration and object mappings",
    ]
    assert phases[2]["handoff_target"] == "build a basic connector to the common objects"
    assert "blocking_inputs_required" in [decision["choice"] for decision in definition["build_receipt"]["decisions"]]
    assert any(
        gap.startswith("Blocking briefing inputs are still missing:")
        for gap in definition["build_receipt"]["data_gaps"]
    )
    assert any(
        command["id"] == "fill_blocking_inputs"
        for command in definition["surface_manifest"]["surface_now"]["commands"]
    )


def test_compile_prose_connector_intake_flow_uses_suggested_inputs_without_blocking(monkeypatch) -> None:
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
        "Im going to feed you an application name, "
        "We will need to search the web via fan out research and Brave to find the docs for that application's API, "
        "bring them back and store them in the db, "
        "then we need to make a plan for a first pass skinny integration for this connector to Praxis, "
        "then that needs to get created and tested until it works."
    )

    result = compiler.compile_prose(prose, conn=_FakeConn())
    definition = result["definition"]
    setup = definition["execution_setup"]
    phases = setup["phases"]

    assert setup["method"]["key"] == "staged_execution"
    assert setup["constraints"]["briefing_fields"] == [
        "Target application or applications in scope",
        "Official API docs entrypoint or outbound internet research target",
        "Authentication setup and credential shape",
        "Persistence contract for captured docs and connector state",
        "Common object scope and target field mappings",
    ]
    assert setup["constraints"]["blocking_inputs"] == []
    assert [phase["title"] for phase in phases] == [
        "Im going to feed you an application name",
        "We will need to search the web via fan out research and Brave to find the docs for that application's API, bring them back and store them in the db",
        "then we need to make a plan for a first pass skinny integration for this connector to Praxis",
        "then that needs to get created and tested until it works",
    ]
    assert any(
        command["id"] == "fill_briefing_fields"
        for command in definition["surface_manifest"]["surface_now"]["commands"]
    )
    assert not any(
        command["id"] == "fill_blocking_inputs"
        for command in definition["surface_manifest"]["surface_now"]["commands"]
    )
    assert result["build_state"] == "ready"
    assert result["build_blockers"] == []


def test_compile_prose_connector_synonyms_stay_buildable_without_exact_magic_words(monkeypatch) -> None:
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
        "Once I give you a SaaS name, inspect that product's developer portal or reference docs, "
        "record the findings in the database, sketch a lean Praxis bridge, implement the adapter, "
        "and verify it with QA coverage until it works."
    )

    result = compiler.compile_prose(prose, conn=_FakeConn())
    definition = result["definition"]
    setup = definition["execution_setup"]

    assert setup["method"]["key"] == "staged_execution"
    assert setup["constraints"]["blocking_inputs"] == []
    assert setup["constraints"]["briefing_fields"] == [
        "Target application or applications in scope",
        "Official API docs entrypoint or outbound internet research target",
        "Authentication setup and credential shape",
        "Persistence contract for captured docs and connector state",
        "Common object scope and target field mappings",
    ]
    assert result["build_state"] == "ready"
    assert result["build_blockers"] == []
