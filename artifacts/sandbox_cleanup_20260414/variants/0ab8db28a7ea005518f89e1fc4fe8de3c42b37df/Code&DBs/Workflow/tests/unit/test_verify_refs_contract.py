from __future__ import annotations

import hashlib
import json
from types import SimpleNamespace

import pytest

import runtime.compile_index as compile_index
import runtime.compiler as compiler
import runtime.operating_model_planner as planner
from runtime.capability_catalog import CapabilityCatalogError
from runtime.compile_artifacts import CompileArtifactRecord
from runtime.verification import VerificationAuthorityError, resolve_verify_commands
from runtime.operating_model_planner import (
    current_compiled_spec,
    plan_definition,
)
from runtime.workflow_graph_compiler import compile_graph_workflow_request, spec_uses_graph_runtime
from runtime.workflow_spec import validate_workflow_spec


class _VerifyRefsConn:
    def __init__(self, *, capability_rows: list[dict[str, object]] | None = None) -> None:
        self.rows: list[tuple[object, ...]] = []
        self.capability_upserts: list[tuple[object, ...]] = []
        self.verify_upserts: list[tuple[object, ...]] = []
        self.capability_rows = capability_rows if capability_rows is not None else [
            {
                "capability_ref": "cap-task-debug",
                "capability_slug": "debug",
                "capability_kind": "task",
                "title": "Debugging",
                "summary": "Diagnose failures and trace problems to root cause.",
                "description": "Use when the work is investigating errors or fixing broken runtime behavior.",
                "route": "task/debug",
                "engines": ["minimal_intent_compile"],
                "signals": ["debug", "diagnose", "trace", "fix", "failure", "bug", "error"],
                "reference_slugs": [],
                "enabled": True,
                "binding_revision": "binding.capability_catalog.task.debug.20260409",
                "decision_ref": "decision.capability_catalog.task.bootstrap.20260409",
            },
            {
                "capability_ref": "cap-task-mechanical-edit",
                "capability_slug": "mechanical_edit",
                "capability_kind": "task",
                "title": "Mechanical edit",
                "summary": "Perform bounded code edits and structural refactors.",
                "description": "Use when the work is file-local edits or mechanical refactors.",
                "route": "task/mechanical_edit",
                "engines": ["minimal_intent_compile"],
                "signals": ["fix", "edit", "rename", "format", "refactor", "cleanup", "patch"],
                "reference_slugs": [],
                "enabled": True,
                "binding_revision": "binding.capability_catalog.task.mechanical_edit.20260409",
                "decision_ref": "decision.capability_catalog.task.bootstrap.20260409",
            },
            {
                "capability_ref": "cap-task-code-generation",
                "capability_slug": "code_generation",
                "capability_kind": "task",
                "title": "Code generation",
                "summary": "Implement or extend code from requirements.",
                "description": "Use when the work is mainly building code or tests.",
                "route": "task/code_generation",
                "engines": ["minimal_intent_compile"],
                "signals": ["build", "implement", "create", "generate", "write", "test", "code"],
                "reference_slugs": [],
                "enabled": True,
                "binding_revision": "binding.capability_catalog.task.code_generation.20260409",
                "decision_ref": "decision.capability_catalog.task.bootstrap.20260409",
            },
        ]

    def execute(self, query: str, *args):
        if "FROM information_schema.columns" in query and "capability_catalog" in query:
            return [{"column_name": column_name} for column_name in (
                "capability_ref",
                "capability_slug",
                "capability_kind",
                "title",
                "summary",
                "description",
                "route",
                "engines",
                "signals",
                "reference_slugs",
                "enabled",
                "binding_revision",
                "decision_ref",
            )]
        if "FROM capability_catalog" in query:
            return list(self.capability_rows)
        if "FROM verify_refs" in query:
            if args and args[0] == "verify_ref.python.py_compile.test":
                return [
                    {
                        "verify_ref": "verify_ref.python.py_compile.test",
                        "verification_ref": "verification.python.py_compile",
                        "label": "Compile sample.py",
                        "description": "Compile Python file",
                        "inputs": {"path": "sample.py"},
                        "enabled": True,
                        "binding_revision": "binding.sample",
                        "decision_ref": "decision.sample",
                    }
                ]
            return []
        if "FROM verification_registry" in query:
            return [
                {
                    "verification_ref": "verification.python.py_compile",
                    "display_name": "Python Bytecode Compile",
                    "executor_kind": "argv",
                    "argv_template": ["python3", "-m", "py_compile", "{path}"],
                    "template_inputs": ["path"],
                    "default_timeout_seconds": 60,
                    "enabled": True,
                }
            ]
        return []

    def execute_many(self, query: str, rows: list[tuple[object, ...]]) -> None:
        self.rows.extend(rows)
        if "INSERT INTO capability_catalog" in query:
            self.capability_upserts.extend(rows)
            return
        if "INSERT INTO verify_refs" in query:
            self.verify_upserts.extend(rows)
            return


def test_workflow_spec_accepts_verify_refs_as_canonical_surface() -> None:
    payload = {
        "prompt": "Do the thing",
        "provider_slug": "anthropic",
        "adapter_type": "cli_llm",
        "verify_refs": ["verify_ref.python.py_compile.test"],
    }
    ok, errors = validate_workflow_spec(payload)
    assert ok
    assert errors == []


def test_workflow_spec_rejects_legacy_verify_bindings() -> None:
    payload = {
        "prompt": "Do the thing",
        "provider_slug": "anthropic",
        "adapter_type": "cli_llm",
        "verify": [
            {
                "verification_ref": "verification.python.py_compile",
                "inputs": {"path": "sample.py"},
            }
        ],
    }
    ok, errors = validate_workflow_spec(payload)
    assert not ok
    assert "unknown field: verify" in errors
    assert payload["verify"][0]["verification_ref"] == "verification.python.py_compile"


def test_resolve_verify_commands_fails_closed_on_missing_verify_ref_row() -> None:
    conn = _VerifyRefsConn()
    with pytest.raises(VerificationAuthorityError):
        resolve_verify_commands(conn, ["verify_ref.missing"])


def test_resolve_verify_commands_reads_verify_ref_rows() -> None:
    conn = _VerifyRefsConn()
    commands = resolve_verify_commands(conn, ["verify_ref.python.py_compile.test"])

    assert len(commands) == 1
    assert commands[0].verification_ref == "verification.python.py_compile"
    assert commands[0].argv == ("python3", "-m", "py_compile", "sample.py")
    assert commands[0].label == "Compile sample.py"


def test_resolve_verify_commands_rejects_legacy_binding_objects() -> None:
    conn = _VerifyRefsConn()
    with pytest.raises(VerificationAuthorityError, match="verify_refs\\[0\\]"):
        resolve_verify_commands(
            conn,
            [
                {
                    "verification_ref": "verification.python.py_compile",
                    "inputs": {"path": "sample.py"},
                }
            ],
        )


def test_compile_spec_emits_verify_refs_and_persists_authority_rows() -> None:
    from runtime.spec_compiler import compile_spec

    conn = _VerifyRefsConn()
    spec, warnings = compile_spec(
        {
            "description": "Fix Python file",
            "write": ["app.py", "app_test.py"],
            "stage": "fix",
        },
        conn=conn,
    )

    assert warnings == []
    assert spec.verify_refs is not None
    assert len(spec.verify_refs) == 2
    assert all(ref.startswith("verify_ref.") for ref in spec.verify_refs)
    assert spec.definition_graph is not None
    assert spec.definition_revision is not None
    assert spec.compiled_prose == spec.prompt
    assert isinstance(spec.narrative_blocks, list)
    assert isinstance(spec.draft_flow, list)
    assert len(conn.verify_upserts) == 2
    assert len(conn.capability_upserts) >= 3
    assert "definition_graph" not in spec.to_dispatch_spec_dict()
    assert spec.to_dispatch_spec_dict()["definition_revision"] == spec.definition_revision
    assert "verify" not in spec.to_dispatch_spec_dict()
    assert spec.capabilities == ["debug", "mechanical_edit"]


def test_compile_spec_fails_closed_when_capability_catalog_is_missing() -> None:
    from runtime.spec_compiler import compile_spec

    conn = _VerifyRefsConn(capability_rows=[])
    with pytest.raises(CapabilityCatalogError):
        compile_spec(
            {
                "description": "Fix Python file",
                "write": ["app.py"],
                "stage": "fix",
            },
            conn=conn,
        )


def test_plan_definition_emits_plan_revision() -> None:
    result = plan_definition(
        {
            "source_prose": "Build a thing",
            "compiled_prose": "Build a thing",
            "definition_revision": "def_1234abcd",
            "references": [],
            "narrative_blocks": [],
            "draft_flow": [],
            "trigger_intent": [],
        }
    )

    assert result["compiled_spec"]["definition_revision"] == "def_1234abcd"
    assert result["compiled_spec"]["plan_revision"].startswith("plan_")


def test_plan_definition_uses_draft_flow_even_when_legacy_jobs_are_present(monkeypatch: pytest.MonkeyPatch) -> None:
    draft_jobs = [
        {
            "label": "draft-step",
            "agent": "integration/gmail",
            "prompt": "Execute the draft flow",
        }
    ]
    monkeypatch.setattr(planner, "_plan_jobs", lambda *_args, **_kwargs: draft_jobs)

    result = plan_definition(
        {
            "source_prose": "Build a thing",
            "compiled_prose": "Build a thing",
            "definition_revision": "def_legacy_jobs",
            "references": [],
            "narrative_blocks": [],
            "draft_flow": [{"id": "step-1", "title": "Draft step", "summary": "Draft step"}],
            "jobs": [
                {
                    "label": "explicit-step",
                    "agent": "integration/gmail",
                    "prompt": "Do the explicit thing",
                }
            ],
            "trigger_intent": [],
        }
    )

    assert result["compiled_spec"]["definition_revision"] == "def_legacy_jobs"
    assert result["compiled_spec"]["plan_revision"].startswith("plan_")
    assert result["compiled_spec"]["jobs"] == draft_jobs
    assert result["planning_notes"][0] == "Planned 1 jobs from draft_flow."


def test_plan_definition_reuses_exact_plan_artifact_without_replanning(monkeypatch: pytest.MonkeyPatch) -> None:
    definition = {
        "source_prose": "Build a thing",
        "compiled_prose": "Build a thing",
        "definition_revision": "def_reuse_1234",
        "compile_provenance": {
            "artifact_kind": "definition",
            "input_fingerprint": "definition.input.1234",
        },
        "references": [],
        "narrative_blocks": [],
        "draft_flow": [],
        "trigger_intent": [],
    }
    first_result = plan_definition(definition)
    compiled_spec = first_result["compiled_spec"]
    payload_json = json.dumps(compiled_spec, sort_keys=True, separators=(",", ":"), default=str)
    reusable = CompileArtifactRecord(
        compile_artifact_id="compile_artifact.plan.reused1234567890",
        artifact_kind="plan",
        artifact_ref=compiled_spec["plan_revision"],
        revision_ref=compiled_spec["plan_revision"],
        parent_artifact_ref=definition["definition_revision"],
        input_fingerprint=compiled_spec["compile_provenance"]["input_fingerprint"],
        content_hash=hashlib.sha256(payload_json.encode("utf-8")).hexdigest(),
        authority_refs=(definition["definition_revision"],),
        payload=compiled_spec,
        decision_ref="decision.compile.plan.reused1234567890",
    )

    monkeypatch.setattr(
        planner.CompileArtifactStore,
        "load_reusable_artifact",
        lambda self, *, artifact_kind, input_fingerprint: reusable,
    )
    monkeypatch.setattr(
        planner,
        "_plan_jobs",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("planner should not run on reuse hit")),
    )

    result = plan_definition(definition, conn=SimpleNamespace())

    assert result["compiled_spec"]["plan_revision"] == compiled_spec["plan_revision"]
    assert result["planning_notes"] == ["Reused plan from exact authority and context match."]
    assert result["reuse_provenance"]["decision"] == "reused"


def test_current_compiled_spec_rejects_exact_fingerprint_mismatch() -> None:
    definition = {
        "source_prose": "Build a thing",
        "compiled_prose": "Build a thing",
        "definition_revision": "def_current_1234",
        "compile_provenance": {
            "artifact_kind": "definition",
            "input_fingerprint": "definition.input.current",
        },
        "references": [],
        "narrative_blocks": [],
        "draft_flow": [],
        "trigger_intent": [],
    }
    compiled_spec = plan_definition(definition)["compiled_spec"]
    stale_definition = dict(definition)
    stale_definition["compiled_prose"] = "Build a different thing"
    stale_definition["definition_revision"] = definition["definition_revision"]

    assert current_compiled_spec(stale_definition, compiled_spec) is None


def test_current_compiled_spec_rejects_stale_plan_even_when_definition_contains_legacy_jobs() -> None:
    definition = {
        "type": "operating_model",
        "source_prose": "Do the explicit thing",
        "compiled_prose": "Do the explicit thing",
        "definition_revision": "def_explicit_state",
        "jobs": [
            {
                "label": "explicit-step",
                "agent": "integration/gmail",
                "prompt": "Do the explicit thing",
            }
        ],
        "trigger_intent": [{"event_type": "email.received"}],
    }
    stale_compiled_spec = {
        "definition_revision": "def_explicit_state",
        "plan_revision": "plan_old_state",
        "name": "Old title",
        "workflow_id": "workflow.old",
        "phase": "build",
        "outcome_goal": "stale",
        "jobs": [{"label": "old-step", "prompt": "stale"}],
        "triggers": [],
        "compile_provenance": {
            "surface_revision": "planner.surface.test",
            "input_fingerprint": "planner.input.stale",
        },
    }

    assert current_compiled_spec(definition, stale_compiled_spec) is None


def test_plan_definition_materializes_legacy_projections_from_definition_graph() -> None:
    from runtime.definition_compile_kernel import build_definition

    definition = build_definition(
        source_prose="When @gmail/search receives a message, review-agent validates it nightly.",
        compiled_prose="When @gmail/search receives a message, review-agent validates it nightly.",
        references=[
            {
                "id": "ref-001",
                "type": "integration",
                "slug": "@gmail/search",
                "span": [5, 18],
                "raw": "@gmail/search",
                "config": {},
                "resolved": True,
                "resolved_to": "integration_registry:gmail/search",
                "display_name": "Gmail Search",
                "description": "Search Gmail",
            },
            {
                "id": "ref-002",
                "type": "agent",
                "slug": "review-agent",
                "span": [39, 51],
                "raw": "review-agent",
                "config": {"route": "auto/review"},
                "resolved": True,
                "resolved_to": "task_type_routing:auto/review",
                "display_name": "Review Agent",
                "description": "Validate output",
            },
        ],
        capabilities=[],
        authority="",
        sla={},
    )
    graph_only_definition = {
        "definition_graph": definition["definition_graph"],
        "definition_revision": definition["definition_revision"],
        "references": definition["references"],
    }

    result = plan_definition(graph_only_definition)

    assert result["compiled_spec"]["definition_revision"] == definition["definition_revision"]
    assert result["compiled_spec"]["jobs"][0]["agent"] == "auto/review"
    assert result["compiled_spec"]["triggers"][0]["event_type"] == definition["trigger_intent"][0]["event_type"]


def test_plan_definition_honors_explicit_phase_route_from_builder() -> None:
    result = plan_definition(
        {
            "source_prose": "Draft a summary and then invoke the downstream workflow.",
            "compiled_prose": "Draft a summary and then invoke the downstream workflow.",
            "definition_revision": "def_explicit_phase_route",
            "references": [],
            "narrative_blocks": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Draft summary",
                    "summary": "Produce the summary.",
                    "depends_on": [],
                    "order": 1,
                },
                {
                    "id": "step-002",
                    "title": "Invoke workflow",
                    "summary": "Call the downstream workflow.",
                    "depends_on": ["step-001"],
                    "order": 2,
                },
            ],
            "execution_setup": {
                "phases": [
                    {
                        "step_id": "step-001",
                        "agent_route": "auto/draft",
                    },
                    {
                        "step_id": "step-002",
                        "agent_route": "@workflow/invoke",
                    },
                ]
            },
            "trigger_intent": [
                {
                    "id": "trigger-001",
                    "event_type": "manual",
                }
            ],
        }
    )

    jobs = result["compiled_spec"]["jobs"]
    assert jobs[0]["agent"] == "auto/draft"
    assert jobs[1]["agent"] == "@workflow/invoke"


def test_plan_definition_emits_after_failure_dependency_edges_from_moon_edge_gates() -> None:
    result = plan_definition(
        {
            "source_prose": "Run fallback remediation when the primary step fails.",
            "compiled_prose": "Run fallback remediation when the primary step fails.",
            "definition_revision": "def_after_failure_gate",
            "references": [],
            "narrative_blocks": [],
            "draft_flow": [
                {
                    "id": "step-001",
                    "title": "Primary step",
                    "summary": "Run the primary step.",
                    "depends_on": [],
                    "order": 1,
                },
                {
                    "id": "step-002",
                    "title": "Fallback step",
                    "summary": "Run only when the primary step fails.",
                    "depends_on": ["step-001"],
                    "order": 2,
                },
            ],
            "execution_setup": {
                "edge_gates": [
                    {
                        "edge_id": "edge-step-001-step-002",
                        "from_node_id": "step-001",
                        "to_node_id": "step-002",
                        "family": "after_failure",
                        "label": "On Failure",
                    }
                ]
            },
            "trigger_intent": [],
        }
    )

    jobs = result["compiled_spec"]["jobs"]
    assert jobs[1]["depends_on"] == ["primary-step"]
    assert jobs[1]["dependency_edges"] == [
        {
            "label": "primary-step",
            "edge_type": "after_failure",
        }
    ]


def test_compile_graph_workflow_request_honors_after_failure_dependency_edges() -> None:
    spec = {
        "name": "Failure Path",
        "workflow_id": "workflow.failure_path",
        "phase": "build",
        "jobs": [
            {
                "label": "primary",
                "agent": "auto/build",
                "prompt": "Run the primary job.",
            },
            {
                "label": "fallback",
                "agent": "auto/build",
                "prompt": "Run only if the primary job fails.",
                "depends_on": ["primary"],
                "dependency_edges": [
                    {
                        "label": "primary",
                        "edge_type": "after_failure",
                    }
                ],
            },
        ],
    }

    assert spec_uses_graph_runtime(spec) is True

    request = compile_graph_workflow_request(spec)

    assert len(request.edges) == 1
    assert request.edges[0].from_node_id == "primary"
    assert request.edges[0].to_node_id == "fallback"
    assert request.edges[0].edge_type == "after_failure"


def test_compile_prose_fails_closed_when_compile_index_snapshot_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(compiler, "_get_connection", lambda: _VerifyRefsConn())
    monkeypatch.setattr(
        compiler,
        "load_compile_index_snapshot",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            compile_index.CompileIndexAuthorityError(
                "compile_index.snapshot_missing",
                "compile index snapshot is missing",
            )
        ),
    )
    # snapshot_missing is refreshable — the compiler tries auto-refresh,
    # which fails with surface_manifest_unavailable in test context.
    monkeypatch.setattr(
        compiler,
        "refresh_compile_index",
        lambda *args, **kwargs: (_ for _ in ()).throw(
            compile_index.CompileIndexAuthorityError(
                "compile_index.surface_manifest_unavailable",
                "compile index surface manifest could not be resolved",
            )
        ),
    )
    with pytest.raises(RuntimeError, match="compile_index.surface_manifest_unavailable"):
        compiler.compile_prose("research something")
