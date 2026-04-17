from __future__ import annotations

import json
from types import SimpleNamespace
import uuid

import pytest

from runtime.compile_artifacts import CompileArtifactStore
import runtime.workflow_graph_compiler as workflow_graph_compiler
from runtime.workflow import _admission
from runtime.workflow import unified


@pytest.fixture(autouse=True)
def _stub_graph_submission_evidence(monkeypatch: pytest.MonkeyPatch) -> None:
    class _FakeWriter:
        def __init__(self, *, database_url):
            self.database_url = database_url

        def close_blocking(self):
            return None

    monkeypatch.setenv("WORKFLOW_DATABASE_URL", "postgresql://unused")
    monkeypatch.setattr(_admission, "PostgresEvidenceWriter", _FakeWriter)
    monkeypatch.setattr(
        _admission,
        "_persist_graph_submission_evidence",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        workflow_graph_compiler,
        "default_native_authority_refs",
        lambda *_args, **_kwargs: ("praxis", "praxis"),
    )
    monkeypatch.setattr(
        _admission,
        "resolve_native_runtime_profile_config",
        lambda *_args, **_kwargs: SimpleNamespace(
            workspace_ref="praxis",
            workdir="/Users/nate/Praxis",
            model_profile_id="model.default",
            provider_policy_id="provider.default",
            sandbox_profile_ref="sandbox.default",
        ),
    )


class _PacketConn:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []
        self.compile_artifact_rows: list[dict[str, object]] = []
        self.next_job_id = 0
        self.edge_inserts: list[tuple[int, int]] = []

    def execute(self, query: str, *args):
        if "INSERT INTO compile_artifacts" in query:
            self.compile_artifact_rows.append(
                {
                    "compile_artifact_id": args[0],
                    "artifact_kind": args[1],
                    "artifact_ref": args[2],
                    "revision_ref": args[3],
                    "parent_artifact_ref": args[4],
                    "input_fingerprint": args[5],
                    "content_hash": args[6],
                    "authority_refs": json.loads(args[7]),
                    "payload": json.loads(args[8]),
                    "decision_ref": args[9],
                }
            )
            return []
        if "FROM compile_artifacts" in query:
            artifact_kind = args[0]
            input_fingerprint = args[1]
            return [
                row
                for row in self.compile_artifact_rows
                if row["artifact_kind"] == artifact_kind and row["input_fingerprint"] == input_fingerprint
            ]
        if "INSERT INTO execution_packets" in query:
            packet_row = {
                "execution_packet_id": args[0],
                "definition_revision": args[1],
                "plan_revision": args[2],
                "packet_revision": args[3],
                "parent_artifact_ref": args[4],
                "packet_version": args[5],
                "packet_hash": args[6],
                "workflow_id": args[7],
                "run_id": args[8],
                "spec_name": args[9],
                "source_kind": args[10],
                "authority_refs": json.loads(args[11]),
                "model_messages": json.loads(args[12]),
                "reference_bindings": json.loads(args[13]),
                "capability_bindings": json.loads(args[14]),
                "verify_refs": json.loads(args[15]),
                "authority_inputs": json.loads(args[16]),
                "file_inputs": json.loads(args[17]),
                "payload": json.loads(args[18]),
                "decision_ref": args[19],
            }
            self.rows.append(packet_row)
            return []
        if "INSERT INTO workflow_definitions" in query or "INSERT INTO admission_decisions" in query:
            return []
        if "INSERT INTO workflow_jobs" in query:
            self.next_job_id += 1
            return [{"id": self.next_job_id}]
        if "INSERT INTO workflow_job_edges" in query:
            self.edge_inserts.append((args[0], args[1]))
            return []
        if "SELECT parent_id, child_id" in query and "FROM workflow_job_edges" in query:
            child_ids = set(args[0] or [])
            return [
                {"parent_id": parent_id, "child_id": child_id}
                for parent_id, child_id in self.edge_inserts
                if child_id in child_ids
            ]
        if "UPDATE workflow_jobs" in query or "UPDATE workflow_runs" in query or "DELETE FROM workflow_runs" in query:
            return []
        if "FROM execution_packets" in query:
            run_id = args[0]
            return [row for row in self.rows if row["run_id"] == run_id]
        if "workflow.run" in query:
            return []
        return []


def test_execution_packet_store_round_trips_shadow_lineage_and_provenance() -> None:
    conn = _PacketConn()
    run_id = f"run.{uuid.uuid4().hex[:10]}"
    store = CompileArtifactStore(conn)
    packet = {
        "definition_revision": "def_1234abcd",
        "plan_revision": "plan_5678efgh",
        "packet_version": 1,
        "workflow_id": "workflow.alpha",
        "run_id": run_id,
        "spec_name": "alpha workflow",
        "source_kind": "workflow_submit",
        "authority_refs": ["def_1234abcd", "plan_5678efgh"],
        "model_messages": [
            {
                "job_label": "build",
                "agent_slug": "anthropic/claude",
                "messages": [
                    {"role": "system", "content": "system instruction"},
                    {"role": "user", "content": "build the thing"},
                ],
            }
        ],
        "reference_bindings": [
            {
                "job_label": "build",
                "agent_slug": "anthropic/claude",
                "depends_on": [],
                "prompt_hash": "prompt_hash_123",
                "route_task_type": "review",
                "route_origin_slug": "origin.slug",
                "route_candidates": ["anthropic/claude"],
            }
        ],
        "capability_bindings": [
            {
                "job_label": "build",
                "agent_slug": "anthropic/claude",
                "route_task_type": "review",
                "capabilities": ["capability.alpha"],
                "route_candidates": ["anthropic/claude"],
            }
        ],
        "verify_refs": ["verify_ref.python.py_compile.test"],
        "authority_inputs": {
            "authority": {"workflow_id": "workflow.alpha"},
            "workflow_row": {"id": "workflow.alpha"},
            "spec_snapshot": {"definition_revision": "def_1234abcd"},
        },
        "file_inputs": {
            "spec_path": "/tmp/spec.json",
            "context_files": ["app.py"],
        },
        "packet_hash": "0" * 64,
        "packet_revision": "packet_0000000000000000:1",
        "decision_ref": "decision.compile.packet.0000000000000000",
        "parent_artifact_ref": "plan_5678efgh",
    }

    store.record_execution_packet(
        packet=packet,
        authority_refs=["def_1234abcd", "plan_5678efgh"],
        decision_ref="decision.compile.packet.0000000000000000",
        parent_artifact_ref="plan_5678efgh",
    )

    loaded = store.load_execution_packets(run_id=run_id)

    assert len(loaded) == 1
    row = loaded[0]
    assert row.definition_revision == "def_1234abcd"
    assert row.plan_revision == "plan_5678efgh"
    assert row.packet_revision == "packet_0000000000000000:1"
    assert row.model_messages[0]["messages"][1]["content"] == "build the thing"
    assert row.reference_bindings[0]["route_task_type"] == "review"
    assert row.capability_bindings[0]["capabilities"] == ["capability.alpha"]
    assert row.verify_refs == ("verify_ref.python.py_compile.test",)
    assert row.authority_inputs["authority"]["workflow_id"] == "workflow.alpha"
    assert row.file_inputs["context_files"] == ["app.py"]


def test_workflow_submit_inline_records_execution_packet_lineage(monkeypatch) -> None:
    recorded: dict[str, object] = {}

    class _Conn:
        def execute(self, query: str, *args):
            if "FROM compile_artifacts" in query:
                return []
            if "INSERT INTO compile_artifacts" in query:
                return []
            if "INSERT INTO execution_packets" in query:
                recorded["args"] = args
                return []
            if "INSERT INTO workflow_definitions" in query or "INSERT INTO admission_decisions" in query:
                return []
            if "INSERT INTO workflow_jobs" in query:
                return [{"id": 1}]
            if "workflow.run" in query:
                return []
            return [{"id": 1}]

    class _Router:
        def resolve_spec_jobs(self, jobs, runtime_profile_ref=None):
            for job in jobs:
                job.setdefault(
                    "_route_plan",
                    type(
                        "Plan",
                        (),
                        {
                            "task_type": "task.alpha",
                            "original_slug": "route.alpha",
                            "chain": ("anthropic/claude",),
                        },
                    )(),
                )

    monkeypatch.setattr("runtime.task_type_router.TaskTypeRouter", lambda conn: _Router())
    monkeypatch.setattr(_admission, "check_idempotency", lambda *args, **kwargs: type("Result", (), {"is_replay": False, "is_conflict": False, "existing_run_id": None, "created_at": None})())
    monkeypatch.setattr(_admission, "record_idempotency", lambda *args, **kwargs: None)
    monkeypatch.setattr(_admission, "_ensure_workflow_authority", lambda *args, **kwargs: {"workflow_id": "workflow.alpha", "request_id": "req_1"})
    monkeypatch.setattr(_admission, "_recompute_workflow_run_state", lambda *args, **kwargs: None)

    spec = {
        "name": "alpha workflow",
        "phase": "build",
        "definition_revision": "def_1234abcd",
        "plan_revision": "plan_5678efgh",
        "jobs": [
            {
                "label": "build",
                "agent": "anthropic/claude",
                "prompt": "build the thing",
                "verify_refs": ["verify_ref.python.py_compile.test"],
                "capabilities": ["capability.alpha"],
            }
        ],
        "outcome_goal": "build the thing",
        "output_dir": "/tmp/out",
    }

    result = unified.submit_workflow_inline(_Conn(), spec, run_id="run.alpha")

    assert str(result["run_id"]).strip()
    assert result["packet_reuse_provenance"]["decision"] == "compiled"
    assert "args" in recorded
    args = recorded["args"]
    assert args[1] == "def_1234abcd"
    assert args[2] == "plan_5678efgh"
    payload = json.loads(args[18])
    assert payload["definition_revision"] == "def_1234abcd"
    assert payload["plan_revision"] == "plan_5678efgh"
    assert payload["model_messages"][0]["messages"][0]["content"].startswith("build the thing")
    assert payload["verify_refs"] == ["verify_ref.python.py_compile.test"]


def test_workflow_submit_inline_reuses_exact_packet_lineage(monkeypatch) -> None:
    class _Router:
        def resolve_spec_jobs(self, jobs, runtime_profile_ref=None):
            for job in jobs:
                job.setdefault(
                    "_route_plan",
                    type(
                        "Plan",
                        (),
                        {
                            "task_type": "task.alpha",
                            "original_slug": "route.alpha",
                            "chain": ("anthropic/claude",),
                        },
                    )(),
                )

    conn = _PacketConn()
    monkeypatch.setattr("runtime.task_type_router.TaskTypeRouter", lambda conn: _Router())
    monkeypatch.setattr(_admission, "check_idempotency", lambda *args, **kwargs: type("Result", (), {"is_replay": False, "is_conflict": False, "existing_run_id": None, "created_at": None})())
    monkeypatch.setattr(_admission, "record_idempotency", lambda *args, **kwargs: None)
    monkeypatch.setattr(_admission, "_ensure_workflow_authority", lambda *args, **kwargs: {"workflow_id": "workflow.alpha", "request_id": "req_1"})
    monkeypatch.setattr(_admission, "_recompute_workflow_run_state", lambda *args, **kwargs: None)

    spec = {
        "name": "alpha workflow",
        "phase": "build",
        "definition_revision": "def_1234abcd",
        "plan_revision": "plan_5678efgh",
        "jobs": [
            {
                "label": "build",
                "agent": "anthropic/claude",
                "prompt": "build the thing",
                "verify_refs": ["verify_ref.python.py_compile.test"],
                "capabilities": ["capability.alpha"],
            }
        ],
        "outcome_goal": "build the thing",
        "output_dir": "/tmp/out",
    }

    first_result = unified.submit_workflow_inline(conn, spec, run_id="run.alpha")
    second_result = unified.submit_workflow_inline(conn, spec, run_id="run.beta")

    assert first_result["packet_reuse_provenance"]["decision"] == "compiled"
    assert second_result["packet_reuse_provenance"]["decision"] == "reused"
    assert len(conn.compile_artifact_rows) == 1
    assert conn.rows[-1]["run_id"] == "run.beta"
    assert conn.rows[-1]["payload"]["compile_provenance"]["reuse"]["decision"] == "reused"


def test_workflow_submit_inline_reuses_child_invocation_packet_lineage(monkeypatch) -> None:
    class _Router:
        def resolve_spec_jobs(self, jobs, runtime_profile_ref=None):
            for job in jobs:
                job.setdefault(
                    "_route_plan",
                    type(
                        "Plan",
                        (),
                        {
                            "task_type": "task.alpha",
                            "original_slug": "route.alpha",
                            "chain": ("anthropic/claude",),
                        },
                    )(),
                )

    conn = _PacketConn()
    monkeypatch.setattr("runtime.task_type_router.TaskTypeRouter", lambda conn: _Router())
    monkeypatch.setattr(_admission, "check_idempotency", lambda *args, **kwargs: type("Result", (), {"is_replay": False, "is_conflict": False, "existing_run_id": None, "created_at": None})())
    monkeypatch.setattr(_admission, "record_idempotency", lambda *args, **kwargs: None)
    monkeypatch.setattr(_admission, "_ensure_workflow_authority", lambda *args, **kwargs: {"workflow_id": "workflow.alpha", "request_id": "req_1"})
    monkeypatch.setattr(_admission, "_recompute_workflow_run_state", lambda *args, **kwargs: None)

    spec = {
        "name": "alpha workflow",
        "phase": "build",
        "definition_revision": "def_1234abcd",
        "plan_revision": "plan_5678efgh",
        "jobs": [
            {
                "label": "build",
                "agent": "anthropic/claude",
                "prompt": "build the thing",
            }
        ],
        "outcome_goal": "build the thing",
    }
    packet_provenance = {
        "source_kind": "workflow_invoke",
        "workflow_row": {
            "id": "workflow.alpha",
            "name": "Alpha Workflow",
            "invocation_count": 4,
            "last_invoked_at": "2026-04-09T12:00:00+00:00",
        },
        "definition_row": {
            "definition_revision": "def_1234abcd",
        },
        "compiled_spec_row": {
            "definition_revision": "def_1234abcd",
            "plan_revision": "plan_5678efgh",
        },
        "file_inputs": {
            "inputs": {
                "ticket": "T-1",
            },
        },
    }

    first_result = unified.submit_workflow_inline(
        conn,
        spec,
        run_id="run.alpha",
        packet_provenance=packet_provenance,
    )
    second_result = unified.submit_workflow_inline(
        conn,
        spec,
        run_id="run.beta",
        packet_provenance=packet_provenance,
    )

    assert first_result["packet_reuse_provenance"]["decision"] == "compiled"
    assert second_result["packet_reuse_provenance"]["decision"] == "reused"
    assert second_result["packet_reuse_provenance"]["reason_code"] == "packet.compile.exact_input_match"
    assert len(conn.compile_artifact_rows) == 1
    assert conn.rows[-1]["payload"]["source_kind"] == "workflow_invoke"


def test_workflow_submit_inline_rejects_stale_child_invocation_packet_lineage(monkeypatch) -> None:
    class _Router:
        def resolve_spec_jobs(self, jobs, runtime_profile_ref=None):
            for job in jobs:
                job.setdefault(
                    "_route_plan",
                    type(
                        "Plan",
                        (),
                        {
                            "task_type": "task.alpha",
                            "original_slug": "route.alpha",
                            "chain": ("anthropic/claude",),
                        },
                    )(),
                )

    conn = _PacketConn()
    monkeypatch.setattr("runtime.task_type_router.TaskTypeRouter", lambda conn: _Router())
    monkeypatch.setattr(_admission, "check_idempotency", lambda *args, **kwargs: type("Result", (), {"is_replay": False, "is_conflict": False, "existing_run_id": None, "created_at": None})())
    monkeypatch.setattr(_admission, "record_idempotency", lambda *args, **kwargs: None)
    monkeypatch.setattr(_admission, "_ensure_workflow_authority", lambda *args, **kwargs: {"workflow_id": "workflow.alpha", "request_id": "req_1"})
    monkeypatch.setattr(_admission, "_recompute_workflow_run_state", lambda *args, **kwargs: None)

    spec = {
        "name": "alpha workflow",
        "phase": "build",
        "definition_revision": "def_1234abcd",
        "plan_revision": "plan_5678efgh",
        "jobs": [
            {
                "label": "build",
                "agent": "anthropic/claude",
                "prompt": "build the thing",
            }
        ],
        "outcome_goal": "build the thing",
    }
    packet_provenance = {
        "source_kind": "workflow_invoke",
        "workflow_row": {
            "id": "workflow.alpha",
            "name": "Alpha Workflow",
        },
        "definition_row": {
            "definition_revision": "def_1234abcd",
        },
        "compiled_spec_row": {
            "definition_revision": "def_1234abcd",
            "plan_revision": "plan_5678efgh",
        },
        "file_inputs": {
            "inputs": {
                "ticket": "T-1",
            },
        },
    }

    first_result = unified.submit_workflow_inline(
        conn,
        spec,
        run_id="run.alpha",
        packet_provenance=packet_provenance,
    )
    assert first_result["packet_reuse_provenance"]["decision"] == "compiled"
    conn.compile_artifact_rows[0]["content_hash"] = "corrupt"

    with pytest.raises(RuntimeError, match="workflow packet lineage reuse failed closed"):
        unified.submit_workflow_inline(
            conn,
            spec,
            run_id="run.beta",
            packet_provenance=packet_provenance,
        )


def test_workflow_submit_inline_rejects_malformed_reusable_packet_lineage(monkeypatch) -> None:
    class _Router:
        def resolve_spec_jobs(self, jobs, runtime_profile_ref=None):
            for job in jobs:
                job.setdefault(
                    "_route_plan",
                    type(
                        "Plan",
                        (),
                        {
                            "task_type": "task.alpha",
                            "original_slug": "route.alpha",
                            "chain": ("anthropic/claude",),
                        },
                    )(),
                )

    conn = _PacketConn()

    monkeypatch.setattr("runtime.task_type_router.TaskTypeRouter", lambda conn: _Router())
    monkeypatch.setattr(_admission, "check_idempotency", lambda *args, **kwargs: type("Result", (), {"is_replay": False, "is_conflict": False, "existing_run_id": None, "created_at": None})())
    monkeypatch.setattr(_admission, "record_idempotency", lambda *args, **kwargs: None)
    monkeypatch.setattr(_admission, "_ensure_workflow_authority", lambda *args, **kwargs: {"workflow_id": "workflow.alpha", "request_id": "req_1"})
    monkeypatch.setattr(_admission, "_recompute_workflow_run_state", lambda *args, **kwargs: None)
    spec = {
        "name": "alpha workflow",
        "phase": "build",
        "definition_revision": "def_1234abcd",
        "plan_revision": "plan_5678efgh",
        "jobs": [{"label": "build", "agent": "anthropic/claude", "prompt": "build the thing"}],
        "outcome_goal": "build the thing",
        "output_dir": "/tmp/out",
    }

    first_result = unified.submit_workflow_inline(conn, spec, run_id="run.alpha")
    assert first_result["packet_reuse_provenance"]["decision"] == "compiled"
    revision_ref = str(conn.compile_artifact_rows[0]["revision_ref"])
    conn.compile_artifact_rows[0]["payload"] = {
        "packet_revision": revision_ref,
    }

    with pytest.raises(RuntimeError, match="workflow packet lineage reuse failed closed"):
        unified.submit_workflow_inline(conn, spec, run_id="run.beta")
