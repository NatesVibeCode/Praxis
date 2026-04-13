from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

import runtime.workflow_chain as workflow_chain


def _write_spec(path: Path, *, name: str) -> None:
    path.write_text(
        json.dumps(
            {
                "name": name,
                "phase": "execute",
                "jobs": [
                    {
                        "label": "job-1",
                        "agent": "human",
                        "prompt": "do the work",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )


def test_load_workflow_chain_orders_waves_and_normalizes_paths(tmp_path: Path) -> None:
    spec_a = tmp_path / "config" / "specs" / "wave_a.json"
    spec_b = tmp_path / "config" / "specs" / "wave_b.json"
    spec_a.parent.mkdir(parents=True, exist_ok=True)
    _write_spec(spec_a, name="Wave A")
    _write_spec(spec_b, name="Wave B")

    coordination = tmp_path / "chain.json"
    coordination.write_text(
        json.dumps(
            {
                "program": "phase_one_chain",
                "validate_order": [
                    "config/specs/wave_b.json",
                    "config/specs/wave_a.json",
                ],
                "waves": [
                    {
                        "wave_id": "wave_b",
                        "depends_on": ["wave_a"],
                        "specs": ["config/specs/wave_b.json"],
                    },
                    {
                        "wave_id": "wave_a",
                        "specs": ["config/specs/wave_a.json"],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    program = workflow_chain.load_workflow_chain(str(coordination), repo_root=str(tmp_path))

    assert program.program == "phase_one_chain"
    assert [wave.wave_id for wave in program.waves] == ["wave_a", "wave_b"]
    assert workflow_chain.iter_chain_spec_paths(program) == (
        "config/specs/wave_b.json",
        "config/specs/wave_a.json",
    )


def test_load_workflow_chain_rejects_unknown_dependency(tmp_path: Path) -> None:
    spec_path = tmp_path / "config" / "specs" / "wave_a.json"
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    _write_spec(spec_path, name="Wave A")

    coordination = tmp_path / "chain.json"
    coordination.write_text(
        json.dumps(
            {
                "program": "broken_chain",
                "waves": [
                    {
                        "wave_id": "wave_a",
                        "depends_on": ["wave_missing"],
                        "specs": ["config/specs/wave_a.json"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(workflow_chain.WorkflowChainError, match="unknown wave"):
        workflow_chain.load_workflow_chain(str(coordination), repo_root=str(tmp_path))


def test_workflow_chain_id_from_result_ref_extracts_chain_id() -> None:
    assert workflow_chain.workflow_chain_id_from_result_ref("workflow_chain:workflow_chain_123") == "workflow_chain_123"
    assert workflow_chain.workflow_chain_id_from_result_ref("workflow_run:dispatch_123") is None
    assert workflow_chain.workflow_chain_id_from_result_ref(None) is None


def test_bootstrap_workflow_chain_schema_applies_control_and_chain_bootstrap(monkeypatch) -> None:
    called: list[str] = []

    class _FakeConn:
        def __init__(self) -> None:
            self.scripts: list[str] = []
            self.queries: list[str] = []

        def execute(self, query: str, *args):
            del args
            self.queries.append(query)
            if "to_regclass('control_commands')" in query:
                return [{"table_name": None}]
            raise AssertionError(f"unexpected query: {query}")

        def execute_script(self, sql: str) -> None:
            self.scripts.append(sql)

    def _bootstrap_control(_conn) -> None:
        called.append("control")

    monkeypatch.setattr("runtime.control_commands.bootstrap_control_commands_schema", _bootstrap_control)
    monkeypatch.setattr(workflow_chain, "_schema_statements", lambda _filename: ("SELECT 1",))

    conn = _FakeConn()
    workflow_chain.bootstrap_workflow_chain_schema(conn)

    assert called == ["control"]
    assert conn.queries == ["SELECT to_regclass('control_commands') AS table_name"]
    assert conn.scripts == ["SELECT 1;\nSELECT 1;\nSELECT 1;"]


def test_load_workflow_chain_rejects_validate_order_drift(tmp_path: Path) -> None:
    spec_a = tmp_path / "config" / "specs" / "wave_a.json"
    spec_b = tmp_path / "config" / "specs" / "wave_b.json"
    spec_a.parent.mkdir(parents=True, exist_ok=True)
    _write_spec(spec_a, name="Wave A")
    _write_spec(spec_b, name="Wave B")

    coordination = tmp_path / "chain.json"
    coordination.write_text(
        json.dumps(
            {
                "program": "drifted_chain",
                "validate_order": [
                    "config/specs/wave_a.json",
                ],
                "waves": [
                    {
                        "wave_id": "wave_a",
                        "specs": ["config/specs/wave_a.json"],
                    },
                    {
                        "wave_id": "wave_b",
                        "specs": ["config/specs/wave_b.json"],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(workflow_chain.WorkflowChainError, match="validate_order must match"):
        workflow_chain.load_workflow_chain(str(coordination), repo_root=str(tmp_path))


def test_spec_rows_use_runtime_workflow_id_and_queue_id(monkeypatch, tmp_path: Path) -> None:
    spec_path = tmp_path / "spec_a.json"
    _write_spec(spec_path, name="Spec A")

    program = workflow_chain.WorkflowChainProgram(
        program="phase_one_chain",
        coordination_path="chain.json",
        mode=None,
        why=None,
        validate_order=(),
        waves=(
            workflow_chain.WorkflowChainWave(
                wave_id="wave_a",
                spec_paths=("spec_a.json",),
            ),
        ),
    )

    spec = SimpleNamespace(
        name="Spec A",
        workflow_id="phase_1_w1_frontdoor_truth_alignment",
        jobs=[{"label": "a"}],
        _raw={"queue_id": "queue-alpha"},
    )

    monkeypatch.setattr(
        "runtime.workflow_spec.WorkflowSpec.load",
        classmethod(lambda _cls, _path: spec),
    )
    monkeypatch.setattr(
        "runtime.workflow._shared._workflow_id_for_spec",
        lambda _spec: "workflow.phase.1.w1.frontdoor.truth.alignment",
    )

    rows = workflow_chain._spec_rows_for_program(program, repo_root=tmp_path)

    assert rows == [
        {
            "wave_id": "wave_a",
            "ordinal": 1,
            "spec_path": "spec_a.json",
            "spec_name": "Spec A",
            "workflow_id": "workflow.phase.1.w1.frontdoor.truth.alignment",
            "spec_workflow_id": "phase_1_w1_frontdoor_truth_alignment",
            "queue_id": "queue-alpha",
            "total_jobs": 1,
        }
    ]


def test_get_workflow_chain_status_preserves_full_depends_on_list(monkeypatch) -> None:
    class _FakeConn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if "FROM workflow_chains" in normalized:
                return [
                    {
                        "chain_id": args[0],
                        "command_id": "control.command.chain.submit.12",
                        "coordination_path": "config/specs/pre_ui_execution_spine/BATCH_COORDINATION.json",
                        "repo_root": "/repo",
                        "program": "phase_1_pre_ui_execution_spine_convergence",
                        "mode": "many_specs",
                        "why": "test",
                        "definition": json.dumps({"waves": []}),
                        "adopt_active": True,
                        "status": "running",
                        "current_wave_id": "wave_b",
                        "requested_by_kind": "cli",
                        "requested_by_ref": "workflow_cli.chain",
                        "last_error_code": None,
                        "last_error_detail": None,
                        "created_at": None,
                        "updated_at": None,
                        "started_at": None,
                        "finished_at": None,
                    }
                ]
            if "FROM workflow_chain_waves" in normalized:
                return [
                    {
                        "chain_id": args[0],
                        "wave_id": "wave_b",
                        "ordinal": 2,
                        "depends_on_wave_id": "wave_shared",
                        "blocked_by_wave_id": None,
                        "status": "running",
                        "created_at": None,
                        "updated_at": None,
                        "started_at": None,
                        "completed_at": None,
                    }
                ]
            if "FROM workflow_chain_wave_dependencies" in normalized:
                return [
                    {
                        "wave_id": "wave_b",
                        "depends_on_wave_id": "wave_a",
                    },
                    {
                        "wave_id": "wave_b",
                        "depends_on_wave_id": "wave_shared",
                    },
                ]
            if "FROM workflow_chain_wave_runs" in normalized:
                return [
                    {
                        "chain_id": args[0],
                        "wave_id": "wave_b",
                        "ordinal": 1,
                        "spec_path": "spec_b.json",
                        "spec_name": "Spec B",
                        "workflow_id": "workflow.spec.b",
                        "spec_workflow_id": "spec_b",
                        "queue_id": None,
                        "command_id": "control.command.submit.1",
                        "run_id": "workflow_b_001",
                        "submission_status": "queued",
                        "run_status": "queued",
                        "completed_jobs": 0,
                        "total_jobs": 1,
                        "created_at": None,
                        "updated_at": None,
                        "started_at": None,
                        "completed_at": None,
                    }
                ]
            raise AssertionError(f"unexpected query: {normalized}")

    monkeypatch.setattr(workflow_chain, "bootstrap_workflow_chain_schema", lambda _conn: None)

    status = workflow_chain.get_workflow_chain_status(_FakeConn(), "workflow_chain_012")

    assert status is not None
    assert status["chain_id"] == "workflow_chain_012"
    assert status["waves"][0]["depends_on"] == ["wave_a", "wave_shared"]
    assert status["waves"][0]["depends_on_wave_id"] is None
    assert status["waves"][0]["runs"][0]["run_id"] == "workflow_b_001"


def test_find_active_run_for_workflow_id_uses_adoption_key_column() -> None:
    class _FakeConn:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        def execute(self, query: str, *args):
            self.calls.append((query, args))
            return [{"run_id": "workflow_123", "workflow_id": args[0], "current_state": "running", "requested_at": None}]

    conn = _FakeConn()
    row = workflow_chain.find_active_run_for_workflow_id(
        conn,
        "workflow.phase.1.w1.frontdoor.truth.alignment",
        queue_id="phase1_w1_frontdoor_truth_alignment",
    )

    assert row is not None
    assert row["run_id"] == "workflow_123"
    assert "adoption_key = $2" in conn.calls[0][0]


def test_assert_unique_adoption_targets_rejects_ambiguous_rows() -> None:
    rows = [
        {
            "workflow_id": "workflow.phase.1.w1.frontdoor.truth.alignment",
            "queue_id": "phase1_w1_frontdoor_truth_alignment",
            "spec_path": "spec_a.json",
        },
        {
            "workflow_id": "workflow.phase.1.w1.frontdoor.truth.alignment",
            "queue_id": "phase1_w1_frontdoor_truth_alignment",
            "spec_path": "spec_b.json",
        },
    ]

    with pytest.raises(workflow_chain.WorkflowChainError, match="ambiguous adoption targets"):
        workflow_chain._assert_unique_adoption_targets(rows)


def test_cancel_active_wave_runs_issues_cancel_commands_and_updates_status(monkeypatch) -> None:
    state = {"chain_id": "workflow_chain_012"}
    wave = {
        "wave_id": "wave_a",
        "runs": [
            {
                "spec_path": "spec_a.json",
                "run_id": "workflow_123",
                "run_status": "running",
            },
            {
                "spec_path": "spec_b.json",
                "run_id": "workflow_456",
                "run_status": "failed",
            },
        ],
    }

    issued: list[object] = []
    updated: list[tuple[str, str, str, int | None, int | None]] = []

    monkeypatch.setattr(
        "runtime.control_commands.execute_control_intent",
        lambda _conn, intent, *, approved_by: issued.append((intent, approved_by))
        or SimpleNamespace(command_id="control.command.cancel.1", command_status="succeeded", result_ref="workflow_run:workflow_123"),
    )
    monkeypatch.setattr(
        "runtime.control_commands.render_control_command_response",
        lambda _conn, _command, *, action, run_id: {
            "status": "cancelled",
            "command_status": "succeeded",
            "command_id": "control.command.cancel.1",
            "run_id": run_id,
            "action": action,
        },
    )
    monkeypatch.setattr(
        "runtime.workflow._status.get_run_status",
        lambda _conn, run_id: {
            "status": "cancelled",
            "completed_jobs": 1,
            "total_jobs": 1,
            "run_id": run_id,
        },
    )
    monkeypatch.setattr(
        workflow_chain,
        "_update_wave_run_status",
        lambda _conn, *, chain_id, wave_id, spec_path, run_status, completed_jobs, total_jobs: updated.append(
            (chain_id, wave_id, spec_path, completed_jobs, total_jobs)
        ),
    )

    cleanup_results = workflow_chain._cancel_active_wave_runs(object(), state, wave)

    assert len(issued) == 1
    intent, approved_by = issued[0]
    assert intent.command_type == "workflow.cancel"
    assert intent.payload == {"run_id": "workflow_123", "include_running": True}
    assert intent.idempotency_key == "workflow.chain.cancel.workflow_chain_012.wave_a.workflow_123"
    assert approved_by == "workflow.chain.cleanup"
    assert cleanup_results == [
        {
            "spec_path": "spec_a.json",
            "run_id": "workflow_123",
            "status": "cancelled",
            "command_status": "succeeded",
            "command_id": "control.command.cancel.1",
            "error_code": None,
            "error_detail": None,
            "run_status": "cancelled",
        }
    ]
    assert updated == [("workflow_chain_012", "wave_a", "spec_a.json", 1, 1)]


def test_advance_workflow_chains_cancels_sibling_runs_before_marking_wave_failed(monkeypatch) -> None:
    state = {
        "chain_id": "workflow_chain_012",
        "status": "running",
        "waves": [
            {
                "wave_id": "wave_a",
                "ordinal": 1,
                "status": "running",
                "runs": [
                    {
                        "spec_path": "spec_a.json",
                        "run_status": "failed",
                        "run_id": "workflow_123",
                    },
                    {
                        "spec_path": "spec_b.json",
                        "run_status": "running",
                        "run_id": "workflow_456",
                    },
                ],
            }
        ],
    }

    class _FakeConn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if "FROM workflow_chains" in normalized:
                return [{"chain_id": "workflow_chain_012"}]
            raise AssertionError(f"unexpected query: {normalized}")

    captured: list[tuple[str, str, list[dict[str, object]]]] = []

    monkeypatch.setattr(workflow_chain, "_recover_stale_dispatch_rows", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(workflow_chain, "_refresh_running_wave_runs", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(workflow_chain, "get_workflow_chain_status", lambda *_args, **_kwargs: state)
    monkeypatch.setattr(
        workflow_chain,
        "_cancel_active_wave_runs",
        lambda _conn, _state, _wave, *, skip_spec_path=None: [
            {
                "spec_path": "spec_b.json",
                "run_id": "workflow_456",
                "status": "cancelled",
                "command_status": "succeeded",
                "command_id": "control.command.cancel.2",
                "error_code": None,
                "error_detail": None,
                "run_status": "cancelled",
            }
        ]
        if skip_spec_path == "spec_a.json"
        else [],
    )
    monkeypatch.setattr(
        workflow_chain,
        "_mark_wave_failed_and_chain_failed",
        lambda _conn, _state, wave, *, error_code, error_detail, cleanup_results=None: captured.append(
            (error_code, error_detail, cleanup_results or [])
        ),
    )

    actions = workflow_chain.advance_workflow_chains(_FakeConn(), chain_id="workflow_chain_012")

    assert actions == 2
    assert captured == [
        (
            "workflow.chain.wave_failed",
            "wave wave_a failed because spec_a.json reached failed",
            [
                {
                    "spec_path": "spec_b.json",
                    "run_id": "workflow_456",
                    "status": "cancelled",
                    "command_status": "succeeded",
                    "command_id": "control.command.cancel.2",
                    "error_code": None,
                    "error_detail": None,
                    "run_status": "cancelled",
                }
            ],
        )
    ]


def test_advance_workflow_chains_marks_chain_cancelled_when_run_cancelled(monkeypatch) -> None:
    state = {
        "chain_id": "workflow_chain_012",
        "status": "running",
        "waves": [
            {
                "wave_id": "wave_a",
                "ordinal": 1,
                "status": "running",
                "runs": [
                    {
                        "spec_path": "spec_a.json",
                        "run_status": "cancelled",
                        "run_id": "workflow_123",
                    }
                ],
            }
        ],
    }

    class _FakeConn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if "FROM workflow_chains" in normalized:
                return [{"chain_id": "workflow_chain_012"}]
            raise AssertionError(f"unexpected query: {normalized}")

    called: list[tuple[str, str]] = []

    monkeypatch.setattr(workflow_chain, "_recover_stale_dispatch_rows", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(workflow_chain, "_refresh_running_wave_runs", lambda *_args, **_kwargs: 0)
    monkeypatch.setattr(workflow_chain, "get_workflow_chain_status", lambda *_args, **_kwargs: state)
    monkeypatch.setattr(workflow_chain, "_cancel_active_wave_runs", lambda *_args, **_kwargs: [])
    monkeypatch.setattr(
        workflow_chain,
        "_mark_wave_cancelled_and_chain_cancelled",
        lambda _conn, _state, wave, *, error_code, error_detail, cleanup_results=None: called.append((error_code, error_detail)),
    )

    actions = workflow_chain.advance_workflow_chains(_FakeConn(), chain_id="workflow_chain_012")

    assert actions == 1
    assert called == [
        (
            "workflow.chain.wave_cancelled",
            "wave wave_a cancelled because spec_a.json reached cancelled",
        )
    ]
