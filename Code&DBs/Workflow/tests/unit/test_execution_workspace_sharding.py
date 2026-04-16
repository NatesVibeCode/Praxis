from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from runtime.workflow import _execution_core as _exec_mod


def _run_row(*, run_id: str = "run.alpha", runtime_profile_ref: str = "praxis") -> dict[str, object]:
    return {
        "run_id": run_id,
        "current_state": "queued",
        "request_envelope": {
            "workflow_id": "workflow.alpha",
            "runtime_profile_ref": runtime_profile_ref,
            "spec_snapshot": {
                "jobs": [
                    {
                        "label": "job-alpha",
                        "prompt": "implement",
                    }
                ]
            },
        },
    }


def _binding_row(*, repo_root: str | None, workdir: str | None) -> dict[str, object]:
    return {
        "fork_worktree_binding_id": "binding.alpha",
        "binding_scope": "proposal",
        "sandbox_session_id": "sandbox.alpha",
        "runtime_profile_ref": "praxis",
        "fork_ref": "refs/heads/feature.alpha",
        "worktree_ref": "worktree.alpha",
        "materialized_repo_root": repo_root,
        "materialized_workdir": workdir,
    }

class _WorkspaceConn:
    def __init__(self, binding_rows: list[dict[str, object]]) -> None:
        self._binding_rows = binding_rows

    def execute(self, query: str, *args):
        del args
        normalized = " ".join(query.split())
        if "FROM fork_worktree_bindings" in normalized:
            return list(self._binding_rows)
        raise AssertionError(f"Unexpected query: {normalized}")


def test_resolve_execution_workspace_defaults_to_worker_repo_root(monkeypatch, tmp_path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    monkeypatch.setattr(
        _exec_mod,
        "resolve_native_runtime_profile_config",
        lambda _ref: SimpleNamespace(repo_root=str(repo_root)),
    )

    workspace = _exec_mod._resolve_execution_workspace(
        repo_root=str(repo_root),
        execution_bundle=None,
    )

    assert workspace["repo_root"] == str(repo_root.resolve())
    assert workspace["workdir"] == str(repo_root.resolve())
    assert workspace["fork_ownership"] is None


def test_resolve_execution_workspace_uses_materialized_binding_paths(monkeypatch, tmp_path) -> None:
    repo_root = tmp_path / "repo"
    shard_root = repo_root / ".worktrees" / "alpha"
    shard_root.mkdir(parents=True)
    monkeypatch.setattr(
        _exec_mod,
        "resolve_native_runtime_profile_config",
        lambda _ref: SimpleNamespace(repo_root=str(repo_root)),
    )

    workspace = _exec_mod._resolve_execution_workspace(
        repo_root=str(repo_root),
        execution_bundle={
            "fork_ownership": _exec_mod._load_active_fork_ownership(
                _WorkspaceConn([_binding_row(repo_root=".worktrees/alpha", workdir=".")]),
                run_row=_run_row(),
                repo_root=str(repo_root),
            )
        },
    )

    assert workspace["repo_root"] == str(shard_root.resolve())
    assert workspace["workdir"] == str(shard_root.resolve())
    assert workspace["fork_ownership"] == {
        "fork_worktree_binding_id": "binding.alpha",
        "binding_scope": "proposal",
        "sandbox_session_id": "sandbox.alpha",
        "runtime_profile_ref": "praxis",
        "fork_ref": "refs/heads/feature.alpha",
        "worktree_ref": "worktree.alpha",
        "materialized_repo_root": str(shard_root.resolve()),
        "materialized_workdir": str(shard_root.resolve()),
    }


def test_resolve_execution_workspace_rejects_missing_materialized_paths(monkeypatch, tmp_path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    monkeypatch.setattr(
        _exec_mod,
        "resolve_native_runtime_profile_config",
        lambda _ref: SimpleNamespace(repo_root=str(repo_root)),
    )

    try:
        _exec_mod._resolve_execution_workspace(
            repo_root=str(repo_root),
            execution_bundle={
                "fork_ownership": _exec_mod._load_active_fork_ownership(
                    _WorkspaceConn([_binding_row(repo_root=None, workdir=None)]),
                    run_row=_run_row(),
                    repo_root=str(repo_root),
                )
            },
        )
    except RuntimeError as exc:
        assert "missing materialized_repo_root/materialized_workdir" in str(exc)
    else:
        raise AssertionError("Expected sharded execution to reject an unmaterialized binding")


def test_resolve_execution_workspace_rejects_paths_outside_boundary(monkeypatch, tmp_path) -> None:
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    outside_root = tmp_path / "outside"
    outside_root.mkdir()
    monkeypatch.setattr(
        _exec_mod,
        "resolve_native_runtime_profile_config",
        lambda _ref: SimpleNamespace(repo_root=str(repo_root)),
    )

    try:
        _exec_mod._resolve_execution_workspace(
            repo_root=str(repo_root),
            execution_bundle={
                "fork_ownership": _exec_mod._load_active_fork_ownership(
                    _WorkspaceConn(
                        [
                            _binding_row(
                                repo_root=str(outside_root),
                                workdir=str(outside_root),
                            )
                        ]
                    ),
                    run_row=_run_row(),
                    repo_root=str(repo_root),
                )
            },
        )
    except RuntimeError as exc:
        assert "escapes the declared workspace boundary" in str(exc)
    else:
        raise AssertionError("Expected sharded execution to reject an out-of-bound worktree")


def test_execute_job_uses_materialized_workdir_for_cli(monkeypatch, tmp_path) -> None:
    repo_root = tmp_path / "repo"
    shard_root = repo_root / ".worktrees" / "alpha"
    shard_root.mkdir(parents=True)

    class _Conn:
        def execute(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT run_id, current_state, request_envelope FROM workflow_runs WHERE run_id = $1":
                return [_run_row()]
            if "FROM fork_worktree_bindings" in normalized:
                return [_binding_row(repo_root=".worktrees/alpha", workdir=".")]
            if "FROM workflow_job_runtime_context" in normalized:
                return []
            if "INSERT INTO workflow_job_runtime_context" in normalized:
                return []
            raise AssertionError(f"Unexpected query: {normalized}")

    captured: dict[str, object] = {}
    completed: dict[str, object] = {}

    monkeypatch.setattr(
        _exec_mod,
        "resolve_native_runtime_profile_config",
        lambda _ref: SimpleNamespace(repo_root=str(repo_root)),
    )
    monkeypatch.setattr(_exec_mod, "mark_running", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        "registry.agent_config.AgentRegistry.load_from_postgres",
        lambda _conn: SimpleNamespace(get=lambda _slug: SimpleNamespace(provider="openai")),
    )
    monkeypatch.setattr(_exec_mod, "_runtime_profile_ref_for_run", lambda *_args, **_kwargs: "praxis")
    monkeypatch.setattr(
        _exec_mod,
        "resolve_execution_transport",
        lambda _config: SimpleNamespace(transport_kind="cli"),
    )
    monkeypatch.setattr(
        "runtime.task_type_router.TaskTypeRouter",
        lambda _conn: SimpleNamespace(resolve_explicit_eligibility=lambda *args, **kwargs: None),
    )
    monkeypatch.setattr(
        _exec_mod,
        "_resolve_job_prompt_authority",
        lambda *_args, **_kwargs: ("implement the feature", None, False, None, None),
    )
    monkeypatch.setattr(_exec_mod, "_build_platform_context", lambda _repo_root: "platform context")
    monkeypatch.setattr(_exec_mod, "_runtime_execution_context_shard", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(
        _exec_mod,
        "_runtime_execution_bundle",
        lambda *_args, **_kwargs: {
            "approval_required": False,
            "mcp_tool_names": ["praxis_query"],
        },
    )
    monkeypatch.setattr(_exec_mod, "_persist_runtime_context_for_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_exec_mod, "_capture_submission_baseline_if_required", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _exec_mod,
        "_run_post_execution_verification",
        lambda _conn, **kwargs: {
            "result": kwargs["result"],
            "final_status": kwargs["result"]["status"],
            "final_error_code": kwargs["result"].get("error_code", ""),
            "verification_summary": None,
            "verification_bindings": None,
            "verification_error": None,
        },
    )
    monkeypatch.setattr(
        _exec_mod,
        "_resolve_submission",
        lambda _conn, **kwargs: SimpleNamespace(
            submission_state=None,
            final_status=kwargs["final_status"],
            final_error_code=kwargs["final_error_code"],
            result=kwargs["result"],
        ),
    )
    monkeypatch.setattr(_exec_mod, "_write_output", lambda *_args, **_kwargs: "")
    monkeypatch.setattr(_exec_mod, "_write_job_receipt", lambda *_args, **_kwargs: "receipt.alpha")
    monkeypatch.setattr(
        _exec_mod,
        "complete_job",
        lambda _conn, _job_id, **kwargs: completed.update(kwargs),
    )

    import runtime.agent_spawner as agent_spawner_module

    monkeypatch.setattr(
        agent_spawner_module.AgentSpawner,
        "preflight",
        lambda self, agent_slug: SimpleNamespace(
            provider=agent_slug.split("/", 1)[0],
            ready=True,
            reason=None,
            checked_at=datetime.now(timezone.utc),
        ),
    )

    def _capture_cli(_config, prompt: str, workdir: str, execution_bundle=None):
        captured["prompt"] = prompt
        captured["workdir"] = workdir
        captured["execution_bundle"] = execution_bundle
        return {
            "status": "succeeded",
            "stdout": "done",
            "stderr": "",
            "exit_code": 0,
            "token_input": 0,
            "token_output": 0,
            "cost_usd": 0.0,
        }

    monkeypatch.setattr(_exec_mod, "_execute_cli", _capture_cli)

    _exec_mod.execute_job(
        _Conn(),
        {
            "id": 15,
            "label": "job-alpha",
            "agent_slug": "openai/gpt-5.4",
            "prompt": "implement the feature",
            "run_id": "run.alpha",
        },
        repo_root=str(repo_root),
    )

    assert completed["status"] == "succeeded"
    assert captured["workdir"] == str(shard_root.resolve())
    assert captured["execution_bundle"]["fork_ownership"] == {
        "fork_worktree_binding_id": "binding.alpha",
        "binding_scope": "proposal",
        "sandbox_session_id": "sandbox.alpha",
        "runtime_profile_ref": "praxis",
        "fork_ref": "refs/heads/feature.alpha",
        "worktree_ref": "worktree.alpha",
        "materialized_repo_root": str(shard_root.resolve()),
        "materialized_workdir": str(shard_root.resolve()),
    }
