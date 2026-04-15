from __future__ import annotations

from runtime import workflow_worker


def test_start_worker_checks_dependency_contract_before_launch(monkeypatch) -> None:
    observed: dict[str, object] = {}

    def _fake_require_runtime_dependencies(*, scope: str = "workflow_worker", manifest_path=None):
        observed["scope"] = scope
        observed["manifest_path"] = manifest_path
        return {
            "ok": True,
            "scope": scope,
            "manifest_path": "/tmp/requirements.runtime.txt",
            "required_count": 9,
            "available_count": 9,
            "missing_count": 0,
            "packages": [],
            "missing": [],
        }

    monkeypatch.setattr(workflow_worker, "require_runtime_dependencies", _fake_require_runtime_dependencies)
    monkeypatch.setattr(workflow_worker, "_build_worker_connection", lambda: "fake-conn")
    monkeypatch.setattr(
        workflow_worker,
        "_run_worker_loop",
        lambda conn, repo_root, *, poll_interval=2.0: observed.update(
            {
                "conn": conn,
                "repo_root": repo_root,
                "poll_interval": poll_interval,
            }
        ),
    )

    workflow_worker.start_worker(
        poll_interval=0.5,
        file_path="/tmp/Praxis/Code&DBs/Workflow/runtime/workflow_worker.py",
    )

    assert observed["scope"] == "workflow_worker"
    assert observed["manifest_path"] is None
    assert observed["conn"] == "fake-conn"
    assert observed["repo_root"] == "/tmp/Praxis"
    assert observed["poll_interval"] == 0.5
