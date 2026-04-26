from __future__ import annotations

from types import SimpleNamespace
from runtime.workflow.ready_specs import evaluate_ready_specs

def test_evaluate_ready_specs_no_rows(monkeypatch) -> None:
    class FakeConn:
        def execute(self, query, *args):
            return []
    
    conn = FakeConn()
    assert evaluate_ready_specs(conn) == 0

def test_evaluate_ready_specs_success(monkeypatch) -> None:
    observed_updates = []
    
    class FakeConn:
        def execute(self, query, *args):
            if "SELECT" in query:
                return [{"spec_id": "spec1", "spec_path": "path/to/spec.json"}]
            if "UPDATE" in query:
                observed_updates.append({"args": args})
                return []
            return []

    class FakeSpec:
        def __init__(self):
            self.name = "test-spec"
            self.jobs = [1, 2]

    monkeypatch.setattr("runtime.workflow_spec.WorkflowSpec.load", lambda path: FakeSpec())
    monkeypatch.setattr("runtime.control_commands.request_workflow_submit_command", 
                        lambda *args, **kwargs: SimpleNamespace(command_id="cmd1"))
    monkeypatch.setattr("runtime.control_commands.render_workflow_submit_response", 
                        lambda *args, **kwargs: {"run_id": "run1"})
    monkeypatch.setattr("runtime.workspace_paths.repo_root", lambda: "/fake/root")

    conn = FakeConn()
    assert evaluate_ready_specs(conn) == 1
    assert len(observed_updates) == 1
    assert observed_updates[0]["args"] == ("spec1", "run1")
