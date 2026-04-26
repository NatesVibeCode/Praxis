from __future__ import annotations

import argparse
from io import StringIO
from types import SimpleNamespace
from surfaces.cli.commands import workflow as workflow_commands

def test_generate_command_routing(monkeypatch) -> None:
    observed = []
    def fake_cmd_generate(args):
        observed.append(args)
        return 0
    
    monkeypatch.setattr("surfaces.cli.workflow_cli.cmd_generate", fake_cmd_generate)
    
    stdout = StringIO()
    exit_code = workflow_commands._generate_command(["manifest.json", "output.json"], stdout=stdout)
    
    assert exit_code == 0
    assert len(observed) == 1
    assert observed[0].manifest_file == "manifest.json"
    assert observed[0].output == "output.json"

def test_validate_command_routing(monkeypatch) -> None:
    observed = []
    def fake_cmd_validate(args):
        observed.append(args)
        return 0
    
    monkeypatch.setattr("surfaces.cli.workflow_cli.cmd_validate", fake_cmd_validate)
    
    stdout = StringIO()
    exit_code = workflow_commands._validate_command(["spec.json"], stdout=stdout)
    
    assert exit_code == 0
    assert len(observed) == 1
    assert observed[0].spec == "spec.json"

def test_stream_command_routing(monkeypatch) -> None:
    observed = []
    def fake_cmd_stream(args):
        observed.append(args)
        return 0
    
    monkeypatch.setattr("surfaces.cli.workflow_cli.cmd_stream", fake_cmd_stream)
    
    stdout = StringIO()
    exit_code = workflow_commands._stream_command(["run1", "--poll-interval", "1.0"], stdout=stdout)
    
    assert exit_code == 0
    assert len(observed) == 1
    assert observed[0].run_id == "run1"
    assert observed[0].poll_interval == 1.0

def test_chain_status_command_routing(monkeypatch) -> None:
    observed = []
    def fake_cmd_chain_status(args):
        observed.append(args)
        return 0
    
    monkeypatch.setattr("surfaces.cli.workflow_cli.cmd_chain_status", fake_cmd_chain_status)
    
    stdout = StringIO()
    exit_code = workflow_commands._chain_status_command(["chain1", "--limit", "5"], stdout=stdout)
    
    assert exit_code == 0
    assert len(observed) == 1
    assert observed[0].chain_id == "chain1"
    assert observed[0].limit == 5
