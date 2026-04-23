from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_path_authority_surfaces_do_not_bake_host_specific_defaults() -> None:
    assert "_SANDBOX_PATH_PREFIX = \"/opt/homebrew/bin:/usr/local/bin:\"" not in _read(
        "Code&DBs/Workflow/runtime/workflow/execution_backends.py"
    )
    assert "/tmp/workflow-sandbox" not in _read("Code&DBs/Workflow/runtime/claims.py")
    assert "tempfile.gettempdir()" not in _read("Code&DBs/Workflow/runtime/claims.py")
    assert "Path(\"/tmp/atlas_heuristic_map.json\")" not in _read(
        "Code&DBs/Workflow/runtime/atlas_graph.py"
    )
    assert "Path(\"/tmp/praxis-engine.log\")" not in _read(
        "Code&DBs/Workflow/runtime/praxis_supervisor.py"
    )
    assert "Path(\"/tmp/praxis-api-server.err\")" not in _read(
        "Code&DBs/Workflow/runtime/praxis_supervisor.py"
    )
    assert "Path(\"/tmp/praxis-workflow-worker.err\")" not in _read(
        "Code&DBs/Workflow/runtime/praxis_supervisor.py"
    )
    assert "Path(\"/tmp/praxis-scheduler.err\")" not in _read(
        "Code&DBs/Workflow/runtime/praxis_supervisor.py"
    )
    assert "tempfile.gettempdir()" not in _read(
        "Code&DBs/Workflow/runtime/praxis_supervisor.py"
    )
    assert 'Path.home() / "Library" / "LaunchAgents"' not in _read(
        "Code&DBs/Workflow/runtime/praxis_supervisor.py"
    )
    assert 'Path.home() / "Library" / "LaunchAgents"' not in _read(
        "Code&DBs/Workflow/runtime/_workflow_database.py"
    )
    assert "Path(__file__).resolve().parents[3]  # /Users/nate/Praxis" not in _read(
        "Code&DBs/Workflow/runtime/daily_heartbeat.py"
    )
    assert "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin" not in _read(
        ".claude/hooks/session-start-standing-orders.sh"
    )
    assert "postgresql://localhost:5432/praxis" not in _read(
        ".claude/hooks/session-start-standing-orders.sh"
    )
    assert "/usr/bin/python3" not in _read(".claude/hooks/post-edit-reindex.sh")
    assert "/Users/nate/.local/bin/praxis" not in _read(
        ".claude/hooks/post-edit-reindex.sh"
    )
    assert "postgresql://localhost:5432/praxis" not in _read(".claude/launch.json")
    assert "CodeDBs/Workflow" not in _read("scripts/test.sh")
    assert "CodeDBs/Workflow" not in _read("scripts/praxis")
    assert "CodeDBs/Workflow" not in _read("scripts/verify_evidence_chain.py")
    assert "/usr/local/bin:/usr/bin:/bin" not in _read(
        "Code&DBs/Workflow/surfaces/app/vite.config.ts"
    )
    assert "~/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin" not in _read(
        "scripts/install_launchd_plist.sh"
    )
    assert "~/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin" not in _read(
        "scripts/install_daily_heartbeat.sh"
    )
    assert "~/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin" not in _read(
        "scripts/install_agent_sessions.sh"
    )
    assert "postgresql://localhost:5432/praxis" not in _read(
        "scripts/install_launchd_plist.sh"
    )
    assert "postgresql://localhost:5432/praxis" not in _read(
        "scripts/install_daily_heartbeat.sh"
    )
    assert "postgresql://localhost:5432/praxis" not in _read(
        "scripts/install_agent_sessions.sh"
    )
    assert "postgresql://localhost:5432/praxis" not in _read(
        "scripts/install_authority_memory_refresh.sh"
    )
    assert 'local_bin="${PRAXIS_LOCAL_BIN_DIR:-${XDG_BIN_HOME:-}}"' in _read(
        "scripts/bootstrap"
    )
    assert 'local_bin="${PRAXIS_LOCAL_BIN_DIR:-${XDG_BIN_HOME:-$HOME/.local/bin}}"' not in _read(
        "scripts/bootstrap"
    )
    assert "postgresql://localhost:5432/praxis" not in _read("scripts/bootstrap")
