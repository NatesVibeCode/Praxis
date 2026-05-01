from __future__ import annotations

from pathlib import Path
import sys

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime.agent_action_fingerprints import build_action_fingerprint_record


def test_build_shell_fingerprint_shapes_absolute_repo_path() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    record = build_action_fingerprint_record(
        tool_name="local_shell",
        tool_input={
            "command": [
                "pytest",
                str(repo_root / "Code&DBs/Workflow/tests/unit/test_agent_action_fingerprints.py"),
                "-q",
            ]
        },
        source_surface="codex:host",
        session_ref="session-1",
        payload_meta={"harness": "codex_cli"},
        repo_root=str(repo_root),
    )
    assert record is not None
    assert record.action_kind == "shell"
    assert record.normalized_command == "pytest Code&DBs/Workflow/**/unit/*.py -q"
    assert record.session_ref == "session-1"
    assert record.payload_meta["raw_tool_name"] == "local_shell"


def test_build_edit_fingerprint_shapes_patch_paths() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    record = build_action_fingerprint_record(
        tool_name="apply_patch",
        tool_input={
            "patch": "\n".join(
                [
                    "*** Begin Patch",
                    "*** Update File: Code&DBs/Workflow/runtime/foo.py",
                    "@@",
                    "-x",
                    "+y",
                    "*** Add File: Code&DBs/Workflow/tests/unit/foo_test.py",
                    "+pass",
                    "*** End Patch",
                ]
            )
        },
        source_surface="codex:host",
        repo_root=str(repo_root),
    )
    assert record is not None
    assert record.action_kind == "multi_edit"
    assert record.path_shape == "Code&DBs/Workflow/**/unit/*.py\nCode&DBs/Workflow/runtime/*.py"


def test_build_read_fingerprint_uses_read_action_kind() -> None:
    repo_root = Path(__file__).resolve().parents[4]
    record = build_action_fingerprint_record(
        tool_name="read_file",
        tool_input={"file_path": str(repo_root / ".claude/hooks/preact_orient_friction.py")},
        source_surface="gemini:host",
        repo_root=str(repo_root),
    )
    assert record is not None
    assert record.action_kind == "read"
    assert record.path_shape == ".claude/hooks/*.py"
