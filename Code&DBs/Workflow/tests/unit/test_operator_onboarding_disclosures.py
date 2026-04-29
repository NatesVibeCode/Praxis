from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import patch


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))


class _FakeSubsystems:
    def get_pg_conn(self):
        return object()

    def get_bug_tracker(self):
        return object()

    def get_bug_tracker_mod(self):
        class _BugStatus:
            FIXED = "FIXED"
            WONT_FIX = "WONT_FIX"
            DEFERRED = "DEFERRED"

        class _Mod:
            BugStatus = _BugStatus

        return _Mod


def test_bug_file_command_attaches_operator_disclosure() -> None:
    from runtime.operations.commands.bug_actions import BugFileCommand, handle_bug_file

    command = BugFileCommand(title="T")
    with patch(
        "surfaces.api.handlers._bug_surface_contract.file_bug_payload",
        return_value={"ok": True, "filed": True, "bug": {"bug_id": "BUG-1"}},
    ):
        with patch(
            "runtime.operations.commands.bug_actions.consume_operator_disclosure",
            return_value={"message": "stored as onboarding disclosure"},
        ):
            payload = handle_bug_file(command, _FakeSubsystems())
    assert payload["operator_disclosure"]["message"] == "stored as onboarding disclosure"


def test_bug_resolve_command_attaches_operator_disclosure() -> None:
    from runtime.operations.commands.bug_actions import BugResolveCommand, handle_bug_resolve

    command = BugResolveCommand(bug_id="BUG-1", status="FIXED")
    with patch(
        "surfaces.api.handlers._bug_surface_contract.resolve_bug_payload",
        return_value={"ok": True, "resolved": True, "bug": {"bug_id": "BUG-1"}},
    ):
        with patch(
            "runtime.operations.commands.bug_actions.consume_operator_disclosure",
            return_value={"message": "resolved disclosure"},
        ):
            payload = handle_bug_resolve(command, _FakeSubsystems())
    assert payload["operator_disclosure"]["message"] == "resolved disclosure"


def test_pattern_materialize_command_attaches_operator_disclosure() -> None:
    from runtime.operations.commands.platform_patterns import (
        PatternMaterializeCandidatesCommand,
        handle_pattern_materialize_candidates,
    )

    command = PatternMaterializeCandidatesCommand(candidate_keys=["alpha"])
    with patch(
        "runtime.operations.commands.platform_patterns.PlatformPatternAuthority"
    ) as authority_cls:
        authority_cls.return_value.materialize_candidates.return_value = {
            "ok": True,
            "materialized_count": 1,
            "patterns": [{"pattern_ref": "PATTERN-1"}],
        }
        with patch(
            "runtime.operations.commands.platform_patterns.consume_operator_disclosure",
            return_value={"message": "pattern disclosure"},
        ):
            payload = handle_pattern_materialize_candidates(command, _FakeSubsystems())
    assert payload["operator_disclosure"]["message"] == "pattern disclosure"
