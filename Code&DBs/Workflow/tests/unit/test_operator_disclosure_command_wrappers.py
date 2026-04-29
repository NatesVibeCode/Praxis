from __future__ import annotations

import sys
import types
import unittest
from pathlib import Path
from unittest.mock import Mock, patch


WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))


def _import_targets():
    try:
        from runtime.operations.commands import bug_actions as bug_actions_module
        from runtime.operations.commands import platform_patterns as platform_patterns_module
    except (ImportError, ModuleNotFoundError):
        pydantic = types.ModuleType("pydantic")

        class _BaseModel:
            def __init__(self, **data):
                for key, value in self.__class__.__dict__.items():
                    if key.startswith("_") or callable(value) or isinstance(
                        value, (classmethod, staticmethod)
                    ):
                        continue
                    setattr(self, key, data.get(key, value))
                for key, value in data.items():
                    setattr(self, key, value)

            def model_dump(self, *, exclude_none=False):
                data = {
                    key: value
                    for key, value in self.__dict__.items()
                    if not (exclude_none and value is None)
                }
                return dict(data)

        def _field(default=None, **kwargs):
            return default

        def _field_validator(*args, **kwargs):
            def decorator(fn):
                return fn

            return decorator

        pydantic.BaseModel = _BaseModel
        pydantic.Field = _field
        pydantic.field_validator = _field_validator

        runtime_repo_policy = types.ModuleType("runtime.repo_policy_onboarding")
        runtime_repo_policy.consume_operator_disclosure = Mock(return_value=None)

        runtime_workspace_paths = types.ModuleType("runtime.workspace_paths")
        runtime_workspace_paths.repo_root = Mock(return_value=str(WORKFLOW_ROOT))

        runtime_platform_patterns = types.ModuleType("runtime.platform_patterns")

        class _FallbackPlatformPatternAuthority:
            def __init__(self, conn):
                self.conn = conn

            def materialize_candidates(self, **kwargs):
                return {"ok": True, "materialized_count": 0, "patterns": []}

        runtime_platform_patterns.PlatformPatternAuthority = _FallbackPlatformPatternAuthority

        sys.modules["pydantic"] = pydantic
        sys.modules["runtime.repo_policy_onboarding"] = runtime_repo_policy
        sys.modules["runtime.workspace_paths"] = runtime_workspace_paths
        sys.modules["runtime.platform_patterns"] = runtime_platform_patterns

        from runtime.operations.commands import bug_actions as bug_actions_module
        from runtime.operations.commands import platform_patterns as platform_patterns_module

    return bug_actions_module, platform_patterns_module


bug_actions, platform_patterns = _import_targets()


class _FakeBugTracker:
    pass


class _FakeBugTrackerMod:
    class BugStatus:
        FIXED = "FIXED"
        WONT_FIX = "WONT_FIX"
        DEFERRED = "DEFERRED"


class _FakeBugSubsystems:
    def __init__(self, pg_conn: object) -> None:
        self._pg_conn = pg_conn

    def get_bug_tracker(self) -> object:
        return _FakeBugTracker()

    def get_bug_tracker_mod(self) -> object:
        return _FakeBugTrackerMod()

    def get_pg_conn(self) -> object:
        return self._pg_conn


class _FakePatternSubsystems:
    def __init__(self, pg_conn: object) -> None:
        self._pg_conn = pg_conn

    def get_pg_conn(self) -> object:
        return self._pg_conn


class OperatorDisclosureWrapperTests(unittest.TestCase):
    def _patch_bug_surface(self, file_payload=None, resolve_payload=None):
        module = types.ModuleType("surfaces.api.handlers._bug_surface_contract")
        if file_payload is not None:
            module.file_bug_payload = Mock(return_value=file_payload)
        if resolve_payload is not None:
            module.resolve_bug_payload = Mock(return_value=resolve_payload)

        patches = {
            "surfaces": types.ModuleType("surfaces"),
            "surfaces.api": types.ModuleType("surfaces.api"),
            "surfaces.api.handlers": types.ModuleType("surfaces.api.handlers"),
            "surfaces.api.handlers._bug_surface_contract": module,
        }
        return patch.dict(sys.modules, patches, clear=False)

    def test_handle_bug_file_appends_operator_disclosure_when_active(self) -> None:
        disclosure = {
            "kind": "operator_onboarding_disclosure",
            "disclosure_kind": "bug",
            "times_shown": 1,
        }
        payload = {"ok": True, "filed": {"bug_id": "bug-1"}}
        with self._patch_bug_surface(file_payload=payload):
            with patch.object(
                bug_actions,
                "consume_operator_disclosure",
                return_value=disclosure,
            ) as consume_mock:
                with patch.object(bug_actions, "workspace_repo_root", return_value=WORKFLOW_ROOT):
                    result = bug_actions.handle_bug_file(
                        bug_actions.BugFileCommand(title="missing docs"),
                        _FakeBugSubsystems(object()),
                    )

        self.assertEqual(result["operator_disclosure"], disclosure)
        consume_mock.assert_called_once()

    def test_handle_bug_file_stays_quiet_without_active_contract(self) -> None:
        payload = {"ok": True, "filed": {"bug_id": "bug-1"}}
        with self._patch_bug_surface(file_payload=payload):
            with patch.object(
                bug_actions,
                "consume_operator_disclosure",
                return_value=None,
            ) as consume_mock:
                with patch.object(bug_actions, "workspace_repo_root", return_value=WORKFLOW_ROOT):
                    result = bug_actions.handle_bug_file(
                        bug_actions.BugFileCommand(title="missing docs"),
                        _FakeBugSubsystems(object()),
                    )

        self.assertNotIn("operator_disclosure", result)
        consume_mock.assert_called_once()

    def test_handle_bug_resolve_appends_operator_disclosure_when_active(self) -> None:
        disclosure = {
            "kind": "operator_onboarding_disclosure",
            "disclosure_kind": "bug",
            "times_shown": 2,
        }
        payload = {"ok": True, "resolved": {"bug_id": "bug-1"}}
        with self._patch_bug_surface(resolve_payload=payload):
            with patch.object(
                bug_actions,
                "consume_operator_disclosure",
                return_value=disclosure,
            ) as consume_mock:
                with patch.object(bug_actions, "workspace_repo_root", return_value=WORKFLOW_ROOT):
                    result = bug_actions.handle_bug_resolve(
                        bug_actions.BugResolveCommand(bug_id="bug-1", status="FIXED"),
                        _FakeBugSubsystems(object()),
                    )

        self.assertEqual(result["operator_disclosure"], disclosure)
        consume_mock.assert_called_once()

    def test_handle_bug_resolve_stays_quiet_without_active_contract(self) -> None:
        payload = {"ok": True, "resolved": {"bug_id": "bug-1"}}
        with self._patch_bug_surface(resolve_payload=payload):
            with patch.object(
                bug_actions,
                "consume_operator_disclosure",
                return_value=None,
            ) as consume_mock:
                with patch.object(bug_actions, "workspace_repo_root", return_value=WORKFLOW_ROOT):
                    result = bug_actions.handle_bug_resolve(
                        bug_actions.BugResolveCommand(bug_id="bug-1", status="FIXED"),
                        _FakeBugSubsystems(object()),
                    )

        self.assertNotIn("operator_disclosure", result)
        consume_mock.assert_called_once()

    def test_handle_pattern_materialize_candidates_appends_operator_disclosure_when_active(self) -> None:
        disclosure = {
            "kind": "operator_onboarding_disclosure",
            "disclosure_kind": "pattern",
            "times_shown": 1,
        }
        payload = {"ok": True, "materialized_count": 1, "patterns": []}
        authority = Mock()
        authority.materialize_candidates.return_value = payload

        with patch.object(platform_patterns, "PlatformPatternAuthority", return_value=authority):
            with patch.object(
                platform_patterns,
                "consume_operator_disclosure",
                return_value=disclosure,
            ) as consume_mock:
                with patch.object(
                    platform_patterns,
                    "workspace_repo_root",
                    return_value=WORKFLOW_ROOT,
                ):
                    result = platform_patterns.handle_pattern_materialize_candidates(
                        platform_patterns.PatternMaterializeCandidatesCommand(),
                        _FakePatternSubsystems(object()),
                    )

        self.assertEqual(result["operator_disclosure"], disclosure)
        consume_mock.assert_called_once()

    def test_handle_pattern_materialize_candidates_stays_quiet_when_nothing_materialized(self) -> None:
        payload = {"ok": True, "materialized_count": 0, "patterns": []}
        authority = Mock()
        authority.materialize_candidates.return_value = payload

        with patch.object(platform_patterns, "PlatformPatternAuthority", return_value=authority):
            with patch.object(platform_patterns, "consume_operator_disclosure") as consume_mock:
                with patch.object(
                    platform_patterns,
                    "workspace_repo_root",
                    return_value=WORKFLOW_ROOT,
                ):
                    result = platform_patterns.handle_pattern_materialize_candidates(
                        platform_patterns.PatternMaterializeCandidatesCommand(),
                        _FakePatternSubsystems(object()),
                    )

        self.assertNotIn("operator_disclosure", result)
        consume_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
