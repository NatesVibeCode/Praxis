"""Tests for runtime.posture — posture enforcement and tool classification."""

from datetime import datetime, timezone

import importlib
import pathlib
import sys

_WORKFLOW_ROOT = str(pathlib.Path(__file__).resolve().parents[2])

# Import posture module directly to avoid the runtime __init__.py which
# pulls in domain.py (requires Python 3.10+ slots=True).
_spec = importlib.util.spec_from_file_location(
    "runtime.posture",
    f"{_WORKFLOW_ROOT}/runtime/posture.py",
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules["runtime.posture"] = _mod
_spec.loader.exec_module(_mod)

CallClassification = _mod.CallClassification
Posture = _mod.Posture
PostureEnforcer = _mod.PostureEnforcer
ToolCall = _mod.ToolCall


def _tc(name: str, **kwargs) -> ToolCall:
    """Shorthand for building a ToolCall with sensible defaults."""
    return ToolCall(
        tool_name=name,
        arguments=kwargs,
        timestamp=datetime.now(tz=timezone.utc),
    )


# ── OBSERVE posture ────────────────────────────────────────────────────


class TestObservePosture:
    def test_observe_blocks_mutate(self):
        enforcer = PostureEnforcer(Posture.OBSERVE)
        verdict = enforcer.check(_tc("create_user"))
        assert not verdict.allowed
        assert verdict.classification is CallClassification.MUTATE
        assert verdict.reason is not None

    def test_observe_allows_read(self):
        enforcer = PostureEnforcer(Posture.OBSERVE)
        verdict = enforcer.check(_tc("get_user"))
        assert verdict.allowed
        assert verdict.classification is CallClassification.READ

    def test_observe_allows_telemetry(self):
        enforcer = PostureEnforcer(Posture.OBSERVE)
        verdict = enforcer.check(_tc("log_event"))
        assert verdict.allowed
        assert verdict.classification is CallClassification.TELEMETRY


# ── OPERATE posture ────────────────────────────────────────────────────


class TestOperatePosture:
    def test_operate_allows_read(self):
        enforcer = PostureEnforcer(Posture.OPERATE)
        assert enforcer.check(_tc("list_items")).allowed

    def test_operate_allows_mutate(self):
        enforcer = PostureEnforcer(Posture.OPERATE)
        assert enforcer.check(_tc("delete_record")).allowed

    def test_operate_allows_telemetry(self):
        enforcer = PostureEnforcer(Posture.OPERATE)
        assert enforcer.check(_tc("emit_metric")).allowed


# ── BUILD posture ──────────────────────────────────────────────────────


class TestBuildPosture:
    def test_build_allows_read(self):
        enforcer = PostureEnforcer(Posture.BUILD)
        assert enforcer.check(_tc("search_index")).allowed

    def test_build_allows_mutate(self):
        enforcer = PostureEnforcer(Posture.BUILD)
        assert enforcer.check(_tc("execute_migration")).allowed

    def test_build_allows_telemetry(self):
        enforcer = PostureEnforcer(Posture.BUILD)
        assert enforcer.check(_tc("track_progress")).allowed


# ── Classification ─────────────────────────────────────────────────────


class TestClassification:
    def test_unknown_tool_classified_as_mutate(self):
        """Fail-closed: unrecognised tools are MUTATE."""
        enforcer = PostureEnforcer(Posture.OBSERVE)
        cls = enforcer.classify(_tc("do_something_weird"))
        assert cls is CallClassification.MUTATE

    def test_custom_classification_overrides_default(self):
        custom = {"delete_record": CallClassification.READ}
        enforcer = PostureEnforcer(Posture.OBSERVE, tool_classifications=custom)
        verdict = enforcer.check(_tc("delete_record"))
        assert verdict.allowed
        assert verdict.classification is CallClassification.READ

    def test_prefix_read_tools(self):
        enforcer = PostureEnforcer(Posture.OBSERVE)
        for prefix in ("get_", "list_", "search_", "query_", "status_", "inspect_", "read_"):
            tc = _tc(f"{prefix}something")
            assert enforcer.classify(tc) is CallClassification.READ, f"Failed for {prefix}"

    def test_prefix_mutate_tools(self):
        enforcer = PostureEnforcer(Posture.OBSERVE)
        for prefix in (
            "create_",
            "update_",
            "delete_",
            "write_",
            "insert_",
            "dispatch_",
            "execute_",
        ):
            tc = _tc(f"{prefix}something")
            assert enforcer.classify(tc) is CallClassification.MUTATE, f"Failed for {prefix}"

    def test_prefix_telemetry_tools(self):
        enforcer = PostureEnforcer(Posture.OBSERVE)
        for prefix in ("log_", "record_", "emit_", "track_"):
            tc = _tc(f"{prefix}something")
            assert enforcer.classify(tc) is CallClassification.TELEMETRY, f"Failed for {prefix}"


# ── Deny log ───────────────────────────────────────────────────────────


class TestDenyLog:
    def test_deny_log_captures_blocked_calls(self):
        enforcer = PostureEnforcer(Posture.OBSERVE)
        tc = _tc("create_user")
        enforcer.check(tc)
        assert len(enforcer.deny_log) == 1
        assert enforcer.deny_log[0].tool_name == "create_user"

    def test_deny_log_not_populated_on_allowed_call(self):
        enforcer = PostureEnforcer(Posture.OBSERVE)
        enforcer.check(_tc("get_user"))
        assert len(enforcer.deny_log) == 0


# ── with_posture ───────────────────────────────────────────────────────


class TestWithPosture:
    def test_with_posture_creates_new_enforcer(self):
        original = PostureEnforcer(Posture.OBSERVE)
        upgraded = original.with_posture(Posture.BUILD)
        assert upgraded is not original

    def test_with_posture_does_not_mutate_original(self):
        original = PostureEnforcer(Posture.OBSERVE)
        original.check(_tc("create_user"))  # denied in OBSERVE
        assert len(original.deny_log) == 1

        upgraded = original.with_posture(Posture.BUILD)
        upgraded.check(_tc("create_user"))  # allowed in BUILD
        # Original deny log unchanged.
        assert len(original.deny_log) == 1
        assert len(upgraded.deny_log) == 0

    def test_with_posture_preserves_custom_classifications(self):
        custom = {"magic_tool": CallClassification.READ}
        original = PostureEnforcer(Posture.OBSERVE, tool_classifications=custom)
        upgraded = original.with_posture(Posture.OPERATE)
        assert upgraded.classify(_tc("magic_tool")) is CallClassification.READ
