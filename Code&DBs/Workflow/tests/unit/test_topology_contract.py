"""Tests for runtime.topology_contract."""

import pytest

import importlib.util
import pathlib
import sys

_WORKFLOW_ROOT = str(pathlib.Path(__file__).resolve().parents[2])

# Import the module directly to avoid the runtime package __init__.py which
# has dependencies that may not be available in this test environment.
_MOD_NAME = "runtime.topology_contract"
_spec = importlib.util.spec_from_file_location(
    _MOD_NAME,
    f"{_WORKFLOW_ROOT}/runtime/topology_contract.py",
)
_mod = importlib.util.module_from_spec(_spec)
sys.modules[_MOD_NAME] = _mod
_spec.loader.exec_module(_mod)

ProfileContract = _mod.ProfileContract
ProfilePosture = _mod.ProfilePosture
PromotionPreflightRails = _mod.PromotionPreflightRails
PromotionStep = _mod.PromotionStep
PromotionState = _mod.PromotionState
TopologyRegistry = _mod.TopologyRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _valid_contract(name: str = "alpha") -> ProfileContract:
    return ProfileContract(
        profile_name=name,
        posture=ProfilePosture.OPERATE,
        receipts_dir="/work/receipts",
        topology_dir="/work/topology",
        workdir="/work",
        allowed_write_roots=("/work", "/tmp"),
        max_concurrent_dispatches=4,
        require_evidence=True,
    )


# ---------------------------------------------------------------------------
# ProfileContract validation
# ---------------------------------------------------------------------------

class TestProfileContractValidation:
    def test_valid_contract_no_errors(self):
        reg = TopologyRegistry()
        reg.register(_valid_contract())
        assert reg.validate("alpha") == []

    def test_receipts_dir_not_under_workdir(self):
        c = ProfileContract(
            profile_name="bad_recv",
            posture=ProfilePosture.BUILD,
            receipts_dir="/elsewhere/receipts",
            topology_dir="/work/topology",
            workdir="/work",
            allowed_write_roots=("/work",),
            max_concurrent_dispatches=1,
            require_evidence=False,
        )
        reg = TopologyRegistry()
        reg.register(c)
        errors = reg.validate("bad_recv")
        assert any("receipts_dir must be under workdir" in e for e in errors)

    def test_topology_dir_not_under_workdir(self):
        c = ProfileContract(
            profile_name="bad_topo",
            posture=ProfilePosture.BUILD,
            receipts_dir="/work/receipts",
            topology_dir="/other/topology",
            workdir="/work",
            allowed_write_roots=("/work",),
            max_concurrent_dispatches=1,
            require_evidence=False,
        )
        reg = TopologyRegistry()
        reg.register(c)
        errors = reg.validate("bad_topo")
        assert any("topology_dir must be under workdir" in e for e in errors)

    def test_relative_allowed_write_root(self):
        c = ProfileContract(
            profile_name="bad_roots",
            posture=ProfilePosture.OBSERVE,
            receipts_dir="/work/receipts",
            topology_dir="/work/topology",
            workdir="/work",
            allowed_write_roots=("relative/path",),
            max_concurrent_dispatches=1,
            require_evidence=False,
        )
        reg = TopologyRegistry()
        reg.register(c)
        errors = reg.validate("bad_roots")
        assert any("must be absolute" in e for e in errors)

    def test_unregistered_profile(self):
        reg = TopologyRegistry()
        errors = reg.validate("ghost")
        assert any("not registered" in e for e in errors)


# ---------------------------------------------------------------------------
# TopologyRegistry register / get / list
# ---------------------------------------------------------------------------

class TestTopologyRegistry:
    def test_register_and_get(self):
        reg = TopologyRegistry()
        c = _valid_contract("one")
        reg.register(c)
        assert reg.get("one") is c

    def test_get_missing_returns_none(self):
        reg = TopologyRegistry()
        assert reg.get("nope") is None

    def test_list_profiles(self):
        reg = TopologyRegistry()
        c1 = _valid_contract("a")
        c2 = _valid_contract("b")
        reg.register(c1)
        reg.register(c2)
        names = {p.profile_name for p in reg.list_profiles()}
        assert names == {"a", "b"}


# ---------------------------------------------------------------------------
# Promotion step sequencing
# ---------------------------------------------------------------------------

class TestPromotionSequencing:
    def test_happy_path_full_sequence(self):
        rails = PromotionPreflightRails()
        s = rails.begin_promotion("p")
        assert s.current_step == PromotionStep.SHOW
        assert s.completed_steps == ()

        s = rails.advance("p")
        assert s.current_step == PromotionStep.ACTIVATE
        assert PromotionStep.SHOW in s.completed_steps

        s = rails.advance("p")
        assert s.current_step == PromotionStep.DRIFT_CHECK
        assert PromotionStep.ACTIVATE in s.completed_steps

        s = rails.advance("p")
        assert s.current_step == PromotionStep.VERIFY
        assert PromotionStep.DRIFT_CHECK in s.completed_steps

        s = rails.advance("p")
        assert s.current_step is None
        assert len(s.completed_steps) == 4

    def test_begin_initializes_at_show(self):
        rails = PromotionPreflightRails()
        s = rails.begin_promotion("x")
        assert s.current_step == PromotionStep.SHOW
        assert s.completed_steps == ()
        assert s.blocked is False

    def test_advance_past_verify_returns_completed(self):
        rails = PromotionPreflightRails()
        rails.begin_promotion("done")
        for _ in range(4):
            rails.advance("done")
        # Already completed -- advance again should return completed state
        s = rails.advance("done")
        assert s.current_step is None
        assert len(s.completed_steps) == 4

    def test_complete_marks_all_done(self):
        rails = PromotionPreflightRails()
        rails.begin_promotion("c")
        s = rails.complete("c")
        assert s.current_step is None
        assert len(s.completed_steps) == 4
        assert s.blocked is False


# ---------------------------------------------------------------------------
# Skip behaviour
# ---------------------------------------------------------------------------

class TestPromotionSkip:
    def test_skip_activate_blocks_with_cause(self):
        rails = PromotionPreflightRails()
        rails.begin_promotion("s")
        rails.advance("s")  # move past SHOW -> now at ACTIVATE
        s = rails.skip("s", PromotionStep.ACTIVATE, "no_target")
        assert s.blocked is True
        assert s.block_reason == "no_target"

    def test_skip_drift_check_blocks_with_cause(self):
        rails = PromotionPreflightRails()
        rails.begin_promotion("s")
        rails.advance("s")  # past SHOW
        rails.advance("s")  # past ACTIVATE -> DRIFT_CHECK
        s = rails.skip("s", PromotionStep.DRIFT_CHECK, "drift_disabled")
        assert s.blocked is True
        assert s.block_reason == "drift_disabled"

    def test_cannot_skip_show(self):
        rails = PromotionPreflightRails()
        rails.begin_promotion("m")
        with pytest.raises(ValueError, match="cannot skip mandatory"):
            rails.skip("m", PromotionStep.SHOW, "reason")

    def test_cannot_skip_verify(self):
        rails = PromotionPreflightRails()
        rails.begin_promotion("m")
        with pytest.raises(ValueError, match="cannot skip mandatory"):
            rails.skip("m", PromotionStep.VERIFY, "reason")

    def test_advance_while_blocked_raises(self):
        rails = PromotionPreflightRails()
        rails.begin_promotion("b")
        rails.advance("b")  # past SHOW -> ACTIVATE
        rails.skip("b", PromotionStep.ACTIVATE, "blocked_reason")
        with pytest.raises(ValueError, match="blocked"):
            rails.advance("b")


# ---------------------------------------------------------------------------
# State retrieval
# ---------------------------------------------------------------------------

class TestPromotionState:
    def test_state_returns_none_for_unknown(self):
        rails = PromotionPreflightRails()
        assert rails.state("unknown") is None

    def test_state_returns_current(self):
        rails = PromotionPreflightRails()
        rails.begin_promotion("q")
        s = rails.state("q")
        assert s is not None
        assert s.profile_name == "q"
        assert s.current_step == PromotionStep.SHOW
