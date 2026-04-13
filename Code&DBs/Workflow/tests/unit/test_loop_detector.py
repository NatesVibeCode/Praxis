"""Tests for runtime.loop_detector."""

from datetime import datetime, timedelta, timezone

import importlib.util
import os
import sys

# Import directly from file to avoid runtime/__init__.py pulling in
# dependencies that may require a newer Python version.
_path = os.path.join(
    os.path.dirname(__file__), os.pardir, os.pardir, "runtime", "loop_detector.py"
)
_spec = importlib.util.spec_from_file_location("loop_detector", os.path.abspath(_path))
_mod = importlib.util.module_from_spec(_spec)
sys.modules["loop_detector"] = _mod
_spec.loader.exec_module(_mod)

FailureRecord = _mod.FailureRecord
LoopDetector = _mod.LoopDetector
LoopVerdict = _mod.LoopVerdict


def _ts(minutes_ago: int = 0) -> datetime:
    """Return a UTC timestamp *minutes_ago* minutes in the past."""
    return datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)


def _failure(
    job: str = "job-a",
    code: str = "ERR_TIMEOUT",
    minutes_ago: int = 0,
    cost: float = 1.0,
    attempt: int = 1,
) -> FailureRecord:
    return FailureRecord(
        job_label=job,
        failure_code=code,
        timestamp=_ts(minutes_ago),
        token_cost=cost,
        attempt_number=attempt,
    )


# ------------------------------------------------------------------
# 1. Consecutive failure detection
# ------------------------------------------------------------------

def test_consecutive_two_failures_ok():
    det = LoopDetector(max_consecutive_failures=3)
    det.record_failure(_failure(job="build"))
    det.record_failure(_failure(job="build"))
    v = det.check("build")
    assert v.action == "proceed" or v.action == "warn"
    assert v.consecutive_failures == 2


def test_consecutive_three_failures_stop():
    det = LoopDetector(max_consecutive_failures=3)
    for i in range(3):
        det.record_failure(_failure(job="build", attempt=i + 1))
    v = det.check("build")
    assert v.action == "stop"
    assert v.consecutive_failures == 3
    assert any("consecutively" in r for r in v.reasons)


# ------------------------------------------------------------------
# 2. Success resets consecutive counter
# ------------------------------------------------------------------

def test_success_resets_consecutive():
    det = LoopDetector(max_consecutive_failures=3)
    det.record_failure(_failure(job="build"))
    det.record_failure(_failure(job="build"))
    det.record_success("build", _ts())
    det.record_failure(_failure(job="build"))
    v = det.check("build")
    assert v.consecutive_failures == 1


# ------------------------------------------------------------------
# 3. Repeating failure code across different jobs
# ------------------------------------------------------------------

def test_repeating_failure_code_warn():
    det = LoopDetector(max_consecutive_failures=10)
    for job in ("alpha", "beta", "gamma"):
        det.record_failure(_failure(job=job, code="ERR_OOM"))
    v = det.check("alpha")
    assert v.action == "warn"
    assert any("ERR_OOM" in r for r in v.reasons)


# ------------------------------------------------------------------
# 4. Token burn threshold
# ------------------------------------------------------------------

def test_token_burn_stop():
    det = LoopDetector(token_burn_threshold=5.0, max_consecutive_failures=100)
    for i in range(3):
        det.record_failure(_failure(job=f"j{i}", code=f"E{i}", cost=2.0))
    v = det.check("j0")
    assert v.action == "stop"
    assert v.total_token_burn >= 5.0
    assert any("token burn" in r for r in v.reasons)


# ------------------------------------------------------------------
# 5. Pattern repetition warning
# ------------------------------------------------------------------

def test_pattern_repetition_warn():
    det = LoopDetector(max_consecutive_failures=100)
    det.record_failure(_failure(job="deploy", code="ERR_DISK"))
    det.record_failure(_failure(job="deploy", code="ERR_DISK"))
    v = det.check("deploy")
    assert v.action == "warn"
    assert any("likely same root cause" in r for r in v.reasons)


# ------------------------------------------------------------------
# 6. Clean state returns proceed
# ------------------------------------------------------------------

def test_clean_state_proceed():
    det = LoopDetector()
    v = det.check("anything")
    assert v.action == "proceed"
    assert v.reasons == ()
    assert v.total_failures_in_window == 0
    assert v.total_token_burn == 0.0
    assert v.consecutive_failures == 0


# ------------------------------------------------------------------
# 7. Window expiry — old failures don't count
# ------------------------------------------------------------------

def test_window_expiry():
    det = LoopDetector(max_consecutive_failures=3, window_minutes=60)
    # 3 failures, but all older than the window
    for i in range(3):
        det.record_failure(_failure(job="stale", minutes_ago=120))
    v = det.check("stale")
    # Consecutive counter is still tracked (not windowed), so that fires.
    # But windowed metrics (token burn, failure count) should be zero.
    assert v.total_failures_in_window == 0
    assert v.total_token_burn == 0.0


# ------------------------------------------------------------------
# 8. Multiple simultaneous issues compound
# ------------------------------------------------------------------

def test_compound_issues():
    det = LoopDetector(max_consecutive_failures=3, token_burn_threshold=5.0)
    # 3 consecutive + high token burn + repeated code + pattern pair
    for i in range(3):
        det.record_failure(_failure(job="hot", code="ERR_LOOP", cost=3.0, attempt=i + 1))
    v = det.check("hot")
    assert v.action == "stop"
    # Should have reasons from multiple strategies
    assert len(v.reasons) >= 2
    assert v.total_token_burn >= 9.0
    assert v.consecutive_failures == 3
