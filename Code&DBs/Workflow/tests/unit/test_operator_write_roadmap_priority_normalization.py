from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from surfaces.api import operator_write


def test_normalize_roadmap_priority_accepts_p0_through_p3() -> None:
    assert operator_write._normalize_roadmap_priority("p0") == "p0"
    assert operator_write._normalize_roadmap_priority("P0") == "p0"
    assert operator_write._normalize_roadmap_priority("p3") == "p3"
    assert operator_write._normalize_roadmap_priority("P3") == "p3"


def test_auto_promoted_bug_priority_tracks_severity_ladder() -> None:
    assert operator_write._auto_promoted_bug_priority("P0") == "p0"
    assert operator_write._auto_promoted_bug_priority("CRITICAL") == "p0"
    assert operator_write._auto_promoted_bug_priority("P1") == "p1"
    assert operator_write._auto_promoted_bug_priority("HIGH") == "p1"
    assert operator_write._auto_promoted_bug_priority("P2") == "p2"
    assert operator_write._auto_promoted_bug_priority("P3") == "p3"
    assert operator_write._auto_promoted_bug_priority(None) == "p2"
