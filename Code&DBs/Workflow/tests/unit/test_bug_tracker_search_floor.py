"""Verifier test for BUG-9475EEB0 — BugTracker.search must not return
unrelated bugs when the query matches nothing.

Touches the live `_subs.get_bug_tracker()` because the bug is in the
SQL/vector branch wiring; a stub wouldn't catch the regression.
"""
from __future__ import annotations

import pytest


def _live_bug_tracker():
    try:
        from surfaces.mcp.subsystems import _subs
        return _subs.get_bug_tracker()
    except Exception as exc:
        pytest.skip(f"live bug tracker unavailable: {exc}")


def test_garbage_query_returns_no_hits():
    bt = _live_bug_tracker()
    hits = bt.search("zzzqxyz qwertyflux mxbtt", limit=5)
    assert hits == [], (
        "Garbage query should return zero hits; vector branch must apply "
        "min_similarity floor instead of returning random recent bugs"
    )


def test_alternate_garbage_query_returns_no_hits():
    bt = _live_bug_tracker()
    hits = bt.search("flibbertigibbet xyzzy asdfgh", limit=5)
    assert hits == []


def test_real_query_still_returns_hits():
    bt = _live_bug_tracker()
    hits = bt.search("provider routing", limit=5)
    assert len(hits) > 0, "Real query must still surface relevant bugs"
    # All returned bugs should mention at least one query token in title
    for b in hits:
        title = (b.title or "").lower()
        description = (b.description or "").lower()
        haystack = f"{title} {description}"
        assert "provider" in haystack or "routing" in haystack, (
            f"Bug {b.bug_id} returned for 'provider routing' but title/desc "
            f"contains neither token: {b.title}"
        )


def test_min_similarity_constant_present():
    """Belt-and-suspenders: confirm the floor exists in source.

    Cheap regression guard — if someone later sets min_similarity=None
    again the unit suite catches it without needing a live DB.
    """
    from runtime import bug_tracker

    text = open(bug_tracker.__file__, encoding="utf-8").read()
    assert "min_similarity=0.3" in text, (
        "BugTracker.search vector branch must keep its min_similarity floor "
        "(BUG-9475EEB0)"
    )
    assert "min_similarity=None" not in text, (
        "BugTracker.search vector branch must not regress to min_similarity=None"
    )
