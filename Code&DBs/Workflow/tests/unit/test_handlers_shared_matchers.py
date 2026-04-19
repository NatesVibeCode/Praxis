"""Tests for the route-matcher helpers in ``surfaces/api/handlers/_shared.py``.

The dispatcher (``_route_to_handler`` in ``surfaces/api/rest.py``) now hands
matchers the URL-encoded path plus any ``?query`` suffix so downstream
handlers can:

* read query-string params (``?category=table``, ``?include_layers=1``)
  from the single ``path`` argument;
* preserve ``%2F`` inside path segments (so an ``object_kind`` like
  ``dataset:slm/review`` survives splitting).

Route matchers, however, only care about the path portion. These tests lock
in that ``_exact``, ``_prefix``, and ``_prefix_suffix`` ignore the
query-string tail so routes keep matching regardless of whether the request
carries query params.
"""
from __future__ import annotations

from surfaces.api.handlers._shared import _exact, _prefix, _prefix_suffix


# --- _exact ---------------------------------------------------------------


def test_exact_matches_bare_path() -> None:
    assert _exact("/api/data-dictionary")("/api/data-dictionary") is True


def test_exact_ignores_query_string() -> None:
    matcher = _exact("/api/data-dictionary")
    assert matcher("/api/data-dictionary?action=list&category=table") is True
    assert matcher("/api/data-dictionary?") is True


def test_exact_rejects_different_path() -> None:
    assert _exact("/api/data-dictionary")("/api/integrations") is False


def test_exact_rejects_path_with_trailing_segment() -> None:
    # The candidate has a subresource appended — not an exact match.
    matcher = _exact("/api/data-dictionary")
    assert matcher("/api/data-dictionary/table:orders") is False


# --- _prefix --------------------------------------------------------------


def test_prefix_matches_bare_path_with_subresource() -> None:
    matcher = _prefix("/api/data-dictionary/")
    assert matcher("/api/data-dictionary/table:orders") is True


def test_prefix_ignores_query_string() -> None:
    matcher = _prefix("/api/data-dictionary/")
    assert (
        matcher("/api/data-dictionary/table:orders?include_layers=1") is True
    )


def test_prefix_ignores_encoded_slash_in_segment() -> None:
    """Object kinds that encode '/' as '%2F' still satisfy the prefix match."""
    matcher = _prefix("/api/data-dictionary/")
    assert matcher("/api/data-dictionary/dataset%3Aslm%2Freview") is True


def test_prefix_rejects_unrelated_path() -> None:
    assert _prefix("/api/data-dictionary/")("/api/integrations/foo") is False


# --- _prefix_suffix -------------------------------------------------------


def test_prefix_suffix_checks_both_ends_on_path_only() -> None:
    matcher = _prefix_suffix("/api/workflow/", "/validate")
    # The suffix check must run on the path portion — the query tail must
    # not "move" the suffix off the end of the path.
    assert matcher("/api/workflow/abc/validate") is True
    assert matcher("/api/workflow/abc/validate?force=1") is True


def test_prefix_suffix_rejects_wrong_suffix() -> None:
    matcher = _prefix_suffix("/api/workflow/", "/validate")
    assert matcher("/api/workflow/abc/other") is False
