"""Regression pin for BUG-CBC73AB3 / cluster key ``failure-code-authority-split``.

Before the fix, two separate modules each defined their own
``_worker_error_code`` helper with identical attribute-probe order
(``failure_code``, ``reason_code``, ``error_code``, ``code``) followed by a
``normalize_failure_code(..., str(exc))`` pass:

* ``runtime/workflow/worker.py`` (card-execution path)
* ``runtime/workflow/_worker_loop.py`` (graph/job worker path)

Two independent authorities for the same decision meant either could drift
without the other noticing — directly the "authority split" the bug names.
The fix introduces ``runtime.self_healing.derive_terminal_reason_code`` as the
single authority and makes each worker module alias its local symbol to that
canonical function. These pins cover:

1. The canonical helper exists and is exported from ``runtime.self_healing``.
2. Both worker-layer ``_worker_error_code`` handles are literally the same
   function object — impossible to drift apart.
3. A stable typed ``reason_code`` on the exception passes through unchanged
   (not silently rewritten by the stderr-rescue regexes).
4. Attribute probe order: ``reason_code`` wins over ``failure_code``.
5. An empty fallback fails closed with ``ValueError`` rather than returning
   the silent ``"unknown"`` sentinel.
6. A generic-wrapper fallback (``worker_exception``) whose exception text
   matches the "failure_code … non-empty string" orchestration pattern IS
   upgraded — preserving the existing stderr-inference rescue behavior so
   this refactor is behavior-preserving, not a silent regression of
   BUG-1FD463F7's sibling-diagnostic path.
"""
from __future__ import annotations

import pytest

from runtime import self_healing
from runtime.workflow import _worker_loop, worker


def test_canonical_authority_is_exported() -> None:
    """The single authority must live on the self_healing module surface."""
    assert hasattr(self_healing, "derive_terminal_reason_code")
    assert "derive_terminal_reason_code" in self_healing.__all__


def test_both_worker_layers_alias_the_same_callable() -> None:
    """Impossible-to-drift guarantee: aliased via identity, not re-implemented.

    If a future refactor re-splits this into two local helpers that happen to
    share behavior today, identity equality drops and this pin fires.
    """
    assert worker._worker_error_code is self_healing.derive_terminal_reason_code
    assert _worker_loop._worker_error_code is self_healing.derive_terminal_reason_code


def test_stable_reason_code_is_not_silently_rewritten() -> None:
    """A typed error with a non-generic ``reason_code`` must survive intact.

    The pre-fix risk was that self-healing's stderr regexes could rewrite a
    perfectly valid upstream reason code. The canonical helper only runs the
    rescue when the code is empty or in the generic-wrapper set, so anything
    operator-meaningful like ``provider.alias_conflict`` stays verbatim.
    """

    class StableErr(RuntimeError):
        reason_code = "provider.alias_conflict"

    got = self_healing.derive_terminal_reason_code(
        StableErr("alias 'shared-a' claimed by acme and contoso"),
        fallback="worker_exception",
    )
    assert got == "provider.alias_conflict"


def test_reason_code_attribute_wins_over_failure_code() -> None:
    """Probe order: ``reason_code`` beats the legacy ``failure_code`` attr.

    Typed errors in the new contract put the stable code on ``reason_code``;
    older adapters still carry ``failure_code``. When both are present, the
    newer attribute must win so operators migrating an adapter don't see
    ghosts of the old code.
    """

    class TwoAttrs(RuntimeError):
        reason_code = "route.all_candidates_blocked"
        failure_code = "legacy.worker_exception"

    assert (
        self_healing.derive_terminal_reason_code(TwoAttrs("blocked"), fallback="worker_exception")
        == "route.all_candidates_blocked"
    )


def test_fallback_must_be_non_empty() -> None:
    """Empty/whitespace fallbacks must raise — no silent ``"unknown"`` sentinel.

    The old helpers accepted any fallback and would cascade to ``"unknown"``
    via ``normalize_failure_code``, so a caller who mistyped a fallback got a
    useless terminal code with no crash. The new contract makes that a
    programming error at the seam where it's introduced.
    """
    with pytest.raises(ValueError):
        self_healing.derive_terminal_reason_code(RuntimeError("boom"), fallback="")
    with pytest.raises(ValueError):
        self_healing.derive_terminal_reason_code(RuntimeError("boom"), fallback="   ")


def test_generic_wrapper_fallback_still_upgraded_via_stderr_rescue() -> None:
    """Behavior-preservation: when the exception text names the orchestration
    envelope failure and the caller only has a wrapper fallback like
    ``worker_exception``, the stderr-rescue still kicks in — exactly as the
    old per-module helpers did. The fix is about *where* this logic lives
    (one place, not two), not about deleting the rescue.
    """
    exc = RuntimeError("failure_code must be a non-empty string")
    got = self_healing.derive_terminal_reason_code(exc, fallback="worker_exception")
    assert got == "orchestration.failure_code_missing"


def test_no_typed_attrs_falls_back_to_caller_supplied_code() -> None:
    """Bare exceptions fall through to the caller's fallback path."""
    got = self_healing.derive_terminal_reason_code(
        ValueError("some runtime problem"),
        fallback="worker_future_exception",
    )
    # No orchestration-envelope signal in the message, so the wrapper falls
    # through normalize_failure_code's passthrough branch.
    assert got == "worker_future_exception"
