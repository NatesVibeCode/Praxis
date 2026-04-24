"""Regression guard: scripts/bootstrap must delegate prereq checks to probes.

Packet 4 moved per-check wording (psql missing, pgvector missing, etc.) out
of ``scripts/bootstrap`` and into the onboarding gate-probe graph so the
error message lives in one place. This test fails if inline prereq checks
reappear in the shell script without a corresponding probe-delegation.
"""

from __future__ import annotations

from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[4]
_BOOTSTRAP = _REPO_ROOT / "scripts" / "bootstrap"


def _read_bootstrap() -> str:
    return _BOOTSTRAP.read_text(encoding="utf-8")


def test_bootstrap_declares_require_gate_helper() -> None:
    body = _read_bootstrap()
    assert "require_gate()" in body, (
        "scripts/bootstrap must declare a require_gate() helper that delegates "
        "prereq checks to runtime.onboarding.bootstrap_cli"
    )
    assert "runtime.onboarding.bootstrap_cli" in body


def test_bootstrap_delegates_psql_check_to_probe() -> None:
    body = _read_bootstrap()
    assert "require_gate platform.psql" in body, (
        "psql-on-PATH check must delegate to platform.psql probe"
    )
    # Inline psql wording must be gone.
    assert "die \"psql not on PATH" not in body


def test_bootstrap_delegates_pgvector_availability_to_probe() -> None:
    body = _read_bootstrap()
    assert "require_gate platform.pgvector" in body, (
        "pgvector availability check must delegate to platform.pgvector probe"
    )
    # Inline pgvector wording must be gone (the CREATE EXTENSION fallback
    # now references 'pgvector extension is available but CREATE...'
    # rather than duplicating install instructions).
    assert "pgvector is not installed" not in body
    assert "brew install pgvector/brew/pgvector" not in body


def test_bootstrap_keeps_python314_check_inline() -> None:
    """python3.14 is a precondition for running the probe CLI itself, so it
    cannot be delegated. This test anchors that intentional exception."""
    body = _read_bootstrap()
    assert "command -v python3.14" in body, (
        "python3.14 precondition check must stay inline; probe CLI needs it"
    )


def test_bootstrap_die_count_does_not_regrow() -> None:
    """Soft anti-regrowth: track the total number of `die \"...\"` calls in
    scripts/bootstrap. Future packets should only reduce this count (or
    justify an increase). If this assertion bumps upward without a matching
    update to the allowlist, reviewers should ask whether the new wording
    belongs in a probe instead of a die string."""
    body = _read_bootstrap()
    die_count = body.count("die \"")
    # Current count after Packet 4 delegation. Lower is better; raising this
    # requires consensus that the new message does not belong in a probe.
    allowed_max = 15
    assert die_count <= allowed_max, (
        f"scripts/bootstrap has {die_count} `die \"...\"` calls; allowed max is "
        f"{allowed_max}. Either delegate the new check to a probe in "
        f"runtime/onboarding/ or update the allowlist in this test with a "
        f"rationale."
    )
