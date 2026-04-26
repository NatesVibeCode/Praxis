"""Regressions for operator docs and queue JSON that must not reintroduce retired loopback DSNs."""

from __future__ import annotations

from pathlib import Path


def _praxis_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _forbidden_dsn_marks() -> tuple[str, ...]:
    return ("localhost:5432", "postgresql://localhost")


def test_operator_markdown_avoids_retired_localhost_dsns() -> None:
    """Hygiene: live handoff / artifact docs use WORKFLOW_DATABASE_URL, not hardcoded psql authority."""
    root = _praxis_root()
    scan_roots = [
        root / "Code&DBs" / "Workflow" / "artifacts" / "workflow",
        root / "artifacts" / "workflow",
        root / "planning",
    ]
    files: list[Path] = []
    for base in scan_roots:
        if base.is_dir():
            files.extend(base.rglob("*.md"))
    assert files, f"expected markdown under {scan_roots}"
    rel = root
    failures: list[str] = []
    for path in sorted({p.resolve() for p in files}):
        text = path.read_text(encoding="utf-8")
        for mark in _forbidden_dsn_marks():
            if mark in text:
                failures.append(f"{path.relative_to(rel)}: contains {mark!r}")
    assert not failures, "\n".join(failures)


def test_cascade_and_workflow_artifact_json_avoids_retired_localhost_dsns() -> None:
    """Queue specs and handoff JSON must not embed copy-pastable loopback authority."""
    root = _praxis_root()
    scan_roots = [
        root / "config" / "cascade" / "specs",
        root / "artifacts" / "workflow",
        root / "Code&DBs" / "Workflow" / "artifacts" / "workflow",
    ]
    files: list[Path] = []
    for base in scan_roots:
        if base.is_dir():
            files.extend(base.rglob("*.json"))
    assert files, f"expected json under {scan_roots}"
    rel = root
    failures: list[str] = []
    for path in sorted({p.resolve() for p in files}):
        text = path.read_text(encoding="utf-8")
        for mark in _forbidden_dsn_marks():
            if mark in text:
                failures.append(f"{path.relative_to(rel)}: contains {mark!r}")
    assert not failures, "\n".join(failures)
