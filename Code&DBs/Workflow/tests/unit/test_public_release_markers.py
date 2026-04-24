from __future__ import annotations

from pathlib import Path


_REPO_ROOT = Path(__file__).resolve().parents[4]
_MARKER = "PUBLIC_RELEASE_" + "REMOVE"
_SOURCE_ROOT = _REPO_ROOT / "Code&DBs" / "Workflow"
_ALLOWED_MARKERS = {
    "adapters/credentials.py": (
        "Nate-private Anthropic direct API block; public builds must use "
        "registry/profile authority for ANTHROPIC_API_KEY users."
    ),
    "adapters/provider_transport.py": (
        "Nate-private CLI-only Anthropic profile; public builds must restore "
        "direct API admission through provider profiles."
    ),
    "runtime/onboarding/probes_provider.py": (
        "Nate-private Anthropic onboarding probe; public builds must probe "
        "direct API credentials when that profile is admitted."
    ),
}


def _marker_files() -> set[str]:
    matches: set[str] = set()
    for path in _SOURCE_ROOT.rglob("*.py"):
        relative = path.relative_to(_SOURCE_ROOT)
        if relative.parts and relative.parts[0] == "tests":
            continue
        text = path.read_text(encoding="utf-8")
        if _MARKER in text:
            matches.add(relative.as_posix())
    return matches


def test_public_release_remove_markers_are_registered() -> None:
    assert _marker_files() == set(_ALLOWED_MARKERS)
    assert all(reason.strip() for reason in _ALLOWED_MARKERS.values())
