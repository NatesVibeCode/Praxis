from __future__ import annotations

from pathlib import Path

import pytest

from runtime.scope_resolver import ScopeResolutionError, resolve_scope


def _write(path: Path, content: str = "") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_resolve_scope_fails_on_unknown_file_ref(tmp_path: Path) -> None:
    _write(tmp_path / "pkg" / "known.py", "VALUE = 1\n")

    with pytest.raises(ScopeResolutionError) as exc_info:
        resolve_scope(["pkg/missing.py"], root_dir=str(tmp_path))

    assert exc_info.value.reason_code == "scope.file_ref_unresolved"
    assert exc_info.value.file_path == "pkg/missing.py"


def test_resolve_scope_fails_on_ambiguous_bare_ref(tmp_path: Path) -> None:
    _write(tmp_path / "alpha" / "service.py", "VALUE = 1\n")
    _write(tmp_path / "beta" / "service.py", "VALUE = 2\n")

    with pytest.raises(ScopeResolutionError) as exc_info:
        resolve_scope(["service"], root_dir=str(tmp_path))

    assert exc_info.value.reason_code == "scope.file_ref_ambiguous"
    assert exc_info.value.matches == ("alpha/service.py", "beta/service.py")


def test_resolve_scope_accepts_exact_relative_ref(tmp_path: Path) -> None:
    _write(tmp_path / "support.py", "def helper():\n    return 1\n")
    _write(tmp_path / "main.py", "import support\n\nVALUE = support.helper()\n")

    resolution = resolve_scope(["main.py"], root_dir=str(tmp_path))

    assert resolution.write_scope == ["main.py"]
    assert resolution.computed_read_scope == ["support.py"]
