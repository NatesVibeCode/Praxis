from __future__ import annotations

import ast
from pathlib import Path

import pytest

import runtime.native_authority as native_authority


_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
_AUTHORITY_SEAMS = {
    "runtime/workflow/orchestrator.py": "default_native_authority_refs",
    "runtime/workflow/runtime_setup.py": "default_native_authority_refs",
    "runtime/spec_compiler.py": "default_native_authority_refs",
    "runtime/workflow_builder.py": "default_native_authority_refs",
    "runtime/model_executor.py": "default_native_runtime_profile_ref_required",
    "surfaces/api/rest.py": "default_native_authority_refs",
}


def _called_function_names(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    names: set[str] = set()

    def _dotted_name(node: ast.AST) -> str | None:
        if isinstance(node, ast.Name):
            return node.id
        if isinstance(node, ast.Attribute):
            base = _dotted_name(node.value)
            return f"{base}.{node.attr}" if base else node.attr
        return None

    for child in ast.walk(tree):
        if isinstance(child, ast.Call):
            name = _dotted_name(child.func)
            if name:
                names.add(name.rsplit(".", 1)[-1])
    return names


def _literal_praxis_project_constants(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    violations: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Constant):
            continue
        if node.value != "praxis-project":
            continue
        violations.append(f"{path}:{node.lineno}:{node.col_offset + 1}")
    return violations


def test_default_native_authority_refs_return_registry_defaults(monkeypatch) -> None:
    monkeypatch.setattr(native_authority, "default_native_workspace_ref", lambda: "workspace://alpha")
    monkeypatch.setattr(
        native_authority,
        "default_native_runtime_profile_ref",
        lambda: "runtime://alpha",
    )

    assert native_authority.default_native_authority_refs() == (
        "workspace://alpha",
        "runtime://alpha",
    )
    assert native_authority.default_native_runtime_profile_ref_required() == "runtime://alpha"


def test_default_native_authority_refs_fail_closed_when_registry_resolution_breaks(monkeypatch) -> None:
    def _raise() -> str:
        raise RuntimeError("native runtime authority unavailable")

    monkeypatch.setattr(native_authority, "default_native_workspace_ref", _raise)

    with pytest.raises(RuntimeError, match="native runtime authority unavailable"):
        native_authority.default_native_authority_refs()


def test_setup_and_factory_modules_use_shared_native_authority_without_praxis_project_fallbacks() -> None:
    for relative_path, expected_call in _AUTHORITY_SEAMS.items():
        path = _WORKFLOW_ROOT / relative_path
        call_names = _called_function_names(path)
        violations = _literal_praxis_project_constants(path)

        assert expected_call in call_names, f"{relative_path} is not using the shared native authority helper"
        assert violations == [], f"{relative_path} still fabricates praxis-project fallback defaults: {violations}"
