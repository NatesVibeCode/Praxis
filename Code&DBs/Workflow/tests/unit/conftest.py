from __future__ import annotations

import importlib
import sys
from pathlib import Path

import pytest

WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))


_UNIT_WORKSPACE_REF = "workspace.unit"
_UNIT_RUNTIME_PROFILE_REF = "runtime_profile.unit"
_NATIVE_AUTHORITY_IMPORTERS = (
    "runtime.workflow.orchestrator",
    "runtime.workflow.runtime_setup",
    "runtime.spec_compiler",
    "runtime.workflow_builder",
    "runtime.workflow_graph_compiler",
    "runtime.workflow._admission",
    "runtime.workflow.receipt_writer",
    "runtime.workflow._shared",
    "runtime.model_executor",
    "surfaces.api.rest",
)
_NATIVE_AUTHORITY_STRICT_TESTS = {
    "test_native_authority.py",
    "test_native_runtime_profile_sync.py",
}


@pytest.fixture(autouse=True)
def _patch_imported_native_authority_defaults(
    monkeypatch: pytest.MonkeyPatch,
    request: pytest.FixtureRequest,
) -> None:
    def _default_native_authority_refs(conn=None) -> tuple[str, str]:
        return (_UNIT_WORKSPACE_REF, _UNIT_RUNTIME_PROFILE_REF)

    def _default_native_runtime_profile_ref_required(conn=None) -> str:
        return _UNIT_RUNTIME_PROFILE_REF

    if Path(str(request.node.fspath)).name not in _NATIVE_AUTHORITY_STRICT_TESTS:
        native_runtime_profile_sync = importlib.import_module("registry.native_runtime_profile_sync")
        monkeypatch.setattr(
            native_runtime_profile_sync,
            "default_native_workspace_ref",
            lambda conn=None: _UNIT_WORKSPACE_REF,
        )
        monkeypatch.setattr(
            native_runtime_profile_sync,
            "default_native_runtime_profile_ref",
            lambda conn=None: _UNIT_RUNTIME_PROFILE_REF,
        )

    for module_name in _NATIVE_AUTHORITY_IMPORTERS:
        module = importlib.import_module(module_name)
        monkeypatch.setattr(
            module,
            "default_native_authority_refs",
            _default_native_authority_refs,
            raising=False,
        )
        monkeypatch.setattr(
            module,
            "default_native_runtime_profile_ref_required",
            _default_native_runtime_profile_ref_required,
            raising=False,
        )
