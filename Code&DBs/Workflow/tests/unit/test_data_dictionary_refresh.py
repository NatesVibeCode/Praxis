from __future__ import annotations

from types import SimpleNamespace
from typing import Any
import sys

from memory.data_dictionary_refresh import refresh_data_dictionary_authority


def _fake_projector_module(
    monkeypatch,
    module_name: str,
    class_name: str,
    calls: list[str],
) -> None:
    fake_module = type(sys)(module_name)

    class _FakeProjector:
        def __init__(self, conn: Any) -> None:
            calls.append(f"{class_name}.__init__")

        def run(self) -> Any:
            calls.append(f"{class_name}.run")
            return SimpleNamespace(ok=True, duration_ms=1.5, error=None, name=class_name)

    setattr(fake_module, class_name, _FakeProjector)
    monkeypatch.setitem(sys.modules, module_name, fake_module)


def test_refresh_data_dictionary_authority_runs_full_bundle(monkeypatch) -> None:
    calls: list[str] = []
    _fake_projector_module(
        monkeypatch,
        "memory.data_dictionary_projector",
        "DataDictionaryProjector",
        calls,
    )
    _fake_projector_module(
        monkeypatch,
        "memory.data_dictionary_lineage_projector",
        "DataDictionaryLineageProjector",
        calls,
    )
    _fake_projector_module(
        monkeypatch,
        "memory.data_dictionary_classifications_projector",
        "DataDictionaryClassificationsProjector",
        calls,
    )
    _fake_projector_module(
        monkeypatch,
        "memory.data_dictionary_quality_projector",
        "DataDictionaryQualityProjector",
        calls,
    )
    _fake_projector_module(
        monkeypatch,
        "memory.data_dictionary_stewardship_projector",
        "DataDictionaryStewardshipProjector",
        calls,
    )
    _fake_projector_module(
        monkeypatch,
        "memory.data_dictionary_drift_projector",
        "DataDictionaryDriftProjector",
        calls,
    )

    result = refresh_data_dictionary_authority(object())

    assert result["ok"] is True
    assert len(result["modules"]) == 6
    assert calls == [
        "DataDictionaryProjector.__init__",
        "DataDictionaryProjector.run",
        "DataDictionaryLineageProjector.__init__",
        "DataDictionaryLineageProjector.run",
        "DataDictionaryClassificationsProjector.__init__",
        "DataDictionaryClassificationsProjector.run",
        "DataDictionaryQualityProjector.__init__",
        "DataDictionaryQualityProjector.run",
        "DataDictionaryStewardshipProjector.__init__",
        "DataDictionaryStewardshipProjector.run",
        "DataDictionaryDriftProjector.__init__",
        "DataDictionaryDriftProjector.run",
    ]
