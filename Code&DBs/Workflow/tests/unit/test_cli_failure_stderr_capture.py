from __future__ import annotations

import importlib
from io import StringIO


def test_failed_cli_command_records_stderr_friction(monkeypatch) -> None:
    workflow_main_module = importlib.import_module("surfaces.cli.main")
    captured: dict[str, object] = {}

    def _fake_record_cli_command_failure(**kwargs: object) -> bool:
        captured.update(kwargs)
        return True

    def _fake_main_impl(*_args: object, **_kwargs: object) -> int:
        import sys

        sys.stderr.write('{"reason_code":"stderr.only","message":"failed on stderr"}\n')
        return 2

    monkeypatch.setattr(
        workflow_main_module,
        "record_cli_command_failure",
        _fake_record_cli_command_failure,
    )
    monkeypatch.setattr(workflow_main_module, "_main_impl", _fake_main_impl)

    stdout = StringIO()
    stderr = StringIO()
    assert workflow_main_module.main(["probe"], stdout=stdout, stderr=stderr) == 2

    assert captured["args"] == ["probe"]
    assert captured["exit_code"] == 2
    assert "stderr.only" in str(captured["output_text"])
    assert "failed on stderr" in str(captured["output_text"])
    assert captured["output_truncated"] is False

