"""Regression wrapper for Moon node detail frontend verifier warnings."""

from __future__ import annotations

from functools import lru_cache
import subprocess
from pathlib import Path


@lru_cache(maxsize=1)
def _run_moon_node_detail_vitest() -> subprocess.CompletedProcess[str]:
    app_root = Path(__file__).resolve().parents[2] / "surfaces" / "app"
    return subprocess.run(
        [
            "npm",
            "-C",
            str(app_root),
            "test",
            "--",
            "--run",
            "src/moon/MoonNodeDetail.test.tsx",
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def _combined_output(result: subprocess.CompletedProcess[str]) -> str:
    return f"{result.stdout}\n{result.stderr}"


def test_vitest_workers_have_localstorage_file_backing() -> None:
    result = _run_moon_node_detail_vitest()
    output = _combined_output(result)

    assert result.returncode == 0, output
    assert "`--localstorage-file` was provided without a valid path" not in output


def test_moon_node_detail_interactions_do_not_emit_act_warnings() -> None:
    result = _run_moon_node_detail_vitest()
    output = _combined_output(result)

    assert result.returncode == 0, output
    assert "not wrapped in act(...)" not in output
