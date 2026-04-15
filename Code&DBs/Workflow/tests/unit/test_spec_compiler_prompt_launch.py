from __future__ import annotations

import sys
from pathlib import Path

_WORKFLOW_ROOT = Path(__file__).resolve().parents[2]
if str(_WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(_WORKFLOW_ROOT))

from runtime import spec_compiler


def test_compile_prompt_launch_spec_uses_provider_default_model_when_model_missing(monkeypatch) -> None:
    monkeypatch.setattr(
        spec_compiler,
        "default_model_for_provider",
        lambda provider_slug: "gpt-4.1" if provider_slug == "openai" else None,
    )

    spec = spec_compiler.compile_prompt_launch_spec(
        prompt="Reply with exactly: OPENAI_DEFAULT_WORKFLOW_OK",
        provider_slug="openai",
        model_slug=None,
    )

    assert spec.jobs[0]["agent"] == "openai/gpt-4.1"
