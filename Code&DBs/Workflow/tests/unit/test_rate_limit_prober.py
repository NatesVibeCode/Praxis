from __future__ import annotations

from runtime import rate_limit_prober
from runtime.rate_limit_prober import ProbeResult, RateLimitProbeModule


def test_rate_limit_probe_module_treats_successful_probes_as_ok(monkeypatch) -> None:
    monkeypatch.setattr(
        rate_limit_prober,
        "probe_all",
        lambda: [
            ProbeResult("google", "gemini-2.5-flash", "ok", "probe succeeded", 12),
            ProbeResult("openai", "gpt-5.4-mini", "ok", "probe succeeded", 15),
        ],
    )

    result = RateLimitProbeModule().run()

    assert result.ok is True
    assert result.error == "google/gemini-2.5-flash: ok (12ms); openai/gpt-5.4-mini: ok (15ms)"


def test_rate_limit_probe_module_reports_only_probe_issues(monkeypatch) -> None:
    monkeypatch.setattr(
        rate_limit_prober,
        "probe_all",
        lambda: [
            ProbeResult("google", "gemini-2.5-flash", "ok", "probe succeeded", 12),
            ProbeResult("anthropic", "claude-sonnet-4-6", "error", "exit 1", 30),
        ],
    )

    result = RateLimitProbeModule().run()

    assert result.ok is False
    assert result.error == "anthropic/claude-sonnet-4-6: error — exit 1"
