from __future__ import annotations

from runtime.workflow.capability_preflight import (
    PreflightResult,
    any_blocking,
    preflight_spec_jobs,
    preflight_verify_command,
)


def _fake_runner(rc: int, stdout: str = "", stderr: str = "", timed_out: bool = False):
    def runner(verify_cmd: str, worker_image: str, timeout_s: float):
        return (rc, stdout, stderr, timed_out)

    return runner


def test_empty_command_is_ok_without_calling_runner():
    called: list[str] = []

    def runner(*args, **kwargs):
        called.append("called")
        return (0, "", "", False)

    result = preflight_verify_command("   ", runner=runner)

    assert result.verdict == "ok"
    assert result.exit_code is None
    assert called == []


def test_command_not_found_is_rejected():
    result = preflight_verify_command(
        "/opt/homebrew/bin/python3 --version",
        runner=_fake_runner(rc=127, stderr="bash: /opt/homebrew/bin/python3: No such file or directory"),
    )

    assert result.verdict == "reject"
    assert result.is_blocking()
    assert "127" in result.reason or "no such file" in result.reason.lower()


def test_module_not_found_is_rejected():
    result = preflight_verify_command(
        "python3 -c 'import fastapi'",
        runner=_fake_runner(
            rc=1,
            stderr="ModuleNotFoundError: No module named 'fastapi'",
        ),
    )

    assert result.verdict == "reject"
    assert "modulenotfounderror" in result.reason.lower()


def test_host_path_cd_failure_is_rejected():
    # The real failure mode that triggered this primitive: `cd /Users/nate/Praxis`
    # inside a worker where only /workspace exists.
    result = preflight_verify_command(
        "cd /Users/nate/Praxis && echo ok",
        runner=_fake_runner(
            rc=1,
            stderr="bash: line 1: cd: /Users/nate/Praxis: No such file or directory",
        ),
    )

    assert result.verdict == "reject"


def test_vacuous_gate_is_warned():
    result = preflight_verify_command(
        "true",
        runner=_fake_runner(rc=0),
    )

    assert result.verdict == "warn"
    assert not result.is_blocking()


def test_normal_pre_artifact_failure_is_ok():
    result = preflight_verify_command(
        "test -f artifacts/output.json",
        runner=_fake_runner(rc=1, stderr=""),
    )

    assert result.verdict == "ok"
    assert not result.is_blocking()


def test_timeout_is_rejected():
    result = preflight_verify_command(
        "sleep 60",
        runner=_fake_runner(rc=-1, timed_out=True),
        timeout_s=1.0,
    )

    assert result.verdict == "reject"
    assert result.timed_out


def test_docker_daemon_failure_is_warned_not_rejected():
    # rc 125 = docker daemon couldn't start the container. Treat as "cannot
    # verify" — we should not block the spec on a host-infra issue.
    result = preflight_verify_command(
        "true",
        runner=_fake_runner(
            rc=125,
            stderr="docker: Error response from daemon: ...",
        ),
    )

    assert result.verdict == "warn"
    assert not result.is_blocking()
    assert not result.docker_available


def test_preflight_spec_jobs_reports_per_label():
    jobs = [
        {"label": "builds_module", "verify_command": "python3 -c 'import fastapi'"},
        {"label": "writes_file", "verify_command": "test -f output.txt"},
        {"label": "no_gate"},  # no verify_command — should be skipped
    ]

    def runner(verify_cmd: str, worker_image: str, timeout_s: float):
        if "fastapi" in verify_cmd:
            return (1, "", "ModuleNotFoundError: No module named 'fastapi'", False)
        return (1, "", "", False)

    reports = preflight_spec_jobs(jobs, runner=runner)

    assert [r.label for r in reports] == ["builds_module", "writes_file"]
    assert reports[0].result.verdict == "reject"
    assert reports[1].result.verdict == "ok"
    assert any_blocking(reports) is True


def test_preflight_spec_jobs_no_blocking_when_all_pass():
    jobs = [
        {"label": "a", "verify_command": "test -f x"},
        {"label": "b", "verify_command": "test -f y"},
    ]

    reports = preflight_spec_jobs(jobs, runner=_fake_runner(rc=1))
    assert any_blocking(reports) is False
    assert all(r.result.verdict == "ok" for r in reports)
