"""Dry-run a spec's verify_command inside the worker image to catch gates
that reference capabilities the worker does not have (missing binaries,
missing Python packages, host-only paths) before the workflow runs.

Classification mirrors _execution_core._run_verify_gate so that anything
this module accepts would also be accepted by the live runtime.
"""

from __future__ import annotations

import dataclasses
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Callable, Literal

Verdict = Literal["ok", "reject", "warn"]

_DEFAULT_WORKER_IMAGE = "praxis-worker:latest"
_DEFAULT_TIMEOUT_S = 30.0

_CAPABILITY_SIGNALS = (
    "modulenotfounderror",
    "no module named",
    "importerror",
    "command not found",
    "no such file or directory",
    "permission denied",
)


@dataclasses.dataclass(frozen=True)
class PreflightResult:
    verdict: Verdict
    reason: str
    exit_code: int | None
    stdout_tail: str
    stderr_tail: str
    timed_out: bool
    docker_available: bool

    def is_blocking(self) -> bool:
        return self.verdict == "reject"


_RunnerResult = tuple[int, str, str, bool]  # (rc, stdout, stderr, timed_out)
Runner = Callable[[str, str, float], _RunnerResult]


def _default_docker_runner(verify_cmd: str, worker_image: str, timeout_s: float) -> _RunnerResult:
    with tempfile.TemporaryDirectory(prefix="praxis-preflight-") as scratch:
        args = [
            "docker", "run", "--rm",
            "-v", f"{scratch}:/workspace",
            "-w", "/workspace",
            worker_image,
            "bash", "-c", verify_cmd,
        ]
        try:
            proc = subprocess.run(
                args,
                capture_output=True,
                text=True,
                timeout=timeout_s,
            )
        except subprocess.TimeoutExpired as exc:
            out = exc.stdout or b""
            err = exc.stderr or b""
            if isinstance(out, (bytes, bytearray)):
                out = out.decode("utf-8", "replace")
            if isinstance(err, (bytes, bytearray)):
                err = err.decode("utf-8", "replace")
            return (-1, str(out or ""), str(err or ""), True)
    return (proc.returncode, proc.stdout or "", proc.stderr or "", False)


def preflight_verify_command(
    verify_cmd: str,
    *,
    worker_image: str = _DEFAULT_WORKER_IMAGE,
    timeout_s: float = _DEFAULT_TIMEOUT_S,
    runner: Runner | None = None,
) -> PreflightResult:
    """Classify a verify_command against the worker image.

    Rules:
    - empty cmd                              -> ok    (nothing to check)
    - docker unavailable                     -> warn  (cannot verify; do not block)
    - rc 126/127, or stderr matches a
      capability signal                      -> reject (gate is unreachable)
    - rc 0 against empty scratch             -> warn  (gate may be vacuous)
    - any other non-zero rc                  -> ok    (pre-artifact failure, normal)
    """

    verify_cmd = str(verify_cmd or "").strip()
    if not verify_cmd:
        return PreflightResult(
            verdict="ok",
            reason="no verify_command to check",
            exit_code=None,
            stdout_tail="",
            stderr_tail="",
            timed_out=False,
            docker_available=True,
        )

    if runner is None:
        if not shutil.which("docker"):
            return PreflightResult(
                verdict="warn",
                reason="docker not on PATH; preflight skipped",
                exit_code=None,
                stdout_tail="",
                stderr_tail="",
                timed_out=False,
                docker_available=False,
            )
        runner = _default_docker_runner

    rc, stdout, stderr, timed_out = runner(verify_cmd, worker_image, timeout_s)
    stdout_tail = (stdout or "")[-500:]
    stderr_tail = (stderr or "")[-500:]

    if timed_out:
        return PreflightResult(
            verdict="reject",
            reason=f"verify_command hung for {timeout_s:.0f}s inside worker image",
            exit_code=None,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            timed_out=True,
            docker_available=True,
        )

    # docker exits 125 when the daemon cannot start the container (image
    # missing, daemon down, etc). Treat as "cannot verify" rather than
    # "gate broken" — spec might be fine; our preflight just could not run.
    if rc == 125:
        return PreflightResult(
            verdict="warn",
            reason="docker could not start worker image; preflight skipped",
            exit_code=rc,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            timed_out=False,
            docker_available=False,
        )

    if rc in (126, 127):
        return PreflightResult(
            verdict="reject",
            reason=f"verify_command exits {rc} inside worker (command not found / not executable)",
            exit_code=rc,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            timed_out=False,
            docker_available=True,
        )

    stderr_lc = (stderr or "").lower()
    stdout_lc = (stdout or "").lower()
    combined_lc = stderr_lc + "\n" + stdout_lc
    matched_signal = next(
        (signal for signal in _CAPABILITY_SIGNALS if signal in combined_lc),
        None,
    )
    if matched_signal is not None:
        return PreflightResult(
            verdict="reject",
            reason=(
                f"verify_command output matches capability signal {matched_signal!r} "
                "(missing module, missing binary, or host-only path)"
            ),
            exit_code=rc,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            timed_out=False,
            docker_available=True,
        )

    if rc == 0:
        return PreflightResult(
            verdict="warn",
            reason=(
                "verify_command exits 0 against an empty workspace; the gate may be "
                "vacuously true — consider asserting something artifact-specific"
            ),
            exit_code=rc,
            stdout_tail=stdout_tail,
            stderr_tail=stderr_tail,
            timed_out=False,
            docker_available=True,
        )

    return PreflightResult(
        verdict="ok",
        reason=(
            f"verify_command ran inside worker image (rc={rc}); failure is consistent "
            "with pre-artifact state rather than a capability gap"
        ),
        exit_code=rc,
        stdout_tail=stdout_tail,
        stderr_tail=stderr_tail,
        timed_out=False,
        docker_available=True,
    )


@dataclasses.dataclass(frozen=True)
class JobPreflightReport:
    label: str
    verify_command: str
    result: PreflightResult


def preflight_spec_jobs(
    jobs: list[dict],
    *,
    worker_image: str = _DEFAULT_WORKER_IMAGE,
    timeout_s: float = _DEFAULT_TIMEOUT_S,
    runner: Runner | None = None,
) -> list[JobPreflightReport]:
    reports: list[JobPreflightReport] = []
    for job in jobs:
        label = str(job.get("label") or job.get("id") or "<unlabeled>")
        verify_cmd = str(job.get("verify_command") or "").strip()
        if not verify_cmd:
            continue
        result = preflight_verify_command(
            verify_cmd,
            worker_image=worker_image,
            timeout_s=timeout_s,
            runner=runner,
        )
        reports.append(
            JobPreflightReport(
                label=label,
                verify_command=verify_cmd,
                result=result,
            )
        )
    return reports


def any_blocking(reports: list[JobPreflightReport]) -> bool:
    return any(r.result.is_blocking() for r in reports)
