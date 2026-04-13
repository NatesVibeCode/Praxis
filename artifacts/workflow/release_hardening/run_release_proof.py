#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from shlex import quote
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
PROOF_ROOT = REPO_ROOT / "artifacts" / "workflow" / "release_hardening"
EVIDENCE_ROOT = PROOF_ROOT / "evidence"
LATEST_EVIDENCE_DIR = EVIDENCE_ROOT / "latest"
QUEUE_RELATIVE_PATH = Path("artifacts/workflow/release_hardening/release_proof_and_public_naming.queue.json")
QUEUE_PATH = REPO_ROOT / QUEUE_RELATIVE_PATH
NATIVE_CONTRACT_PATH = REPO_ROOT / "config" / "DAG_NATIVE_INSTANCE_ENV.contract"
DEFAULT_DB_URL = "postgresql://nate@127.0.0.1:5432/dag_workflow"
DEFAULT_PATH_PREFIX = "/opt/homebrew/bin:/opt/homebrew/Cellar/node/25.6.1/bin"
PYTHON_BIN = "/opt/homebrew/bin/python3"
RELEASE_TEST_FILES = (
    "Code&DBs/Workflow/tests/integration/test_repo_local_primary_operator_entrypoint.py",
    "Code&DBs/Workflow/tests/integration/test_bounded_native_primary_proof.py",
    "Code&DBs/Workflow/tests/integration/test_native_default_parallel_proof.py",
    "Code&DBs/Workflow/tests/integration/test_native_frontdoor.py",
    "Code&DBs/Workflow/tests/integration/test_native_operator_surface_consolidation.py",
    "Code&DBs/Workflow/tests/integration/test_native_cutover_scoreboard.py",
    "Code&DBs/Workflow/tests/unit/test_dependency_truth_surfaces.py",
    "Code&DBs/Workflow/tests/unit/test_control_commands.py",
)


@dataclass(frozen=True, slots=True)
class ProofStep:
    step_id: str
    label: str
    argv: tuple[str, ...]
    claim: str
    required: bool = True
    expected_json: bool = False
    use_native_contract_env: bool = False


def _timestamp_slug() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _command_text(argv: tuple[str, ...]) -> str:
    return " ".join(quote(part) for part in argv)


def _base_env() -> dict[str, str]:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        str(WORKFLOW_ROOT)
        if not existing_pythonpath
        else f"{WORKFLOW_ROOT}{os.pathsep}{existing_pythonpath}"
    )
    env.setdefault("WORKFLOW_DATABASE_URL", DEFAULT_DB_URL)
    env["PATH"] = f"{DEFAULT_PATH_PREFIX}:{env.get('PATH', '')}"
    return env


def _native_contract_env() -> dict[str, str]:
    env = _base_env()
    if not NATIVE_CONTRACT_PATH.exists():
        raise FileNotFoundError(f"native contract missing: {NATIVE_CONTRACT_PATH}")
    for raw_line in NATIVE_CONTRACT_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    env["DAG_RUNTIME_PROFILES_CONFIG"] = str(REPO_ROOT / "config" / "runtime_profiles.json")
    return env


def _proof_steps() -> list[ProofStep]:
    pytest_argv = (
        PYTHON_BIN,
        "-m",
        "pytest",
        "--noconftest",
        "-q",
        *RELEASE_TEST_FILES,
    )
    return [
        ProofStep(
            step_id="git_head",
            label="Record git commit",
            argv=("git", "rev-parse", "HEAD"),
            claim="traceability",
            required=False,
        ),
        ProofStep(
            step_id="git_status",
            label="Record git status",
            argv=("git", "status", "--short"),
            claim="traceability",
            required=False,
        ),
        ProofStep(
            step_id="dag_ctl_doctor",
            label="Read runtime health proof",
            argv=("./scripts/dag-ctl", "doctor", "--json"),
            claim="health_governance",
            expected_json=True,
        ),
        ProofStep(
            step_id="test_selftest",
            label="Run test frontdoor selftest",
            argv=("./scripts/test.sh", "selftest"),
            claim="frontdoor_contract",
            expected_json=True,
        ),
        ProofStep(
            step_id="test_suite_list",
            label="List supported test suites",
            argv=("./scripts/test.sh", "suite", "list"),
            claim="frontdoor_contract",
            expected_json=True,
        ),
        ProofStep(
            step_id="test_suite_focus",
            label="Focus workflow_first_slice suite",
            argv=("./scripts/test.sh", "suite", "focus", "workflow_first_slice"),
            claim="frontdoor_contract",
            expected_json=True,
        ),
        ProofStep(
            step_id="test_plan",
            label="Plan release proof queue coverage",
            argv=("./scripts/test.sh", "plan", str(QUEUE_RELATIVE_PATH)),
            claim="frontdoor_contract",
            expected_json=True,
        ),
        ProofStep(
            step_id="test_check_affected",
            label="Check affected suites for release proof queue",
            argv=("./scripts/test.sh", "check-affected", str(QUEUE_RELATIVE_PATH)),
            claim="frontdoor_contract",
            expected_json=True,
        ),
        ProofStep(
            step_id="test_validate",
            label="Validate release proof queue",
            argv=("./scripts/test.sh", "validate", str(QUEUE_RELATIVE_PATH)),
            claim="frontdoor_contract",
            expected_json=True,
        ),
        ProofStep(
            step_id="native_primary_instance",
            label="Read repo-local native instance contract",
            argv=("./scripts/native-primary.sh",),
            claim="native_contract",
            expected_json=True,
        ),
        ProofStep(
            step_id="release_pytest_slice",
            label="Run bounded release proof test slice",
            argv=pytest_argv,
            claim="proof_slice",
        ),
    ]


def _optional_steps(doctor_payload: dict[str, Any]) -> list[ProofStep]:
    smoke_run_id = doctor_payload.get("smoke_run_id")
    if not isinstance(smoke_run_id, str) or not smoke_run_id.strip():
        return []
    run_id = smoke_run_id.strip()
    cli_main_argv_prefix = (
        PYTHON_BIN,
        "-c",
        "from surfaces.cli.main import main; import sys; raise SystemExit(main(sys.argv[1:]))",
        "native-operator",
    )
    return [
        ProofStep(
            step_id="native_status_smoke_run",
            label="Read native status for the doctor smoke run",
            argv=(*cli_main_argv_prefix, "status", run_id),
            claim="persisted_run_truth",
            required=False,
            expected_json=True,
            use_native_contract_env=True,
        ),
        ProofStep(
            step_id="native_inspect_smoke_run",
            label="Render inspect view for the doctor smoke run",
            argv=(*cli_main_argv_prefix, "inspect", run_id),
            claim="persisted_run_truth",
            required=False,
            use_native_contract_env=True,
        ),
    ]


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _run_step(
    *,
    step: ProofStep,
    step_index: int,
    run_dir: Path,
) -> dict[str, Any]:
    prefix = f"{step_index:02d}_{step.step_id}"
    stdout_path = run_dir / f"{prefix}.stdout.{'json' if step.expected_json else 'txt'}"
    stderr_path = run_dir / f"{prefix}.stderr.txt"
    env = _native_contract_env() if step.use_native_contract_env else _base_env()

    started = time.perf_counter()
    proc = subprocess.run(
        step.argv,
        cwd=str(REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
    )
    duration_s = round(time.perf_counter() - started, 3)

    _write_text(stdout_path, proc.stdout)
    _write_text(stderr_path, proc.stderr)

    parsed_json: dict[str, Any] | list[Any] | None = None
    parse_error: str | None = None
    if step.expected_json and proc.stdout.strip():
        try:
            parsed = json.loads(proc.stdout)
        except json.JSONDecodeError as exc:
            parse_error = f"stdout is not valid JSON: {exc}"
        else:
            if isinstance(parsed, (dict, list)):
                parsed_json = parsed
            else:
                parse_error = f"stdout JSON must decode to object or list, got {type(parsed).__name__}"

    ok = proc.returncode == 0
    if step.expected_json and ok and parsed_json is None:
        ok = False

    warnings: list[str] = []
    errors: list[str] = []
    if parse_error is not None:
        errors.append(parse_error)
    if proc.returncode != 0:
        errors.append(f"command exited with code {proc.returncode}")
    if isinstance(parsed_json, dict):
        warnings.extend(str(item) for item in parsed_json.get("warnings", []) if item is not None)
        errors.extend(str(item) for item in parsed_json.get("errors", []) if item is not None)

    return {
        "step_id": step.step_id,
        "label": step.label,
        "claim": step.claim,
        "required": step.required,
        "command": _command_text(step.argv),
        "exit_code": proc.returncode,
        "ok": ok,
        "duration_s": duration_s,
        "stdout_path": str(stdout_path.relative_to(REPO_ROOT)),
        "stderr_path": str(stderr_path.relative_to(REPO_ROOT)),
        "warnings": warnings,
        "errors": errors,
        "parsed_json": parsed_json,
    }


def _claim_status(*, step_ids: list[str], step_map: dict[str, dict[str, Any]]) -> str:
    matched = [step_map[step_id] for step_id in step_ids if step_id in step_map]
    if not matched:
        return "not_proved"
    if all(step["ok"] for step in matched):
        return "proved"
    if any(step["ok"] for step in matched):
        return "partial"
    return "blocked"


def _residual_risks(
    *,
    step_map: dict[str, dict[str, Any]],
    claim_matrix: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    risks: list[dict[str, Any]] = [
        {
            "risk_id": "rr-mcp-live-roundtrip",
            "severity": "medium",
            "status": "open",
            "summary": "The packet does not execute a live MCP launch/status round trip.",
            "why_open": (
                "The replayable proof uses repo-local CLI wrappers and targeted tests. "
                "It does not exercise MCP transport in-session."
            ),
            "close_with": (
                "Add one repo-local MCP smoke step or an equivalent machine-facing front door "
                "that produces durable run and status evidence."
            ),
            "evidence_paths": [
                "artifacts/workflow/release_hardening/evidence/latest/summary.json",
            ],
        },
        {
            "risk_id": "rr-fresh-setup-not-replayed",
            "severity": "medium",
            "status": "open",
            "summary": "The packet reads `dag-ctl doctor --json`; it does not rerun `dag-ctl setup` from a clean environment.",
            "why_open": (
                "This proves the current health truth surface and targeted invariants, not first-time setup reproducibility."
            ),
            "close_with": (
                "Replay `./scripts/dag-ctl setup` in a bounded clean workspace and capture the resulting proof output."
            ),
            "evidence_paths": [
                "artifacts/workflow/release_hardening/evidence/latest/02_dag_ctl_doctor.stdout.json",
            ],
        },
    ]

    git_status = step_map.get("git_status")
    if git_status and Path(REPO_ROOT / git_status["stdout_path"]).read_text(encoding="utf-8").strip():
        risks.append(
            {
                "risk_id": "rr-dirty-worktree",
                "severity": "high",
                "status": "open",
                "summary": "The worktree is dirty during proof capture.",
                "why_open": (
                    "A green proof packet does not establish that the checked tree is a clean release candidate."
                ),
                "close_with": "Review or clear the dirty files and rerun the proof from the intended release commit.",
                "evidence_paths": [git_status["stdout_path"]],
            }
        )

    doctor_step = step_map.get("dag_ctl_doctor")
    doctor_payload = doctor_step.get("parsed_json") if doctor_step else None
    if isinstance(doctor_payload, dict):
        dependency_truth = doctor_payload.get("dependency_truth")
        if isinstance(dependency_truth, dict) and dependency_truth.get("ok") is False:
            missing = dependency_truth.get("missing")
            missing_names = []
            if isinstance(missing, list):
                for item in missing:
                    if isinstance(item, dict):
                        distribution = item.get("distribution")
                        import_name = item.get("import_name")
                        if isinstance(distribution, str):
                            missing_names.append(distribution)
                        elif isinstance(import_name, str):
                            missing_names.append(import_name)
            risks.append(
                {
                    "risk_id": "rr-missing-runtime-dependencies",
                    "severity": "high",
                    "status": "open",
                    "summary": "The doctor proof reports missing runtime dependencies.",
                    "why_open": ", ".join(missing_names) if missing_names else "dependency_truth.ok=false",
                    "close_with": "Install the missing runtime dependencies and rerun `./scripts/dag-ctl doctor --json` plus this packet.",
                    "evidence_paths": [doctor_step["stdout_path"]],
                }
            )
        health_expectations = {
            "services_ready": True,
            "database_reachable": True,
            "schema_bootstrapped": True,
            "persisted": True,
        }
        mismatches = [
            f"{field}={doctor_payload.get(field)!r}"
            for field, expected in health_expectations.items()
            if doctor_payload.get(field) != expected
        ]
        if mismatches:
            risks.append(
                {
                    "risk_id": "rr-health-truth-not-ready",
                    "severity": "high",
                    "status": "open",
                    "summary": "The runtime health proof does not satisfy the expected ready-state fields.",
                    "why_open": ", ".join(mismatches),
                    "close_with": "Repair the failing health fields, rerun `./scripts/dag-ctl doctor --json`, and rerun this packet.",
                    "evidence_paths": [doctor_step["stdout_path"]],
                }
            )
        sync_status = doctor_payload.get("sync_status")
        if sync_status not in (None, "succeeded"):
            risks.append(
                {
                    "risk_id": "rr-sync-status-not-succeeded",
                    "severity": "medium",
                    "status": "open",
                    "summary": "The doctor proof reports a non-succeeded sync status.",
                    "why_open": f"sync_status={sync_status!r}",
                    "close_with": "Run the bounded repair path and rerun the proof after sync returns to `succeeded`.",
                    "evidence_paths": [doctor_step["stdout_path"]],
                }
            )
        smoke_run_id = doctor_payload.get("smoke_run_id")
        if not isinstance(smoke_run_id, str) or not smoke_run_id.strip():
            risks.append(
                {
                    "risk_id": "rr-no-smoke-run-id",
                    "severity": "medium",
                    "status": "open",
                    "summary": "The doctor proof did not expose a `smoke_run_id` for live persisted-run inspection.",
                    "why_open": "Without a smoke run id, the packet cannot capture native status/inspect evidence for a concrete run.",
                    "close_with": "Produce or repair the smoke proof, then rerun this packet to capture status and inspect evidence.",
                    "evidence_paths": [doctor_step["stdout_path"]],
                }
            )

    if not isinstance(doctor_payload, dict):
        risks.append(
            {
                "risk_id": "rr-health-proof-unavailable",
                "severity": "high",
                "status": "open",
                "summary": "The packet could not parse `dag-ctl doctor --json` into a health proof payload.",
                "why_open": "The canonical readiness surface is unavailable or malformed for this run.",
                "close_with": "Restore the `dag-ctl doctor --json` surface and rerun the proof.",
                "evidence_paths": [
                    "artifacts/workflow/release_hardening/evidence/latest/02_dag_ctl_doctor.stdout.json",
                    "artifacts/workflow/release_hardening/evidence/latest/02_dag_ctl_doctor.stderr.txt",
                ],
            }
        )

    validate_step = step_map.get("test_validate")
    if validate_step and any(
        "agent resolution was blocked by the sandbox permission surface" in warning
        for warning in validate_step.get("warnings", [])
    ):
        risks.append(
            {
                "risk_id": "rr-queue-validation-agent-resolution-blocked",
                "severity": "medium",
                "status": "open",
                "summary": "Queue validation passed, but agent resolution was not exercised in this sandbox.",
                "why_open": "The validation front door surfaced a sandbox-permission warning instead of a full agent-resolution check.",
                "close_with": "Replay queue validation in an environment that can perform the agent-resolution check.",
                "evidence_paths": [validate_step["stdout_path"]],
            }
        )

    persisted_truth_claim = next(
        (claim for claim in claim_matrix if claim["claim"] == "persisted_run_truth"),
        None,
    )
    if persisted_truth_claim and persisted_truth_claim["status"] != "proved":
        persisted_steps = [
            step_map[step_id]
            for step_id in ("native_status_smoke_run", "native_inspect_smoke_run")
            if step_id in step_map
        ]
        persisted_evidence_paths = [
            path
            for step in persisted_steps
            for path in (step["stdout_path"], step["stderr_path"])
        ]
        permission_blocked = False
        for step in persisted_steps:
            stderr_text = Path(REPO_ROOT / step["stderr_path"]).read_text(encoding="utf-8")
            if "PermissionError: [Errno 1] Operation not permitted" in stderr_text:
                permission_blocked = True
                break
        risks.append(
            {
                "risk_id": "rr-persisted-run-truth-partial",
                "severity": "medium",
                "status": "open",
                "summary": (
                    "The packet does not capture live status and inspect evidence for a concrete smoke run."
                ),
                "why_open": (
                    "Native status/inspect readback is blocked by sandboxed socket permissions."
                    if permission_blocked
                    else "Release proof still relies on targeted tests for part of the persisted-run interpretation contract."
                ),
                "close_with": (
                    "Ensure `smoke_run_id` is present, capture both native status and inspect outputs, and rerun the proof."
                ),
                "evidence_paths": persisted_evidence_paths
                or ["artifacts/workflow/release_hardening/evidence/latest/summary.json"],
            }
        )

    failed_required_steps = [
        step["step_id"]
        for step in step_map.values()
        if step["required"] and not step["ok"]
    ]
    if failed_required_steps:
        risks.append(
            {
                "risk_id": "rr-required-proof-step-failed",
                "severity": "high",
                "status": "open",
                "summary": "One or more required proof steps failed.",
                "why_open": ", ".join(failed_required_steps),
                "close_with": "Inspect the step stderr files, repair the failing contract, and rerun the proof.",
                "evidence_paths": [
                    "artifacts/workflow/release_hardening/evidence/latest/summary.json",
                ],
            }
        )

    return risks


def main() -> int:
    if not QUEUE_PATH.exists():
        print(f"missing queue file: {QUEUE_PATH}", file=sys.stderr)
        return 1

    run_dir = EVIDENCE_ROOT / _timestamp_slug()
    run_dir.mkdir(parents=True, exist_ok=False)

    steps = _proof_steps()
    results: list[dict[str, Any]] = []
    doctor_payload: dict[str, Any] = {}

    for index, step in enumerate(steps):
        result = _run_step(step=step, step_index=index, run_dir=run_dir)
        results.append(result)
        if step.step_id == "dag_ctl_doctor" and isinstance(result.get("parsed_json"), dict):
            doctor_payload = dict(result["parsed_json"])

    optional_steps = _optional_steps(doctor_payload)
    for index, step in enumerate(optional_steps, start=len(results)):
        results.append(_run_step(step=step, step_index=index, run_dir=run_dir))

    step_map = {result["step_id"]: result for result in results}
    claim_definitions = [
        {
            "claim": "traceability",
            "description": "Evidence ties back to a concrete commit and worktree state.",
            "step_ids": ["git_head", "git_status"],
        },
        {
            "claim": "health_governance",
            "description": "Canonical runtime health proof is readable through `dag-ctl doctor --json`.",
            "step_ids": ["dag_ctl_doctor"],
        },
        {
            "claim": "frontdoor_contract",
            "description": "The machine-readable test front door covers the required queue/test commands.",
            "step_ids": [
                "test_selftest",
                "test_suite_list",
                "test_suite_focus",
                "test_plan",
                "test_check_affected",
                "test_validate",
            ],
        },
        {
            "claim": "native_contract",
            "description": "Repo-local native instance resolution stays on the checked-in contract.",
            "step_ids": ["native_primary_instance"],
        },
        {
            "claim": "proof_slice",
            "description": "Targeted tests prove the bounded native/frontdoor/control surfaces that back this packet.",
            "step_ids": ["release_pytest_slice"],
        },
        {
            "claim": "persisted_run_truth",
            "description": "A concrete smoke run can be read back through native status and inspect surfaces.",
            "step_ids": ["native_status_smoke_run", "native_inspect_smoke_run"],
        },
    ]
    claim_matrix: list[dict[str, Any]] = []
    for claim in claim_definitions:
        claim_matrix.append(
            {
                **claim,
                "status": _claim_status(step_ids=claim["step_ids"], step_map=step_map),
                "evidence_paths": [
                    step_map[step_id]["stdout_path"]
                    for step_id in claim["step_ids"]
                    if step_id in step_map
                ],
            }
        )

    residual_risks = _residual_risks(step_map=step_map, claim_matrix=claim_matrix)
    overall_ok = all(result["ok"] for result in results if result["required"])
    summary = {
        "proof_packet": "release_hardening",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(REPO_ROOT),
        "queue_file": str(QUEUE_RELATIVE_PATH),
        "evidence_dir": str(run_dir.relative_to(REPO_ROOT)),
        "latest_evidence_dir": str(LATEST_EVIDENCE_DIR.relative_to(REPO_ROOT)),
        "all_required_steps_passed": overall_ok,
        "steps": [
            {
                key: value
                for key, value in result.items()
                if key != "parsed_json"
            }
            for result in results
        ],
        "claim_matrix": claim_matrix,
        "residual_risks": residual_risks,
    }

    summary_path = run_dir / "summary.json"
    risks_path = run_dir / "residual_risks.json"
    _write_text(summary_path, json.dumps(summary, indent=2, sort_keys=True) + "\n")
    _write_text(risks_path, json.dumps(residual_risks, indent=2, sort_keys=True) + "\n")

    if LATEST_EVIDENCE_DIR.exists():
        shutil.rmtree(LATEST_EVIDENCE_DIR)
    shutil.copytree(run_dir, LATEST_EVIDENCE_DIR)

    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
