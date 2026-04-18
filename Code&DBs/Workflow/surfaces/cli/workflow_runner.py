"""Workflow runner library — spec parsing, prompt building, verification, and dry-run execution."""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from runtime.receipt_provenance import (
    build_git_provenance,
    build_mutation_provenance,
    build_workspace_provenance,
    build_write_manifest,
    extract_write_paths,
)
from runtime.execution_transport import resolve_execution_transport
from runtime._workflow_database import resolve_runtime_database_url
from runtime.notifications import dispatch_notification_payload
from runtime.workflow.receipt_writer import prepare_output_artifact
from runtime.workflow.execution_backends import execute_api as execute_api_in_sandbox
from runtime.workflow_spec import WorkflowSpec, WorkflowSpecError

_log = logging.getLogger("workflow_runner")

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class VerifyResult:
    command: str
    passed: bool
    stdout: str
    stderr: str


@dataclass(frozen=True)
class Telemetry:
    """Token usage, cost, and tool-use stats from a CLI agent run."""
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    cost_usd: float = 0.0
    model: str = ""
    duration_api_ms: int = 0
    num_turns: int = 0
    tool_use: dict = None  # {"web_search_requests": 0, ...}

    def __post_init__(self):
        if self.tool_use is None:
            object.__setattr__(self, "tool_use", {})


@dataclass(frozen=True)
class JobExecution:
    job_label: str
    agent_slug: str
    status: str  # 'succeeded' | 'failed' | 'blocked' | 'skipped'
    exit_code: Optional[int]
    stdout: str
    stderr: str
    duration_seconds: float
    verify_passed: Optional[bool]
    retry_count: int
    telemetry: Optional[Telemetry] = None


@dataclass(frozen=True)
class RunResult:
    spec_name: str
    total_jobs: int
    succeeded: int
    failed: int
    skipped: int
    blocked: int
    job_results: tuple
    duration_seconds: float
    receipts_written: tuple


class WorkflowReceiptPersistenceError(RuntimeError):
    """Raised when canonical receipt persistence fails for a workflow job."""

    def __init__(self, *, run_id: str, job_label: str, reason_code: str | None = None) -> None:
        detail = f"canonical receipt persistence failed for job '{job_label}' in run '{run_id}'"
        if reason_code:
            detail = f"{detail} ({reason_code})"
        super().__init__(detail)
        self.run_id = run_id
        self.job_label = job_label
        self.reason_code = reason_code or ""


# ---------------------------------------------------------------------------
# WorkflowRunner
# ---------------------------------------------------------------------------

class WorkflowRunner:
    """Executes workflow specs through the Praxis Engine workflow pipeline."""

    def __init__(
        self,
        config_root: str,
        receipts_dir: str,
        db_path: str = "",
        constraints_db: Optional[str] = None,
        pg_conn=None,
    ) -> None:
        self._config_root = config_root
        self._receipts_dir = receipts_dir
        self._db_path = db_path
        self._pg_conn = pg_conn

        # Load agent registry — prefer Postgres, fall back to JSON
        from registry.agent_config import AgentRegistry
        if pg_conn is not None:
            self._agent_registry = AgentRegistry.load_from_postgres(pg_conn)
        else:
            agents_json = os.path.join(config_root, "agents.json")
            self._agent_registry = AgentRegistry.load(agents_json)

        # Initialize workflow pipeline components
        import runtime.workflow_pipeline as dp
        self._pipeline = dp.WorkflowPipeline(
            governance=dp.GovernanceFilter(),
            conflict_resolver=dp.ConflictResolver(),
            loop_detector=dp.LoopDetector(),
            auto_retry=dp.AutoRetryManager(),
            retry_context_builder=dp.RetryContextBuilder(),
            posture_enforcer=dp.PostureEnforcer(dp.Posture.BUILD),
        )
        self._dp = dp

        # Constraint ledger — prefer Postgres, fall back to SQLite
        self._constraint_ledger = None
        try:
            from runtime.constraint_ledger import ConstraintLedger
            if pg_conn is not None:
                embedder = None
                try:
                    from runtime.embedding_service import EmbeddingService
                    embedder = EmbeddingService()
                except Exception:
                    pass
                self._constraint_ledger = ConstraintLedger(pg_conn, embedder)
            elif constraints_db:
                self._constraint_ledger = ConstraintLedger(constraints_db)
        except Exception:
            pass

        # Friction ledger — Postgres only
        self._friction_ledger = None
        try:
            from runtime.friction_ledger import FrictionLedger
            if pg_conn is not None:
                self._friction_ledger = FrictionLedger(pg_conn)
        except Exception:
            pass

        # Observability hub (lazy, tolerates missing subsystem modules)
        self._obs_hub = None
        try:
            from runtime.observability_hub import ObservabilityHub
            if pg_conn is not None:
                self._obs_hub = ObservabilityHub(pg_conn)
        except Exception:
            pass

        # Ensure receipts directory exists
        os.makedirs(self._receipts_dir, exist_ok=True)

    def _log_advisory_failure(
        self,
        *,
        component: str,
        reason_code: str,
        job_label: str,
        exc: Exception,
        details: dict[str, Any] | None = None,
    ) -> None:
        payload = {
            "component": component,
            "reason_code": reason_code,
            "run_id": str(getattr(self, "_run_id", "") or ""),
            "job_label": job_label,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }
        if details:
            payload.update(details)
        _log.debug(
            "workflow_runner.advisory_failure component=%s reason_code=%s run_id=%s job_label=%s",
            component,
            reason_code,
            payload["run_id"],
            job_label,
            extra={"workflow_runner_advisory_failure": payload},
            exc_info=True,
        )

    def _run_verify(self, verify_commands: list[object], workdir: str) -> list[VerifyResult]:
        """Run verification bindings sequentially using verification_registry authority."""
        if self._pg_conn is None:
            return [
                VerifyResult(
                    command="verification_registry",
                    passed=False,
                    stdout="",
                    stderr="verification requires Postgres authority",
                )
            ]

        from runtime.verification import resolve_verify_commands, run_verify

        commands = resolve_verify_commands(self._pg_conn, verify_commands)
        return [
            VerifyResult(
                command=result.command,
                passed=result.passed,
                stdout=result.stdout,
                stderr=result.stderr,
            )
            for result in run_verify(commands, workdir=workdir)
        ]

    def run_workflow(
        self,
        spec: WorkflowSpec,
        *,
        dry_run: bool = False,
        run_id: str | None = None,
    ) -> RunResult:
        """Execute a workflow spec locally or simulate it in dry-run mode."""
        started = time.monotonic()
        self._run_id = run_id or f"workflow_{uuid.uuid4().hex[:12]}"
        self._workflow_id = spec.workflow_id if spec.workflow_id.startswith("workflow.") else f"workflow.{spec.workflow_id}"
        self._request_id = f"req_{uuid.uuid4().hex[:12]}"

        if dry_run:
            return self._run_dry_run_workflow(spec)

        job_results: list[JobExecution] = []
        receipts_written: list[str] = []
        completed: dict[str, JobExecution] = {}
        pending = {job["label"]: dict(job) for job in spec.jobs}

        while pending:
            progressed = False
            for label, job in list(pending.items()):
                depends_on = list(job.get("depends_on", []) or [])
                if any(dep not in completed for dep in depends_on):
                    continue

                workdir = str(job.get("workdir") or os.getcwd())
                if any(completed[dep].status != "succeeded" for dep in depends_on):
                    result = JobExecution(
                        job_label=label,
                        agent_slug=str(job["agent"]),
                        status="blocked",
                        exit_code=None,
                        stdout="",
                        stderr=f"Blocked by dependency failure: {', '.join(depends_on)}",
                        duration_seconds=0.0,
                        verify_passed=None,
                        retry_count=0,
                    )
                else:
                    gate = self._pipeline.pre_dispatch({
                        "job_label": label,
                        "label": label,
                        "prompt": job.get("prompt", ""),
                        "agent_slug": str(job.get("agent", "")),
                    })
                    if not gate.passed:
                        result = JobExecution(
                            job_label=label,
                            agent_slug=str(job["agent"]),
                            status="blocked",
                            exit_code=None,
                            stdout="",
                            stderr="; ".join(gate.blocked_by),
                            duration_seconds=0.0,
                            verify_passed=None,
                            retry_count=0,
                        )
                    elif dry_run:
                        result = JobExecution(
                            job_label=label,
                            agent_slug=str(job["agent"]),
                            status="succeeded",
                            exit_code=0,
                            stdout=f"[dry-run] Would execute workflow job '{label}'",
                            stderr="",
                            duration_seconds=0.0,
                            verify_passed=None,
                            retry_count=0,
                        )
                    else:
                        execution_job = dict(job)
                        execution_job["prompt"] = self._inject_similarity_context(
                            job.get("prompt", ""),
                            job,
                        )
                        prompt = self._inject_platform_context(execution_job)
                        agent_slug = job["agent"]
                        agent_config = self._agent_registry.get(agent_slug)
                        timeout = int(job.get("timeout", getattr(agent_config, "timeout_seconds", 900)) or 900)
                        result = self._execute_job(job, agent_config, prompt, workdir, timeout)

                        verify_commands = (
                            list(job.get("verify_refs", []) or [])
                            or list(spec.verify_refs or [])
                        )
                        verify_passed = None
                        if result.status == "succeeded" and verify_commands:
                            verify_results = self._run_verify(verify_commands, workdir)
                            verify_passed = all(item.passed for item in verify_results)
                            if not verify_passed:
                                failed_commands = ", ".join(item.command for item in verify_results if not item.passed)
                                result = JobExecution(
                                    job_label=result.job_label,
                                    agent_slug=result.agent_slug,
                                    status="failed",
                                    exit_code=1,
                                    stdout=result.stdout,
                                    stderr=(result.stderr + f"\nVerification failed: {failed_commands}").strip(),
                                    duration_seconds=result.duration_seconds,
                                    verify_passed=False,
                                    retry_count=result.retry_count,
                                    telemetry=result.telemetry,
                                )
                            elif verify_passed is True:
                                result = JobExecution(
                                    job_label=result.job_label,
                                    agent_slug=result.agent_slug,
                                    status=result.status,
                                    exit_code=result.exit_code,
                                    stdout=result.stdout,
                                    stderr=result.stderr,
                                    duration_seconds=result.duration_seconds,
                                    verify_passed=True,
                                    retry_count=result.retry_count,
                                    telemetry=result.telemetry,
                                )

                completed[label] = result
                job_results.append(result)
                pending.pop(label)
                if dry_run:
                    receipts_written.append(f"dry_run:{label}")
                else:
                    receipts_written.append(self._write_receipt(result, spec))
                progressed = True

            if not progressed:
                unresolved_labels = list(pending)
                for label in unresolved_labels:
                    result = JobExecution(
                        job_label=label,
                        agent_slug=str(pending[label]["agent"]),
                        status="blocked",
                        exit_code=None,
                        stdout="",
                        stderr="Blocked by unresolved dependency graph.",
                        duration_seconds=0.0,
                        verify_passed=None,
                        retry_count=0,
                    )
                    completed[label] = result
                    job_results.append(result)
                    receipts_written.append(f"dry_run:{label}" if dry_run else self._write_receipt(result, spec))
                    pending.pop(label)

        duration_seconds = round(time.monotonic() - started, 2)
        succeeded = sum(1 for result in job_results if result.status == "succeeded")
        failed = sum(1 for result in job_results if result.status == "failed")
        blocked = sum(1 for result in job_results if result.status == "blocked")
        skipped = sum(1 for result in job_results if result.status == "skipped")

        result = RunResult(
            spec_name=spec.name,
            total_jobs=len(spec.jobs),
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            blocked=blocked,
            job_results=tuple(job_results),
            duration_seconds=duration_seconds,
            receipts_written=tuple(receipts_written),
        )

        self._emit_batch_summary_notification(result)
        return result

    def _run_dry_run_workflow(self, spec: WorkflowSpec) -> RunResult:
        from runtime.workflow.dry_run import dry_run_workflow

        dry_run_result = dry_run_workflow(spec)
        job_results = tuple(
            JobExecution(
                job_label=job_result.job_label,
                agent_slug=job_result.agent_slug,
                status=job_result.status,
                exit_code=job_result.exit_code,
                stdout=(
                    f"[dry-run] Would execute workflow job '{job_result.job_label}'"
                    if job_result.status == "succeeded"
                    else ""
                ),
                stderr=(
                    "Blocked by dry-run governance or dependency simulation."
                    if job_result.status == "blocked"
                    else ""
                ),
                duration_seconds=job_result.duration_seconds,
                verify_passed=job_result.verify_passed,
                retry_count=job_result.retry_count,
                telemetry=None,
            )
            for job_result in dry_run_result.job_results
        )
        result = RunResult(
            spec_name=dry_run_result.spec_name,
            total_jobs=dry_run_result.total_jobs,
            succeeded=dry_run_result.succeeded,
            failed=dry_run_result.failed,
            skipped=dry_run_result.skipped,
            blocked=dry_run_result.blocked,
            job_results=job_results,
            duration_seconds=dry_run_result.duration_seconds,
            receipts_written=dry_run_result.receipts_written,
        )
        self._emit_batch_summary_notification(result)
        return result

    def _emit_batch_summary_notification(self, result: RunResult) -> None:
        failed = result.failed
        blocked = result.blocked

        try:
            dispatch_notification_payload(
                {
                    "kind": "workflow_batch_summary",
                    "status": "succeeded"
                    if failed == 0 and blocked == 0
                    else "failed"
                    if failed > 0
                    else "blocked",
                    "reason_code": "workflow_runner.batch_complete",
                    "run_id": self._run_id,
                    "workflow_id": self._workflow_id,
                    "spec_name": result.spec_name,
                    "total_jobs": result.total_jobs,
                    "succeeded": result.succeeded,
                    "failed": failed,
                    "skipped": result.skipped,
                    "blocked": blocked,
                    "duration_seconds": result.duration_seconds,
                    "latency_ms": int(result.duration_seconds * 1000),
                    "receipt_count": len(result.receipts_written),
                }
            )
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Job execution
    # ------------------------------------------------------------------

    def _execute_job(
        self,
        job: dict,
        agent_config: Any,
        prompt: str,
        workdir: str,
        timeout: int,
    ) -> JobExecution:
        """Execute a single job via the legacy runner."""
        label = job["label"]
        agent_slug = job["agent"]

        if agent_config is None:
            return JobExecution(
                job_label=label,
                agent_slug=agent_slug,
                status="failed",
                exit_code=None,
                stdout="",
                stderr=f"No agent config found for slug: {agent_slug}",
                duration_seconds=0.0,
                verify_passed=None,
                retry_count=0,
            )

        # Build command based on execution transport
        transport = resolve_execution_transport(agent_config)
        transport_kind = transport.transport_kind

        if transport_kind == "cli":
            return JobExecution(
                job_label=label,
                agent_slug=agent_slug,
                status="blocked",
                exit_code=None,
                stdout="",
                stderr=(
                    "Live CLI subprocess execution is disabled in WorkflowRunner; "
                    "runtime.workflow is the execution authority."
                ),
                duration_seconds=0.0,
                verify_passed=None,
                retry_count=0,
            )
        elif transport_kind == "api":
            return self._execute_api_job(
                label=label,
                agent_slug=agent_slug,
                agent_config=agent_config,
                prompt=prompt,
                timeout=timeout,
            )
        else:
            return JobExecution(
                job_label=label,
                agent_slug=agent_slug,
                status="skipped",
                exit_code=None,
                stdout="[execution transport] Not yet implemented in CLI runner",
                stderr="",
                duration_seconds=0.0,
                verify_passed=None,
                retry_count=0,
            )

    # ------------------------------------------------------------------
    # Verify commands
    # ------------------------------------------------------------------

    _API_MAX_RETRIES = 3
    _API_BACKOFF = (5, 15, 60)  # seconds per retry attempt

    @staticmethod
    def _is_retryable_api_error(exc: Exception) -> bool:
        """Check if an API exception is retryable (rate limit, server error)."""
        from runtime.failure_classifier import classify_failure_from_stderr
        classification = classify_failure_from_stderr(str(exc))
        return classification.is_retryable

    def _execute_api_job(
        self,
        label: str,
        agent_slug: str,
        agent_config: Any,
        prompt: str,
        timeout: int,
    ) -> JobExecution:
        """Execute a job via the normalized sandboxed API transport."""
        start = time.monotonic()
        last_exc = None
        for attempt in range(self._API_MAX_RETRIES + 1):
            if attempt > 0:
                backoff = self._API_BACKOFF[min(attempt - 1, len(self._API_BACKOFF) - 1)]
                print(f"  [retry {attempt}/{self._API_MAX_RETRIES}] {label}: backing off {backoff}s...", flush=True)
                time.sleep(backoff)

            try:
                result = execute_api_in_sandbox(
                    agent_config,
                    prompt,
                    workdir=str(self._repo_root),
                )
                duration = time.monotonic() - start
                return JobExecution(
                    job_label=label,
                    agent_slug=agent_slug,
                    status=str(result.get("status") or "failed"),
                    exit_code=int(result.get("exit_code", 1)) if result.get("exit_code") is not None else None,
                    stdout=str(result.get("stdout") or ""),
                    stderr=str(result.get("stderr") or ""),
                    duration_seconds=round(duration, 2),
                    verify_passed=None,
                    retry_count=attempt,
                )
            except Exception as exc:
                last_exc = exc
                if attempt < self._API_MAX_RETRIES and self._is_retryable_api_error(exc):
                    continue  # retry
                break  # non-retryable or exhausted retries

        duration = time.monotonic() - start
        retry_count = min(attempt, self._API_MAX_RETRIES)
        exc = last_exc or Exception("unknown error")
        return JobExecution(
            job_label=label, agent_slug=agent_slug, status="failed",
            exit_code=1, stdout="", stderr=f"{type(exc).__name__}: {exc}",
            duration_seconds=round(duration, 2), verify_passed=None,
            retry_count=retry_count,
        )

    # ------------------------------------------------------------------
    # Failure classification
    # ------------------------------------------------------------------

    @staticmethod
    def _classify_failure(result: JobExecution) -> str:
        """Classify a failed job into a failure category via the central classifier.

        Returns the FailureCategory value string (e.g. 'rate_limit', 'timeout').
        """
        from runtime.failure_classifier import classify_failure_from_stderr
        classification = classify_failure_from_stderr(
            result.stderr or "",
            exit_code=result.exit_code or 1,
        )
        return classification.category.value

    # Telemetry parsing
    # ------------------------------------------------------------------

    def _get_cli_config(self, provider_slug: str, model_slug: str) -> Optional[dict]:
        """Look up CLI invocation config from provider_model_candidates."""
        if not self._pg_conn:
            return None
        rows = self._pg_conn.execute(
            """SELECT cli_config FROM provider_model_candidates
               WHERE provider_slug = $1 AND model_slug = $2
                 AND status = 'active' AND cli_config != '{}'::jsonb
               LIMIT 1""",
            provider_slug, model_slug,
        )
        if rows:
            cfg = rows[0]["cli_config"]
            if isinstance(cfg, str):
                import json as _json
                cfg = _json.loads(cfg)
            if cfg.get("cmd_template"):
                return cfg
        # Fall back to provider-level
        rows = self._pg_conn.execute(
            """SELECT cli_config FROM provider_model_candidates
               WHERE provider_slug = $1 AND status = 'active'
                 AND cli_config != '{}'::jsonb
               LIMIT 1""",
            provider_slug,
        )
        if rows:
            cfg = rows[0]["cli_config"]
            if isinstance(cfg, str):
                import json as _json
                cfg = _json.loads(cfg)
            if cfg.get("cmd_template"):
                return cfg
        return None

    @staticmethod
    def _parse_cli_telemetry(
        raw_stdout: str, provider: str,
    ) -> tuple[Optional[Telemetry], str]:
        """Parse JSON output from any CLI agent to extract telemetry.

        Uses the transport registry's telemetry parsers — no provider-specific
        branches. Returns (telemetry, result_text) or (None, raw_stdout).
        """
        try:
            data = json.loads(raw_stdout)
        except (json.JSONDecodeError, TypeError):
            return None, raw_stdout

        if not isinstance(data, dict):
            return None, raw_stdout

        # Look up protocol family from provider profile
        try:
            from registry.provider_execution_registry import get_profile
            profile = get_profile(provider)
            protocol = profile.api_protocol_family if profile else None
        except Exception:
            protocol = None

        if not protocol:
            # Fallback: extract result text from common envelope keys
            result_text = data.get("result") or data.get("response") or raw_stdout
            return None, result_text

        from runtime.http_transport import parse_telemetry
        parsed = parse_telemetry(protocol, data)
        if parsed is None:
            result_text = data.get("result") or data.get("response") or raw_stdout
            return None, result_text

        result_text = parsed.get("result_text") or data.get("result") or data.get("response") or raw_stdout
        telemetry = Telemetry(
            input_tokens=parsed.get("input_tokens", 0),
            output_tokens=parsed.get("output_tokens", 0),
            cache_read_tokens=parsed.get("cache_read_tokens", 0),
            cache_creation_tokens=parsed.get("cache_creation_tokens", 0),
            cost_usd=parsed.get("cost_usd", 0.0),
            model=parsed.get("model", ""),
            duration_api_ms=parsed.get("duration_api_ms", 0),
            num_turns=parsed.get("num_turns", 0),
            tool_use=parsed.get("tool_use", {}),
        )
        return telemetry, result_text
    # ------------------------------------------------------------------
    # Functional similarity injection
    # ------------------------------------------------------------------

    def _inject_similarity_context(self, prompt: str, job: dict) -> str:
        """Search for existing modules similar to what this job builds.

        If high-similarity matches are found, inject a constraint block
        telling the agent to use existing infrastructure instead of
        building new modules.  Non-fatal: returns prompt unchanged on
        any error.
        """
        try:
            from runtime.module_indexer import ModuleIndexer
            if self._pg_conn is None:
                return prompt

            indexer = ModuleIndexer(conn=self._pg_conn, repo_root=os.getcwd())

            # Use the job's prompt + label as the search query
            query = f"{job.get('label', '')} {prompt[:500]}"
            results = indexer.search(query, limit=5, threshold=0.4)

            if not results:
                return prompt

            # Only inject if we have genuinely high-similarity matches
            strong = [r for r in results if r["cosine_similarity"] >= 0.4]
            if not strong:
                return prompt

            lines = [
                "",
                "--- EXISTING INFRASTRUCTURE (from module similarity search) ---",
                "Before building new modules, check if these existing implementations",
                "solve your problem. Wire existing code instead of creating new files.",
                "",
            ]
            for r in strong[:5]:
                lines.append(
                    f"  [{r['cosine_similarity']:.2f}] {r['module_path']}::{r['name']} "
                    f"({r['kind']})"
                )
                if r.get("docstring_preview"):
                    lines.append(f"         {r['docstring_preview'][:150]}")
                lines.append("")

            lines.append("--- END EXISTING INFRASTRUCTURE ---")
            lines.append("")

            return prompt + "\n".join(lines)
        except Exception:
            return prompt  # Similarity injection is non-fatal

    # ------------------------------------------------------------------
    # Platform context injection
    # ------------------------------------------------------------------

    def _inject_platform_context(self, job: dict) -> str:
        """Prepend scope-aware platform context to the job prompt.

        Only injects context the agent can actually use based on its
        scope_read/scope_write paths. An agent writing a single Python
        file doesn't get told about the database or migrations.
        """
        prompt = job["prompt"]
        scope_read = job.get("scope_read", [])
        scope_write = job.get("scope_write", [])
        all_paths = scope_read + scope_write

        # Detect what domains the agent's scope touches
        touches_db = any("migration" in p.lower() or ".sql" in p for p in all_paths)
        touches_python = any(p.endswith(".py") for p in all_paths)
        touches_memory = any("memory/" in p for p in all_paths)
        touches_runtime = any("runtime/" in p for p in all_paths)
        touches_mcp = any("surfaces/" in p or "mcp" in p.lower() for p in all_paths)
        touches_tests = any("test" in p.lower() for p in all_paths)
        touches_vectors = touches_db or touches_memory or "embedding" in prompt.lower() or "vector" in prompt.lower()
        touches_infra = any(
            "config/" in p or "launchd" in p or "scripts/" in p or "plist" in p
            for p in all_paths
        ) or any(kw in prompt.lower() for kw in ("service", "launchd", "praxis-ctl", "praxis", "postgres start", "restart"))

        lines = [
            "--- PLATFORM CONTEXT ---",
            "You are an agent in the Praxis autonomous engineering control plane.",
            f"Repository root: {self._config_root.rstrip('/config')}",
            "Generated artifact dirs such as artifacts/workflow_outputs/ are not authoritative repo inputs.",
            "Only inspect those dirs when the task is explicitly about artifact forensics, receipts, or transcript debugging.",
            "",
        ]

        # Only show structure relevant to scope
        if touches_python or not all_paths:
            lines.append("PROJECT STRUCTURE:")
            lines.append("  Code&DBs/Workflow/         — Python source (runtime/, memory/, surfaces/, adapters/)")
            if touches_db:
                lines.append("  Code&DBs/Databases/        — Postgres migrations (001-024)")
            if touches_tests:
                lines.append("  Code&DBs/Workflow/tests/    — Unit and integration tests")
            lines.append("")

        # Only show DB info if scope touches DB, memory, or runtime
        if touches_db or touches_memory or touches_runtime:
            database_url = str(resolve_runtime_database_url(required=False) or "unavailable")
            lines.append(f"DATABASE: {database_url}")
            if touches_vectors:
                lines.append("  - Semantic vector retrieval enabled (384-dim embeddings via all-MiniLM-L6-v2)")
            lines.append("  - All subsystems use SyncPostgresConnection")
            lines.append("  - Postgres params: use $1, $2 (asyncpg positional), NOT %s")
            lines.append("  - JSONB casts: $1::jsonb for JSON inserts")
            if touches_vectors:
                lines.append("  - Vector-store adapter owns similarity casts and thresholds")
            lines.append("")

        # Python conventions only if writing Python
        if touches_python:
            lines.append("CONVENTIONS:")
            lines.append("  - Imports: 'from runtime.X import Y', 'from memory.X import Y'")
            lines.append("  - Type hints: use TYPE_CHECKING for heavy imports")
            if touches_memory:
                lines.append("  - HeartbeatModule ABC: implement name property + run() → HeartbeatModuleResult")
            if touches_tests:
                lines.append("  - Tests: PYTHONPATH='Code&DBs/Workflow' python3 -m pytest --noconftest -q <test>")
            lines.append("")

        # Infrastructure / service management
        if touches_infra:
            lines.append("SERVICES (managed by scripts/praxis):")
            lines.append("  praxis launch    — start Docker services, probe launcher readiness, and open /app")
            lines.append("  praxis doctor    — emit launcher readiness as JSON")
            lines.append("  praxis status    — show Docker service state plus active workflows")
            lines.append("  praxis restart   — restart all (or: praxis restart api|worker|postgres|scheduler)")
            lines.append("  praxis logs      — tail all service logs")
            lines.append("  Runtime control is Docker-only; native launchd install/setup was removed.")
            lines.append("  Docker Compose owns these services:")
            lines.append("    - postgres        (port 5432)")
            lines.append("    - api-server      (port 8420)")
            lines.append("    - workflow-worker (event-bus workflow worker)")
            lines.append("    - scheduler       (60s tick loop)")
            lines.append("  praxis-ctl remains available as a compatibility alias.")
            lines.append("")

        # Scope boundaries
        if scope_write:
            lines.append("SCOPE (files you may create or edit):")
            for p in scope_write:
                lines.append(f"  - {p}")
            lines.append("")

        if scope_read:
            lines.append("REFERENCE FILES (read these before writing):")
            for p in scope_read:
                lines.append(f"  - {p}")
            lines.append("")

        lines.extend([
            "IMPORTANT:",
            "  - Read files before editing them",
        ])
        if touches_db:
            lines.append("  - Use ON CONFLICT for idempotent upserts")
        lines.extend([
            "  - Keep backward compatibility (guard new features behind None checks)",
            "  - Do NOT add unnecessary abstractions or docstrings to code you didn't change",
        ])
        if scope_write:
            lines.append(f"  - ONLY write to files listed in SCOPE above")
        lines.extend([
            "--- END PLATFORM CONTEXT ---",
            "",
        ])

        return "\n".join(lines) + prompt
    def _write_receipt(self, result: JobExecution, spec: WorkflowSpec) -> str:
        """Persist a receipt to Postgres and return a logical receipt reference."""
        now = datetime.now(timezone.utc)
        started_at = now - timedelta(seconds=result.duration_seconds)
        t = result.telemetry or Telemetry()
        failure_code = self._classify_failure(result) if result.status in ("failed", "error") else None
        provider_slug, _, model_slug = result.agent_slug.partition("/")
        workspace_root = str(Path(getattr(spec, "workdir", "") or os.getcwd()).resolve())
        workspace_ref = str(getattr(spec, "workspace_ref", "") or "").strip()
        runtime_profile_ref = str(getattr(spec, "runtime_profile_ref", "") or "").strip()
        write_scope = extract_write_paths(getattr(spec, "scope_write", None))
        workspace_provenance = build_workspace_provenance(
            workspace_root=workspace_root,
            workspace_ref=workspace_ref,
            runtime_profile_ref=runtime_profile_ref,
        )
        git_provenance = build_git_provenance(
            workspace_root=workspace_root,
            workspace_ref=workspace_ref,
            runtime_profile_ref=runtime_profile_ref,
            conn=self._pg_conn,
        )
        write_manifest = (
            build_write_manifest(
                workspace_root=workspace_root,
                write_paths=write_scope,
                source="workflow_runner",
            )
            if write_scope
            else None
        )
        mutation_provenance = (
            build_mutation_provenance(
                workspace_root=workspace_root,
                write_paths=write_scope,
                source="workflow_runner",
            )
            if write_scope
            else None
        )
        receipt = {
            "label": result.job_label,
            "job_label": result.job_label,
            "spec_name": spec.name,
            "workflow_id": spec.workflow_id,
            "phase": spec.phase,
            "agent": result.agent_slug,
            "agent_slug": result.agent_slug,
            "provider_slug": provider_slug or "unknown",
            "model_slug": model_slug or t.model or "",
            "verify_refs": spec.verify_refs,
            "run_id": getattr(self, "_run_id", ""),
            "workflow_id": getattr(self, "_workflow_id", ""),
            "request_id": getattr(self, "_request_id", ""),
            "status": result.status,
            "failure_code": failure_code or (f"exit_{result.exit_code}" if result.status == "failed" else ""),
            "failure_category": failure_code or "",
            "exit_code": result.exit_code,
            "duration_seconds": result.duration_seconds,
            "latency_ms": int(result.duration_seconds * 1000),
            "verify_passed": result.verify_passed,
            "retry_count": result.retry_count,
            "finished_at": now.isoformat(),
            "timestamp": now.isoformat(),
            "input_tokens": t.input_tokens,
            "output_tokens": t.output_tokens,
            "cache_read_tokens": t.cache_read_tokens,
            "cache_creation_tokens": t.cache_creation_tokens,
            "cost_usd": t.cost_usd,
            "total_cost_usd": t.cost_usd,
            "model": t.model,
            "num_turns": t.num_turns,
            "duration_api_ms": t.duration_api_ms,
            "tool_use": t.tool_use,
            "stderr": (result.stderr or "")[-2000:],  # last 2KB of stderr
            "workspace_root": workspace_root,
            "workspace_ref": workspace_ref or None,
            "runtime_profile_ref": runtime_profile_ref or None,
            "write_scope": write_scope or None,
            "workspace_provenance": workspace_provenance,
            "git_provenance": git_provenance,
            "write_manifest": write_manifest,
            "mutation_provenance": mutation_provenance,
        }

        receipt_ref = f"rcpt_{uuid.uuid4().hex[:12]}"
        receipt["receipt_id"] = receipt_ref
        receipt["attempt_no"] = max(1, int(result.retry_count) + 1)
        receipt["started_at"] = started_at.isoformat()
        if self._pg_conn is not None and hasattr(self, '_run_id'):
            from runtime.receipt_store import write_receipt as _write_pg_receipt

            try:
                receipt_write = _write_pg_receipt(receipt, conn=self._pg_conn)
            except Exception as exc:
                reason_code = str(getattr(exc, "reason_code", "") or "").strip() or None
                raise WorkflowReceiptPersistenceError(
                    run_id=str(getattr(self, "_run_id", "") or ""),
                    job_label=result.job_label,
                    reason_code=reason_code,
                ) from exc
            transition_seq = int(receipt_write.get("transition_seq") or 0)
            if transition_seq:
                receipt["transition_seq"] = transition_seq
            receipt_ref = str(receipt_write.get("receipt_id") or receipt_ref)

        # Write output to file if job produced stdout
        output_path = None
        output_text = prepare_output_artifact(result.stdout) if result.stdout else ""
        if output_text:
            output_dir = os.path.join(os.path.dirname(self._receipts_dir), "workflow_outputs")
            os.makedirs(output_dir, exist_ok=True)
            output_filename = f"{now.strftime('%Y%m%dT%H%M%S')}_{result.job_label}.md"
            output_path = os.path.join(output_dir, output_filename)
            with open(output_path, "w", encoding="utf-8") as fh:
                fh.write(output_text)
            receipt["output_path"] = output_path

        # Also save to Postgres platform_registry for searchability
        if output_text and self._pg_conn is not None:
            try:
                import hashlib
                rid = f"dispatch_output.{hashlib.sha256(f'{result.job_label}{now.isoformat()}'.encode()).hexdigest()[:12]}"
                self._pg_conn.execute(
                    """INSERT INTO platform_registry (registry_id, kind, name, category, content, metadata, source_path, created_at, updated_at)
                    VALUES ($1, 'dispatch_output', $2, $3, $4, $5::jsonb, $6, $7, $8)
                    ON CONFLICT (registry_id) DO NOTHING""",
                    rid, result.job_label, spec.phase, output_text,
                    json.dumps(receipt), output_path or "", now, now,
                )
            except Exception as exc:
                self._log_advisory_failure(
                    component="platform_registry",
                    reason_code="workflow_runner.platform_registry_write_failed",
                    job_label=result.job_label,
                    exc=exc,
                    details={
                        "registry_kind": "dispatch_output",
                        "output_path": output_path or "",
                    },
                )

        obs_hub = getattr(self, "_obs_hub", None)
        if obs_hub is not None:
            try:
                obs_hub.ingest_receipt(receipt)
            except Exception as exc:
                # Observability is advisory; receipt durability already happened.
                self._log_advisory_failure(
                    component="observability_ingest",
                    reason_code="workflow_runner.observability_ingest_failed",
                    job_label=result.job_label,
                    exc=exc,
                )

        return receipt_ref
