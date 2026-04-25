"""Job dependency management for workflow chains.

Enables job-to-job dependencies: "run job B only after job A succeeds."
Separate from multi-node workflows — these are independent jobs with
cross-job dependencies resolved at execution time.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Status constants
# ---------------------------------------------------------------------------

CONDITION_SUCCEEDED = "succeeded"
CONDITION_COMPLETED = "completed"
CONDITION_ANY = "any"

_VALID_CONDITIONS = frozenset({CONDITION_SUCCEEDED, CONDITION_COMPLETED, CONDITION_ANY})

# ---------------------------------------------------------------------------
# Domain object
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class JobDependency:
    """Immutable record of one job depending on another."""

    job_id: str
    depends_on_job_id: str
    condition: str  # "succeeded", "completed", or "any"
    created_at: datetime

    def __post_init__(self) -> None:
        if self.condition not in _VALID_CONDITIONS:
            raise ValueError(
                f"condition must be one of {sorted(_VALID_CONDITIONS)}, "
                f"got: {self.condition}"
            )

    def to_dict(self) -> dict[str, Any]:
        return {
            "job_id": self.job_id,
            "depends_on_job_id": self.depends_on_job_id,
            "condition": self.condition,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> JobDependency:
        return cls(
            job_id=d["job_id"],
            depends_on_job_id=d["depends_on_job_id"],
            condition=d["condition"],
            created_at=datetime.fromisoformat(d["created_at"]),
        )


# ---------------------------------------------------------------------------
# Dependency resolver
# ---------------------------------------------------------------------------


class DependencyResolver:
    """In-memory resolver for job dependencies with persistence to disk."""

    def __init__(self) -> None:
        # Map: job_id -> list of job_ids it depends on (with their conditions)
        self._dependencies: dict[str, list[tuple[str, str]]] = {}

    def add_dependency(
        self,
        job_id: str,
        depends_on: str,
        *,
        condition: str = CONDITION_SUCCEEDED,
    ) -> None:
        """Register that job_id depends on depends_on succeeding.

        Parameters
        ----------
        job_id:
            The job that depends on another.
        depends_on:
            The job that must complete first.
        condition:
            When the dependency is satisfied:
            - "succeeded": depends_on must have status "succeeded"
            - "completed": depends_on can have any terminal status
            - "any": dependency always satisfied once depends_on exists
        """
        if condition not in _VALID_CONDITIONS:
            raise ValueError(
                f"condition must be one of {sorted(_VALID_CONDITIONS)}, "
                f"got: {condition}"
            )

        if job_id == depends_on:
            raise ValueError("a job cannot depend on itself")

        if job_id not in self._dependencies:
            self._dependencies[job_id] = []

        # Avoid duplicate dependencies
        for dep_id, _ in self._dependencies[job_id]:
            if dep_id == depends_on:
                _log.debug(
                    "dependency_resolver: job_id=%s already depends on %s",
                    job_id,
                    depends_on,
                )
                return

        self._dependencies[job_id].append((depends_on, condition))
        _log.debug(
            "dependency_resolver: added dependency job_id=%s depends_on=%s condition=%s",
            job_id,
            depends_on,
            condition,
        )

    def is_ready(
        self,
        job_id: str,
        *,
        completed_jobs: dict[str, str],
    ) -> bool:
        """Check if all dependencies of job_id are satisfied.

        Parameters
        ----------
        job_id:
            The job to check.
        completed_jobs:
            Map of job_id -> status for all completed jobs.

        Returns
        -------
        bool:
            True if all dependencies are satisfied (or job has no dependencies).
        """
        if job_id not in self._dependencies:
            return True

        for dep_id, condition in self._dependencies[job_id]:
            if dep_id not in completed_jobs:
                return False

            dep_status = completed_jobs[dep_id]

            if condition == CONDITION_ANY:
                # Always satisfied once dependency exists
                continue
            elif condition == CONDITION_COMPLETED:
                # Must be in a terminal state (any terminal status)
                if dep_status not in {"succeeded", "failed", "cancelled"}:
                    return False
            elif condition == CONDITION_SUCCEEDED:
                # Must have succeeded specifically
                if dep_status != "succeeded":
                    return False

        return True

    def blocked_by(
        self,
        job_id: str,
        *,
        completed_jobs: dict[str, str],
    ) -> list[str]:
        """Return list of unsatisfied dependency job_ids for job_id.

        Parameters
        ----------
        job_id:
            The job to check.
        completed_jobs:
            Map of job_id -> status for all completed jobs.

        Returns
        -------
        list[str]:
            List of job_ids that are blocking job_id (empty if ready).
        """
        if job_id not in self._dependencies:
            return []

        blocked: list[str] = []
        for dep_id, condition in self._dependencies[job_id]:
            if dep_id not in completed_jobs:
                blocked.append(dep_id)
                continue

            dep_status = completed_jobs[dep_id]

            if condition == CONDITION_ANY:
                # Always satisfied once dependency exists
                continue
            elif condition == CONDITION_COMPLETED:
                # Must be in a terminal state
                if dep_status not in {"succeeded", "failed", "cancelled"}:
                    blocked.append(dep_id)
            elif condition == CONDITION_SUCCEEDED:
                # Must have succeeded specifically
                if dep_status != "succeeded":
                    blocked.append(dep_id)

        return blocked

    def dependency_graph(self) -> dict[str, list[str]]:
        """Return full dependency graph for visualization.

        Returns
        -------
        dict[str, list[str]]:
            Map of job_id -> list of job_ids it depends on (without conditions).
        """
        return {
            job_id: [dep_id for dep_id, _ in deps]
            for job_id, deps in self._dependencies.items()
        }

    def save(self, path: Path | str) -> None:
        """Persist dependency graph to JSON file.

        Parameters
        ----------
        path:
            Where to write the JSON file.
        """
        path = Path(path) if isinstance(path, str) else path
        path.parent.mkdir(parents=True, exist_ok=True)

        # Convert to serializable format
        data = {
            "dependencies": [
                {
                    "job_id": job_id,
                    "depends_on": dep_id,
                    "condition": condition,
                }
                for job_id, deps in self._dependencies.items()
                for dep_id, condition in deps
            ],
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }

        with open(path, "w") as f:
            json.dump(data, f, indent=2)
        _log.debug("dependency_resolver: saved to %s", path)

    @classmethod
    def load(cls, path: Path | str) -> DependencyResolver:
        """Load dependency graph from JSON file.

        Parameters
        ----------
        path:
            Where to read the JSON file.

        Returns
        -------
        DependencyResolver:
            Loaded resolver with all dependencies restored.

        Raises
        ------
        FileNotFoundError:
            If the file does not exist.
        """
        path = Path(path) if isinstance(path, str) else path

        if not path.exists():
            _log.debug("dependency_resolver: file not found, returning empty: %s", path)
            return cls()

        with open(path) as f:
            data = json.load(f)

        resolver = cls()
        for dep in data.get("dependencies", []):
            resolver.add_dependency(
                dep["job_id"],
                dep["depends_on"],
                condition=dep.get("condition", CONDITION_SUCCEEDED),
            )
        _log.debug("dependency_resolver: loaded from %s", path)
        return resolver


# ---------------------------------------------------------------------------
# Job submission helpers
# ---------------------------------------------------------------------------


def submit_chain(
    specs: list[Any],
    *,
    sequential: bool = True,
) -> list[str]:
    """Submit multiple jobs where each depends on the previous one succeeding.

    This is a convenience function for creating sequential workflows. Each job
    in the chain will wait for the previous job to succeed before starting.

    Parameters
    ----------
    specs:
        List of WorkflowSpec instances to submit in order.
    sequential:
        If True (default), each job depends on the previous succeeding.
        If False, all jobs are submitted independently (no chain).

    Returns
    -------
    list[str]:
        List of workflow job ids in the same order as specs.

    Notes
    -----
    This function submits one workflow to the unified runtime, then
    returns the created workflow job ids in order.
    """
    try:
        from storage.postgres.connection import SyncPostgresConnection, get_workflow_pool
        from runtime.control_commands import submit_workflow_command

        jobs: list[dict[str, Any]] = []
        for index, spec in enumerate(specs):
            provider_slug = getattr(spec, "provider_slug", None)
            model_slug = getattr(spec, "model_slug", None)
            task_type = getattr(spec, "task_type", None) or "build"
            if provider_slug and model_slug:
                agent = f"{provider_slug}/{model_slug}"
            else:
                agent = f"auto/{task_type}"

            label = getattr(spec, "label", None) or f"job_{index + 1}"
            job = {
                "label": label,
                "agent": agent,
                "prompt": getattr(spec, "prompt", ""),
                "read_scope": list(getattr(spec, "scope_read", []) or []),
                "write_scope": list(getattr(spec, "scope_write", []) or []),
            }
            if sequential and index > 0:
                job["depends_on"] = [jobs[index - 1]["label"]]
            jobs.append(job)

        suffix = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')
        workflow_spec = {
            "name": f"dependency_chain_{suffix}",
            "workflow_id": f"workflow.dependency_chain.{suffix}",
            "phase": "build",
            "jobs": jobs,
        }

        conn = SyncPostgresConnection(get_workflow_pool())
        result = submit_workflow_command(
            conn,
            requested_by_kind="runtime",
            requested_by_ref="job_dependencies.submit_chain",
            inline_spec=workflow_spec,
            spec_name=workflow_spec["name"],
            total_jobs=len(jobs),
        )
        if result.get("error") or not result.get("run_id"):
            raise RuntimeError(str(result.get("error") or result))
        job_rows = conn.execute(
            """SELECT id
               FROM workflow_jobs
               WHERE run_id = $1
               ORDER BY created_at, id""",
            result["run_id"],
        )
        job_ids = [str(row["id"]) for row in (job_rows or [])]
    except Exception as exc:
        _log.warning("job_dependencies: failed to submit workflow chain: %s", exc)
        return []

    if not sequential or len(job_ids) < 2:
        return job_ids

    # Register chain dependencies
    resolver = DependencyResolver()
    for i in range(1, len(job_ids)):
        resolver.add_dependency(
            job_ids[i],
            job_ids[i - 1],
            condition=CONDITION_SUCCEEDED,
        )

    # Save to disk
    artifacts_dir = Path(__file__).parent.parent / "artifacts"
    resolver.save(artifacts_dir / "job_dependencies.json")

    _log.info(
        "job_dependencies: submitted chain of %d workflow jobs (root=%s)",
        len(job_ids),
        job_ids[0],
    )
    return job_ids
