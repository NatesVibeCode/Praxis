"""REST API surface for the Praxis Engine workflow platform.

Exposes all CLI-backed functions as HTTP endpoints so UIs and external
clients can access the same functionality without shelling out.

Dependencies are declared in ``requirements.runtime.txt`` and enforced by
``surfaces.api.server`` before the ASGI app boots.

Launch:
    python -m surfaces.api.server --host 0.0.0.0 --port 8420
    workflow api

The supported product front door remains ``./scripts/praxis launch``.

The module-level ``app`` object is the canonical ASGI app.
"""

from __future__ import annotations

from collections import Counter
from contextlib import asynccontextmanager
import dataclasses
import io
import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.routing import APIRoute
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from adapters.provider_registry import default_llm_adapter_type, default_provider_slug
from contracts.domain import validate_workflow_request
from runtime.native_authority import default_native_authority_refs
from runtime.workflow_graph_compiler import compile_graph_workflow_request, spec_uses_graph_runtime
from .handlers._subsystems import _Subsystems
from .handlers import (
    handle_delete_request,
    handle_get_request,
    handle_post_request,
    handle_put_request,
    path_is_known,
)
from .handlers._shared import REPO_ROOT
from .handlers import workflow_launcher as launcher_handlers
from .handlers.workflow_run import _submit_workflow_via_service_bus

__all__ = ["app"]

logger = logging.getLogger(__name__)


def _unique_operation_id(route: APIRoute) -> str:
    methods = "_".join(
        method.lower()
        for method in sorted(route.methods or set())
        if method != "HEAD"
    )
    path = route.path_format.strip("/") or "root"
    normalized_path = (
        path.replace("/", "_")
        .replace("{", "")
        .replace("}", "")
        .replace(":", "_")
    )
    return f"{route.name}_{normalized_path}_{methods}"


def _ensure_shared_subsystems(target_app: FastAPI) -> _Subsystems | None:
    """Instantiate the shared subsystem container once for API startup wiring."""
    subsystems = getattr(target_app.state, "shared_subsystems", None)
    if subsystems is not None:
        return subsystems
    try:
        subsystems = _Subsystems()
    except Exception:
        logger.exception("failed to initialize shared API subsystems")
        return None
    target_app.state.shared_subsystems = subsystems
    return subsystems


@asynccontextmanager
async def _app_lifespan(target_app: FastAPI):
    _ensure_shared_subsystems(target_app)
    yield


# ---------------------------------------------------------------------------
# App instance
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Praxis Engine API",
    description="HTTP surface for Praxis Engine runtime functions.",
    version="1.0.0",
    lifespan=_app_lifespan,
    generate_unique_id_function=_unique_operation_id,
)

from .handlers.webhook_ingest import webhook_ingest_router
app.include_router(webhook_ingest_router)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve the launcher app assets from the built SPA bundle.
_APP_DIST_DIR = Path(__file__).resolve().parent.parent / "app" / "dist"
_APP_ASSETS_DIR = _APP_DIST_DIR / "assets"
app.mount(
    "/app/assets",
    StaticFiles(directory=str(_APP_ASSETS_DIR), check_dir=False),
    name="launcher-app-assets",
)

_DEFAULT_WORKSPACE_REF, _DEFAULT_RUNTIME_PROFILE_REF = default_native_authority_refs()

_DEFAULT_PROVIDER_SLUG = default_provider_slug()
_DEFAULT_LLM_ADAPTER = default_llm_adapter_type()


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class WorkflowRunRequest(BaseModel):
    """Body for POST /api/workflow-runs."""

    prompt: str
    provider_slug: str = _DEFAULT_PROVIDER_SLUG
    model_slug: str | None = None
    tier: str | None = None
    adapter_type: str = _DEFAULT_LLM_ADAPTER
    timeout: int = 300
    workdir: str | None = None
    max_tokens: int = 4096
    temperature: float = 0.0
    label: str | None = None
    workspace_ref: str = _DEFAULT_WORKSPACE_REF
    runtime_profile_ref: str = _DEFAULT_RUNTIME_PROFILE_REF
    system_prompt: str | None = None
    context_sections: list[dict[str, str]] | None = None
    max_retries: int = 0
    scope_read: list[str] | None = None
    scope_write: list[str] | None = None
    allowed_tools: list[str] | None = None
    verify_refs: list[str] | None = None
    definition_revision: str | None = None
    plan_revision: str | None = None
    packet_provenance: dict[str, Any] | None = None
    output_schema: dict | None = None
    max_context_tokens: int | None = None
    persist: bool = True
    capabilities: list[str] | None = None
    use_cache: bool = False
    task_type: str | None = None
    skip_auto_review: bool = False
    reviews_workflow_id: str | None = None
    review_target_modules: list[str] | None = None


class WorkflowBatchRequest(BaseModel):
    """Body for POST /api/workflow-runs/batch."""

    specs: list[WorkflowRunRequest]
    max_workers: int | None = None


class WorkflowStepRequest(BaseModel):
    """One step in a pipeline request."""

    name: str
    prompt: str
    adapter_type: str = _DEFAULT_LLM_ADAPTER
    provider_slug: str | None = None
    model_slug: str | None = None
    tier: str | None = None
    max_tokens: int = 4096
    depends_on: list[str] = []
    fan_out: bool = False
    fan_out_prompt: str | None = None
    fan_out_max_parallel: int = 4


class PipelineRequest(BaseModel):
    """Body for POST /api/pipeline."""

    steps: list[WorkflowStepRequest]
    timeout: int = 600


class QueueSubmitRequest(BaseModel):
    """Body for POST /api/queue/submit."""

    spec: WorkflowRunRequest
    priority: int = 100
    max_attempts: int = 1


class LauncherRecoverRequest(BaseModel):
    """Body for POST /api/launcher/recover."""

    action: str
    service: str | None = None
    run_id: str | None = None
    open_browser: bool = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _spec_from_request(req: WorkflowRunRequest):
    """Convert a WorkflowRunRequest pydantic model into a WorkflowSpec dataclass."""
    from runtime.workflow import WorkflowSpec

    return WorkflowSpec(
        prompt=req.prompt,
        provider_slug=req.provider_slug,
        model_slug=req.model_slug,
        tier=req.tier,
        adapter_type=req.adapter_type,
        timeout=req.timeout,
        workdir=req.workdir,
        max_tokens=req.max_tokens,
        temperature=req.temperature,
        label=req.label,
        workspace_ref=req.workspace_ref,
        runtime_profile_ref=req.runtime_profile_ref,
        system_prompt=req.system_prompt,
        context_sections=req.context_sections,
        max_retries=req.max_retries,
        scope_read=req.scope_read,
        scope_write=req.scope_write,
        allowed_tools=req.allowed_tools,
        verify_refs=req.verify_refs,
        definition_revision=req.definition_revision,
        plan_revision=req.plan_revision,
        packet_provenance=req.packet_provenance,
        output_schema=req.output_schema,
        max_context_tokens=req.max_context_tokens,
        persist=req.persist,
        capabilities=req.capabilities,
        use_cache=req.use_cache,
        task_type=req.task_type,
        skip_auto_review=req.skip_auto_review,
        reviews_workflow_id=req.reviews_workflow_id,
        review_target_modules=req.review_target_modules,
    )


def _iso_or_none(value: Any) -> str | None:
    return value.isoformat() if value else None


class _BufferedWriter:
    def __init__(self) -> None:
        self._chunks: list[bytes] = []

    def write(self, data: bytes) -> int:
        self._chunks.append(data)
        return len(data)

    def flush(self) -> None:
        return None

    def read(self) -> bytes:
        return b"".join(self._chunks)


class _FastAPIHandlerAdapter:
    """Bridge BaseHTTPRequestHandler-style route handlers onto FastAPI."""

    def __init__(self, request: Request, subsystems: _Subsystems, body: bytes) -> None:
        self.headers = request.headers
        self.path = request.url.path
        if request.url.query:
            self.path = f"{self.path}?{request.url.query}"
        self.rfile = io.BytesIO(body)
        self.subsystems = subsystems
        self._response_headers: dict[str, str] = {
            "Access-Control-Allow-Origin": "*",
        }
        self._status_code = 200
        self._writer = _BufferedWriter()

    @property
    def wfile(self) -> _BufferedWriter:
        return self._writer

    def _send_json(self, status: int, payload: Any) -> None:
        body = json.dumps(payload, default=str).encode("utf-8")
        self._status_code = status
        self._response_headers["Content-Type"] = "application/json"
        self._response_headers["Content-Length"] = str(len(body))
        self._writer = _BufferedWriter()
        self._writer.write(body)

    def _send_bytes(
        self,
        status: int,
        payload: bytes,
        *,
        content_type: str = "application/octet-stream",
        content_disposition: str | None = None,
    ) -> None:
        self._status_code = status
        self._response_headers["Content-Type"] = content_type
        self._response_headers["Content-Length"] = str(len(payload))
        if content_disposition:
            self._response_headers["Content-Disposition"] = content_disposition
        self._writer = _BufferedWriter()
        self._writer.write(payload)

    def send_response(self, status: int) -> None:
        self._status_code = status

    def send_header(self, name: str, value: str) -> None:
        self._response_headers[name] = value

    def end_headers(self) -> None:
        return None

    def to_response(self) -> Response:
        body = self._writer.read()
        headers = dict(self._response_headers)
        media_type = headers.pop("Content-Type", None)
        headers.pop("Content-Length", None)
        return Response(
            content=body,
            status_code=self._status_code,
            media_type=media_type,
            headers=headers,
        )


def _shared_pg_conn():
    subsystems = _ensure_shared_subsystems(app)
    if subsystems is None:
        raise HTTPException(status_code=503, detail="shared subsystems unavailable")
    return subsystems.get_pg_conn()


async def _route_to_handler(request: Request) -> Response:
    """Dispatch a request through the unified handler system.

    This replaces the legacy bridge — same handler functions,
    cleaner dispatch. Every route that uses this is explicitly
    registered as a FastAPI endpoint (no catch-all wildcards).
    """
    subsystems = _ensure_shared_subsystems(app)
    if subsystems is None:
        raise HTTPException(status_code=503, detail="shared subsystems unavailable")

    path = request.url.path.rstrip("/") or "/"
    body = await request.body()
    adapter = _FastAPIHandlerAdapter(request, subsystems, body)

    if request.method == "GET":
        handled = handle_get_request(adapter, path)
    elif request.method == "POST":
        handled = handle_post_request(adapter, path)
    elif request.method == "PUT":
        handled = handle_put_request(adapter, path)
    elif request.method == "DELETE":
        handled = handle_delete_request(adapter, path)
    else:
        handled = False

    if not handled:
        adapter._send_json(404, {"error": f"Not found: {path}"})

    return adapter.to_response()


async def _dispatch_standard_route(request: Request) -> Response:
    """Dispatch a standard-route handler: handler(subsystems, body) -> dict."""
    from .handlers._shared import _ClientError

    subsystems = _ensure_shared_subsystems(app)
    if subsystems is None:
        raise HTTPException(status_code=503, detail="shared subsystems unavailable")

    from .handlers import ROUTES
    path = request.url.path.rstrip("/") or "/"
    handler = ROUTES.get(path)
    if handler is None:
        raise HTTPException(status_code=404, detail=f"Not found: {path}")

    try:
        body_bytes = await request.body()
        body = json.loads(body_bytes) if body_bytes else {}
        if not isinstance(body, dict):
            return JSONResponse({"error": "Request body must be a JSON object"}, status_code=400)
    except (json.JSONDecodeError, ValueError) as exc:
        return JSONResponse({"error": f"Invalid JSON: {exc}"}, status_code=400)

    try:
        result = handler(subsystems, body)
        return JSONResponse(result, status_code=200)
    except _ClientError as exc:
        return JSONResponse({"error": str(exc)}, status_code=400)
    except Exception as exc:
        import traceback
        return JSONResponse(
            {"error": f"{type(exc).__name__}: {exc}", "traceback": traceback.format_exc()},
            status_code=500,
        )


def _read_job_output(output_path: str | None, stdout_preview: str | None) -> tuple[str, str]:
    preview = stdout_preview or ""
    if not output_path:
        return preview, "preview"

    path = Path(output_path)
    if not path.is_file():
        return preview, "preview"

    try:
        return path.read_text(encoding="utf-8"), "file"
    except OSError:
        return preview, "preview"


def _serialize_run_job(row: dict[str, Any]) -> dict[str, Any]:
    status = row["status"]
    output_preview = row.get("stdout_preview") or ""
    return {
        "id": int(row["id"]),
        "label": row["label"],
        "status": status,
        "job_type": row.get("job_type") or "workflow",
        "phase": row.get("phase") or "build",
        "agent_slug": row.get("agent_slug"),
        "resolved_agent": row.get("resolved_agent"),
        "integration_id": row.get("integration_id"),
        "integration_action": row.get("integration_action"),
        "integration_args": row.get("integration_args") or {},
        "attempt": int(row.get("attempt") or 0),
        "duration_ms": int(row.get("duration_ms") or 0),
        "cost_usd": float(row.get("cost_usd") or 0),
        "exit_code": row.get("exit_code"),
        "last_error_code": row.get("last_error_code"),
        "stdout_preview": output_preview,
        "has_output": bool(row.get("output_path") or output_preview),
        "started_at": _iso_or_none(row.get("started_at")),
        "finished_at": _iso_or_none(row.get("finished_at")),
        "created_at": _iso_or_none(row.get("created_at")),
    }


def _launcher_index_response() -> FileResponse | JSONResponse:
    index_path = _APP_DIST_DIR / "index.html"
    if not index_path.is_file():
        return JSONResponse(
            status_code=503,
            content={
                "ok": False,
                "error": "launcher_build_missing",
                "detail": "Launcher build missing. Run ./scripts/praxis launch",
                "launch_url": "http://127.0.0.1:8420/app",
            },
        )
    return FileResponse(index_path)


# ---------------------------------------------------------------------------
# Status endpoints (native FastAPI — richer than handler equivalents)
# ---------------------------------------------------------------------------


@app.post("/api/launcher/recover")
def launcher_recover(req: LauncherRecoverRequest) -> JSONResponse:
    """Run bounded launcher recovery through the preferred launcher command."""
    try:
        status_code, payload = launcher_handlers.launcher_recover_payload(
            action=req.action,
            service=req.service,
            run_id=req.run_id,
            open_browser=req.open_browser,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except launcher_handlers.LauncherAuthorityError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return JSONResponse(status_code=status_code, content=payload)


@app.get("/api/dashboard")
def get_dashboard() -> dict[str, Any]:
    """Return the full consolidated dashboard JSON."""
    from runtime.dashboard import build_dashboard as _build_dashboard

    return _build_dashboard()


@app.get("/api/leaderboard")
def get_leaderboard() -> list[dict[str, Any]]:
    """Return the agent leaderboard as a list of AgentScore dicts."""
    from runtime.leaderboard import build_leaderboard as _build_leaderboard

    scores = _build_leaderboard()
    return [dataclasses.asdict(s) for s in scores]


@app.get("/api/runs/recent")
def list_recent_runs(
    limit: int = Query(default=20, ge=1, le=100),
) -> list[dict[str, Any]]:
    """Return recent workflow runs with job progress summaries."""
    conn = _shared_pg_conn()
    rows = conn.execute(
        """SELECT r.run_id,
                  COALESCE(r.request_envelope->>'name', r.workflow_id) AS spec_name,
                  r.current_state AS status,
                  COALESCE(NULLIF(r.request_envelope->>'total_jobs', ''), '0')::int AS total_jobs,
                  r.requested_at AS created_at,
                  r.finished_at,
                  COUNT(j.id) FILTER (WHERE j.status IN ('succeeded','failed','dead_letter')) as completed_jobs,
                  COALESCE(SUM(j.cost_usd), 0) as total_cost
           FROM workflow_runs r
           LEFT JOIN workflow_jobs j ON j.run_id = r.run_id
           GROUP BY r.run_id, r.workflow_id, r.request_envelope, r.current_state, r.requested_at, r.finished_at
           ORDER BY r.requested_at DESC
           LIMIT $1""",
        limit,
    )
    if not rows:
        return []
    return [
        {
            "run_id": r["run_id"],
            "spec_name": r["spec_name"],
            "status": r["status"],
            "total_jobs": r["total_jobs"],
            "completed_jobs": int(r["completed_jobs"]),
            "total_cost": float(r["total_cost"]),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
            "finished_at": r["finished_at"].isoformat() if r["finished_at"] else None,
        }
        for r in rows
    ]


@app.get("/api/runs/{run_id}")
def get_run_detail(run_id: str) -> dict[str, Any]:
    """Return one workflow run with ordered job details."""
    conn = _shared_pg_conn()
    run_rows = conn.execute(
        """SELECT r.run_id,
                  COALESCE(r.request_envelope->>'name', r.workflow_id) AS spec_name,
                  r.current_state AS status,
                  COALESCE(NULLIF(r.request_envelope->>'total_jobs', ''), '0')::int AS total_jobs,
                  r.requested_at AS created_at,
                  r.finished_at,
                  COUNT(j.id) FILTER (WHERE j.status IN ('succeeded','failed','dead_letter')) as completed_jobs,
                  COALESCE(SUM(j.cost_usd), 0) as total_cost,
                  COALESCE(SUM(j.duration_ms), 0) as total_duration_ms
           FROM workflow_runs r
           LEFT JOIN workflow_jobs j ON j.run_id = r.run_id
           WHERE r.run_id = $1
           GROUP BY r.run_id, r.workflow_id, r.request_envelope, r.current_state, r.requested_at, r.finished_at""",
        run_id,
    )
    if not run_rows:
        raise HTTPException(status_code=404, detail=f"Run not found: {run_id}")

    job_rows = conn.execute(
        """SELECT id, label, status, job_type, phase, agent_slug, resolved_agent,
                  integration_id, integration_action, integration_args, attempt,
                  duration_ms, cost_usd, exit_code, last_error_code, stdout_preview,
                  output_path, created_at, started_at, finished_at
           FROM workflow_jobs
           WHERE run_id = $1
           ORDER BY id""",
        run_id,
    )

    run = run_rows[0]
    jobs = [_serialize_run_job(dict(row)) for row in (job_rows or [])]
    if not jobs:
        jobs = _load_run_jobs_from_status_authority(conn, run_id)
    summary = _build_run_summary(conn, run_id, jobs)
    graph = _build_run_graph(conn, run_id, jobs)

    return {
        "run_id": run["run_id"],
        "spec_name": run["spec_name"],
        "status": run["status"],
        "total_jobs": int(run["total_jobs"] or len(jobs)),
        "completed_jobs": int(run["completed_jobs"] or 0),
        "total_cost": float(run["total_cost"] or 0),
        "total_duration_ms": int(run["total_duration_ms"] or 0),
        "created_at": _iso_or_none(run["created_at"]),
        "finished_at": _iso_or_none(run["finished_at"]),
        "jobs": jobs,
        "summary": summary,
        "graph": graph,
    }


def _load_run_jobs_from_status_authority(conn: Any, run_id: str) -> list[dict[str, Any]]:
    try:
        from runtime.workflow.unified import get_run_status

        status = get_run_status(conn, run_id)
    except Exception:
        return []

    if not isinstance(status, dict):
        return []

    jobs = status.get("jobs")
    if not isinstance(jobs, list):
        return []
    return [
        _serialize_run_job(dict(row))
        for row in jobs
        if isinstance(row, dict) and row.get("label")
    ]


def _parse_json_mapping(raw: object) -> dict[str, Any] | None:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return dict(parsed)
    return None


def _load_run_spec_snapshot(conn: Any, run_id: str) -> dict[str, Any] | None:
    try:
        rows = conn.execute(
            "SELECT request_envelope->'spec_snapshot' AS spec_snapshot FROM workflow_runs WHERE run_id = $1",
            run_id,
        )
    except Exception:
        return None
    if not rows:
        return None
    return _parse_json_mapping(rows[0].get("spec_snapshot"))


def _load_operator_frame_counts(conn: Any, run_id: str) -> dict[str, Counter[str]]:
    try:
        rows = conn.execute(
            """SELECT operator_frame_id, node_id, frame_state
               FROM run_operator_frames
               WHERE run_id = $1
               ORDER BY node_id, operator_frame_id""",
            run_id,
        )
    except Exception:
        return {}

    counts_by_node: dict[str, Counter[str]] = {}
    for row in rows or []:
        node_id = str(row.get("node_id") or "").strip()
        frame_state = str(row.get("frame_state") or "").strip()
        if not node_id or not frame_state:
            continue
        counts_by_node.setdefault(node_id, Counter())[frame_state] += 1
    return counts_by_node


def _graph_condition_for_branch(
    *,
    operator: dict[str, Any],
    branch: str,
) -> dict[str, Any] | None:
    kind = str(operator.get("kind") or "").strip()
    if kind == "if":
        predicate = operator.get("predicate")
        if not isinstance(predicate, dict):
            return None
        field = predicate.get("field")
        op = predicate.get("op", "equals")
        value = predicate.get("value")
        if not isinstance(field, str) or not field.strip() or not isinstance(op, str) or not op.strip():
            return None
        if branch == "then":
            return {"field": field, "op": op, "value": value}
        if branch == "else":
            inverse = {
                "equals": "not_equals",
                "not_equals": "equals",
                "in": "not_in",
                "not_in": "in",
                "eq": "neq",
                "neq": "eq",
                "gt": "lte",
                "gte": "lt",
                "lt": "gte",
                "lte": "gt",
            }.get(op)
            if inverse:
                return {"field": field, "op": inverse, "value": value}
            return {"branch": branch}
        return None

    if kind == "switch":
        field = operator.get("field")
        if not isinstance(field, str) or not field.strip():
            return None
        for case in operator.get("cases") or []:
            if not isinstance(case, dict):
                continue
            if str(case.get("branch") or "").strip() != branch:
                continue
            return {"field": field, "op": "equals", "value": case.get("value")}
        return {"branch": branch}

    return None


def _build_run_graph_from_graph_spec(
    *,
    conn: Any,
    run_id: str,
    jobs: list[dict[str, Any]],
    spec_snapshot: dict[str, Any],
) -> dict[str, Any] | None:
    try:
        compiled_request = compile_graph_workflow_request(spec_snapshot, run_id=run_id)
        validation = validate_workflow_request(compiled_request)
    except Exception:
        return None

    if not validation.is_valid:
        return None

    visible_nodes = [
        node
        for node in compiled_request.nodes
        if not node.template_owner_node_id
    ]
    if not visible_nodes:
        return None

    visible_node_ids = {node.node_id for node in visible_nodes}
    job_by_label = {str(job.get("label") or ""): job for job in jobs}
    operator_frame_counts = _load_operator_frame_counts(conn, run_id)

    graph_nodes: list[dict[str, Any]] = []
    for node in sorted(visible_nodes, key=lambda item: (item.position_index, item.node_id)):
        job = job_by_label.get(node.node_id)
        frame_counts = operator_frame_counts.get(node.node_id, Counter())
        total_frames = sum(frame_counts.values())
        running_frames = frame_counts.get("created", 0) + frame_counts.get("running", 0)
        failed_frames = frame_counts.get("failed", 0) + frame_counts.get("cancelled", 0)
        succeeded_frames = frame_counts.get("succeeded", 0)

        status = "pending"
        if job is not None:
            status = str(job.get("status") or "pending")
        elif total_frames:
            if failed_frames:
                status = "failed"
            elif succeeded_frames == total_frames:
                status = "succeeded"
            elif running_frames:
                status = "running"

        node_payload: dict[str, Any] = {
            "id": node.node_id,
            "label": node.node_id,
            "type": "job",
            "adapter": node.adapter_type,
            "position": int(node.position_index),
            "status": status,
        }
        if job is not None:
            node_payload["cost_usd"] = float(job.get("cost_usd") or 0)
            node_payload["duration_ms"] = int(job.get("duration_ms") or 0)
            node_payload["agent"] = job.get("resolved_agent") or job.get("agent_slug")
            node_payload["attempt"] = int(job.get("attempt") or 0)
            if job.get("last_error_code"):
                node_payload["error_code"] = job.get("last_error_code")
        if total_frames:
            node_payload["fan_out"] = {
                "count": total_frames,
                "succeeded": succeeded_frames,
                "failed": failed_frames,
                "running": running_frames,
            }
        graph_nodes.append(node_payload)

    nodes_by_id = {node.node_id: node for node in visible_nodes}
    graph_edges: list[dict[str, Any]] = []
    for edge in compiled_request.edges:
        if edge.template_owner_node_id:
            continue
        if edge.from_node_id not in visible_node_ids or edge.to_node_id not in visible_node_ids:
            continue
        edge_type = edge.edge_type
        condition: dict[str, Any] | None = None
        release_condition = dict(edge.release_condition or {})
        branch = str(release_condition.get("branch") or "").strip()
        if branch:
            source_node = nodes_by_id.get(edge.from_node_id)
            operator = (
                dict(source_node.inputs.get("operator") or {})
                if source_node is not None and isinstance(source_node.inputs.get("operator"), dict)
                else {}
            )
            condition = _graph_condition_for_branch(operator=operator, branch=branch)
            edge_type = "conditional" if condition is not None else edge.edge_type
        graph_edges.append(
            {
                "id": edge.edge_id,
                "from": edge.from_node_id,
                "to": edge.to_node_id,
                "type": edge_type,
                "condition": condition,
                "data_mapping": dict(edge.payload_mapping or {}),
            }
        )

    return {"nodes": graph_nodes, "edges": graph_edges}


def _build_run_graph(conn: Any, run_id: str, jobs: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Build a DAG from the spec_snapshot and runtime job status.

    Reads the original spec's jobs + depends_on from request_envelope.spec_snapshot,
    then annotates each node with runtime status from workflow_jobs.
    Fan-out jobs (replicate: labels like prefix_01, prefix_02) are collapsed.
    """
    spec_snapshot = _load_run_spec_snapshot(conn, run_id)
    if isinstance(spec_snapshot, dict) and spec_uses_graph_runtime(spec_snapshot):
        graph = _build_run_graph_from_graph_spec(
            conn=conn,
            run_id=run_id,
            jobs=jobs,
            spec_snapshot=spec_snapshot,
        )
        if graph is not None:
            return graph

    if not jobs:
        return None
    try:
        spec_jobs_raw = spec_snapshot.get("jobs") if isinstance(spec_snapshot, dict) else None
        if spec_jobs_raw is None:
            return _build_run_graph_from_jobs(jobs)

        spec_jobs = json.loads(spec_jobs_raw) if isinstance(spec_jobs_raw, str) else spec_jobs_raw
        if not isinstance(spec_jobs, list) or not spec_jobs:
            return _build_run_graph_from_jobs(jobs)

        # Build job status lookup
        job_by_label: dict[str, dict[str, Any]] = {}
        for j in jobs:
            job_by_label[j["label"]] = j

        # Detect fan-out groups from runtime jobs
        spec_labels = {sj.get("label") for sj in spec_jobs if sj.get("label")}
        fan_out_groups: dict[str, list[dict[str, Any]]] = {}
        for j in jobs:
            label = j["label"]
            parts = label.rsplit("_", 1)
            if len(parts) == 2 and parts[1].isdigit() and parts[0] in spec_labels:
                fan_out_groups.setdefault(parts[0], []).append(j)

        # Build nodes from spec jobs
        graph_nodes: list[dict[str, Any]] = []
        for i, sj in enumerate(spec_jobs):
            label = sj.get("label") or f"step_{i}"
            node: dict[str, Any] = {
                "id": label,
                "label": label,
                "type": "job",
                "adapter": sj.get("agent") or "auto",
                "position": i,
            }

            if label in fan_out_groups:
                children = fan_out_groups[label]
                succeeded = sum(1 for c in children if c["status"] == "succeeded")
                failed = sum(1 for c in children if c["status"] in ("failed", "dead_letter"))
                running = sum(1 for c in children if c["status"] in ("running", "claimed"))
                node["fan_out"] = {"count": len(children), "succeeded": succeeded, "failed": failed, "running": running}
                node["cost_usd"] = sum(c["cost_usd"] for c in children)
                node["duration_ms"] = max((c["duration_ms"] for c in children), default=0)
                node["status"] = "succeeded" if succeeded == len(children) else "failed" if failed > 0 else "running" if running > 0 else "pending"
            elif label in job_by_label:
                j = job_by_label[label]
                node["status"] = j["status"]
                node["cost_usd"] = j["cost_usd"]
                node["duration_ms"] = j["duration_ms"]
                node["agent"] = j.get("resolved_agent") or j.get("agent_slug")
                node["attempt"] = j.get("attempt", 0)
                if j.get("last_error_code"):
                    node["error_code"] = j["last_error_code"]
            else:
                node["status"] = "pending"

            graph_nodes.append(node)

        # Build edges from depends_on
        graph_edges: list[dict[str, Any]] = []
        for sj in spec_jobs:
            label = sj.get("label") or ""
            deps = sj.get("depends_on") or []
            for dep in deps:
                graph_edges.append({
                    "id": f"edge-{dep}-{label}",
                    "from": dep,
                    "to": label,
                    "type": "after_success",
                })

        return {"nodes": graph_nodes, "edges": graph_edges}
    except Exception:
        return _build_run_graph_from_jobs(jobs)


def _build_run_graph_from_jobs(jobs: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Fallback: build a simple chain graph from runtime jobs when no spec is available."""
    if len(jobs) < 2:
        return None
    nodes = []
    edges = []
    for i, j in enumerate(jobs):
        nodes.append({
            "id": j["label"],
            "label": j["label"],
            "type": "job",
            "adapter": j.get("agent_slug") or "auto",
            "position": i,
            "status": j["status"],
            "cost_usd": j["cost_usd"],
            "duration_ms": j["duration_ms"],
        })
        if i > 0:
            edges.append({
                "id": f"edge-{jobs[i-1]['label']}-{j['label']}",
                "from": jobs[i - 1]["label"],
                "to": j["label"],
                "type": "after_success",
            })
    return {"nodes": nodes, "edges": edges}


def _build_run_summary(conn: Any, run_id: str, jobs: list[dict[str, Any]]) -> str | None:
    """Build a human-readable summary from job submission summaries."""
    try:
        rows = conn.execute(
            """SELECT s.job_label, s.summary
               FROM workflow_job_submissions s
               WHERE s.run_id = $1 AND s.summary IS NOT NULL AND s.summary != ''
               ORDER BY s.attempt_no DESC, s.sealed_at DESC""",
            run_id,
        )
        if rows:
            # Collect the latest summary per job label
            seen: set[str] = set()
            parts: list[str] = []
            for row in rows:
                label = row.get("job_label") or ""
                if label in seen:
                    continue
                seen.add(label)
                summary = (row.get("summary") or "").strip()
                if summary:
                    parts.append(summary)
            if parts:
                return " ".join(parts) if len(parts) <= 2 else "\n".join(f"- {p}" for p in parts)
    except Exception:
        pass

    # Fallback: status-based summary
    if not jobs:
        return None
    done = [j for j in jobs if j.get("status") in ("succeeded", "failed", "dead_letter")]
    if not done:
        return None
    succeeded = sum(1 for j in done if j.get("status") == "succeeded")
    failed = len(done) - succeeded
    labels = [f"{j.get('label', '?')} ({'done' if j.get('status') == 'succeeded' else 'failed'})" for j in done]
    return f"{succeeded} of {len(jobs)} steps completed: {', '.join(labels)}" if failed else f"All {len(jobs)} steps completed successfully."


@app.get("/api/runs/{run_id}/jobs/{job_id}")
def get_run_job_detail(run_id: str, job_id: int) -> dict[str, Any]:
    """Return one workflow job with best-available output content."""
    conn = _shared_pg_conn()
    rows = conn.execute(
        """SELECT id, run_id, label, status, job_type, phase, agent_slug, resolved_agent,
                  integration_id, integration_action, integration_args, attempt,
                  duration_ms, cost_usd, exit_code, last_error_code, stdout_preview,
                  output_path, receipt_id, created_at, started_at, finished_at
           FROM workflow_jobs
           WHERE run_id = $1 AND id = $2
           LIMIT 1""",
        run_id,
        job_id,
    )
    if not rows:
        raise HTTPException(status_code=404, detail=f"Job not found: {run_id}/{job_id}")

    row = dict(rows[0])
    output, output_source = _read_job_output(row.get("output_path"), row.get("stdout_preview"))
    job = _serialize_run_job(row)
    job["output"] = output
    job["output_source"] = output_source
    job["receipt_id"] = row.get("receipt_id")

    if output:
        try:
            job["output_json"] = json.loads(output)
        except json.JSONDecodeError:
            pass

    return job


@app.get("/api/costs")
def get_costs() -> dict[str, Any]:
    """Return the cost summary from the in-memory cost tracker."""
    from runtime.cost_tracker import get_cost_tracker

    return get_cost_tracker().summary()


# ---------------------------------------------------------------------------
# Handler-backed routes — explicit registrations, no catch-all wildcards.
# Each route calls into the unified handler system via _route_to_handler
# or _dispatch_standard_route.
# ---------------------------------------------------------------------------

# -- Workflows (CRUD, build, compile, refine, commit) --
@app.get("/api/workflows")
async def workflows_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/workflows")
async def workflows_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/workflows/{rest_of_path:path}")
async def workflows_path_get(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.put("/api/workflows/{rest_of_path:path}")
async def workflows_path_put(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.delete("/api/workflows/{rest_of_path:path}")
async def workflows_path_delete(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.post("/api/compile")
async def compile_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/refine-definition")
async def refine_definition_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/plan")
async def plan_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/commit")
async def commit_post(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Files --
@app.get("/api/files")
async def files_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/files")
async def files_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/files/{rest_of_path:path}")
async def files_path_get(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.delete("/api/files/{rest_of_path:path}")
async def files_path_delete(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

# -- Object types & objects --
@app.get("/api/object-types")
async def object_types_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/object-types")
async def object_types_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/object-types/{rest_of_path:path}")
async def object_types_path_get(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.get("/api/objects")
async def objects_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/objects")
async def objects_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/objects/{rest_of_path:path}")
async def objects_path_get(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.put("/api/objects/update")
async def objects_update_put(request: Request) -> Response:
    return await _route_to_handler(request)

@app.delete("/api/objects/delete")
async def objects_delete(request: Request) -> Response:
    return await _route_to_handler(request)

# -- References, documents, search --
@app.get("/api/references")
async def references_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/documents")
async def documents_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/documents")
async def documents_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/documents/{doc_id}/attach")
async def documents_attach_post(request: Request, doc_id: str) -> Response:
    return await _route_to_handler(request)

@app.get("/api/source-options")
async def source_options_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/templates")
async def templates_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/search")
async def search_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/registries/search")
async def registries_search_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/intent/analyze")
async def intent_analyze_get(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Models --
@app.get("/api/models")
async def models_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/models/market")
async def models_market_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/models/run")
async def models_run_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/models/runs/{rest_of_path:path}")
async def models_runs_path_post(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.get("/api/models/runs/{rest_of_path:path}")
async def models_runs_path_get(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

# -- Integrations --
@app.get("/api/integrations")
async def integrations_get(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Chat --
@app.get("/api/chat/conversations")
async def chat_conversations_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/chat/conversations")
async def chat_conversations_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/chat/conversations/{conversation_id}")
async def chat_conversation_get(request: Request, conversation_id: str) -> Response:
    return await _route_to_handler(request)

@app.post("/api/chat/conversations/{conversation_id}/messages")
async def chat_messages_post(request: Request, conversation_id: str) -> Response:
    return await _route_to_handler(request)

# -- Triggers --
@app.get("/api/workflow-triggers")
async def workflow_triggers_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/workflow-triggers")
async def workflow_triggers_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.put("/api/workflow-triggers")
async def workflow_triggers_put(request: Request) -> Response:
    return await _route_to_handler(request)

@app.put("/api/workflow-triggers/{rest_of_path:path}")
async def workflow_triggers_path_put(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

@app.post("/api/trigger/{rest_of_path:path}")
async def trigger_post(request: Request, rest_of_path: str) -> Response:
    return await _route_to_handler(request)

# -- Manifests --
@app.post("/api/manifests/generate")
async def manifests_generate_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/manifests/generate-quick")
async def manifests_generate_quick_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/manifests/refine")
async def manifests_refine_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/manifests/save")
async def manifests_save_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/manifests/save-as")
async def manifests_save_as_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/manifests/{manifest_id}")
async def manifests_get(request: Request, manifest_id: str) -> Response:
    return await _route_to_handler(request)

# -- Checkpoints --
@app.post("/api/checkpoints")
async def checkpoints_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/checkpoints/{checkpoint_id}/approve")
async def checkpoints_approve_post(request: Request, checkpoint_id: str) -> Response:
    return await _route_to_handler(request)

@app.get("/api/checkpoints")
async def checkpoints_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/checkpoints/{checkpoint_id}")
async def checkpoints_detail_get(request: Request, checkpoint_id: str) -> Response:
    return await _route_to_handler(request)

# -- Bugs --
@app.get("/api/bugs")
async def bugs_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/bugs/replay-ready")
async def bugs_replay_ready_get(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Workflow execution (browser-initiated, routed through handler system) --
@app.post("/api/workflow-runs")
async def workflow_runs_handler_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/workflow-runs/{run_id}/stream")
async def workflow_runs_stream_get(request: Request, run_id: str) -> Response:
    return await _route_to_handler(request)

@app.get("/api/workflow-runs/{run_id}/status")
async def workflow_runs_status_get(request: Request, run_id: str) -> Response:
    return await _route_to_handler(request)

@app.post("/api/workflows/run")
async def workflows_run_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/workflow-job")
async def workflow_job_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/workflow-status")
async def workflow_status_alias_get(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Launcher --
@app.get("/api/launcher/status")
async def launcher_status_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.post("/api/launcher/recover")
async def launcher_recover_post(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/platform-overview")
async def platform_overview_get(request: Request) -> Response:
    return await _route_to_handler(request)

@app.get("/api/workflow-templates")
async def workflow_templates_get(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Operator --
@app.post("/api/operator/task-route-eligibility")
async def operator_task_route_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/api/operator/transport-support")
async def operator_transport_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/api/operator/roadmap-write")
async def operator_roadmap_write_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/api/operator/work-item-closeout")
async def operator_closeout_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/api/operator/roadmap-view")
async def operator_roadmap_view_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/api/operator/provider-onboarding")
async def operator_onboarding_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

# -- MCP JSON-RPC bridge --
@app.post("/mcp")
async def mcp_bridge(request: Request) -> Response:
    return await _route_to_handler(request)

# -- Agent orient --
@app.post("/orient")
async def orient_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

# -- Subsystem APIs (standard routes: handler(subsystems, body) -> dict) --
@app.post("/query")
async def query_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/bugs")
async def bugs_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/recall")
async def recall_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/ingest")
async def ingest_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/graph")
async def graph_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/receipts")
async def receipts_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/constraints")
async def constraints_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/friction")
async def friction_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/heal")
async def heal_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/artifacts")
async def artifacts_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/decompose")
async def decompose_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/research")
async def research_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/operator_view")
async def operator_view_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/health")
async def health_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/governance")
async def governance_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/workflow-runs")
async def workflow_runs_standard_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/workflow-validate")
async def workflow_validate_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/status")
async def status_standard_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/wave")
async def wave_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/manifest/generate")
async def manifest_generate_standard_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/manifest/refine")
async def manifest_refine_standard_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/manifest/get")
async def manifest_get_standard_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/heartbeat")
async def heartbeat_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

@app.post("/session")
async def session_post(request: Request) -> Response:
    return await _dispatch_standard_route(request)

# -- Root info --
@app.get("/")
async def root_info(request: Request) -> Response:
    return await _route_to_handler(request)


@app.get("/api/circuits")
def get_circuits() -> dict[str, Any]:
    """Return per-provider circuit breaker states."""
    from runtime.circuit_breaker import get_circuit_breakers

    return get_circuit_breakers().all_states()


@app.get("/api/trust")
def get_trust() -> list[dict[str, Any]]:
    """Return ELO-based trust scores for all (provider, model) pairs."""
    from runtime.trust_scoring import get_trust_scorer

    scorer = get_trust_scorer()
    scores = scorer.all_scores()
    return [
        {
            "provider_slug": s.provider_slug,
            "model_slug": s.model_slug,
            "elo_score": round(s.elo_score, 2),
            "total_runs": s.total_runs,
            "wins": s.wins,
            "losses": s.losses,
            "win_rate": round(s.win_rate, 4),
            "last_updated": s.last_updated.isoformat(),
        }
        for s in scores
    ]


@app.get("/api/reviews")
def get_reviews() -> dict[str, Any]:
    """Return author review summaries with dimension scores."""
    try:
        from runtime.review_tracker import get_review_tracker
        tracker = get_review_tracker()
        authors = tracker.author_summary()
        return {
            "authors": authors,
            "total_reviews": sum(a.get("total_reviews", 0) for a in authors),
        }
    except Exception:
        return {"authors": [], "total_reviews": 0}


@app.get("/api/fitness")
def get_fitness() -> dict[str, Any]:
    """Return capability fitness matrix."""
    try:
        from runtime.capability_router import compute_model_fitness
        fitness = compute_model_fitness()
        result = {}
        for (provider, model, cap), fit in fitness.items():
            key = f"{provider}/{model}"
            if key not in result:
                result[key] = {}
            result[key][cap] = {
                "success_rate": round(fit.success_rate, 3),
                "sample_count": fit.sample_count,
                "avg_latency_ms": fit.avg_latency_ms,
                "avg_cost_usd": round(fit.avg_cost_usd, 4),
                "fitness_score": round(fit.fitness_score, 2),
            }
        return {"models": result}
    except Exception:
        return {"models": {}}


# ---------------------------------------------------------------------------
# Job queue endpoints
# ---------------------------------------------------------------------------

@app.get("/api/queue/stats")
def queue_stats_endpoint() -> dict[str, Any]:
    """Return workflow job statistics grouped by status."""
    try:
        conn = _shared_pg_conn()
        rows = conn.execute(
            """SELECT status, COUNT(*) AS count
               FROM workflow_jobs
               GROUP BY status
               ORDER BY status"""
        )
        counts = {str(row["status"]): int(row["count"]) for row in (rows or [])}
        counts["total"] = sum(counts.values())
        return counts
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/queue/jobs")
def list_queue_jobs(
    status: str | None = Query(default=None, description="Filter by job status"),
    limit: int = Query(default=50, ge=1, le=1000),
) -> list[dict[str, Any]]:
    """List workflow jobs, optionally filtered by status."""
    try:
        conn = _shared_pg_conn()
        params: list[Any] = []
        where = ""
        if status:
            params.append(status)
            where = f"WHERE j.status = ${len(params)}"
        params.append(limit)
        rows = conn.execute(
            f"""SELECT j.id, j.run_id, j.label, j.status, j.agent_slug, j.resolved_agent,
                       j.attempt, j.max_attempts,
                       COALESCE(wr.request_envelope->>'name', wr.workflow_id) AS workflow_name,
                       j.created_at, j.ready_at, j.claimed_at, j.started_at, j.finished_at
                FROM workflow_jobs j
                LEFT JOIN workflow_runs wr ON wr.run_id = j.run_id
                {where}
                ORDER BY j.created_at DESC
                LIMIT ${len(params)}""",
            *params,
        )
        return [
            {
                "id": int(row["id"]),
                "run_id": row["run_id"],
                "workflow_name": row.get("workflow_name"),
                "label": row["label"],
                "status": row["status"],
                "agent_slug": row.get("agent_slug"),
                "resolved_agent": row.get("resolved_agent"),
                "attempt": int(row.get("attempt") or 0),
                "max_attempts": int(row.get("max_attempts") or 0),
                "created_at": _iso_or_none(row.get("created_at")),
                "ready_at": _iso_or_none(row.get("ready_at")),
                "claimed_at": _iso_or_none(row.get("claimed_at")),
                "started_at": _iso_or_none(row.get("started_at")),
                "finished_at": _iso_or_none(row.get("finished_at")),
            }
            for row in (rows or [])
        ]
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.post("/api/queue/submit")
def submit_queue_job(req: QueueSubmitRequest) -> dict[str, Any]:
    """Submit a one-job workflow through the workflow command bus."""
    label = req.spec.label or "api_queue_job"
    task_type = req.spec.task_type or "build"
    if req.spec.model_slug:
        agent = f"{req.spec.provider_slug}/{req.spec.model_slug}"
    else:
        agent = f"auto/{task_type}"
    spec = {
        "name": label,
        "workflow_id": f"workflow.api.{label.lower().replace(' ', '.')}",
        "phase": task_type,
        "workspace_ref": req.spec.workspace_ref,
        "runtime_profile_ref": req.spec.runtime_profile_ref,
        "jobs": [
            {
                "label": label,
                "agent": agent,
                "prompt": req.spec.prompt,
                "read_scope": req.spec.scope_read or [],
                "write_scope": req.spec.scope_write or [],
                "max_attempts": req.max_attempts,
            }
        ],
    }

    try:
        conn = _shared_pg_conn()
        temp_dir = REPO_ROOT / "artifacts" / "workflow"
        temp_dir.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".queue.json",
            dir=str(temp_dir),
            delete=False,
            prefix="queue_submit_",
        ) as handle:
            json.dump(spec, handle)
            spec_path = handle.name

        try:
            result = _submit_workflow_via_service_bus(
                SimpleNamespace(get_pg_conn=lambda: conn),
                spec_path=os.path.relpath(spec_path, str(REPO_ROOT)),
                spec_name=label,
                total_jobs=len(spec["jobs"]),
                requested_by_kind="http",
                requested_by_ref="queue_submit",
            )
        finally:
            os.unlink(spec_path)

        if result.get("error"):
            raise RuntimeError(str(result["error"]))

        return {
            "run_id": result["run_id"],
            "status": result["status"],
            "command_id": result["command_id"],
            "priority": req.priority,
            "note": "priority is accepted for compatibility but scheduling is workflow-runtime driven",
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/api/queue/cancel/{job_id}")
def cancel_queue_job(job_id: str) -> JSONResponse:
    """Cancel a queue-backed workflow through the workflow command bus."""
    run_id: str | None = None
    try:
        from runtime.control_commands import (
            ControlCommandType,
            ControlIntent,
            execute_control_intent,
            render_control_command_failure,
            render_control_command_response,
        )

        conn = _shared_pg_conn()
        rows = conn.execute(
            """SELECT run_id
               FROM workflow_jobs
               WHERE id = $1::bigint
               LIMIT 1""",
            job_id,
        )
    except Exception as exc:
        try:
            from runtime.control_commands import render_control_command_failure

            failure = render_control_command_failure(
                error_code=getattr(exc, "reason_code", "control.command.execution_failed"),
                error_detail=str(exc),
                run_id=run_id,
                job_id=job_id,
            )
            return JSONResponse(status_code=500, content=failure)
        except Exception:
            raise HTTPException(status_code=500, detail=str(exc))

    if rows:
        run_id = str(rows[0]["run_id"])
        try:
            command = execute_control_intent(
                conn,
                ControlIntent(
                    command_type=ControlCommandType.WORKFLOW_CANCEL,
                    requested_by_kind="http",
                    requested_by_ref="queue_cancel",
                    idempotency_key=f"workflow.cancel.http.{job_id}",
                    payload={"run_id": run_id, "include_running": True},
                ),
                approved_by="http.queue_cancel",
            )
        except Exception as exc:
            details = getattr(exc, "details", None)
            failure = render_control_command_failure(
                error_code=getattr(exc, "reason_code", "control.command.execution_failed"),
                error_detail=str(exc),
                run_id=run_id,
                job_id=job_id,
                details=details if isinstance(details, dict) else None,
            )
            return JSONResponse(status_code=500, content=failure)

        result = render_control_command_response(
            conn,
            command,
            action="cancel",
            run_id=run_id,
            job_id=job_id,
        )
        status_code = 200 if result.get("status") == "cancelled" else 409
        return JSONResponse(status_code=status_code, content=result)

    failure = render_control_command_failure(
        error_code="control.command.workflow_cancel_target_not_found",
        error_detail=f"Job {job_id!r} not found or already in a terminal state",
        job_id=job_id,
    )
    return JSONResponse(status_code=404, content=failure)


# ---------------------------------------------------------------------------
# Observability endpoints
# ---------------------------------------------------------------------------

@app.get("/api/metrics")
def get_metrics(days: int = Query(default=7, ge=1)) -> dict[str, Any]:
    """Return the core metrics summary for the last N days."""
    from runtime.observability import get_workflow_metrics_view

    view = get_workflow_metrics_view()
    return {
        "pass_rate_by_model": view.pass_rate_by_model(days=days),
        "cost_by_agent": view.cost_by_agent(days=days),
        "latency_percentiles": view.latency_percentiles(days=days),
        "efficiency_summary": view.efficiency_summary(days=days),
        "failure_category_breakdown": view.failure_category_breakdown(days=days),
        "hourly_workflow_volume": view.hourly_workflow_volume(days=days),
        "capability_distribution": view.capability_distribution(days=days),
    }


@app.get("/api/metrics/heatmap")
def get_metrics_heatmap(days: int = Query(default=7, ge=1)) -> list[dict[str, Any]]:
    """Return the failure code x provider heatmap for the last N days."""
    from runtime.observability import get_workflow_metrics_view

    view = get_workflow_metrics_view()
    return view.failure_heatmap(days=days)


@app.get("/api/events")
def get_events(
    type: str | None = Query(
        default=None,
        alias="type",
        description="Filter by event type (e.g. workflow.failed)",
    ),
    limit: int = Query(default=50, ge=1, le=1000),
) -> dict[str, Any]:
    """Return recent platform events from the durable event log."""
    from runtime.event_log import read_since, read_all_since
    from storage.dev_postgres import get_sync_connection

    try:
        conn = get_sync_connection()
        if type:
            # Filter by channel (event_type in old API maps to channel)
            events = read_since(conn, channel=type, limit=limit)
        else:
            events = read_all_since(conn, limit=limit)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    return {
        "event_type_filter": type,
        "limit": limit,
        "event_count": len(events),
        "events": [e.to_dict() for e in events],
    }


@app.get("/api/receipts")
def list_receipts(limit: int = Query(default=20, ge=1, le=500)) -> list[dict[str, Any]]:
    """Return a listing of recent workflow receipts (metadata, not full content)."""
    from runtime.receipt_store import list_receipts as _list_receipts

    try:
        records = _list_receipts(limit=limit)
        return [
            {"id": r.id, "label": r.label, "agent": r.agent, "status": r.status,
             "timestamp": r.timestamp.isoformat() if r.timestamp else None,
             "run_id": r.run_id}
            for r in records
        ]
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))


@app.get("/api/receipts/{receipt_id}")
def get_receipt(receipt_id: str) -> dict[str, Any]:
    """Return the full JSON content of one receipt by id."""
    from runtime.receipt_store import load_receipt

    rec = load_receipt(receipt_id)
    if rec is None:
        raise HTTPException(status_code=404, detail=f"Receipt not found: {receipt_id}")
    return rec.to_dict()


# ---------------------------------------------------------------------------
# Operations endpoints
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health_check_endpoint() -> Any:
    """Platform health from Postgres — the single source of truth.

    Checks: DB connectivity, worker liveness (recent heartbeats),
    workflow pass rate, and disk space.
    """
    now = datetime.now(timezone.utc)
    checks = []
    overall = "healthy"

    # 1. Postgres connectivity
    try:
        conn = _shared_pg_conn()
        conn.execute("SELECT 1")
        checks.append({"name": "postgres", "ok": True})
    except Exception as exc:
        checks.append({"name": "postgres", "ok": False, "error": str(exc)[:200]})
        overall = "unhealthy"
        return JSONResponse(status_code=503, content={
            "status": overall, "checks": checks, "timestamp": now.isoformat(),
        })

    # 2. Worker liveness (any heartbeat in last 5 minutes?)
    try:
        rows = conn.execute(
            "SELECT count(*) as cnt FROM workflow_jobs WHERE heartbeat_at > now() - interval '5 minutes'"
        )
        active = rows[0]["cnt"] if rows else 0
        # Also check: any job claimed in the last 10 minutes?
        claimed = conn.execute(
            "SELECT count(*) as cnt FROM workflow_jobs WHERE claimed_at > now() - interval '10 minutes'"
        )
        recent_claims = claimed[0]["cnt"] if claimed else 0
        # Worker is alive if either there are active heartbeats OR there are no ready jobs to claim
        ready = conn.execute("SELECT count(*) as cnt FROM workflow_jobs WHERE status = 'ready'")
        ready_cnt = ready[0]["cnt"] if ready else 0
        worker_alive = active > 0 or recent_claims > 0 or ready_cnt == 0
        checks.append({"name": "worker", "ok": worker_alive, "active_jobs": active, "ready_jobs": ready_cnt})
        if not worker_alive:
            overall = "degraded"
    except Exception as exc:
        checks.append({"name": "worker", "ok": False, "error": str(exc)[:200]})

    # 3. Workflow pass rate (last 24h from workflow_jobs)
    try:
        rows = conn.execute("""
            SELECT count(*) as total,
                   count(*) FILTER (WHERE status = 'succeeded') as passed,
                   count(*) FILTER (WHERE status IN ('failed', 'dead_letter')) as failed
            FROM workflow_jobs
            WHERE created_at > now() - interval '24 hours'
        """)
        r = rows[0] if rows else {"total": 0, "passed": 0, "failed": 0}
        pass_rate = round(r["passed"] / r["total"], 3) if r["total"] > 0 else 1.0
        checks.append({
            "name": "workflow", "ok": True,
            "total": r["total"], "passed": r["passed"], "failed": r["failed"],
            "pass_rate": pass_rate,
        })
    except Exception as exc:
        checks.append({"name": "workflow", "ok": False, "error": str(exc)[:200]})

    # 4. Disk space
    import shutil
    usage = shutil.disk_usage(str(Path(__file__).resolve().parents[3]))
    free_gb = round(usage.free / (1024**3), 1)
    checks.append({"name": "disk", "ok": free_gb > 5, "free_gb": free_gb})
    if free_gb <= 5:
        overall = "degraded"

    status_code = 200 if overall == "healthy" else (200 if overall == "degraded" else 503)
    return JSONResponse(status_code=status_code, content={
        "status": overall, "checks": checks, "timestamp": now.isoformat(),
    })


@app.get("/api/scope")
def resolve_scope_endpoint(
    files: list[str] = Query(
        description="Write-scope files to resolve (repeat param for multiple)"
    ),
    root: str = Query(default=".", description="Project root directory"),
) -> dict[str, Any]:
    """Resolve read scope, blast radius, and test scope for write-scope files."""
    from runtime.scope_resolver import resolve_scope as _resolve_scope

    if not files:
        raise HTTPException(status_code=400, detail="At least one file is required")

    try:
        resolution = _resolve_scope(files, root_dir=root)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    return {
        "write_scope": resolution.write_scope,
        "computed_read_scope": resolution.computed_read_scope,
        "test_scope": resolution.test_scope,
        "blast_radius": resolution.blast_radius,
        "context_sections": [
            {"name": s["name"], "content_length": len(s["content"])}
            for s in resolution.context_sections
        ],
    }


# ---------------------------------------------------------------------------
# Live catalog — aggregates platform registries into CatalogItem shapes
# ---------------------------------------------------------------------------

# Real engine primitives — each maps to an actual runtime capability
_STATIC_CATALOG_ITEMS: list[dict[str, Any]] = [
    # Triggers
    {"id": "trigger-manual",    "label": "Manual",       "icon": "trigger", "family": "trigger", "status": "ready", "dropKind": "node", "actionValue": "trigger",          "description": "User-initiated run"},
    {"id": "trigger-webhook",   "label": "Webhook",      "icon": "tool",    "family": "trigger", "status": "ready", "dropKind": "node", "actionValue": "trigger/webhook",  "description": "Inbound webhook with HMAC verification"},
    {"id": "trigger-schedule",  "label": "Schedule",     "icon": "trigger", "family": "trigger", "status": "ready", "dropKind": "node", "actionValue": "trigger/schedule", "description": "Cron or interval trigger"},
    # Gather
    {"id": "gather-research",   "label": "Web Research", "icon": "research", "family": "gather", "status": "ready", "dropKind": "node", "actionValue": "auto/research",   "description": "Search and analyze web sources"},
    {"id": "gather-docs",       "label": "Docs",         "icon": "research", "family": "gather", "status": "ready", "dropKind": "node", "actionValue": "auto/research",   "description": "Read and extract from documents"},
    # Think
    {"id": "think-classify",    "label": "Classify",     "icon": "classify", "family": "think",  "status": "ready", "dropKind": "node", "actionValue": "auto/classify",   "description": "Score, triage, or categorize"},
    {"id": "think-draft",       "label": "Draft",        "icon": "draft",    "family": "think",  "status": "ready", "dropKind": "node", "actionValue": "auto/draft",      "description": "Generate or compose content"},
    {"id": "think-fan-out",     "label": "Fan Out",      "icon": "classify", "family": "think",  "status": "ready", "dropKind": "node", "actionValue": "auto/fan-out",    "description": "Split into parallel sub-tasks and aggregate"},
    # Act (real integration bindings)
    {"id": "act-notify",        "label": "Notify",       "icon": "notify",  "family": "act",    "status": "ready", "dropKind": "node", "actionValue": "@notifications/send", "description": "Send notification (Slack, email, etc.)"},
    {"id": "act-webhook-out",   "label": "HTTP Request", "icon": "tool",    "family": "act",    "status": "ready", "dropKind": "node", "actionValue": "@webhook/post",       "description": "Call an external webhook or API"},
    {"id": "act-invoke",        "label": "Run Workflow",  "icon": "tool",    "family": "act",    "status": "ready", "dropKind": "node", "actionValue": "@workflow/invoke",    "description": "Invoke another workflow as a sub-workflow"},
    # Control (edges — real engine edge types)
    {"id": "ctrl-approval",     "label": "Approval",     "icon": "gate",    "family": "control", "status": "ready", "dropKind": "edge", "gateFamily": "approval",      "description": "Human approval gate"},
    {"id": "ctrl-review",       "label": "Human Review", "icon": "review",  "family": "control", "status": "ready", "dropKind": "edge", "gateFamily": "human_review",  "description": "Manual review before proceeding"},
    {"id": "ctrl-validation",   "label": "Validation",   "icon": "gate",    "family": "control", "status": "ready", "dropKind": "edge", "gateFamily": "validation",    "description": "Automated check gate"},
    {"id": "ctrl-branch",       "label": "Branch",       "icon": "gate",    "family": "control", "status": "ready", "dropKind": "edge", "gateFamily": "conditional",   "description": "Conditional path (equals, in, not_equals, not_in)"},
    {"id": "ctrl-retry",        "label": "Retry",        "icon": "gate",    "family": "control", "status": "ready", "dropKind": "edge", "gateFamily": "retry",         "description": "Retry with backoff + provider failover chain"},
    {"id": "ctrl-on-failure",   "label": "On Failure",   "icon": "gate",    "family": "control", "status": "ready", "dropKind": "edge", "gateFamily": "after_failure", "description": "Run only if upstream step failed"},
]

# Map capability_kind to catalog family
_KIND_TO_FAMILY: dict[str, str] = {
    "task": "think",
    "memory": "gather",
    "fanout": "think",
    "cli": "gather",
    "integration": "act",
}

# Map capability_kind to glyph icon
_KIND_TO_ICON: dict[str, str] = {
    "task": "classify",
    "memory": "research",
    "fanout": "classify",
    "cli": "research",
    "integration": "tool",
}


@app.get("/api/catalog")
def get_catalog() -> dict[str, Any]:
    """Return live catalog items from platform registries + static primitives."""
    items: list[dict[str, Any]] = []
    sources = {"static": 0, "capabilities": 0, "integrations": 0}

    # 1. Static engine primitives (triggers, control flow, core actions)
    items.extend(_STATIC_CATALOG_ITEMS)
    sources["static"] = len(_STATIC_CATALOG_ITEMS)

    # 2. Capability catalog (task types from Postgres)
    try:
        conn = _shared_pg_conn()
        rows = conn.execute(
            """SELECT capability_ref, capability_slug, capability_kind,
                      title, summary, description, route
                 FROM capability_catalog
                WHERE enabled = TRUE
                ORDER BY capability_kind, title"""
        )
        for row in rows or []:
            kind = row.get("capability_kind") or "task"
            family = _KIND_TO_FAMILY.get(kind, "think")
            icon = _KIND_TO_ICON.get(kind, "classify")
            slug = row.get("capability_slug") or ""
            items.append({
                "id": f"cap-{slug.replace('/', '-')}",
                "label": row.get("title") or slug,
                "icon": icon,
                "family": family,
                "status": "ready",
                "dropKind": "node",
                "actionValue": row.get("route") or f"auto/{slug}",
                "description": row.get("summary") or row.get("description") or "",
                "source": "capability",
            })
            sources["capabilities"] += 1
    except Exception as exc:
        logger.warning("catalog: capability_catalog query failed: %s", exc)

    # 3. Integration registry (connected services from Postgres)
    try:
        conn = _shared_pg_conn()
        rows = conn.execute(
            "SELECT id, name, description, provider, capabilities, auth_status, icon FROM integration_registry ORDER BY name"
        )
        for row in rows or []:
            integration_id = row.get("id") or ""
            name = row.get("name") or integration_id
            auth = row.get("auth_status") or "unknown"
            caps = row.get("capabilities")
            if isinstance(caps, str):
                try:
                    caps = json.loads(caps)
                except (json.JSONDecodeError, TypeError):
                    caps = []
            caps = caps or []

            if not caps:
                # One item per integration with no granular capabilities
                items.append({
                    "id": f"int-{integration_id}",
                    "label": name,
                    "icon": row.get("icon") or "tool",
                    "family": "act",
                    "status": "ready" if auth == "connected" else "coming_soon",
                    "dropKind": "node",
                    "actionValue": f"@{integration_id}",
                    "description": row.get("description") or f"Use {name}",
                    "source": "integration",
                    "connectionStatus": auth,
                })
                sources["integrations"] += 1
            else:
                # One item per capability action
                for cap in caps:
                    action = cap.get("action", "") if isinstance(cap, dict) else str(cap)
                    cap_desc = cap.get("description", "") if isinstance(cap, dict) else ""
                    items.append({
                        "id": f"int-{integration_id}-{action}".replace(" ", "-").lower(),
                        "label": f"{name}: {action}" if action else name,
                        "icon": row.get("icon") or "tool",
                        "family": "act",
                        "status": "ready" if auth == "connected" else "coming_soon",
                        "dropKind": "node",
                        "actionValue": f"@{integration_id}/{action}" if action else f"@{integration_id}",
                        "description": cap_desc or row.get("description") or f"Use {name}",
                        "source": "integration",
                        "connectionStatus": auth,
                    })
                    sources["integrations"] += 1
    except Exception as exc:
        logger.warning("catalog: integration_registry query failed: %s", exc)

    # 4. Connector registry (versioned connectors with health)
    try:
        conn = _shared_pg_conn()
        rows = conn.execute(
            "SELECT slug, display_name, version, auth_type, base_url, status, health_status "
            "FROM connector_registry WHERE status = 'active' ORDER BY display_name"
        )
        for row in rows or []:
            slug = row.get("slug") or ""
            health = row.get("health_status") or "unknown"
            items.append({
                "id": f"conn-{slug}",
                "label": row.get("display_name") or slug,
                "icon": "tool",
                "family": "act",
                "status": "ready" if health in ("healthy", "degraded") else "coming_soon",
                "dropKind": "node",
                "actionValue": f"@connector/{slug}",
                "description": f"v{row.get('version', '?')} — {row.get('auth_type', '')} auth — {row.get('base_url', '')}",
                "source": "connector",
                "connectionStatus": health,
            })
            sources["connectors"] = sources.get("connectors", 0) + 1
    except Exception:
        pass  # connector_registry table may not exist yet

    return {
        "items": items,
        "sources": sources,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }


# No catch-all routes — every endpoint is explicitly registered above.


@app.get("/", response_model=None)
def root_redirect() -> Response:
    return RedirectResponse(url="/app")


def _legacy_ui_redirect(path: str = "") -> Response:
    normalized_path = path.strip("/")
    target = f"/app/{normalized_path}" if normalized_path else "/app"
    return RedirectResponse(url=target)


@app.get("/ui", response_model=None)
def legacy_ui_redirect_root() -> Response:
    return _legacy_ui_redirect()


@app.get("/ui/", response_model=None)
def legacy_ui_redirect_root_slash() -> Response:
    return _legacy_ui_redirect()


@app.get("/ui/{path:path}", response_model=None)
def legacy_ui_redirect_path(path: str = "") -> Response:
    return _legacy_ui_redirect(path)


@app.get("/app", response_model=None)
def launcher_app_root() -> Any:
    return _launcher_index_response()


@app.get("/app/", response_model=None)
def launcher_app_root_slash() -> Any:
    return _launcher_index_response()


@app.get("/app/{path:path}", response_model=None)
def launcher_app_path(path: str = "") -> Any:
    del path
    return _launcher_index_response()
