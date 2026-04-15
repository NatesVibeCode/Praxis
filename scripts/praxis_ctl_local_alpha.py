#!/usr/bin/env python3
"""Import-safe local-alpha helper surface.

This module intentionally keeps the helper functions importable because tests
and repo-local health projections depend on them. The direct native control
CLI remains disabled; callers should use Docker or Cloudflare sandbox
authority instead of invoking this file as an executable.
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping
from urllib import error as urllib_error
from urllib import request as urllib_request

REPO_ROOT = Path(__file__).resolve().parents[1]
WORKFLOW_ROOT = REPO_ROOT / "Code&DBs" / "Workflow"
if str(WORKFLOW_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKFLOW_ROOT))

from surfaces.api import operator_read
from runtime.dependency_contract import dependency_truth_report
from runtime.post_workflow_sync import (
    get_workflow_run_sync_status,
    latest_workflow_run_sync_status,
    repair_workflow_run_sync,
    run_post_workflow_sync,
)
from storage.dev_postgres import local_postgres_bootstrap, local_postgres_health


DISABLED_MESSAGE = (
    "Native local-alpha control is disabled. Use Docker or Cloudflare sandbox authority only."
)
DEFAULT_DB_URL = os.environ.get("WORKFLOW_DATABASE_URL", "postgresql://postgres@localhost:5432/praxis")
DEFAULT_API_BASE_URL = "http://127.0.0.1:8420"
DEFAULT_WORKFLOW_API_BASE_URL = "http://127.0.0.1:8420"


def _env_for_authority() -> dict[str, str]:
    return {
        "WORKFLOW_DATABASE_URL": os.environ.get("WORKFLOW_DATABASE_URL", DEFAULT_DB_URL),
    }


def _serialize_exception(exc: BaseException) -> dict[str, Any]:
    return {
        "type": type(exc).__name__,
        "message": str(exc),
        "reason_code": getattr(exc, "reason_code", None),
    }


def _emit(payload: Mapping[str, object], *, indent: int = 2) -> int:
    print(json.dumps(dict(payload), indent=indent, sort_keys=True))
    return 0 if payload.get("ok", True) else 1


def _to_bool(value: str) -> bool:
    lowered = value.lower()
    if lowered in {"1", "true", "yes", "y", "on"}:
        return True
    if lowered in {"0", "false", "no", "n", "off"}:
        return False
    raise ValueError(f"invalid boolean value: {value!r}")


def _http_request(
    url: str,
    *,
    method: str = "GET",
    payload: Mapping[str, object] | None = None,
    headers: Mapping[str, str] | None = None,
    timeout_s: float = 4.0,
) -> tuple[int | None, bytes]:
    body: bytes | None = None
    request_headers = dict(headers or {})
    if payload is not None:
        body = json.dumps(dict(payload)).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    req = urllib_request.Request(url, data=body, headers=request_headers, method=method)
    try:
        with urllib_request.urlopen(req, timeout=timeout_s) as response:
            return int(getattr(response, "status", 200)), response.read()
    except urllib_error.HTTPError as exc:
        return exc.code, exc.read()
    except Exception:
        return None, b""


def _timeout_override(env_name: str, default: float) -> float:
    raw_value = str(os.environ.get(env_name, "")).strip()
    if not raw_value:
        return default
    try:
        parsed = float(raw_value)
    except ValueError:
        return default
    if parsed <= 0:
        return default
    return parsed


def _decode_json(raw: bytes) -> dict[str, object] | None:
    if not raw:
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def _probe_frontdoor_semantics() -> dict[str, object]:
    api_base = os.environ.get("PRAXIS_API_BASE_URL", DEFAULT_API_BASE_URL).rstrip("/")
    workflow_api_base = os.environ.get(
        "PRAXIS_WORKFLOW_API_BASE_URL",
        DEFAULT_WORKFLOW_API_BASE_URL,
    ).rstrip("/")
    workflow_probe_headers = {"X-Praxis-UI": "1"}
    api_health_timeout_s = _timeout_override("PRAXIS_ALPHA_TIMEOUT_API_HEALTH_S", 4.0)
    workflow_orient_timeout_s = _timeout_override("PRAXIS_ALPHA_TIMEOUT_WORKFLOW_ORIENT_S", 10.0)
    mcp_timeout_s = _timeout_override("PRAXIS_ALPHA_TIMEOUT_MCP_S", 6.0)
    ui_timeout_s = _timeout_override("PRAXIS_ALPHA_TIMEOUT_UI_S", 4.0)

    api_health_status, api_health_raw = _http_request(
        f"{api_base}/api/health",
        timeout_s=api_health_timeout_s,
    )
    api_health_payload = _decode_json(api_health_raw)
    api_server_ready = (
        api_health_status == 200
        and isinstance(api_health_payload, dict)
        and str(api_health_payload.get("status") or "").strip().lower() in {"healthy", "degraded"}
    )

    workflow_orient_status, workflow_orient_raw = _http_request(
        f"{workflow_api_base}/orient",
        method="POST",
        payload={},
        headers=workflow_probe_headers,
        timeout_s=workflow_orient_timeout_s,
    )
    workflow_orient_payload = _decode_json(workflow_orient_raw)
    workflow_api_ready = (
        workflow_orient_status == 200
        and isinstance(workflow_orient_payload, dict)
        and str(workflow_orient_payload.get("platform") or "").strip() in {"dag-workflow", "praxis-workflow"}
    )

    mcp_status, mcp_raw = _http_request(
        f"{workflow_api_base}/mcp",
        method="POST",
        payload={
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {"protocolVersion": "2024-11-05"},
        },
        headers=workflow_probe_headers,
        timeout_s=mcp_timeout_s,
    )
    mcp_payload = _decode_json(mcp_raw)
    server_info = mcp_payload.get("result", {}).get("serverInfo") if isinstance(mcp_payload, dict) else {}
    mcp_bridge_ready = (
        mcp_status == 200
        and isinstance(server_info, dict)
        and str(server_info.get("name") or "").strip() in {"praxis-mcp", "praxis-workflow-mcp", "dag-workflow-mcp"}
    )

    ui_status, ui_raw = _http_request(f"{api_base}/app", timeout_s=ui_timeout_s)
    ui_html = ui_raw.decode("utf-8", errors="ignore")
    ui_ready = (
        ui_status == 200
        and "<div id=\"root\"></div>" in ui_html
        and (
            "<title>Praxis</title>" in ui_html
            or "<title>Praxis Engine</title>" in ui_html
            or "<title>Helm</title>" in ui_html
        )
    )

    return {
        "api_server_ready": api_server_ready,
        "workflow_api_ready": workflow_api_ready,
        "mcp_bridge_ready": mcp_bridge_ready,
        "ui_ready": ui_ready,
        "launch_url": f"{api_base}/app",
        "helm_url": f"{api_base}/app/helm",
        "dashboard_url": f"{api_base}/app",
        "api_docs_url": f"{api_base}/docs",
    }


def cmd_db_health() -> int:
    try:
        status = local_postgres_health(env=_env_for_authority())
        return _emit({"ok": True, "database": status.to_json()})
    except Exception as exc:
        return _emit({"ok": False, "database": None, "error": _serialize_exception(exc)})


def cmd_db_bootstrap() -> int:
    try:
        status = local_postgres_bootstrap(env=_env_for_authority())
        return _emit({"ok": True, "database": status.to_json()})
    except Exception as exc:
        return _emit({"ok": False, "database": None, "error": _serialize_exception(exc)})


def _coerce_state_value(value: object, *, field: str, default: object) -> object:
    if field == "smoke_run_id":
        return value if isinstance(value, str) else default
    if field == "persisted":
        return value if isinstance(value, bool) else default
    if field == "sync_status":
        if isinstance(value, str):
            normalized = value.strip().lower()
            if normalized in {"pending", "succeeded", "degraded", "skipped"}:
                return normalized
        return default
    if field == "sync_cycle_id":
        return value if isinstance(value, str) else None
    if field == "sync_error_count":
        return value if isinstance(value, int) and value >= 0 else default
    return default


def _write_state(path: str | Path, value: Mapping[str, object]) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        **dict(value),
        "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    state_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _read_state(path: str | Path) -> dict[str, object]:
    state_path = Path(path)
    if not state_path.is_file():
        return {}
    try:
        payload = json.loads(state_path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def _sync_snapshot(run_id: str | None) -> dict[str, object]:
    try:
        if not isinstance(run_id, str) or not run_id:
            latest = latest_workflow_run_sync_status()
            if latest is None:
                return {
                    "sync_status": "skipped",
                    "sync_cycle_id": None,
                    "sync_error_count": 0,
                }
            return {
                "sync_status": latest.sync_status,
                "sync_cycle_id": latest.sync_cycle_id,
                "sync_error_count": latest.sync_error_count,
            }

        status = get_workflow_run_sync_status(run_id)
        return {
            "sync_status": status.sync_status,
            "sync_cycle_id": status.sync_cycle_id,
            "sync_error_count": status.sync_error_count,
        }
    except Exception:
        return {
            "sync_status": "skipped",
            "sync_cycle_id": None,
            "sync_error_count": 0,
        }


def cmd_smoke(*, state_file: str | None = None) -> int:
    try:
        payload = operator_read.run_native_self_hosted_smoke()
        run_payload = payload.get("run", {})
        run_id = run_payload.get("run_id") if isinstance(run_payload, dict) else None
        sync_snapshot = _sync_snapshot(run_id if isinstance(run_id, str) else None)
        if isinstance(run_id, str) and run_id:
            with contextlib.redirect_stderr(io.StringIO()):
                sync_result = run_post_workflow_sync(run_id)
            sync_snapshot = {
                "sync_status": sync_result.sync_status,
                "sync_cycle_id": sync_result.sync_cycle_id,
                "sync_error_count": sync_result.sync_error_count,
            }
        proof = {
            "ok": True,
            "smoke_run_id": run_id if isinstance(run_id, str) else None,
            "run_id": run_id if isinstance(run_id, str) else None,
            "persisted": bool(run_id) if isinstance(run_id, str) else False,
            "sync_status": sync_snapshot["sync_status"],
            "sync_cycle_id": sync_snapshot["sync_cycle_id"],
            "sync_error_count": sync_snapshot["sync_error_count"],
            "run": dict(payload.get("run", {})),
            "step_order": list(payload.get("step_order", ())) if "step_order" in payload else [],
        }
        if state_file:
            _write_state(
                state_file,
                {
                    "smoke_run_id": proof["smoke_run_id"],
                    "persisted": proof["persisted"],
                    "sync_status": proof["sync_status"],
                    "sync_cycle_id": proof["sync_cycle_id"],
                    "sync_error_count": proof["sync_error_count"],
                    "run_payload": dict(payload.get("run", {})),
                    "step_order": proof["step_order"],
                },
            )
        return _emit(proof)
    except Exception as exc:  # pragma: no cover - integration failure depends on local env
        failure = {
            "ok": False,
            "smoke_run_id": None,
            "run_id": None,
            "persisted": False,
            "sync_status": "degraded",
            "sync_cycle_id": None,
            "sync_error_count": 1,
            "error": _serialize_exception(exc),
        }
        if state_file:
            _write_state(
                state_file,
                {
                    "smoke_run_id": None,
                    "persisted": False,
                    "sync_status": "degraded",
                    "sync_cycle_id": None,
                    "sync_error_count": 1,
                    "error": failure["error"],
                },
            )
        return _emit(failure)


def cmd_doctor(*, services_ready: str, state_file: str) -> int:
    services_ready_value = _to_bool(services_ready)
    database_reachable = False
    schema_bootstrapped = False
    try:
        database = local_postgres_health(env=_env_for_authority())
        database_json = database.to_json()
        database_reachable = bool(database_json.get("database_reachable"))
        schema_bootstrapped = bool(database_json.get("schema_bootstrapped"))
    except Exception:
        database_reachable = False
        schema_bootstrapped = False

    state = _read_state(state_file)
    smoke_run_id = _coerce_state_value(state.get("smoke_run_id"), field="smoke_run_id", default=None)
    sync_snapshot = _sync_snapshot(smoke_run_id if isinstance(smoke_run_id, str) else None)

    state = {
        "smoke_run_id": smoke_run_id,
        "persisted": _coerce_state_value(state.get("persisted"), field="persisted", default=False),
        "sync_status": _coerce_state_value(sync_snapshot["sync_status"], field="sync_status", default="skipped"),
        "sync_cycle_id": _coerce_state_value(sync_snapshot["sync_cycle_id"], field="sync_cycle_id", default=None),
        "sync_error_count": _coerce_state_value(sync_snapshot["sync_error_count"], field="sync_error_count", default=0),
    }
    readiness = _probe_frontdoor_semantics()

    return _emit(
        {
            "services_ready": services_ready_value,
            "database_reachable": database_reachable,
            "schema_bootstrapped": schema_bootstrapped,
            "api_server_ready": readiness["api_server_ready"],
            "workflow_api_ready": readiness["workflow_api_ready"],
            "mcp_bridge_ready": readiness["mcp_bridge_ready"],
            "ui_ready": readiness["ui_ready"],
            "smoke_run_id": state["smoke_run_id"],
            "persisted": state["persisted"],
            "sync_status": state["sync_status"],
            "sync_cycle_id": state["sync_cycle_id"],
            "sync_error_count": state["sync_error_count"],
            "launch_url": readiness["launch_url"],
            "dashboard_url": readiness["dashboard_url"],
            "api_docs_url": readiness["api_docs_url"],
            "dependency_truth": dependency_truth_report(scope="all"),
        }
    )


def cmd_repair_sync(*, run_id: str | None = None) -> int:
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            status = repair_workflow_run_sync(run_id)
        return _emit(
            {
                "ok": True,
                "run_id": status.run_id,
                "sync_status": status.sync_status,
                "sync_cycle_id": status.sync_cycle_id,
                "sync_error_count": status.sync_error_count,
            }
        )
    except Exception as exc:
        return _emit(
            {
                "ok": False,
                "run_id": run_id,
                "sync_status": "degraded",
                "sync_cycle_id": None,
                "sync_error_count": 1,
                "error": _serialize_exception(exc),
            }
        )


def main(argv: list[str] | None = None) -> int:
    del argv
    print(DISABLED_MESSAGE, file=sys.stderr)
    return 1


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
