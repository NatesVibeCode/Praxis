#!/usr/bin/env python3
"""postact_fingerprint — PostToolUse fingerprint writer.

Reads a Claude Code (or compatible) hook payload from stdin, derives a
shape-only fingerprint (no literal values), and inserts a row into
`action_fingerprints` in Praxis.db.

Default-off: the bash wrapper only invokes this when
PRAXIS_FINGERPRINT_ENABLED=1 is exported. This script is also safe to
invoke directly for testing — it fails open on any error.

Source surface tagging via PRAXIS_FINGERPRINT_SOURCE_SURFACE
(default "claude-code:host"). Sandbox / Codex / Gemini harnesses export
their own value so cross-surface frequency counting works.

DSN resolution order:
  1. WORKFLOW_DATABASE_URL env var (preferred — same convention as the
     rest of the workflow stack).
  2. Workflow Python resolver (storage.workflow_database) if importable.
  3. Give up silently. No insert; no error.

Hook never blocks. Any exception → exit 0 with no row written.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from typing import Any


_QUOTED_RE = re.compile(r"""(['"])(?:\\.|(?!\1).)*\1""", re.DOTALL)
_LONG_FLAG_VALUE_RE = re.compile(r"(--[a-zA-Z0-9][\w\-]*)=\S+")
_NUMBER_RE = re.compile(r"\b\d[\d.]*\b")
_HEX_RE = re.compile(r"\b[0-9a-f]{8,}\b", re.IGNORECASE)

_TOOL_TO_KIND = {
    "Bash": "shell",
    "Edit": "edit",
    "Write": "write",
    "MultiEdit": "multi_edit",
    "Read": "read",
}


def _read_payload() -> dict[str, Any]:
    raw = sys.stdin.read()
    if not raw.strip():
        return {}
    try:
        loaded = json.loads(raw)
    except Exception:
        return {}
    return loaded if isinstance(loaded, dict) else {}


def _normalize_path_shape(p: str) -> str:
    """Replace literal path with dir-shape: keep first 3 dirs + extension only.

    Examples:
        /Users/nate/Praxis/Code&DBs/Workflow/runtime/foo.py
            -> /Users/nate/Praxis/.../*.py
        Code&DBs/Databases/migrations/workflow/383.sql
            -> Code&DBs/Databases/migrations/.../*.sql
        bare-token  -> bare-token (not a path)
    """
    if not p:
        return ""
    if "/" not in p:
        return p
    rooted = p.startswith("/")
    parts = [seg for seg in p.split("/") if seg]
    if not parts:
        return "/"
    keep = parts[:3]
    tail = parts[-1]
    has_ext = "." in tail and not tail.startswith(".")
    if has_ext and len(parts) > 3:
        ext = tail.rsplit(".", 1)[-1].lower()
        shape = "/".join(keep) + "/.../*." + ext
    elif has_ext:
        # short path, keep dirs + ext only
        ext = tail.rsplit(".", 1)[-1].lower()
        shape = "/".join(keep[:-1]) + ("/" if len(keep) > 1 else "") + "*." + ext
    elif len(parts) > 3:
        shape = "/".join(keep) + "/..."
    else:
        shape = "/".join(keep)
    return ("/" + shape) if rooted else shape


def _normalize_command(cmd: str) -> str:
    if not cmd:
        return ""
    s = _QUOTED_RE.sub("''", cmd)
    s = _LONG_FLAG_VALUE_RE.sub(r"\1=*", s)
    s = _NUMBER_RE.sub("#N", s)
    s = _HEX_RE.sub("#H", s)
    s = " ".join(s.split())
    return s[:512]


def _build_fingerprint(payload: dict[str, Any]) -> dict[str, Any] | None:
    tool_name = payload.get("tool_name") or ""
    action_kind = _TOOL_TO_KIND.get(tool_name)
    if not action_kind:
        return None
    tool_input = payload.get("tool_input") or {}
    if not isinstance(tool_input, dict):
        tool_input = {}

    normalized_command = None
    path_shape = None

    if action_kind == "shell":
        normalized_command = _normalize_command(str(tool_input.get("command") or ""))
        if not normalized_command:
            return None
        shape_body = normalized_command
    else:
        path_shape = _normalize_path_shape(str(tool_input.get("file_path") or ""))
        if not path_shape:
            return None
        shape_body = f"{action_kind}:{path_shape}"

    shape_input = f"{action_kind}|{shape_body}"
    shape_hash = hashlib.sha256(shape_input.encode("utf-8")).hexdigest()

    source_surface = (
        os.environ.get("PRAXIS_FINGERPRINT_SOURCE_SURFACE")
        or "claude-code:host"
    )
    session_ref = (
        os.environ.get("CLAUDE_SESSION_ID")
        or payload.get("session_id")
        or None
    )

    return {
        "source_surface": source_surface,
        "action_kind": action_kind,
        "operation_name": None,
        "normalized_command": normalized_command,
        "path_shape": path_shape,
        "shape_hash": shape_hash,
        "session_ref": session_ref,
        "payload_meta": {
            "tool_name": tool_name,
            "shape_input": shape_input[:256],
        },
    }


def _resolve_dsn() -> str | None:
    dsn = os.environ.get("WORKFLOW_DATABASE_URL")
    if dsn:
        return dsn
    repo = os.environ.get("CLAUDE_PROJECT_DIR")
    if not repo:
        return None
    wf_root = os.path.join(repo, "Code&DBs", "Workflow")
    if wf_root not in sys.path:
        sys.path.insert(0, wf_root)
    try:
        from storage.workflow_database import resolve_workflow_database_url  # type: ignore
    except Exception:
        return None
    try:
        return resolve_workflow_database_url()
    except Exception:
        return None


def _connect(dsn: str):
    try:
        import psycopg  # type: ignore
        return psycopg.connect(dsn), "psycopg3"
    except Exception:
        pass
    try:
        import psycopg2  # type: ignore
        return psycopg2.connect(dsn), "psycopg2"
    except Exception:
        return None, None


def _insert(row: dict[str, Any]) -> None:
    dsn = _resolve_dsn()
    if not dsn:
        return
    conn, _ = _connect(dsn)
    if conn is None:
        return
    sql = (
        "INSERT INTO action_fingerprints ("
        " source_surface, action_kind, operation_name,"
        " normalized_command, path_shape, shape_hash,"
        " session_ref, payload_meta"
        ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)"
    )
    params = (
        row["source_surface"],
        row["action_kind"],
        row.get("operation_name"),
        row.get("normalized_command"),
        row.get("path_shape"),
        row["shape_hash"],
        row.get("session_ref"),
        json.dumps(row.get("payload_meta") or {}),
    )
    try:
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
            conn.commit()
        finally:
            conn.close()
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return


def main() -> int:
    try:
        payload = _read_payload()
        row = _build_fingerprint(payload)
        if row is None:
            return 0
        _insert(row)
    except Exception:
        return 0
    return 0


if __name__ == "__main__":
    sys.exit(main())
