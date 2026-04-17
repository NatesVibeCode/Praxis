"""File storage authority for the workflow CLI.

Read actions stay query-side: list, get, content.
Write actions stay command-side: upload, delete.
"""

from __future__ import annotations

import base64
import json
import mimetypes
from pathlib import Path
from typing import Any, TextIO

from runtime.file_storage import delete_file, get_file_content, get_file_record, list_files, save_file
from surfaces.cli._db import cli_repo_root, cli_sync_conn

_ALLOWED_SCOPES = {"instance", "step", "workflow"}


def _sync_conn():
    return cli_sync_conn()


def _file_help_text() -> str:
    return "\n".join(
        [
            "usage: workflow files <list|get|content|upload|delete> [args]",
            "",
            "Files authority:",
            "  workflow files list [--scope SCOPE] [--workflow-id ID] [--step-id ID] [--query TEXT] [--limit N] [--json]",
            "  workflow files get <file-id> [--json]",
            "  workflow files content <file-id> [--output-file <path>] [--json]",
            "  workflow files upload <path> [--filename NAME] [--content-type TYPE] [--scope instance|step|workflow] [--workflow-id ID] [--step-id ID] [--description TEXT] [--yes] [--json]",
            "  workflow files delete <file-id> [--yes] [--json]",
            "",
            "Notes:",
            "  - list/get/content are read-side queries",
            "  - upload/delete mutate the uploaded_files table and the upload directory",
        ]
    )


def _render_confirmation(*, stdout: TextIO) -> int:
    stdout.write("confirmation required: rerun with --yes\n")
    return 2


def _file_record_text(record: dict[str, Any]) -> str:
    parts = [
        str(record.get("id") or ""),
        str(record.get("filename") or ""),
        f"scope={record.get('scope') or '-'}",
        f"size={record.get('size_bytes') or 0}",
        f"created_at={record.get('created_at') or '-'}",
    ]
    return "  ".join(part for part in parts if part)


def _render_list_payload(payload: dict[str, Any], *, stdout: TextIO, as_json: bool) -> None:
    if as_json:
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        return

    files = payload.get("files")
    if not isinstance(files, list):
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        return
    if not files:
        stdout.write(str(payload.get("message") or "no files found") + "\n")
        return
    stdout.write(f"{len(files)} file(s)\n")
    for record in files:
        if isinstance(record, dict):
            stdout.write(f"  {_file_record_text(record)}\n")


def _render_record_payload(
    payload: dict[str, Any],
    *,
    stdout: TextIO,
    as_json: bool,
    include_content: bool = False,
) -> None:
    if as_json:
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        return

    file_record = payload.get("file")
    if not isinstance(file_record, dict):
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        return

    stdout.write(_file_record_text(file_record) + "\n")
    description = str(file_record.get("description") or "").strip()
    if description:
        stdout.write(f"  description: {description}\n")
    storage_path = str(file_record.get("storage_path") or "").strip()
    if storage_path:
        stdout.write(f"  storage_path: {storage_path}\n")
    written_to = str(payload.get("written_to") or "").strip()
    if written_to:
        stdout.write(f"  written_to: {written_to}\n")
    byte_count = payload.get("byte_count")
    if byte_count is not None:
        stdout.write(f"  byte_count: {byte_count}\n")

    if include_content and "content_text" in payload:
        stdout.write(str(payload["content_text"]))
        if not str(payload["content_text"]).endswith("\n"):
            stdout.write("\n")


def _render_upload_payload(payload: dict[str, Any], *, stdout: TextIO, as_json: bool) -> None:
    if as_json:
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        return
    record = payload.get("file")
    if isinstance(record, dict):
        stdout.write(
            f"uploaded {record.get('id')}  {record.get('filename')}  "
            f"size={record.get('size_bytes') or 0}  scope={record.get('scope') or '-'}\n"
        )
        return
    stdout.write(json.dumps(payload, indent=2, default=str) + "\n")


def _load_source_file(source_path: str) -> tuple[Path, bytes]:
    path = Path(source_path).expanduser()
    if not path.is_file():
        raise ValueError(f"source file does not exist: {path}")
    return path, path.read_bytes()


def _normalize_scope(scope: str) -> str:
    normalized = scope.strip().lower()
    if normalized not in _ALLOWED_SCOPES:
        raise ValueError("scope must be one of: instance, step, workflow")
    return normalized


def _files_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_file_help_text() + "\n")
        return 2

    action = args[0]
    as_json = False
    confirmed = False
    limit = 100
    scope = None
    workflow_id = None
    step_id = None
    query = None
    file_id = ""
    source_path = ""
    filename = ""
    content_type = ""
    description = ""
    output_file = ""

    i = 1
    while i < len(args):
        token = args[i]
        if token == "--json":
            as_json = True
            i += 1
            continue
        if token == "--yes":
            confirmed = True
            i += 1
            continue
        if token == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
            continue
        if token == "--scope" and i + 1 < len(args):
            scope = _normalize_scope(args[i + 1])
            i += 2
            continue
        if token == "--workflow-id" and i + 1 < len(args):
            workflow_id = args[i + 1]
            i += 2
            continue
        if token == "--step-id" and i + 1 < len(args):
            step_id = args[i + 1]
            i += 2
            continue
        if token == "--query" and i + 1 < len(args):
            query = args[i + 1]
            i += 2
            continue
        if token == "--file-id" and i + 1 < len(args):
            file_id = args[i + 1]
            i += 2
            continue
        if token == "--file-path" and i + 1 < len(args):
            source_path = args[i + 1]
            i += 2
            continue
        if token == "--filename" and i + 1 < len(args):
            filename = args[i + 1]
            i += 2
            continue
        if token == "--content-type" and i + 1 < len(args):
            content_type = args[i + 1]
            i += 2
            continue
        if token == "--description" and i + 1 < len(args):
            description = args[i + 1]
            i += 2
            continue
        if token == "--output-file" and i + 1 < len(args):
            output_file = args[i + 1]
            i += 2
            continue
        if action in {"get", "content", "delete"} and not file_id:
            file_id = token
            i += 1
            continue
        if action == "upload" and not source_path:
            source_path = token
            i += 1
            continue
        stdout.write(f"unexpected argument: {token}\n")
        return 2

    repo_root = str(cli_repo_root())
    try:
        if action == "list":
            conn = _sync_conn()
            payload = {
                "count": 0,
                "files": list_files(
                    conn,
                    scope=scope,
                    workflow_id=workflow_id,
                    step_id=step_id,
                    query=query,
                    limit=limit,
                ),
            }
            payload["count"] = len(payload["files"])
        elif action == "get":
            conn = _sync_conn()
            record = get_file_record(conn, file_id)
            if record is None:
                payload = {"error": f"File not found: {file_id}"}
            else:
                payload = {"file": record}
        elif action == "content":
            conn = _sync_conn()
            payload = _render_content_action(
                conn,
                repo_root=repo_root,
                file_id=file_id,
                output_file=output_file,
                as_json=as_json,
            )
        elif action == "upload":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            if not source_path:
                stdout.write("upload requires a source file path\n")
                return 2
            conn = _sync_conn()
            source, content = _load_source_file(source_path)
            chosen_filename = filename or source.name
            chosen_content_type = content_type or mimetypes.guess_type(chosen_filename)[0] or "application/octet-stream"
            payload = {
                "file": save_file(
                    conn,
                    repo_root,
                    filename=chosen_filename,
                    content=content,
                    content_type=chosen_content_type,
                    scope=scope or "instance",
                    workflow_id=workflow_id,
                    step_id=step_id,
                    description=description,
                )
            }
        elif action == "delete":
            if not confirmed:
                return _render_confirmation(stdout=stdout)
            conn = _sync_conn()
            deleted = delete_file(conn, repo_root, file_id)
            if not deleted:
                payload = {"error": f"File not found: {file_id}"}
            else:
                payload = {"deleted": True, "id": file_id}
        else:
            stdout.write(_file_help_text() + "\n")
            return 2
    except (ValueError, json.JSONDecodeError) as exc:
        payload = {"error": str(exc)}

    if isinstance(payload, dict) and payload.get("error"):
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        return 1

    if action == "list":
        _render_list_payload(payload, stdout=stdout, as_json=as_json)
    elif action == "get":
        _render_record_payload(payload, stdout=stdout, as_json=as_json)
    elif action == "content":
        _render_record_payload(payload, stdout=stdout, as_json=as_json, include_content=True)
    elif action == "upload":
        _render_upload_payload(payload, stdout=stdout, as_json=as_json)
    elif action == "delete":
        if as_json:
            stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        else:
            stdout.write(f"deleted {payload.get('id')}\n")
    else:
        stdout.write(_file_help_text() + "\n")
        return 2
    return 0


def _render_content_action(
    conn: Any,
    *,
    repo_root: str,
    file_id: str,
    output_file: str,
    as_json: bool,
) -> dict[str, Any]:
    file_record = get_file_record(conn, file_id)
    if file_record is None:
        return {"error": f"File not found: {file_id}"}

    payload = get_file_content(conn, repo_root, file_id)
    if payload is None:
        return {"error": f"File content unavailable: {file_id}"}

    content, content_type, filename = payload
    file_record = dict(file_record)
    file_record["content_type"] = content_type or file_record.get("content_type")
    file_record["filename"] = filename or file_record.get("filename")

    if output_file:
        output_path = Path(output_file).expanduser()
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(content)
        return {
            "file": file_record,
            "written_to": str(output_path),
            "byte_count": len(content),
        }

    if as_json:
        return {
            "file": file_record,
            "content_base64": base64.b64encode(content).decode("ascii"),
        }

    try:
        content_text = content.decode("utf-8")
    except UnicodeDecodeError as exc:
        return {
            "error": (
                f"file content is not valid UTF-8 for inline display: {file_id}; "
                "rerun with --output-file or --json"
            )
        }
    return {"file": file_record, "content_text": content_text}


__all__ = ["_files_command"]
