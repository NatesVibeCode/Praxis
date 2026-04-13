"""Graph-controlled output writer.

The graph (not the model) owns all filesystem writes. After a model
produces StructuredOutput via stdout, this module applies the code blocks
to disk under the graph's authority.

Write policy:
  - Only write to paths within the workspace root
  - Validate paths (no directory traversal, no absolute paths)
  - Create parent directories as needed
  - Atomic writes via temp file + rename
  - Return a manifest of what was written for evidence recording
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Result of writing a single code block to disk."""

    file_path: str
    absolute_path: str
    action: str
    bytes_written: int
    success: bool
    error: str | None = None


@dataclass(frozen=True, slots=True)
class WriteManifest:
    """Manifest of all files written by the graph after a dispatch."""

    results: tuple[WriteResult, ...]
    workspace_root: str
    total_files: int
    total_bytes: int
    all_succeeded: bool


def _validate_path(file_path: str, workspace_root: Path) -> tuple[Path, str | None]:
    """Validate a file path is safe to write.

    Returns (resolved_path, error_or_none).
    """
    if not file_path or not file_path.strip():
        return Path(), "empty file path"

    # Reject absolute paths
    if os.path.isabs(file_path):
        return Path(), f"absolute path not allowed: {file_path}"

    # Resolve relative to workspace
    resolved = (workspace_root / file_path).resolve()

    # Ensure it's within workspace (prevent directory traversal)
    try:
        resolved.relative_to(workspace_root.resolve())
    except ValueError:
        return Path(), f"path escapes workspace: {file_path}"

    return resolved, None


def apply_structured_output(
    code_blocks: list[dict[str, Any]],
    *,
    workspace_root: str,
    dry_run: bool = False,
) -> WriteManifest:
    """Apply code blocks from StructuredOutput to the filesystem.

    Parameters
    ----------
    code_blocks:
        List of dicts with keys: file_path, content, language, action.
        This is the serialized form from DeterministicTaskResult.outputs["structured_output"]["code_blocks"].
    workspace_root:
        Root directory for all writes. Paths in code_blocks are relative to this.
    dry_run:
        If True, validate paths but don't actually write files.

    Returns
    -------
    WriteManifest with results for each code block.
    """
    root = Path(workspace_root).resolve()
    results: list[WriteResult] = []

    for block in code_blocks:
        file_path = str(block.get("file_path", ""))
        content = str(block.get("content", ""))
        action = str(block.get("action", "replace"))

        # Validate path
        resolved, error = _validate_path(file_path, root)
        if error:
            results.append(WriteResult(
                file_path=file_path,
                absolute_path="",
                action=action,
                bytes_written=0,
                success=False,
                error=error,
            ))
            continue

        if dry_run:
            results.append(WriteResult(
                file_path=file_path,
                absolute_path=str(resolved),
                action=action,
                bytes_written=len(content.encode("utf-8")),
                success=True,
            ))
            continue

        try:
            # Create parent directories
            resolved.parent.mkdir(parents=True, exist_ok=True)

            # Normalize: treat "create" on existing files as "replace"
            # Models sometimes return action="create" for files that exist
            if action == "create" and resolved.exists():
                action = "replace"

            # Atomic write: write to temp file, then rename
            fd, tmp_path = tempfile.mkstemp(
                dir=str(resolved.parent),
                prefix=".praxis_write_",
                suffix=".tmp",
            )
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as fh:
                    fh.write(content)
                os.replace(tmp_path, str(resolved))
            except Exception:
                # Clean up temp file on failure
                try:
                    os.unlink(tmp_path)
                except OSError:
                    pass
                raise

            results.append(WriteResult(
                file_path=file_path,
                absolute_path=str(resolved),
                action=action,
                bytes_written=len(content.encode("utf-8")),
                success=True,
            ))

        except Exception as exc:
            results.append(WriteResult(
                file_path=file_path,
                absolute_path=str(resolved),
                action=action,
                bytes_written=0,
                success=False,
                error=str(exc),
            ))

    total_bytes = sum(r.bytes_written for r in results)
    all_succeeded = all(r.success for r in results) if results else True

    return WriteManifest(
        results=tuple(results),
        workspace_root=str(root),
        total_files=len(results),
        total_bytes=total_bytes,
        all_succeeded=all_succeeded,
    )
