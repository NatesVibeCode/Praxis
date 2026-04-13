"""Tests for runtime.output_writer — graph-controlled file writing.

Verifies that the graph correctly applies code blocks to the filesystem
and rejects unsafe paths (directory traversal, absolute paths, etc.).
"""

from __future__ import annotations

import os
import tempfile

import pytest

from runtime.output_writer import apply_structured_output, WriteManifest


@pytest.fixture
def workspace(tmp_path):
    """Create a temporary workspace directory."""
    return str(tmp_path)


class TestApplyStructuredOutput:
    """Core output writer functionality."""

    def test_write_single_file(self, workspace):
        blocks = [
            {
                "file_path": "hello.py",
                "content": "print('hello')\n",
                "language": "python",
                "action": "replace",
            }
        ]
        manifest = apply_structured_output(blocks, workspace_root=workspace)
        assert manifest.all_succeeded
        assert manifest.total_files == 1
        assert manifest.total_bytes > 0

        written = os.path.join(workspace, "hello.py")
        assert os.path.exists(written)
        with open(written) as f:
            assert f.read() == "print('hello')\n"

    def test_write_multiple_files(self, workspace):
        blocks = [
            {"file_path": "a.py", "content": "# a\n", "language": "python", "action": "replace"},
            {"file_path": "b.py", "content": "# b\n", "language": "python", "action": "replace"},
        ]
        manifest = apply_structured_output(blocks, workspace_root=workspace)
        assert manifest.all_succeeded
        assert manifest.total_files == 2
        assert os.path.exists(os.path.join(workspace, "a.py"))
        assert os.path.exists(os.path.join(workspace, "b.py"))

    def test_write_nested_path(self, workspace):
        blocks = [
            {
                "file_path": "runtime/domain.py",
                "content": "class RunState:\n    pass\n",
                "language": "python",
                "action": "replace",
            }
        ]
        manifest = apply_structured_output(blocks, workspace_root=workspace)
        assert manifest.all_succeeded
        assert os.path.exists(os.path.join(workspace, "runtime", "domain.py"))

    def test_replace_existing_file(self, workspace):
        path = os.path.join(workspace, "existing.py")
        with open(path, "w") as f:
            f.write("old content\n")

        blocks = [
            {"file_path": "existing.py", "content": "new content\n", "language": "python", "action": "replace"},
        ]
        manifest = apply_structured_output(blocks, workspace_root=workspace)
        assert manifest.all_succeeded
        with open(path) as f:
            assert f.read() == "new content\n"

    def test_create_action_normalizes_to_replace_if_exists(self, workspace):
        """action=create on an existing file normalizes to replace (models often return create)."""
        path = os.path.join(workspace, "existing.py")
        with open(path, "w") as f:
            f.write("existing\n")

        blocks = [
            {"file_path": "existing.py", "content": "new\n", "language": "python", "action": "create"},
        ]
        manifest = apply_structured_output(blocks, workspace_root=workspace)
        assert manifest.all_succeeded
        with open(path) as f:
            assert f.read() == "new\n"

    def test_create_action_succeeds_if_not_exists(self, workspace):
        blocks = [
            {"file_path": "new_file.py", "content": "# new\n", "language": "python", "action": "create"},
        ]
        manifest = apply_structured_output(blocks, workspace_root=workspace)
        assert manifest.all_succeeded


class TestPathSafety:
    """Verify the output writer rejects unsafe paths."""

    def test_reject_absolute_path(self, workspace):
        blocks = [
            {"file_path": "/etc/passwd", "content": "hacked\n", "language": "text", "action": "replace"},
        ]
        manifest = apply_structured_output(blocks, workspace_root=workspace)
        assert not manifest.all_succeeded
        assert "absolute path" in manifest.results[0].error

    def test_reject_directory_traversal(self, workspace):
        blocks = [
            {"file_path": "../../etc/passwd", "content": "hacked\n", "language": "text", "action": "replace"},
        ]
        manifest = apply_structured_output(blocks, workspace_root=workspace)
        assert not manifest.all_succeeded
        assert "escapes workspace" in manifest.results[0].error

    def test_reject_empty_path(self, workspace):
        blocks = [
            {"file_path": "", "content": "test\n", "language": "python", "action": "replace"},
        ]
        manifest = apply_structured_output(blocks, workspace_root=workspace)
        assert not manifest.all_succeeded

    def test_reject_whitespace_path(self, workspace):
        blocks = [
            {"file_path": "   ", "content": "test\n", "language": "python", "action": "replace"},
        ]
        manifest = apply_structured_output(blocks, workspace_root=workspace)
        assert not manifest.all_succeeded


class TestDryRun:
    """Verify dry_run mode doesn't write files."""

    def test_dry_run_no_write(self, workspace):
        blocks = [
            {"file_path": "test.py", "content": "# test\n", "language": "python", "action": "replace"},
        ]
        manifest = apply_structured_output(blocks, workspace_root=workspace, dry_run=True)
        assert manifest.all_succeeded
        assert manifest.total_files == 1
        assert not os.path.exists(os.path.join(workspace, "test.py"))

    def test_dry_run_still_validates_paths(self, workspace):
        blocks = [
            {"file_path": "/etc/passwd", "content": "hacked\n", "language": "text", "action": "replace"},
        ]
        manifest = apply_structured_output(blocks, workspace_root=workspace, dry_run=True)
        assert not manifest.all_succeeded


class TestWriteManifest:
    """Verify WriteManifest metadata."""

    def test_empty_blocks(self, workspace):
        manifest = apply_structured_output([], workspace_root=workspace)
        assert manifest.all_succeeded
        assert manifest.total_files == 0
        assert manifest.total_bytes == 0

    def test_manifest_has_workspace_root(self, workspace):
        manifest = apply_structured_output([], workspace_root=workspace)
        assert manifest.workspace_root == os.path.realpath(workspace)
