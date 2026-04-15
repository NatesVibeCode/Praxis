from __future__ import annotations

from pathlib import Path

import pytest

from runtime import file_storage


class _FakePg:
    def __init__(self, *, insert_error: Exception | None = None, delete_error: Exception | None = None) -> None:
        self.insert_error = insert_error
        self.delete_error = delete_error
        self.storage_path: str | None = None
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    def execute(self, query: str, *params):
        self.calls.append((query, params))
        normalized = " ".join(query.split())
        if normalized.startswith("INSERT INTO uploaded_files"):
            if self.insert_error is not None:
                raise self.insert_error
            self.storage_path = str(params[4])
            return []
        if normalized.startswith("SELECT storage_path FROM uploaded_files WHERE id = $1"):
            if self.storage_path is None:
                return []
            return [{"storage_path": self.storage_path}]
        if normalized.startswith("DELETE FROM uploaded_files WHERE id = $1 RETURNING storage_path"):
            if self.delete_error is not None:
                raise self.delete_error
            storage_path = self.storage_path
            self.storage_path = None
            if storage_path is None:
                return []
            return [{"storage_path": storage_path}]
        raise AssertionError(f"unexpected query: {normalized}")


def test_save_file_removes_disk_artifact_when_metadata_insert_fails(tmp_path: Path) -> None:
    pg = _FakePg(insert_error=RuntimeError("insert failed"))

    with pytest.raises(RuntimeError, match="insert failed"):
        file_storage.save_file(
            pg,
            str(tmp_path),
            filename="report.txt",
            content=b"payload",
        )

    upload_dir = tmp_path / file_storage.UPLOAD_DIR
    assert upload_dir.exists()
    assert list(upload_dir.iterdir()) == []


def test_delete_file_keeps_disk_artifact_when_metadata_delete_fails(tmp_path: Path) -> None:
    upload_dir = file_storage.ensure_upload_dir(str(tmp_path))
    file_path = upload_dir / "file_abc123.txt"
    file_path.write_bytes(b"payload")

    pg = _FakePg(delete_error=RuntimeError("delete failed"))
    pg.storage_path = str(Path(file_storage.UPLOAD_DIR) / file_path.name)

    with pytest.raises(RuntimeError, match="delete failed"):
        file_storage.delete_file(pg, str(tmp_path), "file_abc123")

    assert file_path.read_bytes() == b"payload"


def test_delete_file_removes_disk_artifact_after_metadata_delete(tmp_path: Path) -> None:
    upload_dir = file_storage.ensure_upload_dir(str(tmp_path))
    file_path = upload_dir / "file_abc123.txt"
    file_path.write_bytes(b"payload")

    pg = _FakePg()
    pg.storage_path = str(Path(file_storage.UPLOAD_DIR) / file_path.name)

    deleted = file_storage.delete_file(pg, str(tmp_path), "file_abc123")

    assert deleted is True
    assert not file_path.exists()
