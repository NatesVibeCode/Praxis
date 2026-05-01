"""Shared types for file format read/write primitives."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping


FileFormat = Literal["csv", "txt", "pdf", "docx", "xlsx", "pptx", "md", "html"]


@dataclass(frozen=True)
class ReadLimits:
    max_bytes: int = 10_000_000
    max_chars: int = 250_000
    max_rows: int = 50
    max_pages: int = 50
    max_paragraphs: int = 5_000
    max_slides: int = 100
    max_sheets: int = 20


@dataclass(frozen=True)
class ReadResult:
    path: str
    format: FileFormat
    media_type: str
    content: str
    sections: tuple[str, ...]
    metadata: Mapping[str, Any]
    structured: Mapping[str, Any] = field(default_factory=dict)
    warnings: tuple[str, ...] = ()
    truncated: bool = False
    source_sha256: str = ""
    source_bytes: int = 0


@dataclass(frozen=True)
class WriteResult:
    path: str
    format: FileFormat
    media_type: str
    bytes_written: int
    sha256: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class FormatHandler:
    format: FileFormat
    extensions: tuple[str, ...]
    media_type: str
    can_read: bool
    can_write: bool


FORMAT_HANDLERS: dict[str, FormatHandler] = {
    ".csv": FormatHandler(
        format="csv",
        extensions=(".csv",),
        media_type="text/csv",
        can_read=True,
        can_write=True,
    ),
    ".txt": FormatHandler(
        format="txt",
        extensions=(".txt",),
        media_type="text/plain",
        can_read=True,
        can_write=True,
    ),
    ".pdf": FormatHandler(
        format="pdf",
        extensions=(".pdf",),
        media_type="application/pdf",
        can_read=True,
        can_write=True,
    ),
    ".docx": FormatHandler(
        format="docx",
        extensions=(".docx",),
        media_type=(
            "application/vnd.openxmlformats-officedocument"
            ".wordprocessingml.document"
        ),
        can_read=True,
        can_write=True,
    ),
    ".xlsx": FormatHandler(
        format="xlsx",
        extensions=(".xlsx",),
        media_type=(
            "application/vnd.openxmlformats-officedocument"
            ".spreadsheetml.sheet"
        ),
        can_read=True,
        can_write=True,
    ),
    ".pptx": FormatHandler(
        format="pptx",
        extensions=(".pptx",),
        media_type=(
            "application/vnd.openxmlformats-officedocument"
            ".presentationml.presentation"
        ),
        can_read=True,
        can_write=True,
    ),
    ".md": FormatHandler(
        format="md",
        extensions=(".md",),
        media_type="text/markdown",
        can_read=True,
        can_write=True,
    ),
    ".html": FormatHandler(
        format="html",
        extensions=(".html", ".htm"),
        media_type="text/html",
        can_read=True,
        can_write=True,
    ),
}


class FileFormatError(Exception):
    pass


class UnsupportedFileFormatError(FileFormatError):
    pass


class FileReadError(FileFormatError):
    pass


class FileWriteError(FileFormatError):
    pass


class FileParseError(FileFormatError):
    pass


class FileLimitExceededError(FileFormatError):
    pass


class EncryptedDocumentError(FileFormatError):
    pass


class WriteValidationError(FileFormatError):
    pass
