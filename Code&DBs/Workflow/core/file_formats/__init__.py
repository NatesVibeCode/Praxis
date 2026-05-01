"""File format read/write primitives for CSV, TXT, PDF, DOCX, XLSX, PPTX, Markdown, HTML."""

from core.file_formats.models import (
    EncryptedDocumentError,
    FileFormatError,
    FileLimitExceededError,
    FileParseError,
    FileReadError,
    FileWriteError,
    FORMAT_HANDLERS,
    FormatHandler,
    ReadLimits,
    ReadResult,
    UnsupportedFileFormatError,
    WriteResult,
    WriteValidationError,
)
from core.file_formats.readers import (
    read_csv,
    read_docx,
    read_file,
    read_html,
    read_md,
    read_pdf,
    read_pptx,
    read_txt,
    read_xlsx,
)
from core.file_formats.writers import (
    write_csv,
    write_docx,
    write_html,
    write_md,
    write_pdf,
    write_pptx,
    write_txt,
    write_xlsx,
)

__all__ = [
    # models
    "FileFormat",
    "ReadLimits",
    "ReadResult",
    "WriteResult",
    "FormatHandler",
    "FORMAT_HANDLERS",
    # errors
    "FileFormatError",
    "UnsupportedFileFormatError",
    "FileReadError",
    "FileWriteError",
    "FileParseError",
    "FileLimitExceededError",
    "EncryptedDocumentError",
    "WriteValidationError",
    # readers
    "read_file",
    "read_csv",
    "read_txt",
    "read_pdf",
    "read_docx",
    "read_xlsx",
    "read_pptx",
    "read_md",
    "read_html",
    # writers
    "write_csv",
    "write_txt",
    "write_docx",
    "write_pdf",
    "write_xlsx",
    "write_pptx",
    "write_md",
    "write_html",
]
