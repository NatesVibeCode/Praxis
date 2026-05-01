"""Tests for the FORMAT_HANDLERS registry in core/file_formats."""

from __future__ import annotations

from core.file_formats import FORMAT_HANDLERS


class TestFormatRegistry:
    def test_all_expected_extensions_present(self) -> None:
        assert ".csv" in FORMAT_HANDLERS
        assert ".txt" in FORMAT_HANDLERS
        assert ".pdf" in FORMAT_HANDLERS
        assert ".docx" in FORMAT_HANDLERS

    def test_no_duplicate_formats(self) -> None:
        formats = [h.format for h in FORMAT_HANDLERS.values()]
        assert len(formats) == len(set(formats))

    def test_media_types_present(self) -> None:
        for ext, handler in FORMAT_HANDLERS.items():
            assert handler.media_type, f"No media_type for {ext}"

    def test_all_can_read_and_write(self) -> None:
        for ext, handler in FORMAT_HANDLERS.items():
            assert handler.can_read, f"{ext} cannot read"
            assert handler.can_write, f"{ext} cannot write"

    def test_extension_matches_handler_extensions(self) -> None:
        for ext, handler in FORMAT_HANDLERS.items():
            assert ext in handler.extensions, (
                f"Registry key {ext!r} not in handler.extensions {handler.extensions}"
            )

    def test_formats_are_valid_literals(self) -> None:
        valid = {"csv", "txt", "pdf", "docx", "xlsx", "pptx", "md", "html"}
        for handler in FORMAT_HANDLERS.values():
            assert handler.format in valid

    def test_csv_media_type(self) -> None:
        assert FORMAT_HANDLERS[".csv"].media_type == "text/csv"

    def test_pdf_media_type(self) -> None:
        assert FORMAT_HANDLERS[".pdf"].media_type == "application/pdf"

    def test_docx_media_type_contains_wordprocessingml(self) -> None:
        assert "wordprocessingml" in FORMAT_HANDLERS[".docx"].media_type
