"""Multi-format document extraction for the memory system."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from memory.engine import MemoryEngine

from memory.types import Entity, EntityType


@dataclass(frozen=True)
class ExtractedDocument:
    file_path: str
    format: str
    title: str
    content: str
    sections: tuple[str, ...]
    metadata: dict


class DocumentExtractor:
    """Dispatches file extraction by extension."""

    _FORMATS = (".md", ".json", ".csv", ".txt", ".py")

    def extract(self, file_path: str) -> ExtractedDocument:
        ext = os.path.splitext(file_path)[1].lower()
        if ext not in self._FORMATS:
            raise ValueError(f"Unsupported format: {ext}")
        handler = {
            ".md": self._extract_md,
            ".json": self._extract_json,
            ".csv": self._extract_csv,
            ".txt": self._extract_txt,
            ".py": self._extract_py,
        }[ext]
        return handler(file_path)

    def extract_batch(self, file_paths: list[str]) -> list[ExtractedDocument]:
        results: list[ExtractedDocument] = []
        for fp in file_paths:
            try:
                results.append(self.extract(fp))
            except Exception:
                continue
        return results

    def supported_formats(self) -> tuple[str, ...]:
        return self._FORMATS

    # ---- private handlers ----

    def _extract_md(self, file_path: str) -> ExtractedDocument:
        text = self._read(file_path)
        title = os.path.basename(file_path)
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("# ") and not stripped.startswith("## "):
                title = stripped[2:].strip()
                break
        sections: list[str] = []
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("## "):
                sections.append(stripped[3:].strip())
        return ExtractedDocument(
            file_path=file_path,
            format=".md",
            title=title,
            content=text,
            sections=tuple(sections),
            metadata={"line_count": len(text.splitlines())},
        )

    def _extract_json(self, file_path: str) -> ExtractedDocument:
        raw = self._read(file_path)
        data = json.loads(raw)
        title = os.path.basename(file_path)
        sections: tuple[str, ...] = ()
        if isinstance(data, dict):
            sections = tuple(str(k) for k in data.keys())
        content = json.dumps(data, indent=2)
        return ExtractedDocument(
            file_path=file_path,
            format=".json",
            title=title,
            content=content,
            sections=sections,
            metadata={"type": type(data).__name__},
        )

    def _extract_csv(self, file_path: str) -> ExtractedDocument:
        raw = self._read(file_path)
        lines = raw.splitlines()
        title = os.path.basename(file_path)
        headers: tuple[str, ...] = ()
        if lines:
            headers = tuple(h.strip() for h in lines[0].split(","))
        content = "\n".join(lines[:51])  # header + 50 rows
        return ExtractedDocument(
            file_path=file_path,
            format=".csv",
            title=title,
            content=content,
            sections=headers,
            metadata={"row_count": max(0, len(lines) - 1)},
        )

    def _extract_txt(self, file_path: str) -> ExtractedDocument:
        text = self._read(file_path)
        title = os.path.basename(file_path)
        return ExtractedDocument(
            file_path=file_path,
            format=".txt",
            title=title,
            content=text,
            sections=(),
            metadata={"char_count": len(text)},
        )

    def _extract_py(self, file_path: str) -> ExtractedDocument:
        text = self._read(file_path)
        title = os.path.basename(file_path)
        # Extract module docstring as title
        ds_match = re.match(r'^(?:"""(.*?)"""|\'\'\'(.*?)\'\'\')', text, re.DOTALL)
        if ds_match:
            docstring = (ds_match.group(1) or ds_match.group(2)).strip()
            first_line = docstring.splitlines()[0].strip()
            if first_line:
                title = first_line
        # Extract class/function names via regex
        names: list[str] = []
        for line in text.splitlines():
            m = re.match(r"^class\s+(\w+)", line)
            if m:
                names.append(m.group(1))
                continue
            m = re.match(r"^def\s+(\w+)", line)
            if m:
                names.append(m.group(1))
        return ExtractedDocument(
            file_path=file_path,
            format=".py",
            title=title,
            content=text,
            sections=tuple(names),
            metadata={"line_count": len(text.splitlines())},
        )

    @staticmethod
    def _read(file_path: str) -> str:
        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()


class FilingClerk:
    """Files extracted documents into a MemoryEngine as entities."""

    def __init__(self, engine: "MemoryEngine") -> None:
        self._engine = engine

    def file(self, doc: ExtractedDocument) -> int:
        now = datetime.now(timezone.utc)
        count = 0
        # One 'document' entity for the doc itself
        doc_entity = Entity(
            id=f"doc:{doc.file_path}",
            entity_type=EntityType.document,
            name=doc.title,
            content=doc.content,
            metadata={**doc.metadata, "format": doc.format, "file_path": doc.file_path},
            created_at=now,
            updated_at=now,
            source=doc.file_path,
            confidence=1.0,
        )
        self._engine.insert(doc_entity)
        count += 1
        # One entity per section
        for section in doc.sections:
            sec_entity = Entity(
                id=f"doc:{doc.file_path}##{section}",
                entity_type=EntityType.topic,
                name=section,
                content=f"Section '{section}' of {doc.title}",
                metadata={"parent_doc": doc.file_path},
                created_at=now,
                updated_at=now,
                source=doc.file_path,
                confidence=0.9,
            )
            self._engine.insert(sec_entity)
            count += 1
        return count

    def file_batch(self, docs: list[ExtractedDocument]) -> int:
        total = 0
        for doc in docs:
            total += self.file(doc)
        return total
