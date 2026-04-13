"""Tests for document_extraction and graph_extraction modules."""

from __future__ import annotations

import json
import os
import tempfile

import pytest
from _pg_test_conn import get_test_conn

from memory.document_extraction import DocumentExtractor, ExtractedDocument, FilingClerk
from memory.graph_extraction import (
    EdgeExtractor,
    ExtractedEdge,
    ExtractedNode,
    ExtractionResult,
    GraphExtractor,
    NodeExtractor,
)
from memory.engine import MemoryEngine
from memory.types import EntityType


# --------------- fixtures ---------------

@pytest.fixture
def tmp_dir():
    with tempfile.TemporaryDirectory() as d:
        yield d


@pytest.fixture
def extractor():
    return DocumentExtractor()


@pytest.fixture
def engine():
    return MemoryEngine(conn=get_test_conn())


def _write(directory: str, name: str, content: str) -> str:
    path = os.path.join(directory, name)
    with open(path, "w") as f:
        f.write(content)
    return path


# =========== DocumentExtractor ===========

class TestDocumentExtractor:

    def test_supported_formats(self, extractor):
        fmts = extractor.supported_formats()
        assert ".md" in fmts
        assert ".json" in fmts
        assert ".csv" in fmts
        assert ".txt" in fmts
        assert ".py" in fmts

    def test_extract_md_title_and_sections(self, extractor, tmp_dir):
        path = _write(tmp_dir, "readme.md", "# My Title\n\nIntro\n\n## Setup\n\nStuff\n\n## Usage\n")
        doc = extractor.extract(path)
        assert doc.title == "My Title"
        assert doc.format == ".md"
        assert doc.sections == ("Setup", "Usage")
        assert isinstance(doc, ExtractedDocument)
        assert doc.metadata["line_count"] > 0

    def test_extract_md_no_heading_uses_filename(self, extractor, tmp_dir):
        path = _write(tmp_dir, "notes.md", "Just some text\n")
        doc = extractor.extract(path)
        assert doc.title == "notes.md"

    def test_extract_json_dict(self, extractor, tmp_dir):
        data = {"alpha": 1, "beta": [2, 3]}
        path = _write(tmp_dir, "config.json", json.dumps(data))
        doc = extractor.extract(path)
        assert doc.format == ".json"
        assert doc.title == "config.json"
        assert doc.sections == ("alpha", "beta")
        assert '"alpha"' in doc.content

    def test_extract_json_list(self, extractor, tmp_dir):
        path = _write(tmp_dir, "items.json", json.dumps([1, 2, 3]))
        doc = extractor.extract(path)
        assert doc.sections == ()

    def test_extract_csv(self, extractor, tmp_dir):
        rows = "name,age,city\nAlice,30,NYC\nBob,25,LA\n"
        path = _write(tmp_dir, "people.csv", rows)
        doc = extractor.extract(path)
        assert doc.format == ".csv"
        assert doc.sections == ("name", "age", "city")
        assert doc.metadata["row_count"] == 2

    def test_extract_csv_truncates_at_50_rows(self, extractor, tmp_dir):
        header = "col\n"
        rows = "".join(f"row{i}\n" for i in range(100))
        path = _write(tmp_dir, "big.csv", header + rows)
        doc = extractor.extract(path)
        # content should have header + 50 data rows = 51 lines
        assert len(doc.content.splitlines()) == 51

    def test_extract_txt(self, extractor, tmp_dir):
        path = _write(tmp_dir, "log.txt", "line one\nline two\n")
        doc = extractor.extract(path)
        assert doc.format == ".txt"
        assert doc.title == "log.txt"
        assert doc.sections == ()
        assert "line one" in doc.content

    def test_extract_py_docstring_and_names(self, extractor, tmp_dir):
        code = '"""My Module Title"""\n\nclass Foo:\n    pass\n\ndef bar():\n    pass\n'
        path = _write(tmp_dir, "mod.py", code)
        doc = extractor.extract(path)
        assert doc.title == "My Module Title"
        assert "Foo" in doc.sections
        assert "bar" in doc.sections

    def test_extract_py_no_docstring(self, extractor, tmp_dir):
        code = "class A:\n    pass\n"
        path = _write(tmp_dir, "plain.py", code)
        doc = extractor.extract(path)
        assert doc.title == "plain.py"

    def test_extract_unsupported_raises(self, extractor, tmp_dir):
        path = _write(tmp_dir, "data.xml", "<root/>")
        with pytest.raises(ValueError, match="Unsupported"):
            extractor.extract(path)

    def test_extract_batch_skips_failures(self, extractor, tmp_dir):
        good = _write(tmp_dir, "ok.txt", "hello")
        bad = os.path.join(tmp_dir, "nope.xml")
        with open(bad, "w") as f:
            f.write("<x/>")
        missing = os.path.join(tmp_dir, "ghost.txt")
        results = extractor.extract_batch([good, bad, missing])
        assert len(results) == 1
        assert results[0].title == "ok.txt"

    def test_extracted_document_is_frozen(self, extractor, tmp_dir):
        path = _write(tmp_dir, "f.txt", "x")
        doc = extractor.extract(path)
        with pytest.raises(AttributeError):
            doc.title = "changed"


# =========== FilingClerk ===========

class TestFilingClerk:

    def test_file_creates_doc_and_section_entities(self, engine):
        clerk = FilingClerk(engine)
        doc = ExtractedDocument(
            file_path="/tmp/test.md",
            format=".md",
            title="Test Doc",
            content="# Test Doc\n\n## A\n\n## B\n",
            sections=("A", "B"),
            metadata={"line_count": 5},
        )
        count = clerk.file(doc)
        assert count == 3  # 1 doc + 2 sections
        docs = [e for e in engine.list(EntityType.document) if e.name == "Test Doc"]
        assert len(docs) >= 1
        assert docs[0].name == "Test Doc"

    def test_file_batch_totals(self, engine):
        clerk = FilingClerk(engine)
        docs = [
            ExtractedDocument("/a.txt", ".txt", "A", "aaa", (), {}),
            ExtractedDocument("/b.txt", ".txt", "B", "bbb", ("s1",), {}),
        ]
        total = clerk.file_batch(docs)
        assert total == 3  # 1+0 + 1+1


# =========== NodeExtractor ===========

class TestNodeExtractor:

    def test_decision_node(self):
        text = "DECISION: use sqlite for storage"
        nodes = NodeExtractor().extract(text)
        assert any(n.node_type == "decision" for n in nodes)
        assert any("sqlite" in n.content.lower() for n in nodes)

    def test_constraint_node(self):
        text = "CONSTRAINT: max 100 entities per batch"
        nodes = NodeExtractor().extract(text)
        assert any(n.node_type == "constraint" for n in nodes)

    def test_import_module_node(self):
        text = "import os\nfrom pathlib import Path\n"
        nodes = NodeExtractor().extract(text)
        ids = {n.node_id for n in nodes}
        assert "module:os" in ids
        assert "module:pathlib" in ids

    def test_class_becomes_tool(self):
        text = "class MyRouter:\n    pass\n"
        nodes = NodeExtractor().extract(text)
        assert any(n.node_id == "tool:MyRouter" for n in nodes)

    def test_function_becomes_concept(self):
        text = "def process_batch():\n    pass\n"
        nodes = NodeExtractor().extract(text)
        assert any(n.node_id == "concept:process_batch" for n in nodes)

    def test_pattern_node(self):
        text = "# Pattern: retry with exponential backoff"
        nodes = NodeExtractor().extract(text)
        assert any(n.node_type == "pattern" for n in nodes)

    def test_idempotent_ids(self):
        text = "import os\nimport os\n"
        nodes = NodeExtractor().extract(text)
        ids = [n.node_id for n in nodes]
        assert ids.count("module:os") == 1


# =========== EdgeExtractor ===========

class TestEdgeExtractor:

    def test_depends_on_edge(self):
        nodes = [
            ExtractedNode("tool:Router", "tool", "Router", ""),
            ExtractedNode("module:sqlite", "module", "sqlite", ""),
        ]
        text = "Router depends on sqlite for persistence"
        edges = EdgeExtractor().extract(text, nodes)
        dep = [e for e in edges if e.relation_type == "depends_on"]
        assert len(dep) >= 1
        assert dep[0].source_id == "tool:Router"
        assert dep[0].target_id == "module:sqlite"

    def test_co_reference_edge(self):
        nodes = [
            ExtractedNode("tool:A", "tool", "A", ""),
            ExtractedNode("tool:B", "tool", "B", ""),
        ]
        text = "Both A and B are used here"
        edges = EdgeExtractor().extract(text, nodes)
        related = [e for e in edges if e.relation_type == "related_to"]
        assert len(related) >= 1


# =========== GraphExtractor ===========

class TestGraphExtractor:

    def test_full_extraction(self, tmp_dir):
        code = (
            "DECISION: use graph model\n"
            "import json\n"
            "class Builder:\n"
            "    pass\n"
            "# Builder depends on json\n"
        )
        path = _write(tmp_dir, "sample.py", code)
        result = GraphExtractor().extract_from_file(path)
        assert isinstance(result, ExtractionResult)
        assert len(result.nodes) >= 3  # decision, module:json, tool:Builder
        node_ids = {n.node_id for n in result.nodes}
        assert "module:json" in node_ids
        assert "tool:Builder" in node_ids

    def test_extract_returns_frozen_result(self):
        result = GraphExtractor().extract("DECISION: test\n")
        assert isinstance(result.nodes, tuple)
        assert isinstance(result.edges, tuple)
