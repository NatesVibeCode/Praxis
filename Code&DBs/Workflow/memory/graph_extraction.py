"""Two-pass graph extraction: nodes first, then edges."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass


@dataclass(frozen=True)
class ExtractedNode:
    node_id: str
    node_type: str  # module|tool|constraint|pattern|decision|state|concept|preference
    name: str
    content: str


@dataclass(frozen=True)
class ExtractedEdge:
    source_id: str
    target_id: str
    relation_type: str
    confidence: float


@dataclass(frozen=True)
class ExtractionResult:
    nodes: tuple[ExtractedNode, ...]
    edges: tuple[ExtractedEdge, ...]


class NodeExtractor:
    """Pass 1: deterministic node extraction from text."""

    def extract(self, text: str) -> list[ExtractedNode]:
        seen: dict[str, ExtractedNode] = {}
        for line in text.splitlines():
            stripped = line.strip()

            # DECISION: lines
            if stripped.startswith("DECISION:"):
                value = stripped[len("DECISION:"):].strip()
                name = self._slugify(value)
                nid = f"decision:{name}"
                if nid not in seen:
                    seen[nid] = ExtractedNode(nid, "decision", name, value)

            # CONSTRAINT: lines
            elif stripped.startswith("CONSTRAINT:"):
                value = stripped[len("CONSTRAINT:"):].strip()
                name = self._slugify(value)
                nid = f"constraint:{name}"
                if nid not in seen:
                    seen[nid] = ExtractedNode(nid, "constraint", name, value)

            # Import statements -> module nodes
            elif re.match(r"^(?:import|from)\s+", stripped):
                mod = self._parse_import(stripped)
                if mod:
                    nid = f"module:{mod}"
                    if nid not in seen:
                        seen[nid] = ExtractedNode(nid, "module", mod, stripped)

            # Class definitions -> tool nodes
            elif re.match(r"^class\s+\w+", stripped):
                m = re.match(r"^class\s+(\w+)", stripped)
                if m:
                    name = m.group(1)
                    nid = f"tool:{name}"
                    if nid not in seen:
                        seen[nid] = ExtractedNode(nid, "tool", name, stripped)

            # Function definitions -> concept nodes
            elif re.match(r"^def\s+\w+", stripped):
                m = re.match(r"^def\s+(\w+)", stripped)
                if m:
                    name = m.group(1)
                    nid = f"concept:{name}"
                    if nid not in seen:
                        seen[nid] = ExtractedNode(nid, "concept", name, stripped)

            # # Pattern: comments -> pattern nodes
            elif stripped.startswith("# Pattern:"):
                value = stripped[len("# Pattern:"):].strip()
                name = self._slugify(value)
                nid = f"pattern:{name}"
                if nid not in seen:
                    seen[nid] = ExtractedNode(nid, "pattern", name, value)

        return list(seen.values())

    @staticmethod
    def _slugify(text: str) -> str:
        slug = re.sub(r"[^a-zA-Z0-9]+", ".", text.strip().lower())
        return slug.strip(".")

    @staticmethod
    def _parse_import(line: str) -> str | None:
        # "from foo.bar import baz" -> "foo.bar"
        m = re.match(r"^from\s+([\w.]+)\s+import", line)
        if m:
            return m.group(1)
        # "import foo.bar" -> "foo.bar"
        m = re.match(r"^import\s+([\w.]+)", line)
        if m:
            return m.group(1)
        return None


class EdgeExtractor:
    """Pass 2: extract edges given known nodes."""

    _RELATION_MAP: dict[str, str] = {
        "depends on": "depends_on",
        "requires": "depends_on",
        "imports": "depends_on",
        "implements": "implements",
        "provides": "implements",
        "constrains": "constrains",
        "limits": "constrains",
        "restricts": "constrains",
        "supersedes": "supersedes",
        "replaces": "supersedes",
    }

    def extract(self, text: str, known_nodes: list[ExtractedNode]) -> list[ExtractedEdge]:
        edges: list[ExtractedEdge] = []
        seen: set[tuple[str, str, str]] = set()

        # Build a name->node_id lookup
        name_to_id: dict[str, str] = {}
        for node in known_nodes:
            name_to_id[node.name] = node.node_id

        for line in text.splitlines():
            lower = line.lower()
            for keyword, rel in self._RELATION_MAP.items():
                if keyword in lower:
                    self._match_edge_in_line(
                        line, keyword, rel, name_to_id, edges, seen
                    )

            # Co-reference: if two known node names appear on the same line,
            # create a related_to edge
            found_ids: list[str] = []
            for name, nid in name_to_id.items():
                if name in line:
                    found_ids.append(nid)
            for i in range(len(found_ids)):
                for j in range(i + 1, len(found_ids)):
                    key = (found_ids[i], found_ids[j], "related_to")
                    if key not in seen:
                        seen.add(key)
                        edges.append(ExtractedEdge(
                            source_id=found_ids[i],
                            target_id=found_ids[j],
                            relation_type="related_to",
                            confidence=0.5,
                        ))

        return edges

    def _match_edge_in_line(
        self,
        line: str,
        keyword: str,
        relation: str,
        name_to_id: dict[str, str],
        edges: list[ExtractedEdge],
        seen: set[tuple[str, str, str]],
    ) -> None:
        # Find which known node names appear in the line
        found: list[str] = []
        for name, nid in name_to_id.items():
            if name in line:
                found.append(nid)
        # Try to pair source (before keyword) and target (after keyword)
        lower = line.lower()
        idx = lower.index(keyword)
        before = line[:idx]
        after = line[idx + len(keyword):]
        sources = [nid for name, nid in name_to_id.items() if name in before]
        targets = [nid for name, nid in name_to_id.items() if name in after]
        for s in sources:
            for t in targets:
                if s != t:
                    key = (s, t, relation)
                    if key not in seen:
                        seen.add(key)
                        edges.append(ExtractedEdge(
                            source_id=s,
                            target_id=t,
                            relation_type=relation,
                            confidence=0.8,
                        ))


class GraphExtractor:
    """Runs both extraction passes."""

    def __init__(self) -> None:
        self._node_extractor = NodeExtractor()
        self._edge_extractor = EdgeExtractor()

    def extract(self, text: str) -> ExtractionResult:
        nodes = self._node_extractor.extract(text)
        edges = self._edge_extractor.extract(text, nodes)
        return ExtractionResult(nodes=tuple(nodes), edges=tuple(edges))

    def extract_from_file(self, file_path: str) -> ExtractionResult:
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()
        return self.extract(text)
