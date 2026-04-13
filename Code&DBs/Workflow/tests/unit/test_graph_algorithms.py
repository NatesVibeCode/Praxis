from __future__ import annotations

from datetime import datetime

from memory.graph import (
    BlastRadius,
    EnergyDecayTraversal,
    PageRank,
    RandomWalkWithRestart,
)
from memory.types import Edge, RelationType


def _edge(src: str, tgt: str, weight: float = 1.0) -> Edge:
    """Helper to build a minimal Edge."""
    return Edge(
        source_id=src,
        target_id=tgt,
        relation_type=RelationType.related_to,
        weight=weight,
        metadata={},
        created_at=datetime(2026, 1, 1),
    )


# ── PageRank ──────────────────────────────────────────────────────


class TestPageRank:
    def test_chain_c_highest(self):
        """In A->B->C, C (terminal sink) should have highest rank."""
        edges = [_edge("A", "B"), _edge("B", "C")]
        scores = PageRank().compute(edges)
        assert scores["C"] > scores["B"] > scores["A"]

    def test_cycle_converges(self):
        """A->B->C->A should converge with roughly equal scores."""
        edges = [_edge("A", "B"), _edge("B", "C"), _edge("C", "A")]
        scores = PageRank().compute(edges)
        vals = list(scores.values())
        # All nodes should be close to 1/3
        for v in vals:
            assert abs(v - 1 / 3) < 0.01

    def test_edge_weights(self):
        """Higher-weight edge should funnel more rank to its target."""
        edges = [
            _edge("A", "B", weight=0.9),
            _edge("A", "C", weight=0.1),
        ]
        scores = PageRank().compute(edges)
        assert scores["B"] > scores["C"]

    def test_empty_edges(self):
        scores = PageRank().compute([])
        assert scores == {}


# ── EnergyDecayTraversal ──────────────────────────────────────────


class TestEnergyDecay:
    def test_energy_decreases_with_depth(self):
        edges = [_edge("A", "B"), _edge("B", "C"), _edge("C", "D")]
        result = EnergyDecayTraversal().traverse("A", edges)
        assert result["A"] > result["B"] > result["C"] > result["D"]

    def test_stops_below_threshold(self):
        """With decay=0.1, energy drops below 0.01 quickly."""
        edges = [_edge("A", "B"), _edge("B", "C"), _edge("C", "D")]
        result = EnergyDecayTraversal().traverse(
            "A", edges, decay_factor=0.1
        )
        assert "A" in result
        assert "B" in result
        # 1.0 * 0.1 = 0.1 for B, 0.1 * 0.1 = 0.01 -> right at threshold
        # D should definitely not appear (0.001)
        assert "D" not in result

    def test_empty_edges(self):
        result = EnergyDecayTraversal().traverse("X", [])
        assert result == {"X": 1.0}


# ── RandomWalkWithRestart ────────────────────────────────────────


class TestRWR:
    def test_seeds_highest(self):
        """Seed nodes should have highest scores."""
        edges = [_edge("A", "B"), _edge("B", "C"), _edge("C", "D")]
        scores = RandomWalkWithRestart(restart_prob=0.15).compute(
            ["A"], edges
        )
        assert scores["A"] > scores["B"]
        assert scores["A"] > scores["C"]
        assert scores["A"] > scores["D"]

    def test_disconnected_unreachable(self):
        """Nodes not reachable from seed should get ~0."""
        edges = [_edge("A", "B"), _edge("X", "Y")]
        scores = RandomWalkWithRestart(restart_prob=0.15).compute(
            ["A"], edges
        )
        # X and Y are not reachable from A, so very low
        assert scores["A"] > 0.1
        assert scores.get("X", 0) < 0.01
        assert scores.get("Y", 0) < 0.01

    def test_empty_edges(self):
        scores = RandomWalkWithRestart().compute([], [])
        assert scores == {}


# ── BlastRadius ──────────────────────────────────────────────────


class TestBlastRadius:
    def test_direct_indirect_separation(self):
        edges = [
            _edge("A", "B"),
            _edge("A", "C"),
            _edge("B", "D"),
            _edge("D", "E"),
        ]
        result = BlastRadius().compute("A", edges, max_depth=3, decay=0.5)
        assert result.start_id == "A"
        assert "B" in result.direct
        assert "C" in result.direct
        assert "D" in result.indirect
        assert result.total_affected == len(result.direct) + len(result.indirect)

    def test_empty_edges(self):
        result = BlastRadius().compute("A", [])
        assert result.total_affected == 0
        assert result.direct == {}
        assert result.indirect == {}
