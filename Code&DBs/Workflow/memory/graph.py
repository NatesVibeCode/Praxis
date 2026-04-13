from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List

from memory.types import Edge


@dataclass(frozen=True)
class BlastResult:
    start_id: str
    direct: Dict[str, float]
    indirect: Dict[str, float]
    total_affected: int


class PageRank:
    """Standard PageRank over directed edges with weight-based conductivity."""

    def __init__(
        self,
        damping: float = 0.85,
        max_iterations: int = 100,
        tolerance: float = 1e-6,
    ) -> None:
        self.damping = damping
        self.max_iterations = max_iterations
        self.tolerance = tolerance

    def compute(self, edges: List[Edge]) -> Dict[str, float]:
        if not edges:
            return {}

        # Collect all nodes
        nodes: set[str] = set()
        # outgoing weighted adjacency: source -> list of (target, weight)
        out_adj: dict[str, list[tuple[str, float]]] = defaultdict(list)
        out_weight_sum: dict[str, float] = defaultdict(float)

        for e in edges:
            nodes.add(e.source_id)
            nodes.add(e.target_id)
            out_adj[e.source_id].append((e.target_id, e.weight))
            out_weight_sum[e.source_id] += e.weight

        n = len(nodes)
        node_list = sorted(nodes)
        idx = {nid: i for i, nid in enumerate(node_list)}

        scores = [1.0 / n] * n
        d = self.damping

        for _ in range(self.max_iterations):
            new_scores = [0.0] * n

            # Dangling node mass (nodes with no outgoing edges)
            dangling_mass = 0.0
            for nid in node_list:
                if nid not in out_adj or out_weight_sum[nid] == 0:
                    dangling_mass += scores[idx[nid]]

            # Distribute dangling mass uniformly
            base = (1 - d) / n + d * dangling_mass / n

            for i in range(n):
                new_scores[i] = base

            # Transfer mass along edges weighted by conductivity
            for src, neighbors in out_adj.items():
                ws = out_weight_sum[src]
                if ws == 0:
                    continue
                src_score = scores[idx[src]]
                for tgt, w in neighbors:
                    new_scores[idx[tgt]] += d * src_score * (w / ws)

            # Check convergence
            diff = sum(abs(new_scores[i] - scores[i]) for i in range(n))
            scores = new_scores
            if diff < self.tolerance:
                break

        return {node_list[i]: scores[i] for i in range(n)}


class EnergyDecayTraversal:
    """BFS traversal where energy decays by a factor at each hop."""

    def traverse(
        self,
        start_id: str,
        edges: List[Edge],
        initial_energy: float = 1.0,
        decay_factor: float = 0.5,
        max_depth: int = 5,
    ) -> Dict[str, float]:
        if not edges:
            return {start_id: initial_energy}

        # Build adjacency (directed: follow outgoing edges)
        adj: dict[str, list[tuple[str, float]]] = defaultdict(list)
        for e in edges:
            adj[e.source_id].append((e.target_id, e.weight))

        result: dict[str, float] = {start_id: initial_energy}
        # BFS frontier: list of (node_id, energy_at_node)
        frontier = [(start_id, initial_energy)]

        for _depth in range(max_depth):
            next_frontier: list[tuple[str, float]] = []
            for nid, energy in frontier:
                for tgt, w in adj.get(nid, []):
                    new_energy = energy * decay_factor * w
                    if new_energy <= 0.01:
                        continue
                    # Keep the max energy reaching a node
                    if tgt not in result or new_energy > result[tgt]:
                        result[tgt] = new_energy
                        next_frontier.append((tgt, new_energy))
            if not next_frontier:
                break
            frontier = next_frontier

        return result


class RandomWalkWithRestart:
    """Personalized PageRank via power iteration."""

    def __init__(
        self,
        restart_prob: float = 0.15,
        max_iterations: int = 100,
        tolerance: float = 1e-6,
    ) -> None:
        self.restart_prob = restart_prob
        self.max_iterations = max_iterations
        self.tolerance = tolerance

    def compute(
        self, seed_ids: List[str], edges: List[Edge]
    ) -> Dict[str, float]:
        if not edges and not seed_ids:
            return {}

        nodes: set[str] = set(seed_ids)
        out_adj: dict[str, list[tuple[str, float]]] = defaultdict(list)
        out_weight_sum: dict[str, float] = defaultdict(float)

        for e in edges:
            nodes.add(e.source_id)
            nodes.add(e.target_id)
            out_adj[e.source_id].append((e.target_id, e.weight))
            out_weight_sum[e.source_id] += e.weight

        n = len(nodes)
        if n == 0:
            return {}

        node_list = sorted(nodes)
        idx = {nid: i for i, nid in enumerate(node_list)}

        seed_set = set(seed_ids)
        num_seeds = len(seed_ids)
        restart_vec = [0.0] * n
        for sid in seed_ids:
            restart_vec[idx[sid]] = 1.0 / num_seeds

        alpha = self.restart_prob
        scores = list(restart_vec)  # start at restart distribution

        for _ in range(self.max_iterations):
            new_scores = [0.0] * n

            # Dangling mass redistributed to seeds
            dangling_mass = 0.0
            for nid in node_list:
                if nid not in out_adj or out_weight_sum[nid] == 0:
                    dangling_mass += scores[idx[nid]]

            # Restart component + dangling-to-seeds
            for i in range(n):
                new_scores[i] = alpha * restart_vec[i]
                # dangling mass goes to seeds
                if node_list[i] in seed_set:
                    new_scores[i] += (1 - alpha) * dangling_mass / num_seeds

            # Transfer along edges
            for src, neighbors in out_adj.items():
                ws = out_weight_sum[src]
                if ws == 0:
                    continue
                src_score = scores[idx[src]]
                for tgt, w in neighbors:
                    new_scores[idx[tgt]] += (1 - alpha) * src_score * (w / ws)

            diff = sum(abs(new_scores[i] - scores[i]) for i in range(n))
            scores = new_scores
            if diff < self.tolerance:
                break

        return {node_list[i]: scores[i] for i in range(n)}


class BlastRadius:
    """Compute blast radius using energy-decay, separating direct/indirect."""

    def compute(
        self,
        start_id: str,
        edges: List[Edge],
        max_depth: int = 3,
        decay: float = 0.5,
    ) -> BlastResult:
        if not edges:
            return BlastResult(
                start_id=start_id,
                direct={},
                indirect={},
                total_affected=0,
            )

        # Build adjacency
        adj: dict[str, list[tuple[str, float]]] = defaultdict(list)
        for e in edges:
            adj[e.source_id].append((e.target_id, e.weight))

        direct: dict[str, float] = {}
        indirect: dict[str, float] = {}

        # BFS with depth tracking
        # frontier: list of (node_id, energy, depth)
        frontier = [(start_id, 1.0, 0)]
        visited_energy: dict[str, float] = {start_id: 1.0}

        while frontier:
            next_frontier: list[tuple[str, float, int]] = []
            for nid, energy, depth in frontier:
                if depth >= max_depth:
                    continue
                for tgt, w in adj.get(nid, []):
                    new_energy = energy * decay * w
                    if new_energy <= 0.01:
                        continue
                    new_depth = depth + 1
                    # Only process if we haven't seen this node or found higher energy
                    if tgt not in visited_energy or new_energy > visited_energy[tgt]:
                        visited_energy[tgt] = new_energy
                        if new_depth == 1:
                            direct[tgt] = new_energy
                            indirect.pop(tgt, None)
                        else:
                            if tgt not in direct:
                                indirect[tgt] = new_energy
                        next_frontier.append((tgt, new_energy, new_depth))
            frontier = next_frontier

        total = len(direct) + len(indirect)
        return BlastResult(
            start_id=start_id,
            direct=direct,
            indirect=indirect,
            total_affected=total,
        )
