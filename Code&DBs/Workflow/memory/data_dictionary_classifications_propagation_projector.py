"""Propagate PII / sensitive classifications along lineage edges.

When an upstream object is tagged `pii` or `sensitive`, every reachable
downstream object is almost certainly carrying the same exposure. This
projector walks `data_dictionary_lineage_effective` forward from every
tagged root and emits inherited classifications at the object level
(`field_path=""`) on each downstream node.

Emissions are `source=auto` with `origin_ref.projector =
classifications_lineage_propagation`, so
`replace_projected_classifications` prunes stale rows idempotently when
upstream tags are removed or lineage changes. Operator tags
(`source=operator`) are never overwritten.

Two propagation directions:

* **Forward-flow edges** (`produces`, `consumes`, `derives_from`,
  `projects_to`, `promotes_to`): taint flows src → dst. If the source
  contained PII, the derived downstream surface still carries it.
* **Reverse-flow edges** (`references`): taint flows dst → src. An FK
  like `bugs.filed_by → users.user_id` means `bugs` is *linkable* to a
  PII table; under most privacy regimes linkability = PII.

The tag is emitted with `tag_value=linkable` for reverse-flow
propagation (so operators can visually distinguish "stores raw PII"
from "joinable to PII") and `tag_value=inherited` for forward-flow.
"""
from __future__ import annotations

import logging
import time
from typing import Any, Iterable

from runtime.data_dictionary_classifications import apply_projected_classifications
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok

logger = logging.getLogger(__name__)


_PROJECTOR_TAG = "classifications_lineage_propagation"
_PROPAGATED_TAG_KEYS = ("pii", "sensitive")
_FORWARD_EDGE_KINDS = (
    "produces", "consumes", "derives_from", "projects_to", "promotes_to",
)
_REVERSE_EDGE_KINDS = ("references",)
_ALL_PROPAGATION_EDGE_KINDS = _FORWARD_EDGE_KINDS + _REVERSE_EDGE_KINDS
_MAX_DEPTH = 3


# --- SQL ------------------------------------------------------------------

_SQL_TAGGED_ROOTS = """
SELECT DISTINCT object_kind, tag_key
FROM data_dictionary_classifications_effective
WHERE tag_key = ANY($1)
"""

_SQL_FLOW_EDGES = """
SELECT src_object_kind, dst_object_kind, edge_kind
FROM data_dictionary_lineage_effective
WHERE edge_kind = ANY($1)
  AND src_object_kind IS NOT NULL
  AND dst_object_kind IS NOT NULL
  AND src_object_kind <> dst_object_kind
"""


# --- Projector ------------------------------------------------------------

class DataDictionaryClassificationsPropagationProjector(HeartbeatModule):
    """Walk lineage from every tagged root and emit inherited tags."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "data_dictionary_classifications_propagation_projector"

    # -- graph helpers ----------------------------------------------------

    def _load_adjacency(
        self,
    ) -> tuple[
        dict[str, list[tuple[str, str]]],
        dict[str, list[tuple[str, str]]],
    ]:
        """Build two adjacency maps: forward (src→dst) and reverse (dst→src).

        Forward walks propagate "inherited" PII taint: a derived table
        that reads from a PII source is itself PII.

        Reverse walks propagate "linkable" PII taint along FK
        references: a table that FK-points to a PII table is linkable
        and therefore PII-adjacent.
        """
        forward: dict[str, list[tuple[str, str]]] = {}
        reverse: dict[str, list[tuple[str, str]]] = {}
        rows = self._conn.execute(
            _SQL_FLOW_EDGES, list(_ALL_PROPAGATION_EDGE_KINDS),
        ) or []
        for r in rows:
            src = str(r.get("src_object_kind") or "").strip()
            dst = str(r.get("dst_object_kind") or "").strip()
            ek = str(r.get("edge_kind") or "").strip()
            if not src or not dst:
                continue
            if ek in _FORWARD_EDGE_KINDS:
                forward.setdefault(src, []).append((dst, ek))
            elif ek in _REVERSE_EDGE_KINDS:
                reverse.setdefault(dst, []).append((src, ek))
        return forward, reverse

    def _tagged_roots(self) -> list[tuple[str, str]]:
        rows = self._conn.execute(_SQL_TAGGED_ROOTS, list(_PROPAGATED_TAG_KEYS)) or []
        return [
            (str(r.get("object_kind") or "").strip(),
             str(r.get("tag_key") or "").strip())
            for r in rows
            if r.get("object_kind") and r.get("tag_key")
        ]

    def _walk(
        self,
        adj: dict[str, list[tuple[str, str]]],
        root: str,
        tag_key: str,
        *,
        tag_value: str,
    ) -> Iterable[dict[str, Any]]:
        """BFS from `root` along `adj`, yielding one entry per visited node.

        `tag_value` distinguishes forward-propagated ("inherited") from
        reverse-propagated ("linkable") taint so operators can read the
        emission and understand what kind of PII exposure it encodes.
        """
        visited: set[str] = {root}
        frontier: list[tuple[str, int, str]] = [(root, 0, "")]
        while frontier:
            current, depth, _via = frontier.pop(0)
            if depth >= _MAX_DEPTH:
                continue
            for dst, ek in adj.get(current, []):
                if dst in visited:
                    continue
                visited.add(dst)
                yield {
                    "object_kind": dst,
                    "field_path": "",
                    "tag_key": tag_key,
                    "tag_value": tag_value,
                    "confidence": max(0.4, 0.9 ** (depth + 1)),  # decays with depth
                    "origin_ref": {
                        "projector": _PROJECTOR_TAG,
                        "source_object_kind": root,
                        "via_edge": ek,
                        "direction": "forward" if tag_value == "inherited" else "reverse",
                        "distance": depth + 1,
                    },
                    "metadata": {"parent": current},
                }
                frontier.append((dst, depth + 1, ek))

    # -- entry point ------------------------------------------------------

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        try:
            forward, reverse = self._load_adjacency()
            roots = self._tagged_roots()
            if not roots:
                apply_projected_classifications(
                    self._conn,
                    projector_tag=_PROJECTOR_TAG,
                    entries=[],
                    source="auto",
                )
                return _ok(self.name, t0)

            seen: dict[tuple[str, str], dict[str, Any]] = {}

            def _merge(entry: dict[str, Any]) -> None:
                key = (entry["object_kind"], entry["tag_key"])
                prev = seen.get(key)
                if prev is None or entry["confidence"] > prev["confidence"]:
                    seen[key] = entry

            for obj, tag_key in roots:
                for entry in self._walk(forward, obj, tag_key, tag_value="inherited"):
                    _merge(entry)
                for entry in self._walk(reverse, obj, tag_key, tag_value="linkable"):
                    _merge(entry)

            apply_projected_classifications(
                self._conn,
                projector_tag=_PROJECTOR_TAG,
                entries=list(seen.values()),
                source="auto",
            )
        except Exception as exc:  # noqa: BLE001
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


__all__ = [
    "DataDictionaryClassificationsPropagationProjector",
]
