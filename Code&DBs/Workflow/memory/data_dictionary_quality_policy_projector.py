"""Promote active operator policies into the quality-rules axis.

Every active `architecture_policy` row in `operator_decisions` is an
operator-authored claim. This projector maps each one into an
`inferred`-source quality rule on the affected data-dictionary
target(s), so compliance can be monitored by the same pipeline that
already evaluates rule runs.

Mapping strategy (simple + deterministic):

* Decision keys of the form `architecture-policy::<subsystem>::<slug>`
  are interpreted as policies governing `<subsystem>_*` tables.
  Examples: `data-dictionary`, `workflow`, `bug`, `operator`.
* For every table whose namespace matches, emit a quality rule with:
     rule_kind = `policy_compliance`
     severity  = `warn`   (policy rules are informational by default;
                            operators can upgrade to `error`)
     origin_ref.decision_id / .decision_key preserved so the rule can
     be traced back to the authoritative decision.

The projector only writes at `source=inferred`. Operator-authored rules
(`source=operator`) always win. Projector-owned auto rules
(`source=auto`) remain untouched.
"""
from __future__ import annotations

import logging
import re
import time
from typing import Any

from runtime.data_dictionary_quality import apply_projected_rules
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _fail, _ok

logger = logging.getLogger(__name__)


_PROJECTOR_TAG = "quality_policy_decisions"

# Map decision-key subsystem segment → table namespace prefix.
# Keep this synchronized with the stewardship projector's namespace map.
_SUBSYSTEM_TABLE_PREFIX = {
    "data-dictionary": "data_dictionary_",
    "workflow":        "workflow_",
    "operator":        "operator_",
    "bug":             "bug",        # matches `bug` and `bug_*`
    "bugs":            "bug",
    "capability":      "capability_",
    "cutover":         "cutover_",
    "heartbeat":       "heartbeat_",
    "receipt":         "receipt",
    "adapter":         "adapter_",
    "connector":       "connector_",
    "context":         "context_",
    "conversation":    "conversation",
    "friction":        "friction",
    "agent":           "agent_",
    "constraint":      "constraint",
}

_DECISION_KEY_RE = re.compile(
    r"^architecture-policy::(?P<subsystem>[a-z0-9-]+)::(?P<slug>[a-z0-9-]+)$"
)


_SQL_ACTIVE_POLICIES = """
SELECT operator_decision_id, decision_key, title, rationale
FROM operator_decisions
WHERE decision_kind = 'architecture_policy'
  AND decision_status = 'decided'
  AND (effective_to IS NULL OR effective_to > now())
"""

_SQL_TABLES_IN_NAMESPACE = """
SELECT object_kind
FROM data_dictionary_objects
WHERE object_kind LIKE $1
"""


class DataDictionaryQualityPolicyProjector(HeartbeatModule):
    """Inferred-layer quality rules derived from active operator policies."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    @property
    def name(self) -> str:
        return "data_dictionary_quality_policy_projector"

    def _table_objects_for(self, subsystem: str) -> list[str]:
        prefix = _SUBSYSTEM_TABLE_PREFIX.get(subsystem)
        if not prefix:
            return []
        like = f"table:{prefix}%"
        rows = self._conn.execute(_SQL_TABLES_IN_NAMESPACE, like) or []
        return [str(r.get("object_kind") or "").strip() for r in rows if r.get("object_kind")]

    def run(self) -> HeartbeatModuleResult:
        """One `policy_compliance` row per object, aggregating every policy
        that governs that object into a single rule's expression."""
        t0 = time.monotonic()
        try:
            policies = self._conn.execute(_SQL_ACTIVE_POLICIES) or []

            # Build: object_kind → list of policy entries
            per_object: dict[str, list[dict[str, Any]]] = {}
            for row in policies:
                key = str(row.get("decision_key") or "").strip()
                decision_id = str(row.get("operator_decision_id") or "").strip()
                rationale = str(row.get("rationale") or "").strip()
                title = str(row.get("title") or "").strip()
                if not key or not decision_id:
                    continue
                m = _DECISION_KEY_RE.match(key)
                if not m:
                    continue
                subsystem = m.group("subsystem")
                slug = m.group("slug")
                for object_kind in self._table_objects_for(subsystem):
                    per_object.setdefault(object_kind, []).append({
                        "decision_id": decision_id,
                        "decision_key": key,
                        "slug": slug,
                        "title": title or key,
                        "rationale": rationale,
                    })

            entries: list[dict[str, Any]] = []
            for object_kind, items in per_object.items():
                entries.append({
                    "object_kind": object_kind,
                    "field_path": "",
                    "rule_kind": "policy_compliance",
                    "severity": "warning",
                    "description": (
                        f"Governed by {len(items)} active architecture "
                        f"policy decision(s); see expression.policies."
                    ),
                    "expression": {
                        "check": "operator_decision_active",
                        "policies": [
                            {"decision_key": i["decision_key"], "slug": i["slug"]}
                            for i in items
                        ],
                    },
                    "enabled": True,
                    "confidence": 0.80,
                    "origin_ref": {
                        "projector": _PROJECTOR_TAG,
                        "policy_count": len(items),
                        "decision_keys": [i["decision_key"] for i in items],
                    },
                    "metadata": {"slugs": [i["slug"] for i in items]},
                })

            apply_projected_rules(
                self._conn,
                projector_tag=_PROJECTOR_TAG,
                rules=entries,
                source="inferred",
            )
        except Exception as exc:  # noqa: BLE001
            return _fail(self.name, t0, str(exc))
        return _ok(self.name, t0)


__all__ = ["DataDictionaryQualityPolicyProjector"]
