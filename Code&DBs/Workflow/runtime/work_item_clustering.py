"""Derived work-item clusters for machine-facing operator read surfaces."""

from __future__ import annotations

import re
from collections import Counter, OrderedDict
from collections.abc import Mapping, Sequence
from typing import Any


_TOKEN_PATTERN = re.compile(r"[a-z0-9][a-z0-9._/-]*")
_TITLE_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "bug",
        "bugs",
        "broken",
        "does",
        "error",
        "fails",
        "failure",
        "for",
        "in",
        "is",
        "not",
        "of",
        "on",
        "or",
        "regression",
        "the",
        "to",
        "when",
        "with",
    }
)
_BUG_TAG_PRIORITY = (
    "cluster",
    "failure_code",
    "node_id",
    "job_label",
    "provider_slug",
    "model_slug",
    "surface",
    "component",
    "subsystem",
    "path",
    "module",
)


def _text(value: object) -> str:
    return str(value or "").strip()


def _lower_text(value: object) -> str:
    return _text(value).lower()


def _sequence(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(part.strip() for part in value.split(",") if part.strip())
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return tuple(str(part).strip() for part in value if str(part).strip())
    return ()


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _counter_payload(values: Sequence[str]) -> dict[str, int]:
    return dict(Counter(value for value in values if value))


def _top_values(values: Sequence[str], *, limit: int = 5) -> list[str]:
    return [value for value, _count in Counter(value for value in values if value).most_common(limit)]


def _title_anchor(title: object) -> str | None:
    tokens = [
        token
        for token in _TOKEN_PATTERN.findall(_lower_text(title))
        if len(token) > 2 and token not in _TITLE_STOPWORDS
    ]
    if len(tokens) < 2:
        return None
    return ".".join(tokens[:3])


def _tag_values(tags: Sequence[str]) -> dict[str, list[str]]:
    values: dict[str, list[str]] = {}
    for raw_tag in tags:
        key, sep, value = raw_tag.partition(":")
        if not sep:
            continue
        normalized_key = key.strip().lower()
        normalized_value = value.strip()
        if normalized_key and normalized_value:
            values.setdefault(normalized_key, []).append(normalized_value)
    return values


def _dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))


def _cluster_signal(
    *,
    key: str,
    label: str,
    reason_code: str,
    priority: int,
) -> dict[str, Any]:
    return {
        "cluster_key": key,
        "label": label,
        "reason_code": reason_code,
        "priority": priority,
    }


def _bug_signals(bug: Mapping[str, Any]) -> list[dict[str, Any]]:
    signals: list[dict[str, Any]] = []
    resume_context = _mapping(bug.get("resume_context"))
    for field in ("cluster_key", "cluster_ref", "cluster"):
        value = _text(resume_context.get(field))
        if value:
            signals.append(
                _cluster_signal(
                    key=f"bug.resume_context.{field}:{value}",
                    label=value,
                    reason_code=f"bug_cluster.resume_context.{field}",
                    priority=0,
                )
            )
            break

    tags = _sequence(bug.get("tags"))
    tag_values = _tag_values(tags)
    for tag_key in _BUG_TAG_PRIORITY:
        values = tag_values.get(tag_key) or []
        for value in _dedupe(values):
            signals.append(
                _cluster_signal(
                    key=f"bug.tag.{tag_key}:{value}",
                    label=f"{tag_key}: {value}",
                    reason_code=f"bug_cluster.tag.{tag_key}",
                    priority=1,
                )
            )
    for tag in _dedupe(tags):
        signals.append(
            _cluster_signal(
                key=f"bug.tag.exact:{tag.lower()}",
                label=f"tag: {tag}",
                reason_code="bug_cluster.tag.exact",
                priority=4,
            )
        )

    source_issue_id = _text(bug.get("source_issue_id"))
    if source_issue_id:
        signals.append(
            _cluster_signal(
                key=f"bug.source_issue:{source_issue_id}",
                label=f"source issue: {source_issue_id}",
                reason_code="bug_cluster.source_issue",
                priority=2,
            )
        )

    owner_ref = _text(bug.get("owner_ref"))
    if owner_ref:
        signals.append(
            _cluster_signal(
                key=f"bug.owner:{owner_ref}",
                label=f"owner: {owner_ref}",
                reason_code="bug_cluster.owner_ref",
                priority=5,
            )
        )

    title_anchor = _title_anchor(bug.get("title"))
    if title_anchor:
        signals.append(
            _cluster_signal(
                key=f"bug.title_anchor:{title_anchor}",
                label=f"title: {title_anchor}",
                reason_code="bug_cluster.title_anchor",
                priority=9,
            )
        )
    return signals


def _choose_signal(
    signals: Sequence[dict[str, Any]],
    counts: Counter[str],
    *,
    singleton_key: str,
    singleton_label: str,
    singleton_reason_code: str,
) -> dict[str, Any]:
    for signal in signals:
        if counts[str(signal["cluster_key"])] > 1 or int(signal["priority"]) <= 1:
            return signal
    if signals:
        return dict(signals[0])
    return _cluster_signal(
        key=singleton_key,
        label=singleton_label,
        reason_code=singleton_reason_code,
        priority=99,
    )


def cluster_bug_items(
    bugs: Sequence[Mapping[str, Any]],
    *,
    include_singletons: bool = True,
) -> dict[str, Any]:
    """Cluster serialized bug rows into durable, inspectable work groups."""

    bug_rows = [dict(row) for row in bugs]
    signal_rows = [_bug_signals(row) for row in bug_rows]
    counts: Counter[str] = Counter(
        str(signal["cluster_key"])
        for signals in signal_rows
        for signal in signals
    )

    groups: OrderedDict[str, dict[str, Any]] = OrderedDict()
    for bug, signals in zip(bug_rows, signal_rows):
        bug_id = _text(bug.get("bug_id")) or _text(bug.get("id"))
        signal = _choose_signal(
            signals,
            counts,
            singleton_key=f"bug.singleton:{bug_id or _text(bug.get('title'))}",
            singleton_label=_text(bug.get("title")) or bug_id or "unlabeled bug",
            singleton_reason_code="bug_cluster.singleton",
        )
        cluster_key = str(signal["cluster_key"])
        group = groups.setdefault(
            cluster_key,
            {
                "cluster_key": cluster_key,
                "label": str(signal["label"]),
                "reason_code": str(signal["reason_code"]),
                "bug_ids": [],
                "titles": [],
                "statuses": [],
                "severities": [],
                "categories": [],
                "source_issue_ids": [],
                "tags": [],
                "next_steps": [],
            },
        )
        if bug_id:
            group["bug_ids"].append(bug_id)
        title = _text(bug.get("title"))
        if title:
            group["titles"].append(title)
        group["statuses"].append(_text(bug.get("status")))
        group["severities"].append(_text(bug.get("severity")))
        group["categories"].append(_text(bug.get("category")))
        group["source_issue_ids"].append(_text(bug.get("source_issue_id")))
        group["tags"].extend(_sequence(bug.get("tags")))
        resume_context = _mapping(bug.get("resume_context"))
        group["next_steps"].extend(_sequence(resume_context.get("next_steps")))

    clusters: list[dict[str, Any]] = []
    for index, group in enumerate(groups.values()):
        count = len(group["bug_ids"])
        if count == 1 and not include_singletons:
            continue
        clusters.append(
            {
                "cluster_key": group["cluster_key"],
                "label": group["label"],
                "reason_code": group["reason_code"],
                "count": count,
                "bug_ids": _dedupe(group["bug_ids"]),
                "titles": _dedupe(group["titles"])[:5],
                "status_counts": _counter_payload(group["statuses"]),
                "severity_counts": _counter_payload(group["severities"]),
                "category_counts": _counter_payload(group["categories"]),
                "source_issue_ids": _dedupe(group["source_issue_ids"])[:5],
                "common_tags": _top_values(group["tags"]),
                "next_steps": _dedupe(group["next_steps"])[:5],
                "_order": index,
            }
        )
    clusters.sort(
        key=lambda cluster: (
            int(cluster["count"]),
            str(cluster["reason_code"]) != "bug_cluster.singleton",
            -int(cluster["_order"]),
        ),
        reverse=True,
    )
    for cluster in clusters:
        cluster.pop("_order", None)

    multi_item_count = sum(int(cluster["count"]) for cluster in clusters if int(cluster["count"]) > 1)
    singleton_count = max(0, len(bug_rows) - multi_item_count)
    return {
        "item_kind": "bug",
        "authority": "runtime.work_item_clustering.cluster_bug_items",
        "cluster_count": len(clusters),
        "clustered_item_count": multi_item_count,
        "singleton_count": singleton_count,
        "clusters": clusters,
        "grouping_order": [
            "resume_context.cluster_key",
            "tag:cluster",
            "source_issue_id",
            "failure/provider/runtime tags",
            "shared exact tags",
            "owner_ref",
            "title_anchor",
        ],
    }


def _roadmap_parent_label(
    parent_id: str,
    item_by_id: Mapping[str, Mapping[str, Any]],
) -> str:
    parent = item_by_id.get(parent_id)
    if parent is None:
        return f"parent: {parent_id}"
    return _text(parent.get("title")) or parent_id


def _roadmap_cluster_payload(
    *,
    cluster_key: str,
    label: str,
    reason_code: str,
    item_ids: Sequence[str],
    item_by_id: Mapping[str, Mapping[str, Any]],
    dependency_by_item: Mapping[str, Sequence[str]],
    order: int,
) -> dict[str, Any] | None:
    member_ids = _dedupe(item_ids)
    if not member_ids:
        return None
    rows = [item_by_id[item_id] for item_id in member_ids if item_id in item_by_id]
    return {
        "cluster_key": cluster_key,
        "label": label,
        "reason_code": reason_code,
        "count": len(member_ids),
        "roadmap_item_ids": member_ids,
        "titles": _dedupe([_text(row.get("title")) for row in rows])[:5],
        "status_counts": _counter_payload([_text(row.get("status")) for row in rows]),
        "lifecycle_counts": _counter_payload([_text(row.get("lifecycle")) for row in rows]),
        "priority_counts": _counter_payload([_text(row.get("priority")) for row in rows]),
        "source_bug_ids": _dedupe([_text(row.get("source_bug_id")) for row in rows])[:5],
        "registry_paths": _dedupe(
            [
                path
                for row in rows
                for path in _sequence(row.get("registry_paths"))
            ]
        )[:5],
        "dependency_roadmap_item_ids": _dedupe(
            [
                dependency_id
                for item_id in member_ids
                for dependency_id in dependency_by_item.get(item_id, ())
            ]
        )[:8],
        "_order": order,
    }


def cluster_roadmap_items(
    roadmap_items: Sequence[Mapping[str, Any]],
    *,
    dependencies: Sequence[Mapping[str, Any]] = (),
    include_singletons: bool = False,
) -> dict[str, Any]:
    """Cluster serialized roadmap rows by tree, bug lineage, and registry signals."""

    item_by_id: OrderedDict[str, Mapping[str, Any]] = OrderedDict()
    for row in roadmap_items:
        item_id = _text(row.get("roadmap_item_id"))
        if item_id:
            item_by_id[item_id] = row

    dependency_by_item: dict[str, list[str]] = {}
    for dependency in dependencies:
        item_id = _text(dependency.get("roadmap_item_id"))
        depends_on = _text(dependency.get("depends_on_roadmap_item_id"))
        if item_id and depends_on:
            dependency_by_item.setdefault(item_id, []).append(depends_on)

    clusters: list[dict[str, Any]] = []
    order = 0

    children_by_parent: OrderedDict[str, list[str]] = OrderedDict()
    for item_id, row in item_by_id.items():
        parent_id = _text(row.get("parent_roadmap_item_id"))
        if parent_id:
            children_by_parent.setdefault(parent_id, []).append(item_id)
    for parent_id, child_ids in children_by_parent.items():
        member_ids = ([parent_id] if parent_id in item_by_id else []) + child_ids
        if len(member_ids) < 2 and not include_singletons:
            continue
        payload = _roadmap_cluster_payload(
            cluster_key=f"roadmap.parent:{parent_id}",
            label=_roadmap_parent_label(parent_id, item_by_id),
            reason_code="roadmap_cluster.parent_child_wave",
            item_ids=member_ids,
            item_by_id=item_by_id,
            dependency_by_item=dependency_by_item,
            order=order,
        )
        if payload is not None:
            clusters.append(payload)
            order += 1

    source_bug_groups: OrderedDict[str, list[str]] = OrderedDict()
    registry_groups: OrderedDict[str, list[str]] = OrderedDict()
    decision_groups: OrderedDict[str, list[str]] = OrderedDict()
    for item_id, row in item_by_id.items():
        source_bug_id = _text(row.get("source_bug_id"))
        if source_bug_id:
            source_bug_groups.setdefault(source_bug_id, []).append(item_id)
        for path in _sequence(row.get("registry_paths")):
            registry_groups.setdefault(path, []).append(item_id)
        decision_ref = _text(row.get("decision_ref"))
        if decision_ref:
            decision_groups.setdefault(decision_ref, []).append(item_id)

    for reason_code, prefix, groups in (
        ("roadmap_cluster.source_bug", "roadmap.source_bug", source_bug_groups),
        ("roadmap_cluster.registry_path", "roadmap.registry_path", registry_groups),
        ("roadmap_cluster.decision_ref", "roadmap.decision", decision_groups),
    ):
        for group_ref, item_ids in groups.items():
            if len(item_ids) < 2 and not include_singletons:
                continue
            payload = _roadmap_cluster_payload(
                cluster_key=f"{prefix}:{group_ref}",
                label=group_ref,
                reason_code=reason_code,
                item_ids=item_ids,
                item_by_id=item_by_id,
                dependency_by_item=dependency_by_item,
                order=order,
            )
            if payload is not None:
                clusters.append(payload)
                order += 1

    if include_singletons:
        clustered_ids = {
            item_id
            for cluster in clusters
            for item_id in cluster["roadmap_item_ids"]
        }
        for item_id, row in item_by_id.items():
            if item_id in clustered_ids:
                continue
            payload = _roadmap_cluster_payload(
                cluster_key=f"roadmap.singleton:{item_id}",
                label=_text(row.get("title")) or item_id,
                reason_code="roadmap_cluster.singleton",
                item_ids=(item_id,),
                item_by_id=item_by_id,
                dependency_by_item=dependency_by_item,
                order=order,
            )
            if payload is not None:
                clusters.append(payload)
                order += 1

    clusters.sort(
        key=lambda cluster: (
            int(cluster["count"]),
            str(cluster["reason_code"]) != "roadmap_cluster.singleton",
            -int(cluster["_order"]),
        ),
        reverse=True,
    )
    for cluster in clusters:
        cluster.pop("_order", None)

    clustered_ids = {
        item_id
        for cluster in clusters
        if int(cluster["count"]) > 1
        for item_id in cluster["roadmap_item_ids"]
    }
    return {
        "item_kind": "roadmap_item",
        "authority": "runtime.work_item_clustering.cluster_roadmap_items",
        "cluster_count": len(clusters),
        "clustered_item_count": len(clustered_ids),
        "singleton_count": max(0, len(item_by_id) - len(clustered_ids)),
        "clusters": clusters,
        "grouping_order": [
            "parent_roadmap_item_id",
            "source_bug_id",
            "registry_paths",
            "decision_ref",
        ],
        "membership_policy": "overlapping_clusters_allowed",
    }


__all__ = [
    "cluster_bug_items",
    "cluster_roadmap_items",
]
