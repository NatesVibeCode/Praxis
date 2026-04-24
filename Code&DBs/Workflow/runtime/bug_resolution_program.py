"""Deterministic packaging for the bug-resolution workflow program."""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
import json
import re
from pathlib import Path
from typing import Any


_SLUG_PATTERN = re.compile(r"[^a-z0-9]+")
_TOKEN_PATTERN = re.compile(r"[a-z0-9][a-z0-9._/-]*")

PACKET_REQUIRED_FIELDS: tuple[str, ...] = (
    "bug_ids",
    "authority_owner",
    "cluster",
    "verification_surface",
    "done_criteria",
    "stop_boundary",
    "depends_on_wave",
)

LANE_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "lane_id": "authority_bug_system",
        "label": "Authority / bug system",
        "keywords": (
            "authority",
            "bug tracker",
            "bug surface",
            "underlinked",
            "evidence",
            "replay",
            "operator",
            "semantic assertion",
            "semantic_current_assertions",
            "duplicate semantic assertion",
            "discover can hang",
            "search can hang",
        ),
        "owner_keywords": ("authority", "bugs", "operator", "evidence", "replay"),
        "verification_surface": (
            "workflow orient + bug stats/list/search + replay-ready view must all return "
            "cleanly for the affected path"
        ),
        "done_criteria": (
            "The affected authority path fails closed or succeeds deterministically.",
            "Bug/operator reads prove the repaired behavior without hangs or silent fallback.",
            "Each in-scope bug reaches a proof-backed terminal outcome.",
        ),
        "stop_boundary": (
            "Do not widen into unrelated product work once the bug lane is truthful and auditable."
        ),
    },
    {
        "lane_id": "workflow_runtime",
        "label": "Workflow / runtime",
        "keywords": (
            "workflow",
            "runtime",
            "worker",
            "provider",
            "concurrency",
            "lease",
            "receipt",
            "retry",
            "routing",
            "runner",
            "verifier",
            "claim",
        ),
        "owner_keywords": ("workflow", "runtime", "worker", "routing", "provider"),
        "verification_surface": (
            "Focused runtime or workflow tests plus explicit bug evidence attachment and "
            "terminal resolution"
        ),
        "done_criteria": (
            "The runtime failure mode is fixed or closed with explicit proof.",
            "Verification covers the touched runtime path without broad regression drift.",
            "FIXED bugs attach validates_fix evidence before resolution.",
        ),
        "stop_boundary": (
            "Do not refactor adjacent runtime domains unless they are required to close the packet."
        ),
    },
    {
        "lane_id": "setup_deploy",
        "label": "Setup / deploy",
        "keywords": (
            "setup",
            "bootstrap",
            "compose",
            "docker",
            "restart api",
            "migration",
            "schema",
            "rebuild",
            "launchd",
            "install",
            ".env",
        ),
        "owner_keywords": ("setup", "bootstrap", "deploy", "compose", "migration"),
        "verification_surface": (
            "Setup/bootstrap probes, restart path checks, and migration or rebuild proof for the "
            "affected install lane"
        ),
        "done_criteria": (
            "The setup or deploy path is explicit, reproducible, and no longer blocked by the bug.",
            "Verification proves the supported install or restart path, not an ad hoc workaround.",
            "Any terminal closure explains why a code fix is intentionally deferred.",
        ),
        "stop_boundary": (
            "Do not broaden into unrelated environment support or arbitrary database engines."
        ),
    },
    {
        "lane_id": "app_wiring_frontend",
        "label": "App wiring / frontend",
        "keywords": (
            "manifest",
            "service worker",
            "/sw.js",
            "pwa",
            "app shell",
            "spa html",
            "frontend",
            "ui",
            "browser",
            "thumbnail",
            "asset",
            "route",
        ),
        "owner_keywords": ("frontend", "app", "ui", "browser", "pwa"),
        "verification_surface": (
            "Frontend route or asset proof plus app smoke verification for the affected surface"
        ),
        "done_criteria": (
            "The affected app route, manifest, or asset wiring behaves correctly end to end.",
            "The fix is verified at the UI or route boundary, not only by internal code inspection.",
            "If the right outcome is defer or won't-fix, the user-visible impact is recorded clearly.",
        ),
        "stop_boundary": (
            "Do not redesign the app shell or styling outside the exact broken wiring path."
        ),
    },
    {
        "lane_id": "data_projector",
        "label": "Data / projector",
        "keywords": (
            "projector",
            "projection",
            "data dictionary",
            "memory graph",
            "semantic",
            "recall",
            "embeddings",
            "catalog",
            "lineage",
            "contact authority row",
        ),
        "owner_keywords": ("projection", "projector", "semantic", "memory", "catalog"),
        "verification_surface": (
            "Projection or data-plane integrity checks plus focused query or repository proof"
        ),
        "done_criteria": (
            "The affected projection or data contract becomes explicit and queryable again.",
            "Verification proves the projection or query shape from the owning surface.",
            "Any closure without code makes the data-model trade-off explicit.",
        ),
        "stop_boundary": (
            "Do not widen into unrelated knowledge-graph, catalog, or memory refactors."
        ),
    },
)

_LANE_BY_ID = {item["lane_id"]: item for item in LANE_DEFINITIONS}
_LANE_ORDER = {item["lane_id"]: index for index, item in enumerate(LANE_DEFINITIONS)}

WAVE_DEFINITIONS: tuple[dict[str, Any], ...] = (
    {
        "wave_id": "wave_0_authority_repair",
        "label": "Wave 0: Authority repair",
        "depends_on": (),
        "description": (
            "Repair the bug and operator authority path first so the backlog can be read and "
            "closed truthfully."
        ),
    },
    {
        "wave_id": "wave_1_evidence_normalization",
        "label": "Wave 1: Evidence normalization",
        "depends_on": ("wave_0_authority_repair",),
        "description": (
            "Recover explicit proof for replay-blocked or underlinked bugs before broad fixing."
        ),
    },
    {
        "wave_id": "wave_2_execute",
        "label": "Wave 2: Execute bounded fixes",
        "depends_on": ("wave_1_evidence_normalization",),
        "description": (
            "Run bounded fix or closure packets in parallel once authority and evidence are ready."
        ),
    },
    {
        "wave_id": "wave_3_verify_closeout",
        "label": "Wave 3: Verify and close out",
        "depends_on": ("wave_2_execute",),
        "description": (
            "Re-run backlog proof and ensure every kickoff bug is in a terminal, evidence-backed state."
        ),
    },
)

_WAVE_BY_ID = {item["wave_id"]: item for item in WAVE_DEFINITIONS}
_SEVERITY_ORDER = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}


def utc_now_iso() -> str:
    """Return a stable UTC timestamp string."""
    return datetime.now(timezone.utc).isoformat()


def slugify(value: object, *, default: str = "item") -> str:
    text = _SLUG_PATTERN.sub("-", str(value or "").strip().lower()).strip("-")
    return text or default


def lane_catalog() -> list[dict[str, Any]]:
    return [
        {
            "lane_id": item["lane_id"],
            "label": item["label"],
            "verification_surface": item["verification_surface"],
            "stop_boundary": item["stop_boundary"],
        }
        for item in LANE_DEFINITIONS
    ]


def wave_catalog() -> list[dict[str, Any]]:
    return [
        {
            "wave_id": item["wave_id"],
            "label": item["label"],
            "depends_on": list(item["depends_on"]),
            "description": item["description"],
        }
        for item in WAVE_DEFINITIONS
    ]


def _text(value: object) -> str:
    return str(value or "").strip()


def _lower_text(value: object) -> str:
    return _text(value).lower()


def _sequence(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(part.strip() for part in value.split(",") if part.strip())
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return tuple(str(part).strip() for part in value if str(part).strip())
    return ()


def _bug_haystack(bug: Mapping[str, Any]) -> str:
    parts: list[str] = [
        _text(bug.get("title")),
        _text(bug.get("summary")),
        _text(bug.get("description")),
        _text(bug.get("owner_ref")),
        _text(bug.get("source_issue_id")),
        _text(bug.get("replay_reason_code")),
    ]
    parts.extend(_sequence(bug.get("tags")))
    return " ".join(part for part in parts if part).lower()


def _lane_score(lane_id: str, bug: Mapping[str, Any]) -> int:
    lane = _LANE_BY_ID[lane_id]
    haystack = _bug_haystack(bug)
    owner_ref = _lower_text(bug.get("owner_ref"))
    score = 0
    for keyword in lane["keywords"]:
        if keyword in haystack:
            score += 1
    for keyword in lane["owner_keywords"]:
        if keyword in owner_ref:
            score += 3
    category = _text(bug.get("category")).upper()
    if lane_id == "app_wiring_frontend" and category == "WIRING":
        score += 1
    if lane_id == "workflow_runtime" and category in {"RUNTIME", "TEST"}:
        score += 1
    if lane_id == "authority_bug_system" and category in {"ARCHITECTURE", "VERIFY"}:
        score += 1
    return score


def classify_bug_lane(bug: Mapping[str, Any]) -> str:
    """Assign one deterministic execution lane to a serialized bug row."""
    best_lane = "workflow_runtime"
    best_score = -1
    for lane in _LANE_BY_ID:
        score = _lane_score(lane, bug)
        if score > best_score:
            best_lane = lane
            best_score = score
    if best_score > 0:
        return best_lane
    category = _text(bug.get("category")).upper()
    if category == "WIRING":
        return "app_wiring_frontend" if "manifest" in _bug_haystack(bug) else "workflow_runtime"
    if category in {"ARCHITECTURE", "VERIFY"}:
        return "authority_bug_system"
    return "workflow_runtime"


def _wave_for_packet(*, lane_id: str, replay_ready_count: int, bug_count: int) -> str:
    if lane_id == "authority_bug_system":
        return "wave_0_authority_repair"
    if replay_ready_count >= bug_count and bug_count > 0:
        return "wave_2_execute"
    return "wave_1_evidence_normalization"


def _packet_kind(wave_id: str) -> str:
    if wave_id == "wave_0_authority_repair":
        return "authority_repair"
    if wave_id == "wave_1_evidence_normalization":
        return "evidence_recovery"
    if wave_id == "wave_2_execute":
        return "build_or_close"
    return "verification_closeout"


def _highest_severity(bugs: Sequence[Mapping[str, Any]]) -> str:
    values = [_text(bug.get("severity")).upper() for bug in bugs]
    values = [value for value in values if value in _SEVERITY_ORDER]
    if not values:
        return "P2"
    return sorted(values, key=lambda value: _SEVERITY_ORDER[value])[0]


def _authority_owner(bugs: Sequence[Mapping[str, Any]], *, lane_id: str) -> str:
    owners = [_text(bug.get("owner_ref")) for bug in bugs if _text(bug.get("owner_ref"))]
    if owners:
        return Counter(owners).most_common(1)[0][0]
    return f"lane:{lane_id}"


def _cluster_entry_for_bug(bug: Mapping[str, Any]) -> dict[str, Any]:
    bug_id = _text(bug.get("bug_id"))
    title = _text(bug.get("title")) or bug_id or "unlabeled bug"
    return {
        "cluster_key": f"bug.singleton:{bug_id or slugify(title)}",
        "label": title,
        "reason_code": "bug_cluster.singleton",
        "count": 1,
        "bug_ids": [bug_id] if bug_id else [],
        "titles": [title],
    }


def _unique_clusters(
    bugs: Sequence[Mapping[str, Any]],
    clusters: Sequence[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    bug_by_id = {
        _text(bug.get("bug_id")): dict(bug)
        for bug in bugs
        if _text(bug.get("bug_id"))
    }
    unique: list[dict[str, Any]] = []
    seen_bug_ids: set[str] = set()
    for cluster in clusters or ():
        bug_ids = [
            bug_id for bug_id in (_text(item) for item in _sequence(cluster.get("bug_ids")))
            if bug_id in bug_by_id
        ]
        if not bug_ids:
            continue
        unique.append(
            {
                "cluster_key": _text(cluster.get("cluster_key")) or f"cluster:{len(unique) + 1}",
                "label": _text(cluster.get("label")) or bug_ids[0],
                "reason_code": _text(cluster.get("reason_code")) or "bug_cluster.unknown",
                "count": len(bug_ids),
                "bug_ids": bug_ids,
                "titles": [bug_by_id[bug_id].get("title") for bug_id in bug_ids],
            }
        )
        seen_bug_ids.update(bug_ids)
    for bug_id, bug in bug_by_id.items():
        if bug_id in seen_bug_ids:
            continue
        unique.append(_cluster_entry_for_bug(bug))
    return unique


def derive_bug_packets(
    *,
    program_id: str,
    bugs: Sequence[Mapping[str, Any]],
    clusters: Sequence[Mapping[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    bug_by_id = {
        _text(bug.get("bug_id")): dict(bug)
        for bug in bugs
        if _text(bug.get("bug_id"))
    }
    packets: list[dict[str, Any]] = []
    for cluster in _unique_clusters(bugs, clusters):
        bug_ids = [bug_id for bug_id in cluster["bug_ids"] if bug_id in bug_by_id]
        if not bug_ids:
            continue
        members = [bug_by_id[bug_id] for bug_id in bug_ids]
        lane_counts = Counter(classify_bug_lane(bug) for bug in members)
        lane_id = sorted(
            lane_counts,
            key=lambda item: (-lane_counts[item], _LANE_ORDER[item]),
        )[0]
        replay_ready_count = sum(1 for bug in members if bool(bug.get("replay_ready")))
        wave_id = _wave_for_packet(
            lane_id=lane_id,
            replay_ready_count=replay_ready_count,
            bug_count=len(members),
        )
        lane = _LANE_BY_ID[lane_id]
        cluster_label = _text(cluster.get("label")) or _text(members[0].get("title")) or bug_ids[0]
        packet_slug = slugify(f"{wave_id}-{lane_id}-{cluster_label}")[:96]
        packets.append(
            {
                "packet_id": f"{program_id}.{packet_slug}",
                "packet_slug": packet_slug,
                "packet_kind": _packet_kind(wave_id),
                "wave_id": wave_id,
                "depends_on_wave": list(_WAVE_BY_ID[wave_id]["depends_on"]),
                "lane_id": lane_id,
                "lane_label": lane["label"],
                "bug_ids": bug_ids,
                "bug_titles": [_text(bug.get("title")) for bug in members if _text(bug.get("title"))],
                "highest_severity": _highest_severity(members),
                "authority_owner": _authority_owner(members, lane_id=lane_id),
                "cluster": {
                    "cluster_key": _text(cluster.get("cluster_key")) or packet_slug,
                    "label": cluster_label,
                    "reason_code": _text(cluster.get("reason_code")) or "bug_cluster.unknown",
                },
                "verification_surface": lane["verification_surface"],
                "done_criteria": list(lane["done_criteria"]),
                "stop_boundary": lane["stop_boundary"],
                "replay_ready_count": replay_ready_count,
                "replay_blocked_bug_ids": [
                    _text(bug.get("bug_id"))
                    for bug in members
                    if not bool(bug.get("replay_ready"))
                ],
                "blocked_reason_codes": sorted(
                    {
                        _text(bug.get("replay_reason_code")) or "bug.replay_not_ready"
                        for bug in members
                        if not bool(bug.get("replay_ready"))
                    }
                ),
                "categories": sorted(
                    {
                        _text(bug.get("category")).upper()
                        for bug in members
                        if _text(bug.get("category"))
                    }
                ),
            }
        )
    packets.sort(
        key=lambda packet: (
            next(
                index
                for index, wave in enumerate(WAVE_DEFINITIONS)
                if wave["wave_id"] == packet["wave_id"]
            ),
            _LANE_ORDER[packet["lane_id"]],
            _SEVERITY_ORDER.get(packet["highest_severity"], 99),
            packet["packet_slug"],
        )
    )
    return packets


def build_lane_rollup(packets: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rollups: list[dict[str, Any]] = []
    for lane in LANE_DEFINITIONS:
        lane_packets = [packet for packet in packets if packet.get("lane_id") == lane["lane_id"]]
        if not lane_packets:
            continue
        rollups.append(
            {
                "lane_id": lane["lane_id"],
                "label": lane["label"],
                "packet_count": len(lane_packets),
                "bug_count": sum(len(_sequence(packet.get("bug_ids"))) for packet in lane_packets),
                "wave_ids": sorted({str(packet.get("wave_id")) for packet in lane_packets}),
            }
        )
    return rollups


def build_wave_rollup(packets: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    rollups: list[dict[str, Any]] = []
    for wave in WAVE_DEFINITIONS:
        wave_packets = [packet for packet in packets if packet.get("wave_id") == wave["wave_id"]]
        rollups.append(
            {
                "wave_id": wave["wave_id"],
                "label": wave["label"],
                "depends_on": list(wave["depends_on"]),
                "description": wave["description"],
                "packet_count": len(wave_packets),
                "bug_count": sum(len(_sequence(packet.get("bug_ids"))) for packet in wave_packets),
            }
        )
    return rollups


def _command_status(result: Mapping[str, Any]) -> dict[str, Any]:
    payload = result.get("payload")
    return {
        "ok": bool(result.get("ok")),
        "command": _text(result.get("command")),
        "exit_code": int(result.get("exit_code", 0) or 0),
        "error": _text(result.get("error")) or _text(result.get("stderr")),
        "returned_keys": sorted(payload.keys()) if isinstance(payload, dict) else [],
    }


def build_coordination_payload(
    *,
    program_id: str,
    orient_result: Mapping[str, Any],
    stats_result: Mapping[str, Any],
    list_result: Mapping[str, Any],
    search_result: Mapping[str, Any],
    replay_ready_result: Mapping[str, Any],
    generated_at: str | None = None,
) -> dict[str, Any]:
    from runtime.primitive_contracts import bug_resolved_status_values

    captured_at = generated_at or utc_now_iso()
    payload: dict[str, Any] = {
        "program_id": program_id,
        "captured_at": captured_at,
        "coordination_state": "blocked_authority",
        "frozen_scope_policy": (
            "Only bugs that are OPEN at kickoff and present in this captured snapshot are in scope. "
            "New bugs after captured_at belong to the next intake cycle unless caused by this program."
        ),
        "max_parallel_lanes": 5,
        "packet_contract": {
            "required_fields": list(PACKET_REQUIRED_FIELDS),
            "terminal_statuses": list(bug_resolved_status_values()),
            "fix_requires_validation_evidence": True,
        },
        "authority_checks": {
            "orient": _command_status(orient_result),
            "bug_stats": _command_status(stats_result),
            "bug_list": _command_status(list_result),
            "bug_search": _command_status(search_result),
            "replay_ready_bugs": _command_status(replay_ready_result),
        },
        "lane_catalog": lane_catalog(),
        "waves": wave_catalog(),
        "lane_rollup": [],
        "snapshot": {
            "stats": {},
            "bugs": [],
            "clusters": [],
            "replay_ready_bug_ids": [],
        },
        "packets": [],
        "errors": [],
    }

    critical_results = (
        orient_result,
        stats_result,
        list_result,
        search_result,
        replay_ready_result,
    )
    failures = [result for result in critical_results if not bool(result.get("ok"))]
    if failures:
        payload["errors"] = [
            {
                "command": _text(item.get("command")),
                "error": _text(item.get("error")) or _text(item.get("stderr")),
                "exit_code": int(item.get("exit_code", 0) or 0),
            }
            for item in failures
        ]
        return payload

    stats_payload = dict(stats_result.get("payload") or {})
    list_payload = dict(list_result.get("payload") or {})
    replay_payload = dict(replay_ready_result.get("payload") or {})
    bugs = [dict(item) for item in list_payload.get("bugs") or ()]
    clusters = [dict(item) for item in list_payload.get("clusters") or ()]
    replay_ready_bug_ids = [
        _text(item.get("bug_id"))
        for item in replay_payload.get("bugs") or ()
        if _text(item.get("bug_id"))
    ]
    packets = derive_bug_packets(program_id=program_id, bugs=bugs, clusters=clusters)
    payload["coordination_state"] = "frozen"
    payload["snapshot"] = {
        "stats": dict(stats_payload.get("stats") or {}),
        "returned_count": int(list_payload.get("returned_count", len(bugs)) or len(bugs)),
        "count": int(list_payload.get("count", len(bugs)) or len(bugs)),
        "bugs": bugs,
        "clusters": clusters,
        "replay_ready_bug_ids": replay_ready_bug_ids,
        "replay_ready_count": len(replay_ready_bug_ids),
        "search_probe": dict(search_result.get("payload") or {}),
    }
    payload["packets"] = packets
    payload["lane_rollup"] = build_lane_rollup(packets)
    payload["waves"] = build_wave_rollup(packets)
    return payload


def render_packet_spec_template(
    template_text: str,
    *,
    packet: Mapping[str, Any],
    coordination_path: str,
) -> str:
    """Render one packet template into concrete queue JSON."""
    bug_titles = _sequence(packet.get("bug_titles"))
    replacements = {
        "PACKET_NAME": f"{packet['lane_label']} / {packet['cluster']['label']}",
        "PACKET_SLUG": _text(packet.get("packet_slug")),
        "PACKET_ID": _text(packet.get("packet_id")),
        "PROGRAM_ID": _text(str(packet.get("packet_id", "")).split(".", 1)[0]),
        "BUG_IDS_JSON": json.dumps(list(_sequence(packet.get("bug_ids")))),
        "BUG_IDS_COMMA": ", ".join(_sequence(packet.get("bug_ids"))),
        "BUG_TITLES_MD": "\n".join(f"- {title}" for title in bug_titles) or "- No titles recorded",
        "LANE_ID": _text(packet.get("lane_id")),
        "LANE_LABEL": _text(packet.get("lane_label")),
        "WAVE_ID": _text(packet.get("wave_id")),
        "PACKET_KIND": _text(packet.get("packet_kind")),
        "AUTHORITY_OWNER": _text(packet.get("authority_owner")),
        "CLUSTER_LABEL": _text(_text(packet.get("cluster", {}).get("label"))),
        "CLUSTER_KEY": _text(_text(packet.get("cluster", {}).get("cluster_key"))),
        "VERIFICATION_SURFACE": _text(packet.get("verification_surface")),
        "DONE_CRITERIA_MD": "\n".join(
            f"- {item}" for item in _sequence(packet.get("done_criteria"))
        ),
        "STOP_BOUNDARY": _text(packet.get("stop_boundary")),
        "DEPENDS_ON_WAVE": ", ".join(_sequence(packet.get("depends_on_wave"))) or "none",
        "COORDINATION_PATH": coordination_path,
    }

    rendered = template_text
    for token, value in replacements.items():
        rendered = rendered.replace(f"{{{{{token}}}}}", json.dumps(value)[1:-1])
    json.loads(rendered)
    return rendered


def materialize_packet_specs(
    *,
    coordination: Mapping[str, Any],
    template_text: str,
    coordination_path: str,
    output_dir: str | Path,
) -> list[dict[str, Any]]:
    """Render one spec file per packet from a coordination payload."""
    output_root = Path(output_dir)
    output_root.mkdir(parents=True, exist_ok=True)
    packets = [dict(packet) for packet in coordination.get("packets") or ()]
    materialized: list[dict[str, Any]] = []
    for packet in packets:
        rendered = render_packet_spec_template(
            template_text,
            packet=packet,
            coordination_path=coordination_path,
        )
        output_path = output_root / f"{packet['packet_slug']}.queue.json"
        output_path.write_text(rendered, encoding="utf-8")
        materialized.append(
            {
                "packet_id": packet["packet_id"],
                "packet_slug": packet["packet_slug"],
                "wave_id": packet["wave_id"],
                "lane_id": packet["lane_id"],
                "bug_ids": list(_sequence(packet.get("bug_ids"))),
                "spec_path": str(output_path),
            }
        )
    return materialized


def build_workflow_chain_payload(
    *,
    coordination: Mapping[str, Any],
    packet_specs: Sequence[Mapping[str, Any]],
    program_id: str | None = None,
    max_parallel: int | None = None,
) -> dict[str, Any]:
    """Build a durable workflow-chain coordination payload from packet specs."""

    resolved_program_id = _text(program_id) or _text(coordination.get("program_id")) or "bug_resolution_program"
    resolved_max_parallel = int(max_parallel or coordination.get("max_parallel_lanes") or 5)
    if resolved_max_parallel < 1:
        resolved_max_parallel = 1

    spec_by_packet_slug = {
        _text(item.get("packet_slug")): _text(item.get("spec_path"))
        for item in packet_specs
        if _text(item.get("packet_slug")) and _text(item.get("spec_path"))
    }
    packet_by_slug = {
        _text(packet.get("packet_slug")): dict(packet)
        for packet in coordination.get("packets") or ()
        if _text(packet.get("packet_slug"))
    }
    missing_specs = [
        slug
        for slug in packet_by_slug
        if slug not in spec_by_packet_slug
    ]
    if missing_specs:
        raise ValueError(
            "packet specs missing for: " + ", ".join(sorted(missing_specs)[:10])
        )

    waves: list[dict[str, Any]] = []
    validate_order: list[str] = []
    previous_chain_wave_id: str | None = None
    for wave_index, wave in enumerate(WAVE_DEFINITIONS, start=1):
        wave_packets = [
            packet_by_slug[_text(packet.get("packet_slug"))]
            for packet in coordination.get("packets") or ()
            if _text(packet.get("wave_id")) == wave["wave_id"]
        ]
        for batch_index in range(0, len(wave_packets), resolved_max_parallel):
            batch_packets = wave_packets[batch_index: batch_index + resolved_max_parallel]
            if not batch_packets:
                continue
            batch_number = (batch_index // resolved_max_parallel) + 1
            chain_wave_id = f"{wave['wave_id']}_batch_{batch_number:03d}"
            specs = [
                spec_by_packet_slug[_text(packet.get("packet_slug"))]
                for packet in batch_packets
            ]
            validate_order.extend(specs)
            wave_payload: dict[str, Any] = {
                "wave_id": chain_wave_id,
                "specs": specs,
            }
            if previous_chain_wave_id is not None:
                wave_payload["depends_on"] = [previous_chain_wave_id]
            waves.append(wave_payload)
            previous_chain_wave_id = chain_wave_id

    if not waves:
        raise ValueError("cannot build workflow chain without packet specs")

    return {
        "program": resolved_program_id,
        "mode": "bounded_bug_resolution",
        "why": (
            "Durable execution chain for the frozen bug-resolution program. "
            f"Packets are batched at max_parallel={resolved_max_parallel}."
        ),
        "validate_order": validate_order,
        "waves": waves,
    }


__all__ = [
    "LANE_DEFINITIONS",
    "PACKET_REQUIRED_FIELDS",
    "WAVE_DEFINITIONS",
    "build_coordination_payload",
    "build_lane_rollup",
    "build_workflow_chain_payload",
    "build_wave_rollup",
    "classify_bug_lane",
    "derive_bug_packets",
    "lane_catalog",
    "materialize_packet_specs",
    "render_packet_spec_template",
    "slugify",
    "utc_now_iso",
    "wave_catalog",
]
