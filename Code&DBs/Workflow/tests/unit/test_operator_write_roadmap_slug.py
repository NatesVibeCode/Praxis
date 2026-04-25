from __future__ import annotations

import asyncio
from datetime import datetime

from surfaces.api import operator_write


class _PreviewOnlyConnection:
    async def fetch(self, _query: str, *_args: object):
        return []

    async def fetchrow(self, _query: str, *_args: object):  # pragma: no cover - not expected
        raise AssertionError("preview without parent/source refs should not fetch rows")

    async def close(self) -> None:
        return None


def test_roadmap_write_rejects_full_roadmap_item_id_as_slug() -> None:
    async def _connect_database(_env=None):
        return _PreviewOnlyConnection()

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect_database,
    )

    preview = asyncio.run(
        frontdoor.roadmap_write_async(
            action="preview",
            title="Normalize validation truth envelopes",
            intent_brief="Make validation results impossible to misread.",
            slug="roadmap_item.make.quality.gates.normalize.validation.truth.envelopes",
        )
    )

    assert preview["committed"] is False
    assert preview["blocking_errors"] == [
        "slug must be a roadmap slug fragment, not a full roadmap item id or key: "
        "roadmap_item.make.quality.gates.normalize.validation.truth.envelopes"
    ]
    assert preview["normalized_payload"]["slug"] == (
        "roadmap.item.make.quality.gates.normalize.validation.truth.envelopes"
    )


def test_roadmap_write_missing_intent_brief_gives_direct_hint() -> None:
    async def _connect_database(_env=None):
        return _PreviewOnlyConnection()

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect_database,
    )

    preview = asyncio.run(
        frontdoor.roadmap_write_async(
            action="preview",
            title="Normalize validation truth envelopes",
            intent_brief=None,
        )
    )

    assert preview["committed"] is False
    assert preview["blocking_errors"] == [
        "intent_brief is required; use workflow roadmap write <preview|validate|commit> "
        "--title <title> --intent-brief <brief>",
    ]


def test_roadmap_write_update_mode_reuses_existing_identity_and_allows_reparenting(
    monkeypatch,
) -> None:
    existing_item_id = "roadmap_item.phase_program.build_closure.phase_002"
    parent_item_id = "roadmap_item.phase_program.build_closure"
    existing_row = {
        "roadmap_item_id": existing_item_id,
        "roadmap_key": "roadmap.phase_program.build_closure.phase_002",
        "title": "Phase 002 Build Closure",
        "summary": "Promote closure work with explicit proof gates",
        "item_kind": "capability",
        "status": "active",
        "lifecycle": "claimed",
        "priority": "p1",
        "parent_roadmap_item_id": "roadmap_item.phase_program",
        "source_bug_id": None,
        "source_idea_id": None,
        "registry_paths": ["surfaces/operator"],
        "decision_ref": "decision.2026-04-24.phase-002",
        "created_at": datetime(2026, 4, 20, 12, 0),
        "acceptance_criteria": {
            "tier": "tier_1",
            "phase_ready": True,
            "approval_tag": "operator-write-2026-04-20",
            "outcome_gate": "Promote closure work with explicit proof gates",
            "phase_order": "33.6",
        },
    }

    async def _connect_database(_env=None):
        return _PreviewOnlyConnection()

    async def _fetch_roadmap_item(_self, _conn, *, roadmap_item_id: str):
        if roadmap_item_id == existing_item_id:
            return existing_row
        if roadmap_item_id == parent_item_id:
            return {
                "roadmap_item_id": parent_item_id,
                "roadmap_key": "roadmap.phase_program.build_closure",
                "title": "Phase Program Build Closure",
                "acceptance_criteria": {},
            }
        return None

    async def _roadmap_item_exists(_self, _conn, *, roadmap_item_id: str):
        return roadmap_item_id in {existing_item_id, parent_item_id}

    async def _bug_exists(_self, *_args, **_kwargs):
        return True

    async def _idea_exists(_self, *_args, **_kwargs):
        return True

    async def _roadmap_sibling_phase_orders(_self, *_args, **_kwargs):
        return ()

    frontdoor = operator_write.OperatorControlFrontdoor(
        connect_database=_connect_database,
    )
    monkeypatch.setattr(operator_write.OperatorControlFrontdoor, "_fetch_roadmap_item", _fetch_roadmap_item)
    monkeypatch.setattr(operator_write.OperatorControlFrontdoor, "_roadmap_item_exists", _roadmap_item_exists)
    monkeypatch.setattr(operator_write.OperatorControlFrontdoor, "_bug_exists", _bug_exists)
    monkeypatch.setattr(operator_write.OperatorControlFrontdoor, "_idea_exists", _idea_exists)
    monkeypatch.setattr(
        operator_write.OperatorControlFrontdoor,
        "_roadmap_sibling_phase_orders",
        _roadmap_sibling_phase_orders,
    )

    preview = asyncio.run(
        frontdoor.roadmap_write_async(
            action="preview",
            roadmap_item_id=existing_item_id,
            title="Phase 002 Build Closure",
            intent_brief="Promote closure work with explicit proof gates",
            parent_roadmap_item_id=parent_item_id,
            lifecycle="retired",
            slug="ignored-in-update-mode",
        )
    )

    assert preview["committed"] is False
    assert preview["blocking_errors"] == []
    assert preview["auto_fixes"] == [
        "slug ignored in update mode; roadmap_item_id drives identity",
        "status aligned to retired lifecycle: completed",
    ]
    assert preview["normalized_payload"]["parent_roadmap_item_id"] == parent_item_id
    assert preview["normalized_payload"]["lifecycle"] == "retired"
    assert preview["normalized_payload"]["status"] == "completed"
    assert preview["normalized_payload"]["root_phase_order"] == "33.6"
    assert preview["preview"]["roadmap_items"][0]["roadmap_item_id"] == existing_item_id
    assert preview["preview"]["roadmap_items"][0]["parent_roadmap_item_id"] == parent_item_id
