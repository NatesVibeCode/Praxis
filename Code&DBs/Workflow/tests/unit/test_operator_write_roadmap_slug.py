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
    # status auto_fix now fires earlier in normalization (alongside lifecycle
    # coercion) rather than as a downstream alignment pass, so it precedes the
    # slug auto_fix. Order is incidental — both messages must be present.
    assert set(preview["auto_fixes"]) == {
        "slug ignored in update mode; roadmap_item_id drives identity",
        "status aligned to retired lifecycle: completed",
    }
    assert preview["normalized_payload"]["parent_roadmap_item_id"] == parent_item_id
    assert preview["normalized_payload"]["lifecycle"] == "retired"
    assert preview["normalized_payload"]["status"] == "completed"
    assert preview["normalized_payload"]["priority"] == "p1"
    assert preview["normalized_payload"]["root_phase_order"] == "33.6"
    assert preview["preview"]["roadmap_items"][0]["roadmap_item_id"] == existing_item_id
    assert preview["preview"]["roadmap_items"][0]["parent_roadmap_item_id"] == parent_item_id
    assert preview["preview"]["roadmap_items"][0]["priority"] == "p1"


def test_roadmap_update_explicit_p2_priority_overrides_existing_p1(
    monkeypatch,
) -> None:
    existing_item_id = "roadmap_item.stale.p1.priority"
    existing_row = {
        "roadmap_item_id": existing_item_id,
        "roadmap_key": "roadmap.stale.p1.priority",
        "title": "Stale P1 priority",
        "summary": "Demote stale roadmap work through the update surface",
        "item_kind": "capability",
        "status": "active",
        "lifecycle": "planned",
        "priority": "p1",
        "parent_roadmap_item_id": None,
        "source_bug_id": None,
        "source_idea_id": None,
        "registry_paths": [],
        "decision_ref": None,
        "created_at": datetime(2026, 4, 30, 12, 0),
        "acceptance_criteria": {},
    }

    async def _connect_database(_env=None):
        return _PreviewOnlyConnection()

    async def _fetch_roadmap_item(_self, _conn, *, roadmap_item_id: str):
        return existing_row if roadmap_item_id == existing_item_id else None

    async def _roadmap_item_exists(_self, _conn, *, roadmap_item_id: str):
        return roadmap_item_id == existing_item_id

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
            priority="p2",
        )
    )

    assert preview["committed"] is False
    assert preview["blocking_errors"] == []
    assert preview["normalized_payload"]["priority"] == "p2"
    assert preview["preview"]["roadmap_items"][0]["priority"] == "p2"


def test_roadmap_retire_succeeds_when_existing_row_status_is_outside_input_enum(
    monkeypatch,
) -> None:
    """Retire path must not fail enum validation on the existing row's status.

    Closes BUG-2700B72E: a roadmap row with status='proposed' (not in the input
    enum {active, completed, done}) used to fail
    ValueError("status must be one of active, completed, done") during retire,
    even though retire normalizes lifecycle=retired and forces status=completed
    by design. The auto-fix existed but was unreachable because the enum
    coercion read the existing row's status before the lifecycle-driven fix
    could rewrite it.
    """
    existing_item_id = "roadmap_item.legacy.proposed.row"
    existing_row = {
        "roadmap_item_id": existing_item_id,
        "roadmap_key": "roadmap.legacy.proposed.row",
        "title": "Legacy proposed row",
        "summary": "Pre-existing row with non-canonical status",
        "item_kind": "capability",
        "status": "proposed",
        "lifecycle": "planned",
        "priority": "p2",
        "parent_roadmap_item_id": None,
        "source_bug_id": None,
        "source_idea_id": None,
        "registry_paths": [],
        "decision_ref": None,
        "created_at": datetime(2026, 4, 3, 3, 0),
        "acceptance_criteria": {},
    }

    async def _connect_database(_env=None):
        return _PreviewOnlyConnection()

    async def _fetch_roadmap_item(_self, _conn, *, roadmap_item_id: str):
        return existing_row if roadmap_item_id == existing_item_id else None

    async def _roadmap_item_exists(_self, _conn, *, roadmap_item_id: str):
        return roadmap_item_id == existing_item_id

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
            lifecycle="retired",
        )
    )

    assert preview["committed"] is False
    assert preview["blocking_errors"] == []
    assert preview["normalized_payload"]["lifecycle"] == "retired"
    assert preview["normalized_payload"]["status"] == "completed"
    assert "status aligned to retired lifecycle: completed" in preview["auto_fixes"]


def test_roadmap_update_coerces_out_of_enum_existing_status_with_auto_fix(
    monkeypatch,
) -> None:
    """Update without explicit status must coerce truly unknown values safely.

    Closes the safety-net half of BUG-7DBAEE37: even after widening the
    canonical set to include 'proposed', a row may carry a status no
    enum knows about (legacy import, retired vocabulary, drift). Update
    paths must not fail enum validation on such ghost values — coerce to
    'active' with an auto_fix note rather than raising. The caller didn't
    ask for the bad status, and a stray legacy value shouldn't make the
    caller learn the auto-fix shape.
    """
    existing_item_id = "roadmap_item.legacy.ghost_status.update_target"
    existing_row = {
        "roadmap_item_id": existing_item_id,
        "roadmap_key": "roadmap.legacy.ghost_status.update_target",
        "title": "Legacy row with unknown status vocabulary",
        "summary": "Pre-existing row carrying a status no current enum knows",
        "item_kind": "capability",
        "status": "totally_legacy_ghost_status",
        "lifecycle": "planned",
        "priority": "p2",
        "parent_roadmap_item_id": None,
        "source_bug_id": None,
        "source_idea_id": None,
        "registry_paths": [],
        "decision_ref": None,
        "created_at": datetime(2026, 4, 3, 3, 0),
        "acceptance_criteria": {},
    }

    async def _connect_database(_env=None):
        return _PreviewOnlyConnection()

    async def _fetch_roadmap_item(_self, _conn, *, roadmap_item_id: str):
        return existing_row if roadmap_item_id == existing_item_id else None

    async def _roadmap_item_exists(_self, _conn, *, roadmap_item_id: str):
        return roadmap_item_id == existing_item_id

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
            title="Legacy proposed row (touched up)",
            intent_brief="Tweak metadata without specifying status",
        )
    )

    assert preview["committed"] is False
    assert preview["blocking_errors"] == []
    assert preview["normalized_payload"]["status"] == "active"
    assert any(
        "not in canonical set" in fix and "coerced to 'active'" in fix
        for fix in preview["auto_fixes"]
    ), preview["auto_fixes"]


def test_normalize_roadmap_status_accepts_proposed() -> None:
    """Closes the canonical-set half of BUG-7DBAEE37.

    'proposed' is the legitimate review-pending status used by the activity-truth
    review pattern (see test_native_truth_surface_cutover.py and the
    praxis_operator_roadmap_view 'status' filter description). The write enum
    used to reject it; now the input contract matches the read model.
    """
    assert operator_write._normalize_roadmap_status("proposed") == "proposed"
    assert operator_write._normalize_roadmap_status("PROPOSED") == "proposed"
    assert "proposed" in operator_write._ROADMAP_STATUSES


def test_normalize_roadmap_priority_accepts_p0_through_p3() -> None:
    assert operator_write._normalize_roadmap_priority("p0") == "p0"
    assert operator_write._normalize_roadmap_priority("P0") == "p0"
    assert operator_write._normalize_roadmap_priority("p3") == "p3"
    assert operator_write._normalize_roadmap_priority("P3") == "p3"


def test_auto_promoted_bug_priority_tracks_severity_ladder() -> None:
    assert operator_write._auto_promoted_bug_priority("P0") == "p0"
    assert operator_write._auto_promoted_bug_priority("CRITICAL") == "p0"
    assert operator_write._auto_promoted_bug_priority("P1") == "p1"
    assert operator_write._auto_promoted_bug_priority("HIGH") == "p1"
    assert operator_write._auto_promoted_bug_priority("P2") == "p2"
    assert operator_write._auto_promoted_bug_priority("P3") == "p3"
    assert operator_write._auto_promoted_bug_priority(None) == "p2"
