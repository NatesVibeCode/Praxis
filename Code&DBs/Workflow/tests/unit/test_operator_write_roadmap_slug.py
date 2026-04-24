from __future__ import annotations

import asyncio

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
