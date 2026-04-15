"""Seed the `reference_catalog` table with compiler reference metadata.

The catalog is the authority source for symbolic references like
`@integration/action`, `#object.field`, and `auto/<task>` used by the
compiler and runtime validation surfaces. This module keeps startup wiring
explicit and idempotent.
"""
from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


def seed_reference_catalog(conn: Any) -> int:
    """Seed ``reference_catalog`` rows and return number of rows prepared.

    The function intentionally degrades gracefully: seeding failures are logged
    and surfaced as ``0`` rows seeded rather than aborting startup.
    """
    if conn is None:
        return 0

    rows_seeded = 0
    try:
        from registry.reference_catalog_sync import sync_reference_catalog

        rows_seeded = sync_reference_catalog(conn)
        return rows_seeded
    except Exception as exc:
        logger.warning("reference catalog sync failed: %s", exc)
        return rows_seeded
