"""Full data-dictionary refresh coordinator.

The reproject surfaces use this helper when operators want the data-dictionary
authority refreshed now instead of waiting for the heartbeat cycle.
"""

from __future__ import annotations

import logging
import time
from typing import Any

logger = logging.getLogger(__name__)


def _module_result(module: Any) -> dict[str, Any]:
    return {
        "name": getattr(module, "name", module.__class__.__name__),
        "ok": bool(getattr(module, "ok", True)),
        "duration_ms": getattr(module, "duration_ms", None),
        "error": getattr(module, "error", None),
    }


def refresh_data_dictionary_authority(conn: Any) -> dict[str, Any]:
    """Run the full data-dictionary projection bundle immediately."""
    if conn is None:
        return {
            "ok": False,
            "duration_ms": 0.0,
            "error": "data_dictionary_authority_unavailable",
            "modules": [],
        }

    from memory.data_dictionary_classifications_projector import (
        DataDictionaryClassificationsProjector,
    )
    from memory.data_dictionary_drift_projector import DataDictionaryDriftProjector
    from memory.data_dictionary_lineage_projector import DataDictionaryLineageProjector
    from memory.data_dictionary_projector import DataDictionaryProjector
    from memory.data_dictionary_quality_projector import DataDictionaryQualityProjector
    from memory.data_dictionary_stewardship_projector import (
        DataDictionaryStewardshipProjector,
    )

    t0 = time.monotonic()
    errors: list[str] = []
    modules: list[dict[str, Any]] = []
    for projector_cls in (
        DataDictionaryProjector,
        DataDictionaryLineageProjector,
        DataDictionaryClassificationsProjector,
        DataDictionaryQualityProjector,
        DataDictionaryStewardshipProjector,
        DataDictionaryDriftProjector,
    ):
        module = projector_cls(conn)
        try:
            result = module.run()
        except Exception as exc:  # pragma: no cover - modules should self-report
            logger.exception("data dictionary refresh step %s failed", projector_cls.__name__)
            errors.append(f"{projector_cls.__name__}: {type(exc).__name__}: {exc}")
            modules.append({
                "name": getattr(module, "name", projector_cls.__name__),
                "ok": False,
                "duration_ms": None,
                "error": str(exc),
            })
            continue
        modules.append(_module_result(result))
        if not bool(getattr(result, "ok", True)):
            error_text = str(getattr(result, "error", "") or projector_cls.__name__)
            errors.append(f"{projector_cls.__name__}: {error_text}")

    duration_ms = round((time.monotonic() - t0) * 1000.0, 3)
    return {
        "ok": not errors,
        "duration_ms": duration_ms,
        "error": "; ".join(errors) if errors else None,
        "modules": modules,
    }


__all__ = ["refresh_data_dictionary_authority"]
