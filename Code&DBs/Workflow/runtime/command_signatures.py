"""Pure deterministic signature helpers for the control-command bus.

All functions here are side-effect-free: they take values in, return
canonical strings out.  No DB calls, no imports of mutable subsystems.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from runtime._helpers import _json_compatible

if TYPE_CHECKING:
    pass


def _json_dumps(value: object) -> str:
    return json.dumps(
        _json_compatible(value),
        sort_keys=True,
        separators=(",", ":"),
    )


def _canonical_signature(
    *,
    command_type: str,
    requested_by_kind: str,
    requested_by_ref: str,
    risk_level: str,
    payload: Mapping[str, Any],
) -> str:
    return _json_dumps(
        {
            "command_type": command_type,
            "requested_by_kind": requested_by_kind,
            "requested_by_ref": requested_by_ref,
            "risk_level": risk_level,
            "payload": payload,
        }
    )


def _record_payload_signature(record: Any) -> str:
    """Compute the canonical signature for a ControlCommandRecord."""
    return _canonical_signature(
        command_type=record.command_type,
        requested_by_kind=record.requested_by_kind,
        requested_by_ref=record.requested_by_ref,
        risk_level=record.risk_level,
        payload=record.payload,
    )


def _intent_payload_signature(intent: Any) -> str:
    """Compute the canonical signature for a ControlIntent."""
    return _canonical_signature(
        command_type=intent.command_type,
        requested_by_kind=intent.requested_by_kind,
        requested_by_ref=intent.requested_by_ref,
        risk_level=intent.risk_level,
        payload=intent.payload,
    )
