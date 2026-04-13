"""Posture enforcement for workflow tool calls.

Classifies tool calls and gates them against the active posture level.
Fail-closed: unknown tools are treated as MUTATE.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime


class Posture(enum.Enum):
    """Runtime posture levels, ordered from most to least restrictive."""

    OBSERVE = "observe"  # Read-only, no mutations
    OPERATE = "operate"  # Mutations with receipts
    BUILD = "build"  # Full access


class CallClassification(enum.Enum):
    """How a tool call is classified for posture gating."""

    READ = "read"
    MUTATE = "mutate"
    TELEMETRY = "telemetry"


_DEFAULT_PREFIX_MAP: dict[str, CallClassification] = {
    "get_": CallClassification.READ,
    "list_": CallClassification.READ,
    "search_": CallClassification.READ,
    "query_": CallClassification.READ,
    "status_": CallClassification.READ,
    "inspect_": CallClassification.READ,
    "read_": CallClassification.READ,
    "create_": CallClassification.MUTATE,
    "update_": CallClassification.MUTATE,
    "delete_": CallClassification.MUTATE,
    "write_": CallClassification.MUTATE,
    "insert_": CallClassification.MUTATE,
    "workflow_": CallClassification.MUTATE,
    "execute_": CallClassification.MUTATE,
    "log_": CallClassification.TELEMETRY,
    "record_": CallClassification.TELEMETRY,
    "emit_": CallClassification.TELEMETRY,
    "track_": CallClassification.TELEMETRY,
}

# Which classifications each posture allows.
_POSTURE_ALLOWED: dict[Posture, frozenset[CallClassification]] = {
    Posture.OBSERVE: frozenset({CallClassification.READ, CallClassification.TELEMETRY}),
    Posture.OPERATE: frozenset(
        {CallClassification.READ, CallClassification.MUTATE, CallClassification.TELEMETRY}
    ),
    Posture.BUILD: frozenset(
        {CallClassification.READ, CallClassification.MUTATE, CallClassification.TELEMETRY}
    ),
}


@dataclass(frozen=True)
class ToolCall:
    """An immutable record of a single tool invocation."""

    tool_name: str
    arguments: dict
    timestamp: datetime


@dataclass(frozen=True)
class PostureVerdict:
    """Result of a posture check against a tool call."""

    allowed: bool
    classification: CallClassification
    posture: Posture
    reason: str | None = None


class PostureEnforcer:
    """Gates tool calls against an active posture.

    Immutable-style: use ``with_posture`` to get a new enforcer at a
    different level rather than mutating this one.
    """

    def __init__(
        self,
        posture: Posture,
        tool_classifications: dict[str, CallClassification] | None = None,
    ) -> None:
        self._posture = posture
        self._custom: dict[str, CallClassification] = dict(tool_classifications or {})
        self._deny_log: list[ToolCall] = []

    # -- public API ----------------------------------------------------------

    def classify(self, tool_call: ToolCall) -> CallClassification:
        """Classify a tool call. Unknown tools default to MUTATE (fail-closed)."""
        name = tool_call.tool_name

        # Explicit override wins.
        if name in self._custom:
            return self._custom[name]

        # Prefix matching against defaults.
        for prefix, classification in _DEFAULT_PREFIX_MAP.items():
            if name.startswith(prefix):
                return classification

        # Fail-closed: unknown is MUTATE.
        return CallClassification.MUTATE

    def check(self, tool_call: ToolCall) -> PostureVerdict:
        """Check whether *tool_call* is allowed under the active posture."""
        cls = self.classify(tool_call)
        allowed_set = _POSTURE_ALLOWED[self._posture]
        allowed = cls in allowed_set

        reason: str | None = None
        if not allowed:
            reason = (
                f"{cls.value} call '{tool_call.tool_name}' blocked by "
                f"{self._posture.value} posture"
            )
            self._deny_log.append(tool_call)

        return PostureVerdict(
            allowed=allowed,
            classification=cls,
            posture=self._posture,
            reason=reason,
        )

    @property
    def deny_log(self) -> list[ToolCall]:
        """Append-only log of denied calls."""
        return list(self._deny_log)

    def with_posture(self, posture: Posture) -> PostureEnforcer:
        """Return a *new* enforcer at a different posture level."""
        return PostureEnforcer(posture, tool_classifications=self._custom)
