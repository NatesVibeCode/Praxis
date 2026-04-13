"""Topology contract: profile postures, promotion rails, and registry."""

from __future__ import annotations

import enum
import os
from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class ProfilePosture(enum.Enum):
    OBSERVE = "observe"
    OPERATE = "operate"
    BUILD = "build"


class PromotionStep(enum.Enum):
    SHOW = "show"
    ACTIVATE = "activate"
    DRIFT_CHECK = "drift_check"
    VERIFY = "verify"


_STEP_ORDER: list[PromotionStep] = [
    PromotionStep.SHOW,
    PromotionStep.ACTIVATE,
    PromotionStep.DRIFT_CHECK,
    PromotionStep.VERIFY,
]

_MANDATORY_STEPS: set[PromotionStep] = {PromotionStep.SHOW, PromotionStep.VERIFY}


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class ProfileContract:
    profile_name: str
    posture: ProfilePosture
    receipts_dir: str
    topology_dir: str
    workdir: str
    allowed_write_roots: tuple[str, ...]
    max_concurrent_dispatches: int
    require_evidence: bool


@dataclass(frozen=True)
class PromotionState:
    profile_name: str
    completed_steps: tuple[PromotionStep, ...]
    current_step: Optional[PromotionStep]
    blocked: bool
    block_reason: Optional[str]


# ---------------------------------------------------------------------------
# TopologyRegistry
# ---------------------------------------------------------------------------

class TopologyRegistry:
    """Holds and validates profile contracts."""

    def __init__(self) -> None:
        self._profiles: dict[str, ProfileContract] = {}

    def register(self, contract: ProfileContract) -> None:
        self._profiles[contract.profile_name] = contract

    def get(self, profile_name: str) -> Optional[ProfileContract]:
        return self._profiles.get(profile_name)

    def list_profiles(self) -> list[ProfileContract]:
        return list(self._profiles.values())

    def validate(self, profile_name: str) -> list[str]:
        contract = self._profiles.get(profile_name)
        if contract is None:
            return [f"profile '{profile_name}' not registered"]

        errors: list[str] = []

        if not _is_under(contract.receipts_dir, contract.workdir):
            errors.append("receipts_dir must be under workdir")

        if not _is_under(contract.topology_dir, contract.workdir):
            errors.append("topology_dir must be under workdir")

        for root in contract.allowed_write_roots:
            if not os.path.isabs(root):
                errors.append(f"allowed_write_root '{root}' must be absolute")

        return errors


def _is_under(child: str, parent: str) -> bool:
    """Return True if *child* is equal to or nested under *parent*."""
    c = os.path.normpath(os.path.abspath(child))
    p = os.path.normpath(os.path.abspath(parent))
    return c == p or c.startswith(p + os.sep)


# ---------------------------------------------------------------------------
# PromotionPreflightRails
# ---------------------------------------------------------------------------

class PromotionPreflightRails:
    """Tracks promotion state per profile through a mandatory step sequence."""

    def __init__(self) -> None:
        self._states: dict[str, PromotionState] = {}

    def begin_promotion(self, profile_name: str) -> PromotionState:
        state = PromotionState(
            profile_name=profile_name,
            completed_steps=(),
            current_step=PromotionStep.SHOW,
            blocked=False,
            block_reason=None,
        )
        self._states[profile_name] = state
        return state

    def advance(self, profile_name: str) -> PromotionState:
        state = self._states.get(profile_name)
        if state is None:
            raise ValueError(f"no promotion in progress for '{profile_name}'")
        if state.blocked:
            raise ValueError(f"promotion for '{profile_name}' is blocked: {state.block_reason}")
        if state.current_step is None:
            return state  # already completed

        current_idx = _STEP_ORDER.index(state.current_step)
        completed = state.completed_steps + (state.current_step,)

        next_idx = current_idx + 1
        if next_idx >= len(_STEP_ORDER):
            # All steps done
            new_state = PromotionState(
                profile_name=profile_name,
                completed_steps=completed,
                current_step=None,
                blocked=False,
                block_reason=None,
            )
        else:
            new_state = PromotionState(
                profile_name=profile_name,
                completed_steps=completed,
                current_step=_STEP_ORDER[next_idx],
                blocked=False,
                block_reason=None,
            )

        self._states[profile_name] = new_state
        return new_state

    def complete(self, profile_name: str) -> PromotionState:
        state = self._states.get(profile_name)
        if state is None:
            raise ValueError(f"no promotion in progress for '{profile_name}'")

        new_state = PromotionState(
            profile_name=profile_name,
            completed_steps=tuple(_STEP_ORDER),
            current_step=None,
            blocked=False,
            block_reason=None,
        )
        self._states[profile_name] = new_state
        return new_state

    def skip(self, profile_name: str, step: PromotionStep, cause: str) -> PromotionState:
        if step in _MANDATORY_STEPS:
            raise ValueError(f"cannot skip mandatory step {step.name}")

        state = self._states.get(profile_name)
        if state is None:
            raise ValueError(f"no promotion in progress for '{profile_name}'")

        new_state = PromotionState(
            profile_name=profile_name,
            completed_steps=state.completed_steps,
            current_step=state.current_step,
            blocked=True,
            block_reason=cause,
        )
        self._states[profile_name] = new_state
        return new_state

    def state(self, profile_name: str) -> Optional[PromotionState]:
        return self._states.get(profile_name)
