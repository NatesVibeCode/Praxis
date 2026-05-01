"""Typed payload for materialize-time decisions surfaced to the Review handler.

Foundation slice for the unified materialize + Review front door (decision
filed as the lunchbox at /Users/nate/.claude/plans/and-praxis-phase-to-plan-
enchanted-hanrahan.md).

The future chat-model Review handler reads :class:`MaterializeReviewPayload`
and explains, per packet, what the materializer picked, what runner-ups it
had on hand, and what mappings could not link. Capturing alternatives at
decision time (here, in the materialize loop) is the *honest-Review*
invariant — the handler must never invent runner-ups after the fact.

Authority registration lives in migration ``396_materialize_decision_authority``;
the ``data_dictionary_objects`` row ``materialize.review.packet_decision_record``
points back at :class:`PacketDecisionRecord` via ``metadata.pydantic_model_ref``.

Lane discriminator:
- ``"auto"``     — "Materialize it for me" button (LLM does it; one-shot to graph)
- ``"manifest"`` — "Build the Manifest" button (user authors the scaffold step-by-step)
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

MaterializeDecisionKind = Literal[
    "stage_resolution",
    "write_scope_resolution",
    "source_ref_resolution",
    "agent_selection",
    "data_pill_binding",
    "capability_binding",
    "verification_admission",
]

MaterializeLane = Literal["auto", "manifest"]


class MaterializeAlternative(BaseModel):
    """One runner-up option the materializer had on hand at decision time."""

    ref: str
    reason_not_picked: str
    score: float | None = None


class MaterializeDecision(BaseModel):
    """One judgment call recorded during materialize.

    ``chosen`` is None when the materializer could not converge on a single
    option (e.g. ambiguous data-pill binding); ``alternatives`` is empty
    when there was no real judgment call (single valid option, no
    runner-ups). Consumers must treat empty ``alternatives`` as
    "no judgment call was made," not "data missing."
    """

    decision_kind: MaterializeDecisionKind
    chosen: str | None
    alternatives: list[MaterializeAlternative] = Field(default_factory=list)
    confidence: float | None = None
    notes: str = ""


class PacketDecisionRecord(BaseModel):
    """Per-packet roll-up the Review handler renders for one packet_map entry.

    ``unresolved_options`` carries the failure-mode equivalent: when the
    materializer raises an ``Unresolved*`` error before the per-packet
    ``decisions`` list is fully populated, the structured exception's
    ``available_options`` lift into this field so the Review still has
    something to explain.
    """

    packet_label: str
    decisions: list[MaterializeDecision] = Field(default_factory=list)
    unresolved_options: list[MaterializeAlternative] = Field(default_factory=list)


class MaterializeReviewPayload(BaseModel):
    """Top-level payload returned by the Review handler.

    Uniform across both lanes — the ``lane`` discriminator tells the
    chat-model prompt which framing to use ("here is what we auto-built
    for you" vs. "here is the manifest scaffold you authored"). Foundation
    slice fixes the contract; the handler that actually returns it lands
    in a follow-on slice.
    """

    lane: MaterializeLane
    workflow_id: str
    run_id: str | None = None
    packets: list[PacketDecisionRecord] = Field(default_factory=list)
    warnings: list[str] = Field(default_factory=list)
