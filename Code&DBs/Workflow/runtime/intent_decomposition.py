"""Layer 2 (Decompose): split prose intent into discrete executable steps.

Honest scope: this is a *deterministic* decomposer. It extracts steps that
the author already made explicit in the prose — numbered lists, bulleted
lists, and "first / then / finally" sequences. It does NOT do semantic
splitting of free prose. That's real LLM work; when the caller needs it,
they wrap this module with an LLM extractor that emits explicit step
markers before calling in.

Returns a :class:`DecomposedIntent` carrying:

  - steps: list of :class:`StepIntent` — one per detected step
  - detection_mode: how the steps were found (``numbered_list`` /
    ``bulleted_list`` / ``ordered_phrases`` / ``single_step``)
  - rationale: short explanation the caller (or Canvas UI) can surface
  - warnings: anything that was ambiguous but didn't fail outright

When the prose has no explicit step markers, decompose_intent raises
:class:`DecompositionRequiresLLMError` — fail closed rather than silently
returning "the whole intent as one step" and pretending that's a plan.
Caller decides: reword the intent with explicit markers, wrap this call
with an LLM step extractor, or accept that the intent is already a single
step by passing ``allow_single_step=True`` explicitly.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True)
class StepIntent:
    """One decomposed step of the larger intent.

    Fields mirror what the caller would hand-write as a PlanPacket
    description + stage hint; composition with spec_materializer.PlanPacket
    is done externally (not every decomposed step is a packet).
    """

    index: int
    text: str
    raw_marker: str | None  # the literal "1." / "•" / "first" that matched
    stage_hint: str | None  # optional stage inferred from verb ("test"/"review"/...)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class DecomposedIntent:
    """Output of :func:`decompose_intent`."""

    intent: str
    steps: list[StepIntent]
    detection_mode: str
    rationale: str
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "intent": self.intent,
            "steps": [step.to_dict() for step in self.steps],
            "detection_mode": self.detection_mode,
            "rationale": self.rationale,
            "warnings": list(self.warnings),
        }


class DecompositionRequiresLLMError(ValueError):
    """Raised when the prose has no explicit step markers.

    Free prose decomposition is real LLM work; the deterministic
    decomposer refuses rather than silently treating everything as one
    step. Caller reacts by (a) rewording the intent with explicit
    markers, (b) wrapping with an LLM extractor, or (c) passing
    ``allow_single_step=True`` to accept the whole intent as one step.
    """


# ---------------------------------------------------------------------------
# Step-detection patterns
# ---------------------------------------------------------------------------

# Numbered lists: '1.', '2)', '1 -'  — digits followed by . or ) or -
_NUMBERED_LINE_PATTERN = re.compile(
    r"^\s*(\d+)\s*[.)\-]\s*(.+?)\s*$",
)

# Bulleted lists: '-', '*', '•', '–'
_BULLETED_LINE_PATTERN = re.compile(
    r"^\s*([-*•–])\s+(.+?)\s*$",
)

# Ordered phrases: 'first X, then Y, finally Z' (comma or period separated).
# Whole-sentence split; the ordering words must appear as lowercase tokens
# at the start of each clause to avoid false-positives on 'first-class'.
_ORDERED_MARKERS: tuple[str, ...] = ("first", "then", "next", "after that", "finally", "lastly")


# Stage inference from the first verb — conservative, small table.
#
# 2026-04-27: expanded the review-tier verb set so verification-style steps
# (confirm/validate/check/ensure/inspect/assert/reconcile) route as review
# instead of falling through to the default (build). The default-build
# routing forces submission_required=True via execution_bundle's
# write_scope-and-mutating-task-type heuristic, then auto-seal fails the
# job with workflow_submission.required_missing because the LLM produced
# no on-disk diff. Verification work has no diff by design.
_STAGE_VERB_MAP: dict[str, str] = {
    "build": "build",
    "implement": "build",
    "add": "build",
    "create": "build",
    "write": "build",
    "wire": "build",
    "fix": "fix",
    "repair": "fix",
    "resolve": "fix",
    "patch": "fix",
    "test": "test",
    "verify": "test",
    "review": "review",
    "audit": "review",
    "confirm": "review",
    "validate": "review",
    "check": "review",
    "ensure": "review",
    "inspect": "review",
    "assert": "review",
    "reconcile": "review",
    "document": "review",
    "describe": "review",
    "summarize": "review",
    "report": "review",
    "enumerate": "review",
    "surface": "review",
    "trace": "review",
    "diagnose": "review",
    "research": "research",
    "investigate": "research",
    "explore": "research",
    "study": "research",
}


def _stage_hint_from_text(text: str) -> str | None:
    """Peek at the first token; map known verbs to stages.

    Conservative on purpose — unknown verbs return None so the caller
    fills stage explicitly. No silent defaults.
    """
    match = re.match(r"^\s*([a-zA-Z]+)", text)
    if not match:
        return None
    verb = match.group(1).lower()
    return _STAGE_VERB_MAP.get(verb)


def _extract_numbered_steps(intent: str) -> list[StepIntent]:
    steps: list[StepIntent] = []
    for line in intent.splitlines():
        match = _NUMBERED_LINE_PATTERN.match(line)
        if not match:
            continue
        text = match.group(2).strip()
        if not text:
            continue
        steps.append(
            StepIntent(
                index=len(steps),
                text=text,
                raw_marker=match.group(1),
                stage_hint=_stage_hint_from_text(text),
            )
        )
    return steps


def _extract_bulleted_steps(intent: str) -> list[StepIntent]:
    steps: list[StepIntent] = []
    for line in intent.splitlines():
        match = _BULLETED_LINE_PATTERN.match(line)
        if not match:
            continue
        text = match.group(2).strip()
        if not text:
            continue
        steps.append(
            StepIntent(
                index=len(steps),
                text=text,
                raw_marker=match.group(1),
                stage_hint=_stage_hint_from_text(text),
            )
        )
    return steps


def _extract_ordered_phrase_steps(intent: str) -> list[StepIntent]:
    """Parse 'first X, then Y, finally Z' style prose into ordered steps.

    Splits on a limited whitelist of ordering words. Returns [] if fewer
    than two markers appear — no false-positive 'single-step'.
    """
    lowered = intent.lower()
    # Find marker positions in order-of-appearance.
    positions: list[tuple[int, str]] = []
    for marker in _ORDERED_MARKERS:
        # Require marker appears at start of clause (after whitespace / punctuation).
        for match in re.finditer(rf"(?:^|[\s,.;:])({re.escape(marker)})\b", lowered):
            positions.append((match.start(1), marker))
    positions.sort(key=lambda entry: entry[0])
    if len(positions) < 2:
        return []

    steps: list[StepIntent] = []
    for i, (start, marker) in enumerate(positions):
        end = positions[i + 1][0] if i + 1 < len(positions) else len(intent)
        fragment = intent[start:end].strip()
        # Strip the leading marker word so the step text is the actual action.
        fragment = re.sub(
            rf"^{re.escape(marker)}\b[,:\s]*",
            "",
            fragment,
            count=1,
            flags=re.IGNORECASE,
        ).strip()
        fragment = fragment.rstrip(",.;:")
        if not fragment:
            continue
        steps.append(
            StepIntent(
                index=len(steps),
                text=fragment,
                raw_marker=marker,
                stage_hint=_stage_hint_from_text(fragment),
            )
        )
    return steps


def decompose_intent(
    intent: str,
    *,
    allow_single_step: bool = False,
) -> DecomposedIntent:
    """Split prose intent into ordered steps.

    Args:
        intent: prose describing what the caller wants done.
        allow_single_step: when True, prose without explicit step markers
            is accepted as a single-step decomposition instead of raising.
            Use this when the caller is confident the intent is one step
            and just wants to run it through the rest of the pipeline.

    Returns:
        A :class:`DecomposedIntent` with one entry per detected step.

    Raises:
        ValueError: when the intent is empty.
        DecompositionRequiresLLMError: when no step markers were found and
            ``allow_single_step`` is False.
    """
    text = (intent or "").strip()
    if not text:
        raise ValueError("intent is empty — nothing to decompose")

    numbered = _extract_numbered_steps(text)
    if len(numbered) >= 2:
        return DecomposedIntent(
            intent=text,
            steps=numbered,
            detection_mode="numbered_list",
            rationale=f"parsed {len(numbered)} numbered-list entries",
        )

    bulleted = _extract_bulleted_steps(text)
    if len(bulleted) >= 2:
        return DecomposedIntent(
            intent=text,
            steps=bulleted,
            detection_mode="bulleted_list",
            rationale=f"parsed {len(bulleted)} bulleted-list entries",
        )

    ordered = _extract_ordered_phrase_steps(text)
    if len(ordered) >= 2:
        return DecomposedIntent(
            intent=text,
            steps=ordered,
            detection_mode="ordered_phrases",
            rationale=(
                f"parsed {len(ordered)} ordered-phrase steps (first/then/finally "
                "markers)"
            ),
        )

    if not allow_single_step:
        raise DecompositionRequiresLLMError(
            "intent has no explicit step markers (numbered list, bulleted list, "
            "or first/then/finally ordering). Deterministic decomposition refuses "
            "to treat free prose as a single step silently. Either (1) reword "
            "the intent with explicit markers, (2) wrap with an LLM extractor, "
            "or (3) pass allow_single_step=True to accept the whole intent as "
            "one step."
        )

    return DecomposedIntent(
        intent=text,
        steps=[
            StepIntent(
                index=0,
                text=text,
                raw_marker=None,
                stage_hint=_stage_hint_from_text(text),
            )
        ],
        detection_mode="single_step",
        rationale="caller accepted the whole intent as one step via allow_single_step",
    )


__all__ = [
    "DecomposedIntent",
    "DecompositionRequiresLLMError",
    "StepIntent",
    "decompose_intent",
]
