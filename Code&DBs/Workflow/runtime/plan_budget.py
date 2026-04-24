"""Project token + prompt-size budgets for a ProposedPlan before spend.

Honest scope: this is an *estimate*, not an oracle.

  - Prompt token counts are char-based (``len(rendered_prompt) / 4``), a
    well-known rough approximation. Tokenizer-exact counts require
    per-provider tokenizers that this runtime does not ship.
  - Output token counts default to a conservative per-stage estimate so
    the caller sees a plausible upper bound without depending on model
    max_tokens values that may not be set.
  - Cost projection is NOT included here. Real USD cost depends on the
    resolved model's price card at run time; the right place for that is
    the post-run receipt where actual model + tokens are known. Surfacing
    a guessed cost here would be exactly the "papering over context" the
    caller warned about.

The projection is a Q (read) operation — pure computation from a
ProposedPlan, no DB writes. Caller invokes when budget is a gating
concern; skip it for low-risk launches.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


# Conservative per-stage output-token estimate. Tuned to be a plausible
# upper bound rather than an expected value. Caller can override per
# packet via output_tokens_by_label when they know better.
_OUTPUT_TOKEN_ESTIMATE_BY_STAGE: dict[str, int] = {
    "build": 4000,
    "fix": 3000,
    "review": 2500,
    "test": 2500,
    "research": 3500,
}
_OUTPUT_TOKEN_FALLBACK = 3000

_CHARS_PER_TOKEN = 4  # rough, provider-agnostic rule of thumb


@dataclass(frozen=True)
class JobBudgetEstimate:
    """One job's estimated prompt + output token budget."""

    label: str
    resolved_agent: str | None
    prompt_chars: int
    estimated_prompt_tokens: int
    estimated_output_tokens: int
    estimated_total_tokens: int

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PlanBudgetProjection:
    """Rollup of per-job estimates."""

    jobs: list[JobBudgetEstimate]
    total_prompt_chars: int
    total_estimated_prompt_tokens: int
    total_estimated_output_tokens: int
    total_estimated_tokens: int
    warnings: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "jobs": [job.to_dict() for job in self.jobs],
            "total_prompt_chars": self.total_prompt_chars,
            "total_estimated_prompt_tokens": self.total_estimated_prompt_tokens,
            "total_estimated_output_tokens": self.total_estimated_output_tokens,
            "total_estimated_tokens": self.total_estimated_tokens,
            "warnings": list(self.warnings),
        }


def _estimate_prompt_tokens(chars: int) -> int:
    if chars <= 0:
        return 0
    # Ceiling — better to slightly over-estimate a budget than under.
    return (chars + _CHARS_PER_TOKEN - 1) // _CHARS_PER_TOKEN


def project_plan_budget(
    proposed: Any,
    *,
    output_tokens_by_label: dict[str, int] | None = None,
    output_tokens_by_stage: dict[str, int] | None = None,
) -> PlanBudgetProjection:
    """Project token budgets for every job in a :class:`ProposedPlan`.

    Args:
        proposed: a ``ProposedPlan`` (imported lazily to avoid circular
            dependency in spec_compiler.py).
        output_tokens_by_label: explicit per-packet override; wins over
            stage-based estimate.
        output_tokens_by_stage: caller-supplied override for stage →
            estimate mapping. Merges on top of the built-in defaults.

    Returns:
        :class:`PlanBudgetProjection` with per-job estimates + totals.
    """
    # Import lazily so callers that use plan_budget without the rest of
    # the spec_compiler don't force that import chain.
    from runtime.spec_compiler import ProposedPlan  # noqa: F401

    spec_dict = getattr(proposed, "spec_dict", None) or {}
    preview = getattr(proposed, "preview", None) or {}
    warnings: list[str] = []

    # Merge stage overrides on top of defaults.
    stage_table: dict[str, int] = dict(_OUTPUT_TOKEN_ESTIMATE_BY_STAGE)
    if output_tokens_by_stage:
        for stage, tokens in output_tokens_by_stage.items():
            stage_table[str(stage).strip().lower()] = int(tokens)

    label_override = {
        str(k): int(v) for k, v in (output_tokens_by_label or {}).items()
    }

    # Build a label → resolved_agent lookup from the preview payload so
    # callers can see which model-route will pay this budget.
    resolved_by_label: dict[str, str | None] = {}
    for preview_job in preview.get("jobs") or []:
        label = str(preview_job.get("label") or "").strip()
        if not label:
            continue
        resolved_by_label[label] = preview_job.get("resolved_agent")

    jobs: list[JobBudgetEstimate] = []
    total_prompt_chars = 0
    total_prompt_tokens = 0
    total_output_tokens = 0

    for job in spec_dict.get("jobs") or []:
        label = str(job.get("label") or "").strip() or "?"
        prompt = str(job.get("prompt") or "")
        stage = str(job.get("task_type") or "").strip().lower()

        prompt_chars = len(prompt)
        prompt_tokens = _estimate_prompt_tokens(prompt_chars)

        if label in label_override:
            output_tokens = label_override[label]
        elif stage in stage_table:
            output_tokens = stage_table[stage]
        else:
            output_tokens = _OUTPUT_TOKEN_FALLBACK
            if stage:
                warnings.append(
                    f"{label}: stage {stage!r} has no output-token estimate; "
                    f"using fallback {_OUTPUT_TOKEN_FALLBACK}"
                )

        estimate = JobBudgetEstimate(
            label=label,
            resolved_agent=resolved_by_label.get(label),
            prompt_chars=prompt_chars,
            estimated_prompt_tokens=prompt_tokens,
            estimated_output_tokens=output_tokens,
            estimated_total_tokens=prompt_tokens + output_tokens,
        )
        jobs.append(estimate)
        total_prompt_chars += prompt_chars
        total_prompt_tokens += prompt_tokens
        total_output_tokens += output_tokens

    return PlanBudgetProjection(
        jobs=jobs,
        total_prompt_chars=total_prompt_chars,
        total_estimated_prompt_tokens=total_prompt_tokens,
        total_estimated_output_tokens=total_output_tokens,
        total_estimated_tokens=total_prompt_tokens + total_output_tokens,
        warnings=warnings,
    )


__all__ = [
    "JobBudgetEstimate",
    "PlanBudgetProjection",
    "project_plan_budget",
]
