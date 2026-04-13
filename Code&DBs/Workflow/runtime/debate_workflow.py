"""Debate workflow pattern — multi-perspective analysis via fan-out.

Debates are a reusable workflow pattern that doesn't require new adapters.
They compose existing primitives:
  - Personas (system prompts)
  - Fan-out dispatch (parallel execution)
  - Synthesis (final aggregation)

A debate workflow:
  1. Frames the topic
  2. Fans out one dispatch per persona (each with a perspective system prompt)
  3. Synthesizes all responses into consensus, disagreements, and action items
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from .workflow import WorkflowSpec, WorkflowResult, run_workflow_parallel
from .debate_metrics import DebateMetricsCollector
from .fan_out import aggregate_fan_out_results


@dataclass(frozen=True)
class PersonaDefinition:
    """One perspective in a debate.

    Parameters
    ----------
    name:
        Human-readable persona name (e.g., "Pragmatist").
    perspective:
        System prompt describing this persona's viewpoint and approach.
        Example: "You are a pragmatist. Focus on what ships fastest."
    """

    name: str
    perspective: str


@dataclass(frozen=True)
class DebateConfig:
    """Configuration for a debate workflow.

    Parameters
    ----------
    topic:
        The debate topic or question.
    personas:
        List of PersonaDefinition objects (each is a viewpoint to explore).
    rounds:
        Number of debate rounds (default 1).
    synthesis_prompt:
        Optional custom system prompt for the synthesis step.
        If None, uses a default synthesis prompt.
    tier:
        Routing tier for all dispatches (default "mid").
    max_tokens:
        Token budget for debate and synthesis steps (default 4096).
    temperature:
        LLM temperature (default 0.7, higher for exploration).
    """

    topic: str
    personas: list[PersonaDefinition]
    rounds: int = 1
    synthesis_prompt: str | None = None
    tier: str = "mid"
    max_tokens: int = 4096
    temperature: float = 0.7


def default_personas() -> list[PersonaDefinition]:
    """Return a curated default set of debate personas.

    Returns
    -------
    list of PersonaDefinition
        Four complementary perspectives for well-rounded debates:
        - Pragmatist: execution-focused
        - Skeptic: failure-focused
        - Innovator: novelty-focused
        - Operator: sustainability-focused
    """
    return [
        PersonaDefinition(
            name="Pragmatist",
            perspective=(
                "You are a pragmatist. Your goal is to identify what ships fastest "
                "with minimal risk. Focus on concrete, achievable steps. Call out "
                "impractical ideas. Favor proven approaches over novel ones."
            ),
        ),
        PersonaDefinition(
            name="Skeptic",
            perspective=(
                "You are a skeptic. Your job is to challenge assumptions and find "
                "failure modes. Identify what could go wrong. Point out blind spots. "
                "Be intellectually rigorous and unafraid to say 'this won't work.'"
            ),
        ),
        PersonaDefinition(
            name="Innovator",
            perspective=(
                "You are an innovator. Push for novel approaches and fresh thinking. "
                "Challenge the status quo. Identify opportunities to do something "
                "fundamentally better. Be ambitious and creative."
            ),
        ),
        PersonaDefinition(
            name="Operator",
            perspective=(
                "You are an operator. Think about maintainability, scalability, and "
                "operational burden. What can the team sustainably build and run? "
                "Focus on long-term health, not just short-term wins."
            ),
        ),
    ]


@dataclass(frozen=True)
class DebateResult:
    """Result of a debate workflow.

    Attributes
    ----------
    status:
        "succeeded" or "failed".
    topic:
        The debate topic.
    persona_responses:
        Dict mapping persona name -> their response text.
    synthesis:
        The synthesis response (consensus, disagreements, action items).
    persona_results:
        Full WorkflowResult objects for each persona (for detailed inspection).
    metrics:
        Debate quality metrics for rounds and synthesis.
    """

    status: str
    topic: str
    persona_responses: dict[str, str]
    synthesis: str
    persona_results: list[WorkflowResult]
    metrics: dict | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dict."""
        return {
            "status": self.status,
            "topic": self.topic,
            "persona_responses": self.persona_responses,
            "synthesis": self.synthesis,
            "persona_count": len(self.persona_responses),
            "metrics": self.metrics,
        }


def run_debate(
    config: DebateConfig,
    *,
    metrics_conn: Any | None = None,
) -> DebateResult:
    """Execute a full debate workflow.

    Orchestrates a multi-round debate:
      1. Frame the topic (optional, skipped if only 1 round)
      2. For each round:
         a. Fan out: one dispatch per persona (with perspective system prompt)
         b. Aggregate responses
      3. Synthesize: final dispatch that reads all responses and produces consensus

    Parameters
    ----------
    config:
        DebateConfig with topic, personas, and synthesis settings.

    Returns
    -------
    DebateResult
        Debate outputs: persona responses and synthesis.
    """

    if not config.personas:
        return DebateResult(
            status="failed",
            topic=config.topic,
            persona_responses={},
            synthesis="No personas defined for debate",
            persona_results=[],
        )

    all_persona_results: list[WorkflowResult] = []
    persona_responses: dict[str, str] = {}
    collector = DebateMetricsCollector(conn=metrics_conn)
    debate_id = getattr(config, "debate_id", None) or config.topic[:40]

    # --- Debate rounds ---
    for round_num in range(config.rounds):
        round_label = f"round_{round_num + 1}"

        # Create one spec per persona
        specs: list[WorkflowSpec] = []
        persona_names: list[str] = []

        for persona in config.personas:
            persona_names.append(persona.name)

            # Debate prompt
            debate_prompt = (
                f"Topic: {config.topic}\n\n"
                f"Respond from the perspective of: {persona.name}\n\n"
                f"Provide a detailed, substantive response. Be specific. "
                f"Take a strong position. Identify concrete implications or next steps."
            )

            specs.append(
                WorkflowSpec(
                    prompt=debate_prompt,
                    system_prompt=persona.perspective,
                    tier=config.tier,
                    max_tokens=config.max_tokens,
                    temperature=config.temperature,
                    label=f"debate_{round_label}_{persona.name.lower()}",
                )
            )

        # Fan out all personas in parallel
        results = run_workflow_parallel(specs, max_workers=len(specs))
        all_persona_results.extend(results)
        round_results = dict(zip(persona_names, results))

        for persona_position, (persona_name, result) in enumerate(round_results.items()):
            collector.record_round(
                debate_id=debate_id,
                persona=persona_name,
                text=result.completion or "(no response)",
                duration_seconds=(
                    getattr(result, "latency_ms", 0) / 1000.0
                    if getattr(result, "latency_ms", None) is not None
                    else 0.0
                ),
                round_number=round_num + 1,
                persona_position=persona_position,
            )

        # Collect responses
        for persona, result in zip(persona_names, results):
            completion = result.completion or "(no response)"
            # On subsequent rounds, append to previous responses
            if persona in persona_responses:
                persona_responses[persona] += f"\n\n--- Round {round_num + 1} ---\n{completion}"
            else:
                persona_responses[persona] = completion

    # --- Synthesis ---
    synthesis_system_prompt = (
        config.synthesis_prompt
        or (
            "You are a synthesis agent. Your job is to read multiple perspectives "
            "on a topic and produce a balanced summary. Identify: (1) what survived "
            "across all perspectives (consensus), (2) key disagreements and why, "
            "(3) the strongest first move forward."
        )
    )

    persona_summary = "\n\n".join(
        f"**{name}**:\n{response}" for name, response in persona_responses.items()
    )

    synthesis_prompt = (
        f"Topic: {config.topic}\n\n"
        f"Debate responses from {len(persona_responses)} perspectives:\n\n"
        f"{persona_summary}\n\n"
        f"Synthesize these perspectives. What's the consensus? Where do they disagree? "
        f"What's the strongest first move?"
    )

    synthesis_result = run_workflow_parallel(
        [
            WorkflowSpec(
                prompt=synthesis_prompt,
                system_prompt=synthesis_system_prompt,
                tier=config.tier,
                max_tokens=config.max_tokens,
                temperature=0.3,  # Lower temperature for synthesis
                label="debate_synthesis",
            )
        ],
        max_workers=1,
    )[0]
    synthesis_text = synthesis_result.completion or "(synthesis failed)"
    collector.record_synthesis(
        debate_id=debate_id,
        consensus_points=[],
        disagreements=[],
        synthesis_text=synthesis_text,
    )
    round_metrics, synthesis_metrics = collector.get_debate(debate_id)

    # Determine overall status
    all_succeeded = all(r.status == "succeeded" for r in all_persona_results)
    synthesis_succeeded = synthesis_result.status == "succeeded"
    overall_status = "succeeded" if (all_succeeded and synthesis_succeeded) else "failed"

    result = DebateResult(
        status=overall_status,
        topic=config.topic,
        persona_responses=persona_responses,
        synthesis=synthesis_text,
        persona_results=all_persona_results + [synthesis_result],
        metrics={
            "rounds": [asdict(m) for m in round_metrics],
            "synthesis": asdict(synthesis_metrics) if synthesis_metrics else None,
        },
    )
    try:
        return result
    finally:
        collector.flush(metrics_conn)


__all__ = [
    "DebateConfig",
    "DebateResult",
    "PersonaDefinition",
    "default_personas",
    "run_debate",
]
