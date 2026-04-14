"""Tools: praxis_research_workflow."""
from __future__ import annotations

import json
import re
from typing import Any

from ..subsystems import REPO_ROOT


_SPECS_DIR = REPO_ROOT / "config" / "specs"
_DEFAULT_WORKER_AGENT = "deepseek/deepseek-r3"
_DEFAULT_WORKER_COUNT = 40


def _slugify(topic: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", topic.strip()).strip("_").lower()
    if len(slug) > 60:
        slug = slug[:60].rstrip("_")
    return slug


def _build_spec(topic: str, slug: str, workers: int, agent: str, threshold: int | None = None) -> dict[str, Any]:
    """Build a research fan-out workflow spec programmatically."""
    synth_job: dict[str, Any] = {
        "label": "synthesize",
        "agent": "auto/reasoning",
        "depends_on": ["workers"],
        "prompt": _SYNTHESIS_PROMPT.replace("{{RESEARCH_TOPIC}}", topic),
    }
    if threshold is not None:
        synth_job["dependency_threshold"] = threshold
    return {
        "name": f"Research: {topic}",
        "phase": "research",
        "outcome_goal": f"Produce a cited, multi-angle synthesis on: {topic}",
        "target_repo": str(REPO_ROOT),
        "workdir": str(REPO_ROOT),
        "anti_requirements": [
            "Do not fabricate sources -- every claim must be traceable to a search result or document",
            "Do not write files to disk -- submit all outputs via praxis_submit_research_result",
            "Do not overlap worker scopes -- each worker investigates only its assigned sub-question",
            "Do not produce a final synthesis in a worker job -- that is the synthesize job's responsibility",
        ],
        "jobs": [
            {
                "label": "seed_research",
                "agent": "auto/architecture",
                "prompt": _SEED_PROMPT.replace("{{RESEARCH_TOPIC}}", topic).replace("{{WORKER_COUNT}}", str(workers)),
            },
            {
                "label": "workers",
                "agent": agent,
                "depends_on": ["seed_research"],
                "replicate": workers,
                "prompt": _WORKER_PROMPT.replace("{{RESEARCH_TOPIC}}", topic),
            },
            synth_job,
        ],
    }


def _launch_workflow(spec_path: str) -> dict[str, Any]:
    from surfaces.mcp.tools.workflow import tool_praxis_workflow
    return tool_praxis_workflow({"action": "run", "spec_path": spec_path, "wait": False})


def tool_praxis_research_workflow(params: dict) -> dict:
    """Launch parallel research workflows or query past results."""
    action = params.get("action", "run")

    if action == "run":
        topic = (params.get("topic") or "").strip()
        if not topic:
            return {"error": "topic is required for action='run'"}
        slug = params.get("slug") or _slugify(topic)
        workers = int(params.get("workers", _DEFAULT_WORKER_COUNT))
        agent = params.get("agent") or _DEFAULT_WORKER_AGENT
        threshold = params.get("threshold")
        if threshold is not None:
            threshold = int(threshold)

        spec = _build_spec(topic, slug, workers, agent, threshold=threshold)
        launch_path = _SPECS_DIR / f"research_{slug}.queue.json"
        launch_path.write_text(json.dumps(spec, indent=2), encoding="utf-8")

        run_result = _launch_workflow(str(launch_path))

        return {
            "action": "run",
            "topic": topic,
            "slug": slug,
            "workers": workers,
            "agent": agent,
            "total_jobs": workers + 2,
            "workflow": run_result,
        }

    if action == "list":
        from surfaces.mcp.tools.workflow import tool_praxis_workflow
        all_runs = tool_praxis_workflow({"action": "list"})
        research_runs = [
            r for r in all_runs.get("runs", [])
            if isinstance(r.get("spec_name"), str) and r["spec_name"].startswith("Research:")
        ]
        return {"research_runs": research_runs, "count": len(research_runs)}

    return {"error": f"Unknown action: {action}. Use 'run' or 'list'."}


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_SEED_PROMPT = """\
You are the lead research strategist for a multi-angle investigation.

RESEARCH TOPIC: {{RESEARCH_TOPIC}}

Your job is to decompose this topic into exactly {{WORKER_COUNT}} distinct, non-overlapping research angles. Each angle should explore a different facet so that together they provide comprehensive coverage.

Produce a structured research plan with:

1. OVERVIEW: One paragraph framing the research question and why multiple angles matter.

2. WORKER BRIEFS (exactly {{WORKER_COUNT}}):
   For each worker, provide:
   - worker_index: 1 through {{WORKER_COUNT}}
   - sub_question: The specific question this worker must answer
   - search_strategy: 3-5 specific search queries to try
   - scope_boundary: What this worker should NOT cover (to avoid overlap)
   - expected_output: What a good answer looks like

3. SYNTHESIS CONTRACT:
   - What the final output should contain
   - Known tensions or contradictions to watch for across angles
   - Quality bar: what makes a finding "cited" vs "speculative"

Format the worker briefs as a JSON array inside a ```json fence so downstream workers can parse them cleanly.

Submit your research plan using praxis_submit_research_result. Your summary should be the full plan including all worker briefs and the synthesis contract.

Do NOT write any files to disk. Your submission is the output."""

_WORKER_PROMPT = """\
You are research worker {{WORKER_INDEX}} of {{WORKER_COUNT}} in a parallel investigation.

RESEARCH TOPIC: {{RESEARCH_TOPIC}}

The seed research plan from the previous job is in your context above. Find the worker brief for worker_index={{WORKER_INDEX}} and follow it exactly.

YOUR PROCESS:
1. Read your assigned sub_question and search_strategy from the seed plan
2. Execute the suggested search queries using web search
3. For EACH finding, record:
   - The claim or fact discovered
   - The source URL or document
   - Your confidence level (high / medium / low)
   - Any caveats or contradictions found
4. Stay within your scope_boundary -- do NOT research areas assigned to other workers
5. Note any open questions that the synthesis step should address

OUTPUT FORMAT:
Submit using praxis_submit_research_result with a summary structured as:

  ## Worker {{WORKER_INDEX}} Findings

  ### Sub-question
  [Your assigned sub-question]

  ### Key Findings
  1. [Finding] -- Source: [URL/doc] -- Confidence: [high/medium/low]
  2. [Finding] -- Source: [URL/doc] -- Confidence: [high/medium/low]
  ...

  ### Contradictions or Surprises
  - [anything that conflicts with expected answers]

  ### Open Questions for Synthesis
  - [questions the synthesis step should resolve]

Do NOT write any files to disk. Do NOT attempt to write the final synthesis. Your submission is the output."""

_SYNTHESIS_PROMPT = """\
You are the synthesis lead for a multi-angle research investigation.

RESEARCH TOPIC: {{RESEARCH_TOPIC}}

The seed research plan and all worker findings are in your context above. Your job is to reconcile them into a single, authoritative synthesis.

YOUR PROCESS:
1. Read all worker submissions and the original seed plan's synthesis contract
2. Map each worker's findings to the synthesis contract's requirements
3. Identify and resolve contradictions between workers:
   - Where workers agree, note the convergence and combined confidence
   - Where workers disagree, explain the tension and state which evidence is stronger
4. Flag any gaps -- questions from the synthesis contract that no worker adequately answered
5. Produce a final cited synthesis

OUTPUT FORMAT:
Submit using praxis_submit_research_result with a summary structured as:

  ## Research Synthesis: {{RESEARCH_TOPIC}}

  ### Executive Summary
  [2-3 paragraph synthesis of the most important findings]

  ### Detailed Findings
  [Organized by theme/sub-question, with citations to worker findings and their sources]

  ### Confidence Assessment
  - High confidence: [findings with strong multi-source agreement]
  - Medium confidence: [findings with partial evidence]
  - Low confidence / Speculative: [findings that need more evidence]

  ### Contradictions Resolved
  [How conflicts between workers were adjudicated]

  ### Remaining Gaps
  [What the research did NOT answer and suggested follow-up]

  ### Source Index
  [All unique sources cited across all workers]

Do NOT write any files to disk. Your submission is the output."""


TOOLS: dict[str, tuple[callable, dict[str, Any]]] = {
    "praxis_research_workflow": (
        tool_praxis_research_workflow,
        {
            "description": (
                "Run a parallel multi-angle research workflow on any topic. "
                "One call generates a workflow spec (seed decomposition, N parallel "
                "research workers via replicate, synthesis) and launches it through the service bus.\n\n"
                "USE WHEN: you want deep, cited research on a topic from multiple angles. "
                "The workflow fans out parallel workers (default 40 DeepSeek via OpenRouter) "
                "and synthesizes their findings.\n\n"
                "EXAMPLES:\n"
                "  Run research:  praxis_research_workflow(action='run', topic='AI agent architecture patterns')\n"
                "  Custom count:  praxis_research_workflow(action='run', topic='...', workers=20)\n"
                "  List past runs: praxis_research_workflow(action='list')\n\n"
                "OUTPUT: All results flow through the service bus via praxis_submit_research_result. "
                "No files are written to disk. Use praxis_workflow action='status' to poll progress.\n\n"
                "DO NOT USE: for knowledge graph search (use praxis_research), "
                "or for single-shot research questions (use praxis_query)."
            ),
            "inputSchema": {
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["run", "list"],
                        "default": "run",
                        "description": (
                            "Operation: 'run' (generate spec + launch workflow), "
                            "'list' (show recent research workflow runs)."
                        ),
                    },
                    "topic": {
                        "type": "string",
                        "description": (
                            "The research topic or question to investigate. "
                            "Required for 'run'."
                        ),
                    },
                    "slug": {
                        "type": "string",
                        "description": (
                            "Lowercase identifier for the research run. Auto-derived from "
                            "topic if not provided."
                        ),
                    },
                    "workers": {
                        "type": "number",
                        "default": 40,
                        "description": (
                            "Number of parallel fan-out workers (default 40). "
                            "Each worker investigates one angle of the research topic."
                        ),
                    },
                    "agent": {
                        "type": "string",
                        "default": "deepseek/deepseek-r3",
                        "description": (
                            "Model slug for worker jobs. Default: deepseek/deepseek-r3 (low tier)."
                        ),
                    },
                    "threshold": {
                        "type": "number",
                        "description": (
                            "Minimum successful workers required before synthesis starts. "
                            "If not set, all workers must succeed. Set to e.g. 30 to allow "
                            "synthesis after 30 of 40 workers complete successfully."
                        ),
                    },
                },
            },
        },
    ),
}
