"""LLM-powered app manifest generator.

Takes a user intent + registry matches and returns an app manifest JSON
that the React shell can render. Uses claude CLI for LLM calls.
"""

from __future__ import annotations

import json
import os
import subprocess
import uuid
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from runtime.intent_matcher import MatchResult
    from storage.postgres import SyncPostgresConnection

from runtime.block_catalog import block_ids, format_block_catalog_for_prompt
from runtime.helm_manifest import normalize_helm_bundle, validate_helm_bundle


# ---------------------------------------------------------------------------
# Domain model
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class GeneratedManifest:
    """Result of an LLM manifest generation or refinement."""

    manifest_id: str
    manifest: dict[str, Any]
    version: int
    confidence: float
    explanation: str
    changelog: str = ""
    object_types: tuple[dict[str, Any], ...] = ()


# ---------------------------------------------------------------------------
# Manifest JSON schema (included in LLM prompts)
# ---------------------------------------------------------------------------

_MANIFEST_SCHEMA = """\
{
  "version": 4,
  "kind": "helm_surface_bundle",
  "title": "Workspace title",
  "default_tab_id": "main",
  "tabs": [
    {
      "id": "main",
      "label": "Overview",
      "surface_id": "main",
      "source_option_ids": ["web_search"]
    }
  ],
  "surfaces": {
    "main": {
      "id": "main",
      "title": "Overview",
      "kind": "quadrant_manifest",
      "manifest": {
        "version": 2,
        "grid": "4x4",
        "quadrants": {
          "A1": {
            "module": "<block_id>",
            "span": 1,
            "config": { "...": "..." }
          }
        }
      }
    }
  },
  "source_options": {
    "web_search": {
      "id": "web_search",
      "label": "Web Search",
      "family": "external",
      "kind": "web_search",
      "availability": "ready",
      "activation": "open",
      "description": "Look up current public information when local state is not enough."
    }
  }
}"""

_DEFAULT_MANIFEST_GENERATE_ROUTE = "auto/planner"
_DEFAULT_MANIFEST_REFINE_ROUTE = "auto/medium"
_DEFAULT_SOURCE_OPTIONS = (
    {
        "id": "web_search",
        "label": "Web Search",
        "family": "external",
        "kind": "web_search",
        "availability": "ready",
        "activation": "open",
        "description": "Search current public information when local context is not enough.",
    },
    {
        "id": "external_api",
        "label": "External API",
        "family": "external",
        "kind": "api",
        "availability": "setup_required",
        "activation": "configure",
        "setup_intent": "Set up an external API source for this workspace.",
        "description": "Connect a new API before the workspace can query it.",
    },
    {
        "id": "third_party_dataset",
        "label": "Third-Party Dataset",
        "family": "external",
        "kind": "dataset",
        "availability": "setup_required",
        "activation": "configure",
        "setup_intent": "Set up a third-party dataset source for this workspace.",
        "description": "Attach a dataset feed or import before using it in the workspace.",
    },
)


# ---------------------------------------------------------------------------
# Prompt builders
# ---------------------------------------------------------------------------

def _format_ui_components(matches: "MatchResult") -> str:
    parts: list[str] = []
    for m in matches.ui_components:
        props = m.metadata.get("props_schema", {})
        events = m.metadata.get("emits_events", [])
        parts.append(
            f"- {m.name} (id={m.id}, category={m.category})\n"
            f"  props_schema: {json.dumps(props, indent=2)}\n"
            f"  emits_events: {json.dumps(events)}"
        )
    return "\n".join(parts) if parts else "(none matched)"


def _format_calculations(matches: "MatchResult") -> str:
    parts: list[str] = []
    for m in matches.calculations:
        inp = m.metadata.get("input_schema", {})
        out = m.metadata.get("output_schema", {})
        parts.append(
            f"- {m.name} (id={m.id}, category={m.category})\n"
            f"  input_schema: {json.dumps(inp, indent=2)}\n"
            f"  output_schema: {json.dumps(out, indent=2)}"
        )
    return "\n".join(parts) if parts else "(none matched)"


def _format_workflows(matches: "MatchResult") -> str:
    parts: list[str] = []
    for m in matches.workflows:
        inp = m.metadata.get("input_schema", {})
        trigger = m.metadata.get("trigger_type", "manual")
        parts.append(
            f"- {m.name} (id={m.id}, category={m.category}, trigger={trigger})\n"
            f"  input_schema: {json.dumps(inp, indent=2)}"
        )
    return "\n".join(parts) if parts else "(none matched)"


def _build_generate_prompt(intent: str, matches: "MatchResult") -> str:
    return f"""\
You are a Helm workspace manifest generator. Given a user intent and available blocks,
produce a JSON bundle that a React shell can render as one or more tabs.

## User Intent
{intent}

## Available UI Components
{_format_ui_components(matches)}

## Available Calculations
{_format_calculations(matches)}

## Available Workflows
{_format_workflows(matches)}

## Registered Block IDs
{format_block_catalog_for_prompt()}

## Example Configs for Common Blocks
- data-table: {{"objectType": "contact", "publishSelection": "contact", "columns": []}}
- metric: {{"label": "...", "value": "...", "color": "#..."}}
- chart: {{"endpoint": "...", "path": "...", "type": "bar", "xKey": "...", "yKey": "..."}}
- search-panel: {{"objectType": "...", "placeholder": "..."}}
- button-row: {{"actions": [{{"label": "...", "variant": "...", "createObject": {{"typeId": "...", "defaults": {{}}}}}}]}}
- activity-feed: {{"title": "..."}}
- key-value: {{"subscribeSelection": "...", "title": "..."}}

## Source Option Candidates
{json.dumps(_DEFAULT_SOURCE_OPTIONS, indent=2)}

## Manifest JSON Schema (V4)
{_MANIFEST_SCHEMA}

## Instructions
1. Choose the most relevant blocks, calculations, and workflows for the intent.
2. Output a V4 Helm bundle JSON using the schema above.
3. Surface manifests still use V2 quadrants internally, but the top-level bundle must be version 4.
4. Source options must stay compact, clickable, and optional. They should never carry the main explanation of the app.
5. If the intent requires creating new data types, you can also output an optional 'object_types' array alongside the manifest.

## Output Format
Return ONLY a JSON object with exactly these keys:
- "manifest": the V4 Helm bundle JSON following the schema above
- "object_types": (optional) array of new object types to create (e.g. [{{"type_id": "contact", "name": "Contact", "schema": {{}} }}])
- "explanation": a brief explanation of your design decisions

No markdown fences, no extra text — just the JSON object."""


def _build_refine_prompt(
    intent: str, current_manifest: dict, feedback: str,
) -> str:
    return f"""\
You are a Helm workspace manifest refiner. Update the bundle based on user feedback.

## Original Intent
{intent}

## Current Manifest
{json.dumps(current_manifest, indent=2)}

## User Feedback
{feedback}

## Registered Block IDs
{format_block_catalog_for_prompt()}

## Source Option Candidates
{json.dumps(_DEFAULT_SOURCE_OPTIONS, indent=2)}

## Manifest JSON Schema (V4)
{_MANIFEST_SCHEMA}

## Instructions
Apply the user's feedback to produce an improved V4 bundle.
Keep changes minimal — only modify what the feedback requests.
You can optionally output an 'object_types' array if new types are needed.

## Output Format
Return ONLY a JSON object with these keys:
- "manifest": the updated V4 bundle JSON
- "object_types": (optional) array of new object types
- "explanation": what you changed and why

No markdown fences, no extra text — just the JSON object."""


# ---------------------------------------------------------------------------
# LLM caller
# ---------------------------------------------------------------------------

def _manifest_refine_agent_route() -> str:
    raw = os.environ.get("WORKFLOW_REFINE_AGENT_ROUTE", "").strip()
    return raw or _DEFAULT_MANIFEST_REFINE_ROUTE


def _call_llm(prompt: str, conn=None, *, route_slug: str = _DEFAULT_MANIFEST_GENERATE_ROUTE) -> str:
    """Call the planner CLI via the routing table and return raw result text.

    Uses cli_config from provider_model_candidates — no hardcoded models.
    """
    import json as _json

    env = {**os.environ}
    for k in list(env):
        if k.upper().startswith("CLAUDE") and k.upper() != "CLAUDE_CONFIG_DIR":
            del env[k]

    if conn is None:
        raise RuntimeError(
            "manifest_generator.route_authority_unavailable: "
            "manifest LLM calls require a DB-backed route authority"
        )

    from runtime.task_type_router import TaskTypeRouter

    router = TaskTypeRouter(conn)
    chain = router.resolve_failover_chain(route_slug)
    if not chain:
        raise RuntimeError(
            "manifest_generator.route_chain_empty: "
            f"no provider route tiers resolved for {route_slug}"
        )

    tier_failures: list[str] = []

    for tier in chain:
        provider = tier.provider_slug
        model_slug = tier.model_slug

        # Read CLI config from registry
        cli_config = _get_cli_config_static(conn, provider, model_slug)
        if not cli_config or not cli_config.get("cmd_template"):
            tier_failures.append(f"{provider}/{model_slug}: missing cli_config.cmd_template")
            continue

        cmd = [
            s.replace("{model}", model_slug).replace("{prompt}", prompt)
            for s in cli_config["cmd_template"]
        ]
        envelope_key = cli_config.get("envelope_key")

        try:
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=90, env=env,
            )
            if result.returncode == 0 and result.stdout.strip():
                raw = result.stdout
                if envelope_key:
                    try:
                        envelope = _json.loads(raw)
                        raw = envelope.get(envelope_key, raw)
                    except _json.JSONDecodeError:
                        pass
                return raw.strip()
            tier_failures.append(
                f"{provider}/{model_slug}: rc={result.returncode} stderr={result.stderr.strip()[:500]}"
            )
        except Exception as exc:
            tier_failures.append(f"{provider}/{model_slug}: {type(exc).__name__}: {exc}")

    detail = "; ".join(tier_failures) if tier_failures else "no routed tiers attempted"
    raise RuntimeError(
        "manifest_generator.route_chain_failed: "
        f"all provider route tiers failed for {route_slug}: {detail}"
    )


def _get_cli_config_static(conn, provider_slug: str, model_slug: str):
    """Look up CLI config from provider_model_candidates."""
    import json as _json
    rows = conn.execute(
        """SELECT cli_config FROM provider_model_candidates
           WHERE provider_slug = $1 AND model_slug = $2
             AND status = 'active' AND cli_config != '{}'::jsonb
           LIMIT 1""",
        provider_slug, model_slug,
    )
    if rows:
        cfg = rows[0]["cli_config"]
        if isinstance(cfg, str):
            cfg = _json.loads(cfg)
        if cfg.get("cmd_template"):
            return cfg
    rows = conn.execute(
        """SELECT cli_config FROM provider_model_candidates
           WHERE provider_slug = $1 AND status = 'active'
             AND cli_config != '{}'::jsonb
           LIMIT 1""",
        provider_slug,
    )
    if rows:
        cfg = rows[0]["cli_config"]
        if isinstance(cfg, str):
            cfg = _json.loads(cfg)
        if cfg.get("cmd_template"):
            return cfg
    return None


def _parse_llm_response(raw: str) -> tuple[dict, list[dict], str]:
    """Extract manifest dict, object_types, and explanation from LLM JSON response."""
    # Strip markdown fences if present
    text = raw.strip()
    if text.startswith("```"):
        first_nl = text.find("\n")
        text = text[first_nl + 1:] if first_nl != -1 else text
    if text.endswith("```"):
        text = text[: text.rfind("```")]
    text = text.strip()

    parsed = json.loads(text)
    manifest = parsed.get("manifest", parsed)
    object_types = parsed.get("object_types", [])
    explanation = parsed.get("explanation", "")
    return manifest, object_types, explanation


def _validate_manifest(manifest: dict) -> None:
    validate_helm_bundle(manifest, valid_block_ids=set(block_ids()))


def _normalize_object_types(value: Any) -> tuple[dict[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    normalized: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, dict):
            normalized.append(dict(item))
    return tuple(normalized)


# ---------------------------------------------------------------------------
# ManifestGenerator
# ---------------------------------------------------------------------------

class ManifestGenerator:
    """Generate and refine app manifests via LLM."""

    def __init__(self, conn: "SyncPostgresConnection") -> None:
        self._conn = conn

    # -- Public API ---------------------------------------------------------

    def generate(self, intent: str, match_result: "MatchResult") -> GeneratedManifest:
        """Generate a new manifest from intent + registry matches."""
        prompt = _build_generate_prompt(intent, match_result)
        raw = _call_llm(prompt, conn=self._conn)
        manifest, object_types, explanation = _parse_llm_response(raw)
        manifest = normalize_helm_bundle(manifest, name=f"Generated: {intent[:80]}", description=explanation[:500])
        _validate_manifest(manifest)

        manifest_id = uuid.uuid4().hex[:12]

        # Estimate confidence from match coverage
        confidence = round(min(match_result.coverage_score + 0.2, 1.0), 4)

        return GeneratedManifest(
            manifest_id=manifest_id,
            manifest=manifest,
            version=4,
            confidence=confidence,
            explanation=explanation,
            object_types=_normalize_object_types(object_types),
        )

    def refine(self, manifest_id: str, instruction: str) -> GeneratedManifest:
        """Refine an existing manifest based on user feedback."""
        # Load current manifest JSON
        row = self._conn.fetchrow(
            "SELECT id, manifest, version FROM app_manifests WHERE id = $1",
            manifest_id,
        )
        if row is None:
            raise ValueError(f"Manifest not found: {manifest_id}")

        current_manifest = row["manifest"]
        if isinstance(current_manifest, str):
            current_manifest = json.loads(current_manifest)

        current_version = row["version"]

        # Build prompt
        current_manifest = normalize_helm_bundle(current_manifest, manifest_id=manifest_id)
        prompt = _build_refine_prompt("", current_manifest, instruction)

        # Call Claude CLI, parse JSON response
        raw = _call_llm(
            prompt,
            conn=self._conn,
            route_slug=_manifest_refine_agent_route(),
        )
        new_manifest_raw, object_types, explanation = _parse_llm_response(raw)
        new_manifest = normalize_helm_bundle(new_manifest_raw, manifest_id=manifest_id)
        _validate_manifest(new_manifest)

        new_version = current_version + 1
        changelog = f"Refined manifest based on instruction: {instruction[:200]}"
        if explanation:
            changelog = explanation

        return GeneratedManifest(
            manifest_id=manifest_id,
            manifest=new_manifest,
            version=new_version,
            confidence=1.0,
            explanation=changelog,
            changelog=changelog,
            object_types=_normalize_object_types(object_types),
        )

    def get(self, manifest_id: str) -> GeneratedManifest | None:
        """Load a manifest by ID."""
        row = self._conn.fetchrow(
            "SELECT id, manifest, version, description FROM app_manifests WHERE id = $1",
            manifest_id,
        )
        if row is None:
            return None

        manifest = row["manifest"]
        if isinstance(manifest, str):
            manifest = json.loads(manifest)
        manifest = normalize_helm_bundle(manifest, manifest_id=row["id"])

        return GeneratedManifest(
            manifest_id=row["id"],
            manifest=manifest,
            version=row["version"],
            confidence=1.0,
            explanation=row["description"] or "",
            object_types=(),
        )
