"""Task-driven app assembly with fan-out dispatch.

Flow:
1. Vector pre-suggest: find matching integrations, modules, templates
2. Planner (Opus): validate wiring, define object types, approve layout
3. Fan-out (fast models): each writes one piece to Postgres in parallel
4. Return manifest_id for immediate render
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Optional, TYPE_CHECKING

from runtime.support_ticket_drafts import (
    draft_ticket_responses,
    looks_like_ticket_drafting_task,
)
from storage.postgres.object_lifecycle_repository import (
    create_object_record,
    ensure_object_type_record,
)
from storage.postgres.workflow_runtime_repository import (
    create_app_manifest,
    upsert_app_manifest,
)

logger = logging.getLogger(__name__)


if TYPE_CHECKING:
    from storage.postgres import SyncPostgresConnection
    from runtime.embedding_service import EmbeddingService

from storage.postgres.vector_store import PostgresVectorStore, VectorFilter


@dataclass(frozen=True)
class PreSuggestion:
    """Vector-computed suggestions before planner call."""
    integrations: list[dict] = field(default_factory=list)
    modules: list[dict] = field(default_factory=list)
    templates: list[dict] = field(default_factory=list)
    best_template_score: float = 0.0
    best_integration_score: float = 0.0


@dataclass(frozen=True)
class AssemblyPlan:
    """Planner output: what to build."""
    task: str
    data_sources: list[dict] = field(default_factory=list)
    object_type: Optional[dict] = None  # {name, description, properties: [{name, type, required}]}
    modules: list[dict] = field(default_factory=list)  # [{module_id, quadrant, span, config}]
    layout: dict = field(default_factory=dict)  # quadrant -> module assignment
    seed_records: list[dict] = field(default_factory=list)
    explanation: str = ""


def _validate_and_fix_model(model: dict) -> dict:
    """Validate and fix an operating model from LLM output.

    Ensures action cards have executors, positions are valid, etc.
    """
    cards = model.get("cards", [])

    for card in cards:
        # Ensure position exists
        if "position" not in card:
            card["position"] = {"col": 0, "row": 0}

        # Ensure action/step cards have executors
        if card.get("kind") in ("action", "step"):
            if "executor" not in card:
                card["executor"] = {"name": "DAG Workflow", "kind": "agent"}
            executor = card["executor"]
            if "kind" not in executor:
                executor["kind"] = "agent"
            if "name" not in executor:
                executor["name"] = "DAG Workflow"
            # Normalize executor kinds
            kind = executor["kind"].lower()
            if kind in ("user", "human"):
                executor["kind"] = "human"
            elif kind in ("tool", "mcp", "app", "application"):
                executor["kind"] = "app"
            elif kind in ("agent", "system", "ai", "llm"):
                executor["kind"] = "agent"

            # Ensure dependencies and toolPermissions exist
            if "dependencies" not in card:
                card["dependencies"] = []
            if "toolPermissions" not in card:
                card["toolPermissions"] = []

    model["cards"] = cards
    return model


class TaskAssembler:
    """Orchestrates task → manifest assembly."""

    def __init__(
        self,
        conn: "SyncPostgresConnection",
        embedder: Optional["EmbeddingService"] = None,
    ) -> None:
        self._conn = conn
        self._embedder = embedder
        self._vector_store = (
            PostgresVectorStore(conn, embedder) if embedder is not None else None
        )

    def assemble(self, task: str) -> dict:
        """Task string → {manifest_id, plan_summary}. The main entry point."""

        # Fast path: deterministic support ticket drafting (no LLM needed)
        if looks_like_ticket_drafting_task(task):
            drafts = draft_ticket_responses(task=task, card={}, upstream_outputs={})
            if drafts:
                return {
                    "manifest_id": None,
                    "plan_summary": f"Drafted {len(drafts)} support ticket response(s) deterministically",
                    "drafts": drafts,
                    "source": "deterministic_support_fallback",
                }

        # Step 1: Vector pre-suggest (instant, local)
        suggestions = self._pre_suggest(task)

        # Step 2: Fast path — skip planner if vector matches are strong enough
        fast_result = self._fast_assemble(task, suggestions)
        if fast_result is not None:
            manifest_id, plan = fast_result
        else:
            # Step 2b: Planner call (smart model, ~5-10s)
            plan = self._call_planner(task, suggestions)
            # Step 3: Execute plan (write to Postgres)
            manifest_id = self._execute_plan(plan)

        return {
            "manifest_id": manifest_id,
            "plan_summary": plan.explanation,
            "data_sources": [d.get("name", d.get("id", "")) for d in plan.data_sources],
            "object_type": plan.object_type.get("name") if plan.object_type else None,
            "module_count": len(plan.modules),
        }

    def assemble_operating_model(self, task: str) -> dict:
        """Task string → operating model JSON via the planner CLI chain."""
        import uuid as _uuid
        import os
        import subprocess
        from runtime.task_type_router import TaskTypeRouter

        suggestions = self._pre_suggest(task)
        prompt = self._build_operating_model_prompt(task, suggestions)

        manifest_id = _uuid.uuid4().hex[:12]
        router = TaskTypeRouter(self._conn)
        chain = router.resolve_failover_chain("auto/planner")

        env = dict(os.environ)
        for k in list(env):
            if k.upper().startswith("CLAUDE") and k.upper() != "CLAUDE_CONFIG_DIR":
                del env[k]

        model = None
        for tier in chain:
            provider = tier.provider_slug
            model_slug = tier.model_slug

            try:
                raw = self._call_provider(provider, model_slug, prompt, env)
                if raw:
                    model = self._parse_model_json(raw)
                    if model:
                        router.record_outcome("planner", provider, model_slug, succeeded=True)
                        logger.info("Planner succeeded via %s/%s", provider, model_slug)
                        break
                router.record_outcome("planner", provider, model_slug, succeeded=False)
                logger.warning("Planner %s/%s returned unparseable output", provider, model_slug)
            except Exception as exc:
                router.record_outcome("planner", provider, model_slug, succeeded=False)
                logger.warning("Planner %s/%s failed: %s", provider, model_slug, exc)

        if model is None:
            raise RuntimeError("All planner tiers failed to generate a valid operating model")

        upsert_app_manifest(
            self._conn,
            manifest_id=manifest_id,
            name=model.get("name", task[:80]),
            description=f"Operating model: {task[:200]}",
            manifest={"version": 3, "type": "operating_model", "model": model},
            version=3,
        )

        return {
            "manifest_id": manifest_id,
            "model": model,
            "format": "operating_model",
        }

    def assemble_app(self, task: str) -> dict:
        """Task string → V2 quadrant manifest via two-phase fan-out.

        Phase 1: CLI planner (3-5s) picks module IDs + grid layout
        Phase 2: Haiku API × N (parallel, 1-2s each) hydrates each quadrant config
        """
        import uuid as _uuid
        import os
        import subprocess
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from runtime.task_type_router import TaskTypeRouter

        suggestions = self._pre_suggest(task)

        # --- Phase 1: Skeleton (CLI planner, fast) ---
        skeleton_prompt = self._build_skeleton_prompt(task, suggestions)

        manifest_id = _uuid.uuid4().hex[:12]
        router = TaskTypeRouter(self._conn)
        chain = router.resolve_failover_chain("auto/planner")

        env = dict(os.environ)
        for k in list(env):
            if k.upper().startswith("CLAUDE") and k.upper() != "CLAUDE_CONFIG_DIR":
                del env[k]

        skeleton = None
        for tier in chain:
            provider = tier.provider_slug
            model_slug = tier.model_slug
            try:
                raw = self._call_provider(provider, model_slug, skeleton_prompt, env)
                if raw:
                    skeleton = self._parse_json(raw)
                    if skeleton and "quadrants" in skeleton:
                        skeleton["version"] = 2
                        skeleton.setdefault("grid", "4x4")
                        router.record_outcome("planner", provider, model_slug, succeeded=True)
                        logger.info("Skeleton planner succeeded via %s/%s", provider, model_slug)
                        break
                    skeleton = None
                router.record_outcome("planner", provider, model_slug, succeeded=False)
            except Exception as exc:
                router.record_outcome("planner", provider, model_slug, succeeded=False)
                logger.warning("Skeleton planner %s/%s failed: %s", provider, model_slug, exc)

        if skeleton is None:
            raise RuntimeError("All planner tiers failed to generate app skeleton")

        # --- Phase 2: Hydrate each quadrant config via Haiku API (parallel) ---
        quadrants = skeleton.get("quadrants", {})
        title = skeleton.get("title", task[:80])

        # Get object type schemas for context
        obj_schemas = {}
        rows = self._conn.execute(
            "SELECT type_id, name, property_definitions FROM object_types LIMIT 10"
        )
        for r in rows:
            obj_schemas[r["type_id"]] = {
                "name": r["name"],
                "properties": r["property_definitions"],
            }

        def hydrate_quadrant(cell_id: str, qdef: dict) -> tuple[str, dict]:
            """Fill in config for one quadrant via Haiku API."""
            module_id = qdef.get("module", "")
            existing_config = qdef.get("config", {})

            # Skip if already has meaningful config
            if existing_config.get("endpoint") or existing_config.get("objectType"):
                return cell_id, qdef

            prompt = (
                f"Configure a '{module_id}' module for '{title}' ({task}).\n"
                f"Object types: {json.dumps(list(obj_schemas.keys()))}\n"
                f"EXACT field names the modules expect:\n"
                f"- data-table: objectType (string), endpoint (string), columns ([{{key,label}}]), publishSelection (string)\n"
                f"- metric: label (string), endpoint (string — use 'bugs' or 'objects?type=task')\n"
                f"- chart: endpoint (string), type ('bar'|'pie'|'line'), label (string)\n"
                f"- search-panel: placeholder (string)\n"
                f"- activity-feed: endpoint (string)\n"
                f"- status-grid: endpoint (string)\n"
                f"- bug-card: (no config needed)\n"
                f"API: GET /api/objects?type={{type_id}}, GET /api/bugs\n"
                f"Return ONLY the config JSON object."
            )

            result = self._call_haiku(prompt)
            config = self._parse_json(result)
            if not config:
                raise RuntimeError(f"Haiku hydration returned invalid config for module={module_id!r} cell={cell_id!r}")
            qdef = dict(qdef)
            qdef["config"] = config
            return cell_id, qdef

        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = {
                pool.submit(hydrate_quadrant, cell_id, qdef): cell_id
                for cell_id, qdef in quadrants.items()
            }
            for future in as_completed(futures, timeout=15):
                cell_id, hydrated = future.result(timeout=10)
                quadrants[cell_id] = hydrated

        skeleton["quadrants"] = quadrants

        upsert_app_manifest(
            self._conn,
            manifest_id=manifest_id,
            name=title,
            description=f"App: {task[:200]}",
            manifest=skeleton,
            version=int(skeleton.get("version", 2) or 2),
        )

        return {
            "manifest_id": manifest_id,
            "format": "app",
        }

    def _build_skeleton_prompt(self, task: str, suggestions: PreSuggestion) -> str:
        """Build a short prompt for the layout skeleton — just modules and positions."""
        tool_names = [intg.get("name", "?") for intg in suggestions.integrations]
        rows = self._conn.execute(
            "SELECT name FROM integration_registry WHERE auth_status = 'connected' LIMIT 6"
        )
        for r in rows:
            if r["name"] not in tool_names:
                tool_names.append(r["name"])

        rows = self._conn.execute("SELECT type_id FROM object_types LIMIT 10")
        obj_types = [r["type_id"] for r in rows]

        tools = ", ".join(tool_names)
        objs = ", ".join(obj_types)

        return f"""Return ONLY JSON. Pick modules and layout for: {task}

Modules: data-table, chart, search-panel, activity-feed, button-row, metric, stat-row, status-grid, key-value, dispatch-form, markdown, workflow-builder, intent-box, dropdown-select, bug-card
Data: {tools}
Objects: {objs}

Grid cells: Letter=ROW (A-D), Number=COLUMN (1-4). A1=top-left, D4=bottom-right.
Span: "COLSxROWS" (e.g. "2x1"=2 columns wide, 1 row tall).

{{"version":2,"grid":"4x4","title":"Short Title","quadrants":{{"A1":{{"module":"search-panel","span":"3x1"}},"A4":{{"module":"metric","span":"1x1"}},"B1":{{"module":"data-table","span":"3x3"}},"B4":{{"module":"activity-feed","span":"1x3"}}}}}}

ONLY JSON."""

    def _build_app_prompt(self, task: str, suggestions: PreSuggestion) -> str:
        """Build prompt for generating a V2 quadrant app manifest."""

        # Get available modules
        rows = self._conn.execute(
            "SELECT id, name, description, category FROM registry_ui_components ORDER BY name"
        )
        modules = [f"{r['id']}: {r['description'][:60]}" for r in rows]

        # Get integrations
        tool_names = [intg.get("name", "?") for intg in suggestions.integrations]
        rows = self._conn.execute(
            "SELECT name FROM integration_registry WHERE auth_status = 'connected' LIMIT 6"
        )
        for r in rows:
            if r["name"] not in tool_names:
                tool_names.append(r["name"])

        # Get object types
        rows = self._conn.execute("SELECT type_id, name FROM object_types LIMIT 10")
        obj_types = [f"{r['type_id']}: {r['name']}" for r in rows]

        tool_list = ", ".join(tool_names)
        obj_list = ", ".join(obj_types)

        return f"""Return ONLY JSON. Build an app for: {task}

EXACT module IDs (use ONLY these): data-table, chart, search-panel, activity-feed, button-row, markdown, metric, stat-row, status-grid, key-value, dispatch-form, workflow-builder, intent-box, text-input, dropdown-select, bug-card, model-card

Data: {tool_list}
Objects: {obj_list}

Grid: 4 cols (A-D), 4 rows (1-4). Cells: A1-D4. Span: "COLSxROWS".
Config each module with endpoint, objectType, columns, label as needed.

{{"version":2,"grid":"4x4","title":"Short Title","quadrants":{{"A1":{{"module":"search-panel","span":"2x1"}},"C1":{{"module":"metric","span":"1x1","config":{{"label":"Open","endpoint":"bugs/count?status=open"}}}},"D1":{{"module":"metric","span":"1x1","config":{{"label":"Critical","endpoint":"bugs/count?severity=P0"}}}},"A2":{{"module":"data-table","span":"4x3","config":{{"objectType":"bug","endpoint":"objects?type=bug"}}}}}}}}

ONLY JSON."""

    @staticmethod
    def _call_provider(provider: str, model_slug: str, prompt: str, env: dict) -> str | None:
        """Call a provider CLI using registry profile — no hardcoded commands."""
        import subprocess
        from adapters.provider_registry import build_command, get_profile

        profile = get_profile(provider)
        if not profile:
            return None

        # Strip nesting blockers so Claude CLI works from dispatch
        clean_env = dict(env)
        for k in ("CLAUDECODE", "CLAUDE_CODE_ENTRY_POINT", "CLAUDE_CODE_ENTRYPOINT"):
            clean_env.pop(k, None)

        cmd = build_command(provider, model_slug)
        prompt_mode = (profile.prompt_mode or "stdin").strip().lower()
        if prompt_mode == "argv":
            cmd.append(prompt)

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=int(profile.default_timeout or 90),
            env=clean_env,
            input=prompt if prompt_mode != "argv" else None,
        )
        if result.returncode != 0 or not result.stdout.strip():
            stderr_snippet = (result.stderr or "")[:200]
            raise RuntimeError(
                f"{provider}/{model_slug} rc={result.returncode}: {stderr_snippet}"
            )
        raw = result.stdout
        # Unwrap output envelope if the profile specifies one
        envelope_key = (profile.output_envelope_key or "").strip()
        if envelope_key:
            try:
                envelope = json.loads(raw)
                if isinstance(envelope, dict) and envelope_key in envelope:
                    raw = str(envelope[envelope_key])
            except (json.JSONDecodeError, ValueError):
                pass  # Not JSON — use raw output as-is
        return raw

    @staticmethod
    def _call_haiku(prompt: str) -> str | None:
        """Run Haiku fan-out work inside the unified sandbox contract."""
        from runtime.workflow.execution_backends import execute_api

        sandbox_provider = "seatbelt_local" if sys.platform == "darwin" else "docker_local"
        transient_config = SimpleNamespace(
            provider="anthropic",
            model="claude-haiku-4-5-20251001",
            max_output_tokens=4096,
            timeout_seconds=90,
            execution_transport="api",
            sandbox_provider=sandbox_provider,
            sandbox_policy=SimpleNamespace(
                network_policy="provider_only",
                workspace_materialization="copy",
                secret_allowlist=("ANTHROPIC_API_KEY",),
            ),
        )
        result = execute_api(
            transient_config,
            prompt,
            workdir=str(Path(__file__).resolve().parents[3]),
        )
        if result.get("status") != "succeeded":
            logger.warning("Haiku sandbox execution failed: %s", result.get("stderr", ""))
            raise RuntimeError(str(result.get("stderr") or result.get("error_code") or "haiku_failed"))
        return str(result.get("stdout") or "")

    def _get_cli_config(self, provider_slug: str, model_slug: str) -> dict | None:
        """Look up CLI invocation config from provider_model_candidates."""
        rows = self._conn.execute(
            """SELECT cli_config FROM provider_model_candidates
               WHERE provider_slug = $1 AND model_slug = $2
                 AND status = 'active' AND cli_config != '{}'::jsonb
               LIMIT 1""",
            provider_slug, model_slug,
        )
        if rows:
            cfg = rows[0]["cli_config"]
            if isinstance(cfg, str):
                cfg = json.loads(cfg)
            if cfg.get("cmd_template"):
                return cfg
        # Fall back to any config for this provider
        rows = self._conn.execute(
            """SELECT cli_config FROM provider_model_candidates
               WHERE provider_slug = $1 AND status = 'active'
                 AND cli_config != '{}'::jsonb
               LIMIT 1""",
            provider_slug,
        )
        if rows:
            cfg = rows[0]["cli_config"]
            if isinstance(cfg, str):
                cfg = json.loads(cfg)
            if cfg.get("cmd_template"):
                return cfg
        return None

    @staticmethod
    def _parse_json(raw: str) -> dict | None:
        """Extract any JSON object from LLM output (strips markdown fences)."""
        text = raw.strip()
        if "```json" in text:
            text = text.split("```json", 1)[1]
            text = text.split("```", 1)[0]
        elif "```" in text:
            text = text.split("```", 1)[1]
            text = text.split("```", 1)[0]
        start = text.find("{")
        if start == -1:
            return None
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        return json.loads(text[start:i + 1])
                    except json.JSONDecodeError:
                        return None
        return None

    def _build_operating_model_prompt(self, task: str, suggestions: PreSuggestion) -> str:
        """Build a concise prompt for executable operating models."""

        # Build short tool list from integrations
        tool_names = []
        for intg in suggestions.integrations:
            tool_names.append(intg.get("name", "unknown"))
        try:
            rows = self._conn.execute(
                "SELECT name FROM integration_registry WHERE auth_status = 'connected' LIMIT 6"
            )
            for r in rows:
                if r["name"] not in tool_names:
                    tool_names.append(r["name"])
        except Exception:
            pass
        tool_names.extend(["Praxis Workflow", "System"])

        tools = ", ".join(tool_names)

        return f"""Return ONLY JSON. Task: {task}
Tools: {tools}
Build 6-8 cards across 4 columns. Col 0-1: parallel data gathering. Col 2-3: process/act on results.
Each action needs executor:{{name,kind:"app"|"agent"}}, dependencies[], toolPermissions[].
Kinds: mission(1), decision(1-2), action(3-4), state_knowledge(1).
Schema: {{"id":"str","name":"str","goal":"str","cards":[{{"kind":"str","id":"str","position":{{"col":N,"row":N}},...}}],"edges":[{{"id":"str","from":"id","to":"id","kind":"proceeds_to|mission_to_decision|decision_to_action|action_to_state"}}]}}
Be specific: not "get data" but "Query open bugs via praxis_bugs". ONLY JSON."""

    def _find_repo_root(self):
        """Find the repo root from the current module location."""
        from pathlib import Path
        return Path(__file__).resolve().parents[3]

    @staticmethod
    def _parse_model_json(raw: str) -> dict | None:
        """Extract OperatingModel JSON from model output (may have markdown fences)."""
        text = raw.strip()
        if "```json" in text:
            text = text.split("```json", 1)[1]
            text = text.split("```", 1)[0]
        elif "```" in text:
            text = text.split("```", 1)[1]
            text = text.split("```", 1)[0]
        start = text.find("{")
        if start == -1:
            return None
        depth = 0
        for i in range(start, len(text)):
            if text[i] == "{":
                depth += 1
            elif text[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        model = json.loads(text[start:i + 1])
                        if "cards" in model and "edges" in model:
                            model = _validate_and_fix_model(model)
                            return model
                    except json.JSONDecodeError:
                        pass
                    return None
        return None

    # ------------------------------------------------------------------
    # Step 0.5: Model validation
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Step 1: Vector pre-suggest
    # ------------------------------------------------------------------

    def _pre_suggest(self, task: str) -> PreSuggestion:
        """Fast local vector search across integrations, modules, templates."""
        integrations = []
        modules = []
        templates = []
        best_template_score = 0.0
        best_integration_score = 0.0

        if self._vector_store is not None:
            vector_query = self._vector_store.prepare(task)

            # Search integrations (with similarity score)
            try:
                rows = vector_query.search(
                    "integration_registry",
                    select_columns=("id", "name", "description", "capabilities", "icon"),
                    limit=3,
                    min_similarity=None,
                    score_alias="score",
                )
                integrations = rows
                if integrations:
                    best_integration_score = integrations[0].get("score", 0.0)
            except Exception:
                pass

            # Search modules (with similarity score)
            try:
                rows = vector_query.search(
                    "registry_ui_components",
                    select_columns=("id", "name", "description", "category", "props_schema"),
                    limit=8,
                    min_similarity=None,
                    score_alias="score",
                )
                modules = rows
            except Exception:
                pass

            # Search existing templates (with similarity score)
            try:
                rows = vector_query.search(
                    "app_manifests",
                    select_columns=("id", "name", "description"),
                    filters=(VectorFilter("status", "active"),),
                    limit=3,
                    min_similarity=None,
                    score_alias="score",
                )
                templates = rows
                if templates:
                    best_template_score = templates[0].get("score", 0.0)
            except Exception:
                pass
        else:
            # Fallback: FTS search (no scores available)
            import re
            words = [w for w in re.findall(r'\w+', task.lower()) if len(w) > 2]
            or_query = ' | '.join(words) if words else task
            try:
                rows = self._conn.execute(
                    "SELECT id, name, description, capabilities, icon "
                    "FROM integration_registry WHERE search_vector @@ to_tsquery('english', $1) LIMIT 3",
                    or_query,
                )
                integrations = [dict(r) for r in rows]
            except Exception:
                pass

        return PreSuggestion(
            integrations=integrations,
            modules=modules,
            templates=templates,
            best_template_score=best_template_score,
            best_integration_score=best_integration_score,
        )

    # ------------------------------------------------------------------
    # Step 1b: Fast assembly (skip planner when vectors are strong)
    # ------------------------------------------------------------------

    def _fast_assemble(self, task: str, suggestions: PreSuggestion) -> Optional[tuple[str, AssemblyPlan]]:
        """Skip the planner if vector matches are strong enough.

        Returns (manifest_id, plan) on fast path, or None to fall through to planner.
        """
        # Path A: Near-exact template match — clone it
        if suggestions.best_template_score > 0.85 and suggestions.templates:
            template_id = suggestions.templates[0]["id"]
            manifest_id = self._clone_template(template_id, task)
            if manifest_id:
                plan = AssemblyPlan(
                    task=task,
                    explanation=f"Cloned template '{suggestions.templates[0].get('name', template_id)}' "
                                f"(score={suggestions.best_template_score:.2f})",
                )
                return manifest_id, plan

        # Path B: Integrations + modules cover enough of the task
        if suggestions.integrations and suggestions.modules:
            integration_scores = [i.get("score", 0.0) for i in suggestions.integrations]
            module_scores = [m.get("score", 0.0) for m in suggestions.modules]
            combined_coverage = (
                (max(integration_scores) if integration_scores else 0.0)
                + (sum(sorted(module_scores, reverse=True)[:4]) / 4 if module_scores else 0.0)
            ) / 2
            if combined_coverage > 0.7:
                plan = self._plan_from_suggestions(task, suggestions)
                manifest_id = self._execute_plan(plan)
                return manifest_id, plan

        return None

    def _clone_template(self, template_id: str, task: str) -> Optional[str]:
        """Clone an existing manifest as a new manifest for the given task."""
        rows = self._conn.execute(
            "SELECT manifest, description FROM app_manifests WHERE id = $1",
            template_id,
        )
        if not rows:
            return None
        row = rows[0]
        manifest_data = row["manifest"]
        if isinstance(manifest_data, str):
            manifest_data = json.loads(manifest_data)

        new_id = f"task-{uuid.uuid4().hex[:10]}"
        create_app_manifest(
            self._conn,
            manifest_id=new_id,
            name=task[:50],
            description=f"Cloned from {template_id} for: {task}",
            manifest=manifest_data,
            created_by="task_assembler",
        )

        # Embed the clone
        if self._vector_store is not None:
            self._vector_store.set_embedding(
                "app_manifests",
                "id",
                new_id,
                text=task,
            )

        return new_id

    # ------------------------------------------------------------------
    # Step 2: Planner (one smart model call)
    # ------------------------------------------------------------------

    def _call_planner(self, task: str, suggestions: PreSuggestion) -> AssemblyPlan:
        """One smart model call through direct routed provider execution."""
        from runtime.task_type_router import TaskTypeRouter

        prompt = self._build_planner_prompt(task, suggestions)
        router = TaskTypeRouter(self._conn)
        chain = router.resolve_failover_chain("auto/planner")

        env = dict(os.environ)
        for key in list(env):
            if key.upper().startswith("CLAUDE") and key.upper() != "CLAUDE_CONFIG_DIR":
                del env[key]

        for tier in chain:
            provider = tier.provider_slug
            model_slug = tier.model_slug
            try:
                raw = self._call_provider(provider, model_slug, prompt, env)
                parsed = self._parse_json(raw) if raw else None
                if isinstance(parsed, dict):
                    router.record_outcome("planner", provider, model_slug, succeeded=True)
                    return AssemblyPlan(
                        task=task,
                        data_sources=parsed.get("data_sources", []),
                        object_type=parsed.get("object_type"),
                        modules=parsed.get("modules", []),
                        seed_records=parsed.get("seed_records", []),
                        explanation=parsed.get("explanation", ""),
                    )
                router.record_outcome("planner", provider, model_slug, succeeded=False)
                logger.warning(
                    "Planner %s/%s returned unparseable output",
                    provider,
                    model_slug,
                )
            except Exception as exc:
                router.record_outcome("planner", provider, model_slug, succeeded=False)
                logger.warning("Planner %s/%s failed: %s", provider, model_slug, exc)

        raise RuntimeError("All planner tiers failed to produce an assembly plan")

    def _build_planner_prompt(self, task: str, suggestions: PreSuggestion) -> str:
        parts = [
            f'The user wants to: "{task}"',
            "",
            "Here are pre-matched data sources, modules, and templates from vector search.",
            "Review the suggested wiring and output a plan.",
            "",
        ]

        if suggestions.integrations:
            parts.append("MATCHED INTEGRATIONS:")
            for i in suggestions.integrations:
                caps = i.get("capabilities", [])
                if isinstance(caps, str):
                    try: caps = json.loads(caps)
                    except: caps = []
                cap_names = [c.get("action", "") if isinstance(c, dict) else str(c) for c in caps[:4]]
                parts.append(f"  - {i['name']}: {i.get('description', '')[:80]} (actions: {', '.join(cap_names)})")
            parts.append("")

        if suggestions.modules:
            parts.append("MATCHED UI MODULES:")
            for m in suggestions.modules:
                parts.append(f"  - {m['id']}: {m['name']} ({m.get('category', '')})")
            parts.append("")

        if suggestions.templates:
            parts.append("SIMILAR EXISTING TEMPLATES:")
            for t in suggestions.templates:
                parts.append(f"  - {t['id']}: {t['name']}")
            parts.append("")

        parts.extend([
            "Available module IDs: metric, stat-row, chart, activity-feed, status-grid, markdown, key-value, data-table, text-input, dropdown-select, button-row, search-panel",
            "",
            "Output ONLY valid JSON with this structure:",
            '{',
            '  "data_sources": [{"id": "...", "name": "...", "endpoint": "/api/..."}],',
            '  "object_type": {"name": "...", "description": "...", "properties": [{"name": "...", "type": "text|number|date|email|currency|dropdown", "required": true}]} | null,',
            '  "modules": [{"module_id": "data-table", "quadrant": "A1", "span": "2x2", "config": {...}}, ...],',
            '  "seed_records": [{"prop1": "val1", ...}, ...] | [],',
            '  "explanation": "one line summary"',
            '}',
            "",
            "Rules:",
            "- If an integration matches, use its endpoint as the data source",
            "- If no integration matches, create an object_type as a holding table",
            '- object_type properties should match the task semantics (e.g. "expenses" → amount:currency, date:date, category:dropdown)',
            "- Use data-table for lists, metric for counts, key-value for details, chart for trends, button-row for actions",
            "- Layout in a 4x4 grid (A1-D4), use spans for important modules",
            "- Include 3-5 seed_records if creating an object_type",
        ])

        return "\n".join(parts)

    def _parse_plan(self, task: str, raw: str) -> AssemblyPlan:
        """Parse planner output JSON."""
        # Strip markdown fences
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1] if lines[-1].startswith("```") else lines[1:])

        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            # Try to find JSON in the output
            start = text.find("{")
            end = text.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(text[start:end])
            else:
                return self._plan_from_suggestions(task, PreSuggestion())

        return AssemblyPlan(
            task=task,
            data_sources=data.get("data_sources", []),
            object_type=data.get("object_type"),
            modules=data.get("modules", []),
            seed_records=data.get("seed_records", []),
            explanation=data.get("explanation", ""),
        )

    def _plan_from_suggestions(self, task: str, suggestions: PreSuggestion) -> AssemblyPlan:
        """Fallback: build plan directly from vector suggestions without LLM."""
        modules = []
        object_type = None
        slug = task.lower().replace(" ", "-")[:20]

        if suggestions.integrations:
            # Use first integration — still create a holding object type
            # since we can't call MCP tools directly from the API
            intg = suggestions.integrations[0]
            type_id = slug.replace(" ", "_")
            object_type = {
                "name": intg.get("name", task).title(),
                "description": f"Data from {intg.get('name', 'integration')} for: {task}",
                "properties": [
                    {"name": "name", "type": "text", "required": True},
                    {"name": "status", "type": "dropdown", "options": ["active", "pending", "done"], "default": "active"},
                    {"name": "source", "type": "text", "default": intg.get("name", "")},
                    {"name": "notes", "type": "text"},
                ],
            }
            modules.append({
                "module_id": "data-table", "quadrant": "B1", "span": "4x2",
                "config": {"objectType": type_id, "publishSelection": type_id, "title": intg.get("name", task)},
            })
            modules.append({
                "module_id": "button-row", "quadrant": "A4",
                "config": {"actions": [{"label": "New", "variant": "primary", "createObject": {"typeId": type_id}}]},
            })
            modules.append({
                "module_id": "key-value", "quadrant": "A1", "span": "2x1",
                "config": {"subscribeSelection": type_id, "title": "Details"},
            })
            modules.append({
                "module_id": "metric", "quadrant": "A3",
                "config": {"label": "Total", "value": "0", "color": "#58a6ff"},
            })
        else:
            # Create holding object type
            object_type = {
                "name": slug.replace("-", " ").title(),
                "description": f"Holding table for: {task}",
                "properties": [
                    {"name": "name", "type": "text", "required": True},
                    {"name": "status", "type": "dropdown", "options": ["active", "pending", "done"], "default": "active"},
                    {"name": "notes", "type": "text"},
                ],
            }
            type_id = slug.replace(" ", "_")
            modules.append({
                "module_id": "data-table", "quadrant": "B1", "span": "4x2",
                "config": {"objectType": type_id, "publishSelection": type_id},
            })
            modules.append({
                "module_id": "button-row", "quadrant": "A4",
                "config": {"actions": [{"label": "New", "variant": "primary", "createObject": {"typeId": type_id}}]},
            })
            modules.append({
                "module_id": "key-value", "quadrant": "A1", "span": "2x1",
                "config": {"subscribeSelection": type_id, "title": "Details"},
            })
            modules.append({
                "module_id": "metric", "quadrant": "A3",
                "config": {"label": "Total", "value": "0", "color": "#58a6ff"},
            })

        modules.append({
            "module_id": "search-panel", "quadrant": "D1", "span": "2x1",
            "config": {"objectType": slug.replace(" ", "_") if not suggestions.integrations else None},
        })
        modules.append({
            "module_id": "activity-feed", "quadrant": "D3", "span": "2x1",
            "config": {"title": "Recent Activity"},
        })

        return AssemblyPlan(
            task=task,
            data_sources=[{"id": suggestions.integrations[0]["id"], "name": suggestions.integrations[0]["name"]}] if suggestions.integrations else [],
            object_type=object_type,
            modules=modules,
            explanation=f"Built workspace for: {task}",
        )

    # ------------------------------------------------------------------
    # Step 3: Execute plan (write to Postgres)
    # ------------------------------------------------------------------

    def _execute_plan(self, plan: AssemblyPlan) -> str:
        """Write everything to Postgres. Returns manifest_id."""
        manifest_id = f"task-{uuid.uuid4().hex[:10]}"
        # Create object type if needed
        if plan.object_type:
            ot = plan.object_type
            type_id = ot["name"].lower().replace(" ", "_")
            ensure_object_type_record(
                self._conn,
                type_id=type_id,
                name=ot["name"],
                description=ot.get("description", ""),
                property_definitions=ot.get("properties", []),
            )

            # Seed records if provided
            for record in plan.seed_records:
                oid = f"obj-{uuid.uuid4().hex[:12]}"
                create_object_record(
                    self._conn,
                    object_id=oid,
                    type_id=type_id,
                    properties=record,
                )

        # Build manifest
        quadrants = {}
        for m in plan.modules:
            qid = m.get("quadrant", "A1")
            entry: dict[str, Any] = {"module": m["module_id"]}
            if m.get("span"):
                entry["span"] = m["span"]
            if m.get("config"):
                entry["config"] = m["config"]
            quadrants[qid] = entry

        manifest = {
            "version": 2,
            "grid": "4x4",
            "quadrants": quadrants,
        }

        # Save manifest
        create_app_manifest(
            self._conn,
            manifest_id=manifest_id,
            name=plan.task[:50],
            description=plan.explanation,
            manifest=manifest,
            created_by="task_assembler",
            version=int(manifest.get("version", 2) or 2),
        )

        # Embed the new manifest
        if self._vector_store is not None:
            try:
                self._vector_store.set_embedding(
                    "app_manifests",
                    "id",
                    manifest_id,
                    text=f"{plan.task} {plan.explanation}",
                )
            except Exception:
                pass

        return manifest_id
