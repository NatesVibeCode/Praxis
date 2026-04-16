"""HeartbeatModule that generates time-series metric rollups with trend detection.

Runs AFTER RelationshipMiner in the heartbeat cycle. Implements five rollup types:
- Daily agent rollup (idempotent per day)
- Daily phase rollup
- Weekly platform rollup (Monday or first-of-week)
- Model comparison rollup (daily)
- Trend detection (regression / cost_spike / efficiency_drop lessons)
"""
from __future__ import annotations

import time
import traceback
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from memory.types import Edge, Entity, EntityType, RelationType
from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _ok, _fail

if TYPE_CHECKING:
    from memory.engine import MemoryEngine
    from storage.postgres import SyncPostgresConnection


class RollupGenerator(HeartbeatModule):
    def __init__(self, conn: "SyncPostgresConnection", engine: "MemoryEngine"):
        self._conn = conn
        self._engine = engine

    @property
    def name(self) -> str:
        return 'rollup_generator'

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        errors: list[str] = []
        target_date = datetime.now(timezone.utc).date()

        for label, fn in [
            ('daily_agent_rollups',    lambda: self._daily_agent_rollups(target_date)),
            ('daily_phase_rollups',    lambda: self._daily_phase_rollups(target_date)),
            ('weekly_platform_rollup', self._weekly_platform_rollup),
            ('model_comparison_rollup', lambda: self._model_comparison_rollup(target_date)),
            ('detect_trends',          lambda: self._detect_trends(target_date)),
        ]:
            try:
                fn()
            except Exception:
                errors.append(f"{label}: {traceback.format_exc()}")
        if errors:
            return _fail(self.name, t0, "; ".join(errors))
        return _ok(self.name, t0)

    # -------------------------------------------------------------------------
    # helpers
    # -------------------------------------------------------------------------

    def _entity_exists(self, entity_id: str) -> bool:
        rows = list(self._conn.execute(
            "SELECT id FROM memory_entities WHERE id = $1",
            entity_id,
        ))
        return len(rows) > 0

    # -------------------------------------------------------------------------
    # 1. Daily agent rollup
    # -------------------------------------------------------------------------

    def _daily_agent_rollups(self, target_date) -> tuple[int, int]:
        date_str = target_date.isoformat()
        window_start = datetime(
            target_date.year,
            target_date.month,
            target_date.day,
            tzinfo=timezone.utc,
        )
        window_end = window_start + timedelta(days=1)

        rows = self._conn.execute(
            """
            SELECT
                metadata->>'agent' AS agent,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE metadata->>'status' = 'succeeded') AS passed,
                AVG((metadata->>'cost_usd')::float) AS avg_cost,
                SUM((metadata->>'cost_usd')::float) AS total_cost,
                AVG((metadata->>'input_tokens')::float) AS avg_input,
                AVG((metadata->>'output_tokens')::float) AS avg_output,
                AVG((metadata->>'cache_hit_rate')::float) AS avg_cache,
                AVG((metadata->>'num_turns')::float) AS avg_turns,
                AVG((metadata->>'duration_api_ms')::float) AS avg_duration
            FROM memory_entities
            WHERE source = 'dispatch'
              AND created_at >= $1 AND created_at < $2
            GROUP BY metadata->>'agent'
            HAVING COUNT(*) >= 1
            """,
            window_start, window_end,
        )

        findings = 0
        actions = 0
        now = datetime.now(timezone.utc)

        for row in rows:
            agent = row['agent']
            if not agent:
                continue

            entity_id = f"rollup:agent:{agent}:{date_str}"
            if self._entity_exists(entity_id):
                continue

            total = row['total'] or 0
            passed = row['passed'] or 0
            pass_rate = passed / total if total else 0.0
            avg_cost = row['avg_cost'] or 0.0
            avg_input = row['avg_input'] or 0.0
            avg_output = row['avg_output'] or 0.0
            total_tokens = avg_input + avg_output
            token_efficiency = (pass_rate / total_tokens) if total_tokens > 0 else 0.0

            # Query failure categories for this agent in this window
            external_count = config_count = internal_count = 0
            try:
                cat_rows = self._conn.execute(
                    """SELECT failure_code, COUNT(*) as cnt
                       FROM receipts
                       WHERE inputs->>'agent_slug' = $1
                         AND started_at >= $2 AND started_at < $3
                         AND COALESCE(failure_code, '') != ''
                       GROUP BY failure_code""",
                    agent, window_start, window_end,
                )
                _zone_map = {
                    "timeout": "external", "rate_limit": "external", "provider_error": "external",
                    "network_error": "external", "infrastructure": "external",
                    "credential_error": "config", "model_error": "config", "input_error": "config",
                    "context_overflow": "internal", "parse_error": "internal", "sandbox_error": "internal",
                    "scope_violation": "internal", "verification_failed": "internal",
                }
                for cr in cat_rows:
                    zone = _zone_map.get(cr['failure_code'], 'unknown')
                    cnt = int(cr['cnt'])
                    if zone == 'external':
                        external_count += cnt
                    elif zone == 'config':
                        config_count += cnt
                    elif zone == 'internal':
                        internal_count += cnt
            except Exception:
                pass

            adj_denom = total - external_count
            adjusted_pass_rate = passed / adj_denom if adj_denom > 0 else 0.0

            entity = Entity(
                id=entity_id,
                entity_type=EntityType.metric,
                name=f"Agent rollup: {agent} on {date_str}",
                content=(
                    f"pass_rate={pass_rate:.0%}, adjusted={adjusted_pass_rate:.0%}, total={total}, "
                    f"cost=${row['total_cost'] or 0.0:.4f}"
                ),
                metadata={
                    'agent': agent,
                    'date': date_str,
                    'total': total,
                    'passed': passed,
                    'pass_rate': round(pass_rate, 4),
                    'adjusted_pass_rate': round(adjusted_pass_rate, 4),
                    'external_failures': external_count,
                    'config_failures': config_count,
                    'internal_failures': internal_count,
                    'avg_cost_usd': round(avg_cost, 6),
                    'total_cost_usd': round(row['total_cost'] or 0.0, 6),
                    'avg_input_tokens': round(avg_input, 1),
                    'avg_output_tokens': round(avg_output, 1),
                    'avg_cache_hit_rate': round(row['avg_cache'] or 0.0, 4),
                    'avg_turns': round(row['avg_turns'] or 0.0, 2),
                    'avg_duration_ms': round(row['avg_duration'] or 0.0, 1),
                    'token_efficiency': round(token_efficiency, 8),
                },
                created_at=now,
                updated_at=now,
                source='rollup',
                confidence=0.95,
            )
            self._engine.insert(entity)
            actions += 1
            findings += 1

        return findings, actions

    # -------------------------------------------------------------------------
    # 2. Daily phase rollup
    # -------------------------------------------------------------------------

    def _daily_phase_rollups(self, target_date) -> tuple[int, int]:
        date_str = target_date.isoformat()
        window_start = datetime(
            target_date.year,
            target_date.month,
            target_date.day,
            tzinfo=timezone.utc,
        )
        window_end = window_start + timedelta(days=1)

        rows = self._conn.execute(
            """
            SELECT
                COALESCE(metadata->>'phase', 'unknown') AS phase,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE metadata->>'status' = 'succeeded') AS passed,
                AVG((metadata->>'cost_usd')::float) AS avg_cost,
                SUM((metadata->>'cost_usd')::float) AS total_cost,
                AVG((metadata->>'input_tokens')::float) AS avg_input,
                AVG((metadata->>'output_tokens')::float) AS avg_output,
                AVG((metadata->>'cache_hit_rate')::float) AS avg_cache,
                AVG((metadata->>'num_turns')::float) AS avg_turns,
                AVG((metadata->>'duration_api_ms')::float) AS avg_duration
            FROM memory_entities
            WHERE source = 'dispatch'
              AND created_at >= $1 AND created_at < $2
            GROUP BY COALESCE(metadata->>'phase', 'unknown')
            HAVING COUNT(*) >= 1
            """,
            window_start, window_end,
        )

        findings = 0
        actions = 0
        now = datetime.now(timezone.utc)

        for row in rows:
            phase = row['phase'] or 'unknown'
            entity_id = f"rollup:phase:{phase}:{date_str}"
            if self._entity_exists(entity_id):
                continue

            total = row['total'] or 0
            passed = row['passed'] or 0
            pass_rate = passed / total if total else 0.0

            entity = Entity(
                id=entity_id,
                entity_type=EntityType.metric,
                name=f"Phase rollup: {phase} on {date_str}",
                content=(
                    f"pass_rate={pass_rate:.0%}, total={total}, "
                    f"cost=${row['total_cost'] or 0.0:.4f}"
                ),
                metadata={
                    'phase': phase,
                    'date': date_str,
                    'total': total,
                    'passed': passed,
                    'pass_rate': round(pass_rate, 4),
                    'avg_cost_usd': round(row['avg_cost'] or 0.0, 6),
                    'total_cost_usd': round(row['total_cost'] or 0.0, 6),
                    'avg_input_tokens': round(row['avg_input'] or 0.0, 1),
                    'avg_output_tokens': round(row['avg_output'] or 0.0, 1),
                    'avg_cache_hit_rate': round(row['avg_cache'] or 0.0, 4),
                    'avg_turns': round(row['avg_turns'] or 0.0, 2),
                    'avg_duration_ms': round(row['avg_duration'] or 0.0, 1),
                },
                created_at=now,
                updated_at=now,
                source='rollup',
                confidence=0.95,
            )
            self._engine.insert(entity)
            actions += 1
            findings += 1

        return findings, actions

    # -------------------------------------------------------------------------
    # 3. Weekly platform rollup
    # -------------------------------------------------------------------------

    def _weekly_platform_rollup(self) -> tuple[int, int]:
        target_date = datetime.now(timezone.utc).date()
        iso_year, iso_week, iso_weekday = target_date.isocalendar()

        week_id = f"rollup:platform:{iso_year}-W{iso_week:02d}"
        # Only regenerate on Mondays; other days skip if entity already exists
        if iso_weekday != 1 and self._entity_exists(week_id):
            return 0, 0

        monday = target_date - timedelta(days=iso_weekday - 1)
        week_start = datetime(
            monday.year,
            monday.month,
            monday.day,
            tzinfo=timezone.utc,
        )
        week_end = week_start + timedelta(days=7)

        # Dispatch aggregates
        dispatch_rows = list(self._conn.execute(
            """
            SELECT
                COUNT(*) AS total_jobs,
                SUM((metadata->>'cost_usd')::float) AS total_cost,
                COUNT(*) FILTER (WHERE metadata->>'status' = 'succeeded') AS passed
            FROM memory_entities
            WHERE source = 'dispatch'
              AND created_at >= $1 AND created_at < $2
            """,
            week_start, week_end,
        ))
        dr = dispatch_rows[0] if dispatch_rows else None
        total_jobs = (dr['total_jobs'] or 0) if dr else 0
        total_cost = float(dr['total_cost'] or 0.0) if dr else 0.0
        passed_jobs = (dr['passed'] or 0) if dr else 0
        pass_rate = passed_jobs / total_jobs if total_jobs else 0.0

        # Bugs
        bugs_rows = list(self._conn.execute(
            """
            SELECT
                COUNT(*) AS bugs_filed,
                COUNT(*) FILTER (WHERE metadata->>'is_resolved' = 'true') AS bugs_resolved
            FROM memory_entities
            WHERE source = 'bugs'
              AND created_at >= $1 AND created_at < $2
            """,
            week_start, week_end,
        ))
        br = bugs_rows[0] if bugs_rows else None
        bugs_filed = (br['bugs_filed'] or 0) if br else 0
        bugs_resolved = (br['bugs_resolved'] or 0) if br else 0

        # Constraints mined
        constraints_rows = list(self._conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM memory_entities
            WHERE source = 'constraints'
              AND created_at >= $1 AND created_at < $2
            """,
            week_start, week_end,
        ))
        cr = constraints_rows[0] if constraints_rows else None
        constraints_mined = (cr['cnt'] or 0) if cr else 0

        # Friction events
        friction_rows = list(self._conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM memory_entities
            WHERE source = 'friction'
              AND created_at >= $1 AND created_at < $2
            """,
            week_start, week_end,
        ))
        fr = friction_rows[0] if friction_rows else None
        friction_events = (fr['cnt'] or 0) if fr else 0

        sunday = monday + timedelta(days=6)
        now = datetime.now(timezone.utc)
        entity = Entity(
            id=week_id,
            entity_type=EntityType.metric,
            name=f"Platform rollup: {iso_year}-W{iso_week:02d}",
            content=(
                f"total_jobs={total_jobs}, pass_rate={pass_rate:.0%}, "
                f"cost=${total_cost:.2f}, bugs={bugs_filed}, "
                f"constraints={constraints_mined}, friction={friction_events}"
            ),
            metadata={
                'year': iso_year,
                'week': iso_week,
                'week_start': monday.isoformat(),
                'week_end': sunday.isoformat(),
                'total_jobs': total_jobs,
                'pass_rate': round(pass_rate, 4),
                'total_cost_usd': round(total_cost, 4),
                'bugs_filed': bugs_filed,
                'bugs_resolved': bugs_resolved,
                'constraints_mined': constraints_mined,
                'friction_events': friction_events,
            },
            created_at=now,
            updated_at=now,
            source='rollup',
            confidence=0.95,
        )
        self._engine.insert(entity)
        return 1, 1

    # -------------------------------------------------------------------------
    # 4. Model comparison rollup
    # -------------------------------------------------------------------------

    def _model_comparison_rollup(self, target_date) -> tuple[int, int]:
        date_str = target_date.isoformat()
        entity_id = f"rollup:model_compare:{date_str}"
        if self._entity_exists(entity_id):
            return 0, 0

        window_start = datetime(
            target_date.year,
            target_date.month,
            target_date.day,
            tzinfo=timezone.utc,
        )
        window_end = window_start + timedelta(days=1)

        rows = self._conn.execute(
            """
            SELECT
                metadata->>'model' AS model,
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE metadata->>'status' = 'succeeded') AS passed,
                AVG((metadata->>'cost_usd')::float) AS avg_cost,
                AVG((metadata->>'input_tokens')::float) AS avg_input,
                AVG((metadata->>'output_tokens')::float) AS avg_output
            FROM memory_entities
            WHERE source = 'dispatch'
              AND metadata->>'model' IS NOT NULL
              AND metadata->>'model' != ''
              AND created_at >= $1 AND created_at < $2
            GROUP BY metadata->>'model'
            HAVING COUNT(*) >= 1
            """,
            window_start, window_end,
        )

        per_model: list[dict] = []
        for row in rows:
            model = row['model']
            total = row['total'] or 0
            passed = row['passed'] or 0
            avg_cost = float(row['avg_cost'] or 0.0)
            avg_input = float(row['avg_input'] or 0.0)
            avg_output = float(row['avg_output'] or 0.0)
            pass_rate = passed / total if total else 0.0
            cost_efficiency = pass_rate / avg_cost if avg_cost > 0 else 0.0
            total_tokens = avg_input + avg_output
            token_efficiency = pass_rate / total_tokens if total_tokens > 0 else 0.0
            per_model.append({
                'model': model,
                'total': total,
                'passed': passed,
                'pass_rate': round(pass_rate, 4),
                'avg_cost_usd': round(avg_cost, 6),
                'cost_efficiency': round(cost_efficiency, 6),
                'token_efficiency': round(token_efficiency, 8),
                'avg_input_tokens': round(avg_input, 1),
                'avg_output_tokens': round(avg_output, 1),
            })

        if not per_model:
            return 0, 0

        by_pass_rate = sorted(per_model, key=lambda x: x['pass_rate'], reverse=True)
        by_cost_efficiency = sorted(per_model, key=lambda x: x['cost_efficiency'], reverse=True)
        by_token_efficiency = sorted(per_model, key=lambda x: x['token_efficiency'], reverse=True)

        now = datetime.now(timezone.utc)
        entity = Entity(
            id=entity_id,
            entity_type=EntityType.metric,
            name=f"Model comparison: {date_str}",
            content=(
                f"top_model={by_pass_rate[0]['model']}, "
                f"models={len(per_model)}"
            ),
            metadata={
                'date': date_str,
                'model_count': len(per_model),
                'rankings': {
                    'by_pass_rate': [m['model'] for m in by_pass_rate],
                    'by_cost_efficiency': [m['model'] for m in by_cost_efficiency],
                    'by_token_efficiency': [m['model'] for m in by_token_efficiency],
                },
                'per_model': per_model,
            },
            created_at=now,
            updated_at=now,
            source='rollup',
            confidence=0.95,
        )
        self._engine.insert(entity)
        return 1, 1

    # -------------------------------------------------------------------------
    # 5. Trend detection
    # -------------------------------------------------------------------------

    def _detect_trends(self, target_date) -> tuple[int, int]:
        date_str = target_date.isoformat()
        yesterday = target_date - timedelta(days=1)
        yesterday_str = yesterday.isoformat()

        # Find all agent rollups created today
        rows = self._conn.execute(
            """
            SELECT id, metadata
            FROM memory_entities
            WHERE source = 'rollup'
              AND metadata->>'date' = $1
              AND id LIKE 'rollup:agent:%'
              AND archived = false
            """,
            date_str,
        )

        findings = 0
        actions = 0
        now = datetime.now(timezone.utc)

        for row in rows:
            today_id = row['id']
            meta = row['metadata'] or {}
            if isinstance(meta, str):
                import json as _json
                try:
                    meta = _json.loads(meta)
                except (ValueError, TypeError):
                    meta = {}

            agent = meta.get('agent')
            if not agent:
                continue

            yesterday_id = f"rollup:agent:{agent}:{yesterday_str}"
            yrows = list(self._conn.execute(
                "SELECT metadata FROM memory_entities WHERE id = $1",
                yesterday_id,
            ))
            if not yrows:
                continue

            ymeta = yrows[0]['metadata'] or {}
            if isinstance(ymeta, str):
                import json as _json
                try:
                    ymeta = _json.loads(ymeta)
                except (ValueError, TypeError):
                    ymeta = {}

            # Use adjusted_pass_rate (excludes external failures) for trend detection
            # to prevent provider outages from triggering false regression alerts
            today_pass_rate = float(meta.get('adjusted_pass_rate', meta.get('pass_rate', 0.0)))
            yesterday_pass_rate = float(ymeta.get('adjusted_pass_rate', ymeta.get('pass_rate', 0.0)))
            today_cost = float(meta.get('avg_cost_usd', 0.0))
            yesterday_cost = float(ymeta.get('avg_cost_usd', 0.0))
            today_token_eff = float(meta.get('token_efficiency', 0.0))
            yesterday_token_eff = float(ymeta.get('token_efficiency', 0.0))

            # --- pass rate regression (>= 15% relative drop) ---
            if (
                yesterday_pass_rate > 0
                and (yesterday_pass_rate - today_pass_rate) / yesterday_pass_rate >= 0.15
            ):
                delta = today_pass_rate - yesterday_pass_rate
                severity = 'critical' if delta <= -0.30 else 'warning'
                lesson_id = f"lesson:regression:{agent}:{date_str}"
                if not self._entity_exists(lesson_id):
                    lesson = Entity(
                        id=lesson_id,
                        entity_type=EntityType.lesson,
                        name=f"{agent} pass rate regression on {date_str}",
                        content=(
                            f"{agent} pass rate dropped from "
                            f"{yesterday_pass_rate:.0%} to {today_pass_rate:.0%} on {date_str}"
                        ),
                        metadata={
                            'agent': agent,
                            'date': date_str,
                            'severity': severity,
                            'metric': 'pass_rate',
                            'old_value': round(yesterday_pass_rate, 4),
                            'new_value': round(today_pass_rate, 4),
                            'delta': round(delta, 4),
                        },
                        created_at=now,
                        updated_at=now,
                        source='rollup',
                        confidence=0.9,
                    )
                    self._engine.insert(lesson)
                    self._engine.add_edge(Edge(
                        source_id=lesson_id,
                        target_id=today_id,
                        relation_type=RelationType.regressed_from,
                        weight=0.9,
                        metadata={},
                        created_at=now,
                    ))
                    self._engine.add_edge(Edge(
                        source_id=lesson_id,
                        target_id=yesterday_id,
                        relation_type=RelationType.regressed_from,
                        weight=0.9,
                        metadata={},
                        created_at=now,
                    ))
                    actions += 3
                    findings += 1

            # --- cost spike (>= 50% relative increase) ---
            if (
                yesterday_cost > 0
                and (today_cost - yesterday_cost) / yesterday_cost >= 0.50
            ):
                delta = today_cost - yesterday_cost
                lesson_id = f"lesson:cost_spike:{agent}:{date_str}"
                if not self._entity_exists(lesson_id):
                    lesson = Entity(
                        id=lesson_id,
                        entity_type=EntityType.lesson,
                        name=f"{agent} cost spike on {date_str}",
                        content=(
                            f"{agent} avg cost spiked from "
                            f"${yesterday_cost:.4f} to ${today_cost:.4f} on {date_str}"
                        ),
                        metadata={
                            'agent': agent,
                            'date': date_str,
                            'severity': 'warning',
                            'metric': 'avg_cost_usd',
                            'old_value': round(yesterday_cost, 6),
                            'new_value': round(today_cost, 6),
                            'delta': round(delta, 6),
                        },
                        created_at=now,
                        updated_at=now,
                        source='rollup',
                        confidence=0.85,
                    )
                    self._engine.insert(lesson)
                    self._engine.add_edge(Edge(
                        source_id=lesson_id,
                        target_id=today_id,
                        relation_type=RelationType.regressed_from,
                        weight=0.8,
                        metadata={},
                        created_at=now,
                    ))
                    self._engine.add_edge(Edge(
                        source_id=lesson_id,
                        target_id=yesterday_id,
                        relation_type=RelationType.regressed_from,
                        weight=0.8,
                        metadata={},
                        created_at=now,
                    ))
                    actions += 3
                    findings += 1

            # --- token efficiency drop (>= 20% relative drop) ---
            if (
                yesterday_token_eff > 0
                and (yesterday_token_eff - today_token_eff) / yesterday_token_eff >= 0.20
            ):
                delta = today_token_eff - yesterday_token_eff
                lesson_id = f"lesson:efficiency_drop:{agent}:{date_str}"
                if not self._entity_exists(lesson_id):
                    lesson = Entity(
                        id=lesson_id,
                        entity_type=EntityType.lesson,
                        name=f"{agent} token efficiency drop on {date_str}",
                        content=(
                            f"{agent} token efficiency dropped from "
                            f"{yesterday_token_eff:.6f} to {today_token_eff:.6f} on {date_str}"
                        ),
                        metadata={
                            'agent': agent,
                            'date': date_str,
                            'severity': 'warning',
                            'metric': 'token_efficiency',
                            'old_value': round(yesterday_token_eff, 8),
                            'new_value': round(today_token_eff, 8),
                            'delta': round(delta, 8),
                        },
                        created_at=now,
                        updated_at=now,
                        source='rollup',
                        confidence=0.85,
                    )
                    self._engine.insert(lesson)
                    self._engine.add_edge(Edge(
                        source_id=lesson_id,
                        target_id=today_id,
                        relation_type=RelationType.regressed_from,
                        weight=0.8,
                        metadata={},
                        created_at=now,
                    ))
                    self._engine.add_edge(Edge(
                        source_id=lesson_id,
                        target_id=yesterday_id,
                        relation_type=RelationType.regressed_from,
                        weight=0.8,
                        metadata={},
                        created_at=now,
                    ))
                    actions += 3
                    findings += 1

        return findings, actions
