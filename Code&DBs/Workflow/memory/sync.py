"""MemorySync — heartbeat module that syncs platform data into the knowledge graph.

Pulls from canonical receipts, bugs, workflow_constraints, friction_events, and
operator_decisions. Uses watermark-based incremental sync (500 rows per source
per cycle). All entities get source-tagged for filtering.
"""
from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from memory.types import Entity, EntityType, Edge, RelationType
if TYPE_CHECKING:
    from memory.engine import MemoryEngine
    from storage.postgres.connection import SyncPostgresConnection

from runtime.heartbeat import HeartbeatModule, HeartbeatModuleResult, _ok, _fail

_BATCH_LIMIT = 500
_ATTEMPTED_VERIFICATION_STATUSES = frozenset({"passed", "failed", "error"})


def _json_object(value: object) -> dict:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return {}
        return dict(parsed) if isinstance(parsed, dict) else {}
    return {}


class MemorySync(HeartbeatModule):
    def __init__(self, conn: "SyncPostgresConnection", engine: "MemoryEngine") -> None:
        self._conn = conn
        self._engine = engine

    @property
    def name(self) -> str:
        return "memory_sync"

    def run(self) -> HeartbeatModuleResult:
        t0 = time.monotonic()
        errors = []
        for sync_fn in [self._sync_receipts, self._sync_bugs, self._sync_constraints,
                        self._sync_friction, self._sync_operator_decisions]:
            try:
                sync_fn()
            except Exception as exc:
                errors.append(f"{sync_fn.__name__}: {exc}")
        if errors:
            return _fail(self.name, t0, "; ".join(errors))
        return _ok(self.name, t0)

    def _get_watermark(self, source: str) -> tuple[int, datetime]:
        row = self._conn.fetchrow(
            'SELECT last_synced_id, last_synced_at FROM memory_sync_watermarks WHERE source_name = $1',
            source
        )
        if row:
            return int(row['last_synced_id']), row['last_synced_at']
        return 0, datetime(1970, 1, 1, tzinfo=timezone.utc)

    def _update_watermark(self, source: str, last_id: int, last_ts: datetime, count: int) -> None:
        self._conn.execute(
            '''UPDATE memory_sync_watermarks
               SET last_synced_id = GREATEST(last_synced_id, $2),
                   last_synced_at = GREATEST(last_synced_at, $3),
                   rows_synced = rows_synced + $4,
                   last_cycle_at = NOW()
             WHERE source_name = $1''',
            source, last_id, last_ts, count
        )

    @staticmethod
    def _receipt_entity_id(receipt_id: str | None) -> str:
        token = str(receipt_id or "").strip()
        if not token:
            return "receipt:unknown"
        return token if token.startswith("receipt:") else f"receipt:{token}"

    @staticmethod
    def _verification_entity_id(receipt_id: str | None) -> str:
        token = str(receipt_id or "").strip()
        return f"verification:{token or 'unknown'}"

    @staticmethod
    def _failure_entity_id(receipt_id: str | None) -> str:
        token = str(receipt_id or "").strip()
        return f"failure:{token or 'unknown'}"

    @staticmethod
    def _code_path_entity_id(path: str) -> str:
        digest = hashlib.sha1(path.encode("utf-8")).hexdigest()[:16]
        return f"codepath:{digest}"

    @staticmethod
    def _normalize_paths(raw_value: object) -> list[str]:
        paths: set[str] = set()
        if isinstance(raw_value, str):
            token = raw_value.strip()
            if token and not token.startswith("http"):
                paths.add(token)
        elif isinstance(raw_value, list):
            for item in raw_value:
                if isinstance(item, str):
                    token = item.strip()
                    if token and not token.startswith("http"):
                        paths.add(token)
        return sorted(paths)

    @classmethod
    def _extract_verified_paths(cls, outputs: dict) -> list[str]:
        paths: set[str] = set(cls._normalize_paths(outputs.get("verified_paths")))
        bindings = outputs.get("verification_bindings") or []
        if not isinstance(bindings, list):
            return sorted(paths)
        singular_keys = ("path", "file", "target", "module")
        plural_keys = ("paths", "files", "targets", "write_scope", "file_paths", "modules")
        for binding in bindings:
            if not isinstance(binding, dict):
                continue
            inputs = binding.get("inputs")
            if not isinstance(inputs, dict):
                continue
            for key in singular_keys:
                paths.update(cls._normalize_paths(inputs.get(key)))
            for key in plural_keys:
                paths.update(cls._normalize_paths(inputs.get(key)))
        return sorted(paths)

    @classmethod
    def _extract_mutation_paths(cls, inputs: dict, outputs: dict) -> list[str]:
        write_manifest = outputs.get("write_manifest") or {}
        manifest_results = (
            write_manifest.get("results")
            if isinstance(write_manifest, dict)
            else []
        ) or []
        manifest_paths = [
            str(row.get("file_path") or "").strip()
            for row in manifest_results
            if isinstance(row, dict) and str(row.get("file_path") or "").strip()
        ]
        mutation_provenance = outputs.get("mutation_provenance") or {}
        mutation_paths = (
            mutation_provenance.get("write_paths")
            if isinstance(mutation_provenance, dict)
            else []
        )
        from runtime.receipt_provenance import extract_write_paths
        return extract_write_paths(
            inputs.get("write_scope"),
            inputs.get("scope_write"),
            inputs.get("touch_keys"),
            manifest_paths,
            mutation_paths,
        )

    def _insert_code_unit(self, *, path: str, ts: datetime, source: str, confidence: float) -> str:
        code_entity_id = self._code_path_entity_id(path)
        self._engine.insert(
            Entity(
                id=code_entity_id,
                entity_type=EntityType.code_unit,
                name=path,
                content=f"Tracked code path {path}",
                metadata={
                    "entity_subtype": "code_path",
                    "path": path,
                    "extension": path.rsplit(".", 1)[-1] if "." in path else "",
                },
                created_at=ts,
                updated_at=ts,
                source=source,
                confidence=confidence,
            )
        )
        return code_entity_id

    def _sync_receipt_mutations(
        self,
        *,
        receipt_id: str,
        receipt_entity_id: str,
        ts: datetime,
        inputs: dict,
        outputs: dict,
    ) -> tuple[int, list[str]]:
        mutation_paths = self._extract_mutation_paths(inputs, outputs)
        if not mutation_paths:
            return 0, []

        actions = 0
        for path in mutation_paths:
            code_entity_id = self._insert_code_unit(
                path=path,
                ts=ts,
                source="receipt_mutation",
                confidence=0.9,
            )
            self._engine.add_edge(
                Edge(
                    source_id=receipt_entity_id,
                    target_id=code_entity_id,
                    relation_type=RelationType.produced,
                    weight=1.0,
                    metadata={
                        "edge_kind": "receipt_mutation",
                        "receipt_id": receipt_id,
                        "path": path,
                    },
                    created_at=ts,
                )
            )
            actions += 1
        return actions, mutation_paths

    def _sync_receipt_verification(
        self,
        *,
        receipt_id: str,
        receipt_entity_id: str,
        label: str,
        ts: datetime,
        outputs: dict,
    ) -> int:
        verification = outputs.get("verification") or {}
        if not isinstance(verification, dict):
            verification = {}
        verified_paths = self._extract_verified_paths(outputs)
        verification_status = str(outputs.get("verification_status") or "").strip()
        if not verification and not verified_paths and not verification_status:
            return 0

        verification_entity_id = self._verification_entity_id(receipt_id)
        total = int(verification.get("total") or 0)
        passed = int(verification.get("passed") or 0)
        failed = int(verification.get("failed") or 0)
        status_label = verification_status or ("passed" if verification.get("all_passed") else "failed")
        summary = f"Verification {status_label}."
        if total:
            summary += f" {passed}/{total} checks passed."
        if failed:
            failed_labels = [
                str(result.get("label") or "").strip()
                for result in (verification.get("results") or [])
                if isinstance(result, dict) and not bool(result.get("passed"))
            ]
            failed_labels = [item for item in failed_labels if item]
            if failed_labels:
                summary += f" Failed checks: {', '.join(failed_labels)}."

        self._engine.insert(
            Entity(
                id=verification_entity_id,
                entity_type=EntityType.fact,
                name=f"verification:{label or receipt_id}",
                content=summary.strip(),
                metadata={
                    "entity_subtype": "verification_result",
                    "receipt_id": receipt_id,
                    "job_label": label,
                    "verification_status": status_label or "unknown",
                    "verification": verification,
                    "verified_paths": verified_paths,
                },
                created_at=ts,
                updated_at=ts,
                source="verification",
                confidence=1.0 if verification.get("all_passed") else 0.95,
            )
        )
        self._engine.add_edge(
            Edge(
                source_id=verification_entity_id,
                target_id=receipt_entity_id,
                relation_type=RelationType.recorded_in,
                weight=1.0,
                metadata={"edge_kind": "verification_receipt"},
                created_at=ts,
            )
        )

        actions = 1
        if status_label not in _ATTEMPTED_VERIFICATION_STATUSES:
            return actions
        for path in verified_paths:
            code_entity_id = self._insert_code_unit(
                path=path,
                ts=ts,
                source="verification",
                confidence=0.9,
            )
            self._engine.add_edge(
                Edge(
                    source_id=code_entity_id,
                    target_id=verification_entity_id,
                    relation_type=RelationType.verified_by,
                    weight=1.0,
                    metadata={"edge_kind": "verification_coverage", "path": path},
                    created_at=ts,
                )
            )
            actions += 1
        return actions

    def _sync_receipt_failure(
        self,
        *,
        receipt_id: str,
        receipt_entity_id: str,
        label: str,
        ts: datetime,
        status: str,
        failure_code: str,
        outputs: dict,
        affected_paths: list[str],
    ) -> int:
        if not failure_code and status not in {"failed", "error", "dead_letter", "blocked"}:
            return 0

        failure_entity_id = self._failure_entity_id(receipt_id)
        failure_classification = outputs.get("failure_classification") or {}
        stderr_preview = str(outputs.get("stderr_preview") or "").strip()
        summary = f"Failure {failure_code or status or 'unknown'}."
        if isinstance(failure_classification, dict):
            category = str(failure_classification.get("category") or "").strip()
            if category:
                summary += f" Category: {category}."
        if stderr_preview:
            summary += f" {stderr_preview[:240]}"

        self._engine.insert(
            Entity(
                id=failure_entity_id,
                entity_type=EntityType.fact,
                name=f"failure:{label or receipt_id}",
                content=summary.strip(),
                metadata={
                    "entity_subtype": "failure_result",
                    "receipt_id": receipt_id,
                    "job_label": label,
                    "status": status or "failed",
                    "failure_code": failure_code or "",
                    "failure_classification": failure_classification if isinstance(failure_classification, dict) else {},
                    "affected_paths": affected_paths,
                },
                created_at=ts,
                updated_at=ts,
                source="failure",
                confidence=0.98,
            )
        )
        self._engine.add_edge(
            Edge(
                source_id=failure_entity_id,
                target_id=receipt_entity_id,
                relation_type=RelationType.recorded_in,
                weight=1.0,
                metadata={"edge_kind": "failure_receipt"},
                created_at=ts,
            )
        )

        actions = 1
        for path in affected_paths:
            code_entity_id = self._insert_code_unit(
                path=path,
                ts=ts,
                source="failure",
                confidence=0.85,
            )
            self._engine.add_edge(
                Edge(
                    source_id=code_entity_id,
                    target_id=failure_entity_id,
                    relation_type=RelationType.related_to,
                    weight=0.95,
                    metadata={
                        "edge_kind": "failure_impact",
                        "path": path,
                        "receipt_id": receipt_id,
                    },
                    created_at=ts,
                )
            )
            actions += 1
        return actions

    def _sync_receipt_row(self, row: dict) -> int:
        ts = row['finished_at'] or row['started_at'] or datetime.now(timezone.utc)
        inputs = _json_object(row['inputs'])
        outputs = _json_object(row['outputs'])
        inp = int(outputs.get('token_input') or 0)
        out = int(outputs.get('token_output') or 0)
        cache_read = int(outputs.get('cache_read_tokens') or 0)
        cache_create = int(outputs.get('cache_creation_tokens') or 0)
        cost_val = float(outputs.get('cost_usd') or 0)

        token_efficiency = (out / inp) if inp > 0 else 0.0
        cache_hit_rate = (cache_read / (cache_read + cache_create)) if (cache_read + cache_create) > 0 else 0.0

        label = inputs.get('job_label') or row['node_id'] or ''
        agent = inputs.get('agent_slug') or ''
        status = outputs.get('status') or row['status'] or ''
        model = outputs.get('model') or inputs.get('model') or ''
        duration_ms = int(outputs.get('duration_ms') or 0)
        num_turns = int(outputs.get('num_turns') or 0)
        tool_use = outputs.get('tool_use') or {}
        failure_reason = row.get('failure_code') or outputs.get('error_code') or ''
        receipt_id = str(row['receipt_id'] or '')
        verification = outputs.get('verification') or {}
        verification_status = str(outputs.get('verification_status') or '').strip()
        verified_paths = self._extract_verified_paths(outputs)
        mutation_paths = self._extract_mutation_paths(inputs, outputs)

        content = f"{model or agent} {status}"
        if inp > 0:
            content += f" — {inp}in/{out}out"
        if cache_read > 0:
            content += f", {cache_read} cache"
        if num_turns:
            content += f", {num_turns} turns"
        if cost_val > 0:
            content += f", ${cost_val:.4f}"

        metadata = {
            'receipt_id': receipt_id,
            'run_id': row['run_id'] or '',
            'job_label': label,
            'node_id': row['node_id'] or '',
            'agent': agent,
            'model': model,
            'status': status,
            'input_tokens': inp,
            'output_tokens': out,
            'cache_read_tokens': cache_read,
            'cache_creation_tokens': cache_create,
            'cost_usd': round(cost_val, 6),
            'duration_api_ms': duration_ms,
            'num_turns': num_turns,
            'tool_use': tool_use,
            'token_efficiency': round(token_efficiency, 4),
            'cache_hit_rate': round(cache_hit_rate, 4),
            'failure_reason': failure_reason,
            'workspace_ref': inputs.get('workspace_ref') or '',
            'runtime_profile_ref': inputs.get('runtime_profile_ref') or '',
            'entity_subtype': 'receipt',
        }
        if verification_status:
            metadata['verification_status'] = verification_status
        if isinstance(verification, dict) and verification:
            metadata['verification_total'] = int(verification.get('total') or 0)
            metadata['verification_passed'] = int(verification.get('passed') or 0)
            metadata['verification_failed'] = int(verification.get('failed') or 0)
        if verified_paths:
            metadata['verified_paths'] = verified_paths
        if mutation_paths:
            metadata['mutation_paths'] = mutation_paths

        receipt_entity_id = self._receipt_entity_id(receipt_id)
        entity = Entity(
            id=receipt_entity_id,
            entity_type=EntityType.task,
            name=label,
            content=content,
            metadata=metadata,
            created_at=ts,
            updated_at=ts,
            source='dispatch',
            confidence=0.95,
        )
        self._engine.insert(entity)
        actions = 1
        mutation_actions, mutation_paths = self._sync_receipt_mutations(
            receipt_id=receipt_id,
            receipt_entity_id=receipt_entity_id,
            ts=ts,
            inputs=inputs,
            outputs=outputs,
        )
        actions += mutation_actions
        actions += self._sync_receipt_verification(
            receipt_id=receipt_id,
            receipt_entity_id=receipt_entity_id,
            label=label,
            ts=ts,
            outputs=outputs,
        )
        affected_paths = sorted(set(mutation_paths) | set(verified_paths))
        actions += self._sync_receipt_failure(
            receipt_id=receipt_id,
            receipt_entity_id=receipt_entity_id,
            label=label,
            ts=ts,
            status=status,
            failure_code=failure_reason,
            outputs=outputs,
            affected_paths=affected_paths,
        )
        return actions

    def _sync_receipts(self) -> int:
        last_id, _ = self._get_watermark('receipts')
        rows = self._conn.execute(
            '''SELECT evidence_seq, receipt_id, run_id, node_id, status, started_at, finished_at,
                      failure_code, inputs, outputs
               FROM receipts
               WHERE evidence_seq > $1
               ORDER BY evidence_seq ASC
               LIMIT $2''',
            last_id, _BATCH_LIMIT
        )
        if not rows:
            return 0

        actions = 0
        processed = 0
        max_id = last_id
        max_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
        for r in rows:
            rid = int(r['evidence_seq'])
            max_id = max(max_id, rid)
            ts = r['finished_at'] or r['started_at'] or datetime.now(timezone.utc)
            if ts > max_ts:
                max_ts = ts
            processed += 1
            actions += self._sync_receipt_row(r)

        self._update_watermark('receipts', max_id, max_ts, processed)
        return actions

    def backfill_receipts(
        self,
        *,
        run_id: str | None = None,
        limit: int | None = None,
    ) -> dict[str, int | str | None]:
        params: list[object] = []
        where_clauses: list[str] = []
        if run_id:
            params.append(run_id)
            where_clauses.append(f"run_id = ${len(params)}")
        where = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
        limit_sql = ""
        if limit is not None:
            params.append(max(limit, 0))
            limit_sql = f" LIMIT ${len(params)}"
        rows = self._conn.execute(
            f"""SELECT evidence_seq, receipt_id, run_id, node_id, status, started_at, finished_at,
                       failure_code, inputs, outputs
                  FROM receipts
                  {where}
                 ORDER BY evidence_seq ASC
                 {limit_sql}""",
            *params,
        )
        actions = 0
        synced = 0
        for row in rows or []:
            actions += self._sync_receipt_row(row)
            synced += 1
        return {
            "run_id": run_id,
            "requested_limit": limit,
            "synced_receipts": synced,
            "actions": actions,
        }

    def _sync_bugs(self) -> int:
        _, last_ts = self._get_watermark('bugs')
        rows = self._conn.execute(
            '''SELECT bug_id, title, status, severity, category, description,
                      filed_by, assigned_to, tags, opened_at, updated_at, resolved_at
               FROM bugs WHERE updated_at > $1 ORDER BY updated_at ASC LIMIT $2''',
            last_ts, _BATCH_LIMIT
        )
        if not rows:
            return 0

        count = 0
        max_ts = last_ts
        for r in rows:
            ts = r['updated_at'] or datetime.now(timezone.utc)
            if ts > max_ts:
                max_ts = ts

            metadata = {
                'bug_id': r['bug_id'],
                'severity': r['severity'] or '',
                'status': r['status'] or '',
                'category': r['category'] or '',
                'filed_by': r['filed_by'] or '',
                'assigned_to': r['assigned_to'] or '',
                'tags': r['tags'] or '',
                'is_resolved': r['resolved_at'] is not None,
                'entity_subtype': 'bug',
            }

            entity = Entity(
                id=f"bug:{r['bug_id']}",
                entity_type=EntityType.task,
                name=r['title'] or '',
                content=r['description'] or r['title'] or '',
                metadata=metadata,
                created_at=r['opened_at'] or ts,
                updated_at=ts,
                source='bugs',
                confidence=0.9,
            )
            self._engine.insert(entity)
            count += 1

        self._update_watermark('bugs', 0, max_ts, count)
        return count

    def _sync_constraints(self) -> int:
        _, last_ts = self._get_watermark('workflow_constraints')
        rows = self._conn.execute(
            '''SELECT constraint_id, pattern, constraint_text, confidence,
                      mined_from_jobs, created_at
               FROM workflow_constraints WHERE created_at > $1
               ORDER BY created_at ASC LIMIT $2''',
            last_ts, _BATCH_LIMIT
        )
        if not rows:
            return 0

        count = 0
        max_ts = last_ts
        for r in rows:
            ts = r['created_at'] or datetime.now(timezone.utc)
            if ts > max_ts:
                max_ts = ts

            mined_jobs = (r['mined_from_jobs'] or '').split(',')
            mined_jobs = [j.strip() for j in mined_jobs if j.strip()]

            entity = Entity(
                id=f"constraint:{r['constraint_id']}",
                entity_type=EntityType.constraint,
                name=r['pattern'] or '',
                content=r['constraint_text'] or '',
                metadata={
                    'confidence': float(r['confidence'] or 0),
                    'mined_from_jobs': mined_jobs,
                    'entity_subtype': 'dispatch_constraint',
                },
                created_at=ts,
                updated_at=ts,
                source='constraints',
                confidence=float(r['confidence'] or 0.5),
            )
            self._engine.insert(entity)

            for job_label in mined_jobs:
                receipt_rows = self._conn.execute(
                    """SELECT receipt_id
                       FROM receipts
                       WHERE inputs->>'job_label' = $1
                       ORDER BY evidence_seq DESC
                       LIMIT 1""",
                    job_label,
                )
                if receipt_rows:
                    edge = Edge(
                        source_id=f"constraint:{r['constraint_id']}",
                        target_id=self._receipt_entity_id(receipt_rows[0]['receipt_id']),
                        relation_type=RelationType.derived_from,
                        weight=float(r['confidence'] or 0.5),
                        metadata={},
                        created_at=ts,
                    )
                    self._engine.add_edge(edge)
            count += 1

        self._update_watermark('workflow_constraints', 0, max_ts, count)
        return count

    def _sync_friction(self) -> int:
        _, last_ts = self._get_watermark('friction_events')
        rows = self._conn.execute(
            '''SELECT event_id, friction_type, source, job_label, message, timestamp
               FROM friction_events
               WHERE timestamp > $1 AND is_test = false
               ORDER BY timestamp ASC LIMIT $2''',
            last_ts, _BATCH_LIMIT
        )
        if not rows:
            return 0

        count = 0
        max_ts = last_ts
        for r in rows:
            ts = r['timestamp'] or datetime.now(timezone.utc)
            if ts > max_ts:
                max_ts = ts

            entity = Entity(
                id=f"friction:{r['event_id']}",
                entity_type=EntityType.fact,
                name=f"{r['friction_type']}: {r['source']}",
                content=r['message'] or '',
                metadata={
                    'friction_type': r['friction_type'] or '',
                    'source': r['source'] or '',
                    'job_label': r['job_label'] or '',
                    'entity_subtype': 'friction_event',
                },
                created_at=ts,
                updated_at=ts,
                source='friction',
                confidence=0.9,
            )
            self._engine.insert(entity)

            job_label = r['job_label'] or ''
            if job_label:
                receipt_rows = self._conn.execute(
                    """SELECT receipt_id
                       FROM receipts
                       WHERE inputs->>'job_label' = $1
                       ORDER BY evidence_seq DESC
                       LIMIT 1""",
                    job_label,
                )
                if receipt_rows:
                    edge = Edge(
                        source_id=f"friction:{r['event_id']}",
                        target_id=self._receipt_entity_id(receipt_rows[0]['receipt_id']),
                        relation_type=RelationType.caused_by,
                        weight=0.8,
                        metadata={},
                        created_at=ts,
                    )
                    self._engine.add_edge(edge)
            count += 1

        self._update_watermark('friction_events', 0, max_ts, count)
        return count

    def _sync_operator_decisions(self) -> int:
        _, last_ts = self._get_watermark('operator_decisions')
        rows = self._conn.execute(
            '''SELECT operator_decision_id, decision_key, decision_kind,
                      decision_status, title, rationale, decided_by,
                      decision_source, effective_from, effective_to, decided_at,
                      decision_scope_kind, decision_scope_ref
               FROM operator_decisions WHERE decided_at > $1
               ORDER BY decided_at ASC LIMIT $2''',
            last_ts, _BATCH_LIMIT
        )
        if not rows:
            return 0

        count = 0
        max_ts = last_ts
        for r in rows:
            ts = r['decided_at'] or datetime.now(timezone.utc)
            if ts > max_ts:
                max_ts = ts

            entity = Entity(
                id=f"opdec:{r['operator_decision_id']}",
                entity_type=EntityType.decision,
                name=r['title'] or r['decision_key'] or '',
                content=r['rationale'] or '',
                metadata={
                    'decision_key': r['decision_key'] or '',
                    'decision_kind': r['decision_kind'] or '',
                    'decision_status': r['decision_status'] or '',
                    'decided_by': r['decided_by'] or '',
                    'decision_source': r['decision_source'] or '',
                    'effective_from': (r['effective_from'] or '').isoformat() if r['effective_from'] else '',
                    'effective_to': (r['effective_to'] or '').isoformat() if r['effective_to'] else '',
                    'decision_scope_kind': r['decision_scope_kind'] or '',
                    'decision_scope_ref': r['decision_scope_ref'] or '',
                    'entity_subtype': 'operator_decision',
                },
                created_at=ts,
                updated_at=ts,
                source='operator',
                confidence=0.95,
            )
            self._engine.insert(entity)
            count += 1

        self._update_watermark('operator_decisions', 0, max_ts, count)
        return count
