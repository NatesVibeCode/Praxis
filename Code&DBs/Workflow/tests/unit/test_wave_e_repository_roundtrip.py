from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping, Iterator
from datetime import datetime, timezone

from storage.postgres.command_repository import PostgresCommandRepository
from storage.postgres.evidence_repository import PostgresEvidenceRepository
from storage.postgres.receipt_repository import PostgresReceiptRepository
from storage.postgres.subscription_repository import PostgresSubscriptionRepository


class _Record:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def __iter__(self):
        return iter(self._payload.items())


class _CommandConn:
    def __init__(self) -> None:
        self.by_id: dict[str, dict[str, object]] = {}
        self.by_key: dict[str, dict[str, object]] = {}

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if "INSERT INTO control_commands" in normalized:
            row = {
                "command_id": args[0],
                "command_type": args[1],
                "command_status": args[2],
                "requested_by_kind": args[3],
                "requested_by_ref": args[4],
                "requested_at": args[5],
                "approved_at": args[6],
                "approved_by": args[7],
                "idempotency_key": args[8],
                "risk_level": args[9],
                "payload": json.loads(args[10]),
                "result_ref": args[11],
                "error_code": args[12],
                "error_detail": args[13],
                "created_at": args[14],
                "updated_at": args[15],
            }
            if row["idempotency_key"] in self.by_key:
                return []
            self.by_id[str(row["command_id"])] = row
            self.by_key[str(row["idempotency_key"])] = row
            return [dict(row)]
        if "UPDATE control_commands" in normalized:
            row = self.by_id.get(str(args[0]))
            if row is None:
                return []
            updated = dict(row)
            updated["command_status"] = args[1]
            updated["approved_at"] = args[2]
            updated["approved_by"] = args[3]
            updated["payload"] = json.loads(args[4])
            updated["result_ref"] = args[5]
            updated["error_code"] = args[6]
            updated["error_detail"] = args[7]
            self.by_id[str(args[0])] = updated
            self.by_key[str(updated["idempotency_key"])] = updated
            return [dict(updated)]
        return []


class _SubscriptionConn:
    def __init__(self) -> None:
        self.subscriptions: dict[str, dict[str, object]] = {}
        self.checkpoints: dict[tuple[str, str], dict[str, object]] = {}
        self.system_events: list[dict[str, object]] = []
        self.trigger_ids = {"trigger.wave_e"}

    def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if "INSERT INTO public.event_subscriptions" in normalized:
            row = {
                "subscription_id": args[0],
                "subscription_name": args[1],
                "consumer_kind": args[2],
                "envelope_kind": args[3],
                "workflow_id": args[4],
                "run_id": args[5],
                "cursor_scope": args[6],
                "status": args[7],
                "delivery_policy": json.loads(args[8]),
                "filter_policy": json.loads(args[9]),
                "created_at": args[10],
            }
            self.subscriptions[str(args[0])] = row
            return dict(row)
        if "INSERT INTO public.subscription_checkpoints" in normalized:
            row = {
                "checkpoint_id": args[0],
                "subscription_id": args[1],
                "run_id": args[2],
                "last_evidence_seq": args[3],
                "last_authority_id": args[4],
                "checkpoint_status": args[5],
                "checkpointed_at": args[6],
                "metadata": json.loads(args[7]),
            }
            self.checkpoints[(str(args[1]), str(args[2]))] = row
            return dict(row)
        if "INSERT INTO public.system_events" in normalized:
            row = {
                "id": len(self.system_events) + 1,
                "event_type": args[0],
                "source_id": args[1],
                "source_type": args[2],
                "payload": json.loads(args[3]),
            }
            self.system_events.append(row)
            return {"id": row["id"]}
        return None

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if "UPDATE public.workflow_triggers" in normalized:
            trigger_id = str(args[0])
            return [{"id": trigger_id}] if trigger_id in self.trigger_ids else []
        return []


class _RecordCommandConn(_CommandConn):
    def execute(self, query: str, *args):
        rows = super().execute(query, *args)
        if not rows:
            return rows
        return [_Record(dict(rows[0]))]


class _RecordSubscriptionConn(_SubscriptionConn):
    def fetchrow(self, query: str, *args):
        row = super().fetchrow(query, *args)
        if row is None:
            return None
        return _Record(dict(row))


class _EvidenceConn:
    def __init__(self) -> None:
        self.workflow_definitions: dict[str, dict[str, object]] = {}
        self.admission_decisions: dict[str, dict[str, object]] = {}
        self.workflow_runs: dict[str, dict[str, object]] = {}
        self.workflow_events: dict[str, dict[str, object]] = {}
        self.receipts: dict[str, dict[str, object]] = {}

    async def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if "INSERT INTO workflow_definitions" in normalized:
            self.workflow_definitions.setdefault(
                str(args[0]),
                {
                    "workflow_definition_id": args[0],
                    "workflow_id": args[1],
                    "definition_hash": args[2],
                    "request_envelope": json.loads(args[3]),
                    "created_at": args[4],
                },
            )
            return "INSERT 0 1"
        if "INSERT INTO admission_decisions" in normalized:
            self.admission_decisions.setdefault(
                str(args[0]),
                {
                    "admission_decision_id": args[0],
                    "workflow_id": args[1],
                    "request_id": args[2],
                    "decided_at": args[3],
                    "authority_context_ref": args[4],
                },
            )
            return "INSERT 0 1"
        if "INSERT INTO workflow_runs" in normalized:
            self.workflow_runs.setdefault(
                str(args[0]),
                {
                    "run_id": args[0],
                    "workflow_id": args[1],
                    "request_id": args[2],
                    "request_digest": args[3],
                    "authority_context_digest": args[4],
                    "workflow_definition_id": args[5],
                    "admitted_definition_hash": args[6],
                    "run_idempotency_key": args[7],
                    "request_envelope": json.loads(args[8]),
                    "context_bundle_id": args[9],
                    "admission_decision_id": args[10],
                    "current_state": args[11],
                    "requested_at": args[12],
                    "admitted_at": args[13],
                    "terminal_reason_code": None,
                    "finished_at": None,
                    "last_event_id": None,
                },
            )
            return "INSERT 0 1"
        if "INSERT INTO workflow_events" in normalized:
            self.workflow_events.setdefault(
                str(args[0]),
                {
                    "event_id": args[0],
                    "event_type": args[1],
                    "schema_version": args[2],
                    "workflow_id": args[3],
                    "run_id": args[4],
                    "request_id": args[5],
                    "causation_id": args[6],
                    "node_id": args[7],
                    "occurred_at": args[8],
                    "evidence_seq": args[9],
                    "actor_type": args[10],
                    "reason_code": args[11],
                    "payload": json.loads(args[12]),
                },
            )
            return "INSERT 0 1"
        if "INSERT INTO receipts" in normalized:
            self.receipts.setdefault(
                str(args[0]),
                {
                    "receipt_id": args[0],
                    "receipt_type": args[1],
                    "workflow_id": args[3],
                    "run_id": args[4],
                    "request_id": args[5],
                    "node_id": args[7],
                    "attempt_no": args[8],
                    "status": args[14],
                    "outputs": json.loads(args[16]),
                    "artifacts": json.loads(args[17]),
                    "decision_refs": json.loads(args[19]),
                },
            )
            return "INSERT 0 1"
        if "UPDATE workflow_runs" in normalized:
            row = self.workflow_runs.get(str(args[0]))
            if row is None:
                return "UPDATE 0"
            row["current_state"] = args[1]
            row["terminal_reason_code"] = args[2]
            row["finished_at"] = args[3]
            row["last_event_id"] = args[4]
            return "UPDATE 1"
        return "OK"


class _FrozenMapping(Mapping[str, object]):
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def __getitem__(self, key: str) -> object:
        return self._payload[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._payload)

    def __len__(self) -> int:
        return len(self._payload)


class _ReceiptConn:
    def __init__(self) -> None:
        self.receipts: dict[str, dict[str, object]] = {
            "receipt.wave_e": {
                "receipt_id": "receipt.wave_e",
                "inputs": {"old": True},
                "outputs": {"old": True},
            }
        }
        self.job_completed: list[str] = []
        self.runtime_context: dict[tuple[str, str], dict[str, object]] = {}

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if "UPDATE receipts" in normalized and "RETURNING receipt_id" in normalized:
            row = self.receipts.get(str(args[0]))
            if row is None:
                return []
            row["inputs"] = json.loads(args[1])
            row["outputs"] = json.loads(args[2])
            return [{"receipt_id": args[0]}]
        if "SELECT pg_notify('job_completed', $1)" in normalized:
            self.job_completed.append(str(args[0]))
            return []
        if "INSERT INTO workflow_job_runtime_context" in normalized:
            self.runtime_context[(str(args[0]), str(args[1]))] = {
                "run_id": args[0],
                "job_label": args[1],
                "workflow_id": args[2],
                "execution_context_shard": json.loads(args[3]),
                "execution_bundle": json.loads(args[4]),
            }
            return []
        if "INSERT INTO receipts" in normalized:
            if len(args) == 15:
                self.receipts[str(args[0])] = {
                    "receipt_id": args[0],
                    "status": args[9],
                    "artifacts": json.loads(args[12]),
                    "decision_refs": json.loads(args[14]),
                }
            else:
                self.receipts[str(args[0])] = {
                    "receipt_id": args[0],
                    "status": args[12],
                    "artifacts": json.loads(args[15]),
                    "decision_refs": json.loads(args[17]),
                }
            return []
        return []


def test_command_repository_round_trip_persists_and_updates_control_commands() -> None:
    conn = _CommandConn()
    repository = PostgresCommandRepository(conn)
    requested_at = datetime(2026, 4, 14, 12, 0, tzinfo=timezone.utc)

    inserted = repository.insert_control_command(
        command_id="cmd.wave_e",
        command_type="workflow.submit",
        command_status="requested",
        requested_by_kind="operator",
        requested_by_ref="operator.console",
        requested_at=requested_at,
        approved_at=None,
        approved_by=None,
        idempotency_key="idem.wave_e",
        risk_level="low",
        payload={"spec_path": "wave_e.json"},
        result_ref=None,
        error_code=None,
        error_detail=None,
        created_at=requested_at,
        updated_at=requested_at,
    )
    updated = repository.update_control_command(
        command_id="cmd.wave_e",
        command_status="accepted",
        approved_at=requested_at,
        approved_by="operator.console",
        payload={"spec_path": "wave_e.json", "approved": True},
        result_ref="run.wave_e",
        error_code=None,
        error_detail=None,
    )

    assert inserted is not None
    assert inserted["command_status"] == "requested"
    assert updated["command_status"] == "accepted"
    assert updated["approved_by"] == "operator.console"
    assert updated["payload"] == {"spec_path": "wave_e.json", "approved": True}
    assert updated["result_ref"] == "run.wave_e"


def test_command_repository_accepts_record_like_rows() -> None:
    conn = _RecordCommandConn()
    repository = PostgresCommandRepository(conn)
    requested_at = datetime(2026, 4, 14, 12, 5, tzinfo=timezone.utc)

    inserted = repository.insert_control_command(
        command_id="cmd.wave_e.record",
        command_type="workflow.submit",
        command_status="requested",
        requested_by_kind="operator",
        requested_by_ref="operator.console",
        requested_at=requested_at,
        approved_at=None,
        approved_by=None,
        idempotency_key="idem.wave_e.record",
        risk_level="low",
        payload={"spec_path": "wave_e.json"},
        result_ref=None,
        error_code=None,
        error_detail=None,
        created_at=requested_at,
        updated_at=requested_at,
    )

    assert inserted is not None
    assert inserted["command_id"] == "cmd.wave_e.record"
    assert inserted["payload"] == {"spec_path": "wave_e.json"}


def test_subscription_repository_round_trip_persists_subscription_checkpoint_and_event() -> None:
    conn = _SubscriptionConn()
    repository = PostgresSubscriptionRepository(conn)
    created_at = datetime(2026, 4, 14, 12, 15, tzinfo=timezone.utc)

    definition = repository.upsert_event_subscription(
        subscription_id="subscription.wave_e",
        subscription_name="Wave E Subscription",
        consumer_kind="system",
        envelope_kind="system_event",
        workflow_id=None,
        run_id=None,
        cursor_scope="global",
        status="active",
        delivery_policy={"mode": "replay"},
        filter_policy={"event_type": "workflow.completed"},
        created_at=created_at,
    )
    checkpoint = repository.upsert_subscription_checkpoint(
        subscription_id="subscription.wave_e",
        run_id="run.wave_e",
        last_evidence_seq=9,
        last_authority_id=None,
        checkpoint_status="committed",
        metadata={"cursor": 9},
        checkpointed_at=created_at,
    )
    fired = repository.increment_workflow_trigger_fire_count(trigger_id="trigger.wave_e")
    event_id = repository.insert_system_event(
        event_type="workflow.completed",
        source_id="run.wave_e",
        source_type="workflow_run",
        payload={"status": "succeeded"},
    )

    assert definition["subscription_id"] == "subscription.wave_e"
    assert checkpoint["checkpoint_id"] == "checkpoint:subscription.wave_e:run.wave_e"
    assert checkpoint["last_authority_id"] == "system_event:9"
    assert fired is True
    assert event_id == 1
    assert conn.system_events[0]["payload"] == {"status": "succeeded"}


def test_subscription_repository_accepts_record_like_rows() -> None:
    conn = _RecordSubscriptionConn()
    repository = PostgresSubscriptionRepository(conn)
    created_at = datetime(2026, 4, 14, 12, 20, tzinfo=timezone.utc)

    definition = repository.upsert_event_subscription(
        subscription_id="subscription.wave_e.record",
        subscription_name="Wave E Record Subscription",
        consumer_kind="system",
        envelope_kind="system_event",
        workflow_id=None,
        run_id=None,
        cursor_scope="global",
        status="active",
        delivery_policy={"mode": "replay"},
        filter_policy={"event_type": "workflow.completed"},
        created_at=created_at,
    )

    assert definition["subscription_id"] == "subscription.wave_e.record"
    assert definition["delivery_policy"] == {"mode": "replay"}


def test_evidence_repository_round_trip_persists_workflow_rows_and_terminal_state() -> None:
    conn = _EvidenceConn()
    repository = PostgresEvidenceRepository(conn)
    now = datetime(2026, 4, 14, 12, 30, tzinfo=timezone.utc)

    async def _exercise() -> None:
        await repository.insert_workflow_definition_if_absent(
            workflow_definition_id="def.wave_e",
            workflow_id="workflow.wave_e",
            definition_hash="hash.wave_e",
            request_envelope={"objective": "ship wave e"},
            created_at=now,
        )
        await repository.insert_admission_decision_if_absent(
            admission_decision_id="decision.wave_e",
            workflow_id="workflow.wave_e",
            request_id="request.wave_e",
            decided_at=now,
            authority_context_ref="authority.wave_e",
        )
        await repository.insert_workflow_run_if_absent(
            run_id="run.wave_e",
            workflow_id="workflow.wave_e",
            request_id="request.wave_e",
            request_digest="digest.wave_e",
            authority_context_digest="authority-digest.wave_e",
            workflow_definition_id="def.wave_e",
            admitted_definition_hash="hash.wave_e",
            run_idempotency_key="idem.wave_e",
            request_envelope={"objective": "ship wave e"},
            context_bundle_id="bundle.wave_e",
            admission_decision_id="decision.wave_e",
            current_state="requested",
            requested_at=now,
            admitted_at=now,
        )
        await repository.insert_workflow_event_if_absent(
            event_id="evt.wave_e",
            event_type="workflow.started",
            schema_version=1,
            workflow_id="workflow.wave_e",
            run_id="run.wave_e",
            request_id="request.wave_e",
            causation_id=None,
            node_id="node.wave_e",
            occurred_at=now,
            evidence_seq=1,
            actor_type="runtime",
            reason_code=None,
            payload={"step": "start"},
        )
        await repository.insert_receipt_if_absent(
            receipt_id="receipt.wave_e",
            receipt_type="workflow_job",
            schema_version=1,
            workflow_id="workflow.wave_e",
            run_id="run.wave_e",
            request_id="request.wave_e",
            causation_id=None,
            node_id="node.wave_e",
            attempt_no=1,
            supersedes_receipt_id=None,
            started_at=now,
            finished_at=now,
            evidence_seq=1,
            executor_type="workflow_unified",
            status="succeeded",
            inputs={"job": "wave_e"},
            outputs={"result": "ok"},
            artifacts=[{"kind": "log", "ref": "artifact.wave_e"}],
            failure_code=None,
            decision_refs=[{"decision_ref": "decision.wave_e"}],
        )
        updated = await repository.update_workflow_run_state(
            run_id="run.wave_e",
            new_state="succeeded",
            terminal_reason_code="completed",
            finished_at=now,
            last_event_id="evt.wave_e",
            occurred_at=now,
        )
        assert updated is True

    asyncio.run(_exercise())

    assert conn.workflow_definitions["def.wave_e"]["request_envelope"] == {
        "objective": "ship wave e"
    }
    assert conn.workflow_runs["run.wave_e"]["current_state"] == "succeeded"
    assert conn.workflow_runs["run.wave_e"]["last_event_id"] == "evt.wave_e"
    assert conn.workflow_events["evt.wave_e"]["payload"] == {"step": "start"}
    assert conn.receipts["receipt.wave_e"]["decision_refs"] == [
        {"decision_ref": "decision.wave_e"}
    ]


def test_evidence_repository_normalizes_nested_mapping_payloads_for_json() -> None:
    conn = _EvidenceConn()
    repository = PostgresEvidenceRepository(conn)
    now = datetime(2026, 4, 14, 12, 35, tzinfo=timezone.utc)

    async def _exercise() -> None:
        await repository.insert_workflow_event_if_absent(
            event_id="evt.wave_e.frozen",
            event_type="claim.received",
            schema_version=1,
            workflow_id="workflow.wave_e",
            run_id="run.wave_e",
            request_id="request.wave_e",
            causation_id=None,
            node_id=None,
            occurred_at=now,
            evidence_seq=1,
            actor_type="runtime",
            reason_code=None,
            payload={
                "claim_envelope": _FrozenMapping(
                    {
                        "nodes": [
                            _FrozenMapping(
                                {
                                    "inputs": _FrozenMapping(
                                        {"input_payload": _FrozenMapping({"step": 0})}
                                    )
                                }
                            )
                        ]
                    }
                )
            },
        )

    asyncio.run(_exercise())

    assert conn.workflow_events["evt.wave_e.frozen"]["payload"] == {
        "claim_envelope": {
            "nodes": [
                {
                    "inputs": {
                        "input_payload": {
                            "step": 0,
                        }
                    }
                }
            ]
        }
    }


def test_evidence_repository_normalizes_frozen_request_envelopes_for_definition_and_run_rows() -> None:
    conn = _EvidenceConn()
    repository = PostgresEvidenceRepository(conn)
    now = datetime(2026, 4, 15, 3, 50, tzinfo=timezone.utc)
    request_envelope = {
        "workflow_id": "workflow.control_operator",
        "nodes": [
            _FrozenMapping(
                {
                    "node_id": "route_if",
                    "inputs": _FrozenMapping(
                        {
                            "operator": _FrozenMapping(
                                {
                                    "kind": "if",
                                    "predicate": _FrozenMapping(
                                        {"field": "flag", "op": "equals", "value": True}
                                    ),
                                }
                            )
                        }
                    ),
                }
            )
        ],
        "edges": [
            _FrozenMapping(
                {
                    "edge_id": "edge.route_if.then",
                    "release_condition": _FrozenMapping(
                        {"kind": "branch_selected", "branch": "then"}
                    ),
                }
            )
        ],
    }

    async def _exercise() -> None:
        await repository.insert_workflow_definition_if_absent(
            workflow_definition_id="def.control_operator",
            workflow_id="workflow.control_operator",
            definition_hash="sha256:def",
            request_envelope=request_envelope,
            created_at=now,
        )
        await repository.insert_workflow_run_if_absent(
            run_id="run.control_operator",
            workflow_id="workflow.control_operator",
            request_id="request.control_operator",
            request_digest="digest.control_operator",
            authority_context_digest="authority.control_operator",
            workflow_definition_id="def.control_operator",
            admitted_definition_hash="sha256:def",
            run_idempotency_key="idem.control_operator",
            request_envelope=request_envelope,
            context_bundle_id="bundle.control_operator",
            admission_decision_id="decision.control_operator",
            current_state="claim_accepted",
            requested_at=now,
            admitted_at=now,
        )

    asyncio.run(_exercise())

    assert conn.workflow_definitions["def.control_operator"]["request_envelope"] == {
        "workflow_id": "workflow.control_operator",
        "nodes": [
            {
                "node_id": "route_if",
                "inputs": {
                    "operator": {
                        "kind": "if",
                        "predicate": {
                            "field": "flag",
                            "op": "equals",
                            "value": True,
                        },
                    }
                },
            }
        ],
        "edges": [
            {
                "edge_id": "edge.route_if.then",
                "release_condition": {
                    "kind": "branch_selected",
                    "branch": "then",
                },
            }
        ],
    }
    assert conn.workflow_runs["run.control_operator"]["request_envelope"] == conn.workflow_definitions[
        "def.control_operator"
    ]["request_envelope"]


def test_receipt_repository_round_trip_updates_payloads_and_runtime_context() -> None:
    conn = _ReceiptConn()
    repository = PostgresReceiptRepository(conn)
    now = datetime(2026, 4, 14, 12, 45, tzinfo=timezone.utc)

    updated = repository.update_receipt_payloads(
        receipt_id="receipt.wave_e",
        inputs={"job": "wave_e"},
        outputs={"status": "ok"},
    )
    inserted = repository.insert_receipt_if_absent(
        receipt_id="receipt.wave_e.2",
        workflow_id="workflow.wave_e",
        run_id="run.wave_e",
        request_id="request.wave_e",
        node_id="node.wave_e",
        attempt_no=2,
        started_at=now,
        finished_at=now,
        evidence_seq=2,
        status="succeeded",
        inputs={"job": "wave_e"},
        outputs={"status": "ok"},
        artifacts={"log": "artifact.wave_e"},
        failure_code=None,
    )
    repository.notify_job_completed(run_id="run.wave_e")
    context_key = repository.upsert_workflow_job_runtime_context(
        run_id="run.wave_e",
        job_label="wave_e_tests",
        workflow_id="workflow.wave_e",
        execution_context_shard={"job_label": "wave_e_tests"},
        execution_bundle={"proof": "ready"},
    )

    assert updated is True
    assert inserted == "receipt.wave_e.2"
    assert conn.receipts["receipt.wave_e"]["outputs"] == {"status": "ok"}
    assert conn.job_completed == ["run.wave_e"]
    assert context_key == "wave_e_tests"
    assert conn.runtime_context[("run.wave_e", "wave_e_tests")] == {
        "run_id": "run.wave_e",
        "job_label": "wave_e_tests",
        "workflow_id": "workflow.wave_e",
        "execution_context_shard": {"job_label": "wave_e_tests"},
        "execution_bundle": {"proof": "ready"},
    }
