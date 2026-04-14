from __future__ import annotations

import json
from datetime import datetime, timezone

from runtime.verification import resolve_verify_commands, sync_verify_refs
from storage.postgres.friction_repository import PostgresFrictionRepository
from storage.postgres.verification_repository import PostgresVerificationRepository


class _FrictionConn:
    def __init__(self) -> None:
        self.rows: list[dict[str, object]] = []

    def execute(self, query: str, *args) -> list[dict[str, object]]:
        q = " ".join(query.split())
        if "INSERT INTO friction_events" in q:
            self.rows.append(
                {
                    "event_id": args[0],
                    "friction_type": args[1],
                    "source": args[2],
                    "job_label": args[3],
                    "message": args[4],
                    "timestamp": args[5],
                    "is_test": args[6],
                    "embedding": None,
                }
            )
            return []

        if "SELECT event_id, friction_type, source, job_label, message, timestamp, is_test" in q:
            filtered = list(self.rows)
            arg_index = 0
            if "is_test = $1" in q:
                filtered = [row for row in filtered if row["is_test"] is False]
                arg_index += 1
            if "friction_type = $" in q:
                friction_type = args[arg_index]
                filtered = [row for row in filtered if row["friction_type"] == friction_type]
                arg_index += 1
            if "source = $" in q:
                source = args[arg_index]
                filtered = [row for row in filtered if row["source"] == source]
                arg_index += 1
            if "timestamp >= $" in q:
                since = args[arg_index]
                filtered = [row for row in filtered if row["timestamp"] >= since]
                arg_index += 1
            limit = args[arg_index]
            filtered.sort(key=lambda row: row["timestamp"], reverse=True)
            return [dict(row) for row in filtered[:limit]]

        if "SELECT friction_type, source" in q:
            rows = self.rows
            if "WHERE is_test = false" in q:
                rows = [row for row in rows if row["is_test"] is False]
            return [
                {"friction_type": row["friction_type"], "source": row["source"]}
                for row in rows
            ]

        if "SELECT friction_type" in q and "timestamp >= $1" in q:
            since = args[0]
            rows = [row for row in self.rows if row["timestamp"] >= since]
            if "AND is_test = false" in q:
                rows = [row for row in rows if row["is_test"] is False]
            return [{"friction_type": row["friction_type"]} for row in rows]

        return []


class _VerificationConn:
    def __init__(self) -> None:
        self.capability_outcomes: list[dict[str, object]] = []
        self.verify_refs: dict[str, dict[str, object]] = {}
        self.verification_registry: dict[str, dict[str, object]] = {
            "verification.python.py_compile": {
                "verification_ref": "verification.python.py_compile",
                "display_name": "Python Bytecode Compile",
                "executor_kind": "argv",
                "argv_template": ["python3", "-m", "py_compile", "{path}"],
                "template_inputs": ["path"],
                "default_timeout_seconds": 45,
                "enabled": True,
            }
        }

    def execute(self, query: str, *args) -> list[dict[str, object]]:
        q = " ".join(query.split())
        if "INSERT INTO capability_outcomes" in q:
            self.capability_outcomes.append(
                {
                    "run_id": args[0],
                    "provider_slug": args[1],
                    "model_slug": args[2],
                    "inferred_capabilities": list(args[3]),
                    "succeeded": args[4],
                    "output_quality_signals": args[5],
                    "recorded_at": args[6],
                }
            )
            return []

        if "FROM capability_outcomes" in q:
            rows = sorted(
                self.capability_outcomes,
                key=lambda row: row["recorded_at"],
                reverse=True,
            )
            return [dict(row) for row in rows]

        if "FROM verify_refs" in q:
            row = self.verify_refs.get(args[0])
            return [dict(row)] if row is not None else []

        if "FROM verification_registry" in q:
            requested = set(args[0])
            return [
                dict(row)
                for verification_ref, row in self.verification_registry.items()
                if verification_ref in requested
            ]

        return []

    def execute_many(self, query: str, rows: list[tuple[object, ...]]) -> None:
        q = " ".join(query.split())
        if "INSERT INTO verify_refs" not in q:
            return
        for row in rows:
            self.verify_refs[str(row[0])] = {
                "verify_ref": row[0],
                "verification_ref": row[1],
                "label": row[2],
                "description": row[3],
                "inputs": row[4],
                "enabled": row[5],
                "binding_revision": row[6],
                "decision_ref": row[7],
            }


def test_friction_repository_round_trip_records_and_reads_events() -> None:
    conn = _FrictionConn()
    repository = PostgresFrictionRepository(conn)
    now = datetime(2026, 4, 14, 10, 30, tzinfo=timezone.utc)

    repository.record_friction_event(
        event_id="evt_live",
        friction_type="guardrail_bounce",
        source="wave_f",
        job_label="wave_f_tests",
        message="live event",
        timestamp=now,
        is_test=False,
    )
    repository.record_friction_event(
        event_id="evt_test",
        friction_type="hard_failure",
        source="wave_f",
        job_label="wave_f_tests",
        message="test-only event",
        timestamp=now,
        is_test=True,
    )

    listed = repository.list_friction_events(
        friction_type="guardrail_bounce",
        source="wave_f",
        since=now,
        include_test=False,
    )
    type_source_rows = repository.list_type_source_rows(include_test=False)
    recent_type_rows = repository.list_type_rows_since(since=now, include_test=False)

    assert [row["event_id"] for row in listed] == ["evt_live"]
    assert type_source_rows == [
        {"friction_type": "guardrail_bounce", "source": "wave_f"}
    ]
    assert recent_type_rows == [{"friction_type": "guardrail_bounce"}]


def test_verification_repository_round_trip_persists_outcomes_and_verify_refs() -> None:
    conn = _VerificationConn()
    repository = PostgresVerificationRepository(conn)
    recorded_at = datetime(2026, 4, 14, 11, 0, tzinfo=timezone.utc)

    run_id = repository.record_capability_outcome(
        run_id="run.wave_f.1",
        provider_slug="openai",
        model_slug="gpt-test",
        inferred_capabilities=["code_generation", "debug"],
        succeeded=True,
        output_quality_signals={"code_generation": 0.9, "debug": 0.6},
        recorded_at=recorded_at,
    )
    upserted = repository.upsert_verify_refs(
        verify_refs=[
            {
                "verify_ref": "verify_ref.python.py_compile.wave_f",
                "verification_ref": "verification.python.py_compile",
                "label": "Compile wave_f.py",
                "description": "Compile the Wave F sample module",
                "inputs": {"path": "wave_f.py"},
                "enabled": True,
                "binding_revision": "binding.wave_f",
                "decision_ref": "decision.wave_f",
            }
        ]
    )

    rows = repository.list_capability_outcomes()
    verify_row = repository.load_verify_ref(
        verify_ref="verify_ref.python.py_compile.wave_f"
    )

    assert run_id == "run.wave_f.1"
    assert upserted == 1
    assert len(rows) == 1
    assert rows[0]["run_id"] == "run.wave_f.1"
    assert rows[0]["inferred_capabilities"] == ["code_generation", "debug"]
    assert json.loads(rows[0]["output_quality_signals"]) == {
        "code_generation": 0.9,
        "debug": 0.6,
    }
    assert verify_row is not None
    assert json.loads(str(verify_row["inputs"])) == {"path": "wave_f.py"}
    assert verify_row["verification_ref"] == "verification.python.py_compile"


def test_verification_authority_round_trip_syncs_and_resolves_verify_refs() -> None:
    conn = _VerificationConn()

    persisted = sync_verify_refs(
        conn,
        verify_refs=[
            {
                "verify_ref": "verify_ref.python.py_compile.wave_f",
                "verification_ref": "verification.python.py_compile",
                "label": "Compile wave_f.py",
                "description": "Compile the Wave F sample module",
                "inputs": {"path": "wave_f.py"},
                "enabled": True,
                "binding_revision": "binding.wave_f",
                "decision_ref": "decision.wave_f",
            }
        ],
    )
    commands = resolve_verify_commands(
        conn,
        ["verify_ref.python.py_compile.wave_f"],
    )

    assert persisted == 1
    assert len(commands) == 1
    assert commands[0].verification_ref == "verification.python.py_compile"
    assert commands[0].argv == ("python3", "-m", "py_compile", "wave_f.py")
    assert commands[0].label == "Compile wave_f.py"
    assert commands[0].timeout == 45
