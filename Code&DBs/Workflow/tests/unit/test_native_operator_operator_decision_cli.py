from __future__ import annotations

import json
from io import StringIO

from surfaces.cli import native_operator
from surfaces.cli.main import main as workflow_cli_main


class _FakeInstance:
    def to_contract(self) -> dict[str, str]:
        return {"repo_root": "/tmp/repo", "workdir": "/tmp/repo"}


def test_native_operator_operator_decision_record_uses_shared_gate(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _record_operator_decision(**kwargs):
        captured.update(kwargs)
        return {
            "operator_decision": {
                "operator_decision_id": "operator_decision.architecture_policy.decision_tables.db_native_authority",
                "decision_key": kwargs["decision_key"],
                "decision_kind": kwargs["decision_kind"],
                "decision_status": kwargs["decision_status"],
                "title": kwargs["title"],
                "rationale": kwargs["rationale"],
                "decided_by": kwargs["decided_by"],
                "decision_source": kwargs["decision_source"],
                "decision_scope_kind": kwargs["decision_scope_kind"],
                "decision_scope_ref": kwargs["decision_scope_ref"],
                "effective_from": "2026-04-15T00:00:00+00:00",
                "effective_to": None,
                "decided_at": "2026-04-15T00:00:00+00:00",
                "created_at": "2026-04-15T00:00:00+00:00",
                "updated_at": "2026-04-15T00:00:00+00:00",
            }
        }

    monkeypatch.setattr(native_operator, "resolve_native_instance", lambda env=None: _FakeInstance())
    monkeypatch.setattr(
        native_operator.operator_write,
        "record_operator_decision",
        _record_operator_decision,
    )

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "native-operator",
                "operator-decision",
                "record",
                "--decision-key",
                "architecture-policy::decision-tables::db-native-authority",
                "--kind",
                "architecture_policy",
                "--title",
                "Decision tables are DB-native authority",
                "--rationale",
                "Keep authority in Postgres.",
                "--decided-by",
                "praxis-admin",
                "--decision-source",
                "cto.guidance",
                "--scope-kind",
                "authority_domain",
                "--scope-ref",
                "decision_tables",
            ],
            env={},
            stdout=stdout,
        )
        == 0
    )

    payload = json.loads(stdout.getvalue())
    assert captured["decision_kind"] == "architecture_policy"
    assert captured["decision_scope_kind"] == "authority_domain"
    assert captured["decision_scope_ref"] == "decision_tables"
    assert (
        payload["operator_decision"]["operator_decision_id"]
        == "operator_decision.architecture_policy.decision_tables.db_native_authority"
    )


def test_native_operator_operator_decision_list_uses_shared_gate(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def _list_operator_decisions(**kwargs):
        captured.update(kwargs)
        return {
            "operator_decisions": [
                {
                    "operator_decision_id": "operator_decision.architecture_policy.decision_tables.db_native_authority",
                    "decision_key": "architecture-policy::decision-tables::db-native-authority",
                    "decision_kind": "architecture_policy",
                    "decision_status": "decided",
                    "title": "Decision tables are DB-native authority",
                    "rationale": "Keep authority in Postgres.",
                    "decided_by": "praxis-admin",
                    "decision_source": "cto.guidance",
                    "decision_scope_kind": "authority_domain",
                    "decision_scope_ref": "decision_tables",
                    "effective_from": "2026-04-15T00:00:00+00:00",
                    "effective_to": None,
                    "decided_at": "2026-04-15T00:00:00+00:00",
                    "created_at": "2026-04-15T00:00:00+00:00",
                    "updated_at": "2026-04-15T00:00:00+00:00",
                }
            ],
            "as_of": "2026-04-15T00:00:00+00:00",
        }

    monkeypatch.setattr(native_operator, "resolve_native_instance", lambda env=None: _FakeInstance())
    monkeypatch.setattr(
        native_operator.operator_write,
        "list_operator_decisions",
        _list_operator_decisions,
    )

    stdout = StringIO()
    assert (
        workflow_cli_main(
            [
                "native-operator",
                "operator-decision",
                "list",
                "--kind",
                "architecture_policy",
                "--scope-kind",
                "authority_domain",
                "--scope-ref",
                "decision_tables",
            ],
            env={},
            stdout=stdout,
        )
        == 0
    )

    payload = json.loads(stdout.getvalue())
    assert captured["decision_kind"] == "architecture_policy"
    assert captured["decision_scope_kind"] == "authority_domain"
    assert payload["operator_decisions"][0]["decision_scope_ref"] == "decision_tables"
