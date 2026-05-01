from __future__ import annotations

from runtime.operations.queries import workflow_context


class _Subsystems:
    def get_pg_conn(self) -> object:
        return object()


def test_workflow_context_read_hydrates_list_results_when_include_flags_are_set(monkeypatch) -> None:
    load_calls: list[dict[str, object]] = []

    def _list_context_packs(conn, *, workflow_ref, truth_state, limit):
        return [
            {
                "context_ref": "workflow_context:renewal:abc123",
                "workflow_ref": workflow_ref,
                "truth_state": truth_state or "synthetic",
            }
        ]

    def _load_context_pack(conn, *, context_ref, include_entities, include_bindings, include_transitions):
        load_calls.append(
            {
                "context_ref": context_ref,
                "include_entities": include_entities,
                "include_bindings": include_bindings,
                "include_transitions": include_transitions,
            }
        )
        return {
            "context_ref": context_ref,
            "entities": [{"entity_kind": "object", "label": "Account"}],
            "bindings": [],
        }

    monkeypatch.setattr(workflow_context, "list_context_packs", _list_context_packs)
    monkeypatch.setattr(workflow_context, "load_context_pack", _load_context_pack)

    result = workflow_context.handle_workflow_context_read(
        workflow_context.QueryWorkflowContextRead(
            workflow_ref="workflow.renewal",
            include_transitions=False,
        ),
        _Subsystems(),
    )

    assert result["ok"] is True
    assert result["context_packs"] == [
        {
            "context_ref": "workflow_context:renewal:abc123",
            "entities": [{"entity_kind": "object", "label": "Account"}],
            "bindings": [],
        }
    ]
    assert load_calls == [
        {
            "context_ref": "workflow_context:renewal:abc123",
            "include_entities": True,
            "include_bindings": True,
            "include_transitions": False,
        }
    ]


def test_workflow_context_read_can_return_lightweight_list_results(monkeypatch) -> None:
    loaded: list[str] = []

    def _list_context_packs(conn, *, workflow_ref, truth_state, limit):
        return [{"context_ref": "workflow_context:crm:abc123", "workflow_ref": workflow_ref}]

    def _load_context_pack(*args, **kwargs):
        loaded.append("unexpected")
        return {}

    monkeypatch.setattr(workflow_context, "list_context_packs", _list_context_packs)
    monkeypatch.setattr(workflow_context, "load_context_pack", _load_context_pack)

    result = workflow_context.handle_workflow_context_read(
        workflow_context.QueryWorkflowContextRead(
            workflow_ref="workflow.crm",
            include_entities=False,
            include_bindings=False,
            include_transitions=False,
        ),
        _Subsystems(),
    )

    assert result["context_packs"] == [
        {"context_ref": "workflow_context:crm:abc123", "workflow_ref": "workflow.crm"}
    ]
    assert loaded == []
