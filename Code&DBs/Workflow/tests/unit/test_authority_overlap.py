"""Unit tests for runtime-derived authority overlap discovery."""

from __future__ import annotations

from runtime.workflow.authority_overlap import (
    classify_path,
    discover_authority_overlap,
    is_authority_bearing,
)


def test_classify_path_recognizes_migration_directory() -> None:
    assert classify_path("Code&DBs/Databases/migrations/workflow/342_foo.sql") == "migration_ref"


def test_classify_path_recognizes_handler_command_directory() -> None:
    assert (
        classify_path("Code&DBs/Workflow/runtime/operations/commands/candidate_preflight.py")
        == "handler_ref"
    )


def test_classify_path_recognizes_mcp_tools_directory() -> None:
    assert (
        classify_path("Code&DBs/Workflow/surfaces/mcp/tools/code_change_candidate.py")
        == "mcp_tool"
    )


def test_classify_path_recognizes_api_surface() -> None:
    assert classify_path("Code&DBs/Workflow/surfaces/api/workflow_submission.py") == "http_route"


def test_classify_path_returns_none_for_unrelated_paths() -> None:
    assert classify_path("Code&DBs/Workflow/contracts/operation_catalog.py") is None
    assert classify_path("README.md") is None
    assert classify_path("") is None


def test_is_authority_bearing_true_for_any_authority_path() -> None:
    assert is_authority_bearing(
        [
            "README.md",
            "Code&DBs/Databases/migrations/workflow/342_foo.sql",
        ]
    )


def test_is_authority_bearing_false_for_no_authority_paths() -> None:
    assert not is_authority_bearing(["README.md", "docs/something.md"])


def test_is_authority_bearing_false_for_empty() -> None:
    assert not is_authority_bearing([])


_REGISTER_OPERATION_MIGRATION_BODY = """
BEGIN;

CREATE TABLE IF NOT EXISTS sample_authority_thing (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid()
);

INSERT INTO authority_event_contracts (
    event_contract_ref, event_type, authority_domain_ref
) VALUES (
    'event_contract.sample.thing_committed',
    'sample.thing_committed',
    'authority.workflow_runs'
)
ON CONFLICT (authority_domain_ref, event_type) DO NOTHING;

SELECT register_operation_atomic(
    p_operation_ref         := 'sample-thing-commit',
    p_operation_name        := 'sample.thing_commit',
    p_handler_ref           := 'runtime.operations.commands.sample_thing.handle',
    p_input_model_ref       := 'runtime.operations.commands.sample_thing.SampleCommand',
    p_authority_domain_ref  := 'authority.workflow_runs',
    p_operation_kind        := 'command',
    p_event_type            := 'sample.thing_committed',
    p_event_required        := TRUE
);

COMMIT;
"""


def test_discover_authority_overlap_parses_migration_for_operation_event_and_table() -> None:
    intended_files = ["Code&DBs/Databases/migrations/workflow/999_sample_thing.sql"]
    impacts = discover_authority_overlap(
        intended_files=intended_files,
        file_contents={intended_files[0]: _REGISTER_OPERATION_MIGRATION_BODY},
    )
    keys = {(impact.unit_kind, impact.unit_ref) for impact in impacts}
    assert ("migration_ref", "999_sample_thing.sql") in keys
    assert ("operation_ref", "sample.thing_commit") in keys
    assert ("event_type", "sample.thing_committed") in keys
    assert ("database_object", "sample_authority_thing") in keys


_DROP_TABLE_MIGRATION_BODY = """
BEGIN;
DROP TABLE IF EXISTS legacy_thing;
COMMIT;
"""


def test_discover_authority_overlap_marks_drop_table_as_retire() -> None:
    intended_files = ["Code&DBs/Databases/migrations/workflow/998_drop_legacy_thing.sql"]
    impacts = discover_authority_overlap(
        intended_files=intended_files,
        file_contents={intended_files[0]: _DROP_TABLE_MIGRATION_BODY},
    )
    drop_impacts = [impact for impact in impacts if impact.unit_kind == "database_object"]
    assert len(drop_impacts) == 1
    assert drop_impacts[0].dispatch_effect == "retire"
    assert drop_impacts[0].intent_hint == "retire"
    assert drop_impacts[0].predecessor_unit_kind == "database_object"
    assert drop_impacts[0].predecessor_unit_ref == "legacy_thing"


_PYTHON_HANDLER_BODY = """
from pydantic import BaseModel


class DoStuffCommand(BaseModel):
    name: str


def handle_do_stuff(command, subsystems):
    return {"ok": True}
"""


def test_discover_authority_overlap_finds_handler_class_and_function() -> None:
    intended_files = ["Code&DBs/Workflow/runtime/operations/commands/do_stuff.py"]
    impacts = discover_authority_overlap(
        intended_files=intended_files,
        file_contents={intended_files[0]: _PYTHON_HANDLER_BODY},
    )
    refs = {impact.unit_ref for impact in impacts if impact.unit_kind == "handler_ref"}
    assert any("DoStuffCommand" in ref for ref in refs)
    assert any("handle_do_stuff" in ref for ref in refs)


def test_discover_authority_overlap_dedupes_repeat_keys() -> None:
    body = "INSERT INTO operation_catalog_registry (operation_ref, operation_name) VALUES ('x', 'y');"
    intended_files = ["Code&DBs/Databases/migrations/workflow/997_x.sql"]
    impacts = discover_authority_overlap(
        intended_files=intended_files + intended_files,
        file_contents={intended_files[0]: body + "\n" + body},
    )
    op_keys = [
        (impact.unit_kind, impact.unit_ref)
        for impact in impacts
        if impact.unit_kind == "operation_ref"
    ]
    assert op_keys.count(("operation_ref", "y")) == 1
