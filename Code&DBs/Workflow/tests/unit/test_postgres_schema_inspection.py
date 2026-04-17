from __future__ import annotations

import asyncio

from storage.migrations import WorkflowMigrationExpectedObject
from storage.postgres import schema as postgres_schema


def test_inspect_workflow_schema_treats_expected_operation_rows_as_present(monkeypatch) -> None:
    class _Conn:
        async def fetch(self, query: str, *args):
            normalized = " ".join(query.split())
            if "FROM operation_catalog_registry" in normalized:
                assert args == (["operator.provider_onboarding"],)
                return [{"row_key": "operator.provider_onboarding"}]
            raise AssertionError(f"unexpected query: {query}")

        async def fetchval(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT to_regclass($1::text) IS NOT NULL":
                assert args == ("public.operation_catalog_registry",)
                return True
            raise AssertionError(f"unexpected query: {query}")

    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_readiness_by_migration",
        lambda: (
            (
                "141_operation_catalog_provider_onboarding.sql",
                (
                    WorkflowMigrationExpectedObject(
                        object_type="row",
                        object_name="operation_catalog_registry.operator.provider_onboarding",
                    ),
                ),
            ),
        ),
    )

    readiness = asyncio.run(postgres_schema.inspect_workflow_schema(_Conn()))

    assert readiness.is_bootstrapped is True
    assert readiness.missing_objects == ()
    assert readiness.missing_by_migration == {}


def test_inspect_workflow_schema_reports_missing_expected_operation_rows(monkeypatch) -> None:
    class _Conn:
        async def fetch(self, query: str, *args):
            normalized = " ".join(query.split())
            if "FROM operation_catalog_registry" in normalized:
                assert args == (["operator.provider_onboarding"],)
                return []
            raise AssertionError(f"unexpected query: {query}")

        async def fetchval(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT to_regclass($1::text) IS NOT NULL":
                assert args == ("public.operation_catalog_registry",)
                return True
            raise AssertionError(f"unexpected query: {query}")

    expected = WorkflowMigrationExpectedObject(
        object_type="row",
        object_name="operation_catalog_registry.operator.provider_onboarding",
    )
    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_readiness_by_migration",
        lambda: (("141_operation_catalog_provider_onboarding.sql", (expected,)),),
    )

    readiness = asyncio.run(postgres_schema.inspect_workflow_schema(_Conn()))

    assert readiness.is_bootstrapped is False
    assert readiness.missing_objects == (expected,)
    assert readiness.missing_by_migration == {
        "141_operation_catalog_provider_onboarding.sql": (expected,),
    }


def test_inspect_workflow_schema_accepts_bare_constraint_names(monkeypatch) -> None:
    class _Conn:
        async def fetch(self, query: str, *args):
            normalized = " ".join(query.split())
            assert "position('.' in expected.object_name) = 0" in normalized
            return []

    expected = WorkflowMigrationExpectedObject(
        object_type="constraint",
        object_name="work_item_workflow_bindings_unique_edge",
    )
    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_readiness_by_migration",
        lambda: (("132_issue_backlog_authority.sql", (expected,)),),
    )

    readiness = asyncio.run(postgres_schema.inspect_workflow_schema(_Conn()))

    assert readiness.is_bootstrapped is True
    assert readiness.missing_objects == ()
