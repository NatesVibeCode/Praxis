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


def test_inspect_workflow_schema_treats_expected_workflow_definition_rows_as_present(
    monkeypatch,
) -> None:
    class _Conn:
        async def fetch(self, query: str, *args):
            normalized = " ".join(query.split())
            if "FROM workflow_definitions" in normalized:
                assert args == (["workflow_definition.native_self_hosted_smoke.v1"],)
                return [{"row_key": "workflow_definition.native_self_hosted_smoke.v1"}]
            raise AssertionError(f"unexpected query: {query}")

        async def fetchval(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT to_regclass($1::text) IS NOT NULL":
                assert args == ("public.workflow_definitions",)
                return True
            raise AssertionError(f"unexpected query: {query}")

    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_readiness_by_migration",
        lambda: (
            (
                "149_native_self_hosted_smoke_definition.sql",
                (
                    WorkflowMigrationExpectedObject(
                        object_type="row",
                        object_name=(
                            "workflow_definitions."
                            "workflow_definition.native_self_hosted_smoke.v1"
                        ),
                    ),
                ),
            ),
        ),
    )

    readiness = asyncio.run(postgres_schema.inspect_workflow_schema(_Conn()))

    assert readiness.is_bootstrapped is True
    assert readiness.missing_objects == ()
    assert readiness.missing_by_migration == {}


def test_inspect_workflow_schema_treats_expected_registry_rows_as_present(
    monkeypatch,
) -> None:
    expected = (
        WorkflowMigrationExpectedObject(
            object_type="row",
            object_name="registry_workspace_authority.scratch_agent",
        ),
        WorkflowMigrationExpectedObject(
            object_type="row",
            object_name=(
                "registry_sandbox_profile_authority."
                "sandbox_profile.scratch_agent.default"
            ),
        ),
        WorkflowMigrationExpectedObject(
            object_type="row",
            object_name="registry_runtime_profile_authority.scratch_agent",
        ),
        WorkflowMigrationExpectedObject(
            object_type="row",
            object_name="registry_native_runtime_profile_authority.scratch_agent",
        ),
    )

    class _Conn:
        async def fetch(self, query: str, *args):
            normalized = " ".join(query.split())
            if "FROM registry_workspace_authority" in normalized:
                assert args == (["scratch_agent"],)
                return [{"row_key": "scratch_agent"}]
            if "FROM registry_sandbox_profile_authority" in normalized:
                assert args == (["sandbox_profile.scratch_agent.default"],)
                return [{"row_key": "sandbox_profile.scratch_agent.default"}]
            if "FROM registry_runtime_profile_authority" in normalized:
                assert args == (["scratch_agent"],)
                return [{"row_key": "scratch_agent"}]
            if "FROM registry_native_runtime_profile_authority" in normalized:
                assert args == (["scratch_agent"],)
                return [{"row_key": "scratch_agent"}]
            raise AssertionError(f"unexpected query: {query}")

        async def fetchval(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT to_regclass($1::text) IS NOT NULL":
                assert args[0] in {
                    "public.registry_workspace_authority",
                    "public.registry_sandbox_profile_authority",
                    "public.registry_runtime_profile_authority",
                    "public.registry_native_runtime_profile_authority",
                }
                return True
            raise AssertionError(f"unexpected query: {query}")

    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_readiness_by_migration",
        lambda: (("167_scratch_agent_runtime_lane.sql", expected),),
    )

    readiness = asyncio.run(postgres_schema.inspect_workflow_schema(_Conn()))

    assert readiness.is_bootstrapped is True
    assert readiness.missing_objects == ()
    assert readiness.missing_by_migration == {}


def test_inspect_workflow_schema_treats_expected_provider_authority_rows_as_present(
    monkeypatch,
) -> None:
    expected = (
        WorkflowMigrationExpectedObject(
            object_type="row",
            object_name="provider_cli_profiles.openrouter",
        ),
        WorkflowMigrationExpectedObject(
            object_type="row",
            object_name=(
                "provider_transport_admissions."
                "provider_transport_admission.openrouter.llm_task"
            ),
        ),
        WorkflowMigrationExpectedObject(
            object_type="row",
            object_name="provider_lane_policy.openrouter",
        ),
        WorkflowMigrationExpectedObject(
            object_type="row",
            object_name="provider_model_candidates.candidate.openrouter.auto",
        ),
    )

    class _Conn:
        async def fetch(self, query: str, *args):
            normalized = " ".join(query.split())
            if "FROM provider_cli_profiles" in normalized:
                assert args == (["openrouter"],)
                return [{"row_key": "openrouter"}]
            if "FROM provider_transport_admissions" in normalized:
                assert args == (["provider_transport_admission.openrouter.llm_task"],)
                return [{"row_key": "provider_transport_admission.openrouter.llm_task"}]
            if "FROM provider_lane_policy" in normalized:
                assert args == (["openrouter"],)
                return [{"row_key": "openrouter"}]
            if "FROM provider_model_candidates" in normalized:
                assert args == (["candidate.openrouter.auto"],)
                return [{"row_key": "candidate.openrouter.auto"}]
            raise AssertionError(f"unexpected query: {query}")

        async def fetchval(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT to_regclass($1::text) IS NOT NULL":
                assert args[0] in {
                    "public.provider_cli_profiles",
                    "public.provider_transport_admissions",
                    "public.provider_lane_policy",
                    "public.provider_model_candidates",
                }
                return True
            raise AssertionError(f"unexpected query: {query}")

    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_readiness_by_migration",
        lambda: (("168_openrouter_provider_authority_repair.sql", expected),),
    )

    readiness = asyncio.run(postgres_schema.inspect_workflow_schema(_Conn()))

    assert readiness.is_bootstrapped is True
    assert readiness.missing_objects == ()
    assert readiness.missing_by_migration == {}


def test_inspect_workflow_schema_treats_private_api_allowlist_rows_as_present(
    monkeypatch,
) -> None:
    expected = WorkflowMigrationExpectedObject(
        object_type="row",
        object_name=(
            "private_provider_api_job_allowlist."
            "praxis|compile|llm_task|together|deepseek-ai/DeepSeek-V4-Pro"
        ),
    )

    class _Conn:
        async def fetchrow(self, query: str, *args):
            normalized = " ".join(query.split())
            if "FROM private_provider_api_job_allowlist" in normalized:
                assert args == (
                    "praxis",
                    "compile",
                    "llm_task",
                    "together",
                    "deepseek-ai/DeepSeek-V4-Pro",
                )
                return {"?column?": 1}
            raise AssertionError(f"unexpected query: {query}")

        async def fetchval(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT to_regclass($1::text) IS NOT NULL":
                assert args == ("public.private_provider_api_job_allowlist",)
                return True
            raise AssertionError(f"unexpected query: {query}")

    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_readiness_by_migration",
        lambda: (("266_private_api_compile_only_allowlist.sql", (expected,)),),
    )

    readiness = asyncio.run(postgres_schema.inspect_workflow_schema(_Conn()))

    assert readiness.is_bootstrapped is True
    assert readiness.missing_objects == ()
    assert readiness.missing_by_migration == {}


def test_inspect_workflow_schema_reports_missing_private_api_allowlist_rows(
    monkeypatch,
) -> None:
    expected = WorkflowMigrationExpectedObject(
        object_type="row",
        object_name=(
            "private_provider_api_job_allowlist."
            "praxis|compile|llm_task|together|deepseek-ai/DeepSeek-V4-Pro"
        ),
    )

    class _Conn:
        async def fetchrow(self, query: str, *args):
            normalized = " ".join(query.split())
            if "FROM private_provider_api_job_allowlist" in normalized:
                return None
            raise AssertionError(f"unexpected query: {query}")

        async def fetchval(self, query: str, *args):
            normalized = " ".join(query.split())
            if normalized == "SELECT to_regclass($1::text) IS NOT NULL":
                assert args == ("public.private_provider_api_job_allowlist",)
                return True
            raise AssertionError(f"unexpected query: {query}")

    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_readiness_by_migration",
        lambda: (("266_private_api_compile_only_allowlist.sql", (expected,)),),
    )

    readiness = asyncio.run(postgres_schema.inspect_workflow_schema(_Conn()))

    assert readiness.is_bootstrapped is False
    assert readiness.missing_objects == (expected,)
    assert readiness.missing_by_migration == {
        "266_private_api_compile_only_allowlist.sql": (expected,),
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


def test_inspect_workflow_schema_treats_absent_tables_as_satisfied_when_missing(monkeypatch) -> None:
    class _Conn:
        async def fetch(self, query: str, *args):
            return []

        async def fetchrow(self, query: str, *args):
            normalized = " ".join(query.split())
            if "cls.relkind IN ('r', 'p')" in normalized:
                assert args == ("workflow_notifications",)
                return None
            raise AssertionError(f"unexpected query: {query}")

        async def fetchval(self, query: str, *args):
            raise AssertionError(f"unexpected query: {query}")

    expected = WorkflowMigrationExpectedObject(
        object_type="absent_table",
        object_name="workflow_notifications",
    )
    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_readiness_by_migration",
        lambda: (("148_drop_workflow_notifications.sql", (expected,)),),
    )

    readiness = asyncio.run(postgres_schema.inspect_workflow_schema(_Conn()))

    assert readiness.is_bootstrapped is True
    assert readiness.missing_objects == ()


def test_inspect_workflow_schema_reports_absent_tables_when_still_present(monkeypatch) -> None:
    class _Conn:
        async def fetch(self, query: str, *args):
            return []

        async def fetchrow(self, query: str, *args):
            normalized = " ".join(query.split())
            if "cls.relkind IN ('r', 'p')" in normalized:
                assert args == ("workflow_notifications",)
                return {"?column?": 1}
            raise AssertionError(f"unexpected query: {query}")

        async def fetchval(self, query: str, *args):
            raise AssertionError(f"unexpected query: {query}")

    expected = WorkflowMigrationExpectedObject(
        object_type="absent_table",
        object_name="workflow_notifications",
    )
    monkeypatch.setattr(
        postgres_schema,
        "_workflow_schema_readiness_by_migration",
        lambda: (("148_drop_workflow_notifications.sql", (expected,)),),
    )

    readiness = asyncio.run(postgres_schema.inspect_workflow_schema(_Conn()))

    assert readiness.is_bootstrapped is False
    assert readiness.missing_objects == (expected,)
    assert readiness.missing_by_migration == {
        "148_drop_workflow_notifications.sql": (expected,),
    }
