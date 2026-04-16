from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import asyncpg
import pytest

from runtime.operator_object_relations import (
    PostgresOperatorObjectRelationRepository,
    operator_object_relation_id,
)
from storage.migrations import workflow_bootstrap_migration_statements
from storage.postgres import (
    PostgresConfigurationError,
    bootstrap_control_plane_schema,
    connect_workflow_database,
    resolve_workflow_database_url,
)
from surfaces.api import operator_write

_SCHEMA_BOOTSTRAP_LOCK_ID = 741001


def test_operator_object_relations_frontdoor_persists_functional_areas_and_relations() -> None:
    asyncio.run(_exercise_operator_object_relations_frontdoor_persists_functional_areas_and_relations())


async def _exercise_operator_object_relations_frontdoor_persists_functional_areas_and_relations() -> None:
    env = _workflow_env()
    as_of = datetime(2026, 4, 16, 21, 30, tzinfo=timezone.utc)

    conn = await connect_workflow_database(env=env)
    try:
        await bootstrap_control_plane_schema(conn)
        for filename in (
            "009_bug_and_roadmap_authority.sql",
            "010_operator_control_authority.sql",
            "015_memory_graph.sql",
            "134_operator_object_relations.sql",
        ):
            await _bootstrap_workflow_migration(conn, filename)

        await _seed_operator_decision(conn, as_of=as_of)
        await _seed_bug(conn, as_of=as_of)
        await _seed_document(conn, as_of=as_of)

        area_payload = await operator_write.arecord_functional_area(
            area_slug="checkout",
            title="Checkout",
            summary="Shared checkout semantics across bugs, roadmap, code, and docs.",
            created_at=as_of,
            updated_at=as_of,
            env=env,
        )
        relation_payload = await operator_write.arecord_operator_object_relation(
            relation_kind="grouped_in",
            source_kind="bug",
            source_ref="bug.object-relation.1",
            target_kind="functional_area",
            target_ref="checkout",
            relation_metadata={"origin": "integration-test"},
            bound_by_decision_id="operator_decision.object-relation.1",
            created_at=as_of,
            updated_at=as_of,
            env=env,
        )
        doc_relation_payload = await operator_write.arecord_operator_object_relation(
            relation_kind="described_by",
            source_kind="repo_path",
            source_ref="Code&DBs/Workflow/runtime/checkout.py",
            target_kind="document",
            target_ref="document.checkout.workflow",
            relation_metadata={"origin": "integration-test"},
            created_at=as_of,
            updated_at=as_of,
            env=env,
        )

        area_row = await conn.fetchrow(
            """
            SELECT functional_area_id, area_slug, title, area_status, summary
            FROM functional_areas
            WHERE functional_area_id = $1
            """,
            "functional_area.checkout",
        )
        assert area_row is not None
        assert area_row["area_slug"] == "checkout"
        assert area_row["title"] == "Checkout"
        assert area_row["area_status"] == "active"

        repository = PostgresOperatorObjectRelationRepository(conn)
        grouped_relation_id = operator_object_relation_id(
            relation_kind="grouped_in",
            source_kind="bug",
            source_ref="bug.object-relation.1",
            target_kind="functional_area",
            target_ref="functional_area.checkout",
        )
        grouped_relation = await repository.load_relation(
            operator_object_relation_id=grouped_relation_id,
        )
        assert grouped_relation is not None
        assert grouped_relation.relation_kind == "grouped_in"
        assert grouped_relation.target_ref == "functional_area.checkout"
        assert grouped_relation.bound_by_decision_id == "operator_decision.object-relation.1"
        assert grouped_relation.to_json() == relation_payload["operator_object_relation"]

        doc_relation_id = operator_object_relation_id(
            relation_kind="described_by",
            source_kind="repo_path",
            source_ref="Code&DBs/Workflow/runtime/checkout.py",
            target_kind="document",
            target_ref="document.checkout.workflow",
        )
        doc_relation = await repository.load_relation(
            operator_object_relation_id=doc_relation_id,
        )
        assert doc_relation is not None
        assert doc_relation.source_kind == "repo_path"
        assert doc_relation.target_kind == "document"
        assert doc_relation.to_json() == doc_relation_payload["operator_object_relation"]
        assert area_payload["functional_area"]["functional_area_id"] == "functional_area.checkout"
    finally:
        await conn.close()


async def _bootstrap_workflow_migration(conn, filename: str) -> None:
    async with conn.transaction():
        await conn.execute(
            "SELECT pg_advisory_xact_lock($1::bigint)",
            _SCHEMA_BOOTSTRAP_LOCK_ID,
        )
        for statement in workflow_bootstrap_migration_statements(filename):
            try:
                async with conn.transaction():
                    await conn.execute(statement)
            except asyncpg.PostgresError as exc:
                if getattr(exc, "sqlstate", None) in {"42P07", "42710"}:
                    continue
                raise


async def _seed_operator_decision(conn, *, as_of: datetime) -> None:
    await conn.execute(
        """
        INSERT INTO operator_decisions (
            operator_decision_id,
            decision_key,
            decision_kind,
            decision_status,
            title,
            rationale,
            decided_by,
            decision_source,
            effective_from,
            effective_to,
            decided_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
        )
        ON CONFLICT (operator_decision_id) DO UPDATE SET
            updated_at = EXCLUDED.updated_at
        """,
        "operator_decision.object-relation.1",
        "decision.object-relation.primary",
        "operator_graph",
        "decided",
        "Object relation test decision",
        "Controls semantic relation writes for test coverage.",
        "tests",
        "tests",
        as_of,
        None,
        as_of,
        as_of,
        as_of,
    )


async def _seed_bug(conn, *, as_of: datetime) -> None:
    await conn.execute(
        """
        INSERT INTO bugs (
            bug_id,
            bug_key,
            title,
            status,
            severity,
            priority,
            summary,
            source_kind,
            discovered_in_run_id,
            discovered_in_receipt_id,
            owner_ref,
            decision_ref,
            resolution_summary,
            opened_at,
            resolved_at,
            created_at,
            updated_at
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, NULL, NULL, NULL, $9, NULL, $10, NULL, $11, $12
        )
        ON CONFLICT (bug_id) DO UPDATE SET
            updated_at = EXCLUDED.updated_at
        """,
        "bug.object-relation.1",
        "bug-object-relation-1",
        "Checkout semantic coverage",
        "open",
        "s2",
        "p1",
        "Bug used to anchor operator object relation coverage.",
        "manual",
        "decision.object-relation.primary",
        as_of,
        as_of,
        as_of,
    )


async def _seed_document(conn, *, as_of: datetime) -> None:
    await conn.execute(
        """
        INSERT INTO memory_entities (
            id,
            entity_type,
            name,
            content,
            metadata,
            source,
            confidence,
            archived,
            created_at,
            updated_at
        ) VALUES (
            $1, 'document', $2, $3, '{}'::jsonb, 'tests', 1.0, false, $4, $5
        )
        ON CONFLICT (id) DO UPDATE SET
            updated_at = EXCLUDED.updated_at
        """,
        "document.checkout.workflow",
        "Checkout Workflow",
        "Canonical workflow document for checkout.",
        as_of,
        as_of,
    )


def _workflow_env() -> dict[str, str]:
    try:
        database_url = resolve_workflow_database_url()
    except PostgresConfigurationError as exc:
        pytest.skip(
            "WORKFLOW_DATABASE_URL is required for operator object relation integration test: "
            f"{exc.reason_code}"
        )
    return {"WORKFLOW_DATABASE_URL": database_url}
