from __future__ import annotations

from storage.migrations import workflow_migration_sql_text, workflow_migration_statements


def test_together_compile_primary_never_reapplies_openrouter_as_cli() -> None:
    sql_text = workflow_migration_sql_text("262_together_compile_primary.sql")
    statements = workflow_migration_statements("262_together_compile_primary.sql")

    assert "OpenRouter is HTTP/API-only" in sql_text
    assert "DELETE FROM task_type_routing AS route" in sql_text
    assert "route.provider_slug = 'openrouter'" in sql_text
    assert "route.transport_type = 'CLI'" in sql_text
    assert "provider_transport_admissions AS admission" in sql_text
    assert "admission.transport_kind = 'cli'" in sql_text
    assert "OpenRouter remains API fallback" in sql_text
    assert "AND transport_type = 'API'" in sql_text
    assert any("DELETE FROM task_type_routing AS route" in statement for statement in statements)


def test_retired_together_v32_never_reapplies_together_as_cli() -> None:
    sql_text = workflow_migration_sql_text(
        "364_remove_retired_deepseek_v32_native_profile.sql"
    )
    statements = workflow_migration_statements(
        "364_remove_retired_deepseek_v32_native_profile.sql"
    )

    assert "Together is HTTP/API-only" in sql_text
    assert "DELETE FROM task_type_routing AS route" in sql_text
    assert "route.provider_slug = 'together'" in sql_text
    assert "route.transport_type = 'CLI'" in sql_text
    assert "provider_transport_admissions AS admission" in sql_text
    assert "admission.transport_kind = 'cli'" in sql_text
    assert "AND transport_type = 'API'" in sql_text
    assert any("DELETE FROM task_type_routing AS route" in statement for statement in statements)
