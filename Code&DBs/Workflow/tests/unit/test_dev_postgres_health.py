from __future__ import annotations

from pathlib import Path

import pytest

from storage import dev_postgres
from storage.dev_postgres import DevPostgresConfig, _collect_local_postgres_health
from storage.migrations import workflow_compile_authority_readiness_requirements

_READINESS_REQUIREMENTS = workflow_compile_authority_readiness_requirements()
_REQUIRED_TABLE_TO_FIELDS: dict[str, tuple[str, ...]] = {}
for field_name, required_tables in _READINESS_REQUIREMENTS:
    for table_name in required_tables:
        _required = list(_REQUIRED_TABLE_TO_FIELDS.get(table_name, ()))
        _required.append(field_name)
        _REQUIRED_TABLE_TO_FIELDS[table_name] = tuple(_required)


@pytest.mark.parametrize(
    "missing_table",
    tuple(sorted(_REQUIRED_TABLE_TO_FIELDS)),
)
def test_collect_local_postgres_health_authority_ready_fields_depend_on_required_tables(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    missing_table: str,
) -> None:
    config = DevPostgresConfig(
        data_dir=tmp_path / "postgres-data",
        log_file=tmp_path / "postgres.log",
        database_url="postgresql://127.0.0.1:55432/workflow",
        pg_ctl="pg_ctl",
        cluster_port=55432,
    )
    monkeypatch.setattr(dev_postgres, "_pg_ctl_status", lambda _config: (True, "running"))
    monkeypatch.setattr(dev_postgres, "_read_postmaster_pid", lambda _data_dir: (333, 55432))

    async def _fake_probe_database(_config: DevPostgresConfig) -> tuple[bool, bool, tuple[str, ...]]:
        return True, False, (missing_table,)

    monkeypatch.setattr(dev_postgres, "_probe_database", _fake_probe_database)

    status = _collect_local_postgres_health(config)
    assert status.database_reachable is True
    assert status.schema_bootstrapped is False

    failed_fields = set(_REQUIRED_TABLE_TO_FIELDS[missing_table])
    for field_name, _ in _READINESS_REQUIREMENTS:
        expected = field_name not in failed_fields
        assert getattr(status, field_name) is expected
