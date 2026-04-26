from __future__ import annotations

import asyncio
import pytest
from unittest.mock import AsyncMock

from storage.postgres.schema import _acquire_schema_bootstrap_lock, _SCHEMA_BOOTSTRAP_LOCK_ID

@pytest.mark.anyio
async def test_acquire_schema_bootstrap_lock_uses_advisory_xact_lock(monkeypatch) -> None:
    # Ensure it uses the blocking transaction-level lock pg_advisory_xact_lock
    # instead of the polling pg_try_advisory_xact_lock.
    
    mock_conn = AsyncMock()
    mock_conn.execute.return_value = None
    
    elapsed = await _acquire_schema_bootstrap_lock(mock_conn)
    
    mock_conn.execute.assert_called_once_with(
        "SELECT pg_advisory_xact_lock($1::bigint)",
        _SCHEMA_BOOTSTRAP_LOCK_ID,
    )
    assert isinstance(elapsed, float)
