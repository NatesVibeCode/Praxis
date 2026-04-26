from __future__ import annotations

import os
from storage.postgres.connection import default_postgres_host

def test_default_postgres_host_outside_container(monkeypatch) -> None:
    # Ensure we look like we're outside a container
    monkeypatch.setattr("os.path.exists", lambda path: False)
    
    # Test default
    monkeypatch.delenv("PRAXIS_HOST_SHELL_DATABASE_HOST", raising=False)
    assert default_postgres_host() == "localhost"
    
    # Test override
    monkeypatch.setenv("PRAXIS_HOST_SHELL_DATABASE_HOST", "db.example.com")
    assert default_postgres_host() == "db.example.com"

def test_default_postgres_host_inside_container(monkeypatch) -> None:
    # Ensure we look like we're inside a container
    def fake_exists(path):
        if path == "/.dockerenv":
            return True
        return False
    monkeypatch.setattr("os.path.exists", fake_exists)
    
    assert default_postgres_host() == "host.docker.internal"
