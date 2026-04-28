from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
PRAXIS_UP_SCRIPT = REPO_ROOT / "scripts" / "praxis-up"


def test_praxis_up_uses_compose_service_exec_instead_of_hardcoded_container_names() -> None:
    source = PRAXIS_UP_SCRIPT.read_text(encoding="utf-8")

    assert "praxis-api-server-1" not in source
    assert "praxis-workflow-worker-1" not in source
    assert "docker compose exec -T api-server" in source
    assert "docker compose exec -T workflow-worker" in source
