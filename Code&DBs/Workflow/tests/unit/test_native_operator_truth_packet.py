from __future__ import annotations

import json
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[4]
TRUTH_PACKET_PATH = REPO_ROOT / "artifacts" / "workflow" / "PRAXIS_NATIVE_OPERATOR_TRUTH_PACKET.json"
SMOKE_QUEUE_PATH = REPO_ROOT / "artifacts" / "workflow" / "PRAXIS_NATIVE_SELF_HOSTED_SMOKE.queue.json"


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def test_truth_packet_paths_exist() -> None:
    packet = _load_json(TRUTH_PACKET_PATH)
    truth_surfaces = packet["truth_surfaces"]
    assert isinstance(truth_surfaces, dict)

    for relative_path in truth_surfaces.values():
        assert isinstance(relative_path, str)
        assert (REPO_ROOT / relative_path).exists(), relative_path


def test_smoke_queue_runtime_env_only_uses_primary_inputs() -> None:
    packet = _load_json(TRUTH_PACKET_PATH)
    smoke_queue = _load_json(SMOKE_QUEUE_PATH)

    required_inputs = packet["required_runtime_inputs"]
    derived_values = packet["derived_native_values"]
    assert isinstance(required_inputs, list)
    assert isinstance(derived_values, list)

    smoke_contract = smoke_queue["native_smoke"]
    assert isinstance(smoke_contract, dict)
    runtime_env = smoke_contract["runtime_env"]
    assert isinstance(runtime_env, dict)

    assert set(runtime_env) == set(required_inputs)
    assert not (set(runtime_env) & set(derived_values))


def test_native_operator_truth_docs_do_not_reference_stale_dispatch_paths() -> None:
    packet = _load_json(TRUTH_PACKET_PATH)
    truth_surfaces = packet["truth_surfaces"]
    assert isinstance(truth_surfaces, dict)

    checked_docs = (
        "handoff",
        "env_contract",
        "runbook",
        "hourly_note",
        "recovery",
        "blockers",
        "active_next_moves",
    )
    for key in checked_docs:
        relative_path = truth_surfaces[key]
        assert isinstance(relative_path, str)
        text = (REPO_ROOT / relative_path).read_text(encoding="utf-8")
        assert "artifacts/dispatch/" not in text
        assert "/Users/" not in text, f"Hardcoded /Users/ path found in {key}"
