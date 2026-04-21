from __future__ import annotations

from runtime.context_accumulator import ContextAccumulator


def test_context_accumulator_compresses_old_entries_with_semantic_summary() -> None:
    accumulator = ContextAccumulator(
        max_context_tokens=120,
        keep_recent_full=1,
        max_preview_chars=500,
        max_compressed_chars=32,
    )

    accumulator.add_node_result(
        "node_0",
        "research",
        "succeeded",
        {
            "completion": "raw output " * 140,
            "parsed_output": {
                "summary": "Found the workflow context policy authority.",
            },
        },
    )
    accumulator.add_node_result(
        "node_1",
        "build",
        "succeeded",
        {"completion": "implemented scoped changes " * 12},
    )

    snapshot = accumulator.snapshot()

    assert snapshot.window_decision.compressed_entry_ids == ("node_0",)
    assert snapshot.window_decision.evicted_entry_ids == ()
    assert snapshot.entries[0].output_preview.startswith(
        "Found the workflow context polic"
    )
    assert snapshot.entries[1].state == "full"


def test_context_accumulator_evicts_old_compressed_entries_when_still_over_budget() -> None:
    accumulator = ContextAccumulator(
        max_context_tokens=42,
        keep_recent_full=1,
        max_preview_chars=500,
        max_compressed_chars=24,
    )

    accumulator.add_node_result(
        "node_0",
        "scan",
        "succeeded",
        {"summary": "Older scan output that can be discarded.", "completion": "a" * 600},
    )
    accumulator.add_node_result(
        "node_1",
        "plan",
        "succeeded",
        {"summary": "Older plan output that can be discarded.", "completion": "b" * 600},
    )
    accumulator.add_node_result(
        "node_2",
        "implement",
        "succeeded",
        {"completion": "recent implementation details " * 8},
    )

    decision = accumulator.window_decision()

    assert decision.evicted_entry_ids
    assert "node_2" in decision.retained_entry_ids
    assert "node_2" not in decision.evicted_entry_ids
    assert decision.total_tokens_est <= decision.max_context_tokens or decision.is_over_budget


def test_context_section_exposes_window_metadata() -> None:
    accumulator = ContextAccumulator(max_context_tokens=80, keep_recent_full=1)
    accumulator.add_node_result(
        "node_0",
        "prepare",
        "succeeded",
        {"completion": "prepared durable context metadata"},
    )

    section = accumulator.render_context_section()
    metadata = section["metadata"]["context_window"]

    assert section["name"] == "prior_results"
    assert metadata["max_context_tokens"] == 80
    assert metadata["token_estimate"] == accumulator.token_estimate()
    assert metadata["is_over_budget"] is False
