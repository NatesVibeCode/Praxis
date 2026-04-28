"""Gateway-friendly command for promoting a compose-experiment winner.

Migration 277 registered the ``experiment_promote_winner`` operation but
pointed its binding at a module that did not exist. That left the API route
unmountable and the catalog advertising a dead capability.

This module is the missing CQRS handler. It reads the parent compose-experiment
receipt, extracts the chosen leg, updates the canonical ``task_type_routing``
row for that task type, and returns a compact before/after diff that the
gateway can persist as the ``experiment.winner.promoted`` authority event.
Provider/model knob changes stay in the diff only; they are not auto-applied.
"""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from runtime.receipt_store import load_receipt_payload


class PromoteExperimentWinnerCommand(BaseModel):
    """Input contract for ``experiment_promote_winner``."""

    source_experiment_receipt_id: str
    source_config_index: int = Field(ge=0)
    target_task_type: str | None = None
    caller_ref: str = "mcp.praxis_promote_experiment_winner"


def _load_experiment_run(
    *,
    source_experiment_receipt_id: str,
    source_config_index: int,
) -> dict[str, Any]:
    receipt = load_receipt_payload(source_experiment_receipt_id)
    if receipt is None:
        raise ValueError(
            f"compose_experiment receipt {source_experiment_receipt_id!r} was not found"
        )

    outputs = receipt.get("outputs")
    if not isinstance(outputs, dict):
        raise ValueError(
            f"receipt {source_experiment_receipt_id!r} does not carry compose_experiment outputs"
        )

    report = outputs.get("report")
    if not isinstance(report, dict):
        raise ValueError(
            f"receipt {source_experiment_receipt_id!r} does not include a compose_experiment report"
        )

    summary_table = report.get("summary_table")
    runs = summary_table if isinstance(summary_table, list) else report.get("runs")
    if not isinstance(runs, list):
        raise ValueError(
            f"receipt {source_experiment_receipt_id!r} does not expose per-config run details"
        )
    if source_config_index >= len(runs):
        raise ValueError(
            f"source_config_index {source_config_index} is out of range for receipt "
            f"{source_experiment_receipt_id!r} ({len(runs)} configs)"
        )

    run = runs[source_config_index]
    if not isinstance(run, dict):
        raise ValueError(
            f"config[{source_config_index}] in receipt {source_experiment_receipt_id!r} "
            f"must be a mapping"
        )

    return run


def _select_target_task_type(
    *,
    run: dict[str, Any],
    explicit_target_task_type: str | None,
    source_config_index: int,
    source_experiment_receipt_id: str,
) -> str:
    config = run.get("config")
    config_task_type = None
    if isinstance(config, dict):
        config_task_type = str(config.get("base_task_type") or "").strip() or None

    target_task_type = str(explicit_target_task_type or "").strip() or None
    if target_task_type and config_task_type and target_task_type != config_task_type:
        raise ValueError(
            f"target_task_type {target_task_type!r} does not match config[{source_config_index}] "
            f"base_task_type {config_task_type!r} in receipt {source_experiment_receipt_id!r}"
        )

    resolved = target_task_type or config_task_type
    if not resolved:
        raise ValueError(
            f"config[{source_config_index}] in receipt {source_experiment_receipt_id!r} "
            "does not declare base_task_type; pass target_task_type explicitly"
        )
    return resolved


def handle_promote_experiment_winner(
    command: PromoteExperimentWinnerCommand,
    subsystems: Any,
) -> dict[str, Any]:
    """Promote one compose-experiment leg into durable routing state."""

    run = _load_experiment_run(
        source_experiment_receipt_id=command.source_experiment_receipt_id,
        source_config_index=int(command.source_config_index),
    )
    target_task_type = _select_target_task_type(
        run=run,
        explicit_target_task_type=command.target_task_type,
        source_config_index=int(command.source_config_index),
        source_experiment_receipt_id=command.source_experiment_receipt_id,
    )

    resolved_overrides = run.get("resolved_overrides")
    if not isinstance(resolved_overrides, dict):
        config = run.get("config")
        if isinstance(config, dict):
            resolved_overrides = {
                key: value
                for key, value in config.items()
                if key in {"provider_slug", "model_slug", "temperature", "max_tokens"}
            }
    if not isinstance(resolved_overrides, dict):
        raise ValueError(
            f"config[{command.source_config_index}] in receipt "
            f"{command.source_experiment_receipt_id!r} does not include resolved_overrides"
        )

    temperature = resolved_overrides.get("temperature")
    max_tokens = resolved_overrides.get("max_tokens")
    if temperature is None or max_tokens is None:
        raise ValueError(
            f"config[{command.source_config_index}] in receipt "
            f"{command.source_experiment_receipt_id!r} must resolve temperature and max_tokens"
        )

    conn = subsystems.get_pg_conn()
    with conn.transaction():
        current_rows = conn.execute(
            """
            SELECT task_type, sub_task_type, provider_slug, model_slug, transport_type,
                   temperature, max_tokens
              FROM task_type_routing
             WHERE task_type = $1
             ORDER BY rank ASC, sub_task_type ASC, provider_slug ASC, model_slug ASC, transport_type ASC
             LIMIT 1
            """,
            target_task_type,
        )
        if not current_rows:
            raise ValueError(f"task_type_routing has no row for task_type {target_task_type!r}")

        row = current_rows[0]
        before = {
            "task_type": str(row.get("task_type") or target_task_type),
            "sub_task_type": str(row.get("sub_task_type") or ""),
            "provider_slug": str(row.get("provider_slug") or ""),
            "model_slug": str(row.get("model_slug") or ""),
            "transport_type": str(row.get("transport_type") or ""),
            "temperature": row.get("temperature"),
            "max_tokens": row.get("max_tokens"),
        }
        after = {
            **before,
            "temperature": float(temperature),
            "max_tokens": int(max_tokens),
        }

        if before["temperature"] != after["temperature"] or before["max_tokens"] != after["max_tokens"]:
            conn.execute(
                """
                UPDATE task_type_routing
                   SET temperature = $1,
                       max_tokens = $2,
                       updated_at = now()
                 WHERE task_type = $3
                   AND sub_task_type = $4
                   AND provider_slug = $5
                   AND model_slug = $6
                   AND transport_type = $7
                """,
                after["temperature"],
                after["max_tokens"],
                before["task_type"],
                before["sub_task_type"],
                before["provider_slug"],
                before["model_slug"],
                before["transport_type"],
            )

    diff_keys = [
        key
        for key in ("temperature", "max_tokens")
        if before.get(key) != after.get(key)
    ]
    event_payload = {
        "source_experiment_receipt_id": command.source_experiment_receipt_id,
        "source_config_index": int(command.source_config_index),
        "target_task_type": target_task_type,
        "target_provider_slug": before["provider_slug"],
        "target_model_slug": before["model_slug"],
        "before": before,
        "after": after,
        "diff_keys": diff_keys,
        "caller_ref": command.caller_ref,
    }

    return {
        "ok": True,
        "status": "promoted",
        "source_experiment_receipt_id": command.source_experiment_receipt_id,
        "source_config_index": int(command.source_config_index),
        "target_task_type": target_task_type,
        "before": before,
        "after": after,
        "diff_keys": diff_keys,
        "caller_ref": command.caller_ref,
        "event_payload": event_payload,
    }


__all__ = [
    "PromoteExperimentWinnerCommand",
    "handle_promote_experiment_winner",
]
