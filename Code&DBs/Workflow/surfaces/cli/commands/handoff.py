"""Read-only CLI frontdoor for operation handoff inspection.

Mutation stays on the command side; this surface only exposes committed state.
"""

from __future__ import annotations

import json
from typing import Any, TextIO

from runtime.operations.queries.handoff import (
    QueryHandoffArtifactHistory,
    QueryHandoffArtifactLineage,
    QueryHandoffConsumerStatus,
    QueryHandoffLatestArtifact,
    handle_query_handoff_artifact_history,
    handle_query_handoff_artifact_lineage,
    handle_query_handoff_consumer_status,
    handle_query_handoff_latest,
)
from surfaces.cli._db import cli_sync_conn


def _sync_conn():
    return cli_sync_conn()


def _help_text() -> str:
    return "\n".join(
        [
            "usage: workflow handoff <latest|lineage|status|history> [args]",
            "",
            "Handoff inspection:",
            "  workflow handoff latest   [--artifact-kind KIND] [--artifact-ref REF] [--input-fingerprint FP] [--json]",
            "  workflow handoff lineage  [--artifact-kind KIND] --revision-ref REF [--json]",
            "  workflow handoff status   --subscription-id ID --run-id ID [--limit N] [--json]",
            "  workflow handoff history  [--artifact-kind KIND] [--artifact-ref REF] [--input-fingerprint FP] [--limit N] [--json]",
            "",
            "Notes:",
            "  - latest/lineage/history read the committed compile_artifacts lineage",
            "  - status reads event_subscriptions and subscription_checkpoints",
        ]
    )


def _artifact_summary(record: dict[str, Any] | None) -> str:
    if not isinstance(record, dict):
        return "no artifact"
    parts = [
        str(record.get("artifact_kind") or "-"),
        str(record.get("revision_ref") or "-"),
        f"ref={record.get('artifact_ref') or '-'}",
        f"hash={str(record.get('content_hash') or '')[:12] or '-'}",
        f"created_at={record.get('created_at') or '-'}",
    ]
    return "  ".join(parts)


def _checkpoint_summary(record: dict[str, Any] | None) -> str:
    if not isinstance(record, dict):
        return "no checkpoint"
    return "  ".join(
        [
            str(record.get("subscription_id") or "-"),
            str(record.get("run_id") or "-"),
            f"watermark={record.get('last_evidence_seq') if record.get('last_evidence_seq') is not None else '-'}",
            f"authority={record.get('last_authority_id') or '-'}",
            f"status={record.get('checkpoint_status') or '-'}",
        ]
    )


def _render_payload(payload: dict[str, Any], *, stdout: TextIO, as_json: bool) -> None:
    if as_json:
        stdout.write(json.dumps(payload, indent=2, default=str) + "\n")
        return

    if "artifact" in payload:
        artifact = payload.get("artifact")
        stdout.write(_artifact_summary(artifact if isinstance(artifact, dict) else None) + "\n")
        history = payload.get("history")
        if isinstance(history, list) and len(history) > 1:
            stdout.write(f"  history: {len(history)} item(s)\n")
            for row in history:
                if isinstance(row, dict):
                    stdout.write(f"    {_artifact_summary(row)}\n")
        return

    if "lineage" in payload:
        lineage = payload.get("lineage")
        if not isinstance(lineage, list) or not lineage:
            stdout.write("no lineage found\n")
            return
        stdout.write(f"{len(lineage)} lineage artifact(s)\n")
        for row in lineage:
            if isinstance(row, dict):
                stdout.write(
                    "  "
                    + "  ".join(
                        [
                            str(row.get("artifact_kind") or "-"),
                            str(row.get("revision_ref") or "-"),
                            f"parent={row.get('parent_artifact_ref') or '-'}",
                            f"hash={str(row.get('content_hash') or '')[:12] or '-'}",
                        ]
                    )
                    + "\n"
                )
        return

    if "history" in payload:
        history = payload.get("history")
        if not isinstance(history, list) or not history:
            stdout.write("no history found\n")
            return
        stdout.write(f"{len(history)} handoff artifact(s)\n")
        for row in history:
            if isinstance(row, dict):
                stdout.write(f"  {_artifact_summary(row)}\n")
        return

    if "checkpoint" in payload or "subscription" in payload:
        subscription = payload.get("subscription")
        checkpoint = payload.get("checkpoint")
        stdout.write(
            "subscription: "
            + (
                str(subscription.get("subscription_id"))
                if isinstance(subscription, dict)
                else "-"
            )
            + "\n"
        )
        if isinstance(subscription, dict):
            stdout.write(
                f"  name={subscription.get('subscription_name') or '-'} "
                f"consumer={subscription.get('consumer_kind') or '-'} "
                f"scope={subscription.get('cursor_scope') or '-'} "
                f"status={subscription.get('status') or '-'}\n"
            )
        stdout.write(_checkpoint_summary(checkpoint if isinstance(checkpoint, dict) else None) + "\n")
        return

    stdout.write(json.dumps(payload, indent=2, default=str) + "\n")


def _handoff_command(args: list[str], *, stdout: TextIO) -> int:
    if not args or args[0] in {"-h", "--help"}:
        stdout.write(_help_text() + "\n")
        return 2

    action = args[0]
    as_json = False
    artifact_kind = ""
    artifact_ref = ""
    input_fingerprint = ""
    revision_ref = ""
    subscription_id = ""
    run_id = ""
    limit = 20

    i = 1
    while i < len(args):
        token = args[i]
        if token == "--json":
            as_json = True
            i += 1
            continue
        if token == "--artifact-kind" and i + 1 < len(args):
            artifact_kind = args[i + 1]
            i += 2
            continue
        if token == "--artifact-ref" and i + 1 < len(args):
            artifact_ref = args[i + 1]
            i += 2
            continue
        if token == "--input-fingerprint" and i + 1 < len(args):
            input_fingerprint = args[i + 1]
            i += 2
            continue
        if token == "--revision-ref" and i + 1 < len(args):
            revision_ref = args[i + 1]
            i += 2
            continue
        if token == "--subscription-id" and i + 1 < len(args):
            subscription_id = args[i + 1]
            i += 2
            continue
        if token == "--run-id" and i + 1 < len(args):
            run_id = args[i + 1]
            i += 2
            continue
        if token == "--limit" and i + 1 < len(args):
            limit = int(args[i + 1])
            i += 2
            continue
        stdout.write(f"unexpected argument: {token}\n")
        return 2

    try:
        if action == "latest":
            if not artifact_kind:
                stdout.write("error: --artifact-kind is required\n")
                return 2
            payload = handle_query_handoff_latest(
                QueryHandoffLatestArtifact(
                    artifact_kind=artifact_kind,
                    artifact_ref=artifact_ref or None,
                    input_fingerprint=input_fingerprint or None,
                ),
                _handoff_subsystems(),
            )
        elif action == "lineage":
            if not artifact_kind or not revision_ref:
                stdout.write("error: --artifact-kind and --revision-ref are required\n")
                return 2
            payload = handle_query_handoff_artifact_lineage(
                QueryHandoffArtifactLineage(
                    artifact_kind=artifact_kind,
                    revision_ref=revision_ref,
                ),
                _handoff_subsystems(),
            )
        elif action == "status":
            if not subscription_id or not run_id:
                stdout.write("error: --subscription-id and --run-id are required\n")
                return 2
            payload = handle_query_handoff_consumer_status(
                QueryHandoffConsumerStatus(
                    subscription_id=subscription_id,
                    run_id=run_id,
                    limit=limit,
                ),
                _handoff_subsystems(),
            )
        elif action == "history":
            if not artifact_kind:
                stdout.write("error: --artifact-kind is required\n")
                return 2
            payload = handle_query_handoff_artifact_history(
                QueryHandoffArtifactHistory(
                    artifact_kind=artifact_kind,
                    artifact_ref=artifact_ref or None,
                    input_fingerprint=input_fingerprint or None,
                    limit=limit,
                ),
                _handoff_subsystems(),
            )
        else:
            stdout.write(f"unknown action: {action}\n")
            return 2
    except RuntimeError as exc:
        stdout.write(f"error: {exc}\n")
        return 2

    _render_payload(payload, stdout=stdout, as_json=as_json)
    return 0


def _handoff_subsystems() -> Any:
    class _Subsystems:
        def get_pg_conn(self) -> Any:
            return _sync_conn()

    return _Subsystems()


__all__ = ["_handoff_command"]
