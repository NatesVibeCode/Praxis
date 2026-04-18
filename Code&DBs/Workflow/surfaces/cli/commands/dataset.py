"""CLI front door for the Praxis dataset refinery (`praxis dataset ...`).

This file is intentionally thin: every verb forwards to ``praxis_dataset``
with an ``action`` selector. The MCP tool is the single source of truth
for behavior; the CLI is a friendlier shape for humans typing.
"""

from __future__ import annotations

import json
from typing import Any, TextIO

from surfaces.cli.mcp_tools import load_json_file, print_json, run_cli_tool


_TOOL = "praxis_dataset"


def _help_text() -> str:
    return "\n".join(
        [
            "usage: praxis dataset <action> [args]",
            "",
            "Read:",
            "  praxis dataset summary",
            "  praxis dataset inbox [--kind review|triage|operator_explain]",
            "                       [--specialist slm/review] [--limit N] [--offset N]",
            "  praxis dataset candidates list [--kind review] [--route slm/review]",
            "                                 [--eligibility sft_eligible] [--policy <id>]",
            "                                 [--redaction clean] [--staleness fresh]",
            "                                 [--limit N] [--offset N]",
            "  praxis dataset candidate inspect <candidate_id>",
            "  praxis dataset policy list [--specialist slm/review] [--all]",
            "  praxis dataset policy show <policy_id_or_slug>",
            "  praxis dataset promotions list [--specialist ...] [--family sft]",
            "                                 [--split train] [--all] [--limit N]",
            "  praxis dataset lineage [--promotion <id>] [--candidate <id>]",
            "                         [--specialist ...] [--limit N]",
            "  praxis dataset manifests list [--specialist ...] [--family sft] [--limit N]",
            "",
            "Subscribers / reconcile:",
            "  praxis dataset candidates scan [--limit N] [--backfill]",
            "                                  [--since-days N] [--receipt-ids id1,id2]",
            "  praxis dataset projection refresh [--limit N]",
            "  praxis dataset stale reconcile [--by <slug>]",
            "",
            "Write:",
            "  praxis dataset policy record --slug <slug> --specialist <target>",
            "                               --rubric <file.json> --decided-by <slug>",
            "                               --rationale <text> [--auto-promote]",
            "                               [--supersedes <policy_id>]",
            "  praxis dataset candidate promote <candidate_id> --family sft|preference|eval",
            "                                   --specialist <target> --policy <policy_id>",
            "                                   --payload <file.json> --by <slug>",
            "                                   --rationale <text> [--decision-ref <id>]",
            "                                   [--split train|eval|holdout]",
            "  praxis dataset candidate reject <candidate_id> --by <slug> --reason <text>",
            "  praxis dataset preference suggest [--kind review|triage]",
            "                                    [--specialist slm/review] [--limit N]",
            "  praxis dataset preference create <chosen_id> <rejected_id>",
            "                                   --specialist ... --policy ... --payload <file>",
            "                                   --by <slug> --rationale <text>",
            "                                   --decision-ref <id>",
            "  praxis dataset eval add <candidate_id> --specialist ... --policy ...",
            "                          --payload <file.json> --by <slug>",
            "                          --rationale <text> --decision-ref <id>",
            "  praxis dataset promotion supersede <promotion_id> --reason <text> --by <slug>",
            "",
            "Export:",
            "  praxis dataset export --family sft|preference|eval --specialist <target>",
            "                        --split train|eval|holdout --out <path.jsonl>",
            "                        --by <slug>",
        ]
    )


def _flag_lookup(args: list[str]) -> dict[str, str | bool | list[str]]:
    """Tiny GNU-style parser: --key value | --flag | positional captured separately."""

    out: dict[str, str | bool | list[str]] = {"_positional": []}
    i = 0
    while i < len(args):
        token = args[i]
        if token.startswith("--"):
            key = token[2:].replace("-", "_")
            if i + 1 < len(args) and not args[i + 1].startswith("--"):
                out[key] = args[i + 1]
                i += 2
                continue
            out[key] = True
            i += 1
            continue
        positional = out.setdefault("_positional", [])
        assert isinstance(positional, list)
        positional.append(token)
        i += 1
    return out


def _payload_to_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _emit(stdout: TextIO, payload: dict[str, Any]) -> int:
    print_json(stdout, payload)
    return 0 if not payload.get("error") else 1


def _call(stdout: TextIO, params: dict[str, Any]) -> int:
    rc, payload = run_cli_tool(_TOOL, params)
    print_json(stdout, payload)
    return rc


def _dataset_command(args: list[str], *, stdout: TextIO) -> int:
    """Entry point registered in ``surfaces/cli/praxis.py`` as ``praxis dataset``."""

    if not args or args[0] in {"-h", "--help", "help"}:
        stdout.write(_help_text() + "\n")
        return 0

    head, *rest = args

    if head == "summary":
        return _call(stdout, {"action": "summary"})

    if head == "inbox":
        flags = _flag_lookup(rest)
        params: dict[str, Any] = {"action": "inbox"}
        if "kind" in flags:
            params["candidate_kind"] = flags["kind"]
        if "specialist" in flags:
            params["specialist_target"] = flags["specialist"]
        if "limit" in flags:
            params["limit"] = _payload_to_int(flags["limit"], 25)
        if "offset" in flags:
            params["offset"] = _payload_to_int(flags["offset"], 0)
        return _call(stdout, params)

    if head == "candidates":
        sub = rest[0] if rest else ""
        flags = _flag_lookup(rest[1:])
        if sub == "list":
            params: dict[str, Any] = {"action": "candidates_list"}
            if "kind" in flags:
                params["candidate_kind"] = flags["kind"]
            if "route" in flags:
                params["route_slug"] = flags["route"]
            if "eligibility" in flags:
                params["eligibility"] = flags["eligibility"]
            if "policy" in flags:
                params["policy_id"] = flags["policy"]
            if "redaction" in flags:
                params["redaction_status"] = flags["redaction"]
            if "staleness" in flags:
                params["staleness_status"] = flags["staleness"]
            if "limit" in flags:
                params["limit"] = _payload_to_int(flags["limit"], 50)
            if "offset" in flags:
                params["offset"] = _payload_to_int(flags["offset"], 0)
            return _call(stdout, params)
        if sub == "scan":
            params = {"action": "candidates_scan"}
            if "limit" in flags:
                params["limit"] = _payload_to_int(flags["limit"], 100)
            if flags.get("backfill"):
                params["backfill"] = True
            if "since_days" in flags:
                params["since_days"] = _payload_to_int(flags["since_days"], 7)
            if "receipt_ids" in flags:
                params["receipt_ids"] = str(flags["receipt_ids"])
            return _call(stdout, params)
        stdout.write("usage: praxis dataset candidates <list|scan>\n")
        return 2

    if head == "candidate":
        sub = rest[0] if rest else ""
        positionals = [r for r in rest[1:] if not r.startswith("--")]
        flags = _flag_lookup(rest[1:])
        if sub == "inspect":
            if not positionals:
                stdout.write("usage: praxis dataset candidate inspect <candidate_id>\n")
                return 2
            return _call(stdout, {"action": "candidate_inspect", "candidate_id": positionals[0]})
        if sub == "promote":
            if not positionals:
                stdout.write("usage: praxis dataset candidate promote <candidate_id> --family ...\n")
                return 2
            payload_path = str(flags.get("payload") or "")
            if not payload_path:
                stdout.write("--payload <file.json> is required\n")
                return 2
            payload = load_json_file(payload_path)
            params = {
                "action": "candidate_promote",
                "candidate_ids": [positionals[0]],
                "dataset_family": flags.get("family", "sft"),
                "specialist_target": flags.get("specialist"),
                "policy_id": flags.get("policy"),
                "payload": payload,
                "promoted_by": flags.get("by"),
                "rationale": flags.get("rationale"),
            }
            if "split" in flags:
                params["split_tag"] = flags["split"]
            if "decision_ref" in flags:
                params["decision_ref"] = flags["decision_ref"]
            return _call(stdout, params)
        if sub == "reject":
            if not positionals:
                stdout.write("usage: praxis dataset candidate reject <candidate_id> --by ... --reason ...\n")
                return 2
            return _call(
                stdout,
                {
                    "action": "candidate_reject",
                    "candidate_id": positionals[0],
                    "rejected_by": flags.get("by"),
                    "reason": flags.get("reason"),
                },
            )
        stdout.write("usage: praxis dataset candidate <inspect|promote|reject>\n")
        return 2

    if head == "policy":
        sub = rest[0] if rest else ""
        positionals = [r for r in rest[1:] if not r.startswith("--")]
        flags = _flag_lookup(rest[1:])
        if sub == "list":
            params = {"action": "policy_list"}
            if "specialist" in flags:
                params["specialist_target"] = flags["specialist"]
            if flags.get("all") is True:
                params["active_only"] = False
            return _call(stdout, params)
        if sub == "show":
            if not positionals:
                stdout.write("usage: praxis dataset policy show <policy_id_or_slug>\n")
                return 2
            return _call(
                stdout,
                {"action": "policy_show", "policy_id": positionals[0], "policy_slug": positionals[0]},
            )
        if sub == "record":
            rubric_path = str(flags.get("rubric") or "")
            if not rubric_path:
                stdout.write("--rubric <file.json> is required\n")
                return 2
            rubric = load_json_file(rubric_path)
            params = {
                "action": "policy_record",
                "policy_slug": flags.get("slug"),
                "specialist_target": flags.get("specialist"),
                "rubric": rubric,
                "decided_by": flags.get("decided_by"),
                "rationale": flags.get("rationale"),
                "auto_promote": bool(flags.get("auto_promote", False)),
            }
            if "supersedes" in flags:
                params["supersedes_policy_id"] = flags["supersedes"]
            return _call(stdout, params)
        stdout.write("usage: praxis dataset policy <list|show|record>\n")
        return 2

    if head == "promotions":
        sub = rest[0] if rest else ""
        flags = _flag_lookup(rest[1:])
        if sub == "list":
            params = {"action": "promotions_list"}
            if "specialist" in flags:
                params["specialist_target"] = flags["specialist"]
            if "family" in flags:
                params["dataset_family"] = flags["family"]
            if "split" in flags:
                params["split_tag"] = flags["split"]
            if flags.get("all") is True:
                params["active_only"] = False
            if "limit" in flags:
                params["limit"] = _payload_to_int(flags["limit"], 50)
            if "offset" in flags:
                params["offset"] = _payload_to_int(flags["offset"], 0)
            return _call(stdout, params)
        stdout.write("usage: praxis dataset promotions list [...]\n")
        return 2

    if head == "promotion":
        sub = rest[0] if rest else ""
        positionals = [r for r in rest[1:] if not r.startswith("--")]
        flags = _flag_lookup(rest[1:])
        if sub == "supersede":
            if not positionals:
                stdout.write("usage: praxis dataset promotion supersede <promotion_id> --reason ... --by ...\n")
                return 2
            return _call(
                stdout,
                {
                    "action": "promotion_supersede",
                    "promotion_id": positionals[0],
                    "superseded_reason": flags.get("reason"),
                    "superseded_by_operator": flags.get("by"),
                },
            )
        stdout.write("usage: praxis dataset promotion supersede <promotion_id>\n")
        return 2

    if head == "preference":
        sub = rest[0] if rest else ""
        positionals = [r for r in rest[1:] if not r.startswith("--")]
        flags = _flag_lookup(rest[1:])
        if sub == "suggest":
            params: dict[str, Any] = {"action": "preference_suggest"}
            if "kind" in flags:
                params["candidate_kind"] = flags["kind"]
            if "specialist" in flags:
                params["specialist_target"] = flags["specialist"]
            if "limit" in flags:
                params["limit"] = _payload_to_int(flags["limit"], 20)
            return _call(stdout, params)
        if sub == "create":
            if len(positionals) < 2:
                stdout.write("usage: praxis dataset preference create <chosen_id> <rejected_id> ...\n")
                return 2
            payload_path = str(flags.get("payload") or "")
            if not payload_path:
                stdout.write("--payload <file.json> is required\n")
                return 2
            return _call(
                stdout,
                {
                    "action": "preference_create",
                    "chosen_candidate_id": positionals[0],
                    "rejected_candidate_id": positionals[1],
                    "specialist_target": flags.get("specialist"),
                    "policy_id": flags.get("policy"),
                    "payload": load_json_file(payload_path),
                    "promoted_by": flags.get("by"),
                    "rationale": flags.get("rationale"),
                    "decision_ref": flags.get("decision_ref"),
                    "split_tag": flags.get("split"),
                },
            )
        stdout.write("usage: praxis dataset preference create <chosen> <rejected> ...\n")
        return 2

    if head == "eval":
        sub = rest[0] if rest else ""
        positionals = [r for r in rest[1:] if not r.startswith("--")]
        flags = _flag_lookup(rest[1:])
        if sub == "add":
            if not positionals:
                stdout.write("usage: praxis dataset eval add <candidate_id> ...\n")
                return 2
            payload_path = str(flags.get("payload") or "")
            if not payload_path:
                stdout.write("--payload <file.json> is required\n")
                return 2
            return _call(
                stdout,
                {
                    "action": "eval_add",
                    "candidate_ids": [positionals[0]],
                    "specialist_target": flags.get("specialist"),
                    "policy_id": flags.get("policy"),
                    "payload": load_json_file(payload_path),
                    "promoted_by": flags.get("by"),
                    "rationale": flags.get("rationale"),
                    "decision_ref": flags.get("decision_ref"),
                    "split_tag": flags.get("split", "eval"),
                },
            )
        stdout.write("usage: praxis dataset eval add <candidate_id> ...\n")
        return 2

    if head == "lineage":
        flags = _flag_lookup(rest)
        params = {"action": "lineage"}
        if "promotion" in flags:
            params["promotion_id"] = flags["promotion"]
        if "candidate" in flags:
            params["candidate_id"] = flags["candidate"]
        if "specialist" in flags:
            params["specialist_target"] = flags["specialist"]
        if "limit" in flags:
            params["limit"] = _payload_to_int(flags["limit"], 200)
        return _call(stdout, params)

    if head == "manifests":
        sub = rest[0] if rest else ""
        flags = _flag_lookup(rest[1:])
        if sub == "list":
            params = {"action": "manifests_list"}
            if "specialist" in flags:
                params["specialist_target"] = flags["specialist"]
            if "family" in flags:
                params["dataset_family"] = flags["family"]
            if "limit" in flags:
                params["limit"] = _payload_to_int(flags["limit"], 50)
            return _call(stdout, params)
        stdout.write("usage: praxis dataset manifests list [...]\n")
        return 2

    if head == "projection":
        sub = rest[0] if rest else ""
        flags = _flag_lookup(rest[1:])
        if sub == "refresh":
            params = {"action": "projection_refresh"}
            if "limit" in flags:
                params["limit"] = _payload_to_int(flags["limit"], 100)
            return _call(stdout, params)
        stdout.write("usage: praxis dataset projection refresh [--limit N]\n")
        return 2

    if head == "stale":
        sub = rest[0] if rest else ""
        flags = _flag_lookup(rest[1:])
        if sub == "reconcile":
            params = {"action": "stale_reconcile"}
            if "by" in flags:
                params["reconciled_by"] = flags["by"]
            return _call(stdout, params)
        stdout.write("usage: praxis dataset stale reconcile [--by <slug>]\n")
        return 2

    if head == "export":
        flags = _flag_lookup(rest)
        return _call(
            stdout,
            {
                "action": "export",
                "dataset_family": flags.get("family"),
                "specialist_target": flags.get("specialist"),
                "split_tag": flags.get("split"),
                "output_path": flags.get("out"),
                "exported_by": flags.get("by"),
            },
        )

    stdout.write(f"unknown dataset action: {head}\n")
    stdout.write(_help_text() + "\n")
    return 2


__all__ = ["_dataset_command"]
