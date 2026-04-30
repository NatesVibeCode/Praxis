#!/usr/bin/env python3
"""Wave -1 operator burndown.

Deterministic, no-LLM. Three buckets:

  1. Roadmap dupe collapse: 12 'Review activity truth evidence drift' rows
     filed 2026-04-02. Keep the earliest by created_at; retire the rest via
     praxis_operator_write action=retire.

  2. Bug dupe collapse: bug_572fce93 / bug_20085df2 share an identical title
     filed 13 seconds apart on 2026-04-28 (auto-filer race). Keep the
     earlier; resolve the later as WONT_FIX with a reference back.

  3. Proof-backed closeout sweep across the 31 FIX_PENDING_VERIFICATION
     rows. Uses praxis_operator_closeout preview -> commit so only rows
     with already-attached validates_fix evidence flip to FIXED. Rows
     without evidence stay FPV (correct).

All catalog calls go through bin/praxis-agent so the gateway records
receipts in authority_operation_receipts.
"""
import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[5]
AGENT = REPO / "bin" / "praxis-agent"

DUPE_KEEP_ID = "roadmap_item.67f9968048.activity-truth.cockpit"
DUPE_RETIRE_IDS = [
    "roadmap_item.3bab0a06e3.activity-truth.cockpit",
    "roadmap_item.9557bfd3bc.activity-truth.cockpit",
    "roadmap_item.4379d5d19c.activity-truth.cockpit",
    "roadmap_item.2a66ce827c.activity-truth.cockpit",
    "roadmap_item.f2d2213feb.activity-truth.cockpit",
    "roadmap_item.044572c78f.activity-truth.cockpit",
    "roadmap_item.7e0f249aa7.activity-truth.cockpit",
    "roadmap_item.227b891bbf.activity-truth.cockpit",
    "roadmap_item.6bd2c3bee7.activity-truth.cockpit",
    "roadmap_item.d48079a039.activity-truth.cockpit",
    "roadmap_item.780c33a110.activity-truth.cockpit",
]

BUG_DUPE_KEEP = "BUG-20085DF2"
BUG_DUPE_RETIRE = "BUG-572FCE93"

FPV_BUG_IDS = [
    "BUG-5444AA3C", "BUG-EBE27625", "BUG-D4CC68A9", "BUG-1B959922",
    "BUG-026AB2E7", "BUG-097AB98E", "BUG-534E7290", "BUG-31E77A5E",
    "BUG-72695EF3", "BUG-911BCE24", "BUG-632E6F45", "BUG-1879A498",
    "BUG-D2ED53B4", "BUG-2907B68C", "BUG-110F4EA3", "BUG-D358869B",
    "BUG-F7D535EF", "BUG-B4D18A71", "BUG-9D24097A", "BUG-AE3BE4E0",
]


def call(tool, payload):
    result = subprocess.run(
        [str(AGENT), tool, "--input-json", json.dumps(payload), "--yes"],
        capture_output=True, text=True, timeout=30,
    )
    if result.returncode != 0:
        return {"ok": False, "stderr": result.stderr.strip(), "stdout": result.stdout.strip()}
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"ok": False, "raw": result.stdout.strip()}


def preview_roadmap_dupes():
    out = []
    for key in DUPE_RETIRE_IDS:
        r = call("praxis_operator_write", {
            "action": "retire",
            "roadmap_item_id": key,
            "status": "completed",
            "dry_run": True,
        })
        out.append({
            "key": key,
            "ok": r.get("ok"),
            "errors": r.get("blocking_errors") or r.get("error"),
        })
    return out


def preview_bug_dupe():
    return call("praxis_bugs", {
        "action": "show",
        "bug_id": BUG_DUPE_RETIRE,
    })


def closeout_fpv_preview():
    r = call("praxis_operator_closeout", {
        "action": "preview",
        "bug_ids": FPV_BUG_IDS,
    })
    if isinstance(r, dict):
        return {
            "evaluated": r.get("evaluated", {}).get("bug_ids", []),
            "candidates": [b.get("bug_id") for b in r.get("candidates", {}).get("bugs", [])],
            "skipped": [
                {"bug_id": b.get("bug_id"), "reason": b.get("reason_codes")}
                for b in r.get("skipped", {}).get("bugs", [])
            ],
        }
    return r


def commit_roadmap_dupes():
    out = []
    for key in DUPE_RETIRE_IDS:
        r = call("praxis_operator_write", {
            "action": "retire",
            "roadmap_item_id": key,
            "status": "completed",
            "dry_run": False,
        })
        out.append({
            "key": key,
            "ok": r.get("ok"),
            "committed": r.get("committed"),
            "errors": r.get("blocking_errors") or r.get("error"),
        })
    return out


def commit_bug_dupe():
    return call("praxis_bugs", {
        "action": "resolve",
        "bug_id": BUG_DUPE_RETIRE,
        "status": "WONT_FIX",
        "resolution_summary": f"duplicate auto-file of {BUG_DUPE_KEEP} (Phase B candidate identity tuple)",
    })


def commit_fpv_closeout(proof_backed):
    if not proof_backed:
        return {"closed_count": 0, "note": "no proof-backed FPV rows"}
    commit = call("praxis_operator_closeout", {
        "action": "commit",
        "bug_ids": proof_backed,
    })
    return {"commit": commit, "closed_count": len(proof_backed)}


def extract_proof_backed(preview):
    out = []
    if isinstance(preview, dict):
        for entry in preview.get("candidates", []) or preview.get("ready", []) or []:
            bid = entry.get("bug_id") or entry.get("bug_key")
            if bid:
                out.append(bid)
    return out


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "preview"
    if mode == "preview":
        report = {
            "mode": "preview",
            "roadmap_dupes": preview_roadmap_dupes(),
            "bug_dupe": preview_bug_dupe(),
            "fpv_closeout_preview": closeout_fpv_preview(),
        }
    elif mode == "commit":
        fpv_preview = closeout_fpv_preview()
        proof_backed = extract_proof_backed(fpv_preview)
        report = {
            "mode": "commit",
            "roadmap_dupes": commit_roadmap_dupes(),
            "bug_dupe": commit_bug_dupe(),
            "fpv_closeout": commit_fpv_closeout(proof_backed),
        }
    else:
        print(f"unknown mode: {mode}", file=sys.stderr)
        sys.exit(2)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
