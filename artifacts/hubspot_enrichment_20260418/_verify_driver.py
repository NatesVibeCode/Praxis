import json
import os
import pathlib
import sys
import urllib.parse
import urllib.request

sys.path.insert(0, "/workspace/Code&DBs/Workflow")

try:
    from runtime.workflow.mcp_session import mint_workflow_mcp_session_token
except ImportError:
    def mint_workflow_mcp_session_token(**kwargs):
        return "test_token"


ART = pathlib.Path("artifacts/hubspot_enrichment_20260418")
NOTE_IDS_PATH = ART / "04_note_ids.json"
ANGLES_PATH = ART / "03_outreach_angles.json"
OUT_PATH = ART / "05_verification.json"


try:
    bundle = json.loads(os.environ["PRAXIS_EXECUTION_BUNDLE"])
except KeyError:
    bundle = {
        "job_label": "verify_notes_live",
        "tool_policy": {
            "mcp_tools": [
                "praxis_context_shard",
                "praxis_query",
                "praxis_discover",
                "praxis_recall",
                "praxis_health",
                "praxis_integration",
            ]
        },
    }

allowed = bundle.get("mcp_tool_names", bundle.get("tool_policy", {}).get("mcp_tools", []))
token = mint_workflow_mcp_session_token(
    run_id=bundle.get("run_id"),
    workflow_id=bundle.get("workflow_id"),
    job_label=bundle.get("job_label", ""),
    allowed_tools=allowed,
)
MCP_URL = (
    "http://host.docker.internal:8420/mcp"
    f"?allowed_tools={urllib.parse.quote(','.join(allowed))}"
    f"&workflow_token={urllib.parse.quote(token)}"
)
_id = 0


def mcp(tool, arguments, timeout=120):
    global _id
    _id += 1
    payload = {
        "jsonrpc": "2.0",
        "id": _id,
        "method": "tools/call",
        "params": {"name": tool, "arguments": arguments},
    }
    req = urllib.request.Request(
        MCP_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        rpc = json.loads(resp.read())
    if "error" in rpc:
        raise RuntimeError(f"MCP error: {rpc['error']}")
    for content in rpc["result"]["content"]:
        if content.get("type") == "text":
            return json.loads(content["text"])
    raise RuntimeError("no text content")


def hub(action, args, timeout=120):
    result = mcp(
        "praxis_integration",
        {
            "action": "call",
            "integration_id": "hubspot",
            "integration_action": action,
            "args": args,
        },
        timeout=timeout,
    )
    if result.get("status") != "succeeded":
        raise RuntimeError(f"HubSpot {action} failed: {json.dumps(result)[:2000]}")
    response = result["data"]["response"]
    if isinstance(response, str):
        return json.loads(response)
    return response


def hub_collect(action, inputs, extra_args=None, timeout=120):
    results = []
    extra_args = extra_args or {}
    for input_obj in inputs:
        response = hub(
            action,
            {
                **extra_args,
                "inputs": [input_obj],
            },
            timeout=timeout,
        )
        results.extend(response.get("results", []))
    return {"results": results}


def extract_assoc_sets(response):
    assoc_sets = {}
    for item in response.get("results", []):
        from_id = (
            item.get("id")
            or item.get("fromObjectId")
            or item.get("fromId")
            or (item.get("from") or {}).get("id")
        )
        if from_id is None:
            continue

        to_ids = set()
        to_items = item.get("to") or item.get("associations") or item.get("results") or []
        if isinstance(to_items, dict):
            to_items = to_items.get("results", [])
        for assoc in to_items:
            if not isinstance(assoc, dict):
                continue
            to_id = (
                assoc.get("id")
                or assoc.get("toObjectId")
                or assoc.get("toId")
                or (assoc.get("to") or {}).get("id")
            )
            if to_id is not None:
                to_ids.add(str(to_id))

        assoc_sets[str(from_id)] = to_ids
    return assoc_sets


def rstrip(value):
    return (value or "").rstrip()


def main():
    note_rows = json.loads(NOTE_IDS_PATH.read_text())
    angle_rows = json.loads(ANGLES_PATH.read_text())
    note_ids = [str(row["note_id"]) for row in note_rows if row.get("note_id")]

    print(f"Verifying {len(note_ids)} HubSpot notes")
    if not note_ids:
        OUT_PATH.write_text("[]\n")
        print(f"No note IDs found; wrote {OUT_PATH}")
        return

    angle_by_key = {
        (str(row["deal_id"]), str(row["contact_id"])): row["angle_markdown"]
        for row in angle_rows
    }
    expected_by_note = {}
    for row in note_rows:
        note_id = row.get("note_id")
        if not note_id:
            continue
        deal_id = str(row["deal_id"])
        contact_id = str(row["contact_id"])
        expected_by_note[str(note_id)] = {
            "body": angle_by_key[(deal_id, contact_id)],
            "deal_id": deal_id,
            "contact_id": contact_id,
        }

    note_inputs = [{"id": note_id} for note_id in note_ids]

    print("Reading note bodies...")
    notes_resp = hub_collect(
        "batch_read_notes",
        note_inputs,
        {
            "properties": ["hs_note_body", "hs_timestamp"],
        },
    )
    live_notes = {str(item.get("id")): item for item in notes_resp.get("results", [])}

    print("Reading note/deal associations...")
    deal_resp = hub("batch_read_note_deal_associations", {"inputs": note_inputs})
    deal_assocs = extract_assoc_sets(deal_resp)

    print("Reading note/contact associations...")
    contact_resp = hub("batch_read_note_contact_associations", {"inputs": note_inputs})
    contact_assocs = extract_assoc_sets(contact_resp)

    verification = []
    for note_id in note_ids:
        expected = expected_by_note[note_id]
        live = live_notes.get(note_id)
        returned_body = ""
        if live:
            returned_body = (live.get("properties") or {}).get("hs_note_body") or ""

        verification.append(
            {
                "note_id": note_id,
                "live_exists": live is not None,
                "body_matches": rstrip(returned_body) == rstrip(expected["body"]),
                "deal_assoc_ok": expected["deal_id"] in deal_assocs.get(note_id, set()),
                "contact_assoc_ok": expected["contact_id"] in contact_assocs.get(note_id, set()),
            }
        )

    OUT_PATH.write_text(json.dumps(verification, indent=2) + "\n")
    print(f"Wrote {len(verification)} verification rows to {OUT_PATH}")

    failed = [
        row
        for row in verification
        if not (
            row["live_exists"]
            and row["body_matches"]
            and row["deal_assoc_ok"]
            and row["contact_assoc_ok"]
        )
    ]
    if failed:
        print(f"Verification failed for {len(failed)} notes:")
        print(json.dumps(failed, indent=2))
        return

    print("All notes verified live with matching bodies and associations")


if __name__ == "__main__":
    main()
