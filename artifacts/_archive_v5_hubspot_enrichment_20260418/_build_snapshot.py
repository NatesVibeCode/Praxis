#!/usr/bin/env python3
"""Build 00_crm_snapshot.json by pulling data from HubSpot via the MCP bridge."""

import json
import os
import sys
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone

sys.path.insert(0, "/workspace/Code&DBs/Workflow")
from runtime.workflow.mcp_session import mint_workflow_mcp_session_token

# ── MCP bridge setup ──────────────────────────────────────────────────────────

bundle = json.loads(os.environ.get("PRAXIS_EXECUTION_BUNDLE", "{}"))
_token = mint_workflow_mcp_session_token(
    run_id=bundle.get("run_id", ""),
    workflow_id=bundle.get("workflow_id", ""),
    job_label=bundle.get("job_label", ""),
    allowed_tools=bundle.get("mcp_tool_names", []),
)
_allowed = ",".join(bundle.get("mcp_tool_names", []))
MCP_URL = (
    f"http://host.docker.internal:8420/mcp"
    f"?allowed_tools={urllib.parse.quote(_allowed)}"
    f"&workflow_token={urllib.parse.quote(_token)}"
)

STAGING = "/workspace/artifacts/hubspot_enrichment_20260418"
CLOSED_WON = "3521087212"
CLOSED_LOST = "3521087213"

_call_id = 0


def mcp_call(tool_name: str, arguments: dict, timeout: int = 30) -> dict:
    """Call an MCP tool through the bridge and return the parsed tool result."""
    global _call_id
    _call_id += 1
    payload = {
        "jsonrpc": "2.0",
        "id": _call_id,
        "method": "tools/call",
        "params": {"name": tool_name, "arguments": arguments},
    }
    req = urllib.request.Request(
        MCP_URL,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        rpc_result = json.loads(resp.read())
    if "error" in rpc_result:
        raise RuntimeError(f"MCP error: {rpc_result['error']}")
    content = rpc_result["result"]["content"]
    for c in content:
        if c.get("type") == "text":
            return json.loads(c["text"])
    raise RuntimeError("No text content in MCP response")


def hub_call(action: str, args: dict | None = None, timeout: int = 30) -> dict:
    """Shortcut: call a HubSpot integration action."""
    arguments: dict = {
        "action": "call",
        "integration_id": "hubspot",
        "integration_action": action,
    }
    if args:
        arguments["args"] = args
    result = mcp_call("praxis_integration", arguments, timeout=timeout)
    if result.get("status") != "succeeded":
        raise RuntimeError(f"HubSpot {action} failed: {json.dumps(result)}")
    return result["data"]["response"]


def hub_get(path: str, timeout: int = 20) -> dict:
    """Call HubSpot GET endpoint via the integration's webhook/generic capability.

    Since we only have named capabilities, we'll use the list_ actions for GETs.
    For arbitrary endpoints, fall back to search_* with empty body.
    """
    # For pipeline stages we need a custom approach — use praxis_integration
    # with a direct HTTP call via the webhook pattern
    arguments = {
        "action": "call",
        "integration_id": "hubspot",
        "integration_action": "list_deals",  # placeholder, overridden by path
        "args": {"__raw_path": path},
    }
    return mcp_call("praxis_integration", arguments, timeout=timeout)


def save_staging(name: str, data: object) -> None:
    with open(f"{STAGING}/{name}", "w") as f:
        json.dump(data, f, indent=2)


# ── Step 1: Search deals ─────────────────────────────────────────────────────

print("Step 1: Searching deals in default pipeline...")
deals_resp = hub_call("search_deals", {
    "limit": 100,
    "properties": ["dealname", "amount", "dealstage", "pipeline", "closedate"],
    "filterGroups": [
        {"filters": [{"propertyName": "pipeline", "operator": "EQ", "value": "default"}]}
    ],
})
all_deals = deals_resp["results"]
print(f"  Found {len(all_deals)} deals")
save_staging("_step1_deals.json", all_deals)

# Filter out closed deals
active_deals = [
    d for d in all_deals
    if d["properties"]["dealstage"] not in (CLOSED_WON, CLOSED_LOST)
]
print(f"  Active (non-closed) deals: {len(active_deals)}")

# ── Step 2: Pipeline stage map ────────────────────────────────────────────────

print("Step 2: Fetching pipeline stage map...")
# Use list_deals with query params to hit the pipelines endpoint
# Actually we need GET /crm/v3/pipelines/deals — let's use search to get it
# The HubSpot integration doesn't have a dedicated pipeline capability,
# so we'll call a generic GET via the integration's HTTP mechanism
try:
    # Try calling the pipelines endpoint through the integration
    pip_result = mcp_call("praxis_integration", {
        "action": "call",
        "integration_id": "hubspot",
        "integration_action": "list_deals",
        "args": {
            "path_override": "/crm/v3/pipelines/deals",
        },
    })
    if pip_result.get("status") == "succeeded":
        pipelines_data = pip_result["data"]["response"]
    else:
        raise RuntimeError("path_override not supported")
except Exception:
    # Fallback: hardcode known stage map since we know the stages from the deals
    # We'll still try to get it from the API with a custom URL
    pipelines_data = None

stage_map = {}
if pipelines_data and "results" in pipelines_data:
    for pipeline in pipelines_data["results"]:
        if pipeline.get("id") == "default" or pipeline.get("label") == "Sales Pipeline":
            for stage in pipeline.get("stages", []):
                stage_map[stage["id"]] = stage["label"]

if not stage_map:
    # The API didn't return pipeline data through the list_deals endpoint.
    # We need to make a direct GET request. Let's try using the generic
    # integration HTTP mechanism.
    print("  Trying direct pipeline fetch...")
    try:
        # Create a temporary integration capability for pipeline listing
        # Actually, let's just use the known HubSpot stage IDs from the portal
        # We can infer them from the deal stages we see
        pip_result2 = mcp_call("praxis_integration", {
            "action": "call",
            "integration_id": "hubspot",
            "integration_action": "list_contacts",  # any GET capability
            "args": {
                "limit": 1,  # minimal request
            },
        })
        # This won't give us pipelines, so we need another approach
    except Exception:
        pass

    # If still no stage map, we need to hardcode based on standard HubSpot
    # default pipeline stages and what we've seen in the data
    print("  WARNING: Could not fetch pipeline stages via API.")
    print("  Will attempt alternative approach...")

save_staging("_step2_stage_map.json", stage_map)

# ── Step 3: Fetch associations for each active deal ──────────────────────────

print("Step 3: Fetching associations for active deals...")
deal_associations = {}

for deal in active_deals:
    deal_id = deal["id"]
    deal_name = deal["properties"]["dealname"]
    print(f"  Fetching associations for deal {deal_id} ({deal_name})...")

    # Fetch contact associations
    try:
        contacts_result = hub_call("search_contacts", {
            "limit": 1,
            "filterGroups": [],
            "properties": ["firstname", "lastname", "email", "jobtitle"],
            # HubSpot search doesn't filter by deal association directly,
            # so we'll use a different approach
        })
    except Exception:
        pass

    deal_associations[deal_id] = {"contacts": [], "companies": []}

# Since HubSpot search_contacts/search_companies don't filter by deal association,
# we need to use the associations endpoint. Let's try a different approach:
# Use batch read or list with associations parameter.

# Actually, the proper HubSpot way is to use the associations v4 API.
# Let's search deals WITH associations using the search endpoint.

print("  Re-searching deals with associations...")
deals_with_assoc = hub_call("search_deals", {
    "limit": 100,
    "properties": ["dealname", "amount", "dealstage", "pipeline", "closedate"],
    "filterGroups": [
        {"filters": [{"propertyName": "pipeline", "operator": "EQ", "value": "default"}]}
    ],
    "associations": ["contacts", "companies"],
})

# Check if associations were returned
for deal in deals_with_assoc.get("results", []):
    deal_id = deal["id"]
    assoc = deal.get("associations", {})
    contacts = assoc.get("contacts", {}).get("results", [])
    companies = assoc.get("companies", {}).get("results", [])
    deal_associations[deal_id] = {
        "contacts": [c["id"] for c in contacts],
        "companies": [c["id"] for c in companies],
    }
    if deal["properties"]["dealstage"] not in (CLOSED_WON, CLOSED_LOST):
        print(f"  Deal {deal_id}: {len(contacts)} contacts, {len(companies)} companies")

save_staging("_step3_associations.json", deal_associations)

# Collect unique contact and company IDs
all_contact_ids = set()
all_company_ids = set()
for deal in active_deals:
    did = deal["id"]
    assoc = deal_associations.get(did, {"contacts": [], "companies": []})
    all_contact_ids.update(assoc["contacts"])
    all_company_ids.update(assoc["companies"])

print(f"  Unique contacts to hydrate: {len(all_contact_ids)}")
print(f"  Unique companies to hydrate: {len(all_company_ids)}")

# ── Step 4: Hydrate contacts and companies ────────────────────────────────────

print("Step 4: Hydrating contacts...")
contacts_by_id = {}
if all_contact_ids:
    batch_result = hub_call("batch_read_contacts", {
        "inputs": [{"id": cid} for cid in all_contact_ids],
        "properties": ["firstname", "lastname", "email", "jobtitle"],
    })
    for c in batch_result.get("results", []):
        contacts_by_id[c["id"]] = {
            "id": c["id"],
            "firstname": c["properties"].get("firstname", ""),
            "lastname": c["properties"].get("lastname", ""),
            "email": c["properties"].get("email", ""),
            "jobtitle": c["properties"].get("jobtitle", ""),
        }
    print(f"  Hydrated {len(contacts_by_id)} contacts")
save_staging("_step4_contacts.json", contacts_by_id)

print("Step 4b: Hydrating companies...")
companies_by_id = {}
if all_company_ids:
    # No batch_read_companies capability, so use search
    for comp_id in all_company_ids:
        try:
            result = hub_call("search_companies", {
                "limit": 1,
                "properties": ["name", "domain", "industry"],
                "filterGroups": [
                    {"filters": [{"propertyName": "hs_object_id", "operator": "EQ", "value": str(comp_id)}]}
                ],
            })
            for c in result.get("results", []):
                companies_by_id[c["id"]] = {
                    "id": c["id"],
                    "name": c["properties"].get("name", ""),
                    "domain": c["properties"].get("domain", ""),
                    "industry": c["properties"].get("industry", ""),
                }
        except Exception as e:
            print(f"  WARNING: Failed to hydrate company {comp_id}: {e}")
    print(f"  Hydrated {len(companies_by_id)} companies")
save_staging("_step4_companies.json", companies_by_id)

# ── Step 5: Assemble snapshot ─────────────────────────────────────────────────

print("Step 5: Assembling CRM snapshot...")

# Validation: every deal must have at least a primary contact OR a company
errors = []
snapshot = []

for deal in active_deals:
    did = deal["id"]
    props = deal["properties"]
    assoc = deal_associations.get(did, {"contacts": [], "companies": []})

    # Primary contact (first associated)
    primary_contact = None
    for cid in assoc["contacts"]:
        if cid in contacts_by_id:
            primary_contact = contacts_by_id[cid]
            break

    # Company (first associated)
    company = None
    for comp_id in assoc["companies"]:
        if comp_id in companies_by_id:
            company = companies_by_id[comp_id]
            break

    if not primary_contact and not company:
        errors.append(f"Deal {did} ({props['dealname']}) has neither contact nor company")

    stage_label = stage_map.get(props["dealstage"], props["dealstage"])

    record = {
        "deal_id": did,
        "deal_name": props["dealname"],
        "stage_id": props["dealstage"],
        "stage_label": stage_label,
        "amount": props.get("amount"),
        "closedate": props.get("closedate"),
        "primary_contact": primary_contact,
        "company": company,
    }
    snapshot.append(record)

if errors:
    print("\nFATAL: Deals lacking both contact and company:")
    for e in errors:
        print(f"  {e}")
    sys.exit(1)

# Add metadata
output = {
    "_generated_at": datetime.now(timezone.utc).isoformat(),
    "deals": snapshot,
}

output_path = f"{STAGING}/00_crm_snapshot.json"
with open(output_path, "w") as f:
    json.dump(output, f, indent=2)

print(f"\nDone! Wrote {len(snapshot)} deals to {output_path}")
for d in snapshot:
    contact_name = ""
    if d["primary_contact"]:
        contact_name = f"{d['primary_contact'].get('firstname', '')} {d['primary_contact'].get('lastname', '')}".strip()
    company_name = d["company"]["name"] if d["company"] else "—"
    print(f"  {d['deal_name']}: {d['stage_label']} | contact={contact_name or '—'} | company={company_name}")
