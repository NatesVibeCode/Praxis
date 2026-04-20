import json, os, sys, urllib.parse, urllib.request, pathlib, time
sys.path.insert(0, '/workspace/Code&DBs/Workflow')
try:
    from runtime.workflow.mcp_session import mint_workflow_mcp_session_token
except ImportError:
    def mint_workflow_mcp_session_token(**kwargs):
        return "test_token"

ART = pathlib.Path('artifacts/hubspot_enrichment_20260418')

try:
    bundle = json.loads(os.environ['PRAXIS_EXECUTION_BUNDLE'])
except KeyError:
    bundle = {
        "job_label": "push_notes_to_hubspot",
        "tool_policy": {
            "mcp_tools": ["praxis_context_shard", "praxis_query", "praxis_discover",
                          "praxis_recall", "praxis_health", "praxis_integration"]
        }
    }

allowed = bundle.get('mcp_tool_names', bundle.get('tool_policy', {}).get('mcp_tools', []))
token = mint_workflow_mcp_session_token(
    run_id=bundle.get('run_id'), workflow_id=bundle.get('workflow_id'),
    job_label=bundle.get('job_label', ''), allowed_tools=allowed,
)
MCP_URL = ('http://host.docker.internal:8420/mcp'
           f"?allowed_tools={urllib.parse.quote(','.join(allowed))}"
           f"&workflow_token={urllib.parse.quote(token)}")
_id = 0


def mcp(tool, arguments, timeout=120):
    global _id; _id += 1
    payload = {'jsonrpc': '2.0', 'id': _id, 'method': 'tools/call',
               'params': {'name': tool, 'arguments': arguments}}
    req = urllib.request.Request(MCP_URL, data=json.dumps(payload).encode(),
                                headers={'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            rpc = json.loads(resp.read())
    except Exception as e:
        print(f"Warning: MCP request failed: {e}")
        raise
    if 'error' in rpc:
        raise RuntimeError(f"MCP error: {rpc['error']}")
    for c in rpc['result']['content']:
        if c.get('type') == 'text':
            return json.loads(c['text'])
    raise RuntimeError('no text content')


def hub_create_note(properties, associations, timeout=120):
    arguments = {
        'action': 'call',
        'integration_id': 'hubspot',
        'integration_action': 'create_note',
        'args': {
            'properties': properties,
            'associations': associations,
        }
    }
    result = mcp('praxis_integration', arguments, timeout=timeout)
    if result.get('status') != 'succeeded':
        raise RuntimeError(f"HubSpot create_note failed: {json.dumps(result)[:2000]}")
    return result['data']['response']


def main():
    angles = json.loads((ART / '03_outreach_angles.json').read_text())
    print(f"Loaded {len(angles)} outreach angle records")

    results = []
    for rec in angles:
        deal_id = rec['deal_id']
        contact_id = rec['contact_id']
        angle_md = rec['angle_markdown']
        epoch_ms = str(int(time.time() * 1000))

        print(f"Creating note for deal {deal_id} / contact {contact_id} ...")
        try:
            resp = hub_create_note(
                properties={
                    'hs_note_body': angle_md,
                    'hs_timestamp': epoch_ms,
                },
                associations=[
                    {
                        'to': {'id': deal_id},
                        'types': [{'associationCategory': 'HUBSPOT_DEFINED',
                                   'associationTypeId': 214}],
                    },
                    {
                        'to': {'id': contact_id},
                        'types': [{'associationCategory': 'HUBSPOT_DEFINED',
                                   'associationTypeId': 202}],
                    },
                ],
            )
            note_id = resp.get('id')
            print(f"  -> note_id={note_id}")
            results.append({
                'deal_id': deal_id,
                'contact_id': contact_id,
                'note_id': note_id,
                'hubspot_status': 'succeeded',
            })
        except Exception as e:
            error_detail = str(e)[:500]
            print(f"  -> FAILED: {error_detail}")
            results.append({
                'deal_id': deal_id,
                'contact_id': contact_id,
                'note_id': None,
                'hubspot_status': 'failed',
                'error_detail': error_detail,
            })

    out_path = ART / '04_note_ids.json'
    out_path.write_text(json.dumps(results, indent=2))
    print(f"Wrote {len(results)} results to {out_path}")

    failed = [r for r in results if r['hubspot_status'] == 'failed']
    if failed:
        print(f"WARNING: {len(failed)}/{len(results)} notes failed")
    else:
        print(f"All {len(results)} notes created successfully")


if __name__ == '__main__':
    main()
