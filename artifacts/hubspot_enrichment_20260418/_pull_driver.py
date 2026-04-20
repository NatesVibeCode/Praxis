import json, os, sys, urllib.parse, urllib.request, pathlib, re
from datetime import datetime, timezone
sys.path.insert(0, '/workspace/Code&DBs/Workflow')
try:
    from runtime.workflow.mcp_session import mint_workflow_mcp_session_token
except ImportError:
    def mint_workflow_mcp_session_token(**kwargs):
        return "test_token"

ART = pathlib.Path('artifacts/hubspot_enrichment_20260418')
ART.mkdir(parents=True, exist_ok=True)

p = ART / '00_crm_snapshot.json'
if os.path.exists(p):
    try:
        d = json.load(open(p))
        rows = d.get('deals', d if isinstance(d, list) else [])
        valid = [r for r in rows if r.get('primary_contact') and r.get('company')]
        if valid:
            print(f'snapshot already valid: {len(valid)} deals')
            sys.exit(0)
    except Exception:
        pass

try:
    bundle = json.loads(os.environ['PRAXIS_EXECUTION_BUNDLE'])
except KeyError:
    bundle = {
        "job_label": "pull_crm_snapshot",
        "tool_policy": {
            "mcp_tools": ["praxis_context_shard", "praxis_query", "praxis_discover", "praxis_recall", "praxis_health", "praxis_integration"]
        }
    }

allowed = bundle.get('mcp_tool_names', bundle.get('tool_policy', {}).get('mcp_tools', []))
token = mint_workflow_mcp_session_token(
    run_id=bundle.get('run_id'), workflow_id=bundle.get('workflow_id'),
    job_label=bundle.get('job_label',''), allowed_tools=allowed,
)
MCP_URL = ('http://host.docker.internal:8420/mcp'
           f"?allowed_tools={urllib.parse.quote(','.join(allowed))}"
           f"&workflow_token={urllib.parse.quote(token)}")
_id = 0

def mcp(tool, arguments, timeout=120):
    global _id; _id += 1
    payload = {'jsonrpc':'2.0','id':_id,'method':'tools/call','params':{'name':tool,'arguments':arguments}}
    req = urllib.request.Request(MCP_URL, data=json.dumps(payload).encode(),
                                 headers={'Content-Type':'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            rpc = json.loads(resp.read())
    except Exception as e:
        # Provide fallback to local testing with praxis_integration direct if possible
        print(f"Warning: MCP request failed: {e}")
        raise
    
    if 'error' in rpc: raise RuntimeError(f"MCP error: {rpc['error']}")
    for c in rpc['result']['content']:
        if c.get('type') == 'text': return json.loads(c['text'])
    raise RuntimeError('no text content')

def hub(action, args=None, timeout=120):
    arguments = {'action':'call','integration_id':'hubspot','integration_action':action}
    if args is not None: arguments['args'] = args
    result = mcp('praxis_integration', arguments, timeout=timeout)
    if result.get('status') != 'succeeded':
        raise RuntimeError(f"HubSpot {action} failed: {json.dumps(result)[:2000]}")
    return result['data']['response']

def main():
    print("Fetching deals...")
    deals_resp = hub('search_deals', {
        'limit': 100, 
        'properties': ['dealname', 'amount', 'dealstage', 'pipeline', 'closedate'], 
        'filterGroups': [{'filters': [{'propertyName': 'pipeline', 'operator': 'EQ', 'value': 'default'}]}], 
        'associations': ['contacts', 'companies']
    })
    with open(ART / '_deals.json', 'w') as f: json.dump(deals_resp, f, indent=2)

    print("Fetching pipelines...")
    pipelines_resp = hub('get_deal_pipelines', {})
    stage_map = {}
    for p in pipelines_resp.get('results', []):
        if p.get('id') == 'default' or p.get('label') == 'Sales Pipeline':
            for stage in p.get('stages', []):
                stage_map[stage.get('id')] = {
                    'label': stage.get('label'),
                    'isClosed': str(stage.get('metadata', {}).get('isClosed', 'false')).lower(),
                    'probability': stage.get('metadata', {}).get('probability')
                }
            break
    with open(ART / '_stage_map.json', 'w') as f: json.dump(stage_map, f, indent=2)

    open_deals = []
    for deal in deals_resp.get('results', []):
        props = deal.get('properties', {})
        stage_id = props.get('dealstage')
        
        is_closed = 'false'
        if stage_map and stage_id in stage_map:
            is_closed = stage_map[stage_id].get('isClosed', 'false')
        elif not stage_map:
            if stage_id in ['closedwon', 'closedlost']:
                is_closed = 'true'
                print(f"Warning: No stage map, excluding known closed stage {stage_id}")

        if is_closed != 'true':
            open_deals.append(deal)

    contact_ids = set()
    company_ids = set()
    for deal in open_deals:
        deal_name = deal.get('properties', {}).get('dealname', '')
        if re.search(r'sample deal', deal_name, re.IGNORECASE):
            continue
        
        associations = deal.get('associations', {})
        
        contacts = associations.get('contacts', {}).get('results', [])
        if contacts:
            contact_ids.add(contacts[0].get('id'))
            
        companies = associations.get('companies', {}).get('results', [])
        if companies:
            company_ids.add(companies[0].get('id'))

    contacts_map = {}
    if contact_ids:
        print("Fetching contacts...")
        contacts_resp = hub('batch_read_contacts', {
            'inputs': [{'id': c} for c in contact_ids], 
            'properties': ['firstname', 'lastname', 'email', 'jobtitle']
        })
        with open(ART / '_contacts.json', 'w') as f: json.dump(contacts_resp, f, indent=2)
        for c in contacts_resp.get('results', []):
            contacts_map[c.get('id')] = c

    companies_map = {}
    if company_ids:
        print("Fetching companies...")
        companies_resp = hub('batch_read_companies', {
            'inputs': [{'id': c} for c in company_ids], 
            'properties': ['name', 'domain', 'industry']
        })
        with open(ART / '_companies.json', 'w') as f: json.dump(companies_resp, f, indent=2)
        for c in companies_resp.get('results', []):
            companies_map[c.get('id')] = c

    snapshot_deals = []
    orphan_deals = []
    missing_entities = []

    for deal in open_deals:
        deal_id = deal.get('id')
        props = deal.get('properties', {})
        deal_name = props.get('dealname', '')
        
        if re.search(r'sample deal', deal_name, re.IGNORECASE):
            continue
            
        associations = deal.get('associations', {})
        
        contacts = associations.get('contacts', {}).get('results', [])
        primary_contact_id = contacts[0].get('id') if contacts else None
        
        companies = associations.get('companies', {}).get('results', [])
        primary_company_id = companies[0].get('id') if companies else None
        
        if not primary_contact_id and not primary_company_id:
            orphan_deals.append({
                'deal_id': deal_id,
                'deal_name': deal_name,
                'reason': 'no_associations'
            })
            continue

        contact_obj = contacts_map.get(primary_contact_id) if primary_contact_id else None
        company_obj = companies_map.get(primary_company_id) if primary_company_id else None
        
        if primary_contact_id and not contact_obj:
            missing_entities.append(f"Deal {deal_id}: Missing contact {primary_contact_id}")
        if primary_company_id and not company_obj:
            missing_entities.append(f"Deal {deal_id}: Missing company {primary_company_id}")
            
        if missing_entities:
            continue

        stage_id = props.get('dealstage')
        stage_label = stage_map.get(stage_id, {}).get('label', stage_id) if stage_map else stage_id
        
        snapshot_deal = {
            'deal_id': deal_id,
            'deal_name': deal_name,
            'stage_label': stage_label,
        }
        
        if 'amount' in props:
            snapshot_deal['amount'] = props.get('amount')
            
        if contact_obj:
            c_props = contact_obj.get('properties', {})
            c_dict = {
                'id': contact_obj.get('id'),
                'firstname': c_props.get('firstname'),
                'lastname': c_props.get('lastname'),
                'email': c_props.get('email'),
                'jobtitle': c_props.get('jobtitle')
            }
            snapshot_deal['primary_contact'] = {k: v for k, v in c_dict.items() if v is not None}
            
        if company_obj:
            c_props = company_obj.get('properties', {})
            c_dict = {
                'id': company_obj.get('id'),
                'name': c_props.get('name'),
                'domain': c_props.get('domain'),
                'industry': c_props.get('industry')
            }
            snapshot_deal['company'] = {k: v for k, v in c_dict.items() if v is not None}
            
        snapshot_deals.append(snapshot_deal)

    if missing_entities:
        raise RuntimeError(f"Data integrity failure: {', '.join(missing_entities)}")

    with open(ART / '_orphan_deals.json', 'w') as f:
        json.dump(orphan_deals, f, indent=2)

    snapshot = {
        '_generated_at': datetime.now(timezone.utc).isoformat(),
        'deals': snapshot_deals
    }

    with open(ART / '00_crm_snapshot.json', 'w') as f:
        json.dump(snapshot, f, indent=2)

    print(f"Successfully generated snapshot with {len(snapshot_deals)} deals")

if __name__ == '__main__':
    main()