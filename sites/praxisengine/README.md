# praxisengine.io

Single-page landing site for Praxis Engine. Zero-build static HTML + a
Cloudflare Pages Function for email capture.

## Layout

```
sites/praxisengine/
├── index.html              # the page shell
├── favicon.svg
├── praxis-tokens.css       # shared design tokens
├── page.css                # page layout styles
├── sections.css            # interactive section styles
├── app.jsx                 # React source
├── contract.jsx            # contract demo source
├── canary.jsx              # refund batch demo source
├── loop.jsx                # anti-pattern loop source
├── app.js                  # generated browser bundle served in production
├── vendor/                 # local React runtime files
├── _headers                # CF Pages security + caching headers
├── robots.txt
├── sitemap.xml
├── schema.sql              # D1 subscriber storage schema
├── wrangler.toml           # Pages Function bindings
├── tools/
│   ├── migrate_kv_to_d1.py # one-time KV → D1 subscriber migration
│   └── subscriber_notifier.py # D1 poller, CSV appender, macOS notifier
└── functions/
    └── api/
        └── subscribe.ts    # POST /api/subscribe — CF Pages Function
```

Cloudflare still has nothing to build. Whatever's in this directory is what
gets served. When the JSX source changes, regenerate `app.js` before deploy.

## Local preview

Use any static server. Two options:

```bash
# Option A — Python
cd sites/praxisengine && python3 -m http.server 4173
# → http://localhost:4173

# Option B — wrangler (also runs the Pages Function)
cd sites/praxisengine && npx wrangler pages dev .
# → http://localhost:8788
```

Only `wrangler pages dev` will exercise `/api/subscribe` locally.

## Deploy (first time)

This is the one-time setup. After this, pushes to the connected branch auto-deploy.

1. **Cloudflare dashboard** → Workers & Pages → Create → Pages → Connect to Git
2. Pick the Praxis repo. For **Root directory** enter `sites/praxisengine`.
3. **Build command:** *(leave blank — no build)*
4. **Build output directory:** `.` (just a dot)
5. Save and deploy. First deploy lands on a `*.pages.dev` URL.
6. **Custom domain:** Pages project → Custom domains → Add `praxisengine.io`. Since the domain is already in your Cloudflare account, DNS auto-wires (proxied CNAME). SSL provisions in ~1 minute.

### Alternative: direct upload (no Git wiring)

```bash
npm i -g wrangler
wrangler login
cd sites/praxisengine
wrangler pages deploy . --project-name praxisengine
```

First run prompts you to create the project.

## Enable email persistence

Out of the box, `/api/subscribe` validates but only persists when storage is
bound. The recommended primary store is Cloudflare D1 because it is queryable,
exportable, and lives under the same Cloudflare Pages authority as the site.

### Recommended: D1

```bash
wrangler d1 create praxisengine-subscribers
wrangler d1 execute praxisengine-subscribers --remote --file sites/praxisengine/schema.sql
```

The `wrangler.toml` file binds that database to the Pages Function as
`SUBSCRIBERS_DB`; redeploy after changing bindings.

The durable tables are:

- `subscribers`: one deduped row per email with first/last source, timestamps, and submit count
- `subscriber_events`: append-only submission events for basic auditability

### Optional: KV fallback/rate limit

KV is still useful for lightweight rate limiting and can serve as a persistence
fallback when D1 is absent.

1. Cloudflare dashboard → Workers & Pages → `praxisengine` → **Settings → Functions**
2. Under **KV namespace bindings**, add:
   - Variable name: `SUBSCRIBERS`
   - KV namespace: create one called `praxisengine-subscribers`
3. Redeploy.

## Subscriber notifier

The notifier polls D1 `subscriber_events`, appends new rows to a local CSV, and
fires a macOS desktop notification.

Defaults:

- Database: `praxisengine-subscribers`
- CSV: `~/Documents/PraxisEngine/subscribers.csv`
- State: `~/Library/Application Support/PraxisEngine/subscriber_notifier_state.json`
- Poll interval: 30 minutes

Run one polling pass:

```bash
python3 sites/praxisengine/tools/subscriber_notifier.py run --once
```

Install as a LaunchAgent:

```bash
python3 sites/praxisengine/tools/subscriber_notifier.py install-launch-agent
```

Stop/remove it:

```bash
python3 sites/praxisengine/tools/subscriber_notifier.py uninstall-launch-agent
```

One-time KV to D1 migration:

```bash
python3 sites/praxisengine/tools/migrate_kv_to_d1.py
```

### Forward to an external list (optional)

To forward signups to ConvertKit / Buttondown / Loops / webhook:

1. CF dashboard → your Pages project → Settings → Environment variables → **Production**
2. Add:
   - `SUBSCRIBE_FORWARD_URL` — the target URL
   - `SUBSCRIBE_FORWARD_TOKEN` — bearer token if the target requires auth (optional)
3. Redeploy.

The function POSTs `{email, source, ts}` to that URL. Failures are swallowed so the UI stays snappy.

## Changing copy

The page shell lives in `index.html`; content and interaction live in the JSX
source files:

- **Headline:** look for `Hero` in `app.jsx`
- **Subhead:** look for `className="lede"` in `app.jsx`
- **Contract demo:** `contract.jsx`
- **Refund batch demo:** `canary.jsx`
- **Loop demo:** `loop.jsx`
- **Footer:** `<footer>`

Colors are CSS variables in `praxis-tokens.css`.

## Regenerating app.js

The production page does not run Babel in the browser. Regenerate the checked-in
bundle after changing JSX:

```bash
node -e "const fs=require('fs'); for (const f of ['sites/praxisengine/contract.jsx','sites/praxisengine/canary.jsx','sites/praxisengine/loop.jsx','sites/praxisengine/app.jsx']) process.stdout.write(fs.readFileSync(f,'utf8')+'\n');" | npx --yes esbuild@0.25.12 --loader=jsx --jsx-factory=React.createElement --jsx-fragment=React.Fragment --format=iife --target=es2018 --minify > sites/praxisengine/app.js
```

## Notes

- CSP is strict: only allows Google Fonts and same-origin scripts/styles. If you add analytics or an external form provider, update `_headers`.
- React is vendored locally and `app.js` is checked in so Cloudflare Pages can stay zero-build.
- Custom domain uses Cloudflare's native DNS → no AWS/Vercel/Netlify needed.
