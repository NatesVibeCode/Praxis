# praxisengine.io

Single-page landing site for Praxis Engine. Zero-build static HTML + a
Cloudflare Pages Function for email capture.

## Layout

```
sites/praxisengine/
├── index.html              # the page (all CSS + JS inline)
├── favicon.svg
├── _headers                # CF Pages security + caching headers
├── robots.txt
├── sitemap.xml
└── functions/
    └── api/
        └── subscribe.ts    # POST /api/subscribe — CF Pages Function
```

Nothing to build. Whatever's in this directory is what gets served.

## Local preview

Use any static server. Two options:

```bash
# Option A — Python
cd sites/praxisengine && python3 -m http.server 4173
# → http://localhost:4173

# Option B — wrangler (also runs the Pages Function)
npx wrangler pages dev sites/praxisengine
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

Out of the box, `/api/subscribe` validates + rate-limits but does not persist. To turn on storage:

1. CF dashboard → Workers & Pages → your Pages project → **Settings → Functions**
2. Under **KV namespace bindings**, add:
   - Variable name: `SUBSCRIBERS`
   - KV namespace: create a new one called `praxisengine-subscribers`
3. Redeploy.

Submissions now land in the KV namespace, keyed by `email:<address>`, value is JSON `{email, source, ts, ip, ua}`. Read them back via the dashboard or:

```bash
wrangler kv:key list --binding SUBSCRIBERS --preview false
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

All content lives in `index.html`:

- **Headline:** look for `<h1>`
- **Subhead:** `<p class="lede">`
- **Status chips:** `<div class="meta">`
- **Footer:** `<footer>`

Colors are CSS variables at the top of the `<style>` block (`--bg`, `--fg`, etc.).

## Notes

- CSP is strict: only allows Google Fonts and same-origin scripts/styles. If you add analytics or an external form provider, update `_headers`.
- The page intentionally does not load a JS framework or bundler. Keep it that way until it has more than ~5 pages — at that point, migrate to Astro.
- Custom domain uses Cloudflare's native DNS → no AWS/Vercel/Netlify needed.
