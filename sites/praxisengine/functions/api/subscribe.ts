// Cloudflare Pages Function: POST /api/subscribe
//
// Current behavior:
//   - validates the request
//   - rate-limits lightly by source IP (via KV if bound; otherwise best-effort)
//   - persists to D1 when SUBSCRIBERS_DB is bound
//   - falls back to KV when SUBSCRIBERS is bound but D1 is not
//   - always returns 200 on accepted emails so the UI stays snappy
//
// Recommended persistence:
//   1. Create a D1 database named praxisengine-subscribers
//   2. Apply sites/praxisengine/schema.sql
//   3. Bind it to this Pages project as SUBSCRIBERS_DB
//
// KV fallback:
//   1. In the Cloudflare dashboard: Workers & Pages → your project → Settings → Functions
//   2. Add a KV binding: Variable name = SUBSCRIBERS, Namespace = (create one named praxisengine-subscribers)
//   3. Redeploy. Submissions will appear in the KV namespace keyed by email.
//
// To forward to an external service (ConvertKit, Buttondown, Loops, etc.):
//   - add a secret SUBSCRIBE_FORWARD_URL in the Pages project settings
//   - add a secret SUBSCRIBE_FORWARD_TOKEN if the service needs auth
//   - this function will POST { email, source, ts } to that URL with a Bearer token

interface Env {
  SUBSCRIBERS?: KVNamespace;
  SUBSCRIBERS_DB?: D1Database;
  SUBSCRIBE_FORWARD_URL?: string;
  SUBSCRIBE_FORWARD_TOKEN?: string;
}

interface SubscribeBody {
  email?: string;
  source?: string;
  ts?: number;
}

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

export const onRequestPost: PagesFunction<Env> = async ({ request, env }) => {
  let body: SubscribeBody;
  try {
    body = await request.json();
  } catch {
    return json({ ok: false, error: "invalid_json" }, 400);
  }

  const email = (body.email ?? "").trim().toLowerCase();
  if (!email || email.length > 254 || !EMAIL_RE.test(email)) {
    return json({ ok: false, error: "invalid_email" }, 400);
  }

  const source = (body.source ?? "landing").slice(0, 64);
  const submittedAt = new Date().toISOString();
  const ip = request.headers.get("CF-Connecting-IP") ?? "unknown";

  // Best-effort rate limit: max 5 submissions / hour / IP when KV is bound.
  if (env.SUBSCRIBERS) {
    const rlKey = `rl:${ip}`;
    const prior = Number((await env.SUBSCRIBERS.get(rlKey)) ?? 0);
    if (prior >= 5) {
      return json({ ok: false, error: "rate_limited" }, 429);
    }
    await env.SUBSCRIBERS.put(rlKey, String(prior + 1), { expirationTtl: 3600 });
  }

  // D1 is the primary subscriber authority. It keeps the deduped subscriber
  // record and an append-only event trail without storing IP or user-agent.
  if (env.SUBSCRIBERS_DB) {
    await env.SUBSCRIBERS_DB.batch([
      env.SUBSCRIBERS_DB.prepare(`
        INSERT INTO subscribers (
          email,
          first_source,
          last_source,
          created_at,
          updated_at,
          submit_count
        )
        VALUES (?, ?, ?, ?, ?, 1)
        ON CONFLICT(email) DO UPDATE SET
          last_source = excluded.last_source,
          updated_at = excluded.updated_at,
          submit_count = subscribers.submit_count + 1
      `).bind(email, source, source, submittedAt, submittedAt),
      env.SUBSCRIBERS_DB.prepare(`
        INSERT INTO subscriber_events (email, source, created_at)
        VALUES (?, ?, ?)
      `).bind(email, source, submittedAt),
    ]);
  } else if (env.SUBSCRIBERS) {
    const key = `email:${email}`;
    const payload = JSON.stringify({ email, source, ts: submittedAt });
    await env.SUBSCRIBERS.put(key, payload);
  }

  // Optional forward to external provider.
  if (env.SUBSCRIBE_FORWARD_URL) {
    try {
      await fetch(env.SUBSCRIBE_FORWARD_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(env.SUBSCRIBE_FORWARD_TOKEN
            ? { Authorization: `Bearer ${env.SUBSCRIBE_FORWARD_TOKEN}` }
            : {}),
        },
        body: JSON.stringify({ email, source, ts: submittedAt }),
      });
    } catch {
      // swallow — do not fail the user because a downstream forwarder was flaky
    }
  }

  return json({ ok: true });
};

export const onRequest: PagesFunction<Env> = async ({ request }) => {
  if (request.method === "OPTIONS") {
    return new Response(null, {
      status: 204,
      headers: {
        "Access-Control-Allow-Origin": "https://praxisengine.io",
        "Access-Control-Allow-Methods": "POST, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type",
        "Access-Control-Max-Age": "86400",
      },
    });
  }
  return json({ ok: false, error: "method_not_allowed" }, 405);
};

function json(data: unknown, status = 200): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Cache-Control": "no-store",
    },
  });
}
