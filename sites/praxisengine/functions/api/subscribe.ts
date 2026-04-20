// Cloudflare Pages Function: POST /api/subscribe
//
// Current behavior (v1):
//   - validates the request
//   - rate-limits lightly by source IP (via KV if bound; otherwise best-effort)
//   - persists to a KV namespace named SUBSCRIBERS if bound in the CF dashboard
//   - always returns 200 on accepted emails so the UI stays snappy
//
// To enable persistence:
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
  const ts = typeof body.ts === "number" ? body.ts : Date.now();
  const ip = request.headers.get("CF-Connecting-IP") ?? "unknown";
  const ua = (request.headers.get("User-Agent") ?? "").slice(0, 256);

  // Best-effort rate limit: max 5 submissions / hour / IP when KV is bound.
  if (env.SUBSCRIBERS) {
    const rlKey = `rl:${ip}`;
    const prior = Number((await env.SUBSCRIBERS.get(rlKey)) ?? 0);
    if (prior >= 5) {
      return json({ ok: false, error: "rate_limited" }, 429);
    }
    await env.SUBSCRIBERS.put(rlKey, String(prior + 1), { expirationTtl: 3600 });
  }

  // Persist when KV bound.
  if (env.SUBSCRIBERS) {
    const key = `email:${email}`;
    const payload = JSON.stringify({ email, source, ts, ip, ua });
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
        body: JSON.stringify({ email, source, ts }),
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
