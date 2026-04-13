const JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8",
};

export const BRIDGE_TOKEN_ENV = "BRIDGE_TOKEN";
export const WORKSPACE_ROOT = "/workspace";
export const BRIDGE_TMP_ROOT = "/tmp/praxis-bridge";
export const ARCHIVE_BASE64_PATH = `${BRIDGE_TMP_ROOT}/workspace.tar.gz.b64`;
export const ARCHIVE_BINARY_PATH = `${BRIDGE_TMP_ROOT}/workspace.tar.gz`;
export const BASELINE_MANIFEST_PATH = `${BRIDGE_TMP_ROOT}/baseline-manifest.json`;

const CREATE_SESSION_PATH = /^\/sessions\/create\/?$/;
const SESSION_ACTION_PATH =
  /^\/sessions\/(?<sessionId>[A-Za-z0-9._:-]+)\/(?<action>hydrate|exec|artifacts|destroy)\/?$/;

const MANIFEST_CAPTURE_COMMAND = `
python - <<'PY'
import json
import os
from pathlib import Path

root = Path("/workspace")
manifest = {}
if root.exists():
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        filenames.sort()
        for filename in filenames:
            absolute = Path(dirpath) / filename
            relpath = absolute.relative_to(root).as_posix()
            try:
                stat = absolute.stat()
            except OSError:
                continue
            manifest[relpath] = [int(stat.st_size), int(stat.st_mtime_ns)]

baseline = Path("/tmp/praxis-bridge/baseline-manifest.json")
baseline.parent.mkdir(parents=True, exist_ok=True)
baseline.write_text(json.dumps(manifest, sort_keys=True), encoding="utf-8")
print(json.dumps({"hydrated_files": len(manifest)}))
PY`.trim();

const HYDRATE_ARCHIVE_COMMAND = `
python - <<'PY'
import base64
from pathlib import Path

encoded_path = Path("/tmp/praxis-bridge/workspace.tar.gz.b64")
binary_path = Path("/tmp/praxis-bridge/workspace.tar.gz")
binary_path.parent.mkdir(parents=True, exist_ok=True)
binary_path.write_bytes(base64.b64decode(encoded_path.read_text(encoding="utf-8")))
PY
rm -rf /workspace
mkdir -p /workspace
tar -xzf /tmp/praxis-bridge/workspace.tar.gz -C /
rm -f /tmp/praxis-bridge/workspace.tar.gz /tmp/praxis-bridge/workspace.tar.gz.b64`.trim();

const ARTIFACT_CAPTURE_COMMAND = `
python - <<'PY'
import base64
import json
import os
from pathlib import Path

root = Path("/workspace")
baseline_path = Path("/tmp/praxis-bridge/baseline-manifest.json")
baseline = {}
if baseline_path.exists():
    try:
        baseline = json.loads(baseline_path.read_text(encoding="utf-8") or "{}")
    except json.JSONDecodeError:
        baseline = {}

current = {}
if root.exists():
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        filenames.sort()
        for filename in filenames:
            absolute = Path(dirpath) / filename
            relpath = absolute.relative_to(root).as_posix()
            try:
                stat = absolute.stat()
            except OSError:
                continue
            current[relpath] = [int(stat.st_size), int(stat.st_mtime_ns)]

changed = sorted(
    relpath for relpath, metadata in current.items() if baseline.get(relpath) != metadata
)
artifacts = []
for relpath in changed:
    try:
        content = (root / relpath).read_bytes()
    except OSError:
        continue
    artifacts.append(
        {
            "path": relpath,
            "content_base64": base64.b64encode(content).decode("ascii"),
        }
    )

print(json.dumps({"artifact_refs": changed, "artifacts": artifacts}))
PY`.trim();

export async function handleBridgeRequest(request, env, deps = {}) {
  const url = new URL(request.url);

  if (request.method === "GET" && (url.pathname === "/" || url.pathname === "/healthz")) {
    return jsonResponse({
      ok: true,
      service: "praxis-cloudflare-sandbox-bridge",
      auth_required: Boolean(readBridgeToken(env)),
      routes: [
        "/sessions/create",
        "/sessions/{id}/hydrate",
        "/sessions/{id}/exec",
        "/sessions/{id}/artifacts",
        "/sessions/{id}/destroy",
      ],
    });
  }

  const authFailure = authorizeRequest(request, env);
  if (authFailure) {
    return authFailure;
  }

  if (request.method !== "POST") {
    return errorResponse(405, "method_not_allowed", "Only POST is supported for bridge operations.");
  }

  if (CREATE_SESSION_PATH.test(url.pathname)) {
    const body = await readJsonBody(request);
    if ("errorResponse" in body) {
      return body.errorResponse;
    }
    return handleCreateSession(body.payload, env, deps);
  }

  const match = SESSION_ACTION_PATH.exec(url.pathname);
  if (!match?.groups) {
    return errorResponse(404, "route_not_found", `Unknown bridge route: ${url.pathname}`);
  }

  const { sessionId, action } = match.groups;
  const body = await readJsonBody(request);
  if ("errorResponse" in body) {
    return body.errorResponse;
  }
  const sandbox = resolveSandbox(env, sessionId, deps);

  switch (action) {
    case "hydrate":
      return handleHydrateSession(sandbox, body.payload);
    case "exec":
      return handleExecSession(sandbox, body.payload, deps);
    case "artifacts":
      return handleArtifactsSession(sandbox);
    case "destroy":
      return handleDestroySession(sandbox);
    default:
      return errorResponse(404, "route_not_found", `Unsupported action: ${action}`);
  }
}

export function authorizeRequest(request, env) {
  const expected = readBridgeToken(env);
  if (!expected) {
    return null;
  }
  const header = request.headers.get("authorization") || "";
  if (!header.startsWith("Bearer ")) {
    return errorResponse(401, "unauthorized", "Missing bearer token.");
  }
  const provided = header.slice("Bearer ".length).trim();
  if (provided !== expected) {
    return errorResponse(401, "unauthorized", "Bearer token mismatch.");
  }
  return null;
}

export function readBridgeToken(env) {
  const value = env?.[BRIDGE_TOKEN_ENV];
  return typeof value === "string" ? value.trim() : "";
}

export async function handleCreateSession(payload, env, deps) {
  const policyError = validateSessionPolicy(payload);
  if (policyError) {
    return policyError;
  }

  const providerSessionId = deps.generateSessionId?.() || crypto.randomUUID();
  const sandbox = resolveSandbox(env, providerSessionId, deps);
  const init = await sandbox.exec(`mkdir -p ${WORKSPACE_ROOT} ${BRIDGE_TMP_ROOT}`);
  if (!init?.success) {
    return errorResponse(
      500,
      "sandbox_init_failed",
      init?.stderr || "Cloudflare sandbox failed to initialize workspace.",
    );
  }
  return jsonResponse({ provider_session_id: providerSessionId });
}

export async function handleHydrateSession(sandbox, payload) {
  if ((payload?.workspace_materialization || "copy") !== "copy") {
    return errorResponse(
      400,
      "unsupported_workspace_materialization",
      "Cloudflare bridge currently supports only workspace_materialization='copy'.",
    );
  }
  const archiveBase64 = typeof payload?.archive_base64 === "string" ? payload.archive_base64 : "";
  if (!archiveBase64) {
    return errorResponse(400, "missing_archive", "hydrate requires archive_base64.");
  }

  await sandbox.writeFile(ARCHIVE_BASE64_PATH, archiveBase64);
  const hydrate = await sandbox.exec(HYDRATE_ARCHIVE_COMMAND, {
    cwd: "/",
    timeout: 120_000,
  });
  if (!hydrate?.success) {
    return errorResponse(
      400,
      "hydrate_failed",
      hydrate?.stderr || "Cloudflare sandbox failed to unpack the workspace archive.",
    );
  }

  const manifest = await sandbox.exec(MANIFEST_CAPTURE_COMMAND, {
    cwd: "/",
    timeout: 60_000,
  });
  if (!manifest?.success) {
    return errorResponse(
      500,
      "baseline_manifest_failed",
      manifest?.stderr || "Cloudflare sandbox failed to capture the baseline manifest.",
    );
  }

  const parsed = parseJsonStdout(manifest.stdout, "baseline manifest capture");
  if ("errorResponse" in parsed) {
    return parsed.errorResponse;
  }
  return jsonResponse({
    hydrated_files: integerOrZero(parsed.payload.hydrated_files),
  });
}

export async function handleExecSession(sandbox, payload, deps = {}) {
  const command = typeof payload?.command === "string" ? payload.command : "";
  if (!command.trim()) {
    return errorResponse(400, "missing_command", "exec requires a non-empty command.");
  }

  const startedAt = deps.nowIso?.() || new Date().toISOString();
  const startedMs = deps.nowMs?.() || Date.now();
  try {
    const result = await sandbox.exec(command, {
      cwd: WORKSPACE_ROOT,
      env: sanitizeEnv(payload?.env),
      stdin: typeof payload?.stdin_text === "string" ? payload.stdin_text : "",
      timeout: normalizeTimeoutMs(payload?.timeout_seconds),
    });
    const finishedAt = deps.nowIso?.() || new Date().toISOString();
    const finishedMs = deps.nowMs?.() || Date.now();
    return jsonResponse({
      exit_code: integerOrZero(result?.exitCode),
      stdout: typeof result?.stdout === "string" ? result.stdout : "",
      stderr: typeof result?.stderr === "string" ? result.stderr : "",
      timed_out: false,
      artifact_refs: [],
      started_at: startedAt,
      finished_at: finishedAt,
      provider_latency_ms: Math.max(0, finishedMs - startedMs),
    });
  } catch (error) {
    const finishedAt = deps.nowIso?.() || new Date().toISOString();
    const finishedMs = deps.nowMs?.() || Date.now();
    const message = errorMessage(error);
    const timedOut = /timed out|timeout/i.test(message);
    return jsonResponse({
      exit_code: timedOut ? 124 : 127,
      stdout: "",
      stderr: message,
      timed_out: timedOut,
      artifact_refs: [],
      started_at: startedAt,
      finished_at: finishedAt,
      provider_latency_ms: Math.max(0, finishedMs - startedMs),
    });
  }
}

export async function handleArtifactsSession(sandbox) {
  const capture = await sandbox.exec(ARTIFACT_CAPTURE_COMMAND, {
    cwd: "/",
    timeout: 60_000,
  });
  if (!capture?.success) {
    return errorResponse(
      500,
      "artifact_capture_failed",
      capture?.stderr || "Cloudflare sandbox failed to capture artifact contents.",
    );
  }
  const parsed = parseJsonStdout(capture.stdout, "artifact capture");
  if ("errorResponse" in parsed) {
    return parsed.errorResponse;
  }
  const artifactRefs = Array.isArray(parsed.payload.artifact_refs)
    ? parsed.payload.artifact_refs.map((value) => String(value))
    : [];
  const artifacts = Array.isArray(parsed.payload.artifacts)
    ? parsed.payload.artifacts
        .filter((value) => value && typeof value === "object")
        .map((value) => ({
          path: String(value.path || ""),
          content_base64: String(value.content_base64 || ""),
        }))
        .filter((value) => value.path)
    : [];
  return jsonResponse({
    artifact_refs: artifactRefs,
    artifacts,
  });
}

export async function handleDestroySession(sandbox) {
  await sandbox.destroy();
  return jsonResponse({ ok: true });
}

export function validateSessionPolicy(payload) {
  const networkPolicy = String(payload?.network_policy || "provider_only");
  if (networkPolicy !== "provider_only") {
    return errorResponse(
      400,
      "unsupported_network_policy",
      "Cloudflare bridge currently supports only network_policy='provider_only'.",
    );
  }
  const workspaceMaterialization = String(payload?.workspace_materialization || "copy");
  if (workspaceMaterialization !== "copy") {
    return errorResponse(
      400,
      "unsupported_workspace_materialization",
      "Cloudflare bridge currently supports only workspace_materialization='copy'.",
    );
  }
  return null;
}

export function resolveSandbox(env, sessionId, deps = {}) {
  if (typeof deps.getSandboxForSession === "function") {
    return deps.getSandboxForSession(env, sessionId);
  }
  throw new Error("Bridge runtime requires getSandboxForSession dependency.");
}

export function sanitizeEnv(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return {};
  }
  const sanitized = {};
  for (const [key, raw] of Object.entries(value)) {
    if (!key || raw === undefined || raw === null) {
      continue;
    }
    sanitized[String(key)] = String(raw);
  }
  return sanitized;
}

export function normalizeTimeoutMs(timeoutSeconds) {
  const parsed = Number(timeoutSeconds);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    return undefined;
  }
  return Math.floor(parsed * 1000);
}

export async function readJsonBody(request) {
  try {
    return { payload: await request.json() };
  } catch {
    return {
      errorResponse: errorResponse(400, "invalid_json", "Request body must be valid JSON."),
    };
  }
}

export function parseJsonStdout(stdout, label) {
  try {
    return { payload: JSON.parse(stdout || "{}") };
  } catch {
    return {
      errorResponse: errorResponse(
        500,
        "invalid_sandbox_payload",
        `Cloudflare sandbox returned invalid JSON for ${label}.`,
      ),
    };
  }
}

export function integerOrZero(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    return 0;
  }
  return Math.trunc(parsed);
}

export function errorMessage(error) {
  if (error instanceof Error) {
    return error.message;
  }
  if (typeof error === "string") {
    return error;
  }
  return "Unknown sandbox execution failure.";
}

export function jsonResponse(payload, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: JSON_HEADERS,
  });
}

export function errorResponse(status, code, message) {
  return jsonResponse(
    {
      ok: false,
      error: {
        code,
        message,
      },
    },
    status,
  );
}
