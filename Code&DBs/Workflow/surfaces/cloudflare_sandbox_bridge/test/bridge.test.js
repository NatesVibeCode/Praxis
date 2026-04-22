import test from "node:test";
import assert from "node:assert/strict";

import {
  BRIDGE_TOKEN_ENV,
  archiveBase64Path,
  resolveWorkspaceRoot,
  authorizeRequest,
  handleBridgeRequest,
  handleExecSession,
  sanitizeEnv,
} from "../src/bridge.js";

const BRIDGE_ENV = {
  PRAXIS_CONTAINER_WORKSPACE_ROOT: "bridge-workspace",
  PRAXIS_BRIDGE_TMP_ROOT: "bridge-tmp",
};

function request(path, payload, options = {}) {
  return new Request(`https://bridge.example${path}`, {
    method: options.method || "POST",
    headers: {
      "content-type": "application/json",
      ...(options.headers || {}),
    },
    body: payload === undefined ? undefined : JSON.stringify(payload),
  });
}

function fakeSandbox(overrides = {}) {
  return {
    execCalls: [],
    writeCalls: [],
    destroyed: false,
    async exec(command, options = {}) {
      this.execCalls.push({ command, options });
      return { success: true, exitCode: 0, stdout: "", stderr: "", ...overrides.execResult };
    },
    async writeFile(path, content) {
      this.writeCalls.push({ path, content });
    },
    async destroy() {
      this.destroyed = true;
    },
    ...overrides,
  };
}

test("health endpoint stays unauthenticated and self-describing", async () => {
  const response = await handleBridgeRequest(
    new Request("https://bridge.example/healthz"),
    { [BRIDGE_TOKEN_ENV]: "secret" },
    BRIDGE_ENV,
  );
  assert.equal(response.status, 200);
  const payload = await response.json();
  assert.equal(payload.ok, true);
  assert.equal(payload.auth_required, true);
  assert.deepEqual(payload.routes, [
    "/sessions/create",
    "/sessions/{id}/hydrate",
    "/sessions/{id}/exec",
    "/sessions/{id}/artifacts",
    "/sessions/{id}/destroy",
  ]);
});

test("auth gate rejects missing bearer token when configured", async () => {
  const failure = authorizeRequest(
    new Request("https://bridge.example/sessions/create", { method: "POST" }),
    { [BRIDGE_TOKEN_ENV]: "top-secret" },
  );
  assert.ok(failure);
  assert.equal(failure.status, 401);
});

test("create session allocates a sandbox id and initializes workspace roots", async () => {
  const sandbox = fakeSandbox();
  const response = await handleBridgeRequest(
    request("/sessions/create", {
      sandbox_session_id: "sandbox_session:run.alpha:job.alpha",
      network_policy: "provider_only",
      workspace_materialization: "copy",
    }),
    BRIDGE_ENV,
    {
      generateSessionId: () => "cf-session-123",
      getSandboxForSession(_env, sessionId) {
        assert.equal(sessionId, "cf-session-123");
        return sandbox;
      },
    },
  );
  assert.equal(response.status, 200);
  const payload = await response.json();
  assert.equal(payload.provider_session_id, "cf-session-123");
  assert.equal(sandbox.execCalls.length, 1);
  assert.match(sandbox.execCalls[0].command, new RegExp(resolveWorkspaceRoot(BRIDGE_ENV)));
});

test("create session fails closed for unsupported network policy", async () => {
  const response = await handleBridgeRequest(
    request("/sessions/create", {
      sandbox_session_id: "sandbox_session:run.alpha:job.alpha",
      network_policy: "disabled",
      workspace_materialization: "copy",
    }),
    BRIDGE_ENV,
    {
      getSandboxForSession() {
        throw new Error("should not resolve sandbox");
      },
    },
  );
  assert.equal(response.status, 400);
  const payload = await response.json();
  assert.equal(payload.error.code, "unsupported_network_policy");
});

test("hydrate session writes archive and returns the baseline file count", async () => {
  const sandbox = fakeSandbox({
    async exec(command, options = {}) {
      this.execCalls.push({ command, options });
      if (command.includes("hydrated_files")) {
        return {
          success: true,
          exitCode: 0,
          stdout: JSON.stringify({ hydrated_files: 2 }),
          stderr: "",
        };
      }
      return { success: true, exitCode: 0, stdout: "", stderr: "" };
    },
  });
  const response = await handleBridgeRequest(
    request("/sessions/cf-session/hydrate", {
      archive_base64: "ZXhhbXBsZQ==",
      workspace_materialization: "copy",
    }),
    BRIDGE_ENV,
    {
      getSandboxForSession() {
        return sandbox;
      },
    },
  );
  assert.equal(response.status, 200);
  const payload = await response.json();
  assert.equal(payload.hydrated_files, 2);
  assert.deepEqual(sandbox.writeCalls, [
    { path: archiveBase64Path(BRIDGE_ENV), content: "ZXhhbXBsZQ==" },
  ]);
  assert.equal(sandbox.execCalls.length, 2);
});

test("exec session forwards cwd env stdin and timeout", async () => {
  const sandbox = fakeSandbox({
    execResult: {
      success: false,
      exitCode: 7,
      stdout: "partial",
      stderr: "boom",
    },
  });
  const response = await handleBridgeRequest(
    request("/sessions/cf-session/exec", {
      command: "python worker.py",
      stdin_text: "payload",
      env: { OPENAI_API_KEY: "sk-test", COUNT: 3, EMPTY: null },
      timeout_seconds: 12,
      execution_transport: "cli",
    }),
    BRIDGE_ENV,
    {
      getSandboxForSession() {
        return sandbox;
      },
      nowIso: (() => {
        const values = [
          "2026-04-13T18:00:00.000Z",
          "2026-04-13T18:00:00.250Z",
        ];
        return () => values.shift() || values[values.length - 1];
      })(),
      nowMs: (() => {
        const values = [1_000, 1_250];
        return () => values.shift() || values[values.length - 1];
      })(),
    },
  );
  assert.equal(response.status, 200);
  const payload = await response.json();
  assert.equal(payload.exit_code, 7);
  assert.equal(payload.stderr, "boom");
  assert.equal(payload.timed_out, false);
  assert.equal(payload.provider_latency_ms, 250);
  assert.deepEqual(sandbox.execCalls[0].options, {
    cwd: BRIDGE_ENV.PRAXIS_CONTAINER_WORKSPACE_ROOT,
    env: { OPENAI_API_KEY: "sk-test", COUNT: "3" },
    stdin: "payload",
    timeout: 12_000,
  });
});

test("exec session translates timeout exceptions into sandbox result payloads", async () => {
  const response = await handleExecSession(
    {
      async exec() {
        throw new Error("Command timed out after 5000ms");
      },
    },
    {
      command: "sleep 10",
      timeout_seconds: 5,
    },
    BRIDGE_ENV,
    {
      nowIso: (() => {
        const values = [
          "2026-04-13T18:00:00.000Z",
          "2026-04-13T18:00:05.000Z",
        ];
        return () => values.shift() || values[values.length - 1];
      })(),
      nowMs: (() => {
        const values = [1_000, 6_000];
        return () => values.shift() || values[values.length - 1];
      })(),
    },
  );
  assert.equal(response.status, 200);
  const payload = await response.json();
  assert.equal(payload.timed_out, true);
  assert.equal(payload.exit_code, 124);
});

test("artifacts endpoint returns changed files and base64 content", async () => {
  const sandbox = fakeSandbox({
    execResult: {
      success: true,
      exitCode: 0,
      stdout: JSON.stringify({
        artifact_refs: ["changed.txt"],
        artifacts: [
          { path: "changed.txt", content_base64: "dXBkYXRlZA==" },
        ],
      }),
      stderr: "",
    },
  });
  const response = await handleBridgeRequest(
    request("/sessions/cf-session/artifacts", {
      include_content: true,
    }),
    BRIDGE_ENV,
    {
      getSandboxForSession() {
        return sandbox;
      },
    },
  );
  assert.equal(response.status, 200);
  const payload = await response.json();
  assert.deepEqual(payload.artifact_refs, ["changed.txt"]);
  assert.deepEqual(payload.artifacts, [
    { path: "changed.txt", content_base64: "dXBkYXRlZA==" },
  ]);
});

test("destroy endpoint tears down the sandbox", async () => {
  const sandbox = fakeSandbox();
  const response = await handleBridgeRequest(
    request("/sessions/cf-session/destroy", {
      disposition: "completed",
    }),
    {},
    {
      getSandboxForSession() {
        return sandbox;
      },
    },
  );
  assert.equal(response.status, 200);
  assert.equal(sandbox.destroyed, true);
});

test("sanitizeEnv keeps only defined scalar values", () => {
  assert.deepEqual(
    sanitizeEnv({
      OPENAI_API_KEY: "sk-test",
      COUNT: 2,
      ACTIVE: false,
      NULLISH: null,
      UNSET: undefined,
    }),
    {
      OPENAI_API_KEY: "sk-test",
      COUNT: "2",
      ACTIVE: "false",
    },
  );
});
