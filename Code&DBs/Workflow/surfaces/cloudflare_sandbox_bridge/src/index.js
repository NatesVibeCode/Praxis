import { getSandbox } from "@cloudflare/sandbox";

import { handleBridgeRequest } from "./bridge.js";

export { Sandbox } from "@cloudflare/sandbox";

export default {
  async fetch(request, env) {
    return handleBridgeRequest(request, env, {
      getSandboxForSession(targetEnv, sessionId) {
        return getSandbox(targetEnv.Sandbox, sessionId);
      },
    });
  },
};
