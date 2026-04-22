import { defineConfig, type ViteDevServer } from 'vite';
import react from '@vitejs/plugin-react';
import { spawn, ChildProcess } from 'child_process';
import { readFileSync, watch, FSWatcher } from 'fs';
import net from 'net';
import { dirname, join, resolve } from 'path';
import { fileURLToPath } from 'url';

// Single source of truth for the dev bind address. Every port interaction
// — the free-port probe, the Python child spawn, and the Vite proxy origin —
// goes through this constant. If these ever disagree, the probe can green-
// light a port the server then can't actually bind (e.g. when OrbStack or
// another process already holds *:<port>), and the dev proxy silently
// targets a dead address.
const API_HOST = '127.0.0.1';
const UI_HOST = process.env.PRAXIS_UI_HOST || '127.0.0.1';
const PYTHON_COMMAND = process.env.PRAXIS_PYTHON_COMMAND || (process.platform === 'win32' ? 'python' : 'python3');
const API_WATCH_ENABLED = process.env.PRAXIS_API_WATCH !== '0';

async function canBindPort(port: number, host = API_HOST): Promise<boolean> {
  return await new Promise<boolean>((resolve, reject) => {
    const server = net.createServer();
    server.once('error', (error: NodeJS.ErrnoException) => {
      server.close();
      if (error.code === 'EADDRINUSE' || error.code === 'EACCES') {
        resolve(false);
        return;
      }
      reject(error);
    });
    server.once('listening', () => {
      server.close((closeError) => {
        if (closeError) {
          reject(closeError);
          return;
        }
        resolve(true);
      });
    });
    server.listen(port, host);
  });
}

async function findOpenPort(preferredPort: number, host = API_HOST, maxAttempts = 50): Promise<number> {
  const startPort = Number.isFinite(preferredPort) && preferredPort > 0 ? preferredPort : 1;
  for (let offset = 0; offset < maxAttempts; offset += 1) {
    const candidate = startPort + offset;
    if (await canBindPort(candidate, host)) return candidate;
  }
  throw new Error(`Could not find an open port after ${maxAttempts} attempts starting at ${startPort}`);
}

// Load .env from repo root for API keys
function loadDotEnv(envPath: string): Record<string, string> {
  const envVars: Record<string, string> = {};
  try {
    const content = readFileSync(envPath, 'utf-8');
    for (const line of content.split('\n')) {
      const trimmed = line.trim();
      if (!trimmed || trimmed.startsWith('#')) continue;
      const eqIdx = trimmed.indexOf('=');
      if (eqIdx > 0) {
        envVars[trimmed.slice(0, eqIdx)] = trimmed.slice(eqIdx + 1);
      }
    }
  } catch {}
  return envVars;
}

function normalizeWorkflowDatabaseUrl(value: string): string {
  try {
    const url = new URL(value);
    if (url.protocol !== 'postgresql:' && url.protocol !== 'postgres:') {
      return value;
    }
    if (url.username) {
      return value;
    }
    url.username = 'postgres';
    return url.toString();
  } catch {
    return value;
  }
}

// Auto-start the Python API server as part of Vite lifecycle
function apiServerPlugin(apiPort: number) {
  let proc: ChildProcess | null = null;
  let expectedExit: ChildProcess | null = null;
  let restartTimer: ReturnType<typeof setTimeout> | null = null;
  let shuttingDown = false;
  const backendWatchers: FSWatcher[] = [];

  // Crash-loop circuit breaker: if the child exits unexpectedly more than
  // CRASH_WINDOW_MAX times within CRASH_WINDOW_MS we stop restarting and
  // print a clear diagnosis instead of flooding the log forever.
  const CRASH_WINDOW_MS = 10_000;
  const CRASH_WINDOW_MAX = 3;
  const recentCrashes: number[] = [];
  let circuitOpen = false;

  const appRoot = dirname(fileURLToPath(import.meta.url));
  const repoRoot = resolve(appRoot, '../../../..');
  const workflowRoot = join(repoRoot, 'Code&DBs', 'Workflow');
  const repoEnvPath = join(repoRoot, '.env');
  // Host + args stay in lock-step with the probe in `canBindPort` (both use
  // `API_HOST`). The Vite proxy origin below uses the same constant — if any
  // of the three diverges, the child can bind a different address than the
  // probe measured and the proxy silently points at a dead port.
  const apiArgs = [
    '-m',
    'surfaces.api.server',
    '--host',
    API_HOST,
    '--port',
    String(apiPort),
  ];

  const normalizedWorkflowRoot = workflowRoot.replace(/\\/g, '/');
  const normalizedRepoEnvPath = repoEnvPath.replace(/\\/g, '/');

  function loadApiEnv() {
    const dotEnv = loadDotEnv(repoEnvPath);
    const configuredWorkflowDatabaseUrl = dotEnv.WORKFLOW_DATABASE_URL || process.env.WORKFLOW_DATABASE_URL;
    if (!configuredWorkflowDatabaseUrl) {
      throw new Error(`WORKFLOW_DATABASE_URL must be set in process env or declared in ${normalizedRepoEnvPath}`);
    }
    const workflowDatabaseUrl = normalizeWorkflowDatabaseUrl(
      configuredWorkflowDatabaseUrl,
    );
    return {
      ...process.env,
      ...dotEnv,
      PYTHONPATH: workflowRoot,
      WORKFLOW_DATABASE_URL: workflowDatabaseUrl,
      PRAXIS_API_PORT: String(apiPort),
      PATH: process.env.PATH || '',
    };
  }

  function attachLogging(child: ChildProcess) {
    child.stdout?.on('data', (d: Buffer) => {
      const msg = d.toString().trim();
      if (msg) console.log(`[api] ${msg}`);
    });
    child.stderr?.on('data', (d: Buffer) => {
      const msg = d.toString().trim();
      if (msg) console.error(`[api] ${msg}`);
    });
  }

  function recordCrashAndShouldHalt(): boolean {
    const now = Date.now();
    // Drop samples outside the current window, then push the new one.
    while (recentCrashes.length > 0 && now - recentCrashes[0] > CRASH_WINDOW_MS) {
      recentCrashes.shift();
    }
    recentCrashes.push(now);
    return recentCrashes.length > CRASH_WINDOW_MAX;
  }

  function openCircuit(lastExitReason: string) {
    circuitOpen = true;
    console.error(
      [
        `[api] Python API child crashed ${recentCrashes.length} times in ${CRASH_WINDOW_MS / 1000}s`,
        `[api] Last exit: ${lastExitReason}. Giving up on auto-restart.`,
        `[api] Diagnose:`,
        `[api]   1. Check for another listener on the same port:`,
        `[api]        lsof -iTCP:${apiPort} -sTCP:LISTEN -P -n`,
        `[api]      A wildcard bind (e.g. OrbStack, docker-proxy) at *:${apiPort} does NOT`,
        `[api]      block our loopback bind — so this check is for ${API_HOST}:${apiPort} specifically.`,
        `[api]   2. Run the child directly to see its stderr:`,
        `[api]        WORKFLOW_DATABASE_URL=... PYTHONPATH=${workflowRoot} ${PYTHON_COMMAND} -m surfaces.api.server --host ${API_HOST} --port ${apiPort}`,
        `[api]   3. Fix, then restart Vite to reset the circuit breaker.`,
      ].join('\n'),
    );
  }

  function startApiServer(reason: string) {
    if (shuttingDown || circuitOpen) return;
    const child = spawn(PYTHON_COMMAND, apiArgs, {
      env: loadApiEnv(),
      cwd: workflowRoot,
      stdio: ['ignore', 'pipe', 'pipe'],
    });

    proc = child;
    attachLogging(child);
    child.on('exit', (code: number | null, signal: NodeJS.Signals | null) => {
      const intentional = expectedExit === child;
      if (intentional) {
        expectedExit = null;
        return;
      }
      if (proc === child) proc = null;
      if (shuttingDown) return;
      const exitReason = code !== null ? `code ${code}` : `signal ${signal ?? 'unknown'}`;
      if (recordCrashAndShouldHalt()) {
        openCircuit(exitReason);
        return;
      }
      console.error(`[api] exited unexpectedly (${exitReason}) — restarting in 2s`);
      setTimeout(() => {
        if (!proc) startApiServer('auto-restarted after crash');
      }, 2000);
    });

    console.log(`[api] Python API server ${reason} on ${API_HOST}:${apiPort}`);
  }

  function stopApiServer() {
    if (!proc) return;
    expectedExit = proc;
    proc.kill();
    proc = null;
  }

  function shutdownPlugin() {
    shuttingDown = true;
    if (restartTimer) clearTimeout(restartTimer);
    stopBackendWatchers();
    stopApiServer();
  }

  function scheduleRestart(reason: string) {
    if (restartTimer) clearTimeout(restartTimer);
    restartTimer = setTimeout(() => {
      restartTimer = null;
      // Operator-triggered restart (file change) — reset the circuit breaker
      // so a genuine fix following a crash-loop can take effect without
      // needing a full Vite restart.
      if (circuitOpen) {
        console.log('[api] File change detected — resetting crash-loop circuit breaker.');
        circuitOpen = false;
        recentCrashes.length = 0;
      }
      console.log(`[api] Backend change detected (${reason}) — restarting Python API server...`);
      stopApiServer();
      setTimeout(() => {
        if (!proc) startApiServer('restarted');
      }, 150);
    }, 120);
  }

  function isBackendWatchedFile(file: string): boolean {
    const normalized = file.replace(/\\/g, '/');
    if (normalized === normalizedRepoEnvPath) return true;
    if (!normalized.startsWith(`${normalizedWorkflowRoot}/`)) return false;
    if (
      normalized.includes('/node_modules/')
      || normalized.includes('/dist/')
      || normalized.includes('/.venv/')
      || normalized.includes('/__pycache__/')
      || normalized.includes('/.pytest_cache/')
      || normalized.includes('/.mypy_cache/')
    ) {
      return false;
    }
    return normalized.endsWith('.py') || normalized.endsWith('.json') || normalized.endsWith('.sql');
  }

  function startBackendWatchers() {
    try {
      backendWatchers.push(
        watch(workflowRoot, { recursive: true }, (_eventType, fileName) => {
          if (!fileName) return;
          const relativePath = String(fileName).replace(/\\/g, '/');
          const absolutePath = `${normalizedWorkflowRoot}/${relativePath}`;
          if (isBackendWatchedFile(absolutePath)) scheduleRestart(relativePath);
        }),
      );
    } catch (error) {
      console.error('[api] Failed to watch workflow backend tree', error);
    }

    try {
      backendWatchers.push(
        watch(repoRoot, (_eventType, fileName) => {
          if (String(fileName ?? '') !== '.env') return;
          scheduleRestart('.env');
        }),
      );
    } catch (error) {
      console.error('[api] Failed to watch repo root for .env changes', error);
    }
  }

  function stopBackendWatchers() {
    while (backendWatchers.length > 0) {
      const watcher = backendWatchers.pop();
      watcher?.close();
    }
  }

  return {
    name: 'api-server',
    configureServer(server: ViteDevServer) {
      const handleProcessShutdown = () => {
        shutdownPlugin();
      };

      startApiServer('starting');
      if (API_WATCH_ENABLED) {
        startBackendWatchers();
      }
      process.once('SIGINT', handleProcessShutdown);
      process.once('SIGTERM', handleProcessShutdown);

      server.httpServer?.once('close', () => {
        shutdownPlugin();
      });
    },
    closeBundle() {
      shutdownPlugin();
    },
  };
}

export default defineConfig(async ({ command }) => {
  const isServe = command === 'serve';
  const preferredUiPort = Number.parseInt(process.env.PRAXIS_UI_PORT ?? '5173', 10);
  const preferredApiPort = Number.parseInt(process.env.PRAXIS_API_PORT ?? '8420', 10);

  const [uiPort, apiPort] = isServe
    ? await Promise.all([
      findOpenPort(preferredUiPort),
      findOpenPort(preferredApiPort),
    ])
    : [preferredUiPort, preferredApiPort];

  if (isServe) {
    console.log(`[dev] UI port ${uiPort} | API port ${apiPort}`);
  }

  const apiOrigin = `http://${API_HOST}:${apiPort}`;

  return {
    base: isServe ? '/' : '/app/',
    plugins: isServe ? [react(), apiServerPlugin(apiPort)] : [react()],
    build: {
      rollupOptions: {
        output: {
          manualChunks(id: string) {
            if (id.includes('/node_modules/react/') || id.includes('/node_modules/react-dom/')) {
              return 'vendor-react';
            }
            if (id.includes('/node_modules/cytoscape/')) {
              return 'vendor-cytoscape';
            }
            if (
              id.includes('/node_modules/cytoscape-fcose/')
              || id.includes('/node_modules/cose-base/')
              || id.includes('/node_modules/layout-base/')
            ) {
              return 'vendor-cytoscape-layout';
            }
            if (id.includes('/src/dashboard/ReferencePopover')) {
              return 'dashboard-reference';
            }
            if (id.includes('/src/builder/')) {
              return 'builder';
            }
            if (id.includes('/src/modules/display/')) {
              return 'modules-display';
            }
            if (id.includes('/src/modules/input/')) {
              return 'modules-input';
            }
            if (id.includes('/src/modules/tool/')) {
              return 'modules-tool';
            }
            if (id.includes('/src/modules/composite/')) {
              return 'modules-composite';
            }
            if (
              id.includes('/src/grid/')
              || id.includes('/src/canvas/')
              || id.includes('/src/hooks/')
              || id.includes('/src/world')
              || id.includes('/src/modules/moduleRegistry')
              || id.includes('/src/modules/types')
            ) {
              return 'editor-grid-core';
            }
            if (id.includes('/src/dashboard/')) {
              return 'dashboard';
            }
            if (id.includes('/src/workspace/')) {
              return 'workspace';
            }
            return undefined;
          },
        },
      },
    },
    server: {
      host: UI_HOST,
      port: uiPort,
      strictPort: true,
      proxy: {
        '/api': {
          target: apiOrigin,
          changeOrigin: true,
        },
        '/orient': {
          target: apiOrigin,
          changeOrigin: true,
        },
        '/mcp': {
          target: apiOrigin,
          changeOrigin: true,
        },
      },
    },
  };
});
