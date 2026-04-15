import { defineConfig, type ViteDevServer } from 'vite';
import react from '@vitejs/plugin-react';
import { spawn, ChildProcess } from 'child_process';
import { readFileSync, watch, FSWatcher } from 'fs';
import net from 'net';

async function canBindPort(port: number, host = '127.0.0.1'): Promise<boolean> {
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

async function findOpenPort(preferredPort: number, host = '127.0.0.1', maxAttempts = 50): Promise<number> {
  const startPort = Number.isFinite(preferredPort) && preferredPort > 0 ? preferredPort : 1;
  for (let offset = 0; offset < maxAttempts; offset += 1) {
    const candidate = startPort + offset;
    if (await canBindPort(candidate, host)) return candidate;
  }
  throw new Error(`Could not find an open port after ${maxAttempts} attempts starting at ${startPort}`);
}

// Load .env from repo root for API keys
function loadDotEnv(repoRoot: string): Record<string, string> {
  const envVars: Record<string, string> = {};
  try {
    const content = readFileSync(`${repoRoot}.env`, 'utf-8');
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

  const repoRoot = decodeURIComponent(new URL('../../../../', import.meta.url).pathname);
  const workflowRoot = `${repoRoot}Code&DBs/Workflow`;
  const repoEnvPath = `${repoRoot}.env`;
  const apiArgs = ['-m', 'surfaces.api.server', '--port', String(apiPort)];

  const normalizedWorkflowRoot = workflowRoot.replace(/\\/g, '/');
  const normalizedRepoEnvPath = repoEnvPath.replace(/\\/g, '/');

  function loadApiEnv() {
    const dotEnv = loadDotEnv(repoRoot);
    const workflowDatabaseUrl = normalizeWorkflowDatabaseUrl(
      dotEnv.WORKFLOW_DATABASE_URL
      || process.env.WORKFLOW_DATABASE_URL
      || 'postgresql://postgres@localhost:5432/praxis',
    );
    return {
      ...process.env,
      ...dotEnv,
      PYTHONPATH: workflowRoot,
      WORKFLOW_DATABASE_URL: workflowDatabaseUrl,
      PRAXIS_API_PORT: String(apiPort),
      PATH: process.env.PATH || '/usr/local/bin:/usr/bin:/bin',
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

  function startApiServer(reason: string) {
    if (shuttingDown) return;
    const child = spawn('python3', apiArgs, {
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
      console.error(`[api] exited unexpectedly (${exitReason}) — restarting in 2s`);
      setTimeout(() => {
        if (!proc) startApiServer('auto-restarted after crash');
      }, 2000);
    });

    console.log(`[api] Python API server ${reason} on :${apiPort}`);
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
      startBackendWatchers();
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

  const apiOrigin = `http://127.0.0.1:${apiPort}`;

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
      host: '127.0.0.1',
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
