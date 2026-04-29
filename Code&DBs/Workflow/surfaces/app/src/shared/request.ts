export interface JsonRequestOptions {
  timeoutMs?: number;
  signal?: AbortSignal | null;
}

export class HttpRequestError extends Error {
  readonly status: number;
  readonly body: unknown;

  constructor(message: string, status: number, body: unknown) {
    super(message);
    this.name = 'HttpRequestError';
    this.status = status;
    this.body = body;
  }
}

function isAbortSignal(value: AbortSignal | null | undefined): value is AbortSignal {
  return Boolean(value);
}

export function isAbortError(error: unknown): boolean {
  return error instanceof Error && error.name === 'AbortError';
}

export function isHttpRequestError(error: unknown, status?: number): error is HttpRequestError {
  return error instanceof HttpRequestError && (status === undefined || error.status === status);
}

async function readJsonBody(response: Response): Promise<unknown> {
  try {
    return await response.json();
  } catch {
    return null;
  }
}

function messageFromBody(body: unknown, status: number): string {
  if (body && typeof body === 'object') {
    const error = (body as { error?: unknown }).error;
    if (typeof error === 'string' && error) return error;
    const detail = (body as { detail?: unknown }).detail;
    if (typeof detail === 'string' && detail) return detail;
  }
  return `HTTP ${status}`;
}

export async function fetchJson<T>(
  input: string,
  init: RequestInit = {},
  options: JsonRequestOptions = {},
): Promise<T> {
  const timeoutMs = options.timeoutMs ?? 15000;
  const controller = new AbortController();
  const signals = [init.signal, options.signal].filter(isAbortSignal);
  let timedOut = false;

  const abort = () => {
    controller.abort();
  };

  for (const signal of signals) {
    if (signal.aborted) {
      controller.abort();
      break;
    }
    signal.addEventListener('abort', abort, { once: true });
  }

  const timeout = globalThis.setTimeout(() => {
    timedOut = true;
    controller.abort();
  }, timeoutMs);

  try {
    const response = await fetch(input, {
      ...init,
      signal: controller.signal,
    });
    const body = await readJsonBody(response);
    if (!response.ok) {
      throw new HttpRequestError(messageFromBody(body, response.status), response.status, body);
    }
    return body as T;
  } catch (error) {
    if (timedOut && isAbortError(error)) {
      throw new Error(`Request timed out after ${Math.ceil(timeoutMs / 1000)}s`);
    }
    throw error;
  } finally {
    globalThis.clearTimeout(timeout);
    for (const signal of signals) {
      signal.removeEventListener('abort', abort);
    }
  }
}
