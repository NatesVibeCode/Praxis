const PRAXIS_UI_HEADER = 'X-Praxis-UI';
const FETCH_PATCH_FLAG = '__praxisFetchPatched__';

function isApiRequest(input: RequestInfo | URL): boolean {
  const urlText = typeof input === 'string'
    ? input
    : input instanceof URL
      ? input.toString()
      : input.url;

  try {
    const url = new URL(urlText, window.location.href);
    return url.origin === window.location.origin && url.pathname.startsWith('/api/');
  } catch {
    return false;
  }
}

export function installPraxisApiFetch(): void {
  const globalWindow = window as Window & { __praxisFetchPatched__?: true };
  if (globalWindow[FETCH_PATCH_FLAG]) {
    return;
  }
  globalWindow[FETCH_PATCH_FLAG] = true;

  const originalFetch = window.fetch.bind(window);

  window.fetch = ((input: RequestInfo | URL, init?: RequestInit) => {
    if (!isApiRequest(input)) {
      return originalFetch(input, init);
    }

    const headers = new Headers(init?.headers ?? (input instanceof Request ? input.headers : undefined));
    headers.set(PRAXIS_UI_HEADER, '1');

    return originalFetch(input, {
      ...init,
      headers,
    });
  }) as typeof window.fetch;
}
