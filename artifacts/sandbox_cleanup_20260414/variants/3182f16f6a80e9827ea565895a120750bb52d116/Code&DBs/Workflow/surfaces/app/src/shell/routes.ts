const legacyShellPath = '/app/helm';
const praxisShellPath = '/app';
const launcherPath = '/app/status';

export function isLauncherRoute(pathname: string = window.location.pathname): boolean {
  return pathname === launcherPath || pathname === `${launcherPath}/`;
}

export function shellBasePath(pathname: string = window.location.pathname): string {
  if (pathname.startsWith('/app')) return praxisShellPath;
  return '/';
}

export function launcherBasePath(pathname: string = window.location.pathname): string {
  return launcherPath;
}

export function shellUrl(search: string = '', pathname: string = window.location.pathname): string {
  const base = shellBasePath(pathname);
  if (!search) return base;
  return `${base}${search.startsWith('?') ? search : `?${search}`}`;
}

export function launcherUrl(pathname: string = window.location.pathname): string {
  return launcherBasePath(pathname);
}

export function pushNavigation(url: string, state: unknown = {}): void {
  window.history.pushState(state, '', url);
  window.dispatchEvent(new PopStateEvent('popstate'));
}

export function navigateToShell(search: string = ''): void {
  pushNavigation(shellUrl(search));
}

export function navigateToLauncher(): void {
  pushNavigation(launcherUrl());
}
