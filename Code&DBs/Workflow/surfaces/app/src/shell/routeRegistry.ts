/**
 * Client-side projection of ui_shell_route_registry.
 *
 * Fetches /api/shell/routes once on app boot, caches in module scope, exposes:
 *   - getRoutes()                       — full list (filtered by enabled)
 *   - matchPath(pathname, search)       — URL → {route_id, slot_values} | null
 *   - buildPath(routeId, slotValues)    — state → URL (canonical-aware)
 *   - resolveComponent(routeId)         — registry-driven React.lazy import
 *   - interpolateLabel(template, state) — minimal expression evaluator for
 *                                         tab_label_template and friends
 *
 * Replaces parseShellLocationState + buildShellUrl in shell/state.ts and the
 * STATIC_SURFACES / DYNAMIC_SURFACES dictionaries in surfaceRegistry.tsx.
 *
 * Anchored to decision.shell_navigation_cqrs.20260426.
 */
import { lazy, type ComponentType, type LazyExoticComponent } from 'react';

export interface RouteRegistryRow {
  route_id: string;
  path_template: string;
  surface_name: string;
  state_effect: string;
  notes: string;
  source_refs: unknown[];
  status: 'ready' | 'legacy' | 'deprecated';
  display_order: number;
  binding_revision: string;
  decision_ref: string;
  component_ref: string | null;
  tab_kind_label: string | null;
  tab_label_template: string | null;
  context_label: string | null;
  context_detail_template: string | null;
  nav_description_template: string | null;
  nav_keywords: string[];
  event_bus_kind: string | null;
  keyboard_shortcut: string | null;
  draft_guard_required: boolean;
  is_dynamic: boolean;
  is_canonical_for_surface: boolean;
  tab_strip_position: number | null;
}

export interface MatchResult {
  route_id: string;
  slot_values: Record<string, string | string[]>;
}

let _routes: RouteRegistryRow[] | null = null;
let _routesLoading: Promise<RouteRegistryRow[]> | null = null;

export async function loadRoutes(): Promise<RouteRegistryRow[]> {
  if (_routes) return _routes;
  if (_routesLoading) return _routesLoading;
  _routesLoading = (async () => {
    const res = await fetch('/api/shell/routes');
    if (!res.ok) {
      throw new Error(`shell.routes.fetch_failed: ${res.status} ${res.statusText}`);
    }
    const json = await res.json();
    const routes = Array.isArray(json?.routes) ? (json.routes as RouteRegistryRow[]) : [];
    _routes = routes;
    return routes;
  })();
  try {
    return await _routesLoading;
  } finally {
    _routesLoading = null;
  }
}

export function getRoutes(): RouteRegistryRow[] {
  return _routes ? [..._routes] : [];
}

export function setRoutesForTest(routes: RouteRegistryRow[]): void {
  _routes = [...routes];
}

export function clearRoutesForTest(): void {
  _routes = null;
  _routesLoading = null;
}

function findRoute(routeId: string): RouteRegistryRow | null {
  if (!_routes) return null;
  return _routes.find((row) => row.route_id === routeId) || null;
}

/**
 * Path-template parsers. The slot grammar supports:
 *   /app/run/{run_id}             — single positional slot
 *   /app/manifests?manifest={id}  — query slots
 *   /app/compose?pill={pill[]}    — repeatable query slot (suffix [])
 */

interface PathTemplateParts {
  pathname: string;
  query: Record<string, { value: string; isArray: boolean }>;
}

function parsePathTemplate(template: string): PathTemplateParts {
  const [path, query = ''] = template.split('?', 2);
  const parts: PathTemplateParts = { pathname: path, query: {} };
  if (!query) return parts;
  for (const segment of query.split('&')) {
    if (!segment) continue;
    const [key, raw = ''] = segment.split('=', 2);
    const value = decodeURIComponent(raw);
    const slotMatch = value.match(/^\{([^}]+)\}$/);
    if (slotMatch) {
      const inner = slotMatch[1];
      const isArray = inner.endsWith('[]');
      parts.query[key] = {
        value: isArray ? inner.slice(0, -2) : inner,
        isArray,
      };
    } else {
      parts.query[key] = { value, isArray: false };
    }
  }
  return parts;
}

function pathnameRegex(pathTemplate: string): { regex: RegExp; slotNames: string[] } {
  const slotNames: string[] = [];
  const escaped = pathTemplate
    .split('/')
    .map((seg) => {
      const slotMatch = seg.match(/^\{([^}]+)\}$/);
      if (slotMatch) {
        slotNames.push(slotMatch[1]);
        return '([^/]+)';
      }
      return seg.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    })
    .join('/');
  return { regex: new RegExp(`^${escaped}/?$`), slotNames };
}

export function matchPath(pathname: string, search: string): MatchResult | null {
  if (!_routes) return null;
  const params = new URLSearchParams(search);
  const candidates = [..._routes].sort((a, b) => a.display_order - b.display_order);

  for (const row of candidates) {
    const parts = parsePathTemplate(row.path_template);
    const { regex, slotNames } = pathnameRegex(parts.pathname);
    const m = pathname.match(regex);
    if (!m) continue;

    const slotValues: Record<string, string | string[]> = {};
    slotNames.forEach((name, i) => {
      slotValues[name] = decodeURIComponent(m[i + 1] || '');
    });

    let allQuerySlotsBound = true;
    for (const [key, slot] of Object.entries(parts.query)) {
      if (slot.isArray) {
        const all = params.getAll(key).filter((v) => v && v.trim());
        if (all.length === 0) {
          // Repeatable slots are optional unless the row's path explicitly demands them.
          // Bind empty array so consumers can iterate.
          slotValues[slot.value] = [];
          continue;
        }
        slotValues[slot.value] = all;
        continue;
      }
      const literalLiteral = !slot.value.match(/^[a-zA-Z_][a-zA-Z0-9_]*$/);
      if (literalLiteral) {
        // The template encodes a literal query value, e.g. ?manifest=editor.
        if (params.get(key) !== slot.value) {
          allQuerySlotsBound = false;
          break;
        }
        continue;
      }
      const v = params.get(key);
      if (v === null) {
        // Slot is unbound — only match if pathname-only routes (no required query slots).
        // For now: skip routes whose query slots are unbound.
        allQuerySlotsBound = false;
        break;
      }
      slotValues[slot.value] = v;
    }
    if (!allQuerySlotsBound) continue;

    return { route_id: row.route_id, slot_values: slotValues };
  }
  return null;
}

export function buildPath(
  routeId: string,
  slotValues: Record<string, string | string[]> = {},
): string {
  const row = findRoute(routeId);
  if (!row) return '/app';
  const parts = parsePathTemplate(row.path_template);

  let path = parts.pathname;
  // Replace pathname slots first.
  path = path
    .split('/')
    .map((seg) => {
      const slotMatch = seg.match(/^\{([^}]+)\}$/);
      if (!slotMatch) return seg;
      const slotName = slotMatch[1];
      const value = slotValues[slotName];
      if (Array.isArray(value)) return encodeURIComponent(value.join(','));
      return encodeURIComponent(value || '');
    })
    .join('/');

  const params = new URLSearchParams();
  for (const [key, slot] of Object.entries(parts.query)) {
    if (slot.isArray) {
      const arr = slotValues[slot.value];
      if (Array.isArray(arr)) {
        for (const v of arr) {
          if (v) params.append(key, v);
        }
      }
      continue;
    }
    const literalLiteral = !slot.value.match(/^[a-zA-Z_][a-zA-Z0-9_]*$/);
    if (literalLiteral) {
      params.set(key, slot.value);
      continue;
    }
    const v = slotValues[slot.value];
    if (typeof v === 'string' && v) {
      params.set(key, v);
    } else if (Array.isArray(v) && v.length > 0) {
      params.set(key, v.join(','));
    }
  }
  const query = params.toString();
  return query ? `${path}?${query}` : path;
}

/**
 * Reverse lookup with canonical priority. When state→URL has multiple
 * candidate rows for a surface_name, prefer the row whose required slots are
 * bound. Then prefer is_canonical_for_surface=TRUE rows. Then highest display_order.
 */
export function buildPathForSurface(
  surfaceName: string,
  slotValues: Record<string, string | string[]> = {},
): string {
  if (!_routes) return '/app';
  const candidates = _routes.filter((r) => r.surface_name === surfaceName);
  if (candidates.length === 0) return '/app';

  const slotsBound = (row: RouteRegistryRow): boolean => {
    const parts = parsePathTemplate(row.path_template);
    for (const seg of parts.pathname.split('/')) {
      const m = seg.match(/^\{([^}]+)\}$/);
      if (m && !slotValues[m[1]]) return false;
    }
    for (const slot of Object.values(parts.query)) {
      if (slot.isArray) continue;
      const isLiteral = !slot.value.match(/^[a-zA-Z_][a-zA-Z0-9_]*$/);
      if (isLiteral) continue;
      const v = slotValues[slot.value];
      const ok = (typeof v === 'string' && v) || (Array.isArray(v) && v.length > 0);
      if (!ok) {
        // Allow rows whose only unbound query slot is optional (no slots required).
        // For canonical selection we still need the bound ones to match.
      }
    }
    return true;
  };

  const sorted = [...candidates].sort((a, b) => {
    const aBound = slotsBound(a) ? 1 : 0;
    const bBound = slotsBound(b) ? 1 : 0;
    if (aBound !== bBound) return bBound - aBound;
    if (a.is_canonical_for_surface !== b.is_canonical_for_surface) {
      return a.is_canonical_for_surface ? -1 : 1;
    }
    return a.display_order - b.display_order;
  });

  return buildPath(sorted[0].route_id, slotValues);
}

/**
 * Lazy-import resolver for component_ref strings like
 * "dashboard/Dashboard.Dashboard" or "moon/MoonBuildPage.MoonBuildPage".
 * Format is "<relative-path-from-app-src>.<NamedExport>".
 */
const _componentCache = new Map<string, LazyExoticComponent<ComponentType<unknown>>>();

const COMPONENT_LOADERS: Record<string, () => Promise<{ default: ComponentType<unknown> }>> = {
  'dashboard/Dashboard.Dashboard': () =>
    import('../dashboard/Dashboard').then((m) => ({ default: m.Dashboard as ComponentType<unknown> })),
  'dashboard/CostsPanel.CostsPanel': () =>
    import('../dashboard/CostsPanel').then((m) => ({ default: m.CostsPanel as ComponentType<unknown> })),
  'dashboard/RunDetailView.RunDetailView': () =>
    import('../dashboard/RunDetailView').then((m) => ({ default: m.RunDetailView as ComponentType<unknown> })),
  'moon/MoonBuildPage.MoonBuildPage': () =>
    import('../moon/MoonBuildPage').then((m) => ({ default: m.MoonBuildPage as ComponentType<unknown> })),
  'praxis/ManifestCatalogPage.ManifestCatalogPage': () =>
    import('../praxis/ManifestCatalogPage').then((m) => ({ default: m.ManifestCatalogPage as ComponentType<unknown> })),
  'praxis/ManifestBundleView.ManifestBundleView': () =>
    import('../praxis/ManifestBundleView').then((m) => ({ default: m.ManifestBundleView as ComponentType<unknown> })),
  'praxis/SurfaceComposeView.SurfaceComposeView': () =>
    import('../praxis/SurfaceComposeView').then((m) => ({ default: m.SurfaceComposeView as ComponentType<unknown> })),
  'grid/ManifestEditorPage.ManifestEditorPage': () =>
    import('../grid/ManifestEditorPage').then((m) => ({ default: m.ManifestEditorPage as ComponentType<unknown> })),
  'atlas/AtlasPage.AtlasPage': () =>
    import('../atlas/AtlasPage').then((m) => ({ default: m.AtlasPage as ComponentType<unknown> })),
};

export function resolveComponent(routeId: string): LazyExoticComponent<ComponentType<unknown>> | null {
  const row = findRoute(routeId);
  if (!row || !row.component_ref) return null;
  const cached = _componentCache.get(row.component_ref);
  if (cached) return cached;
  const loader = COMPONENT_LOADERS[row.component_ref];
  if (!loader) return null;
  const component = lazy(loader);
  _componentCache.set(row.component_ref, component);
  return component;
}

/**
 * Minimal template expression evaluator for tab_label_template /
 * context_detail_template / nav_description_template.
 *
 * Supports:
 *   "literal text"
 *   "{{identifier}}"             — substitution
 *   "{{cond ? 'a' : 'b'}}"       — ternary
 *   nested ternary, single-quoted strings, plus operator on strings/numbers
 *   ".length" property accessor on arrays
 *   "===", "!==", ">", "<" comparisons
 *
 * Anything more complex should live in a dedicated component, not in a row.
 */
export function interpolateLabel(template: string | null | undefined, state: object): string {
  if (!template) return '';
  const ctx = state as Record<string, unknown>;
  return template.replace(/\{\{([^}]+)\}\}/g, (_match, expr) => {
    try {
      const value = evaluateExpression(String(expr).trim(), ctx);
      return value == null ? '' : String(value);
    } catch {
      return '';
    }
  });
}

function evaluateExpression(expr: string, state: Record<string, unknown>): unknown {
  // Ternary: cond ? a : b — right-associative for nested cases.
  const ternary = splitTopLevel(expr, '?');
  if (ternary.length >= 2) {
    const [cond, ...restParts] = ternary;
    const rest = restParts.join('?');
    const branches = splitTopLevel(rest, ':');
    if (branches.length >= 2) {
      const truthy = evaluateExpression(cond.trim(), state);
      return truthy
        ? evaluateExpression(branches[0].trim(), state)
        : evaluateExpression(branches.slice(1).join(':').trim(), state);
    }
  }

  // Plus operator: a + b + c
  const plusParts = splitTopLevel(expr, '+');
  if (plusParts.length > 1) {
    return plusParts
      .map((p) => evaluateExpression(p.trim(), state))
      .reduce((acc: unknown, v) => {
        if (typeof acc === 'number' && typeof v === 'number') return acc + v;
        return String(acc ?? '') + String(v ?? '');
      });
  }

  // Comparison
  for (const op of ['===', '!==', '>=', '<=', '>', '<'] as const) {
    const parts = splitTopLevel(expr, op);
    if (parts.length === 2) {
      const a = evaluateExpression(parts[0].trim(), state);
      const b = evaluateExpression(parts[1].trim(), state);
      switch (op) {
        case '===': return a === b;
        case '!==': return a !== b;
        case '>=': return Number(a) >= Number(b);
        case '<=': return Number(a) <= Number(b);
        case '>': return Number(a) > Number(b);
        case '<': return Number(a) < Number(b);
      }
    }
  }

  // String literal
  if ((expr.startsWith('"') && expr.endsWith('"')) || (expr.startsWith("'") && expr.endsWith("'"))) {
    return expr.slice(1, -1);
  }

  // Number literal
  if (/^-?\d+(\.\d+)?$/.test(expr)) return Number(expr);
  if (expr === 'true') return true;
  if (expr === 'false') return false;
  if (expr === 'null') return null;

  // Property access: name.prop or name.prop.subprop
  const segments = expr.split('.');
  let cur: unknown = state[segments[0]];
  for (let i = 1; i < segments.length; i++) {
    if (cur == null) return undefined;
    const seg = segments[i];
    if (seg === 'length' && Array.isArray(cur)) {
      cur = cur.length;
      continue;
    }
    cur = (cur as Record<string, unknown>)[seg];
  }
  return cur;
}

function splitTopLevel(input: string, delimiter: string): string[] {
  const parts: string[] = [];
  let depth = 0;
  let inString: string | null = null;
  let buf = '';
  let i = 0;
  while (i < input.length) {
    const ch = input[i];
    if (inString) {
      buf += ch;
      if (ch === inString && input[i - 1] !== '\\') inString = null;
      i++;
      continue;
    }
    if (ch === '"' || ch === "'") {
      inString = ch;
      buf += ch;
      i++;
      continue;
    }
    if (ch === '(' || ch === '[' || ch === '{') {
      depth++;
      buf += ch;
      i++;
      continue;
    }
    if (ch === ')' || ch === ']' || ch === '}') {
      depth--;
      buf += ch;
      i++;
      continue;
    }
    if (depth === 0 && input.startsWith(delimiter, i)) {
      parts.push(buf);
      buf = '';
      i += delimiter.length;
      continue;
    }
    buf += ch;
    i++;
  }
  parts.push(buf);
  return parts;
}
