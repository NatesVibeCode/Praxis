import type { QuadrantManifest } from '../grid/QuadrantGrid';

const PRAXIS_SURFACE_BUNDLE_KIND = 'praxis_surface_bundle' as const;

export type SourceOptionFamily = 'workspace' | 'connected' | 'reference' | 'external';
export type SourceOptionKind =
  | 'object'
  | 'manifest'
  | 'document'
  | 'integration'
  | 'web_search'
  | 'api'
  | 'dataset';
export type SourceOptionAvailability = 'ready' | 'setup_required' | 'preview';
export type SourceOptionActivation = 'attach' | 'open' | 'configure';

export interface SourceOption {
  id: string;
  label: string;
  family: SourceOptionFamily;
  kind: SourceOptionKind;
  availability: SourceOptionAvailability;
  activation: SourceOptionActivation;
  reference_slug?: string;
  integration_id?: string;
  setup_intent?: string;
  description?: string;
}

export interface PraxisTabDefinition {
  id: string;
  label: string;
  surface_id: string;
  source_option_ids?: string[];
}

export interface QuadrantSurfaceSpec {
  id: string;
  title: string;
  kind: 'quadrant_manifest';
  manifest: QuadrantManifest;
}

export type PraxisSurfaceSpec = QuadrantSurfaceSpec;

export interface PraxisSurfaceBundleV4 {
  version: 4;
  kind: typeof PRAXIS_SURFACE_BUNDLE_KIND;
  title: string;
  default_tab_id: string;
  tabs: PraxisTabDefinition[];
  surfaces: Record<string, PraxisSurfaceSpec>;
  source_options?: Record<string, SourceOption>;
  legacy?: Record<string, unknown>;
  id?: string;
  name?: string;
  description?: string;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function isLegacyManifest(value: unknown): value is QuadrantManifest {
  return isRecord(value)
    && typeof value.version === 'number'
    && typeof value.grid === 'string'
    && isRecord(value.quadrants);
}

function normalizeString(value: unknown, fallback: string): string {
  return typeof value === 'string' && value.trim() ? value : fallback;
}

function normalizeSourceOption(id: string, value: unknown): SourceOption | null {
  if (!isRecord(value)) return null;
  return {
    id,
    label: normalizeString(value.label, id),
    family: (value.family === 'connected' || value.family === 'reference' || value.family === 'external')
      ? value.family
      : 'workspace',
    kind: (
      value.kind === 'manifest'
      || value.kind === 'document'
      || value.kind === 'integration'
      || value.kind === 'web_search'
      || value.kind === 'api'
      || value.kind === 'dataset'
    )
      ? value.kind
      : 'object',
    availability: value.availability === 'setup_required' || value.availability === 'preview'
      ? value.availability
      : 'ready',
    activation: value.activation === 'open' || value.activation === 'configure'
      ? value.activation
      : 'attach',
    reference_slug: typeof value.reference_slug === 'string' ? value.reference_slug : undefined,
    integration_id: typeof value.integration_id === 'string' ? value.integration_id : undefined,
    setup_intent: typeof value.setup_intent === 'string' ? value.setup_intent : undefined,
    description: typeof value.description === 'string' ? value.description : undefined,
  };
}

export function adaptLegacyManifestToBundle(
  manifest: QuadrantManifest,
  meta?: { title?: string; id?: string; description?: string },
): PraxisSurfaceBundleV4 {
  const title = normalizeString(meta?.title ?? manifest.title, meta?.id ?? 'Workspace');
  return {
    version: 4,
    kind: PRAXIS_SURFACE_BUNDLE_KIND,
    title,
    default_tab_id: 'main',
    tabs: [
      {
        id: 'main',
        label: title,
        surface_id: 'main',
        source_option_ids: [],
      },
    ],
    surfaces: {
      main: {
        id: 'main',
        title,
        kind: 'quadrant_manifest',
        manifest: {
          version: 2,
          grid: manifest.grid ?? '4x4',
          title: manifest.title ?? title,
          quadrants: manifest.quadrants ?? {},
        },
      },
    },
    source_options: {},
    legacy: {
      source_manifest_version: 2,
    },
    id: meta?.id,
    name: meta?.title ?? title,
    description: meta?.description,
  };
}

export function normalizePraxisBundle(
  value: unknown,
  meta?: { title?: string; id?: string; description?: string },
): PraxisSurfaceBundleV4 {
  if (!isRecord(value)) {
    return adaptLegacyManifestToBundle({ version: 2, grid: '4x4', quadrants: {} }, meta);
  }

  if (isLegacyManifest(value) && value.version === 2) {
    return adaptLegacyManifestToBundle(value, meta);
  }

  const rawTabs = Array.isArray(value.tabs) ? value.tabs : [];
  const rawSurfaces = isRecord(value.surfaces) ? value.surfaces : {};
  const normalizedTabs: PraxisTabDefinition[] = [];
  rawTabs.forEach((item, index) => {
    if (!isRecord(item)) return;
    const id = normalizeString(item.id, `tab_${index + 1}`);
    const surfaceId = normalizeString(item.surface_id, id);
    normalizedTabs.push({
      id,
      label: normalizeString(item.label, id),
      surface_id: surfaceId,
      source_option_ids: Array.isArray(item.source_option_ids)
        ? item.source_option_ids.filter((entry): entry is string => typeof entry === 'string')
        : [],
    });
  });

  const surfaces = Object.entries(rawSurfaces).reduce<Record<string, PraxisSurfaceSpec>>((acc, [surfaceId, rawSurface]) => {
    if (!isRecord(rawSurface)) return acc;
    const kind = rawSurface.kind === 'quadrant_manifest' ? 'quadrant_manifest' : 'quadrant_manifest';
    const rawManifest = isRecord(rawSurface.manifest)
      ? rawSurface.manifest
      : isRecord(rawSurface.quadrants)
        ? rawSurface
        : { version: 2, grid: '4x4', quadrants: {} };
    acc[surfaceId] = {
      id: normalizeString(rawSurface.id, surfaceId),
      title: normalizeString(rawSurface.title, surfaceId),
      kind,
      manifest: {
        version: 2,
        grid: normalizeString((rawManifest as Record<string, unknown>).grid, '4x4'),
        title: typeof (rawManifest as Record<string, unknown>).title === 'string'
          ? (rawManifest as Record<string, unknown>).title as string
          : normalizeString(rawSurface.title, surfaceId),
        quadrants: isRecord((rawManifest as Record<string, unknown>).quadrants)
          ? ((rawManifest as Record<string, unknown>).quadrants as Record<string, QuadrantManifest['quadrants'][string]>)
          : {},
      },
    };
    return acc;
  }, {});

  const normalizedSourceOptions = isRecord(value.source_options)
    ? Object.entries(value.source_options).reduce<Record<string, SourceOption>>((acc, [id, rawOption]) => {
        const option = normalizeSourceOption(id, rawOption);
        if (option) acc[id] = option;
        return acc;
      }, {})
    : {};

  const title = normalizeString(value.title, meta?.title ?? meta?.id ?? 'Workspace');
  const defaultTab = normalizeString(value.default_tab_id, normalizedTabs[0]?.id ?? 'main');

  if (normalizedTabs.length === 0) {
    return adaptLegacyManifestToBundle({ version: 2, grid: '4x4', quadrants: {} }, meta);
  }

  return {
    version: 4,
    kind: PRAXIS_SURFACE_BUNDLE_KIND,
    title,
    default_tab_id: defaultTab,
    tabs: normalizedTabs,
    surfaces,
    source_options: normalizedSourceOptions,
    legacy: isRecord(value.legacy) ? value.legacy : undefined,
    id: meta?.id ?? (typeof value.id === 'string' ? value.id : undefined),
    name: meta?.title ?? (typeof value.name === 'string' ? value.name : undefined),
    description: meta?.description ?? (typeof value.description === 'string' ? value.description : undefined),
  };
}

export function resolvePraxisBundleTab(bundle: PraxisSurfaceBundleV4, tabId?: string | null): PraxisTabDefinition {
  return bundle.tabs.find((item) => item.id === tabId) ?? bundle.tabs.find((item) => item.id === bundle.default_tab_id) ?? bundle.tabs[0];
}

export function resolvePraxisBundleSurface(bundle: PraxisSurfaceBundleV4, tabId?: string | null): PraxisSurfaceSpec | null {
  const tab = resolvePraxisBundleTab(bundle, tabId);
  return bundle.surfaces[tab.surface_id] ?? null;
}
