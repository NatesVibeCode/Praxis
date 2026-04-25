import { useEffect, useState } from 'react';
import { normalizePraxisBundle, type PraxisSurfaceBundleV4 } from '../praxis/manifest';

/** Seed entry shape consumed by the Workspace New command menu. */
export interface SeedBundleEntry {
  id: string;
  label: string;
  description: string;
  bundle: PraxisSurfaceBundleV4;
}

interface ManifestRow {
  id: string;
  name?: string | null;
  description?: string | null;
  manifest?: unknown;
}

/**
 * Read workspace seed bundles from /api/manifests?status=seed. Replaces the
 * pre-existing hard-coded seedBundles.ts module — closes the filed
 * architecture-policy::surface-catalog::surface-composition-cqrs-direction
 * debt that called seedBundles.ts a parallel registry. Seeds now live in
 * app_manifests with status='seed' (migration 241), so adding a new seed is
 * a row UPSERT, not a code edit.
 */
export function useSeedBundles(): {
  seeds: SeedBundleEntry[];
  loading: boolean;
  error: string | null;
} {
  const [seeds, setSeeds] = useState<SeedBundleEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(null);

    const load = async () => {
      try {
        const res = await fetch('/api/manifests?status=seed&limit=20');
        if (!res.ok) throw new Error(`/api/manifests?status=seed → ${res.status}`);
        const payload = await res.json();
        if (cancelled) return;
        const rows: ManifestRow[] = Array.isArray(payload?.manifests) ? payload.manifests : [];
        const normalized: SeedBundleEntry[] = rows
          .map((row) => {
            if (!row?.id) return null;
            const bundle = normalizePraxisBundle(row.manifest, {
              id: row.id,
              title: row.name ?? row.id,
              description: row.description ?? '',
            });
            return {
              id: row.id,
              label: row.name ?? row.id,
              description: row.description ?? '',
              bundle,
            };
          })
          .filter((entry): entry is SeedBundleEntry => entry !== null);
        setSeeds(normalized);
      } catch (err) {
        if (!cancelled) setError(err instanceof Error ? err.message : String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  return { seeds, loading, error };
}
