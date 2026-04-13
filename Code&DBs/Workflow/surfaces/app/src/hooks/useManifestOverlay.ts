import { useSlice } from './useSlice';
import { world } from '../world';
import { QuadrantManifest } from '../grid/QuadrantGrid';

function deepMerge(target: any, source: any) {
  if (typeof target !== 'object' || target === null) return source;
  if (typeof source !== 'object' || source === null) return source;
  const output = { ...target };
  Object.keys(source).forEach(key => {
    if (typeof source[key] === 'object' && source[key] !== null) {
      if (!(key in target)) {
        Object.assign(output, { [key]: source[key] });
      } else {
        output[key] = deepMerge(target[key], source[key]);
      }
    } else {
      Object.assign(output, { [key]: source[key] });
    }
  });
  return output;
}

export function useManifestOverlay(manifest: QuadrantManifest): QuadrantManifest {
  const overrides = useSlice(world, 'ui.layout.quadrants') as Record<string, any> | null;
  if (!overrides || Object.keys(overrides).length === 0) return manifest;
  
  // Deep merge overrides into the manifest quadrants
  const mergedQuadrants = deepMerge(manifest.quadrants, overrides);
  
  return { ...manifest, quadrants: mergedQuadrants };
}
