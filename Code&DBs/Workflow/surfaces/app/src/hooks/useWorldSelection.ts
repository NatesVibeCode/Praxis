import { useSlice } from './useSlice';
import { world } from '../world';

export function useWorldSelection<T = unknown>(objectType: string): T | null {
  const value = useSlice(world, `shared.selected_${objectType}`);
  return (value as T) ?? null;
}

export function publishSelection(objectType: string, data: unknown): void {
  world.applyDeltas([{
    op: 'put',
    path: `shared.selected_${objectType}`,
    value: data,
    version: world.version + 1,
  }]);
}
