import { ModuleDefinition } from './types';

const modules: Map<string, ModuleDefinition> = new Map();

export function registerModule(def: ModuleDefinition): void {
  modules.set(def.id, def);
}

export function resolveModule(id: string): ModuleDefinition | undefined {
  return modules.get(id);
}

export function listModules(): ModuleDefinition[] {
  return Array.from(modules.values());
}
