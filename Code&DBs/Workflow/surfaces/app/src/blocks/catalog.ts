import catalog from './catalog.v1.json';

export type BlockType = 'display' | 'input' | 'tool' | 'composite';

export interface BlockCatalogEntry {
  id: string;
  name: string;
  type: BlockType;
  defaultSpan: string;
  description: string;
}

export const blockCatalog = catalog as BlockCatalogEntry[];

const blockCatalogMap = new Map(blockCatalog.map((entry) => [entry.id, entry]));

export function getBlockCatalogEntry(id: string): BlockCatalogEntry | undefined {
  return blockCatalogMap.get(id);
}

export function listBlockCatalog(): BlockCatalogEntry[] {
  return [...blockCatalog];
}
