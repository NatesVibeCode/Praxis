type Callback = (value: unknown) => void;

interface Delta {
  op: 'put' | 'delete';
  path: string;
  value?: unknown;
  version: number;
}

interface Snapshot {
  state: Record<string, unknown>;
  version: number;
}

function normalizePath(path: string | null | undefined): string {
  return path == null ? '' : String(path);
}

function hasOwn(obj: object, key: string): boolean {
  return Object.prototype.hasOwnProperty.call(obj, key);
}

function isPlainObject(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object') return false;
  if (Array.isArray(value)) return false;
  const proto = Object.getPrototypeOf(value);
  return proto === null || proto === Object.prototype;
}

function deepMerge(
  existing: Record<string, unknown>,
  incoming: Record<string, unknown>,
): Record<string, unknown> {
  const out = { ...existing };
  for (const key of Object.keys(incoming)) {
    const next = incoming[key];
    const prev = out[key];
    if (isPlainObject(next) && isPlainObject(prev)) {
      out[key] = deepMerge(prev, next);
    } else {
      out[key] = next;
    }
  }
  return out;
}

function getNested(obj: unknown, path: string): unknown {
  const p = normalizePath(path);
  if (p === '') return obj;
  let current: unknown = obj;
  for (const segment of p.split('.')) {
    if (current == null || typeof current !== 'object' || !hasOwn(current as object, segment)) {
      return undefined;
    }
    current = (current as Record<string, unknown>)[segment];
  }
  return current;
}

function setNested(obj: Record<string, unknown>, path: string, value: unknown): void {
  const segments = normalizePath(path).split('.');
  let current: Record<string, unknown> = obj;
  for (let i = 0; i < segments.length - 1; i++) {
    const seg = segments[i];
    const next = current[seg];
    if (next == null || typeof next !== 'object') {
      current[seg] = {};
    }
    current = current[seg] as Record<string, unknown>;
  }
  const last = segments[segments.length - 1];
  const full = normalizePath(path);
  if (full === 'ui.layout') {
    const prev = current[last];
    if (isPlainObject(prev) && isPlainObject(value)) {
      current[last] = deepMerge(prev, value as Record<string, unknown>);
      return;
    }
  }
  current[last] = value;
}

function deleteNested(obj: Record<string, unknown>, path: string): void {
  const segments = normalizePath(path).split('.');
  let current: unknown = obj;
  for (let i = 0; i < segments.length - 1; i++) {
    if (current == null || typeof current !== 'object' || !hasOwn(current as object, segments[i])) {
      return;
    }
    current = (current as Record<string, unknown>)[segments[i]];
  }
  if (current != null && typeof current === 'object') {
    delete (current as Record<string, unknown>)[segments[segments.length - 1]];
  }
}

function isBoundaryPrefix(prefix: string, path: string): boolean {
  return prefix === '' || path === prefix || path.startsWith(`${prefix}.`);
}

function isRelatedPath(subscriberPath: string, changedPath: string): boolean {
  const sp = normalizePath(subscriberPath);
  const cp = normalizePath(changedPath);
  return isBoundaryPrefix(sp, cp) || isBoundaryPrefix(cp, sp);
}

const WORLD_PERSISTENCE_KEY = 'praxis.world.snapshot.v1';

function getWorldStorage(): Storage | null {
  const storage = (globalThis as { localStorage?: Storage | undefined }).localStorage;
  if (!storage) return null;
  if (typeof storage.getItem !== 'function' || typeof storage.setItem !== 'function') return null;
  return storage;
}

function readPersistedSnapshot(): Snapshot | null {
  const storage = getWorldStorage();
  if (!storage) return null;

  try {
    const raw = storage.getItem(WORLD_PERSISTENCE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<Snapshot> | null;
    if (!parsed || typeof parsed !== 'object') return null;
    if (!isPlainObject(parsed.state)) return null;
    if (typeof parsed.version !== 'number' || !Number.isFinite(parsed.version)) return null;
    return {
      state: structuredClone(parsed.state),
      version: Math.max(0, Math.floor(parsed.version)),
    };
  } catch {
    return null;
  }
}

function writePersistedSnapshot(snapshot: Snapshot): void {
  const storage = getWorldStorage();
  if (!storage) return;

  try {
    storage.setItem(WORLD_PERSISTENCE_KEY, JSON.stringify(snapshot));
  } catch {
    // Best effort only. In-memory state remains authoritative if persistence fails.
  }
}

export class World {
  private _committed: Record<string, unknown> = {};
  private _proposed: Record<string, unknown> = {};
  private _version = 0;
  private _subs = new Map<string, Set<Callback>>();
  private _useProposed = false;

  constructor() {
    const snapshot = readPersistedSnapshot();
    if (snapshot) {
      this._committed = structuredClone(snapshot.state);
      this._version = snapshot.version;
    }
  }

  get version(): number {
    return this._version;
  }

  hydrate(snapshot: Snapshot): void {
    this._committed = structuredClone(snapshot.state);
    this._version = snapshot.version;
    this._proposed = {};
    writePersistedSnapshot({ state: this._committed, version: this._version });
    this._notifyAll();
  }

  applyDeltas(deltas: Delta[]): void {
    let mutated = false;
    for (const delta of deltas) {
      if (delta.version <= this._version) continue;
      const path = normalizePath(delta.path);
      if (delta.op === 'put') {
        const val = structuredClone(delta.value);
        if (path === '') {
          this._committed = val as Record<string, unknown>;
        } else {
          setNested(this._committed, path, val);
        }
      } else if (delta.op === 'delete') {
        if (path === '') {
          this._committed = {};
        } else {
          deleteNested(this._committed, path);
        }
      }
      this._version = delta.version;
      mutated = true;
      this._notifyMatching(path);
    }
    if (mutated) {
      writePersistedSnapshot({ state: this._committed, version: this._version });
    }
  }

  propose(path: string, value: unknown): void {
    setNested(this._proposed, normalizePath(path), structuredClone(value));
    this._notifyMatching(normalizePath(path));
  }

  clearProposed(): void {
    this._proposed = {};
    this._notifyAll();
  }

  set(path: string, value: unknown): void {
    this.applyDeltas([{ op: 'put', path, value, version: this._version + 1 }]);
  }

  get(path?: string | null): unknown {
    const p = normalizePath(path);
    const committed = getNested(this._committed, p);
    const proposed = getNested(this._proposed, p);
    if (isPlainObject(committed) && isPlainObject(proposed)) {
      return deepMerge(committed, proposed);
    }
    return proposed !== undefined ? proposed : committed;
  }

  getCommitted(path?: string | null): unknown {
    return getNested(this._committed, normalizePath(path));
  }

  subscribe(path: string | null | undefined, callback: Callback): () => void {
    const p = normalizePath(path);
    let cbs = this._subs.get(p);
    if (!cbs) {
      cbs = new Set();
      this._subs.set(p, cbs);
    }
    cbs.add(callback);
    callback(this.get(p));
    return () => {
      const current = this._subs.get(p);
      if (!current) return;
      current.delete(callback);
      if (current.size === 0) this._subs.delete(p);
    };
  }

  private _notifyAll(): void {
    for (const [path, cbs] of this._subs.entries()) {
      const value = this.get(path);
      for (const cb of Array.from(cbs)) cb(value);
    }
  }

  private _notifyMatching(changedPath: string): void {
    for (const [path, cbs] of this._subs.entries()) {
      if (!isRelatedPath(path, changedPath)) continue;
      const value = this.get(path);
      for (const cb of Array.from(cbs)) cb(value);
    }
  }
}

export const world = new World();
