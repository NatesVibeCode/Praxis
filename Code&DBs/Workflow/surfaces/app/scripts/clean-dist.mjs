import { lstat, mkdir, readdir, rm, rmdir } from 'node:fs/promises';
import { basename, dirname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';

const appRoot = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const outDir = resolve(appRoot, 'dist');

function assertSafeOutDir(path) {
  const relative = path.slice(appRoot.length + 1);
  if (!path.startsWith(`${appRoot}${sep}`) || relative !== 'dist') {
    throw new Error(`Refusing to clean unexpected output directory: ${path}`);
  }
}

async function removePath(path, tolerated) {
  if (basename(path).startsWith('.smbdelete')) {
    tolerated.push(path);
    return;
  }
  try {
    await rm(path, {
      recursive: false,
      force: true,
      maxRetries: 8,
      retryDelay: 125,
    });
    return;
  } catch (error) {
    if (basename(path).startsWith('.smbdelete')) {
      tolerated.push(path);
      return;
    }
    throw error;
  }
}

async function cleanDirectoryContents(path, tolerated) {
  let entries;
  try {
    entries = await readdir(path);
  } catch (error) {
    if (error?.code === 'ENOENT') return;
    throw error;
  }

  for (const entry of entries) {
    const child = resolve(path, entry);
    const childStat = await lstat(child).catch(() => null);
    if (!childStat) continue;
    if (childStat.isDirectory()) {
      await cleanDirectoryContents(child, tolerated);
      const leftovers = await readdir(child).catch(() => []);
      if (leftovers.length > 0 && leftovers.every((item) => item.startsWith('.smbdelete'))) {
        tolerated.push(child);
        continue;
      }
      await rmdir(child).catch((error) => {
        if (error?.code === 'ENOENT') return;
        throw error;
      });
      continue;
    }
    await removePath(child, tolerated);
  }
}

assertSafeOutDir(outDir);
const tolerated = [];
await cleanDirectoryContents(outDir, tolerated);
await mkdir(outDir, { recursive: true });

if (tolerated.length > 0) {
  console.warn(`[clean-dist] kept ${tolerated.length} SMB delete tombstone(s); Vite will ignore them.`);
}
