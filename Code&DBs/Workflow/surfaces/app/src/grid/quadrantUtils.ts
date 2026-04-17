const ROWS = ['A', 'B', 'C', 'D'] as const;
const COLS = ['1', '2', '3', '4'] as const;

type QuadrantCellDef = { span?: string };

export const ALL_CELLS: string[] = ROWS.flatMap(r => COLS.map(c => `${r}${c}`));

export function parseQuadrantId(id: string): { row: number; col: number } {
  const letter = id.charAt(0).toUpperCase();
  const digit = id.charAt(1);
  const row = letter.charCodeAt(0) - 'A'.charCodeAt(0);
  const col = parseInt(digit, 10) - 1;
  return { row, col };
}

export function parseSpan(span: string): { cols: number; rows: number } {
  const [colStr, rowStr] = span.split('x');
  return { cols: parseInt(colStr, 10), rows: parseInt(rowStr, 10) };
}

export function cellIdFromRowCol(row: number, col: number): string | null {
  if (row < 0 || row > 3 || col < 0 || col > 3) return null;
  return `${ROWS[row]}${COLS[col]}`;
}

export function collectQuadrantFootprint(quadrantId: string, span?: string): string[] {
  const { row, col } = parseQuadrantId(quadrantId);
  const { cols, rows } = span ? parseSpan(span) : { cols: 1, rows: 1 };
  const cells: string[] = [];

  for (let currentRow = row; currentRow < row + rows && currentRow < 4; currentRow++) {
    for (let currentCol = col; currentCol < col + cols && currentCol < 4; currentCol++) {
      const cellId = cellIdFromRowCol(currentRow, currentCol);
      if (cellId) cells.push(cellId);
    }
  }

  return cells;
}

export function canQuadrantOccupySpan(
  quadrants: Record<string, QuadrantCellDef>,
  quadrantId: string,
  span: string,
  ignoreQuadrantId: string | null = quadrantId,
): boolean {
  const { row, col } = parseQuadrantId(quadrantId);
  const { cols, rows } = parseSpan(span);

  if (row < 0 || row + rows > 4 || col < 0 || col + cols > 4) {
    return false;
  }

  const nextFootprint = new Set(collectQuadrantFootprint(quadrantId, span));

  for (const [otherQuadrantId, def] of Object.entries(quadrants)) {
    if (ignoreQuadrantId && otherQuadrantId === ignoreQuadrantId) continue;
    const otherFootprint = collectQuadrantFootprint(otherQuadrantId, def.span);
    if (otherFootprint.some((cellId) => nextFootprint.has(cellId))) {
      return false;
    }
  }

  return true;
}

export function availableSpansForQuadrant(
  quadrants: Record<string, QuadrantCellDef>,
  quadrantId: string,
  spanOptions: readonly string[],
): string[] {
  return spanOptions.filter((span) => canQuadrantOccupySpan(quadrants, quadrantId, span, quadrantId));
}

export function getOccupiedCells(
  quadrants: Record<string, QuadrantCellDef>,
): Set<string> {
  const occupied = new Set<string>();
  for (const [id, def] of Object.entries(quadrants)) {
    for (const cellId of collectQuadrantFootprint(id, def.span)) {
      if (cellId !== id) {
        occupied.add(cellId);
      }
    }
  }
  return occupied;
}
