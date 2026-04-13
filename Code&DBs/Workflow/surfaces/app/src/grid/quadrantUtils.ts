const ROWS = ['A', 'B', 'C', 'D'] as const;
const COLS = ['1', '2', '3', '4'] as const;

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

export function getOccupiedCells(
  quadrants: Record<string, { span?: string }>,
): Set<string> {
  const occupied = new Set<string>();
  for (const [id, def] of Object.entries(quadrants)) {
    const { row, col } = parseQuadrantId(id);
    const { cols, rows } = def.span ? parseSpan(def.span) : { cols: 1, rows: 1 };
    for (let r = row; r < row + rows && r < 4; r++) {
      for (let c = col; c < col + cols && c < 4; c++) {
        const cellId = `${ROWS[r]}${COLS[c]}`;
        if (cellId !== id) {
          occupied.add(cellId);
        }
      }
    }
  }
  return occupied;
}
