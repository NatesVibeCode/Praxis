import type { MenuAction, MenuSection } from './menuTypes';

function scoreField(value: string | undefined, query: string, exactWeight: number, partialWeight: number): number {
  if (!value) return 0;
  const normalized = value.toLowerCase();
  if (normalized === query) return exactWeight;
  if (normalized.startsWith(query)) return partialWeight + 20;
  if (normalized.includes(query)) return partialWeight;
  return 0;
}

export function rankMenuAction(action: MenuAction, query: string): number {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) return 1;

  let score = 0;
  score += scoreField(action.label, normalizedQuery, 140, 90);
  score += scoreField(action.description, normalizedQuery, 80, 50);
  score += scoreField(action.meta, normalizedQuery, 50, 35);

  for (const keyword of action.keywords || []) {
    score += scoreField(keyword, normalizedQuery, 70, 40);
  }

  return score;
}

export function filterMenuSections(sections: MenuSection[], query: string): MenuSection[] {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) {
    return sections
      .map((section) => ({ ...section, items: section.items.filter((item) => !item.disabled || item.label) }))
      .filter((section) => section.items.length > 0);
  }

  return sections
    .map((section) => {
      const ranked = section.items
        .map((item) => ({ item, score: rankMenuAction(item, normalizedQuery) }))
        .filter((entry) => entry.score > 0)
        .sort((left, right) => right.score - left.score || left.item.label.localeCompare(right.item.label))
        .map((entry) => entry.item);

      return { ...section, items: ranked };
    })
    .filter((section) => section.items.length > 0);
}

export function flattenMenuSections(sections: MenuSection[]): MenuAction[] {
  return sections.flatMap((section) => section.items);
}
