// Variables land inside WeirQL strings. Several values become a regex alternation, for =~.
export function formatValue(value: string | string[]): string {
  if (Array.isArray(value)) {
    const alts = value.map(escapeRegex);
    return escapeString(alts.length === 1 ? alts[0] : `(${alts.join('|')})`);
  }
  return escapeString(value);
}

function escapeRegex(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

function escapeString(s: string): string {
  return s.replace(/[\\"]/g, '\\$&');
}

export function parseLabelValues(query: string): { label: string; filters: Record<string, string> } | undefined {
  const m = query
    .trim()
    .match(/^label_values\(\s*([A-Za-z_]\w*)\s*((?:,\s*[A-Za-z_]\w*\s*=\s*"(?:[^"\\]|\\.)*"\s*)*)\)$/);
  if (!m) {
    return undefined;
  }
  const filters: Record<string, string> = {};
  for (const f of m[2].matchAll(/([A-Za-z_]\w*)\s*=\s*"((?:[^"\\]|\\.)*)"/g)) {
    filters[f[1]] = f[2].replace(/\\(.)/g, '$1');
  }
  return { label: m[1], filters };
}
