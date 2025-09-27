// scraper.mjs
// Node 20+ (fetch global). Ejecuta: node scraper.mjs
import { writeFile, mkdir, readFile } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { resolve } from 'node:path';

const SRC_URL = 'https://pelotainvernal.com/liga/dominicana-lidom';
const OUT_DIR = resolve('docs');
const OUT_FILE = resolve(OUT_DIR, 'latest.json');

function extractFirstArgArrayOfViewModel(html) {
  const marker = 'new ViewModel(';
  const i = html.indexOf(marker);
  if (i < 0) throw new Error('No se encontró new ViewModel(');
  const start = html.indexOf('[', i);
  if (start < 0) throw new Error('No se encontró inicio de array para el primer argumento');

  let idx = start, depth = 0, inStr = false, quote = null, escape = false;

  while (idx < html.length) {
    const ch = html[idx];

    if (inStr) {
      if (escape) { escape = false; }
      else if (ch === '\\') { escape = true; }
      else if (ch === quote) { inStr = false; quote = null; }
    } else {
      if (ch === '"' || ch === "'") { inStr = true; quote = ch; }
      else if (ch === '[') { depth++; }
      else if (ch === ']') {
        depth--;
        if (depth === 0) {
          const jsonText = html.slice(start, idx + 1);
          return JSON.parse(jsonText);
        }
      }
    }
    idx++;
  }
  throw new Error('No se pudo cerrar el array del primer argumento');
}

function pickUseful(series0) {
  const { league, standings = [], todayGames = [], nearestGames = [], previousGames = [], previousRoundGames = [] } = series0;
  return { league, standings, todayGames, nearestGames, previousGames, previousRoundGames };
}

async function main() {
  const res = await fetch(SRC_URL, { headers: { 'user-agent': 'Mozilla/5.0 (+github.com template)' } });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const html = await res.text();

  const arr = extractFirstArgArrayOfViewModel(html);
  if (!Array.isArray(arr) || !arr.length) throw new Error('Array vacío del ViewModel');

  const series0 = pickUseful(arr[0]);

  const payload = {
    generated_at_utc: new Date().toISOString(),
    source: 'pelotainvernal.com',
    url: SRC_URL,
    series: series0,
  };

  if (!existsSync(OUT_DIR)) await mkdir(OUT_DIR, { recursive: true });

  let prev = null;
  if (existsSync(OUT_FILE)) {
    try { prev = JSON.parse(await readFile(OUT_FILE, 'utf8')); } catch {}
  }
  const nextStr = JSON.stringify(payload, null, 2);
  const prevStr = prev ? JSON.stringify(prev) : '';

  if (nextStr !== prevStr) {
    await writeFile(OUT_FILE, nextStr);
    console.log('Escrito:', OUT_FILE);
    process.exitCode = 0;
  } else {
    console.log('Sin cambios.');
    process.exitCode = 0;
  }
}

main().catch((e) => { console.error('ERROR:', e); process.exitCode = 1; });
