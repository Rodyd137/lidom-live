// scraper.mjs
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, 'docs');
const HISTORY_DIR = path.join(OUT_DIR, 'history');
const SOURCE_HOME = process.env.SOURCE_HOME || 'https://pelotainvernal.com/';
const TZ = process.env.TZ || 'America/Santo_Domingo';

// Util: fecha ISO zulu
function nowISO() {
  return new Date().toISOString();
}

// Extrae el primer argumento de new ViewModel(...) buscando balance de [] / {}
function extractFirstArgOfViewModel(html) {
  const marker = 'ko.applyBindings(new ViewModel(';
  const idx = html.indexOf(marker);
  if (idx === -1) throw new Error('ViewModel call not found');

  let i = idx + marker.length;
  // Salta espacios
  while (/\s/.test(html[i])) i++;

  const open = html[i];
  const close = open === '[' ? ']' : open === '{' ? '}' : null;
  if (!close) throw new Error('Unexpected first-argument opener: ' + open);

  let depth = 0;
  let j = i;
  let inStr = false;
  let esc = false;

  while (j < html.length) {
    const ch = html[j];

    if (inStr) {
      if (esc) {
        esc = false;
      } else if (ch === '\\') {
        esc = true;
      } else if (ch === '"') {
        inStr = false;
      }
      j++;
      continue;
    } else {
      if (ch === '"') {
        inStr = true;
        j++;
        continue;
      }
      if (ch === open) depth++;
      if (ch === close) {
        depth--;
        if (depth === 0) {
          // j inclusive
          const jsonText = html.slice(i, j + 1);
          return jsonText;
        }
      }
    }
    j++;
  }
  throw new Error('Unbalanced brackets while parsing first arg');
}

// Normaliza equipos / juegos mínimamente
function normalize(data) {
  // data es array con un único objeto de serie (home)
  // Estructura esperada: [ { league: {...}, standings: [...], todayGames: [...], nearestGames: [...], upcomingGames: [...], previousGames: [...] } , ...ads, translations]
  if (!Array.isArray(data) || data.length === 0) throw new Error('Unexpected data root');

  const series = data[0];

  const league = series.league ?? {};
  const pkg = {
    source: {
      homepage: SOURCE_HOME,
      scraped_at: nowISO(),
      tz: TZ
    },
    league: {
      id: league.id ?? null,
      name: league.name ?? null,
      seo_url: league.seo_url ?? null,
      season_id: league.season_id ?? null,
      round_name: league.round_name ?? null,
      date_start: league.date_start ?? null,
      date_end: league.date_end ?? null
    },
    standings: (series.standings ?? []).map(s => ({
      team: {
        id: s.team?.id ?? null,
        name: s.team?.name ?? null,
        abbreviation: s.team?.abbreviation ?? null,
        logo: s.team?.logo ?? null,
        permalink: s.team?.permalink ?? null,
        color: s.team?.color ?? null
      },
      wins: s.wins ?? 0,
      loses: s.loses ?? 0,
      gb: s.gb,
      pct: s.pct,
      wins_home: s.wins_home ?? null,
      loses_home: s.loses_home ?? null,
      wins_visitor: s.wins_visitor ?? null,
      loses_visitor: s.loses_visitor ?? null
    })),
    games: {
      today: series.todayGames ?? [],
      upcoming: series.upcomingGames ?? series.nearestGames ?? [],
      previous: series.previousGames ?? series.previousRoundGames ?? []
    }
  };

  // Ordena standings por pct desc si viene como string ".800"
  const pctNum = v => (typeof v === 'string' ? Number(v) : Number(v || 0));
  pkg.standings.sort((a, b) => pctNum(b.pct) - pctNum(a.pct));

  // Ordena juegos por fecha asc
  const byDate = (a, b) => new Date(a.date) - new Date(b.date);
  pkg.games.today.sort(byDate);
  pkg.games.upcoming.sort(byDate);
  pkg.games.previous.sort(byDate);

  return pkg;
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      'user-agent': 'scrape-lidom-bot/1.0 (+github actions)',
      'accept': 'text/html,application/xhtml+xml'
    }
  });
  if (!res.ok) throw new Error(`GET ${url} -> ${res.status}`);
  return res.text();
}

async function main() {
  const html = await fetchText(SOURCE_HOME);
  const firstArgText = extractFirstArgOfViewModel(html);

  // Convierte a JSON válido
  // Nota: El payload ya es JSON-like con comillas dobles/booleanos, debería parsear directo.
  const data = JSON.parse(firstArgText);

  const out = normalize(data);

  // Asegura carpetas
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.mkdirSync(HISTORY_DIR, { recursive: true });

  const latestPath = path.join(OUT_DIR, 'latest.json');
  const tmpPath = path.join(OUT_DIR, 'latest.tmp.json');

  fs.writeFileSync(tmpPath, JSON.stringify(out, null, 2) + '\n', 'utf8');

  // Evita escribir si no cambió (para reducir ruido)
  const old = fs.existsSync(latestPath) ? fs.readFileSync(latestPath, 'utf8') : null;
  const neu = fs.readFileSync(tmpPath, 'utf8');

  if (old !== neu) {
    fs.renameSync(tmpPath, latestPath);

    // Snapshot histórico
    const iso = nowISO().replace(/[:]/g, '').replace(/\..+/, 'Z'); // YYYY-MM-DDTHHMMSSZ
    const snapPath = path.join(HISTORY_DIR, `${iso}.json`);
    fs.writeFileSync(snapPath, neu, 'utf8');

    console.log('Updated docs/latest.json and history snapshot.');
  } else {
    fs.unlinkSync(tmpPath);
    console.log('No content changes.');
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
