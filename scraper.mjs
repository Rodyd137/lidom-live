// scraper.mjs
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, 'docs');
const HISTORY_DIR = path.join(OUT_DIR, 'history');
const LATEST_PATH = path.join(OUT_DIR, 'latest.json');
const SOURCE_HOME = process.env.SOURCE_HOME || 'https://pelotainvernal.com/';
const TZ = process.env.TZ || 'America/Santo_Domingo';

// ---- Utilidades ----
function nowISO() {
  return new Date().toISOString();
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

// Extrae SOLO el 1er argumento de new ViewModel(...) (home)
function extractFirstArgOfViewModel(html) {
  const marker = 'ko.applyBindings(new ViewModel(';
  const idx = html.indexOf(marker);
  if (idx === -1) throw new Error('ViewModel call not found');

  let i = idx + marker.length;
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
          const jsonText = html.slice(i, j + 1);
          return jsonText;
        }
      }
    }
    j++;
  }
  throw new Error('Unbalanced brackets while parsing first arg');
}

// ---- NUEVO: parsea los 4 args del ViewModel del calendario ----
function extractViewModelArgs(html) {
  const marker = 'ko.applyBindings(new ViewModel(';
  const start = html.indexOf(marker);
  if (start === -1) throw new Error('ViewModel call not found (calendar)');

  let i = start + marker.length;
  const args = [];

  function readArg() {
    // saltar espacios y comas iniciales
    while (/\s|,/.test(html[i])) i++;
    const ch = html[i];

    // objetos o arreglos
    if (ch === '{' || ch === '[') {
      const open = ch;
      const close = ch === '{' ? '}' : ']';
      let depth = 0, j = i, inStr = false, esc = false;
      while (j < html.length) {
        const c = html[j];
        if (inStr) {
          if (esc) esc = false;
          else if (c === '\\') esc = true;
          else if (c === '"') inStr = false;
        } else {
          if (c === '"') inStr = true;
          else if (c === open) depth++;
          else if (c === close) { depth--; if (depth === 0) { j++; break; } }
        }
        j++;
      }
      const text = html.slice(i, j);
      i = j;
      return JSON.parse(text);
    }

    // strings
    if (ch === '"' || ch === "'") {
      const quote = ch;
      let j = i + 1, esc = false;
      while (j < html.length) {
        const c = html[j];
        if (esc) esc = false;
        else if (c === '\\') esc = true;
        else if (c === quote) { j++; break; }
        j++;
      }
      const text = html.slice(i, j);
      i = j;
      return JSON.parse(text);
    }

    // literales (número/true/false/null) hasta , o )
    let j = i;
    while (j < html.length && !/[,\)]/.test(html[j])) j++;
    const raw = html.slice(i, j).trim();
    i = j;
    if (raw === 'null') return null;
    if (raw === 'true') return true;
    if (raw === 'false') return false;
    const num = Number(raw);
    if (!Number.isNaN(num)) return num;
    return raw;
  }

  while (args.length < 4) args.push(readArg());
  return args; // [serie, translations, teams, months]
}

// ---- Normalización de home ----
function normalize(data) {
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

  const pctNum = v => (typeof v === 'string' ? Number(v) : Number(v || 0));
  pkg.standings.sort((a, b) => pctNum(b.pct) - pctNum(a.pct));

  const byDate = (a, b) => new Date(a.date) - new Date(b.date);
  pkg.games.today.sort(byDate);
  pkg.games.upcoming.sort(byDate);
  pkg.games.previous.sort(byDate);

  return pkg;
}

// ---- NUEVO: POST del calendario por mes/página ----
async function fetchCalendarPage(url, { month, page = 1, team_id = '' }) {
  const body = new URLSearchParams({
    month,
    page: String(page),
    team_id: String(team_id || '')
  }).toString();

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'user-agent': 'scrape-lidom-bot/1.0 (+github actions)',
      'accept': 'application/json, text/javascript, */*; q=0.01',
      'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
      'x-requested-with': 'XMLHttpRequest'
    },
    body
  });
  if (!res.ok) throw new Error(`POST ${url} -> ${res.status}`);

  const pageCount = Number(res.headers.get('X-Pagination-Page-Count') || '1') || 1;
  const data = await res.json(); // array de juegos
  return { data, pageCount };
}

// ---- NUEVO: trae y concatena TODO el calendario disponible ----
async function fetchFullCalendarForLeague(seo_url) {
  const calendarUrl = new URL(`/liga/${seo_url}/calendario`, SOURCE_HOME).href;
  const html = await fetchText(calendarUrl);

  const [seriesObj, _translations, _teams, months] = extractViewModelArgs(html);

  const all = [];
  for (const m of months || []) {
    const monthKey = m.date; // "10-2025"
    let page = 1;
    let pageCount = 1;
    do {
      const { data, pageCount: pc } = await fetchCalendarPage(calendarUrl, { month: monthKey, page });
      pageCount = pc;
      if (Array.isArray(data)) all.push(...data);
      page++;
    } while (page <= pageCount);
  }

  // De-dupe por id o por clave derivada
  const map = new Map();
  const kf = g => g?.id != null ? `id:${g.id}` : `key:${g?.date}|${g?.awayTeam?.id ?? '?'}@${g?.homeTeam?.id ?? '?'}`;
  for (const g of all) map.set(kf(g), g);

  const games = Array.from(map.values());
  games.sort((a, b) => new Date(a.date) - new Date(b.date));
  return { games, months, calendarUrl, seriesObj };
}

// ---- NUEVO: indexa por YYYY-MM-DD ----
function indexByDate(games) {
  const out = {};
  for (const g of games) {
    const d = (g.date || '').slice(0, 10);
    if (!d) continue;
    (out[d] ||= []).push(g);
  }
  for (const k of Object.keys(out)) out[k].sort((a, b) => new Date(a.date) - new Date(b.date));
  return out;
}

// ---- Main ----
async function main() {
  // 1) Home para standings + ventana corta
  const html = await fetchText(SOURCE_HOME);
  const firstArgText = extractFirstArgOfViewModel(html);
  const data = JSON.parse(firstArgText); // payload JSON-like
  const out = normalize(data);

  // 2) Calendario completo por meses/páginas (si existe seo_url)
  if (out?.league?.seo_url) {
    try {
      const full = await fetchFullCalendarForLeague(out.league.seo_url);
      const allGames = full.games;

      // reconstruir hoy / próximos / previos de forma robusta
      const now = new Date();
      const todayYMD = now.toISOString().slice(0, 10);
      const startOfToday = new Date(`${todayYMD}T00:00:00`);
      const isToday = g => (g.date || '').startsWith(todayYMD);
      const isFuture = g => new Date(g.date) > now && !isToday(g);
      const isPast = g => new Date(g.date) < startOfToday;

      out.games = {
        today: allGames.filter(isToday),
        upcoming: allGames.filter(isFuture).slice(0, 200),
        previous: allGames.filter(isPast).slice(-200)
      };

      // Añadidos útiles para el consumidor
      out.by_date = indexByDate(allGames);
      out.calendar_days = Object.keys(out.by_date).sort();
    } catch (e) {
      console.warn('Full calendar fetch failed, keeping narrow window:', e.message);
    }
  }

  // 3) Persistencia (solo si cambió)
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.mkdirSync(HISTORY_DIR, { recursive: true });

  const tmpPath = path.join(OUT_DIR, 'latest.tmp.json');
  fs.writeFileSync(tmpPath, JSON.stringify(out, null, 2) + '\n', 'utf8');

  const old = fs.existsSync(LATEST_PATH) ? fs.readFileSync(LATEST_PATH, 'utf8') : null;
  const neu = fs.readFileSync(tmpPath, 'utf8');

  if (old !== neu) {
    fs.renameSync(tmpPath, LATEST_PATH);
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
