// scraper.mjs
// Node 20+, sin dependencias externas

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));

const OUT_DIR = path.join(__dirname, 'docs');
const HISTORY_DIR = path.join(OUT_DIR, 'history');

const TZ = process.env.TZ || 'America/Santo_Domingo';
const SOURCE_HOME = (process.env.SOURCE_HOME || 'https://pelotainvernal.com/').replace(/\/+$/, '') + '/';
const LEAGUE_SEO = process.env.LEAGUE_SEO || 'dominicana-lidom';

// Páginas a raspar (en orden). Puedes agregar más si hace falta.
const PAGES = [
  '', // home
  `liga/${LEAGUE_SEO}`,
  `liga/${LEAGUE_SEO}/resultados`,
  `liga/${LEAGUE_SEO}/calendario`,
  `liga/${LEAGUE_SEO}/tabla-de-posiciones`,
];

const FETCH_DETAILS = (process.env.FETCH_DETAILS || 'false').toLowerCase() === 'true';
const DETAILS_LIMIT = Number(process.env.DETAILS_LIMIT || 20); // para no abusar

// ------------------------------ Utils generales ------------------------------

function nowISO() {
  return new Date().toISOString();
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

async function fetchText(url, tries = 3) {
  const hdrs = {
    'user-agent': 'scrape-lidom-bot/1.1 (+github actions)',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'accept-language': 'es-DO,es;q=0.9,en;q=0.8',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
  };

  let lastErr;
  for (let k = 0; k < tries; k++) {
    try {
      const res = await fetch(url, { headers: hdrs, redirect: 'follow' });
      if (!res.ok) throw new Error(`GET ${url} -> ${res.status}`);
      return await res.text();
    } catch (err) {
      lastErr = err;
      await sleep(500 * (k + 1));
    }
  }
  throw lastErr;
}

// ------------------------------ Parser ViewModel ------------------------------
//
// Las páginas de Pelota Invernal usan Knockout. Hemos visto dos firmas:
//   1) Home:    ko.applyBindings(new ViewModel([ <series>, <ads>, <i18n> ]));
//   2) Otras:   ko.applyBindings(new ViewModel(<series>, <i18n>));
//
// Necesitamos extraer SIEMPRE el *primer argumento* de ese new ViewModel(...).
// El primer argumento puede ser un Array (home) o un Object (otras).
//

function extractFirstArgOfViewModel(html) {
  const marker = 'ko.applyBindings(new ViewModel(';
  const idx = html.indexOf(marker);
  if (idx === -1) throw new Error('ViewModel call not found');

  let i = idx + marker.length;
  while (/\s/.test(html[i])) i++; // espacios

  const opener = html[i];
  const closer = opener === '[' ? ']' : opener === '{' ? '}' : null;
  if (!closer) throw new Error('Unexpected first-argument opener: ' + opener);

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
        inStr = true; j++; continue;
      }
      if (ch === opener) depth++;
      if (ch === closer) {
        depth--;
        if (depth === 0) {
          return html.slice(i, j + 1);
        }
      }
    }
    j++;
  }
  throw new Error('Unbalanced JSON while parsing first arg');
}

function safeParse(jsonText, urlHint = '') {
  try {
    return JSON.parse(jsonText);
  } catch (e) {
    const snippet = jsonText.slice(0, 200);
    throw new Error(`JSON parse error (${urlHint}): ${e.message}\nSnippet: ${snippet}`);
  }
}

// Detección de shape y “normalización” mínima a un objeto tipo serie
// - Si llega array (home), tomamos data[0] como “serie compuesta”
// - Si llega object (otras), usamos ese object directo
function extractSeriesShape(arg) {
  if (Array.isArray(arg)) {
    // Home suele venir: [ { league, standings, todayGames, nearestGames/upcomingGames, previousGames/previousRoundGames }, ADS, I18N ]
    // Tomamos el primer elemento con pinta de serie
    const firstObj = arg.find(x => x && typeof x === 'object' && (x.league || x.todayGames || x.upcomingGames || x.previousGames));
    return firstObj || {};
  }
  // Otras páginas: objeto directo
  return arg || {};
}

// ------------------------------ Normalización/Acopio ------------------------------

// Limpieza básica de un juego
function cleanGame(g) {
  if (!g || typeof g !== 'object') return null;
  const date = g.date || g.datetime || null;
  const id = g.id ?? null;

  const teamShape = t => {
    if (!t) return null;
    return {
      id: t.id ?? null,
      name: t.name ?? null,
      abbreviation: t.abbreviation ?? null,
      logo: t.logo ?? null,
      permalink: t.permalink ?? null,
      color: t.color ?? null,
      runs: t.runs ?? null,
      hits: t.hits ?? null,
      errors: t.errors ?? null,
      pitcher: t.pitcher ?? null,
      era: t.era ?? null,
      record: t.record ?? null,
      debut: t.debut ?? null,
      runs_half: t.runs_half ?? null,
    };
  };

  return {
    id,
    season_id: g.season_id ?? null,
    status: g.status ?? null,
    date,
    part: g.part ?? null,
    roundText: g.roundText ?? null,
    currentInningNum: g.currentInningNum ?? null,
    battingTeam: g.battingTeam ?? null,
    base: g.base ?? null,
    balls: g.balls ?? null,
    strikes: g.strikes ?? null,
    outs: g.outs ?? null,
    moneyline_g: g.moneyline_g ?? null,
    moneyline_h: g.moneyline_h ?? null,
    handicap_g: g.handicap_g ?? null,
    handicap_h: g.handicap_h ?? null,
    over_under: g.over_under ?? null,
    lastPlayByPlay: g.lastPlayByPlay ?? null,
    atBat: g.atBat ?? null,
    comment: g.comment ?? null,
    winningPitcher: g.winningPitcher ?? null,
    losingPitcher: g.losingPitcher ?? null,
    savingPitcher: g.savingPitcher ?? null,
    adImage: g.adImage ?? null,
    adLink: g.adLink ?? null,
    banner: g.banner ?? null,
    gameAlert: g.gameAlert ?? null,
    awayTeam: teamShape(g.awayTeam),
    homeTeam: teamShape(g.homeTeam),
  };
}

function pctNum(v) {
  if (v == null) return 0;
  if (typeof v === 'string') return Number(v.replace(',', '.')) || 0;
  return Number(v) || 0;
}

function sortByDateAsc(a, b) {
  const da = new Date(a.date || 0).getTime();
  const db = new Date(b.date || 0).getTime();
  return da - db;
}

// Funde standings (elige el más “completo” y ordenado)
function mergeStandings(cur = [], incoming = []) {
  const best = incoming.length > cur.length ? incoming : cur;
  const sorted = [...best].sort((a, b) => pctNum(b.pct) - pctNum(a.pct));
  return sorted;
}

// Index por fecha: YYYY-MM-DD -> juegos[]
function indexByDate(gamesArr) {
  const byDate = {};
  for (const g of gamesArr) {
    if (!g?.date) continue;
    const d = new Date(g.date);
    if (isNaN(+d)) continue;
    const key = d.toISOString().slice(0, 10);
    (byDate[key] = byDate[key] || []).push(g);
  }
  for (const k of Object.keys(byDate)) {
    byDate[k].sort(sortByDateAsc);
  }
  return byDate;
}

// ------------------------------ Raspar páginas y unificar ------------------------------

async function parseSeriesFromURL(url) {
  const html = await fetchText(url);
  const argText = extractFirstArgOfViewModel(html);
  const arg = safeParse(argText, url);
  const series = extractSeriesShape(arg);
  return series;
}

function collectFromSeries(series, acc) {
  // league / metadatos
  if (series.league && !acc.league) {
    acc.league = {
      id: series.league.id ?? null,
      name: series.league.name ?? null,
      seo_url: series.league.seo_url ?? null,
      season_id: series.league.season_id ?? null,
      round_name: series.league.round_name ?? null,
      date_start: series.league.date_start ?? null,
      date_end: series.league.date_end ?? null,
    };
  }

  // standings si trae
  if (Array.isArray(series.standings) && series.standings.length) {
    const cleaned = series.standings.map(s => ({
      team: {
        id: s.team?.id ?? null,
        name: s.team?.name ?? null,
        abbreviation: s.team?.abbreviation ?? null,
        logo: s.team?.logo ?? null,
        permalink: s.team?.permalink ?? null,
        color: s.team?.color ?? null,
      },
      wins: s.wins ?? 0,
      loses: s.loses ?? 0,
      gb: s.gb ?? null,
      pct: s.pct ?? null,
      wins_home: s.wins_home ?? null,
      loses_home: s.loses_home ?? null,
      wins_visitor: s.wins_visitor ?? null,
      loses_visitor: s.loses_visitor ?? null,
    }));
    acc.standings = mergeStandings(acc.standings, cleaned);
  }

  // posibles claves de juegos en diferentes páginas
  const GAME_KEYS = [
    'todayGames',
    'nearestGames',
    'upcomingGames',
    'previousGames',
    'previousRoundGames',
    'games',          // por si alguna variante usa `games`
  ];

  for (const key of GAME_KEYS) {
    const arr = series[key];
    if (!Array.isArray(arr)) continue;
    for (const raw of arr) {
      const g = cleanGame(raw);
      if (!g) continue;
      const keyId = g.id ?? `${g.date}::${g?.awayTeam?.id ?? '?'}@${g?.homeTeam?.id ?? '?'}`;
      // desduplicar (preferimos el que tenga más info)
      const prev = acc.mapGames.get(keyId);
      if (!prev) {
        acc.mapGames.set(keyId, g);
      } else {
        // merge superficial prefiriendo campos no-nulos del nuevo
        const merged = { ...prev, ...Object.fromEntries(Object.entries(g).filter(([, v]) => v != null)) };
        // merge teams superficial
        merged.awayTeam = { ...(prev.awayTeam || {}), ...(g.awayTeam || {}) };
        merged.homeTeam = { ...(prev.homeTeam || {}), ...(g.homeTeam || {}) };
        acc.mapGames.set(keyId, merged);
      }
    }
  }
}

async function maybeEnrichGameDetails(acc) {
  if (!FETCH_DETAILS) return;
  const games = [...acc.mapGames.values()].sort(sortByDateAsc);
  const subset = games.slice(0, DETAILS_LIMIT);

  for (const g of subset) {
    if (!g.id) continue;
    const url = SOURCE_HOME + String(g.id);
    try {
      const html = await fetchText(url, 2);
      // En páginas de detalle deben aplicar también ko.applyBindings(new ViewModel(...))
      const argText = extractFirstArgOfViewModel(html);
      const arg = safeParse(argText, url);
      const series = extractSeriesShape(arg);

      // A veces el detalle viene como un solo juego "game" o arrays similares:
      const candidate =
        series?.game ||
        (Array.isArray(series?.todayGames) && series.todayGames.find(x => (x?.id ?? null) === g.id)) ||
        (Array.isArray(series?.previousGames) && series.previousGames.find(x => (x?.id ?? null) === g.id)) ||
        null;

      if (candidate) {
        const det = cleanGame(candidate);
        const merged = { ...g, ...Object.fromEntries(Object.entries(det).filter(([, v]) => v != null)) };
        merged.awayTeam = { ...(g.awayTeam || {}), ...(det.awayTeam || {}) };
        merged.homeTeam = { ...(g.homeTeam || {}), ...(det.homeTeam || {}) };
        // update in map
        const keyId = g.id ?? `${g.date}::${g?.awayTeam?.id ?? '?'}@${g?.homeTeam?.id ?? '?'}`;
        acc.mapGames.set(keyId, merged);
      }
    } catch {
      // Silencioso: si falla detalle, seguimos
    }
  }
}

// ------------------------------ Salida final ------------------------------

function buildOutput(acc) {
  const allGames = [...acc.mapGames.values()].sort(sortByDateAsc);

  // “ventanas” aproximadas para compat retro
  const now = Date.now();
  const startOfToday = new Date(new Date().toISOString().slice(0, 10)).getTime();
  const endOfToday = startOfToday + 24 * 60 * 60 * 1000 - 1;

  const isToday = g => {
    const t = new Date(g.date || 0).getTime();
    return t >= startOfToday && t <= endOfToday;
  };
  const isFuture = g => new Date(g.date || 0).getTime() > endOfToday;
  const isPast = g => new Date(g.date || 0).getTime() < startOfToday;

  const today = allGames.filter(isToday);
  const upcoming = allGames.filter(isFuture);
  const previous = allGames.filter(isPast);

  return {
    source: {
      homepage: SOURCE_HOME,
      scraped_at: nowISO(),
      tz: TZ,
    },
    league: acc.league || {
      id: null, name: null, seo_url: LEAGUE_SEO, season_id: null,
      round_name: null, date_start: null, date_end: null
    },
    standings: acc.standings || [],
    games: {
      today,
      upcoming,
      previous,
    },
    by_date: indexByDate(allGames),
  };
}

// ------------------------------ Main ------------------------------

async function main() {
  const acc = {
    league: null,
    standings: [],
    mapGames: new Map(),
  };

  // 1) Recoger series de varias páginas
  for (const rel of PAGES) {
    const url = SOURCE_HOME + rel.replace(/^\//, '');
    try {
      const series = await parseSeriesFromURL(url);
      collectFromSeries(series, acc);
    } catch (e) {
      console.warn(`WARN: no se pudo parsear ${url}: ${e.message}`);
    }
  }

  // 2) (Opcional) enriquecer con páginas de detalle /<id>
  await maybeEnrichGameDetails(acc);

  // 3) Ensamblar salida
  const out = buildOutput(acc);

  // 4) Persistir (solo si cambia)
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.mkdirSync(HISTORY_DIR, { recursive: true });

  const latestPath = path.join(OUT_DIR, 'latest.json');
  const tmpPath = path.join(OUT_DIR, 'latest.tmp.json');

  fs.writeFileSync(tmpPath, JSON.stringify(out, null, 2) + '\n', 'utf8');

  const old = fs.existsSync(latestPath) ? fs.readFileSync(latestPath, 'utf8') : null;
  const neu = fs.readFileSync(tmpPath, 'utf8');

  if (old !== neu) {
    fs.renameSync(tmpPath, latestPath);

    const iso = nowISO().replace(/[:]/g, '').replace(/\..+/, 'Z');
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
