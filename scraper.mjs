// scraper.mjs
// Node 20+ (usa fetch nativo)

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, 'docs');
const HISTORY_DIR = path.join(OUT_DIR, 'history');
const GAMES_DIR = path.join(OUT_DIR, 'games');
const LATEST_PATH = path.join(OUT_DIR, 'latest.json');

const SOURCE_HOME = process.env.SOURCE_HOME || 'https://pelotainvernal.com/';
const TZ = process.env.TZ || 'America/Santo_Domingo';

// Control de detalles
const DETAILS_FETCH = (process.env.DETAILS_FETCH || 'recent').toLowerCase(); // recent | all | none
const DETAILS_DAYS = Number(process.env.DETAILS_DAYS || 14);
const DETAILS_CONCURRENCY = Math.max(1, Number(process.env.DETAILS_CONCURRENCY || 4));

// ---------- Utils ----------
function nowISO() {
  return new Date().toISOString();
}

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      'user-agent': 'scrape-lidom-bot/1.2 (+github actions)',
      'accept': 'text/html,application/xhtml+xml',
    },
  });
  if (!res.ok) throw new Error(`GET ${url} -> ${res.status}`);
  return res.text();
}

async function fetchJsonPOST(url, bodyObj) {
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'user-agent': 'scrape-lidom-bot/1.2 (+github actions)',
      'accept': 'application/json, text/javascript, */*; q=0.01',
      'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
      'x-requested-with': 'XMLHttpRequest',
    },
    body: new URLSearchParams(bodyObj).toString(),
  });
  if (!res.ok) throw new Error(`POST ${url} -> ${res.status}`);
  const pageCount = Number(res.headers.get('X-Pagination-Page-Count') || '1') || 1;
  const data = await res.json();
  return { data, pageCount };
}

function formatYMDInTZ(date, tz) {
  return new Intl.DateTimeFormat('en-CA', { timeZone: tz, year: 'numeric', month: '2-digit', day: '2-digit' }).format(date);
}

function gameYMD(g) {
  return (g?.date ?? '').slice(0, 10);
}

function sortByDateStrAsc(a, b) {
  const da = gameYMD(a), db = gameYMD(b);
  if (da && db && da !== db) return da < db ? -1 : 1;
  return new Date(a.date) - new Date(b.date);
}

function dedupeGames(list) {
  const map = new Map();
  const key = g => (g?.id != null) ? `id:${g.id}` : `key:${g?.date}|${g?.awayTeam?.id ?? '?'}@${g?.homeTeam?.id ?? '?'}`;
  for (const g of list) map.set(key(g), g);
  return Array.from(map.values());
}

function indexByDate(games) {
  const out = {};
  for (const g of games) {
    const d = gameYMD(g);
    if (!d) continue;
    (out[d] ||= []).push(g);
  }
  for (const d of Object.keys(out)) out[d].sort(sortByDateStrAsc);
  return out;
}

function daysBetweenYMD(a, b) {
  const [ay, am, ad] = a.split('-').map(Number);
  const [by, bm, bd] = b.split('-').map(Number);
  const da = Date.UTC(ay, am - 1, ad);
  const db = Date.UTC(by, bm - 1, bd);
  return Math.round((da - db) / 86400000);
}

// ---------- Parser genérico de new ViewModel(...) ----------
function extractAllNewViewModelArgs(html) {
  const marker = 'new ViewModel(';
  const results = [];
  let start = html.indexOf(marker);
  while (start !== -1) {
    let i = start + marker.length;
    const args = [];

    function readArg() {
      while (/\s|,/.test(html[i])) i++;
      const ch = html[i];

      if (ch === '{' || ch === '[') {
        const open = ch, close = ch === '{' ? '}' : ']';
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
            else if (c === ')' && depth === 0) break;
          }
          j++;
        }
        const text = html.slice(i, j);
        i = j;
        return JSON.parse(text);
      }

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

    // Leer hasta ')'
    while (true) {
      const peek = html.slice(i).trimStart();
      if (peek[0] === ')') { i += html.slice(i).indexOf(')') + 1; break; }
      args.push(readArg());
      if (html[i] === ',') i++;
    }

    results.push(args);
    start = html.indexOf(marker, i);
  }
  return results;
}

function extractFirstNewViewModelArgs(html) {
  const all = extractAllNewViewModelArgs(html);
  if (!all.length) throw new Error('new ViewModel(...) not found');
  // Preferimos la invocación con más argumentos por seguridad
  all.sort((a, b) => b.length - a.length);
  return all[0];
}

// ---------- Normalización HOME ----------
function normalizeHomePayload(series) {
  // series: objeto principal pasado como 1er arg al ViewModel de la home (si existe)
  const league = series?.league ?? {};
  const pkg = {
    source: { homepage: SOURCE_HOME, scraped_at: nowISO(), tz: TZ },
    league: {
      id: league.id ?? series?.id ?? null,
      name: league.name ?? series?.name ?? null,
      seo_url: league.seo_url ?? series?.seo_url ?? null,
      season_id: league.season_id ?? series?.season_id ?? null,
      round_name: league.round_name ?? series?.round_name ?? null,
      date_start: league.date_start ?? series?.date_start ?? null,
      date_end: league.date_end ?? series?.date_end ?? null,
    },
    standings: [],
    games: { today: [], upcoming: [], previous: [] },
  };
  return pkg;
}

// ---------- Calendario y Resultados ----------
async function fetchFullCalendarForLeague(seo_url) {
  const calendarUrl = new URL(`/liga/${seo_url}/calendario`, SOURCE_HOME).href;
  const html = await fetchText(calendarUrl);
  const args = extractFirstNewViewModelArgs(html);
  // esperado: [seriesObj, translations, teams, months]
  const months = args[3] || [];

  const all = [];
  for (const m of months || []) {
    const monthKey = m.date; // "10-2025"
    let page = 1, pageCount = 1;
    do {
      const { data, pageCount: pc } = await fetchJsonPOST(calendarUrl, { month: monthKey, page, team_id: '' });
      pageCount = pc;
      if (Array.isArray(data)) all.push(...data);
      page++;
    } while (page <= pageCount);
  }

  const games = dedupeGames(all).sort(sortByDateStrAsc);
  return { games, months, calendarUrl };
}

async function fetchFullResultsForLeague(seo_url) {
  const resultsUrl = new URL(`/liga/${seo_url}/resultados`, SOURCE_HOME).href;
  let page = 1, pageCount = 1;
  const all = [];
  do {
    const { data, pageCount: pc } = await fetchJsonPOST(resultsUrl, { page });
    pageCount = pc;
    if (Array.isArray(data)) all.push(...data);
    page++;
  } while (page <= pageCount);

  const games = dedupeGames(all).sort(sortByDateStrAsc);
  return { games, pageCount, resultsUrl };
}

// ---------- Helpers de detalle ----------
function buildLineScore(inningsArr = [], awayTotals, homeTotals) {
  // inningsArr: [{num, awayTeamRuns, homeTeamRuns}, ...]
  const innings = [];
  let maxNum = 0;
  for (const inn of inningsArr) {
    const num = Number(inn.num);
    if (!num) continue;
    maxNum = Math.max(maxNum, num);
  }
  // Ordenar por num asc
  const sorted = [...inningsArr].sort((a, b) => a.num - b.num);
  for (const inn of sorted) {
    innings.push({
      num: inn.num,
      away: typeof inn.awayTeamRuns === 'number' ? inn.awayTeamRuns : null,
      home: typeof inn.homeTeamRuns === 'number' ? inn.homeTeamRuns : null,
    });
  }
  const totals = {
    away: { R: awayTotals?.runs ?? null, H: awayTotals?.hits ?? null, E: awayTotals?.errors ?? null },
    home: { R: homeTotals?.runs ?? null, H: homeTotals?.hits ?? null, E: homeTotals?.errors ?? null },
  };
  return {
    innings,
    extras: maxNum > 9,
    totals,
  };
}

function parseInningLabel(label = '') {
  // Ejemplos: "Alta 9na", "Baja 4ta", "Alta 10ma"
  const m = label.match(/^\s*(Alta|Baja)\s+(\d+)\s*(?:ra|da|ta|ma|va|na)?/i);
  if (!m) return { half: null, num: null, label };
  const half = m[1].toLowerCase().startsWith('alta') ? 'top' : 'bottom';
  const num = Number(m[2]);
  return { half, num, label };
}

function groupPlayByPlay(logs = []) {
  // logs: [{ inning: "Baja 4ta", play_by_play: [{reference_id, message, scored, is_primary, date}, ...] }, ...]
  // Los logs suelen venir en orden descendente (lo último primero). Normalizamos 1..N asc, top antes de bottom.
  const bucket = new Map(); // key "num-top" / "num-bottom"
  for (const blk of logs) {
    const meta = parseInningLabel(blk.inning);
    const key = `${meta.num}-${meta.half}`;
    if (!bucket.has(key)) {
      bucket.set(key, { num: meta.num, half: meta.half, label: meta.label, plays: [] });
    }
    for (const p of blk.play_by_play || []) {
      bucket.get(key).plays.push({
        reference_id: p.reference_id,
        text: p.message,
        scored: p.scored === 1,
        is_primary: !!p.is_primary,
        ts: p.date ?? null,
      });
    }
  }
  // Orden: num asc, top antes que bottom
  const order = Array.from(bucket.values()).sort((a, b) => {
    if (a.num !== b.num) return a.num - b.num;
    const h = (x) => (x.half === 'top' ? 0 : 1);
    return h(a) - h(b);
  });
  return order;
}

// ---------- Detalle de Juego ----------
function normalizeGameDetails(rawGame, rawLogs, siblingGames) {
  const awayT = rawGame.awayTeam || {};
  const homeT = rawGame.homeTeam || {};

  // Linescore (carreras por inning + totales R/H/E)
  const linescore = buildLineScore(rawGame.innings || [], { runs: awayT.runs, hits: awayT.hits, errors: awayT.errors },
                                                    { runs: homeT.runs, hits: homeT.hits, errors: homeT.errors });

  // Play-by-play agrupado por inning/mitad
  const plays_by_inning = groupPlayByPlay(rawLogs || []);

  // Estructura final de detalle
  return {
    id: rawGame.id,
    season_id: rawGame.season_id ?? null,
    status: rawGame.status,
    date: rawGame.date,
    roundText: rawGame.roundText ?? null,
    currentInningNum: rawGame.currentInningNum ?? null,
    lastPlayByPlay: rawGame.lastPlayByPlay ?? null,
    counts: { balls: rawGame.balls ?? null, strikes: rawGame.strikes ?? null, outs: rawGame.outs ?? null },
    betting: {
      moneyline: { away: rawGame.moneyline_g ?? null, home: rawGame.moneyline_h ?? null },
      handicap: { away: rawGame.handicap_g ?? null, home: rawGame.handicap_h ?? null },
      over_under: rawGame.over_under ?? null,
    },
    pitchers: {
      winning: rawGame.winningPitcher ?? null,
      losing: rawGame.losingPitcher ?? null,
      save: rawGame.savingPitcher ?? null,
    },
    teams: {
      away: {
        id: awayT.id, name: awayT.name, abbr: awayT.abbreviation, color: awayT.color, logo: awayT.logo, permalink: awayT.permalink,
        totals: { R: awayT.runs ?? null, H: awayT.hits ?? null, E: awayT.errors ?? null, H2: awayT.runs_half ?? null },
        probables: { pitcher: awayT.pitcher ?? null, era: awayT.era ?? null, record: awayT.record ?? null, debut: !!awayT.debut },
        players: Array.isArray(awayT.players) ? awayT.players : [],
      },
      home: {
        id: homeT.id, name: homeT.name, abbr: homeT.abbreviation, color: homeT.color, logo: homeT.logo, permalink: homeT.permalink,
        totals: { R: homeT.runs ?? null, H: homeT.hits ?? null, E: homeT.errors ?? null, H2: homeT.runs_half ?? null },
        probables: { pitcher: homeT.pitcher ?? null, era: homeT.era ?? null, record: homeT.record ?? null, debut: !!homeT.debut },
        players: Array.isArray(homeT.players) ? homeT.players : [],
      },
    },
    base_state: { battingTeam: rawGame.battingTeam ?? null, base: rawGame.base ?? null },
    comment: rawGame.comment ?? '',
    linescore,
    plays_by_inning,
    // Conservamos el arreglo "innings" tal como viene por si lo quieres usar crudo:
    innings_raw: Array.isArray(rawGame.innings) ? rawGame.innings.map(i => ({
      num: i.num, part: i.part, is_last: i.is_last,
      awayTeamRuns: i.awayTeamRuns, homeTeamRuns: i.homeTeamRuns,
    })) : [],
    // Otros juegos del día (para navegación)
    siblingGames: Array.isArray(siblingGames) ? siblingGames.map(s => ({
      id: s.id,
      date: s.date,
      status: s.status,
      roundText: s.roundText ?? null,
      awayTeam: s.awayTeam ? {
        id: s.awayTeam.id, name: s.awayTeam.name, abbr: s.awayTeam.abbreviation,
        logo: s.awayTeam.logo, runs: s.awayTeam.runs ?? null, hits: s.awayTeam.hits ?? null, errors: s.awayTeam.errors ?? null,
        permalink: s.awayTeam.permalink ?? null
      } : null,
      homeTeam: s.homeTeam ? {
        id: s.homeTeam.id, name: s.homeTeam.name, abbr: s.homeTeam.abbreviation,
        logo: s.homeTeam.logo, runs: s.homeTeam.runs ?? null, hits: s.homeTeam.hits ?? null, errors: s.homeTeam.errors ?? null,
        permalink: s.homeTeam.permalink ?? null
      } : null,
    })) : [],
  };
}

async function fetchGameDetails(gameId) {
  const url = new URL(`/${gameId}`, SOURCE_HOME).href;
  const html = await fetchText(url);
  // Detalle: new ViewModel(gameData, siblingGames, logs, translations)
  const args = extractFirstNewViewModelArgs(html);
  const gameData = args[0];
  const siblingGames = args[1] || [];
  const logs = args[2] || [];
  const normalized = normalizeGameDetails(gameData, logs, siblingGames);
  return { url, data: normalized };
}

// ---------- Selección de detalles ----------
function pickWhichDetailsToFetch(allGames) {
  if (DETAILS_FETCH === 'none') return [];
  const todayYMD = formatYMDInTZ(new Date(), TZ);
  if (DETAILS_FETCH === 'all') {
    return Array.from(new Set(allGames.map(g => g.id))).filter(Boolean);
  }
  // recent: +- DETAILS_DAYS
  const ids = [];
  for (const g of allGames) {
    const d = gameYMD(g);
    if (!d) continue;
    const diff = Math.abs(daysBetweenYMD(d, todayYMD));
    if (diff <= DETAILS_DAYS) ids.push(g.id);
  }
  return Array.from(new Set(ids)).filter(Boolean);
}

async function pLimit(concurrency, tasks) {
  const results = [];
  let i = 0, running = 0;
  return new Promise((resolve) => {
    const startNext = () => {
      if (i >= tasks.length && running === 0) return resolve(results);
      while (running < concurrency && i < tasks.length) {
        const idx = i++;
        running++;
        tasks[idx]()
          .then((res) => { results[idx] = res; })
          .catch((err) => { results[idx] = { error: err?.message || String(err) }; })
          .finally(() => { running--; startNext(); });
      }
    };
    startNext();
  });
}

// ---------- Main ----------
async function main() {
  // 0) Preparar carpetas
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.mkdirSync(HISTORY_DIR, { recursive: true });
  fs.mkdirSync(GAMES_DIR, { recursive: true });

  // 1) Intentar home (si existiera un ViewModel ahí)
  let out = {
    source: { homepage: SOURCE_HOME, scraped_at: nowISO(), tz: TZ },
    league: { id: null, name: null, seo_url: null, season_id: null, round_name: null, date_start: null, date_end: null },
    standings: [],
    games: { today: [], upcoming: [], previous: [] },
  };

  try {
    const homeHtml = await fetchText(SOURCE_HOME);
    const homeArgs = extractFirstNewViewModelArgs(homeHtml);
    out = normalizeHomePayload(homeArgs[0]);
  } catch {
    // Si la home no tiene ViewModel, seguimos con las rutas de liga directamente
    // (de todos modos completaremos out más abajo al fusionar calendario y resultados)
  }

  // 2) Calendario completo
  let calendarGames = [];
  if (out?.league?.seo_url) {
    try {
      const fullCal = await fetchFullCalendarForLeague(out.league.seo_url);
      calendarGames = fullCal.games;
    } catch (e) {
      console.warn('Full calendar fetch failed:', e.message);
    }
  } else {
    // fallback: intenta LIDOM por defecto
    try {
      const fullCal = await fetchFullCalendarForLeague('dominicana-lidom');
      out.league.seo_url = 'dominicana-lidom';
      calendarGames = fullCal.games;
    } catch (e) {
      console.warn('Fallback calendar fetch failed:', e.message);
    }
  }

  // 3) Resultados completos
  let resultsGames = [];
  if (out?.league?.seo_url) {
    try {
      const fullRes = await fetchFullResultsForLeague(out.league.seo_url);
      resultsGames = fullRes.games;
    } catch (e) {
      console.warn('Full results fetch failed:', e.message);
    }
  }

  // 4) Fusión, clasificaciones por fecha
  const allGames = dedupeGames([...calendarGames, ...resultsGames]).sort(sortByDateStrAsc);

  const todayYMD = formatYMDInTZ(new Date(), TZ);
  const isToday = g => gameYMD(g) === todayYMD;
  const isFuture = g => gameYMD(g) > todayYMD;
  const isPast   = g => gameYMD(g) < todayYMD;

  out.games = {
    today: allGames.filter(isToday),
    upcoming: allGames.filter(isFuture),
    previous: allGames.filter(isPast),
  };

  out.by_date = indexByDate(allGames);
  out.calendar_days = Object.keys(out.by_date).sort();

  // 5) Detalles de juegos (con linescore + PBP)
  const targetIds = pickWhichDetailsToFetch(allGames);

  const tasks = targetIds.map(id => async () => {
    try {
      const { url, data } = await fetchGameDetails(id);
      const filePath = path.join(GAMES_DIR, `${id}.json`);
      const payload = {
        source: { url, scraped_at: nowISO(), tz: TZ },
        game: data, // <-- incluye linescore & plays_by_inning
      };
      fs.writeFileSync(filePath, JSON.stringify(payload, null, 2) + '\n', 'utf8');
      return { id, ok: true, file: `games/${id}.json` };
    } catch (err) {
      return { id, ok: false, error: err?.message || String(err) };
    }
  });

  const detailResults = await pLimit(DETAILS_CONCURRENCY, tasks);

  // 6) Índice de detalles disponibles
  const files = {};
  for (const r of detailResults) {
    if (r?.ok && r?.file) files[r.id] = r.file;
  }
  out.details_index = {
    mode: DETAILS_FETCH,
    days_window: DETAILS_DAYS,
    concurrency: DETAILS_CONCURRENCY,
    count: Object.keys(files).length,
    files,
  };

  // 7) Persistencia con snapshot si cambió latest.json
  const tmpPath = path.join(OUT_DIR, 'latest.tmp.json');
  fs.writeFileSync(tmpPath, JSON.stringify(out, null, 2) + '\n', 'utf8');

  const old = fs.existsSync(LATEST_PATH) ? fs.readFileSync(LATEST_PATH, 'utf8') : null;
  const neu = fs.readFileSync(tmpPath, 'utf8');

  if (old !== neu) {
    fs.renameSync(tmpPath, LATEST_PATH);
    const iso = nowISO().replace(/[:]/g, '').replace(/\..+/, 'Z');
    const snapPath = path.join(HISTORY_DIR, `${iso}.json`);
    fs.writeFileSync(snapPath, neu, 'utf8');
    console.log(`Updated docs/latest.json (${out.calendar_days.length} days) and ${Object.keys(files).length} game details.`);
  } else {
    fs.unlinkSync(tmpPath);
    console.log('No content changes on latest.json.');
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
