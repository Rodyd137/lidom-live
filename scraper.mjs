// scraper.mjs
// Node 20+ (fetch nativo)

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

// Detalle: recent | all | none
const DETAILS_FETCH = (process.env.DETAILS_FETCH || 'recent').toLowerCase();
const DETAILS_DAYS = Number(process.env.DETAILS_DAYS || 14);
const DETAILS_CONCURRENCY = Math.max(1, Number(process.env.DETAILS_CONCURRENCY || 4));

// Estados del sitio
const STATUS = { NOT_STARTED:1, LIVE:2, PREVIEW:3, DELAYED:4, SUSPENDED:5, FINAL:6, POSTPONED:7 };
// Por defecto, solo bajamos detalles de juegos que suelen tener PBP/linescore listo
const DETAILS_STATUS_SET = new Set(
  (process.env.DETAILS_STATUSES || '2,4,5,6')  // LIVE, DELAYED, SUSPENDED, FINAL
    .split(',')
    .map(n => Number(n.trim()))
    .filter(Boolean)
);

/* ============ Utils ============ */
function nowISO() { return new Date().toISOString(); }

async function fetchText(url) {
  const res = await fetch(url, {
    headers: {
      'user-agent': 'lidom-scraper/1.3',
      'accept': 'text/html,application/xhtml+xml'
    }
  });
  if (!res.ok) throw new Error(`GET ${url} -> ${res.status}`);
  return res.text();
}

async function fetchJsonPOST(url, bodyObj) {
  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'user-agent': 'lidom-scraper/1.3',
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

function gameYMD(g) { return (g?.date ?? '').slice(0, 10); }

function sortByDateStrAsc(a, b) {
  const da = gameYMD(a), db = gameYMD(b);
  if (da && db && da !== db) return da < db ? -1 : 1;
  return new Date(a.date) - new Date(b.date);
}

function dedupeGames(list) {
  const map = new Map();
  const key = g => (g?.id != null) ? `id:${g.id}` : `key:${g?.date}|${g?.awayTeam?.id ?? '?'}@${g?.homeTeam?.id ?? '?'}`;
  for (const g of list) if (g) map.set(key(g), g);
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

function stripTags(s='') { return s.replace(/<[^>]*>/g, '').replace(/\s+/g,' ').trim(); }

/* ============ Parser de new ViewModel(...) robusto ============ */
function extractAllNewViewModelArgs(html) {
  const marker = 'new ViewModel(';
  const results = [];
  let pos = 0;
  while (true) {
    const start = html.indexOf(marker, pos);
    if (start === -1) break;
    let i = start + marker.length;
    const args = [];

    function readArg() {
      while (/\s|,/.test(html[i])) i++;
      const ch = html[i];

      // objetos/arrays JSON
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

      // literales
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

    while (true) {
      const ahead = html.slice(i).trimStart();
      if (ahead[0] === ')') { i += html.slice(i).indexOf(')') + 1; break; }
      args.push(readArg());
      if (html[i] === ',') i++;
    }

    results.push(args);
    pos = i;
  }
  return results;
}

function pickViewModelForDetails(allCalls) {
  // Detalle: 4 args: [gameData, siblingGames, logs, translations]
  for (const args of allCalls) {
    const g = args[0];
    if (g && typeof g === 'object' && (g.awayTeam && g.homeTeam)) return args;
  }
  // fallback: la de mayor número de args
  return allCalls.sort((a,b)=>b.length-a.length)[0];
}

function extractFirstNewViewModelArgs(html) {
  const all = extractAllNewViewModelArgs(html);
  if (!all.length) throw new Error('new ViewModel(...) not found');
  return pickViewModelForDetails(all);
}

/* ============ Normalizadores básicos (home) ============ */
function _firstSeries(seriesLike) {
  // HOME puede traer un objeto o un array [series]
  return Array.isArray(seriesLike) ? (seriesLike[0] || {}) : (seriesLike || {});
}

function normalizeHomePayload(seriesLike) {
  const series = _firstSeries(seriesLike);
  const league = series?.league ?? {};
  // standings si vienen en el home
  const standings = Array.isArray(series?.standings) ? series.standings : [];
  return {
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
    standings,
    games: { today: [], upcoming: [], previous: [] },
  };
}

// Unión de juegos del HOME: today/upcoming/previous y nearestGames (si existe)
function collectHomeGamesUnion(seriesLike) {
  const s = _firstSeries(seriesLike);
  const pool = [];
  const pushArr = (arr) => Array.isArray(arr) && pool.push(...arr);
  pushArr(s?.todayGames);
  pushArr(s?.upcomingGames);
  pushArr(s?.previousGames);
  pushArr(s?.nearestGames); // algunas plantillas lo traen
  return dedupeGames(pool).sort(sortByDateStrAsc);
}

/* ============ Calendario / Resultados ============ */
async function fetchFullCalendarForLeague(seo_url) {
  const calendarUrl = new URL(`/liga/${seo_url}/calendario`, SOURCE_HOME).href;
  const html = await fetchText(calendarUrl);
  const args = extractFirstNewViewModelArgs(html); // [series, translations, teams, months] (varía, pero el 4to suele ser months)
  const months = args[3] || [];

  const all = [];
  for (const m of months || []) {
    const monthKey = m.date;
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

/* ============ Linescore y PBP ============ */
function buildLineScore(inningsArr = [], awayTotals, homeTotals) {
  const sorted = [...inningsArr]
    .filter(i => i && typeof i.num !== 'undefined')
    .sort((a, b) => Number(a.num) - Number(b.num));

  const innings = sorted.map(inn => ({
    num: Number(inn.num),
    away: (typeof inn.awayTeamRuns === 'number') ? inn.awayTeamRuns : null,
    home: (typeof inn.homeTeamRuns === 'number') ? inn.homeTeamRuns : null,
  }));

  const maxNum = innings.reduce((m, i) => Math.max(m, i.num), 0);
  const totals = {
    away: { R: awayTotals?.runs ?? null, H: awayTotals?.hits ?? null, E: awayTotals?.errors ?? null },
    home: { R: homeTotals?.runs ?? null, H: homeTotals?.hits ?? null, E: homeTotals?.errors ?? null },
  };

  const cumulative = { away: [], home: [] };
  let ca = 0;
  for (const i of innings) { ca += i.away ?? 0; cumulative.away.push({ num: i.num, R: ca }); }
  let hb = 0;
  for (const i of innings) { hb += i.home ?? 0; cumulative.home.push({ num: i.num, R: hb }); }

  return { innings, extras: maxNum > 9, totals, cumulative };
}

function parseInningLabel(label = '') {
  const m = label.match(/^\s*(Alta|Baja)\s+(\d+)\s*(?:ra|da|ta|ma|va|na)?/i);
  if (!m) return { half: null, num: null, label };
  const half = m[1].toLowerCase().startsWith('alta') ? 'top' : 'bottom';
  const num = Number(m[2]);
  return { half, num, label };
}

function groupPlayByPlay(logs = []) {
  const bucket = new Map(); // key `${num}-${half}`
  for (const blk of logs) {
    const meta = parseInningLabel(blk.inning);
    const key = `${meta.num}-${meta.half}`;
    if (!bucket.has(key)) bucket.set(key, { num: meta.num, half: meta.half, label: meta.label, plays: [] });
    for (const p of blk.play_by_play || []) {
      bucket.get(key).plays.push({
        reference_id: p.reference_id,
        text: p.message,
        scored: p.scored === 1,
        is_primary: !!p.is_primary,
        ts: typeof p.date === 'number' ? p.date : null,
      });
    }
  }
  const order = Array.from(bucket.values()).sort((a, b) => {
    if (a.num !== b.num) return a.num - b.num;
    const ord = x => x.half === 'top' ? 0 : 1;
    return ord(a) - ord(b);
  });

  for (const half of order) {
    half.plays.sort((x, y) => {
      if (x.ts != null && y.ts != null && x.ts !== y.ts) return x.ts - y.ts;
      if (x.is_primary !== y.is_primary) return (x.is_primary ? -1 : 1);
      return 0;
    });
  }
  return order;
}

function summarizeInningPlays(playsByInning = []) {
  return playsByInning.map(h => ({
    num: h.num,
    half: h.half,
    label: h.label,
    total_plays: h.plays.length,
    scoring_plays: h.plays.filter(p => p.scored).length,
  }));
}

/* ============ Parseo de tablas de estadísticas (HTML) ============ */
function extractStatsTablesFromHTML(html) {
  const tables = [];
  const re = /<h4[^>]*>([\s\S]*?)<\/h4>\s*<table[^>]*class="stats-table"[^>]*>([\s\S]*?)<\/table>/gi;
  let m;
  while ((m = re.exec(html)) !== null) {
    const title = stripTags(m[1]);
    const tbody = m[2];

    const rn = /<tr[^>]*class="round-name"[^>]*>[\s\S]*?<th[^>]*colspan="[^"]*"[^>]*>([\s\S]*?)<\/th>[\s\S]*?<\/tr>/i.exec(tbody);
    const round = rn ? stripTags(rn[1]) : null;

    const headerMatch = /<tr>\s*<th[^>]*>[\s\S]*?<\/th>([\s\S]*?)<\/tr>/i.exec(tbody.replace(/<tr[^>]*class="round-name"[\s\S]*?<\/tr>/i,''));
    let columns = [];
    if (headerMatch) {
      columns = Array.from(headerMatch[1].matchAll(/<th[^>]*>([\s\S]*?)<\/th>/gi)).map(x => stripTags(x[1]));
    }

    const teamRows = [];
    const rowRe = /<tr>\s*<th[^>]*>([\s\S]*?)<\/th>([\s\S]*?)<\/tr>/gi;
    const tbodyNoRound = tbody.replace(/<thead>[\s\S]*?<\/thead>/gi, '').replace(/<\/?tbody>/gi,'');
    let row;
    while ((row = rowRe.exec(tbodyNoRound)) !== null) {
      const name = stripTags(row[1]);
      const tds = Array.from(row[2].matchAll(/<td[^>]*>([\s\S]*?)<\/td>/gi)).map(x => stripTags(x[1]));
      if (!tds.length) continue;
      const numbers = tds.map(v => {
        const n = Number(v.replace(',', '.'));
        return Number.isNaN(n) ? v : n;
      });
      teamRows.push({ team: name, values: numbers });
    }

    const teams = {};
    for (const r of teamRows) {
      const obj = {};
      for (let i = 0; i < Math.min(columns.length, r.values.length); i++) {
        obj[columns[i]] = r.values[i];
      }
      teams[r.team] = obj;
    }

    tables.push({ title, round, columns, teams });
  }
  return tables;
}

/* ============ Normalizador de Detalle ============ */
function normalizeGameDetails(rawGame, rawLogs, siblingGames, htmlForTables='') {
  const awayT = rawGame.awayTeam || {};
  const homeT = rawGame.homeTeam || {};

  const linescore = buildLineScore(
    rawGame.innings || [],
    { runs: awayT.runs, hits: awayT.hits, errors: awayT.errors },
    { runs: homeT.runs, hits: homeT.hits, errors: homeT.errors }
  );

  const plays_by_inning = groupPlayByPlay(rawLogs || []);
  const plays_summary = summarizeInningPlays(plays_by_inning);
  const stats_tables = extractStatsTablesFromHTML(htmlForTables);

  return {
    id: rawGame.id,
    season_id: rawGame.season_id ?? null,
    status: rawGame.status,
    date: rawGame.date,
    roundText: rawGame.roundText ?? null,
    currentInningNum: rawGame.currentInningNum ?? null,
    lastPlayByPlay: rawGame.lastPlayByPlay ?? null,
    counts: { balls: rawGame.balls ?? null, strikes: rawGame.strikes ?? null, outs: rawGame.outs ?? null },
    base_state: { battingTeam: rawGame.battingTeam ?? null, base: rawGame.base ?? null },
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
    comment: rawGame.comment ?? '',
    linescore,
    plays_by_inning,
    plays_summary_by_inning: plays_summary,
    innings_raw: Array.isArray(rawGame.innings) ? rawGame.innings.map(i => ({
      num: i.num, part: i.part, is_last: i.is_last,
      awayTeamRuns: i.awayTeamRuns, homeTeamRuns: i.homeTeamRuns,
    })) : [],
    siblingGames: Array.isArray(siblingGames) ? siblingGames.map(s => ({
      id: s.id, date: s.date, status: s.status, roundText: s.roundText ?? null,
      awayTeam: s.awayTeam ? { id: s.awayTeam.id, name: s.awayTeam.name, abbr: s.awayTeam.abbreviation, logo: s.awayTeam.logo, runs: s.awayTeam.runs ?? null, hits: s.awayTeam.hits ?? null, errors: s.awayTeam.errors ?? null, permalink: s.awayTeam.permalink ?? null } : null,
      homeTeam: s.homeTeam ? { id: s.homeTeam.id, name: s.homeTeam.name, abbr: s.homeTeam.abbreviation, logo: s.homeTeam.logo, runs: s.homeTeam.runs ?? null, hits: s.homeTeam.hits ?? null, errors: s.homeTeam.errors ?? null, permalink: s.homeTeam.permalink ?? null } : null,
    })) : [],
    stats_tables,
  };
}

/* ============ Detalles de juego ============ */
async function fetchGameDetails(gameId) {
  const url = new URL(`/${gameId}`, SOURCE_HOME).href;
  const html = await fetchText(url);
  // Detalle: new ViewModel(gameData, siblingGames, logs, translations)
  const args = extractFirstNewViewModelArgs(html);
  const gameData = args[0];
  const siblingGames = args[1] || [];
  const logs = args[2] || [];

  const normalized = normalizeGameDetails(gameData, logs, siblingGames, html);
  return { url, data: normalized };
}

/* ============ Selector de detalles a bajar ============ */
function pickWhichDetailsToFetch(sourceGames) {
  if (DETAILS_FETCH === 'none') return [];

  const todayYMD = formatYMDInTZ(new Date(), TZ);

  // Solo juegos con estados que suelen exponer detalle (PBP/linescore)
  const candidates = (sourceGames || []).filter(g => DETAILS_STATUS_SET.has(Number(g?.status)));

  if (DETAILS_FETCH === 'all') {
    return Array.from(new Set(candidates.map(g => g.id))).filter(Boolean);
  }

  // recent: ±DETAILS_DAYS desde hoy
  const ids = [];
  for (const g of candidates) {
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

/* ============ Main ============ */
async function main() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  fs.mkdirSync(HISTORY_DIR, { recursive: true });
  fs.mkdirSync(GAMES_DIR, { recursive: true });

  // Base del índice
  let out = {
    source: { homepage: SOURCE_HOME, scraped_at: nowISO(), tz: TZ },
    league: { id: null, name: null, seo_url: null, season_id: null, round_name: null, date_start: null, date_end: null },
    standings: [],
    games: { today: [], upcoming: [], previous: [] },
  };

  // HOME (puede venir objeto o array en el primer arg de ViewModel)
  let homeSeriesArg = null;
  let homeGames = [];
  try {
    const homeHtml = await fetchText(SOURCE_HOME);
    const homeArgs = extractFirstNewViewModelArgs(homeHtml);
    homeSeriesArg = homeArgs[0];                         // <- puede ser objeto o array [series]
    out = normalizeHomePayload(homeSeriesArg);           // <- ahora soporta array
    homeGames = collectHomeGamesUnion(homeSeriesArg);    // <- unión home (incluye LIVE aunque /resultados falle)
  } catch { /* ok */ }

  // Calendario
  let calendarGames = [];
  if (out?.league?.seo_url) {
    try {
      const fullCal = await fetchFullCalendarForLeague(out.league.seo_url);
      calendarGames = fullCal.games;
    } catch (e) { console.warn('Full calendar fetch failed:', e.message); }
  } else {
    try {
      const fullCal = await fetchFullCalendarForLeague('dominicana-lidom');
      out.league.seo_url = 'dominicana-lidom';
      calendarGames = fullCal.games;
    } catch (e) { console.warn('Fallback calendar fetch failed:', e.message); }
  }

  // Resultados
  let resultsGames = [];
  if (out?.league?.seo_url) {
    try {
      const fullRes = await fetchFullResultsForLeague(out.league.seo_url);
      resultsGames = fullRes.games;
    } catch (e) { console.warn('Full results fetch failed:', e.message); }
  }

  // Fusión (HOME union + calendario + resultados)
  const allGames = dedupeGames([...homeGames, ...calendarGames, ...resultsGames]).sort(sortByDateStrAsc);
  const todayYMD = formatYMDInTZ(new Date(), TZ);

  out.games = {
    today: allGames.filter(g => gameYMD(g) === todayYMD),
    upcoming: allGames.filter(g => gameYMD(g) >  todayYMD),
    previous: allGames.filter(g => gameYMD(g) <  todayYMD),
  };

  out.by_date = indexByDate(allGames);
  out.calendar_days = Object.keys(out.by_date).sort();

  // Detalles: preferimos SIEMPRE los id que vienen de /resultados; si no hay, usamos el total (que ya incluye HOME)
  const detailSourceGames = resultsGames.length ? resultsGames : allGames;
  const targetIds = pickWhichDetailsToFetch(detailSourceGames);

  const tasks = targetIds.map(id => async () => {
    try {
      const { url, data } = await fetchGameDetails(id);
      const filePath = path.join(GAMES_DIR, `${id}.json`);
      const payload = { source: { url, scraped_at: nowISO(), tz: TZ }, game: data };
      fs.writeFileSync(filePath, JSON.stringify(payload, null, 2) + '\n', 'utf8');
      return { id, ok: true, file: `games/${id}.json` };
    } catch (err) {
      return { id, ok: false, error: err?.message || String(err) };
    }
  });

  const detailResults = await pLimit(DETAILS_CONCURRENCY, tasks);
  const files = {};
  for (const r of detailResults) if (r?.ok && r.file) files[r.id] = r.file;

  out.details_index = {
    mode: DETAILS_FETCH,
    days_window: DETAILS_DAYS,
    concurrency: DETAILS_CONCURRENCY,
    count: Object.keys(files).length,
    files,
  };

  // Persistencia
  const tmpPath = path.join(OUT_DIR, 'latest.tmp.json');
  fs.writeFileSync(tmpPath, JSON.stringify(out, null, 2) + '\n', 'utf8');

  const old = fs.existsSync(LATEST_PATH) ? fs.readFileSync(LATEST_PATH, 'utf8') : null;
  const neu = fs.readFileSync(tmpPath, 'utf8');

  if (old !== neu) {
    fs.renameSync(tmpPath, LATEST_PATH);
    const iso = nowISO().replace(/[:]/g, '').replace(/\..+/, 'Z');
    const snapPath = path.join(HISTORY_DIR, `${iso}.json`);
    fs.writeFileSync(snapPath, neu, 'utf8');
    console.log(`Updated docs/latest.json and ${Object.keys(files).length} game details.`);
  } else {
    fs.unlinkSync(tmpPath);
    console.log('No changes.');
  }
}

main().catch(err => { console.error(err); process.exit(1); });
