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
const LIVE_PATH = path.join(OUT_DIR, 'live.json');

const SOURCE_HOME = process.env.SOURCE_HOME || 'https://pelotainvernal.com/';
const TZ = process.env.TZ || 'America/Santo_Domingo';

// === NUEVO: loop “mientras hay juegos” ===
const LIVE_WHILE_GAMES = (process.env.LIVE_WHILE_GAMES || '0') === '1';
const LIVE_SLEEP_SECONDS = Math.max(5, Number(process.env.LIVE_SLEEP_SECONDS || 300)); // 5 min por defecto
const LIVE_MAX_MINUTES = Math.max(5, Number(process.env.LIVE_MAX_MINUTES || 240));    // 4h tope
const sleep = (ms) => new Promise(res => setTimeout(res, ms));

// Detalle: recent | all | none
const DETAILS_FETCH = (process.env.DETAILS_FETCH || 'recent').toLowerCase();
const DETAILS_DAYS = Number(process.env.DETAILS_DAYS || 14);
const DETAILS_CONCURRENCY = Math.max(1, Number(process.env.DETAILS_CONCURRENCY || 4));

// Estados del sitio
const STATUS = { NOT_STARTED:1, LIVE:2, PREVIEW:3, DELAYED:4, SUSPENDED:5, FINAL:6, POSTPONED:7 };
const LIVELIKE = new Set([STATUS.LIVE, STATUS.DELAYED, STATUS.SUSPENDED]);

// Por defecto, solo bajamos detalles de juegos que suelen tener PBP/linescore listo;
// PERO ahora forzaremos SIEMPRE los detalles de hoy, independiente del status
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
      'user-agent': 'lidom-scraper/1.5',
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
      'user-agent': 'lidom-scraper/1.5',
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
  const isoA = (a?.date || '').replace(' ', 'T');
  const isoB = (b?.date || '').replace(' ', 'T');
  return new Date(isoA) - new Date(isoB);
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

/* === Bases & “Threat” para live.json === */
function decodeBases(baseCode) {
  const c = Number(baseCode) || 0;
  const on1B = [2,5,6,8].includes(c);
  const on2B = [3,5,7,8].includes(c);
  const on3B = [4,6,7,8].includes(c);
  const runners = (on1B?1:0) + (on2B?1:0) + (on3B?1:0);
  return { on1B, on2B, on3B, runners, mask: c };
}
const RE = {
  0: { 0:0.50, 2:0.86, 3:1.10, 4:1.30, 5:1.60, 6:1.65, 7:1.95, 8:2.25 },
  1: { 0:0.27, 2:0.50, 3:0.70, 4:0.95, 5:1.05, 6:1.15, 7:1.40, 8:1.60 },
  2: { 0:0.10, 2:0.25, 3:0.35, 4:0.40, 5:0.50, 6:0.55, 7:0.65, 8:0.80 },
};
function runExpectancy(outs, baseMask) {
  const o = Math.min(Math.max(Number(outs)||0, 0), 2);
  const row = RE[o] || RE[0];
  return row[baseMask] ?? row[0];
}
function threatScore(outs, baseMask) {
  const re = runExpectancy(outs, baseMask);
  return Math.round(Math.max(0, Math.min(100, (re / 2.25) * 100)));
}

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
function getHomeTodayGames(seriesLike) {
  const s = _firstSeries(seriesLike);
  const arr = Array.isArray(s?.todayGames) ? s.todayGames : [];
  return dedupeGames(arr).sort(sortByDateStrAsc);
}

/* ============ Calendario / Resultados ============ */
async function fetchFullCalendarForLeague(seo_url) {
  const calendarUrl = new URL(`/liga/${seo_url}/calendario`, SOURCE_HOME).href;
  const html = await fetchText(calendarUrl);
  const args = extractFirstNewViewModelArgs(html); // [series, translations, teams, months]
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

  const base_runners = decodeBases(rawGame.base);

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
    base_runners,
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
  const candidates = (sourceGames || []).filter(g => DETAILS_STATUS_SET.has(Number(g?.status)));

  if (DETAILS_FETCH === 'all') {
    return Array.from(new Set(candidates.map(g => g.id))).filter(Boolean);
  }

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

/* === Helper: escribir archivo solo si cambió (evita commits vacíos) === */
function writeIfChanged(pathname, content) {
  const neu = typeof content === 'string' ? content : JSON.stringify(content, null, 2) + '\n';
  const old = fs.existsSync(pathname) ? fs.readFileSync(pathname, 'utf8') : null;
  if (old !== neu) fs.writeFileSync(pathname, neu, 'utf8');
}

/* === Construye live.json a partir de today + detalles === */
function buildLiveSummary(todayList = [], todayDetailsMap = {}) {
  const trackedIds = [];
  const liveArr = [];

  for (const g of todayList) {
    const status = Number(g?.status);
    if (!LIVELIKE.has(status)) continue;
    const id = g?.id;
    if (!id) continue;
    trackedIds.push(id);

    const det = todayDetailsMap[id] || null;

    // Preferimos datos del detalle (tienen counts/base/lastPlayByPlay confiables)
    if (det) {
      const baseMask = Number(det?.base_state?.base) || 0;
      liveArr.push({
        id,
        status: det.status,
        roundText: det.roundText,
        currentInningNum: det.currentInningNum,
        atBat: det.atBat || null,
        lastPlayByPlay: det.lastPlayByPlay || null,
        counts: { balls: det.counts?.balls ?? null, strikes: det.counts?.strikes ?? null, outs: det.counts?.outs ?? null },
        base: decodeBases(baseMask),
        battingTeam: det.base_state?.battingTeam ?? null,
        score: {
          away: { name: det.teams?.away?.name, abbr: det.teams?.away?.abbr, R: det.teams?.away?.totals?.R, H: det.teams?.away?.totals?.H, E: det.teams?.away?.totals?.E },
          home: { name: det.teams?.home?.name, abbr: det.teams?.home?.abbr, R: det.teams?.home?.totals?.R, H: det.teams?.home?.totals?.H, E: det.teams?.home?.totals?.E },
        },
        threat: threatScore(det.counts?.outs ?? 0, baseMask),
        updated_at: nowISO(),
      });
      continue;
    }

    // Fallback al objeto "shallow" del home/resultados si el detalle falló
    const baseMask = Number(g?.base) || 0;
    liveArr.push({
      id,
      status,
      roundText: g.roundText ?? null,
      currentInningNum: g.currentInningNum ?? null,
      atBat: g.atBat || null,
      lastPlayByPlay: g.lastPlayByPlay || null,
      counts: { balls: g.balls ?? null, strikes: g.strikes ?? null, outs: g.outs ?? null },
      base: decodeBases(baseMask),
      battingTeam: g.battingTeam ?? null,
      score: {
        away: { name: g.awayTeam?.name, abbr: g.awayTeam?.abbreviation, R: g.awayTeam?.runs, H: g.awayTeam?.hits, E: g.awayTeam?.errors },
        home: { name: g.homeTeam?.name, abbr: g.homeTeam?.abbreviation, R: g.homeTeam?.runs, H: g.homeTeam?.hits, E: g.homeTeam?.errors },
      },
      threat: threatScore(g.outs ?? 0, baseMask),
      updated_at: nowISO(),
    });
  }

  return {
    source: { mode: 'scraper', homepage: SOURCE_HOME, scraped_at: nowISO(), tz: TZ },
    tracked_ids: trackedIds,
    live: liveArr,
  };
}

/* ============ Una corrida de scrape (retorna cantidad de juegos live) ============ */
async function scrapeOnce() {
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
  let homeToday = [];
  try {
    const homeHtml = await fetchText(SOURCE_HOME);
    const homeArgs = extractFirstNewViewModelArgs(homeHtml);
    homeSeriesArg = homeArgs[0];                         // [ series ] o {series}
    out = normalizeHomePayload(homeSeriesArg);           // liga + standings
    homeGames = collectHomeGamesUnion(homeSeriesArg);    // unión home
    homeToday = getHomeTodayGames(homeSeriesArg);        // HOY directo del home
  } catch (e) {
    console.warn('Home fetch/parse failed:', e.message);
  }

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

  // “Hoy” preferimos lo que trae el HOME; si vacío, caemos a filtro por fecha
  const todayFromHome = homeToday && homeToday.length ? homeToday : allGames.filter(g => gameYMD(g) === todayYMD);

  out.games = {
    today: todayFromHome,
    upcoming: allGames.filter(g => gameYMD(g) >  todayYMD),
    previous: allGames.filter(g => gameYMD(g) <  todayYMD),
  };

  out.by_date = indexByDate(allGames);
  out.calendar_days = Object.keys(out.by_date).sort();

  // ====== DETALLES ======
  const detailSourceGames = resultsGames.length ? resultsGames : allGames;
  const baseTargetIds = pickWhichDetailsToFetch(detailSourceGames);

  // Forzar detalles de HOY
  const todayIds = (out.games.today || []).map(g => g?.id).filter(Boolean);
  const targetIds = Array.from(new Set([...baseTargetIds, ...todayIds]));

  const tasks = targetIds.map(id => async () => {
    try {
      const { url, data } = await fetchGameDetails(id);
      const filePath = path.join(GAMES_DIR, `${id}.json`);
      const payload = { source: { url, scraped_at: nowISO(), tz: TZ }, game: data };
      fs.writeFileSync(filePath, JSON.stringify(payload, null, 2) + '\n', 'utf8');
      return { id, ok: true, file: `games/${id}.json`, detail: data };
    } catch (err) {
      return { id, ok: false, error: err?.message || String(err) };
    }
  });

  const detailResults = await pLimit(DETAILS_CONCURRENCY, tasks);
  const files = {};
  const today_details = {};
  for (const r of detailResults) {
    if (r?.ok && r.file) files[r.id] = r.file;
    if (r?.ok && todayIds.includes(r.id)) today_details[r.id] = r.detail; // embed para hoy
  }

  out.details_index = {
    mode: DETAILS_FETCH,
    days_window: DETAILS_DAYS,
    concurrency: DETAILS_CONCURRENCY,
    count: Object.keys(files).length,
    files,
  };

  // Embebemos detalles de HOY
  out.today_details = today_details;

  // ====== LIVE SUMMARY (para “near-realtime”) ======
  const livePayload = buildLiveSummary(out.games.today || [], today_details || {});
  writeIfChanged(LIVE_PATH, livePayload);

  // ====== Persistencia de latest + snapshot si cambió ======
  const tmpPath = path.join(OUT_DIR, 'latest.tmp.json');
  fs.writeFileSync(tmpPath, JSON.stringify(out, null, 2) + '\n', 'utf8');

  const old = fs.existsSync(LATEST_PATH) ? fs.readFileSync(LATEST_PATH, 'utf8') : null;
  const neu = fs.readFileSync(tmpPath, 'utf8');

  if (old !== neu) {
    fs.renameSync(tmpPath, LATEST_PATH);
    const iso = nowISO().replace(/[:]/g, '').replace(/\..+/, 'Z');
    const snapPath = path.join(HISTORY_DIR, `${iso}.json`);
    fs.writeFileSync(snapPath, neu, 'utf8');
    console.log(`Updated docs/latest.json, docs/live.json and ${Object.keys(files).length} game details. (today embedded: ${Object.keys(today_details).length})`);
  } else {
    fs.unlinkSync(tmpPath);
    console.log('No changes (latest). Live summary may still have updated if needed.');
  }

  return { liveCount: (livePayload?.live?.length || 0) };
}

/* ============ Main con loop opcional ============ */
async function main() {
  const t0 = Date.now();
  const maxMs = LIVE_MAX_MINUTES * 60_000;

  let iter = 0;
  while (true) {
    iter++;
    console.log(`\n===== SCRAPE ITERATION ${iter} @ ${nowISO()} =====`);
    const { liveCount } = await scrapeOnce();

    if (!LIVE_WHILE_GAMES) break;          // modo tradicional: una sola corrida
    if (liveCount <= 0) break;             // no hay juegos live -> salir
    if (Date.now() - t0 >= maxMs) break;   // tope de seguridad

    console.log(`Hay ${liveCount} juego(s) en vivo. Próximo tick en ${LIVE_SLEEP_SECONDS}s...`);
    await sleep(LIVE_SLEEP_SECONDS * 1000);
  }
}

main().catch(err => { console.error(err); process.exit(1); });
