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
      'user-agent': 'scrape-lidom-bot/1.1 (+github actions)',
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
      'user-agent': 'scrape-lidom-bot/1.1 (+github actions)',
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

// ---------- Parse ViewModel ----------
function extractViewModelArgs(html) {
  const marker = 'ko.applyBindings(new ViewModel(';
  const start = html.indexOf(marker);
  if (start === -1) throw new Error('ViewModel call not found');

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

  while (args.length < 5) {
    const peek = html.slice(i).trimStart();
    if (peek.startsWith(')')) break;
    args.push(readArg());
    if (html[i] === ',') i++;
  }

  return args;
}

// ---------- Normalización HOME ----------
function normalizeHomePayload(data) {
  if (!Array.isArray(data) || data.length === 0) throw new Error('Unexpected data root');
  const series = data[0];
  const league = series.league ?? {};

  const pkg = {
    source: { homepage: SOURCE_HOME, scraped_at: nowISO(), tz: TZ },
    league: {
      id: league.id ?? null,
      name: league.name ?? null,
      seo_url: league.seo_url ?? null,
      season_id: league.season_id ?? null,
      round_name: league.round_name ?? null,
      date_start: league.date_start ?? null,
      date_end: league.date_end ?? null,
    },
    standings: (series.standings ?? []).map(s => ({
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
      gb: s.gb,
      pct: s.pct,
      wins_home: s.wins_home ?? null,
      loses_home: s.loses_home ?? null,
      wins_visitor: s.wins_visitor ?? null,
      loses_visitor: s.loses_visitor ?? null,
    })),
    games: {
      today: series.todayGames ?? [],
      upcoming: series.upcomingGames ?? series.nearestGames ?? [],
      previous: series.previousGames ?? series.previousRoundGames ?? [],
    },
  };

  const pctNum = v => (typeof v === 'string' ? Number(v) : Number(v || 0));
  pkg.standings.sort((a, b) => pctNum(b.pct) - pctNum(a.pct));

  const byDate = sortByDateStrAsc;
  pkg.games.today.sort(byDate);
  pkg.games.upcoming.sort(byDate);
  pkg.games.previous.sort(byDate);

  return pkg;
}

// ---------- Calendario y Resultados ----------
async function fetchFullCalendarForLeague(seo_url) {
  const calendarUrl = new URL(`/liga/${seo_url}/calendario`, SOURCE_HOME).href;
  const html = await fetchText(calendarUrl);
  const [_seriesObj, _translations, _teams, months] = extractViewModelArgs(html);

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

// ---------- Detalle de Juego ----------
function normalizeGameDetails(data, logs, siblingGames) {
  // data = objeto principal del juego (ya viene con innings, equipos, contadores, etc.)
  // logs = [{ inning: "Alta 1ra", play_by_play: [{reference_id, message, scored, is_primary, date}, ...] }, ...]
  const base = {
    id: data.id,
    season_id: data.season_id ?? null,
    status: data.status,
    date: data.date,
    part: data.part ?? null,
    roundText: data.roundText ?? null,
    currentInningNum: data.currentInningNum ?? null,
    lastPlayByPlay: data.lastPlayByPlay ?? null,
    balls: data.balls ?? null,
    strikes: data.strikes ?? null,
    outs: data.outs ?? null,
    moneyline_g: data.moneyline_g ?? null,
    moneyline_h: data.moneyline_h ?? null,
    handicap_g: data.handicap_g ?? null,
    handicap_h: data.handicap_h ?? null,
    over_under: data.over_under ?? null,
    winningPitcher: data.winningPitcher ?? null,
    losingPitcher: data.losingPitcher ?? null,
    savingPitcher: data.savingPitcher ?? null,
    battingTeam: data.battingTeam ?? null,
    base: data.base ?? null,
    comment: data.comment ?? '',
    awayTeam: data.awayTeam ? {
      id: data.awayTeam.id,
      name: data.awayTeam.name,
      abbreviation: data.awayTeam.abbreviation,
      color: data.awayTeam.color,
      logo: data.awayTeam.logo,
      permalink: data.awayTeam.permalink,
      runs: data.awayTeam.runs ?? null,
      hits: data.awayTeam.hits ?? null,
      errors: data.awayTeam.errors ?? null,
      runs_half: data.awayTeam.runs_half ?? null,
      pitcher: data.awayTeam.pitcher ?? null,
      era: data.awayTeam.era ?? null,
      record: data.awayTeam.record ?? null,
      debut: data.awayTeam.debut ?? null,
      players: Array.isArray(data.awayTeam.players) ? data.awayTeam.players : [],
    } : null,
    homeTeam: data.homeTeam ? {
      id: data.homeTeam.id,
      name: data.homeTeam.name,
      abbreviation: data.homeTeam.abbreviation,
      color: data.homeTeam.color,
      logo: data.homeTeam.logo,
      permalink: data.homeTeam.permalink,
      runs: data.homeTeam.runs ?? null,
      hits: data.homeTeam.hits ?? null,
      errors: data.homeTeam.errors ?? null,
      runs_half: data.homeTeam.runs_half ?? null,
      pitcher: data.homeTeam.pitcher ?? null,
      era: data.homeTeam.era ?? null,
      record: data.homeTeam.record ?? null,
      debut: data.homeTeam.debut ?? null,
      players: Array.isArray(data.homeTeam.players) ? data.homeTeam.players : [],
    } : null,
    innings: Array.isArray(data.innings) ? data.innings.map(i => ({
      num: i.num,
      part: i.part,
      is_last: i.is_last,
      awayTeamRuns: i.awayTeamRuns,
      homeTeamRuns: i.homeTeamRuns,
    })) : [],
    logs: Array.isArray(logs) ? logs.map(g => ({
      inning: g.inning,
      play_by_play: Array.isArray(g.play_by_play) ? g.play_by_play.map(p => ({
        reference_id: p.reference_id,
        message: p.message,
        scored: p.scored,
        is_primary: p.is_primary,
        date: p.date,
      })) : [],
    })) : [],
    siblingGames: Array.isArray(siblingGames) ? siblingGames.map(s => ({
      id: s.id,
      date: s.date,
      status: s.status,
      roundText: s.roundText ?? null,
      awayTeam: s.awayTeam ? {
        id: s.awayTeam.id, name: s.awayTeam.name, abbreviation: s.awayTeam.abbreviation,
        logo: s.awayTeam.logo, runs: s.awayTeam.runs ?? null, hits: s.awayTeam.hits ?? null, errors: s.awayTeam.errors ?? null,
        permalink: s.awayTeam.permalink ?? null
      } : null,
      homeTeam: s.homeTeam ? {
        id: s.homeTeam.id, name: s.homeTeam.name, abbreviation: s.homeTeam.abbreviation,
        logo: s.homeTeam.logo, runs: s.homeTeam.runs ?? null, hits: s.homeTeam.hits ?? null, errors: s.homeTeam.errors ?? null,
        permalink: s.homeTeam.permalink ?? null
      } : null,
    })) : [],
  };

  return base;
}

async function fetchGameDetails(gameId) {
  const url = new URL(`/${gameId}`, SOURCE_HOME).href;
  const html = await fetchText(url);
  const [gameData, siblingGames, logs /* , translations */] = extractViewModelArgs(html);
  const normalized = normalizeGameDetails(gameData, logs, siblingGames);
  return { url, data: normalized };
}

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

  // 1) HOME
  const homeHtml = await fetchText(SOURCE_HOME);
  const homeArgs = extractViewModelArgs(homeHtml);
  const out = normalizeHomePayload(homeArgs[0]);

  // 2) Calendario completo
  let calendarGames = [];
  if (out?.league?.seo_url) {
    try {
      const fullCal = await fetchFullCalendarForLeague(out.league.seo_url);
      calendarGames = fullCal.games;
    } catch (e) {
      console.warn('Full calendar fetch failed:', e.message);
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

  // 5) Detalles de juegos (opcional, controlado por env)
  const targetIds = pickWhichDetailsToFetch(allGames);

  const tasks = targetIds.map(id => async () => {
    try {
      const { url, data } = await fetchGameDetails(id);
      const filePath = path.join(GAMES_DIR, `${id}.json`);
      const payload = {
        source: { url, scraped_at: nowISO(), tz: TZ },
        game: data,
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
