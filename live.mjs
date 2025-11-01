// live.mjs
// Node 20+. Realtime LIDOM from PelotaInvernal WS.
// deps opcional: npm i ws   (si no está, el script hace exit 0)

import fs from 'node:fs';
import path from 'node:path';
import crypto from 'node:crypto';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, 'docs');
const LIVE_DIR = path.join(OUT_DIR, 'live');
const LIVE_SUMMARY = path.join(OUT_DIR, 'live.json');
const LATEST_PATH = path.join(OUT_DIR, 'latest.json');

const SOURCE_HOME = process.env.SOURCE_HOME || 'https://pelotainvernal.com/';
const TZ = process.env.TZ || 'America/Santo_Domingo';

// Control en CI
const MAX_RUN_MS  = Number(process.env.LIVE_MAX_MS   || 8 * 60_000);  // 8 min
const MAX_IDLE_MS = Number(process.env.LIVE_IDLE_MS  || 90_000);      // 90 s sin mensajes
const SUMMARY_THROTTLE_MS = Number(process.env.LIVE_SUMMARY_THROTTLE_MS || 1500);

// Estados
const STATUS = { NOT_STARTED:1, LIVE:2, PREVIEW:3, DELAYED:4, SUSPENDED:5, FINAL:6, POSTPONED:7 };
const LIVELIKE = new Set([STATUS.LIVE, STATUS.DELAYED, STATUS.SUSPENDED]);

/* ===== Helpers ===== */
function nowISO() { return new Date().toISOString(); }
function safeMkdir(p) { fs.mkdirSync(p, { recursive: true }); }
safeMkdir(OUT_DIR); safeMkdir(LIVE_DIR);

function sha1(s){ return crypto.createHash('sha1').update(s).digest('hex'); }

/** Carga IDs de hoy con estados live/delayed/suspended desde latest.json */
function loadAllowlistFromLatest() {
  try {
    const raw = fs.readFileSync(LATEST_PATH, 'utf8');
    const j = JSON.parse(raw);
    const today = Array.isArray(j?.games?.today) ? j.games.today : [];
    const ids = today
      .filter(g => LIVELIKE.has(Number(g?.status)))
      .map(g => g?.id)
      .filter(Boolean);
    return new Set(ids);
  } catch {
    return null; // no latest.json => aceptar todo
  }
}

// Bases -> flags
function decodeBases(baseCode) {
  const c = Number(baseCode) || 0;
  const on1B = [2,5,6,8].includes(c);
  const on2B = [3,5,7,8].includes(c);
  const on3B = [4,6,7,8].includes(c);
  const runners = (on1B?1:0) + (on2B?1:0) + (on3B?1:0);
  return { on1B, on2B, on3B, runners, mask: c };
}

// Run Expectancy (proxy)
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

function deepMerge(dst, src) {
  for (const k of Object.keys(src || {})) {
    if (src[k] && typeof src[k] === 'object' && !Array.isArray(src[k])) {
      if (!dst[k] || typeof dst[k] !== 'object') dst[k] = {};
      deepMerge(dst[k], src[k]);
    } else {
      dst[k] = src[k];
    }
  }
  return dst;
}

/* ===== Estado runtime ===== */
const games = new Map(); // id -> last state
let lastSummaryWrite = 0;
let lastSummaryHash = '';
let allowlist = loadAllowlistFromLatest();

function writeFileAtomic(filePath, content) {
  const tmp = filePath + '.tmp';
  fs.writeFileSync(tmp, content, 'utf8');
  fs.renameSync(tmp, filePath);
}

function writeSummary(force=false) {
  const now = Date.now();
  if (!force && (now - lastSummaryWrite < SUMMARY_THROTTLE_MS)) return;

  const arr = [];
  for (const [id, g] of games) {
    arr.push({
      id,
      status: g.status,
      roundText: g.roundText,
      currentInningNum: g.currentInningNum,
      atBat: g.atBat || null,
      lastPlayByPlay: g.lastPlayByPlay || null,
      counts: { balls: g.balls ?? null, strikes: g.strikes ?? null, outs: g.outs ?? null },
      base: decodeBases(g.base),
      battingTeam: g.battingTeam,
      score: {
        away: { name: g.awayTeam?.name, abbr: g.awayTeam?.abbreviation, R: g.awayTeam?.runs, H: g.awayTeam?.hits, E: g.awayTeam?.errors },
        home: { name: g.homeTeam?.name, abbr: g.homeTeam?.abbreviation, R: g.homeTeam?.runs, H: g.homeTeam?.hits, E: g.homeTeam?.errors },
      },
      threat: threatScore(g.outs ?? 0, Number(g.base)||0),
      updated_at: nowISO(),
    });
  }

  const payload = {
    source:{ ws:'wss://s.pelotainvernal.com', scraped_at: nowISO(), tz: TZ },
    tracked_ids: allowlist ? Array.from(allowlist) : 'all',
    live: arr
  };

  const content = JSON.stringify(payload, null, 2) + '\n';
  const h = sha1(content);
  if (!force && h === lastSummaryHash) return;

  writeFileAtomic(LIVE_SUMMARY, content);
  lastSummaryHash = h;
  lastSummaryWrite = now;
}

function appendEvent(id, evt) {
  const p = path.join(LIVE_DIR, `${id}.ndjson`);
  fs.appendFileSync(p, JSON.stringify(evt) + '\n', 'utf8');
}

function onGameUpdate(g) {
  if (!g?.id) return;
  if (allowlist && !allowlist.has(g.id)) return; // ignorar juegos fuera de hoy/live

  const prev = games.get(g.id) || {};
  const prevKey = `${prev.roundText}|${prev.currentInningNum}|${prev.balls}|${prev.strikes}|${prev.outs}|${prev.base}|${prev.lastPlayByPlay}`;
  const nextKey = `${g.roundText}|${g.currentInningNum}|${g.balls}|${g.strikes}|${g.outs}|${g.base}|${g.lastPlayByPlay}`;

  const merged = deepMerge({ updated_at: nowISO() }, prev);
  deepMerge(merged, g);
  games.set(g.id, merged);

  if (prevKey !== nextKey) {
    appendEvent(g.id, {
      ts: nowISO(),
      id: g.id,
      state: {
        roundText: merged.roundText,
        currentInningNum: merged.currentInningNum,
        atBat: merged.atBat ?? null,
        lastPlayByPlay: merged.lastPlayByPlay ?? null,
        counts: { balls: merged.balls ?? null, strikes: merged.strikes ?? null, outs: merged.outs ?? null },
        base: decodeBases(merged.base),
        battingTeam: merged.battingTeam,
        score: {
          away: { R: merged.awayTeam?.runs, H: merged.awayTeam?.hits, E: merged.awayTeam?.errors },
          home: { R: merged.homeTeam?.runs, H: merged.homeTeam?.hits, E: merged.homeTeam?.errors },
        },
        threat: threatScore(merged.outs ?? 0, Number(merged.base)||0),
      },
    });
  }
  writeSummary();
}

/* ===== WS client con backoff e idle-exit ===== */
let WebSocket; // <- dinámico
let ws = null;
let pingTimer = null;
let hbTimer = null; // heartbeat de la web (send timestamp)
let idleTimer = null;
let startTime = Date.now();
let lastMsgTs = Date.now();
let backoffMs = 2000;

function clearTimers() {
  if (pingTimer) { clearInterval(pingTimer); pingTimer = null; }
  if (hbTimer)   { clearInterval(hbTimer);   hbTimer = null; }
  if (idleTimer) { clearInterval(idleTimer); idleTimer = null; }
}

function shutdown(code=0, reason='') {
  try { writeSummary(true); } catch {}
  clearTimers();
  if (ws && ws.readyState === WebSocket.OPEN) { try { ws.close(); } catch {} }
  if (reason) console.log(`[live] exit: ${reason}`);
  process.exit(code);
}

function scheduleIdleWatch() {
  if (idleTimer) clearInterval(idleTimer);
  idleTimer = setInterval(() => {
    const now = Date.now();
    if (now - startTime > MAX_RUN_MS) shutdown(0, 'max runtime');
    if (now - lastMsgTs > MAX_IDLE_MS) shutdown(0, 'idle timeout');
  }, 5000);
}

function startWS() {
  const url = 'wss://s.pelotainvernal.com';
  ws = new WebSocket(url, { headers: { 'user-agent': 'lidom-live/1.1' } });

  ws.on('open', () => {
    console.log(`[live] conectado -> ${url}`);
    backoffMs = 2000; // reset backoff
    // Ping nativo cada 10s
    pingTimer = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        try { ws.ping(); } catch {}
      }
    }, 10_000);
    // Heartbeat (envía timestamp)
    hbTimer = setInterval(() => {
      if (ws && ws.readyState === WebSocket.OPEN) {
        try { ws.send(Date.now().toString()); } catch {}
      }
    }, 10_000);
    scheduleIdleWatch();
  });

  ws.on('pong', () => { /* opcional: medir RTT */ });

  ws.on('message', (buf) => {
    lastMsgTs = Date.now();
    try {
      const msg = JSON.parse(buf.toString('utf8'));
      if (msg?.g) onGameUpdate(msg.g);
      if (msg?.c === 'reload') {
        allowlist = loadAllowlistFromLatest() || allowlist;
      }
    } catch { /* ignore parse */ }
  });

  ws.on('close', () => {
    clearTimers();
    const ms = Math.min(backoffMs, 30_000);
    console.log(`[live] close -> reconectar en ${ms}ms`);
    setTimeout(startWS, ms);
    backoffMs = Math.min(backoffMs * 2, 30_000);
  });

  ws.on('error', () => {
    // silencio; el 'close' hace backoff
  });
}

/* ===== Bootstrap ===== */
(async () => {
  try {
    const m = await import('ws');
    WebSocket = m.default || m.WebSocket || m;
  } catch {
    console.log('[live] paquete "ws" no instalado; skip sin error.');
    process.exit(0);
  }

  allowlist = loadAllowlistFromLatest(); // allowlist inicial
  startWS();
  console.log(`[live] escribiendo ${LIVE_SUMMARY} + docs/live/*.ndjson (allowlist: ${allowlist ? allowlist.size : 'all'})`);

  // Salida limpia
  process.on('SIGINT',  () => shutdown(0, 'SIGINT'));
  process.on('SIGTERM', () => shutdown(0, 'SIGTERM'));
})();

