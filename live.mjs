// live.mjs
// Node 20+. Realtime LIDOM from PelotaInvernal WS.
// npm i ws

import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import WebSocket from 'ws';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, 'docs');
const LIVE_DIR = path.join(OUT_DIR, 'live');
const LIVE_SUMMARY = path.join(OUT_DIR, 'live.json');

const SOURCE_HOME = process.env.SOURCE_HOME || 'https://pelotainvernal.com/';
const TZ = process.env.TZ || 'America/Santo_Domingo';

// ===== Helpers =====
function nowISO() { return new Date().toISOString(); }
function safeMkdir(p) { fs.mkdirSync(p, { recursive: true }); }
safeMkdir(OUT_DIR); safeMkdir(LIVE_DIR);

// Base mask del sitio (por HTML):
// Activa celda 1B con [2,5,6,8], 2B con [3,5,7,8], 3B con [4,6,7,8]
// => Mapeo:
//  0: vacías
//  2: 1B
//  3: 2B
//  4: 3B
//  5: 1B+2B
//  6: 1B+3B
//  7: 2B+3B
//  8: bases llenas
function decodeBases(baseCode) {
  const c = Number(baseCode) || 0;
  const on1B = [2,5,6,8].includes(c);
  const on2B = [3,5,7,8].includes(c);
  const on3B = [4,6,7,8].includes(c);
  const runners = (on1B?1:0) + (on2B?1:0) + (on3B?1:0);
  return { on1B, on2B, on3B, runners, mask: c };
}

// Tabla RE (run expectancy) simplificada (aprox MLB, sirve de proxy LIDOM).
// Índice by outs (0..2) y mask (0..8 con los valores que usa la web).
// Los valores son aproximaciones útiles para features "amenaza de carrera".
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

// Score Threat simple (0..100) — combina RE y outs:
function threatScore(outs, baseMask) {
  const re = runExpectancy(outs, baseMask);
  // Normalizamos aprox: 0..2.25 => 0..100
  return Math.round(Math.max(0, Math.min(100, (re / 2.25) * 100)));
}

// Pequeña utilidad de merge profundo (sin arrays):
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

// ===== Estado runtime =====
const games = new Map(); // id -> state
let lastSummaryWrite = 0;

function writeSummary() {
  const now = Date.now();
  if (now - lastSummaryWrite < 1200) return; // throttle
  lastSummaryWrite = now;

  const arr = [];
  for (const [id, g] of games) {
    const s = {
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
    };
    arr.push(s);
  }
  const payload = { source:{ ws:'wss://s.pelotainvernal.com', scraped_at: nowISO(), tz: TZ }, live: arr };
  fs.writeFileSync(LIVE_SUMMARY, JSON.stringify(payload, null, 2) + '\n', 'utf8');
}

function appendEvent(id, evt) {
  const p = path.join(LIVE_DIR, `${id}.ndjson`);
  fs.appendFileSync(p, JSON.stringify(evt) + '\n', 'utf8');
}

function onGameUpdate(g) {
  if (!g?.id) return;
  const prev = games.get(g.id) || {};
  // Detectar “cambios” interesantes (para eventos)
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

// ===== WebSocket client =====
function startWS() {
  const url = 'wss://s.pelotainvernal.com';
  const ws = new WebSocket(url, { headers: { 'user-agent': 'lidom-live/1.0' } });

  let hb = null;
  ws.on('open', () => {
    // Heartbeat cada 10s (igual que la web)
    hb = setInterval(() => { try { ws.send(Date.now().toString()); } catch (_) {} }, 10_000);
  });

  ws.on('message', (buf) => {
    try {
      const msg = JSON.parse(buf.toString('utf8'));
      if (msg?.g) onGameUpdate(msg.g);
      if (msg?.c === 'reload') {
        // opcional: marcar en summary
        // appendEvent('meta', { ts: nowISO(), type:'reload' })
      }
    } catch (e) {
      // ignore
    }
  });

  ws.on('close', () => {
    if (hb) clearInterval(hb);
    setTimeout(startWS, 2000); // reconectar
  });

  ws.on('error', () => {
    // silencio y que reconecte
  });
}

startWS();
console.log(`[live] conectado a WS y escribiendo ${LIVE_SUMMARY} + docs/live/*.ndjson`);
