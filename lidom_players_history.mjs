// scripts/lidom_players_history.mjs
// Node 20+
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as cheerio from "cheerio";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// TLS tolerante opcional
if (process.env.ALLOW_INSECURE_TLS === "1") {
  process.env.NODE_TLS_REJECT_UNAUTHORIZED = "0";
}

// ===== Config =====
const BASE = "https://estadisticas.lidom.com";
const LIDOM_STATS_URL = process.env.LIDOM_STATS_URL || `${BASE}/Lider`;
const DETAIL_URL = (id) => `${BASE}/Miembro/Detalle?idMiembro=${id}`;

const CONCURRENCY = Math.max(1, Number(process.env.HIST_CONCURRENCY || 3));
const REQUEST_DELAY_MS = Math.max(0, Number(process.env.HIST_DELAY_MS || 300));
const MAX_SEASONS_PER_PLAYER = Math.max(0, Number(process.env.HIST_MAX_SEASONS || 0));
const IDS_PER_RUN = Math.max(0, Number(process.env.HIST_IDS_PER_RUN || 60)); // ⭐ lote por corrida
const OVERWRITE = String(process.env.HIST_OVERWRITE || "0") === "1";
const SHARDS = Math.max(1, Number(process.env.HIST_SHARDS || 1));
const SHARD_INDEX = Math.min(Math.max(0, Number(process.env.HIST_SHARD_INDEX || 0)), SHARDS - 1);

// ===== Paths =====
function resolveRepoRoot() {
  const hereDocs = path.join(__dirname, "docs");
  if (fs.existsSync(hereDocs)) return __dirname;
  const upDocs = path.join(__dirname, "..", "docs");
  if (fs.existsSync(upDocs)) return path.join(__dirname, "..");
  return process.cwd();
}
const REPO_ROOT = resolveRepoRoot();
const OUT_DIR = path.join(REPO_ROOT, "docs", "stats");
const OUT_PATH = path.join(OUT_DIR, "jugadores_history.json");
const OUT_DIR_BYID = path.join(OUT_DIR, "jugadores_history", "by_id");
const JUGADORES_PATH = path.join(OUT_DIR, "jugadores.json");
const LIDERES_PATH   = path.join(OUT_DIR, "lideres.json");

// ===== Utils =====
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const trim1 = (s) => (s ?? "").replace(/\s+/g, " ").trim();
const cleanNum = (s) => {
  if (s == null) return null;
  const t = String(s).trim().replace(/\u00A0/g, " ").replace(/,/g, "");
  if (t === "" || t === "-") return null;
  const n = Number(t);
  return Number.isNaN(n) ? t : n;
};
function normalizeHeader(h, i) {
  let s = (h ?? "").toLowerCase().replace(/\s+/g, "_").replace(/[().%]/g, "");
  if (!s || /^_+$/.test(s)) s = `col${i}`;
  s = s.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  return s;
}
async function fetchHTML(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0 (LidomPlayersHistoryBot/1.0; +https://github.com/rodydiaz)",
      "accept-language": "es-DO,es;q=0.9,en;q=0.8"
    }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText} @ ${url}`);
  return await res.text();
}

// ===== Tablas =====
function getHeaders($, $t) {
  const $thead = $t.children("thead");
  let headerCells = $thead.length
    ? $thead.children("tr").last().children("th")
    : $t.children("tr").first().children("th");
  if (!headerCells.length) headerCells = $t.children("tr").first().children("td");
  const raw = headerCells.toArray().map((c, i) => {
    const txt = $(c).text().trim();
    return txt === "" ? `col${i}` : txt;
  });
  const norm = raw.map((h, i) => normalizeHeader(h, i));
  return { raw, norm };
}
function padHeadersIfNeeded(headersNorm, firstRowCellCount) {
  let H = headersNorm.slice();
  if (firstRowCellCount > H.length) {
    const diff = firstRowCellCount - H.length;
    const pads = Array.from({ length: diff }, (_, k) => (k === 0 ? "rank" : `col_pad_${k-1}`));
    H = [...pads, ...H];
  }
  return H;
}
function tableHas(cols, ...keys) {
  const U = cols.map(c => c.toUpperCase());
  return keys.every(k => U.includes(k.toUpperCase()));
}
function parseAnyTable($, $t) {
  const $tbody = $t.children("tbody");
  if (!$tbody.length) return null;
  const $rows = $tbody.children("tr");
  if ($rows.length < 1) return null;

  const { norm } = getHeaders($, $t);
  const firstCells = $rows.first().children("td,th").length;
  const headers = padHeadersIfNeeded(norm, firstCells);

  const out = [];
  $rows.each((_, tr) => {
    const $tr = $(tr);
    const $cells = $tr.children("td,th");
    if (!$cells.length) return;
    const cells = $cells.toArray();
    const obj = {};
    headers.forEach((h, i) => {
      const node = cells[i];
      if (!node) { obj[h] = null; return; }
      const $c = $(node);
      const txt = $c.text().trim();
      obj[h] = cleanNum(txt);
    });
    const a = $tr.find("a[href*='Miembro/Detalle']").first();
    if (a.length) {
      const href = a.attr("href") || "";
      const m = href.match(/idMiembro=(\d+)/i);
      if (m) obj.jugador_id = Number(m[1]);
      obj.jugador = obj.jugador || trim1(a.text());
    }
    out.push(obj);
  });
  return { headers, rows: out };
}
function classifyTables($) {
  const results = {
    batting_season: [],
    fielding_season: [],
    batting_game_log: [],
    fielding_game_log: [],
    vs_pitchers: []
  };
  $("table").each((_, tbl) => {
    const parsed = parseAnyTable($, $(tbl));
    if (!parsed) return;
    const H = parsed.headers;
    if (tableHas(H, "temporada", "equipo") && tableHas(H, "avg", "ops")) {
      results.batting_season.push(...parsed.rows); return;
    }
    if (tableHas(H, "temporada", "equipo") && tableHas(H, "inn", "tc", "po")) {
      results.fielding_season.push(...parsed.rows); return;
    }
    if (tableHas(H, "fecha", "oponente") && tableHas(H, "ab", "avg")) {
      results.batting_game_log.push(...parsed.rows); return;
    }
    if (tableHas(H, "fecha", "oponente") && tableHas(H, "posiciones", "inn")) {
      results.fielding_game_log.push(...parsed.rows); return;
    }
    if (tableHas(H, "lanzadores") && tableHas(H, "ab", "avg")) {
      results.vs_pitchers.push(...parsed.rows); return;
    }
  });
  return results;
}
function parseProfile($, fallbackName = null) {
  const name = trim1(($("h1,h2,h3").first().text()) || fallbackName);
  const text = $("body").text().replace(/\s+/g, " ").trim();
  function between(label, nextLabels) {
    const i = text.toLowerCase().indexOf((label + ":").toLowerCase());
    if (i === -1) return null;
    const start = i + label.length + 1;
    let end = text.length;
    for (const nl of nextLabels) {
      const j = text.toLowerCase().indexOf((nl + ":").toLowerCase(), start);
      if (j !== -1) end = Math.min(end, j);
    }
    return trim1(text.slice(start, end));
  }
  const labels = ["Nacionalidad","Debut","Equipo","Fecha Nacimiento","Peso","Posiciones","Lugar de Nacimiento","Pies/Pulgadas","Batea/Lanza"];
  const prof = {
    nombre: name || null,
    nacionalidad: between("Nacionalidad", labels.filter(l => l !== "Nacionalidad")),
    debut: between("Debut", labels.filter(l => l !== "Debut")),
    equipo: between("Equipo", labels.filter(l => l !== "Equipo")),
    fecha_nacimiento: between("Fecha Nacimiento", labels.filter(l => l !== "Fecha Nacimiento")),
    peso: between("Peso", labels.filter(l => l !== "Peso")),
    posiciones: between("Posiciones", labels.filter(l => l !== "Posiciones")),
    lugar_nacimiento: between("Lugar de Nacimiento", labels.filter(l => l !== "Lugar de Nacimiento")),
    estatura_pies_pulgadas: between("Pies/Pulgadas", labels.filter(l => l !== "Pies/Pulgadas")),
    batea_lanza: between("Batea/Lanza", labels.filter(l => l !== "Batea/Lanza")),
  };
  Object.keys(prof).forEach(k => {
    if (typeof prof[k] === "string" && prof[k] !== null) {
      prof[k] = prof[k].replace(/\s{2,}/g, " ").trim();
      if (prof[k] === "") prof[k] = null;
    }
  });
  return prof;
}
function discoverSeasonUrls($, id) {
  const found = new Map(); // label -> url
  $("select option").each((_, opt) => {
    const $o = $(opt);
    const label = trim1($o.text());
    const val = trim1($o.attr("value"));
    if (!label) return;
    if (!/\b(19|20)\d{2}\b/.test(label) && !/Serie/i.test(label)) return;
    if (val && /^https?:\/\//i.test(val)) found.set(label, val);
    else if (val) found.set(label, `${DETAIL_URL(id)}&idTemporada=${encodeURIComponent(val)}`);
  });
  $("a[href*='Miembro/Detalle']").each((_, a) => {
    const href = $(a).attr("href") || "";
    if (/idMiembro=/.test(href) && /(idTemporada|Temporada|anio)=/.test(href)) {
      const label = trim1($(a).text()) || href;
      const url = href.startsWith("http") ? href : `${BASE}${href.startsWith("/") ? "" : "/"}${href}`;
      found.set(label, url);
    }
  });
  const dedup = new Map();
  for (const [label, url] of found.entries()) dedup.set(url, { label, url });
  return Array.from(dedup.values());
}

// ===== IDs =====
function loadIdsFromLocal() {
  const ids = new Map();
  if (fs.existsSync(JUGADORES_PATH)) {
    const data = JSON.parse(fs.readFileSync(JUGADORES_PATH, "utf8"));
    for (const id of Object.keys(data.jugadores || {})) {
      const n = Number(id);
      const nombre = data.jugadores[id]?.nombre || null;
      if (n) ids.set(n, { nombre });
    }
  }
  if (!ids.size && fs.existsSync(LIDERES_PATH)) {
    const data = JSON.parse(fs.readFileSync(LIDERES_PATH, "utf8"));
    for (const kind of ["bateo","pitcheo"]) {
      for (const row of data[kind] || []) {
        if (row?.jugador_id) ids.set(row.jugador_id, { nombre: row.jugador || null });
      }
    }
  }
  return ids;
}
async function loadIdsFromRaw() {
  const ids = new Map();
  const repo = process.env.GITHUB_REPOSITORY || "";
  if (!repo) return ids;
  async function pull(rel) {
    for (const p of [`main/${rel}`, `refs/heads/main/${rel}`]) {
      const url = `https://raw.githubusercontent.com/${repo}/${p}`;
      try {
        const res = await fetch(url);
        if (res.ok) return await res.text();
      } catch {}
    }
    return null;
  }
  const jug = await pull("docs/stats/jugadores.json");
  if (jug) try {
    const obj = JSON.parse(jug);
    for (const id of Object.keys(obj.jugadores || {})) {
      const n = Number(id);
      const nombre = obj.jugadores[id]?.nombre || null;
      if (n) ids.set(n, { nombre });
    }
  } catch {}
  if (!ids.size) {
    const lid = await pull("docs/stats/lideres.json");
    if (lid) try {
      const obj = JSON.parse(lid);
      for (const kind of ["bateo","pitcheo"]) {
        for (const row of obj[kind] || []) {
          if (row?.jugador_id) ids.set(row.jugador_id, { nombre: row.jugador || null });
        }
      }
    } catch {}
  }
  return ids;
}
async function loadIdsFromLeadersPage() {
  const ids = new Map();
  const html = await fetchHTML(LIDOM_STATS_URL);
  const $ = cheerio.load(html);
  $("a[href*='Miembro/Detalle']").each((_, a) => {
    const href = $(a).attr("href") || "";
    const m = href.match(/idMiembro=(\d+)/i);
    if (m) {
      const n = Number(m[1]);
      const nombre = trim1($(a).text());
      if (n) ids.set(n, { nombre: nombre || null });
    }
  });
  return ids;
}
async function ensureIds() {
  let ids = loadIdsFromLocal();
  if (ids.size) return ids;
  ids = await loadIdsFromRaw();
  if (ids.size) return ids;
  try {
    ids = await loadIdsFromLeadersPage();
    if (ids.size) return ids;
  } catch {}
  return new Map();
}

// ===== Persistencia por jugador =====
function listProcessedIds() {
  if (!fs.existsSync(OUT_DIR_BYID)) return new Set();
  const files = fs.readdirSync(OUT_DIR_BYID).filter(f => /^\d+\.json$/.test(f));
  return new Set(files.map(f => Number(f.replace(".json",""))));
}
function savePlayerFile(id, data) {
  fs.mkdirSync(OUT_DIR_BYID, { recursive: true });
  const p = path.join(OUT_DIR_BYID, `${id}.json`);
  fs.writeFileSync(p, JSON.stringify(data, null, 2), "utf8");
}

// ===== Procesar jugador =====
async function fetchPlayerSeasons(id, fallbackName) {
  const html0 = await fetchHTML(DETAIL_URL(id));
  const $0 = cheerio.load(html0);
  const profile = parseProfile($0, fallbackName);
  const baseTables = classifyTables($0);

  const seasons = discoverSeasonUrls($0, id);
  const seasonsLimited = (MAX_SEASONS_PER_PLAYER && seasons.length > MAX_SEASONS_PER_PLAYER)
    ? seasons.slice(0, MAX_SEASONS_PER_PLAYER)
    : seasons;

  const por_temporada = {};
  for (const s of seasonsLimited) {
    try {
      const html = await fetchHTML(s.url);
      const $ = cheerio.load(html);
      por_temporada[s.label] = classifyTables($);
      if (REQUEST_DELAY_MS) await sleep(REQUEST_DELAY_MS);
    } catch (e) {
      console.warn(`WARN temporada falló ${id} [${s.label}] → ${e?.message || e}`);
    }
  }

  return {
    jugador_id: id,
    nombre: profile.nombre || fallbackName || null,
    perfil: profile,
    tablas_actual: baseTables,
    por_temporada,
    source: DETAIL_URL(id)
  };
}

// ===== Merge final =====
function mergeAllToOne() {
  fs.mkdirSync(OUT_DIR, { recursive: true });
  if (!fs.existsSync(OUT_DIR_BYID)) {
    fs.writeFileSync(OUT_PATH, JSON.stringify({ meta:{generated_at:new Date().toISOString(), total:0}, jugadores:{} }, null, 2));
    return { total: 0 };
  }
  const files = fs.readdirSync(OUT_DIR_BYID).filter(f => /^\d+\.json$/.test(f));
  const out = {};
  for (const f of files) {
    try {
      const id = Number(f.replace(".json",""));
      const obj = JSON.parse(fs.readFileSync(path.join(OUT_DIR_BYID, f), "utf8"));
      out[id] = obj;
    } catch {}
  }
  const payload = { meta:{ generated_at:new Date().toISOString(), total:Object.keys(out).length }, jugadores: out };
  fs.writeFileSync(OUT_PATH, JSON.stringify(payload, null, 2), "utf8");
  return { total: payload.meta.total };
}

// ===== Runner =====
async function run() {
  const idsMap = await ensureIds();
  const all = Array.from(idsMap.entries()).sort((a,b)=>a[0]-b[0]); // [[id,{nombre}]...]
  if (!all.length) {
    console.error("No hay IDs disponibles (ni locales, ni raw, ni scrape de líderes).");
    process.exit(1);
  }

  // Sharding
  const sharded = all.filter((_, idx) => (idx % SHARDS) === SHARD_INDEX);

  // Skip/overwrite
  const processed = listProcessedIds();
  const pending = sharded.filter(([id]) => OVERWRITE ? true : !processed.has(id));

  // Lote
  const batch = IDS_PER_RUN ? pending.slice(0, IDS_PER_RUN) : pending;
  console.log(`Total IDs: ${all.length} | Shard ${SHARD_INDEX+1}/${SHARDS}: ${sharded.length} | Ya hechos: ${processed.size} | Este lote: ${batch.length}`);

  let cursor = 0;
  const out = {};
  async function worker() {
    while (cursor < batch.length) {
      const myIdx = cursor++;
      const [id, meta] = batch[myIdx];
      try {
        const data = await fetchPlayerSeasons(id, meta?.nombre || null);
        savePlayerFile(id, data); // persistir inmediato (resume)
        out[id] = true;
        console.log(`OK historial jugador ${id} (${meta?.nombre || "s/n"})`);
      } catch (e) {
        console.error(`FAIL historial jugador ${id}:`, e?.message || e);
      }
      if (REQUEST_DELAY_MS) await sleep(REQUEST_DELAY_MS);
    }
  }
  const workers = Array.from({ length: CONCURRENCY }, () => worker());
  await Promise.all(workers);

  const { total } = mergeAllToOne();
  console.log(`OK → ${OUT_PATH} | total fusionado: ${total}`);
}

run().catch(err => {
  console.error("Fatal:", err?.stack || err);
  process.exit(1);
});
