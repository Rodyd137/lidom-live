// Node 20+ (fetch nativo)
// scripts/lidom_players.mjs
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as cheerio from "cheerio";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// ====== Config ======
const BASE = "https://estadisticas.lidom.com";
const DETAIL_URL = (id) => `${BASE}/Miembro/Detalle?idMiembro=${id}`;

const CONCURRENCY = Math.max(1, Number(process.env.PLAYERS_CONCURRENCY || 4));
const REQUEST_DELAY_MS = Math.max(0, Number(process.env.PLAYERS_DELAY_MS || 250));
const FALLBACK_SCRAPE_LIDER = (process.env.FALLBACK_SCRAPE_LIDER || "0") === "1";
const LIDER_URL = process.env.LIDOM_STATS_URL || `${BASE}/Lider`;

// ====== Paths ======
function resolveRepoRoot() {
  const hereDocs = path.join(__dirname, "docs");
  if (fs.existsSync(hereDocs)) return __dirname;
  const upDocs = path.join(__dirname, "..", "docs");
  if (fs.existsSync(upDocs)) return path.join(__dirname, "..");
  return process.cwd();
}
const REPO_ROOT = resolveRepoRoot();
const OUT_DIR = path.join(REPO_ROOT, "docs", "stats");
const OUT_PATH = path.join(OUT_DIR, "jugadores.json");
const LIDERES_PATH = path.join(REPO_ROOT, "docs", "stats", "lideres.json");

// ====== Utils ======
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const hasLetters = (s) => /[A-Za-zÁÉÍÓÚáéíóúÑñÜü]/.test(s || "");
const trim1 = (s) => (s ?? "").replace(/\s+/g, " ").trim();
const cleanNum = (s) => {
  if (s == null) return null;
  const t = String(s).trim().replace(/\u00A0/g, " ").replace(/,/g, "");
  if (t === "" || t === "-") return null;
  const n = Number(t);
  return Number.isNaN(n) ? t : n;
};

function normalizeHeader(h, i) {
  let s = (h ?? "").toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[().%]/g, "");
  if (!s || /^_+$/.test(s)) s = `col${i}`;
  // quitar acentos
  s = s.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  return s;
}

async function fetchHTML(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0 (LidomPlayersBot/1.0; +https://github.com/rodydiaz)",
      "accept-language": "es-DO,es;q=0.9,en;q=0.8"
    }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText} @ ${url}`);
  return await res.text();
}

// ====== Generic table parser (child-only to avoid wrappers) ======
function getHeaders($, $t) {
  const $thead = $t.children("thead");
  let headerCells = $thead.length
    ? $thead.children("tr").last().children("th")
    : $t.children("tr").first().children("th");

  if (!headerCells.length)
    headerCells = $t.children("tr").first().children("td");

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
    const pads = Array.from({ length: diff }, (_, k) => (k === 0 ? "rank" : `col_pad_${k - 1}`));
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

    // Filas decorativas (título/filtros)
    const rowText = $tr.text().replace(/\s+/g, " ").trim();
    if (/LIDERES|Filtrar\s*\+|Serie Regular/i.test(rowText) && $cells.length < 5) return;

    const cells = $cells.toArray();
    const obj = {};
    headers.forEach((h, i) => {
      const node = cells[i];
      if (!node) { obj[h] = null; return; }
      const $c = $(node);
      const txt = $c.text().trim();
      obj[h] = cleanNum(txt);
    });
    // intenta sacar nombre/id si viene hiperlinkado
    const a = $tr.find("a[href*='Miembro/Detalle']").first();
    if (a.length) {
      const href = a.attr("href") || "";
      const m = href.match(/idMiembro=(\d+)/i);
      if (m) obj.jugador_id = Number(m[1]);
      obj.jugador = obj.jugador || trim1(a.text()) || obj.jugador;
    }
    out.push(obj);
  });

  return { headers, rows: out };
}

// ====== Page-specific classification ======
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

    // season (bateo)
    if (tableHas(H, "temporada", "equipo") && tableHas(H, "avg", "ops")) {
      results.batting_season.push(...parsed.rows);
      return;
    }
    // season (fildeo)
    if (tableHas(H, "temporada", "equipo") && tableHas(H, "inn", "tc", "po", "rf")) {
      results.fielding_season.push(...parsed.rows);
      return;
    }
    // game logs bateo
    if (tableHas(H, "fecha", "oponente") && tableHas(H, "ab", "avg", "ops")) {
      results.batting_game_log.push(...parsed.rows);
      return;
    }
    // game logs fildeo
    if (tableHas(H, "fecha", "oponente") && tableHas(H, "posiciones", "inn", "tc", "po")) {
      results.fielding_game_log.push(...parsed.rows);
      return;
    }
    // vs lanzadores
    if (tableHas(H, "lanzadores") && tableHas(H, "ab", "avg")) {
      results.vs_pitchers.push(...parsed.rows);
      return;
    }
  });

  return results;
}

// ====== Profile extraction ======
function parseProfile($, fallbackName = null) {
  // Nombre: intenta h1/h2/h3; si no, fallback del leaders
  const name = trim1(($("h1,h2,h3").first().text()) || fallbackName);

  const text = $("body").text().replace(/\s+/g, " ").trim();

  function between(label, nextLabels) {
    const i = text.toLowerCase().indexOf((label + ":").toLowerCase());
    if (i === -1) return null;
    const start = i + label.length + 1;
    // próximo label más cercano
    let end = text.length;
    for (const nl of nextLabels) {
      const j = text.toLowerCase().indexOf((nl + ":").toLowerCase(), start);
      if (j !== -1) { end = Math.min(end, j); }
    }
    return trim1(text.slice(start, end));
  }

  const labels = [
    "Nacionalidad", "Debut", "Equipo",
    "Fecha Nacimiento", "Peso", "Posiciones",
    "Lugar de Nacimiento", "Pies/Pulgadas", "Batea/Lanza"
  ];

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

  // Limpieza básica
  Object.keys(prof).forEach(k => {
    if (typeof prof[k] === "string" && prof[k] !== null) {
      prof[k] = prof[k].replace(/\s{2,}/g, " ").trim();
      if (prof[k] === "") prof[k] = null;
    }
  });

  return prof;
}

// ====== Gather player IDs ======
async function loadPlayerIdsFromLeaders() {
  if (!fs.existsSync(LIDERES_PATH)) {
    if (!FALLBACK_SCRAPE_LIDER) {
      console.warn("WARN: docs/stats/lideres.json no existe y FALLBACK_SCRAPE_LIDER=0. No hay IDs.");
      return new Map();
    }
    // Scrapea /Lider para obtener IDs
    console.log("No hay lideres.json; scrapeando /Lider para IDs...");
    const html = await fetchHTML(LIDER_URL);
    const $ = cheerio.load(html);
    const ids = new Map(); // id -> { nombre, fuente: 'lider' }

    $("table").each((_, tbl) => {
      const parsed = parseAnyTable($, $(tbl));
      if (!parsed) return;
      parsed.rows.forEach(r => {
        if (r.jugador_id) {
          const name = trim1(r.jugador) || null;
          if (!ids.has(r.jugador_id)) ids.set(r.jugador_id, { nombre: name, fuente: "lider" });
        }
      });
    });
    return ids;
  }

  const data = JSON.parse(fs.readFileSync(LIDERES_PATH, "utf8"));
  const ids = new Map();
  for (const kind of ["bateo", "pitcheo"]) {
    for (const row of data[kind] || []) {
      if (row && row.jugador_id) {
        const name = trim1(row.jugador) || null;
        if (!ids.has(row.jugador_id)) ids.set(row.jugador_id, { nombre: name, fuente: "lider" });
      }
    }
  }
  return ids;
}

// ====== Fetch & parse one player ======
async function fetchPlayer(id, fallbackName) {
  const html = await fetchHTML(DETAIL_URL(id));
  const $ = cheerio.load(html);

  const profile = parseProfile($, fallbackName);
  const tables = classifyTables($);

  return {
    jugador_id: id,
    nombre: profile.nombre || fallbackName || null,
    perfil: profile,
    tablas: tables,
    source: DETAIL_URL(id)
  };
}

// ====== Runner with concurrency ======
async function run() {
  const idsMap = await loadPlayerIdsFromLeaders();
  if (!idsMap.size) {
    console.error("No se obtuvieron IDs de jugadores. Aborta.");
    process.exit(1);
  }
  console.log(`Total jugadores a procesar: ${idsMap.size}`);

  const entries = Array.from(idsMap.entries()); // [ [id, {nombre}], ... ]
  const out = {}; // id -> data

  let idx = 0;
  async function worker() {
    while (idx < entries.length) {
      const myIndex = idx++;
      const [id, meta] = entries[myIndex];
      try {
        const data = await fetchPlayer(id, meta.nombre);
        out[id] = data;
        console.log(`OK jugador ${id} (${meta.nombre || "sin nombre"})`);
      } catch (e) {
        console.error(`FAIL jugador ${id}:`, e?.message || e);
      }
      if (REQUEST_DELAY_MS) await sleep(REQUEST_DELAY_MS);
    }
  }

  const workers = Array.from({ length: CONCURRENCY }, () => worker());
  await Promise.all(workers);

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const payload = {
    meta: {
      generated_at: new Date().toISOString(),
      total: Object.keys(out).length,
      source: {
        leaders_json: fs.existsSync(LIDERES_PATH) ? "docs/stats/lideres.json" : null,
        fallback_scraped: FALLBACK_SCRAPE_LIDER
      }
    },
    jugadores: out
  };
  fs.writeFileSync(OUT_PATH, JSON.stringify(payload, null, 2), "utf8");
  console.log(`OK → ${OUT_PATH} (jugadores: ${payload.meta.total})`);
}

run().catch(err => {
  console.error("Fatal:", err?.stack || err);
  process.exit(1);
});
