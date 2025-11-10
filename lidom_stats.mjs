// Node 20+ (fetch nativo)
// scripts/lidom_stats.mjs
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as cheerio from "cheerio";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

// === Resolver raíz del repo de forma segura ===
function resolveRepoRoot() {
  const hereDocs = path.join(__dirname, "docs");
  if (fs.existsSync(hereDocs)) return __dirname;
  const upDocs = path.join(__dirname, "..", "docs");
  if (fs.existsSync(upDocs)) return path.join(__dirname, "..");
  return process.cwd();
}
const REPO_ROOT = resolveRepoRoot();
const OUT_DIR = path.join(REPO_ROOT, "docs", "stats");
const OUT_PATH = path.join(OUT_DIR, "lideres.json");
const SOURCE = process.env.LIDOM_STATS_URL || "https://estadisticas.lidom.com/Lider";

// Utils
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const hasLetters = (s) => /[A-Za-zÁÉÍÓÚáéíóúÑñÜü]/.test(s || "");

function cleanNum(s) {
  if (s == null) return null;
  const t = String(s).trim().replace(/\u00A0/g, " ").replace(/,/g, "");
  if (t === "" || t === "-") return null;
  const n = Number(t);
  return Number.isNaN(n) ? t : n;
}

function normalizeHeader(h, i) {
  let s = (h ?? "").toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[().%]/g, "");
  if (!s || /^_+$/.test(s)) s = `col${i}`;
  // quitar acentos
  s = s.normalize("NFD").replace(/[\u0300-\u036f]/g, "");
  return s;
}

// === Headers: usar hijos inmediatos del <table> y preservar posiciones vacías
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

// Si la primera fila tiene más celdas (por ejemplo un "#" a la izquierda), pad al inicio
function padHeadersIfNeeded(headersNorm, firstRowCellCount) {
  let H = headersNorm.slice();
  if (firstRowCellCount > H.length) {
    const diff = firstRowCellCount - H.length;
    const pads = Array.from({ length: diff }, (_, k) => (k === 0 ? "rank" : `col_pad_${k - 1}`));
    H = [...pads, ...H];
  }
  return H;
}

function tableKind(headersNorm) {
  const H = headersNorm.map(h => h.toUpperCase());
  if (H.includes("OPS") && H.includes("AVG")) return "bateo";
  if (H.includes("ERA") && H.includes("WHIP")) return "pitcheo";
  return "desconocido";
}

// ✅ Manejo robusto de filas con menos celdas y fallback de jugador/equipo
function rowToObj($, $row, headersNorm) {
  const cells = $row.children("td,th").toArray();
  const obj = {};

  // Fallback: intenta detectar el índice del jugador por el link a la ficha
  let playerIdx = -1;
  cells.some((node, idx) => {
    const a = $(node).find("a[href*='Miembro/Detalle']");
    if (a.length) {
      const href = a.attr("href") || "";
      const m = href.match(/idMiembro=(\d+)/i);
      obj.jugador_id = m ? Number(m[1]) : null;
      obj.jugador = a.text().trim() || $(node).text().trim();
      playerIdx = idx;
      return true;
    }
    return false;
  });

  headersNorm.forEach((h, i) => {
    const node = cells[i];
    if (!node) {
      if (obj[h] === undefined) obj[h] = null;
      return;
    }
    const $c = $(node);
    const txt = $c.text().trim();

    if (h === "jugador" && !obj.jugador) {
      const a = $c.find("a[href*='Miembro/Detalle']");
      if (a.length) {
        const href = a.attr("href") || "";
        const m = href.match(/idMiembro=(\d+)/i);
        obj.jugador_id = m ? Number(m[1]) : null;
        obj.jugador = a.text().trim() || txt;
      } else {
        obj.jugador = txt;
      }
    } else if (h === "equipo") {
      obj.equipo = txt;
    } else {
      obj[h] = cleanNum(txt);
    }
  });

  // Si no hay header 'equipo' o quedó vacío, intenta vecino del jugador
  const hasEquipoHeader = headersNorm.includes("equipo");
  if ((!hasEquipoHeader || !obj.equipo) && playerIdx >= 0) {
    const neighs = [cells[playerIdx + 1], cells[playerIdx - 1]].filter(Boolean);
    for (const n of neighs) {
      const val = $(n).text().trim();
      if (hasLetters(val) && val.length <= 40) { obj.equipo = obj.equipo || val; break; }
    }
  }

  return obj;
}

function parseTables(html) {
  const $ = cheerio.load(html);
  const results = { bateo: [], pitcheo: [] };

  $("table").each((_, tbl) => {
    const $t = $(tbl);

    // Evita tablas "wrapper": exige <tbody> directo con varias filas
    const $tbody = $t.children("tbody");
    if (!$tbody.length) return;
    const $rows = $tbody.children("tr");
    if ($rows.length < 2) return;

    // Headers del mismo table (hijos inmediatos)
    const { raw, norm } = getHeaders($, $t);
    if (norm.length < 3) return;

    // Alinear headers con # de celdas de la primera fila real
    const firstCells = $rows.first().children("td,th").length;
    const headers = padHeadersIfNeeded(norm, firstCells);

    // Clasificar tabla
    const kind = tableKind(headers);
    if (kind === "desconocido") return;

    // Recorre filas reales; ignorar filas de título/filtros
    $rows.each((_, tr) => {
      const $tr = $(tr);

      const $cells = $tr.children("td,th");
      if (!$cells.length) return;

      const rowText = $tr.text().replace(/\s+/g, " ").trim();
      if (/LIDERES DE BATEO|Filtrar\s*\+|Serie Regular/i.test(rowText) && $cells.length < 5) return;

      const obj = rowToObj($, $tr, headers);

      // Limpieza: si jugador vacío pero equipo parece nombre propio (y no es equipo), swap
      if ((!obj.jugador || String(obj.jugador).trim() === "") && hasLetters(obj.equipo)) {
        const equi = String(obj.equipo || "");
        if (!/aguilas|gigantes|tigres|leones|estrellas|toros/i.test(equi)) {
          obj.jugador = equi;
          obj.equipo = null;
        }
      }

      const hasVal = Object.values(obj || {}).some(v => v !== null && v !== "" && v !== undefined);
      if (hasVal) results[kind].push(obj);
    });
  });

  return results;
}

async function fetchHTML(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0 (LidomBot/1.0; +https://github.com/rodydiaz)",
      "accept-language": "es-DO,es;q=0.9,en;q=0.8"
    }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return await res.text();
}

async function runOnce() {
  const html = await fetchHTML(SOURCE);
  const { bateo, pitcheo } = parseTables(html);

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const payload = {
    meta: {
      source: SOURCE,
      generated_at: new Date().toISOString(),
      counts: { bateo: bateo.length, pitcheo: pitcheo.length }
    },
    bateo,
    pitcheo
  };
  fs.writeFileSync(OUT_PATH, JSON.stringify(payload, null, 2), "utf8");
  console.log(`OK → ${OUT_PATH} (bateo: ${bateo.length}, pitcheo: ${pitcheo.length})`);
}

async function run() {
  try {
    await runOnce();
  } catch (err) {
    console.error("Scraper LIDOM/Lider error:", err?.stack || err);
    await sleep(1200);
    try {
      await runOnce();
    } catch (e2) {
      console.error("Retry failed:", e2?.stack || e2);
      process.exit(1);
    }
  }
}

run();
