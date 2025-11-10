// Node 20+ (fetch nativo)
// scripts/lidom_stats.mjs
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as cheerio from "cheerio";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, "..", "docs", "stats");
const OUT_PATH = path.join(OUT_DIR, "lideres.json");
const SOURCE = process.env.LIDOM_STATS_URL || "https://estadisticas.lidom.com/Lider";

// Utils
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function cleanNum(s) {
  if (s == null) return null;
  const t = String(s).trim().replace(/\u00A0/g, " ").replace(/,/g, "");
  if (t === "" || t === "-") return null;
  const n = Number(t);
  return Number.isNaN(n) ? t : n;
}

function normalizeHeaders(headers) {
  return headers.map(h => h.toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[().%]/g, "")
    .replace(/á/g, "a").replace(/é/g, "e").replace(/í/g, "i").replace(/ó/g, "o").replace(/ú/g, "u"));
}

function tableKind(headers) {
  const H = headers.map(h => h.toUpperCase());
  if (H.includes("OPS") && H.includes("AVG")) return "bateo";
  if (H.includes("ERA") && H.includes("WHIP")) return "pitcheo";
  return "desconocido";
}

// ✅ FIX: manejar filas con menos celdas que headers (colspan/rowspan)
function rowToObj($, $row, headers) {
  const cells = $row.find("td,th").toArray();
  const obj = {};
  headers.forEach((h, i) => {
    const node = cells[i];
    if (!node) {            // si falta la celda, no rompemos
      obj[h] = null;
      return;
    }
    const $c = $(node);

    if (h === "jugador") {
      const a = $c.find("a[href*='Miembro/Detalle']").attr("href");
      const idMatch = a?.match(/idMiembro=(\d+)/i);
      obj.jugador_id = idMatch ? Number(idMatch[1]) : null;
      obj.jugador = $c.text().trim();
    } else if (h === "equipo") {
      obj.equipo = $c.text().trim();
    } else {
      obj[h] = cleanNum($c.text());
    }
  });
  return obj;
}

function parseTables(html) {
  const $ = cheerio.load(html);
  const results = { bateo: [], pitcheo: [] };

  $("table").each((_, tbl) => {
    const $t = $(tbl);

    // headers
    let headerCells = $t.find("thead tr:first th");
    if (!headerCells.length) headerCells = $t.find("tr:first th");
    if (!headerCells.length) headerCells = $t.find("tr:first td");
    const rawHeaders = headerCells.toArray().map(c => $(c).text().trim()).filter(Boolean);
    if (rawHeaders.length < 3) return;

    const kind = tableKind(rawHeaders);
    if (kind === "desconocido") return;

    const headers = normalizeHeaders(rawHeaders);

    // filas
    let rows = $t.find("tbody tr");
    if (!rows.length) rows = $t.find("tr").slice(1);

    rows.each((_, tr) => {
      const $tr = $(tr);
      // omitir filas que solo tengan <th> (encabezados repetidos)
      if ($tr.find("th").length && !$tr.find("td").length) return;

      const obj = rowToObj($, $tr, headers);
      const hasVal = obj && Object.values(obj).some(v => v !== null && v !== "" && v !== undefined);
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
    // Pequeño retry por si fue un bachecito de red
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
