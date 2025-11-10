// Node 20+
// scripts/lidom_stats.mjs
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import * as cheerio from "cheerio";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const OUT_DIR = path.join(__dirname, "..", "docs", "stats");
const SOURCE = process.env.LIDOM_STATS_URL || "https://estadisticas.lidom.com/Lider";

// Helpers
const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const cleanNum = (s) => {
  if (s == null) return null;
  const t = String(s).trim().replace(/\u00A0/g, " ").replace(/,/g, "");
  if (t === "" || t === "-") return null;
  // números tipo ".286" o "13.1"
  const n = Number(t);
  return Number.isNaN(n) ? t : n;
};

function normalizeHeaders(headers) {
  return headers.map(h => h.toLowerCase()
    .replace(/\s+/g, "_")
    .replace(/[().%]/g, "")
    .replace(/í/g, "i")
    .replace(/é/g, "e")
    .replace(/ó/g, "o")
    .replace(/á/g, "a")
    .replace(/ú/g, "u"));
}

// Detecta tipo por headers
function tableKind(headers) {
  const H = headers.map(h => h.toUpperCase());
  if (H.includes("OPS") && H.includes("AVG")) return "bateo";
  if (H.includes("ERA") && H.includes("WHIP")) return "pitcheo";
  return "desconocido";
}

function rowToObj($, $row, headers) {
  const cells = $row.find("td,th").toArray();
  const obj = {};
  headers.forEach((h, i) => {
    const $c = $(cells[i] || {});
    if (!$c || !$c.length) return;
    // Si la celda del jugador trae enlace, saca id si existe
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

  // Busca todas las tablas y clasifícalas por encabezados
  const results = { bateo: [], pitcheo: [] };

  $("table").each((_, tbl) => {
    const $t = $(tbl);
    // headers: usa thead si existe; si no, la primera fila
    let headerCells = $t.find("thead tr:first th");
    if (!headerCells.length) headerCells = $t.find("tr:first th");
    if (!headerCells.length) headerCells = $t.find("tr:first td");

    const rawHeaders = headerCells.toArray().map(c => $(c).text().trim());
    if (!rawHeaders.length) return;

    const kind = tableKind(rawHeaders);
    if (kind === "desconocido") return;

    const headers = normalizeHeaders(rawHeaders);
    // filas de data
    let rows = $t.find("tbody tr");
    if (!rows.length) rows = $t.find("tr").slice(1); // sin tbody

    rows.each((_, tr) => {
      const obj = rowToObj($, $(tr), headers);
      // filtra filas vacías
      const hasVal = Object.values(obj || {}).some(v => v !== null && v !== "" && v !== undefined);
      if (hasVal) results[kind].push(obj);
    });
  });

  return results;
}

async function fetchHTML(url) {
  const res = await fetch(url, {
    headers: {
      "user-agent": "Mozilla/5.0 (compatible; LidomBot/1.0; +https://github.com/rodydiaz)",
      "accept-language": "es-DO,es;q=0.9,en;q=0.8"
    }
  });
  if (!res.ok) throw new Error(`HTTP ${res.status} ${res.statusText}`);
  return await res.text();
}

async function main() {
  const html = await fetchHTML(SOURCE);
  const { bateo, pitcheo } = parseTables(html);

  const payload = {
    meta: {
      source: SOURCE,
      generated_at: new Date().toISOString(),
      counts: { bateo: bateo.length, pitcheo: pitcheo.length }
    },
    bateo,
    pitcheo
  };

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const outPath = path.join(OUT_DIR, "lideres.json");
  fs.writeFileSync(outPath, JSON.stringify(payload, null, 2), "utf8");
  console.log(`OK → ${outPath} (${bateo.length} bateo, ${pitcheo.length} pitcheo)`);
}

main().catch(async (err) => {
  console.error("Scraper LIDOM/Lider error:", err?.stack || err);
  // retry corto por si hay caída momentánea
  await sleep(1500);
  try {
    const html = await fetchHTML(SOURCE);
    const { bateo, pitcheo } = parseTables(html);
    const payload = {
      meta: { source: SOURCE, generated_at: new Date().toISOString(), retry: true, counts:{bateo: bateo.length, pitcheo: pitcheo.length} },
      bateo, pitcheo
    };
    fs.mkdirSync(OUT_DIR, { recursive: true });
    const outPath = path.join(OUT_DIR, "lideres.json");
    fs.writeFileSync(outPath, JSON.stringify(payload, null, 2), "utf8");
    console.log(`OK (retry) → ${outPath}`);
  } catch (e2) {
    console.error("Retry failed:", e2?.stack || e2);
    process.exit(1);
  }
});
