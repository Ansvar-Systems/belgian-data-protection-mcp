#!/usr/bin/env npx tsx
/**
 * Ingestion crawler for the Belgian Data Protection Authority
 * (GBA / APD — Gegevensbeschermingsautoriteit / Autorité de protection des données).
 *
 * Crawls decisions (beslissingen/décisions) and guidelines (adviezen/avis,
 * aanbevelingen/recommandations) from the official websites and stores them
 * in the SQLite database used by the MCP server.
 *
 * Data sources:
 *   - https://www.gegevensbeschermingsautoriteit.be  (Dutch)
 *   - https://www.autoriteprotectiondonnees.be       (French)
 *
 * Usage:
 *   npx tsx scripts/ingest-apd.ts                  # full crawl
 *   npx tsx scripts/ingest-apd.ts --resume         # skip already-ingested references
 *   npx tsx scripts/ingest-apd.ts --dry-run        # crawl + parse but do not write DB
 *   npx tsx scripts/ingest-apd.ts --force           # drop existing data, re-ingest from scratch
 *   npx tsx scripts/ingest-apd.ts --resume --limit 50  # ingest at most 50 new items
 */

import Database from "better-sqlite3";
import * as cheerio from "cheerio";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["GBA_DB_PATH"] ?? "data/gba.db";
const RATE_LIMIT_MS = 1_500;
const MAX_RETRIES = 3;
const RETRY_BACKOFF_MS = 3_000;
const PAGE_SIZE = 25;
const REQUEST_TIMEOUT_MS = 30_000;

const BASE_NL = "https://www.gegevensbeschermingsautoriteit.be";
const BASE_FR = "https://www.autoriteprotectiondonnees.be";

/** Search paths on the NL site (FR mirrors the same publication slugs). */
const SEARCH_CONFIGS = {
  decisions: {
    searchType: "decision",
    label: "decisions",
  },
  advice: {
    searchType: "advice",
    label: "advice/opinions (adviezen)",
  },
  recommendations: {
    searchType: "recommendation",
    label: "recommendations (aanbevelingen)",
  },
} as const;

// ---------------------------------------------------------------------------
// CLI flags
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const FLAG_RESUME = args.includes("--resume");
const FLAG_DRY_RUN = args.includes("--dry-run");
const FLAG_FORCE = args.includes("--force");
const FLAG_LIMIT = (() => {
  const idx = args.indexOf("--limit");
  if (idx !== -1 && args[idx + 1]) {
    const n = parseInt(args[idx + 1]!, 10);
    return Number.isFinite(n) && n > 0 ? n : Infinity;
  }
  return Infinity;
})();

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

function log(msg: string): void {
  const ts = new Date().toISOString();
  process.stderr.write(`[${ts}] ${msg}\n`);
}

function progress(current: number, total: number, label: string): void {
  const pct = total > 0 ? ((current / total) * 100).toFixed(1) : "?";
  log(`  ${label}: ${current}/${total} (${pct}%)`);
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchWithRetry(
  url: string,
  retries = MAX_RETRIES,
): Promise<Response> {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const controller = new AbortController();
      const timeout = setTimeout(
        () => controller.abort(),
        REQUEST_TIMEOUT_MS,
      );
      const res = await fetch(url, {
        signal: controller.signal,
        headers: {
          "User-Agent":
            "AnsvarMCP/1.0 (belgian-data-protection-mcp; contact: hello@ansvar.ai)",
          Accept: "text/html,application/xhtml+xml,application/pdf,*/*",
          "Accept-Language": "nl-BE,nl;q=0.9,fr-BE;q=0.8,fr;q=0.7,en;q=0.5",
        },
      });
      clearTimeout(timeout);

      if (res.ok) return res;

      // Retry on server errors, not on 404
      if (res.status >= 500 && attempt < retries) {
        log(
          `  HTTP ${res.status} for ${url} — retry ${attempt}/${retries}`,
        );
        await sleep(RETRY_BACKOFF_MS * attempt);
        continue;
      }

      throw new Error(`HTTP ${res.status} ${res.statusText} — ${url}`);
    } catch (err) {
      if (attempt === retries) throw err;
      const msg = err instanceof Error ? err.message : String(err);
      log(`  Fetch error (attempt ${attempt}/${retries}): ${msg}`);
      await sleep(RETRY_BACKOFF_MS * attempt);
    }
  }
  throw new Error(`Unreachable: exhausted retries for ${url}`);
}

async function fetchHtml(url: string): Promise<string> {
  const res = await fetchWithRetry(url);
  return res.text();
}

async function fetchPdfText(url: string): Promise<string> {
  // We fetch the PDF as an ArrayBuffer but since we cannot reliably
  // parse PDF in pure Node without heavy deps, we return a placeholder
  // and record the URL.  The text extraction from PDFs is handled
  // separately — here we at least detect availability and size.
  //
  // For now, we extract what we can from the HTML listing metadata
  // and note the PDF URL in full_text for later enrichment.
  const res = await fetchWithRetry(url);
  const contentType = res.headers.get("content-type") ?? "";

  // Some "PDF" URLs actually serve HTML decision pages
  if (contentType.includes("text/html")) {
    return res.text();
  }

  // Actual PDF — read raw bytes and attempt naive text extraction
  const buf = Buffer.from(await res.arrayBuffer());
  const text = extractTextFromPdfBuffer(buf);
  return text;
}

/**
 * Naive PDF text extraction.  Handles the common case of text-based PDFs
 * produced by the GBA (not scanned images).  Extracts text between
 * BT/ET operators and decodes common PDF string encodings.
 *
 * For production quality, consider pdf-parse or pdfjs-dist, but this
 * avoids adding heavy native dependencies to the ingestion script.
 */
function extractTextFromPdfBuffer(buf: Buffer): string {
  const raw = buf.toString("latin1");
  const chunks: string[] = [];

  // Try to find uncompressed text objects first
  const textRegex = /\(([^)]*)\)\s*Tj/g;
  let textMatch: RegExpExecArray | null;
  while ((textMatch = textRegex.exec(raw)) !== null) {
    if (textMatch[1]) {
      chunks.push(decodePdfString(textMatch[1]));
    }
  }

  // Also try TJ arrays: [(text) num (text) ...] TJ
  const tjRegex = /\[((?:\([^)]*\)|[^\]])*)\]\s*TJ/gi;
  let tjMatch: RegExpExecArray | null;
  while ((tjMatch = tjRegex.exec(raw)) !== null) {
    if (!tjMatch[1]) continue;
    const inner = tjMatch[1];
    const parts = /\(([^)]*)\)/g;
    let partMatch: RegExpExecArray | null;
    while ((partMatch = parts.exec(inner)) !== null) {
      if (partMatch[1]) {
        chunks.push(decodePdfString(partMatch[1]));
      }
    }
  }

  if (chunks.length === 0) {
    return "[PDF content — text extraction requires enrichment pipeline]";
  }

  // Join and clean up
  let text = chunks.join(" ");
  // Collapse whitespace
  text = text.replace(/\s+/g, " ").trim();
  return text;
}

function decodePdfString(s: string): string {
  // Handle octal escapes like \050 \051 \134
  return s.replace(/\\(\d{3})/g, (_, oct: string) =>
    String.fromCharCode(parseInt(oct, 8)),
  ).replace(/\\n/g, "\n")
    .replace(/\\r/g, "\r")
    .replace(/\\t/g, "\t")
    .replace(/\\\\/g, "\\")
    .replace(/\\\(/g, "(")
    .replace(/\\\)/g, ")");
}

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

interface ListingEntry {
  title: string;
  pdfPath: string;
  year: string;
  summary: string;
}

/**
 * Parse a search results page and extract all listing entries.
 * Returns the entries and the total result count.
 */
function parseSearchResults(html: string): {
  entries: ListingEntry[];
  totalResults: number;
} {
  const $ = cheerio.load(html);
  const entries: ListingEntry[] = [];

  // Total results from the heading (e.g. "1204 resultaten gevonden")
  let totalResults = 0;
  const headingText = $("h1")
    .filter((_, el) => {
      const t = $(el).text();
      return /\d+\s+(resultaten|résultats)/.test(t);
    })
    .first()
    .text();
  const countMatch = headingText.match(/(\d[\d.]*)/);
  if (countMatch?.[1]) {
    totalResults = parseInt(countMatch[1].replace(/\./g, ""), 10);
  }

  // Each result has an h3 > a with href to the PDF
  $("h3 a[href*='/publications/']").each((_, el) => {
    const $a = $(el);
    const href = $a.attr("href");
    const title = $a.text().trim();
    if (!href || !title) return;

    // The sibling <p> elements contain year, summary, and category
    const $h3 = $a.closest("h3");
    const siblings: string[] = [];
    $h3.nextAll("p").each((_, p) => {
      siblings.push($(p).text().trim());
    });

    // First sibling is typically the year
    const year = siblings[0] ?? "";
    // Second sibling is the summary (skip if it's "Categorie : ...")
    let summary = "";
    for (let i = 1; i < siblings.length; i++) {
      const s = siblings[i];
      if (s && !s.startsWith("Categorie") && !s.startsWith("Catégorie")) {
        summary = s;
        break;
      }
    }

    entries.push({ title, pdfPath: href, year, summary });
  });

  return { entries, totalResults };
}

/**
 * Classify a decision title into a type.
 */
function classifyDecisionType(title: string): string {
  const lower = title.toLowerCase();
  if (
    lower.includes("sanctie") ||
    lower.includes("sanction") ||
    lower.includes("amende") ||
    lower.includes("boete") ||
    lower.includes("geldboete")
  ) {
    return "sanction";
  }
  if (lower.includes("beslissing ten gronde") || lower.includes("décision quant au fond")) {
    return "beslissing_ten_gronde";
  }
  if (lower.includes("bevel") || lower.includes("injonction")) {
    return "bevel";
  }
  if (
    lower.includes("waarschuwing") ||
    lower.includes("avertissement") ||
    lower.includes("berisping") ||
    lower.includes("réprimande")
  ) {
    return "waarschuwing";
  }
  if (lower.includes("zonder gevolg") || lower.includes("classement sans suite")) {
    return "zonder_gevolg";
  }
  if (lower.includes("schikking") || lower.includes("transaction")) {
    return "schikking";
  }
  if (lower.includes("arrest") || lower.includes("arrêt")) {
    return "arrest";
  }
  if (lower.includes("ordonnantie") || lower.includes("ordonnance")) {
    return "ordonnantie";
  }
  return "beslissing";
}

/**
 * Classify a guideline title into a type.
 */
function classifyGuidelineType(title: string, searchType: string): string {
  const lower = title.toLowerCase();
  if (searchType === "advice" || lower.includes("advies") || lower.includes("avis")) {
    return "advies";
  }
  if (
    searchType === "recommendation" ||
    lower.includes("aanbeveling") ||
    lower.includes("recommandation") ||
    lower.includes("recommendation")
  ) {
    return "aanbeveling";
  }
  if (lower.includes("checklist")) return "checklist";
  if (lower.includes("gids") || lower.includes("guide")) return "guide";
  if (lower.includes("faq")) return "FAQ";
  return "aanbeveling";
}

/**
 * Extract a reference identifier from the title or PDF path.
 * Examples:
 *   "Beslissing ten gronde nr. 56/2026 van 12 maart 2026" → "GBA-2026-056"
 *   "Advies nr. 48/2026 van 17 maart 2026"                → "GBA-ADV-2026-048"
 *   "Aanbeveling 01/2025 ..."                              → "GBA-REC-2025-001"
 */
function extractReference(
  title: string,
  pdfPath: string,
  category: "decision" | "guideline",
): string {
  // Try to extract number/year from title: "nr. 56/2026" or "n° 64/2026" or "01/2025"
  const numYearMatch = title.match(
    /(?:nr\.?|n[°o]\.?|#)?\s*(\d{1,4})\s*\/\s*(\d{4})/i,
  );
  if (numYearMatch?.[1] && numYearMatch[2]) {
    const num = numYearMatch[1].padStart(3, "0");
    const year = numYearMatch[2];
    if (category === "decision") {
      return `GBA-${year}-${num}`;
    }
    // Distinguish advice from recommendation in guidelines
    const lower = title.toLowerCase();
    if (lower.includes("advies") || lower.includes("avis")) {
      return `GBA-ADV-${year}-${num}`;
    }
    return `GBA-REC-${year}-${num}`;
  }

  // Fall back to slug from PDF path
  const slug = pdfPath
    .replace(/^.*\/publications\//, "")
    .replace(/\.pdf$/i, "")
    .replace(/[^a-zA-Z0-9-]/g, "-")
    .substring(0, 80);
  const prefix = category === "decision" ? "GBA-DEC" : "GBA-PUB";
  return `${prefix}-${slug}`;
}

/**
 * Parse a date from a Dutch title string.
 * "van 18 maart 2026" → "2026-03-18"
 */
function parseDateFromTitle(title: string): string | null {
  const MONTHS_NL: Record<string, string> = {
    januari: "01",
    februari: "02",
    maart: "03",
    april: "04",
    mei: "05",
    juni: "06",
    juli: "07",
    augustus: "08",
    september: "09",
    oktober: "10",
    november: "11",
    december: "12",
  };
  const MONTHS_FR: Record<string, string> = {
    janvier: "01",
    février: "02",
    mars: "03",
    avril: "04",
    mai: "05",
    juin: "06",
    juillet: "07",
    août: "08",
    septembre: "09",
    octobre: "10",
    novembre: "11",
    décembre: "12",
  };
  const months = { ...MONTHS_NL, ...MONTHS_FR };

  // Match "van 18 maart 2026" or "du 18 mars 2026" or "of 11 december 2020"
  const match = title.match(
    /(?:van|du|of)\s+(\d{1,2})\s+([a-zéû]+)\s+(\d{4})/i,
  );
  if (match?.[1] && match[2] && match[3]) {
    const day = match[1].padStart(2, "0");
    const monthStr = match[2].toLowerCase();
    const month = months[monthStr];
    const year = match[3];
    if (month) {
      return `${year}-${month}-${day}`;
    }
  }

  // Try year-only from the listing entry
  const yearMatch = title.match(/(\d{4})/);
  if (yearMatch?.[1]) {
    return `${yearMatch[1]}-01-01`;
  }

  return null;
}

/**
 * Try to extract a fine amount from text.
 * Patterns: "600.000 euro", "EUR 250.000", "250 000 EUR"
 */
function extractFineAmount(text: string): number | null {
  const patterns = [
    /(\d[\d.]*[\d])\s*(?:euro|EUR|€)/i,
    /(?:euro|EUR|€)\s*(\d[\d.]*[\d])/i,
    /amende[^.]{0,40}?(\d[\d.\s]*\d)\s*(?:euro|EUR|€)/i,
    /boete[^.]{0,40}?(\d[\d.\s]*\d)\s*(?:euro|EUR|€)/i,
    /geldboete[^.]{0,40}?(\d[\d.\s]*\d)\s*(?:euro|EUR|€)/i,
  ];

  for (const pattern of patterns) {
    const match = text.match(pattern);
    if (match?.[1]) {
      const numStr = match[1].replace(/\s/g, "").replace(/\./g, "");
      const amount = parseInt(numStr, 10);
      if (Number.isFinite(amount) && amount > 0) {
        return amount;
      }
    }
  }
  return null;
}

/**
 * Try to extract GDPR article references from text.
 * Returns JSON array string or null.
 */
function extractGdprArticles(text: string): string | null {
  const articles = new Set<string>();

  // "art. 5", "artikel 17", "article 6", "art. 6(1)(f)"
  const re =
    /(?:art(?:ikel|icle)?\.?\s*)(\d{1,3})(?:\s*(?:\(\d+\))*(?:\s*(?:,|en|et|und)\s*(?:art(?:ikel|icle)?\.?\s*)?(\d{1,3}))*)?/gi;
  let match: RegExpExecArray | null;

  while ((match = re.exec(text)) !== null) {
    if (match[1]) {
      const num = parseInt(match[1], 10);
      // GDPR has articles 1-99
      if (num >= 1 && num <= 99) {
        articles.add(match[1]);
      }
    }
    if (match[2]) {
      const num = parseInt(match[2], 10);
      if (num >= 1 && num <= 99) {
        articles.add(match[2]);
      }
    }
  }

  // Also catch "AVG" / "RGPD" references with article numbers
  const avgRe = /(?:AVG|RGPD|GDPR)\s*[,-]?\s*art(?:ikel|icle)?\.?\s*(\d{1,3})/gi;
  while ((match = avgRe.exec(text)) !== null) {
    if (match[1]) {
      const num = parseInt(match[1], 10);
      if (num >= 1 && num <= 99) {
        articles.add(match[1]);
      }
    }
  }

  if (articles.size === 0) return null;
  return JSON.stringify([...articles].sort((a, b) => parseInt(a, 10) - parseInt(b, 10)));
}

// ---------------------------------------------------------------------------
// Database
// ---------------------------------------------------------------------------

function openDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  if (FLAG_FORCE && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    log(`Deleted existing database at ${DB_PATH}`);
  }
  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  return db;
}

function getExistingReferences(db: Database.Database): Set<string> {
  const refs = new Set<string>();
  const rows = db
    .prepare("SELECT reference FROM decisions")
    .all() as { reference: string }[];
  for (const r of rows) refs.add(r.reference);

  const gRows = db
    .prepare("SELECT reference FROM guidelines WHERE reference IS NOT NULL")
    .all() as { reference: string }[];
  for (const r of gRows) refs.add(r.reference);

  return refs;
}

// ---------------------------------------------------------------------------
// Crawl functions
// ---------------------------------------------------------------------------

/**
 * Build the search URL for a given publication type and page.
 */
function buildSearchUrl(
  base: string,
  searchType: string,
  page: number,
): string {
  const searchPath =
    base === BASE_NL ? "/burger/zoeken" : "/citoyen/chercher";
  const params = new URLSearchParams();
  params.set("q", "");
  params.append("search_category[]", "taxonomy:publications");
  params.append("search_type[]", searchType);
  params.set("s", "recent");
  params.set("l", String(PAGE_SIZE));
  params.set("p", String(page));
  return `${base}${searchPath}?${params.toString()}`;
}

/**
 * Crawl all pages of a search type and collect listing entries.
 */
async function crawlListings(
  base: string,
  searchType: string,
  label: string,
): Promise<ListingEntry[]> {
  const allEntries: ListingEntry[] = [];
  let page = 0;
  let totalResults = 0;

  log(`Crawling ${label} from ${base === BASE_NL ? "NL" : "FR"} site...`);

  // Fetch first page to get total
  const firstUrl = buildSearchUrl(base, searchType, 0);
  const firstHtml = await fetchHtml(firstUrl);
  const firstParsed = parseSearchResults(firstHtml);
  totalResults = firstParsed.totalResults;
  allEntries.push(...firstParsed.entries);

  log(`  Found ${totalResults} total ${label} results`);

  if (firstParsed.entries.length === 0) {
    return allEntries;
  }

  // Crawl remaining pages
  const totalPages = Math.ceil(totalResults / PAGE_SIZE);
  for (page = 1; page < totalPages; page++) {
    await sleep(RATE_LIMIT_MS);
    const url = buildSearchUrl(base, searchType, page);
    progress(page + 1, totalPages, `page`);

    try {
      const html = await fetchHtml(url);
      const parsed = parseSearchResults(html);
      if (parsed.entries.length === 0) {
        log(`  No more entries on page ${page + 1}, stopping`);
        break;
      }
      allEntries.push(...parsed.entries);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`  Error on page ${page + 1}: ${msg} — continuing`);
    }
  }

  log(`  Collected ${allEntries.length} listing entries for ${label}`);
  return allEntries;
}

/**
 * Fetch a PDF, extract text, and return the body content.
 */
async function fetchDecisionContent(
  pdfUrl: string,
): Promise<string> {
  try {
    const text = await fetchPdfText(pdfUrl);
    if (text.length < 50) {
      return `[PDF available at ${pdfUrl} — content pending enrichment]`;
    }
    return text;
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    return `[PDF fetch failed: ${msg} — URL: ${pdfUrl}]`;
  }
}

// ---------------------------------------------------------------------------
// Main ingestion
// ---------------------------------------------------------------------------

interface IngestStats {
  decisionsFound: number;
  decisionsInserted: number;
  decisionsSkipped: number;
  guidelinesFound: number;
  guidelinesInserted: number;
  guidelinesSkipped: number;
  errors: number;
}

async function ingestDecisions(
  db: Database.Database,
  existingRefs: Set<string>,
  stats: IngestStats,
): Promise<void> {
  log("=== Phase 1: Decisions ===");

  // Crawl NL listings (primary — decisions are published in both languages
  // but with the same reference numbers; NL site is the primary source)
  const entries = await crawlListings(
    BASE_NL,
    SEARCH_CONFIGS.decisions.searchType,
    SEARCH_CONFIGS.decisions.label,
  );

  stats.decisionsFound = entries.length;

  const insertStmt = db.prepare(`
    INSERT OR IGNORE INTO decisions
      (reference, title, date, type, entity_name, fine_amount, summary, full_text, topics, gdpr_articles, status)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  let processed = 0;
  for (const entry of entries) {
    if (processed >= FLAG_LIMIT) {
      log(`  Reached --limit ${FLAG_LIMIT}, stopping`);
      break;
    }

    const reference = extractReference(entry.title, entry.pdfPath, "decision");

    if (FLAG_RESUME && existingRefs.has(reference)) {
      stats.decisionsSkipped++;
      continue;
    }

    const pdfUrl = `${BASE_NL}${entry.pdfPath.startsWith("/") ? "" : "/"}${entry.pdfPath}`;

    log(`  [${processed + 1}] ${reference}: ${entry.title.substring(0, 80)}`);

    // Rate limit before PDF fetch
    await sleep(RATE_LIMIT_MS);

    let fullText: string;
    try {
      fullText = await fetchDecisionContent(pdfUrl);
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      log(`    PDF fetch error: ${msg}`);
      fullText = `[PDF unavailable — URL: ${pdfUrl}]`;
      stats.errors++;
    }

    const date = parseDateFromTitle(entry.title);
    const type = classifyDecisionType(entry.title);
    const fineAmount = extractFineAmount(fullText) ?? extractFineAmount(entry.summary);
    const gdprArticles = extractGdprArticles(fullText);

    if (!FLAG_DRY_RUN) {
      try {
        insertStmt.run(
          reference,
          entry.title,
          date,
          type,
          null, // entity_name — not reliably extractable from listing
          fineAmount,
          entry.summary || null,
          fullText,
          null, // topics — requires NLP classification
          gdprArticles,
          "final",
        );
        stats.decisionsInserted++;
        existingRefs.add(reference);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        log(`    DB insert error: ${msg}`);
        stats.errors++;
      }
    } else {
      log(`    [dry-run] would insert: ${reference} (type=${type}, date=${date})`);
      stats.decisionsInserted++;
    }

    processed++;
  }
}

async function ingestGuidelines(
  db: Database.Database,
  existingRefs: Set<string>,
  stats: IngestStats,
): Promise<void> {
  log("=== Phase 2: Guidelines (adviezen + aanbevelingen) ===");

  const insertStmt = db.prepare(`
    INSERT OR IGNORE INTO guidelines
      (reference, title, date, type, summary, full_text, topics, language)
    VALUES
      (?, ?, ?, ?, ?, ?, ?, ?)
  `);

  for (const [, config] of Object.entries(SEARCH_CONFIGS)) {
    // Skip decisions — handled in phase 1
    if (config.searchType === "decision") continue;

    // Crawl NL listings
    const nlEntries = await crawlListings(BASE_NL, config.searchType, config.label);
    stats.guidelinesFound += nlEntries.length;

    let processed = 0;
    for (const entry of nlEntries) {
      if (processed >= FLAG_LIMIT) {
        log(`  Reached --limit ${FLAG_LIMIT}, stopping ${config.label}`);
        break;
      }

      const reference = extractReference(entry.title, entry.pdfPath, "guideline");

      if (FLAG_RESUME && existingRefs.has(reference)) {
        stats.guidelinesSkipped++;
        continue;
      }

      const pdfUrl = `${BASE_NL}${entry.pdfPath.startsWith("/") ? "" : "/"}${entry.pdfPath}`;

      log(`  [${processed + 1}] ${reference}: ${entry.title.substring(0, 80)}`);

      await sleep(RATE_LIMIT_MS);

      let fullText: string;
      try {
        fullText = await fetchDecisionContent(pdfUrl);
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        log(`    PDF fetch error: ${msg}`);
        fullText = `[PDF unavailable — URL: ${pdfUrl}]`;
        stats.errors++;
      }

      const date = parseDateFromTitle(entry.title);
      const type = classifyGuidelineType(entry.title, config.searchType);

      // Detect language from the title — NL site content is usually NL
      // but some documents are explicitly in FR
      const lang = /\b(décision|avis|recommandation|arrêt)\b/i.test(entry.title) ? "fr" : "nl";

      if (!FLAG_DRY_RUN) {
        try {
          insertStmt.run(
            reference,
            entry.title,
            date,
            type,
            entry.summary || null,
            fullText,
            null, // topics — requires NLP classification
            lang,
          );
          stats.guidelinesInserted++;
          existingRefs.add(reference);
        } catch (err) {
          const msg = err instanceof Error ? err.message : String(err);
          log(`    DB insert error: ${msg}`);
          stats.errors++;
        }
      } else {
        log(`    [dry-run] would insert: ${reference} (type=${type}, date=${date}, lang=${lang})`);
        stats.guidelinesInserted++;
      }

      processed++;
    }
  }
}

async function seedTopics(db: Database.Database): Promise<void> {
  log("=== Phase 0: Topics ===");

  interface TopicSeed {
    id: string;
    name_nl: string;
    name_en: string;
    description: string;
  }

  const topics: TopicSeed[] = [
    { id: "cookies", name_nl: "Cookies en trackers", name_en: "Cookies and trackers", description: "Plaatsen en lezen van cookies en trackers (ePrivacy-richtlijn, art. 129 WEC)." },
    { id: "direct_marketing", name_nl: "Direct marketing", name_en: "Direct marketing", description: "Verwerking van persoonsgegevens voor direct-marketingdoeleinden (art. 6(1)(f) en Overweging 47 AVG)." },
    { id: "camerabewaking", name_nl: "Camerabewaking", name_en: "CCTV surveillance", description: "Camerabewaking van publieke en private ruimten (Camerawet 21 maart 2007)." },
    { id: "werknemerscontrole", name_nl: "Werknemerscontrole", name_en: "Employee monitoring", description: "Controle van werknemers door werkgevers, inclusief e-mailmonitoring en geolocatie." },
    { id: "doorgiften", name_nl: "Internationale doorgiften", name_en: "International transfers", description: "Doorgifte van persoonsgegevens naar derde landen (art. 44-49 AVG)." },
    { id: "toestemming", name_nl: "Toestemming", name_en: "Consent", description: "Geldige toestemming als rechtsgrondslag voor verwerking (art. 7 AVG)." },
    { id: "gegevensbescherming_effect_beoordeling", name_nl: "Gegevensbeschermingseffectbeoordeling", name_en: "DPIA", description: "Beoordeling bij risicovolle verwerkingen (art. 35 AVG)." },
    { id: "kinderen", name_nl: "Gegevens van kinderen", name_en: "Children's data", description: "Verwerking van persoonsgegevens van minderjarigen (art. 8 AVG)." },
    { id: "profilering", name_nl: "Profilering", name_en: "Profiling", description: "Geautomatiseerde verwerking ter beoordeling van persoonlijke aspecten (art. 22 AVG)." },
    { id: "transparantie", name_nl: "Transparantie en informatieplicht", name_en: "Transparency", description: "Informatieplicht en transparantievereisten (art. 12-14 AVG)." },
    { id: "beveiliging", name_nl: "Beveiliging van verwerking", name_en: "Security of processing", description: "Technische en organisatorische maatregelen (art. 32 AVG)." },
    { id: "rechten_betrokkenen", name_nl: "Rechten van betrokkenen", name_en: "Data subject rights", description: "Inzage, rectificatie, wissing, beperking, overdraagbaarheid (art. 15-20 AVG)." },
    { id: "dpo", name_nl: "Functionaris voor gegevensbescherming", name_en: "DPO", description: "Aanstelling en positie van de functionaris voor gegevensbescherming (art. 37-39 AVG)." },
    { id: "datalek", name_nl: "Datalekken", name_en: "Data breaches", description: "Meldingsplicht bij datalekken (art. 33-34 AVG)." },
    { id: "gezondheidsgegevens", name_nl: "Gezondheidsgegevens", name_en: "Health data", description: "Verwerking van gezondheidsgegevens en medische dossiers (art. 9 AVG)." },
    { id: "biometrie", name_nl: "Biometrische gegevens", name_en: "Biometric data", description: "Verwerking van biometrische gegevens (art. 9 AVG)." },
    { id: "telecom", name_nl: "Telecommunicatie en ePrivacy", name_en: "Telecom and ePrivacy", description: "Elektronische communicatie, metadata, locatiegegevens (ePrivacy-richtlijn)." },
    { id: "overheid", name_nl: "Overheidsverwerking", name_en: "Public sector processing", description: "Verwerking door overheden en bestuursorganen." },
  ];

  const stmt = db.prepare(
    "INSERT OR IGNORE INTO topics (id, name_nl, name_en, description) VALUES (?, ?, ?, ?)",
  );

  if (!FLAG_DRY_RUN) {
    const tx = db.transaction(() => {
      for (const t of topics) {
        stmt.run(t.id, t.name_nl, t.name_en, t.description);
      }
    });
    tx();
  }

  log(`  Seeded ${topics.length} topics`);
}

// ---------------------------------------------------------------------------
// Entry point
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  log("Belgian Data Protection (APD/GBA) ingestion crawler");
  log(`  Database: ${DB_PATH}`);
  log(`  Flags: resume=${FLAG_RESUME} dry-run=${FLAG_DRY_RUN} force=${FLAG_FORCE} limit=${FLAG_LIMIT === Infinity ? "none" : FLAG_LIMIT}`);
  log("");

  const db = openDb();
  const existingRefs = getExistingReferences(db);
  log(`  Existing references in DB: ${existingRefs.size}`);

  const stats: IngestStats = {
    decisionsFound: 0,
    decisionsInserted: 0,
    decisionsSkipped: 0,
    guidelinesFound: 0,
    guidelinesInserted: 0,
    guidelinesSkipped: 0,
    errors: 0,
  };

  try {
    await seedTopics(db);
    await ingestDecisions(db, existingRefs, stats);
    await ingestGuidelines(db, existingRefs, stats);
  } catch (err) {
    const msg = err instanceof Error ? err.message : String(err);
    log(`FATAL: ${msg}`);
    stats.errors++;
  } finally {
    // Print summary
    log("");
    log("=== Ingestion Summary ===");
    log(`  Decisions found:     ${stats.decisionsFound}`);
    log(`  Decisions inserted:  ${stats.decisionsInserted}`);
    log(`  Decisions skipped:   ${stats.decisionsSkipped}`);
    log(`  Guidelines found:    ${stats.guidelinesFound}`);
    log(`  Guidelines inserted: ${stats.guidelinesInserted}`);
    log(`  Guidelines skipped:  ${stats.guidelinesSkipped}`);
    log(`  Errors:              ${stats.errors}`);

    if (!FLAG_DRY_RUN) {
      const decisionCount = (
        db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }
      ).cnt;
      const guidelineCount = (
        db.prepare("SELECT count(*) as cnt FROM guidelines").get() as { cnt: number }
      ).cnt;
      const topicCount = (
        db.prepare("SELECT count(*) as cnt FROM topics").get() as { cnt: number }
      ).cnt;
      log("");
      log("Database totals:");
      log(`  Topics:     ${topicCount}`);
      log(`  Decisions:  ${decisionCount}`);
      log(`  Guidelines: ${guidelineCount}`);
    }

    db.close();
    log("\nDone.");
  }
}

main().catch((err) => {
  const msg = err instanceof Error ? err.message : String(err);
  process.stderr.write(`Fatal: ${msg}\n`);
  process.exit(1);
});
