require("dotenv").config();
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl:
    process.env.DATABASE_URL && process.env.DATABASE_URL.includes("localhost")
      ? false
      : { rejectUnauthorized: false },
});

const CATEGORY = 3; // Roblox catalog category for clothing/accessory marketplace search
const PAGE_LIMIT = 30;
const MAX_PAGES_PER_PASS = Number(process.env.CRAWL_PAGES_PER_SUBTAB || 3);
const DELAY_MS = Number(process.env.CRAWL_DELAY_MS || 2200);
const INCLUDE_NOT_FOR_SALE =
  String(process.env.INCLUDE_NOT_FOR_SALE || "true") === "true";

// Structured crawl plan: for layered-priority tabs we crawl layered query first,
// then a classic/fallback query so layered appears first in sorted results.
const CRAWL_PLAN = [
  { key: "all", passes: [{ keyword: "", isLayered: true }] },

  { key: "classic_shirts", passes: [{ keyword: "classic shirt template", isLayered: false }] },
  { key: "classic_pants", passes: [{ keyword: "classic pants template", isLayered: false }] },
  { key: "classic_t_shirts", passes: [{ keyword: "classic t shirt", isLayered: false }] },

  { key: "shirts", passes: [
    { keyword: "layered shirt ugc", isLayered: true },
    { keyword: "shirt", isLayered: false },
  ]},
  { key: "jackets", passes: [
    { keyword: "layered jacket ugc", isLayered: true },
    { keyword: "jacket", isLayered: false },
  ]},
  { key: "sweaters", passes: [
    { keyword: "layered sweater ugc", isLayered: true },
    { keyword: "sweater", isLayered: false },
  ]},
  { key: "t_shirts", passes: [
    { keyword: "layered t shirt ugc", isLayered: true },
    { keyword: "t shirt", isLayered: false },
  ]},
  { key: "pants", passes: [
    { keyword: "layered pants ugc", isLayered: true },
    { keyword: "pants", isLayered: false },
  ]},
  { key: "shorts", passes: [
    { keyword: "layered shorts ugc", isLayered: true },
    { keyword: "shorts", isLayered: false },
  ]},
  { key: "dresses_skirts", passes: [
    { keyword: "layered dress skirt ugc", isLayered: true },
    { keyword: "dress skirt", isLayered: false },
  ]},
  { key: "shoes", passes: [
    { keyword: "ugc shoes layered", isLayered: true },
    { keyword: "shoes", isLayered: false },
  ]},
];

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildUrl({ keyword, cursor }) {
  const url = new URL("https://catalog.roblox.com/v1/search/items/details");
  url.searchParams.set("Category", String(CATEGORY));
  url.searchParams.set("Limit", String(PAGE_LIMIT));
  url.searchParams.set("SortType", "3");
  url.searchParams.set("IncludeNotForSale", INCLUDE_NOT_FOR_SALE ? "true" : "false");

  if (keyword && keyword.trim()) {
    url.searchParams.set("Keyword", keyword.trim());
  }
  if (cursor) {
    url.searchParams.set("Cursor", cursor);
  }

  return url.toString();
}

async function fetchJsonWithRetry(url, tries = 5) {
  let attempt = 0;

  while (attempt < tries) {
    attempt += 1;

    const res = await fetch(url, {
      headers: {
        "User-Agent": "ny-catalog-backend/1.0",
        Accept: "application/json",
      },
    });

    if (res.ok) {
      return res.json();
    }

    const bodyText = await res.text().catch(() => "");
    if (res.status === 429) {
      const backoff = 1200 * attempt;
      console.log(`[429] rate limited, retrying in ${backoff}ms`);
      await sleep(backoff);
      continue;
    }

    throw new Error(`HTTP ${res.status} on ${url}\n${bodyText}`);
  }

  throw new Error(`HTTP 429 persisted after retries for ${url}`);
}

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.catalog_items (
      asset_id BIGINT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      creator_name TEXT,
      creator_id BIGINT,
      creator_type TEXT,
      item_type TEXT,
      category TEXT DEFAULT 'clothing',
      thumbnail_url TEXT,
      is_offsale BOOLEAN DEFAULT FALSE,
      is_limited BOOLEAN DEFAULT FALSE,
      is_limited_unique BOOLEAN DEFAULT FALSE,
      price_robux INTEGER,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.catalog_item_subtabs (
      asset_id BIGINT NOT NULL REFERENCES public.catalog_items(asset_id) ON DELETE CASCADE,
      subtab_key TEXT NOT NULL,
      is_layered BOOLEAN DEFAULT FALSE,
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (asset_id, subtab_key)
    );
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_category ON public.catalog_items(category);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_updated ON public.catalog_items(updated_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_subtabs_key ON public.catalog_item_subtabs(subtab_key);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_subtabs_layered ON public.catalog_item_subtabs(subtab_key, is_layered);`);
}

function normalizeItem(raw) {
  const creator = raw.creator || {};

  // creator id can appear in multiple forms depending on payload shape
  const creatorIdRaw =
    creator.id ??
    creator.creatorTargetId ??
    raw.creatorTargetId ??
    raw.creatorId ??
    null;

  const creatorId = Number.isFinite(Number(creatorIdRaw))
    ? Number(creatorIdRaw)
    : null;

  const assetId = Number(raw.id);
  const thumbnail = raw.thumbnailUrl || "";

  return {
    asset_id: Number.isFinite(assetId) ? assetId : null,
    name: raw.name || "Unknown",
    description: raw.description || "",
    creator_name: creator.name || raw.creatorName || "",
    creator_id: creatorId,
    creator_type: creator.type || raw.creatorType || "",
    item_type: raw.itemType || raw.assetType || raw.assetTypeName || "",
    category: "clothing",
    thumbnail_url: thumbnail,
    is_offsale:
      raw.itemRestrictions?.includes?.("Offsale") ||
      raw.isOffsale === true ||
      false,
    is_limited:
      raw.itemRestrictions?.includes?.("Limited") ||
      raw.isLimited === true ||
      false,
    is_limited_unique:
      raw.itemRestrictions?.includes?.("LimitedUnique") ||
      raw.isLimitedUnique === true ||
      false,
    price_robux: Number.isFinite(raw.price) ? raw.price : null,
  };
}

async function upsertItem(item) {
  await pool.query(
    `
    INSERT INTO public.catalog_items (
      asset_id, name, description, creator_name, creator_id, creator_type,
      item_type, category, thumbnail_url, is_offsale, is_limited, is_limited_unique, price_robux, updated_at
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,NOW()
    )
    ON CONFLICT (asset_id) DO UPDATE SET
      name = EXCLUDED.name,
      description = EXCLUDED.description,
      creator_name = COALESCE(NULLIF(EXCLUDED.creator_name, ''), public.catalog_items.creator_name),
      creator_id = COALESCE(EXCLUDED.creator_id, public.catalog_items.creator_id),
      creator_type = COALESCE(NULLIF(EXCLUDED.creator_type, ''), public.catalog_items.creator_type),
      item_type = COALESCE(NULLIF(EXCLUDED.item_type, ''), public.catalog_items.item_type),
      category = EXCLUDED.category,
      thumbnail_url = COALESCE(NULLIF(EXCLUDED.thumbnail_url, ''), public.catalog_items.thumbnail_url),
      is_offsale = EXCLUDED.is_offsale,
      is_limited = EXCLUDED.is_limited,
      is_limited_unique = EXCLUDED.is_limited_unique,
      price_robux = EXCLUDED.price_robux,
      updated_at = NOW()
    `,
    [
      item.asset_id,
      item.name,
      item.description,
      item.creator_name,
      item.creator_id,
      item.creator_type,
      item.item_type,
      item.category,
      item.thumbnail_url,
      item.is_offsale,
      item.is_limited,
      item.is_limited_unique,
      item.price_robux,
    ]
  );
}

async function upsertSubtabMapping(assetId, subtabKey, isLayered) {
  await pool.query(
    `
    INSERT INTO public.catalog_item_subtabs (asset_id, subtab_key, is_layered, updated_at)
    VALUES ($1, $2, $3, NOW())
    ON CONFLICT (asset_id, subtab_key) DO UPDATE SET
      is_layered = EXCLUDED.is_layered,
      updated_at = NOW()
    `,
    [assetId, subtabKey, !!isLayered]
  );
}

async function crawlPass(subtabKey, passConfig) {
  let cursor = null;
  let pages = 0;
  let upserts = 0;

  while (pages < MAX_PAGES_PER_PASS) {
    const url = buildUrl({ keyword: passConfig.keyword, cursor });
    const json = await fetchJsonWithRetry(url);

    const rows = Array.isArray(json.data) ? json.data : [];
    for (const raw of rows) {
      const item = normalizeItem(raw);
      if (!Number.isFinite(item.asset_id)) continue;

      await upsertItem(item);
      await upsertSubtabMapping(item.asset_id, subtabKey, passConfig.isLayered);
      upserts += 1;
    }

    pages += 1;
    cursor = json.nextPageCursor || null;

    if (!cursor) break;
    await sleep(DELAY_MS);
  }

  console.log(`[crawl] ${subtabKey} | keyword="${passConfig.keyword}" | layered=${passConfig.isLayered} | pages=${pages}, upserts=${upserts}`);
}

async function main() {
  try {
    await pool.query("SELECT 1");
    console.log("DB connected");

    await ensureSchema();

    for (const tab of CRAWL_PLAN) {
      for (const pass of tab.passes) {
        await crawlPass(tab.key, pass);
        await sleep(DELAY_MS);
      }
    }

    console.log("Crawl complete");
    process.exit(0);
  } catch (err) {
    console.error("crawl failed:", err);
    process.exit(1);
  }
}

main();