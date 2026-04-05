require("dotenv").config();
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl:
    process.env.DATABASE_URL && process.env.DATABASE_URL.includes("localhost")
      ? false
      : { rejectUnauthorized: false },
});

const CATEGORY = 3;
const PAGE_LIMIT = 30;
const MAX_PAGES_PER_PASS = Number(process.env.CRAWL_PAGES_PER_SUBTAB || 3);
const DELAY_MS = Number(process.env.CRAWL_DELAY_MS || 2200);
const INCLUDE_NOT_FOR_SALE =
  String(process.env.INCLUDE_NOT_FOR_SALE || "true") === "true";

const LAYERED_TABS = new Set([
  "shirts",
  "jackets",
  "sweaters",
  "t_shirts",
  "pants",
  "shorts",
  "dresses_skirts",
  "shoes",
]);

const CLASSIC_TABS = new Set([
  "classic_shirts",
  "classic_pants",
  "classic_t_shirts",
]);

const CRAWL_PLAN = [
  { key: "all", passes: [{ keyword: "", intent: "all" }] },

  { key: "classic_shirts", passes: [{ keyword: "classic shirt template", intent: "classic" }] },
  { key: "classic_pants", passes: [{ keyword: "classic pants template", intent: "classic" }] },
  { key: "classic_t_shirts", passes: [{ keyword: "classic t shirt", intent: "classic" }] },

  { key: "shirts", passes: [
    { keyword: "layered shirt", intent: "layered" },
    { keyword: "shirt", intent: "fallback" },
  ]},
  { key: "jackets", passes: [
    { keyword: "layered jacket", intent: "layered" },
    { keyword: "jacket", intent: "fallback" },
  ]},
  { key: "sweaters", passes: [
    { keyword: "layered sweater", intent: "layered" },
    { keyword: "sweater", intent: "fallback" },
  ]},
  { key: "t_shirts", passes: [
    { keyword: "layered t shirt", intent: "layered" },
    { keyword: "t shirt", intent: "fallback" },
  ]},
  { key: "pants", passes: [
    { keyword: "layered pants", intent: "layered" },
    { keyword: "pants", intent: "fallback" },
  ]},
  { key: "shorts", passes: [
    { keyword: "layered shorts", intent: "layered" },
    { keyword: "shorts", intent: "fallback" },
  ]},
  { key: "dresses_skirts", passes: [
    { keyword: "layered dress skirt", intent: "layered" },
    { keyword: "dress skirt", intent: "fallback" },
  ]},
  { key: "shoes", passes: [
    { keyword: "layered shoes", intent: "layered" },
    { keyword: "shoes", intent: "fallback" },
  ]},
];

const CLASSIC_TEXT_RE =
  /(classic shirt|classic pants|classic t-shirt|classic t shirt|template|2d clothing|2d)/i;

// STRICT layered signals (ugc alone does NOT qualify)
const STRONG_LAYERED_RE =
  /(layered|shirt accessory|pants accessory|jacket accessory|sweater accessory|shorts accessory|dress skirt accessory|shoe accessory|left shoe|right shoe|3d|mesh)/i;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function buildUrl({ keyword, cursor }) {
  const url = new URL("https://catalog.roblox.com/v1/search/items/details");
  url.searchParams.set("Category", String(CATEGORY));
  url.searchParams.set("Limit", String(PAGE_LIMIT));
  url.searchParams.set("SortType", "3");
  url.searchParams.set("IncludeNotForSale", INCLUDE_NOT_FOR_SALE ? "true" : "false");

  if (keyword && keyword.trim()) url.searchParams.set("Keyword", keyword.trim());
  if (cursor) url.searchParams.set("Cursor", cursor);
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

    if (res.ok) return res.json();

    const text = await res.text().catch(() => "");
    if (res.status === 429) {
      const backoff = 1200 * attempt;
      console.log(`[429] rate limited, retrying in ${backoff}ms`);
      await sleep(backoff);
      continue;
    }

    throw new Error(`HTTP ${res.status} ${url}\n${text}`);
  }

  throw new Error(`HTTP 429 persisted after retries: ${url}`);
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

  return {
    asset_id: Number.isFinite(assetId) ? assetId : null,
    name: raw.name || "Unknown",
    description: raw.description || "",
    creator_name: creator.name || raw.creatorName || "",
    creator_id: creatorId,
    creator_type: creator.type || raw.creatorType || "",
    item_type: raw.itemType || raw.assetType || raw.assetTypeName || "",
    category: "clothing",
    thumbnail_url: raw.thumbnailUrl || "",
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

function computeLayeredFlag(subtabKey, passIntent, item) {
  if (CLASSIC_TABS.has(subtabKey)) return false;
  if (!LAYERED_TABS.has(subtabKey)) return false;

  const text = `${item.name || ""} ${item.description || ""}`.toLowerCase();

  // Never layered if clearly classic/2D template-ish
  if (CLASSIC_TEXT_RE.test(text)) return false;

  // Layered pass must include strong layered signals
  if (passIntent === "layered") {
    return STRONG_LAYERED_RE.test(text);
  }

  // Fallback pass is non-layered
  return false;
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
      is_layered = public.catalog_item_subtabs.is_layered OR EXCLUDED.is_layered,
      updated_at = NOW()
    `,
    [assetId, subtabKey, !!isLayered]
  );
}

async function rebuildSubtabMappings(subtabKey) {
  await pool.query(`DELETE FROM public.catalog_item_subtabs WHERE subtab_key = $1`, [subtabKey]);
}

async function crawlPass(subtabKey, passConfig) {
  let cursor = null;
  let pages = 0;
  let upserts = 0;
  let layeredMapped = 0;

  while (pages < MAX_PAGES_PER_PASS) {
    const url = buildUrl({ keyword: passConfig.keyword, cursor });
    const json = await fetchJsonWithRetry(url);
    const rows = Array.isArray(json.data) ? json.data : [];

    for (const raw of rows) {
      const item = normalizeItem(raw);
      if (!Number.isFinite(item.asset_id)) continue;

      await upsertItem(item);

      const isLayered = computeLayeredFlag(subtabKey, passConfig.intent, item);
      if (isLayered) layeredMapped += 1;

      await upsertSubtabMapping(item.asset_id, subtabKey, isLayered);
      upserts += 1;
    }

    pages += 1;
    cursor = json.nextPageCursor || null;
    if (!cursor) break;

    await sleep(DELAY_MS);
  }

  console.log(
    `[crawl] ${subtabKey} | keyword="${passConfig.keyword}" | intent=${passConfig.intent} | pages=${pages}, upserts=${upserts}, layeredMapped=${layeredMapped}`
  );
}

async function main() {
  try {
    await pool.query("SELECT 1");
    console.log("DB connected");

    await ensureSchema();

    for (const tab of CRAWL_PLAN) {
      await rebuildSubtabMappings(tab.key);

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