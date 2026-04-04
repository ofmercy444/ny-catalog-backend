require("dotenv").config();
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const CATEGORY = 3; // Roblox catalog category used in your current approach
const PAGE_LIMIT = 30;
const MAX_PAGES_PER_SUBTAB = Number(process.env.CRAWL_PAGES_PER_SUBTAB || 8);
const DELAY_MS = Number(process.env.CRAWL_DELAY_MS || 1200);
const INCLUDE_NOT_FOR_SALE = String(process.env.INCLUDE_NOT_FOR_SALE || "true") === "true";

const SUBTABS = [
  { key: "all", keyword: "" },
  { key: "classic_shirts", keyword: "classic shirts" },
  { key: "classic_pants", keyword: "classic pants" },
  { key: "shirts", keyword: "shirts" },
  { key: "jackets", keyword: "jacket" },
  { key: "sweaters", keyword: "sweater" },
  { key: "t_shirts", keyword: "t shirts" },
  { key: "pants", keyword: "pants" },
  { key: "shorts", keyword: "shorts" },
  { key: "dresses_skirts", keyword: "dresses skirts" },
  { key: "shoes", keyword: "shoes footwear" },
  { key: "classic_t_shirts", keyword: "classic t shirts" },
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

  if (keyword && keyword.trim()) url.searchParams.set("Keyword", keyword.trim());
  if (cursor) url.searchParams.set("Cursor", cursor);

  return url.toString();
}

async function fetchJsonWithRetry(url, tries = 5) {
  for (let attempt = 1; attempt <= tries; attempt += 1) {
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
      console.log(`[429] Rate limited, retrying in ${backoff}ms`);
      await sleep(backoff);
      continue;
    }

    throw new Error(`HTTP ${res.status} ${url}\n${text}`);
  }

  throw new Error(`HTTP 429 persisted after retries: ${url}`);
}

async function ensureSchema() {
  // Create table if missing
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.catalog_items (
      asset_id BIGINT NOT NULL,
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

  // Ensure required columns exist
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS description TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS creator_name TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS creator_id BIGINT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS creator_type TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS item_type TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS category TEXT DEFAULT 'clothing';`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS thumbnail_url TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS is_offsale BOOLEAN DEFAULT FALSE;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS is_limited BOOLEAN DEFAULT FALSE;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS is_limited_unique BOOLEAN DEFAULT FALSE;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS price_robux INTEGER;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();`);

  // Normalize key type and data quality
  await pool.query(`
    ALTER TABLE public.catalog_items
    ALTER COLUMN asset_id TYPE BIGINT
    USING asset_id::bigint;
  `);
  await pool.query(`DELETE FROM public.catalog_items WHERE asset_id IS NULL;`);
  await pool.query(`
    DELETE FROM public.catalog_items a
    USING public.catalog_items b
    WHERE a.asset_id = b.asset_id
      AND a.ctid < b.ctid;
  `);

  // Ensure the exact unique constraint name used by ON CONFLICT
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'catalog_items_asset_id_key'
      ) THEN
        ALTER TABLE public.catalog_items
        ADD CONSTRAINT catalog_items_asset_id_key UNIQUE (asset_id);
      END IF;
    END $$;
  `);

  // Helpful indexes
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_category ON public.catalog_items(category);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_item_type ON public.catalog_items(item_type);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_name_lower ON public.catalog_items((lower(name)));`);
}

function normalizeItem(raw) {
  const creator = raw.creator || {};
  return {
    asset_id: Number(raw.id),
    name: raw.name || "Unknown",
    description: raw.description || "",
    creator_name: creator.name || raw.creatorName || "",
    creator_id: creator.id ? Number(creator.id) : null,
    creator_type: creator.type || "",
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
    ON CONFLICT ON CONSTRAINT catalog_items_asset_id_key
    DO UPDATE SET
      name = EXCLUDED.name,
      description = EXCLUDED.description,
      creator_name = EXCLUDED.creator_name,
      creator_id = EXCLUDED.creator_id,
      creator_type = EXCLUDED.creator_type,
      item_type = EXCLUDED.item_type,
      category = EXCLUDED.category,
      thumbnail_url = EXCLUDED.thumbnail_url,
      is_offsale = EXCLUDED.is_offsale,
      is_limited = EXCLUDED.is_limited,
      is_limited_unique = EXCLUDED.is_limited_unique,
      price_robux = EXCLUDED.price_robux,
      updated_at = NOW();
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

async function crawlSubtab(subtab) {
  let cursor = null;
  let pages = 0;
  let upserts = 0;

  while (pages < MAX_PAGES_PER_SUBTAB) {
    const url = buildUrl({ keyword: subtab.keyword, cursor });
    const json = await fetchJsonWithRetry(url);
    const rows = Array.isArray(json.data) ? json.data : [];

    for (const raw of rows) {
      const item = normalizeItem(raw);
      if (!Number.isFinite(item.asset_id)) continue;
      await upsertItem(item);
      upserts += 1;
    }

    pages += 1;
    cursor = json.nextPageCursor || null;

    if (!cursor) break;
    await sleep(DELAY_MS);
  }

  console.log(`[crawl] ${subtab.key}: pages=${pages}, upserts=${upserts}`);
}

async function main() {
  try {
    await pool.query("SELECT 1");
    console.log("DB connected");

    await ensureSchema();

    for (const subtab of SUBTABS) {
      await crawlSubtab(subtab);
      await sleep(DELAY_MS);
    }

    console.log("Crawl complete");
    process.exit(0);
  } catch (err) {
    console.error("crawl failed:", err);
    process.exit(1);
  }
}

main();