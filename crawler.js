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
const SHOES_MAX_PAGES_PER_PASS = Number(
  process.env.CRAWL_SHOES_PAGES_PER_PASS || Math.max(MAX_PAGES_PER_PASS, 6)
);

const DELAY_MS = Number(process.env.CRAWL_DELAY_MS || 2200);
const ASSET_META_DELAY_MS = Number(process.env.CRAWL_ASSET_META_DELAY_MS || 120);
const INCLUDE_NOT_FOR_SALE =
  String(process.env.INCLUDE_NOT_FOR_SALE || "true") === "true";

// Stronger shoe crawl coverage to populate type 70/71 faster
const SHOE_KEYWORDS = [
  "layered shoes",
  "3d shoes",
  "shoe accessory",
  "left shoe",
  "right shoe",
  "sneakers",
  "boots",
  "heels",
  "shoe",
];

const CRAWL_PLAN = [
  { key: "all", passes: [{ keyword: "" }] },

  { key: "classic_shirts", passes: [{ keyword: "classic shirt template" }] },
  { key: "classic_pants", passes: [{ keyword: "classic pants template" }] },
  { key: "classic_t_shirts", passes: [{ keyword: "classic t shirt" }] },

  { key: "shirts", passes: [{ keyword: "layered shirt" }, { keyword: "shirt" }] },
  { key: "jackets", passes: [{ keyword: "layered jacket" }, { keyword: "jacket" }] },
  { key: "sweaters", passes: [{ keyword: "layered sweater" }, { keyword: "sweater" }] },
  { key: "t_shirts", passes: [{ keyword: "layered t shirt" }, { keyword: "t shirt" }] },
  { key: "pants", passes: [{ keyword: "layered pants" }, { keyword: "pants" }] },
  { key: "shorts", passes: [{ keyword: "layered shorts" }, { keyword: "shorts" }] },
  { key: "dresses_skirts", passes: [{ keyword: "layered dress skirt" }, { keyword: "dress skirt" }] },

  // Shoes receives expanded keyword coverage + more pages/pass.
  {
    key: "shoes",
    pagesPerPass: SHOES_MAX_PAGES_PER_PASS,
    passes: SHOE_KEYWORDS.map((keyword) => ({ keyword })),
  },
];

const memoryAssetTypeCache = new Map();

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
      console.log(`[429] ${url} retry in ${backoff}ms`);
      await sleep(backoff);
      continue;
    }

    throw new Error(`HTTP ${res.status} ${url}\n${text}`);
  }

  throw new Error(`HTTP 429 persisted after retries: ${url}`);
}

async function fetchAssetMetaWithRetry(assetId, tries = 4) {
  const url = `https://economy.roblox.com/v2/assets/${assetId}/details`;
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
      const json = await res.json();
      return {
        asset_type_id: Number(json.AssetTypeId ?? json.assetTypeId ?? null) || null,
        asset_type_name: String(
          json.AssetType ?? json.assetType ?? json.AssetTypeName ?? json.assetTypeName ?? ""
        ),
      };
    }

    if (res.status === 429) {
      const backoff = 900 * attempt;
      await sleep(backoff);
      continue;
    }

    return { asset_type_id: null, asset_type_name: "" };
  }

  return { asset_type_id: null, asset_type_name: "" };
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
      asset_type_id INTEGER,
      asset_type_name TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS asset_type_id INTEGER;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS asset_type_name TEXT;`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_category ON public.catalog_items(category);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_updated ON public.catalog_items(updated_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_asset_type_id ON public.catalog_items(asset_type_id);`);
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
    asset_type_id: null,
    asset_type_name: "",
  };
}

async function getKnownAssetTypes(assetIds) {
  if (assetIds.length === 0) return new Map();

  const result = await pool.query(
    `
    SELECT asset_id, asset_type_id, asset_type_name
    FROM public.catalog_items
    WHERE asset_id = ANY($1::bigint[])
    `,
    [assetIds]
  );

  const map = new Map();
  for (const row of result.rows) {
    const key = String(row.asset_id);
    map.set(key, {
      asset_type_id: row.asset_type_id == null ? null : Number(row.asset_type_id),
      asset_type_name: row.asset_type_name || "",
    });
  }

  return map;
}

async function enrichAssetTypes(items) {
  const ids = items.map((i) => i.asset_id).filter((id) => Number.isFinite(id));
  const knownMap = await getKnownAssetTypes(ids);

  for (const item of items) {
    const key = String(item.asset_id);
    if (!item.asset_id) continue;

    if (memoryAssetTypeCache.has(key)) {
      const meta = memoryAssetTypeCache.get(key);
      item.asset_type_id = meta.asset_type_id;
      item.asset_type_name = meta.asset_type_name;
      continue;
    }

    const known = knownMap.get(key);
    if (known && known.asset_type_id != null) {
      memoryAssetTypeCache.set(key, known);
      item.asset_type_id = known.asset_type_id;
      item.asset_type_name = known.asset_type_name;
      continue;
    }

    const fetched = await fetchAssetMetaWithRetry(item.asset_id);
    memoryAssetTypeCache.set(key, fetched);
    item.asset_type_id = fetched.asset_type_id;
    item.asset_type_name = fetched.asset_type_name;

    await sleep(ASSET_META_DELAY_MS);
  }
}

async function upsertItem(item) {
  await pool.query(
    `
    INSERT INTO public.catalog_items (
      asset_id, name, description, creator_name, creator_id, creator_type,
      item_type, category, thumbnail_url, is_offsale, is_limited, is_limited_unique, price_robux,
      asset_type_id, asset_type_name, updated_at
    )
    VALUES (
      $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NOW()
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
      asset_type_id = COALESCE(EXCLUDED.asset_type_id, public.catalog_items.asset_type_id),
      asset_type_name = COALESCE(NULLIF(EXCLUDED.asset_type_name, ''), public.catalog_items.asset_type_name),
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
      item.asset_type_id,
      item.asset_type_name,
    ]
  );
}

async function crawlPass(passConfig, maxPagesForThisPass) {
  let cursor = null;
  let pages = 0;
  let upserts = 0;
  let enriched = 0;

  while (pages < maxPagesForThisPass) {
    const url = buildUrl({ keyword: passConfig.keyword, cursor });
    const json = await fetchJsonWithRetry(url);
    const rows = Array.isArray(json.data) ? json.data : [];
    const items = rows.map(normalizeItem).filter((i) => Number.isFinite(i.asset_id));

    await enrichAssetTypes(items);

    for (const item of items) {
      await upsertItem(item);
      upserts += 1;
      if (item.asset_type_id != null) enriched += 1;
    }

    pages += 1;
    cursor = json.nextPageCursor || null;
    if (!cursor) break;

    await sleep(DELAY_MS);
  }

  console.log(
    `[crawl-pass] keyword="${passConfig.keyword}" pages=${pages}, maxPages=${maxPagesForThisPass}, upserts=${upserts}, enrichedType=${enriched}`
  );
}

async function main() {
  try {
    await pool.query("SELECT 1");
    console.log("DB connected");

    await ensureSchema();

    for (const tab of CRAWL_PLAN) {
      const pagesForTab = Number(tab.pagesPerPass || MAX_PAGES_PER_PASS);

      for (const pass of tab.passes) {
        await crawlPass(pass, pagesForTab);
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