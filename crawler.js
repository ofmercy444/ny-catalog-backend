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
const SHOES_PAGES_PER_PASS = Number(process.env.CRAWL_SHOES_PAGES_PER_SUBTAB || 6);

const DELAY_MS = Number(process.env.CRAWL_DELAY_MS || 2200);
const ASSET_META_DELAY_MS = Number(process.env.CRAWL_ASSET_META_DELAY_MS || 120);
const INCLUDE_NOT_FOR_SALE =
  String(process.env.INCLUDE_NOT_FOR_SALE || "true") === "true";

const SHOE_LEFT_TYPE = 70;
const SHOE_RIGHT_TYPE = 71;

const CRAWL_PLAN = [
  { key: "all", passes: [{ keyword: "", intent: "all" }] },

  { key: "classic_shirts", passes: [{ keyword: "classic shirt template", intent: "classic" }] },
  { key: "classic_pants", passes: [{ keyword: "classic pants template", intent: "classic" }] },
  { key: "classic_t_shirts", passes: [{ keyword: "classic t shirt", intent: "classic" }] },

  { key: "shirts", passes: [{ keyword: "layered shirt", intent: "layered" }, { keyword: "shirt", intent: "fallback" }] },
  { key: "jackets", passes: [{ keyword: "layered jacket", intent: "layered" }, { keyword: "jacket", intent: "fallback" }] },
  { key: "sweaters", passes: [{ keyword: "layered sweater", intent: "layered" }, { keyword: "sweater", intent: "fallback" }] },
  { key: "t_shirts", passes: [{ keyword: "layered t shirt", intent: "layered" }, { keyword: "t shirt", intent: "fallback" }] },
  { key: "pants", passes: [{ keyword: "layered pants", intent: "layered" }, { keyword: "pants", intent: "fallback" }] },
  { key: "shorts", passes: [{ keyword: "layered shorts", intent: "layered" }, { keyword: "shorts", intent: "fallback" }] },
  { key: "dresses_skirts", passes: [{ keyword: "layered dress skirt", intent: "layered" }, { keyword: "dress skirt", intent: "fallback" }] },

  {
    key: "shoes",
    pagesPerPass: SHOES_PAGES_PER_PASS,
    passes: [
      { keyword: "layered shoes", intent: "layered" },
      { keyword: "shoe accessory", intent: "layered" },
      { keyword: "sneakers", intent: "layered" },
      { keyword: "heels", intent: "layered" },
      { keyword: "left shoe", intent: "layered" },
      { keyword: "right shoe", intent: "layered" },
      { keyword: "shoes", intent: "fallback" },
    ],
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

async function fetchAssetDetailsWithRetry(assetId, tries = 4) {
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

    if (res.ok) return res.json();

    if (res.status === 429) {
      const backoff = 900 * attempt;
      await sleep(backoff);
      continue;
    }

    return null;
  }

  return null;
}

async function fetchBundleDetailsWithRetry(bundleId, tries = 4) {
  const url = `https://catalog.roblox.com/v1/bundles/${bundleId}/details`;
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

    if (res.status === 429) {
      const backoff = 900 * attempt;
      await sleep(backoff);
      continue;
    }

    return null;
  }

  return null;
}

function extractAssetMeta(details) {
  if (!details) return { asset_type_id: null, asset_type_name: "" };
  return {
    asset_type_id: Number(details.AssetTypeId ?? details.assetTypeId ?? null) || null,
    asset_type_name: String(
      details.AssetType ??
        details.assetType ??
        details.AssetTypeName ??
        details.assetTypeName ??
        ""
    ),
  };
}

function normalizeSearchItem(raw) {
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

function normalizeEconomyAsset(details) {
  const creator = details.Creator || details.creator || {};
  const creatorIdRaw =
    creator.Id ??
    creator.id ??
    details.CreatorTargetId ??
    details.creatorTargetId ??
    null;

  const creatorId = Number.isFinite(Number(creatorIdRaw)) ? Number(creatorIdRaw) : null;
  const assetId = Number(details.AssetId ?? details.assetId ?? details.Id ?? details.id);
  const meta = extractAssetMeta(details);

  return {
    asset_id: Number.isFinite(assetId) ? assetId : null,
    name: String(details.Name ?? details.name ?? "Unknown"),
    description: String(details.Description ?? details.description ?? ""),
    creator_name: String(creator.Name ?? creator.name ?? details.CreatorName ?? details.creatorName ?? ""),
    creator_id: creatorId,
    creator_type: String(creator.CreatorType ?? creator.creatorType ?? details.CreatorType ?? details.creatorType ?? ""),
    item_type: String(details.AssetType ?? details.assetType ?? details.AssetTypeName ?? details.assetTypeName ?? ""),
    category: "clothing",
    thumbnail_url: "",
    is_offsale: !(details.IsForSale ?? details.isForSale ?? true),
    is_limited: Boolean(details.IsLimited ?? details.isLimited ?? false),
    is_limited_unique: Boolean(details.IsLimitedUnique ?? details.isLimitedUnique ?? false),
    price_robux: Number.isFinite(Number(details.PriceInRobux ?? details.priceInRobux))
      ? Number(details.PriceInRobux ?? details.priceInRobux)
      : null,
    asset_type_id: meta.asset_type_id,
    asset_type_name: meta.asset_type_name,
  };
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

    const details = await fetchAssetDetailsWithRetry(item.asset_id);
    const fetched = extractAssetMeta(details);
    memoryAssetTypeCache.set(key, fetched);
    item.asset_type_id = fetched.asset_type_id;
    item.asset_type_name = fetched.asset_type_name;

    await sleep(ASSET_META_DELAY_MS);
  }
}

async function upsertItem(item) {
  if (!Number.isFinite(item.asset_id)) return;

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

function isBundleRow(raw) {
  const itemType = String(raw.itemType || raw.assetType || raw.assetTypeName || "").toLowerCase();
  return itemType.includes("bundle");
}

function extractBundleAssetIds(bundleDetails) {
  const items = Array.isArray(bundleDetails?.items) ? bundleDetails.items : [];
  return items
    .filter((x) => String(x.type || "").toLowerCase() === "asset")
    .map((x) => Number(x.id))
    .filter((id) => Number.isFinite(id));
}

async function expandAndUpsertShoesFromBundle(raw) {
  const bundleId = Number(raw.id);
  if (!Number.isFinite(bundleId)) return 0;

  const bundleDetails = await fetchBundleDetailsWithRetry(bundleId);
  if (!bundleDetails) return 0;

  const childAssetIds = extractBundleAssetIds(bundleDetails);
  if (childAssetIds.length === 0) return 0;

  let inserted = 0;

  for (const childAssetId of childAssetIds) {
    const details = await fetchAssetDetailsWithRetry(childAssetId);
    const meta = extractAssetMeta(details);

    if (![SHOE_LEFT_TYPE, SHOE_RIGHT_TYPE].includes(Number(meta.asset_type_id))) {
      await sleep(ASSET_META_DELAY_MS);
      continue;
    }

    const item = normalizeEconomyAsset(details || {});
    if (!Number.isFinite(item.asset_id)) {
      await sleep(ASSET_META_DELAY_MS);
      continue;
    }

    await upsertItem(item);
    inserted += 1;
    await sleep(ASSET_META_DELAY_MS);
  }

  return inserted;
}

async function crawlPass(tabKey, passConfig, pagesPerPass) {
  let cursor = null;
  let pages = 0;
  let upserts = 0;
  let layeredMapped = 0;
  let shoeBundleChildrenInserted = 0;

  while (pages < pagesPerPass) {
    const url = buildUrl({ keyword: passConfig.keyword, cursor });
    const json = await fetchJsonWithRetry(url);
    const rows = Array.isArray(json.data) ? json.data : [];
    const items = rows.map(normalizeSearchItem).filter((i) => Number.isFinite(i.asset_id));

    await enrichAssetTypes(items);

    for (const item of items) {
      await upsertItem(item);
      upserts += 1;

      const t = Number(item.asset_type_id);
      if (t >= 64 && t <= 72) layeredMapped += 1;
    }

    if (tabKey === "shoes") {
      for (const raw of rows) {
        if (!isBundleRow(raw)) continue;
        const added = await expandAndUpsertShoesFromBundle(raw);
        shoeBundleChildrenInserted += added;
      }
    }

    pages += 1;
    cursor = json.nextPageCursor || null;
    if (!cursor) break;

    await sleep(DELAY_MS);
  }

  console.log(
    `[crawl] ${tabKey} | keyword="${passConfig.keyword}" | intent=${passConfig.intent || "unknown"} | pages=${pages}, upserts=${upserts}, layeredMapped=${layeredMapped}, shoeBundleChildrenInserted=${shoeBundleChildrenInserted}`
  );
}

async function main() {
  try {
    await pool.query("SELECT 1");
    console.log("DB connected");

    await ensureSchema();

    for (const tab of CRAWL_PLAN) {
      const pagesPerPass = Number(tab.pagesPerPass || MAX_PAGES_PER_PASS);

      for (const pass of tab.passes) {
        await crawlPass(tab.key, pass, pagesPerPass);
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