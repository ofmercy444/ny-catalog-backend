require("dotenv").config();
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl:
    process.env.DATABASE_URL && process.env.DATABASE_URL.includes("localhost")
      ? false
      : { rejectUnauthorized: false },
});

const CLOTHING_CATEGORY = 3;
const ACCESSORIES_CATEGORY = 11;

const PAGE_LIMIT = clampLimit(Number(process.env.CRAWL_PAGE_LIMIT || 30));

const MAX_CLOTHING_PAGES_PER_PASS = Number(
  process.env.CRAWL_PAGES_PER_CLOTHING_PASS ??
    process.env.CRAWL_PAGES_PER_SUBTAB ??
    0
);
const MAX_ACCESSORY_PAGES_PER_PASS = Number(
  process.env.CRAWL_PAGES_PER_ACCESSORY_PASS ??
    process.env.CRAWL_PAGES_PER_SUBTAB ??
    0
);

const SHOE_BUNDLE_PAGES = Number(process.env.CRAWL_SHOE_BUNDLE_PAGES || 0);

const DELAY_MS = Number(process.env.CRAWL_DELAY_MS || 9000);
const ASSET_META_DELAY_MS = Number(process.env.CRAWL_ASSET_META_DELAY_MS || 1400);
const INCLUDE_NOT_FOR_SALE = String(process.env.INCLUDE_NOT_FOR_SALE || "true") === "true";

const SEARCH_RETRIES = Number(process.env.CRAWL_SEARCH_RETRIES || 2);
const DETAIL_RETRIES = Number(process.env.CRAWL_DETAIL_RETRIES || 2);
const RETRY_BASE_MS = Number(process.env.CRAWL_RETRY_BASE_MS || 1200);
const MAX_RETRY_BACKOFF_MS = Number(process.env.CRAWL_MAX_RETRY_BACKOFF_MS || 12000);
const RATE_LIMIT_STREAK_TRIGGER = Number(process.env.CRAWL_RATE_LIMIT_STREAK_TRIGGER || 10);
const RATE_LIMIT_COOLDOWN_MS = Number(process.env.CRAWL_RATE_LIMIT_COOLDOWN_MS || 240000);

const MAX_META_LOOKUPS_PER_PASS = Number(process.env.CRAWL_MAX_META_LOOKUPS_PER_PASS || 80);

const MAX_CLOTHING_TERMS_PER_TAB = Number(process.env.CRAWL_MAX_CLOTHING_TERMS_PER_TAB || 8);
const MAX_ACCESSORY_TERMS_PER_TYPE = Number(process.env.CRAWL_MAX_ACCESSORY_TERMS_PER_TYPE || 8);
const ROTATION_HOURS = Number(process.env.CRAWL_ROTATION_HOURS || 2);

// Clothing-accessory canonical mapping
const TYPE_TO_GROUP = {
  2: { category: "clothing", subcategory: "classic_t_shirts" },
  11: { category: "clothing", subcategory: "classic_shirts" },
  12: { category: "clothing", subcategory: "classic_pants" },

  64: { category: "clothing", subcategory: "t_shirts" },
  65: { category: "clothing", subcategory: "shirts" },
  66: { category: "clothing", subcategory: "pants" },
  67: { category: "clothing", subcategory: "jackets" },
  68: { category: "clothing", subcategory: "sweaters" },
  69: { category: "clothing", subcategory: "shorts" },
  70: { category: "clothing", subcategory: "left_shoe" },
  71: { category: "clothing", subcategory: "right_shoe" },
  72: { category: "clothing", subcategory: "dresses_skirts" },

  8: { category: "accessories", subcategory: "head" },
  41: { category: "accessories", subcategory: "hair" },
  42: { category: "accessories", subcategory: "face" },
  43: { category: "accessories", subcategory: "neck" },
  44: { category: "accessories", subcategory: "shoulder" },
  45: { category: "accessories", subcategory: "front" },
  46: { category: "accessories", subcategory: "back" },
  47: { category: "accessories", subcategory: "waist" },
};

const ASSET_TYPE_NAME_TO_ID = {
  tshirt: 2,
  shirt: 11,
  pants: 12,
  tshirtaccessory: 64,
  shirtaccessory: 65,
  pantsaccessory: 66,
  jacketaccessory: 67,
  sweateraccessory: 68,
  shortsaccessory: 69,
  leftshoeaccessory: 70,
  rightshoeaccessory: 71,
  dressskirtaccessory: 72,
  hat: 8,
  hairaccessory: 41,
  faceaccessory: 42,
  neckaccessory: 43,
  shoulderaccessory: 44,
  frontaccessory: 45,
  backaccessory: 46,
  waistaccessory: 47,
};

const CLOTHING_TERMS = {
  all: [
    "shirt",
    "pants",
    "jacket",
    "sweater",
    "hoodie",
    "t-shirt",
    "dress",
    "skirt",
    "streetwear",
    "y2k",
  ],
};

const ACCESSORY_TERMS_BY_TYPE = {
  8: ["hat", "cap", "beanie", "crown"],
  41: ["hair", "hairstyle", "wig", "ponytail", "braid", "bangs"],
  42: ["face accessory", "mask", "glasses"],
  43: ["necklace", "choker", "scarf"],
  44: ["shoulder pet", "pauldron"],
  45: ["front accessory", "chain"],
  46: ["back accessory", "wings", "cape", "backpack"],
  47: ["waist accessory", "belt", "tail"],
};

const SHOE_BUNDLE_TERMS = [
  "shoes",
  "heels",
  "boots",
  "sneakers",
  "sandals",
  "loafer",
];

let consecutive429 = 0;

function clampLimit(n) {
  return n === 10 || n === 28 || n === 30 ? n : 30;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function jitter(max = 500) {
  return Math.floor(Math.random() * max);
}

function normalizeTypeName(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "");
}

function asNumber(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
}

function parseAssetTypeId(item = {}) {
  if (!item || typeof item !== "object") return null;
  const fromAssetType = asNumber(item.assetType);
  if (fromAssetType !== null) return fromAssetType;
  const fromAssetTypeId = asNumber(item.assetTypeId);
  if (fromAssetTypeId !== null) return fromAssetTypeId;

  const name = normalizeTypeName(item.assetTypeName || item.assetTypeDisplayName || "");
  return ASSET_TYPE_NAME_TO_ID[name] ?? null;
}

function classifyByType(assetTypeId) {
  return TYPE_TO_GROUP[Number(assetTypeId)] || { category: "unknown", subcategory: "unknown" };
}

function hashString(s) {
  let h = 2166136261;
  for (let i = 0; i < s.length; i += 1) {
    h ^= s.charCodeAt(i);
    h += (h << 1) + (h << 4) + (h << 7) + (h << 8) + (h << 24);
  }
  return h >>> 0;
}

function limitedRotatedTerms(source, limit, seed, laneKey) {
  const arr = Array.from(new Set((source || []).map((s) => String(s).trim()).filter(Boolean)));
  if (arr.length === 0 || limit <= 0) return [];
  const offset = Math.abs(hashString(`${seed}:${laneKey}`)) % arr.length;
  const rotated = arr.slice(offset).concat(arr.slice(0, offset));
  return rotated.slice(0, Math.min(limit, rotated.length));
}

async function fetchJsonWithRetry(url, tries, label) {
  const maxAttempts = Math.max(1, Number(tries) || 0);

  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const res = await fetch(url, {
        headers: {
          "User-Agent": "ny-catalog-backend/clothing-accessories-crawler",
          Accept: "application/json",
        },
      });

      if (res.ok) {
        consecutive429 = 0;
        return await res.json();
      }

      if (res.status === 429) {
        consecutive429 += 1;
        const waitMs = Math.min(MAX_RETRY_BACKOFF_MS, RETRY_BASE_MS * attempt * attempt) + jitter(700);
        console.log(`[429] ${label} retry ${attempt}/${maxAttempts} in ${waitMs}ms`);
        await sleep(waitMs);
        if (consecutive429 >= RATE_LIMIT_STREAK_TRIGGER) {
          console.log(`[rate-limit] cooldown ${RATE_LIMIT_COOLDOWN_MS}ms`);
          consecutive429 = 0;
          await sleep(RATE_LIMIT_COOLDOWN_MS);
        }
        continue;
      }

      if (res.status === 400) return null;

      if (res.status >= 500 && attempt < maxAttempts) {
        const waitMs = Math.min(MAX_RETRY_BACKOFF_MS, RETRY_BASE_MS * attempt) + jitter(600);
        await sleep(waitMs);
        continue;
      }
      return null;
    } catch {
      if (attempt >= maxAttempts) return null;
      const waitMs = Math.min(MAX_RETRY_BACKOFF_MS, RETRY_BASE_MS * attempt) + jitter(600);
      await sleep(waitMs);
    }
  }

  return null;
}

function buildSearchUrl({ category, subcategory, keyword, cursor }) {
  const p = new URLSearchParams();
  p.set("Category", String(category));
  p.set("Limit", String(PAGE_LIMIT));
  p.set("SortType", "3");
  p.set("IncludeNotForSale", INCLUDE_NOT_FOR_SALE ? "true" : "false");
  if (subcategory !== undefined && subcategory !== null) p.set("Subcategory", String(subcategory));
  if (keyword) p.set("Keyword", String(keyword));
  if (cursor) p.set("Cursor", String(cursor));
  return `https://catalog.roblox.com/v1/search/items/details?${p.toString()}`;
}

function buildBundleSearchUrl({ keyword, cursor }) {
  const p = new URLSearchParams();
  p.set("Limit", String(PAGE_LIMIT));
  p.set("SortType", "3");
  if (keyword) p.set("Keyword", String(keyword));
  if (cursor) p.set("Cursor", String(cursor));
  return `https://catalog.roblox.com/v1/search/bundles/details?${p.toString()}`;
}

async function fetchAssetDetails(assetId) {
  const url = `https://economy.roblox.com/v2/assets/${assetId}/details`;
  return fetchJsonWithRetry(url, DETAIL_RETRIES, `asset-detail:${assetId}`);
}

async function fetchBundleDetails(bundleId) {
  const url = `https://catalog.roblox.com/v1/bundles/${bundleId}/details`;
  return fetchJsonWithRetry(url, DETAIL_RETRIES, `bundle-detail:${bundleId}`);
}

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.catalog_items (
      id BIGSERIAL PRIMARY KEY,
      asset_id BIGINT UNIQUE NOT NULL,
      name TEXT,
      description TEXT,
      creator_id BIGINT,
      creator_name TEXT,
      creator_type TEXT,
      creator_target_id BIGINT,
      creator_shop_name TEXT,
      price_robux INTEGER,
      is_for_sale BOOLEAN,
      is_limited BOOLEAN,
      is_limited_unique BOOLEAN,
      remaining INTEGER,
      lowest_price INTEGER,
      units_available_for_consumption INTEGER,
      asset_type_id INTEGER,
      asset_type_name TEXT,
      category TEXT,
      subcategory TEXT,
      item_url TEXT,
      thumbnail_url TEXT,
      raw JSONB DEFAULT '{}'::jsonb,
      first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.catalog_bundles (
      bundle_id BIGINT PRIMARY KEY,
      name TEXT,
      description TEXT,
      creator_name TEXT,
      creator_id BIGINT,
      creator_type TEXT,
      bundle_type TEXT,
      thumbnail_url TEXT,
      category TEXT,
      subcategory TEXT,
      is_offsale BOOLEAN DEFAULT FALSE,
      price_robux INTEGER,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.bundle_asset_links (
      bundle_id BIGINT NOT NULL,
      asset_id BIGINT NOT NULL,
      role TEXT,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (bundle_id, asset_id)
    );
  `);
}

function mapCommonFields(item, detail, resolvedTypeId, resolvedTypeName) {
  const creator = detail?.creator || item?.creator || {};
  const creatorName = creator?.name || item?.creatorName || "";
  const creatorId = asNumber(creator?.id ?? item?.creatorTargetId);
  const creatorType = creator?.type || item?.creatorType || "";
  const creatorTargetId = asNumber(item?.creatorTargetId ?? creator?.id);

  const name = item?.name || detail?.name || "";
  const description = item?.description || detail?.description || "";
  const itemUrl = item?.itemUrl || (item?.id ? `https://www.roblox.com/catalog/${item.id}` : null);
  const thumbnail = item?.thumbnailUrl || item?.thumbnail || null;

  const typeId = resolvedTypeId ?? parseAssetTypeId(item) ?? parseAssetTypeId(detail) ?? null;
  const typeName =
    resolvedTypeName ||
    item?.assetTypeName ||
    detail?.assetType ||
    detail?.assetTypeName ||
    null;

  const grouping = classifyByType(typeId);

  return {
    asset_id: asNumber(item?.id ?? detail?.assetId),
    name,
    description,
    creator_id: creatorId,
    creator_name: creatorName,
    creator_type: creatorType,
    creator_target_id: creatorTargetId,
    creator_shop_name: creatorName,
    price_robux: asNumber(item?.price ?? detail?.priceInRobux),
    is_for_sale:
      typeof item?.isForSale === "boolean"
        ? item.isForSale
        : typeof detail?.isForSale === "boolean"
        ? detail.isForSale
        : null,
    is_limited:
      typeof item?.isLimited === "boolean"
        ? item.isLimited
        : typeof detail?.isLimited === "boolean"
        ? detail.isLimited
        : null,
    is_limited_unique:
      typeof item?.isLimitedUnique === "boolean"
        ? item.isLimitedUnique
        : typeof detail?.isLimitedUnique === "boolean"
        ? detail.isLimitedUnique
        : null,
    remaining: asNumber(item?.unitsAvailableForConsumption ?? detail?.remaining),
    lowest_price: asNumber(item?.lowestPrice ?? detail?.lowestPrice),
    units_available_for_consumption: asNumber(item?.unitsAvailableForConsumption),
    asset_type_id: typeId,
    asset_type_name: typeName,
    category: grouping.category,
    subcategory: grouping.subcategory,
    item_url: itemUrl,
    thumbnail_url: thumbnail,
    raw: { item, detail },
  };
}

async function upsertCatalogItem(record) {
  if (!record?.asset_id) return false;
  await pool.query(
    `
    INSERT INTO public.catalog_items (
      asset_id, name, description,
      creator_id, creator_name, creator_type, creator_target_id, creator_shop_name,
      price_robux, is_for_sale, is_limited, is_limited_unique, remaining, lowest_price,
      units_available_for_consumption,
      asset_type_id, asset_type_name, category, subcategory,
      item_url, thumbnail_url, raw, updated_at
    )
    VALUES (
      $1,$2,$3,
      $4,$5,$6,$7,$8,
      $9,$10,$11,$12,$13,$14,
      $15,
      $16,$17,$18,$19,
      $20,$21,$22,NOW()
    )
    ON CONFLICT (asset_id) DO UPDATE SET
      name = EXCLUDED.name,
      description = EXCLUDED.description,
      creator_id = EXCLUDED.creator_id,
      creator_name = EXCLUDED.creator_name,
      creator_type = EXCLUDED.creator_type,
      creator_target_id = EXCLUDED.creator_target_id,
      creator_shop_name = EXCLUDED.creator_shop_name,
      price_robux = EXCLUDED.price_robux,
      is_for_sale = EXCLUDED.is_for_sale,
      is_limited = EXCLUDED.is_limited,
      is_limited_unique = EXCLUDED.is_limited_unique,
      remaining = EXCLUDED.remaining,
      lowest_price = EXCLUDED.lowest_price,
      units_available_for_consumption = EXCLUDED.units_available_for_consumption,
      asset_type_id = EXCLUDED.asset_type_id,
      asset_type_name = EXCLUDED.asset_type_name,
      category = EXCLUDED.category,
      subcategory = EXCLUDED.subcategory,
      item_url = EXCLUDED.item_url,
      thumbnail_url = EXCLUDED.thumbnail_url,
      raw = EXCLUDED.raw,
      updated_at = NOW()
    `,
    [
      record.asset_id,
      record.name,
      record.description,
      record.creator_id,
      record.creator_name,
      record.creator_type,
      record.creator_target_id,
      record.creator_shop_name,
      record.price_robux,
      record.is_for_sale,
      record.is_limited,
      record.is_limited_unique,
      record.remaining,
      record.lowest_price,
      record.units_available_for_consumption,
      record.asset_type_id,
      record.asset_type_name,
      record.category,
      record.subcategory,
      record.item_url,
      record.thumbnail_url,
      JSON.stringify(record.raw || {}),
    ]
  );
  return true;
}

async function upsertCatalogBundle(record) {
  if (!record?.bundle_id) return false;
  await pool.query(
    `
    INSERT INTO public.catalog_bundles (
      bundle_id, name, description, creator_name, creator_id, creator_type,
      bundle_type, thumbnail_url, category, subcategory, is_offsale, price_robux, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,NOW())
    ON CONFLICT (bundle_id) DO UPDATE SET
      name = EXCLUDED.name,
      description = EXCLUDED.description,
      creator_name = EXCLUDED.creator_name,
      creator_id = EXCLUDED.creator_id,
      creator_type = EXCLUDED.creator_type,
      bundle_type = EXCLUDED.bundle_type,
      thumbnail_url = EXCLUDED.thumbnail_url,
      category = EXCLUDED.category,
      subcategory = EXCLUDED.subcategory,
      is_offsale = EXCLUDED.is_offsale,
      price_robux = EXCLUDED.price_robux,
      updated_at = NOW()
    `,
    [
      record.bundle_id,
      record.name,
      record.description,
      record.creator_name,
      record.creator_id,
      record.creator_type,
      record.bundle_type,
      record.thumbnail_url,
      record.category,
      record.subcategory,
      record.is_offsale,
      record.price_robux,
    ]
  );
  return true;
}

async function upsertBundleAssetLink(bundleId, assetId, role) {
  if (!bundleId || !assetId) return;
  await pool.query(
    `
    INSERT INTO public.bundle_asset_links (bundle_id, asset_id, role, updated_at)
    VALUES ($1,$2,$3,NOW())
    ON CONFLICT (bundle_id, asset_id) DO UPDATE SET
      role = EXCLUDED.role,
      updated_at = NOW()
    `,
    [bundleId, assetId, role || null]
  );
}

async function processSearchPage(items, metaLookupBudget) {
  let seen = 0;
  let upserts = 0;
  let forcedMetaLookups = 0;

  for (const item of items || []) {
    seen += 1;

    let resolvedTypeId = parseAssetTypeId(item);
    let resolvedTypeName = item?.assetTypeName || null;
    let detail = null;

    if (!resolvedTypeId && metaLookupBudget.count < metaLookupBudget.max) {
      detail = await fetchAssetDetails(item.id);
      metaLookupBudget.count += 1;
      forcedMetaLookups += 1;
      resolvedTypeId = parseAssetTypeId(detail);
      resolvedTypeName = detail?.assetType || detail?.assetTypeName || resolvedTypeName;
      await sleep(ASSET_META_DELAY_MS + jitter(300));
    }

    const mapped = mapCommonFields(item, detail, resolvedTypeId, resolvedTypeName);
    if (mapped.category === "clothing" || mapped.category === "accessories") {
      if (await upsertCatalogItem(mapped)) upserts += 1;
    }
  }

  return { seen, upserts, forcedMetaLookups };
}

async function crawlSearchLane({ laneLabel, category, keyword = null, maxPages = 1, maxMetaLookups = MAX_META_LOOKUPS_PER_PASS }) {
  let cursor = null;
  let pages = 0;
  let totalSeen = 0;
  let totalUpserts = 0;
  const budget = { count: 0, max: Math.max(0, maxMetaLookups) };

  while (pages < maxPages) {
    const url = buildSearchUrl({ category, keyword, cursor });
    const data = await fetchJsonWithRetry(url, SEARCH_RETRIES, laneLabel);
    if (!data || !Array.isArray(data.data)) break;

    const { seen, upserts } = await processSearchPage(data.data, budget);
    pages += 1;
    totalSeen += seen;
    totalUpserts += upserts;

    console.log(`[crawl] ${laneLabel} pages=${pages} seen=${seen} upserts=${upserts}`);
    cursor = data.nextPageCursor || null;
    if (!cursor) break;
    await sleep(DELAY_MS + jitter(500));
  }

  console.log(`[crawl] ${laneLabel} totalSeen=${totalSeen} totalUpserts=${totalUpserts}`);
}

async function crawlShoeBundles(runSeed) {
  if (SHOE_BUNDLE_PAGES <= 0) {
    console.log("[crawl-bundles] skipped (CRAWL_SHOE_BUNDLE_PAGES=0)");
    return;
  }

  const terms = limitedRotatedTerms(
    SHOE_BUNDLE_TERMS,
    SHOE_BUNDLE_TERMS.length,
    runSeed,
    "shoe-bundles"
  );

  for (const kw of terms) {
    let cursor = null;
    let pages = 0;

    while (pages < SHOE_BUNDLE_PAGES) {
      const data = await fetchJsonWithRetry(buildBundleSearchUrl({ keyword: kw, cursor }), SEARCH_RETRIES, `shoe-bundle:${kw}`);
      if (!data || !Array.isArray(data.data)) break;

      for (const b of data.data) {
        const bundleId = asNumber(b?.id);
        if (!bundleId) continue;

        const name = String(b?.name || "");
        if (!/shoe|boot|heel|sneaker|sandal|loafer/i.test(name)) continue;

        const creator = b?.creator || {};
        await upsertCatalogBundle({
          bundle_id: bundleId,
          name,
          description: String(b?.description || ""),
          creator_name: String(creator?.name || b?.creatorName || ""),
          creator_id: asNumber(creator?.id || b?.creatorId),
          creator_type: String(creator?.type || b?.creatorType || ""),
          bundle_type: String(b?.bundleType || ""),
          thumbnail_url: "",
          category: "clothing",
          subcategory: "shoes",
          is_offsale: false,
          price_robux: asNumber(b?.price || b?.product?.priceInRobux),
        });

        const details = await fetchBundleDetails(bundleId);
        const items = Array.isArray(details?.items) ? details.items : [];
        for (const bi of items) {
          const assetId = asNumber(bi?.id ?? bi?.assetId);
          if (!assetId) continue;
          const t = asNumber(bi?.type ?? bi?.assetType ?? bi?.assetTypeId);
          let role = null;
          if (t === 70) role = "left_shoe";
          if (t === 71) role = "right_shoe";
          await upsertBundleAssetLink(bundleId, assetId, role);
        }
        await sleep(ASSET_META_DELAY_MS + jitter(300));
      }

      pages += 1;
      cursor = data.nextPageCursor || null;
      if (!cursor) break;
      await sleep(DELAY_MS + jitter(500));
    }
  }
}

function buildPlans(runSeed) {
  const clothingPlan = [];
  if (MAX_CLOTHING_PAGES_PER_PASS > 0) {
    const terms = limitedRotatedTerms(
      CLOTHING_TERMS.all,
      MAX_CLOTHING_TERMS_PER_TAB,
      runSeed,
      "clothing:all"
    );
    for (const kw of terms) {
      clothingPlan.push({
        laneLabel: `clothing:${kw}`,
        category: CLOTHING_CATEGORY,
        keyword: kw,
      });
    }
  }

  const accessoryPlan = [];
  if (MAX_ACCESSORY_PAGES_PER_PASS > 0) {
    for (const [typeIdStr, terms] of Object.entries(ACCESSORY_TERMS_BY_TYPE)) {
      const typeId = Number(typeIdStr);
      const rotated = limitedRotatedTerms(
        terms,
        MAX_ACCESSORY_TERMS_PER_TYPE,
        runSeed,
        `accessories:${typeId}`
      );

      for (const kw of rotated) {
        accessoryPlan.push({
          laneLabel: `accessories:type${typeId}:${kw}`,
          category: ACCESSORIES_CATEGORY,
          keyword: kw,
        });
      }
    }
  }
  return { clothingPlan, accessoryPlan };
}

async function main() {
  try {
    console.log("[startup] crawler config", {
      CRAWL_PAGE_LIMIT_raw: process.env.CRAWL_PAGE_LIMIT,
      CRAWL_PAGES_PER_CLOTHING_PASS_raw: process.env.CRAWL_PAGES_PER_CLOTHING_PASS,
      CRAWL_PAGES_PER_ACCESSORY_PASS_raw: process.env.CRAWL_PAGES_PER_ACCESSORY_PASS,
      CRAWL_SHOE_BUNDLE_PAGES_raw: process.env.CRAWL_SHOE_BUNDLE_PAGES,
      CRAWL_DELAY_MS_raw: process.env.CRAWL_DELAY_MS,
      INCLUDE_NOT_FOR_SALE,
    });

    await pool.query("SELECT 1");
    await ensureSchema();
    const runSeed = String(Math.floor(Date.now() / (1000 * 60 * 60 * ROTATION_HOURS)));

    const { clothingPlan, accessoryPlan } = buildPlans(runSeed);

    if (MAX_CLOTHING_PAGES_PER_PASS > 0) {
      for (const pass of clothingPlan) {
        await crawlSearchLane({
          laneLabel: pass.laneLabel,
          category: pass.category,
          keyword: pass.keyword,
          maxPages: MAX_CLOTHING_PAGES_PER_PASS,
          maxMetaLookups: MAX_META_LOOKUPS_PER_PASS,
        });
        await sleep(DELAY_MS + jitter(500));
      }
    } else {
      console.log("[crawl] clothing passes skipped (MAX_CLOTHING_PAGES_PER_PASS=0)");
    }

    if (MAX_ACCESSORY_PAGES_PER_PASS > 0) {
      for (const pass of accessoryPlan) {
        await crawlSearchLane({
          laneLabel: pass.laneLabel,
          category: pass.category,
          keyword: pass.keyword,
          maxPages: MAX_ACCESSORY_PAGES_PER_PASS,
          maxMetaLookups: MAX_META_LOOKUPS_PER_PASS,
        });
        await sleep(DELAY_MS + jitter(500));
      }
    } else {
      console.log("[crawl] accessory passes skipped (MAX_ACCESSORY_PAGES_PER_PASS=0)");
    }

    await crawlShoeBundles(runSeed);
    console.log("Crawl complete");
    process.exit(0);
  } catch (err) {
    console.error("crawl failed:", err);
    process.exit(1);
  }
}

main();