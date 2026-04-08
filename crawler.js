require("dotenv").config();
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl:
    process.env.DATABASE_URL && process.env.DATABASE_URL.includes("localhost")
      ? false
      : { rejectUnauthorized: false },
});

// Roblox catalog categories used by this crawler endpoint
const CLOTHING_CATEGORY = 3;
const ACCESSORIES_CATEGORY = 11;
const AVATAR_ANIMATIONS_CATEGORY = 12;
const COMMUNITY_CREATIONS_CATEGORY = 13;

// IMPORTANT: category 4 (BodyParts) is not reliably supported by this endpoint for keyword lanes.
// We discover body items through supported categories and classify by asset_type_id.
const BODY_DISCOVERY_CATEGORIES = [ACCESSORIES_CATEGORY, AVATAR_ANIMATIONS_CATEGORY, CLOTHING_CATEGORY];

const CATEGORY_MATRIX = [CLOTHING_CATEGORY, ACCESSORIES_CATEGORY, AVATAR_ANIMATIONS_CATEGORY, COMMUNITY_CREATIONS_CATEGORY];
const SUBCATEGORY_MATRIX = [
  1, 3, 4, 9, 10, 12, 13, 14, 15, 19, 20, 21, 22, 23, 24, 25, 26, 27,
  37, 38, 39, 40, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66,
];

const PAGE_LIMIT = clampLimit(Number(process.env.CRAWL_PAGE_LIMIT || 30));
const MATRIX_PAGES_PER_PAIR = Number(process.env.CRAWL_MATRIX_PAGES_PER_PAIR || 0);

const MAX_CLOTHING_PAGES_PER_PASS = Number(
  process.env.CRAWL_PAGES_PER_CLOTHING_PASS ??
    process.env.CRAWL_PAGES_PER_SUBTAB ??
    0
);
const MAX_BODY_PAGES_PER_PASS = Number(
  process.env.CRAWL_PAGES_PER_BODY_PASS ??
    process.env.CRAWL_PAGES_PER_SUBTAB ??
    0
);
const MAX_ACCESSORY_PAGES_PER_PASS = Number(
  process.env.CRAWL_PAGES_PER_ACCESSORY_PASS ??
    process.env.CRAWL_PAGES_PER_SUBTAB ??
    0
);
const MAX_ACCESSORY_SUBCATEGORY_PAGES_PER_PASS = Number(
  process.env.CRAWL_PAGES_PER_ACCESSORY_SUBCATEGORY_PASS || 1
);

const SHOE_BUNDLE_PAGES = Number(process.env.CRAWL_SHOE_BUNDLE_PAGES || 0);

const DELAY_MS = Number(process.env.CRAWL_DELAY_MS || 9000);
const ASSET_META_DELAY_MS = Number(process.env.CRAWL_ASSET_META_DELAY_MS || 1400);
const INCLUDE_NOT_FOR_SALE = String(process.env.INCLUDE_NOT_FOR_SALE || "true") === "true";

const SEARCH_RETRIES = Number(process.env.CRAWL_SEARCH_RETRIES || 1);
const DETAIL_RETRIES = Number(process.env.CRAWL_DETAIL_RETRIES || 3);
const RETRY_BASE_MS = Number(process.env.CRAWL_RETRY_BASE_MS || 1200);
const MAX_RETRY_BACKOFF_MS = Number(process.env.CRAWL_MAX_RETRY_BACKOFF_MS || 12000);
const RATE_LIMIT_STREAK_TRIGGER = Number(process.env.CRAWL_RATE_LIMIT_STREAK_TRIGGER || 14);
const RATE_LIMIT_COOLDOWN_MS = Number(process.env.CRAWL_RATE_LIMIT_COOLDOWN_MS || 240000);

const MAX_META_LOOKUPS_PER_PASS = Number(process.env.CRAWL_MAX_META_LOOKUPS_PER_PASS || 80);
const MAX_MATRIX_META_LOOKUPS_PER_PAIR = Number(
  process.env.CRAWL_MAX_MATRIX_META_LOOKUPS_PER_PAIR || 40
);

const MAX_CLOTHING_TERMS_PER_TAB = Number(process.env.CRAWL_MAX_CLOTHING_TERMS_PER_TAB || 2);
const MAX_BODY_TERMS_PER_TAB = Number(process.env.CRAWL_MAX_BODY_TERMS_PER_TAB || 8);
const MAX_ACCESSORY_TERMS_PER_TYPE = Number(process.env.CRAWL_MAX_ACCESSORY_TERMS_PER_TYPE || 4);

const ROTATION_HOURS = Number(process.env.CRAWL_ROTATION_HOURS || 2);
const ACCESSORY_SUBCATEGORY_SWEEP_IDS = String(
  process.env.CRAWL_ACCESSORY_SUBCATEGORY_SWEEP_IDS || "20"
)
  .split(",")
  .map((s) => Number(String(s).trim()))
  .filter((n) => Number.isFinite(n) && n > 0);

const TYPE_TO_GROUP = {
  // Classic clothing
  2: { category: "clothing", subcategory: "tops" },
  11: { category: "clothing", subcategory: "shirts" },
  12: { category: "clothing", subcategory: "pants" },

  // Layered clothing / clothing accessories
  64: { category: "clothing", subcategory: "jackets" },
  65: { category: "clothing", subcategory: "sweaters" },
  66: { category: "clothing", subcategory: "shorts" },
  67: { category: "clothing", subcategory: "left_shoe" },
  68: { category: "clothing", subcategory: "right_shoe" },
  69: { category: "clothing", subcategory: "dress_skirt" },
  70: { category: "clothing", subcategory: "left_shoe" },
  71: { category: "clothing", subcategory: "right_shoe" },
  72: { category: "clothing", subcategory: "tops" },

  // Accessories
  8: { category: "accessories", subcategory: "head" },
  41: { category: "accessories", subcategory: "hair" },
  42: { category: "accessories", subcategory: "face" },
  43: { category: "accessories", subcategory: "neck" },
  44: { category: "accessories", subcategory: "shoulder" },
  45: { category: "accessories", subcategory: "front" },
  46: { category: "accessories", subcategory: "back" },
  47: { category: "accessories", subcategory: "waist" },

  // Body-related canonical types
  17: { category: "body", subcategory: "heads" },
  18: { category: "body", subcategory: "faces" },
  27: { category: "body", subcategory: "bodies" },
  28: { category: "body", subcategory: "bodies" },
  29: { category: "body", subcategory: "bodies" },
  30: { category: "body", subcategory: "bodies" },
  31: { category: "body", subcategory: "bodies" },
  48: { category: "body", subcategory: "animations" },
  49: { category: "body", subcategory: "animations" },
  50: { category: "body", subcategory: "animations" },
  51: { category: "body", subcategory: "animations" },
  52: { category: "body", subcategory: "animations" },
  53: { category: "body", subcategory: "animations" },
  54: { category: "body", subcategory: "animations" },
  55: { category: "body", subcategory: "animations" },
  56: { category: "body", subcategory: "animations" },
  61: { category: "body", subcategory: "animations" },
  79: { category: "body", subcategory: "heads" },
};

const ASSET_TYPE_NAME_TO_ID = {
  shirt: 11,
  pants: 12,
  tshirt: 2,
  tshirtaccessory: 72,
  jacketaccessory: 64,
  sweateraccessory: 65,
  shortsaccessory: 66,
  leftshoeaccessory: 70,
  rightshoeaccessory: 71,
  dressskirtaccessory: 69,
  hat: 8,
  hairaccessory: 41,
  faceaccessory: 42,
  neckaccessory: 43,
  shoulderaccessory: 44,
  frontaccessory: 45,
  backaccessory: 46,
  waistaccessory: 47,
  head: 17,
  face: 18,
  torso: 27,
  rightarm: 28,
  leftarm: 29,
  leftleg: 30,
  rightleg: 31,
  dynamichead: 79,
};

const CLOTHING_TERMS = {
  all: [
    "y2k",
    "streetwear",
    "jacket",
    "hoodie",
    "sweater",
    "shirt",
    "pants",
    "dress",
    "skirt",
    "heels",
    "boots",
    "sneakers",
  ],
};

const BODY_TERMS = {
  all: ["body", "avatar body", "character body", "rthro", "dynamic head", "head", "face"],
  heads: ["head", "dynamic head", "anime head", "stylized head"],
  bodies: ["torso", "arms", "legs", "body", "rthro body"],
  animations: ["animation", "idle animation", "walk animation", "run animation", "emote animation"],
  hair: ["hair", "hairstyle", "wig", "ponytail", "braid", "curly hair", "wavy hair"],
};

// Temporary focus mode: dedicate accessory discovery budget to HairAccessory (41).
const ACCESSORY_TERMS_BY_TYPE = {
  41: [
    "hair",
    "hairstyle",
    "hair accessory",
    "wig",
    "ponytail",
    "pigtails",
    "braid",
    "bob",
    "pixie",
    "long hair",
    "short hair",
    "curly hair",
    "wavy hair",
    "straight hair",
    "bangs",
    "fringe",
  ],
};

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
          "User-Agent": "ny-catalog-backend/full-crawl",
          Accept: "application/json",
        },
      });

      if (res.ok) {
        consecutive429 = 0;
        return await res.json();
      }

      if (res.status === 429) {
        consecutive429 += 1;
        const waitMs =
          Math.min(MAX_RETRY_BACKOFF_MS, RETRY_BASE_MS * attempt * attempt) + jitter(700);

        console.log(`[429] ${label} retry ${attempt}/${maxAttempts} in ${waitMs}ms -> ${url}`);
        await sleep(waitMs);

        if (consecutive429 >= RATE_LIMIT_STREAK_TRIGGER) {
          console.log(
            `[rate-limit] cooldown ${RATE_LIMIT_COOLDOWN_MS}ms after streak=${consecutive429}`
          );
          consecutive429 = 0;
          await sleep(RATE_LIMIT_COOLDOWN_MS);
        }
        continue;
      }

      if (res.status === 400) {
        const text = await res.text().catch(() => "");
        console.log(`[400] ${label} give up -> ${url} ${text.slice(0, 220)}`);
        return null;
      }

      if (res.status >= 500 && attempt < maxAttempts) {
        const waitMs = Math.min(MAX_RETRY_BACKOFF_MS, RETRY_BASE_MS * attempt) + jitter(600);
        console.log(`[${res.status}] ${label} retry ${attempt}/${maxAttempts} in ${waitMs}ms`);
        await sleep(waitMs);
        continue;
      }

      const text = await res.text().catch(() => "");
      console.log(`[${res.status}] ${label} give up -> ${url} ${text.slice(0, 220)}`);
      return null;
    } catch (err) {
      if (attempt >= maxAttempts) {
        console.log(`[error] ${label} give up -> ${url} :: ${err.message}`);
        return null;
      }

      const waitMs = Math.min(MAX_RETRY_BACKOFF_MS, RETRY_BASE_MS * attempt) + jitter(600);
      console.log(`[error] ${label} retry ${attempt}/${maxAttempts} in ${waitMs}ms -> ${err.message}`);
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

async function fetchAssetDetails(assetId) {
  const url = `https://economy.roblox.com/v2/assets/${assetId}/details`;
  return fetchJsonWithRetry(url, DETAIL_RETRIES, `asset-detail:${assetId}`);
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
    CREATE INDEX IF NOT EXISTS idx_catalog_items_category_subcategory
    ON public.catalog_items (category, subcategory);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_catalog_items_asset_type_id
    ON public.catalog_items (asset_type_id);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_catalog_items_updated_at
    ON public.catalog_items (updated_at DESC);
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.catalog_bundles (
      bundle_id BIGINT PRIMARY KEY,
      name TEXT,
      description TEXT,
      creator_name TEXT,
      creator_id BIGINT,
      thumbnail_url TEXT,
      category TEXT,
      subcategory TEXT,
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
    if (await upsertCatalogItem(mapped)) upserts += 1;
  }

  return { seen, upserts, forcedMetaLookups };
}

async function crawlSearchLane({
  laneLabel,
  category,
  subcategory = null,
  keyword = null,
  maxPages = 1,
  maxMetaLookups = MAX_META_LOOKUPS_PER_PASS,
}) {
  let cursor = null;
  let pages = 0;
  let totalSeen = 0;
  let totalUpserts = 0;
  let totalForcedMetaLookups = 0;
  const budget = { count: 0, max: Math.max(0, maxMetaLookups) };

  while (pages < maxPages) {
    const url = buildSearchUrl({ category, subcategory, keyword, cursor });
    const data = await fetchJsonWithRetry(url, SEARCH_RETRIES, laneLabel);
    if (!data || !Array.isArray(data.data)) break;

    const { seen, upserts, forcedMetaLookups } = await processSearchPage(data.data, budget);

    pages += 1;
    totalSeen += seen;
    totalUpserts += upserts;
    totalForcedMetaLookups += forcedMetaLookups;

    console.log(
      `[crawl] ${laneLabel} pages=${pages} seen=${seen} upserts=${upserts} forcedMetaLookups=${forcedMetaLookups}`
    );

    cursor = data.nextPageCursor || null;
    if (!cursor) break;

    await sleep(DELAY_MS + jitter(500));
  }

  console.log(
    `[crawl] ${laneLabel} totalSeen=${totalSeen} totalUpserts=${totalUpserts} totalForcedMetaLookups=${totalForcedMetaLookups}`
  );
}

async function crawlCategorySubcategoryMatrix() {
  if (MATRIX_PAGES_PER_PAIR <= 0) {
    console.log("[crawl-matrix] skipped (CRAWL_MATRIX_PAGES_PER_PAIR=0)");
    return;
  }

  for (const category of CATEGORY_MATRIX) {
    for (const subcategory of SUBCATEGORY_MATRIX) {
      await crawlSearchLane({
        laneLabel: `matrix:cat${category}:sub${subcategory}`,
        category,
        subcategory,
        keyword: null,
        maxPages: MATRIX_PAGES_PER_PAIR,
        maxMetaLookups: MAX_MATRIX_META_LOOKUPS_PER_PAIR,
      });
      await sleep(DELAY_MS + jitter(500));
    }
  }
}

async function crawlAccessorySubcategorySweep() {
  if (MAX_ACCESSORY_SUBCATEGORY_PAGES_PER_PASS <= 0) {
    console.log("[crawl-acc-sub] skipped (CRAWL_PAGES_PER_ACCESSORY_SUBCATEGORY_PASS=0)");
    return;
  }
  if (ACCESSORY_SUBCATEGORY_SWEEP_IDS.length === 0) {
    console.log("[crawl-acc-sub] skipped (no subcategory IDs configured)");
    return;
  }

  for (const subcategory of ACCESSORY_SUBCATEGORY_SWEEP_IDS) {
    await crawlSearchLane({
      laneLabel: `acc-sub:cat11:sub${subcategory}`,
      category: ACCESSORIES_CATEGORY,
      subcategory,
      keyword: null,
      maxPages: MAX_ACCESSORY_SUBCATEGORY_PAGES_PER_PASS,
      maxMetaLookups: MAX_MATRIX_META_LOOKUPS_PER_PAIR,
    });
    await sleep(DELAY_MS + jitter(500));
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
        subcategory: null,
        keyword: kw,
      });
    }
  }

  const bodyPlan = [];
  if (MAX_BODY_PAGES_PER_PASS > 0) {
    for (const key of Object.keys(BODY_TERMS)) {
      const terms = limitedRotatedTerms(
        BODY_TERMS[key],
        MAX_BODY_TERMS_PER_TAB,
        runSeed,
        `body:${key}`
      );

      for (const kw of terms) {
        for (const category of BODY_DISCOVERY_CATEGORIES) {
          bodyPlan.push({
            laneLabel: `body:${key}:${kw}:cat${category}`,
            category,
            subcategory: null,
            keyword: kw,
          });
        }
      }
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
        `acc:${typeId}`
      );

      for (const kw of rotated) {
        accessoryPlan.push({
          laneLabel: `accessories:type${typeId}:${kw}`,
          category: ACCESSORIES_CATEGORY,
          subcategory: null,
          keyword: kw,
        });
      }
    }
  }

  return { clothingPlan, bodyPlan, accessoryPlan };
}

async function crawlShoeBundles() {
  if (SHOE_BUNDLE_PAGES <= 0) {
    console.log("[crawl-bundles] skipped (CRAWL_SHOE_BUNDLE_PAGES=0)");
    return;
  }
  console.log("[crawl-bundles] placeholder lane enabled; no-op for now");
}

async function pruneInvalidShoeBundles() {
  // no-op
}

async function main() {
  try {
    console.log("[startup] crawler config", {
      CRAWL_PAGE_LIMIT_raw: process.env.CRAWL_PAGE_LIMIT,
      PAGE_LIMIT_computed: PAGE_LIMIT,
      CRAWL_PAGES_PER_CLOTHING_PASS_raw: process.env.CRAWL_PAGES_PER_CLOTHING_PASS,
      CRAWL_PAGES_PER_BODY_PASS_raw: process.env.CRAWL_PAGES_PER_BODY_PASS,
      CRAWL_PAGES_PER_ACCESSORY_PASS_raw: process.env.CRAWL_PAGES_PER_ACCESSORY_PASS,
      CRAWL_PAGES_PER_ACCESSORY_SUBCATEGORY_PASS_raw:
        process.env.CRAWL_PAGES_PER_ACCESSORY_SUBCATEGORY_PASS,
      CRAWL_ACCESSORY_SUBCATEGORY_SWEEP_IDS_raw:
        process.env.CRAWL_ACCESSORY_SUBCATEGORY_SWEEP_IDS,
      CRAWL_MATRIX_PAGES_PER_PAIR_raw: process.env.CRAWL_MATRIX_PAGES_PER_PAIR,
      CRAWL_SHOE_BUNDLE_PAGES_raw: process.env.CRAWL_SHOE_BUNDLE_PAGES,
      CRAWL_DELAY_MS_raw: process.env.CRAWL_DELAY_MS,
      CRAWL_ASSET_META_DELAY_MS_raw: process.env.CRAWL_ASSET_META_DELAY_MS,
    });

    await pool.query("SELECT 1");
    console.log("DB connected");
    await ensureSchema();

    const runSeed = String(Math.floor(Date.now() / (1000 * 60 * 60 * ROTATION_HOURS)));

    await crawlCategorySubcategoryMatrix();
    await crawlAccessorySubcategorySweep();

    const { clothingPlan, bodyPlan, accessoryPlan } = buildPlans(runSeed);

    if (MAX_CLOTHING_PAGES_PER_PASS > 0) {
      for (const pass of clothingPlan) {
        await crawlSearchLane({
          laneLabel: pass.laneLabel,
          category: pass.category,
          subcategory: pass.subcategory,
          keyword: pass.keyword,
          maxPages: MAX_CLOTHING_PAGES_PER_PASS,
          maxMetaLookups: MAX_META_LOOKUPS_PER_PASS,
        });
        await sleep(DELAY_MS + jitter(500));
      }
    } else {
      console.log("[crawl] clothing passes skipped (MAX_CLOTHING_PAGES_PER_PASS=0)");
    }

    if (MAX_BODY_PAGES_PER_PASS > 0) {
      for (const pass of bodyPlan) {
        await crawlSearchLane({
          laneLabel: pass.laneLabel,
          category: pass.category,
          subcategory: pass.subcategory,
          keyword: pass.keyword,
          maxPages: MAX_BODY_PAGES_PER_PASS,
          maxMetaLookups: MAX_META_LOOKUPS_PER_PASS,
        });
        await sleep(DELAY_MS + jitter(500));
      }
    } else {
      console.log("[crawl] body passes skipped (MAX_BODY_PAGES_PER_PASS=0)");
    }

    if (MAX_ACCESSORY_PAGES_PER_PASS > 0) {
      for (const pass of accessoryPlan) {
        await crawlSearchLane({
          laneLabel: pass.laneLabel,
          category: pass.category,
          subcategory: pass.subcategory,
          keyword: pass.keyword,
          maxPages: MAX_ACCESSORY_PAGES_PER_PASS,
          maxMetaLookups: MAX_META_LOOKUPS_PER_PASS,
        });
        await sleep(DELAY_MS + jitter(500));
      }
    } else {
      console.log("[crawl] accessory passes skipped (MAX_ACCESSORY_PAGES_PER_PASS=0)");
    }

    await crawlShoeBundles();
    await pruneInvalidShoeBundles();

    console.log("Crawl complete");
    process.exit(0);
  } catch (err) {
    console.error("crawl failed:", err);
    process.exit(1);
  }
}

main();