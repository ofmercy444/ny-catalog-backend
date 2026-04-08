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
const ACCESSORY_DISCOVERY_CATEGORIES = [11, 13];

const PAGE_LIMIT = Number(process.env.CRAWL_PAGE_LIMIT || 30);

// Separate pass budgets (important)
const MAX_CLOTHING_PAGES_PER_PASS = Number(
  process.env.CRAWL_PAGES_PER_CLOTHING_PASS ??
    process.env.CRAWL_PAGES_PER_SUBTAB ??
    1
);
const MAX_ACCESSORY_PAGES_PER_PASS = Number(
  process.env.CRAWL_PAGES_PER_ACCESSORY_PASS ??
    process.env.CRAWL_PAGES_PER_SUBTAB ??
    1
);

const SHOE_BUNDLE_PAGES = Number(process.env.CRAWL_SHOE_BUNDLE_PAGES || 1);

const DELAY_MS = Number(process.env.CRAWL_DELAY_MS || 7000);
const ASSET_META_DELAY_MS = Number(process.env.CRAWL_ASSET_META_DELAY_MS || 900);
const INCLUDE_NOT_FOR_SALE =
  String(process.env.INCLUDE_NOT_FOR_SALE || "true") === "true";

const SEARCH_RETRIES = Number(process.env.CRAWL_SEARCH_RETRIES || 1);
const DETAIL_RETRIES = Number(process.env.CRAWL_DETAIL_RETRIES || 3);
const RETRY_BASE_MS = Number(process.env.CRAWL_RETRY_BASE_MS || 1200);
const MAX_RETRY_BACKOFF_MS = Number(process.env.CRAWL_MAX_RETRY_BACKOFF_MS || 12000);

const RATE_LIMIT_COOLDOWN_MS = Number(process.env.CRAWL_RATE_LIMIT_COOLDOWN_MS || 180000);
const RATE_LIMIT_STREAK_TRIGGER = Number(process.env.CRAWL_RATE_LIMIT_STREAK_TRIGGER || 5);

const MAX_META_LOOKUPS_PER_PASS = Number(process.env.CRAWL_MAX_META_LOOKUPS_PER_PASS || 80);

const MAX_CLOTHING_TERMS_PER_TAB = Number(process.env.CRAWL_MAX_CLOTHING_TERMS_PER_TAB || 6);
const MAX_ACCESSORY_TERMS_PER_TYPE = Number(process.env.CRAWL_MAX_ACCESSORY_TERMS_PER_TYPE || 8);
const MAX_GLOBAL_TERMS_PER_RUN = Number(process.env.CRAWL_MAX_GLOBAL_TERMS_PER_RUN || 8);
const MAX_SHOE_TERMS_PER_RUN = Number(process.env.CRAWL_MAX_SHOE_TERMS_PER_RUN || 18);

const MAX_HAIR_TERMS_PER_RUN = Number(process.env.CRAWL_MAX_HAIR_TERMS_PER_RUN || 10);
const HAIR_FOCUS_PAGES = Number(process.env.CRAWL_HAIR_FOCUS_PAGES || 1);
const HAIR_META_LOOKUPS_PER_RUN = Number(process.env.CRAWL_HAIR_META_LOOKUPS_PER_RUN || 800);
const HAIR_DIRECT_PAGES = Number(process.env.CRAWL_HAIR_DIRECT_PAGES || 1);

const ROTATION_HOURS = Number(process.env.CRAWL_ROTATION_HOURS || 2);

console.log("[startup] crawler config", {
  CRAWL_PAGE_LIMIT_raw: process.env.CRAWL_PAGE_LIMIT,
  PAGE_LIMIT_computed: PAGE_LIMIT,
  CRAWL_PAGES_PER_CLOTHING_PASS_raw: process.env.CRAWL_PAGES_PER_CLOTHING_PASS,
  CRAWL_PAGES_PER_ACCESSORY_PASS_raw: process.env.CRAWL_PAGES_PER_ACCESSORY_PASS,
  CRAWL_PAGES_PER_SUBTAB_raw: process.env.CRAWL_PAGES_PER_SUBTAB,
  CRAWL_SHOE_BUNDLE_PAGES_raw: process.env.CRAWL_SHOE_BUNDLE_PAGES,
  CRAWL_DELAY_MS_raw: process.env.CRAWL_DELAY_MS,
  CRAWL_ASSET_META_DELAY_MS_raw: process.env.CRAWL_ASSET_META_DELAY_MS,
});

const CLASSIC_TSHIRT_TYPE = 2;
const CLASSIC_SHIRT_TYPE = 11;
const CLASSIC_PANTS_TYPE = 12;

const HAT_ACCESSORY_TYPE = 8;
const HAIR_ACCESSORY_TYPE = 41;
const FACE_ACCESSORY_TYPE = 42;
const NECK_ACCESSORY_TYPE = 43;
const SHOULDER_ACCESSORY_TYPE = 44;
const FRONT_ACCESSORY_TYPE = 45;
const BACK_ACCESSORY_TYPE = 46;
const WAIST_ACCESSORY_TYPE = 47;

const SHOE_LEFT_TYPE = 70;
const SHOE_RIGHT_TYPE = 71;

const ACCESSORY_TYPES = [
  HAT_ACCESSORY_TYPE,
  HAIR_ACCESSORY_TYPE,
  FACE_ACCESSORY_TYPE,
  NECK_ACCESSORY_TYPE,
  SHOULDER_ACCESSORY_TYPE,
  FRONT_ACCESSORY_TYPE,
  BACK_ACCESSORY_TYPE,
  WAIST_ACCESSORY_TYPE,
];

const TYPE_TO_GROUP = {
  [CLASSIC_TSHIRT_TYPE]: { category: "clothing", subcategory: "classic_t_shirts" },
  [CLASSIC_SHIRT_TYPE]: { category: "clothing", subcategory: "classic_shirts" },
  [CLASSIC_PANTS_TYPE]: { category: "clothing", subcategory: "classic_pants" },

  64: { category: "clothing", subcategory: "t_shirts" },
  65: { category: "clothing", subcategory: "shirts" },
  66: { category: "clothing", subcategory: "pants" },
  67: { category: "clothing", subcategory: "jackets" },
  68: { category: "clothing", subcategory: "sweaters" },
  69: { category: "clothing", subcategory: "shorts" },
  70: { category: "clothing", subcategory: "shoes" },
  71: { category: "clothing", subcategory: "shoes" },
  72: { category: "clothing", subcategory: "dresses_skirts" },

  [HAT_ACCESSORY_TYPE]: { category: "accessories", subcategory: "hats" },
  [HAIR_ACCESSORY_TYPE]: { category: "accessories", subcategory: "hair" },
  [FACE_ACCESSORY_TYPE]: { category: "accessories", subcategory: "faces" },
  [NECK_ACCESSORY_TYPE]: { category: "accessories", subcategory: "neck" },
  [SHOULDER_ACCESSORY_TYPE]: { category: "accessories", subcategory: "shoulder" },
  [FRONT_ACCESSORY_TYPE]: { category: "accessories", subcategory: "front" },
  [BACK_ACCESSORY_TYPE]: { category: "accessories", subcategory: "back" },
  [WAIST_ACCESSORY_TYPE]: { category: "accessories", subcategory: "waist" },
};

const ASSET_TYPE_NAME_TO_ID = {
  tshirt: 2,
  shirt: 11,
  pants: 12,
  hat: 8,
  hairaccessory: 41,
  faceaccessory: 42,
  neckaccessory: 43,
  shoulderaccessory: 44,
  frontaccessory: 45,
  backaccessory: 46,
  waistaccessory: 47,
  tshirtaccessory: 64,
  shirtaccessory: 65,
  pantsaccessory: 66,
  jacketaccessory: 67,
  sweateraccessory: 68,
  shortsaccessory: 69,
  leftshoeaccessory: 70,
  rightshoeaccessory: 71,
  dressskirtaccessory: 72,
};

const CLOTHING_KEYWORDS = {
  all: ["", "y2k", "coquette", "streetwear", "vintage", "retro", "high fashion", "editorial inspired"],
  classic_shirts: ["classic shirt", "2d shirt", "template shirt", "legacy shirt", "retro shirt"],
  classic_pants: ["classic pants", "2d pants", "template pants", "legacy pants", "retro pants"],
  classic_t_shirts: ["classic t-shirt", "classic tee", "2d t shirt", "graphic classic tee"],
  shirts: ["layered shirt", "shirt", "top", "blouse", "crop top", "baby tee", "cami", "tank top"],
  jackets: ["layered jacket", "jacket", "hoodie", "coat", "bomber jacket", "puffer", "windbreaker", "blazer"],
  sweaters: ["layered sweater", "sweater", "cardigan", "knit", "pullover", "crewneck", "turtleneck", "chunky knit"],
  t_shirts: ["layered t shirt", "t-shirt", "tee", "graphic tee", "vintage tee", "logo tee", "oversized tee", "band tee"],
  pants: ["layered pants", "pants", "jeans", "cargo pants", "joggers", "sweatpants", "wide leg pants", "flare pants"],
  shorts: ["layered shorts", "shorts", "denim shorts", "cargo shorts", "athletic shorts", "bike shorts", "running shorts", "jorts"],
  dresses_skirts: ["layered dress", "layered skirt", "dress", "skirt", "mini dress", "maxi dress", "pleated skirt", "bodycon dress"],
};

const ACCESSORY_KEYWORDS = {
  [HAT_ACCESSORY_TYPE]: [
    "hat",
    "head accessory",
    "headband",
    "beanie",
    "cap",
    "beret",
    "helmet",
    "bucket hat",
    "tiara",
  ],
  [HAIR_ACCESSORY_TYPE]: [
    "hair",
    "hairstyle",
    "wig",
    "mullet",
    "wolf cut",
    "pixie",
    "bob",
    "ponytail",
    "braid",
    "braids",
    "locs",
    "dreads",
    "updo",
    "with bangs",
    "w/ bangs",
    "w bangs",
  ],
  [FACE_ACCESSORY_TYPE]: [
    "face accessory",
    "bangs",
    "fringe",
    "face bangs",
    "mask",
    "glasses",
    "goggles",
    "visor",
    "sunglasses",
  ],
  [NECK_ACCESSORY_TYPE]: [
    "neck accessory",
    "necklace",
    "choker",
    "scarf",
    "tie",
    "pendant",
    "chain necklace",
  ],
  [SHOULDER_ACCESSORY_TYPE]: [
    "shoulder accessory",
    "shoulder pet",
    "shoulder plush",
    "pauldron",
    "shoulder bag",
  ],
  [FRONT_ACCESSORY_TYPE]: [
    "front accessory",
    "crossbody",
    "front bag",
    "harness",
    "front pouch",
  ],
  [BACK_ACCESSORY_TYPE]: [
    "back accessory",
    "backpack",
    "wings",
    "cape",
    "jetpack",
    "guitar back",
  ],
  [WAIST_ACCESSORY_TYPE]: [
    "waist accessory",
    "belt",
    "waist chain",
    "fanny pack",
    "hip bag",
    "utility belt",
  ],
};

const GLOBAL_STYLE_TERMS = [
  "high fashion",
  "runway inspired",
  "editorial inspired",
  "luxury streetwear",
  "quiet luxury",
  "loud luxury",
  "street luxe",
  "urban chic",
  "capsule wardrobe",
  "signature style",
  "dark coquette",
  "soft grunge",
  "cyber y2k",
  "futuristic y2k",
];

const SHOE_BUNDLE_KEYWORDS = [
  "shoes",
  "shoe bundle",
  "sneakers",
  "heels",
  "high heels",
  "stiletto",
  "pumps",
  "platform shoes",
  "wedge heels",
  "boots",
  "ankle boots",
  "knee high boots",
  "thigh high boots",
  "sandals",
  "strappy sandals",
  "loafers",
  "mules",
  "clogs",
  "flats",
  "mary jane",
  "oxford shoes",
];

const HAIR_FOCUS_KEYWORDS = [
  "hair",
  "hairstyle",
  "wig",
  "mullet",
  "wolf cut",
  "pixie",
  "bob",
  "ponytail",
  "braid",
  "with bangs",
  "w/ bangs",
  "w bangs",
];

const memoryAssetTypeCache = new Map();
let consecutive429 = 0;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function jitter(max = 500) {
  return Math.floor(Math.random() * max);
}

function hashSeed(s) {
  let h = 2166136261;
  const str = String(s || "");
  for (let i = 0; i < str.length; i += 1) {
    h ^= str.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return Math.abs(h >>> 0);
}

function rotatedSlice(list, maxCount, seedKey) {
  const arr = [...new Set((list || []).filter(Boolean))];
  if (arr.length <= maxCount) return arr;
  const seed = hashSeed(seedKey);
  const start = seed % arr.length;
  const out = [];
  for (let i = 0; i < maxCount; i += 1) {
    out.push(arr[(start + i) % arr.length]);
  }
  return out;
}

function cleanTypeName(v) {
  return String(v || "").toLowerCase().replace(/[^a-z]/g, "");
}

function toAssetTypeIdFromSearch(raw) {
  const idCandidates = [raw.assetTypeId, raw.AssetTypeId, raw.assetType];
  for (const c of idCandidates) {
    const n = Number(c);
    if (Number.isFinite(n)) return n;
  }
  const byName = cleanTypeName(raw.assetTypeName || raw.AssetTypeName || raw.assetType);
  return ASSET_TYPE_NAME_TO_ID[byName] ?? null;
}

function toAssetTypeNameFromSearch(raw) {
  const n = raw.assetTypeName || raw.AssetTypeName || raw.assetType || "";
  return String(n || "");
}

function classifyByType(assetTypeId, fallbackCategory = "clothing", fallbackSubcategory = null) {
  if (assetTypeId != null && TYPE_TO_GROUP[assetTypeId]) return TYPE_TO_GROUP[assetTypeId];
  return { category: fallbackCategory, subcategory: fallbackSubcategory };
}

function isBundleLike(raw) {
  const t = String(raw.itemType || raw.assetType || raw.assetTypeName || "").toLowerCase();
  return t.includes("bundle") || t.includes("package");
}

function isShoeLikeTitle(name) {
  const n = String(name || "").toLowerCase();
  return (
    n.includes("shoe") ||
    n.includes("sneaker") ||
    n.includes("boot") ||
    n.includes("heel") ||
    n.includes("stiletto") ||
    n.includes("loafer") ||
    n.includes("sandal") ||
    n.includes("mary jane") ||
    n.includes("mule") ||
    n.includes("clog")
  );
}

function isHairTypeCandidate(assetTypeId, assetTypeName) {
  const t = Number(assetTypeId);
  if (t === HAIR_ACCESSORY_TYPE) return true;
  const normalized = String(assetTypeName || "").toLowerCase().replace(/[^a-z]/g, "");
  return normalized === "hairaccessory" || normalized === "hair";
}

function buildSearchUrl({ category, keyword, cursor, limit, subcategory }) {
  const url = new URL("https://catalog.roblox.com/v1/search/items/details");
  const finalLimit = Number(limit || PAGE_LIMIT);

  url.searchParams.set("Category", String(category));
  url.searchParams.set("Limit", String(finalLimit));
  url.searchParams.set("SortType", "3");
  url.searchParams.set("IncludeNotForSale", INCLUDE_NOT_FOR_SALE ? "true" : "false");
  if (subcategory && String(subcategory).trim()) {
    url.searchParams.set("Subcategory", String(subcategory).trim());
  }
  if (keyword && keyword.trim()) url.searchParams.set("Keyword", keyword.trim());
  if (cursor) url.searchParams.set("Cursor", cursor);

  return url.toString();
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
        const waitMs = Math.min(MAX_RETRY_BACKOFF_MS, RETRY_BASE_MS * attempt * attempt) + jitter(700);
        console.log(`[429] ${label} retry ${attempt}/${maxAttempts} in ${waitMs}ms -> ${url}`);
        await sleep(waitMs);

        if (consecutive429 >= RATE_LIMIT_STREAK_TRIGGER) {
          console.log(`[rate-limit] cooldown ${RATE_LIMIT_COOLDOWN_MS}ms after streak=${consecutive429}`);
          consecutive429 = 0;
          await sleep(RATE_LIMIT_COOLDOWN_MS);
        }
        continue;
      }

      if (res.status === 400) {
        const text = await res.text().catch(() => "");
        console.log(`[400] ${label} give up -> ${url} ${text.slice(0, 160)}`);
        return null;
      }

      if (res.status >= 500 && attempt < maxAttempts) {
        const waitMs = Math.min(MAX_RETRY_BACKOFF_MS, RETRY_BASE_MS * attempt) + jitter(600);
        console.log(`[${res.status}] ${label} retry ${attempt}/${maxAttempts} in ${waitMs}ms`);
        await sleep(waitMs);
        continue;
      }

      const text = await res.text().catch(() => "");
      console.log(`[${res.status}] ${label} give up -> ${url} ${text.slice(0, 160)}`);
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

async function fetchAssetDetailsWithRetry(assetId, tries = DETAIL_RETRIES) {
  const url = `https://economy.roblox.com/v2/assets/${assetId}/details`;
  return fetchJsonWithRetry(url, tries, `asset:${assetId}`);
}

async function fetchBundleDetailsWithRetry(bundleId, tries = DETAIL_RETRIES) {
  const url = `https://catalog.roblox.com/v1/bundles/${bundleId}/details`;
  return fetchJsonWithRetry(url, tries, `bundle:${bundleId}`);
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

  const creatorId = Number.isFinite(Number(creatorIdRaw)) ? Number(creatorIdRaw) : null;
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
    subcategory: null,
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
    asset_type_id: toAssetTypeIdFromSearch(raw),
    asset_type_name: toAssetTypeNameFromSearch(raw),
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
    subcategory: null,
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
      subcategory TEXT,
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

  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.catalog_bundles (
      bundle_id BIGINT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      creator_name TEXT,
      creator_id BIGINT,
      creator_type TEXT,
      bundle_type TEXT,
      category TEXT DEFAULT 'clothing',
      subcategory TEXT DEFAULT 'misc',
      item_type TEXT DEFAULT 'bundle',
      thumbnail_url TEXT,
      is_offsale BOOLEAN DEFAULT FALSE,
      price_robux INTEGER,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.bundle_asset_links (
      bundle_id BIGINT NOT NULL,
      asset_id BIGINT NOT NULL,
      role TEXT,
      asset_type_id INTEGER,
      sort_order INTEGER DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (bundle_id, asset_id)
    );
  `);

  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS asset_type_id INTEGER;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS asset_type_name TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS subcategory TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_bundles ADD COLUMN IF NOT EXISTS subcategory TEXT DEFAULT 'misc';`);
  await pool.query(`ALTER TABLE public.catalog_bundles ADD COLUMN IF NOT EXISTS item_type TEXT DEFAULT 'bundle';`);
  await pool.query(`ALTER TABLE public.catalog_bundles ADD COLUMN IF NOT EXISTS is_offsale BOOLEAN DEFAULT FALSE;`);
  await pool.query(`ALTER TABLE public.catalog_bundles ADD COLUMN IF NOT EXISTS price_robux INTEGER;`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_category ON public.catalog_items(category);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_subcategory ON public.catalog_items(subcategory);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_updated ON public.catalog_items(updated_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_asset_type_id ON public.catalog_items(asset_type_id);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_bundles_subcategory ON public.catalog_bundles(subcategory);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_bundle_links_bundle_id ON public.bundle_asset_links(bundle_id);`);
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
    map.set(String(row.asset_id), {
      asset_type_id: row.asset_type_id == null ? null : Number(row.asset_type_id),
      asset_type_name: row.asset_type_name || "",
    });
  }
  return map;
}

async function enrichAssetTypes(items, maxLookups = MAX_META_LOOKUPS_PER_PASS) {
  const ids = items.map((i) => i.asset_id).filter((id) => Number.isFinite(id));
  const knownMap = await getKnownAssetTypes(ids);

  let lookedUp = 0;
  for (const item of items) {
    if (!item.asset_id) continue;
    const key = String(item.asset_id);

    if (item.asset_type_id != null) {
      memoryAssetTypeCache.set(key, {
        asset_type_id: item.asset_type_id,
        asset_type_name: item.asset_type_name || "",
      });
      continue;
    }

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

    if (lookedUp >= maxLookups) continue;

    const details = await fetchAssetDetailsWithRetry(item.asset_id);
    if (details) {
      const meta = extractAssetMeta(details);
      memoryAssetTypeCache.set(key, meta);
      item.asset_type_id = meta.asset_type_id;
      item.asset_type_name = meta.asset_type_name;
    }

    lookedUp += 1;
    await sleep(ASSET_META_DELAY_MS);
  }
}

async function upsertItem(item) {
  if (!Number.isFinite(item.asset_id)) return;

  await pool.query(
    `
    INSERT INTO public.catalog_items (
      asset_id, name, description, creator_name, creator_id, creator_type,
      item_type, category, subcategory, thumbnail_url, is_offsale, is_limited, is_limited_unique,
      price_robux, asset_type_id, asset_type_name, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW())
    ON CONFLICT (asset_id) DO UPDATE SET
      name = EXCLUDED.name,
      description = EXCLUDED.description,
      creator_name = COALESCE(NULLIF(EXCLUDED.creator_name, ''), public.catalog_items.creator_name),
      creator_id = COALESCE(EXCLUDED.creator_id, public.catalog_items.creator_id),
      creator_type = COALESCE(NULLIF(EXCLUDED.creator_type, ''), public.catalog_items.creator_type),
      item_type = COALESCE(NULLIF(EXCLUDED.item_type, ''), public.catalog_items.item_type),
      category = EXCLUDED.category,
      subcategory = COALESCE(EXCLUDED.subcategory, public.catalog_items.subcategory),
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
      item.subcategory,
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

async function upsertBundle(bundle) {
  await pool.query(
    `
    INSERT INTO public.catalog_bundles (
      bundle_id, name, description, creator_name, creator_id, creator_type,
      bundle_type, category, subcategory, item_type, thumbnail_url, is_offsale, price_robux, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,'bundle',$10,$11,$12,NOW())
    ON CONFLICT (bundle_id) DO UPDATE SET
      name = EXCLUDED.name,
      description = EXCLUDED.description,
      creator_name = COALESCE(NULLIF(EXCLUDED.creator_name, ''), public.catalog_bundles.creator_name),
      creator_id = COALESCE(EXCLUDED.creator_id, public.catalog_bundles.creator_id),
      creator_type = COALESCE(NULLIF(EXCLUDED.creator_type, ''), public.catalog_bundles.creator_type),
      bundle_type = COALESCE(NULLIF(EXCLUDED.bundle_type, ''), public.catalog_bundles.bundle_type),
      category = EXCLUDED.category,
      subcategory = EXCLUDED.subcategory,
      item_type = 'bundle',
      thumbnail_url = COALESCE(NULLIF(EXCLUDED.thumbnail_url, ''), public.catalog_bundles.thumbnail_url),
      is_offsale = EXCLUDED.is_offsale,
      price_robux = EXCLUDED.price_robux,
      updated_at = NOW()
    `,
    [
      bundle.bundle_id,
      bundle.name,
      bundle.description,
      bundle.creator_name,
      bundle.creator_id,
      bundle.creator_type,
      bundle.bundle_type,
      bundle.category,
      bundle.subcategory,
      bundle.thumbnail_url,
      bundle.is_offsale,
      bundle.price_robux,
    ]
  );
}

async function upsertBundleLink(link) {
  await pool.query(
    `
    INSERT INTO public.bundle_asset_links (
      bundle_id, asset_id, role, asset_type_id, sort_order, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,NOW())
    ON CONFLICT (bundle_id, asset_id) DO UPDATE SET
      role = COALESCE(EXCLUDED.role, public.bundle_asset_links.role),
      asset_type_id = COALESCE(EXCLUDED.asset_type_id, public.bundle_asset_links.asset_type_id),
      sort_order = EXCLUDED.sort_order,
      updated_at = NOW()
    `,
    [link.bundle_id, link.asset_id, link.role, link.asset_type_id, link.sort_order]
  );
}

function buildPlan(runSeed) {
  const clothingPlan = [];

  for (const [tabKey, keywords] of Object.entries(CLOTHING_KEYWORDS)) {
    const slice = rotatedSlice(keywords, MAX_CLOTHING_TERMS_PER_TAB, `${runSeed}:cloth:${tabKey}`);
    clothingPlan.push({
      tabKey,
      passes: slice.map((kw) => ({ keyword: kw, categoryId: CLOTHING_CATEGORY })),
    });
  }

  const globalStyle = rotatedSlice(GLOBAL_STYLE_TERMS, MAX_GLOBAL_TERMS_PER_RUN, `${runSeed}:global`);
  if (globalStyle.length > 0) {
    const allTab = clothingPlan.find((x) => x.tabKey === "all");
    if (allTab) {
      for (const g of globalStyle) allTab.passes.push({ keyword: g, categoryId: CLOTHING_CATEGORY });
    }
  }

  const accessoryPlan = [];
  for (const typeId of ACCESSORY_TYPES) {
    const keys = ACCESSORY_KEYWORDS[typeId] || [];
    const slice = rotatedSlice(keys, MAX_ACCESSORY_TERMS_PER_TYPE, `${runSeed}:acc:${typeId}`);
    const passes = [];
    for (const kw of slice) {
      for (const catId of ACCESSORY_DISCOVERY_CATEGORIES) {
        passes.push({ keyword: kw, categoryId: catId, targetTypeId: typeId });
      }
    }
    accessoryPlan.push({ typeId, passes });
  }

  return { clothingPlan, accessoryPlan };
}

async function crawlPass(pass, tabKey, mode, pagesLimit) {
  let cursor = null;
  let pages = 0;
  let seen = 0;
  let upserts = 0;

  while (pages < pagesLimit) {
    const url = buildSearchUrl({
      category: pass.categoryId,
      keyword: pass.keyword,
      cursor,
      limit: PAGE_LIMIT,
    });

    const json = await fetchJsonWithRetry(url, SEARCH_RETRIES, `${mode}:${tabKey}:${pass.keyword || "all"}`);
    if (!json) break;

    const rows = Array.isArray(json.data) ? json.data : [];
    if (rows.length === 0) break;

    const items = rows.map(normalizeSearchItem).filter((i) => Number.isFinite(i.asset_id));
    await enrichAssetTypes(items);

    for (const item of items) {
      const t = item.asset_type_id == null ? null : Number(item.asset_type_id);

      if (mode === "accessory_type_target" && t !== pass.targetTypeId) continue;

      const classified = classifyByType(
        t,
        mode === "accessory_type_target" ? "accessories" : "clothing",
        tabKey
      );

      item.category = classified.category;
      item.subcategory = classified.subcategory;

      await upsertItem(item);
      upserts += 1;
    }

    seen += items.length;
    pages += 1;
    cursor = json.nextPageCursor || null;
    if (!cursor) break;

    await sleep(DELAY_MS + jitter(700));
  }

  console.log(
    `[crawl] mode=${mode} tab=${tabKey} kw="${pass.keyword}" cat=${pass.categoryId} pages=${pages} seen=${seen} upserts=${upserts}`
  );
}

async function crawlHairFocused(runSeed) {
  const hairTerms = rotatedSlice(
    HAIR_FOCUS_KEYWORDS,
    MAX_HAIR_TERMS_PER_RUN,
    `${runSeed}:hairfocus`
  );

  let totalSeen = 0;
  let totalUpserts = 0;
  let forcedMetaLookups = 0;

  for (const keyword of hairTerms) {
    // Category 11 supports HairAccessory subcategory. Category 13 does not.
    for (const categoryId of [11, 13]) {
      let cursor = null;
      let pages = 0;

      while (pages < HAIR_FOCUS_PAGES) {
        let json = null;

        if (categoryId === 11) {
          const strictUrl = buildSearchUrl({
            category: categoryId,
            keyword,
            cursor,
            limit: PAGE_LIMIT,
            subcategory: "HairAccessory",
          });
          json = await fetchJsonWithRetry(strictUrl, SEARCH_RETRIES, `hair-focus-strict:${keyword}:cat${categoryId}`);

          if (!json) {
            const fallbackUrl = buildSearchUrl({
              category: categoryId,
              keyword,
              cursor,
              limit: PAGE_LIMIT,
            });
            json = await fetchJsonWithRetry(fallbackUrl, SEARCH_RETRIES, `hair-focus-fallback:${keyword}:cat${categoryId}`);
          }
        } else {
          const fallbackUrl = buildSearchUrl({
            category: categoryId,
            keyword,
            cursor,
            limit: PAGE_LIMIT,
          });
          json = await fetchJsonWithRetry(fallbackUrl, SEARCH_RETRIES, `hair-focus-fallback:${keyword}:cat${categoryId}`);
        }

        if (!json) break;

        const rows = Array.isArray(json.data) ? json.data : [];
        if (rows.length === 0) break;

        const items = rows.map(normalizeSearchItem).filter((i) => Number.isFinite(i.asset_id));
        await enrichAssetTypes(items, HAIR_META_LOOKUPS_PER_RUN);

        let upserts = 0;
        for (const item of items) {
          let t = item.asset_type_id == null ? null : Number(item.asset_type_id);

          if (t == null && forcedMetaLookups < HAIR_META_LOOKUPS_PER_RUN) {
            const details = await fetchAssetDetailsWithRetry(item.asset_id);
            if (details) {
              const meta = extractAssetMeta(details);
              item.asset_type_id = meta.asset_type_id;
              item.asset_type_name = meta.asset_type_name;
              t = meta.asset_type_id == null ? null : Number(meta.asset_type_id);
            }
            forcedMetaLookups += 1;
            await sleep(ASSET_META_DELAY_MS);
          }

          if (!isHairTypeCandidate(t, item.asset_type_name)) continue;

          item.asset_type_id = HAIR_ACCESSORY_TYPE;
          item.asset_type_name = "HairAccessory";
          item.category = "accessories";
          item.subcategory = "hair";
          await upsertItem(item);
          upserts += 1;
        }

        totalSeen += items.length;
        totalUpserts += upserts;
        pages += 1;

        console.log(
          `[crawl-hair] kw="${keyword}" cat=${categoryId} pages=${pages} seen=${items.length} upserts=${upserts} forcedMetaLookups=${forcedMetaLookups}`
        );

        cursor = json.nextPageCursor || null;
        if (!cursor) break;
        await sleep(DELAY_MS + jitter(700));
      }
    }
  }

  console.log(`[crawl-hair] totalSeen=${totalSeen} totalUpserts=${totalUpserts}`);
}

async function crawlHairDirectSubcategory() {
  let cursor = null;
  let pages = 0;
  let totalSeen = 0;
  let totalUpserts = 0;

  while (pages < HAIR_DIRECT_PAGES) {
    const url = buildSearchUrl({
      category: 11,
      keyword: "",
      cursor,
      limit: PAGE_LIMIT,
      subcategory: "HairAccessory",
    });

    const json = await fetchJsonWithRetry(url, SEARCH_RETRIES, "hair-direct:cat11");
    if (!json) break;

    const rows = Array.isArray(json.data) ? json.data : [];
    if (rows.length === 0) break;

    const items = rows.map(normalizeSearchItem).filter((i) => Number.isFinite(i.asset_id));
    await enrichAssetTypes(items, HAIR_META_LOOKUPS_PER_RUN);

    let upserts = 0;
    for (const item of items) {
      if (!isHairTypeCandidate(item.asset_type_id, item.asset_type_name)) continue;
      item.asset_type_id = HAIR_ACCESSORY_TYPE;
      item.asset_type_name = "HairAccessory";
      item.category = "accessories";
      item.subcategory = "hair";
      await upsertItem(item);
      upserts += 1;
    }

    totalSeen += items.length;
    totalUpserts += upserts;
    pages += 1;

    console.log(`[crawl-hair-direct] pages=${pages} seen=${items.length} upserts=${upserts}`);

    cursor = json.nextPageCursor || null;
    if (!cursor) break;
    await sleep(DELAY_MS + jitter(500));
  }

  console.log(`[crawl-hair-direct] totalSeen=${totalSeen} totalUpserts=${totalUpserts}`);
}

async function crawlShoeBundles(runSeed) {
  const seenBundles = new Set();
  let discovered = 0;
  let acceptedPairs = 0;
  let linkedAssets = 0;

  const shoeTerms = rotatedSlice(SHOE_BUNDLE_KEYWORDS, MAX_SHOE_TERMS_PER_RUN, `${runSeed}:shoebundles`);

  for (const categoryId of [CLOTHING_CATEGORY, ...ACCESSORY_DISCOVERY_CATEGORIES]) {
    for (const keyword of shoeTerms) {
      let cursor = null;
      let pages = 0;

      while (pages < SHOE_BUNDLE_PAGES) {
        const url = buildSearchUrl({
          category: categoryId,
          keyword,
          cursor,
          limit: PAGE_LIMIT,
        });

        const json = await fetchJsonWithRetry(url, SEARCH_RETRIES, `shoe-search:${keyword}`);
        if (!json) break;

        const rows = Array.isArray(json.data) ? json.data : [];
        for (const raw of rows) {
          const bundleId = Number(raw.id);
          if (!Number.isFinite(bundleId)) continue;
          if (seenBundles.has(bundleId)) continue;
          if (!isBundleLike(raw) && !isShoeLikeTitle(raw.name || "")) continue;

          const bundleDetails = await fetchBundleDetailsWithRetry(bundleId);
          if (!bundleDetails || !Array.isArray(bundleDetails.items)) continue;

          seenBundles.add(bundleId);
          discovered += 1;

          const creator = bundleDetails.creator || raw.creator || {};
          const creatorIdRaw = creator.id ?? creator.creatorTargetId ?? raw.creatorId ?? null;
          const creatorId = Number.isFinite(Number(creatorIdRaw)) ? Number(creatorIdRaw) : null;

          let hasLeft = false;
          let hasRight = false;
          let sortOrder = 0;
          const linkDrafts = [];

          for (const child of bundleDetails.items) {
            const childType = String(child.type || "").toLowerCase();
            const childAssetId = Number(child.id);
            if (childType !== "asset" || !Number.isFinite(childAssetId)) continue;

            const bundleReportedType = Number(child.assetType ?? null);

            const assetDetails = await fetchAssetDetailsWithRetry(childAssetId);
            if (!assetDetails) {
              await sleep(ASSET_META_DELAY_MS);
              continue;
            }

            const item = normalizeEconomyAsset(assetDetails);
            if (!Number.isFinite(item.asset_id)) {
              await sleep(ASSET_META_DELAY_MS);
              continue;
            }

            const finalType =
              Number.isFinite(Number(item.asset_type_id))
                ? Number(item.asset_type_id)
                : (Number.isFinite(bundleReportedType) ? bundleReportedType : null);

            const classified = classifyByType(finalType, "clothing", null);
            item.category = classified.category;
            item.subcategory = classified.subcategory;

            await upsertItem(item);

            let role = null;
            if (finalType === SHOE_LEFT_TYPE) {
              role = "left_shoe";
              hasLeft = true;
            } else if (finalType === SHOE_RIGHT_TYPE) {
              role = "right_shoe";
              hasRight = true;
            }

            linkDrafts.push({
              bundle_id: bundleId,
              asset_id: item.asset_id,
              role,
              asset_type_id: finalType,
              sort_order: sortOrder,
            });

            sortOrder += 1;
            await sleep(ASSET_META_DELAY_MS);
          }

          if (!hasLeft || !hasRight) continue;
          acceptedPairs += 1;

          await upsertBundle({
            bundle_id: bundleId,
            name: String(bundleDetails.name ?? raw.name ?? `Bundle ${bundleId}`),
            description: String(bundleDetails.description ?? raw.description ?? ""),
            creator_name: String(creator.name ?? raw.creatorName ?? ""),
            creator_id: creatorId,
            creator_type: String(creator.type ?? raw.creatorType ?? ""),
            bundle_type: String(bundleDetails.bundleType ?? raw.itemType ?? "Bundle"),
            category: "clothing",
            subcategory: "shoes",
            thumbnail_url: `rbxthumb://type=BundleThumbnail&id=${bundleId}&w=420&h=420`,
            is_offsale: false,
            price_robux: null,
          });

          for (const l of linkDrafts) {
            await upsertBundleLink(l);
            linkedAssets += 1;
          }

          await sleep(ASSET_META_DELAY_MS);
        }

        pages += 1;
        cursor = json.nextPageCursor || null;
        if (!cursor) break;
        await sleep(DELAY_MS + jitter(700));
      }
    }
  }

  console.log(`[crawl-bundles] discovered=${discovered} acceptedPairs=${acceptedPairs} linkedAssets=${linkedAssets}`);
}

async function pruneInvalidShoeBundles() {
  await pool.query(
    `
    DELETE FROM public.catalog_bundles b
    WHERE lower(coalesce(b.subcategory, '')) = 'shoes'
      AND (
        NOT EXISTS (
          SELECT 1 FROM public.bundle_asset_links l
          WHERE l.bundle_id = b.bundle_id AND l.asset_type_id = $1
        )
        OR
        NOT EXISTS (
          SELECT 1 FROM public.bundle_asset_links l
          WHERE l.bundle_id = b.bundle_id AND l.asset_type_id = $2
        )
      )
    `,
    [SHOE_LEFT_TYPE, SHOE_RIGHT_TYPE]
  );

  await pool.query(
    `
    DELETE FROM public.bundle_asset_links l
    WHERE NOT EXISTS (
      SELECT 1 FROM public.catalog_bundles b WHERE b.bundle_id = l.bundle_id
    )
    `
  );
}

async function main() {
  try {
    await pool.query("SELECT 1");
    console.log("DB connected");

    await ensureSchema();

    const runSeed = String(Math.floor(Date.now() / (1000 * 60 * 60 * ROTATION_HOURS)));
    const { clothingPlan, accessoryPlan } = buildPlan(runSeed);

    // Always run hair lanes first.
    await crawlHairDirectSubcategory();
    await crawlHairFocused(runSeed);

    if (MAX_CLOTHING_PAGES_PER_PASS > 0) {
      for (const tab of clothingPlan) {
        for (const pass of tab.passes) {
          await crawlPass(pass, tab.tabKey, "clothing", MAX_CLOTHING_PAGES_PER_PASS);
          await sleep(DELAY_MS + jitter(500));
        }
      }
    } else {
      console.log("[crawl] clothing passes skipped (MAX_CLOTHING_PAGES_PER_PASS=0)");
    }

    if (MAX_ACCESSORY_PAGES_PER_PASS > 0) {
      for (const target of accessoryPlan) {
        const targetLabel = TYPE_TO_GROUP[target.typeId]?.subcategory || String(target.typeId);
        for (const pass of target.passes) {
          await crawlPass(pass, targetLabel, "accessory_type_target", MAX_ACCESSORY_PAGES_PER_PASS);
          await sleep(DELAY_MS + jitter(500));
        }
      }
    } else {
      console.log("[crawl] accessory passes skipped (MAX_ACCESSORY_PAGES_PER_PASS=0)");
    }

    await crawlShoeBundles(runSeed);
    await pruneInvalidShoeBundles();

    console.log("Crawl complete");
    process.exit(0);
  } catch (err) {
    console.error("crawl failed:", err);
    process.exit(1);
  }
}

main();