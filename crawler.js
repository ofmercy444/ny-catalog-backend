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
const ALT_DISCOVERY_CATEGORIES = [11, 13];

const PAGE_LIMIT = 30;
const MAX_PAGES_PER_PASS = Number(process.env.CRAWL_PAGES_PER_SUBTAB || 3);
const SHOE_BUNDLE_PAGES = Number(process.env.CRAWL_SHOE_BUNDLE_PAGES || 5);

const DELAY_MS = Number(process.env.CRAWL_DELAY_MS || 2200);
const ASSET_META_DELAY_MS = Number(process.env.CRAWL_ASSET_META_DELAY_MS || 120);
const INCLUDE_NOT_FOR_SALE = String(process.env.INCLUDE_NOT_FOR_SALE || "true") === "true";

const SHOE_LEFT_TYPE = 70;
const SHOE_RIGHT_TYPE = 71;

const CRAWL_PLAN = [
  { key: "all", passes: [{ keyword: "", intent: "all", category: CLOTHING_CATEGORY }] },
  { key: "classic_shirts", passes: [{ keyword: "classic shirt template", intent: "classic", category: CLOTHING_CATEGORY }] },
  { key: "classic_pants", passes: [{ keyword: "classic pants template", intent: "classic", category: CLOTHING_CATEGORY }] },
  { key: "classic_t_shirts", passes: [{ keyword: "classic t shirt", intent: "classic", category: CLOTHING_CATEGORY }] },

  { key: "shirts", passes: [{ keyword: "layered shirt", intent: "layered", category: CLOTHING_CATEGORY }, { keyword: "shirt", intent: "fallback", category: CLOTHING_CATEGORY }] },
  { key: "jackets", passes: [{ keyword: "layered jacket", intent: "layered", category: CLOTHING_CATEGORY }, { keyword: "jacket", intent: "fallback", category: CLOTHING_CATEGORY }] },
  { key: "sweaters", passes: [{ keyword: "layered sweater", intent: "layered", category: CLOTHING_CATEGORY }, { keyword: "sweater", intent: "fallback", category: CLOTHING_CATEGORY }] },
  { key: "t_shirts", passes: [{ keyword: "layered t shirt", intent: "layered", category: CLOTHING_CATEGORY }, { keyword: "t shirt", intent: "fallback", category: CLOTHING_CATEGORY }] },
  { key: "pants", passes: [{ keyword: "layered pants", intent: "layered", category: CLOTHING_CATEGORY }, { keyword: "pants", intent: "fallback", category: CLOTHING_CATEGORY }] },
  { key: "shorts", passes: [{ keyword: "layered shorts", intent: "layered", category: CLOTHING_CATEGORY }, { keyword: "shorts", intent: "fallback", category: CLOTHING_CATEGORY }] },
  { key: "dresses_skirts", passes: [{ keyword: "layered dress skirt", intent: "layered", category: CLOTHING_CATEGORY }, { keyword: "dress skirt", intent: "fallback", category: CLOTHING_CATEGORY }] },
];

const SHOE_BUNDLE_KEYWORDS = [
  "shoes",
  "sneakers",
  "heels",
  "boots",
  "stilettos",
  "sandals",
  "loafers",
  "platform shoes",
  "ankle boots",
];

const memoryAssetTypeCache = new Map();

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
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
    n.includes("sandal")
  );
}

function buildSearchUrl({ category, keyword, cursor, limit }) {
  const url = new URL("https://catalog.roblox.com/v1/search/items/details");
  url.searchParams.set("Category", String(category));
  url.searchParams.set("Limit", String(limit || PAGE_LIMIT));
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
        "User-Agent": "ny-catalog-backend/strict-shoes",
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
        "User-Agent": "ny-catalog-backend/strict-shoes",
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
        "User-Agent": "ny-catalog-backend/strict-shoes",
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
      thumbnail_url TEXT,
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
  await pool.query(`ALTER TABLE public.catalog_bundles ADD COLUMN IF NOT EXISTS subcategory TEXT DEFAULT 'misc';`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_category ON public.catalog_items(category);`);
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
    const meta = extractAssetMeta(details);
    memoryAssetTypeCache.set(key, meta);
    item.asset_type_id = meta.asset_type_id;
    item.asset_type_name = meta.asset_type_name;

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
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,NOW())
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

async function upsertBundle(bundle) {
  await pool.query(
    `
    INSERT INTO public.catalog_bundles (
      bundle_id, name, description, creator_name, creator_id, creator_type,
      bundle_type, category, subcategory, thumbnail_url, updated_at
    )
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,NOW())
    ON CONFLICT (bundle_id) DO UPDATE SET
      name = EXCLUDED.name,
      description = EXCLUDED.description,
      creator_name = COALESCE(NULLIF(EXCLUDED.creator_name,''), public.catalog_bundles.creator_name),
      creator_id = COALESCE(EXCLUDED.creator_id, public.catalog_bundles.creator_id),
      creator_type = COALESCE(NULLIF(EXCLUDED.creator_type,''), public.catalog_bundles.creator_type),
      bundle_type = COALESCE(NULLIF(EXCLUDED.bundle_type,''), public.catalog_bundles.bundle_type),
      category = EXCLUDED.category,
      subcategory = EXCLUDED.subcategory,
      thumbnail_url = COALESCE(NULLIF(EXCLUDED.thumbnail_url,''), public.catalog_bundles.thumbnail_url),
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

async function crawlItemPass(tabKey, passConfig) {
  let cursor = null;
  let pages = 0;
  let upserts = 0;
  let layeredMapped = 0;

  while (pages < MAX_PAGES_PER_PASS) {
    const url = buildSearchUrl({
      category: passConfig.category || CLOTHING_CATEGORY,
      keyword: passConfig.keyword,
      cursor,
      limit: PAGE_LIMIT,
    });

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

    pages += 1;
    cursor = json.nextPageCursor || null;
    if (!cursor) break;
    await sleep(DELAY_MS);
  }

  console.log(
    `[crawl] ${tabKey} | keyword="${passConfig.keyword}" | intent=${passConfig.intent || "unknown"} | pages=${pages}, upserts=${upserts}, layeredMapped=${layeredMapped}`
  );
}

async function crawlShoeBundles() {
  const seenBundles = new Set();
  let discovered = 0;
  let acceptedPairs = 0;
  let linkedAssets = 0;

  for (const category of [CLOTHING_CATEGORY, ...ALT_DISCOVERY_CATEGORIES]) {
    for (const keyword of SHOE_BUNDLE_KEYWORDS) {
      let cursor = null;
      let pages = 0;

      while (pages < SHOE_BUNDLE_PAGES) {
        const url = buildSearchUrl({
          category,
          keyword,
          cursor,
          limit: PAGE_LIMIT,
        });

        const json = await fetchJsonWithRetry(url);
        const rows = Array.isArray(json.data) ? json.data : [];

        for (const raw of rows) {
          const candidateId = Number(raw.id);
          if (!Number.isFinite(candidateId)) continue;
          if (seenBundles.has(candidateId)) continue;

          if (!isBundleLike(raw) && !isShoeLikeTitle(raw.name || "")) continue;

          const bundleDetails = await fetchBundleDetailsWithRetry(candidateId);
          if (!bundleDetails || !Array.isArray(bundleDetails.items)) continue;

          seenBundles.add(candidateId);
          discovered += 1;

          const creator = bundleDetails.creator || raw.creator || {};
          const creatorIdRaw = creator.id ?? creator.creatorTargetId ?? raw.creatorId ?? null;
          const creatorId = Number.isFinite(Number(creatorIdRaw)) ? Number(creatorIdRaw) : null;

          const bundleName = String(bundleDetails.name ?? raw.name ?? `Bundle ${candidateId}`);

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

            await upsertItem(item);

            const finalType =
              Number.isFinite(Number(item.asset_type_id))
                ? Number(item.asset_type_id)
                : (Number.isFinite(bundleReportedType) ? bundleReportedType : null);

            let role = null;
            if (finalType === SHOE_LEFT_TYPE) {
              role = "left_shoe";
              hasLeft = true;
            } else if (finalType === SHOE_RIGHT_TYPE) {
              role = "right_shoe";
              hasRight = true;
            }

            linkDrafts.push({
              bundle_id: candidateId,
              asset_id: item.asset_id,
              role,
              asset_type_id: finalType,
              sort_order: sortOrder,
            });

            sortOrder += 1;
            await sleep(ASSET_META_DELAY_MS);
          }

          // STRICT RULE: only keep bundles that truly contain both left and right shoe assets.
          if (!hasLeft || !hasRight) {
            continue;
          }

          acceptedPairs += 1;

          await upsertBundle({
            bundle_id: candidateId,
            name: bundleName,
            description: String(bundleDetails.description ?? raw.description ?? ""),
            creator_name: String(creator.name ?? raw.creatorName ?? ""),
            creator_id: creatorId,
            creator_type: String(creator.type ?? raw.creatorType ?? ""),
            bundle_type: String(bundleDetails.bundleType ?? raw.itemType ?? "Bundle"),
            category: "clothing",
            subcategory: "shoes",
            thumbnail_url: `rbxthumb://type=BundleThumbnail&id=${candidateId}&w=420&h=420`,
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
        await sleep(DELAY_MS);
      }
    }
  }

  console.log(
    `[crawl-bundles] shoes discovered=${discovered}, acceptedPairs=${acceptedPairs}, linkedAssets=${linkedAssets}`
  );
}

async function pruneInvalidShoeBundles() {
  // Keep only shoes bundles that have both left and right linked.
  await pool.query(
    `
    DELETE FROM public.catalog_bundles b
    WHERE b.subcategory = 'shoes'
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

  // Remove orphan links
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

    for (const tab of CRAWL_PLAN) {
      for (const pass of tab.passes) {
        await crawlItemPass(tab.key, pass);
        await sleep(DELAY_MS);
      }
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