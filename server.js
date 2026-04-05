require("dotenv").config();
const Fastify = require("fastify");
const { Pool } = require("pg");
const Redis = require("ioredis");

const app = Fastify({ logger: true });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl:
    process.env.DATABASE_URL && process.env.DATABASE_URL.includes("localhost")
      ? false
      : { rejectUnauthorized: false },
});

const redis = process.env.REDIS_URL ? new Redis(process.env.REDIS_URL) : null;
const PORT = Number(process.env.PORT || 3000);

const CLASSIC_TSHIRT_TYPE = 2;
const CLASSIC_SHIRT_TYPE = 11;
const CLASSIC_PANTS_TYPE = 12;

const SHOE_LEFT_TYPE = 70;
const SHOE_RIGHT_TYPE = 71;

const LAYERED_TYPES = [64, 65, 66, 67, 68, 69, 70, 71, 72];
const NON_SHOE_LAYERED_TYPES = [64, 65, 66, 67, 68, 69, 72];
const CLASSIC_CLOTHING_TYPES = [CLASSIC_TSHIRT_TYPE, CLASSIC_SHIRT_TYPE, CLASSIC_PANTS_TYPE];

const PLACEHOLDER = "rbxasset://textures/ui/GuiImagePlaceholder.png";

const SUBTAB_ALIASES = {
  all: "all",
  classicshirts: "classic_shirts",
  "classic shirts": "classic_shirts",
  classic_shirts: "classic_shirts",
  classicpants: "classic_pants",
  "classic pants": "classic_pants",
  classic_pants: "classic_pants",
  classictshirts: "classic_t_shirts",
  "classic t-shirts": "classic_t_shirts",
  "classic t shirts": "classic_t_shirts",
  classic_t_shirts: "classic_t_shirts",

  shirts: "shirts",
  jackets: "jackets",
  sweaters: "sweaters",
  tshirts: "t_shirts",
  "t-shirts": "t_shirts",
  "t shirts": "t_shirts",
  t_shirts: "t_shirts",
  pants: "pants",
  shorts: "shorts",
  dressesandskirts: "dresses_skirts",
  "dresses & skirts": "dresses_skirts",
  "dresses and skirts": "dresses_skirts",
  dresses_skirts: "dresses_skirts",
  shoes: "shoes",
};

const TERM_ALIASES = {
  tee: ["t-shirt", "t shirt", "tshirt"],
  tshirt: ["t-shirt", "t shirt", "tee"],
  tshirts: ["t-shirts", "t shirts", "tees"],
  sneaker: ["trainer"],
  sneakers: ["trainers"],
  heel: ["stiletto", "pump"],
  heels: ["stilettos", "pumps"],
  loafer: ["oxford"],
  loafers: ["oxfords"],
  jacket: ["coat"],
  coats: ["jackets"],
  skirt: ["skirts"],
  skirts: ["skirt"],
  short: ["shorts"],
  shorts: ["short"],
};

function uniq(arr) {
  return [...new Set(arr)];
}

function normalizeTabKey(raw) {
  const cleaned = String(raw || "all")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/-/g, " ");
  const compact = cleaned.replace(/[&\s_-]/g, "");
  return SUBTAB_ALIASES[cleaned] || SUBTAB_ALIASES[compact] || "all";
}

function buildCreatorAvatar(creatorType, creatorId) {
  if (!creatorId) return PLACEHOLDER;
  if (String(creatorType || "").toLowerCase() === "group") {
    return `rbxthumb://type=GroupIcon&id=${creatorId}&w=150&h=150`;
  }
  return `rbxthumb://type=AvatarHeadShot&id=${creatorId}&w=150&h=150`;
}

function normalizeQuery(qRaw) {
  return String(qRaw || "")
    .toLowerCase()
    .trim()
    .replace(/[_-]+/g, " ")
    .replace(/\s+/g, " ");
}

function expandWordForms(word) {
  const w = String(word || "").toLowerCase().trim();
  if (!w) return [];

  const out = new Set([w]);

  if (w.includes("-")) out.add(w.replace(/-/g, " "));
  if (w.includes(" ")) out.add(w.replace(/\s+/g, "-"));

  if (w.endsWith("ies") && w.length > 3) out.add(`${w.slice(0, -3)}y`);
  if (w.endsWith("y") && w.length > 2) out.add(`${w.slice(0, -1)}ies`);

  if (w.endsWith("es") && w.length > 3) out.add(w.slice(0, -2));
  if (w.endsWith("s") && w.length > 2) out.add(w.slice(0, -1));
  if (!w.endsWith("s")) out.add(`${w}s`);

  return [...out];
}

function buildSearchTerms(qRaw) {
  const normalized = normalizeQuery(qRaw);
  if (!normalized) {
    return {
      normalized: "",
      phraseLike: null,
      qNumeric: null,
      tokenLikes: null,
      coreTokens: [],
      expandedTokens: [],
    };
  }

  const baseTokens = normalized.split(" ").filter(Boolean);
  const expanded = new Set();

  for (const base of baseTokens) {
    for (const form of expandWordForms(base)) expanded.add(form);

    const aliasList = TERM_ALIASES[base] || [];
    for (const alias of aliasList) {
      for (const form of expandWordForms(alias)) expanded.add(form);
    }
  }

  // phrase variants for hyphen/space
  expanded.add(normalized.replace(/\s+/g, "-"));
  expanded.add(normalized.replace(/-/g, " "));

  const expandedTokens = uniq([...expanded].filter((t) => t.length > 1));
  const tokenLikes = expandedTokens.length > 0 ? expandedTokens.map((t) => `%${t}%`) : null;
  const qNumeric = /^\d+$/.test(String(qRaw || "").trim()) ? Number(String(qRaw).trim()) : null;

  return {
    normalized,
    phraseLike: `%${normalized}%`,
    qNumeric,
    tokenLikes,
    coreTokens: baseTokens,
    expandedTokens,
  };
}

function termMatch(text, token) {
  return String(text || "").toLowerCase().includes(String(token || "").toLowerCase());
}

function hasAnyForm(text, baseToken) {
  const forms = expandWordForms(baseToken);
  return forms.some((f) => termMatch(text, f));
}

function computeRank(item, terms) {
  if (!terms || !terms.normalized) return 0;

  const name = String(item.name || "").toLowerCase();
  const creator = String(item.creator_name || "").toLowerCase();
  let score = 0;

  // Tier 1: exact phrase
  if (name.includes(terms.normalized)) score += 120;

  // Tier 2: all core tokens present (in any order, with singular/plural handling)
  if (terms.coreTokens.length > 0) {
    const allPresent = terms.coreTokens.every((t) => hasAnyForm(name, t));
    if (allPresent) score += 70;
  }

  // Tier 3: partial token overlap
  let hits = 0;
  for (const tok of terms.expandedTokens) {
    if (name.includes(tok)) hits += 1;
  }
  score += Math.min(hits, 8) * 12;

  // creator + numeric boosts
  if (creator && creator.includes(terms.normalized)) score += 25;
  if (terms.qNumeric !== null && Number(item.asset_id) === terms.qNumeric) score += 200;

  return score;
}

function mapItemRow(r) {
  const assetId = Number(r.asset_id);
  return {
    asset_id: assetId,
    name: r.name || "Item",
    category: r.category || "clothing",
    subcategory: r.subcategory || null,
    item_type: r.item_type || "asset",
    asset_type_id: r.asset_type_id == null ? null : Number(r.asset_type_id),
    asset_type_name: r.asset_type_name || null,
    creator_id: r.creator_id == null ? null : Number(r.creator_id),
    creator_name: r.creator_name || "Unknown creator",
    creator_type: r.creator_type || null,
    creator_avatar_url: r.creator_avatar_url || buildCreatorAvatar(r.creator_type, r.creator_id),
    description: r.description || "",
    thumbnail_url: `rbxthumb://type=Asset&id=${assetId}&w=420&h=420`,
    thumbnail_bundle_url: `rbxthumb://type=BundleThumbnail&id=${assetId}&w=420&h=420`,
    thumbnail_raw_url: r.thumbnail_url || "",
    is_offsale: !!r.is_offsale,
    is_limited: !!r.is_limited,
    is_limited_unique: !!r.is_limited_unique,
    price_robux: r.price_robux == null ? null : Number(r.price_robux),
    detail_kind: "asset",
    is_bundle_parent: false,
    role: r.role || null,
    updated_at: r.updated_at || null,
    _sort_bucket: Number(r.sort_bucket || 0),
  };
}

function mapBundleRow(r) {
  const bundleId = Number(r.bundle_id || r.asset_id);
  return {
    asset_id: bundleId,
    name: r.name || "Bundle",
    category: r.category || "clothing",
    subcategory: r.subcategory || "shoes",
    item_type: "bundle",
    asset_type_id: null,
    asset_type_name: null,
    creator_id: r.creator_id == null ? null : Number(r.creator_id),
    creator_name: r.creator_name || "Unknown creator",
    creator_type: r.creator_type || null,
    creator_avatar_url: r.creator_avatar_url || buildCreatorAvatar(r.creator_type, r.creator_id),
    description: r.description || "",
    thumbnail_url: `rbxthumb://type=BundleThumbnail&id=${bundleId}&w=420&h=420`,
    thumbnail_bundle_url: `rbxthumb://type=BundleThumbnail&id=${bundleId}&w=420&h=420`,
    thumbnail_raw_url: r.thumbnail_url || "",
    is_offsale: !!r.is_offsale,
    is_limited: false,
    is_limited_unique: false,
    price_robux: r.price_robux == null ? null : Number(r.price_robux),
    detail_kind: "bundle",
    is_bundle_parent: true,
    role: null,
    updated_at: r.updated_at || null,
    _sort_bucket: Number(r.sort_bucket || 1),
  };
}

function sanitizeItem(item) {
  const clone = { ...item };
  delete clone._sort_bucket;
  delete clone._rank;
  return clone;
}

function getSubtabSpec(subtab) {
  if (subtab === "classic_shirts") return { mode: "classic", allowedTypes: [CLASSIC_SHIRT_TYPE] };
  if (subtab === "classic_pants") return { mode: "classic", allowedTypes: [CLASSIC_PANTS_TYPE] };
  if (subtab === "classic_t_shirts") return { mode: "classic", allowedTypes: [CLASSIC_TSHIRT_TYPE] };

  if (subtab === "shirts") {
    return {
      mode: "layered",
      layeredTypes: [65],
      fallbackClassicTypes: [CLASSIC_SHIRT_TYPE, CLASSIC_TSHIRT_TYPE],
      fallbackTitleRegex: "(shirt|top|tee|t-shirt|t shirt)",
    };
  }
  if (subtab === "jackets") {
    return {
      mode: "layered",
      layeredTypes: [67],
      fallbackClassicTypes: [CLASSIC_SHIRT_TYPE, CLASSIC_TSHIRT_TYPE],
      fallbackTitleRegex: "(jacket|coat|hoodie|zip[ -]?up)",
    };
  }
  if (subtab === "sweaters") {
    return {
      mode: "layered",
      layeredTypes: [68],
      fallbackClassicTypes: [CLASSIC_SHIRT_TYPE, CLASSIC_TSHIRT_TYPE],
      fallbackTitleRegex: "(sweater|cardigan|knit)",
    };
  }
  if (subtab === "t_shirts") {
    return {
      mode: "layered",
      layeredTypes: [64],
      fallbackClassicTypes: [CLASSIC_TSHIRT_TYPE, CLASSIC_SHIRT_TYPE],
      fallbackTitleRegex: "(t-shirt|t shirt|tee)",
    };
  }
  if (subtab === "pants") {
    return {
      mode: "layered",
      layeredTypes: [66],
      fallbackClassicTypes: [CLASSIC_PANTS_TYPE],
      fallbackTitleRegex: "(pants|jeans|trousers|sweatpants|cargo)",
    };
  }
  if (subtab === "shorts") {
    return {
      mode: "layered",
      layeredTypes: [69],
      fallbackClassicTypes: [CLASSIC_PANTS_TYPE],
      fallbackTitleRegex: "(shorts?)",
    };
  }
  if (subtab === "dresses_skirts") {
    return {
      mode: "layered",
      layeredTypes: [72],
      fallbackClassicTypes: [CLASSIC_SHIRT_TYPE, CLASSIC_PANTS_TYPE, CLASSIC_TSHIRT_TYPE],
      fallbackTitleRegex: "(dress|skirt|gown)",
    };
  }

  if (subtab === "shoes") return { mode: "shoes_bundle_parents" };

  return {
    mode: "all_strict",
    layeredTypes: NON_SHOE_LAYERED_TYPES,
    classicTypes: CLASSIC_CLOTHING_TYPES,
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

  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS subcategory TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_bundles ADD COLUMN IF NOT EXISTS subcategory TEXT DEFAULT 'misc';`);
  await pool.query(`ALTER TABLE public.catalog_bundles ADD COLUMN IF NOT EXISTS item_type TEXT DEFAULT 'bundle';`);
  await pool.query(`ALTER TABLE public.catalog_bundles ADD COLUMN IF NOT EXISTS is_offsale BOOLEAN DEFAULT FALSE;`);
  await pool.query(`ALTER TABLE public.catalog_bundles ADD COLUMN IF NOT EXISTS price_robux INTEGER;`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_category ON public.catalog_items(category);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_updated ON public.catalog_items(updated_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_asset_type_id ON public.catalog_items(asset_type_id);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_name_lower ON public.catalog_items((lower(name)));`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_creator_name_lower ON public.catalog_items((lower(creator_name)));`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_bundles_subcategory ON public.catalog_bundles(subcategory);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_bundles_name_lower ON public.catalog_bundles((lower(name)));`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_bundles_creator_name_lower ON public.catalog_bundles((lower(creator_name)));`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_bundle_links_bundle_id ON public.bundle_asset_links(bundle_id);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_bundle_links_asset_id ON public.bundle_asset_links(asset_id);`);
}

let schemaReady = false;
async function ensureSchemaOnce() {
  if (schemaReady) return;
  await ensureSchema();
  schemaReady = true;
}

app.get("/", async () => ({ ok: true, service: "catalog-backend" }));
app.get("/health", async () => ({ ok: true }));

app.get("/catalog/search", async (req, reply) => {
  try {
    await ensureSchemaOnce();

    const category = String(req.query.category || "clothing").toLowerCase();
    const subtab = normalizeTabKey(req.query.subtab || "all");
    const limit = Math.min(Math.max(Number(req.query.limit || 30), 1), 60);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const terms = buildSearchTerms(req.query.q || "");
    const hasQuery = terms.normalized.length > 0;

    const cacheKey = `search:v29:${category}:${subtab}:${terms.normalized}:${limit}:${offset}`;
    if (redis) {
      const cached = await redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    }

    const spec = getSubtabSpec(subtab);

    // For query mode, fetch wider then rank client-side.
    const widenedLimit = hasQuery
      ? Math.min(700, Math.max(limit * 10, offset + limit + 120))
      : limit;
    const widenedOffset = hasQuery ? 0 : offset;

    let items = [];

    if (spec.mode === "shoes_bundle_parents") {
      const sql = `
        SELECT
          b.*,
          1 AS sort_bucket,
          CASE
            WHEN lower(coalesce(b.creator_type, '')) = 'group' AND b.creator_id IS NOT NULL
              THEN 'rbxthumb://type=GroupIcon&id=' || b.creator_id::text || '&w=150&h=150'
            WHEN b.creator_id IS NOT NULL
              THEN 'rbxthumb://type=AvatarHeadShot&id=' || b.creator_id::text || '&w=150&h=150'
            ELSE '${PLACEHOLDER}'
          END AS creator_avatar_url
        FROM public.catalog_bundles b
        WHERE lower(coalesce(b.category, '')) = $1
          AND lower(coalesce(b.subcategory, '')) = 'shoes'
          AND EXISTS (
            SELECT 1 FROM public.bundle_asset_links l
            WHERE l.bundle_id = b.bundle_id AND l.asset_type_id = ${SHOE_LEFT_TYPE}
          )
          AND EXISTS (
            SELECT 1 FROM public.bundle_asset_links l
            WHERE l.bundle_id = b.bundle_id AND l.asset_type_id = ${SHOE_RIGHT_TYPE}
          )
          AND (
            $2::text IS NULL
            OR lower(coalesce(b.name,'')) LIKE $2
            OR lower(coalesce(b.creator_name,'')) LIKE $2
            OR ($3::bigint IS NOT NULL AND b.bundle_id = $3)
            OR ($4::text[] IS NOT NULL AND lower(coalesce(b.name,'')) LIKE ANY($4::text[]))
            OR ($4::text[] IS NOT NULL AND lower(coalesce(b.creator_name,'')) LIKE ANY($4::text[]))
            OR EXISTS (
              SELECT 1
              FROM public.bundle_asset_links l
              JOIN public.catalog_items i ON i.asset_id = l.asset_id
              WHERE l.bundle_id = b.bundle_id
                AND (
                  lower(coalesce(i.name,'')) LIKE $2
                  OR lower(coalesce(i.creator_name,'')) LIKE $2
                  OR ($3::bigint IS NOT NULL AND i.asset_id = $3)
                  OR ($4::text[] IS NOT NULL AND lower(coalesce(i.name,'')) LIKE ANY($4::text[]))
                  OR ($4::text[] IS NOT NULL AND lower(coalesce(i.creator_name,'')) LIKE ANY($4::text[]))
                )
            )
          )
        ORDER BY b.updated_at DESC, b.bundle_id DESC
        LIMIT $5 OFFSET $6;
      `;
      const { rows } = await pool.query(sql, [
        category,
        terms.phraseLike,
        terms.qNumeric,
        terms.tokenLikes,
        widenedLimit,
        widenedOffset,
      ]);
      items = rows.map(mapBundleRow);
    } else if (spec.mode === "all_strict") {
      const sql = `
        WITH item_rows AS (
          SELECT
            i.asset_id,
            i.name,
            i.category,
            i.subcategory,
            i.item_type,
            i.asset_type_id,
            i.asset_type_name,
            i.creator_id,
            i.creator_name,
            i.creator_type,
            i.description,
            i.thumbnail_url,
            i.is_offsale,
            i.is_limited,
            i.is_limited_unique,
            i.price_robux,
            i.updated_at,
            false AS is_bundle_parent,
            'asset'::text AS detail_kind,
            NULL::text AS role,
            CASE
              WHEN i.asset_type_id = ANY($7::int[]) THEN 0
              WHEN i.asset_type_id = ANY($8::int[]) THEN 2
              ELSE 3
            END AS sort_bucket,
            CASE
              WHEN lower(coalesce(i.creator_type, '')) = 'group' AND i.creator_id IS NOT NULL
                THEN 'rbxthumb://type=GroupIcon&id=' || i.creator_id::text || '&w=150&h=150'
              WHEN i.creator_id IS NOT NULL
                THEN 'rbxthumb://type=AvatarHeadShot&id=' || i.creator_id::text || '&w=150&h=150'
              ELSE '${PLACEHOLDER}'
            END AS creator_avatar_url
          FROM public.catalog_items i
          WHERE lower(coalesce(i.category, '')) = $1
            AND (
              i.asset_type_id = ANY($7::int[])
              OR i.asset_type_id = ANY($8::int[])
            )
            AND (
              $2::text IS NULL
              OR lower(coalesce(i.name,'')) LIKE $2
              OR lower(coalesce(i.creator_name,'')) LIKE $2
              OR ($3::bigint IS NOT NULL AND i.asset_id = $3)
              OR ($4::text[] IS NOT NULL AND lower(coalesce(i.name,'')) LIKE ANY($4::text[]))
              OR ($4::text[] IS NOT NULL AND lower(coalesce(i.creator_name,'')) LIKE ANY($4::text[]))
            )
        ),
        bundle_rows AS (
          SELECT
            b.bundle_id AS asset_id,
            b.name,
            b.category,
            b.subcategory,
            'bundle'::text AS item_type,
            NULL::int AS asset_type_id,
            NULL::text AS asset_type_name,
            b.creator_id,
            b.creator_name,
            b.creator_type,
            b.description,
            b.thumbnail_url,
            b.is_offsale,
            false AS is_limited,
            false AS is_limited_unique,
            b.price_robux,
            b.updated_at,
            true AS is_bundle_parent,
            'bundle'::text AS detail_kind,
            NULL::text AS role,
            1 AS sort_bucket,
            CASE
              WHEN lower(coalesce(b.creator_type, '')) = 'group' AND b.creator_id IS NOT NULL
                THEN 'rbxthumb://type=GroupIcon&id=' || b.creator_id::text || '&w=150&h=150'
              WHEN b.creator_id IS NOT NULL
                THEN 'rbxthumb://type=AvatarHeadShot&id=' || b.creator_id::text || '&w=150&h=150'
              ELSE '${PLACEHOLDER}'
            END AS creator_avatar_url
          FROM public.catalog_bundles b
          WHERE lower(coalesce(b.category, '')) = $1
            AND lower(coalesce(b.subcategory, '')) = 'shoes'
            AND EXISTS (
              SELECT 1 FROM public.bundle_asset_links l
              WHERE l.bundle_id = b.bundle_id AND l.asset_type_id = ${SHOE_LEFT_TYPE}
            )
            AND EXISTS (
              SELECT 1 FROM public.bundle_asset_links l
              WHERE l.bundle_id = b.bundle_id AND l.asset_type_id = ${SHOE_RIGHT_TYPE}
            )
            AND (
              $2::text IS NULL
              OR lower(coalesce(b.name,'')) LIKE $2
              OR lower(coalesce(b.creator_name,'')) LIKE $2
              OR ($3::bigint IS NOT NULL AND b.bundle_id = $3)
              OR ($4::text[] IS NOT NULL AND lower(coalesce(b.name,'')) LIKE ANY($4::text[]))
              OR ($4::text[] IS NOT NULL AND lower(coalesce(b.creator_name,'')) LIKE ANY($4::text[]))
              OR EXISTS (
                SELECT 1
                FROM public.bundle_asset_links l
                JOIN public.catalog_items i ON i.asset_id = l.asset_id
                WHERE l.bundle_id = b.bundle_id
                  AND (
                    lower(coalesce(i.name,'')) LIKE $2
                    OR lower(coalesce(i.creator_name,'')) LIKE $2
                    OR ($3::bigint IS NOT NULL AND i.asset_id = $3)
                    OR ($4::text[] IS NOT NULL AND lower(coalesce(i.name,'')) LIKE ANY($4::text[]))
                    OR ($4::text[] IS NOT NULL AND lower(coalesce(i.creator_name,'')) LIKE ANY($4::text[]))
                  )
              )
            )
        )
        SELECT * FROM (
          SELECT * FROM item_rows
          UNION ALL
          SELECT * FROM bundle_rows
        ) u
        ORDER BY u.sort_bucket ASC, u.updated_at DESC, u.asset_id DESC
        LIMIT $5 OFFSET $6;
      `;
      const { rows } = await pool.query(sql, [
        category,
        terms.phraseLike,
        terms.qNumeric,
        terms.tokenLikes,
        widenedLimit,
        widenedOffset,
        NON_SHOE_LAYERED_TYPES,
        CLASSIC_CLOTHING_TYPES,
      ]);
      items = rows.map((r) => (r.is_bundle_parent ? mapBundleRow(r) : mapItemRow(r)));
    } else {
      const params = [category, terms.phraseLike, terms.qNumeric, terms.tokenLikes];
      let where = `
        WHERE lower(coalesce(i.category, '')) = $1
          AND (
            $2::text IS NULL
            OR lower(coalesce(i.name,'')) LIKE $2
            OR lower(coalesce(i.creator_name,'')) LIKE $2
            OR ($3::bigint IS NOT NULL AND i.asset_id = $3)
            OR ($4::text[] IS NOT NULL AND lower(coalesce(i.name,'')) LIKE ANY($4::text[]))
            OR ($4::text[] IS NOT NULL AND lower(coalesce(i.creator_name,'')) LIKE ANY($4::text[]))
          )
      `;
      let orderSql = "i.updated_at DESC, i.asset_id DESC";

      if (spec.mode === "classic") {
        params.push(spec.allowedTypes);
        where += ` AND i.asset_type_id = ANY($${params.length}::int[])`;
      } else if (spec.mode === "layered") {
        params.push(spec.layeredTypes);
        const layeredIdx = params.length;

        if (spec.fallbackClassicTypes.length > 0) {
          params.push(spec.fallbackClassicTypes);
          const fallbackIdx = params.length;
          params.push(spec.fallbackTitleRegex);
          const regexIdx = params.length;

          where += `
            AND (
              i.asset_type_id = ANY($${layeredIdx}::int[])
              OR (
                i.asset_type_id = ANY($${fallbackIdx}::int[])
                AND lower(coalesce(i.name,'')) ~ $${regexIdx}
              )
            )
          `;
        } else {
          where += ` AND i.asset_type_id = ANY($${layeredIdx}::int[])`;
        }

        orderSql = `
          CASE WHEN i.asset_type_id = ANY($${layeredIdx}::int[]) THEN 0 ELSE 1 END,
          i.updated_at DESC,
          i.asset_id DESC
        `;
      }

      params.push(widenedLimit, widenedOffset);
      const sql = `
        SELECT
          i.*,
          CASE
            WHEN lower(coalesce(i.creator_type, '')) = 'group' AND i.creator_id IS NOT NULL
              THEN 'rbxthumb://type=GroupIcon&id=' || i.creator_id::text || '&w=150&h=150'
            WHEN i.creator_id IS NOT NULL
              THEN 'rbxthumb://type=AvatarHeadShot&id=' || i.creator_id::text || '&w=150&h=150'
            ELSE '${PLACEHOLDER}'
          END AS creator_avatar_url,
          0 AS sort_bucket
        FROM public.catalog_items i
        ${where}
        ORDER BY ${orderSql}
        LIMIT $${params.length - 1}
        OFFSET $${params.length};
      `;
      const { rows } = await pool.query(sql, params);
      items = rows.map(mapItemRow);
    }

    // Additive ranking tiers only when query exists.
    if (hasQuery) {
      for (const it of items) {
        it._rank = computeRank(it, terms);
      }
      items.sort((a, b) => {
        if ((b._rank || 0) !== (a._rank || 0)) return (b._rank || 0) - (a._rank || 0);
        if ((a._sort_bucket || 0) !== (b._sort_bucket || 0)) return (a._sort_bucket || 0) - (b._sort_bucket || 0);

        const aTime = new Date(a.updated_at || 0).getTime();
        const bTime = new Date(b.updated_at || 0).getTime();
        if (bTime !== aTime) return bTime - aTime;

        return Number(b.asset_id || 0) - Number(a.asset_id || 0);
      });
    }

    const paged = items.slice(offset, offset + limit).map(sanitizeItem);
    const nextOffset = items.length > offset + limit ? offset + limit : null;

    const response = {
      items: paged,
      nextOffset,
      subtabKey: subtab,
    };

    if (redis) {
      await redis.set(cacheKey, JSON.stringify(response), "EX", 120);
    }

    return response;
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "catalog_search_failed" });
  }
});

app.get("/catalog/item/:id", async (req, reply) => {
  try {
    await ensureSchemaOnce();

    const id = Number(req.params.id);
    const kind = String(req.query.kind || "asset").toLowerCase();

    if (!Number.isFinite(id) || id <= 0) {
      return reply.code(400).send({ error: "invalid_id" });
    }

    if (kind === "bundle") {
      const parentRes = await pool.query(
        `
        SELECT
          b.*,
          CASE
            WHEN lower(coalesce(b.creator_type, '')) = 'group' AND b.creator_id IS NOT NULL
              THEN 'rbxthumb://type=GroupIcon&id=' || b.creator_id::text || '&w=150&h=150'
            WHEN b.creator_id IS NOT NULL
              THEN 'rbxthumb://type=AvatarHeadShot&id=' || b.creator_id::text || '&w=150&h=150'
            ELSE '${PLACEHOLDER}'
          END AS creator_avatar_url
        FROM public.catalog_bundles b
        WHERE b.bundle_id = $1
        LIMIT 1
      `,
        [id]
      );

      if (parentRes.rowCount === 0) {
        return reply.code(404).send({ error: "not_found" });
      }

      const parent = mapBundleRow(parentRes.rows[0]);

      const childRes = await pool.query(
        `
        SELECT
          i.*,
          l.role,
          l.asset_type_id,
          CASE
            WHEN lower(coalesce(i.creator_type, '')) = 'group' AND i.creator_id IS NOT NULL
              THEN 'rbxthumb://type=GroupIcon&id=' || i.creator_id::text || '&w=150&h=150'
            WHEN i.creator_id IS NOT NULL
              THEN 'rbxthumb://type=AvatarHeadShot&id=' || i.creator_id::text || '&w=150&h=150'
            ELSE '${PLACEHOLDER}'
          END AS creator_avatar_url
        FROM public.bundle_asset_links l
        JOIN public.catalog_items i ON i.asset_id = l.asset_id
        WHERE l.bundle_id = $1
        ORDER BY
          CASE
            WHEN l.asset_type_id = ${SHOE_LEFT_TYPE} THEN 0
            WHEN l.asset_type_id = ${SHOE_RIGHT_TYPE} THEN 1
            ELSE 2
          END,
          i.updated_at DESC
      `,
        [id]
      );

      const bundle_items = childRes.rows.map((r) => sanitizeItem(mapItemRow(r)));

      return {
        item: sanitizeItem(parent),
        bundle_items,
        detail_mode: "bundle_parent",
        can_wear: true,
        can_purchase: true,
        show_accessory_scalers: false,
      };
    }

    const itemRes = await pool.query(
      `
      SELECT
        i.*,
        CASE
          WHEN lower(coalesce(i.creator_type, '')) = 'group' AND i.creator_id IS NOT NULL
            THEN 'rbxthumb://type=GroupIcon&id=' || i.creator_id::text || '&w=150&h=150'
          WHEN i.creator_id IS NOT NULL
            THEN 'rbxthumb://type=AvatarHeadShot&id=' || i.creator_id::text || '&w=150&h=150'
          ELSE '${PLACEHOLDER}'
        END AS creator_avatar_url
      FROM public.catalog_items i
      WHERE i.asset_id = $1
      LIMIT 1
    `,
      [id]
    );

    if (itemRes.rowCount === 0) {
      return reply.code(404).send({ error: "not_found" });
    }

    const item = mapItemRow(itemRes.rows[0]);

    const linkRes = await pool.query(
      `
      SELECT
        l.bundle_id,
        l.role,
        b.creator_id AS parent_creator_id,
        b.creator_type AS parent_creator_type,
        b.creator_name AS parent_creator_name
      FROM public.bundle_asset_links l
      JOIN public.catalog_bundles b ON b.bundle_id = l.bundle_id
      WHERE l.asset_id = $1
        AND lower(coalesce(b.subcategory, '')) = 'shoes'
        AND EXISTS (
          SELECT 1 FROM public.bundle_asset_links l2
          WHERE l2.bundle_id = b.bundle_id AND l2.asset_type_id = ${SHOE_LEFT_TYPE}
        )
        AND EXISTS (
          SELECT 1 FROM public.bundle_asset_links l3
          WHERE l3.bundle_id = b.bundle_id AND l3.asset_type_id = ${SHOE_RIGHT_TYPE}
        )
      LIMIT 1
    `,
      [id]
    );

    if (linkRes.rowCount > 0) {
      const link = linkRes.rows[0];

      if (!item.creator_id && link.parent_creator_id) {
        item.creator_id = Number(link.parent_creator_id);
        item.creator_type = link.parent_creator_type;
        if (!item.creator_name || item.creator_name === "Unknown creator") {
          item.creator_name = link.parent_creator_name || item.creator_name;
        }
        item.creator_avatar_url = buildCreatorAvatar(link.parent_creator_type, link.parent_creator_id);
      }

      item.role = link.role || item.role;
      item.subcategory = "shoes";

      return {
        item: sanitizeItem(item),
        bundle_items: [],
        detail_mode: "bundle_child",
        can_wear: true,
        can_purchase: false,
        show_accessory_scalers: false,
      };
    }

    return {
      item: sanitizeItem(item),
      bundle_items: [],
      detail_mode: "regular",
      can_wear: true,
      can_purchase: true,
      show_accessory_scalers: false,
    };
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "catalog_item_failed" });
  }
});

app.listen({ port: PORT, host: "0.0.0.0" }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});