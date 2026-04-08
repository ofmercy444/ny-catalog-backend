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

const LAYERED_TYPES = [64, 65, 66, 67, 68, 69, SHOE_LEFT_TYPE, SHOE_RIGHT_TYPE, 72];
const LAYERED_NON_SHOE_TYPES = [64, 65, 66, 67, 68, 69, 72];
const CLASSIC_TYPES = [CLASSIC_TSHIRT_TYPE, CLASSIC_SHIRT_TYPE, CLASSIC_PANTS_TYPE];

const ACCESSORY_UI_TYPES = [
  HAT_ACCESSORY_TYPE,
  FACE_ACCESSORY_TYPE,
  NECK_ACCESSORY_TYPE,
  SHOULDER_ACCESSORY_TYPE,
  FRONT_ACCESSORY_TYPE,
  BACK_ACCESSORY_TYPE,
  WAIST_ACCESSORY_TYPE,
  HAIR_ACCESSORY_TYPE,
];

const CLOTHING_SUBTAB_ALIASES = {
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

const ACCESSORY_SUBTAB_ALIASES = {
  all: "all",
  head: "head",
  hat: "head",
  hats: "head",
  face: "face",
  neck: "neck",
  shoulder: "shoulder",
  shoulders: "shoulder",
  front: "front",
  back: "back",
  waist: "waist",
  hair: "hair",
};

const ACCESSORY_SUBTAB_SET = new Set(Object.values(ACCESSORY_SUBTAB_ALIASES));
const CLOTHING_SUBTAB_SET = new Set(Object.values(CLOTHING_SUBTAB_ALIASES));

const HAIR_ANCHOR_RE =
  /(^|[^a-z0-9])(hair|hairstyle|wig|weave|extensions?|ponytail|pigtails?|braids?|locs?|dreads?|bun|updo|bob|lob|pixie|mullet|wolf[ -]?cut|shag|hime[ -]?cut|jellyfish[ -]?cut|octopus[ -]?cut|butterfly[ -]?cut)([^a-z0-9]|$)/i;
const HAIR_STYLE_RE =
  /(^|[^a-z0-9])(bun|double bun|space buns|odango|ponytail|pigtails?|braids?|french braid|dutch braid|fishtail braid|cornrows|twists|layered|shag|wolf cut|hime cut|blunt cut|undercut|fade|bangs?|spiky hair|slick back|half up half down|bantu knots|dreadlocks|locs)([^a-z0-9]|$)/i;
const HAIR_TEXTURE_RE =
  /(^|[^a-z0-9])(curly|curls?|coily|kinky|wavy|waves?|crimped|straight|silky|sleek|glass hair|frizzy|messy|tousled|fluffy|voluminous|thick|coarse|fine|smooth|wet look|damp|defined curls|flowing strands)([^a-z0-9]|$)/i;
const HAIR_COLOR_RE =
  /(^|[^a-z0-9])(brunette|brown|blonde|blond|platinum|ginger|auburn|red|burgundy|black|jet black|silver|gray|grey|white|pink|purple|violet|blue|teal|turquoise|green|emerald|ombre|balayage|highlights|split dye|rainbow|holographic|iridescent|gradient)([^a-z0-9]|$)/i;
const HAIR_AESTHETIC_RE =
  /(^|[^a-z0-9])(y2k|kawaii|coquette|harajuku|magical girl|anime|goth|punk|emo|grunge|streetwear|techwear|cyberpunk|futuristic|cosplay|vtuber|chibi|angelcore|demoncore|fairycore)([^a-z0-9]|$)/i;
const HAIR_BANGS_WORD_RE = /(^|[^a-z0-9])bangs?([^a-z0-9]|$)/i;
const HAIR_BANGS_MOD_RE = /(^|[^a-z0-9])(with|w\/|w)\s+bangs?([^a-z0-9]|$)/i;
const HAIR_NOISE_RE = /(^|[^a-z0-9])(horns|halo|headphones|mask|crown|helmet)([^a-z0-9]|$)/i;

function computeHairScore(title, assetTypeId) {
  const t = String(title || "").toLowerCase();

  const hasAnchor = HAIR_ANCHOR_RE.test(t);
  const hasStyle = HAIR_STYLE_RE.test(t);
  const hasTexture = HAIR_TEXTURE_RE.test(t);
  const hasColor = HAIR_COLOR_RE.test(t);
  const hasAesthetic = HAIR_AESTHETIC_RE.test(t);
  const hasNoise = HAIR_NOISE_RE.test(t);
  const hasBangsWord = HAIR_BANGS_WORD_RE.test(t);
  const hasBangsMod = HAIR_BANGS_MOD_RE.test(t);

  if (!hasAnchor) return { approved: false, score: 0 };
  if (!hasStyle && !hasTexture) return { approved: false, score: 0 };
  if (hasBangsWord && !hasBangsMod && !hasStyle && !hasTexture) return { approved: false, score: 0 };

  let score = 0;
  if (Number(assetTypeId) === HAIR_ACCESSORY_TYPE) score += 2;
  if (hasAnchor) score += 2;
  if (hasStyle) score += 2;
  if (hasTexture) score += 2;
  if (hasColor) score += 1;
  if (hasAesthetic) score += 1;
  if (hasBangsMod) score += 1;
  if (hasNoise && !hasStyle && !hasTexture) score -= 3;

  return { approved: score >= 5, score };
}

function normalizeCategory(rawCategory, rawSubtab) {
  const c = String(rawCategory || "").toLowerCase().trim();
  const s = String(rawSubtab || "").toLowerCase().trim();

  if (c === "accessories" || c === "accessory") return "accessories";
  if (c === "clothing" || c === "clothes" || c === "apparel") return "clothing";

  if (ACCESSORY_SUBTAB_SET.has(ACCESSORY_SUBTAB_ALIASES[s] || s)) return "accessories";
  if (CLOTHING_SUBTAB_SET.has(CLOTHING_SUBTAB_ALIASES[s] || s)) return "clothing";
  return "clothing";
}

function normalizeTabKey(raw, category) {
  const cleaned = String(raw || "all")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/-/g, " ");
  const compact = cleaned.replace(/[&\s_-]/g, "");
  const source =
    String(category || "").toLowerCase() === "accessories"
      ? ACCESSORY_SUBTAB_ALIASES
      : CLOTHING_SUBTAB_ALIASES;
  return source[cleaned] || source[compact] || "all";
}

function getSubtabSpec(category, subtab) {
  if (category === "accessories") {
    if (subtab === "head") return { mode: "typed", allowedTypes: [HAT_ACCESSORY_TYPE] };
    if (subtab === "face") return { mode: "typed", allowedTypes: [FACE_ACCESSORY_TYPE] };
    if (subtab === "neck") return { mode: "typed", allowedTypes: [NECK_ACCESSORY_TYPE] };
    if (subtab === "shoulder") return { mode: "typed", allowedTypes: [SHOULDER_ACCESSORY_TYPE] };
    if (subtab === "front") return { mode: "typed", allowedTypes: [FRONT_ACCESSORY_TYPE] };
    if (subtab === "back") return { mode: "typed", allowedTypes: [BACK_ACCESSORY_TYPE] };
    if (subtab === "waist") return { mode: "typed", allowedTypes: [WAIST_ACCESSORY_TYPE] };
    if (subtab === "hair") return { mode: "hair_qualified", allowedTypes: ACCESSORY_UI_TYPES };
    return { mode: "typed", allowedTypes: ACCESSORY_UI_TYPES };
  }

  if (subtab === "all") return { mode: "all_ranked" };
  if (subtab === "shoes") return { mode: "shoe_parents" };
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
  return { mode: "all_ranked" };
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
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_name_lower ON public.catalog_items((lower(name)));`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_bundles_category ON public.catalog_bundles(category);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_bundles_subcategory ON public.catalog_bundles(subcategory);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_bundles_updated ON public.catalog_bundles(updated_at DESC);`);
}

let schemaReady = false;
async function ensureSchemaOnce() {
  if (schemaReady) return;
  await ensureSchema();
  schemaReady = true;
}

app.get("/", async () => ({ ok: true, service: "catalog-backend" }));
app.get("/health", async () => ({ ok: true }));

function assetSelect() {
  return `
    i.asset_id,
    i.name,
    i.category,
    coalesce(i.subcategory, '') AS subcategory,
    i.item_type,
    i.asset_type_id,
    i.asset_type_name,
    i.creator_id,
    i.creator_name,
    i.creator_type,
    i.description,
    'rbxthumb://type=Asset&id=' || i.asset_id::text || '&w=420&h=420' AS thumbnail_url,
    'rbxthumb://type=BundleThumbnail&id=' || i.asset_id::text || '&w=420&h=420' AS thumbnail_bundle_url,
    coalesce(i.thumbnail_url, '') AS thumbnail_raw_url,
    i.is_offsale,
    i.is_limited,
    i.is_limited_unique,
    i.price_robux,
    i.updated_at,
    CASE WHEN i.asset_type_id = ANY(ARRAY[${LAYERED_TYPES.join(",")} ]::int[]) THEN true ELSE false END AS is_layered,
    CASE
      WHEN lower(coalesce(i.creator_type, '')) = 'group' AND i.creator_id IS NOT NULL
        THEN 'rbxthumb://type=GroupIcon&id=' || i.creator_id::text || '&w=150&h=150'
      WHEN i.creator_id IS NOT NULL
        THEN 'rbxthumb://type=AvatarHeadShot&id=' || i.creator_id::text || '&w=150&h=150'
      ELSE 'rbxasset://textures/ui/GuiImagePlaceholder.png'
    END AS creator_avatar_url,
    'asset' AS detail_kind,
    false AS is_bundle_parent
  `;
}

function bundleSelect() {
  return `
    b.bundle_id AS asset_id,
    b.name,
    lower(coalesce(b.category, 'clothing')) AS category,
    lower(coalesce(b.subcategory, '')) AS subcategory,
    'bundle' AS item_type,
    NULL::int AS asset_type_id,
    ''::text AS asset_type_name,
    b.creator_id,
    b.creator_name,
    b.creator_type,
    coalesce(b.description, '') AS description,
    'rbxthumb://type=BundleThumbnail&id=' || b.bundle_id::text || '&w=420&h=420' AS thumbnail_url,
    'rbxthumb://type=BundleThumbnail&id=' || b.bundle_id::text || '&w=420&h=420' AS thumbnail_bundle_url,
    coalesce(b.thumbnail_url, '') AS thumbnail_raw_url,
    coalesce(b.is_offsale, false) AS is_offsale,
    false AS is_limited,
    false AS is_limited_unique,
    b.price_robux,
    b.updated_at,
    true AS is_layered,
    CASE
      WHEN lower(coalesce(b.creator_type, '')) = 'group' AND b.creator_id IS NOT NULL
        THEN 'rbxthumb://type=GroupIcon&id=' || b.creator_id::text || '&w=150&h=150'
      WHEN b.creator_id IS NOT NULL
        THEN 'rbxthumb://type=AvatarHeadShot&id=' || b.creator_id::text || '&w=150&h=150'
      ELSE 'rbxasset://textures/ui/GuiImagePlaceholder.png'
    END AS creator_avatar_url,
    'bundle' AS detail_kind,
    true AS is_bundle_parent
  `;
}

app.get("/catalog/search", async (req, reply) => {
  try {
    await ensureSchemaOnce();

    const category = normalizeCategory(req.query.category, req.query.subtab);
    const subtab = normalizeTabKey(req.query.subtab || "all", category);
    const q = String(req.query.q || "").trim().toLowerCase();
    const limit = Math.min(Math.max(Number(req.query.limit || 30), 1), 60);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const cacheKey = `search:v21:${category}:${subtab}:${q}:${limit}:${offset}`;
    if (redis) {
      const cached = await redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    }

    const spec = getSubtabSpec(category, subtab);
    let rows = [];
    let nextOffset = null;

    if (category === "clothing" && spec.mode === "shoe_parents") {
      const params = [];
      let where =
        "WHERE lower(coalesce(b.category, 'clothing')) = 'clothing' AND lower(coalesce(b.subcategory, '')) = 'shoes'";
      if (q.length > 0) {
        params.push(`%${q}%`);
        where += ` AND lower(coalesce(b.name,'')) LIKE $${params.length}`;
      }
      params.push(limit);
      const limitIdx = params.length;
      params.push(offset);
      const offsetIdx = params.length;

      const sql = `
        SELECT ${bundleSelect()}
        FROM public.catalog_bundles b
        ${where}
        ORDER BY b.updated_at DESC, b.bundle_id DESC
        LIMIT $${limitIdx}
        OFFSET $${offsetIdx};
      `;
      ({ rows } = await pool.query(sql, params));
      nextOffset = rows.length === limit ? offset + limit : null;
    } else if (category === "clothing" && spec.mode === "all_ranked") {
      const params = [];
      let itemFilter = "";
      let bundleFilter = "";
      if (q.length > 0) {
        params.push(`%${q}%`);
        const qIdx = params.length;
        itemFilter = ` AND lower(coalesce(i.name,'')) LIKE $${qIdx}`;
        bundleFilter = ` AND lower(coalesce(b.name,'')) LIKE $${qIdx}`;
      }
      params.push(LAYERED_NON_SHOE_TYPES);
      const layeredIdx = params.length;
      params.push(CLASSIC_TYPES);
      const classicIdx = params.length;
      params.push(limit);
      const limitIdx = params.length;
      params.push(offset);
      const offsetIdx = params.length;

      const sql = `
        WITH layered_assets AS (
          SELECT ${assetSelect()}, 0 AS rank_bucket
          FROM public.catalog_items i
          WHERE lower(i.category) = 'clothing'
            AND i.asset_type_id = ANY($${layeredIdx}::int[])
            ${itemFilter}
        ),
        shoe_parents AS (
          SELECT ${bundleSelect()}, 1 AS rank_bucket
          FROM public.catalog_bundles b
          WHERE lower(coalesce(b.category, 'clothing')) = 'clothing'
            AND lower(coalesce(b.subcategory, '')) = 'shoes'
            ${bundleFilter}
        ),
        classic_assets AS (
          SELECT ${assetSelect()}, 2 AS rank_bucket
          FROM public.catalog_items i
          WHERE lower(i.category) = 'clothing'
            AND i.asset_type_id = ANY($${classicIdx}::int[])
            ${itemFilter}
        )
        SELECT * FROM (
          SELECT * FROM layered_assets
          UNION ALL
          SELECT * FROM shoe_parents
          UNION ALL
          SELECT * FROM classic_assets
        ) ranked
        ORDER BY rank_bucket ASC, updated_at DESC, asset_id DESC
        LIMIT $${limitIdx}
        OFFSET $${offsetIdx};
      `;
      ({ rows } = await pool.query(sql, params));
      nextOffset = rows.length === limit ? offset + limit : null;
    } else if (category === "accessories" && spec.mode === "hair_qualified") {
      const params = ["accessories", spec.allowedTypes];
      let where = `
        WHERE lower(i.category) = $1
          AND i.asset_type_id = ANY($2::int[])
      `;

      if (q.length > 0) {
        params.push(`%${q}%`);
        where += ` AND lower(coalesce(i.name,'')) LIKE $${params.length}`;
      }

      const fetchLimit = Math.min(400, Math.max(140, limit * 8 + offset));
      params.push(fetchLimit);

      const sql = `
        SELECT ${assetSelect()}
        FROM public.catalog_items i
        ${where}
        ORDER BY i.updated_at DESC, i.asset_id DESC
        LIMIT $${params.length};
      `;
      ({ rows } = await pool.query(sql, params));

      const filtered = rows.filter((r) => computeHairScore(r.name, r.asset_type_id).approved);
      rows = filtered.slice(offset, offset + limit);
      nextOffset = filtered.length > offset + limit ? offset + limit : null;
    } else {
      const params = [category];
      let where = "WHERE lower(i.category) = $1";

      if (q.length > 0) {
        where += ` AND lower(coalesce(i.name,'')) LIKE $${params.length + 1}`;
        params.push(`%${q}%`);
      }

      let orderSql = "i.updated_at DESC, i.asset_id DESC";

      if (spec.mode === "typed" || spec.mode === "classic") {
        where += ` AND i.asset_type_id = ANY($${params.length + 1}::int[])`;
        params.push(spec.allowedTypes);
      } else if (spec.mode === "layered") {
        const layeredIdx = params.length + 1;
        params.push(spec.layeredTypes);

        if (spec.fallbackClassicTypes.length > 0) {
          const fallbackTypesIdx = params.length + 1;
          params.push(spec.fallbackClassicTypes);

          const fallbackRegexIdx = params.length + 1;
          params.push(spec.fallbackTitleRegex);

          where += `
            AND (
              i.asset_type_id = ANY($${layeredIdx}::int[])
              OR (
                i.asset_type_id = ANY($${fallbackTypesIdx}::int[])
                AND lower(coalesce(i.name,'')) ~ $${fallbackRegexIdx}
              )
            )
          `;
        } else {
          where += ` AND i.asset_type_id = ANY($${layeredIdx}::int[])`;
        }

        orderSql = `
          CASE
            WHEN i.asset_type_id = ANY($${layeredIdx}::int[]) THEN 0
            ELSE 1
          END,
          i.updated_at DESC,
          i.asset_id DESC
        `;
      }

      params.push(limit, offset);
      const sql = `
        SELECT ${assetSelect()}
        FROM public.catalog_items i
        ${where}
        ORDER BY ${orderSql}
        LIMIT $${params.length - 1}
        OFFSET $${params.length};
      `;
      ({ rows } = await pool.query(sql, params));
      nextOffset = rows.length === limit ? offset + limit : null;
    }

    const response = {
      items: rows,
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

app.listen({ port: PORT, host: "0.0.0.0" }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});