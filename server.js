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

// Canonical Roblox type ids
const CLASSIC_TSHIRT_TYPE = 2;
const CLASSIC_SHIRT_TYPE = 11;
const CLASSIC_PANTS_TYPE = 12;

const SHOE_LEFT_TYPE = 70;
const SHOE_RIGHT_TYPE = 71;

const LAYERED_TYPES = [64, 65, 66, 67, 68, 69, 70, 71, 72];
const CLASSIC_CLOTHING_TYPES = [CLASSIC_TSHIRT_TYPE, CLASSIC_SHIRT_TYPE, CLASSIC_PANTS_TYPE];

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

function normalizeTabKey(raw) {
  const cleaned = String(raw || "all")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/-/g, " ");
  const compact = cleaned.replace(/[&\s_-]/g, "");
  return SUBTAB_ALIASES[cleaned] || SUBTAB_ALIASES[compact] || "all";
}

function normalizeBundleBaseTitle(name) {
  return String(name || "")
    .toLowerCase()
    .replace(/\((left|right)\)/g, "")
    .replace(/\b(left|right)\b/g, "")
    .replace(/[|:–—-]+\s*(left|right)\b/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function getSubtabSpec(subtab) {
  // Strict classic tabs
  if (subtab === "classic_shirts") {
    return { mode: "classic", allowedTypes: [CLASSIC_SHIRT_TYPE] };
  }
  if (subtab === "classic_pants") {
    return { mode: "classic", allowedTypes: [CLASSIC_PANTS_TYPE] };
  }
  if (subtab === "classic_t_shirts") {
    return { mode: "classic", allowedTypes: [CLASSIC_TSHIRT_TYPE] };
  }

  // Layered-priority tabs
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
  if (subtab === "shoes") {
    return {
      mode: "layered",
      layeredTypes: [SHOE_LEFT_TYPE, SHOE_RIGHT_TYPE],
      fallbackClassicTypes: [],
      fallbackTitleRegex: "(shoe|shoes|sneaker|sneakers|boot|boots|heel|heels)",
    };
  }

  // "all" strict: layered first, classic after
  return {
    mode: "all_strict",
    layeredTypes: LAYERED_TYPES,
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
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_name_lower ON public.catalog_items((lower(name)));`);
}

let schemaReady = false;
async function ensureSchemaOnce() {
  if (schemaReady) return;
  await ensureSchema();
  schemaReady = true;
}

function mapItemRow(row) {
  return {
    asset_id: row.asset_id,
    name: row.name,
    category: row.category,
    item_type: row.item_type,
    asset_type_id: row.asset_type_id,
    asset_type_name: row.asset_type_name,
    creator_id: row.creator_id,
    creator_name: row.creator_name,
    creator_type: row.creator_type,
    description: row.description,
    thumbnail_url: `rbxthumb://type=Asset&id=${row.asset_id}&w=420&h=420`,
    thumbnail_bundle_url: `rbxthumb://type=BundleThumbnail&id=${row.asset_id}&w=420&h=420`,
    thumbnail_raw_url: row.thumbnail_url || "",
    is_offsale: row.is_offsale,
    is_limited: row.is_limited,
    is_limited_unique: row.is_limited_unique,
    price_robux: row.price_robux,
    updated_at: row.updated_at,
    is_layered: LAYERED_TYPES.includes(Number(row.asset_type_id)),
    creator_avatar_url:
      String(row.creator_type || "").toLowerCase() === "group" && row.creator_id != null
        ? `rbxthumb://type=GroupIcon&id=${row.creator_id}&w=150&h=150`
        : row.creator_id != null
        ? `rbxthumb://type=AvatarHeadShot&id=${row.creator_id}&w=150&h=150`
        : "rbxasset://textures/ui/GuiImagePlaceholder.png",
  };
}

async function getShoeBundleItems(baseItem) {
  if (![SHOE_LEFT_TYPE, SHOE_RIGHT_TYPE].includes(Number(baseItem.asset_type_id))) {
    return [];
  }

  const baseName = normalizeBundleBaseTitle(baseItem.name);
  const creatorId = baseItem.creator_id == null ? null : Number(baseItem.creator_id);

  const params = [];
  let where = `
    WHERE lower(category) = 'clothing'
      AND asset_type_id IN (${SHOE_LEFT_TYPE}, ${SHOE_RIGHT_TYPE})
  `;

  if (creatorId != null) {
    params.push(creatorId);
    where += ` AND creator_id = $${params.length}`;
  }

  params.push(500);

  const { rows } = await pool.query(
    `
    SELECT
      asset_id, name, category, item_type, asset_type_id, asset_type_name,
      creator_id, creator_name, creator_type, description, thumbnail_url,
      is_offsale, is_limited, is_limited_unique, price_robux, updated_at
    FROM public.catalog_items
    ${where}
    ORDER BY updated_at DESC, asset_id DESC
    LIMIT $${params.length}
    `,
    params
  );

  const exact = rows.filter((r) => normalizeBundleBaseTitle(r.name) === baseName);
  const poolRows =
    exact.length > 0
      ? exact
      : rows.filter((r) => {
          const n = normalizeBundleBaseTitle(r.name);
          return n && (n.includes(baseName) || baseName.includes(n));
        });

  let left = null;
  let right = null;

  for (const r of poolRows) {
    if (!left && Number(r.asset_type_id) === SHOE_LEFT_TYPE) left = r;
    if (!right && Number(r.asset_type_id) === SHOE_RIGHT_TYPE) right = r;
    if (left && right) break;
  }

  if (!left && Number(baseItem.asset_type_id) === SHOE_LEFT_TYPE) {
    left = baseItem;
  }
  if (!right && Number(baseItem.asset_type_id) === SHOE_RIGHT_TYPE) {
    right = baseItem;
  }

  const out = [];
  if (left) {
    out.push({
      asset_id: left.asset_id,
      name: left.name,
      asset_type_id: left.asset_type_id,
      asset_type_name: left.asset_type_name,
      role: "left_shoe",
      thumbnail_url: `rbxthumb://type=Asset&id=${left.asset_id}&w=150&h=150`,
      thumbnail_bundle_url: `rbxthumb://type=BundleThumbnail&id=${left.asset_id}&w=150&h=150`,
      thumbnail_raw_url: left.thumbnail_url || "",
    });
  }
  if (right) {
    out.push({
      asset_id: right.asset_id,
      name: right.name,
      asset_type_id: right.asset_type_id,
      asset_type_name: right.asset_type_name,
      role: "right_shoe",
      thumbnail_url: `rbxthumb://type=Asset&id=${right.asset_id}&w=150&h=150`,
      thumbnail_bundle_url: `rbxthumb://type=BundleThumbnail&id=${right.asset_id}&w=150&h=150`,
      thumbnail_raw_url: right.thumbnail_url || "",
    });
  }

  return out;
}

app.get("/", async () => ({ ok: true, service: "catalog-backend" }));
app.get("/health", async () => ({ ok: true }));

app.get("/catalog/search", async (req, reply) => {
  try {
    await ensureSchemaOnce();

    const category = String(req.query.category || "clothing").toLowerCase();
    const subtab = normalizeTabKey(req.query.subtab || "all");
    const q = String(req.query.q || "").trim().toLowerCase();
    const limit = Math.min(Math.max(Number(req.query.limit || 30), 1), 60);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const cacheKey = `search:v14:${category}:${subtab}:${q}:${limit}:${offset}`;
    if (redis) {
      const cached = await redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    }

    const spec = getSubtabSpec(subtab);

    const params = [category];
    let where = "WHERE lower(i.category) = $1";

    // title-only search
    if (q.length > 0) {
      where += ` AND lower(coalesce(i.name,'')) LIKE $${params.length + 1}`;
      params.push(`%${q}%`);
    }

    let orderSql = "i.updated_at DESC, i.asset_id DESC";

    if (spec.mode === "classic") {
      where += ` AND i.asset_type_id = ANY($${params.length + 1}::int[])`;
      params.push(spec.allowedTypes);
    } else if (spec.mode === "all_strict") {
      const layeredIdx = params.length + 1;
      params.push(spec.layeredTypes);

      const classicIdx = params.length + 1;
      params.push(spec.classicTypes);

      where += `
        AND (
          i.asset_type_id = ANY($${layeredIdx}::int[])
          OR i.asset_type_id = ANY($${classicIdx}::int[])
        )
      `;

      orderSql = `
        CASE
          WHEN i.asset_type_id = ANY($${layeredIdx}::int[]) THEN 0
          ELSE 1
        END,
        i.updated_at DESC,
        i.asset_id DESC
      `;
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
      SELECT
        i.asset_id,
        i.name,
        i.category,
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
        i.updated_at
      FROM public.catalog_items i
      ${where}
      ORDER BY ${orderSql}
      LIMIT $${params.length - 1}
      OFFSET $${params.length};
    `;

    const { rows } = await pool.query(sql, params);
    const items = rows.map(mapItemRow);

    const response = {
      items,
      nextOffset: items.length === limit ? offset + limit : null,
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

app.get("/catalog/item/:assetId", async (req, reply) => {
  try {
    await ensureSchemaOnce();

    const assetId = Number(req.params.assetId);
    if (!Number.isFinite(assetId)) {
      return reply.code(400).send({ error: "invalid_asset_id" });
    }

    const { rows } = await pool.query(
      `
      SELECT
        asset_id, name, category, item_type, asset_type_id, asset_type_name,
        creator_id, creator_name, creator_type, description, thumbnail_url,
        is_offsale, is_limited, is_limited_unique, price_robux, updated_at
      FROM public.catalog_items
      WHERE asset_id = $1
      LIMIT 1
      `,
      [assetId]
    );

    if (rows.length === 0) {
      return reply.code(404).send({ error: "item_not_found" });
    }

    const item = mapItemRow(rows[0]);
    const bundle_items = await getShoeBundleItems(item);

    return { item, bundle_items };
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "catalog_item_failed" });
  }
});

app.listen({ port: PORT, host: "0.0.0.0" }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});