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

const SUBTAB_ALIASES = {
  all: "all",
  classicshirts: "classic_shirts",
  "classic shirts": "classic_shirts",
  classic_shirts: "classic_shirts",
  classicpants: "classic_pants",
  "classic pants": "classic_pants",
  classic_pants: "classic_pants",
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
  classictshirts: "classic_t_shirts",
  "classic t-shirts": "classic_t_shirts",
  "classic t shirts": "classic_t_shirts",
  classic_t_shirts: "classic_t_shirts",
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
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.catalog_item_subtabs (
      asset_id BIGINT NOT NULL REFERENCES public.catalog_items(asset_id) ON DELETE CASCADE,
      subtab_key TEXT NOT NULL,
      is_layered BOOLEAN DEFAULT FALSE,
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (asset_id, subtab_key)
    );
  `);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_category ON public.catalog_items(category);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_updated ON public.catalog_items(updated_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_subtabs_key ON public.catalog_item_subtabs(subtab_key);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_subtabs_layered ON public.catalog_item_subtabs(subtab_key, is_layered);`);
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
    const q = String(req.query.q || "").trim().toLowerCase();
    const limit = Math.min(Math.max(Number(req.query.limit || 30), 1), 60);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const cacheKey = `search:v10:${category}:${subtab}:${q}:${limit}:${offset}`;
    if (redis) {
      const cached = await redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    }

    const params = [category];
    let where = "WHERE lower(i.category) = $1";

    if (q.length > 0) {
      where += ` AND lower(coalesce(i.name,'')) LIKE $${params.length + 1}`;
      params.push(`%${q}%`);
    }

    let sql;
    if (subtab === "all") {
      // global layered score per asset (bool_or across all mapped subtabs)
      params.push(limit, offset);
      sql = `
        SELECT
          i.asset_id,
          i.name,
          i.category,
          i.item_type,
          i.creator_id,
          i.creator_name,
          i.creator_type,
          i.description,
          'rbxthumb://type=Asset&id=' || i.asset_id::text || '&w=420&h=420' AS thumbnail_url,
          'rbxthumb://type=BundleThumbnail&id=' || i.asset_id::text || '&w=420&h=420' AS thumbnail_bundle_url,
          COALESCE(i.thumbnail_url, '') AS thumbnail_raw_url,
          i.is_offsale,
          i.is_limited,
          i.is_limited_unique,
          i.price_robux,
          i.updated_at,
          COALESCE(stg.is_layered, false) AS is_layered,
          CASE
            WHEN lower(coalesce(i.creator_type, '')) = 'group' AND i.creator_id IS NOT NULL
              THEN 'rbxthumb://type=GroupIcon&id=' || i.creator_id::text || '&w=150&h=150'
            WHEN i.creator_id IS NOT NULL
              THEN 'rbxthumb://type=AvatarHeadShot&id=' || i.creator_id::text || '&w=150&h=150'
            ELSE 'rbxasset://textures/ui/GuiImagePlaceholder.png'
          END AS creator_avatar_url
        FROM public.catalog_items i
        LEFT JOIN LATERAL (
          SELECT bool_or(s.is_layered) AS is_layered
          FROM public.catalog_item_subtabs s
          WHERE s.asset_id = i.asset_id
        ) stg ON true
        ${where}
        ORDER BY COALESCE(stg.is_layered, false) DESC, i.updated_at DESC, i.asset_id DESC
        LIMIT $${params.length - 1}
        OFFSET $${params.length};
      `;
    } else {
      where += ` AND st.subtab_key = $${params.length + 1}`;
      params.push(subtab);
      params.push(limit, offset);

      sql = `
        SELECT
          i.asset_id,
          i.name,
          i.category,
          i.item_type,
          i.creator_id,
          i.creator_name,
          i.creator_type,
          i.description,
          'rbxthumb://type=Asset&id=' || i.asset_id::text || '&w=420&h=420' AS thumbnail_url,
          'rbxthumb://type=BundleThumbnail&id=' || i.asset_id::text || '&w=420&h=420' AS thumbnail_bundle_url,
          COALESCE(i.thumbnail_url, '') AS thumbnail_raw_url,
          i.is_offsale,
          i.is_limited,
          i.is_limited_unique,
          i.price_robux,
          i.updated_at,
          st.is_layered,
          CASE
            WHEN lower(coalesce(i.creator_type, '')) = 'group' AND i.creator_id IS NOT NULL
              THEN 'rbxthumb://type=GroupIcon&id=' || i.creator_id::text || '&w=150&h=150'
            WHEN i.creator_id IS NOT NULL
              THEN 'rbxthumb://type=AvatarHeadShot&id=' || i.creator_id::text || '&w=150&h=150'
            ELSE 'rbxasset://textures/ui/GuiImagePlaceholder.png'
          END AS creator_avatar_url
        FROM public.catalog_items i
        JOIN public.catalog_item_subtabs st ON st.asset_id = i.asset_id
        ${where}
        ORDER BY st.is_layered DESC, i.updated_at DESC, i.asset_id DESC
        LIMIT $${params.length - 1}
        OFFSET $${params.length};
      `;
    }

    const { rows } = await pool.query(sql, params);

    const response = {
      items: rows,
      nextOffset: rows.length === limit ? offset + limit : null,
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