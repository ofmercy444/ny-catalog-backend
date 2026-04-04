require("dotenv").config();
const Fastify = require("fastify");
const { Pool } = require("pg");
const Redis = require("ioredis");

const app = Fastify({ logger: true });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const redis = process.env.REDIS_URL ? new Redis(process.env.REDIS_URL) : null;

app.get("/health", async () => ({ ok: true }));

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

const SUBTAB_KEYWORDS = {
  all: [],
  classic_shirts: ["classic shirt", "classic tee", "classic t-shirt"],
  classic_pants: ["classic pants"],
  shirts: ["shirt", "tee", "top"],
  jackets: ["jacket", "coat", "hoodie", "blazer", "outerwear"],
  sweaters: ["sweater", "cardigan", "knit", "pullover"],
  t_shirts: ["t-shirt", "tshirt", "tee", "graphic tee"],
  pants: ["pants", "jeans", "trousers", "cargo"],
  shorts: ["shorts"],
  dresses_skirts: ["dress", "skirt", "gown"],
  shoes: ["shoe", "shoes", "sneaker", "boot", "heels", "heel", "sandal", "loafer"],
  classic_t_shirts: ["classic t-shirt", "classic tshirt", "classic tee"],
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

function includesAny(text, words) {
  for (const w of words) {
    if (text.includes(w)) return true;
  }
  return false;
}

function matchesSubtab(item, subtabKey) {
  if (subtabKey === "all") return true;

  const name = String(item.name || "").toLowerCase();
  const itemType = String(item.item_type || "").toLowerCase();
  const desc = String(item.description || "").toLowerCase();
  const text = `${name} ${itemType} ${desc}`;

  // broad keyword gate first
  const keywords = SUBTAB_KEYWORDS[subtabKey] || [];
  let hit = keywords.length === 0 || includesAny(text, keywords);

  // refinement rules
  if (subtabKey === "classic_shirts") {
    hit = hit && name.includes("classic");
  } else if (subtabKey === "classic_pants") {
    hit = hit && name.includes("classic");
  } else if (subtabKey === "classic_t_shirts") {
    hit = hit && name.includes("classic");
  } else if (subtabKey === "shirts") {
    // shirts should not capture classic variants heavily
    if (name.includes("classic")) hit = false;
  } else if (subtabKey === "pants") {
    if (name.includes("classic")) hit = false;
  } else if (subtabKey === "t_shirts") {
    if (name.includes("classic")) hit = false;
  }

  return hit;
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

  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS description TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS creator_name TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS creator_id BIGINT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS creator_type TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS item_type TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS category TEXT DEFAULT 'clothing';`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS thumbnail_url TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS is_offsale BOOLEAN DEFAULT FALSE;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS is_limited BOOLEAN DEFAULT FALSE;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS is_limited_unique BOOLEAN DEFAULT FALSE;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS price_robux INTEGER;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_category ON public.catalog_items(category);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_item_type ON public.catalog_items(item_type);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_name_lower ON public.catalog_items((lower(name)));`);
}

let schemaReady = false;
async function ensureSchemaOnce() {
  if (schemaReady) return;
  await ensureSchema();
  schemaReady = true;
}

app.get("/catalog/search", async (req, reply) => {
  try {
    await ensureSchemaOnce();

    const category = String(req.query.category || "clothing").toLowerCase();
    const subtab = normalizeTabKey(req.query.subtab || "all");
    const q = String(req.query.q || "").trim().toLowerCase();
    const limit = Math.min(Math.max(Number(req.query.limit || 30), 1), 60);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const cacheKey = `search:${category}:${subtab}:${q}:${limit}:${offset}`;
    if (redis) {
      const cached = await redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    }

    const params = [category];
    let where = "WHERE lower(category) = $1";

    if (q.length > 0) {
      params.push(`%${q}%`);
      where += ` AND lower(name) LIKE $${params.length}`;
    }

    // Pull a bigger DB chunk, filter in JS to avoid hard misses.
    const dbChunkSize = Math.max(limit * 6, 180);
    let dbOffset = offset;
    let passes = 0;
    const maxPasses = 10;

    const filtered = [];
    const fallback = [];

    while (filtered.length < limit && passes < maxPasses) {
      passes += 1;
      const scanParams = [...params, dbChunkSize, dbOffset];

      const sql = `
        SELECT
          asset_id,
          name,
          category,
          item_type,
          creator_id,
          creator_name,
          description,
          thumbnail_url,
          is_offsale,
          is_limited,
          is_limited_unique,
          price_robux,
          updated_at
        FROM public.catalog_items
        ${where}
        ORDER BY updated_at DESC
        LIMIT $${scanParams.length - 1} OFFSET $${scanParams.length};
      `;

      const result = await pool.query(sql, scanParams);
      const rows = result.rows;
      if (rows.length === 0) break;

      for (const row of rows) {
        fallback.push(row);
        if (matchesSubtab(row, subtab)) {
          filtered.push(row);
          if (filtered.length >= limit) break;
        }
      }

      dbOffset += rows.length;
      if (rows.length < dbChunkSize) break;
    }

    // If subtab is too sparse, don't return blank screen.
    const items =
    subtab === "all"
      ? fallback.slice(0, limit)
      : filtered.slice(0, limit);

    const response = {
      items,
      nextOffset: items.length === limit ? dbOffset : null,
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

const port = Number(process.env.PORT || 3000);
app.listen({ port, host: "0.0.0.0" }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});