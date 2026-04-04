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

function normalizeTabKey(raw) {
  const cleaned = String(raw || "all")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/-/g, " ");
  const compact = cleaned.replace(/[&\s_-]/g, "");
  return SUBTAB_ALIASES[cleaned] || SUBTAB_ALIASES[compact] || "all";
}

function matchesSubtab(item, subtabKey) {
  if (subtabKey === "all") return true;

  const itemType = String(item.item_type || "").toLowerCase();
  const name = String(item.name || "").toLowerCase();

  switch (subtabKey) {
    case "classic_shirts":
      return itemType.includes("shirt") && name.includes("classic");
    case "classic_pants":
      return itemType.includes("pants") && name.includes("classic");
    case "shirts":
      return itemType.includes("shirt") && !name.includes("classic");
    case "jackets":
      return itemType.includes("jacket");
    case "sweaters":
      return itemType.includes("sweater");
    case "t_shirts":
      return itemType.includes("tshirt") || itemType.includes("t-shirt");
    case "pants":
      return itemType.includes("pants") && !name.includes("classic");
    case "shorts":
      return itemType.includes("shorts") || name.includes("shorts");
    case "dresses_skirts":
      return (
        itemType.includes("dress") ||
        itemType.includes("skirt") ||
        name.includes("dress") ||
        name.includes("skirt")
      );
    case "shoes":
      return itemType.includes("shoe") || name.includes("shoe");
    case "classic_t_shirts":
      return (itemType.includes("tshirt") || itemType.includes("t-shirt")) && name.includes("classic");
    default:
      return true;
  }
}

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS catalog_items (
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

  await pool.query(`ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS category TEXT DEFAULT 'clothing';`);
  await pool.query(`ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS description TEXT;`);
  await pool.query(`ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS creator_name TEXT;`);
  await pool.query(`ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS creator_id BIGINT;`);
  await pool.query(`ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS creator_type TEXT;`);
  await pool.query(`ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS item_type TEXT;`);
  await pool.query(`ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS thumbnail_url TEXT;`);
  await pool.query(`ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS is_offsale BOOLEAN DEFAULT FALSE;`);
  await pool.query(`ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS is_limited BOOLEAN DEFAULT FALSE;`);
  await pool.query(`ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS is_limited_unique BOOLEAN DEFAULT FALSE;`);
  await pool.query(`ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS price_robux INTEGER;`);
  await pool.query(`ALTER TABLE catalog_items ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW();`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_category ON catalog_items(category);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_item_type ON catalog_items(item_type);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_name_lower ON catalog_items((lower(name)));`);
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

    // Because subtab classification is computed, we may need to scan multiple DB chunks.
    const dbChunkSize = Math.max(limit * 5, 120);
    let dbOffset = offset;
    let passes = 0;
    const maxPasses = 8;
    const items = [];

    while (items.length < limit && passes < maxPasses) {
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
        FROM catalog_items
        ${where}
        ORDER BY updated_at DESC
        LIMIT $${scanParams.length - 1} OFFSET $${scanParams.length};
      `;

      const result = await pool.query(sql, scanParams);
      const rows = result.rows;
      if (rows.length === 0) break;

      for (const row of rows) {
        if (!matchesSubtab(row, subtab)) continue;
        items.push(row);
        if (items.length >= limit) break;
      }

      dbOffset += rows.length;
      if (rows.length < dbChunkSize) break;
    }

    const response = {
      items: items.slice(0, limit),
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