require("dotenv").config();
const Fastify = require("fastify");
const { Pool } = require("pg");
const Redis = require("ioredis");

const app = Fastify({ logger: true });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const redis = process.env.REDIS_URL
  ? new Redis(process.env.REDIS_URL, {
      maxRetriesPerRequest: 1,
      enableReadyCheck: false,
    })
  : null;

/**
 * Normalize UI labels to stable keys
 */
function normalizeKey(value, fallback = "all") {
  return String(value || fallback)
    .trim()
    .toLowerCase()
    .replace(/&/g, "and")
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "");
}

/**
 * Map clothing subtabs to possible asset_type values.
 * Adjust these to match your DB data over time.
 */
function clothingTypeFilters(subtabKey) {
  const map = {
    all: [],
    classic_shirts: ["classic_shirt", "classicshirts"],
    classic_pants: ["classic_pants", "classicpants"],
    shirts: ["shirt", "shirts"],
    jackets: ["jacket", "jackets"],
    sweaters: ["sweater", "sweaters"],
    t_shirts: ["tshirt", "t_shirt", "tshirts", "t_shirts"],
    pants: ["pants", "pant"],
    shorts: ["shorts", "short"],
    dresses_and_skirts: ["dress", "dresses", "skirt", "skirts", "dresses_skirts"],
    shoes: ["shoe", "shoes"],
    classic_t_shirts: ["classic_tshirt", "classic_t_shirt", "classictshirts", "classic_tshirts"],
  };

  return map[subtabKey] || [];
}

app.get("/health", async () => {
  let dbOk = false;
  let redisOk = false;

  try {
    await pool.query("SELECT 1");
    dbOk = true;
  } catch {}

  if (redis) {
    try {
      const pong = await redis.ping();
      redisOk = pong === "PONG";
    } catch {}
  } else {
    redisOk = true;
  }

  return { ok: dbOk && redisOk, dbOk, redisOk };
});

app.get("/", async () => ({ message: "Catalog backend running" }));

app.get("/catalog/search", async (req, reply) => {
  try {
    const category = normalizeKey(req.query.category, "clothing");
    const subtab = normalizeKey(req.query.subtab, "all");
    const q = String(req.query.q || "").trim();
    const limit = Math.min(Math.max(Number(req.query.limit) || 24, 1), 60);
    const offset = Math.max(Number(req.query.cursor) || 0, 0);

    const cacheKey = `search:${category}:${subtab}:${q}:${limit}:${offset}`;
    if (redis) {
      const cached = await redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    }

    const where = [];
    const params = [];
    let p = 1;

    // category filter (your DB should contain category like "clothing")
    where.push(`LOWER(COALESCE(category, '')) = $${p++}`);
    params.push(category);

    // subtab filter:
    // 1) direct subtab/subcategory match
    // 2) fallback asset_type match for clothing
    if (subtab !== "all") {
      const typeFilters = category === "clothing" ? clothingTypeFilters(subtab) : [];

      if (typeFilters.length > 0) {
        const placeholders = typeFilters.map(() => `$${p++}`);
        params.push(...typeFilters);

        where.push(`
          (
            LOWER(COALESCE(subtab, subcategory, '')) = $${p++}
            OR LOWER(COALESCE(asset_type, '')) IN (${placeholders.join(", ")})
          )
        `);
        params.push(subtab);
      } else {
        where.push(`LOWER(COALESCE(subtab, subcategory, '')) = $${p++}`);
        params.push(subtab);
      }
    }

    if (q.length > 0) {
      where.push(`(
        name ILIKE $${p}
        OR creator_name ILIKE $${p}
        OR description ILIKE $${p}
      )`);
      params.push(`%${q}%`);
      p++;
    }

    const sql = `
      SELECT
        asset_id,
        name,
        description,
        creator_id,
        creator_name,
        asset_type,
        category,
        COALESCE(subtab, subcategory) AS subtab,
        thumbnail_url,
        is_offsale,
        is_limited,
        is_hidden
      FROM catalog_items
      WHERE ${where.join(" AND ")}
      ORDER BY updated_at DESC NULLS LAST, asset_id DESC
      LIMIT $${p++}
      OFFSET $${p++}
    `;

    params.push(limit, offset);

    const { rows } = await pool.query(sql, params);

    const nextCursor = rows.length === limit ? offset + limit : null;
    const result = {
      items: rows,
      nextCursor,
      debug: { category, subtab, q, limit, offset, count: rows.length },
    };

    if (redis) {
      await redis.set(cacheKey, JSON.stringify(result), "EX", 120);
    }

    return result;
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "search_failed" });
  }
});

const port = Number(process.env.PORT || 3000);
app.listen({ port, host: "0.0.0.0" }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});