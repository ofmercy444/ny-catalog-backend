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
    const category = String(req.query.category || "clothing").trim().toLowerCase();
    const subtab = String(req.query.subtab || "all").trim().toLowerCase();
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
    let i = 1;

    where.push(`LOWER(category) = $${i++}`);
    params.push(category);

    if (subtab !== "all") {
      // supports either 'subtab' or fallback to 'subcategory'
      where.push(`LOWER(COALESCE(subtab, subcategory, '')) = $${i++}`);
      params.push(subtab);
    }

    if (q.length > 0) {
      where.push(`(name ILIKE $${i} OR creator_name ILIKE $${i})`);
      params.push(`%${q}%`);
      i++;
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
      ORDER BY asset_id DESC
      LIMIT $${i++}
      OFFSET $${i++}
    `;

    params.push(limit, offset);

    const { rows } = await pool.query(sql, params);

    const nextCursor = rows.length === limit ? offset + limit : null;
    const result = {
      items: rows,
      nextCursor,
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