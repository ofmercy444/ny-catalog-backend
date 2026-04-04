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

app.get("/catalog/search", async (req, reply) => {
  try {
    const subtab = String(req.query.subtab || "all").toLowerCase();
    const q = String(req.query.q || "").trim().toLowerCase();
    const limit = Math.min(Math.max(Number(req.query.limit || 30), 1), 60);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const cacheKey = `search:${subtab}:${q}:${limit}:${offset}`;
    if (redis) {
      const cached = await redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    }

    const params = [];
    let where = "WHERE 1=1";

    if (subtab !== "all") {
      params.push(subtab);
      where += ` AND subtab_key = $${params.length}`;
    }

    if (q.length > 0) {
      params.push(`%${q}%`);
      where += ` AND lower(name) LIKE $${params.length}`;
    }

    params.push(limit);
    const limitParam = `$${params.length}`;
    params.push(offset);
    const offsetParam = `$${params.length}`;

    const sql = `
      SELECT
        asset_id,
        name,
        creator_name,
        category,
        asset_type,
        subtab_key,
        description,
        thumbnail_url,
        is_offsale,
        is_limited,
        updated_at
      FROM catalog_items
      ${where}
      ORDER BY asset_id DESC
      LIMIT ${limitParam} OFFSET ${offsetParam};
    `;

    const result = await pool.query(sql, params);
    const items = result.rows;

    const response = {
      items,
      nextOffset: items.length === limit ? offset + limit : null,
    };

    if (redis) {
      await redis.set(cacheKey, JSON.stringify(response), "EX", 120);
    }

    return response;
  } catch (err) {
    app.log.error(err);
    return reply.code(500).send({ error: "search_failed" });
  }
});

const port = Number(process.env.PORT || 3000);
app.listen({ port, host: "0.0.0.0" }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});