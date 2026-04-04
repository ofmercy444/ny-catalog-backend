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
    const q = (req.query.q || "").trim();
    const category = (req.query.category || "clothing").trim();
    const subcategory = (req.query.subcategory || "all").trim();
    const limit = Math.min(Number(req.query.limit || 40), 100);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const cacheKey = `search:${category}:${subcategory}:${q}:${limit}:${offset}`;
    if (redis) {
      const cached = await redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    }

    const values = [category, limit, offset];
    let where = `WHERE category = $1`;

    if (subcategory.toLowerCase() !== "all") {
      values.push(subcategory);
      where += ` AND subcategory = $${values.length}`;
    }

    if (q.length > 0) {
      values.push(`%${q.toLowerCase()}%`);
      where += ` AND LOWER(name) LIKE $${values.length}`;
    }

    const sql = `
      SELECT asset_id, name, creator_name, thumbnail_url, is_offsale, is_limited, is_hidden
      FROM catalog_items
      ${where}
      ORDER BY asset_id DESC
      LIMIT $2 OFFSET $3
    `;

    const { rows } = await pool.query(sql, values);

    const result = {
      items: rows,
      nextOffset: rows.length === limit ? offset + limit : null,
    };

    if (redis) {
      await redis.set(cacheKey, JSON.stringify(result), "EX", 60);
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