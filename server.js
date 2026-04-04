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

app.get("/", async () => ({ message: "Catalog backend running" }));

app.get("/catalog/search", async (req, reply) => {
  const {
    subcategory = "all",
    q = "",
    limit = "40",
    offset = "0",
  } = req.query || {};

  const safeLimit = Math.min(Math.max(parseInt(limit, 10) || 40, 1), 100);
  const safeOffset = Math.max(parseInt(offset, 10) || 0, 0);

  const cacheKey = `search:${subcategory}:${q}:${safeLimit}:${safeOffset}`;

  if (redis) {
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);
  }

  const values = [];
  let where = "WHERE 1=1";

  if (subcategory && subcategory.toLowerCase() !== "all") {
    values.push(subcategory.toLowerCase());
    where += ` AND LOWER(subcategory) = $${values.length}`;
  }

  if (q && q.trim() !== "") {
    values.push(`%${q.trim().toLowerCase()}%`);
    where += ` AND LOWER(name) LIKE $${values.length}`;
  }

  values.push(safeLimit);
  values.push(safeOffset);

  const sql = `
    SELECT
      asset_id,
      name,
      asset_type,
      subcategory,
      creator_id,
      creator_name,
      description,
      thumbnail_url,
      is_offsale,
      is_limited,
      is_hidden
    FROM catalog_items
    ${where}
    ORDER BY asset_id DESC
    LIMIT $${values.length - 1}
    OFFSET $${values.length};
  `;

  const { rows } = await pool.query(sql, values);

  const result = {
    items: rows,
    nextOffset: rows.length === safeLimit ? safeOffset + safeLimit : null,
  };

  if (redis) {
    await redis.set(cacheKey, JSON.stringify(result), "EX", 120);
  }

  return result;
});

const port = Number(process.env.PORT || 3000);

app.listen({ port, host: "0.0.0.0" }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});