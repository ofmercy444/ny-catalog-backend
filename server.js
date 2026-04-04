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

function toInt(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

app.get("/health", async () => {
  let dbOk = false;
  try {
    await pool.query("SELECT 1");
    dbOk = true;
  } catch (_) {}
  return { ok: true, dbOk };
});

app.get("/catalog/search", async (req, reply) => {
  const category = String(req.query.category || "clothing").toLowerCase();
  const subtab = String(req.query.subtab || "all").toLowerCase();
  const q = String(req.query.q || "").trim().toLowerCase();
  const limit = Math.min(Math.max(toInt(req.query.limit, 60), 1), 120);
  const cursor = req.query.cursor ? toInt(req.query.cursor, null) : null;

  const cacheKey = `search:${category}:${subtab}:${q}:${limit}:${cursor ?? "none"}`;
  if (redis) {
    const cached = await redis.get(cacheKey);
    if (cached) return JSON.parse(cached);
  }

  const values = [];
  const where = [];

  values.push(category);
  where.push(`LOWER(category) = $${values.length}`);

  if (subtab !== "all") {
    values.push(subtab);
    where.push(`LOWER(subtab) = $${values.length}`);
  }

  if (q.length > 0) {
    values.push(`%${q}%`);
    where.push(`(LOWER(name) LIKE $${values.length} OR LOWER(COALESCE(description,'')) LIKE $${values.length})`);
  }

  if (cursor !== null) {
    values.push(cursor);
    where.push(`asset_id < $${values.length}`);
  }

  values.push(limit + 1);

  const sql = `
    SELECT
      asset_id,
      name,
      description,
      creator_id,
      creator_name,
      asset_type,
      category,
      subtab,
      thumbnail_url,
      is_offsale,
      is_limited,
      is_hidden
    FROM catalog_items
    ${where.length ? `WHERE ${where.join(" AND ")}` : ""}
    ORDER BY asset_id DESC
    LIMIT $${values.length}
  `;

  const { rows } = await pool.query(sql, values);

  let nextCursor = null;
  let items = rows;
  if (rows.length > limit) {
    const extra = rows[rows.length - 1];
    nextCursor = String(extra.asset_id);
    items = rows.slice(0, limit);
  }

  const payload = { items, nextCursor };

  if (redis) {
    await redis.set(cacheKey, JSON.stringify(payload), "EX", 60);
  }

  return payload;
});

const port = Number(process.env.PORT || 3000);
app.listen({ port, host: "0.0.0.0" }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});