require("dotenv").config();
const Fastify = require("fastify");
const { Pool } = require("pg");
const Redis = require("ioredis");

const app = Fastify({ logger: true });

const PORT = Number(process.env.PORT || 3000);

// --------- DB ----------
if (!process.env.DATABASE_URL) {
  app.log.error("Missing DATABASE_URL env var");
}

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// --------- Redis (optional) ----------
let redis = null;
if (process.env.REDIS_URL) {
  try {
    redis = new Redis(process.env.REDIS_URL);
    redis.on("error", (err) => app.log.warn({ err }, "Redis connection warning"));
  } catch (err) {
    app.log.warn({ err }, "Redis init failed, continuing without cache");
    redis = null;
  }
} else {
  app.log.info("REDIS_URL not set, running without cache");
}

// --------- Schema ----------
async function ensureSchema() {
  const sql = `
    CREATE TABLE IF NOT EXISTS catalog_items (
      asset_id BIGINT PRIMARY KEY,
      name TEXT NOT NULL,
      asset_type TEXT,
      subtab TEXT,
      creator_id BIGINT,
      creator_name TEXT,
      description TEXT,
      thumbnail_url TEXT,
      is_offsale BOOLEAN DEFAULT FALSE,
      is_limited BOOLEAN DEFAULT FALSE,
      is_hidden BOOLEAN DEFAULT FALSE,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX IF NOT EXISTS idx_catalog_items_subtab ON catalog_items(subtab);
    CREATE INDEX IF NOT EXISTS idx_catalog_items_name ON catalog_items(name);
  `;
  await pool.query(sql);
}

// --------- Routes ----------
app.get("/health", async () => {
  return { ok: true };
});

app.get("/", async () => {
  return { message: "Catalog backend running" };
});

// basic search endpoint
app.get("/catalog/search", async (req, reply) => {
  const q = String(req.query.q || "").trim();
  const subtab = String(req.query.subtab || "all").toLowerCase();
  const limit = Math.min(Number(req.query.limit || 60), 100);
  const offset = Math.max(Number(req.query.offset || 0), 0);

  try {
    const values = [];
    let where = "WHERE 1=1";

    if (subtab !== "all") {
      values.push(subtab);
      where += ` AND LOWER(COALESCE(subtab, '')) = $${values.length}`;
    }

    if (q.length > 0) {
      values.push(`%${q.toLowerCase()}%`);
      where += ` AND LOWER(name) LIKE $${values.length}`;
    }

    values.push(limit);
    values.push(offset);

    const sql = `
      SELECT asset_id, name, asset_type, subtab, creator_id, creator_name, description,
             thumbnail_url, is_offsale, is_limited, is_hidden
      FROM catalog_items
      ${where}
      ORDER BY updated_at DESC
      LIMIT $${values.length - 1}
      OFFSET $${values.length};
    `;

    const result = await pool.query(sql, values);

    return {
      items: result.rows,
      nextOffset: result.rows.length === limit ? offset + limit : null,
    };
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "Search failed" });
  }
});

// admin upsert endpoint (protect later with API key)
app.post("/catalog/admin/upsert", async (req, reply) => {
  const items = Array.isArray(req.body?.items) ? req.body.items : [];
  if (items.length === 0) return { ok: true, upserted: 0 };

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    const sql = `
      INSERT INTO catalog_items (
        asset_id, name, asset_type, subtab, creator_id, creator_name, description,
        thumbnail_url, is_offsale, is_limited, is_hidden, updated_at
      )
      VALUES (
        $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW()
      )
      ON CONFLICT (asset_id) DO UPDATE SET
        name = EXCLUDED.name,
        asset_type = EXCLUDED.asset_type,
        subtab = EXCLUDED.subtab,
        creator_id = EXCLUDED.creator_id,
        creator_name = EXCLUDED.creator_name,
        description = EXCLUDED.description,
        thumbnail_url = EXCLUDED.thumbnail_url,
        is_offsale = EXCLUDED.is_offsale,
        is_limited = EXCLUDED.is_limited,
        is_hidden = EXCLUDED.is_hidden,
        updated_at = NOW();
    `;

    for (const it of items) {
      await client.query(sql, [
        Number(it.asset_id),
        String(it.name || "Unknown Item"),
        it.asset_type || null,
        it.subtab || null,
        it.creator_id ? Number(it.creator_id) : null,
        it.creator_name || null,
        it.description || null,
        it.thumbnail_url || null,
        Boolean(it.is_offsale),
        Boolean(it.is_limited),
        Boolean(it.is_hidden),
      ]);
    }

    await client.query("COMMIT");
    return { ok: true, upserted: items.length };
  } catch (err) {
    await client.query("ROLLBACK");
    req.log.error(err);
    return reply.code(500).send({ error: "Upsert failed" });
  } finally {
    client.release();
  }
});

// --------- Boot ----------
(async () => {
  await ensureSchema();
  await app.listen({ port: PORT, host: "0.0.0.0" });
  app.log.info(`Server listening on :${PORT}`);
})();