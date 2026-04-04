require("dotenv").config();

const Fastify = require("fastify");
const { Pool } = require("pg");
const Redis = require("ioredis");

const app = Fastify({ logger: true });

const PORT = Number(process.env.PORT || 3000);
const DATABASE_URL = process.env.DATABASE_URL;
const REDIS_URL = process.env.REDIS_URL || "";
const ADMIN_KEY = process.env.ADMIN_KEY || ""; // optional but recommended

if (!DATABASE_URL) {
  throw new Error("Missing DATABASE_URL environment variable");
}

const pool = new Pool({
  connectionString: DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

const redis = REDIS_URL ? new Redis(REDIS_URL) : null;

// ---------- Helpers ----------
function normalizeSubtab(value) {
  if (!value) return "All";
  return String(value).trim();
}

function parseLimit(value, fallback = 40, max = 100) {
  const n = Number(value);
  if (!Number.isFinite(n) || n <= 0) return fallback;
  return Math.min(Math.floor(n), max);
}

function parseCursor(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n < 0) return 0;
  return Math.floor(n);
}

function likeEscape(input) {
  return String(input).replace(/[%_]/g, "\\$&");
}

function isAdmin(req) {
  if (!ADMIN_KEY) return false;
  return req.headers["x-admin-key"] === ADMIN_KEY;
}

// ---------- Schema ----------
async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS catalog_items (
      asset_id BIGINT PRIMARY KEY,
      name TEXT NOT NULL,
      creator_name TEXT,
      description TEXT,
      thumbnail_url TEXT,
      main_tab TEXT NOT NULL DEFAULT 'Clothing',
      subtab TEXT NOT NULL DEFAULT 'All',
      is_offsale BOOLEAN NOT NULL DEFAULT false,
      is_limited BOOLEAN NOT NULL DEFAULT false,
      price_robux INTEGER,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_catalog_main_subtab
      ON catalog_items (main_tab, subtab);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS idx_catalog_name_lower
      ON catalog_items (LOWER(name));
  `);
}

// ---------- Health ----------
app.get("/health", async () => ({ ok: true }));

app.get("/", async () => ({
  message: "Catalog backend running",
  docs: [
    "GET /health",
    "GET /catalog/search",
    "GET /catalog/item/:assetId",
    "GET /catalog/suggested/:assetId",
    "POST /catalog/admin/upsert (requires x-admin-key)",
  ],
}));

// ---------- Search ----------
app.get("/catalog/search", async (req, reply) => {
  const mainTab = String(req.query.mainTab || "Clothing");
  const subtab = normalizeSubtab(req.query.subtab);
  const q = String(req.query.q || "").trim();
  const limit = parseLimit(req.query.limit, 40, 100);
  const offset = parseCursor(req.query.cursor);

  const cacheKey = `search:${mainTab}:${subtab}:${q}:${limit}:${offset}`;

  if (redis) {
    const cached = await redis.get(cacheKey);
    if (cached) {
      return JSON.parse(cached);
    }
  }

  const params = [];
  let where = `WHERE main_tab = $${params.push(mainTab)}`;

  if (subtab.toLowerCase() !== "all") {
    where += ` AND subtab = $${params.push(subtab)}`;
  }

  if (q.length > 0) {
    const escaped = `%${likeEscape(q.toLowerCase())}%`;
    where += ` AND (LOWER(name) LIKE $${params.push(escaped)} ESCAPE '\\' OR LOWER(creator_name) LIKE $${params.push(escaped)} ESCAPE '\\')`;
  }

  const totalSql = `SELECT COUNT(*)::INT AS total FROM catalog_items ${where};`;
  const totalRes = await pool.query(totalSql, params);
  const total = totalRes.rows[0]?.total || 0;

  const pagedParams = [...params, limit, offset];
  const sql = `
    SELECT
      asset_id, name, creator_name, description, thumbnail_url,
      main_tab, subtab, is_offsale, is_limited, price_robux
    FROM catalog_items
    ${where}
    ORDER BY updated_at DESC, asset_id DESC
    LIMIT $${pagedParams.length - 1}
    OFFSET $${pagedParams.length};
  `;
  const res = await pool.query(sql, pagedParams);

  const nextOffset = offset + res.rows.length;
  const nextCursor = nextOffset < total ? String(nextOffset) : null;

  const payload = {
    items: res.rows,
    nextCursor,
    total,
    limit,
    cursor: String(offset),
  };

  if (redis) {
    await redis.set(cacheKey, JSON.stringify(payload), "EX", 120);
  }

  return payload;
});

// ---------- Item Detail ----------
app.get("/catalog/item/:assetId", async (req, reply) => {
  const assetId = Number(req.params.assetId);
  if (!Number.isFinite(assetId)) {
    return reply.code(400).send({ error: "Invalid assetId" });
  }

  const res = await pool.query(
    `
    SELECT
      asset_id, name, creator_name, description, thumbnail_url,
      main_tab, subtab, is_offsale, is_limited, price_robux
    FROM catalog_items
    WHERE asset_id = $1
    LIMIT 1;
    `,
    [assetId]
  );

  if (res.rowCount === 0) {
    return reply.code(404).send({ error: "Item not found" });
  }

  return res.rows[0];
});

app.get("/catalog/suggested/:assetId", async (req, reply) => {
  const assetId = Number(req.params.assetId);
  const limit = parseLimit(req.query.limit, 18, 30);

  if (!Number.isFinite(assetId)) {
    return reply.code(400).send({ error: "Invalid assetId" });
  }

  const itemRes = await pool.query(
    `SELECT main_tab, subtab FROM catalog_items WHERE asset_id = $1 LIMIT 1;`,
    [assetId]
  );

  if (itemRes.rowCount === 0) {
    return { items: [] };
  }

  const { main_tab, subtab } = itemRes.rows[0];

  const res = await pool.query(
    `
    SELECT
      asset_id, name, creator_name, description, thumbnail_url,
      main_tab, subtab, is_offsale, is_limited, price_robux
    FROM catalog_items
    WHERE main_tab = $1
      AND subtab = $2
      AND asset_id <> $3
    ORDER BY updated_at DESC, asset_id DESC
    LIMIT $4;
    `,
    [main_tab, subtab, assetId, limit]
  );

  return { items: res.rows };
});

// ---------- Admin Upsert ----------
app.post("/catalog/admin/upsert", async (req, reply) => {
  if (!isAdmin(req)) {
    return reply.code(401).send({ error: "Unauthorized" });
  }

  const items = Array.isArray(req.body?.items) ? req.body.items : [];
  if (items.length === 0) {
    return reply.code(400).send({ error: "Body must contain non-empty items[]" });
  }

  const client = await pool.connect();
  try {
    await client.query("BEGIN");

    for (const raw of items) {
      const assetId = Number(raw.asset_id);
      if (!Number.isFinite(assetId)) continue;

      await client.query(
        `
        INSERT INTO catalog_items (
          asset_id, name, creator_name, description, thumbnail_url,
          main_tab, subtab, is_offsale, is_limited, price_robux, updated_at
        )
        VALUES (
          $1, $2, $3, $4, $5,
          $6, $7, $8, $9, $10, NOW()
        )
        ON CONFLICT (asset_id) DO UPDATE SET
          name = EXCLUDED.name,
          creator_name = EXCLUDED.creator_name,
          description = EXCLUDED.description,
          thumbnail_url = EXCLUDED.thumbnail_url,
          main_tab = EXCLUDED.main_tab,
          subtab = EXCLUDED.subtab,
          is_offsale = EXCLUDED.is_offsale,
          is_limited = EXCLUDED.is_limited,
          price_robux = EXCLUDED.price_robux,
          updated_at = NOW();
        `,
        [
          assetId,
          String(raw.name || "Unknown Item"),
          raw.creator_name ? String(raw.creator_name) : null,
          raw.description ? String(raw.description) : null,
          raw.thumbnail_url ? String(raw.thumbnail_url) : null,
          String(raw.main_tab || "Clothing"),
          String(raw.subtab || "All"),
          Boolean(raw.is_offsale),
          Boolean(raw.is_limited),
          Number.isFinite(Number(raw.price_robux)) ? Number(raw.price_robux) : null,
        ]
      );
    }

    await client.query("COMMIT");

    if (redis) {
      // lightweight invalidation strategy
      const keys = await redis.keys("search:*");
      if (keys.length > 0) await redis.del(...keys);
    }

    return { ok: true, upserted: items.length };
  } catch (err) {
    await client.query("ROLLBACK");
    req.log.error(err);
    return reply.code(500).send({ error: "Upsert failed" });
  } finally {
    client.release();
  }
});

// ---------- Boot ----------
(async () => {
  await ensureSchema();
  await app.listen({ port: PORT, host: "0.0.0.0" });
  app.log.info(`Server listening on :${PORT}`);
})();