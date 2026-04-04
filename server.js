require("dotenv").config();
const Fastify = require("fastify");
const { Pool } = require("pg");

const app = Fastify({ logger: true });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl:
    process.env.DATABASE_URL && process.env.DATABASE_URL.includes("localhost")
      ? false
      : { rejectUnauthorized: false },
});

const PORT = Number(process.env.PORT || 3000);

// Shared text bucket for matching
const TEXT_SQL = "lower(coalesce(name,'') || ' ' || coalesce(description,''))";

// Classic/2D detection
const CLASSIC_REGEX =
  "(classic shirt|classic pants|classic t-shirt|classic t shirt|template shirt|template pants|template|2d clothing|2d)";

function normalizeTabKey(v) {
  return String(v || "all")
    .toLowerCase()
    .trim()
    .replace(/&/g, "and")
    .replace(/\s+/g, "_");
}

function getTabFilterSql(subtabKey) {
  switch (subtabKey) {
    case "classic_shirts":
      return `${TEXT_SQL} ~ '(classic shirt|template shirt)'`;

    case "classic_pants":
      return `${TEXT_SQL} ~ '(classic pants|template pants)'`;

    case "classic_t_shirts":
      return `${TEXT_SQL} ~ '(classic t-shirt|classic t shirt|t-shirt template|t shirt template)'`;

    case "shirts":
      return `${TEXT_SQL} ~ '(shirt|top|blouse)' AND NOT (${TEXT_SQL} ~ '${CLASSIC_REGEX}')`;

    case "jackets":
      return `${TEXT_SQL} ~ '(jacket|coat|hoodie|zip up|zip-up)' AND NOT (${TEXT_SQL} ~ '${CLASSIC_REGEX}')`;

    case "sweaters":
      return `${TEXT_SQL} ~ '(sweater|cardigan|knit)' AND NOT (${TEXT_SQL} ~ '${CLASSIC_REGEX}')`;

    case "t_shirts":
      return `${TEXT_SQL} ~ '(t-shirt|t shirt|tee)' AND NOT (${TEXT_SQL} ~ '${CLASSIC_REGEX}')`;

    case "pants":
      return `${TEXT_SQL} ~ '(pants|trousers|jeans|sweatpants|cargo)' AND NOT (${TEXT_SQL} ~ '${CLASSIC_REGEX}')`;

    case "shorts":
      return `${TEXT_SQL} ~ '(shorts)' AND NOT (${TEXT_SQL} ~ '${CLASSIC_REGEX}')`;

    case "dresses_skirts":
      return `${TEXT_SQL} ~ '(dress|skirt|gown)' AND NOT (${TEXT_SQL} ~ '${CLASSIC_REGEX}')`;

    case "shoes":
      return `${TEXT_SQL} ~ '(shoe|shoes|sneaker|sneakers|boot|boots|heel|heels|loafer|slipper)' AND NOT (${TEXT_SQL} ~ '${CLASSIC_REGEX}')`;

    case "all":
    default:
      return "TRUE";
  }
}

function getLayeredOrderSql(subtabKey) {
  const layeredFirstTabs = new Set([
    "shirts",
    "jackets",
    "sweaters",
    "t_shirts",
    "pants",
    "shorts",
    "dresses_skirts",
    "shoes",
    "all",
  ]);

  const classicFirstTabs = new Set([
    "classic_shirts",
    "classic_pants",
    "classic_t_shirts",
  ]);

  if (layeredFirstTabs.has(subtabKey)) {
    return "is_layered DESC"; // layered first
  }
  if (classicFirstTabs.has(subtabKey)) {
    return "is_layered ASC"; // classic first
  }
  return "updated_at DESC";
}

app.get("/", async () => ({ ok: true, service: "catalog-backend" }));
app.get("/health", async () => ({ ok: true }));

app.get("/catalog/search", async (req, reply) => {
  try {
    const category = String(req.query.category || "clothing").toLowerCase();
    const subtabKey = normalizeTabKey(req.query.subtab || "all");
    const q = String(req.query.q || "").trim().toLowerCase();

    const limit = Math.max(1, Math.min(60, Number(req.query.limit || 30)));
    const offset = Math.max(0, Number(req.query.offset || 0));

    const tabFilterSql = getTabFilterSql(subtabKey);
    const layeredOrderSql = getLayeredOrderSql(subtabKey);

    const isLayeredSql = `NOT (${TEXT_SQL} ~ '${CLASSIC_REGEX}')`;

    const params = [category, limit, offset];
    let qFilterSql = "TRUE";

    if (q.length > 0) {
      params.push(`%${q}%`);
      qFilterSql = `${TEXT_SQL} LIKE $4`;
    }

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
        updated_at,
        ${isLayeredSql} AS is_layered
      FROM public.catalog_items
      WHERE category = $1
        AND (${tabFilterSql})
        AND (${qFilterSql})
      ORDER BY ${layeredOrderSql}, updated_at DESC, asset_id DESC
      LIMIT $2
      OFFSET $3
    `;

    const { rows } = await pool.query(sql, params);

    return {
      items: rows,
      nextOffset: rows.length === limit ? offset + limit : null,
      subtabKey,
    };
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "catalog_search_failed" });
  }
});

app
  .listen({ port: PORT, host: "0.0.0.0" })
  .then(() => app.log.info(`Catalog backend listening on ${PORT}`))
  .catch((err) => {
    app.log.error(err);
    process.exit(1);
  });