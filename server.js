require("dotenv").config();
const Fastify = require("fastify");
const { Pool } = require("pg");
const Redis = require("ioredis");

const app = Fastify({ logger: true });

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl:
    process.env.DATABASE_URL && process.env.DATABASE_URL.includes("localhost")
      ? false
      : { rejectUnauthorized: false },
});

const redis = process.env.REDIS_URL ? new Redis(process.env.REDIS_URL) : null;
const PORT = Number(process.env.PORT || 3000);

const CLASSIC_TSHIRT_TYPE = 2;
const CLASSIC_SHIRT_TYPE = 11;
const CLASSIC_PANTS_TYPE = 12;
const SHOE_LEFT_TYPE = 70;
const SHOE_RIGHT_TYPE = 71;

const LAYERED_TYPES = [64, 65, 66, 67, 68, 69, 70, 71, 72];
const NON_SHOE_LAYERED_TYPES = [64, 65, 66, 67, 68, 69, 72];
const CLASSIC_CLOTHING_TYPES = [CLASSIC_TSHIRT_TYPE, CLASSIC_SHIRT_TYPE, CLASSIC_PANTS_TYPE];

const SUBTAB_ALIASES = {
  all: "all",
  classicshirts: "classic_shirts",
  "classic shirts": "classic_shirts",
  classic_shirts: "classic_shirts",
  classicpants: "classic_pants",
  "classic pants": "classic_pants",
  classic_pants: "classic_pants",
  classictshirts: "classic_t_shirts",
  "classic t-shirts": "classic_t_shirts",
  "classic t shirts": "classic_t_shirts",
  classic_t_shirts: "classic_t_shirts",
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

function getSubtabSpec(subtab) {
  if (subtab === "classic_shirts") return { mode: "classic", allowedTypes: [CLASSIC_SHIRT_TYPE] };
  if (subtab === "classic_pants") return { mode: "classic", allowedTypes: [CLASSIC_PANTS_TYPE] };
  if (subtab === "classic_t_shirts") return { mode: "classic", allowedTypes: [CLASSIC_TSHIRT_TYPE] };

  if (subtab === "shirts") {
    return {
      mode: "layered",
      layeredTypes: [65],
      fallbackClassicTypes: [CLASSIC_SHIRT_TYPE, CLASSIC_TSHIRT_TYPE],
      fallbackTitleRegex: "(shirt|top|tee|t-shirt|t shirt)",
    };
  }
  if (subtab === "jackets") {
    return {
      mode: "layered",
      layeredTypes: [67],
      fallbackClassicTypes: [CLASSIC_SHIRT_TYPE, CLASSIC_TSHIRT_TYPE],
      fallbackTitleRegex: "(jacket|coat|hoodie|zip[ -]?up)",
    };
  }
  if (subtab === "sweaters") {
    return {
      mode: "layered",
      layeredTypes: [68],
      fallbackClassicTypes: [CLASSIC_SHIRT_TYPE, CLASSIC_TSHIRT_TYPE],
      fallbackTitleRegex: "(sweater|cardigan|knit)",
    };
  }
  if (subtab === "t_shirts") {
    return {
      mode: "layered",
      layeredTypes: [64],
      fallbackClassicTypes: [CLASSIC_TSHIRT_TYPE, CLASSIC_SHIRT_TYPE],
      fallbackTitleRegex: "(t-shirt|t shirt|tee)",
    };
  }
  if (subtab === "pants") {
    return {
      mode: "layered",
      layeredTypes: [66],
      fallbackClassicTypes: [CLASSIC_PANTS_TYPE],
      fallbackTitleRegex: "(pants|jeans|trousers|sweatpants|cargo)",
    };
  }
  if (subtab === "shorts") {
    return {
      mode: "layered",
      layeredTypes: [69],
      fallbackClassicTypes: [CLASSIC_PANTS_TYPE],
      fallbackTitleRegex: "(shorts?)",
    };
  }
  if (subtab === "dresses_skirts") {
    return {
      mode: "layered",
      layeredTypes: [72],
      fallbackClassicTypes: [CLASSIC_SHIRT_TYPE, CLASSIC_PANTS_TYPE, CLASSIC_TSHIRT_TYPE],
      fallbackTitleRegex: "(dress|skirt|gown)",
    };
  }

  if (subtab === "shoes") return { mode: "shoes_bundle_parents" };

  return {
    mode: "all_strict",
    layeredTypes: NON_SHOE_LAYERED_TYPES,
    classicTypes: CLASSIC_CLOTHING_TYPES,
  };
}

async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.catalog_items (
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
      asset_type_id INTEGER,
      asset_type_name TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.catalog_bundles (
      bundle_id BIGINT PRIMARY KEY,
      name TEXT NOT NULL,
      description TEXT,
      creator_name TEXT,
      creator_id BIGINT,
      creator_type TEXT,
      bundle_type TEXT,
      category TEXT DEFAULT 'clothing',
      subcategory TEXT DEFAULT 'misc',
      thumbnail_url TEXT,
      updated_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);

  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.bundle_asset_links (
      bundle_id BIGINT NOT NULL,
      asset_id BIGINT NOT NULL,
      role TEXT,
      asset_type_id INTEGER,
      sort_order INTEGER DEFAULT 0,
      updated_at TIMESTAMPTZ DEFAULT NOW(),
      PRIMARY KEY (bundle_id, asset_id)
    );
  `);

  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS asset_type_id INTEGER;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS asset_type_name TEXT;`);
  await pool.query(`ALTER TABLE public.catalog_bundles ADD COLUMN IF NOT EXISTS subcategory TEXT DEFAULT 'misc';`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_category ON public.catalog_items(category);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_updated ON public.catalog_items(updated_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_asset_type_id ON public.catalog_items(asset_type_id);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_name_lower ON public.catalog_items((lower(name)));`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_bundles_subcategory ON public.catalog_bundles(subcategory);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_bundles_updated ON public.catalog_bundles(updated_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_bundles_name_lower ON public.catalog_bundles((lower(name)));`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_bundle_links_bundle_id ON public.bundle_asset_links(bundle_id);`);
}

let schemaReady = false;
async function ensureSchemaOnce() {
  if (schemaReady) return;
  await ensureSchema();
  schemaReady = true;
}

function buildCreatorAvatar(creatorType, creatorId) {
  const t = String(creatorType || "").toLowerCase();
  const id = Number(creatorId);
  if (!Number.isFinite(id) || id <= 0) return "rbxasset://textures/ui/GuiImagePlaceholder.png";
  return t === "group"
    ? `rbxthumb://type=GroupIcon&id=${id}&w=150&h=150`
    : `rbxthumb://type=AvatarHeadShot&id=${id}&w=150&h=150`;
}

function mapItemRow(row) {
  return {
    asset_id: row.asset_id,
    detail_kind: "asset",
    is_bundle_parent: false,
    name: row.name,
    category: row.category,
    item_type: row.item_type,
    asset_type_id: row.asset_type_id,
    asset_type_name: row.asset_type_name,
    creator_id: row.creator_id,
    creator_name: row.creator_name,
    creator_type: row.creator_type,
    description: row.description,
    thumbnail_url: `rbxthumb://type=Asset&id=${row.asset_id}&w=420&h=420`,
    thumbnail_bundle_url: `rbxthumb://type=BundleThumbnail&id=${row.asset_id}&w=420&h=420`,
    thumbnail_raw_url: row.thumbnail_url || "",
    is_offsale: row.is_offsale,
    is_limited: row.is_limited,
    is_limited_unique: row.is_limited_unique,
    price_robux: row.price_robux,
    updated_at: row.updated_at,
    is_layered: LAYERED_TYPES.includes(Number(row.asset_type_id)),
    creator_avatar_url: buildCreatorAvatar(row.creator_type, row.creator_id),
  };
}

function mapBundleRow(row) {
  const thumb =
    row.thumbnail_url && row.thumbnail_url !== ""
      ? row.thumbnail_url
      : `rbxthumb://type=BundleThumbnail&id=${row.bundle_id}&w=420&h=420`;

  return {
    asset_id: row.bundle_id,
    bundle_id: row.bundle_id,
    detail_kind: "bundle",
    is_bundle_parent: true,
    name: row.name,
    category: row.category || "clothing",
    item_type: row.bundle_type || "Bundle",
    asset_type_id: null,
    asset_type_name: "Bundle",
    creator_id: row.creator_id,
    creator_name: row.creator_name,
    creator_type: row.creator_type,
    description: row.description,
    thumbnail_url: thumb,
    thumbnail_bundle_url: thumb,
    thumbnail_raw_url: row.thumbnail_url || "",
    is_offsale: false,
    is_limited: false,
    is_limited_unique: false,
    price_robux: null,
    updated_at: row.updated_at,
    is_layered: false,
    creator_avatar_url: buildCreatorAvatar(row.creator_type, row.creator_id),
  };
}

function mapBundleChildRow(r) {
  const finalType = r.asset_type_id != null ? r.asset_type_id : r.link_asset_type_id;
  const role =
    r.role ||
    (Number(finalType) === SHOE_LEFT_TYPE
      ? "left_shoe"
      : Number(finalType) === SHOE_RIGHT_TYPE
      ? "right_shoe"
      : null);

  return {
    asset_id: r.asset_id,
    detail_kind: "asset",
    is_bundle_parent: false,
    name: r.name || `Asset ${r.asset_id}`,
    item_type: r.item_type || "",
    description: r.description || "",
    creator_name: r.creator_name || "",
    creator_id: r.creator_id,
    creator_type: r.creator_type || "",
    asset_type_id: finalType,
    asset_type_name: r.asset_type_name || "",
    thumbnail_url: `rbxthumb://type=Asset&id=${r.asset_id}&w=150&h=150`,
    thumbnail_bundle_url: `rbxthumb://type=BundleThumbnail&id=${r.asset_id}&w=150&h=150`,
    thumbnail_raw_url: r.thumbnail_url || "",
    role,
    creator_avatar_url: buildCreatorAvatar(r.creator_type, r.creator_id),
  };
}

function buildItemWhereAndOrder(spec, q, params) {
  let where = "WHERE lower(i.category) = $1";
  let orderSql = "i.updated_at DESC, i.asset_id DESC";

  if (q.length > 0) {
    where += ` AND lower(coalesce(i.name,'')) LIKE $${params.length + 1}`;
    params.push(`%${q}%`);
  }

  if (spec.mode === "classic") {
    where += ` AND i.asset_type_id = ANY($${params.length + 1}::int[])`;
    params.push(spec.allowedTypes);
  } else if (spec.mode === "all_strict") {
    const layeredIdx = params.length + 1;
    params.push(spec.layeredTypes);

    const classicIdx = params.length + 1;
    params.push(spec.classicTypes);

    where += `
      AND (
        i.asset_type_id = ANY($${layeredIdx}::int[])
        OR i.asset_type_id = ANY($${classicIdx}::int[])
      )
    `;

    orderSql = `
      CASE
        WHEN i.asset_type_id = ANY($${layeredIdx}::int[]) THEN 0
        ELSE 1
      END,
      i.updated_at DESC,
      i.asset_id DESC
    `;
  } else if (spec.mode === "layered") {
    const layeredIdx = params.length + 1;
    params.push(spec.layeredTypes);

    if (spec.fallbackClassicTypes.length > 0) {
      const fallbackTypesIdx = params.length + 1;
      params.push(spec.fallbackClassicTypes);

      const fallbackRegexIdx = params.length + 1;
      params.push(spec.fallbackTitleRegex);

      where += `
        AND (
          i.asset_type_id = ANY($${layeredIdx}::int[])
          OR (
            i.asset_type_id = ANY($${fallbackTypesIdx}::int[])
            AND lower(coalesce(i.name,'')) ~ $${fallbackRegexIdx}
          )
        )
      `;
    } else {
      where += ` AND i.asset_type_id = ANY($${layeredIdx}::int[])`;
    }

    orderSql = `
      CASE
        WHEN i.asset_type_id = ANY($${layeredIdx}::int[]) THEN 0
        ELSE 1
      END,
      i.updated_at DESC,
      i.asset_id DESC
    `;
  }

  return { where, orderSql };
}

async function fetchBundleChildren(bundleId) {
  const linksRes = await pool.query(
    `
    SELECT
      l.bundle_id, l.asset_id, l.role, l.asset_type_id AS link_asset_type_id, l.sort_order,
      i.name, i.description, i.creator_name, i.creator_id, i.creator_type, i.item_type,
      i.asset_type_id, i.asset_type_name, i.thumbnail_url
    FROM public.bundle_asset_links l
    LEFT JOIN public.catalog_items i ON i.asset_id = l.asset_id
    WHERE l.bundle_id = $1
      AND l.asset_type_id IN (${SHOE_LEFT_TYPE}, ${SHOE_RIGHT_TYPE})
    ORDER BY l.sort_order ASC, l.asset_id ASC
    `,
    [bundleId]
  );

  const mapped = linksRes.rows.map(mapBundleChildRow);
  const left = mapped.find((x) => Number(x.asset_type_id) === SHOE_LEFT_TYPE);
  const right = mapped.find((x) => Number(x.asset_type_id) === SHOE_RIGHT_TYPE);

  const ordered = [];
  if (left) ordered.push(left);
  if (right) ordered.push(right);
  return ordered;
}

app.get("/", async () => ({ ok: true, service: "catalog-backend" }));
app.get("/health", async () => ({ ok: true }));

app.get("/catalog/search", async (req, reply) => {
  try {
    await ensureSchemaOnce();

    const category = String(req.query.category || "clothing").toLowerCase();
    const subtab = normalizeTabKey(req.query.subtab || "all");
    const q = String(req.query.q || "").trim().toLowerCase();
    const limit = Math.min(Math.max(Number(req.query.limit || 30), 1), 60);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    const cacheKey = `search:v25:${category}:${subtab}:${q}:${limit}:${offset}`;
    if (redis) {
      const cached = await redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    }

    const spec = getSubtabSpec(subtab);

    // SHOES subtab: parent bundles only, searchable by parent OR child shoe names
    if (spec.mode === "shoes_bundle_parents") {
      const params = [category];
      let where = `
        WHERE lower(b.category) = $1
          AND lower(b.subcategory) = 'shoes'
          AND EXISTS (
            SELECT 1 FROM public.bundle_asset_links l
            WHERE l.bundle_id = b.bundle_id AND l.asset_type_id = ${SHOE_LEFT_TYPE}
          )
          AND EXISTS (
            SELECT 1 FROM public.bundle_asset_links l
            WHERE l.bundle_id = b.bundle_id AND l.asset_type_id = ${SHOE_RIGHT_TYPE}
          )
      `;

      if (q.length > 0) {
        const qIdx = params.length + 1;
        params.push(`%${q}%`);
        where += `
          AND (
            lower(coalesce(b.name,'')) LIKE $${qIdx}
            OR EXISTS (
              SELECT 1
              FROM public.bundle_asset_links l2
              JOIN public.catalog_items i2 ON i2.asset_id = l2.asset_id
              WHERE l2.bundle_id = b.bundle_id
                AND lower(coalesce(i2.name,'')) LIKE $${qIdx}
            )
          )
        `;
      }

      params.push(limit, offset);

      const res = await pool.query(
        `
        SELECT
          b.bundle_id, b.name, b.description, b.creator_name, b.creator_id, b.creator_type,
          b.bundle_type, b.category, b.subcategory, b.thumbnail_url, b.updated_at
        FROM public.catalog_bundles b
        ${where}
        ORDER BY b.updated_at DESC, b.bundle_id DESC
        LIMIT $${params.length - 1}
        OFFSET $${params.length}
        `,
        params
      );

      const items = res.rows.map(mapBundleRow);
      const response = {
        items,
        nextOffset: items.length === limit ? offset + limit : null,
        subtabKey: subtab,
      };

      if (redis) await redis.set(cacheKey, JSON.stringify(response), "EX", 120);
      return response;
    }

    // ALL subtab: exclude child shoes from item rows, include shoe bundle parents (searchable)
    if (spec.mode === "all_strict") {
      const params = [category];
      let itemNameFilter = "";
      let bundleNameFilter = "";

      if (q.length > 0) {
        const qIdx = params.length + 1;
        params.push(`%${q}%`);
        itemNameFilter = `AND lower(coalesce(i.name,'')) LIKE $${qIdx}`;
        bundleNameFilter = `
          AND (
            lower(coalesce(b.name,'')) LIKE $${qIdx}
            OR EXISTS (
              SELECT 1
              FROM public.bundle_asset_links l2
              JOIN public.catalog_items i2 ON i2.asset_id = l2.asset_id
              WHERE l2.bundle_id = b.bundle_id
                AND lower(coalesce(i2.name,'')) LIKE $${qIdx}
            )
          )
        `;
      }

      params.push(limit, offset);

      const res = await pool.query(
        `
        WITH item_rows AS (
          SELECT
            i.asset_id::bigint AS entity_id,
            'asset'::text AS detail_kind,
            false AS is_bundle_parent,

            i.asset_id,
            NULL::bigint AS bundle_id,
            i.name,
            i.category,
            i.item_type,
            i.asset_type_id,
            i.asset_type_name,
            i.creator_id,
            i.creator_name,
            i.creator_type,
            i.description,
            i.thumbnail_url,
            i.is_offsale,
            i.is_limited,
            i.is_limited_unique,
            i.price_robux,
            i.updated_at,

            CASE
              WHEN i.asset_type_id = ANY(ARRAY[${NON_SHOE_LAYERED_TYPES.join(",")}]::int[]) THEN 0
              ELSE 2
            END AS rank_group
          FROM public.catalog_items i
          WHERE lower(i.category) = $1
            ${itemNameFilter}
            AND (
              i.asset_type_id = ANY(ARRAY[${NON_SHOE_LAYERED_TYPES.join(",")}]::int[])
              OR i.asset_type_id = ANY(ARRAY[${CLASSIC_CLOTHING_TYPES.join(",")}]::int[])
            )
        ),
        bundle_rows AS (
          SELECT
            b.bundle_id::bigint AS entity_id,
            'bundle'::text AS detail_kind,
            true AS is_bundle_parent,

            NULL::bigint AS asset_id,
            b.bundle_id,
            b.name,
            b.category,
            b.bundle_type AS item_type,
            NULL::int AS asset_type_id,
            'Bundle'::text AS asset_type_name,
            b.creator_id,
            b.creator_name,
            b.creator_type,
            b.description,
            b.thumbnail_url,
            false AS is_offsale,
            false AS is_limited,
            false AS is_limited_unique,
            NULL::int AS price_robux,
            b.updated_at,

            1 AS rank_group
          FROM public.catalog_bundles b
          WHERE lower(b.category) = $1
            AND lower(b.subcategory) = 'shoes'
            AND EXISTS (
              SELECT 1 FROM public.bundle_asset_links l
              WHERE l.bundle_id = b.bundle_id AND l.asset_type_id = ${SHOE_LEFT_TYPE}
            )
            AND EXISTS (
              SELECT 1 FROM public.bundle_asset_links l
              WHERE l.bundle_id = b.bundle_id AND l.asset_type_id = ${SHOE_RIGHT_TYPE}
            )
            ${bundleNameFilter}
        )
        SELECT *
        FROM (
          SELECT * FROM item_rows
          UNION ALL
          SELECT * FROM bundle_rows
        ) u
        ORDER BY u.rank_group ASC, u.updated_at DESC, u.entity_id DESC
        LIMIT $${params.length - 1}
        OFFSET $${params.length}
        `,
        params
      );

      const items = res.rows.map((r) =>
        r.detail_kind === "bundle"
          ? mapBundleRow({
              bundle_id: r.bundle_id,
              name: r.name,
              description: r.description,
              creator_name: r.creator_name,
              creator_id: r.creator_id,
              creator_type: r.creator_type,
              bundle_type: r.item_type,
              category: r.category,
              subcategory: "shoes",
              thumbnail_url: r.thumbnail_url,
              updated_at: r.updated_at,
            })
          : mapItemRow({
              asset_id: r.asset_id,
              name: r.name,
              category: r.category,
              item_type: r.item_type,
              asset_type_id: r.asset_type_id,
              asset_type_name: r.asset_type_name,
              creator_id: r.creator_id,
              creator_name: r.creator_name,
              creator_type: r.creator_type,
              description: r.description,
              thumbnail_url: r.thumbnail_url,
              is_offsale: r.is_offsale,
              is_limited: r.is_limited,
              is_limited_unique: r.is_limited_unique,
              price_robux: r.price_robux,
              updated_at: r.updated_at,
            })
      );

      const response = {
        items,
        nextOffset: items.length === limit ? offset + limit : null,
        subtabKey: subtab,
      };

      if (redis) await redis.set(cacheKey, JSON.stringify(response), "EX", 120);
      return response;
    }

    // other subtabs
    const params = [category];
    const { where, orderSql } = buildItemWhereAndOrder(spec, q, params);
    params.push(limit, offset);

    const res = await pool.query(
      `
      SELECT
        i.asset_id, i.name, i.category, i.item_type, i.asset_type_id, i.asset_type_name,
        i.creator_id, i.creator_name, i.creator_type, i.description, i.thumbnail_url,
        i.is_offsale, i.is_limited, i.is_limited_unique, i.price_robux, i.updated_at
      FROM public.catalog_items i
      ${where}
      ORDER BY ${orderSql}
      LIMIT $${params.length - 1}
      OFFSET $${params.length}
      `,
      params
    );

    const items = res.rows.map(mapItemRow);
    const response = {
      items,
      nextOffset: items.length === limit ? offset + limit : null,
      subtabKey: subtab,
    };

    if (redis) await redis.set(cacheKey, JSON.stringify(response), "EX", 120);
    return response;
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "catalog_search_failed" });
  }
});

app.get("/catalog/item/:id", async (req, reply) => {
  try {
    await ensureSchemaOnce();

    const id = Number(req.params.id);
    const kind = String(req.query.kind || "asset").toLowerCase();

    if (!Number.isFinite(id)) {
      return reply.code(400).send({ error: "invalid_id" });
    }

    if (kind === "bundle") {
      const bundleRes = await pool.query(
        `
        SELECT
          bundle_id, name, description, creator_name, creator_id, creator_type,
          bundle_type, category, subcategory, thumbnail_url, updated_at
        FROM public.catalog_bundles
        WHERE bundle_id = $1
          AND lower(subcategory) = 'shoes'
        LIMIT 1
        `,
        [id]
      );

      if (bundleRes.rows.length === 0) {
        return reply.code(404).send({ error: "bundle_not_found" });
      }

      const bundleItems = await fetchBundleChildren(id);

      return {
        item: mapBundleRow(bundleRes.rows[0]),
        bundle_items: bundleItems,
        detail_mode: "bundle_parent",
        can_wear: true,
        can_purchase: true,
        show_accessory_scalers: false,
      };
    }

    const assetRes = await pool.query(
      `
      SELECT
        asset_id, name, category, item_type, asset_type_id, asset_type_name,
        creator_id, creator_name, creator_type, description, thumbnail_url,
        is_offsale, is_limited, is_limited_unique, price_robux, updated_at
      FROM public.catalog_items
      WHERE asset_id = $1
      LIMIT 1
      `,
      [id]
    );

    if (assetRes.rows.length === 0) {
      return reply.code(404).send({ error: "item_not_found" });
    }

    const item = mapItemRow(assetRes.rows[0]);
    const t = Number(item.asset_type_id);

    if (t === SHOE_LEFT_TYPE || t === SHOE_RIGHT_TYPE) {
      return {
        item,
        bundle_items: [],
        detail_mode: "bundle_child",
        can_wear: true,
        can_purchase: false,
        show_accessory_scalers: false,
      };
    }

    return {
      item,
      bundle_items: [],
      detail_mode: "regular",
      can_wear: true,
      can_purchase: true,
      show_accessory_scalers: false,
    };
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "catalog_item_failed" });
  }
});

app.listen({ port: PORT, host: "0.0.0.0" }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});