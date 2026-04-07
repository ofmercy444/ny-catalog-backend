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

// Canonical Roblox type ids
const CLASSIC_TSHIRT_TYPE = 2;
const CLASSIC_SHIRT_TYPE = 11;
const CLASSIC_PANTS_TYPE = 12;
const HAT_ACCESSORY_TYPE = 8;
const HAIR_ACCESSORY_TYPE = 41;
const FACE_ACCESSORY_TYPE = 42;
const NECK_ACCESSORY_TYPE = 43;
const SHOULDER_ACCESSORY_TYPE = 44;
const FRONT_ACCESSORY_TYPE = 45;
const BACK_ACCESSORY_TYPE = 46;
const WAIST_ACCESSORY_TYPE = 47;

const LAYERED_TYPES = [64, 65, 66, 67, 68, 69, 70, 71, 72];
const ACCESSORY_UI_TYPES = [
  HAT_ACCESSORY_TYPE,
  FACE_ACCESSORY_TYPE,
  NECK_ACCESSORY_TYPE,
  SHOULDER_ACCESSORY_TYPE,
  FRONT_ACCESSORY_TYPE,
  BACK_ACCESSORY_TYPE,
  WAIST_ACCESSORY_TYPE,
];

const CLOTHING_SUBTAB_ALIASES = {
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

const ACCESSORY_SUBTAB_ALIASES = {
  all: "all",
  head: "head",
  hat: "head",
  hats: "head",
  face: "face",
  neck: "neck",
  shoulder: "shoulder",
  shoulders: "shoulder",
  front: "front",
  back: "back",
  waist: "waist",
  hair: "hair",
};

function normalizeTabKey(raw, category) {
  const cleaned = String(raw || "all")
    .toLowerCase()
    .trim()
    .replace(/\s+/g, " ")
    .replace(/-/g, " ");
  const compact = cleaned.replace(/[&\s_-]/g, "");
  const source = String(category || "").toLowerCase() === "accessories"
    ? ACCESSORY_SUBTAB_ALIASES
    : CLOTHING_SUBTAB_ALIASES;
  return source[cleaned] || source[compact] || "all";
}

function getSubtabSpec(category, subtab) {
  if (category === "accessories") {
    if (subtab === "head") return { mode: "typed", allowedTypes: [HAT_ACCESSORY_TYPE] };
    if (subtab === "face") return { mode: "typed", allowedTypes: [FACE_ACCESSORY_TYPE] };
    if (subtab === "neck") return { mode: "typed", allowedTypes: [NECK_ACCESSORY_TYPE] };
    if (subtab === "shoulder") return { mode: "typed", allowedTypes: [SHOULDER_ACCESSORY_TYPE] };
    if (subtab === "front") return { mode: "typed", allowedTypes: [FRONT_ACCESSORY_TYPE] };
    if (subtab === "back") return { mode: "typed", allowedTypes: [BACK_ACCESSORY_TYPE] };
    if (subtab === "waist") return { mode: "typed", allowedTypes: [WAIST_ACCESSORY_TYPE] };
    if (subtab === "hair") return { mode: "typed", allowedTypes: [HAIR_ACCESSORY_TYPE] };
    // Accessories "all" excludes hair; hair is shown in Body > Hair.
    return { mode: "typed", allowedTypes: ACCESSORY_UI_TYPES };
  }

  // Strict classic tabs
  if (subtab === "classic_shirts") {
    return {
      mode: "classic",
      allowedTypes: [CLASSIC_SHIRT_TYPE],
    };
  }
  if (subtab === "classic_pants") {
    return {
      mode: "classic",
      allowedTypes: [CLASSIC_PANTS_TYPE],
    };
  }
  if (subtab === "classic_t_shirts") {
    return {
      mode: "classic",
      allowedTypes: [CLASSIC_TSHIRT_TYPE],
    };
  }

  // Layered-priority tabs
  if (subtab === "shirts") {
    return {
      mode: "layered",
      layeredTypes: [65], // ShirtAccessory
      fallbackClassicTypes: [CLASSIC_SHIRT_TYPE, CLASSIC_TSHIRT_TYPE],
      fallbackTitleRegex: "(shirt|top|tee|t-shirt|t shirt)",
    };
  }
  if (subtab === "jackets") {
    return {
      mode: "layered",
      layeredTypes: [67], // JacketAccessory
      fallbackClassicTypes: [CLASSIC_SHIRT_TYPE, CLASSIC_TSHIRT_TYPE],
      fallbackTitleRegex: "(jacket|coat|hoodie|zip[ -]?up)",
    };
  }
  if (subtab === "sweaters") {
    return {
      mode: "layered",
      layeredTypes: [68], // SweaterAccessory
      fallbackClassicTypes: [CLASSIC_SHIRT_TYPE, CLASSIC_TSHIRT_TYPE],
      fallbackTitleRegex: "(sweater|cardigan|knit)",
    };
  }
  if (subtab === "t_shirts") {
    return {
      mode: "layered",
      layeredTypes: [64], // TShirtAccessory
      fallbackClassicTypes: [CLASSIC_TSHIRT_TYPE, CLASSIC_SHIRT_TYPE],
      fallbackTitleRegex: "(t-shirt|t shirt|tee)",
    };
  }
  if (subtab === "pants") {
    return {
      mode: "layered",
      layeredTypes: [66], // PantsAccessory
      fallbackClassicTypes: [CLASSIC_PANTS_TYPE],
      fallbackTitleRegex: "(pants|jeans|trousers|sweatpants|cargo)",
    };
  }
  if (subtab === "shorts") {
    return {
      mode: "layered",
      layeredTypes: [69], // ShortsAccessory
      fallbackClassicTypes: [CLASSIC_PANTS_TYPE],
      fallbackTitleRegex: "(shorts?)",
    };
  }
  if (subtab === "dresses_skirts") {
    return {
      mode: "layered",
      layeredTypes: [72], // DressSkirtAccessory
      fallbackClassicTypes: [CLASSIC_SHIRT_TYPE, CLASSIC_PANTS_TYPE, CLASSIC_TSHIRT_TYPE],
      fallbackTitleRegex: "(dress|skirt|gown)",
    };
  }
  if (subtab === "shoes") {
    return {
      mode: "layered",
      layeredTypes: [70, 71], // LeftShoe/RightShoe
      fallbackClassicTypes: [], // no true classic shoes equivalent
      fallbackTitleRegex: "(shoe|shoes|sneaker|sneakers|boot|boots|heel|heels)",
    };
  }

  // "all"
  return { mode: "all" };
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

  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS asset_type_id INTEGER;`);
  await pool.query(`ALTER TABLE public.catalog_items ADD COLUMN IF NOT EXISTS asset_type_name TEXT;`);

  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_category ON public.catalog_items(category);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_updated ON public.catalog_items(updated_at DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_asset_type_id ON public.catalog_items(asset_type_id);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS idx_catalog_items_name_lower ON public.catalog_items((lower(name)));`);
}

let schemaReady = false;
async function ensureSchemaOnce() {
  if (schemaReady) return;
  await ensureSchema();
  schemaReady = true;
}

app.get("/", async () => ({ ok: true, service: "catalog-backend" }));
app.get("/health", async () => ({ ok: true }));

app.get("/catalog/search", async (req, reply) => {
  try {
    await ensureSchemaOnce();

    const category = String(req.query.category || "clothing").toLowerCase();
    const subtab = normalizeTabKey(req.query.subtab || "all", category);
    const q = String(req.query.q || "").trim().toLowerCase();
    const limit = Math.min(Math.max(Number(req.query.limit || 30), 1), 60);
    const offset = Math.max(Number(req.query.offset || 0), 0);

    // cache namespace bump
    const cacheKey = `search:v12:${category}:${subtab}:${q}:${limit}:${offset}`;
    if (redis) {
      const cached = await redis.get(cacheKey);
      if (cached) return JSON.parse(cached);
    }

    const spec = getSubtabSpec(category, subtab);

    const params = [category];
    let where = "WHERE lower(i.category) = $1";

    // title-only search (name only)
    if (q.length > 0) {
      where += ` AND lower(coalesce(i.name,'')) LIKE $${params.length + 1}`;
      params.push(`%${q}%`);
    }

    let orderSql = "i.updated_at DESC, i.asset_id DESC";

    if (spec.mode === "typed" || spec.mode === "classic") {
      where += ` AND i.asset_type_id = ANY($${params.length + 1}::int[])`;
      params.push(spec.allowedTypes);
      // strict typed filtering for accessory/classic tabs
    } else if (spec.mode === "layered") {
      const layeredParamIdx = params.length + 1;
      params.push(spec.layeredTypes);

      if (spec.fallbackClassicTypes.length > 0) {
        const fallbackTypesIdx = params.length + 1;
        params.push(spec.fallbackClassicTypes);

        const fallbackRegexIdx = params.length + 1;
        params.push(spec.fallbackTitleRegex);

        where += `
          AND (
            i.asset_type_id = ANY($${layeredParamIdx}::int[])
            OR (
              i.asset_type_id = ANY($${fallbackTypesIdx}::int[])
              AND lower(coalesce(i.name,'')) ~ $${fallbackRegexIdx}
            )
          )
        `;
      } else {
        where += ` AND i.asset_type_id = ANY($${layeredParamIdx}::int[])`;
      }

      // layered first, fallback classic after
      orderSql = `
        CASE
          WHEN i.asset_type_id = ANY($${layeredParamIdx}::int[]) THEN 0
          ELSE 1
        END,
        i.updated_at DESC,
        i.asset_id DESC
      `;
    } else {
      // mode all: no strict type slicing
    }

    params.push(limit, offset);

    const sql = `
      SELECT
        i.asset_id,
        i.name,
        i.category,
        i.item_type,
        i.asset_type_id,
        i.asset_type_name,
        i.creator_id,
        i.creator_name,
        i.creator_type,
        i.description,

        'rbxthumb://type=Asset&id=' || i.asset_id::text || '&w=420&h=420' AS thumbnail_url,
        'rbxthumb://type=BundleThumbnail&id=' || i.asset_id::text || '&w=420&h=420' AS thumbnail_bundle_url,
        COALESCE(i.thumbnail_url, '') AS thumbnail_raw_url,

        i.is_offsale,
        i.is_limited,
        i.is_limited_unique,
        i.price_robux,
        i.updated_at,

        CASE
          WHEN i.asset_type_id = ANY(ARRAY[${LAYERED_TYPES.join(",")} ]::int[]) THEN true
          ELSE false
        END AS is_layered,

        CASE
          WHEN lower(coalesce(i.creator_type, '')) = 'group' AND i.creator_id IS NOT NULL
            THEN 'rbxthumb://type=GroupIcon&id=' || i.creator_id::text || '&w=150&h=150'
          WHEN i.creator_id IS NOT NULL
            THEN 'rbxthumb://type=AvatarHeadShot&id=' || i.creator_id::text || '&w=150&h=150'
          ELSE 'rbxasset://textures/ui/GuiImagePlaceholder.png'
        END AS creator_avatar_url
      FROM public.catalog_items i
      ${where}
      ORDER BY ${orderSql}
      LIMIT $${params.length - 1}
      OFFSET $${params.length};
    `;

    const { rows } = await pool.query(sql, params);

    const response = {
      items: rows,
      nextOffset: rows.length === limit ? offset + limit : null,
      subtabKey: subtab,
    };

    if (redis) {
      await redis.set(cacheKey, JSON.stringify(response), "EX", 120);
    }

    return response;
  } catch (err) {
    req.log.error(err);
    return reply.code(500).send({ error: "catalog_search_failed" });
  }
});

app.listen({ port: PORT, host: "0.0.0.0" }).catch((err) => {
  app.log.error(err);
  process.exit(1);
});