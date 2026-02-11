require("dotenv").config();

const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json());

// ✅ вместо app.options("*", cors());
app.options(/.*/, cors());


const ADMINS = new Set([
  "R_Kasymkhan",
  "K_Ermek",
  "B_Erkin",
  "1"
]);

function isAdmin(login) {
  return ADMINS.has(String(login || "").trim());
}

// ===============================
// PostgreSQL (Render)
// ===============================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// ===============================
// Init: sequences for FT and ZVK ids
// ===============================
async function initDb() {
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS ft_id_seq START 1;`);
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS zvk_id_seq START 1;`);
  console.log("DB init OK (ft_id_seq, zvk_id_seq)");
}

initDb().catch((e) => console.error("DB init error:", e));

// ===============================
// Health
// ===============================
app.get("/", (req, res) => res.send("Service-NS API работает 🚀 v-status-1"));


app.get("/db-ping", async (req, res) => {
  try {
    const r = await pool.query("SELECT NOW() as now");
    res.json({ ok: true, now: r.rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ===================================================================
// FT
// ===================================================================

// POST: save one FT row
app.post("/save-ft", async (req, res) => {
  try {
    const {
      input_date,
      input_name,
      division,
      object,
      contractor,
      invoice_no,
      invoice_date,
      invoice_pdf,
      amount,
    } = req.body;

    const query = `
      INSERT INTO ft
      (id_ft, input_date, input_name, division, "object", contractor, invoice_no, invoice_date, invoice_pdf, amount)
      VALUES
      ('FT' || nextval('ft_id_seq')::text, $1,$2,$3,$4,$5,$6,$7,$8,$9)
      RETURNING id_ft
    `;

    const values = [
      input_date || "",
      input_name || "",
      division || "",
      object || "",
      contractor || "",
      invoice_no || "",
      invoice_date || "",
      invoice_pdf || "",
      amount === "" || amount === undefined || amount === null ? null : Number(amount),
    ];

    const result = await pool.query(query, values);
    res.json({ success: true, id_ft: result.rows[0].id_ft });
  } catch (err) {
    console.error("SAVE FT ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// POST: save many FT rows (batch)
app.post("/save-ft-batch", async (req, res) => {
  const client = await pool.connect();
  try {
    const { header, rows } = req.body;

    if (!rows || !Array.isArray(rows) || rows.length === 0) {
      return res.status(400).json({ success: false, error: "rows is empty" });
    }

    const h = header || {};
    const values = [];
    const chunks = rows.map((r, i) => {
      const base = i * 9;

      values.push(
        h.input_date || "",
        h.input_name || "",
        h.division || "",
        h.object || "",
        r.contractor || "",
        r.invoice_no || "",
        r.invoice_date || "",
        r.invoice_pdf || "",
        (r.amount === "" || r.amount === undefined || r.amount === null) ? null : Number(r.amount)
      );

      return `(
        'FT' || nextval('ft_id_seq')::text,
        $${base + 1}, $${base + 2}, $${base + 3}, $${base + 4},
        $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}
      )`;
    });

    const query = `
      INSERT INTO ft
      (id_ft, input_date, input_name, division, "object", contractor, invoice_no, invoice_date, invoice_pdf, amount)
      VALUES ${chunks.join(",")}
      RETURNING id_ft
    `;

    await client.query("BEGIN");
    const result = await client.query(query, values);
    await client.query("COMMIT");

    res.json({ success: true, ids: result.rows.map((x) => x.id_ft) });
  } catch (err) {
    await client.query("ROLLBACK");
    console.error("BATCH FT ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  } finally {
    client.release();
  }
});

// GET: last FT rows (для таблицы/выбора)
app.get("/ft", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 200), 500);

    const result = await pool.query(
      `SELECT id_ft, input_date, input_name, division, "object",
              contractor, invoice_no, invoice_date, invoice_pdf, amount
       FROM ft
       ORDER BY
         COALESCE(NULLIF(regexp_replace(id_ft,'\\D','','g'),''),'0')::int DESC
       LIMIT $1`,
      [limit]
    );

    res.json({ success: true, rows: result.rows });
  } catch (err) {
    console.error("GET FT ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ===================================================================
// ZVK (привязка к заявке)
// ===================================================================

// POST: создать привязку (ZFT1, ZFT2...)
// body: { creator_login, zvk_name, id_ft, amount }
app.post("/save-zvk", async (req, res) => {
  try {
    const { creator_login, zvk_name, id_ft, amount } = req.body;

    if (!creator_login || !zvk_name || !id_ft) {
      return res.status(400).json({
        success: false,
        error: "creator_login, zvk_name and id_ft are required"
      });
    }

    const query = `
      INSERT INTO zvk
      (id_zvk, zvk_date, zvk_name, id_ft, amount, creator_login)
      VALUES
      ('ZFT' || nextval('zvk_id_seq')::text, NOW(), $1, $2, $3, $4)
      RETURNING id_zvk, zvk_date
    `;

    const values = [
      String(zvk_name).trim(),
      String(id_ft).trim(),
      (amount === "" || amount === undefined || amount === null) ? null : Number(amount),
      String(creator_login).trim()
    ];

    const r = await pool.query(query, values);
    res.json({ success: true, id_zvk: r.rows[0].id_zvk, zvk_date: r.rows[0].zvk_date });
  } catch (err) {
    console.error("SAVE ZVK ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// GET: последние привязки (только свои, админ — все)
app.get("/zvk", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 200), 500);
    const login = String(req.query.login || "").trim();

    if (!login) {
      return res.status(400).json({ success:false, error:"login is required" });
    }

    const admin = isAdmin(login);

    const query = admin
      ? `SELECT id_zvk, zvk_date, zvk_name, id_ft, amount, creator_login
         FROM zvk
         ORDER BY zvk_date DESC
         LIMIT $1`
      : `SELECT id_zvk, zvk_date, zvk_name, id_ft, amount, creator_login
         FROM zvk
         WHERE creator_login = $2
         ORDER BY zvk_date DESC
         LIMIT $1`;

    const params = admin ? [limit] : [limit, login];

    const result = await pool.query(query, params);
    res.json({ success:true, rows: result.rows, admin });
  } catch (err) {
    console.error("GET ZVK ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ===================================================================
// ZVK STATUS (3-я таблица)
// таблица: public.zvk_status (id_zvk PK, stat_date auto, status, src_d, src_o)
// ===================================================================

// GET: ZVK + текущий статус (только свои, админ — все)
app.get("/zvk-with-status", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 300), 500);
    const login = String(req.query.login || "").trim();

    if (!login) return res.status(400).json({ success:false, error:"login is required" });

    const admin = isAdmin(login);

    const qAdmin = `
      SELECT z.id_zvk, z.zvk_date, z.zvk_name, z.id_ft, z.amount,
             s.stat_date, s.status, s.src_d, s.src_o
      FROM zvk z
      LEFT JOIN zvk_status s ON s.id_zvk = z.id_zvk
      ORDER BY z.zvk_date DESC
      LIMIT $1
    `;

    const qUser = `
      SELECT z.id_zvk, z.zvk_date, z.zvk_name, z.id_ft, z.amount,
             s.stat_date, s.status, s.src_d, s.src_o
      FROM zvk z
      LEFT JOIN zvk_status s ON s.id_zvk = z.id_zvk
      WHERE z.creator_login = $2
      ORDER BY z.zvk_date DESC
      LIMIT $1
    `;

    const r = admin
      ? await pool.query(qAdmin, [limit])
      : await pool.query(qUser, [limit, login]);

    res.json({ success:true, rows:r.rows, admin });
  } catch (e) {
    console.error("ZVK-WITH-STATUS ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  }
});

// POST: создать/обновить текущий статус (СтатДата авто NOW)
app.post("/upsert-zvk-status", async (req, res) => {
  try {
    const { login, id_zvk, status, src_d, src_o } = req.body;

    if (!login || !id_zvk || !status) {
      return res.status(400).json({ success:false, error:"login, id_zvk, status required" });
    }

    const admin = isAdmin(login);

    // ✅ Проверка доступа: пользователь меняет только свои ZVK
    if (!admin) {
      const own = await pool.query(
        `SELECT 1 FROM zvk WHERE id_zvk=$1 AND creator_login=$2 LIMIT 1`,
        [String(id_zvk).trim(), String(login).trim()]
      );
      if (own.rowCount === 0) {
        return res.status(403).json({ success:false, error:"Нет доступа к этому ID ZVK" });
      }
    }

    const r = await pool.query(
      `INSERT INTO zvk_status (id_zvk, status, src_d, src_o, stat_date)
       VALUES ($1,$2,$3,$4,NOW())
       ON CONFLICT (id_zvk)
       DO UPDATE SET status=EXCLUDED.status, src_d=EXCLUDED.src_d, src_o=EXCLUDED.src_o, stat_date=NOW()
       RETURNING id_zvk, stat_date, status, src_d, src_o`,
      [
        String(id_zvk).trim(),
        String(status).trim(),
        (src_d ?? "").toString().trim(),
        (src_o ?? "").toString().trim(),
      ]
    );

    res.json({ success:true, row:r.rows[0], admin });
  } catch (e) {
    console.error("UPSERT-ZVK-STATUS ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  }
});

// ===============================
// Start
// ===============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT));
