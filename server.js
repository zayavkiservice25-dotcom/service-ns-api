require("dotenv").config();

const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json());

// ✅ CORS preflight
app.options(/.*/, cors());

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
app.get("/", (req, res) => res.send("Service-NS API работает 🚀 v-ftzvk-OK"));

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
// columns: id_ft, input_date, input_name, division, object, contractor,
//          invoice_no, invoice_date, invoice_pdf, sum_ft
// ===================================================================

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
      sum_ft,
    } = req.body;

    const query = `
      INSERT INTO ft
      (id_ft, input_date, input_name, division, "object", contractor, invoice_no, invoice_date, invoice_pdf, sum_ft)
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
      sum_ft === "" || sum_ft === undefined || sum_ft === null ? null : Number(sum_ft),
    ];

    const result = await pool.query(query, values);
    res.json({ success: true, id_ft: result.rows[0].id_ft });
  } catch (err) {
    console.error("SAVE FT ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

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
        r.sum_ft === "" || r.sum_ft === undefined || r.sum_ft === null ? null : Number(r.sum_ft)
      );

      return `(
        'FT' || nextval('ft_id_seq')::text,
        $${base + 1}, $${base + 2}, $${base + 3}, $${base + 4},
        $${base + 5}, $${base + 6}, $${base + 7}, $${base + 8}, $${base + 9}
      )`;
    });

    const query = `
      INSERT INTO ft
      (id_ft, input_date, input_name, division, "object", contractor, invoice_no, invoice_date, invoice_pdf, sum_ft)
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

app.get("/ft", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 200), 500);
    const login = String(req.query.login || "").trim();
    const admin = String(req.query.is_admin || "0") === "1";

    if (!login) return res.status(400).json({ success: false, error: "login is required" });

    const qAdmin = `
      SELECT id_ft, input_date, input_name, division, "object",
             contractor, invoice_no, invoice_date, invoice_pdf, sum_ft
      FROM ft
      ORDER BY COALESCE(NULLIF(regexp_replace(id_ft,'\\D','','g'),''),'0')::int DESC
      LIMIT $1
    `;

    const qUser = `
      SELECT id_ft, input_date, input_name, division, "object",
             contractor, invoice_no, invoice_date, invoice_pdf, sum_ft
      FROM ft
      WHERE input_name = $2
      ORDER BY COALESCE(NULLIF(regexp_replace(id_ft,'\\D','','g'),''),'0')::int DESC
      LIMIT $1
    `;

    const r = admin ? await pool.query(qAdmin, [limit]) : await pool.query(qUser, [limit, login]);
    res.json({ success: true, rows: r.rows, admin });
  } catch (e) {
    console.error("GET FT ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// ===================================================================
// ZVK
// columns: id_zvk, zvk_date, zvk_name, id_ft, sum_zvk, status_zvk(optional)
// ===================================================================

app.post("/save-zvk", async (req, res) => {
  try {
    const { zvk_name, id_ft, sum_zvk, status_zvk } = req.body;

    if (!zvk_name || !id_ft) {
      return res.status(400).json({ success: false, error: "zvk_name and id_ft are required" });
    }

    const query = `
      INSERT INTO zvk
      (id_zvk, zvk_date, zvk_name, id_ft, sum_zvk, status_zvk)
      VALUES
      ('ZFT' || nextval('zvk_id_seq')::text, NOW(), $1, $2, $3, $4)
      RETURNING id_zvk, zvk_date
    `;

    const values = [
      String(zvk_name).trim(),
      String(id_ft).trim(),
      sum_zvk === "" || sum_zvk === undefined || sum_zvk === null ? null : Number(sum_zvk),
      status_zvk ? String(status_zvk).trim() : null,
    ];

    const r = await pool.query(query, values);
    res.json({ success: true, id_zvk: r.rows[0].id_zvk, zvk_date: r.rows[0].zvk_date });
  } catch (err) {
    console.error("SAVE ZVK ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/zvk", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 200), 500);

    const result = await pool.query(
      `SELECT id_zvk, zvk_date, zvk_name, id_ft, sum_zvk, status_zvk
       FROM zvk
       ORDER BY zvk_date DESC
       LIMIT $1`,
      [limit]
    );

    res.json({ success: true, rows: result.rows });
  } catch (err) {
    console.error("GET ZVK ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ===================================================================
// FT + ZVK FULL (VIEW)
// ===================================================================
app.get("/ft-zvk-full", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 300), 500);
    const login = String(req.query.login || "").trim();
    const admin = String(req.query.is_admin || "0") === "1";

    if (!login) return res.status(400).json({ success: false, error: "login is required" });

    const qAdmin = `
      SELECT *
      FROM ft_zvk_full
      ORDER BY COALESCE(NULLIF(regexp_replace(id_ft,'\\D','','g'),''),'0')::int DESC
      LIMIT $1
    `;

    const qUser = `
      SELECT *
      FROM ft_zvk_full
      WHERE COALESCE(input_name,'') = $2
      ORDER BY COALESCE(NULLIF(regexp_replace(id_ft,'\\D','','g'),''),'0')::int DESC
      LIMIT $1
    `;

    const r = admin ? await pool.query(qAdmin, [limit]) : await pool.query(qUser, [limit, login]);
    res.json({ success: true, rows: r.rows, admin });
  } catch (e) {
    console.error("FT-ZVK-FULL ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// ===================================================================
// ZVK_STATUS  (БЕЗ КОЛОНКИ status)
// columns: id_zvk, stat_date, src_d, src_o
// ===================================================================

// GET: ZVK + src_d/src_o + stat_date
app.get("/zvk-with-status", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 300), 500);

    const q = `
      SELECT z.id_zvk, z.zvk_date, z.zvk_name, z.id_ft, z.sum_zvk, z.status_zvk,
             s.stat_date, s.src_d, s.src_o
      FROM zvk z
      LEFT JOIN zvk_status s ON s.id_zvk = z.id_zvk
      ORDER BY z.zvk_date DESC
      LIMIT $1
    `;

    const r = await pool.query(q, [limit]);
    res.json({ success: true, rows: r.rows });
  } catch (e) {
    console.error("ZVK-WITH-STATUS ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// POST: upsert src_d/src_o (status НЕ отправляем)
app.post("/upsert-zvk-status", async (req, res) => {
  const client = await pool.connect();
  try {
    const { login, id_zvk, src_d, src_o } = req.body;

    if (!login || !id_zvk) {
      return res.status(400).json({ success: false, error: "login, id_zvk required" });
    }

    await client.query("BEGIN");

    const r = await client.query(
      `INSERT INTO zvk_status (id_zvk, src_d, src_o, stat_date)
       VALUES ($1,$2,$3,NOW())
       ON CONFLICT (id_zvk)
       DO UPDATE SET src_d=EXCLUDED.src_d, src_o=EXCLUDED.src_o, stat_date=NOW()
       RETURNING id_zvk, stat_date, src_d, src_o`,
      [
        String(id_zvk).trim(),
        (src_d ?? "").toString().trim(),
        (src_o ?? "").toString().trim(),
      ]
    );

    await client.query("COMMIT");
    res.json({ success: true, row: r.rows[0] });
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("UPSERT-ZVK-STATUS ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  } finally {
    client.release();
  }
});

// ===============================
// Start
// ===============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT));
