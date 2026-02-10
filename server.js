require("dotenv").config();

const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json());

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
app.get("/", (req, res) => res.send("Service-NS API работает 🚀"));

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
// body: { input_date, input_name, division, object, contractor, invoice_no, invoice_date, invoice_pdf, amount }
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
      `SELECT id_ft, input_date, input_name, division, "object", contractor, invoice_no, invoice_date, invoice_pdf, amount
       FROM ft
       ORDER BY id_ft DESC
       LIMIT $1`,
      [limit]
    );
    res.json(result.rows);
  } catch (err) {
    console.error("GET FT ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ===================================================================
// ZVK (привязка к заявке)  -> таблица: id_zvk, zvk_date, zvk_name, id_ft, amount
// ===================================================================

// POST: создать привязку (Zft1, Zft2...)
// body: { zvk_name, id_ft, amount }
app.post("/save-zvk", async (req, res) => {
  try {
    const { zvk_name, id_ft, amount } = req.body;

    if (!zvk_name || !id_ft) {
      return res.status(400).json({ success: false, error: "zvk_name and id_ft are required" });
    }

    const query = `
      INSERT INTO zvk
      (id_zvk, zvk_date, zvk_name, id_ft, amount)
      VALUES
      ('Zft' || nextval('zvk_id_seq')::text, NOW(), $1, $2, $3)
      RETURNING id_zvk, zvk_date
    `;

    const values = [
      String(zvk_name),
      String(id_ft),
      (amount === "" || amount === undefined || amount === null) ? null : Number(amount),
    ];

    const result = await pool.query(query, values);
    res.json({ success: true, id_zvk: result.rows[0].id_zvk, zvk_date: result.rows[0].zvk_date });
  } catch (err) {
    console.error("SAVE ZVK ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// GET: последние привязки
app.get("/zvk", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 200), 500);
    const result = await pool.query(
      `SELECT id_zvk, zvk_date, zvk_name, id_ft, amount
       FROM zvk
       ORDER BY zvk_date DESC
       LIMIT $1`,
      [limit]
    );
    res.json(result.rows);
  } catch (err) {
    console.error("GET ZVK ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT));
