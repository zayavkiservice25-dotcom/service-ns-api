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
// Init: sequence for FT ids
// ===============================
async function initDb() {
  // sequence будет выдавать 1,2,3...
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS ft_id_seq START 1;`);
  console.log("DB init OK (ft_id_seq)");
}

initDb().catch((e) => {
  console.error("DB init error:", e);
});

// ===============================
// Health
// ===============================
app.get("/", (req, res) => res.send("Service-NS FT API работает 🚀"));

app.get("/db-ping", async (req, res) => {
  try {
    const r = await pool.query("SELECT NOW() as now");
    res.json({ ok: true, now: r.rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ===============================
// POST: save one FT row
// body: { input_date, input_name, division, object, contractor, invoice_no, invoice_date, invoice_pdf, amount }
// ===============================
app.post("/save-ft", async (req, res) => {
  try {
    const {
      input_date,
      input_name,
      division,
      object,        // это значение для колонки ft.object
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

// ===============================
// POST: save many FT rows (batch)
// body: { header: { input_date, input_name, division, object }, rows: [{ contractor, invoice_no, invoice_date, invoice_pdf, amount }, ...] }
// ===============================
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

      // id_ft генерим на каждую строку
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

// ===============================
// GET: last FT rows
// ===============================
app.get("/ft", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 50), 200);
    const result = await pool.query(
      `SELECT * FROM ft ORDER BY id_ft DESC LIMIT $1`,
      [limit]
    );
    res.json(result.rows);
  } catch (err) {
    console.error("GET FT ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("FT Server started on port " + PORT));
