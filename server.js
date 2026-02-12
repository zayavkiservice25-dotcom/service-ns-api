require("dotenv").config();

const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json());
app.options(/.*/, cors());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// ===============================
// Init
// ===============================
async function initDb() {
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS ft_id_seq START 1;`);
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS zvk_id_seq START 1;`);
  console.log("DB init OK");
}
initDb().catch(console.error);

// ===============================
// Health
// ===============================
app.get("/", (req, res) => res.send("Service-NS API работает 🚀 v-ftzvk-final-fixed"));

app.get("/db-ping", async (req, res) => {
  try {
    const r = await pool.query("SELECT NOW() as now");
    res.json({ ok: true, now: r.rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// =====================================================
// FT
// =====================================================
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

    const q = `
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

    const r = await pool.query(q, values);
    res.json({ success: true, id_ft: r.rows[0].id_ft });
  } catch (e) {
    console.error("SAVE FT ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/ft", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 200), 500);
    const login = String(req.query.login || "").trim();
    const loginNorm = login.toLowerCase();
    const admin =
      String(req.query.is_admin || "0") === "1" ||
      loginNorm === "b_erkin"; // ✅ B_Erkin всегда админ

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

// =====================================================
// ZVK (создание если нужно)
// =====================================================
app.post("/save-zvk", async (req, res) => {
  try {
    const { id_ft, sum_zvk, status_zvk } = req.body;
    if (!id_ft) return res.status(400).json({ success: false, error: "id_ft is required" });

    const q = `
      INSERT INTO zvk (id_zvk, id_ft, sum_zvk, status_zvk)
      VALUES ('ZFT' || nextval('zvk_id_seq')::text, $1, $2, $3)
      RETURNING id_zvk
    `;

    const values = [
      String(id_ft).trim(),
      sum_zvk === "" || sum_zvk === undefined || sum_zvk === null ? null : Number(sum_zvk),
      status_zvk ? String(status_zvk).trim() : null,
    ];

    const r = await pool.query(q, values);
    res.json({ success: true, id_zvk: r.rows[0].id_zvk });
  } catch (e) {
    console.error("SAVE ZVK ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// =====================================================
// FT+ZVK FULL (VIEW) — B_Erkin видит все
// =====================================================
app.get("/ft-zvk-full", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 300), 500);
    const login = String(req.query.login || "").trim();
    const loginNorm = login.toLowerCase();
    const admin =
      String(req.query.is_admin || "0") === "1" ||
      loginNorm === "b_erkin";

    if (!login) return res.status(400).json({ success: false, error: "login is required" });

    const qAdmin = `
      SELECT * FROM ft_zvk_full
      ORDER BY COALESCE(NULLIF(regexp_replace(id_ft,'\\D','','g'),''),'0')::int DESC
      LIMIT $1
    `;

    const qUser = `
      SELECT * FROM ft_zvk_full
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

// =====================================================
// 1) Инициатор: сохраняет src_d / src_o (БЕЗ created_at)
// =====================================================
app.post("/upsert-zvk-src", async (req, res) => {
  const client = await pool.connect();
  try {
    const { login, id_zvk, src_d, src_o } = req.body;
    if (!login || !id_zvk) return res.status(400).json({ success: false, error: "login, id_zvk required" });

    await client.query("BEGIN");

    const r = await client.query(
      `INSERT INTO zvk_status (id_zvk, src_d, src_o)
       VALUES ($1,$2,$3)
       ON CONFLICT (id_zvk)
       DO UPDATE SET src_d=EXCLUDED.src_d, src_o=EXCLUDED.src_o
       RETURNING id_zvk, src_d, src_o`,
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
    console.error("UPSERT-ZVK-SRC ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  } finally {
    client.release();
  }
});

// =====================================================
// 2) B_Erkin: согласование + оплата
// - agree_name -> zvk_agree (без created_at)
// - is_paid + created_at авто -> zvk_pay (created_at обновляем NOW())
// =====================================================
app.post("/upsert-zvk-approve-pay", async (req, res) => {
  const client = await pool.connect();
  try {
    const { login, id_zvk, agree_name, is_paid } = req.body;
    if (!login || !id_zvk) {
      return res.status(400).json({ success: false, error: "login, id_zvk required" });
    }

    // защита
    if (String(login).trim().toLowerCase() !== "b_erkin") {
      return res.status(403).json({ success: false, error: "only B_Erkin allowed" });
    }

    await client.query("BEGIN");

    // 1) agree (БЕЗ created_at)
    await client.query(
      `INSERT INTO zvk_agree (id_zvk, agree_name)
       VALUES ($1,$2)
       ON CONFLICT (id_zvk)
       DO UPDATE SET agree_name=EXCLUDED.agree_name`,
      [String(id_zvk).trim(), (agree_name ?? "").toString().trim() || null]
    );

    // 2) pay (created_at = ОплатДата)
    await client.query(
      `INSERT INTO zvk_pay (id_zvk, is_paid, created_at)
       VALUES ($1, $2, CASE WHEN $2 = 'Да' THEN NOW() ELSE NULL END)
       ON CONFLICT (id_zvk)
       DO UPDATE SET
         is_paid = EXCLUDED.is_paid,
         created_at = CASE
           WHEN EXCLUDED.is_paid = 'Да' AND zvk_pay.created_at IS NULL THEN NOW()
           WHEN EXCLUDED.is_paid <> 'Да' THEN NULL
           ELSE zvk_pay.created_at
         END`,
      [String(id_zvk).trim(), (is_paid ?? "").toString().trim() || null]
    );

    await client.query("COMMIT");
    res.json({ success: true });
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("UPSERT-APPROVE-PAY ERROR:", e);
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
