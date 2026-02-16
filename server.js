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
// Init DB (создаём если нет)
// ===============================
async function initDb() {
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS ft_id_seq START 1;`);
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS zvk_id_seq START 1;`);

  // FT (если у тебя уже есть — просто пропустит)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ft (
      id_ft text PRIMARY KEY,
      input_date timestamptz,
      input_name text,
      division text,
      "object" text,
      contractor text,
      invoice_no text,
      invoice_date date,
      invoice_pdf text,
      sum_ft numeric
    );
  `);

  // ZVK/ZFT
  await pool.query(`
    CREATE TABLE IF NOT EXISTS zvk (
      id_zvk text PRIMARY KEY,
      id_ft text,
      zvk_date timestamptz,
      zvk_name text,
      to_pay numeric,
      request_flag text
    );
  `);

  // Остаток FT
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ft_balance (
      id_ft text PRIMARY KEY,
      balance_ft numeric NOT NULL DEFAULT 0
    );
  `);

  // Статус/источники (без stat_date!)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS zvk_status (
      id_zvk text PRIMARY KEY,
      status_time timestamptz,
      src_d text,
      src_o text
    );
  `);

  // Согласование
  await pool.query(`
    CREATE TABLE IF NOT EXISTS zvk_agree (
      id_zvk text PRIMARY KEY,
      agree_name text,
      agree_time timestamptz
    );
  `);

  // Админ: реестр / оплачено
  await pool.query(`
    CREATE TABLE IF NOT EXISTS zvk_admin (
      id_zvk text PRIMARY KEY,
      registry_flag text,
      is_paid text,
      pay_time timestamptz
    );
  `);

  console.log("DB init OK");
}
initDb().catch(console.error);

// ===============================
// Health
// ===============================
app.get("/", (req, res) => res.send("Service-NS API работает 🚀 v-ftzvk-fixed-join"));

app.get("/db-ping", async (req, res) => {
  try {
    const r = await pool.query("SELECT NOW() as now");
    res.json({ ok: true, now: r.rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// =====================================================
// GET FT (+ balance)
// =====================================================
app.get("/ft", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 500), 500);
    const login = String(req.query.login || "").trim();
    const isAdmin = String(req.query.is_admin || "0") === "1";

    if (!login) return res.status(400).json({ success: false, error: "login is required" });

    const qAdmin = `
      SELECT
        f.*,
        b.balance_ft
      FROM ft f
      LEFT JOIN ft_balance b ON b.id_ft = f.id_ft
      ORDER BY COALESCE(NULLIF(regexp_replace(f.id_ft,'\\D','','g'),''),'0')::int DESC
      LIMIT $1
    `;

    const qUser = `
      SELECT
        f.*,
        b.balance_ft
      FROM ft f
      LEFT JOIN ft_balance b ON b.id_ft = f.id_ft
      WHERE f.input_name = $2
      ORDER BY COALESCE(NULLIF(regexp_replace(f.id_ft,'\\D','','g'),''),'0')::int DESC
      LIMIT $1
    `;

    const r = isAdmin
      ? await pool.query(qAdmin, [limit])
      : await pool.query(qUser, [limit, login]);

    res.json({ success: true, rows: r.rows, admin: isAdmin });
  } catch (e) {
    console.error("GET FT ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// =====================================================
// SAVE ZVK (создать)
// =====================================================
app.post("/save-zvk", async (req, res) => {
  try {
    const { id_ft, zvk_name, sum_zvk, status_zvk } = req.body;

    if (!id_ft) return res.status(400).json({ success: false, error: "id_ft is required" });

    const toPayNum =
      (sum_zvk === "" || sum_zvk === undefined || sum_zvk === null) ? 0 : Number(sum_zvk);

    const r = await pool.query(
      `
      INSERT INTO zvk (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
      VALUES (
        'ZFT' || nextval('zvk_id_seq')::text,
        $1,
        NOW(),
        COALESCE($2,''),
        $3,
        'Нет'
      )
      RETURNING id_zvk, zvk_date
      `,
      [String(id_ft).trim(), zvk_name ? String(zvk_name).trim() : "", toPayNum]
    );

    // статус (если передали)
    if (status_zvk) {
      await pool.query(
        `
        INSERT INTO zvk_status (id_zvk, status_time)
        VALUES ($1, NOW())
        ON CONFLICT (id_zvk)
        DO UPDATE SET status_time = NOW()
        `,
        [r.rows[0].id_zvk]
      );
    }

    res.json({ success: true, id_zvk: r.rows[0].id_zvk, zvk_date: r.rows[0].zvk_date });
  } catch (e) {
    console.error("SAVE ZVK ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// =====================================================
// UPDATE ZFT: К оплате (сумма) + Заявка
// =====================================================
app.post("/update-zft", async (req, res) => {
  try {
    const { id_zvk, to_pay, request_flag, zvk_name } = req.body;
    if (!id_zvk) return res.status(400).json({ success: false, error: "id_zvk is required" });

    const toPayNum =
      (to_pay === "" || to_pay === undefined || to_pay === null) ? null : Number(to_pay);

    const r = await pool.query(
      `
      UPDATE zvk
      SET
        to_pay = COALESCE($2, to_pay),
        request_flag = COALESCE($3, request_flag),
        zvk_name = COALESCE($4, zvk_name)
      WHERE id_zvk = $1
      RETURNING *
      `,
      [
        String(id_zvk).trim(),
        toPayNum,
        request_flag ? String(request_flag).trim() : null,
        zvk_name ? String(zvk_name).trim() : null,
      ]
    );

    res.json({ success: true, row: r.rows[0] || null });
  } catch (e) {
    console.error("UPDATE ZFT ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// =====================================================
// Инициатор: Источник Див / Источник Объект (+status_time авто)
// =====================================================
app.post("/upsert-zvk-src", async (req, res) => {
  const client = await pool.connect();
  try {
    const { id_zvk, src_d, src_o } = req.body;
    if (!id_zvk) return res.status(400).json({ success: false, error: "id_zvk required" });

    await client.query("BEGIN");

    const r = await client.query(
      `
      INSERT INTO zvk_status (id_zvk, status_time, src_d, src_o)
      VALUES ($1, NOW(), $2, $3)
      ON CONFLICT (id_zvk)
      DO UPDATE SET
        status_time = NOW(),
        src_d = EXCLUDED.src_d,
        src_o = EXCLUDED.src_o
      RETURNING *
      `,
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
// Админ: Реестр + Оплачено (pay_time авто если Да)
// is_admin приходит из вебаппа (роль Админ)
// =====================================================
app.post("/upsert-zvk-admin", async (req, res) => {
  const client = await pool.connect();
  try {
    const { is_admin, id_zvk, registry_flag, is_paid } = req.body;

    if (!id_zvk) return res.status(400).json({ success: false, error: "id_zvk required" });

    // принимаем: true / "true" / 1 / "1"
    const adminOk =
      is_admin === true || is_admin === 1 || is_admin === "1" || String(is_admin).toLowerCase() === "true";

    if (!adminOk) return res.status(403).json({ success: false, error: "only admin allowed" });

    await client.query("BEGIN");

    const r = await client.query(
      `
      INSERT INTO zvk_admin (id_zvk, registry_flag, is_paid, pay_time)
      VALUES (
        $1,
        $2,
        $3,
        CASE WHEN $3 = 'Да' THEN NOW() ELSE NULL END
      )
      ON CONFLICT (id_zvk)
      DO UPDATE SET
        registry_flag = COALESCE(EXCLUDED.registry_flag, zvk_admin.registry_flag),
        is_paid = COALESCE(EXCLUDED.is_paid, zvk_admin.is_paid),
        pay_time = CASE
          WHEN EXCLUDED.is_paid = 'Да' AND zvk_admin.pay_time IS NULL THEN NOW()
          WHEN EXCLUDED.is_paid <> 'Да' THEN NULL
          ELSE zvk_admin.pay_time
        END
      RETURNING *
      `,
      [
        String(id_zvk).trim(),
        registry_flag ? String(registry_flag).trim() : null,
        is_paid ? String(is_paid).trim() : null,
      ]
    );

    await client.query("COMMIT");
    res.json({ success: true, row: r.rows[0] });
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("UPSERT-ZVK-ADMIN ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  } finally {
    client.release();
  }
});

// =====================================================
// JOIN: FT + ZVK + balance + status + agree + admin
// (БЕЗ zs.stat_date !!!)
// =====================================================
app.get("/ft-zvk-join", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 500), 500);

    const q = `
      SELECT
        f.id_ft, f.input_date, f.input_name, f.division, f."object",
        f.contractor, f.invoice_no, f.invoice_date, f.invoice_pdf, f.sum_ft,
        b.balance_ft,

        z.id_zvk, z.zvk_date, z.zvk_name, z.to_pay, z.request_flag,

        zs.stat_date,
        zs.src_d,
        zs.src_o,

        za.agree_date,
        za.agree_name,

        adm.registry_flag,
        adm.pay_time,
        adm.is_paid

      FROM ft f
      LEFT JOIN zvk z ON trim(z.id_ft) = trim(f.id_ft)
      LEFT JOIN ft_balance b ON b.id_ft = f.id_ft
      LEFT JOIN zvk_status zs ON zs.id_zvk = z.id_zvk
      LEFT JOIN zvk_agree  za ON za.id_zvk = z.id_zvk
      LEFT JOIN zvk_admin adm ON adm.id_zvk = z.id_zvk

      ORDER BY
        COALESCE(NULLIF(regexp_replace(f.id_ft,'\\D','','g'),''),'0')::int DESC,
        COALESCE(z.zvk_date, NOW()) DESC
      LIMIT $1
    `;

    const r = await pool.query(q, [limit]);
    res.json({ success: true, rows: r.rows });
  } catch (e) {
    console.error("FT-ZVK-JOIN ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// ===============================
// Start
// ===============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT));
