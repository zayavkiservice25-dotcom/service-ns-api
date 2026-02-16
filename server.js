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

  // ZVK/ZFT (история: допускаем много строк на один FT)
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

  // Статус/источники (1 строка на id_zvk)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS zvk_status (
      id_zvk text PRIMARY KEY,
      status_time timestamptz,
      src_d text,
      src_o text
    );
  `);

  // Согласование (1 строка на id_zvk)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS zvk_agree (
      id_zvk text PRIMARY KEY,
      agree_name text,
      agree_time timestamptz
    );
  `);

  // Админ (1 строка на id_zvk)
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
app.get("/", (req, res) => res.send("Service-NS API работает 🚀 v-ftzvk-history-final"));

app.get("/db-ping", async (req, res) => {
  try {
    const r = await pool.query("SELECT NOW() as now");
    res.json({ ok: true, now: r.rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// =====================================================
// GET FT (+ balance view)
// =====================================================
app.get("/ft", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 500), 500);
    const login = String(req.query.login || "").trim();
    const isAdmin = String(req.query.is_admin || "0") === "1";

    if (!login) return res.status(400).json({ success: false, error: "login is required" });

    const qAdmin = `
      SELECT f.*, b.balance_ft
      FROM ft f
      LEFT JOIN ft_balance b ON b.id_ft = f.id_ft
      ORDER BY COALESCE(NULLIF(regexp_replace(f.id_ft,'\\D','','g'),''),'0')::int DESC
      LIMIT $1
    `;

    const qUser = `
      SELECT f.*, b.balance_ft
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
// CREATE ZFT (история): новая строка в zvk для выбранного FT
// =====================================================
app.post("/zvk-save", async (req, res) => {
  try {
    const { id_ft, user_name, to_pay, request_flag } = req.body;
    if (!id_ft) return res.status(400).json({ success:false, error:"id_ft is required" });

    const ft = String(id_ft).trim();
    const name = (user_name || "СИСТЕМА").toString().trim();
    const flag = (request_flag || "Нет").toString().trim();
    const toPayNum = (to_pay === "" || to_pay === undefined || to_pay === null) ? 0 : Number(to_pay);
    if (Number.isNaN(toPayNum)) return res.status(400).json({ success:false, error:"to_pay must be number" });

    // 1) найти последний цикл ZFT по FT (последний id_zvk)
    const lastCycle = await pool.query(
      `
      SELECT z.id_zvk
      FROM zvk z
      WHERE z.id_ft = $1
      ORDER BY
        COALESCE(NULLIF(regexp_replace(z.id_zvk,'\\D','','g'),''),'0')::int DESC,
        z.zvk_date DESC NULLS LAST
      LIMIT 1
      `,
      [ft]
    );

    let id_zvk = lastCycle.rows[0]?.id_zvk || null;

    // 2) если цикл есть — проверить оплачено ли
    if (id_zvk) {
      const paid = await pool.query(`SELECT is_paid FROM zvk_admin WHERE id_zvk=$1`, [id_zvk]);
      if (paid.rows[0]?.is_paid === "Да") id_zvk = null; // цикл закрыт → стартуем новый
    }

    // 3) если активного цикла нет → создаём новый ZFT (ZFT2, ZFT3...)
    if (!id_zvk) {
      const created = await pool.query(
        `SELECT 'ZFT' || nextval('zvk_id_seq')::text AS id_zvk`
      );
      id_zvk = created.rows[0].id_zvk;

      // первая строка цикла: СИСТЕМА / Нет / sum_ft (как у тебя)
      const sumFtRow = await pool.query(`SELECT sum_ft FROM ft WHERE id_ft=$1`, [ft]);
      const sumFt = Number(sumFtRow.rows[0]?.sum_ft || 0);

      await pool.query(
        `
        INSERT INTO zvk (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
        VALUES ($1, $2, NOW(), 'СИСТЕМА', $3, 'Нет')
        `,
        [id_zvk, ft, sumFt]
      );
    }

    // 4) добавить НОВУЮ строку истории в этом же цикле (id_zvk тот же)
    const r = await pool.query(
      `
      INSERT INTO zvk (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
      VALUES ($1, $2, NOW(), $3, $4, $5)
      RETURNING id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag
      `,
      [id_zvk, ft, name, toPayNum, flag]
    );

    res.json({ success:true, row:r.rows[0], id_zvk });
  } catch (e) {
    console.error("ZVK-SAVE ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  }
});


// =====================================================
// Инициатор: Источник Див / Источник Объект (+status_time авто)
// Пишем на конкретный id_zvk (обычно на последний ZFT)
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
// =====================================================
app.post("/upsert-zvk-admin", async (req, res) => {
  const client = await pool.connect();
  try {
    const { is_admin, id_zvk, registry_flag, is_paid } = req.body;
    if (!id_zvk) return res.status(400).json({ success: false, error: "id_zvk required" });

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
// JOIN: читаем из VIEW (там уже “последний ZFT”)
// =====================================================
app.get("/ft-zvk-join", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 500), 500);

    const q = `
      SELECT v.*, b.balance_ft
      FROM ft_zvk_full v
      LEFT JOIN ft_balance b ON b.id_ft = v.id_ft
      ORDER BY
        COALESCE(NULLIF(regexp_replace(v.id_ft,'\\D','','g'),''),'0')::int DESC
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
