// server.js (FULL) — Service-NS API 🚀
// Логика:
// 1) FT хранится в ft
// 2) ZVK (zvk) хранит "историю строк" внутри одного цикла id_zvk (ZFT1 повторяется)
// 3) Пока zvk_admin.is_paid != 'Да' -> новые сохранения пишутся с тем же id_zvk
// 4) Когда zvk_admin.is_paid = 'Да' -> следующий save создаёт новый цикл id_zvk = ZFT2
//
// ВАЖНО ПРО БАЗУ:
// - Чтобы разрешить повторение id_zvk (ZFT1 много строк), В БД должен быть скрытый PK "id" (bigserial)
//   и id_zvk НЕ должен быть PRIMARY KEY.
//   Сделай миграцию один раз:
//     ALTER TABLE zvk DROP CONSTRAINT IF EXISTS zvk_pkey;
//     ALTER TABLE zvk ADD COLUMN IF NOT EXISTS id bigserial;
//     ALTER TABLE zvk ADD CONSTRAINT zvk_pkey PRIMARY KEY (id);

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

  // ZVK/ZFT (история)
  // ⚠️ НЕ создаём тут PRIMARY KEY на id_zvk.
  // Если таблица уже существует со старым PK — миграцию делай вручную (см. комментарий вверху).
  await pool.query(`
    CREATE TABLE IF NOT EXISTS zvk (
      id_zvk text,
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

  // Индексы для скорости
  await pool.query(`CREATE INDEX IF NOT EXISTS zvk_idx_ft_date ON zvk (id_ft, zvk_date DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS zvk_idx_zvk_date ON zvk (id_zvk, zvk_date DESC);`);

  console.log("DB init OK");
}
initDb().catch(console.error);

// ===============================
// Health
// ===============================
app.get("/", (req, res) => res.send("Service-NS API работает 🚀 v-ftzvk-history-final-2"));

app.get("/db-ping", async (req, res) => {
  try {
    const r = await pool.query("SELECT NOW() as now");
    res.json({ ok: true, now: r.rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// =====================================================
// GET FT
// =====================================================
app.get("/ft", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 500), 500);
    const login = String(req.query.login || "").trim();
    const isAdmin = String(req.query.is_admin || "0") === "1";

    if (!login) return res.status(400).json({ success: false, error: "login is required" });

    const qAdmin = `
      SELECT f.*
      FROM ft f
      ORDER BY COALESCE(NULLIF(substring(f.id_ft from '\\d+'), ''), '0')::int DESC
      LIMIT $1
    `;

    const qUser = `
      SELECT f.*
      FROM ft f
      WHERE f.input_name = $2
      ORDER BY COALESCE(NULLIF(substring(f.id_ft from '\\d+'), ''), '0')::int DESC
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
// SAVE (история) — /zvk-save
// Пока is_paid != 'Да' -> пишем в тот же id_zvk
// После is_paid = 'Да' -> создаём новый id_zvk (ZFT2...)
// =====================================================
app.post("/zvk-save", async (req, res) => {
  try {
    const { id_ft, user_name, to_pay, request_flag } = req.body;
    if (!id_ft) return res.status(400).json({ success: false, error: "id_ft is required" });

    const ft = String(id_ft).trim();
    const name = (user_name || "СИСТЕМА").toString().trim();
    const flag = (request_flag || "Нет").toString().trim();

    const toPayNum =
      (to_pay === "" || to_pay === undefined || to_pay === null) ? 0 : Number(to_pay);
    if (Number.isNaN(toPayNum)) {
      return res.status(400).json({ success: false, error: "to_pay must be number" });
    }

    // 1) последний цикл ZFT по FT
    const lastCycle = await pool.query(
      `
      SELECT z.id_zvk
      FROM zvk z
      WHERE z.id_ft = $1
      ORDER BY
        COALESCE(NULLIF(substring(z.id_zvk from '\\d+'), ''), '0')::int DESC,
        z.zvk_date DESC NULLS LAST
      LIMIT 1
      `,
      [ft]
    );

    let id_zvk = lastCycle.rows[0]?.id_zvk || null;

    // 2) если есть цикл — проверяем оплату
    if (id_zvk) {
      const paid = await pool.query(`SELECT is_paid FROM zvk_admin WHERE id_zvk=$1`, [id_zvk]);
      if (paid.rows[0]?.is_paid === "Да") id_zvk = null; // цикл закрыт
    }

    // 3) если цикла нет — создаём новый id_zvk и первую строку "СИСТЕМА/Нет/sum_ft"
    if (!id_zvk) {
      const created = await pool.query(`SELECT 'ZFT' || nextval('zvk_id_seq')::text AS id_zvk`);
      id_zvk = created.rows[0].id_zvk;

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

    // 4) добавляем новую строку истории (тот же id_zvk)
    const r = await pool.query(
      `
      INSERT INTO zvk (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
      VALUES ($1, $2, NOW(), $3, $4, $5)
      RETURNING id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag
      `,
      [id_zvk, ft, name, toPayNum, flag]
    );

    res.json({ success: true, row: r.rows[0], id_zvk });
  } catch (e) {
    console.error("ZVK-SAVE ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// =====================================================
// Инициатор: Источник Див / Источник Объект
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
// Админ: Реестр + Оплачено
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
// JOIN: читаем из VIEW ft_zvk_full
// Важно: сортировка безопасная через substring(... '\d+')
// =====================================================
app.get("/ft-zvk-join", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 500), 500);

    const q = `
      SELECT v.*
      FROM ft_zvk_full v
      ORDER BY
        COALESCE(NULLIF(substring(v.id_ft from '\\d+'), ''), '0')::int DESC,
        v.zvk_date DESC NULLS LAST
      LIMIT $1
    `;

    const r = await pool.query(q, [limit]);
    res.json({ success: true, rows: r.rows });
  } catch (e) {
    console.error("FT-ZVK-JOIN ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});
// =====================================================
// SAVE FT (создать FT + авто баланс + авто ZFT1 через триггер или отдельно)
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
      sum_ft
    } = req.body;

    if (!input_name) return res.status(400).json({ success:false, error:"input_name is required" });
    if (!division || !object) return res.status(400).json({ success:false, error:"division/object required" });
    if (!contractor) return res.status(400).json({ success:false, error:"contractor required" });
    if (!invoice_no) return res.status(400).json({ success:false, error:"invoice_no required" });
    if (!invoice_date) return res.status(400).json({ success:false, error:"invoice_date required" });

    const sumNum = (sum_ft === "" || sum_ft === null || sum_ft === undefined) ? 0 : Number(sum_ft);
    if (Number.isNaN(sumNum)) return res.status(400).json({ success:false, error:"sum_ft must be number" });

    // новый ID FT
    const idRow = await pool.query(`SELECT 'FT' || nextval('ft_id_seq')::text AS id_ft`);
    const id_ft = idRow.rows[0].id_ft;

    // сохранить FT
    const r = await pool.query(
      `
      INSERT INTO ft
        (id_ft, input_date, input_name, division, "object", contractor, invoice_no, invoice_date, invoice_pdf, sum_ft)
      VALUES
        ($1, $2, $3, $4, $5, $6, $7, to_date($8,'DD.MM.YYYY'), $9, $10)
      RETURNING id_ft
      `,
      [
        id_ft,
        input_date ? new Date(input_date) : new Date(),
        String(input_name).trim(),
        String(division).trim(),
        String(object).trim(),
        String(contractor).trim(),
        String(invoice_no).trim(),
        String(invoice_date).trim(),  // приходит "dd.mm.yyyy"
        invoice_pdf ? String(invoice_pdf).trim() : "",
        sumNum
      ]
    );

    // balance_ft (если используешь)
    await pool.query(
      `
      INSERT INTO ft_balance (id_ft, balance_ft)
      VALUES ($1, $2)
      ON CONFLICT (id_ft) DO UPDATE SET balance_ft = EXCLUDED.balance_ft
      `,
      [id_ft, sumNum]
    );

    // ✅ если у тебя НЕТ триггера на авто ZFT1 — создадим ZFT1 тут
    await pool.query(
      `
      INSERT INTO zvk (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
      VALUES ('ZFT' || nextval('zvk_id_seq')::text, $1, NOW(), 'СИСТЕМА', $2, 'Нет')
      `,
      [id_ft, sumNum]
    );

    res.json({ success:true, id_ft: r.rows[0].id_ft });
  } catch (e) {
    console.error("SAVE-FT ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  }
});

// ===============================
// Start
// ===============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT));
