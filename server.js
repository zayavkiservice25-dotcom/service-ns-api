require("dotenv").config();

const express = require("express");
const cors = require("cors");
const path = require("path");
const { Pool } = require("pg");

const app = express();

app.use(cors());
app.use(express.json({ limit: "20mb" }));
app.use(express.urlencoded({ extended: true, limit: "20mb" }));
app.use((err, req, res, next) => {
  if (err) {
    console.error("❌ JSON parse error:", err.message);
    return res.status(400).json({ success:false, error:"BAD_JSON", message: err.message });
  }
  next();
});

app.get("/ping", (req,res)=>res.json({ok:true, ts: Date.now()}));  // ✅ правильно

app.options(/.*/, cors());

// 🔥 СТАТИКА (гарантированный путь)
app.use("/public", express.static(process.cwd() + "/public"));

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.DATABASE_URL?.includes('render.com') 
    ? { rejectUnauthorized: false }  // SSL только для Render
    : false                          // без SSL для локальной БД
});

// =====================================================
// INIT DB + МИГРАЦИИ
// =====================================================
async function initDb()  {
  // sequences
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS ft_id_seq START 1;`);
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS zvk_id_seq START 1;`);

  // FT
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

  await pool.query(`ALTER TABLE public.ft ADD COLUMN IF NOT EXISTS pay_purpose text;`);
  await pool.query(`ALTER TABLE public.ft ADD COLUMN IF NOT EXISTS dds_article text;`);
  await pool.query(`ALTER TABLE public.ft ADD COLUMN IF NOT EXISTS contract_no text;`);
  await pool.query(`ALTER TABLE public.ft ADD COLUMN IF NOT EXISTS contract_date date;`);

  // ✅ СИНХРОНИЗИРУЕМ ft_id_seq (чтобы после FT334 пошло FT335)
  await pool.query(`
    SELECT setval(
      'public.ft_id_seq',
      COALESCE((SELECT MAX((regexp_replace(id_ft, '\\D','','g'))::bigint) FROM public.ft), 0)
    );
  `);

  // ZVK (история строк)
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

  // ✅ технический PK id (bigserial)
  await pool.query(`ALTER TABLE public.zvk ADD COLUMN IF NOT EXISTS id bigserial;`);
  await pool.query(`ALTER TABLE public.zvk DROP CONSTRAINT IF EXISTS zvk_pkey;`);
  await pool.query(`ALTER TABLE public.zvk ADD CONSTRAINT zvk_pkey PRIMARY KEY (id);`);

  // ✅ СИНХРОНИЗИРУЕМ sequence bigserial для zvk.id (исправляет duplicate zvk_pkey)
  await pool.query(`
    SELECT setval(
      pg_get_serial_sequence('public.zvk','id'),
      COALESCE((SELECT MAX(id) FROM public.zvk), 0)
    );
  `);

  // ✅ СИНХРОНИЗИРУЕМ zvk_id_seq (чтобы ZFT продолжался дальше)
  await pool.query(`
    SELECT setval(
      'public.zvk_id_seq',
      COALESCE((SELECT MAX((regexp_replace(id_zvk, '\\D','','g'))::bigint) FROM public.zvk), 0)
    );
  `);

  // ✅ Источник по строке истории (zvk_row_id UNIQUE)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS zvk_status (
      zvk_row_id bigint,
      status_time timestamptz,
      src_d text,
      src_o text
    );
  `);

  await pool.query(`
    CREATE UNIQUE INDEX IF NOT EXISTS zvk_status_row_uq
    ON zvk_status (zvk_row_id);
  `);

  // ✅ Оплата по строке истории (zvk_row_id PK)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS zvk_pay (
      zvk_row_id bigint PRIMARY KEY,
      registry_flag text,
      is_paid text,
      agree_time timestamptz,
      pay_time timestamptz
    );
  `);

  // (опционально) согласование по id_zvk
  await pool.query(`
    CREATE TABLE IF NOT EXISTS zvk_agree (
      id_zvk text PRIMARY KEY,
      agree_name text,
      agree_time timestamptz
    );
  `);

  // индексы
  await pool.query(`CREATE INDEX IF NOT EXISTS zvk_idx_ft_date ON zvk (id_ft, zvk_date DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS zvk_idx_zvk_date ON zvk (id_zvk, zvk_date DESC);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS zvk_idx_id ON zvk (id);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS zvk_status_row_idx ON zvk_status (zvk_row_id);`);
  await pool.query(`CREATE INDEX IF NOT EXISTS zvk_pay_row_idx ON zvk_pay (zvk_row_id);`);

  // ✅ VIEW: ИСТОРИЯ
  await pool.query(`
    CREATE OR REPLACE VIEW ft_zvk_history_v2 AS
    SELECT
      f.id_ft,
      f.input_date,
      f.input_name,
      f.division,
      f."object" AS object,
      f.contractor,

      f.pay_purpose,
      f.dds_article,
      f.contract_no,
      f.contract_date,

      f.invoice_no,
      f.invoice_date,
      f.invoice_pdf,
      f.sum_ft,

      z.id_zvk,
      z.zvk_date,
      z.zvk_name,
      z.to_pay,
      z.request_flag,

      z.id AS zvk_row_id,

      s.status_time,
      s.src_d,
      s.src_o,

      p.agree_time,
      p.registry_flag,
      p.pay_time,
      p.is_paid

    FROM ft f
    LEFT JOIN zvk z ON z.id_ft = f.id_ft

    LEFT JOIN LATERAL (
      SELECT s.*
      FROM zvk_status s
      WHERE s.zvk_row_id = z.id
      ORDER BY s.status_time DESC NULLS LAST
      LIMIT 1
    ) s ON TRUE

    LEFT JOIN zvk_pay p ON p.zvk_row_id = z.id;
  `);

  // ✅ VIEW: ТЕКУЩЕЕ
  await pool.query(`
    CREATE OR REPLACE VIEW ft_zvk_current_v2 AS
    WITH ranked AS (
      SELECT
        v.*,
        ROW_NUMBER() OVER (
          PARTITION BY v.id_ft, v.id_zvk
          ORDER BY v.zvk_date DESC NULLS LAST, v.zvk_row_id DESC
        ) AS rn
      FROM ft_zvk_history_v2 v
    )
    SELECT *
    FROM ranked
    WHERE rn = 1
      AND NOT (zvk_name = 'СИСТЕМА' AND COALESCE(request_flag,'') = 'Нет');
  `);

  // ✅ Таблица для данных от 1С
  await pool.query(`
    CREATE TABLE IF NOT EXISTS data_from_1c (
      id SERIAL PRIMARY KEY,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      raw_data JSONB NOT NULL
    );
  `);
  // =========================
  // REGISTRY HEAD
  // =========================
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.registry_head (
      id bigserial PRIMARY KEY,
      registry_no bigint,
      registry_date date DEFAULT CURRENT_DATE,
      created_by text,
      division text,
      total_amount numeric(18,2) DEFAULT 0,
      items_count integer DEFAULT 0,
      workflow_stage text DEFAULT 'Инициация',
      agree_status text,
      execution_status text,
      archive_flag text DEFAULT 'Нет',
      pdf_url text,
      created_at timestamptz DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE SEQUENCE IF NOT EXISTS public.registry_no_seq START 1;
  `);

  await pool.query(`
    SELECT setval(
      'public.registry_no_seq',
      COALESCE((SELECT MAX(registry_no) FROM public.registry_head), 0)
    );
  `);

  await pool.query(`
    ALTER TABLE public.registry_head
    ALTER COLUMN registry_no SET DEFAULT nextval('public.registry_no_seq');
  `);

  // =========================
  // REGISTRY ITEMS
  // =========================
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.registry_items (
      id bigserial PRIMARY KEY,
      registry_id bigint NOT NULL,
      zvk_row_id bigint,
      id_ft text,
      id_zvk text,
      object text,
      contractor text,
      pay_purpose text,
      dds_article text,
      contract_no text,
      invoice_no text,
      invoice_date date,
      invoice_pdf text,
      src_d text,
      src_o text,
      to_pay numeric(18,2) DEFAULT 0
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS registry_items_registry_id_idx
    ON public.registry_items (registry_id);
  `);

  // =========================
  // REGISTRY APPROVE LOG
  // =========================
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.registry_approve_log (
      id bigserial PRIMARY KEY,
      registry_id bigint NOT NULL,
      stage_name text NOT NULL,
      approver_login text,
      approver_name text,
      action_type text NOT NULL,
      comment_text text,
      created_at timestamptz DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS registry_approve_log_registry_id_idx
    ON public.registry_approve_log (registry_id);
  `);

  // =========================
  // APPROVAL COLUMNS
  // =========================
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_buh_name text;`);
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_buh_status text;`);
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_buh_time timestamptz;`);
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_buh_comment text;`);

  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_fin_name text;`);
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_fin_status text;`);
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_fin_time timestamptz;`);
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_fin_comment text;`);

  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_zam_name text;`);
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_zam_status text;`);
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_zam_time timestamptz;`);
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_zam_comment text;`);

  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_ud_name text;`);
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_ud_status text;`);
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_ud_time timestamptz;`);
  await pool.query(`ALTER TABLE public.registry_head ADD COLUMN IF NOT EXISTS acc_ud_comment text;`);
  console.log("DB init OK ✅");
}

initDb().catch((e) => console.error("DB init error:", e));

// =====================================================
// Health
// =====================================================
app.get("/", (req, res) => res.send("Service-NS API работает 🚀 v-fixed-full-2"));

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
    const isAll   = String(req.query.is_all   || "0") === "1"; // ✅ НОВОЕ
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

     const r = (isAdmin || isAll)
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
// ✅ Пока ПОСЛЕДНЯЯ строка цикла НЕ оплачена -> пишем в тот же id_zvk
// ✅ Если ПОСЛЕДНЯЯ строка оплачена -> создаём новый id_zvk
// ✅ Возвращает zvk_row_id (это zvk.id)
// =====================================================
// =====================================================
// SAVE (история) — /zvk-save
// ✅ Права:
//    - Админ / is_all (R_Kasymkhan) → может создавать/менять всем
//    - Инициатор / Оператор → может создавать/менять ТОЛЬКО свои FT (ft.input_name == login)
// ✅ Пока ПОСЛЕДНЯЯ строка цикла НЕ оплачена -> пишем в тот же id_zvk
// ✅ Если ПОСЛЕДНЯЯ строка оплачена -> создаём новый id_zvk
// ✅ Возвращает zvk_row_id (это zvk.id)
// =====================================================

// helpers (если их ещё нет выше по файлу)
function isTruthy(v){
  return v === true || v === 1 || v === "1" || String(v).toLowerCase() === "true";
}
function normLogin(v){
  return String(v || "").trim().toLowerCase();
}
async function canEditFtByLogin(poolOrClient, id_ft, login){
  const ft = String(id_ft || "").trim();
  const lg = normLogin(login);
  if (!ft || !lg) return false;

  const r = await poolOrClient.query(
    `SELECT 1
     FROM ft
     WHERE id_ft = $1
       AND lower(trim(input_name)) = $2
     LIMIT 1`,
    [ft, lg]
  );
  return r.rowCount > 0;
}
async function canEditRowByLogin(poolOrClient, zvk_row_id, login) {
  const rid = Number(zvk_row_id);
  const lg  = normLogin(login);

  if (!rid || Number.isNaN(rid) || !lg) return false;

  const r = await poolOrClient.query(
    `
    SELECT 1
    FROM zvk z
    JOIN ft f ON f.id_ft = z.id_ft
    WHERE z.id = $1
      AND lower(trim(f.input_name)) = $2
    LIMIT 1
    `,
    [rid, lg]
  );

  return r.rowCount > 0;
}

app.post("/zvk-save", async (req, res) => {
  try {
    const {
      id_ft,
      user_name,
      to_pay,
      request_flag,
      login,
      is_admin,
      is_all,
      can_edit_all   // ✅ ДОБАВИЛИ
    } = req.body;

    if (!id_ft) return res.status(400).json({ success:false, error:"id_ft is required" });

    const actor = String(login || user_name || "").trim();
    if (!actor) return res.status(400).json({ success:false, error:"login required" });

    // ✅ админ / супервайзер
    const adminOk =
      isTruthy(is_admin) ||
      isTruthy(is_all) ||
      isTruthy(can_edit_all);

    // ✅ НЕ админ/супер -> только свои FT
    if (!adminOk) {
      const ok = await canEditFtByLogin(pool, id_ft, actor);
      if (!ok) return res.status(403).json({ success:false, error:"NO_RIGHTS_THIS_FT" });
    }

    const ft = String(id_ft).trim();
    const name = (user_name || actor || "СИСТЕМА").toString().trim();
    const flag = (request_flag || "Нет").toString().trim();

    const toPayNum =
      (to_pay === "" || to_pay === undefined || to_pay === null) ? 0 : Number(to_pay);

    if (Number.isNaN(toPayNum)) {
      return res.status(400).json({ success:false, error:"to_pay must be number" });
    }

    const lastCycle = await pool.query(
      `
      SELECT z.id_zvk
      FROM zvk z
      WHERE z.id_ft = $1
      ORDER BY
        COALESCE(NULLIF(substring(z.id_zvk from '\\d+'), ''), '0')::int DESC,
        z.zvk_date DESC NULLS LAST,
        z.id DESC
      LIMIT 1
      `,
      [ft]
    );

    let id_zvk = lastCycle.rows[0]?.id_zvk || null;

    if (id_zvk) {
      const lastRow = await pool.query(
        `
        SELECT z.id
        FROM zvk z
        WHERE z.id_zvk = $1
        ORDER BY z.zvk_date DESC NULLS LAST, z.id DESC
        LIMIT 1
        `,
        [id_zvk]
      );

      const lastRowId = lastRow.rows[0]?.id || null;

      if (lastRowId) {
        const paid = await pool.query(
          `SELECT is_paid FROM zvk_pay WHERE zvk_row_id=$1`,
          [Number(lastRowId)]
        );
        if (paid.rows[0]?.is_paid === "Да") id_zvk = null;
      }
    }

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

    const r = await pool.query(
      `
      INSERT INTO zvk (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
      VALUES ($1, $2, NOW(), $3, $4, $5)
      RETURNING id, id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag
      `,
      [id_zvk, ft, name, toPayNum, flag]
    );

    res.json({
      success: true,
      row: r.rows[0],
      id_zvk,
      zvk_row_id: r.rows[0].id,
    });

  } catch (e) {
    console.error("ZVK-SAVE ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  }
});

// =====================================================
// ✅ Источник по строке истории
// POST /zvk-status-row  { zvk_row_id, src_d, src_o }
// =====================================================
app.post("/zvk-status-row", async (req, res) => {
  try {
    const { zvk_row_id, src_d, src_o, login, is_admin, can_edit_all, is_all } = req.body;

    if (!zvk_row_id)
      return res.status(400).json({ success:false, error:"zvk_row_id required" });

    const rid = Number(zvk_row_id);
    if (Number.isNaN(rid))
      return res.status(400).json({ success:false, error:"zvk_row_id must be number" });

    const actor = String(login || "").trim();
    if (!actor) return res.status(400).json({ success:false, error:"login required" });

    const adminOk = isTruthy(is_admin) || isTruthy(can_edit_all) || String(is_all || "0") === "1";

    // ✅ НЕ админ -> можно менять ТОЛЬКО свои строки (по FT.input_name)
    if (!adminOk) {
      const ok = await canEditRowByLogin(pool, rid, actor);
      if (!ok) return res.status(403).json({ success:false, error:"NO_RIGHTS_THIS_ROW" });
    }

    // --- дальше твой upsert ---
    const r = await pool.query(
      `
      INSERT INTO zvk_status (zvk_row_id, status_time, src_d, src_o)
      VALUES ($1, NOW(), $2, $3)
      ON CONFLICT (zvk_row_id)
      DO UPDATE SET
        status_time = NOW(),
        src_d = EXCLUDED.src_d,
        src_o = EXCLUDED.src_o
      RETURNING *
      `,
      [rid, String(src_d || ""), String(src_o || "")]
    );

    res.json({ success:true, row: r.rows[0] });

  } catch (e) {
    console.error("ZVK-STATUS-ROW ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  }
});
// =====================================================
// ✅ Оплата/Реестр — ПО СТРОКЕ истории (zvk_row_id)
// POST /zvk-pay-row  { is_admin, zvk_row_id, registry_flag, is_paid }
// ✅ + авто-создание следующего ZFT (СИСТЕМА)
// ✅ FIX: авто-строка СИСТЕМА создаётся с is_paid=NULL (пусто), а НЕ "Нет"
// =====================================================
app.post("/zvk-pay-row", async (req, res) => {
  const client = await pool.connect();
  try {
    const { is_admin, zvk_row_id, registry_flag, is_paid } = req.body;

    const adminOk =
      is_admin === true || is_admin === 1 || is_admin === "1" ||
      String(is_admin).toLowerCase() === "true";

    if (!adminOk)
      return res.status(403).json({ success:false, error:"only admin allowed" });

    if (!zvk_row_id)
      return res.status(400).json({ success:false, error:"zvk_row_id required" });

    await client.query("BEGIN");

    const r = await client.query(
      `
      INSERT INTO zvk_pay (zvk_row_id, registry_flag, is_paid, pay_time, agree_time)
      VALUES (
        $1,
        $2,
        $3,
        CASE WHEN $3 = 'Да' THEN NOW() ELSE NULL END,
        CASE WHEN $2 IN ('Да','Обнуление') THEN NOW() ELSE NULL END
      )
      ON CONFLICT (zvk_row_id)
      DO UPDATE SET
        registry_flag = EXCLUDED.registry_flag,

        agree_time = CASE
          WHEN EXCLUDED.registry_flag IN ('Да','Обнуление')
            THEN COALESCE(zvk_pay.agree_time, NOW())
          WHEN EXCLUDED.registry_flag = 'Нет'
            THEN NULL
          ELSE zvk_pay.agree_time
        END,

        is_paid = EXCLUDED.is_paid,

        pay_time = CASE
          WHEN EXCLUDED.is_paid = 'Да'
            THEN COALESCE(zvk_pay.pay_time, NOW())
          WHEN EXCLUDED.is_paid <> 'Да'
            THEN NULL
          ELSE zvk_pay.pay_time
        END
      RETURNING *;
      `,
      [
        Number(zvk_row_id),
        registry_flag ? String(registry_flag).trim() : null,
        is_paid ? String(is_paid).trim() : null,
      ]
    );

    const reg = (registry_flag ? String(registry_flag).trim() : "");
    const paid = (is_paid ? String(is_paid).trim() : "");

    // ===============================
    // ✅ АВТО-СОЗДАНИЕ СЛЕДУЮЩЕГО ZFT (СИСТЕМА)
    // Когда: Реестр=Да/Обнуление и Оплачено=Да
    // ===============================
    if (paid === "Да" && (reg === "Да" || reg === "Обнуление")) {

      const zr = await client.query(
        `SELECT id, id_ft, id_zvk, to_pay
         FROM zvk
         WHERE id = $1
         LIMIT 1`,
        [Number(zvk_row_id)]
      );

      const zrow = zr.rows[0];

      if (zrow) {
        const ft = String(zrow.id_ft);
        const paidToPay = Number(zrow.to_pay || 0);

        // ✅ берём баланс из последней строки "СИСТЕМА" текущего цикла (id_zvk)
        const baseRow = await client.query(
          `
          SELECT z.to_pay
          FROM zvk z
          WHERE z.id_ft = $1
            AND z.id_zvk = $2
            AND z.zvk_name = 'СИСТЕМА'
          ORDER BY z.id DESC
          LIMIT 1
          `,
          [ft, String(zrow.id_zvk)]
        );

        const baseBalance = Number(baseRow.rows[0]?.to_pay || 0);

        let remaining = 0;
        if (reg === "Обнуление") remaining = 0;
        else remaining = Math.max(baseBalance - paidToPay, 0);

        // создаем новый ZFT только если остаток > 0
        if (remaining > 0) {

          // защита от дубля
          const already = await client.query(
            `
            SELECT 1
            FROM zvk z
            WHERE z.id_ft = $1
              AND z.zvk_name = 'СИСТЕМА'
              AND COALESCE(z.request_flag,'') = 'Нет'
              AND z.to_pay = $2
              AND z.id_zvk <> $3
            ORDER BY z.id DESC
            LIMIT 1
            `,
            [ft, remaining, String(zrow.id_zvk)]
          );

          if (already.rowCount === 0) {
            const created = await client.query(
              `SELECT 'ZFT' || nextval('zvk_id_seq')::text AS id_zvk`
            );
            const newIdZvk = created.rows[0].id_zvk;

            const ins = await client.query(
              `
              INSERT INTO zvk (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
              VALUES ($1, $2, NOW(), 'СИСТЕМА', $3, 'Нет')
              RETURNING id
              `,
              [newIdZvk, ft, remaining]
            );

            const newRowId = ins.rows[0]?.id;

            // ✅ FIX: не ставим is_paid='Нет' для СИСТЕМА (пусть будет ПУСТО)
            if (newRowId) {
              await client.query(
                `
                INSERT INTO zvk_pay (zvk_row_id, registry_flag, is_paid, pay_time, agree_time)
                VALUES ($1, NULL, NULL, NULL, NULL)
                ON CONFLICT (zvk_row_id) DO NOTHING
                `,
                [Number(newRowId)]
              );
            }
          }
        }
      }
    }

    await client.query("COMMIT");
    res.json({ success:true, row: r.rows[0] });

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("ZVK-PAY-ROW ERROR:", e);
    res.status(500).json({ success:false, error: e.message });
  } finally {
    client.release();
  }
});

// =====================================================
// JOIN: читаем из VIEW ft_zvk_current_v2
// =====================================================


app.get("/ft-zvk-join", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 500), 500);
    const login = String(req.query.login || "").trim();

    const isAdmin    = String(req.query.is_admin || "0") === "1";
    const isAll      = String(req.query.is_all || "0") === "1";          // R_Kasymkhan
    const isOperator = String(req.query.is_operator || "0") === "1";     // ✅ Оператор

    if (!login) return res.status(400).json({ success:false, error:"login is required" });

    let query = "";
    let params = [limit];

    if (isAdmin || isAll || isOperator) {
      // ✅ админ/супер/оператор видят всё
      query = `
        SELECT v.*
        FROM ft_zvk_current_v2 v
        ORDER BY
          COALESCE(NULLIF(substring(v.id_ft from '\\d+'), ''), '0')::int DESC,
          v.zvk_date DESC NULLS LAST,
          v.zvk_row_id DESC
        LIMIT $1
      `;
    } else {
      // ✅ инициатор видит только своё
      query = `
        SELECT v.*
        FROM ft_zvk_current_v2 v
        WHERE lower(trim(v.input_name)) = lower(trim($2))
        ORDER BY
          COALESCE(NULLIF(substring(v.id_ft from '\\d+'), ''), '0')::int DESC,
          v.zvk_date DESC NULLS LAST,
          v.zvk_row_id DESC
        LIMIT $1
      `;
      params.push(login);
    }

    const r = await pool.query(query, params);

    // ✅ can_edit: админ/супер — true; иначе только свои FT
    const loginNorm = normLogin(login);

    const rows = r.rows.map(x => ({
      ...x,
      can_edit: (isAdmin || isAll)
        ? true
        : (normLogin(x.input_name) === loginNorm)
    }));

    res.json({ success:true, rows, count: rows.length, isAdmin, isOperator });

  } catch (e) {
    console.error("FT-ZVK-JOIN ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  }
});
// =====================================================
// SAVE FT (создать FT + авто ZFT + строка СИСТЕМА)
// =====================================================
app.post("/save-ft", async (req, res) => {
  try {
    const {
  input_date,
  input_name,
  division,
  object,
  contractor,

  pay_purpose,
  dds_article,
  contract_no,
  contract_date,

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

    const idRow = await pool.query(`SELECT 'FT' || nextval('ft_id_seq')::text AS id_ft`);
    const id_ft = idRow.rows[0].id_ft;

    let inputDateFormatted = input_date;
    if (input_date && typeof input_date === "string") {
      inputDateFormatted = new Date(input_date);
    }

  const r = await pool.query(
  `
  INSERT INTO public.ft
    (id_ft, input_date, input_name, division, "object", contractor,
     pay_purpose, dds_article, contract_no, contract_date,
     invoice_no, invoice_date, invoice_pdf, sum_ft)
  VALUES
    ($1, $2, $3, $4, $5, $6,
     $7, $8, $9, $10,
     $11, $12, $13, $14)
  RETURNING id_ft
  `,
  [
    id_ft,
    inputDateFormatted,
    String(input_name).trim(),
    String(division).trim(),
    String(object).trim(),
    String(contractor).trim(),

    pay_purpose ? String(pay_purpose).trim() : null,
    dds_article ? String(dds_article).trim() : null,
    contract_no ? String(contract_no).trim() : null,
    contract_date ? contract_date : null,  // YYYY-MM-DD или null

    String(invoice_no).trim(),
    invoice_date ? invoice_date : null,    // YYYY-MM-DD или null
    invoice_pdf ? String(invoice_pdf).trim() : null,
    sumNum
  ]
);

    const zftRow = await pool.query(`SELECT 'ZFT' || nextval('zvk_id_seq')::text AS id_zvk`);
    const id_zvk = zftRow.rows[0].id_zvk;

    await pool.query(
      `
      INSERT INTO zvk (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
      VALUES ($1, $2, NOW(), 'СИСТЕМА', $3, 'Нет')
      `,
      [id_zvk, id_ft, sumNum]
    );

    res.json({ success:true, id_ft: r.rows[0].id_ft, id_zvk });
  } catch (e) {
    console.error("SAVE-FT ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  }
});

app.post("/io-save", async (req, res) => {
  const client = await pool.connect();
  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];
    if (!rows.length) return res.status(400).json({ success:false, error:"rows required" });

    const toNum = (v) => {
      if (v === null || v === undefined || v === "") return null;
      const n = Number(String(v).replace(/\s/g,"").replace(",", "."));
      return Number.isFinite(n) ? n : null;
    };

    await client.query("BEGIN");

    let insPrihod = 0, insPerevod = 0;

    for (const r of rows) {
      const sum = toNum(r.sum);
      if (sum === null) continue;

      const obj    = r.object || null;
      const divIn  = r.div_in || null;
      const ddsIn  = r.dds_in || null;
      const divOut = r.div_out || null;
      const ddsOut = r.dds_out || null;

      // prihod6 (doc_time сам = NOW())
      await client.query(
        `INSERT INTO public.prihod6 (amount_in, object_name, division_in, dds_in)
         VALUES ($1,$2,$3,$4)`,
        [sum, obj, divIn, ddsIn]
      );
      insPrihod++;

      // perevod7 (doc_time сам = NOW())
      await client.query(
        `INSERT INTO public.perevod7 (amount_out, object_name, division_out, dds_out)
         VALUES ($1,$2,$3,$4)`,
        [sum, obj, divOut, ddsOut]
      );
      insPerevod++;
    }

    await client.query("COMMIT");
    res.json({ success:true, inserted:{ prihod6: insPrihod, perevod7: insPerevod } });

  } catch (e) {
    await client.query("ROLLBACK");
    res.status(500).json({ success:false, error:e.message });
  } finally {
    client.release();
  }
});

app.get("/division-svod", async (req, res) => {
  const client = await pool.connect();
  try {
    const r = await client.query(`
      SELECT
        division_dds,
        amount_in,
        amount_out,
        to_pay_paid,
        balance,
        to_pay_registry,
        balance_after_registry
      FROM public.division_svod_web_v1
      ORDER BY division_dds
    `);
    res.json({ ok: true, rows: r.rows });
  } catch (e) {
    console.error(e);
    res.status(500).json({ ok: false, error: String(e.message || e) });
  } finally {
    client.release();
  }
});

// =====================================================
// ✅ НОВЫЕ ЭНДПОИНТЫ ДЛЯ 1С
// =====================================================

// Эндпоинт для приема любых JSON данных от 1С
app.post("/from-1c", async (req, res) => {
  try {
    console.log("=== /from-1c HIT ===");
    console.log("Content-Type:", req.headers["content-type"]);
    console.log("Content-Length:", req.headers["content-length"]);

    // что реально пришло
    console.log("Body typeof:", typeof req.body);
    console.log("Body:", req.body);

    const empty =
      !req.body ||
      (typeof req.body === "object" && Object.keys(req.body).length === 0);

    // если тело пустое — НЕ сохраняем, сразу говорим 1С что пусто
    if (empty) {
      console.log("⚠️ EMPTY BODY from 1C");
      return res.status(400).json({ success: false, error: "EMPTY_JSON_BODY" });
    }

    // сохраняем как есть
    const result = await pool.query(
      "INSERT INTO data_from_1c (raw_data) VALUES ($1) RETURNING id",
      [req.body]
    );

    res.json({
      success: true,
      message: "Данные сохранены",
      id: result.rows[0].id,
    });
  } catch (error) {
    console.error("❌ Ошибка в /from-1c:", error);
    res.status(500).json({ success: false, error: error.message });
  }
});

// Эндпоинт для просмотра последних записей от 1С
app.get('/last-data', async (req, res) => {
    try {
        const limit = Math.min(Number(req.query.limit || 10), 100);
        
        const result = await pool.query(
            'SELECT id, created_at, raw_data FROM data_from_1c ORDER BY id DESC LIMIT $1',
            [limit]
        );
        
        res.json({
            success: true,
            count: result.rows.length,
            rows: result.rows
        });
    } catch (error) {
        console.error('❌ Ошибка в /last-data:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

// Эндпоинт для получения конкретной записи по ID
app.get('/data/:id', async (req, res) => {
    try {
        const id = parseInt(req.params.id);
        
        const result = await pool.query(
            'SELECT id, created_at, raw_data FROM data_from_1c WHERE id = $1',
            [id]
        );
        
        if (result.rows.length === 0) {
            return res.status(404).json({ 
                success: false, 
                error: 'Запись не найдена' 
            });
        }
        
        res.json({
            success: true,
            row: result.rows[0]
        });
    } catch (error) {
        console.error('❌ Ошибка в /data/:id:', error);
        res.status(500).json({ 
            success: false, 
            error: error.message 
        });
    }
});

app.get("/registry", async (req, res) => {
  try {
    const login = String(req.query.login || "").trim();
    if (!login) {
      return res.status(400).json({ success:false, error:"login required" });
    }

const q = `
SELECT
  v.object              AS object,
  v.id_zvk              AS reg_no,
  v.contractor          AS contractor,
  v.pay_purpose         AS pay_purpose,
  v.dds_article         AS dds_article,
  v.contract_no         AS contract_no,
  v.invoice_no          AS invoice_no,
  v.invoice_date        AS invoice_date,
  v.invoice_pdf         AS invoice_pdf,
  COALESCE(v.src_d,'')  AS src_d,
  COALESCE(v.src_o,'')  AS src_o,
  COALESCE(v.to_pay,0)  AS to_pay
FROM ft_zvk_current_v2 v
WHERE lower(trim(v.input_name)) = lower(trim($1))
  AND v.request_flag = 'Да'
  AND NULLIF(trim(COALESCE(v.registry_flag,'')), '') IS NULL   -- ✅ Реестр пусто (__EMPTY__)
  AND COALESCE(v.to_pay,0) <> 0
ORDER BY
  COALESCE(NULLIF(substring(v.id_ft from '\\d+'), ''), '0')::int DESC,
  v.zvk_date DESC NULLS LAST,
  v.zvk_row_id DESC;
`;
    const { rows } = await pool.query(q, [login]);
    const total = rows.reduce((sum, r) => sum + Number(r.to_pay || 0), 0);

    res.json({ success:true, total, rows });

  } catch (e) {
    console.error("registry error", e);
    res.status(500).json({
      success:false,
      error:"SERVER_ERROR",
      message:String(e?.message||e)
    });
  }
});

app.get("/svod-object", async (req, res) => {
  try {

    const result = await pool.query(`
      SELECT
        object_name,
        amount_in,
        to_pay,
        balance,
        registry,
        balance_registry
      FROM public.svod_object_v1
      ORDER BY object_name
    `);

    res.json({
      ok: true,
      rows: result.rows
    });

  } catch (err) {
    console.error("❌ /svod-object error:", err);

    res.status(500).json({
      ok: false,
      error: err.message
    });
  }
});

// =====================================================
// CREATE REGISTRY
// =====================================================

app.post("/create-registry", async (req, res) => {
  const client = await pool.connect();

  try {

    const { row_ids, login } = req.body;

    if (!Array.isArray(row_ids) || row_ids.length === 0) {
      return res.status(400).json({
        success:false,
        error:"row_ids required"
      });
    }

    await client.query("BEGIN");

    // 1️⃣ создаем шапку реестра
    const head = await client.query(`
      INSERT INTO registry_head (created_by)
      VALUES ($1)
      RETURNING id, registry_no
    `,[login || null]);

    const registry_id = head.rows[0].id;
    const registry_no = head.rows[0].registry_no;

    // 2️⃣ вставляем строки реестра
    const items = await client.query(`
     INSERT INTO registry_items
(
  registry_id,
  zvk_row_id,
  id_ft,
  id_zvk,
  object,
  contractor,
  pay_purpose,
  dds_article,
  contract_no,
  invoice_no,
  invoice_date,
  invoice_pdf,
  src_d,
  src_o,
  to_pay
)
     SELECT
  $1,
  v.zvk_row_id,
  v.id_ft,
  v.id_zvk,
  v.object,
  v.contractor,
  v.pay_purpose,
  v.dds_article,
  v.contract_no,
  v.invoice_no,
  v.invoice_date,
  v.invoice_pdf,
  v.src_d,
  v.src_o,
  v.to_pay
FROM ft_zvk_current_v2 v
      WHERE v.zvk_row_id = ANY($2::bigint[])
      RETURNING to_pay
    `,[registry_id,row_ids]);

    // 3️⃣ считаем сумму и количество
    const total = items.rows.reduce((s,r)=>s+Number(r.to_pay||0),0);
    const count = items.rows.length;

    await client.query(`
      UPDATE registry_head
      SET total_amount=$1,
          items_count=$2
      WHERE id=$3
    `,[total,count,registry_id]);

    await client.query(`
  UPDATE public.registry_head
  SET
    workflow_stage = 'Главный бухгалтер',
    agree_status = 'На согласовании',

    acc_buh_name = 'Жасулан Сулейменов',
    acc_buh_status = 'Ожидает',

    acc_fin_name = 'Динара Омарбекова',
    acc_fin_status = 'Ожидает',

    acc_zam_name = 'Марат Койлыбаев',
    acc_zam_status = 'Ожидает',

    acc_ud_name = 'Ермек Касенов',
    acc_ud_status = 'Ожидает'
  WHERE id = $1
`, [registry_id]);
    

    await client.query("COMMIT");

    res.json({
      success:true,
      registry_id,
      registry_no
    });

  } catch(e) {

    await client.query("ROLLBACK");

    console.error("CREATE REGISTRY ERROR:",e);

    res.status(500).json({
      success:false,
      error:e.message
    });

  } finally {
    client.release();
  }
});

app.get("/registry-list", async (req,res)=>{
  try{
    const r = await pool.query(`
      SELECT
        id,
        registry_no,
        registry_date,
        created_by,
        items_count,
        total_amount,
        workflow_stage
      FROM registry_head
      ORDER BY id DESC
    `);

    res.json({
      success:true,
      rows:r.rows
    });

  }catch(e){
    res.status(500).json({
      success:false,
      error:e.message
    });
  }
});

app.get("/registry-card", async (req, res) => {
  try {
    const id = Number(req.query.id);

    if (!id) {
      return res.status(400).json({
        success: false,
        error: "id required"
      });
    }

 const headRes = await pool.query(`
  SELECT
    id,
    registry_no,
    registry_date,
    created_by,
    division,
    total_amount,
    items_count,
    workflow_stage,
    agree_status,
    execution_status,
    archive_flag,
    pdf_url,
    created_at,

    acc_buh_name,
    acc_buh_status,
    acc_buh_time,
    acc_buh_comment,

    acc_fin_name,
    acc_fin_status,
    acc_fin_time,
    acc_fin_comment,

    acc_zam_name,
    acc_zam_status,
    acc_zam_time,
    acc_zam_comment,

    acc_ud_name,
    acc_ud_status,
    acc_ud_time,
    acc_ud_comment

  FROM registry_head
  WHERE id = $1
  LIMIT 1
`, [id]);

    if (!headRes.rows.length) {
      return res.status(404).json({
        success: false,
        error: "registry not found"
      });
    }

    const itemsRes = await pool.query(`
      SELECT
  registry_id,
  zvk_row_id,
  id_ft,
  id_zvk,
  object,
  contractor,
  pay_purpose,
  dds_article,
  contract_no,
  invoice_no,
  invoice_date,
  invoice_pdf,
  src_d,
  src_o,
  to_pay
FROM registry_items
      WHERE registry_id = $1
      ORDER BY id
    `, [id]);

    res.json({
      success: true,
      head: headRes.rows[0],
      items: itemsRes.rows
    });

  } catch (e) {
    console.error("REGISTRY CARD ERROR:", e);
    res.status(500).json({
      success: false,
      error: e.message
    });
  }
});

app.post("/registry-approve", async (req, res) => {
  const client = await pool.connect();

  try {
    const {
      registry_id,
      stage,
      action,
      login,
      name,
      comment
    } = req.body;

    if (!registry_id) {
      return res.status(400).json({ success:false, error:"registry_id required" });
    }

    if (!stage) {
      return res.status(400).json({ success:false, error:"stage required" });
    }

    if (!action) {
      return res.status(400).json({ success:false, error:"action required" });
    }

    await client.query("BEGIN");

    const regRes = await client.query(
      `SELECT * FROM public.registry_head WHERE id = $1 LIMIT 1`,
      [Number(registry_id)]
    );

    if (!regRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ success:false, error:"registry not found" });
    }

    const reg = regRes.rows[0];
    const stageName = String(stage || "").trim();
    const actionName = String(action || "").trim();
    // ----------------------------
    // REJECT
    // ----------------------------
    if (actionName === "reject") {
      let rejectSql = "";
      let rejectParams = [];

      if (stageName === "Главный бухгалтер") {
        rejectSql = `
          UPDATE public.registry_head
          SET
            acc_buh_status = 'Отклонено',
            acc_buh_time = NOW(),
            acc_buh_comment = $2,
            workflow_stage = 'Инициация',
            agree_status = 'Отклонено'
          WHERE id = $1
        `;
        rejectParams = [Number(registry_id), String(comment || "")];
      }

      else if (
  stageName === "Заместитель директора по финансам" ||
  stageName === "Зам. директора по финансам"
) {
        rejectSql = `
          UPDATE public.registry_head
          SET
            acc_fin_status = 'Отклонено',
            acc_fin_time = NOW(),
            acc_fin_comment = $2,
            workflow_stage = 'Инициация',
            agree_status = 'Отклонено'
          WHERE id = $1
        `;
        rejectParams = [Number(registry_id), String(comment || "")];
      }

      else if (stageName === "Заместитель директора") {
        rejectSql = `
          UPDATE public.registry_head
          SET
            acc_zam_status = 'Отклонено',
            acc_zam_time = NOW(),
            acc_zam_comment = $2,
            workflow_stage = 'Инициация',
            agree_status = 'Отклонено'
          WHERE id = $1
        `;
        rejectParams = [Number(registry_id), String(comment || "")];
      }

      else if (stageName === "Управляющий директор") {
        rejectSql = `
          UPDATE public.registry_head
          SET
            acc_ud_status = 'Отклонено',
            acc_ud_time = NOW(),
            acc_ud_comment = $2,
            workflow_stage = 'Инициация',
            agree_status = 'Отклонено'
          WHERE id = $1
        `;
        rejectParams = [Number(registry_id), String(comment || "")];
      }

      else {
        await client.query("ROLLBACK");
        return res.status(400).json({ success:false, error:"unknown stage" });
      }

      await client.query(rejectSql, rejectParams);

      await client.query(
        `
        INSERT INTO public.registry_approve_log
          (registry_id, stage_name, approver_login, approver_name, action_type, comment_text)
        VALUES ($1, $2, $3, $4, 'reject', $5)
        `,
        [
          Number(registry_id),
          String(stage),
          String(login || ""),
          String(name || ""),
          String(comment || "")
        ]
      );

      await client.query("COMMIT");
      return res.json({ success:true, moved_to:"Инициация", action:"reject" });
    }

    // ----------------------------
    // APPROVE
    // ----------------------------
    if (action === "approve") {
      let approveSql = "";
      let approveParams = [];
      let nextStage = "";

  if (stageName === "Главный бухгалтер") {
  approveSql = `
    UPDATE public.registry_head
    SET
      acc_buh_status = 'Согласовано',
      acc_buh_time = NOW(),
      acc_buh_comment = $2,
      workflow_stage = 'Заместитель директора по финансам',
      agree_status = 'На согласовании'
    WHERE id = $1
  `;
  approveParams = [Number(registry_id), String(comment || "")];
  nextStage = "Заместитель директора по финансам";
}

      else if (
  stageName === "Заместитель директора по финансам" ||
  stageName === "Зам. директора по финансам"
) {
        approveSql = `
          UPDATE public.registry_head
          SET
            acc_fin_status = 'Согласовано',
            acc_fin_time = NOW(),
            acc_fin_comment = $2,
            workflow_stage = 'Заместитель директора',
            agree_status = 'На согласовании'
          WHERE id = $1
        `;
        approveParams = [Number(registry_id), String(comment || "")];
        nextStage = "Заместитель директора";
      }

      else if (stageName === "Заместитель директора") {
        approveSql = `
          UPDATE public.registry_head
          SET
            acc_zam_status = 'Согласовано',
            acc_zam_time = NOW(),
            acc_zam_comment = $2,
            workflow_stage = 'Управляющий директор',
            agree_status = 'На согласовании'
          WHERE id = $1
        `;
        approveParams = [Number(registry_id), String(comment || "")];
        nextStage = "Управляющий директор";
      }

      else if (stageName === "Управляющий директор") {
        approveSql = `
          UPDATE public.registry_head
          SET
            acc_ud_status = 'Согласовано',
            acc_ud_time = NOW(),
            acc_ud_comment = $2,
            workflow_stage = 'Исполнение платежей',
            agree_status = 'Согласовано'
          WHERE id = $1
        `;
        approveParams = [Number(registry_id), String(comment || "")];
        nextStage = "Исполнение платежей";
      }

      else {
        await client.query("ROLLBACK");
        return res.status(400).json({ success:false, error:"unknown stage" });
      }

      await client.query(approveSql, approveParams);

      await client.query(
        `
        INSERT INTO public.registry_approve_log
          (registry_id, stage_name, approver_login, approver_name, action_type, comment_text)
        VALUES ($1, $2, $3, $4, 'approve', $5)
        `,
        [
          Number(registry_id),
          String(stage),
          String(login || ""),
          String(name || ""),
          String(comment || "")
        ]
      );

      await client.query("COMMIT");
      return res.json({ success:true, moved_to:nextStage, action:"approve" });
    }

    await client.query("ROLLBACK");
    return res.status(400).json({ success:false, error:"unknown action" });

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("REGISTRY-APPROVE ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  } finally {
    client.release();
  }
});

// =====================================================
// Start
// =====================================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT));