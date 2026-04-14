require("dotenv").config();

const express = require("express");
const cors = require("cors");
const path = require("path");
const { Pool } = require("pg");
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const APP_BASE_URL = process.env.APP_BASE_URL;
const app = express();
const nodemailer = require("nodemailer");
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

const MAIL_USER = process.env.MAIL_USER;
const MAIL_PASS = process.env.MAIL_PASS;

const mailTransporter = nodemailer.createTransport({
  service: "gmail",
  auth: {
    user: MAIL_USER,
    pass: MAIL_PASS
  }
});

function normalizeEmail(v) {
  return String(v || "").trim().toLowerCase();
}

function generateTempPassword(length = 8) {
  const chars = "ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz23456789";
  let result = "";
  for (let i = 0; i < length; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}


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
s.status_comment,

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

await pool.query(`
  CREATE TABLE IF NOT EXISTS public.docs_from_1c (
    doc_number TEXT PRIMARY KEY,
    doc_date TIMESTAMPTZ,
    organization_name TEXT,
    counterparty_name TEXT,
    total_amount NUMERIC(18,2),
    items JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
  );
`);
  
await pool.query(`
  CREATE TABLE IF NOT EXISTS public.users (
    id bigserial PRIMARY KEY,
    email text UNIQUE NOT NULL,
    password text NOT NULL,
    phone text,
    last_name text,
    first_name text,
    middle_name text,
    organization_name text,
    role text DEFAULT 'user',
    is_active boolean DEFAULT true,
    created_at timestamptz DEFAULT now()
  );
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS users_email_idx
  ON public.users (lower(trim(email)));
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
  ALTER TABLE public.registry_head
  ADD COLUMN IF NOT EXISTS chat_map jsonb;
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
  ALTER TABLE public.registry_items
  ADD COLUMN IF NOT EXISTS input_name text;
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
  // REQUEST HEAD
  // =========================
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.request_head (
      id bigserial PRIMARY KEY,
      request_no bigint,
      request_date date DEFAULT CURRENT_DATE,
      created_by text,
      total_amount numeric(18,2) DEFAULT 0,
      items_count integer DEFAULT 0,
      workflow_stage text DEFAULT 'Главный бухгалтер',
      agree_status text DEFAULT 'На согласовании',
      archive_flag text DEFAULT 'Нет',
      pdf_url text,
      created_at timestamptz DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE SEQUENCE IF NOT EXISTS public.request_no_seq START 1;
  `);

  await pool.query(`
    SELECT setval(
      'public.request_no_seq',
      COALESCE((SELECT MAX(request_no) FROM public.request_head), 0)
    );
  `);

  await pool.query(`
    ALTER TABLE public.request_head
    ALTER COLUMN request_no SET DEFAULT nextval('public.request_no_seq');
  `);

  // =========================
  // REQUEST ITEMS
  // =========================
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.request_items (
      id bigserial PRIMARY KEY,
      request_id bigint NOT NULL,
      zvk_row_id bigint,
      id_ft text,
      id_zvk text,
      object text,
      input_name text,
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
    CREATE INDEX IF NOT EXISTS request_items_request_id_idx
    ON public.request_items (request_id);
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS request_items_zvk_row_id_idx
    ON public.request_items (zvk_row_id);
  `);

  // =========================
  // REQUEST APPROVE LOG
  // =========================
  await pool.query(`
    CREATE TABLE IF NOT EXISTS public.request_approve_log (
      id bigserial PRIMARY KEY,
      request_id bigint NOT NULL,
      stage_name text NOT NULL,
      approver_login text,
      approver_name text,
      action_type text NOT NULL,
      comment_text text,
      created_at timestamptz DEFAULT now()
    );
  `);

  await pool.query(`
    CREATE INDEX IF NOT EXISTS request_approve_log_request_id_idx
    ON public.request_approve_log (request_id);
  `);

  // =========================
  // REQUEST APPROVAL COLUMNS
  // =========================
  await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_buh_name text;`);
  await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_buh_status text;`);
  await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_buh_time timestamptz;`);
  await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_buh_comment text;`);

  await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_zam_name text;`);
  await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_zam_status text;`);
  await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_zam_time timestamptz;`);
  await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_zam_comment text;`);

  await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_ud_name text;`);
  await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_ud_status text;`);
  await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_ud_time timestamptz;`);
  await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_ud_comment text;`);

await pool.query(`
  CREATE TABLE IF NOT EXISTS public.io_history (
    id bigserial PRIMARY KEY,
    created_at timestamptz DEFAULT now(),
    input_date_text text,
    sum_value numeric(18,2),
    object_name text,
    div_in text,
    dds_in text,
    div_out text,
    dds_out text
  );
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS io_history_created_at_idx
  ON public.io_history (created_at DESC);
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

app.post("/register", async (req, res) => {
  try {
    const {
      email,
      password,
      phone,
      last_name,
      first_name,
      middle_name,
      organization_name
    } = req.body || {};

    const emailNorm = normalizeEmail(email);
    const pass = String(password || "").trim();

    if (!emailNorm) {
      return res.status(400).json({ success:false, message:"Почта обязательна" });
    }

    if (!pass) {
      return res.status(400).json({ success:false, message:"Пароль обязателен" });
    }

    if (!first_name || !last_name) {
      return res.status(400).json({ success:false, message:"Имя и фамилия обязательны" });
    }

    const exists = await pool.query(
      `SELECT id FROM public.users WHERE lower(trim(email)) = $1 LIMIT 1`,
      [emailNorm]
    );

    if (exists.rowCount > 0) {
      return res.status(400).json({
        success:false,
        message:"Пользователь с такой почтой уже существует"
      });
    }

    const r = await pool.query(`
      INSERT INTO public.users (
        email,
        password,
        phone,
        last_name,
        first_name,
        middle_name,
        organization_name,
        role,
        is_active
      )
      VALUES ($1,$2,$3,$4,$5,$6,$7,'user',true)
      RETURNING id, email, role, first_name, last_name
    `, [
      emailNorm,
      pass,
      phone ? String(phone).trim() : null,
      last_name ? String(last_name).trim() : null,
      first_name ? String(first_name).trim() : null,
      middle_name ? String(middle_name).trim() : null,
      organization_name ? String(organization_name).trim() : null
    ]);

    return res.json({
      success:true,
      user:r.rows[0]
    });

  } catch (e) {
    console.error("REGISTER ERROR:", e);
    return res.status(500).json({
      success:false,
      message:"Ошибка сервера"
    });
  }
});

app.post("/login", async (req, res) => {
  try {
    const { email, password } = req.body || {};

    const emailNorm = normalizeEmail(email);
    const pass = String(password || "").trim();

    if (!emailNorm || !pass) {
      return res.status(400).json({
        success:false,
        message:"Введите почту и пароль"
      });
    }

    // тестовый админ
    if (emailNorm === "admin" && pass === "admin") {
      return res.json({
        success:true,
        user:{
          id: 0,
          email:"admin",
          role:"admin",
          first_name:"Admin",
          last_name:"Test"
        }
      });
    }

    const r = await pool.query(`
      SELECT
        id,
        email,
        password,
        role,
        is_active,
        first_name,
        last_name,
        middle_name,
        organization_name,
        phone
      FROM public.users
      WHERE lower(trim(email)) = $1
      LIMIT 1
    `, [emailNorm]);

    if (!r.rowCount) {
      return res.status(401).json({
        success:false,
        message:"Пользователь не найден"
      });
    }

    const user = r.rows[0];

    if (user.is_active === false) {
      return res.status(403).json({
        success:false,
        message:"Аккаунт отключен"
      });
    }

    if (String(user.password || "") !== pass) {
      return res.status(401).json({
        success:false,
        message:"Неверный пароль"
      });
    }

    return res.json({
      success:true,
      user:{
        id:user.id,
        email:user.email,
        role:user.role,
        first_name:user.first_name,
        last_name:user.last_name,
        middle_name:user.middle_name,
        organization_name:user.organization_name,
        phone:user.phone
      }
    });

  } catch (e) {
    console.error("LOGIN ERROR:", e);
    return res.status(500).json({
      success:false,
      message:"Ошибка сервера"
    });
  }
});


app.post("/forgot-password", async (req, res) => {
  try {
    const { email } = req.body || {};
    const emailNorm = normalizeEmail(email);

    if (!emailNorm) {
      return res.status(400).json({
        success: false,
        message: "Укажите почту"
      });
    }

    const userRes = await pool.query(`
      SELECT id, email, first_name
      FROM public.users
      WHERE lower(trim(email)) = $1
      LIMIT 1
    `, [emailNorm]);

    if (!userRes.rowCount) {
      return res.status(404).json({
        success: false,
        message: "Пользователь с такой почтой не найден"
      });
    }

    const user = userRes.rows[0];
    const tempPassword = generateTempPassword(8);

    await pool.query(`
      UPDATE public.users
      SET password = $2
      WHERE id = $1
    `, [user.id, tempPassword]);

    await mailTransporter.sendMail({
      from: `"Service NS" <${MAIL_USER}>`,
      to: user.email,
      subject: "Сброс пароля — Service NS",
      text:
        `Здравствуйте${user.first_name ? ", " + user.first_name : ""}!\n\n` +
        `Ваш временный пароль: ${tempPassword}\n\n` +
        `Используйте его для входа в систему.\n` +
        `После входа рекомендуется сменить пароль.\n\n` +
        `Service NS`,
      html:
        `<p>Здравствуйте${user.first_name ? ", " + user.first_name : ""}!</p>` +
        `<p>Ваш временный пароль: <b>${tempPassword}</b></p>` +
        `<p>Используйте его для входа в систему.</p>` +
        `<p>После входа рекомендуется сменить пароль.</p>` +
        `<p><b>Service NS</b></p>`
    });

    return res.json({
      success: true,
      message: "Новый пароль отправлен на почту"
    });

  } catch (e) {
    console.error("FORGOT-PASSWORD ERROR:", e);
    return res.status(500).json({
      success: false,
      message: "Не удалось отправить письмо"
    });
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

app.post("/create-request", async (req, res) => {
  const client = await pool.connect();

  try {
    const { row_ids, login, pdf_url } = req.body;

    const ids = Array.isArray(row_ids)
      ? row_ids.map(x => Number(x)).filter(Boolean)
      : [];

    if (!ids.length) {
      return res.status(400).json({ success:false, error:"row_ids required" });
    }

    if (!login) {
      return res.status(400).json({ success:false, error:"login required" });
    }

    await client.query("BEGIN");

    const head = await client.query(`
      INSERT INTO public.request_head
        (created_by, pdf_url)
      VALUES ($1, $2)
      RETURNING id, request_no
    `, [
      String(login || "").trim(),
      pdf_url ? String(pdf_url).trim() : null
    ]);

    const request_id = head.rows[0].id;
    const request_no = head.rows[0].request_no;

    const items = await client.query(`
      INSERT INTO public.request_items
      (
        request_id,
        zvk_row_id,
        id_ft,
        id_zvk,
        object,
        input_name,
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
        v.input_name,
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
      FROM public.ft_zvk_current_v2 v
      WHERE v.zvk_row_id = ANY($2::bigint[])
      RETURNING to_pay
    `, [request_id, ids]);

    const total = items.rows.reduce((s, r) => s + Number(r.to_pay || 0), 0);
    const count = items.rows.length;

await client.query(`
  UPDATE public.request_head
  SET
    total_amount = $1,
    items_count = $2,
    workflow_stage = 'Главный бухгалтер',
    agree_status = 'На согласовании',

    acc_buh_name = 'Жасулан Сулейменов',
    acc_buh_status = 'Ожидает',

    acc_zam_name = NULL,
    acc_zam_status = NULL,
    acc_zam_time = NULL,
    acc_zam_comment = NULL,

    acc_ud_name = NULL,
    acc_ud_status = NULL,
    acc_ud_time = NULL,
    acc_ud_comment = NULL
  WHERE id = $3
`, [total, count, request_id]);

    await client.query(`
      INSERT INTO public.request_approve_log
        (request_id, stage_name, approver_login, approver_name, action_type, comment_text)
      VALUES ($1, $2, $3, $4, 'create', $5)
    `, [
      request_id,
      'Инициация',
      String(login || ""),
      String(login || ""),
      'Заявка создана и отправлена на согласование'
    ]);

    await client.query("COMMIT");

    res.json({
      success:true,
      request_id,
      request_no,
      total_amount: total,
      items_count: count
    });

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("CREATE-REQUEST ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  } finally {
    client.release();
  }
});

app.get("/request-list", async (req, res) => {
  try {
    const login = String(req.query.login || "").trim().toLowerCase();
    if (!login) {
      return res.status(400).json({ success:false, error:"login required" });
    }

    const fullViewLogins = ["s_zhasulan", "b_erkin"];

    let r;

    if (fullViewLogins.includes(login)) {
      r = await pool.query(`
        SELECT
          h.id,
          h.request_no,
          h.request_date,
          h.created_by,
          h.total_amount,
          h.items_count,
          h.workflow_stage,
          h.agree_status,
          h.acc_buh_status,
          h.archive_flag,
          h.created_at,

          COUNT(i.id) AS total_rows,
          COUNT(*) FILTER (
            WHERE COALESCE(s.chief_approved, '') = 'Да'
          ) AS approved_rows,

          STRING_AGG(
            DISTINCT CASE
              WHEN COALESCE(s.chief_approved, '') = 'Да' THEN i.id_zvk
              ELSE NULL
            END,
            ', '
          ) AS approved_zfts

        FROM public.request_head h
        LEFT JOIN public.request_items i
          ON i.request_id = h.id
        LEFT JOIN public.zvk_status s
          ON s.zvk_row_id = i.zvk_row_id

        WHERE COALESCE(h.archive_flag, 'Нет') <> 'Да'
        GROUP BY
          h.id,
          h.request_no,
          h.request_date,
          h.created_by,
          h.total_amount,
          h.items_count,
          h.workflow_stage,
          h.agree_status,
          h.acc_buh_status,
          h.archive_flag,
          h.created_at
        ORDER BY h.id DESC
      `);
    } else {
      r = await pool.query(`
        SELECT
          h.id,
          h.request_no,
          h.request_date,
          h.created_by,
          h.total_amount,
          h.items_count,
          h.workflow_stage,
          h.agree_status,
          h.acc_buh_status,
          h.archive_flag,
          h.created_at,

          COUNT(i.id) AS total_rows,
          COUNT(*) FILTER (
            WHERE COALESCE(s.chief_approved, '') = 'Да'
          ) AS approved_rows,

          STRING_AGG(
            DISTINCT CASE
              WHEN COALESCE(s.chief_approved, '') = 'Да' THEN i.id_zvk
              ELSE NULL
            END,
            ', '
          ) AS approved_zfts

        FROM public.request_head h
        LEFT JOIN public.request_items i
          ON i.request_id = h.id
        LEFT JOIN public.zvk_status s
          ON s.zvk_row_id = i.zvk_row_id

        WHERE lower(trim(h.created_by)) = lower(trim($1))
          AND COALESCE(h.archive_flag, 'Нет') <> 'Да'
        GROUP BY
          h.id,
          h.request_no,
          h.request_date,
          h.created_by,
          h.total_amount,
          h.items_count,
          h.workflow_stage,
          h.agree_status,
          h.acc_buh_status,
          h.archive_flag,
          h.created_at
        ORDER BY h.id DESC
      `, [login]);
    }

    res.json({ success:true, rows:r.rows });

  } catch (e) {
    console.error("REQUEST-LIST ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  }
});

app.get("/request-card", async (req, res) => {
  try {
    const id = Number(req.query.id);

    if (!id) {
      return res.status(400).json({ success:false, error:"id required" });
    }

    const headRes = await pool.query(`
      SELECT *
      FROM public.request_head
      WHERE id = $1
      LIMIT 1
    `, [id]);

    if (!headRes.rows.length) {
      return res.status(404).json({ success:false, error:"request not found" });
    }

    const itemsRes = await pool.query(`
SELECT
  i.request_id,
  i.zvk_row_id,
  i.id_ft,
  i.id_zvk,
  i.object,
  i.input_name,
  i.contractor,
  i.pay_purpose,
  i.dds_article,
  i.contract_no,
  i.invoice_no,
  i.invoice_date,
  i.invoice_pdf,
  i.src_d,
  i.src_o,
  i.to_pay,
  COALESCE(s.chief_approved, '') AS chief_approved
FROM public.request_items i
LEFT JOIN public.zvk_status s
  ON s.zvk_row_id = i.zvk_row_id
WHERE i.request_id = $1
ORDER BY i.id
    `, [id]);

    const logRes = await pool.query(`
      SELECT
        id,
        stage_name,
        approver_login,
        approver_name,
        action_type,
        comment_text,
        created_at
      FROM public.request_approve_log
      WHERE request_id = $1
      ORDER BY id
    `, [id]);

    res.json({
      success:true,
      head: headRes.rows[0],
      items: itemsRes.rows,
      log: logRes.rows
    });

  } catch (e) {
    console.error("REQUEST-CARD ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  }
});

app.post("/request-approve", async (req, res) => {
  const client = await pool.connect();

  try {
    const {
      request_id,
      stage,
      action,
      login,
      name,
      comment
    } = req.body;

    if (!request_id) {
      return res.status(400).json({ success:false, error:"request_id required" });
    }

    if (!stage) {
      return res.status(400).json({ success:false, error:"stage required" });
    }

    if (!action) {
      return res.status(400).json({ success:false, error:"action required" });
    }

    const actor = String(login || "").trim().toLowerCase();
    const stageName = String(stage || "").trim();
    const actionName = String(action || "").trim();

    // ✅ согласовывает только главбух
    if (actor !== "s_zhasulan") {
      return res.status(403).json({ success:false, error:"NO_RIGHTS_TO_APPROVE_REQUEST" });
    }

    // ✅ у заявки только один этап согласования
    if (stageName !== "Главный бухгалтер") {
      return res.status(400).json({ success:false, error:"ONLY_MAIN_ACCOUNTANT_STAGE_ALLOWED" });
    }

    await client.query("BEGIN");

    const reqRes = await client.query(
      `SELECT * FROM public.request_head WHERE id = $1 LIMIT 1`,
      [Number(request_id)]
    );

    if (!reqRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ success:false, error:"request not found" });
    }

    if (actionName === "reject") {
      await client.query(`
        UPDATE public.request_head
        SET
          acc_buh_status = 'Отклонено',
          acc_buh_time = NOW(),
          acc_buh_comment = $2,
          workflow_stage = 'Инициация',
          agree_status = 'Отклонено'
        WHERE id = $1
      `, [
        Number(request_id),
        String(comment || "")
      ]);

      await client.query(`
        INSERT INTO public.request_approve_log
          (request_id, stage_name, approver_login, approver_name, action_type, comment_text)
        VALUES ($1, $2, $3, $4, 'reject', $5)
      `, [
        Number(request_id),
        'Главный бухгалтер',
        String(login || ""),
        String(name || ""),
        String(comment || "")
      ]);

      await client.query("COMMIT");
      return res.json({
        success:true,
        action:"reject",
        moved_to:"Инициация"
      });
    }

    if (actionName === "approve") {
      await client.query(`
        UPDATE public.request_head
        SET
          acc_buh_status = 'Согласовано',
          acc_buh_time = NOW(),
          acc_buh_comment = $2,
          workflow_stage = 'Согласовано',
          agree_status = 'Согласовано'
        WHERE id = $1
      `, [
        Number(request_id),
        String(comment || "")
      ]);

      await client.query(`
        INSERT INTO public.request_approve_log
          (request_id, stage_name, approver_login, approver_name, action_type, comment_text)
        VALUES ($1, $2, $3, $4, 'approve', $5)
      `, [
        Number(request_id),
        'Главный бухгалтер',
        String(login || ""),
        String(name || ""),
        String(comment || "")
      ]);

      await client.query("COMMIT");
      return res.json({
        success:true,
        action:"approve",
        moved_to:"Согласовано"
      });
    }

    await client.query("ROLLBACK");
    return res.status(400).json({ success:false, error:"unknown action" });

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("REQUEST-APPROVE ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  } finally {
    client.release();
  }
});

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
  VALUES ($1, $2, NOW(), 'СИСТЕМА', 0, 'Нет')
  `,
  [id_zvk, ft]
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
    const { zvk_row_id, src_d, src_o, status_comment } = req.body;

    if (!zvk_row_id) {
      return res.status(400).json({ success:false, error:"zvk_row_id required" });
    }

    const r = await pool.query(`
      INSERT INTO zvk_status (zvk_row_id, status_time, src_d, src_o, status_comment)
      VALUES ($1, NOW(), $2, $3, $4)
      ON CONFLICT (zvk_row_id)
      DO UPDATE SET
        status_time = NOW(),
        src_d = COALESCE(EXCLUDED.src_d, zvk_status.src_d),
        src_o = COALESCE(EXCLUDED.src_o, zvk_status.src_o),
        status_comment = EXCLUDED.status_comment
      RETURNING *
    `, [
      Number(zvk_row_id),
      src_d || null,
      src_o || null,
      status_comment || ""
    ]);

    res.json({ success:true, row:r.rows[0] });

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

async function rebuildFtTail(client, zvk_row_id) {
  const zr = await client.query(
    `
    SELECT id_ft
    FROM zvk
    WHERE id = $1
    LIMIT 1
    `,
    [Number(zvk_row_id)]
  );

  const ft = String(zr.rows[0]?.id_ft || "").trim();
  if (!ft) {
    return { success:false, reason:"FT_NOT_FOUND" };
  }

const hasReset = await client.query(
  `
  SELECT 1
  FROM zvk z
  JOIN zvk_pay p ON p.zvk_row_id = z.id
  WHERE z.id_ft = $1
    AND p.registry_flag = 'Обнуление'
    AND lower(trim(COALESCE(z.zvk_name,''))) <> 'система'
  LIMIT 1
  `,
  [ft]
);

if (hasReset.rowCount > 0) {
  return {
    success: true,
    ft,
    remaining: 0,
    created: false,
    reason: "HAS_OBNULENIE"
  };
}

  // 1. сумма FT
  const ftRes = await client.query(
    `
    SELECT COALESCE(sum_ft, 0) AS sum_ft
    FROM ft
    WHERE id_ft = $1
    LIMIT 1
    `,
    [ft]
  );

  const ftSum = Number(ftRes.rows[0]?.sum_ft || 0);

  // 2. сколько уже ушло в реестр по обычным строкам
  const usedRes = await client.query(
    `
    SELECT COALESCE(SUM(COALESCE(z.to_pay,0)),0) AS used_sum
    FROM zvk z
    JOIN zvk_pay p ON p.zvk_row_id = z.id
    WHERE z.id_ft = $1
      AND p.registry_flag = 'Да'
      AND lower(trim(COALESCE(z.zvk_name,''))) <> 'система'
    `,
    [ft]
  );

  const usedSum = Number(usedRes.rows[0]?.used_sum || 0);
  const remaining = Math.max(ftSum - usedSum, 0);

  // 3. удалить ВСЕ открытые системные хвосты по этому FT
  const tails = await client.query(
    `
    SELECT z.id
    FROM zvk z
    LEFT JOIN zvk_pay p ON p.zvk_row_id = z.id
    WHERE z.id_ft = $1
      AND lower(trim(COALESCE(z.zvk_name,''))) = 'система'
      AND COALESCE(z.request_flag,'') = 'Нет'
      AND COALESCE(p.registry_flag,'') <> 'Да'
      AND COALESCE(p.is_paid,'') <> 'Да'
    `,
    [ft]
  );

  for (const row of tails.rows) {
    const rid = Number(row.id);
    await client.query(`DELETE FROM zvk_pay WHERE zvk_row_id = $1`, [rid]);
    await client.query(`DELETE FROM zvk_status WHERE zvk_row_id = $1`, [rid]);
    await client.query(`DELETE FROM zvk WHERE id = $1`, [rid]);
  }

  // 4. если остаток есть -> создать только ОДИН хвост
  if (remaining > 0) {
    const created = await client.query(
      `SELECT 'ZFT' || nextval('zvk_id_seq')::text AS id_zvk`
    );
    const newIdZvk = created.rows[0].id_zvk;

    const ins = await client.query(
      `
      INSERT INTO zvk (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
      VALUES ($1, $2, NOW(), 'СИСТЕМА', 0, 'Нет')
      RETURNING id, id_zvk
      `,
      [newIdZvk, ft]
    );

    const newRowId = Number(ins.rows[0].id);

    await client.query(
      `
      INSERT INTO zvk_pay (zvk_row_id, registry_flag, is_paid, pay_time, agree_time)
      VALUES ($1, NULL, NULL, NULL, NULL)
      ON CONFLICT (zvk_row_id) DO NOTHING
      `,
      [newRowId]
    );

    return {
      success:true,
      ft,
      remaining,
      created:true,
      id_zvk:newIdZvk,
      zvk_row_id:newRowId
    };
  }

  return {
    success:true,
    ft,
    remaining:0,
    created:false
  };
}

app.post("/zvk-pay-row", async (req, res) => {
  const client = await pool.connect();

  try {
    const { is_admin, zvk_row_id, registry_flag, is_paid } = req.body;

    const adminOk =
      is_admin === true || is_admin === 1 || is_admin === "1" ||
      String(is_admin).toLowerCase() === "true";

    if (!adminOk) {
      return res.status(403).json({ success:false, error:"only admin allowed" });
    }

    if (!zvk_row_id) {
      return res.status(400).json({ success:false, error:"zvk_row_id required" });
    }

    await client.query("BEGIN");

    const rawReg =
      registry_flag === undefined || registry_flag === null
        ? ""
        : String(registry_flag).trim();

    const reg =
      rawReg === "" || rawReg === "-" || rawReg === "—"
        ? null
        : rawReg;

    const rawPaid =
      is_paid === undefined || is_paid === null
        ? ""
        : String(is_paid).trim();

    const paid =
      rawPaid === "" || rawPaid === "-" || rawPaid === "—"
        ? null
        : rawPaid;

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
          WHEN COALESCE(EXCLUDED.registry_flag,'') = ''
            THEN NULL
          ELSE zvk_pay.agree_time
        END,

        is_paid = EXCLUDED.is_paid,

        pay_time = CASE
          WHEN EXCLUDED.is_paid = 'Да'
            THEN COALESCE(zvk_pay.pay_time, NOW())
          WHEN COALESCE(EXCLUDED.is_paid,'') <> 'Да'
            THEN NULL
          ELSE zvk_pay.pay_time
        END
      RETURNING *;
      `,
      [Number(zvk_row_id), reg, paid]
    );

    let rebuild = null;
    let deletedTail = null;

    const ftRowRes = await client.query(
      `
      SELECT id_ft
      FROM zvk
      WHERE id = $1
      LIMIT 1
      `,
      [Number(zvk_row_id)]
    );

    const id_ft = String(ftRowRes.rows[0]?.id_ft || "").trim();

    if (id_ft) {
      // если Реестр очистили -> удалить только последний авто-хвост
      if (reg === null) {
        deletedTail = await deleteLastAutoTailByFt(client, Number(zvk_row_id));
      }

     // если Реестр = Да -> пересобрать хвост
if (reg === "Да") {
  rebuild = await rebuildFtTail(client, Number(zvk_row_id));
}
    }

    await client.query("COMMIT");

    return res.json({
      success: true,
      row: r.rows[0],
      rebuild,
      deletedTail
    });

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("ZVK-PAY-ROW ERROR:", e);
    return res.status(500).json({ success:false, error:e.message });
  } finally {
    client.release();
  }
});

async function deleteLastAutoTailByFt(client, zvk_row_id) {
  const zr = await client.query(
    `
    SELECT id_ft, id_zvk
    FROM zvk
    WHERE id = $1
    LIMIT 1
    `,
    [Number(zvk_row_id)]
  );

  const row = zr.rows[0];
  if (!row) return { success:false, reason:"ROW_NOT_FOUND" };

  const ft = String(row.id_ft || "").trim();
  const currentIdZvk = String(row.id_zvk || "").trim();

  if (!ft) return { success:false, reason:"FT_NOT_FOUND" };

  // ищем самый последний авто-хвост СИСТЕМА/Нет, но не текущий цикл
  const tailRes = await client.query(
    `
    SELECT z.id, z.id_zvk
    FROM zvk z
    LEFT JOIN zvk_pay p ON p.zvk_row_id = z.id
    WHERE z.id_ft = $1
      AND z.id_zvk <> $2
      AND lower(trim(COALESCE(z.zvk_name,''))) = 'система'
      AND COALESCE(z.request_flag,'') = 'Нет'
      AND COALESCE(p.registry_flag,'') = ''
      AND COALESCE(p.is_paid,'') = ''
    ORDER BY
      COALESCE(NULLIF(substring(z.id_zvk from '\\d+'), ''), '0')::int DESC,
      z.id DESC
    LIMIT 1
    `,
    [ft, currentIdZvk]
  );

  if (!tailRes.rowCount) {
    return { success:true, deleted:false, reason:"NO_TAIL_FOUND" };
  }

  const tailId = Number(tailRes.rows[0].id);

  await client.query(`DELETE FROM zvk_pay WHERE zvk_row_id = $1`, [tailId]);
  await client.query(`DELETE FROM zvk_status WHERE zvk_row_id = $1`, [tailId]);
  await client.query(`DELETE FROM zvk WHERE id = $1`, [tailId]);

  return {
    success:true,
    deleted:true,
    deleted_row_id: tailId,
    deleted_id_zvk: tailRes.rows[0].id_zvk
  };
}

async function resetFtToInitialState(client, id_ft) {
  // 1. найти самый первый ZFT/системную строку
  const firstRes = await client.query(
    `
    SELECT z.id, z.id_zvk
    FROM zvk z
    WHERE z.id_ft = $1
      AND lower(trim(COALESCE(z.zvk_name,''))) = 'система'
    ORDER BY
      COALESCE(NULLIF(substring(z.id_zvk from '\\d+'), ''), '0')::int ASC,
      z.id ASC
    LIMIT 1
    `,
    [id_ft]
  );

  const firstRow = firstRes.rows[0];
  if (!firstRow) return { success:false, reason:"FIRST_SYSTEM_NOT_FOUND" };

  const keepRowId = Number(firstRow.id);

  // 2. удалить все остальные строки этого FT
  const allRows = await client.query(
    `
    SELECT id
    FROM zvk
    WHERE id_ft = $1
      AND id <> $2
    `,
    [id_ft, keepRowId]
  );

  for (const row of allRows.rows) {
    const rid = Number(row.id);
    await client.query(`DELETE FROM zvk_pay WHERE zvk_row_id = $1`, [rid]);
    await client.query(`DELETE FROM zvk_status WHERE zvk_row_id = $1`, [rid]);
    await client.query(`DELETE FROM zvk WHERE id = $1`, [rid]);
  }

  // 3. у первой системной строки вернуть начальные значения
  await client.query(
    `
    UPDATE zvk
    SET
      zvk_name = 'СИСТЕМА',
      to_pay = 0,
      request_flag = 'Нет',
      zvk_date = NOW()
    WHERE id = $1
    `,
    [keepRowId]
  );

  await client.query(
    `
    DELETE FROM zvk_pay
    WHERE zvk_row_id = $1
    `,
    [keepRowId]
  );

  await client.query(
    `
    DELETE FROM zvk_status
    WHERE zvk_row_id = $1
    `,
    [keepRowId]
  );

  await client.query(
    `
    INSERT INTO zvk_pay (zvk_row_id, registry_flag, is_paid, pay_time, agree_time)
    VALUES ($1, NULL, NULL, NULL, NULL)
    ON CONFLICT (zvk_row_id) DO UPDATE SET
      registry_flag = NULL,
      is_paid = NULL,
      pay_time = NULL,
      agree_time = NULL
    `,
    [keepRowId]
  );

  return { success:true, reset:true, keep_row_id: keepRowId, id_zvk: firstRow.id_zvk };
}
// =====================================================
// JOIN: читаем из VIEW ft_zvk_current_v2
// =====================================================


app.get("/ft-zvk-join", async (req, res) => {
  try {
    const login = String(req.query.login || "").trim();

    const isAdmin    = String(req.query.is_admin || "0") === "1";
    const isAll      = String(req.query.is_all || "0") === "1";
    const isOperator = String(req.query.is_operator || "0") === "1";

    if (!login) return res.status(400).json({ success:false, error:"login is required" });

    let query = "";
    let params = [];

    if (isAdmin || isAll || isOperator) {
      // ✅ B_Erkin / Админ / Оператор / Супервайзер видят всё БЕЗ LIMIT
      query = `
        SELECT v.*
        FROM ft_zvk_current_v2 v
        ORDER BY
          COALESCE(NULLIF(substring(v.id_ft from '\\d+'), ''), '0')::int DESC,
          v.zvk_date DESC NULLS LAST,
          v.zvk_row_id DESC
      `;
    } else {
      // ✅ остальным тоже убираем лимит, но оставляем только свои строки
      query = `
        SELECT v.*
        FROM ft_zvk_current_v2 v
        WHERE lower(trim(v.input_name)) = lower(trim($1))
        ORDER BY
          COALESCE(NULLIF(substring(v.id_ft from '\\d+'), ''), '0')::int DESC,
          v.zvk_date DESC NULLS LAST,
          v.zvk_row_id DESC
      `;
      params = [login];
    }

    const r = await pool.query(query, params);

    const loginNorm = normLogin(login);

    const rows = r.rows.map(x => ({
      ...x,
      can_edit: (isAdmin || isAll)
        ? true
        : (normLogin(x.input_name) === loginNorm)
    }));

    res.json({
      success: true,
      rows,
      count: rows.length,
      isAdmin,
      isOperator
    });

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
  VALUES ($1, $2, NOW(), 'СИСТЕМА', 0, 'Нет')
  `,
  [id_zvk, id_ft]
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
    if (!rows.length) {
      return res.status(400).json({ success:false, error:"rows required" });
    }

    const toNum = (v) => {
      if (v === null || v === undefined || v === "") return null;
      const n = Number(String(v).replace(/\s/g,"").replace(",", "."));
      return Number.isFinite(n) ? n : null;
    };

    await client.query("BEGIN");

    let insPrihod = 0;
    let insPerevod = 0;
    let insHistory = 0;

    for (const r of rows) {
      const sum = toNum(r.sum);
      if (sum === null) continue;

      const inputDate = r.input_date || null;
      const obj    = r.object || null;
      const divIn  = r.div_in || null;
      const ddsIn  = r.dds_in || null;
      const divOut = r.div_out || null;
      const ddsOut = r.dds_out || null;

      await client.query(
        `INSERT INTO public.prihod6 (amount_in, object_name, division_in, dds_in)
         VALUES ($1,$2,$3,$4)`,
        [sum, obj, divIn, ddsIn]
      );
      insPrihod++;

      await client.query(
        `INSERT INTO public.perevod7 (amount_out, object_name, division_out, dds_out)
         VALUES ($1,$2,$3,$4)`,
        [sum, obj, divOut, ddsOut]
      );
      insPerevod++;

      await client.query(
        `INSERT INTO public.io_history
          (input_date_text, sum_value, object_name, div_in, dds_in, div_out, dds_out)
         VALUES ($1,$2,$3,$4,$5,$6,$7)`,
        [inputDate, sum, obj, divIn, ddsIn, divOut, ddsOut]
      );
      insHistory++;
    }

    await client.query("COMMIT");

    res.json({
      success:true,
      inserted:{
        prihod6: insPrihod,
        perevod7: insPerevod,
        io_history: insHistory
      }
    });

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("IO-SAVE ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  } finally {
    client.release();
  }
});

app.get("/io-history", async (req, res) => {
  const client = await pool.connect();
  try {
    const limit = Math.min(Number(req.query.limit || 200), 500);

    const r = await client.query(`
      SELECT
        id,
        input_date_text,
        sum_value,
        object_name,
        div_in,
        dds_in,
        div_out,
        dds_out
      FROM public.io_history
      ORDER BY id DESC
      LIMIT $1
    `, [limit]);

    res.json({
      success: true,
      rows: r.rows
    });
  } catch (e) {
    console.error("IO-HISTORY ERROR:", e);
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
    console.log("=== /from-1c ===");
    console.log(req.body);

    const docs = Array.isArray(req.body) ? req.body : [req.body];

    if (!docs.length) {
      return res.status(400).json({
        success: false,
        error: "EMPTY_JSON_BODY"
      });
    }

    let insertedCount = 0;

    for (const data of docs) {
      if (!data || typeof data !== "object") continue;

      if (!data.number) {
        return res.status(400).json({
          success: false,
          error: "number required"
        });
      }

      await pool.query(
        `
        INSERT INTO public.docs_from_1c (
          doc_number,
          doc_date,
          organization_name,
          counterparty_name,
          total_amount,
          items
        )
        VALUES ($1,$2,$3,$4,$5,$6)
        ON CONFLICT (doc_number) DO UPDATE SET
          doc_date = EXCLUDED.doc_date,
          organization_name = EXCLUDED.organization_name,
          counterparty_name = EXCLUDED.counterparty_name,
          total_amount = EXCLUDED.total_amount,
          items = EXCLUDED.items
        `,
        [
          String(data.number).trim(),
          data.date || null,
          data.organization_name || null,
          data.counterparty_name || null,
          data.total_amount ?? 0,
          JSON.stringify(data.items || [])
        ]
      );

      insertedCount++;
    }

    res.json({
      success: true,
      inserted_count: insertedCount,
      message: "Данные приняты"
    });

  } catch (error) {
    console.error("❌ /from-1c error:", error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Эндпоинт для просмотра последних записей от 1С
app.get("/last-data", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 10), 100);

    const result = await pool.query(`
      SELECT *
      FROM public.docs_from_1c
      ORDER BY created_at DESC
      LIMIT $1
    `, [limit]);

    res.json({
      success: true,
      rows: result.rows
    });

  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});


// Эндпоинт для получения конкретной записи по ID
app.get("/data/:number", async (req, res) => {
  try {
    const number = String(req.params.number || "").trim();

    const result = await pool.query(
      `
      SELECT *
      FROM public.docs_from_1c
      WHERE doc_number = $1
      `,
      [number]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({
        success: false,
        error: "Запись не найдена"
      });
    }

    res.json({
      success: true,
      row: result.rows[0]
    });
  } catch (error) {
    console.error("❌ Ошибка в /data/:number:", error);
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
    const { row_ids, login, mode, chat_map } = req.body;

    const ids = Array.isArray(row_ids)
      ? row_ids.map(x => Number(x)).filter(Boolean)
      : [];

    const isEmptyRegistry = ids.length === 0;
    const registryMode = String(mode || "").trim() || (isEmptyRegistry ? "transfer" : "payment");

    await client.query("BEGIN");

    // 1️⃣ создаем шапку реестра + сохраняем chat_map
    const head = await client.query(`
      INSERT INTO registry_head (created_by, chat_map)
      VALUES ($1, $2::jsonb)
      RETURNING id, registry_no
    `, [
      login || null,
      JSON.stringify(chat_map || {})
    ]);

    const registry_id = head.rows[0].id;
    const registry_no = head.rows[0].registry_no;

    let total = 0;
    let count = 0;

    // 2️⃣ если строки выбраны — вставляем их
    if (!isEmptyRegistry) {
      const items = await client.query(`
        INSERT INTO registry_items
        (
          registry_id,
          zvk_row_id,
          id_ft,
          id_zvk,
          object,
          input_name,
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
          v.input_name,
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
      `, [registry_id, ids]);

      total = items.rows.reduce((s, r) => s + Number(r.to_pay || 0), 0);
      count = items.rows.length;
    }

    // 3️⃣ обновляем шапку
    await client.query(`
      UPDATE registry_head
      SET total_amount = $1,
          items_count = $2
      WHERE id = $3
    `, [total, count, registry_id]);

    await client.query(`
      UPDATE public.registry_head
      SET
        workflow_stage = 'Главный бухгалтер',
        agree_status = 'На согласовании',

        acc_buh_name = 'Жасулан Сулейменов',
        acc_buh_status = 'Ожидает',

        acc_zam_name = 'Марат Койлыбаев',
        acc_zam_status = 'Ожидает',

        acc_ud_name = 'Ермек Касенов',
        acc_ud_status = 'Ожидает'
      WHERE id = $1
    `, [registry_id]);

    await client.query("COMMIT");

    try {
      await sendRegistryTelegramNotification({
        registryId: registry_id,
        registryNo: registry_no,
        stage: "Главный бухгалтер",
        totalAmount: total,
        createdBy: login || "",
        chatMap: chat_map || {}
      });
    } catch (tgErr) {
      console.error("telegram registry notify error:", tgErr);
    }

    res.json({
      success: true,
      registry_id,
      registry_no,
      mode: registryMode,
      is_empty_registry: isEmptyRegistry
    });

  } catch(e) {
    await client.query("ROLLBACK");
    console.error("CREATE REGISTRY ERROR:", e);
    res.status(500).json({
      success: false,
      error: e.message
    });
  } finally {
    client.release();
  }
});


const APPROVER_LOGINS = [
  "s_zhasulan",
  "k_marat",
  "k_ermek",
  "k_arailym",
  "zh_elena",
  "b_erkin",
  "b_erkin2"
];

const DIVISION_WATCHERS = {
  "Дорога": ["k_talimzhan", "t_azat"],
  "Механизация": ["k_talimzhan", "t_azat"],
  "Мост": ["k_talimzhan", "t_azat"],
  "Офис": ["k_talimzhan", "t_azat"],
  "Сети": ["k_talimzhan", "t_azat"],
  "СК Жилой дом": ["k_talimzhan", "t_azat"],
  "Sapa asphalt": ["k_talimzhan", "t_azat"],
  "Smart Estate": ["k_talimzhan", "t_azat"]
};


async function getEmployeesByRegistry(registryId, client) {
  const r = await client.query(`
    SELECT DISTINCT lower(trim(COALESCE(input_name,''))) AS input_name
    FROM public.registry_items
    WHERE registry_id = $1
      AND NULLIF(trim(COALESCE(input_name,'')), '') IS NOT NULL
  `, [registryId]);

  return r.rows
    .map(x => String(x.input_name || "").trim().toLowerCase())
    .filter(Boolean);
}
function getWatcherDivisions(login) {
  const lg = String(login || "").trim().toLowerCase();
  const result = [];

  for (const [division, watchers] of Object.entries(DIVISION_WATCHERS)) {
    if ((watchers || []).map(x => String(x).toLowerCase()).includes(lg)) {
      result.push(division);
    }
  }

  return result;
}

app.get("/registry-list", async (req, res) => {
  try {
    const login = String(req.query.login || "").trim().toLowerCase();
    if (!login) {
      return res.status(400).json({ success:false, error:"login required" });
    }

    // 1. Согласующие видят всё
    if (APPROVER_LOGINS.includes(login)) {
      const r = await pool.query(`
        SELECT
          h.id,
          h.registry_no,
          h.registry_date,
          h.created_by,
          h.items_count,
          h.total_amount,
          h.workflow_stage,
          h.archive_flag,
          COALESCE(
            STRING_AGG(DISTINCT NULLIF(TRIM(i.src_d), ''), ', ')
              FILTER (WHERE NULLIF(TRIM(i.src_d), '') IS NOT NULL),
            ''
          ) AS src_d
        FROM public.registry_head h
        LEFT JOIN public.registry_items i
          ON i.registry_id = h.id
        WHERE COALESCE(h.archive_flag, 'Нет') <> 'Да'
        GROUP BY
          h.id,
          h.registry_no,
          h.registry_date,
          h.created_by,
          h.items_count,
          h.total_amount,
          h.workflow_stage,
          h.archive_flag
        ORDER BY h.id DESC
      `);

      return res.json({ success:true, rows:r.rows });
    }

    // 2. Наблюдатели видят только свои дивизионы
    const watcherDivisions = getWatcherDivisions(login);

    if (watcherDivisions.length > 0) {
      const r = await pool.query(`
        SELECT
          h.id,
          h.registry_no,
          h.registry_date,
          h.created_by,
          h.items_count,
          h.total_amount,
          h.workflow_stage,
          h.archive_flag,
          COALESCE(
            STRING_AGG(DISTINCT NULLIF(TRIM(i.src_d), ''), ', ')
              FILTER (WHERE NULLIF(TRIM(i.src_d), '') IS NOT NULL),
            ''
          ) AS src_d
        FROM public.registry_head h
        LEFT JOIN public.registry_items i
          ON i.registry_id = h.id
        WHERE COALESCE(h.archive_flag, 'Нет') <> 'Да'
          AND EXISTS (
            SELECT 1
            FROM public.registry_items i2
            WHERE i2.registry_id = h.id
              AND TRIM(COALESCE(i2.src_d,'')) = ANY($1::text[])
          )
        GROUP BY
          h.id,
          h.registry_no,
          h.registry_date,
          h.created_by,
          h.items_count,
          h.total_amount,
          h.workflow_stage,
          h.archive_flag
        ORDER BY h.id DESC
      `, [watcherDivisions]);

      return res.json({ success:true, rows:r.rows });
    }
    // 3. Сотрудники видят только те реестры, где встречается их input_name
    const employeeCheck = await pool.query(`
      SELECT 1
      FROM public.registry_items
      WHERE lower(trim(COALESCE(input_name,''))) = lower(trim($1))
      LIMIT 1
    `, [login]);

    if (employeeCheck.rowCount > 0) {
      const r = await pool.query(`
        SELECT
          h.id,
          h.registry_no,
          h.registry_date,
          h.created_by,
          h.items_count,
          h.total_amount,
          h.workflow_stage,
          h.archive_flag,
          COALESCE(
            STRING_AGG(DISTINCT NULLIF(TRIM(i.src_d), ''), ', ')
              FILTER (WHERE NULLIF(TRIM(i.src_d), '') IS NOT NULL),
            ''
          ) AS src_d
        FROM public.registry_head h
        LEFT JOIN public.registry_items i
          ON i.registry_id = h.id
        WHERE COALESCE(h.archive_flag, 'Нет') <> 'Да'
          AND EXISTS (
            SELECT 1
            FROM public.registry_items i2
            WHERE i2.registry_id = h.id
              AND lower(trim(COALESCE(i2.input_name,''))) = lower(trim($1))
          )
        GROUP BY
          h.id,
          h.registry_no,
          h.registry_date,
          h.created_by,
          h.items_count,
          h.total_amount,
          h.workflow_stage,
          h.archive_flag
        ORDER BY h.id DESC
      `, [login]);

      return res.json({ success:true, rows:r.rows });
    }

    // 4. Остальным доступа нет
    return res.status(403).json({
      success:false,
      error:"NO_ACCESS_TO_REGISTRY"
    });

  } catch (e) {
    console.error("REGISTRY-LIST ERROR:", e);
    res.status(500).json({
      success: false,
      error: e.message
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
    chat_map,

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
    input_name,
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

      else if (stageName === "Исполнение платежей") {
        const executor = await getExecutorByRegistry(Number(registry_id), client);

        rejectSql = `
          UPDATE public.registry_head
          SET
            execution_status = 'Отклонено',
            workflow_stage = 'Инициация',
            agree_status = 'Отклонено'
          WHERE id = $1
        `;
        rejectParams = [Number(registry_id)];

        await client.query(
          `
          INSERT INTO public.registry_approve_log
            (registry_id, stage_name, approver_login, approver_name, action_type, comment_text)
          VALUES ($1, $2, $3, $4, 'reject', $5)
          `,
          [
            Number(registry_id),
            String(stage),
            executor,
            executor === "Zh_Elena" ? "Елена" : "Арайлым",
            String(comment || "")
          ]
        );

        await client.query(rejectSql, rejectParams);
        await client.query("COMMIT");

        return res.json({ success:true, moved_to:"Инициация", action:"reject" });
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
    if (actionName === "approve") {
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

      else if (stageName === "Исполнение платежей") {
        approveSql = `
          UPDATE public.registry_head
          SET
            execution_status = 'На исполнении',
            workflow_stage = 'Контроль и архивирование',
            agree_status = 'Согласовано'
          WHERE id = $1
        `;
        approveParams = [Number(registry_id)];
        nextStage = "Контроль и архивирование";
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

 if (nextStage) {
  try {
    const savedChatMap = reg.chat_map || {};

    await sendRegistryTelegramNotification({
      registryId: registry_id,
      registryNo: reg.registry_no,
      stage: nextStage,
      totalAmount: reg.total_amount,
      createdBy: reg.created_by || "",
      chatMap: savedChatMap
    });
  } catch (tgErr) {
    console.error("telegram next stage notify error:", tgErr);
  }
}

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


app.post("/registry-move-stage", async (req, res) => {
  const client = await pool.connect();

  try {
    const { registry_id, from_stage, to_stage, login, name } = req.body;

    if (!registry_id) {
      return res.status(400).json({ success:false, error:"registry_id required" });
    }

    if (!from_stage || !to_stage) {
      return res.status(400).json({ success:false, error:"from_stage and to_stage required" });
    }

    const actor = String(login || "").trim().toLowerCase();
    const fromS = String(from_stage || "").trim();
    const toS   = String(to_stage || "").trim();

    await client.query("BEGIN");

    const regRes = await client.query(
      `SELECT * FROM public.registry_head WHERE id = $1 LIMIT 1`,
      [Number(registry_id)]
    );

    if (!regRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ success:false, error:"registry not found" });
    }

    // Ермек может двигать всё
    if (actor === "k_ermek") {
      await client.query(
        `
        UPDATE public.registry_head
        SET workflow_stage = $2
        WHERE id = $1
        `,
        [Number(registry_id), toS]
      );

      await client.query(
        `
        INSERT INTO public.registry_approve_log
          (registry_id, stage_name, approver_login, approver_name, action_type, comment_text)
        VALUES ($1, $2, $3, $4, 'move', $5)
        `,
        [
          Number(registry_id),
          `${fromS} -> ${toS}`,
          String(login || ""),
          String(name || ""),
          "Перемещение через канбан"
        ]
      );

      await client.query("COMMIT");

      try {
await sendRegistryTelegramNotification({
  registryId: Number(registry_id),
  registryNo: regRes.rows[0].registry_no,
  stage: toS,
  totalAmount: regRes.rows[0].total_amount,
  createdBy: regRes.rows[0].created_by || "",
  chatMap: regRes.rows[0].chat_map || {}
});
      } catch (tgErr) {
        console.error("telegram move-stage notify error:", tgErr);
      }

      return res.json({ success:true, moved_to: toS });
    }

    const allowed =
      (actor === "s_zhasulan" && fromS === "Главный бухгалтер" && toS === "Заместитель директора") ||
      (actor === "k_marat"    && fromS === "Заместитель директора" && toS === "Управляющий директор") ||
      ((actor === "k_arailym" || actor === "zh_elena") &&
        fromS === "Исполнение платежей" &&
        toS === "Контроль и архивирование");

    if (!allowed) {
      await client.query("ROLLBACK");
      return res.status(403).json({ success:false, error:"NO_RIGHTS_TO_MOVE_STAGE" });
    }

    await client.query(
      `
      UPDATE public.registry_head
      SET workflow_stage = $2
      WHERE id = $1
      `,
      [Number(registry_id), toS]
    );

    await client.query(
      `
      INSERT INTO public.registry_approve_log
        (registry_id, stage_name, approver_login, approver_name, action_type, comment_text)
      VALUES ($1, $2, $3, $4, 'move', $5)
      `,
      [
        Number(registry_id),
        `${fromS} -> ${toS}`,
        String(login || ""),
        String(name || ""),
        "Перемещение через канбан"
      ]
    );

    await client.query("COMMIT");

try {
  await sendRegistryTelegramNotification({
    registryId: Number(registry_id),
    registryNo: regRes.rows[0].registry_no,
    stage: toS,
    totalAmount: regRes.rows[0].total_amount,
    createdBy: regRes.rows[0].created_by || "",
    chatMap: regRes.rows[0].chat_map || {}
  });
} catch (tgErr) {
  console.error("telegram move-stage notify error:", tgErr);
}

    return res.json({ success:true, moved_to: toS });

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("REGISTRY-MOVE-STAGE ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  } finally {
    client.release();
  }
});


app.post("/registry-send-to-archive", async (req, res) => {
  const client = await pool.connect();

  try {
    const { registry_id, login, name } = req.body;

    if (!registry_id) {
      return res.status(400).json({ success:false, error:"registry_id required" });
    }

    const actor = String(login || "").trim().toLowerCase();

    const allowed = ["b_erkin", "b_erkin2"];
    if (!allowed.includes(actor)) {
      return res.status(403).json({ success:false, error:"NO_RIGHTS_TO_ARCHIVE" });
    }

    await client.query("BEGIN");

    const regRes = await client.query(`
      SELECT id, workflow_stage, archive_flag
      FROM public.registry_head
      WHERE id = $1
      LIMIT 1
    `, [Number(registry_id)]);

    if (!regRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ success:false, error:"registry not found" });
    }

    const reg = regRes.rows[0];

    if (String(reg.archive_flag || "").trim() === "Да") {
      await client.query("ROLLBACK");
      return res.status(400).json({ success:false, error:"ALREADY_ARCHIVED" });
    }

    if (String(reg.workflow_stage || "").trim() !== "Контроль и архивирование") {
      await client.query("ROLLBACK");
      return res.status(400).json({ success:false, error:"NOT_READY_FOR_ARCHIVE" });
    }

    await client.query(`
      UPDATE public.registry_head
      SET
        archive_flag = 'Да',
        execution_status = 'Исполнено'
      WHERE id = $1
    `, [Number(registry_id)]);

    await client.query(`
      INSERT INTO public.zvk_pay (zvk_row_id, registry_flag, is_paid, agree_time, pay_time)
      SELECT
        ri.zvk_row_id,
        'Да',
        'Да',
        NOW(),
        NOW()
      FROM public.registry_items ri
      WHERE ri.registry_id = $1
        AND ri.zvk_row_id IS NOT NULL
      ON CONFLICT (zvk_row_id)
      DO UPDATE SET
        registry_flag = 'Да',
        is_paid = 'Да',
        agree_time = COALESCE(zvk_pay.agree_time, NOW()),
        pay_time = COALESCE(zvk_pay.pay_time, NOW())
    `, [Number(registry_id)]);

    await client.query(`
      INSERT INTO public.registry_approve_log
        (registry_id, stage_name, approver_login, approver_name, action_type, comment_text)
      VALUES ($1, $2, $3, $4, 'archive', $5)
    `, [
      Number(registry_id),
      'Контроль и архивирование',
      String(login || ""),
      String(name || ""),
      'Отправлено в архив'
    ]);

    await client.query("COMMIT");

    res.json({ success:true, archived:true });

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("REGISTRY-SEND-TO-ARCHIVE ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  } finally {
    client.release();
  }
});

app.get("/registry-archive-list", async (req,res)=>{
  try{
    const r = await pool.query(`
      SELECT
        id,
        registry_no,
        registry_date,
        created_by,
        items_count,
        total_amount,
        workflow_stage,
        archive_flag,
        execution_status
      FROM public.registry_head
      WHERE COALESCE(archive_flag,'Нет') = 'Да'
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

async function tgRequest(method, payload) {
  const resp = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/${method}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {})
  });

  const data = await resp.json();
  if (!data.ok) {
    throw new Error(data.description || "Telegram API error");
  }
  return data.result;
}

async function sendTelegramMessage(chatId, text, replyMarkup) {
  return tgRequest("sendMessage", {
    chat_id: String(chatId),
    text,
    parse_mode: "HTML",
    reply_markup: replyMarkup || undefined
  });
}


function getApproverByStage(stage) {
  const s = String(stage || "").trim();

  if (s === "Главный бухгалтер") {
    return { login: "S_Zhasulan", name: "Жасулан Сулейменов" };
  }

  if (s === "Заместитель директора") {
    return { login: "K_Marat", name: "Марат Койлыбаев" };
  }

  if (s === "Управляющий директор") {
    return { login: "K_Ermek", name: "Ермек Касенов" };
  }

  if (s === "Исполнение платежей") {
    return { login: "K_Arailym", name: "Арайлым" };
  }

  if (s === "Контроль и архивирование") {
    return { login: "B_Erkin", name: "B_Erkin" };
  }

  return { login: "telegram_user", name: "Telegram" };
}


app.post("/telegram-webhook", async (req, res) => {
  try {
    const update = req.body || {};

    if (update.message) {
      const msg = update.message;
      const chatId = String(msg.chat?.id || "");
      const text = String(msg.text || "").trim();

      if (text === "/start") {
        await sendTelegramMessage(
          chatId,
          "Бот подключен ✅\nНапишите ваш логин системы."
        );
      } else if (text) {
        await sendTelegramMessage(
          chatId,
          `Ваш chat_id: <b>${chatId}</b>\nЛогин: <b>${text}</b>`
        );
      }
    }

    if (update.callback_query) {
      const cb = update.callback_query;
      const callbackId = cb.id;
      const chatId = String(cb.message?.chat?.id || "");
      const data = String(cb.data || "");

    const [action, registryId, stage] = data.split("|");

let approver = getApproverByStage(stage);

if (String(stage || "").trim() === "Исполнение платежей") {
  const client = await pool.connect();
  try {
    const executor = await getExecutorByRegistry(Number(registryId), client);

    if (executor === "Zh_Elena") {
      approver = { login: "Zh_Elena", name: "Елена" };
    } else {
      approver = { login: "K_Arailym", name: "Арайлым" };
    }
  } finally {
    client.release();
  }
}

const resp = await fetch(`${APP_BASE_URL}/registry-approve`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    registry_id: Number(registryId),
    stage: stage,
    action: action,
    login: approver.login,
    name: approver.name,
    comment: action === "reject" ? "Отклонено из Telegram" : "Согласовано из Telegram"
  })
});

      const result = await resp.json();

      await tgRequest("answerCallbackQuery", {
        callback_query_id: callbackId,
        text: result?.success
          ? (action === "approve" ? "Согласовано" : "Отклонено")
          : (result?.error || "Ошибка")
      });

      if (result?.success) {
        await sendTelegramMessage(
          chatId,
          action === "approve"
            ? `✅ Реестр №${registryId} согласован`
            : `❌ Реестр №${registryId} отклонён`
        );
      }
    }

    return res.json({ ok: true });
  } catch (e) {
    console.error("TELEGRAM WEBHOOK ERROR:", e);
    return res.status(500).json({ ok: false, error: e.message });
  }
});

async function getExecutorByRegistry(registryId, client) {
  const r = await client.query(`
    SELECT DISTINCT TRIM(COALESCE(src_d,'')) AS src_d
    FROM registry_items
    WHERE registry_id = $1
  `, [registryId]);

  const divisions = r.rows.map(x => x.src_d);

  const elenaDivs = [
    "СК Жилой дом",
    "Sapa asphalt",
    "Smart Estate"
  ];

  const hasElena = divisions.some(d => elenaDivs.includes(d));

  return hasElena ? "Zh_Elena" : "K_Arailym";
}



async function getWatchersByRegistry(registryId, client) {
  const r = await client.query(`
    SELECT DISTINCT TRIM(COALESCE(src_d,'')) AS src_d
    FROM registry_items
    WHERE registry_id = $1
  `, [registryId]);

  const divisions = r.rows.map(x => x.src_d);
  const watchers = new Set();

  for (const d of divisions) {
    const list = DIVISION_WATCHERS[d] || [];
    list.forEach(w => watchers.add(w));
  }

  const executor = await getExecutorByRegistry(registryId, client);

  const excluded = new Set([
    "S_Zhasulan", // главный бухгалтер
    "K_Marat",    // заместитель директора
    "K_Ermek",    // утверждающий
    executor      // исполнитель
  ]);

  return Array.from(watchers).filter(w => !excluded.has(w));
}

async function getRegistryChatMap(registryId, client) {
  const r = await client.query(`
    SELECT chat_map
    FROM public.registry_head
    WHERE id = $1
    LIMIT 1
  `, [Number(registryId)]);

  return r.rows[0]?.chat_map || {};
}

async function sendRegistryTelegramNotification({
  registryId,
  registryNo,
  stage,
  totalAmount,
  createdBy,
  chatMap = {}
}) {
  try {
    let finalChatMap = chatMap || {};

    // если chatMap не передали — читаем из БД
    if (!finalChatMap || Object.keys(finalChatMap).length === 0) {
      const client = await pool.connect();
      try {
        finalChatMap = await getRegistryChatMap(registryId, client);
      } finally {
        client.release();
      }
    }

    let approverLogin = "";

    if (stage === "Главный бухгалтер") {
      approverLogin = "s_zhasulan";
    } else if (stage === "Заместитель директора") {
      approverLogin = "k_marat";
    } else if (stage === "Управляющий директор") {
      approverLogin = "k_ermek";
    } else if (stage === "Исполнение платежей") {
      const client = await pool.connect();
      try {
        const executor = await getExecutorByRegistry(registryId, client);
        approverLogin = String(executor || "").trim().toLowerCase();
      } finally {
        client.release();
      }
    } else if (stage === "Контроль и архивирование") {
      approverLogin = "b_erkin";
    }

    const approverLoginNorm = String(approverLogin || "").trim().toLowerCase();
    const approverChatId = finalChatMap[approverLoginNorm] || "";

    const openUrl =
      `https://script.google.com/macros/s/AKfycbySY2CFP3WJ9M_MW5HiDZvSScGCTn2SCOLW68SS1Gt5q-CsHGk9lve06PkeKnuZwZ-j/exec?page=registryCard&id=${registryId}`;

    const amountText = Number(totalAmount || 0).toLocaleString("ru-RU", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });

    const approverText =
      `📌 <b>Реестр на согласовании</b>\n\n` +
      `Реестр №: <b>${registryNo}</b>\n` +
      `Инициатор: <b>${createdBy || "-"}</b>\n` +
      `Этап: <b>${stage}</b>\n` +
      `Сумма: <b>${amountText}</b>`;

    const infoText =
      `📣 <b>Обновление по реестру</b>\n\n` +
      `Реестр №: <b>${registryNo}</b>\n` +
      `Инициатор: <b>${createdBy || "-"}</b>\n` +
      `Текущий этап: <b>${stage}</b>\n` +
      `Сумма: <b>${amountText}</b>`;

    const replyMarkup = {
      inline_keyboard: [
        [
          { text: "✅ Согласовать", callback_data: `approve|${registryId}|${stage}` },
          { text: "❌ Отклонить", callback_data: `reject|${registryId}|${stage}` }
        ],
        [
          { text: "📄 Открыть реестр", url: openUrl }
        ]
      ]
    };

    const viewOnlyMarkup = {
      inline_keyboard: [
        [
          { text: "📄 Открыть реестр", url: openUrl }
        ]
      ]
    };

    // -------------------------------------------------
    // 1. Основной согласующий / исполнитель
    // -------------------------------------------------
    if (approverChatId) {
      await sendTelegramMessage(approverChatId, approverText, replyMarkup);
    } else {
      console.log("❌ chat_id не найден для согласующего:", approverLoginNorm, "stage:", stage);
    }

    // -------------------------------------------------
    // 2. Наблюдатели — НА КАЖДОМ ЭТАПЕ
    // -------------------------------------------------
    const client1 = await pool.connect();
    let watchers = [];

    try {
      watchers = await getWatchersByRegistry(registryId, client1);
    } finally {
      client1.release();
    }

    for (const w of watchers) {
      const watcherLogin = String(w || "").trim().toLowerCase();
      const cid = finalChatMap[watcherLogin];
      if (!cid) continue;

      // чтобы согласующему не дублировать второе сообщение
      if (watcherLogin === approverLoginNorm) continue;

      await sendTelegramMessage(
        cid,
        `👀 <b>Наблюдатель</b>\n\n${infoText}`,
        viewOnlyMarkup
      );
    }

    console.log("watchers notified:", { registryId, stage, watchers });

    // -------------------------------------------------
    // 3. Сотрудники по input_name — НА КАЖДОМ ЭТАПЕ
    // -------------------------------------------------
    const client2 = await pool.connect();
    let employees = [];

    try {
      employees = await getEmployeesByRegistry(registryId, client2);
    } finally {
      client2.release();
    }

    for (const empLoginRaw of employees) {
      const empLogin = String(empLoginRaw || "").trim().toLowerCase();
      const cid = finalChatMap[empLogin];
      if (!cid) continue;

      // чтобы согласующему не дублировать второе сообщение
      if (empLogin === approverLoginNorm) continue;

      await sendTelegramMessage(
        cid,
        `📥 <b>Ваши заявки есть в реестре</b>\n\n` +
        `Реестр №: <b>${registryNo}</b>\n` +
        `Ваш логин: <b>${empLogin}</b>\n` +
        `Инициатор: <b>${createdBy || "-"}</b>\n` +
        `Текущий этап: <b>${stage}</b>\n` +
        `Сумма: <b>${amountText}</b>`,
        viewOnlyMarkup
      );
    }

    console.log("employees notified:", { registryId, stage, employees });

  } catch (e) {
    console.error("telegram send error:", e);
  }
}


//NeW APP
app.post("/update-profile", async (req, res) => {
  try {
    const {
      email,
      first_name,
      last_name,
      middle_name,
      phone
    } = req.body || {};

    const emailNorm = normalizeEmail(email);

    if (!emailNorm) {
      return res.status(400).json({
        success: false,
        message: "Email обязателен"
      });
    }

    const r = await pool.query(`
      UPDATE public.users
      SET
        first_name = COALESCE($1, first_name),
        last_name = COALESCE($2, last_name),
        middle_name = COALESCE($3, middle_name),
        phone = COALESCE($4, phone)
      WHERE lower(trim(email)) = $5
      RETURNING id, email, first_name, last_name, middle_name, phone
    `, [
      first_name ? String(first_name).trim() : null,
      last_name ? String(last_name).trim() : null,
      middle_name ? String(middle_name).trim() : null,
      phone ? String(phone).trim() : null,
      emailNorm
    ]);

    if (!r.rowCount) {
      return res.status(404).json({
        success: false,
        message: "Пользователь не найден"
      });
    }

    res.json({
      success: true,
      user: r.rows[0]
    });

  } catch (e) {
    console.error("UPDATE PROFILE ERROR:", e);
    res.status(500).json({
      success: false,
      message: "Ошибка сервера"
    });
  }
});

app.post("/change-password", async (req, res) => {
  try {
    const { email, old_password, new_password } = req.body || {};

    const emailNorm = normalizeEmail(email);
    const oldPass = String(old_password || "").trim();
    const newPass = String(new_password || "").trim();

    if (!emailNorm || !oldPass || !newPass) {
      return res.status(400).json({
        success: false,
        message: "Заполните все поля"
      });
    }

    if (newPass.length < 6) {
      return res.status(400).json({
        success: false,
        message: "Новый пароль должен быть не менее 6 символов"
      });
    }

    const userRes = await pool.query(`
      SELECT id, password
      FROM public.users
      WHERE lower(trim(email)) = $1
      LIMIT 1
    `, [emailNorm]);

    if (!userRes.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Пользователь не найден"
      });
    }

    const user = userRes.rows[0];

    if (String(user.password || "") !== oldPass) {
      return res.status(400).json({
        success: false,
        message: "Старый пароль неверный"
      });
    }

    await pool.query(`
      UPDATE public.users
      SET password = $1
      WHERE id = $2
    `, [newPass, user.id]);

    res.json({ success: true });

  } catch (e) {
    console.error("CHANGE PASSWORD ERROR:", e);
    res.status(500).json({
      success: false,
      message: "Ошибка сервера"
    });
  }
});

app.post("/approve-rows", async (req, res) => {
  try {
    const { ids, login, request_id } = req.body;

    if (!Array.isArray(ids) || !ids.length) {
      return res.status(400).json({ success: false, error: "ids required" });
    }
    if (!login) {
      return res.status(400).json({ success: false, error: "login required" });
    }
    if (!request_id) {
      return res.status(400).json({ success: false, error: "request_id required" });
    }

    const client = await pool.connect();
    try {
      await client.query("BEGIN");

      const isChief = login === "S_Zhasulan";   // <- ваш логин ГлавБухг
      const isAdmin = login === "B_Erkin";      // <- замените на логин Админа

      if (!isChief && !isAdmin) {
        throw new Error("Нет прав на согласование");
      }

      if (isChief) {
        await client.query(`
          UPDATE zvk
          SET chief_approved = 'Да',
              chief_approved_by = $1,
              chief_approved_at = NOW()
          WHERE id = ANY($2::int[])
            AND request_id = $3
        `, [login, ids, request_id]);
      }

      if (isAdmin) {
        await client.query(`
          UPDATE zvk
          SET admin_approved = 'Да',
              admin_approved_by = $1,
              admin_approved_at = NOW()
          WHERE id = ANY($2::int[])
            AND request_id = $3
            AND chief_approved = 'Да'
        `, [login, ids, request_id]);
      }

      // пересчет шапки заявки
      await client.query(`
        UPDATE requests r
        SET chief_approved_rows = x.chief_cnt,
            admin_approved_rows = x.admin_cnt,
            registry_flag = CASE
              WHEN x.total_cnt > 0 AND x.admin_cnt = x.total_cnt THEN 'Да'
              ELSE 'Нет'
            END,
            agree_status = CASE
              WHEN x.total_cnt > 0 AND x.admin_cnt = x.total_cnt THEN 'Согласовано'
              WHEN x.total_cnt > 0 AND x.chief_cnt = x.total_cnt THEN 'На согласовании у Админа'
              WHEN x.chief_cnt > 0 THEN 'Частично согласовано ГлавБухг'
              ELSE 'На согласовании у ГлавБухг'
            END
        FROM (
          SELECT
            request_id,
            COUNT(*) AS total_cnt,
            COUNT(*) FILTER (WHERE chief_approved = 'Да') AS chief_cnt,
            COUNT(*) FILTER (WHERE admin_approved = 'Да') AS admin_cnt
          FROM zvk
          WHERE request_id = $1
          GROUP BY request_id
        ) x
        WHERE r.id = x.request_id
      `, [request_id]);

      await client.query("COMMIT");
      res.json({ success: true });

    } catch (e) {
      await client.query("ROLLBACK");
      res.status(500).json({ success: false, error: e.message });
    } finally {
      client.release();
    }

  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});


// =====================================================
// Start
// =====================================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT))