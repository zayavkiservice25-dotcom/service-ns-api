require("dotenv").config();

const express = require("express");
const cors = require("cors");
const path = require("path");
const axios = require("axios");
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
  ALTER TABLE public.zvk_status
  ADD COLUMN IF NOT EXISTS status_comment text;
`);

await pool.query(`
  ALTER TABLE public.zvk_status
  ADD COLUMN IF NOT EXISTS chief_approved text;
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

await pool.query(`
  CREATE TABLE IF NOT EXISTS public.registry_stage_approvals (
    id bigserial PRIMARY KEY,
    registry_id bigint NOT NULL,
    stage_name text NOT NULL,
    approver_login text NOT NULL,
    approver_name text,
    status text DEFAULT 'Ожидает',
    action_time timestamptz,
    comment_text text
  );
`);

await pool.query(`
  CREATE UNIQUE INDEX IF NOT EXISTS registry_stage_approvals_uq
  ON public.registry_stage_approvals (registry_id, stage_name, approver_login);
`);

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
s.chief_approved,

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
    is_active boolean DEFAULT true,
    created_at timestamptz DEFAULT now()
  );
`);
await pool.query(`
  ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS chat_id text;
`);

await pool.query(`
  ALTER TABLE public.users 
  ADD COLUMN IF NOT EXISTS login text;
`);

await pool.query(`
  ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS role_ft text;
`);

await pool.query(`
  ALTER TABLE public.users
  ADD COLUMN IF NOT EXISTS role_hr text;
`);

await pool.query(`
  CREATE UNIQUE INDEX IF NOT EXISTS users_login_idx 
  ON public.users (lower(trim(login)));
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
  ADD COLUMN IF NOT EXISTS pay_account text;
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
  CREATE TABLE IF NOT EXISTS public.registry_transfers (
    id bigserial PRIMARY KEY,
    registry_id bigint NOT NULL,
    src_object text,
    acc_from text,
    dds_from text,
    acc_to text,
    dds_to text,
    sum numeric(18,2) DEFAULT 0
  );
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS registry_transfers_registry_id_idx
  ON public.registry_transfers (registry_id);
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
  ALTER TABLE public.request_head
  ADD COLUMN IF NOT EXISTS chat_map jsonb;
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


await pool.query(`
  CREATE TABLE IF NOT EXISTS public.user_role_history (
    id bigserial PRIMARY KEY,
    login text NOT NULL,
    old_role_ft text,
    new_role_ft text,
    old_role_hr text,
    new_role_hr text,
    changed_at timestamptz DEFAULT now()
  );
`);

await pool.query(`
  CREATE TABLE IF NOT EXISTS public.notifications (
    id bigserial PRIMARY KEY,
    user_login text NOT NULL,
    type text NOT NULL,              -- request / registry
    title text NOT NULL,
    message text,
    entity_id bigint,
    entity_page text,                -- request_card / registry_card
    is_read boolean DEFAULT false,
    created_at timestamptz DEFAULT now()
  );
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS notifications_user_login_idx
  ON public.notifications (lower(trim(user_login)), is_read, created_at DESC);
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS user_role_history_login_idx
  ON public.user_role_history (lower(trim(login)));
`);
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
      login,
      password,
      phone,
      last_name,
      first_name,
      middle_name
    } = req.body || {};

    const emailNorm = normalizeEmail(email);
    const loginNorm = String(login || "").trim().toLowerCase();
    const pass = String(password || "").trim();

    if (!emailNorm) {
      return res.status(400).json({ success:false, message:"Почта обязательна" });
    }

    if (!loginNorm) {
      return res.status(400).json({ success:false, message:"Логин обязателен" });
    }

    if (!pass) {
      return res.status(400).json({ success:false, message:"Пароль обязателен" });
    }

    if (!first_name || !last_name) {
      return res.status(400).json({ success:false, message:"Имя и фамилия обязательны" });
    }

    const emailExists = await pool.query(
      `SELECT id FROM public.users WHERE lower(trim(email)) = $1 LIMIT 1`,
      [emailNorm]
    );

    if (emailExists.rowCount > 0) {
      return res.status(400).json({
        success:false,
        message:"Пользователь с такой почтой уже существует"
      });
    }

    const loginExists = await pool.query(
      `SELECT id FROM public.users WHERE lower(trim(login)) = $1 LIMIT 1`,
      [loginNorm]
    );

    if (loginExists.rowCount > 0) {
      return res.status(400).json({
        success:false,
        message:"Пользователь с таким логином уже существует"
      });
    }

const r = await pool.query(`
  INSERT INTO public.users (
    email,
    login,
    password,
    phone,
    last_name,
    first_name,
    middle_name,
    role_ft,
    role_hr,
    is_active
  )
  VALUES ($1,$2,$3,$4,$5,$6,$7,NULL,NULL,true)
  RETURNING id, email, login, role_ft, role_hr, first_name, last_name
`, [
      emailNorm,
      loginNorm,
      pass,
      phone ? String(phone).trim() : null,
      last_name ? String(last_name).trim() : null,
      first_name ? String(first_name).trim() : null,
      middle_name ? String(middle_name).trim() : null
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
    const { login, password } = req.body || {};

    if (!login || !password) {
      return res.status(400).json({
        success: false,
        message: "Логин и пароль обязательны"
      });
    }

    const q = `
      SELECT *
      FROM users
      WHERE lower(trim(login)) = lower(trim($1))
        AND is_active = true
      LIMIT 1
    `;

    const r = await pool.query(q, [login]);

    if (!r.rows.length) {
      return res.json({
        success: false,
        message: "Пользователь не найден"
      });
    }

    const user = r.rows[0];

    if (user.password !== password) {
      return res.json({
        success: false,
        message: "Неверный пароль"
      });
    }

return res.json({
  success: true,
  user: {
    login: user.login,
    email: user.email,
    role_ft: user.role_ft,
    role_hr: user.role_hr,
    first_name: user.first_name,
    last_name: user.last_name
  }
});

  } catch (e) {
    console.error("LOGIN ERROR:", e);
    res.status(500).json({
      success: false,
      message: "Ошибка сервера"
    });
  }
});
app.get("/profile", async (req, res) => {
  try {
    const login = String(req.query.login || "").trim();

    if (!login) {
      return res.status(400).json({
        success: false,
        message: "Логин не передан"
      });
    }

    const q = await pool.query(`
      SELECT
        id,
        login,
        email,
        phone,
        role_ft,
        role_hr,
        first_name,
        last_name,
        middle_name,
        is_active
      FROM public.users
      WHERE lower(trim(login)) = lower(trim($1))
      LIMIT 1
    `, [login]);

    if (!q.rows.length) {
      return res.status(404).json({
        success: false,
        message: "Пользователь не найден"
      });
    }

    return res.json({
      success: true,
      user: q.rows[0]
    });

  } catch (e) {
    console.error("PROFILE ERROR:", e);
    return res.status(500).json({
      success: false,
      message: "Ошибка сервера"
    });
  }
});


app.get("/employees", async (req, res) => {
  try {
    const r = await pool.query(`
SELECT
  id,
  email,
  phone,
  last_name,
  first_name,
  middle_name,
  organization_name,
  role_ft,
  role_hr,
  is_active,
  created_at,
  login
FROM users
ORDER BY id ASC;
    `);

    return res.json({
      success: true,
      rows: r.rows
    });

  } catch (e) {
    console.error("EMPLOYEES ERROR:", e);
    return res.status(500).json({
      success: false,
      message: "Ошибка сервера"
    });
  }
});

app.post("/update-user-roles", async (req, res) => {
  const client = await pool.connect();

  try {
    const { login, role_ft, role_hr, actor_login } = req.body || {};

    const loginNorm = String(login || "").trim();
    const actorLoginNorm = String(actor_login || "").trim();

    const newRoleFt = String(role_ft || "").trim().toLowerCase();
    const newRoleHr = String(role_hr || "").trim().toLowerCase();

    const allowedRoles = ["initiator", "operator", "supervisor", "admin"];

    if (!loginNorm) {
      return res.status(400).json({
        success: false,
        message: "Не передан login"
      });
    }

    if (!actorLoginNorm) {
      return res.status(400).json({
        success: false,
        message: "Не передан actor_login"
      });
    }

    if (!allowedRoles.includes(newRoleFt) || !allowedRoles.includes(newRoleHr)) {
      return res.status(400).json({
        success: false,
        message: "Недопустимая роль"
      });
    }

    await client.query("BEGIN");

    // 1. кто делает изменение
    const actorRes = await client.query(
      `
      SELECT id, login, role_ft, role_hr
      FROM public.users
      WHERE lower(trim(login)) = lower(trim($1))
      LIMIT 1
      `,
      [actorLoginNorm]
    );

    if (!actorRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({
        success: false,
        message: "Текущий пользователь не найден"
      });
    }

const actor = actorRes.rows[0];

// только логин admin может менять роли
const isMainAdmin = String(actor.login || "").trim().toLowerCase() === "admin";

if (!isMainAdmin) {
  await client.query("ROLLBACK");
  return res.status(403).json({
    success: false,
    message: "Только пользователь admin может изменять роли"
  });
}
    // 3. кого меняем
    const userRes = await client.query(
      `
      SELECT id, login, role_ft, role_hr
      FROM public.users
      WHERE lower(trim(login)) = lower(trim($1))
      LIMIT 1
      `,
      [loginNorm]
    );

    if (!userRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({
        success: false,
        message: "Пользователь не найден"
      });
    }

    const user = userRes.rows[0];
    const oldRoleFt = String(user.role_ft || "").trim().toLowerCase();
    const oldRoleHr = String(user.role_hr || "").trim().toLowerCase();

    if (oldRoleFt === newRoleFt && oldRoleHr === newRoleHr) {
      await client.query("ROLLBACK");
      return res.json({
        success: true,
        message: "Изменений нет",
        row: {
          id: user.id,
          login: user.login,
          role_ft: user.role_ft,
          role_hr: user.role_hr
        }
      });
    }

    const updRes = await client.query(
      `
      UPDATE public.users
      SET role_ft = $1,
          role_hr = $2
      WHERE lower(trim(login)) = lower(trim($3))
      RETURNING id, login, role_ft, role_hr
      `,
      [newRoleFt, newRoleHr, loginNorm]
    );

    await client.query(
      `
      INSERT INTO public.user_role_history
      (
        login,
        old_role_ft,
        new_role_ft,
        old_role_hr,
        new_role_hr,
        changed_at
      )
      VALUES ($1, $2, $3, $4, $5, NOW())
      `,
      [
        user.login,
        oldRoleFt || null,
        newRoleFt,
        oldRoleHr || null,
        newRoleHr
      ]
    );

    await client.query("COMMIT");

    return res.json({
      success: true,
      message: "Роли успешно обновлены",
      row: updRes.rows[0]
    });
  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}

    console.error("update-user-roles error:", e);

    return res.status(500).json({
      success: false,
      message: "Ошибка сервера"
    });
  } finally {
    client.release();
  }
});


app.get("/user-role-history", async (req, res) => {
  try {
    const login = String(req.query.login || "").trim();

    if (!login) {
      return res.status(400).json({
        success: false,
        message: "login required"
      });
    }

    const r = await pool.query(
      `
      SELECT
        id,
        login,
        old_role_ft,
        new_role_ft,
        old_role_hr,
        new_role_hr,
        changed_at
      FROM public.user_role_history
      WHERE lower(trim(login)) = lower(trim($1))
      ORDER BY changed_at DESC, id DESC
      `,
      [login]
    );

    return res.json({
      success: true,
      rows: r.rows
    });
  } catch (e) {
    console.error("USER-ROLE-HISTORY ERROR:", e);
    return res.status(500).json({
      success: false,
      message: "Ошибка сервера"
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
    const { row_ids, login, pdf_url, chat_map } = req.body;
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
        (created_by, pdf_url, chat_map)
      VALUES ($1, $2, $3::jsonb)
      RETURNING id, request_no
    `, [
      String(login || "").trim(),
      pdf_url ? String(pdf_url).trim() : null,
      JSON.stringify(chat_map || {})
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
        acc_zam_name = 'B_Erkin',
        acc_zam_status = 'Ожидает'
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

    try {
      await notifyRequestCreatedToInitiator({
        requestId: request_id,
        requestNo: request_no,
        createdBy: String(login || "").trim(),
        totalAmount: total,
        chatMap: chat_map || {}
      });
    } catch (e) {
      console.error("notify initiator created error:", e);
    }

    try {
      await sendRequestTelegramNotification({
        requestId: request_id,
        requestNo: request_no,
        stage: "Главный бухгалтер",
        totalAmount: total,
        createdBy: String(login || "").trim()
      });
    } catch (tgErr) {
      console.error("request telegram notify error:", tgErr);
    }

    try {
      await createNotification({
        userLogin: String(login || "").trim(),
        type: "request",
        title: `Заявка №${request_no} создана`,
        message: `Сумма: ${Number(total || 0).toLocaleString("ru-RU")} ₸`,
        entityId: request_id,
        entityPage: "request_card"
      });
    } catch (e) {
      console.error("create notification request created error:", e);
    }

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
    const q = `
      SELECT
        h.id,
        h.request_no,
        h.request_date,
        h.created_by,
        h.total_amount,
        h.items_count,
        h.workflow_stage,
        h.agree_status,

        COALESCE(x.total_rows, 0) AS total_rows,
        COALESCE(x.chief_approved_rows, 0) AS chief_approved_rows,

        CASE
          WHEN COALESCE(h.acc_zam_status, '') = 'Согласовано'
          THEN COALESCE(x.total_rows, 0)
          ELSE 0
        END AS admin_approved_rows,

        COALESCE(x.approved_zfts_chief, '') AS approved_zfts_chief,

        CASE
          WHEN COALESCE(h.acc_zam_status, '') = 'Согласовано'
          THEN 'Да'
          ELSE ''
        END AS approved_zfts_admin

      FROM public.request_head h

      LEFT JOIN (
        SELECT
          i.request_id,
          COUNT(*) AS total_rows,

          COUNT(*) FILTER (
            WHERE COALESCE(s.chief_approved,'') = 'Да'
          ) AS chief_approved_rows,

          STRING_AGG(
            CASE
              WHEN COALESCE(s.chief_approved,'') = 'Да'
              THEN COALESCE(i.id_zvk::text, i.zvk_row_id::text)
            END,
            ', '
          ) AS approved_zfts_chief

        FROM public.request_items i
        LEFT JOIN public.zvk_status s
          ON s.zvk_row_id = i.zvk_row_id

        GROUP BY i.request_id
      ) x ON x.request_id = h.id

      ORDER BY h.request_date DESC, h.id DESC
    `;

    const { rows } = await pool.query(q);

    res.json({ success: true, rows });

  } catch (e) {
    console.error("request-list error:", e);
    res.status(500).json({ success: false, error: e.message });
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
        COALESCE(s.chief_approved, '') AS chief_approved,
        CASE
          WHEN COALESCE(h.acc_zam_status, '') = 'Согласовано' THEN 'Да'
          ELSE ''
        END AS admin_approved
      FROM public.request_items i
      LEFT JOIN public.zvk_status s
        ON s.zvk_row_id = i.zvk_row_id
      LEFT JOIN public.request_head h
        ON h.id = i.request_id
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

    const actor = String(login || "").trim().toLowerCase();
    const actionName = String(action || "").trim();

    await client.query("BEGIN");

    const reqRes = await client.query(
      `SELECT * FROM public.request_head WHERE id = $1 LIMIT 1`,
      [Number(request_id)]
    );

    if (!reqRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ success:false, error:"request not found" });
    }

    const reqHead = reqRes.rows[0];

    if (actor === "s_zhasulan") {
      if (
        String(reqHead.acc_buh_status || "").trim() === "Согласовано" ||
        String(reqHead.acc_buh_status || "").trim() === "Отклонено"
      ) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          success: false,
          error: "Главбух уже принял решение"
        });
      }
    }

    if (actor === "b_erkin") {
      if (
        String(reqHead.acc_zam_status || "").trim() === "Согласовано" ||
        String(reqHead.acc_zam_status || "").trim() === "Отклонено"
      ) {
        await client.query("ROLLBACK");
        return res.status(400).json({
          success: false,
          error: "Админ уже принял решение"
        });
      }
    }

    if (actionName === "reject") {
      await client.query(`
        UPDATE public.request_head
        SET
          acc_buh_status = CASE
            WHEN $2 = 's_zhasulan' THEN 'Отклонено'
            ELSE acc_buh_status
          END,
          acc_zam_status = CASE
            WHEN $2 = 'b_erkin' THEN 'Отклонено'
            ELSE acc_zam_status
          END,
          workflow_stage = 'Инициация',
          agree_status = 'Отклонено'
        WHERE id = $1
      `, [Number(request_id), actor]);

      await client.query(`
        INSERT INTO public.request_approve_log
          (request_id, stage_name, approver_login, approver_name, action_type, comment_text)
        VALUES ($1,$2,$3,$4,'reject',$5)
      `, [
        Number(request_id),
        String(stage || ""),
        String(login || ""),
        String(name || ""),
        String(comment || "")
      ]);

      await client.query("COMMIT");

      try {
        await createNotification({
          userLogin: reqHead.created_by,
          type: "request",
          title: `Заявка №${reqHead.request_no} отклонена`,
          message: String(comment || "Заявка была отклонена"),
          entityId: Number(request_id),
          entityPage: "request_card"
        });
      } catch (e) {
        console.error("create notification request reject error:", e);
      }

      return res.json({ success:true, action:"reject" });
    }

    if (actor === "s_zhasulan" && actionName === "approve") {
      const itemRes = await client.query(`
        SELECT zvk_row_id
        FROM public.request_items
        WHERE request_id = $1
      `, [Number(request_id)]);

      const ids = itemRes.rows.map(x => Number(x.zvk_row_id)).filter(Boolean);

      if (ids.length) {
        await client.query(`
          INSERT INTO public.zvk_status (zvk_row_id, chief_approved, status_time)
          SELECT x, 'Да', NOW()
          FROM unnest($1::bigint[]) AS x
          ON CONFLICT (zvk_row_id)
          DO UPDATE SET
            chief_approved = 'Да',
            status_time = NOW()
        `, [ids]);
      }

      await client.query(`
        UPDATE public.request_head
        SET
          acc_buh_name = $2,
          acc_buh_status = 'Согласовано',
          acc_buh_time = NOW(),
          acc_buh_comment = $3,
          workflow_stage = 'Админ',
          agree_status = 'На согласовании'
        WHERE id = $1
      `, [
        Number(request_id),
        String(name || "Жасулан Сулейменов"),
        String(comment || "")
      ]);

      await client.query(`
        INSERT INTO public.request_approve_log
          (request_id, stage_name, approver_login, approver_name, action_type, comment_text)
        VALUES ($1,$2,$3,$4,'approve',$5)
      `, [
        Number(request_id),
        "Главный бухгалтер",
        String(login || ""),
        String(name || ""),
        String(comment || "")
      ]);

      await client.query("COMMIT");

      try {
        await notifyRequestApprovedToInitiator({
          requestNo: reqHead.request_no,
          createdBy: reqHead.created_by,
          chatMap: reqHead.chat_map || {},
          stage: "Главный бухгалтер"
        });
      } catch (e) {
        console.error("notify chief approve error:", e);
      }

      try {
        await createNotification({
          userLogin: reqHead.created_by,
          type: "request",
          title: `ГлавБухг согласовал заявку №${reqHead.request_no}`,
          message: "Заявка переведена на этап Админ",
          entityId: Number(request_id),
          entityPage: "request_card"
        });
      } catch (e) {
        console.error("create notification chief approve error:", e);
      }

      try {
        await sendRequestTelegramNotification({
          requestId: Number(request_id),
          requestNo: reqHead.request_no,
          stage: "Админ",
          totalAmount: Number(reqHead.total_amount || 0),
          createdBy: String(reqHead.created_by || "").trim()
        });
      } catch (tgErr) {
        console.error("request telegram notify admin error:", tgErr);
      }

      return res.json({
        success:true,
        stage:"Главбух",
        moved_to:"Админ"
      });
    }

    if (actor === "b_erkin" && actionName === "approve") {
      await client.query(`
        UPDATE public.request_head
        SET
          acc_zam_status = 'Согласовано',
          acc_zam_time = NOW(),
          acc_zam_comment = $2,
          workflow_stage = 'Согласовано',
          agree_status = 'Согласовано'
        WHERE id = $1
      `, [
        Number(request_id),
        String(comment || "")
      ]);

      const itemRes = await client.query(`
        SELECT zvk_row_id
        FROM public.request_items
        WHERE request_id = $1
      `, [Number(request_id)]);

      const ids = itemRes.rows.map(x => Number(x.zvk_row_id)).filter(Boolean);

      if (ids.length) {
        await client.query(`
          INSERT INTO public.zvk_pay (zvk_row_id, registry_flag, agree_time)
          SELECT x, 'Да', NOW()
          FROM unnest($1::bigint[]) AS x
          ON CONFLICT (zvk_row_id)
          DO UPDATE SET
            registry_flag = 'Да',
            agree_time = NOW()
        `, [ids]);
      }

      await client.query(`
        INSERT INTO public.request_approve_log
          (request_id, stage_name, approver_login, approver_name, action_type, comment_text)
        VALUES ($1,$2,$3,$4,'approve',$5)
      `, [
        Number(request_id),
        "Админ",
        String(login || ""),
        String(name || ""),
        String(comment || "")
      ]);

      await client.query("COMMIT");

      try {
        await notifyRequestApprovedToInitiator({
          requestNo: reqHead.request_no,
          createdBy: reqHead.created_by,
          chatMap: reqHead.chat_map || {},
          stage: "Админ"
        });
      } catch (e) {
        console.error("notify final approve error:", e);
      }

      try {
        await createNotification({
          userLogin: reqHead.created_by,
          type: "request",
          title: `Админ утвердил заявку №${reqHead.request_no}`,
          message: "Заявка полностью согласована",
          entityId: Number(request_id),
          entityPage: "request_card"
        });
      } catch (e) {
        console.error("create notification admin approve error:", e);
      }

      return res.json({
        success:true,
        stage:"Админ",
        final:true
      });
    }

    await client.query("ROLLBACK");

    return res.status(403).json({
      success:false,
      error:"NO_RIGHTS"
    });

  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}

    console.error("REQUEST-APPROVE ERROR:", e);

    return res.status(500).json({
      success:false,
      error:e.message
    });
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
    const { zvk_row_id, src_d, src_o, status_comment, login, is_admin, can_edit_all, is_all } = req.body;

    if (!zvk_row_id)
      return res.status(400).json({ success:false, error:"zvk_row_id required" });

    const rid = Number(zvk_row_id);
    if (Number.isNaN(rid))
      return res.status(400).json({ success:false, error:"zvk_row_id must be number" });

    const actor = String(login || "").trim();
    if (!actor) return res.status(400).json({ success:false, error:"login required" });

    const adminOk = isTruthy(is_admin) || isTruthy(can_edit_all) || String(is_all || "0") === "1";

    if (!adminOk) {
      const ok = await canEditRowByLogin(pool, rid, actor);
      if (!ok) return res.status(403).json({ success:false, error:"NO_RIGHTS_THIS_ROW" });
    }

    const hasStatusComment = Object.prototype.hasOwnProperty.call(req.body, "status_comment");

    const r = await pool.query(
      `
      INSERT INTO zvk_status (zvk_row_id, status_time, src_d, src_o, status_comment)
      VALUES ($1, NOW(), $2, $3, $4)
      ON CONFLICT (zvk_row_id)
      DO UPDATE SET
        status_time = NOW(),
        src_d = COALESCE(EXCLUDED.src_d, zvk_status.src_d),
        src_o = COALESCE(EXCLUDED.src_o, zvk_status.src_o),
        status_comment = CASE
          WHEN $5 THEN EXCLUDED.status_comment
          ELSE zvk_status.status_comment
        END
      RETURNING *
      `,
      [
        rid,
        src_d ?? null,
        src_o ?? null,
        hasStatusComment ? String(status_comment || "") : null,
        hasStatusComment
      ]
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
    const q = await pool.query(`
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

    return res.json({
      ok: true,
      rows: q.rows
    });
  } catch (e) {
    console.error("SVOD OBJECT ERROR:", e);
    return res.status(500).json({
      ok: false,
      error: e.message
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
// 3️⃣ определяем дивизион из строк реестра
let divisionText = null;

if (!isEmptyRegistry) {
  const divRes = await client.query(`
    SELECT STRING_AGG(DISTINCT NULLIF(TRIM(src_d), ''), ', ') AS division_text
    FROM public.registry_items
    WHERE registry_id = $1
  `, [registry_id]);

  divisionText = String(divRes.rows[0]?.division_text || "").trim() || null;
}

// 4️⃣ обновляем шапку
await client.query(`
  UPDATE public.registry_head
  SET
    total_amount = $1,
    items_count = $2,
    division = $3,
    workflow_stage = 'Черновик',
    agree_status = 'Черновик'
  WHERE id = $4
`, [total, count, divisionText, registry_id]);

    await client.query("COMMIT");



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
  "k_marat",
  "v_shevchenko",
  "k_ermek",
  "k_arailym",
  "zh_elena",
  "s_zhasulan",
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

    const watcherDivisions = getWatcherDivisions(login);

    // 1) Согласующие видят всё
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
          h.division,
          h.pay_account,
          COALESCE(
            STRING_AGG(DISTINCT NULLIF(TRIM(i.src_d), ''), ', ')
              FILTER (WHERE NULLIF(TRIM(i.src_d), '') IS NOT NULL),
            ''
          ) AS src_d
        FROM public.registry_head h
        LEFT JOIN public.registry_items i
          ON i.registry_id = h.id
        WHERE COALESCE(h.archive_flag, 'Нет') <> 'Да'
          AND COALESCE(h.workflow_stage, '') <> 'Черновик'
        GROUP BY
          h.id,
          h.registry_no,
          h.registry_date,
          h.created_by,
          h.items_count,
          h.total_amount,
          h.workflow_stage,
          h.archive_flag,
          h.division,
          h.pay_account
        ORDER BY h.id DESC
      `);

      return res.json({ success:true, rows:r.rows });
    }

    // 2) Наблюдатели видят только свои дивизионы
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
          h.division,
          h.pay_account,
          COALESCE(
            STRING_AGG(DISTINCT NULLIF(TRIM(i.src_d), ''), ', ')
              FILTER (WHERE NULLIF(TRIM(i.src_d), '') IS NOT NULL),
            ''
          ) AS src_d
        FROM public.registry_head h
        LEFT JOIN public.registry_items i
          ON i.registry_id = h.id
        WHERE COALESCE(h.archive_flag, 'Нет') <> 'Да'
          AND COALESCE(h.workflow_stage, '') <> 'Черновик'
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
          h.archive_flag,
          h.division,
          h.pay_account
        ORDER BY h.id DESC
      `, [watcherDivisions]);

      return res.json({ success:true, rows:r.rows });
    }

    // 3) Создатель реестра видит свои
    const createdRes = await pool.query(`
      SELECT
        h.id,
        h.registry_no,
        h.registry_date,
        h.created_by,
        h.items_count,
        h.total_amount,
        h.workflow_stage,
        h.archive_flag,
        h.division,
        h.pay_account,
        COALESCE(
          STRING_AGG(DISTINCT NULLIF(TRIM(i.src_d), ''), ', ')
            FILTER (WHERE NULLIF(TRIM(i.src_d), '') IS NOT NULL),
          ''
        ) AS src_d
      FROM public.registry_head h
      LEFT JOIN public.registry_items i
        ON i.registry_id = h.id
      WHERE COALESCE(h.archive_flag, 'Нет') <> 'Да'
        AND COALESCE(h.workflow_stage, '') <> 'Черновик'
        AND lower(trim(COALESCE(h.created_by,''))) = lower(trim($1))
      GROUP BY
        h.id,
        h.registry_no,
        h.registry_date,
        h.created_by,
        h.items_count,
        h.total_amount,
        h.workflow_stage,
        h.archive_flag,
        h.division,
        h.pay_account
      ORDER BY h.id DESC
    `, [login]);

    if (createdRes.rowCount > 0) {
      return res.json({ success:true, rows: createdRes.rows });
    }

    // 4) Сотрудник видит только те реестры, где его login есть в input_name
    const employeeRes = await pool.query(`
      SELECT
        h.id,
        h.registry_no,
        h.registry_date,
        h.created_by,
        h.items_count,
        h.total_amount,
        h.workflow_stage,
        h.archive_flag,
        h.division,
        h.pay_account,
        COALESCE(
          STRING_AGG(DISTINCT NULLIF(TRIM(i.src_d), ''), ', ')
            FILTER (WHERE NULLIF(TRIM(i.src_d), '') IS NOT NULL),
          ''
        ) AS src_d
      FROM public.registry_head h
      LEFT JOIN public.registry_items i
        ON i.registry_id = h.id
      WHERE COALESCE(h.archive_flag, 'Нет') <> 'Да'
        AND COALESCE(h.workflow_stage, '') <> 'Черновик'
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
        h.archive_flag,
        h.division,
        h.pay_account
      ORDER BY h.id DESC
    `, [login]);

    if (employeeRes.rowCount > 0) {
      return res.json({ success:true, rows: employeeRes.rows });
    }

    // 5) Остальным нельзя
    return res.status(403).json({
      success:false,
      error:"NO_ACCESS_TO_REGISTRY"
    });

  } catch (e) {
    console.error("REGISTRY-LIST ERROR:", e);
    res.status(500).json({
      success:false,
      error:e.message
    });
  }
});

app.get("/registry-card", async (req, res) => {
  try {
    console.log("REGISTRY-CARD query =", req.query);

    const rawId = String(req.query.id || "").trim();
    const id = Number(rawId);

    if (!rawId || !Number.isFinite(id) || id <= 0) {
      return res.status(400).json({
        success: false,
        error: "id required",
        got: req.query.id || null
      });
    }

    const transfersRes = await pool.query(`
      SELECT
        id,
        registry_id,
        src_object,
        acc_from,
        dds_from,
        acc_to,
        dds_to,
        sum
      FROM public.registry_transfers
      WHERE registry_id = $1
      ORDER BY id
    `, [id]);

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
        pay_account
      FROM public.registry_head
      WHERE id = $1
      LIMIT 1
    `, [id]);

    if (!headRes.rows.length) {
      return res.status(404).json({
        success: false,
        error: "registry not found"
      });
    }

    const head = headRes.rows[0];

    if (!String(head.division || "").trim()) {
      const divRes = await pool.query(`
        SELECT STRING_AGG(DISTINCT NULLIF(TRIM(src_d), ''), ', ') AS division_text
        FROM public.registry_items
        WHERE registry_id = $1
      `, [id]);

      head.division = String(divRes.rows[0]?.division_text || "").trim() || "";
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
      FROM public.registry_items
      WHERE registry_id = $1
      ORDER BY id
    `, [id]);
const approvalsRes = await pool.query(`
  SELECT
    id,
    registry_id,
    stage_name,
    approver_login,
    approver_name,
    status,
    action_time,
    comment_text
  FROM public.registry_stage_approvals
  WHERE registry_id = $1
  ORDER BY id
`, [id]);


return res.json({
  success: true,
  head,
  items: itemsRes.rows,
  transfers: transfersRes.rows,
  approvals: approvalsRes.rows
});

  } catch (e) {
    console.error("REGISTRY CARD ERROR:", e);
    return res.status(500).json({
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

    if (actionName === "reject") {
      await client.query(`
        UPDATE public.registry_head
        SET
          workflow_stage = 'Черновик',
          agree_status = 'Отклонено'
        WHERE id = $1
      `, [Number(registry_id)]);

      await client.query(`
        UPDATE public.registry_stage_approvals
        SET
          status = 'Отклонено',
          action_time = NOW(),
          comment_text = $4
        WHERE registry_id = $1
          AND stage_name = $2
          AND lower(trim(approver_login)) = lower(trim($3))
      `, [
        Number(registry_id),
        stageName,
        String(login || ""),
        String(comment || "")
      ]);

      await client.query(`
        INSERT INTO public.registry_approve_log
          (registry_id, stage_name, approver_login, approver_name, action_type, comment_text)
        VALUES ($1, $2, $3, $4, 'reject', $5)
      `, [
        Number(registry_id),
        stageName,
        String(login || ""),
        String(name || ""),
        String(comment || "")
      ]);

      await client.query("COMMIT");
      return res.json({ success:true, moved_to:"Черновик", action:"reject" });
    }

    if (actionName === "approve") {
      if (stageName === "Согласование") {
        await client.query(`
          UPDATE public.registry_stage_approvals
          SET
            status = 'Согласовано',
            action_time = NOW(),
            comment_text = $4
          WHERE registry_id = $1
            AND stage_name = $2
            AND lower(trim(approver_login)) = lower(trim($3))
        `, [
          Number(registry_id),
          "Согласование",
          String(login || ""),
          String(comment || "")
        ]);

        const chk = await client.query(`
          SELECT
            COUNT(*) AS total_cnt,
            COUNT(*) FILTER (WHERE status = 'Согласовано') AS ok_cnt
          FROM public.registry_stage_approvals
          WHERE registry_id = $1
            AND stage_name = 'Согласование'
        `, [Number(registry_id)]);

        const totalCnt = Number(chk.rows[0]?.total_cnt || 0);
        const okCnt = Number(chk.rows[0]?.ok_cnt || 0);

        let nextStage = "Согласование";

        if (totalCnt > 0 && totalCnt === okCnt) {
          nextStage = "Утверждение";

          await client.query(`
            UPDATE public.registry_head
            SET
              workflow_stage = 'Утверждение',
              agree_status = 'На согласовании'
            WHERE id = $1
          `, [Number(registry_id)]);

          const approvers = getApproverByStage("Утверждение");
          for (const a of approvers) {
            await client.query(`
              INSERT INTO public.registry_stage_approvals
                (registry_id, stage_name, approver_login, approver_name, status)
              VALUES ($1, $2, $3, $4, 'Ожидает')
              ON CONFLICT (registry_id, stage_name, approver_login)
              DO NOTHING
            `, [
              Number(registry_id),
              "Утверждение",
              String(a.login || "").trim(),
              String(a.name || "").trim()
            ]);
          }
        }

        await client.query(`
          INSERT INTO public.registry_approve_log
            (registry_id, stage_name, approver_login, approver_name, action_type, comment_text)
          VALUES ($1, $2, $3, $4, 'approve', $5)
        `, [
          Number(registry_id),
          stageName,
          String(login || ""),
          String(name || ""),
          String(comment || "")
        ]);

        await client.query("COMMIT");
        return res.json({ success:true, moved_to: nextStage, action:"approve" });
      }

      if (stageName === "Утверждение") {
        await client.query(`
          UPDATE public.registry_stage_approvals
          SET
            status = 'Согласовано',
            action_time = NOW(),
            comment_text = $4
          WHERE registry_id = $1
            AND stage_name = $2
            AND lower(trim(approver_login)) = lower(trim($3))
        `, [
          Number(registry_id),
          "Утверждение",
          String(login || ""),
          String(comment || "")
        ]);

        await client.query(`
          UPDATE public.registry_head
          SET
            workflow_stage = 'Исполнение платежей',
            agree_status = 'Согласовано'
          WHERE id = $1
        `, [Number(registry_id)]);

        await client.query(`
          INSERT INTO public.registry_approve_log
            (registry_id, stage_name, approver_login, approver_name, action_type, comment_text)
          VALUES ($1, $2, $3, $4, 'approve', $5)
        `, [
          Number(registry_id),
          stageName,
          String(login || ""),
          String(name || ""),
          String(comment || "")
        ]);

        await client.query("COMMIT");
        return res.json({ success:true, moved_to:"Исполнение платежей", action:"approve" });
      }

      if (stageName === "Исполнение платежей") {
        await client.query(`
          UPDATE public.registry_head
          SET
            execution_status = 'На исполнении',
            workflow_stage = 'Контроль и архивирование',
            agree_status = 'Согласовано'
          WHERE id = $1
        `, [Number(registry_id)]);

        await client.query(`
          INSERT INTO public.registry_approve_log
            (registry_id, stage_name, approver_login, approver_name, action_type, comment_text)
          VALUES ($1, $2, $3, $4, 'approve', $5)
        `, [
          Number(registry_id),
          stageName,
          String(login || ""),
          String(name || ""),
          String(comment || "")
        ]);

        await client.query("COMMIT");
        return res.json({ success:true, moved_to:"Контроль и архивирование", action:"approve" });
      }

      if (stageName === "Контроль и архивирование") {
        await client.query(`
          UPDATE public.registry_head
          SET
            archive_flag = 'Да',
            execution_status = 'Исполнено'
          WHERE id = $1
        `, [Number(registry_id)]);

        await client.query(`
          INSERT INTO public.registry_approve_log
            (registry_id, stage_name, approver_login, approver_name, action_type, comment_text)
          VALUES ($1, $2, $3, $4, 'archive', $5)
        `, [
          Number(registry_id),
          stageName,
          String(login || ""),
          String(name || ""),
          String(comment || "")
        ]);

        await client.query("COMMIT");
        return res.json({ success:true, moved_to:"Архив", action:"approve" });
      }
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

const allowed = [
  "k_marat",
  "v_shevchenko",
  "k_ermek",
  "k_arailym",
  "zh_elena",
  "s_zhasulan",
  "b_erkin",
  "b_erkin2"
].includes(actor);

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

async function sendTelegramMessage(chatId, text, replyMarkup = null) {
  try {
    const body = {
      chat_id: String(chatId || "").trim(),
      text: String(text || ""),
      parse_mode: "HTML"
    };

    if (replyMarkup) {
      body.reply_markup = replyMarkup;
    }

    const resp = await fetch(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body)
      }
    );

    const result = await resp.json().catch(() => ({}));

    if (!resp.ok || result.ok === false) {
      console.error("sendTelegramMessage error:", result);
    }

    return result;
  } catch (e) {
    console.error("sendTelegramMessage fatal error:", e);
    return null;
  }
}

async function answerTelegramCallback(callbackId, text) {
  try {
    const resp = await fetch(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/answerCallbackQuery`,
      {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          callback_query_id: String(callbackId || ""),
          text: String(text || ""),
          show_alert: false
        })
      }
    );

    return await resp.json().catch(() => ({}));
  } catch (e) {
    console.error("answerTelegramCallback fatal error:", e);
    return null;
  }
}

async function editTelegramReplyMarkup(chatId, messageId, replyMarkup) {
  if (!TELEGRAM_BOT_TOKEN || !chatId || !messageId) return;

  try {
    await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/editMessageReplyMarkup`,
      {
        chat_id: chatId,
        message_id: messageId,
        reply_markup: replyMarkup
      }
    );
  } catch (e) {
    console.error("editTelegramReplyMarkup error:", e?.response?.data || e.message || e);
  }
}

async function getUserChatIdByLogin(login, chatMap = {}) {
  const loginNorm = String(login || "").trim().toLowerCase();

  const fromMap =
    chatMap?.[loginNorm] ||
    chatMap?.[login] ||
    "";

  if (fromMap) return String(fromMap).trim();

  const q = await pool.query(
    `
    SELECT chat_id
    FROM public.users
    WHERE lower(trim(login)) = lower(trim($1))
    LIMIT 1
    `,
    [loginNorm]
  );

  return String(q.rows[0]?.chat_id || "").trim();
}

async function createNotification({
  userLogin,
  type,
  title,
  message,
  entityId,
  entityPage
}) {
  try {
    if (!userLogin) return;

    await pool.query(`
      INSERT INTO public.notifications
      (
        user_login,
        type,
        title,
        message,
        entity_id,
        entity_page
      )
      VALUES ($1,$2,$3,$4,$5,$6)
    `, [
      String(userLogin || "").trim(),
      String(type || "").trim(),
      String(title || "").trim(),
      String(message || "").trim(),
      entityId ? Number(entityId) : null,
      entityPage ? String(entityPage).trim() : null
    ]);

  } catch (e) {
    console.error("createNotification error:", e);
  }
}

function getApproverByStage(stage) {
  const s = String(stage || "").trim();

  if (s === "Согласование") {
    return [
      { login: "K_Marat", name: "Койлибаев Марат" },
      { login: "V_Shevchenko", name: "Шевченко Владимир" }
    ];
  }

  if (s === "Утверждение") {
    return [
      { login: "K_Ermek", name: "Ермек Касенов" }
    ];
  }

  if (s === "Исполнение платежей") {
    return [
      { login: "K_Arailym", name: "Арайлым Касенова" }
    ];
  }

  if (s === "Контроль и архивирование") {
    return [
      { login: "b_erkin", name: "Еркин" }
    ];
  }

  return [];
}

async function sendRegistryTelegramNotification({
  registryId,
  registryNo,
  stage,
  totalAmount,
  createdBy,
  chatMap = {},
  action = "",
  actorLogin = "",
  actorName = ""
}) {
  const client = await pool.connect();

  try {
    const headRes = await client.query(`
      SELECT
        id,
        registry_no,
        created_by,
        total_amount,
        workflow_stage,
        agree_status,
        execution_status,
        archive_flag,
        chat_map
      FROM public.registry_head
      WHERE id = $1
      LIMIT 1
    `, [Number(registryId)]);

    const head = headRes.rows[0];
    if (!head) return;

    const regNo = registryNo || head.registry_no || registryId;
    const currentStage = String(stage || head.workflow_stage || "").trim();
    const amount = Number(totalAmount ?? head.total_amount ?? 0);

    const finalChatMap = {
      ...(head.chat_map || {}),
      ...(chatMap || {})
    };

    const approvalsRes = await client.query(`
      SELECT
        stage_name,
        approver_login,
        approver_name,
        status
      FROM public.registry_stage_approvals
      WHERE registry_id = $1
      ORDER BY id
    `, [Number(registryId)]);

    let approvals = approvalsRes.rows || [];

    if (!approvals.some(a => String(a.stage_name || "") === currentStage)) {
      const list = getApproverByStage(currentStage);
      const arr = Array.isArray(list) ? list : (list ? [list] : []);

      for (const a of arr) {
        await client.query(`
          INSERT INTO public.registry_stage_approvals
            (registry_id, stage_name, approver_login, approver_name, status)
          VALUES ($1,$2,$3,$4,'Ожидает')
          ON CONFLICT (registry_id, stage_name, approver_login)
          DO NOTHING
        `, [
          Number(registryId),
          currentStage,
          String(a.login || "").trim(),
          String(a.name || a.login || "").trim()
        ]);
      }

      const again = await client.query(`
        SELECT stage_name, approver_login, approver_name, status
        FROM public.registry_stage_approvals
        WHERE registry_id = $1
        ORDER BY id
      `, [Number(registryId)]);

      approvals = again.rows || [];
    }

    const openUrl =
      `https://script.google.com/macros/s/AKfycbySY2CFP3WJ9M_MW5HiDZvSScGCTn2SCOLW68SS1Gt5q-CsHGk9lve06PkeKnuZwZ-j/exec?page=registryCard&id=${Number(registryId)}`;

    const pendingCurrent = approvals.filter(a =>
      String(a.stage_name || "") === currentStage &&
      String(a.status || "").trim() !== "Согласовано"
    );

    const approvedCurrent = approvals.filter(a =>
      String(a.stage_name || "") === currentStage &&
      String(a.status || "").trim() === "Согласовано"
    );

    const pendingText = pendingCurrent.length
      ? pendingCurrent.map(a => `⏳ ${a.approver_name || a.approver_login}`).join("\n")
      : "Нет ожидающих";

    const approvedText = approvedCurrent.length
      ? approvedCurrent.map(a => `✅ ${a.approver_name || a.approver_login}`).join("\n")
      : "Пока нет";

    let titleText = `Реестр №${regNo} на этапе ${currentStage}`;
    let titleHtml = `📌 <b>Реестр на этапе: ${currentStage}</b>`;

    if (action === "approve") {
      titleText = `Реестр №${regNo} согласован`;
      titleHtml = `✅ <b>${actorName || actorLogin} согласовал реестр</b>`;
    }

    if (action === "reject") {
      titleText = `Реестр №${regNo} отклонен`;
      titleHtml = `❌ <b>${actorName || actorLogin} отклонил реестр</b>`;
    }

    const infoText =
      `${titleHtml}\n\n` +
      `Реестр №: <b>${regNo}</b>\n` +
      `Инициатор: <b>${head.created_by || createdBy || "-"}</b>\n` +
      `Этап: <b>${currentStage}</b>\n` +
      `Сумма: <b>${amount.toLocaleString("ru-RU", {
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
      })}</b>\n\n` +
      `<b>Согласовали:</b>\n${approvedText}\n\n` +
      `<b>Остались:</b>\n${pendingText}`;

    const usersToNotify = new Set();

    if (head.created_by) {
      usersToNotify.add(String(head.created_by).trim().toLowerCase());
    }

    approvals.forEach(a => {
      if (a.approver_login) {
        usersToNotify.add(String(a.approver_login).trim().toLowerCase());
      }
    });

    const inputUsersRes = await client.query(`
      SELECT DISTINCT lower(trim(COALESCE(input_name,''))) AS login
      FROM public.registry_items
      WHERE registry_id = $1
        AND NULLIF(trim(COALESCE(input_name,'')), '') IS NOT NULL
    `, [Number(registryId)]);

    inputUsersRes.rows.forEach(u => {
      if (u.login) {
        usersToNotify.add(String(u.login).trim().toLowerCase());
      }
    });

    if (actorLogin) {
      usersToNotify.add(String(actorLogin).trim().toLowerCase());
    }

    for (const login of usersToNotify) {
      await createNotification({
        userLogin: login,
        type: "registry",
        title: titleText,
        message:
          action === "approve"
            ? `${actorName || actorLogin} согласовал. Этап: ${currentStage}`
            : action === "reject"
              ? `${actorName || actorLogin} отклонил. Этап: ${currentStage}`
              : `Этап: ${currentStage}. Сумма: ${amount.toLocaleString("ru-RU")} ₸`,
        entityId: Number(registryId),
        entityPage: "registryCard"
      });

      const chatId = await getUserChatIdByLogin(login, finalChatMap);

      if (chatId) {
        await sendTelegramMessage(chatId, infoText, {
          inline_keyboard: [
            [
              {
                text: "🔍 Открыть реестр",
                url: openUrl
              }
            ]
          ]
        });
      }
    }

    for (const a of pendingCurrent) {
      const approverLoginNorm = String(a.approver_login || "").trim().toLowerCase();
      const approverName = String(a.approver_name || a.approver_login || "").trim();
      const chatId = await getUserChatIdByLogin(approverLoginNorm, finalChatMap);

      if (!chatId) continue;

      await sendTelegramMessage(
        chatId,
        `📌 <b>Нужно согласовать реестр</b>\n\n` +
        `Реестр №: <b>${regNo}</b>\n` +
        `Этап: <b>${currentStage}</b>\n` +
        `Сумма: <b>${amount.toLocaleString("ru-RU", {
          minimumFractionDigits: 2,
          maximumFractionDigits: 2
        })}</b>`,
        {
          inline_keyboard: [
            [
              {
                text: "✅ Согласовать",
                callback_data: `approve|${Number(registryId)}|${currentStage}|${approverLoginNorm}|${encodeURIComponent(approverName)}`
              },
              {
                text: "❌ Отклонить",
                callback_data: `reject|${Number(registryId)}|${currentStage}|${approverLoginNorm}|${encodeURIComponent(approverName)}`
              }
            ],
            [
              {
                text: "🔍 Открыть реестр",
                url: openUrl
              }
            ]
          ]
        }
      );
    }

  } catch (e) {
    console.error("sendRegistryTelegramNotification error:", e);
  } finally {
    client.release();
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
  const client = await pool.connect();
  try {
    const { ids, login, request_id } = req.body;

    if (!login) {
      return res.status(400).json({ success: false, error: "login required" });
    }
    if (!request_id) {
      return res.status(400).json({ success: false, error: "request_id required" });
    }

    await client.query("BEGIN");

    const loginNorm = String(login || "").trim();

    if (loginNorm === "S_Zhasulan") {
      if (!Array.isArray(ids) || !ids.length) {
        throw new Error("ids required for chief approval");
      }

      const normIds = ids.map(x => Number(x)).filter(Number.isFinite);
      if (!normIds.length) {
        throw new Error("correct ids required");
      }

      await client.query(`
        UPDATE public.zvk_status
        SET chief_approved = 'Да'
        WHERE zvk_row_id = ANY($1::bigint[])
      `, [normIds]);

      await client.query(`
        UPDATE public.request_head h
        SET
          acc_buh_status = CASE
            WHEN x.total_rows > 0 AND x.chief_approved_rows = x.total_rows THEN 'Согласовано'
            WHEN x.chief_approved_rows > 0 THEN 'Частично согласовано'
            ELSE 'Ожидает'
          END,
          acc_buh_time = NOW(),
          workflow_stage = CASE
            WHEN x.total_rows > 0 AND x.chief_approved_rows = x.total_rows THEN 'Админ'
            ELSE 'Главный бухгалтер'
          END,
          agree_status = CASE
            WHEN x.total_rows > 0 AND x.chief_approved_rows = x.total_rows THEN 'На согласовании у Админа'
            WHEN x.chief_approved_rows > 0 THEN 'Частично согласовано'
            ELSE 'На согласовании'
          END
        FROM (
          SELECT
            i.request_id,
            COUNT(*) AS total_rows,
            COUNT(*) FILTER (WHERE COALESCE(s.chief_approved,'') = 'Да') AS chief_approved_rows
          FROM public.request_items i
          LEFT JOIN public.zvk_status s
            ON s.zvk_row_id = i.zvk_row_id
          WHERE i.request_id = $1
          GROUP BY i.request_id
        ) x
        WHERE h.id = x.request_id
      `, [request_id]);

    } else if (loginNorm === "B_Erkin") {
  const checkRes = await client.query(`
    SELECT
      COUNT(*) AS total_rows,
      COUNT(*) FILTER (WHERE COALESCE(s.chief_approved,'') = 'Да') AS chief_approved_rows
    FROM public.request_items i
    LEFT JOIN public.zvk_status s
      ON s.zvk_row_id = i.zvk_row_id
    WHERE i.request_id = $1
  `, [request_id]);

  const totalRows = Number(checkRes.rows[0]?.total_rows || 0);
  const chiefApprovedRows = Number(checkRes.rows[0]?.chief_approved_rows || 0);

  if (!totalRows) {
    throw new Error("request items not found");
  }

  if (chiefApprovedRows !== totalRows) {
    throw new Error("Сначала ГлавБухг должен согласовать все строки");
  }

  // 1. Обновляем шапку заявки
  await client.query(`
    UPDATE public.request_head
    SET
      acc_zam_name = 'Еркин',
      acc_zam_status = 'Согласовано',
      acc_zam_time = NOW(),
      workflow_stage = 'Завершено',
      agree_status = 'Согласовано'
    WHERE id = $1
  `, [request_id]);

  // 2. Ставим Реестр = Да по всем строкам заявки
  await client.query(`
    INSERT INTO public.zvk_pay (zvk_row_id, registry_flag, agree_time)
    SELECT
      i.zvk_row_id,
      'Да',
      NOW()
    FROM public.request_items i
    WHERE i.request_id = $1
      AND i.zvk_row_id IS NOT NULL
    ON CONFLICT (zvk_row_id)
    DO UPDATE SET
      registry_flag = 'Да',
      agree_time = COALESCE(zvk_pay.agree_time, NOW())
  `, [request_id]);
} else {
      throw new Error("У пользователя нет прав на согласование");
    }

    await client.query("COMMIT");
    return res.json({ success: true });

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("approve-rows error:", e);
    return res.status(500).json({ success: false, error: e.message });
  } finally {
    client.release();
  }
});

app.post("/update-row", async (req,res)=>{
  try{
    const {
      zvk_row_id,
      request_flag,
      to_pay,
      src_o,
      status_comment,
      is_paid,
      login
    } = req.body;

    // запрет если уже в реестре
    const check = await pool.query(`
      SELECT registry_flag FROM zvk_pay WHERE zvk_row_id=$1
    `,[zvk_row_id]);

    if (check.rows[0]?.registry_flag === "Да"){
      return res.json({ success:false, error:"LOCKED_BY_REGISTRY" });
    }

    // обновление
    await pool.query(`
      UPDATE zvk
      SET request_flag=$1,
          to_pay=$2
      WHERE id=$3
    `,[request_flag, to_pay, zvk_row_id]);

    await pool.query(`
      INSERT INTO zvk_status(zvk_row_id, src_o, status_comment)
      VALUES($1,$2,$3)
      ON CONFLICT (zvk_row_id)
      DO UPDATE SET
        src_o=EXCLUDED.src_o,
        status_comment=EXCLUDED.status_comment
    `,[zvk_row_id, src_o, status_comment]);

    // только админ
    if (is_paid !== null){
      await pool.query(`
        UPDATE zvk_pay
        SET is_paid=$1
        WHERE zvk_row_id=$2
      `,[is_paid, zvk_row_id]);
    }

    res.json({ success:true });

  }catch(e){
    console.error(e);
    res.status(500).json({ success:false, error:e.message });
  }
});

app.post("/registry-save", async (req, res) => {
  const client = await pool.connect();

  try {
    const { registry_id, login, pay_account, transfers } = req.body || {};

    if (!registry_id) {
      return res.status(400).json({ success:false, error:"registry_id required" });
    }

    await client.query("BEGIN");

const divRes = await client.query(`
  SELECT STRING_AGG(DISTINCT NULLIF(TRIM(src_d), ''), ', ') AS division_text
  FROM public.registry_items
  WHERE registry_id = $1
`, [Number(registry_id)]);

const divisionText = String(divRes.rows[0]?.division_text || "").trim() || null;

await client.query(`
  UPDATE public.registry_head
  SET
    pay_account = $2,
    division = $3
  WHERE id = $1
`, [
  Number(registry_id),
  String(pay_account || "").trim() || null,
  divisionText
]);

    await client.query(`
      DELETE FROM public.registry_transfers
      WHERE registry_id = $1
    `, [Number(registry_id)]);

    const rows = Array.isArray(transfers) ? transfers : [];

    for (const row of rows) {
      await client.query(`
        INSERT INTO public.registry_transfers
        (
          registry_id,
          src_object,
          acc_from,
          dds_from,
          acc_to,
          dds_to,
          sum
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7)
      `, [
        Number(registry_id),
        String(row.src_object || "").trim() || null,
        String(row.acc_from || "").trim() || null,
        String(row.dds_from || "").trim() || null,
        String(row.acc_to || "").trim() || null,
        String(row.dds_to || "").trim() || null,
        Number(row.sum || 0)
      ]);
    }

    await client.query("COMMIT");
    res.json({ success:true });

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("REGISTRY-SAVE ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  } finally {
    client.release();
  }
});

app.post("/registry-submit", async (req, res) => {
  const client = await pool.connect();

  try {
    const { registry_id, login, pay_account, transfers } = req.body || {};

    if (!registry_id) {
      return res.status(400).json({ success:false, error:"registry_id required" });
    }

    await client.query("BEGIN");

const divRes = await client.query(`
  SELECT STRING_AGG(DISTINCT NULLIF(TRIM(src_d), ''), ', ') AS division_text
  FROM public.registry_items
  WHERE registry_id = $1
`, [Number(registry_id)]);

const divisionText = String(divRes.rows[0]?.division_text || "").trim() || null;

await client.query(`
  UPDATE public.registry_head
  SET
    pay_account = $2,
    division = $3
  WHERE id = $1
`, [
  Number(registry_id),
  String(pay_account || "").trim() || null,
  divisionText
]);

    await client.query(`
      DELETE FROM public.registry_transfers
      WHERE registry_id = $1
    `, [Number(registry_id)]);

    const rows = Array.isArray(transfers) ? transfers : [];

    for (const row of rows) {
      await client.query(`
        INSERT INTO public.registry_transfers
        (
          registry_id,
          src_object,
          acc_from,
          dds_from,
          acc_to,
          dds_to,
          sum
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7)
      `, [
        Number(registry_id),
        String(row.src_object || "").trim() || null,
        String(row.acc_from || "").trim() || null,
        String(row.dds_from || "").trim() || null,
        String(row.acc_to || "").trim() || null,
        String(row.dds_to || "").trim() || null,
        Number(row.sum || 0)
      ]);
    }

    await client.query(`
      UPDATE public.registry_head
      SET
        workflow_stage = 'Согласование',
        agree_status = 'На согласовании'
      WHERE id = $1
    `, [Number(registry_id)]);
await client.query(`
  DELETE FROM public.registry_stage_approvals
  WHERE registry_id = $1
`, [Number(registry_id)]);

    const approvers = getApproverByStage("Согласование");

    for (const a of approvers) {
      await client.query(`
        INSERT INTO public.registry_stage_approvals
          (registry_id, stage_name, approver_login, approver_name, status)
        VALUES ($1, $2, $3, $4, 'Ожидает')
        ON CONFLICT (registry_id, stage_name, approver_login)
        DO NOTHING
      `, [
        Number(registry_id),
        "Согласование",
        String(a.login || "").trim(),
        String(a.name || "").trim()
      ]);
    }

    await client.query(`
      INSERT INTO public.registry_approve_log
      (
        registry_id,
        stage_name,
        approver_login,
        approver_name,
        action_type,
        comment_text
      )
      VALUES ($1, $2, $3, $4, 'submit', $5)
    `, [
      Number(registry_id),
      'Черновик -> Согласование',
      String(login || ""),
      String(login || ""),
      'Реестр отправлен из карточки'
    ]);

    const regRes = await client.query(`
      SELECT id, registry_no, total_amount, created_by, chat_map
      FROM public.registry_head
      WHERE id = $1
      LIMIT 1
    `, [Number(registry_id)]);

    await client.query("COMMIT");

    const reg = regRes.rows[0];

    try {
      await sendRegistryTelegramNotification({
        registryId: reg.id,
        registryNo: reg.registry_no,
        stage: "Согласование",
        totalAmount: reg.total_amount,
        createdBy: reg.created_by || "",
        chatMap: reg.chat_map || {}
      });
    } catch (tgErr) {
      console.error("telegram submit notify error:", tgErr);
    }

    res.json({ success:true });

  } catch (e) {
    await client.query("ROLLBACK");
    console.error("REGISTRY-SUBMIT ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  } finally {
    client.release();
  }
});

async function sendRequestTelegramNotification({
  requestId,
  requestNo,
  stage,
  totalAmount,
  createdBy
}) {
  try {
    const stageName = String(stage || "").trim();

    let targetLogin = "";
    let stageLabel = "";

    if (stageName === "Главный бухгалтер") {
      targetLogin = "s_zhasulan";
      stageLabel = "Главный бухгалтер";
    } else if (stageName === "Админ") {
      targetLogin = "b_erkin";
      stageLabel = "Админ";
    } else {
      console.log("sendRequestTelegramNotification: неизвестный этап:", stageName);
      return;
    }

    const userRes = await pool.query(`
      SELECT chat_id, first_name, last_name, login
      FROM public.users
      WHERE lower(trim(login)) = lower(trim($1))
      LIMIT 1
    `, [targetLogin]);

    if (!userRes.rows.length) {
      console.log("❌ Пользователь для Telegram не найден:", targetLogin);
      return;
    }

    const userRow = userRes.rows[0];
    const chatId = String(userRow.chat_id || "").trim();

    if (!chatId) {
      console.log("❌ chat_id пустой у пользователя:", targetLogin);
      return;
    }

    const approverName =
      [userRow.last_name, userRow.first_name]
        .filter(Boolean)
        .join(" ")
        .trim() || userRow.login || targetLogin;

    const safeRequestId = Number(requestId);
    const safeRequestNo = requestNo ?? "";
    const safeTotal = Number(totalAmount || 0).toLocaleString("ru-RU", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2
    });

    const openUrl = `${APP_BASE_URL}?page=request_card&id=${safeRequestId}`;

    const text =
      `📄 Заявка №${safeRequestNo}\n` +
      `👤 Инициатор: ${String(createdBy || "").trim() || "-"}\n` +
      `💰 Сумма: ${safeTotal}\n` +
      `📍 Этап: ${stageLabel}\n` +
      `👨‍💼 Получатель: ${approverName}`;

    await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
      {
        chat_id: chatId,
        text,
   reply_markup: {
  inline_keyboard: [
    [
      {
        text: "✅ Согласовать",
        callback_data: `request_approve|${safeRequestId}|${stageLabel}`
      },
      {
        text: "❌ Отклонить",
        callback_data: `request_reject|${safeRequestId}|${stageLabel}`
      }
    ],
    [
      {
        text: "🔍 Открыть заявку",
        url: openUrl
      }
    ]
  ]
}
      },
      {
        timeout: 15000
      }
    );
    await createNotification({
      userLogin: targetLogin,
      type: "request",
      title: `Заявка №${safeRequestNo} на согласовании`,
      message: `Этап: ${stageLabel}. Сумма: ${safeTotal} ₸`,
      entityId: safeRequestId,
      entityPage: "request_card"
    });
    console.log("✅ Telegram уведомление отправлено:", {
      stage: stageLabel,
      targetLogin,
      chatId,
      requestId: safeRequestId,
      requestNo: safeRequestNo
    });

  } catch (e) {
    console.error("❌ sendRequestTelegramNotification error:", e?.response?.data || e.message || e);
  }
}

app.get("/dict/divisions", async (req, res) => {
  try {
    const q = await pool.query(`
      SELECT name
      FROM spravochnik_division
      ORDER BY name
    `);
    res.json({ success: true, items: q.rows.map(r => r.name) });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/dict/objects", async (req, res) => {
  try {
    const q = await pool.query(`
      SELECT name
      FROM spravochnik_object
      ORDER BY name
    `);
    res.json({ success: true, items: q.rows.map(r => r.name) });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/dict/dds", async (req, res) => {
  try {
    const q = await pool.query(`
      SELECT name
      FROM spravochnik_dds
      ORDER BY name
    `);
    res.json({ success: true, items: q.rows.map(r => r.name) });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/dict/contractors", async (req, res) => {
  try {
    const q = await pool.query(`
      SELECT DISTINCT contractor
      FROM spravochnik_contracts
      ORDER BY contractor
    `);
    res.json({ success: true, items: q.rows.map(r => r.contractor) });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/dict/contracts", async (req, res) => {
  try {
    const contractor = String(req.query.contractor || "").trim();

    if (!contractor) {
      return res.json({ success: true, items: [] });
    }

    const q = await pool.query(`
      SELECT contract
      FROM spravochnik_contracts
      WHERE contractor = $1
      ORDER BY contract
    `, [contractor]);

    res.json({ success: true, items: q.rows.map(r => r.contract) });
  } catch (e) {
    res.status(500).json({ success: false, error: e.message });
  }
});

app.get("/dict/source-objects", async (req, res) => {
  try {
    const q = await pool.query(`
      SELECT name
      FROM spravochnik_istochnikobject
      ORDER BY name
    `);

    res.json({
      success: true,
      items: q.rows.map(r => r.name)
    });
  } catch (e) {
    console.error("DICT SOURCE OBJECTS ERROR:", e);
    res.status(500).json({
      success: false,
      error: e.message
    });
  }
});

app.get("/registry-dict/accounts", async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT name
      FROM public.spr_account
      ORDER BY name
    `);

    res.json({
      success: true,
      items: r.rows.map(x => x.name)
    });

  } catch (e) {
    res.status(500).json({ success:false, error:e.message });
  }
});

app.get("/registry-dict/dds", async (req, res) => {
  try {
    const r = await pool.query(`
      SELECT name
      FROM public.spr_dds_registry
      ORDER BY name
    `);

    res.json({
      success: true,
      items: r.rows.map(x => x.name)
    });
  } catch (e) {
    res.status(500).json({ success:false, error:e.message });
  }
});

app.get("/division-dicts", async (req, res) => {
  try {
    const [divs, objects, dds] = await Promise.all([
      pool.query(`SELECT name FROM public.spravochnik_division ORDER BY name`),
      pool.query(`SELECT name FROM public.spravochnik_istochnikobject ORDER BY name`),
      pool.query(`SELECT name FROM public.spravochnik_dds ORDER BY name`)
    ]);

    res.json({
      success: true,
      divs: divs.rows.map(r => r.name),
      objects: objects.rows.map(r => r.name),
      dds: dds.rows.map(r => r.name)
    });
  } catch (e) {
    res.status(500).json({ success:false, error:e.message });
  }
});


app.get("/notifications", async (req, res) => {
  try {
    const login = String(req.query.login || "").trim();
    const filter = String(req.query.filter || "all").trim();

    if (!login) {
      return res.status(400).json({ success:false, error:"login required" });
    }

    let whereFilter = "";
    if (filter === "unread") whereFilter = "AND is_read = false";
    if (filter === "read") whereFilter = "AND is_read = true";

    const r = await pool.query(`
      SELECT
        id,
        type,
        title,
        message,
        entity_id,
        entity_page,
        is_read,
        created_at
      FROM public.notifications
      WHERE lower(trim(user_login)) = lower(trim($1))
      ${whereFilter}
      ORDER BY created_at DESC
      LIMIT 100
    `, [login]);

    const cnt = await pool.query(`
      SELECT COUNT(*)::int AS unread_count
      FROM public.notifications
      WHERE lower(trim(user_login)) = lower(trim($1))
        AND is_read = false
    `, [login]);

    return res.json({
      success: true,
      rows: r.rows,
      unread_count: Number(cnt.rows[0]?.unread_count || 0)
    });

  } catch (e) {
    return res.status(500).json({ success:false, error:e.message });
  }
});


// =====================================================
// Start
// =====================================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT))