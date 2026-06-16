// =====================================================
// Модуль отдельного сайта «Реестр платежей» удалён.
// Сохранена общая FT-логика registry_flag для оплаты и обнуления.
// =====================================================

require("dotenv").config();

const express = require("express");
const cors = require("cors");
const path = require("path");
const { Pool } = require("pg");
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
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_marat_name text;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_marat_status text;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_marat_time timestamptz;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_marat_comment text;`);

await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_zhasulan_name text;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_zhasulan_status text;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_zhasulan_time timestamptz;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_zhasulan_comment text;`);

await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_shevchenko_name text;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_shevchenko_status text;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_shevchenko_time timestamptz;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_shevchenko_comment text;`);

await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_ermek_name text;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_ermek_status text;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_ermek_time timestamptz;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS acc_ermek_comment text;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS approve_ermek_name text;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS approve_ermek_status text;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS approve_ermek_time timestamptz;`);
await pool.query(`ALTER TABLE public.request_head ADD COLUMN IF NOT EXISTS approve_ermek_comment text;`);


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
  ALTER TABLE public.prihod6
  ADD COLUMN IF NOT EXISTS io_history_id bigint;
`);

await pool.query(`
  ALTER TABLE public.perevod7
  ADD COLUMN IF NOT EXISTS io_history_id bigint;
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS prihod6_io_history_id_idx
  ON public.prihod6 (io_history_id);
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS perevod7_io_history_id_idx
  ON public.perevod7 (io_history_id);
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
const loginOriginal = String(login || "").trim();

if (/[А-Яа-яЁё]/.test(loginOriginal)) {
  return res.status(400).json({
    success: false,
    message: "В логине нельзя использовать русские буквы"
  });
}

if (!/^[A-Za-z]_[A-Za-z]+$/.test(loginOriginal)) {
  return res.status(400).json({
    success: false,
    message:
      "Логин должен быть в формате A_Sagyndyk: первая буква фамилии, затем _ и имя"
  });
}
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
    role_lzk: user.role_lzk,
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
  role_lzk,
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
        role_lzk,
        is_active,
        created_at,
        login
      FROM public.users
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
    const { login, role_ft, role_hr, role_lzk, actor_login } = req.body || {};

    const loginNorm = String(login || "").trim();
    const actorLoginNorm = String(actor_login || "").trim();

    const newRoleFt = String(role_ft || "").trim().toLowerCase();
    const newRoleHr = String(role_hr || "").trim().toLowerCase();
    const newRoleLzk = String(role_lzk || "").trim().toLowerCase();

    const allowedRoles = [
  "initiator",
  "operator",
  "supervisor",
  "admin",
  "pto",
  "editor",
  "supplier"
];

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

    if (
      (newRoleFt && !allowedRoles.includes(newRoleFt)) ||
      (newRoleHr && !allowedRoles.includes(newRoleHr)) ||
      (newRoleLzk && !allowedRoles.includes(newRoleLzk))
    ) {
      return res.status(400).json({
        success: false,
        message: "Недопустимая роль"
      });
    }

    await client.query("BEGIN");

    const actorRes = await client.query(`
      SELECT id, login, role_ft, role_hr, role_lzk
      FROM public.users
      WHERE lower(trim(login)) = lower(trim($1))
      LIMIT 1
    `, [actorLoginNorm]);

    if (!actorRes.rows.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({
        success: false,
        message: "Текущий пользователь не найден"
      });
    }

    const actor = actorRes.rows[0];
    const isMainAdmin = String(actor.login || "").trim().toLowerCase() === "admin";

    if (!isMainAdmin) {
      await client.query("ROLLBACK");
      return res.status(403).json({
        success: false,
        message: "Только пользователь admin может изменять роли"
      });
    }

    const userRes = await client.query(`
      SELECT id, login, role_ft, role_hr, role_lzk
      FROM public.users
      WHERE lower(trim(login)) = lower(trim($1))
      LIMIT 1
    `, [loginNorm]);

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
    const oldRoleLzk = String(user.role_lzk || "").trim().toLowerCase();

    if (
      oldRoleFt === newRoleFt &&
      oldRoleHr === newRoleHr &&
      oldRoleLzk === newRoleLzk
    ) {
      await client.query("ROLLBACK");
      return res.json({
        success: true,
        message: "Изменений нет",
        row: {
          id: user.id,
          login: user.login,
          role_ft: user.role_ft,
          role_hr: user.role_hr,
          role_lzk: user.role_lzk
        }
      });
    }

    const updRes = await client.query(`
      UPDATE public.users
      SET role_ft = NULLIF($1, ''),
          role_hr = NULLIF($2, ''),
          role_lzk = NULLIF($3, '')
      WHERE lower(trim(login)) = lower(trim($4))
      RETURNING id, login, role_ft, role_hr, role_lzk
    `, [newRoleFt, newRoleHr, newRoleLzk, loginNorm]);

    await client.query(`
      INSERT INTO public.user_role_history
      (
        login,
        old_role_ft,
        new_role_ft,
        old_role_hr,
        new_role_hr,
        old_role_lzk,
        new_role_lzk,
        changed_at
      )
      VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    `, [
      user.login,
      oldRoleFt || null,
      newRoleFt,
      oldRoleHr || null,
      newRoleHr,
      oldRoleLzk || null,
      newRoleLzk
    ]);

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
    old_role_lzk,
    new_role_lzk,
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
// ✅ b_erkin может менять основные поля FT
// POST /ft-update-main
// =====================================================
app.post("/ft-update-main", async (req, res) => {
  try {
    const {
      id_ft,
      login,
      division,
      object,
      contractor,
      pay_purpose,
      dds_article,
      contract_no,
      invoice_no,
      sum_ft
    } = req.body || {};

    const actor = String(login || "").trim().toLowerCase();
    const idFt = String(id_ft || "").trim();

    if (!idFt) {
      return res.status(400).json({ success:false, error:"id_ft required" });
    }

    // ✅ доступ только b_erkin
    if (actor !== "b_erkin") {
      return res.status(403).json({ success:false, error:"NO_RIGHTS" });
    }

    const sumNum = Number(
      String(sum_ft || 0)
        .replace(/\s/g, "")
        .replace(",", ".")
    );

    if (!Number.isFinite(sumNum)) {
      return res.status(400).json({ success:false, error:"sum_ft must be number" });
    }

    const r = await pool.query(`
      UPDATE public.ft
      SET
        division = $2,
        "object" = $3,
        contractor = $4,
        pay_purpose = $5,
        dds_article = $6,
        contract_no = $7,
        invoice_no = $8,
        sum_ft = $9
      WHERE id_ft = $1
      RETURNING *
    `, [
      idFt,
      String(division || "").trim(),
      String(object || "").trim(),
      String(contractor || "").trim(),
      String(pay_purpose || "").trim(),
      String(dds_article || "").trim(),
      String(contract_no || "").trim(),
      String(invoice_no || "").trim(),
      sumNum
    ]);

    if (!r.rowCount) {
      return res.status(404).json({ success:false, error:"FT_NOT_FOUND" });
    }

    return res.json({ success:true, row:r.rows[0] });

  } catch (e) {
    console.error("FT-UPDATE-MAIN ERROR:", e);
    return res.status(500).json({ success:false, error:e.message });
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

function getDivisionPayRule(login) {
  const lg = String(login || "").trim().toLowerCase();

  if (lg === "zh_elena") {
    return {
      mode: "only",
      divisions: ["СК Жилой дом", "Smart Estate"]
    };
  }

  if (lg === "s_zhasulan") {
    return {
      mode: "only",
      divisions: ["Sapa asphalt"]
    };
  }

  if (lg === "k_arailym") {
    return {
      mode: "except",
      divisions: ["СК Жилой дом", "Smart Estate", "Sapa asphalt"]
    };
  }

  return null;
}

function canSetPaid(login, roleFt) {
  const lg = String(login || "").trim().toLowerCase();
  const role = String(roleFt || "").trim().toLowerCase();

  return (
    lg === "zh_elena" ||
    lg === "s_zhasulan" ||
    lg === "k_arailym"
  );
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
    const { row_ids, login } = req.body || {};

    const ids = Array.isArray(row_ids)
      ? row_ids.map(x => Number(x)).filter(Boolean)
      : [];

    if (!ids.length) {
      return res.status(400).json({
        success: false,
        error: "row_ids required"
      });
    }

    if (!login) {
      return res.status(400).json({
        success: false,
        error: "login required"
      });
    }

    await client.query("BEGIN");

    const createdRequests = [];

    for (const oneRowId of ids) {
      const head = await client.query(`
        INSERT INTO public.request_head (created_by)
        VALUES ($1)
        RETURNING id, request_no
      `, [
        String(login || "").trim()
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
          division,
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
          v.division,
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
        WHERE v.zvk_row_id = $2
        RETURNING to_pay
      `, [request_id, oneRowId]);

      const total = items.rows.reduce((s, r) => s + Number(r.to_pay || 0), 0);
      const count = items.rows.length;

      await client.query(`
        UPDATE public.request_head
        SET
          total_amount = $1,
          items_count = $2,

          acc_zhasulan_name = 'Сулейменов Жасулан',
          acc_zhasulan_status = 'Ожидает',
          acc_zhasulan_time = NULL,
          acc_zhasulan_comment = NULL,

          acc_shevchenko_name = 'Шевченко Владимир',
          acc_shevchenko_status = 'Ожидает',
          acc_shevchenko_time = NULL,
          acc_shevchenko_comment = NULL,

          acc_marat_name = 'Койлибаев Марат',
          acc_marat_status = 'Ожидает',
          acc_marat_time = NULL,
          acc_marat_comment = NULL,

          acc_ermek_name = 'Касенов Ермек',
          acc_ermek_status = 'Ожидает',
          acc_ermek_time = NULL,
          acc_ermek_comment = NULL,

          approve_ermek_name = 'Касенов Ермек',
          approve_ermek_status = 'Ожидает',
          approve_ermek_time = NULL,
          approve_ermek_comment = NULL
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
  'Заявка создана'
]);

// ✅ Уведомление согласующим, когда заявка попала в "Отправленные заявки"
const notifyUsers = [
  "s_zhasulan"
];

for (const userLogin of notifyUsers) {
  await client.query(`
    INSERT INTO public.notifications
      (
        user_login,
        type,
        title,
        message,
        entity_id,
        entity_page,
        is_read,
        created_at
      )
    VALUES
      ($1, 'request', $2, $3, $4, 'request_card', false, NOW())
  `, [
    userLogin,
    `Заявка №${request_no} создана`,
    `Заявка попала в отправленные заявки. Сумма: ${Number(total || 0).toLocaleString("ru-RU")} ₸`,
    request_id
  ]);
}

createdRequests.push({
  request_id,
  request_no,
  row_id: oneRowId,
  total_amount: total,
  items_count: count
});
    }

    await client.query("COMMIT");

    return res.json({
      success: true,
      count: createdRequests.length,
      requests: createdRequests,
      request_id: createdRequests[0]?.request_id || null,
      request_no: createdRequests[0]?.request_no || null
    });

  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}

    console.error("CREATE-REQUEST ERROR:", e);

    return res.status(500).json({
      success: false,
      error: e.message
    });

  } finally {
    client.release();
  }
});

app.post("/create-registry", async (req, res) => {
  const client = await pool.connect();

  try {
    const { row_ids, login } = req.body || {};

    const ids = Array.isArray(row_ids)
      ? row_ids.map(x => Number(x)).filter(Boolean)
      : [];

    if (!ids.length) {
      return res.status(400).json({
        success: false,
        error: "row_ids required"
      });
    }

    if (!login) {
      return res.status(400).json({
        success: false,
        error: "login required"
      });
    }

    await client.query("BEGIN");

    const head = await client.query(`
      INSERT INTO public.registry_head
        (created_by, workflow_stage, agree_status, archive_flag)
      VALUES
        ($1, 'Инициация', 'Черновик', 'Нет')
      RETURNING id, registry_no
    `, [
      String(login || "").trim()
    ]);

    const registry_id = head.rows[0].id;
    const registry_no = head.rows[0].registry_no;

    const items = await client.query(`
      INSERT INTO public.registry_items
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
      FROM public.ft_zvk_current_v2 v
      WHERE v.zvk_row_id = ANY($2::bigint[])
      RETURNING to_pay, zvk_row_id
    `, [registry_id, ids]);

    if (!items.rowCount) {
      throw new Error("Строки для реестра не найдены");
    }

    const total = items.rows.reduce((s, r) => s + Number(r.to_pay || 0), 0);
    const count = items.rows.length;

    await client.query(`
      UPDATE public.registry_head
      SET
        total_amount = $1,
        items_count = $2
      WHERE id = $3
    `, [total, count, registry_id]);

    await client.query("COMMIT");

    return res.json({
      success: true,
      registry_id,
      registry_no,
      total_amount: total,
      items_count: count
    });

  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}

    console.error("CREATE-REGISTRY ERROR:", e);

    return res.status(500).json({
      success: false,
      error: e.message
    });

  } finally {
    client.release();
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
        pay_account,
        chat_map,
        registry_mode
      FROM public.registry_head
      WHERE id = $1
      LIMIT 1
    `, [id]);

    if (!headRes.rowCount) {
      return res.status(404).json({
        success: false,
        error: "Реестр не найден"
      });
    }

    const itemsRes = await pool.query(`
      SELECT
        i.registry_id,
        i.zvk_row_id,
        i.id_ft,
        i.id_zvk,

        COALESCE(cur.object, i.object) AS object,
        COALESCE(cur.input_name, i.input_name) AS input_name,
        COALESCE(cur.contractor, i.contractor) AS contractor,
        COALESCE(cur.pay_purpose, i.pay_purpose) AS pay_purpose,
        COALESCE(cur.dds_article, i.dds_article) AS dds_article,
        COALESCE(cur.contract_no, i.contract_no) AS contract_no,
        COALESCE(cur.invoice_no, i.invoice_no) AS invoice_no,
        COALESCE(cur.invoice_date, i.invoice_date) AS invoice_date,
        COALESCE(cur.invoice_pdf, i.invoice_pdf) AS invoice_pdf,
        COALESCE(cur.src_d, i.src_d) AS src_d,
        COALESCE(cur.src_o, i.src_o) AS src_o,
        COALESCE(cur.to_pay, i.to_pay) AS to_pay,

        COALESCE(cur.request_flag, '') AS request_flag,
        COALESCE(cur.registry_flag, '') AS registry_flag,
        COALESCE(cur.is_paid, '') AS is_paid

      FROM public.registry_items i

      LEFT JOIN public.ft_zvk_current_v2 cur
        ON cur.zvk_row_id = i.zvk_row_id

      WHERE i.registry_id = $1

      ORDER BY i.id
    `, [id]);

    return res.json({
      success: true,
      head: headRes.rows[0],
      items: itemsRes.rows,
      transfers: []
    });

  } catch (e) {
    console.error("REGISTRY-CARD ERROR:", e);

    return res.status(500).json({
      success: false,
      error: e.message
    });
  }
});


app.post("/request-created-bulk", async (req, res) => {
  try {
    const rowIds = Array.isArray(req.body.row_ids)
      ? req.body.row_ids.map(Number).filter(Boolean)
      : [];

    const login = String(req.body.login || "").trim();
    const value = String(req.body.value || "Да").trim();

    if (!rowIds.length) {
      return res.status(400).json({
        success: false,
        error: "row_ids required"
      });
    }

    await pool.query(
      `
      UPDATE public.zvk_status
      SET chief_approved = $1
      WHERE zvk_row_id = ANY($2::bigint[])
      `,
      [value, rowIds]
    );

    return res.json({
      success: true,
      updated: rowIds.length
    });

  } catch (e) {
    console.error("request-created-bulk error:", e);
    return res.status(500).json({
      success: false,
      error: e.message
    });
  }
});

app.post("/registry-save", async (req, res) => {
  const client = await pool.connect();

  try {
    const registry_id = Number(req.body?.registry_id);
    const login = String(req.body?.login || "").trim();
    const transfers = Array.isArray(req.body?.transfers)
      ? req.body.transfers
      : [];

    if (!registry_id) {
      return res.status(400).json({
        success: false,
        error: "registry_id required"
      });
    }

    await client.query("BEGIN");

    const exists = await client.query(`
      SELECT id
      FROM public.registry_head
      WHERE id = $1
      LIMIT 1
    `, [registry_id]);

    if (!exists.rowCount) {
      throw new Error("Реестр не найден");
    }

    await client.query(`
      DELETE FROM public.registry_transfers
      WHERE registry_id = $1
    `, [registry_id]);

    for (const t of transfers) {
      const amount = Number(
        String(t.amount || 0)
          .replace(/\s/g, "")
          .replace(",", ".")
      ) || 0;

      await client.query(`
        INSERT INTO public.registry_transfers
        (
          registry_id,
          src_o,
          debit_account,
          debit_dds,
          credit_account,
          credit_dds,
          amount
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7)
      `, [
        registry_id,
        String(t.src_o || "").trim(),
        String(t.debit_account || "").trim(),
        String(t.debit_dds || "").trim(),
        String(t.credit_account || "").trim(),
        String(t.credit_dds || "").trim(),
        amount
      ]);
    }

    await client.query(`
      UPDATE public.registry_head
      SET
        workflow_stage = COALESCE(workflow_stage, 'Инициация'),
        agree_status = COALESCE(agree_status, 'Черновик')
      WHERE id = $1
    `, [registry_id]);

    await client.query("COMMIT");

    return res.json({
      success: true,
      registry_id,
      saved: true,
      transfers_count: transfers.length
    });

  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}

    console.error("REGISTRY-SAVE ERROR:", e);

    return res.status(500).json({
      success: false,
      error: e.message
    });

  } finally {
    client.release();
  }
});

async function resetExpiredZhasulanRequests() {
  const client = await pool.connect();

  try {
    await client.query("BEGIN");

    const expired = await client.query(`
      SELECT
        h.id AS request_id,
        h.request_no,
        ARRAY_AGG(i.zvk_row_id) AS row_ids
      FROM public.request_head h
      JOIN public.request_items i
        ON i.request_id = h.id
      WHERE COALESCE(h.acc_zhasulan_status, '') = 'Согласовано'
        AND COALESCE(h.approve_ermek_status, '') NOT IN ('Согласовано', 'Утверждено', 'Да')
        AND h.acc_zhasulan_time IS NOT NULL
        AND (NOW() AT TIME ZONE 'Asia/Almaty') >=
            (
              ((h.acc_zhasulan_time AT TIME ZONE 'Asia/Almaty')::date + INTERVAL '2 days')
              + TIME '18:00'
            )
      GROUP BY h.id, h.request_no
    `);

    for (const row of expired.rows) {
      const requestId = Number(row.request_id);
      const rowIds = (row.row_ids || []).map(Number).filter(Boolean);

      if (!requestId || !rowIds.length) continue;

      // Заявка = пусто
      await client.query(`
        UPDATE public.zvk
        SET request_flag = NULL
        WHERE id = ANY($1::bigint[])
      `, [rowIds]);

      // Реестр = пусто
      await client.query(`
        UPDATE public.zvk_pay
        SET
          registry_flag = NULL,
          agree_time = NULL
        WHERE zvk_row_id = ANY($1::bigint[])
      `, [rowIds]);

      // Источник Объект = пусто
      await client.query(`
        INSERT INTO public.zvk_status (zvk_row_id, status_time, src_o)
        SELECT x, NOW(), ''
        FROM unnest($1::bigint[]) AS x
        ON CONFLICT (zvk_row_id)
        DO UPDATE SET
          src_o = '',
          status_time = NOW()
      `, [rowIds]);

      // ЗаявкаСоздано = пусто, если колонка chief_approved есть
      const hasChiefApproved = await client.query(`
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'zvk_status'
          AND column_name = 'chief_approved'
        LIMIT 1
      `);

      if (hasChiefApproved.rowCount) {
        await client.query(`
          UPDATE public.zvk_status
          SET chief_approved = NULL
          WHERE zvk_row_id = ANY($1::bigint[])
        `, [rowIds]);
      }

      // Удаляем отправленную заявку, чтобы инициатор создал заново
      await client.query(`
        DELETE FROM public.request_approve_log
        WHERE request_id = $1
      `, [requestId]);

      await client.query(`
        DELETE FROM public.request_items
        WHERE request_id = $1
      `, [requestId]);

      await client.query(`
        DELETE FROM public.request_head
        WHERE id = $1
      `, [requestId]);

      // Пересобираем хвост FT
      for (const rid of rowIds) {
        if (typeof rebuildFtTail === "function") {
          await rebuildFtTail(client, rid);
        }
      }
    }

    await client.query("COMMIT");

    return {
      success: true,
      reset_count: expired.rows.length
    };

  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}

    console.error("resetExpiredZhasulanRequests error:", e);

    return {
      success: false,
      error: e.message
    };

  } finally {
    client.release();
  }
}

app.get("/request-list", async (req, res) => {
  try {
    await resetExpiredZhasulanRequests();

    const login = String(req.query.login || "").trim().toLowerCase();
    const roleFt = String(req.query.role_ft || req.query.role || "").trim().toLowerCase();

    if (!login) {
      return res.status(400).json({
        success: false,
        error: "login required"
      });
    }

    let whereSql = "";
    const params = [];

    // ✅ 1. Жасулан видит все заявки, где он еще НЕ согласовал и НЕ отклонил
if (login === "s_zhasulan") {
  // Жасулан видит все заявки, даже после своего согласования
  whereSql = "";

} else if (
  login === "v_shevchenko" ||
  login === "k_marat" ||
  login === "k_ermek"
) {
      whereSql = `
        WHERE COALESCE(acc_zhasulan_status, '') = 'Согласовано'
      `;

    // ✅ 3. Только настоящий админ видит все
    } else if (
      login === "admin" ||
      login === "b_erkin" ||
      roleFt === "admin" ||
      roleFt === "админ" ||
      roleFt === "administrator"
    ) {
      whereSql = "";

    // ✅ 4. Инициатор видит только свои заявки
    } else {
      params.push(login);
      whereSql = `
        WHERE lower(trim(created_by)) = $1
           OR id IN (
             SELECT request_id
             FROM public.request_items
             WHERE lower(trim(input_name)) = $1
           )
      `;
    }

    const r = await pool.query(`
      SELECT
        id,
        request_no,
        request_date,
        created_by,
        total_amount,
        items_count,
        created_at,

        acc_zhasulan_status,
        acc_zhasulan_time,
        acc_zhasulan_comment,

        acc_shevchenko_status,
        acc_shevchenko_time,
        acc_shevchenko_comment,

        acc_marat_status,
        acc_marat_time,
        acc_marat_comment,

        acc_ermek_status,
        acc_ermek_time,
        acc_ermek_comment,

        approve_ermek_status,
        approve_ermek_time,
        approve_ermek_comment

      FROM public.request_head
      ${whereSql}
      ORDER BY id DESC
    `, params);

    return res.json({
      success: true,
      rows: r.rows
    });

  } catch (e) {
    console.error("REQUEST-LIST ERROR:", e);
    return res.status(500).json({
      success: false,
      error: e.message
    });
  }
});
app.post("/zvk-save", async (req, res) => {
  try {
    const {
      id_ft,
      zvk_row_id,
      user_name,
      to_pay,
      request_flag,
      login,
      is_admin,
      is_all,
      can_edit_all
    } = req.body || {};

    if (!id_ft) {
      return res.status(400).json({ success:false, error:"id_ft is required" });
    }

    const actor = String(login || user_name || "").trim();
    if (!actor) {
      return res.status(400).json({ success:false, error:"login required" });
    }

    const adminOk =
      isTruthy(is_admin) ||
      isTruthy(is_all) ||
      isTruthy(can_edit_all) ||
      actor.toLowerCase() === "b_erkin";

    const ft = String(id_ft).trim();
   let flag = String(request_flag || "Нет").trim();

if (!["Да", "Нет", "Обнуление"].includes(flag)) {
  flag = "Нет";
}

// ✅ ЖЁСТКО: если пришло Нет — сумма 0 и имя СИСТЕМА
const isNoRequest = flag === "Нет";

const toPayNum = isNoRequest
  ? 0
  : (
      to_pay === "" || to_pay === undefined || to_pay === null
        ? 0
        : Number(to_pay)
    );

    if (Number.isNaN(toPayNum)) {
      return res.status(400).json({ success:false, error:"to_pay must be number" });
    }

    // ✅ ГЛАВНОЕ: если пришёл zvk_row_id — обновляем выбранную строку, не создаём новую
    if (zvk_row_id) {
      const rid = Number(zvk_row_id);

      if (!rid || Number.isNaN(rid)) {
        return res.status(400).json({ success:false, error:"bad zvk_row_id" });
      }

      if (!adminOk) {
        const ok = await canEditRowByLogin(pool, rid, actor);
        if (!ok) {
          return res.status(403).json({ success:false, error:"NO_RIGHTS_THIS_ROW" });
        }
      }

      const finalName = flag === "Нет"
        ? "СИСТЕМА"
        : String(user_name || actor || "СИСТЕМА").trim();

      const upd = await pool.query(`
        UPDATE public.zvk
           SET request_flag = $1,
               to_pay       = $2,
               zvk_name     = $3,
               zvk_date     = NOW()
         WHERE id = $4
         RETURNING id, id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag
      `, [
        flag,
        toPayNum,
        finalName,
        rid
      ]);

      if (!upd.rows.length) {
        return res.status(404).json({
          success:false,
          error:"zvk_row_id not found"
        });
      }

      // ✅ если Заявка = Нет, очищаем источник объект
      if (flag === "Нет") {
        await pool.query(`
          INSERT INTO public.zvk_status (zvk_row_id, src_o, status_time)
          VALUES ($1, '', NOW())
          ON CONFLICT (zvk_row_id)
          DO UPDATE SET
            src_o = '',
            status_time = NOW()
        `, [rid]);
      }

      return res.json({
        success: true,
        updated: true,
        row: upd.rows[0],
        id_zvk: upd.rows[0].id_zvk,
        zvk_row_id: upd.rows[0].id
      });
    }

    // ✅ ниже старая логика создания новой строки, если zvk_row_id не пришёл
    if (!adminOk) {
      const ok = await canEditFtByLogin(pool, id_ft, actor);
      if (!ok) {
        return res.status(403).json({ success:false, error:"NO_RIGHTS_THIS_FT" });
      }
    }

    const exists = await pool.query(
      `SELECT 1 FROM public.zvk WHERE id_ft = $1 LIMIT 1`,
      [ft]
    );

    const isFirst = exists.rowCount === 0;

    let finalName;
    let finalToPay;
    let finalFlag;

    if (isFirst) {
      finalName = "СИСТЕМА";
      finalToPay = 0;
      finalFlag = "Нет";
    } else if (flag === "Нет") {
      finalName = "СИСТЕМА";
      finalToPay = 0;
      finalFlag = "Нет";
    } else {
      finalName = String(user_name || actor || "СИСТЕМА").trim();
      finalToPay = toPayNum;
      finalFlag = flag;
    }

    const lastCycle = await pool.query(
      `
      SELECT z.id_zvk
      FROM public.zvk z
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
        FROM public.zvk z
        WHERE z.id_zvk = $1
        ORDER BY z.zvk_date DESC NULLS LAST, z.id DESC
        LIMIT 1
        `,
        [id_zvk]
      );

      const lastRowId = lastRow.rows[0]?.id || null;

      if (lastRowId) {
        const paid = await pool.query(
          `SELECT is_paid FROM public.zvk_pay WHERE zvk_row_id = $1`,
          [Number(lastRowId)]
        );

        if (paid.rows[0]?.is_paid === "Да") {
          id_zvk = null;
        }
      }
    }

    if (!id_zvk) {
      const created = await pool.query(
        `SELECT 'ZFT' || nextval('public.zvk_id_seq')::text AS id_zvk`
      );
      id_zvk = created.rows[0].id_zvk;
    }

    const r = await pool.query(
      `
      INSERT INTO public.zvk
        (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
      VALUES
        ($1, $2, NOW(), $3, $4, $5)
      RETURNING id, id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag
      `,
      [id_zvk, ft, finalName, finalToPay, finalFlag]
    );

    return res.json({
      success: true,
      row: r.rows[0],
      id_zvk,
      zvk_row_id: r.rows[0].id
    });

  } catch (e) {
    console.error("ZVK-SAVE ERROR:", e);
    return res.status(500).json({ success:false, error:e.message });
  }
});
app.post("/zvk-bulk-request-flag", async (req, res) => {
  const client = await pool.connect();

  try {
    const { row_ids, request_flag, login, is_admin, is_all, can_edit_all } = req.body || {};

    const ids = Array.isArray(row_ids)
      ? row_ids.map(x => Number(x)).filter(Boolean)
      : [];

    const actor = String(login || "").trim();
    const flag = String(request_flag || "").trim();

    if (!ids.length) {
      return res.status(400).json({ success:false, error:"row_ids required" });
    }

    if (!actor) {
      return res.status(400).json({ success:false, error:"login required" });
    }

    if (!["Да", "Нет", "Обнуление"].includes(flag)) {
      return res.status(400).json({ success:false, error:"bad request_flag" });
    }

    const adminOk =
      isTruthy(is_admin) ||
      isTruthy(is_all) ||
      isTruthy(can_edit_all) ||
      actor.toLowerCase() === "b_erkin";

    if (!adminOk) {
      return res.status(403).json({ success:false, error:"NO_RIGHTS" });
    }

    await client.query("BEGIN");

    await client.query(`
      UPDATE public.zvk
      SET
        request_flag = $2,
        zvk_name = CASE
          WHEN $2 = 'Нет' THEN 'СИСТЕМА'
          ELSE $3
        END,
        to_pay = CASE
          WHEN $2 = 'Нет' THEN 0
          ELSE to_pay
        END,
        zvk_date = NOW()
      WHERE id = ANY($1::bigint[])
    `, [ids, flag, actor]);

    await client.query(`
      INSERT INTO public.zvk_status (zvk_row_id, src_o, status_time)
      SELECT x, CASE WHEN $2 = 'Нет' THEN '' ELSE COALESCE(s.src_o, '') END, NOW()
      FROM unnest($1::bigint[]) AS x
      LEFT JOIN public.zvk_status s ON s.zvk_row_id = x
      ON CONFLICT (zvk_row_id)
      DO UPDATE SET
        src_o = CASE WHEN $2 = 'Нет' THEN '' ELSE public.zvk_status.src_o END,
        status_time = NOW()
    `, [ids, flag]);

    await client.query("COMMIT");

    return res.json({
      success:true,
      updated: ids.length,
      request_flag: flag
    });

  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}
    console.error("zvk-bulk-request-flag error:", e);
    return res.status(500).json({ success:false, error:e.message });
  } finally {
    client.release();
  }
});

// =====================================================
// ✅ Источник по строке истории
// POST /zvk-status-row  { zvk_row_id, src_d, src_o }
// =====================================================
app.post("/zvk-status-row", async (req, res) => {
  try {
    const { zvk_row_id, src_d, src_o, status_comment, login, is_admin, can_edit_all, is_all } = req.body;

    const rid = Number(zvk_row_id);
    if (isNaN(rid)) {
      return res.status(400).json({ success: false, error: "zvk_row_id must be a number" });
    }

    // Проверка прав
    const actor = String(login || "").trim();
    const adminOk =
  isTruthy(is_admin) ||
  isTruthy(can_edit_all) ||
  String(is_all || "0") === "1" ||
  actor.toLowerCase() === "b_erkin";
    if (!adminOk) {
      const ok = await canEditRowByLogin(pool, rid, actor);
      if (!ok) return res.status(403).json({ success: false, error: "NO_RIGHTS_THIS_ROW" });
    }

    const hasStatusComment = Object.prototype.hasOwnProperty.call(req.body, "status_comment");

    const result = await pool.query(
      `
      INSERT INTO zvk_status (zvk_row_id, status_time, src_d, src_o, status_comment)
      VALUES ($1, NOW(), $2, $3, $4)
      ON CONFLICT (zvk_row_id)
      DO UPDATE SET
        status_time = NOW(),
        src_d = CASE
                  WHEN EXCLUDED.src_d IS NULL THEN NULL
                  ELSE COALESCE(EXCLUDED.src_d, zvk_status.src_d)
                END,
        src_o = CASE
                  WHEN EXCLUDED.src_o IS NULL THEN NULL
                  ELSE COALESCE(EXCLUDED.src_o, zvk_status.src_o)
                END,
        status_comment = CASE
                           WHEN $5 THEN EXCLUDED.status_comment
                           ELSE zvk_status.status_comment
                         END
      RETURNING *
      `,
      [rid, src_d ?? null, src_o ?? null, hasStatusComment ? String(status_comment || "") : null, hasStatusComment]
    );

    res.json({ success: true, row: result.rows[0] });
  } catch (e) {
    console.error("ZVK-STATUS-ROW ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
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
      VALUES ($1, $2, NOW(), 'СИСТЕМА', $3, 'Нет')
      RETURNING id, id_zvk
      `,
      [newIdZvk, ft, remaining]
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
    const { is_admin, zvk_row_id, registry_flag, is_paid, login } = req.body;

    const actor = String(login || "").trim().toLowerCase();

    const adminOk =
      is_admin === true || is_admin === 1 || is_admin === "1" ||
      String(is_admin).toLowerCase() === "true";

    const payOk = adminOk || actor === "b_erkin";

    if (!payOk) {
      return res.status(403).json({ success:false, error:"only b_erkin/admin allowed" });
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

    
// ✅ если Реестр = Обнуление -> очистить Источник Объект
if (reg === "Обнуление") {
  await client.query(`
    INSERT INTO public.zvk_status (zvk_row_id, src_o, status_time)
    VALUES ($1, '', NOW())
    ON CONFLICT (zvk_row_id)
    DO UPDATE SET
      src_o = '',
      status_time = NOW()
  `, [Number(zvk_row_id)]);
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
UPDATE zvk z
SET
  zvk_name = 'СИСТЕМА',
  to_pay = COALESCE(f.sum_ft, 0),
  request_flag = 'Нет',
  zvk_date = NOW()
FROM ft f
WHERE z.id = $1
  AND f.id_ft = z.id_ft
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

    // active = без оплаченных
    // paid = только оплаченные
    // all = все
    const paidMode = String(req.query.paid || "active").trim();

    if (!login) {
      return res.status(400).json({ success:false, error:"login is required" });
    }

    const where = [];
    const params = [];

// ✅ Разделение оплаченных только для Админа
if (isAdmin || isAll) {
  if (paidMode === "paid") {
    // ✅ Оплаченные: только Оплачено = Да
    // ❌ Реестр = Обнуление сюда НЕ входит
    where.push(`(
      COALESCE(v.is_paid, '') = 'Да'
      AND COALESCE(v.registry_flag, '') <> 'Обнуление'
    )`);
  } else if (paidMode === "reset") {
    // ✅ Обнуленные: любые Заявка, но Реестр = Обнуление
    where.push(`(
      COALESCE(v.registry_flag, '') = 'Обнуление'
    )`);
  } else {
    // ✅ Обычная таблица: активные, без оплаченных и без обнуленных реестров
    where.push(`(
      COALESCE(v.is_paid, '') <> 'Да'
      AND COALESCE(v.registry_flag, '') <> 'Обнуление'
    )`);
  }
}

// ✅ Для инициатора/оператора НЕ фильтруем оплаченные вообще

    if (!(isAdmin || isAll || isOperator)) {
      params.push(login);
      where.push(`lower(trim(v.input_name)) = lower(trim($${params.length}))`);
    }


    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

const query = `
  SELECT v.*
  FROM public.ft_zvk_current_v2 v
  ${whereSql}
  ORDER BY
    COALESCE(NULLIF(substring(v.id_ft from '\\d+'), ''), '0')::int DESC,
    v.zvk_date DESC NULLS LAST,
    v.zvk_row_id DESC
`;

    const r = await pool.query(query, params);

    return res.json({
      success: true,
      rows: r.rows,
      paidMode
    });

  } catch (e) {
    console.error("FT-ZVK-JOIN ERROR:", e);
    return res.status(500).json({ success:false, error:e.message });
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

// =====================================================
// СОЗДАНИЕ ВХ / ИСХ + ИСТОРИЯ
// =====================================================
app.post("/io-save", async (req, res) => {
  const client = await pool.connect();

  try {
    const rows = Array.isArray(req.body?.rows) ? req.body.rows : [];

    if (!rows.length) {
      return res.status(400).json({
        success: false,
        error: "rows required"
      });
    }

    await client.query("BEGIN");

    let inserted = 0;

    for (const row of rows) {
      const inputDate = String(row?.input_date || "").trim();
      const objectName = String(row?.object || "").trim();
      const divIn = String(row?.div_in || "").trim();
      const ddsIn = String(row?.dds_in || "").trim();
      const divOut = String(row?.div_out || "").trim();
      const ddsOut = String(row?.dds_out || "").trim();

      const sumValue = Number(
        String(row?.sum || "")
          .replace(/\s/g, "")
          .replace(",", ".")
      );

      if (!Number.isFinite(sumValue) || sumValue <= 0) {
        throw new Error("Введите правильную сумму");
      }

      if (!objectName) {
        throw new Error("Источник Объект не указан");
      }

      if (!divIn) {
        throw new Error("Дивизион Вх не указан");
      }

      if (!ddsIn) {
        throw new Error("ДДСвх не указан");
      }

      if (!divOut) {
        throw new Error("Дивизион Исх не указан");
      }

      if (!ddsOut) {
        throw new Error("ДДСисх не указан");
      }

      // 1. Сначала создаём историю
      const historyResult = await client.query(
        `
        INSERT INTO public.io_history
        (
          input_date_text,
          sum_value,
          object_name,
          div_in,
          dds_in,
          div_out,
          dds_out
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7)
        RETURNING id
        `,
        [
          inputDate,
          sumValue,
          objectName,
          divIn,
          ddsIn,
          divOut,
          ddsOut
        ]
      );

      const historyId = Number(historyResult.rows[0].id);

      // 2. Создаём ВХ
      await client.query(
        `
        INSERT INTO public.prihod6
        (
          amount_in,
          object_name,
          division_in,
          dds_in,
          io_history_id
        )
        VALUES ($1,$2,$3,$4,$5)
        `,
        [
          sumValue,
          objectName,
          divIn,
          ddsIn,
          historyId
        ]
      );

      // 3. Создаём ИСХ
      await client.query(
        `
        INSERT INTO public.perevod7
        (
          amount_out,
          object_name,
          division_out,
          dds_out,
          io_history_id
        )
        VALUES ($1,$2,$3,$4,$5)
        `,
        [
          sumValue,
          objectName,
          divOut,
          ddsOut,
          historyId
        ]
      );

      inserted++;
    }

    await client.query("COMMIT");

    return res.json({
      success: true,
      inserted
    });

  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}

    console.error("IO-SAVE ERROR:", e);

    return res.status(500).json({
      success: false,
      error: e.message
    });

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

// =====================================================
// ИЗМЕНЕНИЕ ВХ / ИСХ ИЗ ИСТОРИИ
// Работает и для новых, и для старых записей
// =====================================================
app.put("/io-history/:id", async (req, res) => {
  const client = await pool.connect();

  try {
    const historyId = Number(req.params.id);
    const login = String(req.body?.login || "").trim();

    if (!Number.isInteger(historyId) || historyId <= 0) {
      return res.status(400).json({
        success: false,
        error: "Некорректный ID записи"
      });
    }

    if (!login) {
      return res.status(400).json({
        success: false,
        error: "Логин не передан"
      });
    }

    const userResult = await client.query(
      `
      SELECT login, role_ft, is_active
      FROM public.users
      WHERE lower(trim(login)) = lower(trim($1))
      LIMIT 1
      `,
      [login]
    );

    if (!userResult.rows.length) {
      return res.status(404).json({
        success: false,
        error: "Пользователь не найден"
      });
    }

    const user = userResult.rows[0];

    if (user.is_active === false) {
      return res.status(403).json({
        success: false,
        error: "Пользователь отключён"
      });
    }

    const role = String(user.role_ft || "")
      .trim()
      .toLowerCase();

    const actorLogin = String(user.login || "")
      .trim()
      .toLowerCase();

    const canEdit =
      role === "admin" ||
      role === "админ" ||
      role === "администратор" ||
      actorLogin === "b_erkin";

    if (!canEdit) {
      return res.status(403).json({
        success: false,
        error: "Нет доступа на изменение"
      });
    }

    const inputDate = String(req.body?.input_date || "").trim();
    const objectName = String(req.body?.object || "").trim();
    const divIn = String(req.body?.div_in || "").trim();
    const ddsIn = String(req.body?.dds_in || "").trim();
    const divOut = String(req.body?.div_out || "").trim();
    const ddsOut = String(req.body?.dds_out || "").trim();

    const sumValue = Number(
      String(req.body?.sum || "")
        .replace(/\s/g, "")
        .replace(",", ".")
    );

    if (!Number.isFinite(sumValue) || sumValue <= 0) {
      return res.status(400).json({
        success: false,
        error: "Введите правильную сумму"
      });
    }

    if (!objectName) {
      return res.status(400).json({
        success: false,
        error: "Источник Объект не указан"
      });
    }

    if (!divIn || !ddsIn || !divOut || !ddsOut) {
      return res.status(400).json({
        success: false,
        error: "Заполните все поля"
      });
    }

    await client.query("BEGIN");

    // Берём старые значения до изменения
    const oldResult = await client.query(
      `
      SELECT
        id,
        created_at,
        input_date_text,
        sum_value,
        object_name,
        div_in,
        dds_in,
        div_out,
        dds_out
      FROM public.io_history
      WHERE id = $1
      LIMIT 1
      `,
      [historyId]
    );

    if (!oldResult.rows.length) {
      await client.query("ROLLBACK");

      return res.status(404).json({
        success: false,
        error: "Запись истории не найдена"
      });
    }

    const old = oldResult.rows[0];

    // =================================================
    // 1. ПРИХОД
    // Сначала пробуем по io_history_id
    // =================================================
    let prihodResult = await client.query(
      `
      UPDATE public.prihod6
      SET
        amount_in = $1,
        object_name = $2,
        division_in = $3,
        dds_in = $4
      WHERE io_history_id = $5
      `,
      [
        sumValue,
        objectName,
        divIn,
        ddsIn,
        historyId
      ]
    );

    // Если старая запись и io_history_id пустой
    if (prihodResult.rowCount === 0) {
      prihodResult = await client.query(
        `
        UPDATE public.prihod6
        SET
          amount_in = $1,
          object_name = $2,
          division_in = $3,
          dds_in = $4,
          io_history_id = $5
        WHERE ctid = (
          SELECT ctid
          FROM public.prihod6
          WHERE io_history_id IS NULL
            AND COALESCE(amount_in, 0) = COALESCE($6::numeric, 0)
            AND trim(COALESCE(object_name, '')) =
                trim(COALESCE($7::text, ''))
            AND trim(COALESCE(division_in, '')) =
                trim(COALESCE($8::text, ''))
            AND trim(COALESCE(dds_in, '')) =
                trim(COALESCE($9::text, ''))
          ORDER BY
            ABS(
              EXTRACT(
                EPOCH FROM (
                  doc_time - COALESCE($10::timestamptz, doc_time)
                )
              )
            ) ASC,
            doc_time DESC
          LIMIT 1
        )
        `,
        [
          sumValue,
          objectName,
          divIn,
          ddsIn,
          historyId,

          old.sum_value,
          old.object_name,
          old.div_in,
          old.dds_in,
          old.created_at
        ]
      );
    }

    // =================================================
    // 2. ПЕРЕВОД
    // Сначала пробуем по io_history_id
    // =================================================
    let perevodResult = await client.query(
      `
      UPDATE public.perevod7
      SET
        amount_out = $1,
        object_name = $2,
        division_out = $3,
        dds_out = $4
      WHERE io_history_id = $5
      `,
      [
        sumValue,
        objectName,
        divOut,
        ddsOut,
        historyId
      ]
    );

    // Если старая запись и io_history_id пустой
    if (perevodResult.rowCount === 0) {
      perevodResult = await client.query(
        `
        UPDATE public.perevod7
        SET
          amount_out = $1,
          object_name = $2,
          division_out = $3,
          dds_out = $4,
          io_history_id = $5
        WHERE ctid = (
          SELECT ctid
          FROM public.perevod7
          WHERE io_history_id IS NULL
            AND COALESCE(amount_out, 0) = COALESCE($6::numeric, 0)
            AND trim(COALESCE(object_name, '')) =
                trim(COALESCE($7::text, ''))
            AND trim(COALESCE(division_out, '')) =
                trim(COALESCE($8::text, ''))
            AND trim(COALESCE(dds_out, '')) =
                trim(COALESCE($9::text, ''))
          ORDER BY
            ABS(
              EXTRACT(
                EPOCH FROM (
                  doc_time - COALESCE($10::timestamptz, doc_time)
                )
              )
            ) ASC,
            doc_time DESC
          LIMIT 1
        )
        `,
        [
          sumValue,
          objectName,
          divOut,
          ddsOut,
          historyId,

          old.sum_value,
          old.object_name,
          old.div_out,
          old.dds_out,
          old.created_at
        ]
      );
    }

    if (
      prihodResult.rowCount === 0 ||
      perevodResult.rowCount === 0
    ) {
      await client.query("ROLLBACK");

      return res.status(404).json({
        success: false,
        error:
          "Связанные строки в prihod6 или perevod7 не найдены. " +
          "История и база не были изменены.",
        updated: {
          prihod: prihodResult.rowCount,
          perevod: perevodResult.rowCount
        }
      });
    }

    // Историю меняем только после успешного изменения базы
    await client.query(
      `
      UPDATE public.io_history
      SET
        input_date_text = $1,
        sum_value = $2,
        object_name = $3,
        div_in = $4,
        dds_in = $5,
        div_out = $6,
        dds_out = $7
      WHERE id = $8
      `,
      [
        inputDate,
        sumValue,
        objectName,
        divIn,
        ddsIn,
        divOut,
        ddsOut,
        historyId
      ]
    );

    await client.query("COMMIT");

    return res.json({
      success: true,
      message: "История и база обновлены",
      updated: {
        history: 1,
        prihod: prihodResult.rowCount,
        perevod: perevodResult.rowCount
      }
    });

  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}

    console.error("IO-HISTORY UPDATE ERROR:", e);

    return res.status(500).json({
      success: false,
      error: e.message
    });

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
    console.error("DIVISION-SVOD ERROR:", e);
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



app.get("/svod-object", async (req, res) => {
  const client = await pool.connect();

  try {
    const r = await client.query(`
      SELECT
        object_name,
        amount_in,
        to_pay,
        balance,

        -- новые поля
        ft_kasenov,
        balance_kasenov,

        registry,
        balance_registry,
        ft_zayavka,
        balance_zayavka
      FROM public.svod_object_v1
      ORDER BY object_name
    `);

    res.json({ ok: true, rows: r.rows });

  } catch (e) {
    console.error("SVOD-OBJECT ERROR:", e);
    res.status(500).json({ ok: false, error: String(e.message || e) });
  } finally {
    client.release();
  }
});






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

async function setRequestRegistryYes(client, request_id) {
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
      agree_time = COALESCE(public.zvk_pay.agree_time, NOW())
  `, [request_id]);

  const items = await client.query(`
    SELECT zvk_row_id
    FROM public.request_items
    WHERE request_id = $1
      AND zvk_row_id IS NOT NULL
  `, [request_id]);

  for (const row of items.rows) {
    await rebuildFtTail(client, Number(row.zvk_row_id));
  }
}

app.get("/request-card", async (req, res) => {
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
        request_no,
        request_date,
        created_by,
        total_amount,
        items_count,
        created_at,

        acc_zhasulan_status,
        acc_zhasulan_time,
        acc_zhasulan_comment,

        acc_shevchenko_status,
        acc_shevchenko_time,
        acc_shevchenko_comment,

        acc_marat_status,
        acc_marat_time,
        acc_marat_comment,

        acc_ermek_status,
        acc_ermek_time,
        acc_ermek_comment,

        approve_ermek_status,
        approve_ermek_time,
        approve_ermek_comment
      FROM public.request_head
      WHERE id = $1
      LIMIT 1
    `, [id]);

    if (!headRes.rowCount) {
      return res.status(404).json({
        success: false,
        error: "Заявка не найдена"
      });
    }

    const itemsRes = await pool.query(`
      SELECT
        i.id,
        i.request_id,
        i.zvk_row_id,
        i.id_ft,
        i.id_zvk,
        i.object,
        i.division,
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

        COALESCE(cur.request_flag, '') AS request_flag,
        COALESCE(cur.registry_flag, '') AS registry_flag,
        COALESCE(cur.is_paid, '') AS is_paid
      FROM public.request_items i
      LEFT JOIN public.ft_zvk_current_v2 cur
        ON cur.zvk_row_id = i.zvk_row_id
      WHERE i.request_id = $1
      ORDER BY i.id ASC
    `, [id]);

    return res.json({
      success: true,
      head: headRes.rows[0],
      items: itemsRes.rows
    });

  } catch (e) {
    console.error("REQUEST-CARD ERROR:", e);
    return res.status(500).json({
      success: false,
      error: e.message
    });
  }
});

app.post("/approve-rows", async (req, res) => {
  const client = await pool.connect();

  try {
    const request_id = Number(req.body?.request_id || req.body?.id);
    const loginNorm = String(req.body?.login || "").trim().toLowerCase();
    const action = String(req.body?.action || "agree").trim().toLowerCase();
    const comment = String(req.body?.comment || "").trim();

    if (!request_id) {
      return res.status(400).json({ success:false, error:"request_id required" });
    }

    if (!loginNorm) {
      return res.status(400).json({ success:false, error:"login required" });
    }

    const agreeApprovers = {
      s_zhasulan: {
        title: "Сулейменов Жасулан",
        nameCol: "acc_zhasulan_name",
        statusCol: "acc_zhasulan_status",
        timeCol: "acc_zhasulan_time",
        commentCol: "acc_zhasulan_comment"
      },

      v_shevchenko: {
        title: "Шевченко Владимир",
        nameCol: "acc_shevchenko_name",
        statusCol: "acc_shevchenko_status",
        timeCol: "acc_shevchenko_time",
        commentCol: "acc_shevchenko_comment"
      },

      k_marat: {
        title: "Койлибаев Марат",
        nameCol: "acc_marat_name",
        statusCol: "acc_marat_status",
        timeCol: "acc_marat_time",
        commentCol: "acc_marat_comment"
      },

      k_ermek: {
        title: "Касенов Ермек",
        nameCol: "acc_ermek_name",
        statusCol: "acc_ermek_status",
        timeCol: "acc_ermek_time",
        commentCol: "acc_ermek_comment"
      }
    };

    const approveApprovers = {
      k_ermek: {
        title: "Касенов Ермек",
        nameCol: "approve_ermek_name",
        statusCol: "approve_ermek_status",
        timeCol: "approve_ermek_time",
        commentCol: "approve_ermek_comment"
      }
    };

    let a = null;
    let stageName = "";

    if (action === "agree" || action === "reject_agree") {
      a = agreeApprovers[loginNorm];
      stageName = "Согласование";
    }

    if (action === "approve" || action === "reject_approve") {
      a = approveApprovers[loginNorm];
      stageName = "Утверждение";
    }

    if (!a) {
      return res.status(403).json({
        success:false,
        error:"Нет прав на это действие"
      });
    }

    await client.query("BEGIN");

    const exists = await client.query(`
      SELECT
        id,
        request_no,
        total_amount,
        acc_zhasulan_status
      FROM public.request_head
      WHERE id = $1
      LIMIT 1
    `, [request_id]);

    if (!exists.rowCount) {
      throw new Error("Заявка не найдена");
    }

    const headRow = exists.rows[0];

    if (
      loginNorm !== "s_zhasulan" &&
      (
        action === "agree" ||
        action === "reject_agree" ||
        action === "approve" ||
        action === "reject_approve"
      ) &&
      String(headRow.acc_zhasulan_status || "").trim() !== "Согласовано"
    ) {
      throw new Error("Сначала должен согласовать Сулейменов Жасулан");
    }

    const isReject = action === "reject_agree" || action === "reject_approve";
    const statusText = isReject ? "Отклонено" : "Согласовано";

    await client.query(`
      UPDATE public.request_head
      SET
        ${a.nameCol} = $2,
        ${a.statusCol} = $3,
        ${a.timeCol} = NOW(),
        ${a.commentCol} = $4
      WHERE id = $1
    `, [
      request_id,
      a.title,
      statusText,
      comment
    ]);

    await client.query(`
      INSERT INTO public.request_approve_log
        (request_id, stage_name, approver_login, approver_name, action_type, comment_text)
      VALUES ($1, $2, $3, $4, $5, $6)
    `, [
      request_id,
      stageName,
      loginNorm,
      a.title,
      action,
      comment
    ]);

    if (
      loginNorm === "s_zhasulan" &&
      action === "agree"
    ) {
      const notifyUsersAfterZhasulan = [
        "v_shevchenko",
        "k_marat",
        "k_ermek"
      ];

      for (const userLogin of notifyUsersAfterZhasulan) {
        await client.query(`
          INSERT INTO public.notifications
            (
              user_login,
              type,
              title,
              message,
              entity_id,
              entity_page,
              is_read,
              created_at
            )
          VALUES
            ($1, 'request', $2, $3, $4, 'request_card', false, NOW())
        `, [
          userLogin,
          `Заявка №${headRow.request_no} согласована Жасуланом`,
          `Заявка доступна для согласования. Сумма: ${Number(headRow.total_amount || 0).toLocaleString("ru-RU")} ₸`,
          request_id
        ]);
      }
    }

    // Согласование или утверждение Ермека => авто Реестр = Да
    if (
      action === "agree" ||
      (action === "approve" && loginNorm === "k_ermek")
    ) {
      await setRequestRegistryYes(client, request_id);
    }

    await client.query("COMMIT");

    return res.json({
      success:true,
      request_id,
      login: loginNorm,
      action,
      status: statusText,
      registry_flag: (
        action === "agree" ||
        (action === "approve" && loginNorm === "k_ermek")
      ) ? "Да" : ""
    });

  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}

    console.error("approve-rows error:", e);

    return res.status(500).json({
      success:false,
      error:e.message
    });

  } finally {
    client.release();
  }
});

app.post("/request-items-paid-bulk", async (req, res) => {
  const client = await pool.connect();

  try {
    const request_id = Number(req.body?.request_id);

    const row_ids = Array.isArray(req.body?.row_ids)
      ? req.body.row_ids.map(Number).filter(Boolean)
      : [];

    const loginNorm = String(req.body?.login || "").trim().toLowerCase();
    const paidValue = String(req.body?.is_paid || "").trim();

    if (!request_id) {
      return res.status(400).json({
        success: false,
        error: "request_id required"
      });
    }

    if (!row_ids.length) {
      return res.status(400).json({
        success: false,
        error: "row_ids required"
      });
    }

    if (!["Да", "Нет"].includes(paidValue)) {
      return res.status(400).json({
        success: false,
        error: "is_paid must be Да or Нет"
      });
    }

    // ✅ Оплату могут ставить только эти пользователи
    const canPay =
      loginNorm === "zh_elena" ||
      loginNorm === "k_arailym" ||
      loginNorm === "s_zhasulan" ||
      loginNorm === "b_erkin" ||
      loginNorm === "admin";

    if (!canPay) {
      return res.status(403).json({
        success: false,
        error: "Нет прав ставить Оплачено"
      });
    }

    await client.query("BEGIN");

    const head = await client.query(`
      SELECT approve_ermek_status
      FROM public.request_head
      WHERE id = $1
      LIMIT 1
    `, [request_id]);

    if (!head.rowCount) {
      throw new Error("Заявка не найдена");
    }

    const approveStatus = String(head.rows[0].approve_ermek_status || "").trim();

    if (!["Согласовано", "Утверждено", "Да"].includes(approveStatus)) {
      throw new Error("Оплачено можно ставить только после утверждения Ермека");
    }

    await client.query(`
      INSERT INTO public.zvk_pay
        (zvk_row_id, is_paid, pay_time)
      SELECT
        i.zvk_row_id,
        CASE WHEN $3 = 'Да' THEN 'Да' ELSE NULL END,
        CASE WHEN $3 = 'Да' THEN NOW() ELSE NULL END
      FROM public.request_items i
      WHERE i.request_id = $1
        AND i.zvk_row_id = ANY($2::bigint[])
        AND i.zvk_row_id IS NOT NULL
      ON CONFLICT (zvk_row_id)
      DO UPDATE SET
        is_paid = EXCLUDED.is_paid,
        pay_time = EXCLUDED.pay_time
    `, [request_id, row_ids, paidValue]);

    await client.query("COMMIT");

    return res.json({
      success: true,
      request_id,
      paid: paidValue,
      updated: row_ids.length
    });

  } catch (e) {
    try { await client.query("ROLLBACK"); } catch (_) {}

    console.error("request-items-paid-bulk error:", e);

    return res.status(500).json({
      success: false,
      error: e.message
    });

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
app.post("/notifications/read-all", async (req, res) => {
  try {
    const { login } = req.body || {};

    if (!login) {
      return res.status(400).json({
        success:false,
        error:"login required"
      });
    }

    await pool.query(`
      UPDATE public.notifications
      SET is_read = true
      WHERE lower(trim(user_login)) = lower(trim($1))
    `, [String(login).trim()]);

    return res.json({ success:true });

  } catch (e) {
    console.error("READ ALL ERROR:", e);
    return res.status(500).json({
      success:false,
      error:e.message
    });
  }
});
app.post("/notifications/read", async (req, res) => {
  try {
    const { id, login } = req.body || {};

    if (!id || !login) {
      return res.status(400).json({
        success:false,
        error:"id and login required"
      });
    }

    await pool.query(`
      UPDATE public.notifications
      SET is_read = true
      WHERE id = $1
        AND lower(trim(user_login)) = lower(trim($2))
    `, [
      Number(id),
      String(login).trim()
    ]);

    return res.json({ success:true });

  } catch (e) {
    console.error("READ ONE ERROR:", e);
    return res.status(500).json({
      success:false,
      error:e.message
    });
  }
});

app.get("/matrix-sources", async (req, res) => {
  try {
    const { date_from, date_to } = req.query;

    const params = [];
    let where = `
      WHERE COALESCE(TRIM(src_o), '') <> ''
        AND COALESCE(TRIM(object), '') <> ''
        AND COALESCE(TRIM(is_paid), '') = 'Да'
    `;

    if (date_from) {
      params.push(date_from);
      where += ` AND pay_time::date >= $${params.length}::date`;
    }

    if (date_to) {
      params.push(date_to);
      where += ` AND pay_time::date <= $${params.length}::date`;
    }

    const result = await pool.query(`
      SELECT
        id_ft,
        id_zvk,
        input_date,
        zvk_date,
        pay_time,

        division,
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
        to_pay,
        request_flag,
        status_comment,
        chief_approved,
        registry_flag,
        is_paid

      FROM public.ft_zvk_current_v2
      ${where}
      ORDER BY 
        pay_time DESC NULLS LAST,
        object,
        src_o,
        contractor
    `, params);

    res.json({ success:true, rows: result.rows });

  } catch (e) {
    console.error("matrix-sources error:", e);
    res.status(500).json({ success:false, error:e.message });
  }
});


// =====================================================
// Start
// =====================================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT))