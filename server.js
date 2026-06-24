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

// =====================================================
// ИНТЕГРАЦИЯ С 1С
// Таблицы заранее создаются при запуске сервера
// =====================================================

// 1. Шапка документа поступления
await pool.query(`
  CREATE TABLE IF NOT EXISTS public.doc_receipts (
    document_id text PRIMARY KEY,
    base_id text,
    document_number text,
    document_posted boolean,
    document_date timestamptz,

    organization_bin text,
    organization_name text,

    warehouse_id text,
    warehouse_name text,

    counterparty_id text,
    counterparty_bin text,
    counterparty_name text,

    contract_id text,
    contract_name text,

    currency_name text,
    income_kpn text,
    settlement_account text,
    advance_account text,

    vat_enable boolean,
    vat_mode text,

    document_sum numeric(18,2),
    document_commentary text,
    document_author_name text,
    document_type text,

    advance_withheld numeric(18,2),
    guarantee_withheld numeric(18,2),
    penalty_withheld numeric(18,2),
    other_withheld numeric(18,2),

    target_entity text,
    action_required boolean,
    is_executed boolean,
    is_managerial boolean,

    id_dov text,
    deleted boolean DEFAULT false,

    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
  );
`);

// 2. Товары документа
await pool.query(`
  CREATE TABLE IF NOT EXISTS public.doc_items (
    document_id text NOT NULL,
    item_id text NOT NULL,

    item_name text,
    quantity numeric(18,6),
    price numeric(18,2),
    amount numeric(18,2),

    vat_percent numeric(10,4),
    vat_amount numeric(18,2),
    amount_with_vat numeric(18,2),

    vat_account text,
    turnover_type text,
    receipt_type_name text,

    cost_account_bu text,
    cost_account_nu text,
    in_group text,

    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),

    PRIMARY KEY (document_id, item_id),

    CONSTRAINT doc_items_document_fk
      FOREIGN KEY (document_id)
      REFERENCES public.doc_receipts(document_id)
      ON DELETE CASCADE
  );
`);

// 3. Услуги документа
await pool.query(`
  CREATE TABLE IF NOT EXISTS public.doc_services (
    document_id text NOT NULL,
    service_id text NOT NULL,

    service_name text,
    service_content text,

    quantity numeric(18,6),
    price numeric(18,2),
    amount numeric(18,2),

    vat_percent numeric(10,4),
    vat_amount numeric(18,2),
    amount_with_vat numeric(18,2),

    vat_account text,
    turnover_type text,
    receipt_type_name text,

    cost_account_bu text,
    cost_account_nu text,

    project_id text,
    project_name text,

    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now(),

    PRIMARY KEY (document_id, service_id),

    CONSTRAINT doc_services_document_fk
      FOREIGN KEY (document_id)
      REFERENCES public.doc_receipts(document_id)
      ON DELETE CASCADE
  );
`);

// 4. Контрагенты
await pool.query(`
  CREATE TABLE IF NOT EXISTS public.ref_counterparties (
    counterparty_id text PRIMARY KEY,
    counterparty_name text,
    individual_or_legal text,
    group_name text,
    counterparty_bin text,
    counterparty_kbe text,
    is_government_institution boolean,
    is_small_retail_outlet boolean,
    residence_country text,
    vat_series text,
    vat_number text,
    vat_date date,
    bank_account text,
    bank_name text,
    counterparty_comment text,
    deleted boolean DEFAULT false,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
  );
`);

// 5. Склады
await pool.query(`
  CREATE TABLE IF NOT EXISTS public.ref_warehouses (
    warehouse_id text PRIMARY KEY,
    warehouse_name text,
    warehouse_comment text,
    deleted boolean DEFAULT false,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
  );
`);

// 6. Товары и услуги
await pool.query(`
  CREATE TABLE IF NOT EXISTS public.ref_products (
    product_id text PRIMARY KEY,
    product_code text,
    product_name text,
    is_group boolean,
    is_service boolean,
    article text,
    unit text,
    vat_percent numeric(10,4),
    tnvd_code text,
    kpvd_code text,
    nkt_code text,
    product_type text,
    product_group text,
    product_comment text,
    deleted boolean DEFAULT false,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
  );
`);

// 7. Договоры контрагентов
await pool.query(`
  CREATE TABLE IF NOT EXISTS public.ref_counterparties_contracts (
    contract_id text PRIMARY KEY,
    contract_number text,
    contract_date date,
    contract_name text,
    contract_type text,
    organization_name text,
    organization_bin text,
    counterparty_id text,
    counterparty_bin text,
    counterparty_name text,
    deleted boolean DEFAULT false,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
  );
`);

// 8. Проекты
await pool.query(`
  CREATE TABLE IF NOT EXISTS public.ref_project_groups (
    project_id text PRIMARY KEY,
    project_name text,
    deleted boolean DEFAULT false,
    created_at timestamptz DEFAULT now(),
    updated_at timestamptz DEFAULT now()
  );
`);

// =====================================================
// ДОПОЛНИТЕЛЬНЫЕ ИНДЕКСЫ ДЛЯ ПОИСКА
// =====================================================

await pool.query(`
  CREATE INDEX IF NOT EXISTS doc_receipts_document_date_idx
  ON public.doc_receipts (document_date);
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS doc_receipts_counterparty_id_idx
  ON public.doc_receipts (counterparty_id);
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS doc_receipts_warehouse_id_idx
  ON public.doc_receipts (warehouse_id);
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS doc_receipts_contract_id_idx
  ON public.doc_receipts (contract_id);
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS doc_items_item_id_idx
  ON public.doc_items (item_id);
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS doc_services_service_id_idx
  ON public.doc_services (service_id);
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS doc_services_project_id_idx
  ON public.doc_services (project_id);
`);

await pool.query(`
  CREATE INDEX IF NOT EXISTS ref_contracts_counterparty_id_idx
  ON public.ref_counterparties_contracts (counterparty_id);
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
    const body = req.body || {};
    const actor = String(body.login || "").trim().toLowerCase();
    const idFt = String(body.id_ft || "").trim();

    if (!idFt) {
      return res.status(400).json({ success:false, error:"id_ft required" });
    }

    // Доступ только b_erkin.
    if (actor !== "b_erkin") {
      return res.status(403).json({ success:false, error:"NO_RIGHTS" });
    }

    /*
     * ЗАЩИТА ОТ ОЧИСТКИ СТРОКИ:
     * массовое изменение Реестр/Оплачено не должно обновлять основные поля FT.
     * Даже если старый клиент повторно отправит пустую карточку, пустые строки
     * здесь НЕ заменят существующие значения в базе.
     */
    const textOrNull = (name) => {
      if (!Object.prototype.hasOwnProperty.call(body, name)) return null;
      const value = String(body[name] ?? "").trim();
      return value === "" ? null : value;
    };

    let sumValue = null;
    if (Object.prototype.hasOwnProperty.call(body, "sum_ft")) {
      const raw = String(body.sum_ft ?? "")
        .replace(/\s/g, "")
        .replace(",", ".")
        .trim();

      if (raw !== "") {
        const parsed = Number(raw);
        if (!Number.isFinite(parsed)) {
          return res.status(400).json({ success:false, error:"sum_ft must be number" });
        }
        sumValue = parsed;
      }
    }

    const r = await pool.query(`
      UPDATE public.ft
      SET
        division    = COALESCE($2, division),
        "object"    = COALESCE($3, "object"),
        contractor  = COALESCE($4, contractor),
        pay_purpose = COALESCE($5, pay_purpose),
        dds_article = COALESCE($6, dds_article),
        contract_no = COALESCE($7, contract_no),
        invoice_no  = COALESCE($8, invoice_no),
        sum_ft      = COALESCE($9, sum_ft)
      WHERE id_ft = $1
      RETURNING *
    `, [
      idFt,
      textOrNull("division"),
      textOrNull("object"),
      textOrNull("contractor"),
      textOrNull("pay_purpose"),
      textOrNull("dds_article"),
      textOrNull("contract_no"),
      textOrNull("invoice_no"),
      sumValue
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

const ISMAGULOV_LOGIN = "zhas";

const ISMAGULOV_OBJECTS = new Set([
  "05-М-Акм. Есиль",
  "32-М-АлмО. Подкова Алматы",
  "34-Д-Акм. Акколь",
  "46-М-Жет. Алмалы 145+950км",
  "47-М-Жет. Коктерек 2+708км",
  "48-М-Жет. Тюгельбай 5+250км",
  "49-М-Жет. Кабанбай 23+850км",
  "50-М-Жет. Койлык 6+890км",
  "51-М-Жет. Молалы 64+870км",
  "52-М-Жет. Карабулак 54+411км",
  "53-М-Жет. Тастобе 3+462км",
  "55-М-Жет. Сарыозек Обход",
  "57-Д-Акм. Макинск",
  "58-Д-Аст. Улица 37",
  "61-Д-Акм. Жалтырколь",
  "63-Д-Аст. Оренбургская",
  "64-Д-Жет. Хоргос",
  "67-М-Акт. Жем",
  "75-М-Крг. Шилы",
  "76-М-Крг. Шат",
  "77-М-Акт. Кауылжыр",
  "78-Д-Аст. Уркер"
]);

function normalizeRequestObject(value) {
  return String(value || "")
    .replace(/\u00A0/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function objectNeedsIsmagulov(value) {
  return ISMAGULOV_OBJECTS.has(normalizeRequestObject(value));
}

async function requestNeedsIsmagulov(client, requestId) {
  const result = await client.query(`
    SELECT object
    FROM public.request_items
    WHERE request_id = $1
  `, [Number(requestId)]);

  return result.rows.some(row => objectNeedsIsmagulov(row.object));
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

acc_zhas_name = 'Исмагулов Жаслан',
acc_zhas_status = CASE
  WHEN EXISTS (
    SELECT 1
    FROM public.request_items ri
    WHERE ri.request_id = $3
      AND ri.object = ANY($4::text[])
  )
  THEN 'Ожидает'
  ELSE 'Не требуется'
END,
acc_zhas_time = NULL,
acc_zhas_comment = NULL,

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
     `, [
  total,
  count,
  request_id,
  Array.from(ISMAGULOV_OBJECTS)
]);

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

    const login = String(req.query.login || "")
      .trim()
      .toLowerCase();

    const roleFt = String(
      req.query.role_ft ||
      req.query.role ||
      ""
    ).trim().toLowerCase();

    if (!login) {
      return res.status(400).json({
        success: false,
        error: "login required"
      });
    }

    let whereSql = "";
    const params = [];

    if (login === "s_zhasulan") {
      // Сулейменов видит все заявки.
      whereSql = "";

    } else if (login === ISMAGULOV_LOGIN) {
      // Исмагулов видит только свои объекты
      // и только после согласования Сулейменова.
      params.push(Array.from(ISMAGULOV_OBJECTS));

      whereSql = `
        WHERE COALESCE(acc_zhasulan_status, '') = 'Согласовано'
          AND EXISTS (
            SELECT 1
            FROM public.request_items ri
            WHERE ri.request_id = request_head.id
              AND ri.object = ANY($1::text[])
          )
      `;

    } else if (
      login === "v_shevchenko" ||
      login === "k_marat" ||
      login === "k_ermek"
    ) {
      /*
       * Для объектов Исмагулова:
       * сначала Сулейменов, потом Исмагулов.
       *
       * Для остальных объектов:
       * достаточно согласования Сулейменова.
       */
      params.push(Array.from(ISMAGULOV_OBJECTS));

      whereSql = `
        WHERE COALESCE(acc_zhasulan_status, '') = 'Согласовано'
          AND (
            NOT EXISTS (
              SELECT 1
              FROM public.request_items ri
              WHERE ri.request_id = request_head.id
                AND ri.object = ANY($1::text[])
            )
            OR COALESCE(acc_zhas_status, '') = 'Согласовано'
          )
      `;

    } else if (
      login === "admin" ||
      login === "b_erkin" ||
      roleFt === "admin" ||
      roleFt === "админ" ||
      roleFt === "administrator"
    ) {
      whereSql = "";

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

    const result = await pool.query(`
      SELECT
        id,
        request_no,
        request_date,
        created_by,
        total_amount,
        items_count,
        created_at,

        acc_zhasulan_name,
        acc_zhasulan_status,
        acc_zhasulan_time,
        acc_zhasulan_comment,

        acc_zhas_name,
        acc_zhas_status,
        acc_zhas_time,
        acc_zhas_comment,

        acc_shevchenko_name,
        acc_shevchenko_status,
        acc_shevchenko_time,
        acc_shevchenko_comment,

        acc_marat_name,
        acc_marat_status,
        acc_marat_time,
        acc_marat_comment,

        acc_ermek_name,
        acc_ermek_status,
        acc_ermek_time,
        acc_ermek_comment,

        approve_ermek_name,
        approve_ermek_status,
        approve_ermek_time,
        approve_ermek_comment

      FROM public.request_head
      ${whereSql}
      ORDER BY id DESC
    `, params);

    return res.json({
      success: true,
      rows: result.rows
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

  // Обнуленные:
  // показываем только строки, где Реестр = Обнуление
  where.push(`
    COALESCE(TRIM(v.registry_flag), '') = 'Обнуление'
  `);

} else {

  // Обычная таблица:
  // показываем неоплаченные строки,
  // где Реестр не равен Обнуление
  where.push(`(
    COALESCE(TRIM(v.is_paid), '') <> 'Да'
    AND COALESCE(TRIM(v.registry_flag), '') <> 'Обнуление'
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
    const requestId = Number(
      req.body?.request_id ||
      req.body?.id
    );

    const login = String(req.body?.login || "")
      .trim()
      .toLowerCase();

    const action = String(req.body?.action || "agree")
      .trim()
      .toLowerCase();

    const comment = String(req.body?.comment || "").trim();

    if (!requestId) {
      return res.status(400).json({
        success: false,
        error: "request_id required"
      });
    }

    if (!login) {
      return res.status(400).json({
        success: false,
        error: "login required"
      });
    }

    const agreeApprovers = {
      s_zhasulan: {
        title: "Сулейменов Жасулан",
        nameCol: "acc_zhasulan_name",
        statusCol: "acc_zhasulan_status",
        timeCol: "acc_zhasulan_time",
        commentCol: "acc_zhasulan_comment"
      },

      zhas: {
        title: "Исмагулов Жаслан",
        nameCol: "acc_zhas_name",
        statusCol: "acc_zhas_status",
        timeCol: "acc_zhas_time",
        commentCol: "acc_zhas_comment"
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

    let approver = null;
    let stageName = "";

    if (
      action === "agree" ||
      action === "reject_agree"
    ) {
      approver = agreeApprovers[login];
      stageName = "Согласование";
    }

    if (
      action === "approve" ||
      action === "reject_approve"
    ) {
      approver = approveApprovers[login];
      stageName = "Утверждение";
    }

    if (!approver) {
      return res.status(403).json({
        success: false,
        error: "Нет прав на это действие"
      });
    }

    await client.query("BEGIN");

    const headResult = await client.query(`
      SELECT
        id,
        request_no,
        total_amount,

        acc_zhasulan_status,
        acc_zhas_status,

        acc_shevchenko_status,
        acc_marat_status,
        acc_ermek_status,
        approve_ermek_status

      FROM public.request_head
      WHERE id = $1
      LIMIT 1
      FOR UPDATE
    `, [requestId]);

    if (!headResult.rowCount) {
      throw new Error("Заявка не найдена");
    }

    const head = headResult.rows[0];
    const needsIsmagulov = await requestNeedsIsmagulov(
      client,
      requestId
    );

    /*
     * 1. Исмагулов может действовать только после Сулейменова.
     */
    if (
      login === ISMAGULOV_LOGIN &&
      String(head.acc_zhasulan_status || "").trim() !== "Согласовано"
    ) {
      throw new Error(
        "Сначала должен согласовать Сулейменов Жасулан"
      );
    }

    /*
     * 2. Исмагулов не может согласовывать чужие объекты.
     */
    if (
      login === ISMAGULOV_LOGIN &&
      !needsIsmagulov
    ) {
      throw new Error(
        "Для этого объекта согласование Исмагулова не требуется"
      );
    }

    /*
     * 3. Шевченко, Койлибаев и Касенов ждут Сулейменова.
     */
    if (
      (
        login === "v_shevchenko" ||
        login === "k_marat" ||
        login === "k_ermek"
      ) &&
      String(head.acc_zhasulan_status || "").trim() !== "Согласовано"
    ) {
      throw new Error(
        "Сначала должен согласовать Сулейменов Жасулан"
      );
    }

    /*
     * 4. Для специальных объектов они также ждут Исмагулова.
     */
    if (
      needsIsmagulov &&
      (
        login === "v_shevchenko" ||
        login === "k_marat" ||
        login === "k_ermek"
      ) &&
      String(head.acc_zhas_status || "").trim() !== "Согласовано"
    ) {
      throw new Error(
        "Сначала должен согласовать Исмагулов Жаслан"
      );
    }

    const isReject =
      action === "reject_agree" ||
      action === "reject_approve";

    const statusText = isReject
      ? "Отклонено"
      : "Согласовано";

    await client.query(`
      UPDATE public.request_head
      SET
        ${approver.nameCol} = $2,
        ${approver.statusCol} = $3,
        ${approver.timeCol} = NOW(),
        ${approver.commentCol} = $4
      WHERE id = $1
    `, [
      requestId,
      approver.title,
      statusText,
      comment
    ]);

    await client.query(`
      INSERT INTO public.request_approve_log
      (
        request_id,
        stage_name,
        approver_login,
        approver_name,
        action_type,
        comment_text
      )
      VALUES ($1, $2, $3, $4, $5, $6)
    `, [
      requestId,
      stageName,
      login,
      approver.title,
      action,
      comment
    ]);

    /*
     * После Сулейменова:
     * специальные объекты отправляются Исмагулову;
     * остальные сразу Шевченко, Марату и Ермеку.
     */
    if (
      login === "s_zhasulan" &&
      action === "agree"
    ) {
      const nextUsers = needsIsmagulov
        ? [ISMAGULOV_LOGIN]
        : ["v_shevchenko", "k_marat", "k_ermek"];

      for (const nextLogin of nextUsers) {
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
          (
            $1,
            'request',
            $2,
            $3,
            $4,
            'request_card',
            false,
            NOW()
          )
        `, [
          nextLogin,
          `Заявка №${head.request_no} ожидает согласования`,
          `Сумма: ${Number(head.total_amount || 0).toLocaleString("ru-RU")} ₸`,
          requestId
        ]);
      }
    }

    /*
     * После Исмагулова заявка открывается троим.
     */
    if (
      login === ISMAGULOV_LOGIN &&
      action === "agree"
    ) {
      for (
        const nextLogin of [
          "v_shevchenko",
          "k_marat",
          "k_ermek"
        ]
      ) {
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
          (
            $1,
            'request',
            $2,
            $3,
            $4,
            'request_card',
            false,
            NOW()
          )
        `, [
          nextLogin,
          `Заявка №${head.request_no} согласована Исмагуловым`,
          `Заявка доступна для согласования. Сумма: ${Number(head.total_amount || 0).toLocaleString("ru-RU")} ₸`,
          requestId
        ]);
      }
    }

    /*
     * Реестр = Да только:
     * Шевченко, Койлибаев, Касенов
     * или финальное утверждение Касенова.
     *
     * Сулейменов и Исмагулов Реестр не меняют.
     */
    const shouldSetRegistryYes =
      (
        action === "agree" &&
        (
          login === "v_shevchenko" ||
          login === "k_marat" ||
          login === "k_ermek"
        )
      ) ||
      (
        action === "approve" &&
        login === "k_ermek"
      );

    if (shouldSetRegistryYes) {
      await setRequestRegistryYes(client, requestId);
    }

    await client.query("COMMIT");

    return res.json({
      success: true,
      request_id: requestId,
      login,
      action,
      status: statusText,
      needs_ismagulov: needsIsmagulov,
      registry_flag: shouldSetRegistryYes ? "Да" : null
    });

  } catch (e) {
    try {
      await client.query("ROLLBACK");
    } catch (_) {}

    console.error("APPROVE-ROWS ERROR:", e);

    return res.status(500).json({
      success: false,
      error: e.message
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
// JSON API-ПРИЁМНИКИ ДЛЯ 1С
// =====================================================

function oneCArray(body) {
  if (Array.isArray(body)) return body;
  if (Array.isArray(body?.rows)) return body.rows;
  if (Array.isArray(body?.data)) return body.data;
  return body && typeof body === "object" ? [body] : [];
}

function oneCText(value) {
  if (value === undefined || value === null) return null;

  const result = String(value).trim();
  return result === "" ? null : result;
}

function oneCNumber(value) {
  if (value === undefined || value === null || value === "") return null;

  const result = Number(
    String(value)
      .replace(/\s/g, "")
      .replace(",", ".")
  );

  return Number.isFinite(result) ? result : null;
}

function oneCBoolean(value) {
  if (value === undefined || value === null || value === "") return null;
  if (typeof value === "boolean") return value;

  const normalized = String(value).trim().toLowerCase();

  if (["true", "1", "да", "yes"].includes(normalized)) return true;
  if (["false", "0", "нет", "no"].includes(normalized)) return false;

  return null;
}

// Необязательная защита API ключом.
// В Render можно добавить переменную 1C_API_KEY.
// 1С должна передавать заголовок: x-api-key
function checkOneCApiKey(req, res, next) {
  const expectedKey = String(process.env.ONE_C_API_KEY || "").trim();

  // Пока ключ не задан в ENV, запросы пропускаются.
  if (!expectedKey) return next();

  const receivedKey = String(req.headers["x-api-key"] || "").trim();

  if (receivedKey !== expectedKey) {
    return res.status(401).json({
      success: false,
      error: "INVALID_API_KEY",
      message: "Неверный API-ключ"
    });
  }

  next();
}

// Проверка работы API
app.get("/api/1c/health", (req, res) => {
  return res.json({
    success: true,
    service: "1C integration API",
    time: new Date().toISOString()
  });
});


// =====================================================
// 1. ПРИЁМ ДОКУМЕНТОВ doc_receipts
// Внутри: doc_items и doc_services
// =====================================================

app.post(
  "/api/1c/doc_receipts",
  checkOneCApiKey,
  async (req, res) => {
    const client = await pool.connect();

    try {
      const documents = oneCArray(req.body);

      if (!documents.length) {
        return res.status(400).json({
          success: false,
          error: "EMPTY_BODY",
          message: "JSON не содержит документов"
        });
      }

      await client.query("BEGIN");

      const results = [];

      for (const doc of documents) {
        const documentId = oneCText(doc.document_id);

        if (!documentId) {
          throw new Error("В одном из документов отсутствует document_id");
        }

        const oldDocument = await client.query(
          `
          SELECT document_id
          FROM public.doc_receipts
          WHERE document_id = $1
          LIMIT 1
          `,
          [documentId]
        );

        const operation =
          oldDocument.rowCount > 0 ? "updated" : "inserted";

        await client.query(
          `
          INSERT INTO public.doc_receipts (
            document_id,
            base_id,
            document_number,
            document_posted,
            document_date,
            organization_bin,
            organization_name,
            warehouse_id,
            warehouse_name,
            counterparty_id,
            counterparty_bin,
            counterparty_name,
            contract_id,
            contract_name,
            currency_name,
            income_kpn,
            settlement_account,
            advance_account,
            vat_enable,
            vat_mode,
            document_sum,
            document_commentary,
            document_author_name,
            document_type,
            advance_withheld,
            guarantee_withheld,
            penalty_withheld,
            other_withheld,
            target_entity,
            action_required,
            is_executed,
            is_managerial,
            id_dov,
            deleted,
            updated_at
          )
          VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
            $11,$12,$13,$14,$15,$16,$17,$18,$19,$20,
            $21,$22,$23,$24,$25,$26,$27,$28,$29,$30,
            $31,$32,$33,$34,NOW()
          )
          ON CONFLICT (document_id)
          DO UPDATE SET
            base_id = EXCLUDED.base_id,
            document_number = EXCLUDED.document_number,
            document_posted = EXCLUDED.document_posted,
            document_date = EXCLUDED.document_date,
            organization_bin = EXCLUDED.organization_bin,
            organization_name = EXCLUDED.organization_name,
            warehouse_id = EXCLUDED.warehouse_id,
            warehouse_name = EXCLUDED.warehouse_name,
            counterparty_id = EXCLUDED.counterparty_id,
            counterparty_bin = EXCLUDED.counterparty_bin,
            counterparty_name = EXCLUDED.counterparty_name,
            contract_id = EXCLUDED.contract_id,
            contract_name = EXCLUDED.contract_name,
            currency_name = EXCLUDED.currency_name,
            income_kpn = EXCLUDED.income_kpn,
            settlement_account = EXCLUDED.settlement_account,
            advance_account = EXCLUDED.advance_account,
            vat_enable = EXCLUDED.vat_enable,
            vat_mode = EXCLUDED.vat_mode,
            document_sum = EXCLUDED.document_sum,
            document_commentary = EXCLUDED.document_commentary,
            document_author_name = EXCLUDED.document_author_name,
            document_type = EXCLUDED.document_type,
            advance_withheld = EXCLUDED.advance_withheld,
            guarantee_withheld = EXCLUDED.guarantee_withheld,
            penalty_withheld = EXCLUDED.penalty_withheld,
            other_withheld = EXCLUDED.other_withheld,
            target_entity = EXCLUDED.target_entity,
            action_required = EXCLUDED.action_required,
            is_executed = EXCLUDED.is_executed,
            is_managerial = EXCLUDED.is_managerial,
            id_dov = EXCLUDED.id_dov,
            deleted = EXCLUDED.deleted,
            updated_at = NOW()
          `,
          [
            documentId,
            oneCText(doc.base_id),
            oneCText(doc.document_number),
            oneCBoolean(doc.document_posted),
            oneCText(doc.document_date),

            oneCText(doc.organization_bin),
            oneCText(doc.organization_name),

            oneCText(doc.warehouse_id),
            oneCText(doc.warehouse_name),

            oneCText(doc.counterparty_id),
            oneCText(doc.counterparty_bin),
            oneCText(doc.counterparty_name),

            oneCText(doc.contract_id),
            oneCText(doc.contract_name),

            oneCText(doc.currency_name),
            oneCText(doc.income_kpn),
            oneCText(doc.settlement_account),
            oneCText(doc.advance_account),

            oneCBoolean(doc.vat_enable),
            oneCText(doc.vat_mode),

            oneCNumber(doc.document_sum),
            oneCText(doc.document_commentary),
            oneCText(doc.document_author_name),
            oneCText(doc.document_type),

            oneCNumber(doc.advance_withheld),
            oneCNumber(doc.guarantee_withheld),
            oneCNumber(doc.penalty_withheld),
            oneCNumber(doc.other_withheld),

            oneCText(doc.target_entity),
            oneCBoolean(doc.action_required),
            oneCBoolean(doc.is_executed),
            oneCBoolean(doc.is_managerial),

            oneCText(doc.id_dov),
            oneCBoolean(doc.deleted) ?? false
          ]
        );

        /*
         * При повторной отправке документа очищаем старые массивы
         * и записываем актуальные строки из 1С.
         */
        await client.query(
          `DELETE FROM public.doc_items WHERE document_id = $1`,
          [documentId]
        );

        await client.query(
          `DELETE FROM public.doc_services WHERE document_id = $1`,
          [documentId]
        );

        const docItems = Array.isArray(doc.doc_items)
          ? doc.doc_items
          : [];

        for (const item of docItems) {
          const itemId = oneCText(item.item_id);

          if (!itemId) {
            throw new Error(
              `В doc_items документа ${documentId} отсутствует item_id`
            );
          }

          await client.query(
            `
            INSERT INTO public.doc_items (
              document_id,
              item_id,
              item_name,
              quantity,
              price,
              amount,
              vat_percent,
              vat_amount,
              amount_with_vat,
              vat_account,
              turnover_type,
              receipt_type_name,
              cost_account_bu,
              cost_account_nu,
              in_group,
              updated_at
            )
            VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,
              $9,$10,$11,$12,$13,$14,$15,NOW()
            )
            `,
            [
              documentId,
              itemId,
              oneCText(item.item_name),
              oneCNumber(item.quantity),
              oneCNumber(item.price),
              oneCNumber(item.amount),
              oneCNumber(item.vat_percent),
              oneCNumber(item.vat_amount),
              oneCNumber(item.amount_with_vat),
              oneCText(item.vat_account),
              oneCText(item.turnover_type),
              oneCText(item.receipt_type_name),
              oneCText(item.cost_account_bu),
              oneCText(item.cost_account_nu),
              oneCText(item.in_group)
            ]
          );
        }

        const docServices = Array.isArray(doc.doc_services)
          ? doc.doc_services
          : [];

        for (const service of docServices) {
          const serviceId = oneCText(service.service_id);

          if (!serviceId) {
            throw new Error(
              `В doc_services документа ${documentId} отсутствует service_id`
            );
          }

          await client.query(
            `
            INSERT INTO public.doc_services (
              document_id,
              service_id,
              service_name,
              service_content,
              quantity,
              price,
              amount,
              vat_percent,
              vat_amount,
              amount_with_vat,
              vat_account,
              turnover_type,
              receipt_type_name,
              cost_account_bu,
              cost_account_nu,
              project_id,
              project_name,
              updated_at
            )
            VALUES (
              $1,$2,$3,$4,$5,$6,$7,$8,$9,
              $10,$11,$12,$13,$14,$15,$16,$17,NOW()
            )
            `,
            [
              documentId,
              serviceId,
              oneCText(service.service_name),
              oneCText(service.service_content),
              oneCNumber(service.quantity),
              oneCNumber(service.price),
              oneCNumber(service.amount),
              oneCNumber(service.vat_percent),
              oneCNumber(service.vat_amount),
              oneCNumber(service.amount_with_vat),
              oneCText(service.vat_account),
              oneCText(service.turnover_type),
              oneCText(service.receipt_type_name),
              oneCText(service.cost_account_bu),
              oneCText(service.cost_account_nu),
              oneCText(service.project_id),
              oneCText(service.project_name)
            ]
          );
        }

        results.push({
          document_id: documentId,
          operation,
          doc_items_count: docItems.length,
          doc_services_count: docServices.length
        });
      }

      await client.query("COMMIT");

      return res.json({
        success: true,
        received: documents.length,
        results
      });

    } catch (error) {
      try {
        await client.query("ROLLBACK");
      } catch (_) {}

      console.error("1C DOC_RECEIPTS ERROR:", error);

      return res.status(500).json({
        success: false,
        error: "DOC_RECEIPTS_ERROR",
        message: error.message
      });

    } finally {
      client.release();
    }
  }
);


// =====================================================
// 2. КОНТРАГЕНТЫ
// =====================================================

app.post(
  "/api/1c/ref_counterparties",
  checkOneCApiKey,
  async (req, res) => {
    try {
      const rows = oneCArray(req.body);
      let saved = 0;

      for (const row of rows) {
        const id = oneCText(row.counterparty_id);

        if (!id) {
          return res.status(400).json({
            success: false,
            message: "counterparty_id обязателен"
          });
        }

        await pool.query(
          `
          INSERT INTO public.ref_counterparties (
            counterparty_id,
            counterparty_name,
            individual_or_legal,
            group_name,
            counterparty_bin,
            counterparty_kbe,
            is_government_institution,
            is_small_retail_outlet,
            residence_country,
            vat_series,
            vat_number,
            vat_date,
            bank_account,
            bank_name,
            counterparty_comment,
            deleted,
            updated_at
          )
          VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,$9,
            $10,$11,$12,$13,$14,$15,$16,NOW()
          )
          ON CONFLICT (counterparty_id)
          DO UPDATE SET
            counterparty_name = EXCLUDED.counterparty_name,
            individual_or_legal = EXCLUDED.individual_or_legal,
            group_name = EXCLUDED.group_name,
            counterparty_bin = EXCLUDED.counterparty_bin,
            counterparty_kbe = EXCLUDED.counterparty_kbe,
            is_government_institution =
              EXCLUDED.is_government_institution,
            is_small_retail_outlet =
              EXCLUDED.is_small_retail_outlet,
            residence_country = EXCLUDED.residence_country,
            vat_series = EXCLUDED.vat_series,
            vat_number = EXCLUDED.vat_number,
            vat_date = EXCLUDED.vat_date,
            bank_account = EXCLUDED.bank_account,
            bank_name = EXCLUDED.bank_name,
            counterparty_comment = EXCLUDED.counterparty_comment,
            deleted = EXCLUDED.deleted,
            updated_at = NOW()
          `,
          [
            id,
            oneCText(row.counterparty_name),
            oneCText(row.individual_or_legal),
            oneCText(row.group_name),
            oneCText(row.counterparty_bin),
            oneCText(row.counterparty_kbe),
            oneCBoolean(row.is_government_institution),
            oneCBoolean(row.is_small_retail_outlet),
            oneCText(row.residence_country),
            oneCText(row.vat_series),
            oneCText(row.vat_number),
            oneCText(row.vat_date),
            oneCText(row.bank_account),
            oneCText(row.bank_name),
            oneCText(row.counterparty_comment),
            oneCBoolean(row.deleted) ?? false
          ]
        );

        saved++;
      }

      return res.json({ success: true, received: rows.length, saved });

    } catch (error) {
      console.error("1C COUNTERPARTIES ERROR:", error);

      return res.status(500).json({
        success: false,
        message: error.message
      });
    }
  }
);


// =====================================================
// 3. СКЛАДЫ
// =====================================================

app.post(
  "/api/1c/ref_warehouses",
  checkOneCApiKey,
  async (req, res) => {
    try {
      const rows = oneCArray(req.body);
      let saved = 0;

      for (const row of rows) {
        const id = oneCText(row.warehouse_id);

        if (!id) {
          return res.status(400).json({
            success: false,
            message: "warehouse_id обязателен"
          });
        }

        await pool.query(
          `
          INSERT INTO public.ref_warehouses (
            warehouse_id,
            warehouse_name,
            warehouse_comment,
            deleted,
            updated_at
          )
          VALUES ($1,$2,$3,$4,NOW())
          ON CONFLICT (warehouse_id)
          DO UPDATE SET
            warehouse_name = EXCLUDED.warehouse_name,
            warehouse_comment = EXCLUDED.warehouse_comment,
            deleted = EXCLUDED.deleted,
            updated_at = NOW()
          `,
          [
            id,
            oneCText(row.warehouse_name),
            oneCText(row.warehouse_comment),
            oneCBoolean(row.deleted) ?? false
          ]
        );

        saved++;
      }

      return res.json({ success: true, received: rows.length, saved });

    } catch (error) {
      console.error("1C WAREHOUSES ERROR:", error);

      return res.status(500).json({
        success: false,
        message: error.message
      });
    }
  }
);


// =====================================================
// 4. ТОВАРЫ И УСЛУГИ
// =====================================================

app.post(
  "/api/1c/ref_products",
  checkOneCApiKey,
  async (req, res) => {
    try {
      const rows = oneCArray(req.body);
      let saved = 0;

      for (const row of rows) {
        const id = oneCText(row.product_id);

        if (!id) {
          return res.status(400).json({
            success: false,
            message: "product_id обязателен"
          });
        }

        await pool.query(
          `
          INSERT INTO public.ref_products (
            product_id,
            product_code,
            product_name,
            is_group,
            is_service,
            article,
            unit,
            vat_percent,
            tnvd_code,
            kpvd_code,
            nkt_code,
            product_type,
            product_group,
            product_comment,
            deleted,
            updated_at
          )
          VALUES (
            $1,$2,$3,$4,$5,$6,$7,$8,
            $9,$10,$11,$12,$13,$14,$15,NOW()
          )
          ON CONFLICT (product_id)
          DO UPDATE SET
            product_code = EXCLUDED.product_code,
            product_name = EXCLUDED.product_name,
            is_group = EXCLUDED.is_group,
            is_service = EXCLUDED.is_service,
            article = EXCLUDED.article,
            unit = EXCLUDED.unit,
            vat_percent = EXCLUDED.vat_percent,
            tnvd_code = EXCLUDED.tnvd_code,
            kpvd_code = EXCLUDED.kpvd_code,
            nkt_code = EXCLUDED.nkt_code,
            product_type = EXCLUDED.product_type,
            product_group = EXCLUDED.product_group,
            product_comment = EXCLUDED.product_comment,
            deleted = EXCLUDED.deleted,
            updated_at = NOW()
          `,
          [
            id,
            oneCText(row.product_code),
            oneCText(row.product_name),
            oneCBoolean(row.is_group),
            oneCBoolean(row.is_service),
            oneCText(row.article),
            oneCText(row.unit),
            oneCNumber(row.vat_percent),
            oneCText(row.tnvd_code),
            oneCText(row.kpvd_code),
            oneCText(row.nkt_code),
            oneCText(row.product_type),
            oneCText(row.product_group),
            oneCText(row.product_comment),
            oneCBoolean(row.deleted) ?? false
          ]
        );

        saved++;
      }

      return res.json({ success: true, received: rows.length, saved });

    } catch (error) {
      console.error("1C PRODUCTS ERROR:", error);

      return res.status(500).json({
        success: false,
        message: error.message
      });
    }
  }
);


// =====================================================
// 5. ДОГОВОРЫ КОНТРАГЕНТОВ
// =====================================================

app.post(
  "/api/1c/ref_counterparties_contracts",
  checkOneCApiKey,
  async (req, res) => {
    try {
      const rows = oneCArray(req.body);
      let saved = 0;

      for (const row of rows) {
        const id = oneCText(row.contract_id);

        if (!id) {
          return res.status(400).json({
            success: false,
            message: "contract_id обязателен"
          });
        }

        await pool.query(
          `
          INSERT INTO public.ref_counterparties_contracts (
            contract_id,
            contract_number,
            contract_date,
            contract_name,
            contract_type,
            organization_name,
            organization_bin,
            counterparty_id,
            counterparty_bin,
            counterparty_name,
            deleted,
            updated_at
          )
          VALUES (
            $1,$2,$3,$4,$5,$6,
            $7,$8,$9,$10,$11,NOW()
          )
          ON CONFLICT (contract_id)
          DO UPDATE SET
            contract_number = EXCLUDED.contract_number,
            contract_date = EXCLUDED.contract_date,
            contract_name = EXCLUDED.contract_name,
            contract_type = EXCLUDED.contract_type,
            organization_name = EXCLUDED.organization_name,
            organization_bin = EXCLUDED.organization_bin,
            counterparty_id = EXCLUDED.counterparty_id,
            counterparty_bin = EXCLUDED.counterparty_bin,
            counterparty_name = EXCLUDED.counterparty_name,
            deleted = EXCLUDED.deleted,
            updated_at = NOW()
          `,
          [
            id,
            oneCText(row.contract_number),
            oneCText(row.contract_date),
            oneCText(row.contract_name),
            oneCText(row.contract_type),
            oneCText(row.organization_name),
            oneCText(row.organization_bin),
            oneCText(row.counterparty_id),
            oneCText(row.counterparty_bin),
            oneCText(row.counterparty_name),
            oneCBoolean(row.deleted) ?? false
          ]
        );

        saved++;
      }

      return res.json({ success: true, received: rows.length, saved });

    } catch (error) {
      console.error("1C CONTRACTS ERROR:", error);

      return res.status(500).json({
        success: false,
        message: error.message
      });
    }
  }
);


// =====================================================
// 6. ПРОЕКТЫ
// =====================================================

app.post(
  "/api/1c/ref_project_groups",
  checkOneCApiKey,
  async (req, res) => {
    try {
      const rows = oneCArray(req.body);
      let saved = 0;

      for (const row of rows) {
        const id = oneCText(row.project_id);

        if (!id) {
          return res.status(400).json({
            success: false,
            message: "project_id обязателен"
          });
        }

        await pool.query(
          `
          INSERT INTO public.ref_project_groups (
            project_id,
            project_name,
            deleted,
            updated_at
          )
          VALUES ($1,$2,$3,NOW())
          ON CONFLICT (project_id)
          DO UPDATE SET
            project_name = EXCLUDED.project_name,
            deleted = EXCLUDED.deleted,
            updated_at = NOW()
          `,
          [
            id,
            oneCText(row.project_name),
            oneCBoolean(row.deleted) ?? false
          ]
        );

        saved++;
      }

      return res.json({ success: true, received: rows.length, saved });

    } catch (error) {
      console.error("1C PROJECT GROUPS ERROR:", error);

      return res.status(500).json({
        success: false,
        message: error.message
      });
    }
  }
);


// =====================================================
// Start
// =====================================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT))