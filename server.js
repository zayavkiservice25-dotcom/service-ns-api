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

  // Остаток по FT (если вдруг ещё не было)
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ft_balance (
      id_ft text PRIMARY KEY,
      balance_ft numeric NOT NULL DEFAULT 0
    );
  `);

  console.log("DB init OK");
}
initDb().catch(console.error);

// ===============================
// Health
// ===============================
app.get("/", (req, res) =>
  res.send("Service-NS API работает 🚀 v-ftzvk-auto-zft")
);

app.get("/db-ping", async (req, res) => {
  try {
    const r = await pool.query("SELECT NOW() as now");
    res.json({ ok: true, now: r.rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// =====================================================
// FT + авто ZFT + авто Остаток
// =====================================================
app.post("/save-ft", async (req, res) => {
  const client = await pool.connect();
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

    const sumNum =
      sum_ft === "" || sum_ft === undefined || sum_ft === null
        ? null
        : Number(sum_ft);

    await client.query("BEGIN");

    // 1) создаём FT
    const rFT = await client.query(
      `
      INSERT INTO ft
      (id_ft, input_date, input_name, division, "object", contractor, invoice_no, invoice_date, invoice_pdf, sum_ft)
      VALUES
      ('FT' || nextval('ft_id_seq')::text, $1,$2,$3,$4,$5,$6,$7,$8,$9)
      RETURNING id_ft, sum_ft
      `,
      [
        input_date || "",
        input_name || "",
        division || "",
        object || "",
        contractor || "",
        invoice_no || "",
        invoice_date || "",
        invoice_pdf || "",
        sumNum,
      ]
    );

    const id_ft = rFT.rows[0].id_ft;
    const ft_sum = Number(rFT.rows[0].sum_ft || 0);

    // 2) ✅ авто создаём ZFT в ТВОЮ таблицу zvk
    // zvk: id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag
    const rZ = await client.query(
      `
      INSERT INTO zvk (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
      VALUES (
        'ZFT' || nextval('zvk_id_seq')::text,
        $1,
        (NOW() AT TIME ZONE 'Asia/Almaty'),
        'СИСТЕМА',
        $2,
        'нет'
      )
      RETURNING id_zvk
      `,
      [id_ft, ft_sum]
    );

    // 3) ✅ авто записываем остаток (равен сумме счета)
    await client.query(
      `
      INSERT INTO ft_balance (id_ft, balance_ft)
      VALUES ($1, $2)
      ON CONFLICT (id_ft)
      DO UPDATE SET balance_ft = EXCLUDED.balance_ft
      `,
      [id_ft, ft_sum]
    );

    await client.query("COMMIT");

    res.json({ success: true, id_ft, id_zvk: rZ.rows[0].id_zvk });
  } catch (e) {
    await client.query("ROLLBACK");
    console.error("SAVE FT ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  } finally {
    client.release();
  }
});

// =====================================================
// GET FT (+ остаток)
// =====================================================
app.get("/ft", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 500), 500);
    const login = String(req.query.login || "").trim();
    const loginNorm = login.toLowerCase();

    const admin =
      String(req.query.is_admin || "0") === "1" || loginNorm === "b_erkin";

    if (!login) {
      return res.status(400).json({ success: false, error: "login is required" });
    }

    const qAdmin = `
      SELECT
        f.id_ft, f.input_date, f.input_name, f.division, f."object",
        f.contractor, f.invoice_no, f.invoice_date, f.invoice_pdf, f.sum_ft,
        b.balance_ft
      FROM ft f
      LEFT JOIN ft_balance b ON b.id_ft = f.id_ft
      ORDER BY COALESCE(NULLIF(regexp_replace(f.id_ft,'\\D','','g'),''),'0')::int DESC
      LIMIT $1
    `;

    const qUser = `
      SELECT
        f.id_ft, f.input_date, f.input_name, f.division, f."object",
        f.contractor, f.invoice_no, f.invoice_date, f.invoice_pdf, f.sum_ft,
        b.balance_ft
      FROM ft f
      LEFT JOIN ft_balance b ON b.id_ft = f.id_ft
      WHERE f.input_name = $2
      ORDER BY COALESCE(NULLIF(regexp_replace(f.id_ft,'\\D','','g'),''),'0')::int DESC
      LIMIT $1
    `;

    const r = admin
      ? await pool.query(qAdmin, [limit])
      : await pool.query(qUser, [limit, login]);

    res.json({ success: true, rows: r.rows, admin });
  } catch (e) {
    console.error("GET FT ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// =====================================================
// ZVK: создать вручную (если нужно)
// (под твою таблицу zvk)
// =====================================================
app.post("/save-zvk", async (req, res) => {
  try {
    const { id_ft, to_pay, request_flag, zvk_name } = req.body;
    if (!id_ft) return res.status(400).json({ success: false, error: "id_ft is required" });

    const r = await pool.query(
      `
      INSERT INTO zvk (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
      VALUES (
        'ZFT' || nextval('zvk_id_seq')::text,
        $1,
        (NOW() AT TIME ZONE 'Asia/Almaty'),
        COALESCE($2, 'СИСТЕМА'),
        COALESCE($3, 0),
        COALESCE($4, 'нет')
      )
      RETURNING id_zvk
      `,
      [
        String(id_ft).trim(),
        zvk_name ? String(zvk_name).trim() : null,
        (to_pay === "" || to_pay === undefined || to_pay === null) ? 0 : Number(to_pay),
        request_flag ? String(request_flag).trim() : null,
      ]
    );

    res.json({ success: true, id_zvk: r.rows[0].id_zvk });
  } catch (e) {
    console.error("SAVE ZVK ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// =====================================================
// UPDATE ZFT: поменять "К оплате" и "Заявка да/нет" по выбранному ZFT
// =====================================================
app.post("/update-zft", async (req, res) => {
  try {
    const { id_zvk, to_pay, request_flag, zvk_name } = req.body;
    if (!id_zvk) return res.status(400).json({ success: false, error: "id_zvk is required" });

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
        (to_pay === "" || to_pay === undefined || to_pay === null) ? null : Number(to_pay),
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
// JOIN (то, что ты называешь "2-й вебапп = соединение")
// FT + ZFT + Остаток
// =====================================================
app.get("/ft-zvk-join", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 500), 500);

    const q = `
      SELECT
        f.id_ft, f.input_date, f.input_name, f.division, f."object",
        f.contractor, f.invoice_no, f.invoice_date, f.invoice_pdf, f.sum_ft,

        z.id_zvk, z.zvk_date, z.zvk_name, z.to_pay, z.request_flag,

        b.balance_ft
      FROM ft f
      LEFT JOIN zvk z ON trim(z.id_ft) = trim(f.id_ft)
      LEFT JOIN ft_balance b ON b.id_ft = f.id_ft
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

// =====================================================
// 1) Инициатор: сохраняет src_d / src_o (таблица zvk_status)
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
// =====================================================
app.post("/upsert-zvk-approve-pay", async (req, res) => {
  const client = await pool.connect();
  try {
    const { login, id_zvk, agree_name, is_paid } = req.body;
    if (!login || !id_zvk) {
      return res.status(400).json({ success: false, error: "login, id_zvk required" });
    }

    if (String(login).trim().toLowerCase() !== "b_erkin") {
      return res.status(403).json({ success: false, error: "only B_Erkin allowed" });
    }

    await client.query("BEGIN");

    await client.query(
      `INSERT INTO zvk_agree (id_zvk, agree_name)
       VALUES ($1,$2)
       ON CONFLICT (id_zvk)
       DO UPDATE SET agree_name=EXCLUDED.agree_name`,
      [String(id_zvk).trim(), (agree_name ?? "").toString().trim() || null]
    );

    await client.query(
      `INSERT INTO zvk_pay (id_zvk, is_paid, created_at)
       VALUES ($1, $2, CASE WHEN $2 = 'Да' THEN (NOW() AT TIME ZONE 'Asia/Almaty') ELSE NULL END)
       ON CONFLICT (id_zvk)
       DO UPDATE SET
         is_paid = EXCLUDED.is_paid,
         created_at = CASE
           WHEN EXCLUDED.is_paid = 'Да' AND zvk_pay.created_at IS NULL THEN (NOW() AT TIME ZONE 'Asia/Almaty')
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

// =====================================================
// Проверка последовательностей (для админа)
// =====================================================
app.get("/check-sequences", async (req, res) => {
  try {
    const login = String(req.query.login || "").trim().toLowerCase();
    if (login !== "b_erkin") {
      return res.status(403).json({ success: false, error: "Только B_Erkin" });
    }

    const ftSeq = await pool.query("SELECT last_value, is_called FROM ft_id_seq;");
    const zvkSeq = await pool.query("SELECT last_value, is_called FROM zvk_id_seq;");

    const maxFt = await pool.query("SELECT MAX(CAST(REGEXP_REPLACE(id_ft, '\\D', '', 'g') AS INTEGER)) as max_id FROM ft;");
    const maxZvk = await pool.query("SELECT MAX(CAST(REGEXP_REPLACE(id_zvk, '\\D', '', 'g') AS INTEGER)) as max_id FROM zvk;");

    res.json({
      success: true,
      ft_sequence: {
        last_value: ftSeq.rows[0].last_value,
        is_called: ftSeq.rows[0].is_called,
        next_id: ftSeq.rows[0].is_called ? Number(ftSeq.rows[0].last_value) + 1 : ftSeq.rows[0].last_value,
        max_id_in_table: maxFt.rows[0].max_id || 0
      },
      zvk_sequence: {
        last_value: zvkSeq.rows[0].last_value,
        is_called: zvkSeq.rows[0].is_called,
        next_id: zvkSeq.rows[0].is_called ? Number(zvkSeq.rows[0].last_value) + 1 : zvkSeq.rows[0].last_value,
        max_id_in_table: maxZvk.rows[0].max_id || 0
      }
    });
  } catch (e) {
    console.error("CHECK SEQUENCES ERROR:", e);
    res.status(500).json({ success: false, error: e.message });
  }
});

// ===============================
// Start
// ===============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT));
