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
// Init (только sequence, без таблицы ft_balance!)
// ===============================
async function initDb() {
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS ft_id_seq START 1;`);
  await pool.query(`CREATE SEQUENCE IF NOT EXISTS zvk_id_seq START 1;`);
  console.log("DB init OK (sequences)");
}
initDb().catch(console.error);

// ===============================
// Health
// ===============================
app.get("/", (req, res) => res.send("Service-NS API работает 🚀 v-ftzvk-view-card"));

app.get("/db-ping", async (req, res) => {
  try {
    const r = await pool.query("SELECT NOW() as now");
    res.json({ ok: true, now: r.rows[0].now });
  } catch (e) {
    res.status(500).json({ ok: false, error: e.message });
  }
});

// =====================================================
// SAVE FT  + авто создание ZFT (СИСТЕМА)
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
      sum_ft === "" || sum_ft === undefined || sum_ft === null ? null : Number(sum_ft);

    await client.query("BEGIN");

    // 1) FT
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

    // 2) авто ZFT
    const rZ = await client.query(
      `
      INSERT INTO zvk (id_zvk, id_ft, zvk_date, zvk_name, to_pay, request_flag)
      VALUES (
        'ZFT' || nextval('zvk_id_seq')::text,
        $1,
        (NOW() AT TIME ZONE 'Asia/Almaty'),
        'СИСТЕМА',
        $2,
        'Нет'
      )
      RETURNING id_zvk
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
// VIEW: ft_zvk_full (главная таблица для 2-го вебаппа)
// =====================================================
app.get("/view-ftzvk", async (req, res) => {
  try {
    const limit = Math.min(Number(req.query.limit || 500), 500);
    const login = String(req.query.login || "").trim();
    const role  = String(req.query.role || "").trim().toLowerCase();

    if (!login) return res.status(400).json({ success:false, error:"login is required" });

    const isAdmin = role.includes("админ") || login.toLowerCase() === "b_erkin";

    const qAdmin = `
      SELECT * FROM ft_zvk_full
      ORDER BY
        COALESCE(NULLIF(regexp_replace(id_ft,'\\D','','g'),''),'0')::int DESC,
        COALESCE(NULLIF(regexp_replace(id_zvk,'\\D','','g'),''),'0')::int DESC
      LIMIT $1
    `;

    const qUser = `
      SELECT * FROM ft_zvk_full
      WHERE COALESCE(input_name,'') = $2
      ORDER BY
        COALESCE(NULLIF(regexp_replace(id_ft,'\\D','','g'),''),'0')::int DESC,
        COALESCE(NULLIF(regexp_replace(id_zvk,'\\D','','g'),''),'0')::int DESC
      LIMIT $1
    `;

    const r = isAdmin
      ? await pool.query(qAdmin, [limit])
      : await pool.query(qUser, [limit, login]);

    res.json({ success:true, rows:r.rows, admin:isAdmin });
  } catch (e) {
    console.error("VIEW FTZVK ERROR:", e);
    res.status(500).json({ success:false, error:e.message });
  }
});

// =====================================================
// UPDATE ZFT (инициатор): К оплате + Заявка + ЗаявИмя
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
        zvk_name = COALESCE($4, zvk_name),
        zvk_date = (NOW() AT TIME ZONE 'Asia/Almaty')
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
// Инициатор: Источник Див/Объект (zvk_status)
// =====================================================
app.post("/upsert-zvk-src", async (req, res) => {
  const client = await pool.connect();
  try {
    const { login, id_zvk, src_d, src_o } = req.body;
    if (!login || !id_zvk) return res.status(400).json({ success: false, error: "login, id_zvk required" });

    await client.query("BEGIN");

    const r = await client.query(
      `INSERT INTO zvk_status (id_zvk, src_d, src_o, created_at)
       VALUES ($1,$2,$3,(NOW() AT TIME ZONE 'Asia/Almaty'))
       ON CONFLICT (id_zvk)
       DO UPDATE SET
         src_d=EXCLUDED.src_d,
         src_o=EXCLUDED.src_o,
         created_at=EXCLUDED.created_at
       RETURNING id_zvk, src_d, src_o, created_at`,
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
// Админ (B_Erkin): Реестр + Оплата (zvk_agree, zvk_pay)
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

    // Реестр (agree_name = Да/Нет/Обнуление)
    await client.query(
      `INSERT INTO zvk_agree (id_zvk, agree_name, created_at)
       VALUES ($1,$2,(NOW() AT TIME ZONE 'Asia/Almaty'))
       ON CONFLICT (id_zvk)
       DO UPDATE SET agree_name=EXCLUDED.agree_name, created_at=EXCLUDED.created_at`,
      [String(id_zvk).trim(), (agree_name ?? "").toString().trim() || null]
    );

    // Оплата (is_paid = Да/Нет) + created_at только если Да
    await client.query(
      `INSERT INTO zvk_pay (id_zvk, is_paid, created_at)
       VALUES ($1, $2, CASE WHEN $2 = 'Да' THEN (NOW() AT TIME ZONE 'Asia/Almaty') ELSE NULL END)
       ON CONFLICT (id_zvk)
       DO UPDATE SET
         is_paid = EXCLUDED.is_paid,
         created_at = CASE
           WHEN EXCLUDED.is_paid = 'Да' THEN (NOW() AT TIME ZONE 'Asia/Almaty')
           ELSE NULL
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
// sequences check (admin)
// =====================================================
app.get("/check-sequences", async (req, res) => {
  try {
    const login = String(req.query.login || "").trim().toLowerCase();
    if (login !== "b_erkin") {
      return res.status(403).json({ success: false, error: "Только B_Erkin" });
    }

    const ftSeq = await pool.query("SELECT last_value, is_called FROM ft_id_seq;");
    const zvkSeq = await pool.query("SELECT last_value, is_called FROM zvk_id_seq;");

    res.json({
      success: true,
      ft_sequence: ftSeq.rows[0],
      zvk_sequence: zvkSeq.rows[0],
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
