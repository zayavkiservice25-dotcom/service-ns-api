require("dotenv").config();

const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json());

// ===============================
// Подключение к PostgreSQL (Render)
// ===============================
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// ===============================
// Проверка сервера
// ===============================
app.get("/", (req, res) => {
  res.send("Service-NS API работает 🚀");
});

// ===============================
// Проверка подключения к БД
// ===============================
app.get("/db-ping", async (req, res) => {
  try {
    const r = await pool.query("SELECT NOW() as now");
    res.json({ ok: true, now: r.rows[0].now });
  } catch (e) {
    console.error("DB-PING ERROR:", e);
    res.status(500).json({ ok: false, error: e.message });
  }
});

// ===============================
// Сохранение заявки
// ===============================
app.post("/save-request", async (req, res) => {
  try {
    console.log("SAVE BODY:", req.body); // ✅ ЛОГ что пришло

    const {
      login,
      object,
      date,
      kon,
      tru,
      grp,
      tmc,
      unit,
      qty,
      note,
      deadline,
    } = req.body;

    const query = `
      INSERT INTO requests
      (login, object, date, kon, tru, grp, tmc, unit, qty, note, deadline)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
      RETURNING id
    `;

    const values = [
      login || "",
      object || "",
      date || "",
      kon || "",
      tru || "",
      grp || "",
      tmc || "",
      unit || "",
      qty === "" || qty === undefined ? null : qty,
      note || "",
      deadline || "",
    ];

    const result = await pool.query(query, values);

    res.json({ success: true, id: result.rows[0].id });
  } catch (err) {
    console.error("SAVE ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ===============================
// Посмотреть последние заявки
// ===============================
app.get("/requests", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT * FROM requests ORDER BY id DESC LIMIT 20"
    );
    res.json(result.rows);
  } catch (err) {
    console.error("REQUESTS ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});

// ===============================
// Batch: сохранить сразу несколько строк
// ===============================
app.post("/save-request-batch", async (req, res) => {
  try {
    const { header, rows } = req.body;

    if (!rows || !Array.isArray(rows) || rows.length === 0) {
      return res.status(400).json({ success: false, error: "rows is empty" });
    }

    const values = [];
    const chunks = rows.map((r, i) => {
      const base = i * 11;

      values.push(
        header?.login || "",
        header?.object || "",
        header?.date || "",
        r.kon || "",
        r.tru || "",
        r.grp || "",
        r.tmc || "",
        r.unit || "",
        (r.qty === "" || r.qty === undefined ? null : r.qty),
        r.note || "",
        r.deadline || ""
      );

      return `($${base+1},$${base+2},$${base+3},$${base+4},$${base+5},$${base+6},$${base+7},$${base+8},$${base+9},$${base+10},$${base+11})`;
    });

    const query = `
      INSERT INTO requests
      (login, object, date, kon, tru, grp, tmc, unit, qty, note, deadline)
      VALUES ${chunks.join(",")}
      RETURNING id
    `;

    const result = await pool.query(query, values);
    res.json({ success: true, ids: result.rows.map(x => x.id) });

  } catch (err) {
    console.error("BATCH SAVE ERROR:", err);
    res.status(500).json({ success: false, error: err.message });
  }
});


// ===============================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT));
