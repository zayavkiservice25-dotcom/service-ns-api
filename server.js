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
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT));
