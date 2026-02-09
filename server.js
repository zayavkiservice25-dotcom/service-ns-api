const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();
app.use(cors());
app.use(express.json());

// подключение к PostgreSQL из Render
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: { rejectUnauthorized: false },
});

// проверка сервера
app.get("/", (req, res) => {
  res.send("Service-NS API работает 🚀");
});

// сохранение заявки
app.post("/save-request", async (req, res) => {
  try {
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
    ];

    const result = await pool.query(query, values);

    res.json({ success: true, id: result.rows[0].id });
  } catch (err) {
    console.error(err);
    res.status(500).json({ success: false, error: err.message });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log("Server started on port " + PORT));
