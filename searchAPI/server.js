const express = require("express");
const cors = require("cors");

const { connectDB, closeDB } = require("./db");
const { search } = require("./search");

const app = express();
const PORT = 3000;

// ── Middleware ─────────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json());

// Simple request logger
app.use((req, _res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  next();
});

// ── Routes ─────────────────────────────────────────────────────────────────────

// GET /search?q=<query>
app.get("/search", async (req, res) => {
  try {
    const query = (req.query.q || "").trim();

    // Input validation
    if (!query) {
      return res.status(400).json({ error: "Missing query parameter: q" });
    }
    if (query.length > 200) {
      return res.status(400).json({ error: "Query too long (max 200 chars)" });
    }

    const results = await search(query);
    return res.json({ query, count: results.length, results });
  } catch (err) {
    console.error("Search error:", err);
    return res.status(500).json({ error: "Internal search error" });
  }
});

// GET /health  — quick sanity check
app.get("/health", (_req, res) => {
  res.json({ status: "ok", ts: new Date().toISOString() });
});

// 404 fallback
app.use((_req, res) => {
  res.status(404).json({ error: "Not found" });
});

// ── Boot ───────────────────────────────────────────────────────────────────────
connectDB()
  .then(() => {
    app.listen(PORT, () => {
      console.log(`🔍 Search API running → http://localhost:${PORT}`);
      console.log(`   Try: http://localhost:${PORT}/search?q=python`);
    });
  })
  .catch((err) => {
    console.error("Failed to connect to MongoDB:", err);
    process.exit(1);
  });

// Graceful shutdown
process.on("SIGINT", async () => {
  await closeDB();
  process.exit(0);
});
process.on("SIGTERM", async () => {
  await closeDB();
  process.exit(0);
});
