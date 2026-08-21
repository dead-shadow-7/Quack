const path = require("path");

const express = require("express");
const cors = require("cors");

const { connectDB, closeDB } = require("./db");
const { search } = require("./search");

const app = express();

// Overridable for Docker/AWS; the defaults are the original local values.
const PORT = Number(process.env.PORT) || 3000;
const HOST = process.env.HOST || "0.0.0.0"; // containers must not bind loopback
const FRONTEND_DIR =
  process.env.FRONTEND_DIR || path.join(__dirname, "..", "frontend");

// ── Middleware ─────────────────────────────────────────────────────────────────
app.use(cors());
app.use(express.json());

// Simple request logger
app.use((req, _res, next) => {
  console.log(`[${new Date().toISOString()}] ${req.method} ${req.url}`);
  next();
});

// Serve the static frontend from this same process, so one container is enough.
// Mounted after the logger so asset requests still show up in `docker logs`.
// No-ops harmlessly if FRONTEND_DIR is absent (e.g. API-only deployments).
app.use(express.static(FRONTEND_DIR));

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
    app.listen(PORT, HOST, () => {
      console.log(`🔍 Search API running → http://localhost:${PORT}`);
      console.log(`   Try: http://localhost:${PORT}/search?q=python`);
      console.log(`   UI:  http://localhost:${PORT}/  (from ${FRONTEND_DIR})`);
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
