const express = require("express");
const cors = require("cors");

const { connectDB } = require("./db");
const { search } = require("./search");

const app = express();

app.use(cors());

app.get("/search", async (req, res) => {
  const query = req.query.q;

  if (!query) {
    return res.json([]);
  }

  const results = await search(query);

  res.json(results);
});

const PORT = 3000;

connectDB().then(() => {
  app.listen(PORT, () => {
    console.log("Search API running on port", PORT);
  });
});
