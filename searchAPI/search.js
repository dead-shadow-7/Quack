const NodeCache = require("node-cache");
const { getDB } = require("./db");
const { tokenize } = require("./utils");

// ── Cache: store results for 5 minutes (ttl in seconds) ──────────────────────
const cache = new NodeCache({ stdTTL: 300, checkperiod: 60 });

const MAX_RESULTS = 10;
const SNIPPET_CHARS = 160; // characters to show as result snippet

/**
 * Extract a short content snippet around the first matching term.
 */
function extractSnippet(content = "", terms = []) {
  if (!content) return "";

  // Try to find a sentence containing one of the query terms
  const lower = content.toLowerCase();
  let best = 0;

  for (const term of terms) {
    const pos = lower.indexOf(term);
    if (pos !== -1) {
      best = Math.max(0, pos - 40); // a little context before the match
      break;
    }
  }

  const raw = content.slice(best, best + SNIPPET_CHARS).trim();
  return (
    (best > 0 ? "…" : "") + raw + (raw.length === SNIPPET_CHARS ? "…" : "")
  );
}

/**
 * Combine TF-IDF and PageRank into a single ranking score.
 * PageRank is log-compressed so it nudges rather than dominates.
 */
function combinedScore(tfidf, pagerank = 0) {
  return tfidf * (1 + Math.log(1 + pagerank));
}

/**
 * Main search function.
 * @param {string} query   raw user query string
 * @returns {Promise<Array>} ranked results
 */
async function search(query) {
  if (!query || !query.trim()) return [];

  const cacheKey = query.trim().toLowerCase();
  const cached = cache.get(cacheKey);
  if (cached) return cached;

  const db = getDB();
  const terms = tokenize(query);

  if (terms.length === 0) return [];

  // ── Fetch all index entries in parallel ───────────────────────────────────
  const indexCol = db.collection("index");
  const entries = await Promise.all(
    terms.map((term) => indexCol.findOne({ word: term })),
  );

  // ── Aggregate scores across terms ─────────────────────────────────────────
  const docMap = {}; // url → { url, title, tfidf, pagerank }

  for (const entry of entries) {
    if (!entry) continue;

    for (const doc of entry.docs) {
      const { url, title, score, pagerank = 0 } = doc;

      if (!docMap[url]) {
        docMap[url] = { url, title, tfidf: 0, pagerank };
      }

      docMap[url].tfidf += score;
      // Keep the highest pagerank seen (all entries for same url are identical)
      docMap[url].pagerank = Math.max(docMap[url].pagerank, pagerank);
    }
  }

  // ── Sort by combined score ─────────────────────────────────────────────────
  const ranked = Object.values(docMap)
    .map((d) => ({
      url: d.url,
      title: d.title,
      score: combinedScore(d.tfidf, d.pagerank),
    }))
    .sort((a, b) => b.score - a.score)
    .slice(0, MAX_RESULTS);

  // ── Fetch snippets from documents collection (parallel) ───────────────────
  const docsCol = db.collection("documents");
  const topUrls = ranked.map((r) => r.url);

  const docCursor = await docsCol
    .find({ url: { $in: topUrls } }, { projection: { url: 1, content: 1 } })
    .toArray();

  const contentMap = {};
  for (const d of docCursor) contentMap[d.url] = d.content || "";

  const results = ranked.map((r) => ({
    url: r.url,
    title: r.title,
    snippet: extractSnippet(contentMap[r.url], terms),
    score: Math.round(r.score * 1e6) / 1e6,
  }));

  cache.set(cacheKey, results);
  return results;
}

module.exports = { search };
