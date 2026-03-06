const natural = require("natural");

const stemmer = natural.PorterStemmer;

// Mirror the Python stopword list closely enough for consistent stemming
const STOPWORDS = new Set([
  "a",
  "an",
  "the",
  "and",
  "or",
  "but",
  "in",
  "on",
  "at",
  "to",
  "for",
  "of",
  "with",
  "by",
  "from",
  "is",
  "was",
  "are",
  "were",
  "be",
  "been",
  "being",
  "have",
  "has",
  "had",
  "do",
  "does",
  "did",
  "will",
  "would",
  "could",
  "should",
  "may",
  "might",
  "shall",
  "can",
  "need",
  "this",
  "that",
  "these",
  "those",
  "it",
  "its",
  "i",
  "my",
  "we",
  "our",
  "you",
  "your",
  "he",
  "his",
  "she",
  "her",
  "they",
  "their",
  "what",
  "which",
  "who",
  "how",
  "when",
  "where",
  "why",
  "all",
  "each",
  "every",
  "both",
  "few",
  "more",
  "most",
  "other",
  "some",
  "such",
  "no",
  "not",
  "only",
  "same",
  "so",
  "than",
  "too",
  "very",
  "just",
  "as",
  "if",
  "up",
  "out",
  "about",
  "into",
  "over",
]);

const MAX_QUERY_TERMS = 20; // prevent abuse / huge queries

/**
 * Tokenize, remove stopwords, stem — mirrors the Python text_processing.py
 * pipeline so query terms match what's stored in the index.
 *
 * @param {string} text
 * @returns {string[]} stemmed tokens
 */
function tokenize(text) {
  if (!text || typeof text !== "string") return [];

  return text
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, " ")
    .split(/\s+/)
    .filter((w) => w.length > 1 && !STOPWORDS.has(w))
    .slice(0, MAX_QUERY_TERMS)
    .map((w) => stemmer.stem(w));
}

module.exports = { tokenize };
