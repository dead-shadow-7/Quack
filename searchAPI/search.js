const { getDB } = require("./db");
const { tokenize } = require("./utils");

async function search(query) {
  const db = getDB();

  const terms = tokenize(query);

  const index = db.collection("index");

  let docScores = {};

  for (const term of terms) {
    const entry = await index.findOne({ word: term });

    if (!entry) continue;

    for (const doc of entry.docs) {
      const url = doc.url;

      if (!docScores[url]) {
        docScores[url] = { title: doc.title, url: doc.url, score: 0 };
      }

      docScores[url].score += doc.score;
    }
  }

  const results = Object.values(docScores)
    .sort((a, b) => b.score - a.score)
    .slice(0, 10);

  return results;
}

module.exports = { search };
