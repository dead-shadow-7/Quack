const { MongoClient } = require("mongodb");

let db;

async function connectDB() {
  const client = new MongoClient("mongodb://localhost:27017/");
  await client.connect();
  db = client.db("search_engine");
  console.log("Connected to MongoDB");
}

function getDB() {
  return db;
}

module.exports = { connectDB, getDB };
