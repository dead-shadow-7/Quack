const { MongoClient } = require("mongodb");

// Overridable for Docker/AWS; the defaults are the original local values.
const MONGO_URI = process.env.MONGO_URI || "mongodb://localhost:27017/";
const MONGO_DB = process.env.MONGO_DB || "search_engine";

let db;
let client;

async function connectDB() {
  client = new MongoClient(MONGO_URI, {
    maxPoolSize: 10, // connection pool — reuse connections
    serverSelectionTimeoutMS: 5000,
  });

  await client.connect();
  db = client.db(MONGO_DB);
  console.log("Connected to MongoDB");
}

function getDB() {
  if (!db) throw new Error("DB not initialised — call connectDB() first");
  return db;
}

async function closeDB() {
  if (client) await client.close();
}

module.exports = { connectDB, getDB, closeDB };
