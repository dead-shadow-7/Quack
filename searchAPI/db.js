const { MongoClient } = require("mongodb");

let db;
let client;

async function connectDB() {
  client = new MongoClient("mongodb://localhost:27017/", {
    maxPoolSize: 10, // connection pool — reuse connections
    serverSelectionTimeoutMS: 5000,
  });

  await client.connect();
  db = client.db("search_engine");
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
