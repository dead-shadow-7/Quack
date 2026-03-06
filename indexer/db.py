from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["search_engine"]

# ── Collections ────────────────────────────────────────────────────────────────
documents = db["documents"]
visited   = db["visited"]
index     = db["index"]

# ── Crawler indexes ────────────────────────────────────────────────────────────
visited.create_index("url", unique=True)
documents.create_index("url", unique=True)

# Full-text search (MongoDB native, used as fallback)
documents.create_index([("title", "text"), ("content", "text")])

# ── Indexer indexes ────────────────────────────────────────────────────────────
index.create_index("word", unique=True)