import os

from pymongo import MongoClient

# Overridable for Docker/AWS; the defaults are the original local values.
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB  = os.environ.get("MONGO_DB",  "search_engine")

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]

documents = db["documents"]
visited   = db["visited"]

# Unique indexes prevent duplicate crawls
visited.create_index("url",  unique=True)
documents.create_index("url", unique=True)

# Optional: text index for full-text search later
documents.create_index([("title", "text"), ("content", "text")])