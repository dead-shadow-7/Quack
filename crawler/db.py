from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["search_engine"]

documents = db["documents"]
visited   = db["visited"]

# Unique indexes prevent duplicate crawls
visited.create_index("url",  unique=True)
documents.create_index("url", unique=True)

# Optional: text index for full-text search later
documents.create_index([("title", "text"), ("content", "text")])