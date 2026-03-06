from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["search_engine"]    

documents = db["documents"]     
visited = db["visited"]


visited.create_index("url", unique=True)
documents.create_index("url", unique=True)