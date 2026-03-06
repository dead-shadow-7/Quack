from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["search_engine"]    

documents = db["documents"]     
index = db["index"]

index.create_index("word", unique=True)