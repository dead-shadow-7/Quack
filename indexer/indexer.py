import math
from db import documents, index
from text_processing import process_text

def build_index():
    index.drop()
    index.create_index("word", unique=True)
    all_docs = list(documents.find())
    total = len(all_docs)

    
    doc_freq = {}
    doc_tokens = {}

    for doc in all_docs:
        tokens = process_text(doc["content"])
        doc_tokens[doc["_id"]] = tokens
        unique_tokens = set(tokens)
        for word in unique_tokens:
            doc_freq[word] = doc_freq.get(word, 0) + 1

    
    for doc in all_docs:
        tokens = doc_tokens[doc["_id"]]
        token_count = len(tokens)

        if token_count == 0:
            continue

       
        tf_counts = {}
        for word in tokens:
            tf_counts[word] = tf_counts.get(word, 0) + 1

        for word, count in tf_counts.items():
            tf = count / token_count
            idf = math.log(total / doc_freq[word])
            score = tf * idf

            index.update_one(
                {"word": word},
                {"$push": {"docs": {"url": doc["url"], "title": doc["title"], "score": score}}},
                upsert=True
            )

    print("Indexing complete:", len(doc_freq), "unique words")

if __name__ == "__main__":
    build_index()