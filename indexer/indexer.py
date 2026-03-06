"""
indexer.py  —  Production-grade inverted index builder

Pipeline:
  Phase 0  →  count total documents
  Phase 1  →  paginate docs, tokenize ONCE, cache tokens, compute DF
  Phase 1b →  paginate link graph, compute PageRank (scipy sparse)
  Phase 2  →  compute TF-IDF per doc (from cache), stream postings → Redis
  Phase 3  →  flush Redis → MongoDB in one bulk write per word batch
  Cleanup  →  clear Redis workspace

Why Redis?
  • $push to MongoDB gets slower as arrays grow (O(n) rewrites)
  • Redis RPUSH is O(1) regardless of list size — speed stays flat at 50k+ docs
  • Crash-safe: if Phase 2 dies, Redis still has partial data
  • Observable: inspect with redis-cli while running

Requirements:
  pip install pymongo redis numpy scipy
  docker run -d -p 6379:6379 redis:alpine   (or any local Redis)
"""

import math
import time
import json
from collections import defaultdict

import numpy as np
from scipy.sparse import lil_matrix
from pymongo import UpdateOne

import redis

from db import documents, index
from text_processing import process_text

# ── Config ─────────────────────────────────────────────────────────────────────
TITLE_BOOST    = 2.5    # title tokens weighted this many extra times
BULK_BATCH     = 500    # MongoDB bulk_write batch size
PAGE_SIZE      = 200    # docs fetched per MongoDB round-trip
REDIS_PREFIX   = "idx:" # Redis key prefix  e.g. idx:python
REDIS_PIPE_SZ  = 200    # Redis pipeline flush size (commands per round-trip)
PR_ITERATIONS  = 20     # PageRank power-iteration steps
PR_DAMPING     = 0.85   # standard PageRank damping factor

# ── Redis connection ───────────────────────────────────────────────────────────
r = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)


# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def fetch_page(last_id, projection: dict) -> tuple:
    """
    Fetch one page of documents via _id pagination.
    Returns (batch, new_last_id).
    No cursor is kept open between calls — zero timeout risk.
    """
    query = {} if last_id is None else {"_id": {"$gt": last_id}}
    batch = list(
        documents
        .find(query, projection)
        .sort("_id", 1)
        .limit(PAGE_SIZE)
    )
    return batch, (batch[-1]["_id"] if batch else None)


def print_progress(done: int, total: int, t_start: float, label: str = ""):
    elapsed = time.time() - t_start
    rate    = done / max(elapsed, 0.001)
    eta     = max(0, (total - done) / max(rate, 0.001))  # clamp to 0, no negatives
    bar     = ("█" * min(30, done * 30 // max(total, 1))).ljust(30, "░")
    print(
        f"  [{bar}] {done:>7,}/{total:,}  "
        f"{rate:.1f}/s  ETA {eta:.0f}s  {label}"
    )


# ══════════════════════════════════════════════════════════════════════════════
# PageRank
# ══════════════════════════════════════════════════════════════════════════════

def compute_pagerank(url_list: list[str], link_map: dict[str, list[str]]) -> dict[str, float]:
    print("  Building sparse transition matrix…")
    url_ix = {url: i for i, url in enumerate(url_list)}
    N      = len(url_list)

    if N == 0:
        return {}

    M = lil_matrix((N, N), dtype=np.float32)

    for src, targets in link_map.items():
        i       = url_ix.get(src)
        targets = [url_ix[t] for t in targets if t in url_ix]
        if i is not None and targets:
            for j in targets:
                M[j, i] = 1.0 / len(targets)

    M  = M.tocsr()
    pr = np.full(N, 1.0 / N, dtype=np.float32)

    for step in range(PR_ITERATIONS):
        dangling = float(pr[[i for i, u in enumerate(url_list)
                              if not link_map.get(u)]].sum())
        pr = ((1 - PR_DAMPING) / N
              + PR_DAMPING * (M.dot(pr) + dangling / N))
        if (step + 1) % 5 == 0:
            print(f"    iteration {step + 1}/{PR_ITERATIONS}  "
                  f"max={float(pr.max()):.6f}")

    return {url: float(pr[i]) for i, url in enumerate(url_list)}


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def build_index():
    t_total = time.time()

    # ── sanity check Redis ─────────────────────────────────────────────────────
    try:
        r.ping()
        print("✅ Redis connected")
    except redis.ConnectionError:
        print("   Redis not reachable — start it with:")
        print("   docker run -d -p 6379:6379 redis:alpine")
        return

    # ── Phase 0 ───────────────────────────────────────────────────────────────
    print("\n" + "─" * 64)
    print("  Phase 0 — counting documents…")
    total = documents.count_documents({})
    if total == 0:
        print("  No documents found. Run the crawler first.")
        return
    print(f"  Total documents : {total:,}")

    # ── Phase 1: tokenize once, cache, compute DF ─────────────────────────────
    print("\n  Phase 1 — tokenising & caching (single pass)…")

    # token_cache[url] = (content_tokens, title_tokens, title_str)
    token_cache: dict[str, tuple] = {}
    doc_freq:    dict[str, int]   = defaultdict(int)
    last_id = None
    done    = 0
    t1      = time.time()

    while True:
        batch, last_id = fetch_page(last_id, {"url": 1, "content": 1, "title": 1})
        if not batch:
            break

        for doc in batch:
            url   = doc.get("url",     "")
            title = doc.get("title",   "")
            ct    = process_text(doc.get("content", ""))
            tt    = process_text(title)

            token_cache[url] = (ct, tt, title)

            for word in set(ct) | set(tt):
                doc_freq[word] += 1

            done += 1

        if done % 2000 == 0 or done == total:
            print_progress(done, total, t1, "tokenising")

    print(f"  Unique words    : {len(doc_freq):,}")
    print(f"  Phase 1 done in {time.time()-t1:.1f}s")

    # ── Phase 1b: PageRank ────────────────────────────────────────────────────
    print("\n  Phase 1b — computing PageRank…")
    t1b     = time.time()
    last_id = None
    url_list: list[str]        = []
    link_map: dict[str, list]  = {}

    while True:
        batch, last_id = fetch_page(last_id, {"url": 1, "links": 1})
        if not batch:
            break
        for doc in batch:
            url = doc.get("url", "")
            url_list.append(url)
            link_map[url] = doc.get("links", [])

    pagerank = compute_pagerank(url_list, link_map)
    del url_list, link_map          # free RAM
    print(f"  Phase 1b done in {time.time()-t1b:.1f}s")

    # ── Phase 2: TF-IDF → Redis ───────────────────────────────────────────────
    print("\n  Phase 2 — computing TF-IDF and streaming to Redis…")

    # Clear any leftover Redis keys from a previous crashed run
    existing = r.keys(f"{REDIS_PREFIX}*")
    if existing:
        print(f"  Clearing {len(existing):,} leftover Redis keys…")
        r.delete(*existing)

    t2             = time.time()
    docs_processed = 0
    pipe           = r.pipeline(transaction=False)
    pipe_count     = 0
    default_pr     = 1.0 / max(len(pagerank), 1)

    for url, (content_tokens, title_tokens, title) in token_cache.items():
        pr             = pagerank.get(url, default_pr)
        boosted_tokens = content_tokens + title_tokens * int(TITLE_BOOST)
        token_count    = len(boosted_tokens)

        if token_count == 0:
            continue

        tf_counts: dict[str, int] = defaultdict(int)
        for word in boosted_tokens:
            tf_counts[word] += 1

        for word, count in tf_counts.items():
            tf    = count / token_count
            idf   = math.log(1 + total / (1 + doc_freq.get(word, 0)))
            tfidf = round(tf * idf, 6)

            # Store as compact pipe-delimited string to save Redis RAM
            entry = f"{url}\x00{title}\x00{tfidf}\x00{round(pr, 8)}"
            pipe.rpush(f"{REDIS_PREFIX}{word}", entry)
            pipe_count += 1

            if pipe_count >= REDIS_PIPE_SZ:
                pipe.execute()
                pipe_count = 0

        docs_processed += 1

        if docs_processed % 2000 == 0 or docs_processed == total:
            if pipe_count:
                pipe.execute()
                pipe_count = 0
            print_progress(docs_processed, total, t2, "→ Redis")

    if pipe_count:
        pipe.execute()

    del token_cache, pagerank       # free RAM before flush
    print(f"  Phase 2 done in {time.time()-t2:.1f}s")

    # ── Phase 3: flush Redis → MongoDB ───────────────────────────────────────
    print("\n  Phase 3 — flushing Redis → MongoDB…")

    index.drop()
    index.create_index("word", unique=True)

    
    total_redis_keys = r.dbsize()
    print(f"  Words to flush  : {total_redis_keys:,}")

    t3          = time.time()
    total_words = 0
    ops:  list[UpdateOne] = []
    scan_cursor = 0         # line 258: string

    def flush_mongo():
        if ops:
            index.bulk_write(ops, ordered=False)
            ops.clear()

    while True:
        scan_cursor, keys = r.scan(scan_cursor, match=f"{REDIS_PREFIX}*", count=500)

        for key in keys:
            word     = key[len(REDIS_PREFIX):]
            entries  = r.lrange(key, 0, -1)
            postings = []

            for e in entries:
                parts = e.split("\x00")
                if len(parts) == 4:
                    postings.append({
                        "url":      parts[0],
                        "title":    parts[1],
                        "score":    float(parts[2]),
                        "pagerank": float(parts[3]),
                    })

            # Sort postings by score desc so search.js gets them pre-ranked
            postings.sort(key=lambda x: x["score"], reverse=True)

            ops.append(
                UpdateOne(
                    {"word": word},
                    {"$set": {"docs": postings}},
                    upsert=True,
                )
            )
            total_words += 1

            if len(ops) >= BULK_BATCH:
                flush_mongo()
                
                if total_words % 50000 == 0:
                    print_progress(total_words, total_redis_keys, t3, "→ MongoDB")

        if scan_cursor == 0:
            break

    flush_mongo()

    
    print_progress(total_words, total_redis_keys, t3, "→ MongoDB")
    print(f"  Phase 3 done in {time.time()-t3:.1f}s")

    # ── Cleanup Redis ─────────────────────────────────────────────────────────
    print("\n  Cleaning up Redis…")
    all_keys = r.keys(f"{REDIS_PREFIX}*")
    if all_keys:
        r.delete(*all_keys)
    print("  Redis cleared ✅")

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = time.time() - t_total
    mins    = int(elapsed // 60)
    secs    = int(elapsed % 60)
    print(
        f"\n{'─'*64}\n"
        f"  ✅  Indexing complete\n"
        f"  Documents indexed : {docs_processed:,}\n"
        f"  Unique words      : {total_words:,}\n"
        f"  Total time        : {mins}m {secs}s\n"
        f"{'─'*64}"
    )


if __name__ == "__main__":
    build_index()