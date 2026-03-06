"""
indexer.py  –  Builds an inverted TF-IDF index with:
  • Streaming cursor   (no full corpus loaded into RAM)
  • Smoothed IDF       (no division-by-zero / negative scores)
  • Title boost        (x2.5 weight for title tokens)
  • Bulk writes        (one MongoDB round-trip per word batch)
  • PageRank           (computed from the stored link graph)
  • Two-pass design    (pass 1: count DF  |  pass 2: score & write)
"""

import math
import time
from collections import defaultdict

from pymongo import UpdateOne
from db import documents, index
from text_processing import process_text

import numpy as np
from scipy.sparse import lil_matrix
from scipy.sparse.linalg import norm

# ── Config ─────────────────────────────────────────────────────────────────────
TITLE_BOOST   = 2.5      # weight multiplier for tokens that appear in the title
BULK_BATCH    = 500      # number of UpdateOne ops per bulk_write call
PR_ITERATIONS = 20       # PageRank power-iteration steps
PR_DAMPING    = 0.85     # standard damping factor


# ══════════════════════════════════════════════════════════════════════════════
# PageRank
# ══════════════════════════════════════════════════════════════════════════════

def compute_pagerank(docs):
    print("  Computing PageRank…")
    urls   = [d["url"] for d in docs]
    url_ix = {url: i for i, url in enumerate(urls)}
    N      = len(urls)
    if N == 0:
        return {}

    M = lil_matrix((N, N), dtype=np.float32)

    for d in docs:
        src  = d["url"]
        i    = url_ix.get(src)
        targets = [url_ix[l] for l in d.get("links", []) if l in url_ix]
        if targets:
            for j in targets:
                M[j, i] = 1.0 / len(targets)

    M = M.tocsr()
    pr = np.full(N, 1.0 / N, dtype=np.float32)

    for step in range(PR_ITERATIONS):
        pr = (1 - PR_DAMPING) / N + PR_DAMPING * M.dot(pr)
        if (step + 1) % 5 == 0:
            print(f"    PageRank iteration {step+1}/{PR_ITERATIONS}")

    return {url: float(pr[i]) for i, url in enumerate(urls)}

# ══════════════════════════════════════════════════════════════════════════════
# Main indexer
# ══════════════════════════════════════════════════════════════════════════════

def build_index():
    t_start = time.time()

    print("─" * 60)
    print("  Phase 0 — counting documents…")
    total = documents.count_documents({})
    if total == 0:
        print("  No documents found. Run the crawler first.")
        return
    print(f"  Total documents: {total:,}")

    # ── Phase 1: stream once to collect document frequencies ──────────────────
    print("\n  Phase 1 — computing document frequencies…")
    doc_freq: dict[str, int] = defaultdict(int)   # word → # docs containing it

    for doc in documents.find({}, {"content": 1, "title": 1}, no_cursor_timeout=True):
        content_tokens = set(process_text(doc.get("content", "")))
        title_tokens   = set(process_text(doc.get("title",   "")))
        all_tokens     = content_tokens | title_tokens
        for word in all_tokens:
            doc_freq[word] += 1

    print(f"  Unique words found: {len(doc_freq):,}")

    # ── Phase 1b: load docs list (urls + links only) for PageRank ─────────────
    print("\n  Phase 1b — loading link graph for PageRank…")
    docs_for_pr = list(
        documents.find({}, {"url": 1, "links": 1, "_id": 0})
    )
    pagerank = compute_pagerank(docs_for_pr)

    # ── Phase 2: stream again, compute TF-IDF, bulk-write to index ────────────
    print("\n  Phase 2 — building TF-IDF index…")

    index.drop()
    index.create_index("word", unique=True)

    # word → list of {url, title, score, pagerank}
    word_postings: dict[str, list[dict]] = defaultdict(list)
    ops_pending: list[UpdateOne] = []
    docs_processed = 0

    def flush_ops():
        if ops_pending:
            index.bulk_write(ops_pending, ordered=False)
            ops_pending.clear()

    for doc in documents.find(
        {},
        {"url": 1, "title": 1, "content": 1, "_id": 0},
        no_cursor_timeout=True,
    ):
        url   = doc.get("url",   "")
        title = doc.get("title", "")
        pr    = pagerank.get(url, 1.0 / max(len(pagerank), 1))

        content_tokens = process_text(doc.get("content", ""))
        title_tokens   = process_text(title)

        # Build boosted token list: title tokens added TITLE_BOOST times
        boosted_tokens = content_tokens + title_tokens * int(TITLE_BOOST)
        token_count    = len(boosted_tokens)

        if token_count == 0:
            continue

        # TF counts over the boosted list
        tf_counts: dict[str, int] = defaultdict(int)
        for word in boosted_tokens:
            tf_counts[word] += 1

        for word, count in tf_counts.items():
            tf  = count / token_count
            # Smoothed IDF — never zero, never negative
            idf = math.log(1 + total / (1 + doc_freq.get(word, 0)))
            tfidf = tf * idf

            ops_pending.append(
                UpdateOne(
                    {"word": word},
                    {"$push": {"docs": {
                        "url":      url,
                        "title":    title,
                        "score":    round(tfidf, 6),
                        "pagerank": round(pr, 8),
                    }}},
                    upsert=True,
                )
            )

            if len(ops_pending) >= BULK_BATCH:
                flush_ops()

        docs_processed += 1
        if docs_processed % 500 == 0:
            flush_ops()
            elapsed = time.time() - t_start
            rate    = docs_processed / elapsed
            eta     = (total - docs_processed) / max(rate, 0.001)
            print(
                f"  [{docs_processed:>6,} / {total:,}]  "
                f"{rate:.1f} docs/s  |  ETA {eta:.0f}s"
            )

    flush_ops()

    elapsed = time.time() - t_start
    print(
        f"\n{'─'*60}\n"
        f"  ✅  Indexing complete\n"
        f"  Documents indexed : {docs_processed:,}\n"
        f"  Unique words      : {len(doc_freq):,}\n"
        f"  Time elapsed      : {elapsed:.1f}s\n"
        f"{'─'*60}"
    )


if __name__ == "__main__":
    build_index()