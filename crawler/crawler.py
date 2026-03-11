import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import urllib.robotparser
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from db import documents, visited
from utils import normalize_url
import threading
import time

# ─── Config ────────────────────────────────────────────────────────────────────
MAX_PAGES       = 20000
MAX_DEPTH       = 3
MAX_CONTENT_CHARS = 50000
THREADS         = 64
CRAWL_DELAY     = 0.3
BUFFER_SIZE     = 100
TIMEOUT         = 8
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}
EXCLUDED_EXTENSIONS = {
    ".exe", ".zip", ".pdf", ".png", ".jpg", ".jpeg", ".gif",
    ".mp4", ".mp3", ".rar", ".7z", ".tar", ".gz", ".svg",
    ".ico", ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
}
JUNK_TAGS = ["script", "style", "nav", "footer", "header", "aside", "noscript"]

# ─── Seed URLs (popular sites first, deep pages later) ─────────────────────────
SEED_URLS = [
    # Tech & Programming
    "https://www.python.org/",
    "https://docs.python.org/",
    "https://pypi.org/",
    "https://nodejs.org/",
    "https://developer.mozilla.org/",
    "https://www.w3schools.com/",
    "https://stackoverflow.com/",
    "https://github.com/explore",
    "https://dev.to/",
    "https://hackernoon.com/",
    "https://css-tricks.com/",
    "https://www.smashingmagazine.com/",

    # Science & Knowledge
    "https://www.wikipedia.org/",
    "https://en.wikipedia.org/wiki/Main_Page",
    "https://www.britannica.com/",
    "https://www.sciencedaily.com/",
    "https://phys.org/",
    "https://www.nationalgeographic.com/",
    "https://arxiv.org/",
    "https://pubmed.ncbi.nlm.nih.gov/",

    # News & Current Events
    "https://news.ycombinator.com/",
    "https://www.bbc.com/news",
    "https://www.reuters.com/",
    "https://apnews.com/",
    "https://www.theguardian.com/",
    "https://techcrunch.com/",
    "https://arstechnica.com/",
    "https://www.wired.com/",

    # Learning & Education
    "https://realpython.com/",
    "https://www.geeksforgeeks.org/",
    "https://www.khanacademy.org/",
    "https://www.freecodecamp.org/",
    "https://www.tutorialspoint.com/",

    # Reference & Docs
    "https://docs.djangoproject.com/",
    "https://flask.palletsprojects.com/",
    "https://fastapi.tiangolo.com/",
    "https://numpy.org/doc/",
    "https://pandas.pydata.org/docs/",
    "https://pytorch.org/docs/",
    "https://www.tensorflow.org/",
    "https://kubernetes.io/docs/",
    "https://docs.docker.com/",

    # Business & Finance
    "https://www.investopedia.com/",
    "https://hbr.org/",

    # Health & Medicine
    "https://www.webmd.com/",
    "https://medlineplus.gov/",
    "https://www.healthline.com/",
    "https://www.mayoclinic.org/",

    # Scraping-Friendly
    "http://books.toscrape.com/",
    "http://quotes.toscrape.com/",

    # General Interest
    "https://www.reddit.com/r/programming/",
    "https://www.reddit.com/r/science/",
    "https://www.reddit.com/r/technology/",
    "https://medium.com/topic/technology",
]

# ─── Shared state ──────────────────────────────────────────────────────────────
queue_lock      = threading.Lock()
stats_lock      = threading.Lock()
queue: deque    = deque((url, 0) for url in SEED_URLS)   # (url, depth)
queued_set: set = set(SEED_URLS)                          # fast membership test
robots_cache: dict = {}
robots_lock = threading.Lock()
stats = {
    "crawled":  0,
    "failed":   0,
    "skipped":  0,
    "current":  "",          # last URL being processed
    "active":   0,           # threads currently crawling
}

# Write buffer – batch-insert to MongoDB
write_buffer: list = []
buffer_lock = threading.Lock()


# ─── Helpers ───────────────────────────────────────────────────────────────────

def is_excluded(url: str) -> bool:
    parsed = urlparse(url)
    return any(parsed.path.lower().endswith(ext) for ext in EXCLUDED_EXTENSIONS)

def is_allowed(url: str) -> bool:
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    
    with robots_lock:
        if base not in robots_cache:
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(f"{base}/robots.txt")
            try:
                rp.read()
            except Exception:
                rp = None  # if robots.txt unreachable, assume allowed
            robots_cache[base] = rp
        rp = robots_cache[base]
    
    if rp is None:
        return True
    return rp.can_fetch(HEADERS["User-Agent"], url)

def clean_text(soup: BeautifulSoup) -> str:
    """Remove junk tags, then extract plain text."""
    for tag in soup(JUNK_TAGS):
        tag.extract()
    return " ".join(soup.get_text().split())[:MAX_CONTENT_CHARS]


def flush_buffer(force: bool = False):
    """Write buffered docs to MongoDB when buffer is full or forced."""
    with buffer_lock:
        if not write_buffer:
            return
        if not force and len(write_buffer) < BUFFER_SIZE:
            return
        batch = write_buffer.copy()
        write_buffer.clear()

    if batch:
        try:
            documents.insert_many(batch, ordered=False)
        except Exception:
            pass  # duplicate-key errors are fine (unique index on url)


def print_stats():
    with stats_lock:
        c = stats["crawled"]
        f = stats["failed"]
        s = stats["skipped"]
        q = len(queue)
        a = stats["active"]
        current = stats["current"]
    bar = "█" * min(40, c * 40 // max(MAX_PAGES, 1))
    bar = bar.ljust(40, "░")
    pct = c * 100 // max(MAX_PAGES, 1)
    print(
        f"\n{'─'*70}\n"
        f"  Progress : [{bar}] {pct}%\n"
        f"  Crawled  : {c:,}   Failed: {f:,}   Skipped: {s:,}\n"
        f"  Queue    : {q:,}   Threads active: {a}\n"
        f"  Current  : {current}\n"
        f"{'─'*70}"
    )


# ─── Core crawl worker ─────────────────────────────────────────────────────────

def crawl_url(url: str, depth: int):
    with stats_lock:
        stats["active"] += 1
        stats["current"] = url

    new_links = []

    try:
        # Already visited?
        if visited.find_one({"url": url}):
            with stats_lock:
                stats["skipped"] += 1
            return new_links

        if is_excluded(url):
            with stats_lock:
                stats["skipped"] += 1
            return new_links

        if not is_allowed(url):
            with stats_lock:
                stats["skipped"] += 1
            return new_links

        # HEAD check for content-type
        head = requests.head(url, timeout=TIMEOUT, headers=HEADERS, allow_redirects=True)
        if "text/html" not in head.headers.get("Content-Type", ""):
            with stats_lock:
                stats["skipped"] += 1
            return new_links

        res = requests.get(url, timeout=TIMEOUT, headers=HEADERS)
        if res.status_code != 200:
            with stats_lock:
                stats["failed"] += 1
            return new_links

        soup   = BeautifulSoup(res.text, "html.parser")
        title  = soup.title.string.strip() if soup.title and soup.title.string else ""
        text   = clean_text(soup)

        # Collect outgoing links (store full list for graph)
        raw_links = []
        for a in soup.find_all("a", href=True):
            lnk = normalize_url(url, a["href"])
            if lnk:
                raw_links.append(lnk)

        # Mark visited in Mongo (upsert-style via unique index)
        try:
            visited.insert_one({"url": url})
        except Exception:
            with stats_lock:
                stats["skipped"] += 1
            return new_links

        # Buffer the document
        with buffer_lock:
            write_buffer.append({
                "url":   url,
                "title": title,
                "content": text,
                "links": raw_links,       # web graph
                "depth": depth,
            })

        flush_buffer()

        with stats_lock:
            stats["crawled"] += 1
            c = stats["crawled"]

        if c % 25 == 0:
            print_stats()

        # Return new links for the scheduler to enqueue
        if depth < MAX_DEPTH:
            new_links = raw_links

        time.sleep(CRAWL_DELAY)

    except Exception as e:
        with stats_lock:
            stats["failed"] += 1
        print(f"  ✗ Error [{url}]: {e}")

    finally:
        with stats_lock:
            stats["active"] -= 1

    return new_links


# ─── Main loop ─────────────────────────────────────────────────────────────────

def crawl():
    print(f"🕷  Crawler starting — MAX_PAGES={MAX_PAGES:,}  THREADS={THREADS}  DEPTH={MAX_DEPTH}")
    print(f"   Seed URLs: {len(SEED_URLS)}  |  Initial queue: {len(queue)}\n")

    with ThreadPoolExecutor(max_workers=THREADS) as pool:
        futures = {}

        while True:
            with stats_lock:
                total_done = stats["crawled"] + stats["failed"] + stats["skipped"]

            if total_done >= MAX_PAGES:
                break

            # Fill the thread pool from the queue
            with queue_lock:
                while queue and len(futures) < THREADS * 2:
                    url, depth = queue.popleft()
                    fut = pool.submit(crawl_url, url, depth)
                    futures[fut] = (url, depth)

            if not futures:
                break

            # Collect completed futures
            done_futures = [f for f in futures if f.done()]
            for fut in done_futures:
                url, depth = futures.pop(fut)
                new_links = fut.result()

                # Enqueue new links if under depth limit
                if depth < MAX_DEPTH:
                    with queue_lock:
                        for lnk in new_links:
                            if lnk not in queued_set:
                                queued_set.add(lnk)
                                queue.append((lnk, depth + 1))

            time.sleep(0.05)

    # Final flush
    flush_buffer(force=True)
    print("\n✅  Crawl complete!")
    print_stats()


if __name__ == "__main__":
    crawl()