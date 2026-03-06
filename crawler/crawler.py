import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from db import documents, visited
from utils import normalize_url
import time

queue = [
    "https://www.python.org/",
    "https://docs.python.org/",
    "https://pypi.org/",

    "https://www.wikipedia.org/",
    "https://en.wikipedia.org/wiki/Main_Page",

    "https://news.ycombinator.com/",
    "https://stackoverflow.com/",
    "https://github.com/explore",

    "http://books.toscrape.com/",
    "http://quotes.toscrape.com/",

    "https://realpython.com/",
    "https://www.geeksforgeeks.org/",
    "https://developer.mozilla.org/",
    "https://www.w3schools.com/"
]

MAX_PAGES = 2000

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

EXCLUDED_EXTENSIONS = {'.exe', '.zip', '.pdf', '.png', '.jpg', '.jpeg', '.gif', '.mp4', '.mp3', '.rar', '.7z', '.tar', '.gz', '.svg', '.ico', '.css', '.js'}

def crawl():

    count = 0
    print("Starting crawl, queue length:", len(queue))

    while queue and count < MAX_PAGES:

        url = queue.pop(0)
        print("Processing:", url)

        if visited.find_one({"url": url}):
            print("Already visited:", url)
            continue

        try:
            
            if any(url.lower().endswith(ext) for ext in EXCLUDED_EXTENSIONS):
                print("Skipping non-HTML:", url)
                continue

            head = requests.head(url, timeout=5, headers=HEADERS, allow_redirects=True)
            content_type = head.headers.get("Content-Type", "")

            if "text/html" not in content_type:
                print("Skipping non-HTML content:", content_type, url)
                continue

            res = requests.get(url, timeout=5, headers=HEADERS)

            if res.status_code != 200:
                print("Non-200 status:", res.status_code, url)
                continue

            soup = BeautifulSoup(res.text, "html.parser")

            title = soup.title.string if soup.title else ""

            text = " ".join(soup.get_text().split())

            documents.insert_one({
                "url": url,
                "title": title,
                "content": text
            })

            visited.insert_one({"url": url})

            print("Crawled:", url)

            for a in soup.find_all("a", href=True):

                link = normalize_url(url, a["href"])

                if not visited.find_one({"url": link}) and link not in queue:
                    queue.append(link)

            count += 1

            time.sleep(1)

        except Exception as e:
            print("Error:", url, e)


if __name__ == "__main__":
    crawl()