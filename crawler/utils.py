from urllib.parse import urljoin, urlparse

def normalize_url(base, link):
    url = urljoin(base, link)
    parsed = urlparse(url)

    clean = parsed.scheme + "://" + parsed.netloc + parsed.path
    return clean