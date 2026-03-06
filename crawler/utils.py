from urllib.parse import urljoin, urlparse, urlencode, parse_qs

# Query params that are purely tracking noise – strip them
TRACKING_PARAMS = {
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "ref", "referrer", "fbclid", "gclid", "mc_cid", "mc_eid",
    "source", "via", "share", "from",
}


def normalize_url(base: str, link: str) -> str | None:
    """
    Resolve `link` against `base`, then canonicalize:
      - drop fragment  (#section)
      - strip tracking query params
      - remove trailing slash from path
      - lowercase scheme + netloc
    Returns None for non-http(s) URLs (mailto:, javascript:, etc.)
    """
    try:
        url    = urljoin(base, link)
        parsed = urlparse(url)

        # Only index http / https
        if parsed.scheme not in ("http", "https"):
            return None

        # Strip tracking params; keep the rest
        original_params = parse_qs(parsed.query, keep_blank_values=True)
        clean_params    = {
            k: v for k, v in original_params.items()
            if k.lower() not in TRACKING_PARAMS
        }
        clean_query = urlencode(clean_params, doseq=True) if clean_params else ""

        # Rebuild – lowercase scheme/host, no fragment, trimmed path
        path = parsed.path.rstrip("/") or "/"
        clean = f"{parsed.scheme.lower()}://{parsed.netloc.lower()}{path}"
        if clean_query:
            clean += f"?{clean_query}"

        return clean

    except Exception:
        return None