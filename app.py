from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
import requests
import markdownify
import json
import time
import os
import threading

# ---------------- CONFIG ----------------
MAX_PAGES = 10
MAX_DEPTH = 2
THREADS = 10

HEADERS = {"User-Agent": "Mozilla/5.0"}

# Proxy support via environment variable:
# export PROXY_URL="http://USER:PASS@HOST:PORT"

PROXIES = {"http": PROXY_URL, "https": PROXY_URL} if PROXY_URL else None
# ----------------------------------------


app = FastAPI(title="Streaming Markdown Crawler API")


# ---------- UTILITY FUNCTIONS ----------

def is_internal(base, link):
    """Check if link belongs to the same domain."""
    return urlparse(base).netloc == urlparse(link).netloc


def is_login_page(url):
    """Skip login / auth URLs."""
    login_keywords = ["login", "signin", "sign-in", "auth", "account"]
    return any(k in url.lower() for k in login_keywords)


def is_tracking(url):
    """Skip tracking URLs."""
    tracking_keys = ["utm_", "ref=", "tracking", "gclid", "fbclid"]
    return any(k in url.lower() for k in tracking_keys)


def extract_media(soup, base_url):
    """Collect PDF and image links."""
    pdfs = []
    images = []

    # PDF links
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if href.lower().endswith(".pdf"):
            full = urljoin(base_url, href)
            pdfs.append({"name": full.split("/")[-1], "url": full})

    # Image links
    for img in soup.find_all("img", src=True):
        src = img["src"]
        lower = src.lower()
        if any(lower.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"]):
            full = urljoin(base_url, src)
            images.append({"name": full.split("/")[-1], "url": full})

    return pdfs, images


# ---------- SINGLE PAGE CRAWLER ----------

def crawl_single(url, depth, base_url, visited, lock, max_depth, max_pages):

    if depth > max_depth:
        return None

    if is_login_page(url) or is_tracking(url):
        return None

    # avoid duplicates + limit pages
    with lock:
        if url in visited or len(visited) >= max_pages:
            return None
        visited.add(url)

    print(f"[Crawling] {url} (depth={depth})")

    try:
        response = requests.get(url, headers=HEADERS, proxies=PROXIES, timeout=15)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, "html.parser")

        markdown = markdownify.markdownify(response.text, heading_style="ATX")
        pdfs, images = extract_media(soup, url)

        # page result
        page_data = {
            "url": url,
            "markdown": markdown,
            "pdfs": pdfs,
            "images": images,
        }

    except Exception as e:
        print("Error:", e)
        return None

    # find internal links for next level crawling
    next_links = []
    for a in soup.find_all("a", href=True):
        next_url = urljoin(url, a["href"])
        if is_internal(base_url, next_url):
            next_links.append((next_url, depth + 1))

    return {"page_data": page_data, "next_links": next_links}


# ---------- STREAMING ENDPOINT ----------

@app.get("/crawl", response_class=StreamingResponse)
def crawl_endpoint(url: str, max_pages: int = MAX_PAGES, max_depth: int = MAX_DEPTH):

    def stream():
        visited = set()
        lock = threading.Lock()

        executor = ThreadPoolExecutor(max_workers=THREADS)
        queue = [(url, 0)]

        while queue and len(visited) < max_pages:
            futures = []

            for (u, d) in queue:
                futures.append(executor.submit(
                    crawl_single, u, d, url, visited, lock, max_depth, max_pages
                ))

            queue = []

            for fut in futures:
                result = fut.result()

                if not result:
                    continue

                # send JSON chunk immediately
                yield json.dumps(result["page_data"]) + "\n"

                if result["next_links"]:
                    queue.extend(result["next_links"])

        executor.shutdown()

    return StreamingResponse(stream(), media_type="application/json")
