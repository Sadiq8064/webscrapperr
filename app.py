from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
import requests
import json
import re
import threading

# ---------------- CONFIG ----------------
MAX_PAGES = 10
MAX_DEPTH = 2
THREADS = 10
HEADERS = {"User-Agent": "Mozilla/5.0"}
CHUNK_SIZE = 800  # ~800 words per chunk
# ----------------------------------------

app = FastAPI(title="Chunk-Based Web Crawler API")


# ---------------- UTILS ----------------

def is_internal(base, link):
    return urlparse(base).netloc == urlparse(link).netloc


def is_login_page(url):
    return any(k in url.lower() for k in ["login", "signin", "auth", "account"])


def is_tracking(url):
    return any(k in url.lower() for k in ["utm_", "ref=", "tracking", "gclid", "fbclid"])


def chunk_text(text, chunk_size=CHUNK_SIZE):
    words = text.split()
    chunks = []
    for i in range(0, len(words), chunk_size):
        chunk = " ".join(words[i:i + chunk_size]).strip()
        if chunk:
            chunks.append(chunk)
    return chunks


# -------- Extract Media --------

def extract_media(soup, base_url):
    pdfs, images = [], []

    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if href.lower().endswith(".pdf"):
            full = urljoin(base_url, href)
            pdfs.append({"name": full.split("/")[-1], "url": full})

    for img in soup.find_all("img", src=True):
        src = img["src"].lower()
        if any(src.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"]):
            full = urljoin(base_url, src)
            images.append({"name": full.split("/")[-1], "url": full})

    return pdfs, images


# -------- Extract Dynamic API URLs (VERY IMPORTANT) --------

def extract_dynamic_api_urls(html, base_url):
    patterns = [
        r'["\'](\/api\/[^"\']+)["\']',
        r'["\'](\/[A-Za-z0-9\/_-]*Get[A-Za-z0-9\/_-]+)["\']',
        r'["\'](\/[A-Za-z0-9\/_-]*Fetch[A-Za-z0-9\/_-]+)["\']',
        r'["\'](\/[A-Za-z0-9\/_-]*detail[A-Za-z0-9\/_-]+)["\']',
        r'["\'](\/[A-Za-z0-9\/_-]*overview[A-Za-z0-9\/_-]+)["\']',
        r'["\'](\/Course\/[A-Za-z0-9\/_-]+)["\']'
    ]

    api_urls = set()
    regex = re.compile("|".join(patterns), re.IGNORECASE)

    for m in regex.finditer(html):
        full = urljoin(base_url, m.group(1))
        api_urls.add(full)

    return list(api_urls)


def extract_text_from_json(data):
    """
    Recursively grab any text from API JSON responses.
    """
    texts = []

    if isinstance(data, dict):
        for v in data.values():
            texts.extend(extract_text_from_json(v))

    elif isinstance(data, list):
        for item in data:
            texts.extend(extract_text_from_json(item))

    elif isinstance(data, str):
        if len(data.split()) > 3:
            texts.append(data)

    return texts


# -------- Extract Clean HTML Text --------

def extract_clean_html_text(soup):
    for tag in ["script", "style", "nav", "header", "footer", "noscript", "form", "aside"]:
        for t in soup.find_all(tag):
            t.decompose()

    text_parts = []

    # headings
    for h in soup.find_all(["h1", "h2", "h3"]):
        txt = h.get_text(" ", strip=True)
        if txt:
            text_parts.append(txt)

    # paragraphs
    for p in soup.find_all("p"):
        txt = p.get_text(" ", strip=True)
        if txt and len(txt.split()) > 5:
            text_parts.append(txt)

    return "\n".join(text_parts).strip()


# ------------------- SINGLE PAGE SCRAPER -------------------

def crawl_single(url, depth, base_url, visited, lock, max_depth, max_pages):

    if depth > max_depth:
        return None

    if is_login_page(url) or is_tracking(url):
        return None

    with lock:
        if url in visited or len(visited) >= max_pages:
            return None
        visited.add(url)

    print(f"[Crawling] {url}")

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()

        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        # static HTML text
        html_text = extract_clean_html_text(soup)

        # extract PDFs & images
        pdfs, images = extract_media(soup, url)

        # dynamic API URLs
        api_urls = extract_dynamic_api_urls(html, url)

        api_text_parts = []

        # call API endpoints
        for api_url in api_urls:
            try:
                r = requests.get(api_url, headers=HEADERS, timeout=10)
                if r.status_code == 200:
                    data = r.json()
                    api_text_parts.extend(extract_text_from_json(data))
            except:
                continue

        full_text = "\n".join([html_text] + api_text_parts).strip()

        # chunking
        chunks = chunk_text(full_text)

        page_data = {
            "url": url,
            "chunks": chunks,
            "pdfs": pdfs,
            "images": images
        }

    except Exception as e:
        print("Error:", e)
        return None

    # next links
    next_links = []
    for a in soup.find_all("a", href=True):
        next_url = urljoin(url, a["href"])
        if is_internal(base_url, next_url):
            next_links.append((next_url, depth + 1))

    return {"page_data": page_data, "next_links": next_links}


# ------------------- STREAMING ENDPOINT -------------------

@app.get("/crawl", response_class=StreamingResponse)
def crawl_endpoint(url: str, max_pages: int = MAX_PAGES, max_depth: int = MAX_DEPTH):

    def stream():
        visited = set()
        lock = threading.Lock()
        executor = ThreadPoolExecutor(max_workers=THREADS)

        queue = [(url, 0)]

        while queue and len(visited) < max_pages:
            futures = [executor.submit(
                crawl_single, u, d, url, visited, lock, max_depth, max_pages
            ) for (u, d) in queue]

            queue = []

            for fut in futures:
                result = fut.result()
                if not result:
                    continue

                yield json.dumps(result["page_data"], ensure_ascii=False) + "\n"

                if result["next_links"]:
                    queue.extend(result["next_links"])

        executor.shutdown()

    return StreamingResponse(stream(), media_type="application/json")

