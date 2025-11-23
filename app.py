from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
import requests
import json
import threading

# ---------------- CONFIG ----------------
MAX_PAGES = 10        # maximum pages per crawl
MAX_DEPTH = 2         # how deep to follow links
THREADS = 10          # worker threads
HEADERS = {"User-Agent": "Mozilla/5.0"}
# ----------------------------------------

app = FastAPI(title="Streaming Structured Web Crawler API")


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


def extract_media(soup: BeautifulSoup, base_url: str):
    """Collect PDF and image links."""
    pdfs = []
    images = []

    # PDF links
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if href and href.lower().endswith(".pdf"):
            full = urljoin(base_url, href)
            pdfs.append({"name": full.split("/")[-1], "url": full})

    # Image links
    for img in soup.find_all("img", src=True):
        src = img["src"]
        if not src:
            continue
        lower = src.lower()
        if any(lower.endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg"]):
            full = urljoin(base_url, src)
            images.append({"name": full.split("/")[-1], "url": full})

    return pdfs, images


def extract_links(soup: BeautifulSoup, base_url: str):
    """Collect all non-junk links (for metadata)."""
    links = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if not href:
            continue
        # ignore anchors, js, mailto
        if href.startswith("#") or href.lower().startswith(("javascript:", "mailto:")):
            continue

        full = urljoin(base_url, href)
        text = a.get_text(" ", strip=True)
        links.append({"text": text, "url": full})

    return links


def build_structured_content(soup: BeautifulSoup, url: str):
    """
    Build structured, cleaned content:
    - title
    - clean_text (joined sections)
    - sections: [{heading, content}]
    """
    # Remove obvious noise tags from the tree for cleaner extraction
    for tag_name in ["script", "style", "noscript", "header", "footer", "nav", "form", "aside"]:
        for t in soup.find_all(tag_name):
            t.decompose()

    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()

    body = soup.body or soup  # fallback to soup if no <body>

    sections = []
    current_heading = "Page"
    current_text_parts = []
    seen_paragraphs = set()

    # Iterate through elements in document order
    for elem in body.descendants:
        if not hasattr(elem, "name"):
            continue

        # Headings: start a new section
        if elem.name in {"h1", "h2", "h3"}:
            # Flush previous section
            if current_text_parts:
                content = " ".join(current_text_parts).strip()
                if content:
                    sections.append(
                        {"heading": current_heading.strip(), "content": content}
                    )
                current_text_parts = []

            heading_text = elem.get_text(" ", strip=True)
            if heading_text:
                current_heading = heading_text

        # Paragraphs: main content
        elif elem.name == "p":
            text = elem.get_text(" ", strip=True)
            # basic noise filter: non-empty, some length, not already seen
            if text and len(text.split()) >= 5 and text not in seen_paragraphs:
                seen_paragraphs.add(text)
                current_text_parts.append(text)

    # Flush last section
    if current_text_parts:
        content = " ".join(current_text_parts).strip()
        if content:
            sections.append({"heading": current_heading.strip(), "content": content})

    # Clean text for embedding: heading + content joined
    clean_chunks = []
    for sec in sections:
        if sec["content"]:
            if sec["heading"]:
                clean_chunks.append(sec["heading"])
            clean_chunks.append(sec["content"])
    clean_text = "\n\n".join(clean_chunks)

    return title, clean_text, sections


# ---------- SINGLE PAGE CRAWLER ----------

def crawl_single(url, depth, base_url, visited, lock, max_depth, max_pages):

    # depth / filter checks
    if depth > max_depth:
        return None
    if is_login_page(url) or is_tracking(url):
        return None

    # visited & page limit
    with lock:
        if url in visited or len(visited) >= max_pages:
            return None
        visited.add(url)

    print(f"[Crawling] {url} (depth={depth})")

    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "html.parser")

        pdfs, images = extract_media(soup, url)
        links = extract_links(soup, url)
        title, clean_text, sections = build_structured_content(soup, url)

        page_data = {
            "url": url,
            "title": title,
            "clean_text": clean_text,
            "sections": sections,
            "links": links,
            "pdfs": pdfs,
            "images": images,
        }

    except Exception as e:
        print("Error:", e)
        return None

    # Find internal links for next crawl layer
    next_links = []
    for a in soup.find_all("a", href=True):
        next_url = urljoin(url, a["href"])
        if is_internal(base_url, next_url):
            next_links.append((next_url, depth + 1))

    return {"page_data": page_data, "next_links": next_links}


# ---------- STREAMING ENDPOINT ----------

@app.get("/crawl", response_class=StreamingResponse)
def crawl_endpoint(url: str, max_pages: int = MAX_PAGES, max_depth: int = MAX_DEPTH):
    """
    Stream structured page data for each crawled URL.
    Response: one JSON object per line (NDJSON).
    """

    def stream():
        visited = set()
        lock = threading.Lock()
        executor = ThreadPoolExecutor(max_workers=THREADS)
        queue = [(url, 0)]

        while queue and len(visited) < max_pages:
            futures = [
                executor.submit(
                    crawl_single, u, d, url, visited, lock, max_depth, max_pages
                )
                for (u, d) in queue
            ]

            queue = []

            for fut in futures:
                result = fut.result()
                if not result:
                    continue

                # Send one page as a JSON line
                yield json.dumps(result["page_data"], ensure_ascii=False) + "\n"

                if result["next_links"]:
                    queue.extend(result["next_links"])

        executor.shutdown()

    return StreamingResponse(stream(), media_type="application/json")
