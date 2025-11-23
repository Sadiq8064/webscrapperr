from fastapi import FastAPI
from fastapi.responses import StreamingResponse
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
import requests
import json
import threading
import re

# ---------------- CONFIG ----------------
MAX_PAGES = 10        # maximum pages per crawl
MAX_DEPTH = 2         # how deep to follow links
THREADS = 10          # worker threads
HEADERS = {"User-Agent": "Mozilla/5.0"}
MAX_API_CALLS_PER_PAGE = 5  # limit API calls per page for safety
# ----------------------------------------

app = FastAPI(title="Streaming Structured Web Crawler API")


# ---------- UTILITY FUNCTIONS ----------

def is_internal(base, link):
    """Check if link belongs to the same domain."""
    try:
        return urlparse(base).netloc == urlparse(link).netloc
    except Exception:
        return False


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


def json_to_sections(obj, heading_prefix="JSON"):
    """
    Convert arbitrary JSON into section list:
    [{ "heading": ..., "content": ... }]
    Only keeps readable, non-URL-ish text fields (Option B).
    """
    texts_by_key = {}

    # keys that likely represent headings / groups
    HEADING_KEYS = {"title", "heading", "name"}
    CONTENT_KEYS = {
        "description", "overview", "summary", "details",
        "body", "content", "text", "objective", "objectives",
        "outcome", "outcomes", "eligibility", "syllabus"
    }

    def add_text(key_path, text):
        text = text.strip()
        if not text:
            return
        # ignore URLs and super-short fragments
        if text.lower().startswith(("http://", "https://")):
            return
        if len(text.split()) < 5:  # fewer than 5 words = probably noise
            return
        key = key_path or heading_prefix
        texts_by_key.setdefault(key, []).append(text)

    def walk(o, path, last_key=None):
        if isinstance(o, dict):
            for k, v in o.items():
                new_path = f"{path} / {k}" if path else k
                walk(v, new_path, k)
        elif isinstance(o, list):
            for v in o:
                walk(v, path, last_key)
        elif isinstance(o, str):
            # Only keep strings that come from "content-ish" keys or
            # top-level-ish locations. This is Option B: readable text only.
            key_lower = (last_key or "").lower()
            if key_lower in CONTENT_KEYS or key_lower in HEADING_KEYS or not last_key:
                add_text(path, o)

    walk(obj, heading_prefix, None)

    sections = []
    for heading, chunks in texts_by_key.items():
        content = " ".join(chunks).strip()
        if content:
            sections.append({"heading": heading, "content": content})

    return sections


def extract_json_sections_from_scripts(soup: BeautifulSoup, base_url: str):
    """
    Hybrid step 1:
    - Extract JSON-LD and other JSON blobs from <script> tags.
    - Convert them into sections (readable text only).
    """
    sections = []

    # 1) JSON-LD or explicit JSON script tags
    for script in soup.find_all("script"):
        script_type = (script.get("type") or "").lower()
        script_text = script.string or script.get_text()
        if not script_text:
            continue

        # JSON-LD or generic JSON inside script
        if "json" in script_type:
            try:
                data = json.loads(script_text)
                sections.extend(json_to_sections(data, heading_prefix="JSON-LD"))
            except Exception:
                continue
            continue

    # 2) Look for common framework data assignments in generic script tags
    assign_patterns = [
        r"__NEXT_DATA__\s*=\s*(\{.*?\})",
        r"__NUXT__\s*=\s*(\{.*?\})",
        r"__INITIAL_STATE__\s*=\s*(\{.*?\})",
        r"window\.__INITIAL_STATE__\s*=\s*(\{.*?\})",
    ]
    combined_pattern = re.compile("|".join(assign_patterns), re.DOTALL)

    for script in soup.find_all("script"):
        script_type = (script.get("type") or "").lower()
        if "json" in script_type:
            # already processed above
            continue
        script_text = script.string or script.get_text()
        if not script_text:
            continue

        for match in combined_pattern.finditer(script_text):
            for group in match.groups():
                if not group:
                    continue
                candidate = group.strip()
                try:
                    data = json.loads(candidate)
                    sections.extend(json_to_sections(data, heading_prefix="App State"))
                except Exception:
                    continue

    return sections


def extract_api_urls_from_scripts(soup: BeautifulSoup, base_url: str):
    """
    Hybrid step 2:
    - Find JSON API calls in script tags:
      * fetch("...")
      * axios.get("...")
      * $.get("...")
      * any literal "/api/..." URL in scripts
    - Return a set of internal URLs to try as JSON APIs.
    """
    api_urls = set()

    # Classic JS patterns: fetch, axios, $.get, etc.
    pattern = re.compile(
        r"""(?:fetch|axios\.get|axios\.post|axios\(|\$\.getJSON|\$\.get)\s*\(\s*["']([^"']+)["']""",
        re.IGNORECASE,
    )

    # Generic "/api/..." literals anywhere in JS
    api_literal_pattern = re.compile(r"""["'](\/api\/[^"'<>]+)["']""", re.IGNORECASE)

    for script in soup.find_all("script"):
        text = script.string or script.get_text()
        if not text:
            continue

        # fetch/axios/etc
        for match in pattern.finditer(text):
            raw = match.group(1).strip()
            if not raw:
                continue
            full = urljoin(base_url, raw)
            if is_internal(base_url, full):
                api_urls.add(full)

        # bare "/api/..." literals
        for match in api_literal_pattern.finditer(text):
            raw = match.group(1).strip()
            if not raw:
                continue
            full = urljoin(base_url, raw)
            if is_internal(base_url, full):
                api_urls.add(full)

    return list(api_urls)


def fetch_api_json_sections(api_urls, base_url: str):
    """
    For each detected API URL:
    - Fetch via GET
    - If JSON, convert to sections (readable text only).
    """
    sections = []
    count = 0

    for url in api_urls:
        if count >= MAX_API_CALLS_PER_PAGE:
            break
        try:
            resp = requests.get(url, headers=HEADERS, timeout=10)
            content_type = resp.headers.get("Content-Type", "").lower()
            text = resp.text.strip()
            if "json" not in content_type and not text.startswith(("{", "[")):
                continue

            data = resp.json()
            heading_prefix = f"API {urlparse(url).path or '/'}"
            sections.extend(json_to_sections(data, heading_prefix=heading_prefix))
            count += 1
        except Exception:
            continue

    return sections


def sections_to_clean_text(sections):
    """Convert sections into a single clean_text string."""
    chunks = []
    for sec in sections:
        heading = (sec.get("heading") or "").strip()
        content = (sec.get("content") or "").strip()
        if content:
            if heading:
                chunks.append(heading)
            chunks.append(content)
    return "\n\n".join(chunks)


def build_structured_content_from_html(soup: BeautifulSoup, url: str):
    """
    Build structured, cleaned content from static HTML only:
    - title
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

    return title, sections


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

        # 1) PDFs and images
        pdfs, images = extract_media(soup, url)

        # 2) All links (for metadata)
        links = extract_links(soup, url)

        # 3) Hybrid JSON from scripts (inline JSON, framework state)
        json_script_sections = extract_json_sections_from_scripts(soup, url)

        # 4) API URLs from scripts + JSON from APIs
        api_urls = extract_api_urls_from_scripts(soup, url)
        api_sections = fetch_api_json_sections(api_urls, url)

        # 5) Static HTML content sections
        title, html_sections = build_structured_content_from_html(soup, url)

        # 6) Merge sections: HTML + JSON-script + API JSON
        all_sections = html_sections + json_script_sections + api_sections

        # 7) Build final clean_text
        clean_text = sections_to_clean_text(all_sections)

        page_data = {
            "url": url,
            "title": title,
            "clean_text": clean_text,
            "sections": all_sections,
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
