# cloud_function/main.py
# Per-listing scraper: saves ALL visible text from each car listing page.
import os, io, time, datetime as dt, requests, re, csv
from typing import List
from bs4 import BeautifulSoup
from google.cloud import storage
from flask import Request, jsonify

# ---- Config (overridable via env vars in deploy.yml) ----
BUCKET_NAME        = os.environ["BUCKET_NAME"]
BASE_SITE          = os.environ.get("BASE_SITE", "https://newhaven.craigslist.org")
SEARCH_PATH        = os.environ.get("SEARCH_PATH", "/search/cta")   # cars+trucks
MAX_PAGES          = int(os.environ.get("MAX_PAGES", "1"))          # search pages to scan
MAX_ITEMS_PER_RUN  = int(os.environ.get("MAX_ITEMS_PER_RUN", "50")) # safety cap per run
DELAY_SECS         = float(os.environ.get("DELAY_SECS", "1.0"))     # polite delay between requests
USER_AGENT         = os.environ.get("USER_AGENT", "UConn-OPIM-Student-Scraper/1.0")

HDRS = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.8"}

# -- Helpers -------------------------------------------------------------------

def _page_url(base: str, path: str, page: int) -> str:
    # Craigslist uses s=<offset> with 120 results/page
    if page == 0:
        return f"{base}{path}?hasPic=1&srchType=T"
    return f"{base}{path}?hasPic=1&srchType=T&s={page*120}"

import re
POST_PAGE_RE = re.compile(r"/(\d+)\.html?$")

def _extract_listing_links(html: str) -> list[str]:
    """Return absolute URLs to individual listings from a search results page.
       Handles classic/new layouts and falls back to regex if needed.
    """
    soup = BeautifulSoup(html, "html.parser")
    links = set()

    # Classic layout
    for a in soup.select("a.result-title, a.result-title.hdrlnk"):
        href = a.get("href")
        if href: links.add(href)

    # Newer layout (cl-static / cl-search)
    for a in soup.select("li.cl-search-result a.titlestring"):
        href = a.get("href")
        if href: links.add(href)

    # Fallback: any anchor that looks like a posting
    for a in soup.select("li.cl-search-result a, .result-row a, a[href$='.html']"):
        href = a.get("href")
        if href and POST_PAGE_RE.search(href):
            links.add(href)

    # Final fallback: regex scan of raw HTML
    # matches absolute or relative post URLs ending with /<post_id>.html
    for m in re.findall(r'href="([^"]+?/\d+\.html)"', html):
        links.add(m)

    # Normalize to absolute
    abs_links = []
    for href in links:
        if href.startswith("//"):
            abs_links.append(f"https:{href}")
        elif href.startswith("/"):
            abs_links.append(f"{BASE_SITE}{href}")
        else:
            abs_links.append(href)

    # keep only post pages (…/<post_id>.html)
    abs_links = [u for u in abs_links if POST_PAGE_RE.search(u)]
    return abs_links


POST_ID_RE = re.compile(r"/(\d+)\.html?$")

def _post_id_from_url(url: str) -> str:
    m = POST_ID_RE.search(url)
    return m.group(1) if m else ""

def _visible_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript", "template"]):
        tag.decompose()
    raw = soup.get_text(separator="\n")
    lines = [ln.strip() for ln in raw.splitlines()]
    lines = [ln for ln in lines if ln and not ln.isspace()]
    dedup = []
    for ln in lines:
        if not dedup or ln != dedup[-1]:
            dedup.append(ln)
    return "\n".join(dedup) + "\n"

def _upload_text(bucket: str, object_name: str, text: str):
    storage.Client().bucket(bucket).blob(object_name)\
        .upload_from_string(text, content_type="text/plain")

def _upload_csv(bucket: str, object_name: str, rows: List[dict], header: List[str]):
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=header)
    w.writeheader()
    w.writerows(rows)
    storage.Client().bucket(bucket).blob(object_name)\
        .upload_from_string(buf.getvalue(), content_type="text/csv")

# -- Entry point ----------------------------------------------------------------

def entrypoint(request: Request):
    """HTTP GET. Optional query overrides:
       ?pages=2&max=40&base=https://hartford.craigslist.org&path=/search/cta
    """
    pages = min(MAX_PAGES, int(request.args.get("pages", MAX_PAGES)))
    max_items = min(MAX_ITEMS_PER_RUN, int(request.args.get("max", MAX_ITEMS_PER_RUN)))
    base = request.args.get("base", BASE_SITE)
    path = request.args.get("path", SEARCH_PATH)

    # 1) Build run folder: YYYYMMDDHHMMSS (UTC)
    run_id = dt.datetime.utcnow().strftime("%Y%m%d%H%M%S")
    run_prefix = f"scrapes/{run_id}"

    # 2) Collect listing links from search pages
    listing_urls = []
    for p in range(pages):
        url = _page_url(base, path, p)
        r = requests.get(url, headers=HDRS, timeout=25)
        r.raise_for_status()
        listing_urls.extend(_extract_listing_links(r.text))
        if p < pages - 1:
            time.sleep(DELAY_SECS)

    # 3) Deduplicate + cap to max_items (classroom safety)
    seen = set()
    urls = []
    for u in listing_urls:
        pid = _post_id_from_url(u)
        if pid and pid not in seen:
            seen.add(pid)
            urls.append((pid, u))
        if len(urls) >= max_items:
            break

    # 4) Fetch each listing page and write one TXT per listing
    index_rows = []
    for i, (pid, u) in enumerate(urls, start=1):
        try:
            r = requests.get(u, headers=HDRS, timeout=25)
            r.raise_for_status()
            text = _visible_text_from_html(r.text)
            obj = f"{run_prefix}/{pid}.txt"
            _upload_text(BUCKET_NAME, obj, text)
            index_rows.append({"post_id": pid, "url": u, "object": obj})
            if i < len(urls):
                time.sleep(DELAY_SECS)
        except Exception as e:
            # record failure in index for transparency
            index_rows.append({"post_id": pid, "url": u, "object": "", "error": str(e)})

    # 5) Write an optional index.csv for the run (handy for ETL later)
    if index_rows:
        header = sorted(index_rows[0].keys())
        _upload_csv(BUCKET_NAME, f"{run_prefix}/index.csv", index_rows, header)

    # after building `listing_urls` and `urls`
    return jsonify({
        "ok": True,
        "run_id": run_id,
        "pages_scanned": pages,
        "candidates_found": len(listing_urls),   # <-- add this
        "items_attempted": len(urls),
        "saved_prefix": run_prefix
    })

