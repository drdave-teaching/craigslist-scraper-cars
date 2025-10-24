# Save ALL visible text from the Craigslist search page(s) into a .txt on GCS.
import os, io, time, datetime as dt, requests
from bs4 import BeautifulSoup
from google.cloud import storage
from flask import Request, jsonify

# ---- Config (can be overridden via deploy.yml env vars) ----
BUCKET_NAME = os.environ["BUCKET_NAME"]
BASE_SITE   = os.environ.get("BASE_SITE", "https://newhaven.craigslist.org")
SEARCH_PATH = os.environ.get("SEARCH_PATH", "/search/cta")   # cars+trucks
MAX_PAGES   = int(os.environ.get("MAX_PAGES", "1"))          # polite: 1 page default
DELAY_SECS  = float(os.environ.get("DELAY_SECS", "1.0"))
USER_AGENT  = os.environ.get("USER_AGENT", "UConn-OPIM-Student-Scraper/1.0")

HDRS = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.8"}

def _page_url(page: int) -> str:
    # Craigslist uses s=<offset> with 120 results/page
    if page == 0:
        return f"{BASE_SITE}{SEARCH_PATH}?hasPic=1&srchType=T"
    return f"{BASE_SITE}{SEARCH_PATH}?hasPic=1&srchType=T&s={page*120}"

def _visible_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # remove non-visible elements
    for tag in soup(["script", "style", "noscript", "template"]):
        tag.decompose()

    # get text; normalize whitespace and drop empty lines
    raw = soup.get_text(separator="\n")
    lines = [ln.strip() for ln in raw.splitlines()]
    # drop very short noise lines
    lines = [ln for ln in lines if ln and not ln.isspace()]
    # collapse consecutive duplicates
    dedup = []
    for ln in lines:
        if not dedup or ln != dedup[-1]:
            dedup.append(ln)
    return "\n".join(dedup) + "\n"

def _upload_text(object_name: str, text: str):
    storage.Client().bucket(BUCKET_NAME).blob(object_name)\
        .upload_from_string(text, content_type="text/plain")

def entrypoint(request: Request):
    """GET ok. Optional overrides: ?pages=2&base=https://hartford.craigslist.org&path=/search/cta"""
    pages = min(MAX_PAGES, int(request.args.get("pages", MAX_PAGES)))
    base  = request.args.get("base", BASE_SITE)
    path  = request.args.get("path", SEARCH_PATH)

    # allow overrides to persist only for this call
    global BASE_SITE, SEARCH_PATH
    BASE_SITE, SEARCH_PATH = base, path

    parts = []
    for p in range(pages):
        url = _page_url(p)
        r = requests.get(url, headers=HDRS, timeout=25)
        r.raise_for_status()
        txt = _visible_text_from_html(r.text)
        # header to mark which page the text came from
        parts.append(f"===== PAGE {p+1} | {url} =====\n{txt}")
        if p < pages - 1:
            time.sleep(DELAY_SECS)

    out_text = "\n".join(parts)
    ts = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    object_name = f"scrapes/page_text_{ts}.txt"
    _upload_text(object_name, out_text)

    return jsonify({"ok": True, "pages": pages, "txt": object_name})
